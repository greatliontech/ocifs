//go:build linux || darwin

package projection

import (
	"archive/tar"
	"fmt"
	"hash/fnv"
	"maps"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// Merged is the writable projection's resolution kernel: the
// immutable view kernel composed with a live upper, presenting the
// merge (writable.md REQ-writable-presented) behind the same
// operations the read kernel serves. The on-disk upper is the single
// source of truth; the in-memory index is a cache rebuilt from a
// walk alone (projection.md REQ-proj-upper-truth) — Refresh rebuilds
// it after upper mutation, already-materialized nodes stay
// point-in-time captures, and every new resolution reads the current
// index.
//
// The merge assumes an all-presenting fidelity envelope (FUSE,
// FSKit): every view entry the inner kernel classified is presented,
// so a shadowed path's view identity always exists. ProjFS's
// OS-native write model does not consume this kernel.
type Merged struct {
	inner     *Projection
	upperRoot string
	root      *Node

	mu  sync.RWMutex
	idx *upperIndex

	// write is present on writable merges (NewMergedWritable); nil
	// means every mutating operation returns ErrReadOnly.
	write *writeState
	// wmu serializes mutating operations end to end — dialect steps
	// and index maintenance; readers only ever block on mu's brief
	// index critical sections, never on write-path I/O.
	wmu sync.Mutex
}

// upperIndex is one immutable snapshot of the walked upper state
// plus derived per-directory listings; Refresh swaps the whole
// value.
type upperIndex struct {
	st *upper.State
	// kids maps a directory path ("." for the upper root) to its
	// sorted entry-child basenames.
	kids map[string][]string
	// wh maps a directory path to its sorted whiteout-child
	// basenames — verifier input; whiteouts also occlude through
	// st.Whiteouts.
	wh map[string][]string
}

// NewMerged composes the view kernel with the upper at upperRoot,
// walking it once; the caller Refreshes after external mutation.
func NewMerged(inner *Projection, upperRoot string) (*Merged, error) {
	// The merge assumes the all-presenting byte-order envelope; a
	// folding comparator or name filter would let shadow suppression
	// (exact-path) and lookup (comparator-equal) diverge. Enforced,
	// not just assumed.
	c := inner.Capabilities()
	if c.Compare != nil || c.ValidName != nil || !c.Symlinks || !c.FIFOs || !c.Devices {
		return nil, fmt.Errorf("merged projection requires an all-presenting byte-order envelope: %w", ErrNotSupported)
	}
	m := &Merged{
		inner:     inner,
		upperRoot: upperRoot,
	}
	m.root = &Node{
		id:   RootID,
		kind: KindDir,
		path: ".",
		name: ".",
		view: inner.Root(),
	}
	if err := m.Refresh(); err != nil {
		return nil, err
	}
	return m, nil
}

// Refresh rebuilds the in-memory upper index from a walk of the
// upper alone (REQ-proj-upper-truth): the invalidation point after
// upper mutation. Snapshots and nodes materialized earlier are
// unaffected; new resolutions observe the new state.
func (m *Merged) Refresh() error {
	st, err := upper.Walk(m.upperRoot)
	if err != nil {
		return err
	}
	idx := &upperIndex{
		st:   st,
		kids: map[string][]string{},
		wh:   map[string][]string{},
	}
	for p := range st.Entries {
		d, b := splitParent(p)
		idx.kids[d] = append(idx.kids[d], b)
	}
	for p := range st.Whiteouts {
		d, b := splitParent(p)
		idx.wh[d] = append(idx.wh[d], b)
	}
	for _, l := range idx.kids {
		sort.Strings(l)
	}
	for _, l := range idx.wh {
		sort.Strings(l)
	}
	m.mu.Lock()
	m.idx = idx
	m.mu.Unlock()
	return nil
}

// Readers hold mu.RLock for the duration of a resolution — the
// write path mutates the index in place under mu.Lock, so a
// resolution observes one consistent index state. Nodes and
// snapshots copy what they keep; nothing references index maps
// after the lock drops.

// index returns the current index for quiescent callers (tests):
// the pointer is only safe to use while no writer runs.
func (m *Merged) index() *upperIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.idx
}

func splitParent(p string) (dir, base string) {
	d, b := path.Split(p)
	d = strings.TrimSuffix(d, "/")
	if d == "" {
		d = "."
	}
	return d, b
}

func childPath(dir, name string) string {
	if dir == "." {
		return name
	}
	return dir + "/" + name
}

// baseDead reports whether the base entry at p is occluded outright:
// its own whiteout, or an ancestor marker or opaque. A root opaque
// is unrepresentable — the walker refuses a marker at the upper
// root — so no root arm exists.
func baseDead(idx *upperIndex, p string) bool {
	return idx.st.OccludesBase(p)
}

// Node is one presented entry of the merge. Immutable after
// materialization — a point-in-time capture; identity is
// deterministic so re-resolution after Refresh yields the same ID
// for the same live object. Upper-backed nodes pin their inode open
// (REQ-proj-identity's recycling guard); Close releases the pin.
type Node struct {
	id   ID
	kind Kind
	path string
	name string
	// view is the inner kernel's entry when the base contributes:
	// the whole presentation for base-presented nodes, identity and
	// base children for shadowed ones. Nil for upper-born nodes.
	view *Entry
	// up is the upper's presented entry when the upper contributes;
	// its attributes shadow the base's entirely. Nil for
	// base-presented nodes.
	up  *upper.Entry
	pin *os.File
}

func (n *Node) ID() ID       { return n.id }
func (n *Node) Kind() Kind   { return n.kind }
func (n *Node) Path() string { return n.path }
func (n *Node) Name() string { return n.name }

// UpperBacked reports whether the node's presentation (attributes,
// content) comes from the upper.
func (n *Node) UpperBacked() bool { return n.up != nil }

// HostPath returns the upper node backing an upper-backed entry
// ("" for base-presented nodes). The pin (Pin) keeps the inode
// alive even if the path is unlinked afterwards.
func (n *Node) HostPath() string {
	if n.up == nil {
		return ""
	}
	return n.up.HostPath
}

// Pin returns the open handle pinning an upper-backed node's inode,
// nil when no pin is held. On linux the pin is an O_PATH descriptor
// usable for reopening content whatever happens to the path.
func (n *Node) Pin() *os.File { return n.pin }

// Close releases the inode pin. The node stays readable as a
// record; content access through a recycled inode is no longer
// guarded. A node has a single owner: Close is not safe to race
// with itself or with Pin.
func (n *Node) Close() error {
	if n.pin == nil {
		return nil
	}
	err := n.pin.Close()
	n.pin = nil
	return err
}

// Header returns the node's presented attribute record: the view
// header for base-presented nodes, a header synthesized from the
// upper's presented truth (overrides resolved) for upper-backed
// ones.
func (n *Node) Header() tar.Header {
	if n.up == nil {
		return n.view.Header()
	}
	h := tar.Header{
		Name:    n.path,
		Mode:    int64(n.up.Mode),
		Uid:     n.up.UID,
		Gid:     n.up.GID,
		ModTime: n.up.ModTime,
		Size:    n.up.Size,
	}
	switch n.up.Kind {
	case upper.KindFile:
		h.Typeflag = tar.TypeReg
	case upper.KindDir:
		h.Typeflag = tar.TypeDir
	case upper.KindSymlink:
		h.Typeflag = tar.TypeSymlink
		h.Linkname = n.up.Target
	case upper.KindFifo:
		h.Typeflag = tar.TypeFifo
	case upper.KindCharDev:
		h.Typeflag = tar.TypeChar
		h.Devmajor = int64(n.up.Rdev.Major)
		h.Devminor = int64(n.up.Rdev.Minor)
	case upper.KindBlockDev:
		h.Typeflag = tar.TypeBlock
		h.Devmajor = int64(n.up.Rdev.Major)
		h.Devminor = int64(n.up.Rdev.Minor)
	case upper.KindSocket:
		// The tar vocabulary has no socket type; Kind() carries the
		// truth and the header stays a plain attribute record.
	}
	return h
}

// Xattrs returns the node's presented extended attributes: the
// upper's resolved set for upper-backed nodes; for base-presented
// nodes the recorded attributes with the machinery namespace
// stripped — reserved names on base content are inert
// (writable.md REQ-writable-reserved).
func (n *Node) Xattrs() map[string]string {
	if n.up != nil {
		return maps.Clone(n.up.Xattrs)
	}
	h := n.view.Header()
	return upper.PresentedBaseXattrs(&h)
}

// ContentDigest returns a base-presented file's content-CAS key;
// zero for upper-backed nodes, whose content is HostPath's.
func (n *Node) ContentDigest() v1.Hash {
	if n.up != nil || n.view == nil {
		return v1.Hash{}
	}
	return n.view.ContentDigest()
}

// LinkTarget returns a symlink's verbatim target.
func (n *Node) LinkTarget() string {
	if n.up != nil {
		return n.up.Target
	}
	return n.view.LinkTarget()
}

// Nlink returns the presented link count: the upper inode's count
// for upper-backed entries, 1 for base entries (the read path's
// independent-node rule, REQ-writable-presented).
func (n *Node) Nlink() uint64 {
	if n.up != nil {
		return n.up.Nlink
	}
	return 1
}

// Root returns the merge's root node: ID 2 always. Attributes are
// the base root's until the upper's root record exists
// (writable.md REQ-writable-dialect), then the record's — a
// shadow-in-place of the fixed root identity. No pin: the root's
// identity never derives from an inode.
func (m *Merged) Root() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.idx.st.Root != nil {
		r := *m.idx.st.Root
		return &Node{id: RootID, kind: KindDir, path: ".", name: ".", view: m.inner.Root(), up: &r}
	}
	return m.root
}

// Lookup resolves name within dir against the current merge:
// an upper entry shadows the base entry at its path entirely, a
// whiteout occludes the base subtree, an opaque directory presents
// only upper content beneath it, everything else presents the base
// (REQ-writable-presented). ok=false means not presented; a non-nil
// error is an envelope or upper-access failure. Upper-backed
// results hold an inode pin — the caller owns Close.
func (m *Merged) Lookup(dir *Node, name string) (*Node, bool, error) {
	if dir.kind != KindDir {
		return nil, false, nil
	}
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return nil, false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := m.idx
	p := childPath(dir.path, name)
	if ue, ok := idx.st.Entries[p]; ok {
		n, err := m.materialize(p, name, ue, idx, true)
		if err != nil {
			return nil, false, err
		}
		return n, true, nil
	}
	if baseDead(idx, p) {
		return nil, false, nil
	}
	if dir.view == nil || dir.view.Kind() != KindDir {
		return nil, false, nil
	}
	ve, ok := m.inner.Lookup(dir.view, name)
	if !ok {
		return nil, false, nil
	}
	return &Node{id: ve.ID(), kind: ve.Kind(), path: ve.Path(), name: ve.Name(), view: ve}, true, nil
}

func kindFromUpper(k upper.Kind) Kind {
	switch k {
	case upper.KindFile:
		return KindFile
	case upper.KindDir:
		return KindDir
	case upper.KindSymlink:
		return KindSymlink
	case upper.KindFifo:
		return KindFIFO
	case upper.KindCharDev:
		return KindCharDevice
	case upper.KindBlockDev:
		return KindBlockDevice
	case upper.KindSocket:
		return KindSocket
	}
	return KindFile
}

// materialize builds the presented node for an upper entry at p,
// deciding identity per REQ-proj-identity: a shadow-in-place keeps
// the view-derived ID (the base entry is visible modulo the shadow
// and the entry is not hardlink-migrated); everything else derives
// upper-born identity from the upper inode, failing loudly outside
// the partition. pin controls whether the upper inode is pinned
// open (Lookup pins; enumeration snapshots do not).
func (m *Merged) materialize(p, name string, ue upper.Entry, idx *upperIndex, pin bool) (*Node, error) {
	var pinFile *os.File
	var pinnedIno uint64
	if pin {
		// Pin before deriving identity: the pin is opened by path,
		// so the inode it holds — not the walked snapshot's — is
		// the one the recycling guard protects, and the ID must
		// name it (REQ-proj-identity). A stand-in is a regular
		// host file whatever kind it presents.
		pinKind := ue.Kind
		if ue.StandIn {
			pinKind = upper.KindFile
		}
		f, err := openPin(ue.HostPath, pinKind)
		if err != nil {
			return nil, fmt.Errorf("pinning upper %q: %w", p, err)
		}
		pinFile = f
		if f != nil {
			var st unix.Stat_t
			if err := unix.Fstat(int(f.Fd()), &st); err != nil {
				f.Close()
				return nil, fmt.Errorf("pinned upper %q: %w", p, err)
			}
			pinnedIno = st.Ino
		}
	}
	id, view, err := m.presentedID(p, ue, pinnedIno, idx)
	if err != nil {
		if pinFile != nil {
			pinFile.Close()
		}
		return nil, err
	}
	return &Node{
		id:   id,
		kind: kindFromUpper(ue.Kind),
		path: p,
		name: name,
		view: view,
		up:   &ue,
		pin:  pinFile,
	}, nil
}

// presentedID resolves the identity for the upper entry at p, and
// the view entry when the identity is view-derived.
func (m *Merged) presentedID(p string, ue upper.Entry, pinnedIno uint64, idx *upperIndex) (ID, *Entry, error) {
	// Hardlink migration: any linkable kind sharing its upper inode
	// carries the group's upper-born identity (REQ-writable-hardlink
	// has no kind restriction); directories alone carry nlink >= 2
	// without being linkable.
	linked := ue.Kind != upper.KindDir && ue.Nlink > 1
	if !baseDead(idx, p) && !linked {
		if ve, ok := m.viewAt(p); ok {
			// Shadow-in-place: content or metadata replacing the
			// same logical object keeps the logical identity.
			return ve.ID(), ve, nil
		}
	}
	// The pinned inode is the identity source when a pin is held:
	// the ID must name the inode the pin guards, not the walked
	// snapshot's — a rename-over between walk and pin would
	// otherwise leave the guarded number unreferenced and mintable
	// for a second live object.
	ino := ue.Ino
	if pinnedIno != 0 {
		ino = pinnedIno
	}
	if ino >= uint64(upperIDBase) {
		return 0, nil, fmt.Errorf("upper inode %d for %q: %w", ino, p, ErrIdentityRange)
	}
	return upperIDBase | ID(ino), nil, nil
}

// viewAt resolves a cleaned view path in the inner kernel.
func (m *Merged) viewAt(p string) (*Entry, bool) {
	cur := m.inner.Root()
	if p == "." {
		return cur, true
	}
	for _, part := range strings.Split(p, "/") {
		next, ok := m.inner.Lookup(cur, part)
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

// DirEntry is one row of an enumeration snapshot: enough for a
// backend's readdir without materializing (or pinning) the child.
type DirEntry struct {
	Name string
	ID   ID
	Kind Kind
}

// DirSnapshot is an immutable, comparator-sorted enumeration of one
// merged directory (REQ-proj-enumeration): concurrent enumerations
// never disturb each other and positions resume exactly. The
// verifier derives from the upper directory's state — constant
// while the upper leaves the directory alone, different after
// membership-changing mutation.
type DirSnapshot struct {
	entries  []DirEntry
	verifier uint64
	cmp      func(a, b string) int
}

func (s *DirSnapshot) Len() int            { return len(s.entries) }
func (s *DirSnapshot) At(i int) DirEntry   { return s.entries[i] }
func (s *DirSnapshot) Verifier() uint64    { return s.verifier }
func (s *DirSnapshot) Entries() []DirEntry { return s.entries }

// Seek returns the index of the first entry ordered strictly after
// name — the resume position for an enumeration that last returned
// name.
func (s *DirSnapshot) Seek(name string) int {
	return sort.Search(len(s.entries), func(i int) bool {
		return s.cmp(s.entries[i].Name, name) > 0
	})
}

// OpenDir builds the enumeration snapshot for dir: base children
// and upper children merged under the backend comparator, markers
// and machinery invisible, occlusion applied
// (REQ-writable-presented).
func (m *Merged) OpenDir(dir *Node) (*DirSnapshot, error) {
	if dir.kind != KindDir {
		return nil, ErrNotDir
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := m.idx
	cmp := m.inner.caps.compare

	var rows []DirEntry
	for _, name := range idx.kids[dir.path] {
		p := childPath(dir.path, name)
		ue := idx.st.Entries[p]
		id, _, err := m.presentedID(p, ue, 0, idx)
		if err != nil {
			return nil, err
		}
		rows = append(rows, DirEntry{Name: name, ID: id, Kind: kindFromUpper(ue.Kind)})
	}
	if dir.view != nil && dir.view.Kind() == KindDir {
		for _, ve := range dir.view.Children() {
			p := ve.Path()
			if _, shadowed := idx.st.Entries[p]; shadowed {
				continue
			}
			if baseDead(idx, p) {
				continue
			}
			rows = append(rows, DirEntry{Name: ve.Name(), ID: ve.ID(), Kind: ve.Kind()})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return cmp(rows[i].Name, rows[j].Name) < 0 })

	return &DirSnapshot{
		entries:  rows,
		verifier: dirVerifier(dir.path, idx),
		cmp:      cmp,
	}, nil
}

// dirVerifier hashes the upper's contribution to one directory's
// listing — entry names with their inode identities, whiteout
// names, the opaque flag — so the verifier changes exactly when the
// upper mutates the listing (REQ-proj-enumeration). A kind change
// cannot happen without an inode change, so the inode carries both;
// attribute-only mutation of children moves nothing, and the
// immutable base contribution needs no hashing. fnv64a is not
// collision-resistant: a crafted collision costs only a missed
// cookie invalidation for the mutator who crafted it.
func dirVerifier(dirPath string, idx *upperIndex) uint64 {
	h := fnv.New64a()
	if idx.st.Opaque[dirPath] {
		h.Write([]byte("opq\x00"))
	}
	for _, name := range idx.kids[dirPath] {
		ue := idx.st.Entries[childPath(dirPath, name)]
		fmt.Fprintf(h, "e\x00%s\x00%d\x00", name, ue.Ino)
	}
	for _, name := range idx.wh[dirPath] {
		fmt.Fprintf(h, "w\x00%s\x00", name)
	}
	return h.Sum64()
}
