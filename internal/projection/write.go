//go:build linux || darwin

package projection

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// The write engine: provider-mediated mutation of the merge
// (writable.md §Write semantics). Every operation is a sequence of
// dialect steps whose orderings keep each crash prefix a coherent
// tree (REQ-writable-crash), followed by an exact incremental index
// update — the index stays a cache a fresh Walk reproduces
// (REQ-proj-upper-truth; the property test asserts the equality).
// Writes serialize on wmu; index updates take mu.Lock briefly so
// readers never block on write-path I/O.

// writeState is present on writable merges only.
type writeState struct {
	w *upper.Writer
	// content resolves a base entry's digest to its CAS bytes for
	// copy-up.
	content func(v1.Hash) (io.ReadCloser, error)
}

// NewMergedWritable composes a writable merge: the read kernel plus
// the dialect writer and the copy-up content source.
func NewMergedWritable(inner *Projection, upperRoot string, content func(v1.Hash) (io.ReadCloser, error)) (*Merged, error) {
	m, err := NewMerged(inner, upperRoot)
	if err != nil {
		return nil, err
	}
	m.write = &writeState{w: upper.NewWriter(upperRoot), content: content}
	return m, nil
}

func (m *Merged) writable() error {
	if m.write == nil {
		return ErrReadOnly
	}
	return nil
}

// checkChildName refuses names the mount surface must not accept:
// non-names, and the dialect's reserved marker namespace
// (REQ-writable-reserved).
func checkChildName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return ErrNotFound
	}
	if strings.HasPrefix(name, upper.WhiteoutPrefix) {
		return ErrReserved
	}
	return nil
}

// ---- index maintenance (all under m.mu.Lock) ----

func insertSorted(l []string, s string) []string {
	i := sort.SearchStrings(l, s)
	if i < len(l) && l[i] == s {
		return l
	}
	l = append(l, "")
	copy(l[i+1:], l[i:])
	l[i] = s
	return l
}

func removeSorted(l []string, s string) []string {
	i := sort.SearchStrings(l, s)
	if i < len(l) && l[i] == s {
		return append(l[:i], l[i+1:]...)
	}
	return l
}

func (idx *upperIndex) setEntry(e upper.Entry) {
	if _, ok := idx.st.Entries[e.Path]; !ok {
		d, b := splitParent(e.Path)
		idx.kids[d] = insertSorted(idx.kids[d], b)
	}
	idx.st.Entries[e.Path] = e
}

func (idx *upperIndex) delEntry(p string) {
	if _, ok := idx.st.Entries[p]; ok {
		delete(idx.st.Entries, p)
		d, b := splitParent(p)
		idx.kids[d] = removeSorted(idx.kids[d], b)
	}
}

func (idx *upperIndex) setWhiteout(p string) {
	if !idx.st.Whiteouts[p] {
		idx.st.Whiteouts[p] = true
		d, b := splitParent(p)
		idx.wh[d] = insertSorted(idx.wh[d], b)
	}
}

func (idx *upperIndex) delWhiteout(p string) {
	if idx.st.Whiteouts[p] {
		delete(idx.st.Whiteouts, p)
		d, b := splitParent(p)
		idx.wh[d] = removeSorted(idx.wh[d], b)
	}
}

// reindex re-stats the given upper paths and applies the results —
// the op's exact-effect maintenance. "." re-reads the root record.
// A stat failure aborts all-or-nothing: the index stays at its
// prior coherent state until the next op on the path re-stats (or a
// Refresh rebuilds) — accepted staleness, never a torn update.
func (m *Merged) reindex(paths ...string) error {
	type upd struct {
		p  string
		e  upper.Entry
		ok bool
	}
	var us []upd
	var root *upper.Entry
	var rootTouched bool
	seen := map[string]bool{}
	for _, p := range paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		if p == "." {
			r, err := upper.StatRoot(m.upperRoot)
			if err != nil {
				return err
			}
			root, rootTouched = r, true
			continue
		}
		e, ok, err := upper.Stat(m.upperRoot, p)
		if err != nil {
			return err
		}
		us = append(us, upd{p: p, e: e, ok: ok})
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range us {
		if u.ok {
			m.idx.setEntry(u.e)
		} else {
			m.idx.delEntry(u.p)
		}
	}
	if rootTouched {
		m.idx.st.Root = root
	}
	return nil
}

func (m *Merged) markWhiteout(p string) {
	m.mu.Lock()
	m.idx.setWhiteout(p)
	m.mu.Unlock()
}

// ---- presented-truth helpers ----

// baseVisibleAt reports whether the base contributes a visible
// entry at p under the current index (writable.md's base-visible
// rule — shadowing does not occlude).
func (m *Merged) baseVisibleAt(p string) bool {
	if _, ok := m.viewAt(p); !ok {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !baseDead(m.idx, p)
}

// presentedTime normalizes an unrecorded time to the epoch.
func presentedTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Unix(0, 0)
	}
	return t
}

// ---- spine materialization (REQ-writable-copyup's ancestor arm) ----

// ensureSpine materializes every missing ancestor of p in the upper
// with its presented attributes, returning the created directory
// paths (deepest last) for time restoration and reindexing. The
// direct parent's modification time is the caller's concern: a
// logically mutated parent keeps its natural new time, an
// implementation-only spine dir is restored to presented truth.
func (m *Merged) ensureSpine(p string) ([]string, error) {
	d := path.Dir(p)
	if d == "." {
		return nil, nil
	}
	var parts []string
	for d != "." {
		parts = append([]string{d}, parts...)
		d = path.Dir(d)
	}
	var made []string
	for _, a := range parts {
		e, ok, err := upper.Stat(m.upperRoot, a)
		if err != nil {
			return made, err
		}
		if ok {
			if e.Kind != upper.KindDir {
				return made, fmt.Errorf("ancestor %q: %w", a, ErrNotDir)
			}
			continue
		}
		ve, ok := m.viewAt(a)
		if !ok || ve.Kind() != KindDir {
			return made, fmt.Errorf("ancestor %q not a presented directory: %w", a, ErrNotFound)
		}
		h := ve.Header()
		// Fully formed before the rename — a crashed spine step
		// leaves no half-attributed directory (REQ-writable-copyup).
		if err := m.write.w.PublishDir(a, uint32(h.Mode)&0o7777, h.Uid, h.Gid, presentedTime(h.ModTime), upper.PresentedBaseXattrs(&h)); err != nil {
			return made, err
		}
		made = append(made, a)
	}
	return made, nil
}

// restoreSpineTimes restores presented modification times on
// implementation-materialized spine dirs — after the target
// mutation, so child churn cannot move them again.
func (m *Merged) restoreSpineTimes(made []string, skip string) error {
	for _, a := range made {
		if a == skip {
			continue
		}
		ve, ok := m.viewAt(a)
		if !ok {
			continue
		}
		if err := m.write.w.SetTimes(a, presentedTime(ve.Header().ModTime)); err != nil {
			return err
		}
	}
	return nil
}

// setEntryXattr stores one presented xattr on an upper node,
// escaping when the host refuses (REQ-writable-fidelity).
func (m *Merged) setEntryXattr(rel, name, val string) error {
	host := filepath.Join(m.upperRoot, filepath.FromSlash(rel))
	err := unix.Lsetxattr(host, name, []byte(val), 0)
	if err == nil {
		return nil
	}
	if err == unix.EPERM || err == unix.EACCES || err == unix.ENOTSUP || err == unix.EOPNOTSUPP || err == unix.EINVAL {
		return m.write.w.SetEscapedXattr(rel, name, []byte(val))
	}
	return &os.PathError{Op: "lsetxattr", Path: rel + ":" + name, Err: err}
}

// ---- copy-up (REQ-writable-copyup) ----

// copyUpEntry materializes the base entry at p into the upper —
// content from the CAS, recorded attributes preserved, overrides
// where the host refuses — atomically: the published node appears
// fully formed or not at all. limit caps copied file bytes (for
// truncate); < 0 copies everything.
func (m *Merged) copyUpEntry(p string, ve *Entry, limit int64) error {
	h := ve.Header()
	xattrs := upper.PresentedBaseXattrs(&h)
	ownerRec := ""
	if h.Uid != os.Geteuid() || h.Gid != os.Getegid() {
		ownerRec = fmt.Sprintf("%d:%d", h.Uid, h.Gid)
	}
	mtime := presentedTime(h.ModTime)
	w := m.write.w
	switch ve.Kind() {
	case KindFile:
		rc, err := m.write.content(ve.ContentDigest())
		if err != nil {
			return err
		}
		defer rc.Close()
		var r io.Reader = rc
		if limit >= 0 {
			// A size-bounded copy-up serves truncate: the size is
			// changing, so the modification time is now — never the
			// base's (POSIX marks mtime on truncate).
			r = io.LimitReader(rc, limit)
			mtime = time.Time{}
		}
		if ownerRec != "" {
			if xattrs == nil {
				xattrs = map[string]string{}
			}
			xattrs[upper.XattrOwner] = ownerRec
		}
		return w.PublishFile(p, r, uint32(h.Mode)&0o7777, mtime, xattrs)
	case KindDir:
		// Fully formed on the temporary, one rename — directories
		// copy up as atomically as files (REQ-writable-copyup).
		return w.PublishDir(p, uint32(h.Mode)&0o7777, h.Uid, h.Gid, mtime, xattrs)
	case KindSymlink:
		if ownerRec == "" && len(xattrs) == 0 {
			return w.PublishSymlink(ve.LinkTarget(), p, mtime)
		}
		return w.MakeStandIn(p, upper.KindSymlink, ve.LinkTarget(), upper.Rdev{}, 0o777, h.Uid, h.Gid, mtime, xattrs)
	case KindFIFO:
		if ownerRec == "" && len(xattrs) == 0 {
			return w.PublishFifo(p, uint32(h.Mode)&0o7777, mtime)
		}
		return w.MakeStandIn(p, upper.KindFifo, "", upper.Rdev{}, uint32(h.Mode)&0o7777, h.Uid, h.Gid, mtime, xattrs)
	case KindCharDevice:
		return w.MakeStandIn(p, upper.KindCharDev, "", upper.Rdev{Major: uint32(h.Devmajor), Minor: uint32(h.Devminor)}, uint32(h.Mode)&0o7777, h.Uid, h.Gid, mtime, xattrs)
	case KindBlockDevice:
		return w.MakeStandIn(p, upper.KindBlockDev, "", upper.Rdev{Major: uint32(h.Devmajor), Minor: uint32(h.Devminor)}, uint32(h.Mode)&0o7777, h.Uid, h.Gid, mtime, xattrs)
	}
	return fmt.Errorf("copy-up of kind %v: %w", ve.Kind(), ErrNotSupported)
}

// ensureUpper copies a presented entry up if the upper does not
// hold it yet, spine included; idempotent. Returns whether a
// copy-up happened and the spine dirs made.
func (m *Merged) ensureUpper(p string, limit int64) (bool, []string, error) {
	if _, ok, err := upper.Stat(m.upperRoot, p); err != nil {
		return false, nil, err
	} else if ok {
		return false, nil, nil
	}
	ve, ok := m.viewAt(p)
	if !ok {
		return false, nil, ErrNotFound
	}
	made, err := m.ensureSpine(p)
	if err != nil {
		return false, made, err
	}
	if err := m.copyUpEntry(p, ve, limit); err != nil {
		return false, made, err
	}
	return true, made, nil
}

// finishOp restores spine times and reindexes everything an op
// touched.
func (m *Merged) finishOp(made []string, skipTime string, paths ...string) error {
	if err := m.restoreSpineTimes(made, skipTime); err != nil {
		return err
	}
	return m.reindex(append(made, paths...)...)
}

// ---- mutating operations ----

// Create publishes an empty regular file at name under dir and
// returns the node plus an open read-write handle on the upper
// file. The parent directory's natural new modification time is
// kept (POSIX); other spine dirs restore presented times.
func (m *Merged) Create(dir *Node, name string, mode uint32) (*Node, *os.File, error) {
	if err := m.writable(); err != nil {
		return nil, nil, err
	}
	if err := checkChildName(name); err != nil {
		return nil, nil, err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if ex, ok, err := m.Lookup(dir, name); err != nil {
		return nil, nil, err
	} else if ok {
		ex.Close()
		return nil, nil, ErrExists
	}
	p := childPath(dir.path, name)
	made, err := m.ensureSpine(p)
	if err != nil {
		return nil, nil, err
	}
	if err := m.write.w.PublishFile(p, nil, mode, time.Time{}, nil); err != nil {
		return nil, nil, err
	}
	if err := m.finishOp(made, path.Dir(p), p, path.Dir(p)); err != nil {
		return nil, nil, err
	}
	n, ok, err := m.Lookup(dir, name)
	if err != nil || !ok {
		return nil, nil, fmt.Errorf("created %q not presented: %w", p, ErrIO)
	}
	f, err := os.OpenFile(n.HostPath(), os.O_RDWR, 0)
	if err != nil {
		n.Close()
		return nil, nil, err
	}
	return n, f, nil
}

// Mkdir creates a directory at name under dir; over a whiteout it
// creates beside the marker (REQ-writable-delete's recreation rule).
func (m *Merged) Mkdir(dir *Node, name string, mode uint32) (*Node, error) {
	if err := m.writable(); err != nil {
		return nil, err
	}
	if err := checkChildName(name); err != nil {
		return nil, err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if ex, ok, err := m.Lookup(dir, name); err != nil {
		return nil, err
	} else if ok {
		ex.Close()
		return nil, ErrExists
	}
	p := childPath(dir.path, name)
	made, err := m.ensureSpine(p)
	if err != nil {
		return nil, err
	}
	if err := m.write.w.Mkdir(p, mode); err != nil {
		return nil, err
	}
	if err := m.finishOp(made, path.Dir(p), p, path.Dir(p)); err != nil {
		return nil, err
	}
	n, ok, err := m.Lookup(dir, name)
	if err != nil || !ok {
		return nil, fmt.Errorf("created %q not presented: %w", p, ErrIO)
	}
	return n, nil
}

// Symlink creates a symlink at name under dir.
func (m *Merged) Symlink(dir *Node, name, target string) (*Node, error) {
	if err := m.writable(); err != nil {
		return nil, err
	}
	if err := checkChildName(name); err != nil {
		return nil, err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if ex, ok, err := m.Lookup(dir, name); err != nil {
		return nil, err
	} else if ok {
		ex.Close()
		return nil, ErrExists
	}
	p := childPath(dir.path, name)
	made, err := m.ensureSpine(p)
	if err != nil {
		return nil, err
	}
	if err := m.write.w.Symlink(target, p); err != nil {
		return nil, err
	}
	if err := m.finishOp(made, path.Dir(p), p, path.Dir(p)); err != nil {
		return nil, err
	}
	n, ok, err := m.Lookup(dir, name)
	if err != nil || !ok {
		return nil, fmt.Errorf("created %q not presented: %w", p, ErrIO)
	}
	return n, nil
}

// Unlink removes the non-directory at name under dir: a whiteout
// for base-visible content (marker before removal, so no crash
// prefix resurrects the base), plain removal for upper-only
// entries (REQ-writable-delete).
func (m *Merged) Unlink(dir *Node, name string) error {
	if err := m.writable(); err != nil {
		return err
	}
	if err := checkChildName(name); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	n, ok, err := m.Lookup(dir, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	defer n.Close()
	if n.Kind() == KindDir {
		return ErrNotDir
	}
	p := n.path
	baseVisible := m.baseVisibleAt(p)
	hasUpper := n.UpperBacked()

	var made []string
	if baseVisible {
		// Marker first: every crash prefix keeps the base hidden or
		// the old entry present, never a resurrected base.
		made, err = m.ensureSpine(p)
		if err != nil {
			return err
		}
		if err := m.write.w.Whiteout(p); err != nil {
			return err
		}
		m.markWhiteout(p)
	}
	if hasUpper {
		if err := m.write.w.Remove(p); err != nil {
			return err
		}
	}
	return m.finishOp(made, path.Dir(p), p, path.Dir(p))
}

// Rmdir removes the directory at name under dir: the merged
// directory must present empty; a base-visible directory earns its
// whiteout before the upper directory dismantles beneath it
// (REQ-writable-delete's ordering — hide, then dismantle).
func (m *Merged) Rmdir(dir *Node, name string) error {
	if err := m.writable(); err != nil {
		return err
	}
	if err := checkChildName(name); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	n, ok, err := m.Lookup(dir, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	defer n.Close()
	if n.Kind() != KindDir {
		return ErrNotDir
	}
	snap, err := m.OpenDir(n)
	if err != nil {
		return err
	}
	if snap.Len() != 0 {
		return ErrNotEmpty
	}
	p := n.path
	baseVisible := m.baseVisibleAt(p)

	var made []string
	if baseVisible {
		made, err = m.ensureSpine(p)
		if err != nil {
			return err
		}
		if err := m.write.w.Whiteout(p); err != nil {
			return err
		}
		m.markWhiteout(p)
	}
	if n.UpperBacked() {
		// Dismantle beneath the marker (or beneath no base at all):
		// interior markers first, then the opaque, then the
		// directory.
		m.mu.RLock()
		interior := append([]string{}, m.idx.wh[p]...)
		opaque := m.idx.st.Opaque[p]
		m.mu.RUnlock()
		for _, b := range interior {
			ip := childPath(p, b)
			if err := m.write.w.RemoveMarker(ip); err != nil {
				return err
			}
			m.mu.Lock()
			m.idx.delWhiteout(ip)
			m.mu.Unlock()
		}
		if opaque {
			if err := m.write.w.RemoveOpaque(p); err != nil {
				return err
			}
			m.mu.Lock()
			delete(m.idx.st.Opaque, p)
			m.mu.Unlock()
		}
		if err := m.write.w.RemoveDir(p); err != nil {
			return err
		}
	}
	return m.finishOp(made, path.Dir(p), p, path.Dir(p))
}

// OpenWrite prepares a regular file for content mutation: copy-up
// when the base backs it, then an O_RDWR handle on the upper file.
// The returned node is the (possibly migrated) upper-backed
// presentation.
func (m *Merged) OpenWrite(n *Node) (*Node, *os.File, error) {
	if err := m.writable(); err != nil {
		return nil, nil, err
	}
	if n.Kind() != KindFile {
		return nil, nil, ErrNotDir
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	p := n.path
	if _, made, err := m.ensureUpper(p, -1); err != nil {
		return nil, nil, err
	} else if err := m.finishOp(made, "", append(made, p, path.Dir(p))...); err != nil {
		return nil, nil, err
	}
	nn, err := m.NodeAt(p)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(nn.HostPath(), os.O_RDWR, 0)
	if err != nil {
		nn.Close()
		return nil, nil, err
	}
	return nn, f, nil
}

// Truncate sets a regular file's size, copying up at most size
// bytes when the base backs it.
func (m *Merged) Truncate(n *Node, size int64) error {
	if err := m.writable(); err != nil {
		return err
	}
	if n.Kind() != KindFile {
		return ErrNotDir
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	p := n.path
	copied, made, err := m.ensureUpper(p, size)
	if err != nil {
		return err
	}
	e, ok, err := upper.Stat(m.upperRoot, p)
	if err != nil || !ok {
		return fmt.Errorf("truncate %q: %w", p, ErrIO)
	}
	if !copied || e.Size != size {
		if err := os.Truncate(e.HostPath, size); err != nil {
			return err
		}
	}
	return m.finishOp(made, "", append(made, p, path.Dir(p))...)
}

// SetMode applies permission bits; the root routes through the root
// record (REQ-writable-dialect).
func (m *Merged) SetMode(n *Node, mode uint32) error {
	if err := m.writable(); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if n.path == "." {
		if err := m.ensureRootRecord(); err != nil {
			return err
		}
		if err := m.write.w.SetRootMode(mode); err != nil {
			return err
		}
		return m.reindex(".")
	}
	if n.Kind() == KindSymlink {
		return ErrNotSupported
	}
	p := n.path
	_, made, err := m.ensureUpper(p, -1)
	if err != nil {
		return err
	}
	if err := m.write.w.SetMode(p, mode); err != nil {
		return err
	}
	return m.finishOp(made, "", append(made, p, path.Dir(p))...)
}

// SetOwner applies ownership — natively or as the override record,
// converting to a stand-in when the node kind cannot carry one
// (REQ-writable-fidelity). The root routes through the root record.
func (m *Merged) SetOwner(n *Node, uid, gid int) error {
	if err := m.writable(); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if n.path == "." {
		// First root mutation: stamp the requested owner, then
		// restore the previously presented mode and times — a
		// chown-only op must not flip them to host creation noise
		// (REQ-writable-fidelity). An existing record needs the
		// owner replaced alone.
		m.mu.RLock()
		recorded := m.idx.st.Root != nil
		m.mu.RUnlock()
		if err := m.write.w.RecordRoot(uid, gid); err != nil {
			return err
		}
		if !recorded {
			h := m.inner.Root().Header()
			if err := m.write.w.SetRootMode(uint32(h.Mode) & 0o7777); err != nil {
				return err
			}
			if err := m.write.w.SetRootTimes(presentedTime(h.ModTime)); err != nil {
				return err
			}
		}
		return m.reindex(".")
	}
	p := n.path
	_, made, err := m.ensureUpper(p, -1)
	if err != nil {
		return err
	}
	if err := m.write.w.SetOwner(p, uid, gid); err != nil {
		if err == upper.ErrNeedsStandIn {
			if err := m.write.w.ConvertToStandIn(p, uid, gid); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return m.finishOp(made, "", append(made, p, path.Dir(p))...)
}

// SetTimes applies a modification time; the root routes through the
// root record.
func (m *Merged) SetTimes(n *Node, mtime time.Time) error {
	if err := m.writable(); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if n.path == "." {
		if err := m.ensureRootRecord(); err != nil {
			return err
		}
		if err := m.write.w.SetRootTimes(mtime); err != nil {
			return err
		}
		return m.reindex(".")
	}
	p := n.path
	_, made, err := m.ensureUpper(p, -1)
	if err != nil {
		return err
	}
	if err := m.write.w.SetTimes(p, mtime); err != nil {
		return err
	}
	return m.finishOp(made, "", append(made, p, path.Dir(p))...)
}

// ensureRootRecord stamps the root record with the currently
// presented owner when none exists — the first root-mutating
// attribute change makes root attributes deliberate
// (REQ-writable-dialect). Mode and times must then reflect the
// previously presented values before the specific change applies.
func (m *Merged) ensureRootRecord() error {
	m.mu.RLock()
	recorded := m.idx.st.Root != nil
	m.mu.RUnlock()
	if recorded {
		return nil
	}
	h := m.inner.Root().Header()
	// Owner record first — machinery on the root without it is
	// damage, and SetRootMode may write a mode record. The stamping
	// sequence's crash intermediate presents the host root's
	// attributes under the recorded owner (REQ-writable-fidelity).
	if err := m.write.w.RecordRoot(h.Uid, h.Gid); err != nil {
		return err
	}
	if err := m.write.w.SetRootMode(uint32(h.Mode) & 0o7777); err != nil {
		return err
	}
	return m.write.w.SetRootTimes(presentedTime(h.ModTime))
}

// Flushed refreshes a path's index entry after content mutation
// through a handle — writes to an open upper file happen outside
// dialect steps, so the glue notifies on flush, fsync, and release.
func (m *Merged) Flushed(p string) error {
	if err := m.writable(); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	return m.reindex(p, path.Dir(p))
}

// NodeAt resolves the presented node at a cleaned view path — the
// write path's post-op re-resolution and the glue's refresh hook.
func (m *Merged) NodeAt(p string) (*Node, error) {
	if p == "." {
		return m.Root(), nil
	}
	cur := m.Root()
	parts := strings.Split(p, "/")
	for i, part := range parts {
		n, ok, err := m.Lookup(cur, part)
		if cur != nil && i > 0 {
			cur.Close()
		}
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrNotFound
		}
		cur = n
	}
	return cur, nil
}
