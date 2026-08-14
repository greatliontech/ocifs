// Package projection is the portable kernel every mount backend
// serves from (docs/specs/projection.md): it turns a unified view
// plus consumer-configured extra directories into an immutable,
// identity-stable, comparator-sorted node tree, and records every
// omission the backend's declared capabilities force as a projection
// report — the tree and the report are two arms of one
// classification, so an entry can never be silently absent.
//
// The kernel is synchronous, store-ignorant (content is named by
// digest only), and both path- and ID-addressable, so it serves FUSE
// and FSKit (stable numeric IDs, positional cursors) and ProjFS
// (path addressing, platform comparator, ContentID = digest) without
// favoring either; identity never depends on the capability set, so
// the same image projects the same IDs on every backend and across
// remounts.
package projection

import (
	"archive/tar"
	"bytes"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
)

// ID is a projected entry's numeric identity (inode number, FSKit
// item ID). Path-addressed backends ignore it.
type ID uint64

const (
	// RootID is the projection root's identity
	// (REQ-proj-identity; FSKit reserves 2 for the root).
	RootID ID = 2
	// viewIDBase starts the view-derived range: view entries are
	// assigned 16 upward in unified-view order.
	viewIDBase ID = 16
	// syntheticIDBase starts the non-view range, from which
	// consumer-configured extra directories draw; upper-born entries
	// of a future writable stage draw from a disjoint partition of
	// the non-view space above it, so upper-born identity can never
	// shift with the extras configuration (REQ-proj-identity).
	syntheticIDBase ID = 1 << 62
)

// Kind is a projected entry's node kind.
type Kind int

const (
	KindFile Kind = iota
	KindDir
	KindSymlink
	KindFIFO
	KindCharDevice
	KindBlockDevice
)

func (k Kind) String() string {
	switch k {
	case KindFile:
		return "file"
	case KindDir:
		return "dir"
	case KindSymlink:
		return "symlink"
	case KindFIFO:
		return "fifo"
	case KindCharDevice:
		return "char-device"
	case KindBlockDevice:
		return "block-device"
	default:
		return "unknown"
	}
}

// Capabilities declares what a backend can present — the fidelity
// envelope's kind axis and the backend's name comparator
// (REQ-proj-fidelity, REQ-proj-enumeration). Attribute-level
// envelope handling (mode, ownership) is each backend's own
// projection of the entry's recorded header.
type Capabilities struct {
	// Compare orders and equates names in a directory: the backend's
	// own comparator (REQ-proj-enumeration). Nil means byte order.
	// Names comparing equal collide (REQ-proj-case): the first entry
	// in unified-view order wins, losers are reported.
	Compare func(a, b string) int
	// ValidName reports whether the platform namespace can hold a
	// name (REQ-proj-fidelity: NTFS-illegal characters, trailing
	// dot/space, reserved device names on ProjFS). Nil means every
	// name is representable. An entry with an unrepresentable name is
	// omitted and reported, its subtree by containment.
	ValidName func(name string) bool
	// Symlinks, FIFOs, and Devices declare whether the backend can
	// present the kind; an unsupported kind is omitted and reported.
	Symlinks bool
	FIFOs    bool
	Devices  bool
}

func (c Capabilities) compare(a, b string) int {
	if c.Compare == nil {
		return bytes.Compare([]byte(a), []byte(b))
	}
	return c.Compare(a, b)
}

// Entry is one projected node. Immutable after New returns.
type Entry struct {
	name     string
	path     string
	id       ID
	kind     Kind
	header   tar.Header
	digest   v1.Hash
	parent   *Entry
	children []*Entry // comparator-sorted; nil unless KindDir
}

// Name returns the entry's base name in its stored spelling — the
// canonical name a case-folding backend reports for any matching
// lookup.
func (e *Entry) Name() string { return e.name }

// Path returns the entry's cleaned, root-relative view path ("." for
// the root).
func (e *Entry) Path() string { return e.path }

func (e *Entry) ID() ID     { return e.id }
func (e *Entry) Kind() Kind { return e.kind }

// Header returns a copy of the entry's recorded tar header — the
// full attribute record each backend projects down to its envelope.
// Synthetic entries (the root without a view record, extra
// directories) carry a minimal directory header. The copy is
// isolated: its map fields are cloned, so no caller can reach kernel
// state through it.
func (e *Entry) Header() tar.Header {
	h := e.header
	h.PAXRecords = maps.Clone(h.PAXRecords)
	//lint:ignore SA1019 cloned only so the deprecated field cannot alias kernel state
	h.Xattrs = maps.Clone(h.Xattrs)
	return h
}

// LinkTarget returns a symlink's verbatim target.
func (e *Entry) LinkTarget() string { return e.header.Linkname }

// ContentDigest returns a regular file's content-CAS key — the bytes
// the projection must serve (REQ-proj-content) and the value for a
// backend's content-versioning slot (ProjFS ContentID).
func (e *Entry) ContentDigest() v1.Hash { return e.digest }

// Size returns the entry's recorded size.
func (e *Entry) Size() int64 { return e.header.Size }

// Parent returns the containing directory, nil for the root.
func (e *Entry) Parent() *Entry { return e.parent }

// Len returns the number of children of a directory entry.
func (e *Entry) Len() int { return len(e.children) }

// At returns the i-th child in comparator order. The snapshot is
// immutable, so an index held across calls resumes exactly
// (REQ-proj-enumeration).
func (e *Entry) At(i int) *Entry { return e.children[i] }

// Children returns the comparator-sorted child snapshot. The slice
// is shared and must not be modified.
func (e *Entry) Children() []*Entry { return e.children }

// projection-internal: find the insertion/match position for name
// under cmp. Children are sorted by cmp, so binary search applies.
func searchChildren(children []*Entry, name string, cmp func(a, b string) int) (int, bool) {
	i := sort.Search(len(children), func(i int) bool {
		return cmp(children[i].name, name) >= 0
	})
	if i < len(children) && cmp(children[i].name, name) == 0 {
		return i, true
	}
	return i, false
}

// Projection is the immutable kernel state one mount serves.
type Projection struct {
	caps   Capabilities
	root   *Entry
	byID   map[ID]*Entry
	report Report
}

// Root returns the projection root (ID 2).
func (p *Projection) Root() *Entry { return p.root }

// Capabilities returns the declared envelope this projection was
// built under, so a backend can recover its own declaration (the
// comparator, the symlink decision) without re-plumbing it.
func (p *Projection) Capabilities() Capabilities { return p.caps }

// ByID resolves a presented entry by numeric identity.
func (p *Projection) ByID(id ID) (*Entry, bool) {
	e, ok := p.byID[id]
	return e, ok
}

// Lookup resolves name within dir under the backend comparator,
// returning the presented entry with its canonical stored spelling.
func (p *Projection) Lookup(dir *Entry, name string) (*Entry, bool) {
	if dir.kind != KindDir {
		return nil, false
	}
	i, ok := searchChildren(dir.children, name, p.caps.compare)
	if !ok {
		return nil, false
	}
	return dir.children[i], true
}

// Seek returns the index of the first child of dir ordered strictly
// after name — the resume position for an enumeration that last
// returned name (REQ-proj-enumeration).
func (p *Projection) Seek(dir *Entry, name string) int {
	cmp := p.caps.compare
	return sort.Search(len(dir.children), func(i int) bool {
		return cmp(dir.children[i].name, name) > 0
	})
}

// Report returns a copy of the projection report accumulated at
// build time; the kernel state stays unreachable through it.
func (p *Projection) Report() Report {
	return Report{Entries: slices.Clone(p.report.Entries)}
}

func kindOf(h *tar.Header) (Kind, bool) {
	switch h.Typeflag {
	case tar.TypeReg, tar.TypeLink:
		// Hardlinks arrive pre-resolved: content digest and size
		// carry the target's content, so they present as independent
		// nodes (REQ-proj-fidelity).
		return KindFile, true
	case tar.TypeDir:
		return KindDir, true
	case tar.TypeSymlink:
		return KindSymlink, true
	case tar.TypeFifo:
		return KindFIFO, true
	case tar.TypeChar:
		return KindCharDevice, true
	case tar.TypeBlock:
		return KindBlockDevice, true
	default:
		return 0, false
	}
}

// supported reports whether caps can present kind, with the report
// reason when not.
func (c Capabilities) supported(k Kind) (bool, Reason) {
	switch k {
	case KindSymlink:
		if !c.Symlinks {
			return false, ReasonSymlinkUnsupported
		}
	case KindFIFO:
		if !c.FIFOs {
			return false, ReasonFIFOUnsupported
		}
	case KindCharDevice, KindBlockDevice:
		if !c.Devices {
			return false, ReasonDeviceUnsupported
		}
	}
	return true, ""
}

func syntheticDirHeader(name string) tar.Header {
	return tar.Header{Typeflag: tar.TypeDir, Name: name, Mode: 0o755}
}

// New builds the projection kernel for a unified view plus extra
// directories under a backend's declared capabilities.
//
// Identity is assigned before any capability filtering, from the
// view alone: the root is 2, and every view entry — presented or not
// — draws its ID from 16 upward in unified-view order, so the same
// image projects the same IDs whatever the backend
// (REQ-proj-identity). Entries absent from the view (extra
// directories) draw from the disjoint synthetic range.
//
// Every view entry is then classified exactly once: placed in the
// tree, or recorded in the report with its reason — an unsupported
// kind, a name colliding under the backend comparator with an
// earlier entry (the first in unified-view order wins,
// REQ-proj-case), or containment inside an omitted directory.
func New(view *layer.View, extraDirs []string, caps Capabilities) (*Projection, error) {
	p := &Projection{
		caps: caps,
		byID: make(map[ID]*Entry),
	}
	p.root = &Entry{
		name:   ".",
		path:   ".",
		id:     RootID,
		kind:   KindDir,
		header: syntheticDirHeader("."),
	}

	byPath := map[string]*Entry{".": p.root}
	// omitted maps an omitted directory path to the reason its
	// descendants inherit: containment inside an entry the backend
	// does not present.
	omitted := map[string]Reason{}
	// viewKids records, per view directory, every child name with
	// whether it is a non-directory — presented or not — so
	// extra-directory conflicts are judged against the view under the
	// backend comparator (api.md REQ-api-extra-dirs): the comparator
	// is matrix-declared per backend, but within one comparator the
	// outcome never depends on the capability set.
	viewKids := map[string][]viewKid{}

	nextViewID := viewIDBase
	for _, ve := range view.Entries() {
		// View names arrive cleaned (layer-semantics.md); the kernel
		// trusts that contract and errors below when it is broken.
		name := ve.Header.Name
		hdr := ve.Header
		if name == "." {
			// The view's root record projects onto the fixed root
			// identity; it is never a child.
			p.root.header = hdr
			continue
		}
		id := nextViewID
		nextViewID++

		dir, base := path.Split(name)
		dir = strings.TrimSuffix(dir, "/")
		if dir == "" {
			dir = "."
		}

		kind, ok := kindOf(&hdr)
		if !ok {
			viewKids[dir] = append(viewKids[dir], viewKid{name: base, nonDir: true})
			p.report.add(name, ReasonKindUnknown, fmt.Sprintf("tar type %q", hdr.Typeflag))
			continue
		}
		viewKids[dir] = append(viewKids[dir], viewKid{name: base, nonDir: kind != KindDir})
		switch kind {
		case KindCharDevice, KindBlockDevice:
			// Every backend envelope presents devices as typed nodes
			// without device numbers (REQ-proj-fidelity); dropping
			// them once here enforces it for all three.
			hdr.Devmajor, hdr.Devminor = 0, 0
		case KindFile:
			// A hardlink-derived file's link fact is unification
			// history: it presents as an independent regular node
			// (REQ-proj-fidelity), so both the lingering target and
			// the link typeflag are normalized away — either would
			// misproject the entry on a backend reading the raw
			// header.
			hdr.Typeflag = tar.TypeReg
			hdr.Linkname = ""
		}

		parent, present := byPath[dir]
		if !present {
			if reason, wasOmitted := omitted[dir]; wasOmitted {
				// Containment: the ancestor was omitted, so this
				// entry cannot be presented either; it inherits the
				// ancestor's reason.
				p.report.add(name, reason, "inside omitted "+dir)
				if kind == KindDir {
					omitted[name] = reason
				}
				continue
			}
			// The unified view is complete: every parent precedes its
			// children as a directory (layer-semantics.md). A missing
			// parent is a violated input contract, never presentable
			// state.
			return nil, fmt.Errorf("view entry %q: parent %q missing from view", name, dir)
		}
		if parent.kind != KindDir {
			return nil, fmt.Errorf("view entry %q: parent %q is not a directory", name, dir)
		}

		if caps.ValidName != nil && !caps.ValidName(base) {
			p.report.add(name, ReasonNameUnrepresentable, "")
			if kind == KindDir {
				omitted[name] = ReasonNameUnrepresentable
			}
			continue
		}
		if ok, reason := caps.supported(kind); !ok {
			p.report.add(name, reason, "")
			if kind == KindDir {
				omitted[name] = reason
			}
			continue
		}

		e := &Entry{
			name:   base,
			path:   name,
			id:     id,
			kind:   kind,
			header: hdr,
			digest: ve.Digest,
			parent: parent,
		}
		i, exists := searchChildren(parent.children, base, caps.compare)
		if exists {
			// Two view names collide under the backend comparator:
			// the first entry in unified-view order won already
			// (REQ-proj-case).
			winner := parent.children[i]
			p.report.add(name, ReasonCaseCollision, "collides with "+winner.path)
			if kind == KindDir {
				omitted[name] = ReasonCaseCollision
			}
			continue
		}
		parent.children = append(parent.children, nil)
		copy(parent.children[i+1:], parent.children[i:])
		parent.children[i] = e
		byPath[name] = e
		p.byID[id] = e
	}

	if err := p.addExtraDirs(extraDirs, viewKids); err != nil {
		return nil, err
	}

	p.byID[RootID] = p.root
	return p, nil
}

// addExtraDirs anchors the consumer-configured extra directories
// (api.md REQ-api-extra-dirs) into the tree. Components already
// presented are reused; missing components draw synthetic IDs
// deterministically — the configured set is sorted, so the same
// configuration projects the same IDs across remounts. A component
// that exists as a non-directory is a configuration conflict, not an
// omission.
// viewKid is one view directory child as the view records it: the
// base name and whether it is a non-directory, independent of what
// any envelope presents.
type viewKid struct {
	name   string
	nonDir bool
}

func (p *Projection) addExtraDirs(extraDirs []string, viewKids map[string][]viewKid) error {
	dirs := make([]string, 0, len(extraDirs))
	for _, d := range extraDirs {
		clean := path.Clean(d)
		if clean == "." {
			continue
		}
		if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("extra directory %q escapes the mount root", d)
		}
		if p.caps.ValidName != nil {
			for _, part := range strings.Split(clean, "/") {
				if !p.caps.ValidName(part) {
					// Same configuration-error class as a view
					// conflict: the platform namespace cannot hold
					// the configured name (REQ-proj-fidelity).
					return fmt.Errorf("extra directory %q: %q is not representable on this platform: %w", d, part, ErrNotDir)
				}
			}
		}
		dirs = append(dirs, clean)
	}
	sort.Strings(dirs)

	nextID := syntheticIDBase
	for _, d := range dirs {
		cur := p.root
		curPath := "."
		for _, part := range strings.Split(d, "/") {
			if curPath == "." {
				curPath = part
			} else {
				curPath = curPath + "/" + part
			}
			// The view judges conflicts, not the presented tree, and
			// under the backend comparator: an envelope-omitted
			// non-directory — whatever its spelling — must fail the
			// same configuration under every capability set, and a
			// synthetic directory must never occupy an omitted view
			// entry's name slot.
			for _, k := range viewKids[cur.path] {
				// Unrepresentable view names are omitted, never
				// presented, so they cannot conflict — and the
				// platform comparator must not see them (it may
				// reject bytes like NUL that the validator filters).
				if p.caps.ValidName != nil && !p.caps.ValidName(k.name) {
					continue
				}
				if k.nonDir && p.caps.compare(k.name, part) == 0 {
					return fmt.Errorf("extra directory %q: %q in %q exists as a non-directory in the view: %w", d, k.name, cur.path, ErrNotDir)
				}
			}
			i, exists := searchChildren(cur.children, part, p.caps.compare)
			if exists {
				next := cur.children[i]
				if next.kind != KindDir {
					return fmt.Errorf("extra directory %q: %q exists as a %s in the view: %w", d, next.path, next.kind, ErrNotDir)
				}
				// Continue under the canonical stored spelling, which
				// on a case-folding comparator may differ from the
				// configured one.
				curPath = next.path
				cur = next
				continue
			}
			e := &Entry{
				name:   part,
				path:   curPath,
				id:     nextID,
				kind:   KindDir,
				header: syntheticDirHeader(part),
				parent: cur,
			}
			nextID++
			cur.children = append(cur.children, nil)
			copy(cur.children[i+1:], cur.children[i:])
			cur.children[i] = e
			p.byID[e.id] = e
			cur = e
		}
	}
	return nil
}
