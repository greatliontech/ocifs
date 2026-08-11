package layer

import (
	"archive/tar"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

const (
	whiteoutPrefix = ".wh."
	opaqueMarker   = ".wh..wh..opq"
)

// ErrPathEscape reports a layer entry whose cleaned path cannot
// exist in a real root filesystem — it escapes the image root, or
// exceeds the length bound below; unification fails on it
// (REQ-unify-contained).
var ErrPathEscape = errors.New("layer entry escapes the image root")

// maxNameLen bounds a cleaned entry name (REQ-unify-contained): no
// materializable root filesystem holds longer paths, and unbounded
// names make full-path construction quadratic in depth.
const maxNameLen = 4096

// reservedComponent reports whether any non-basename component of a
// cleaned name lies in the whiteout namespace: such a path is
// unrepresentable in the layer dialect (re-serializing it would read
// back as markers), so entries under it are inert.
func reservedComponent(name string) bool {
	for c := range strings.SplitSeq(name, "/") {
		if strings.HasPrefix(c, whiteoutPrefix) {
			return true
		}
	}
	return false
}

// node is one tree position during unification. entry is nil for an
// implied directory (children exist, no layer spoke about the path
// itself). children is non-nil exactly for directories, implied or
// explicit.
type node struct {
	entry    *Entry
	children map[string]*node
}

func (n *node) isDir() bool { return n.children != nil }

func newDirNode(e *Entry) *node {
	return &node{entry: e, children: map[string]*node{}}
}

// cleanName normalizes a tar entry name to the model's path form:
// leading slashes stripped, then slash-cleaned *relatively*, so that
// dot-dot traversal survives as a leading ".." component for escapes
// to detect instead of being silently absorbed by a root-anchored
// clean. The empty name and "/" clean to ".".
func cleanName(name string) string {
	return path.Clean(strings.TrimLeft(name, "/"))
}

// escapes reports whether a cleaned name points above the image root
// (REQ-unify-contained).
func escapes(name string) bool {
	return name == ".." || strings.HasPrefix(name, "../")
}

// metaType reports tar entry kinds that are archive metadata rather
// than filesystem objects.
func metaType(flag byte) bool {
	switch flag {
	case tar.TypeXHeader, tar.TypeXGlobalHeader, tar.TypeGNULongName, tar.TypeGNULongLink:
		return true
	}
	return false
}

// Unify resolves a stack of layers, ordered base to top, into the
// unified view (docs/specs/layer-semantics.md).
func Unify(layers []Layer) (*View, error) {
	root := newDirNode(nil)

	for li, l := range layers {
		// Pass 1 — whiteout markers, applied to the state built from
		// lower layers only. Content entries of this same layer are
		// invisible here, which is what makes whiteout application
		// independent of tar order.
		for _, e := range l {
			if metaType(e.Header.Typeflag) {
				continue
			}
			name := cleanName(e.Header.Name)
			if escapes(name) || len(name) > maxNameLen {
				return nil, fmt.Errorf("layer %d entry %q: %w", li, e.Header.Name, ErrPathEscape)
			}
			dir, base := path.Split(name)
			dir = strings.TrimSuffix(dir, "/")
			if dir == "" {
				dir = "."
			}
			if base == opaqueMarker {
				if d := lookupNode(root, dir); d != nil && d.isDir() {
					d.children = map[string]*node{}
				}
				continue
			}
			if !strings.HasPrefix(base, whiteoutPrefix) {
				continue
			}
			target := base[len(whiteoutPrefix):]
			// Degenerate or reserved markers have no effect.
			if target == "" || target == "." || target == ".." || strings.HasPrefix(target, whiteoutPrefix) {
				continue
			}
			if d := lookupNode(root, dir); d != nil && d.isDir() {
				delete(d.children, target)
			}
		}

		// Pass 2 — content entries in tar order; later entries
		// overwrite earlier ones (sequential-extraction semantics).
		for _, e := range l {
			if metaType(e.Header.Typeflag) {
				continue
			}
			name := cleanName(e.Header.Name)
			base := path.Base(name)
			if strings.HasPrefix(base, whiteoutPrefix) {
				continue // marker, already applied in pass 1
			}
			if reservedComponent(name) {
				continue // unrepresentable path: inert (REQ-unify-clean)
			}
			if e.Header.Typeflag == 0 { // legacy regular-file flag
				e.Header.Typeflag = tar.TypeReg
			}
			isDir := e.Header.Typeflag == tar.TypeDir

			if name == "." {
				if !isDir {
					return nil, fmt.Errorf("layer %d entry %q: non-directory at the image root: %w", li, e.Header.Name, ErrPathEscape)
				}
				e.Header.Name = "."
				root.entry = &e
				continue
			}

			parent := root
			components := strings.Split(name, "/")
			discarded := false
			for _, c := range components[:len(components)-1] {
				child, ok := parent.children[c]
				if !ok {
					child = newDirNode(nil) // implied directory
					parent.children[c] = child
				} else if !child.isDir() {
					// Ancestor is a non-directory: the entry is
					// discarded (REQ-unify-clean).
					discarded = true
					break
				}
				parent = child
			}
			if discarded {
				continue
			}

			if e.Header.Typeflag == tar.TypeLink {
				// Hardlinks resolve at their own position, like
				// os.Link at extraction time; the captured content
				// identity rides the entry (REQ-unify-hardlink).
				target := cleanName(e.Header.Linkname)
				if target == name {
					continue // self-link: linking to one's own inode is a no-op
				}
				tn := lookupNode(root, target)
				if tn == nil || tn.entry == nil ||
					(tn.entry.Header.Typeflag != tar.TypeReg && tn.entry.Header.Typeflag != tar.TypeLink) {
					continue // unresolvable link: omitted
				}
				e.Digest = tn.entry.Digest
				e.Header.Size = tn.entry.Header.Size
				// A hardlink shares its target's inode, so the
				// inode attributes are the target's, not the link
				// header's.
				e.Header.Mode = tn.entry.Header.Mode
				e.Header.Uid = tn.entry.Header.Uid
				e.Header.Gid = tn.entry.Header.Gid
				e.Header.Uname = tn.entry.Header.Uname
				e.Header.Gname = tn.entry.Header.Gname
				e.Header.ModTime = tn.entry.Header.ModTime
				e.Header.AccessTime = tn.entry.Header.AccessTime
				e.Header.ChangeTime = tn.entry.Header.ChangeTime
				e.Header.Xattrs = tn.entry.Header.Xattrs //nolint:staticcheck // carried for legacy producers
				e.Header.PAXRecords = tn.entry.Header.PAXRecords
				e.Header.Linkname = target
			}

			e.Header.Name = name
			existing := parent.children[base]
			switch {
			case isDir && existing != nil && existing.isDir():
				// Directory over directory: attributes update,
				// children survive.
				existing.entry = &e
			case isDir:
				parent.children[base] = newDirNode(&e)
			default:
				// Non-directory placement replaces whatever was
				// there, subtree included.
				parent.children[base] = &node{entry: &e}
			}
		}
	}

	return buildView(root), nil
}

func lookupNode(root *node, name string) *node {
	if name == "." {
		return root
	}
	n := root
	for c := range strings.SplitSeq(name, "/") {
		if !n.isDir() {
			return nil
		}
		child, ok := n.children[c]
		if !ok {
			return nil
		}
		n = child
	}
	return n
}

func buildView(root *node) *View {
	var entries []Entry
	if root.entry != nil {
		entries = append(entries, *root.entry)
	}
	var walk func(prefix string, n *node)
	walk = func(prefix string, n *node) {
		for name, child := range n.children {
			full := name
			if prefix != "" {
				full = prefix + "/" + name
			}
			switch {
			case child.entry != nil:
				// The placement site already wrote the cleaned full
				// path; a second write here would be a redundant
				// source that could silently disagree.
				entries = append(entries, *child.entry)
			case child.isDir():
				// Implied directory: extraction made it real, so
				// the view carries it as a synthesized plain
				// directory (REQ-unify-sorted).
				entries = append(entries, Entry{Header: tar.Header{
					Name:     full,
					Typeflag: tar.TypeDir,
					Mode:     0o755,
				}})
			}
			if child.isDir() {
				walk(full, child)
			}
		}
	}
	walk("", root)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Header.Name < entries[j].Header.Name
	})
	index := make(map[string]int, len(entries))
	for i := range entries {
		index[entries[i].Header.Name] = i
	}
	return &View{entries: entries, index: index}
}
