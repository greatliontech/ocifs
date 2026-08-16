// Package export materializes a unified view (internal/layer) into a
// real directory tree — a prepared root filesystem
// (docs/specs/export.md). The materializer works entirely through an
// os.Root handle, so every write resolves inside the export root by
// construction (REQ-export-contained): no created path can traverse
// a symlink, and hardlinks can only name in-root targets. Content is
// copied out of the content CAS, never linked into it
// (REQ-export-copy, REQ-export-immutable).
package export

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
)

// Materialize writes view into root. blobPath resolves a
// regular-file entry's digest to its content-CAS path. The caller
// provides a freshly created, empty root and renames it into place
// afterwards (REQ-export-atomic is the caller's obligation).
//
// Ordering: the sorted view yields parents before children, so one
// forward pass creates the tree; hardlinks are deferred until every
// regular file exists (a link may sort before its target); directory
// permissions and times are applied in a reverse pass, children
// first, so restrictive modes cannot block the export and creating
// children cannot disturb recorded directory times
// (REQ-export-fidelity).
func Materialize(root *os.Root, view *layer.View, blobPath func(v1.Hash) string) error {
	// euid 0 includes user-namespace root: recorded ownership is
	// applied natively there too, and a recorded id outside the
	// namespace's mapping fails the chown (EINVAL) — surfaced, never
	// silently degraded to unprivileged export. A recorded id of -1
	// (reachable: tar base-256 encodings can carry negative ids)
	// makes that id's half of the chown a no-op rather than an
	// applied value.
	privileged := os.Geteuid() == 0
	// folded maps the case-folded spelling of every created path to
	// its original: on a case-insensitive target two distinct view
	// paths land on one filesystem entry, the second create fails
	// EEXIST, and the export refuses naming both paths
	// (REQ-export-fidelity).
	folded := make(map[string]string, view.Len())
	collide := func(name string, err error) error {
		return collisionError(folded, name, err)
	}
	created := func(name string) {
		if _, ok := folded[strings.ToLower(name)]; !ok {
			folded[strings.ToLower(name)] = name
		}
	}

	var links []layer.Entry
	entries := view.Entries()
	for i := range entries {
		e := &entries[i]
		name := e.Header.Name
		if name == "." {
			// The root's own attributes; applied in the reverse pass.
			continue
		}
		switch e.Header.Typeflag {
		case tar.TypeDir:
			// Mode applied in the reverse pass, after contents.
			if err := root.Mkdir(name, 0o755); err != nil {
				return collide(name, err)
			}
		case tar.TypeReg:
			if err := copyBlob(root, name, e, blobPath); err != nil {
				return collide(name, err)
			}
			if err := applyFileAttrs(root, name, e, privileged); err != nil {
				return err
			}
		case tar.TypeLink:
			// Deferred: the target may sort after the link.
			links = append(links, *e)
			continue
		case tar.TypeSymlink:
			// The recorded target verbatim — absolute and dangling
			// targets are the consumer's to interpret inside their
			// root (REQ-export-fidelity).
			if err := root.Symlink(e.Header.Linkname, name); err != nil {
				return collide(name, err)
			}
			if privileged {
				if err := root.Lchown(name, e.Header.Uid, e.Header.Gid); err != nil {
					return err
				}
			}
			if err := lchmod(root, name, e.Header.FileInfo().Mode().Perm()); err != nil {
				return err
			}
			if err := lchtimes(root, name, e.Header.ModTime); err != nil {
				return err
			}
		case tar.TypeFifo:
			if err := mkfifo(root, name); err != nil {
				return collide(name, err)
			}
			if err := applyFileAttrs(root, name, e, privileged); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock:
			// Omitted: device nodes need privilege and rootfs
			// consumers provide their own /dev (REQ-export-fidelity).
			continue
		default:
			return fmt.Errorf("entry %q has unsupported type %q", name, e.Header.Typeflag)
		}
		created(name)
	}

	// A deferred link's target may itself be a deferred link — the
	// view admits chains (REQ-unify-hardlink: an earlier resolved
	// hardlink carries regular content) and even cycles (a later
	// layer can re-link a chain member back onto another) — so links
	// materialize in chain-depth order, plain-file targets first,
	// and materializeLink's on-disk check breaks any remaining cycle:
	// the first member with no on-disk target becomes an independent
	// copy of its captured content — always faithful — and the rest
	// link onto it.
	sort.SliceStable(links, func(i, j int) bool {
		return linkDepth(view, &links[i]) < linkDepth(view, &links[j])
	})
	for i := range links {
		if err := materializeLink(root, view, &links[i], blobPath, privileged, collide); err != nil {
			return err
		}
		created(links[i].Header.Name)
	}

	// Reverse pass: directory attributes, children before parents,
	// root last.
	for i := view.Len() - 1; i >= 0; i-- {
		e := &entries[i]
		if e.Header.Typeflag != tar.TypeDir && e.Header.Name != "." {
			continue
		}
		name := e.Header.Name
		if privileged {
			if err := root.Chown(name, e.Header.Uid, e.Header.Gid); err != nil {
				return err
			}
		}
		if err := root.Chmod(name, e.Header.FileInfo().Mode().Perm()|modeSpecial(e)); err != nil {
			return err
		}
		if !e.Header.ModTime.IsZero() {
			if err := root.Chtimes(name, e.Header.ModTime, e.Header.ModTime); err != nil {
				return err
			}
		}
	}
	return nil
}

// collisionError diagnoses a create failure: an EEXIST in a fresh
// export root means the target filesystem folded this path onto an
// earlier one (case-insensitive hosts), and the export refuses
// naming both paths — exact or refused, never silently substituted
// (REQ-export-fidelity).
func collisionError(folded map[string]string, name string, err error) error {
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	if prior, ok := folded[strings.ToLower(name)]; ok && prior != name {
		return fmt.Errorf("target filesystem cannot hold both %q and %q as distinct entries", prior, name)
	}
	return err
}

// modeSpecial extracts the setuid/setgid/sticky bits from the
// recorded header (REQ-export-fidelity applies them verbatim).
func modeSpecial(e *layer.Entry) fs.FileMode {
	return e.Header.FileInfo().Mode() & (fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
}

// copyBlob copies the entry's content out of the CAS into a fresh
// exported file — a copy, never a link into the store
// (REQ-export-copy); the CAS entry itself is never opened for
// writing (REQ-export-immutable).
func copyBlob(root *os.Root, name string, e *layer.Entry, blobPath func(v1.Hash) string) error {
	src, err := os.Open(blobPath(e.Digest))
	if err != nil {
		return fmt.Errorf("entry %q: content %s: %w", name, e.Digest, err)
	}
	defer src.Close()
	dst, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return fmt.Errorf("entry %q: %w", name, err)
	}
	return dst.Close()
}

// applyFileAttrs applies ownership (privileged only), permissions
// including special bits, and recorded times to a non-directory,
// non-symlink entry. Ownership first: chown clears setuid/setgid.
func applyFileAttrs(root *os.Root, name string, e *layer.Entry, privileged bool) error {
	if privileged {
		if err := root.Chown(name, e.Header.Uid, e.Header.Gid); err != nil {
			return err
		}
	}
	if err := root.Chmod(name, e.Header.FileInfo().Mode().Perm()|modeSpecial(e)); err != nil {
		return err
	}
	if e.Header.ModTime.IsZero() {
		return nil
	}
	return root.Chtimes(name, e.Header.ModTime, e.Header.ModTime)
}

// materializeLink places one hardlink entry: a link to the exported
// target when the target still carries the content identity the link
// captured, an independent copy of the captured content otherwise —
// fidelity within the tree, never a link into the CAS
// (REQ-export-copy).
func materializeLink(root *os.Root, view *layer.View, e *layer.Entry, blobPath func(v1.Hash) string, privileged bool, collide func(string, error) error) error {
	name := e.Header.Name
	target := e.Header.Linkname
	// Linking requires more than equal content identity: the two
	// entries would share one inode, so their recorded inode
	// attributes must agree too — a target replaced by a same-bytes
	// file with different attributes is a new inode, and the link
	// keeps its captured one via an independent copy
	// (REQ-export-fidelity over REQ-export-copy's permissive link).
	if te, ok := view.Lookup(target); ok &&
		(te.Header.Typeflag == tar.TypeReg || te.Header.Typeflag == tar.TypeLink) &&
		te.Digest == e.Digest && te.Header.Size == e.Header.Size &&
		te.Header.Mode == e.Header.Mode &&
		te.Header.Uid == e.Header.Uid && te.Header.Gid == e.Header.Gid &&
		te.Header.ModTime.Equal(e.Header.ModTime) {
		// The target must already be on disk (chain-depth ordering
		// guarantees it); anything else falls through to the copy.
		if _, err := root.Lstat(target); err == nil {
			if err := root.Link(target, name); err != nil {
				return collide(name, err)
			}
			// The link shares the target's inode; the target's
			// attribute application covers it (the view gave both
			// the same inode attributes).
			return nil
		}
	}
	if err := copyBlob(root, name, e, blobPath); err != nil {
		return collide(name, err)
	}
	return applyFileAttrs(root, name, e, privileged)
}

// linkDepth counts the chain of link entries from e to a plain-file
// target: 0 for a regular target, 1 for a link onto it, and so on.
// The view-size bound terminates cyclic chains (a view can hold
// them); a bound-exceeding depth just sorts last, and the on-disk
// check at materialization breaks the cycle with a copy.
func linkDepth(view *layer.View, e *layer.Entry) int {
	depth := 0
	cur := *e
	for depth <= view.Len() {
		te, ok := view.Lookup(cur.Header.Linkname)
		if !ok || te.Header.Typeflag != tar.TypeLink {
			return depth
		}
		depth++
		cur = te
	}
	return depth
}

// dirOf returns the parent directory of a cleaned root-relative
// path, "." for a top-level name — the shape os.Root methods expect.
func dirOf(name string) string {
	d := path.Dir(name)
	if d == "/" || d == "" {
		return "."
	}
	return d
}
