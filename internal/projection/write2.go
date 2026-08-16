//go:build linux || darwin

package projection

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// Write path B: rename, hardlinks, node creation, and the extended
// attribute surface (writable.md REQ-writable-rename/-hardlink,
// REQ-writable-reserved/-fidelity).

// baseVisibleBeneath reports whether any base-visible content lives
// strictly beneath dir under the current index — the EXDEV judgment
// for directory renames (REQ-writable-rename: recursive
// provider-side copy-up of unbounded trees is not owed). An occluded
// child's subtree is dead wholesale and needs no descent.
func (m *Merged) baseVisibleBeneath(dir string) bool {
	ve, ok := m.viewAt(dir)
	if !ok || ve.Kind() != KindDir {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var walk func(e *Entry) bool
	walk = func(e *Entry) bool {
		for _, c := range e.Children() {
			if !baseDead(m.idx, c.Path()) {
				return true
			}
			// The child's own occlusion (a whiteout there, or an
			// ancestor's) also kills everything beneath it.
		}
		return false
	}
	return walk(ve)
}

// Rename implements POSIX rename on the merged tree
// (REQ-writable-rename), ordered destination-first: the destination
// materializes before the source's whiteout lands, so the one crash
// residual is the entry present at both paths. A directory holding
// base-visible content (itself or beneath) returns ErrCrossDevice —
// userspace falls back to copy-and-delete.
func (m *Merged) Rename(srcDir *Node, srcName string, dstDir *Node, dstName string) error {
	if err := m.writable(); err != nil {
		return err
	}
	if err := checkChildName(srcName); err != nil {
		return err
	}
	if err := checkChildName(dstName); err != nil {
		return err
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()

	src, ok, err := m.Lookup(srcDir, srcName)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	defer src.Close()
	sp := src.Path()
	dp := childPath(dstDir.path, dstName)
	if sp == dp {
		return nil
	}
	if strings.HasPrefix(dp, sp+"/") {
		return fmt.Errorf("rename into own subtree: %w", ErrInvalid)
	}

	dst, dstOK, err := m.Lookup(dstDir, dstName)
	if err != nil {
		return err
	}
	if dstOK {
		defer dst.Close()
	}

	srcBaseVisible := m.baseVisibleAt(sp)
	if src.Kind() == KindDir {
		// A directory renames natively only when nothing base-visible
		// lives at or beneath it (upper-born, or recreated over its
		// whiteout).
		if srcBaseVisible || m.baseVisibleBeneath(sp) {
			return ErrCrossDevice
		}
		if dstOK {
			if dst.Kind() != KindDir {
				return ErrNotDir
			}
			snap, err := m.OpenDir(dst)
			if err != nil {
				return err
			}
			if snap.Len() != 0 {
				return ErrNotEmpty
			}
		}
	} else if dstOK && dst.Kind() == KindDir {
		return ErrIsDir
	}

	// Two names of one inode: POSIX rename is a successful no-op.
	if dstOK && src.UpperBacked() && dst.UpperBacked() &&
		src.Kind() != KindDir && src.up.Ino == dst.up.Ino {
		return nil
	}

	made, err := m.ensureSpine(dp)
	if err != nil {
		return err
	}

	dstBaseVisible := dstOK && m.baseVisibleAt(dp)
	// A replacing directory destination composes rmdir's compound
	// before the swap: hide (marker), dismantle, then the swap lands
	// beside the marker — rmdir's declared intermediates plus the
	// destination-absent step immediately before the swap
	// (REQ-writable-rename's replacing-directory clause).
	if dstOK && dst.Kind() == KindDir && dstBaseVisible {
		if err := m.write.w.Whiteout(dp); err != nil {
			return err
		}
		m.markWhiteout(dp)
	}
	if dstOK && dst.Kind() == KindDir && dst.UpperBacked() {
		m.mu.RLock()
		interior := append([]string{}, m.idx.wh[dp]...)
		opaque := m.idx.st.Opaque[dp]
		m.mu.RUnlock()
		for _, b := range interior {
			ip := childPath(dp, b)
			if err := m.write.w.RemoveMarker(ip); err != nil {
				return err
			}
			m.mu.Lock()
			m.idx.delWhiteout(ip)
			m.mu.Unlock()
		}
		if opaque {
			if err := m.write.w.RemoveOpaque(dp); err != nil {
				return err
			}
			m.mu.Lock()
			delete(m.idx.st.Opaque, dp)
			m.mu.Unlock()
		}
		if err := m.write.w.RemoveDir(dp); err != nil {
			return err
		}
		// The old destination entry leaves the index with its
		// directory — no transient collision with the moved subtree.
		m.mu.Lock()
		m.idx.delEntry(dp)
		m.mu.Unlock()
	}

	switch {
	case !src.UpperBacked():
		// Base-only source: materialize the destination as a direct
		// copy-up AT the destination path — both paths present until
		// the source marker lands (the declared residual). The
		// marker's own parent spine materializes before it.
		ve, ok := m.viewAt(sp)
		if !ok {
			return ErrNotFound
		}
		if err := m.copyUpEntry(dp, ve, -1); err != nil {
			return err
		}
		smade, err := m.ensureSpine(sp)
		if err != nil {
			return err
		}
		made = append(made, smade...)
		if err := m.write.w.Whiteout(sp); err != nil {
			return err
		}
		m.markWhiteout(sp)
	case srcBaseVisible && src.Kind() != KindDir:
		// Shadowing source: the destination materializes as a
		// hardlink of the upper node — via a temp-and-swap so an
		// existing upper destination is replaced atomically (both
		// paths present: the declared residual) — then the source
		// marker hides the base and the source entry retires.
		if err := m.write.w.LinkReplace(sp, dp); err != nil {
			return err
		}
		if err := m.write.w.Whiteout(sp); err != nil {
			return err
		}
		m.markWhiteout(sp)
		if err := m.write.w.Remove(sp); err != nil {
			return err
		}
	default:
		// Upper-born source (files and renameable directories): one
		// native swap; an existing upper destination non-directory is
		// replaced atomically.
		if err := m.write.w.Rename(sp, dp); err != nil {
			return err
		}
	}

	// A replaced non-directory destination's base visibility earns
	// its whiteout AFTER the swap: the new entry already shadows the
	// base, so every crash prefix presents the old tree, the
	// both-paths residual, or the new tree — never a vanished
	// destination (REQ-writable-crash).
	if dstBaseVisible && dst.Kind() != KindDir {
		if err := m.write.w.Whiteout(dp); err != nil {
			return err
		}
		m.markWhiteout(dp)
	}

	// Interior markers and opaque flags moved with a directory swap;
	// rebuild their index projection for the subtree.
	if src.Kind() == KindDir {
		m.mu.Lock()
		m.idx.moveSubtree(sp, dp, m.upperRoot)
		m.mu.Unlock()
	}
	return m.finishOp(made, path.Dir(dp), sp, dp, path.Dir(sp), path.Dir(dp))
}

// moveSubtree rewrites index paths under a renamed directory —
// entries, whiteouts, opaque flags, host paths, and their
// per-directory listings. Attributes and inodes are untouched by a
// rename, so the stat truth carries over. Under mu.Lock.
func (idx *upperIndex) moveSubtree(from, to, upperRoot string) {
	rewrite := func(p string) (string, bool) {
		if p == from {
			return to, true
		}
		if strings.HasPrefix(p, from+"/") {
			return to + p[len(from):], true
		}
		return p, false
	}
	ents := idx.st.Entries
	idx.st.Entries = make(map[string]upper.Entry, len(ents))
	for p, e := range ents {
		np, moved := rewrite(p)
		if moved {
			e.Path = np
			e.HostPath = filepath.Join(upperRoot, filepath.FromSlash(np))
		}
		idx.st.Entries[np] = e
	}
	wh := idx.st.Whiteouts
	idx.st.Whiteouts = make(map[string]bool, len(wh))
	for p := range wh {
		np, _ := rewrite(p)
		idx.st.Whiteouts[np] = true
	}
	op := idx.st.Opaque
	idx.st.Opaque = make(map[string]bool, len(op))
	for p := range op {
		np, _ := rewrite(p)
		idx.st.Opaque[np] = true
	}
	idx.kids = map[string][]string{}
	idx.wh = map[string][]string{}
	for p := range idx.st.Entries {
		d, b := splitParent(p)
		idx.kids[d] = insertSorted(idx.kids[d], b)
	}
	for p := range idx.st.Whiteouts {
		d, b := splitParent(p)
		idx.wh[d] = insertSorted(idx.wh[d], b)
	}
}

// Link creates a real hardlink in the upper (REQ-writable-hardlink):
// a base-visible target copies up first and migrates to the
// upper-born identity it now shares with its link.
func (m *Merged) Link(target *Node, dir *Node, name string) (*Node, error) {
	if err := m.writable(); err != nil {
		return nil, err
	}
	if err := checkChildName(name); err != nil {
		return nil, err
	}
	if target.Kind() == KindDir {
		return nil, ErrNotDir
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if ex, ok, err := m.Lookup(dir, name); err != nil {
		return nil, err
	} else if ok {
		ex.Close()
		return nil, ErrExists
	}
	tp := target.Path()
	if _, tmade, err := m.ensureUpper(tp, -1); err != nil {
		return nil, err
	} else if err := m.finishOp(tmade, "", append(tmade, tp)...); err != nil {
		return nil, err
	}
	p := childPath(dir.path, name)
	made, err := m.ensureSpine(p)
	if err != nil {
		return nil, err
	}
	if err := m.write.w.Link(tp, p); err != nil {
		return nil, err
	}
	if err := m.finishOp(made, path.Dir(p), tp, p, path.Dir(p), path.Dir(tp)); err != nil {
		return nil, err
	}
	n, ok, err := m.Lookup(dir, name)
	if err != nil || !ok {
		return nil, fmt.Errorf("linked %q not presented: %w", p, ErrIO)
	}
	return n, nil
}

// Mknod creates a FIFO or socket natively, and a device stand-in
// (REQ-writable-fidelity: an unprivileged writer cannot create the
// real node; presentation follows the record).
func (m *Merged) Mknod(dir *Node, name string, kind Kind, mode uint32, rdev upper.Rdev) (*Node, error) {
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
	switch kind {
	case KindFIFO:
		if err := m.write.w.Mkfifo(p, mode); err != nil {
			return nil, err
		}
	case KindSocket:
		if err := m.write.w.Mksock(p, mode); err != nil {
			return nil, err
		}
	case KindCharDevice:
		if err := m.write.w.MakeStandIn(p, upper.KindCharDev, "", rdev, mode, os.Getuid(), os.Getgid(), time.Time{}, nil); err != nil {
			return nil, err
		}
	case KindBlockDevice:
		if err := m.write.w.MakeStandIn(p, upper.KindBlockDev, "", rdev, mode, os.Getuid(), os.Getgid(), time.Time{}, nil); err != nil {
			return nil, err
		}
	default:
		return nil, ErrNotSupported
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

// SetXattr stores an extended attribute at the mount surface:
// machinery names are unforgeable (REQ-writable-reserved),
// XATTR_CREATE/XATTR_REPLACE judge the presented set, a
// host-refused permission class records the escape, and a caller's
// invalid argument surfaces — never silently escaped (the copy-up
// preservation policy does not apply to caller stores).
func (m *Merged) SetXattr(n *Node, name string, value []byte, flags uint32) error {
	if err := m.writable(); err != nil {
		return err
	}
	if strings.HasPrefix(name, upper.XattrNS) {
		return ErrReserved
	}
	if name == "" {
		return ErrNotSupported
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	p := n.path
	if p == "." {
		return ErrNotSupported
	}
	if flags&(unix.XATTR_CREATE|unix.XATTR_REPLACE) != 0 {
		cur, err := m.NodeAt(p)
		if err != nil {
			return err
		}
		_, present := cur.Xattrs()[name]
		cur.Close()
		if flags&unix.XATTR_CREATE != 0 && present {
			return ErrExists
		}
		if flags&unix.XATTR_REPLACE != 0 && !present {
			return ErrNoAttr
		}
	}
	_, made, err := m.ensureUpper(p, -1)
	if err != nil {
		return err
	}
	e, ok, err := upper.Stat(m.upperRoot, p)
	if err != nil || !ok {
		return ErrNotFound
	}
	if err := m.write.w.SetNativeXattr(p, name, value); err != nil {
		refused := errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) ||
			errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP)
		if !refused {
			// A caller's invalid argument (or any other failure)
			// surfaces — never silently escaped; only host refusal
			// of a valid store routes to the record.
			return err
		}
		switch e.Kind {
		case upper.KindSymlink, upper.KindFifo, upper.KindSocket:
			if !e.StandIn {
				// The native node cannot carry the record: convert,
				// then escape onto the stand-in.
				if err := m.write.w.ConvertToStandIn(p, e.UID, e.GID); err != nil {
					return err
				}
			}
		}
		if err := m.write.w.SetEscapedXattr(p, name, []byte(value)); err != nil {
			return err
		}
		// One stored form per name: a stale native copy (from an
		// earlier permitted store) would race the escape at
		// presentation time.
		_ = unix.Lremovexattr(filepath.Join(m.upperRoot, filepath.FromSlash(p)), name)
	} else {
		// Counterpart cleanup: a stale escape record must not
		// shadow-race the fresh native store.
		_ = unix.Lremovexattr(e.HostPath, upper.XattrEscapePrefix+name)
	}
	return m.finishOp(made, "", append(made, p)...)
}

// RemoveXattr removes a presented extended attribute — native or
// escaped; machinery names are unforgeable.
func (m *Merged) RemoveXattr(n *Node, name string) error {
	if err := m.writable(); err != nil {
		return err
	}
	if strings.HasPrefix(name, upper.XattrNS) {
		return ErrReserved
	}
	m.wmu.Lock()
	defer m.wmu.Unlock()
	p := n.path
	if p == "." {
		return ErrNotSupported
	}
	if _, present := n.Xattrs()[name]; !present {
		return ErrNoAttr
	}
	_, made, err := m.ensureUpper(p, -1)
	if err != nil {
		return err
	}
	removed, err := m.write.w.RemoveXattrRecord(p, name)
	if err != nil {
		return err
	}
	if !removed {
		return ErrNoAttr
	}
	return m.finishOp(made, "", append(made, p)...)
}
