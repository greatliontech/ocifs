//go:build linux || darwin

package upper

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Writer applies dialect mutations to an upper root. Every mutation
// is a sequence of atomic filesystem steps; when a step hook is set
// (tests only), it runs before each step and an error aborts the
// mutation there — so a crash-prefix property test exercises the
// code's real step order, not a re-derivation of it
// (REQ-writable-crash).
type Writer struct {
	root string
	// step, when non-nil, gates every atomic filesystem step.
	step   func(desc string) error
	tmpSeq atomic.Uint64
}

// NewWriter returns a Writer over the upper root.
func NewWriter(root string) *Writer {
	return &Writer{root: root}
}

// SetStepHook installs the test-only crash gate.
func (w *Writer) SetStepHook(h func(desc string) error) { w.step = h }

func (w *Writer) gate(desc string) error {
	if w.step != nil {
		return w.step(desc)
	}
	return nil
}

func (w *Writer) host(rel string) string {
	return filepath.Join(w.root, filepath.FromSlash(rel))
}

// checkName refuses paths the dialect reserves: a basename in the
// whiteout namespace is indistinguishable from a marker
// (REQ-writable-reserved). Marker-writing primitives construct
// marker names internally and never pass them here.
func checkName(rel string) error {
	base := path.Base(rel)
	if strings.HasPrefix(base, WhiteoutPrefix) {
		return fmt.Errorf("upper: name %q is in the reserved marker namespace", rel)
	}
	if rel == "" || rel == "." || rel == ".." || path.IsAbs(rel) ||
		path.Clean(rel) != rel || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("upper: path %q is not a clean relative path", rel)
	}
	return nil
}

// tempName draws a reserved publish-temporary name in dir.
func (w *Writer) tempName(dir string) string {
	return path.Join(dir, TempPrefix+strconv.FormatUint(w.tmpSeq.Add(1), 10)+"."+strconv.Itoa(os.Getpid()))
}

// PublishFile writes a regular file at rel atomically: content and
// attributes land in a reserved temporary, one rename publishes
// (REQ-writable-copyup's publish discipline). xattrs are applied to
// the temporary before the rename, machinery and payload alike, so
// the published node appears fully formed.
func (w *Writer) PublishFile(rel string, content io.Reader, mode uint32, mtime time.Time, xattrs map[string]string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	tmp := w.host(w.tempName(path.Dir(rel)))
	if err := w.gate("temp-create " + rel); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fs.FileMode(mode&0o777))
	if err != nil {
		return err
	}
	if content != nil {
		if _, err := io.Copy(f, content); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	// The umask may have stripped bits; restore the full unix mode
	// including special bits on the not-yet-published temporary.
	if err := unix.Chmod(tmp, mode&0o7777); err != nil {
		os.Remove(tmp)
		return &os.PathError{Op: "chmod", Path: tmp, Err: err}
	}
	for name, val := range xattrs {
		if err := unix.Lsetxattr(tmp, name, []byte(val), 0); err != nil {
			os.Remove(tmp)
			return &os.PathError{Op: "lsetxattr", Path: tmp + ":" + name, Err: err}
		}
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(tmp, mtime, mtime); err != nil {
			os.Remove(tmp)
			return err
		}
	}
	// A gate abort simulates a crash and leaves the orphan exactly
	// as a crash would; only real failures clean up.
	if err := w.gate("publish " + rel); err != nil {
		return err
	}
	return os.Rename(tmp, w.host(rel))
}

// Mkdir creates a directory as an atomic publish: built at a
// reserved temp name with its full umask-proof mode, renamed into
// place refusing an existing destination (POSIX EEXIST).
func (w *Writer) Mkdir(rel string, mode uint32) error {
	if err := checkName(rel); err != nil {
		return err
	}
	tmp := w.host(w.tempName(path.Dir(rel)))
	if err := w.gate("mkdir-temp " + rel); err != nil {
		return err
	}
	if err := os.Mkdir(tmp, fs.FileMode(mode&0o777)); err != nil {
		return err
	}
	// Full mode (umask-proof) lands on the invisible temporary; one
	// rename publishes the fully-formed directory.
	if err := unix.Chmod(tmp, mode&0o7777); err != nil {
		os.Remove(tmp)
		return &os.PathError{Op: "chmod", Path: tmp, Err: err}
	}
	if err := w.gate("mkdir-publish " + rel); err != nil {
		return err
	}
	if err := renameNoReplace(tmp, w.host(rel)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Symlink creates a native symlink — one atomic step.
func (w *Writer) Symlink(target, rel string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("symlink " + rel); err != nil {
		return err
	}
	return os.Symlink(target, w.host(rel))
}

// Mkfifo creates a native FIFO node as an atomic publish, like
// Mkdir.
func (w *Writer) Mkfifo(rel string, mode uint32) error {
	if err := checkName(rel); err != nil {
		return err
	}
	tmp := w.host(w.tempName(path.Dir(rel)))
	if err := w.gate("mkfifo-temp " + rel); err != nil {
		return err
	}
	if err := unix.Mkfifo(tmp, mode&0o777); err != nil {
		return &os.PathError{Op: "mkfifo", Path: tmp, Err: err}
	}
	if err := unix.Chmod(tmp, mode&0o7777); err != nil {
		os.Remove(tmp)
		return &os.PathError{Op: "chmod", Path: tmp, Err: err}
	}
	if err := w.gate("mkfifo-publish " + rel); err != nil {
		return err
	}
	if err := renameNoReplace(tmp, w.host(rel)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Link creates a hardlink to an existing upper entry — one atomic
// step (REQ-writable-hardlink; the copy-up of a base-visible target
// is the caller's prior mutation).
func (w *Writer) Link(oldRel, newRel string) error {
	if err := checkName(oldRel); err != nil {
		return err
	}
	if err := checkName(newRel); err != nil {
		return err
	}
	if err := w.gate("link " + newRel); err != nil {
		return err
	}
	return os.Link(w.host(oldRel), w.host(newRel))
}

// Rename renames within the upper — one atomic step; the
// destination-first composition with whiteouts is the caller's
// ordering (REQ-writable-rename).
func (w *Writer) Rename(oldRel, newRel string) error {
	if err := checkName(oldRel); err != nil {
		return err
	}
	if err := checkName(newRel); err != nil {
		return err
	}
	if err := w.gate("rename " + oldRel + " -> " + newRel); err != nil {
		return err
	}
	return os.Rename(w.host(oldRel), w.host(newRel))
}

// Remove unlinks one upper entry — one atomic step. It refuses
// markers by construction (checkName).
func (w *Writer) Remove(rel string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("remove " + rel); err != nil {
		return err
	}
	return os.Remove(w.host(rel))
}

// Whiteout records the deletion of the base entry at rel — one
// atomic marker create, idempotent (markers are scoped-monotone;
// re-creating an existing marker is a no-op).
func (w *Writer) Whiteout(rel string) error {
	// The target is an ordinary presented path: reserved basenames
	// would mint degenerate markers the walker refuses as damage.
	if err := checkName(rel); err != nil {
		return err
	}
	marker := w.host(path.Join(path.Dir(rel), WhiteoutPrefix+path.Base(rel)))
	if err := w.gate("whiteout " + rel); err != nil {
		return err
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o000)
	if errors.Is(err, fs.ErrExist) {
		return nil // markers are idempotent; re-opening a 0-mode file would EACCES
	}
	if err != nil {
		return err
	}
	return f.Close()
}

// RemoveMarker unlinks the whiteout marker for rel — legal only
// beneath a directory whose own whiteout exists (the rmdir
// dismantling arm, REQ-writable-delete); the caller owns that
// ordering.
func (w *Writer) RemoveMarker(rel string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("remove-marker " + rel); err != nil {
		return err
	}
	return os.Remove(w.host(path.Join(path.Dir(rel), WhiteoutPrefix+path.Base(rel))))
}

// RemoveDir removes an empty upper directory — one atomic step; the
// hide-before-dismantle ordering is the caller's
// (REQ-writable-delete).
func (w *Writer) RemoveDir(rel string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("rmdir " + rel); err != nil {
		return err
	}
	return os.Remove(w.host(rel))
}

// Opaque marks the upper directory at rel opaque — one atomic
// marker create, idempotent.
func (w *Writer) Opaque(rel string) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("opaque " + rel); err != nil {
		return err
	}
	f, err := os.OpenFile(w.host(path.Join(rel, OpaqueMarker)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o000)
	if errors.Is(err, fs.ErrExist) {
		return nil // idempotent, as Whiteout
	}
	if err != nil {
		return err
	}
	return f.Close()
}

// ErrNeedsStandIn reports an override the node kind cannot carry
// natively (the kernel refuses user xattrs on symlinks, FIFOs, and
// sockets); the caller converts the node first (ConvertToStandIn)
// and retries.
var ErrNeedsStandIn = errors.New("upper: node kind cannot carry overrides; convert to a stand-in")

// SetOwner applies ownership: natively when the host permits,
// otherwise as override xattrs on the caller-owned node — clearing
// setuid/setgid exactly as a native chown would
// (REQ-writable-fidelity). Symlinks, FIFOs, and sockets cannot carry
// the override; ErrNeedsStandIn tells the caller to convert.
func (w *Writer) SetOwner(rel string, uid, gid int) error {
	host := w.host(rel)
	if err := w.gate("chown " + rel); err != nil {
		return err
	}
	if err := os.Lchown(host, uid, gid); err == nil {
		return nil
	} else if !errors.Is(err, unix.EPERM) && !errors.Is(err, unix.EINVAL) {
		return err
	}
	var lst unix.Stat_t
	if err := unix.Lstat(host, &lst); err != nil {
		return &os.PathError{Op: "lstat", Path: host, Err: err}
	}
	switch uint32(lst.Mode) & unix.S_IFMT {
	case unix.S_IFREG, unix.S_IFDIR:
	default:
		return ErrNeedsStandIn
	}
	// Clear-first: the one crash intermediate is the cleared mode
	// with the old owner — an interrupted override chown only ever
	// reduces privilege (REQ-writable-fidelity).
	if uint32(lst.Mode)&(unix.S_ISUID|unix.S_ISGID) != 0 {
		if err := w.gate("chown-clear-sugid " + rel); err != nil {
			return err
		}
		if err := unix.Chmod(host, uint32(lst.Mode)&0o1777); err != nil {
			return &os.PathError{Op: "chmod", Path: host, Err: err}
		}
	}
	if err := w.gate("chown-override " + rel); err != nil {
		return err
	}
	if err := unix.Lsetxattr(host, XattrOwner, []byte(strconv.Itoa(uid)+":"+strconv.Itoa(gid)), 0); err != nil {
		return &os.PathError{Op: "lsetxattr", Path: host, Err: err}
	}
	return nil
}

// SetEscapedXattr records an extended attribute the host refused
// natively, verbatim under the escape name (REQ-writable-fidelity).
// The real name must not itself be machinery.
func (w *Writer) SetEscapedXattr(rel, name string, value []byte) error {
	if strings.HasPrefix(name, XattrNS) {
		return fmt.Errorf("upper: xattr %q is in the reserved namespace", name)
	}
	if name == "" {
		return fmt.Errorf("upper: empty xattr name")
	}
	if err := w.gate("xattr-escape " + rel); err != nil {
		return err
	}
	if err := unix.Lsetxattr(w.host(rel), XattrEscapePrefix+name, value, 0); err != nil {
		return &os.PathError{Op: "lsetxattr", Path: rel + ":" + name, Err: err}
	}
	return nil
}

// MakeStandIn publishes a stand-in node at rel: kind, target or
// device numbers, ownership overrides, and mode all land on a
// reserved temporary published by one rename — the node appears
// fully formed or not at all (REQ-writable-dialect).
func (w *Writer) MakeStandIn(rel string, kind Kind, target string, rdev Rdev, mode uint32, uid, gid int, mtime time.Time, extra map[string]string) error {
	xattrs := map[string]string{
		XattrOwner: strconv.Itoa(uid) + ":" + strconv.Itoa(gid),
	}
	switch kind {
	case KindCharDev:
		xattrs[XattrKind] = "char"
		xattrs[XattrRdev] = fmt.Sprintf("%d:%d", rdev.Major, rdev.Minor)
	case KindBlockDev:
		xattrs[XattrKind] = "block"
		xattrs[XattrRdev] = fmt.Sprintf("%d:%d", rdev.Major, rdev.Minor)
	case KindFifo:
		xattrs[XattrKind] = "fifo"
	case KindSocket:
		xattrs[XattrKind] = "socket"
	case KindSymlink:
		if target == "" {
			return fmt.Errorf("upper: symlink stand-in %q needs a target", rel)
		}
		xattrs[XattrKind] = "symlink"
		xattrs[XattrTarget] = target
	default:
		return fmt.Errorf("upper: kind %d is a native node, not a stand-in", kind)
	}
	for name, val := range extra {
		xattrs[XattrEscapePrefix+name] = val
	}
	return w.PublishFile(rel, nil, mode, mtime, xattrs)
}

// ConvertToStandIn replaces a native symlink, FIFO, or socket with
// the stand-in carrying the same presented truth — the escape for
// overrides the native node cannot hold. The replacement publishes
// by rename, so the path always holds one coherent node.
func (w *Writer) ConvertToStandIn(rel string, uid, gid int) error {
	host := w.host(rel)
	var lst unix.Stat_t
	if err := unix.Lstat(host, &lst); err != nil {
		return &os.PathError{Op: "lstat", Path: host, Err: err}
	}
	mtime := time.Unix(lst.Mtim.Sec, lst.Mtim.Nsec)
	mode := uint32(lst.Mode) & 0o7777
	switch uint32(lst.Mode) & unix.S_IFMT {
	case unix.S_IFLNK:
		target, err := os.Readlink(host)
		if err != nil {
			return err
		}
		return w.MakeStandIn(rel, KindSymlink, target, Rdev{}, mode, uid, gid, mtime, nil)
	case unix.S_IFIFO:
		return w.MakeStandIn(rel, KindFifo, "", Rdev{}, mode, uid, gid, mtime, nil)
	case unix.S_IFSOCK:
		return w.MakeStandIn(rel, KindSocket, "", Rdev{}, mode, uid, gid, mtime, nil)
	}
	return fmt.Errorf("upper: %q is not a convertible native node", rel)
}

// StripMachineryXattrs removes every reserved-namespace xattr from a
// node — copy-up's strip of base-borne machinery
// (REQ-writable-reserved: base content cannot smuggle records).
func StripMachineryXattrs(hostPath string) error {
	names, err := listXattrs(hostPath)
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasPrefix(name, XattrNS) {
			if err := unix.Lremovexattr(hostPath, name); err != nil {
				return &os.PathError{Op: "lremovexattr", Path: hostPath + ":" + name, Err: err}
			}
		}
	}
	return nil
}

// Sweep removes crash-orphaned reserved temporaries — the serving
// provider's act on mount, never a reader's (REQ-writable-dialect).
func Sweep(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(path.Base(filepath.ToSlash(p)), TempPrefix) {
			if rmErr := os.RemoveAll(p); rmErr != nil {
				return rmErr
			}
			if d.IsDir() {
				return filepath.SkipDir
			}
		}
		return nil
	})
}

// SetTimes applies a modification time to an upper node without
// following symlinks — one atomic step (the write path's utimens
// arm; also what normalizes directory times after child churn).
func (w *Writer) SetTimes(rel string, mtime time.Time) error {
	if err := checkName(rel); err != nil {
		return err
	}
	if err := w.gate("settimes " + rel); err != nil {
		return err
	}
	ts := unix.NsecToTimespec(mtime.UnixNano())
	err := unix.UtimesNanoAt(unix.AT_FDCWD, w.host(rel), []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		return &os.PathError{Op: "settimes", Path: rel, Err: err}
	}
	return nil
}
