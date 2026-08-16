//go:build linux

package fusefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/projection"
)

// NewWritable builds the FUSE root node for a writable merged
// projection (writable.md): reads resolve through the merge, writes
// route through the provider's write engine, and handles opened
// before a copy-up observe the copied-up object afterwards — one
// logical object, whatever its storage (REQ-writable-copyup).
func NewWritable(m *projection.Merged, blobPath func(v1.Hash) string) fs.InodeEmbedder {
	return &wnode{s: &wshared{m: m, blobPath: blobPath}, n: m.Root()}
}

type wshared struct {
	m        *projection.Merged
	blobPath func(v1.Hash) string
}

// wnode serves one presented entry of the merge. n is the current
// point-in-time presentation, re-pointed under mu after mutation;
// open handles register here so a copy-up can swap their backing.
type wnode struct {
	fs.Inode
	s *wshared

	mu      sync.Mutex
	n       *projection.Node
	handles map[*whandle]struct{}
}

// whandle is one open file handle. Base-backed handles read the CAS
// blob until a copy-up swaps them to the upper file.
type whandle struct {
	mu    sync.Mutex
	f     *os.File
	upper bool
}

func errno(err error) syscall.Errno {
	switch {
	case err == nil:
		return fs.OK
	case errors.Is(err, projection.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, projection.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, projection.ErrExists):
		return syscall.EEXIST
	case errors.Is(err, projection.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, projection.ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, projection.ErrReserved):
		return syscall.EPERM
	case errors.Is(err, projection.ErrNotSupported):
		return syscall.ENOTSUP
	case errors.Is(err, os.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, os.ErrPermission):
		return syscall.EACCES
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return e
	}
	return syscall.EIO
}

// cur returns the node's current presentation.
func (w *wnode) cur() *projection.Node {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

// repoint swaps the presentation after a mutation and, when the
// entry migrated into the upper, swaps registered base handles to
// the upper file — handle continuity across copy-up.
func (w *wnode) repoint(nn *projection.Node) {
	w.mu.Lock()
	defer w.mu.Unlock()
	old := w.n
	w.n = nn
	if old != nil && old != nn {
		old.Close()
	}
	if !nn.UpperBacked() {
		return
	}
	for h := range w.handles {
		h.mu.Lock()
		if !h.upper {
			if nf, err := os.Open(nn.HostPath()); err == nil {
				h.f.Close()
				h.f, h.upper = nf, true
			} else {
				// Loud, never silently stale: a handle that cannot
				// follow the copy-up turns EIO.
				h.f.Close()
				h.f, h.upper = nil, true
			}
		}
		h.mu.Unlock()
	}
}

// refresh re-resolves the node's path and repoints.
func (w *wnode) refresh() error {
	nn, err := w.s.m.NodeAt(w.cur().Path())
	if err != nil {
		return err
	}
	w.repoint(nn)
	return nil
}

func (w *wnode) register(h *whandle) {
	w.mu.Lock()
	if w.handles == nil {
		w.handles = map[*whandle]struct{}{}
	}
	w.handles[h] = struct{}{}
	w.mu.Unlock()
}

func (w *wnode) unregister(h *whandle) {
	w.mu.Lock()
	delete(w.handles, h)
	w.mu.Unlock()
}

func (w *wnode) fillAttr(out *fuse.Attr) {
	n := w.cur()
	h := n.Header()
	out.Mode = typeMode(n.Kind()) | uint32(h.Mode)&0o7777
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.Nlink = uint32(n.Nlink())
	mt := h.ModTime
	if mt.IsZero() {
		mt = time.Unix(0, 0)
	}
	// Live size and times for upper-backed files: content mutation
	// happens through handles, outside index maintenance.
	if n.UpperBacked() && n.Kind() == projection.KindFile && n.Pin() != nil {
		var st unix.Stat_t
		if err := unix.Fstat(int(n.Pin().Fd()), &st); err == nil {
			out.Size = uint64(st.Size)
			mt = time.Unix(st.Mtim.Sec, st.Mtim.Nsec)
		}
	}
	at, ct := h.AccessTime, h.ChangeTime
	if at.IsZero() {
		at = mt
	}
	if ct.IsZero() {
		ct = mt
	}
	out.SetTimes(&at, &mt, &ct)
}

var _ = (fs.NodeGetattrer)((*wnode)(nil))

func (w *wnode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	w.fillAttr(&out.Attr)
	return fs.OK
}

func (w *wnode) newChild(ctx context.Context, n *projection.Node, out *fuse.EntryOut) *fs.Inode {
	cn := &wnode{s: w.s, n: n}
	cn.fillAttr(&out.Attr)
	inode := w.NewInode(ctx, cn, fs.StableAttr{
		Mode: typeMode(n.Kind()),
		Ino:  uint64(n.ID()),
	})
	// An existing inode for this ID keeps serving: hand it the fresh
	// node (closing its old pin) so nothing leaks — one pin per live
	// inode, never per lookup.
	if prev, ok := inode.Operations().(*wnode); ok && prev != cn {
		prev.repoint(n)
	}
	return inode
}

var _ = (fs.NodeLookuper)((*wnode)(nil))

func (w *wnode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n, ok, err := w.s.m.Lookup(w.cur(), name)
	if err != nil {
		return nil, errno(err)
	}
	if !ok {
		return nil, syscall.ENOENT
	}
	return w.newChild(ctx, n, out), fs.OK
}

var _ = (fs.NodeReaddirer)((*wnode)(nil))

func (w *wnode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	snap, err := w.s.m.OpenDir(w.cur())
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, snap.Len())
	for i := 0; i < snap.Len(); i++ {
		row := snap.At(i)
		entries = append(entries, fuse.DirEntry{
			Name: row.Name,
			Mode: typeMode(row.Kind),
			Ino:  uint64(row.ID),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

var _ = (fs.NodeReadlinker)((*wnode)(nil))

func (w *wnode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n := w.cur()
	if n.Kind() != projection.KindSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(n.LinkTarget()), fs.OK
}

var _ = (fs.NodeOpener)((*wnode)(nil))

func (w *wnode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n := w.cur()
	if n.Kind() != projection.KindFile {
		return nil, 0, syscall.EINVAL
	}
	writing := flags&uint32(os.O_WRONLY|os.O_RDWR) != 0
	if writing {
		nn, f, err := w.s.m.OpenWrite(n)
		if err != nil {
			return nil, 0, errno(err)
		}
		w.repoint(nn)
		if flags&uint32(os.O_TRUNC) != 0 {
			if err := f.Truncate(0); err != nil {
				f.Close()
				return nil, 0, errno(err)
			}
			_ = w.s.m.Flushed(nn.Path())
		}
		h := &whandle{f: f, upper: true}
		w.register(h)
		return h, 0, fs.OK
	}
	var f *os.File
	var err error
	upper := n.UpperBacked()
	if upper {
		f, err = os.Open(n.HostPath())
	} else {
		f, err = os.Open(w.s.blobPath(n.ContentDigest()))
	}
	if err != nil {
		return nil, 0, syscall.EIO
	}
	h := &whandle{f: f, upper: upper}
	w.register(h)
	return h, 0, fs.OK
}

var _ = (fs.NodeCreater)((*wnode)(nil))

func (w *wnode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	n, f, err := w.s.m.Create(w.cur(), name, mode&0o7777)
	if err != nil {
		return nil, nil, 0, errno(err)
	}
	inode := w.newChild(ctx, n, out)
	h := &whandle{f: f, upper: true}
	if cn, ok := inode.Operations().(*wnode); ok {
		cn.register(h)
	}
	return inode, h, 0, fs.OK
}

var _ = (fs.NodeMkdirer)((*wnode)(nil))

func (w *wnode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n, err := w.s.m.Mkdir(w.cur(), name, mode&0o7777)
	if err != nil {
		return nil, errno(err)
	}
	return w.newChild(ctx, n, out), fs.OK
}

var _ = (fs.NodeSymlinker)((*wnode)(nil))

func (w *wnode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n, err := w.s.m.Symlink(w.cur(), name, target)
	if err != nil {
		return nil, errno(err)
	}
	return w.newChild(ctx, n, out), fs.OK
}

var _ = (fs.NodeUnlinker)((*wnode)(nil))

func (w *wnode) Unlink(ctx context.Context, name string) syscall.Errno {
	return errno(w.s.m.Unlink(w.cur(), name))
}

var _ = (fs.NodeRmdirer)((*wnode)(nil))

func (w *wnode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return errno(w.s.m.Rmdir(w.cur(), name))
}

var _ = (fs.NodeSetattrer)((*wnode)(nil))

func (w *wnode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	m := w.s.m
	n := w.cur()
	h, _ := fh.(*whandle)
	// A path that no longer resolves with a live upper handle is the
	// open-but-unlinked file (mkstemp-and-unlink): attribute ops
	// apply through the descriptor — no path, no walker, no records.
	// The kernel sends SETATTR without a handle for fchmod/fchown, so
	// the node's registered upper handles serve as the fallback.
	pickHandle := func() *whandle {
		if h != nil && h.upper {
			return h
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		for hh := range w.handles {
			if hh.upper {
				return hh
			}
		}
		return nil
	}
	fdOp := func(err error, apply func(fd int) error) syscall.Errno {
		if !errors.Is(err, projection.ErrNotFound) {
			return errno(err)
		}
		hh := pickHandle()
		if hh == nil {
			return errno(err)
		}
		hh.mu.Lock()
		defer hh.mu.Unlock()
		if hh.f == nil {
			return syscall.EIO
		}
		if aerr := apply(int(hh.f.Fd())); aerr != nil {
			return errno(aerr)
		}
		return fs.OK
	}

	// Ownership before mode: a chown clears setuid/setgid, and a
	// mode in the same request then applies verbatim — the kernel's
	// own notify_change order.
	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
		hd := n.Header()
		nu, ng := hd.Uid, hd.Gid
		if uok {
			nu = int(uid)
		}
		if gok {
			ng = int(gid)
		}
		if err := m.SetOwner(n, nu, ng); err != nil {
			if e := fdOp(err, func(fd int) error { return unix.Fchown(fd, nu, ng) }); e != fs.OK {
				return e
			}
		}
	}
	if mode, ok := in.GetMode(); ok {
		if err := m.SetMode(n, mode&0o7777); err != nil {
			if e := fdOp(err, func(fd int) error { return unix.Fchmod(fd, mode&0o7777) }); e != fs.OK {
				return e
			}
		}
	}
	if sz, ok := in.GetSize(); ok {
		if err := m.Truncate(n, int64(sz)); err != nil {
			if e := fdOp(err, func(fd int) error { return unix.Ftruncate(fd, int64(sz)) }); e != fs.OK {
				return e
			}
		}
	}
	if mt, ok := in.GetMTime(); ok {
		if err := m.SetTimes(n, mt); err != nil {
			e := fdOp(err, func(fd int) error {
				ts := unix.NsecToTimespec(mt.UnixNano())
				return unix.UtimesNanoAt(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd), []unix.Timespec{ts, ts}, 0)
			})
			if e != fs.OK {
				return e
			}
		}
	}
	if err := w.refresh(); err != nil {
		if !errors.Is(err, projection.ErrNotFound) {
			return errno(err)
		}
		// Unlinked: attrs served from the descriptor where live
		// (size, times via the pin); mode/owner of an unlinked file
		// are best-effort in the reply.
	}
	w.fillAttr(&out.Attr)
	return fs.OK
}

var _ = (fs.NodeReader)((*wnode)(nil))

func (w *wnode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h, ok := fh.(*whandle)
	if !ok {
		return nil, syscall.EIO
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.f == nil {
		return nil, syscall.EIO
	}
	nr, err := h.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:nr]), fs.OK
}

var _ = (fs.NodeWriter)((*wnode)(nil))

func (w *wnode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	h, ok := fh.(*whandle)
	if !ok {
		return 0, syscall.EIO
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.upper || h.f == nil {
		return 0, syscall.EBADF
	}
	nw, err := h.f.WriteAt(data, off)
	if err != nil {
		return uint32(nw), errno(err)
	}
	return uint32(nw), fs.OK
}

var _ = (fs.NodeFlusher)((*wnode)(nil))

func (w *wnode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	if h, ok := fh.(*whandle); ok && h.upper {
		_ = w.s.m.Flushed(w.cur().Path())
	}
	return fs.OK
}

var _ = (fs.NodeFsyncer)((*wnode)(nil))

func (w *wnode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	h, ok := fh.(*whandle)
	if !ok {
		return syscall.EIO
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.f == nil {
		return syscall.EIO
	}
	if err := h.f.Sync(); err != nil {
		return syscall.EIO
	}
	return fs.OK
}

var _ = (fs.NodeReleaser)((*wnode)(nil))

func (w *wnode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	h, ok := fh.(*whandle)
	if !ok {
		return fs.OK
	}
	w.unregister(h)
	h.mu.Lock()
	err := h.f.Close()
	upper := h.upper
	h.mu.Unlock()
	if upper {
		_ = w.s.m.Flushed(w.cur().Path())
	}
	if err != nil {
		return syscall.EIO
	}
	return fs.OK
}
