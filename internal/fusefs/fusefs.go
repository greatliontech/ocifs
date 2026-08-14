//go:build linux

// Package fusefs serves a projection kernel tree as a read-only FUSE
// filesystem — the linux backend glue (docs/specs/projection.md).
// Inode numbers are the kernel's view-derived IDs, enumeration reads
// the kernel's immutable comparator-sorted snapshots, and
// regular-file bytes come from the content CAS through a
// caller-supplied digest resolver. Every attribute served comes from
// the entry's recorded header — directories and the root included —
// and mutation denial is the kernel-level ro mount option the
// orchestration applies (REQ-proj-ro), not per-operation fallbacks.
package fusefs

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/projection"
)

// Capabilities is the FUSE fidelity envelope (REQ-proj-fidelity):
// byte-ordered case-sensitive namespace, symlinks verbatim, FIFOs
// and devices as typed nodes.
func Capabilities() projection.Capabilities {
	return projection.Capabilities{Symlinks: true, FIFOs: true, Devices: true}
}

// RootStableAttr pins the FUSE root's identity to the kernel's fixed
// root ID (REQ-proj-identity).
func RootStableAttr() *fs.StableAttr {
	return &fs.StableAttr{Mode: fuse.S_IFDIR, Ino: uint64(projection.RootID)}
}

// New builds the FUSE root node for a projection. blobPath resolves
// a regular-file entry's content digest to its on-disk CAS blob.
func New(p *projection.Projection, blobPath func(v1.Hash) string) fs.InodeEmbedder {
	return &node{s: &shared{p: p, blobPath: blobPath}, e: p.Root()}
}

type shared struct {
	p        *projection.Projection
	blobPath func(v1.Hash) string
}

// node serves one projected entry. All state lives in the immutable
// kernel; nodes are stateless views over it.
type node struct {
	fs.Inode
	s *shared
	e *projection.Entry
}

func typeMode(k projection.Kind) uint32 {
	switch k {
	case projection.KindDir:
		return fuse.S_IFDIR
	case projection.KindSymlink:
		return syscall.S_IFLNK
	case projection.KindFIFO:
		return syscall.S_IFIFO
	case projection.KindCharDevice:
		return syscall.S_IFCHR
	case projection.KindBlockDevice:
		return syscall.S_IFBLK
	default:
		return fuse.S_IFREG
	}
}

// fillAttr projects the entry's recorded header into a FUSE attr:
// the full linux envelope — mode bits including suid/sgid/sticky,
// ownership, timestamps, size (REQ-proj-fidelity). Device numbers
// were already dropped by the kernel.
func (n *node) fillAttr(out *fuse.Attr) {
	h := n.e.Header()
	out.Mode = typeMode(n.e.Kind()) | uint32(h.Mode)&0o7777
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	// Every projected entry is one independent node.
	out.Nlink = 1
	// Unrecorded timestamp slots present as the modification time,
	// an unrecorded modification time as the Unix epoch — never as
	// the zero time's year-one artifact (REQ-proj-fidelity).
	mt := h.ModTime
	if mt.IsZero() {
		mt = time.Unix(0, 0)
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

var _ = (fs.NodeGetattrer)((*node)(nil))

func (n *node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.fillAttr(&out.Attr)
	return fs.OK
}

var _ = (fs.NodeLookuper)((*node)(nil))

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	child, ok := n.s.p.Lookup(n.e, name)
	if !ok {
		return nil, syscall.ENOENT
	}
	cn := &node{s: n.s, e: child}
	cn.fillAttr(&out.Attr)
	return n.NewInode(ctx, cn, fs.StableAttr{
		Mode: typeMode(child.Kind()),
		Ino:  uint64(child.ID()),
	}), fs.OK
}

var _ = (fs.NodeReaddirer)((*node)(nil))

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 0, n.e.Len())
	for _, c := range n.e.Children() {
		entries = append(entries, fuse.DirEntry{
			Name: c.Name(),
			Mode: typeMode(c.Kind()),
			Ino:  uint64(c.ID()),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

var _ = (fs.NodeReadlinker)((*node)(nil))

func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n.e.Kind() != projection.KindSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(n.e.LinkTarget()), fs.OK
}

var _ = (fs.NodeOpener)((*node)(nil))

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n.e.Kind() != projection.KindFile {
		return nil, 0, syscall.EINVAL
	}
	f, err := os.Open(n.s.blobPath(n.e.ContentDigest()))
	if err != nil {
		// The store's local trust boundary: a missing blob is damage,
		// surfaced as I/O error (projection.ErrIO's FUSE mapping).
		return nil, 0, syscall.EIO
	}
	return &handle{f: f}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

type handle struct {
	f *os.File
}

var _ = (fs.NodeReader)((*node)(nil))

func (n *node) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h, ok := fh.(*handle)
	if !ok {
		return nil, syscall.EIO
	}
	nr, err := h.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	// Short reads only at EOF (REQ-proj-content): the result carries
	// exactly the bytes read, never the buffer's stale tail.
	return fuse.ReadResultData(dest[:nr]), fs.OK
}

var _ = (fs.NodeReleaser)((*node)(nil))

func (n *node) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	if h, ok := fh.(*handle); ok {
		if err := h.f.Close(); err != nil {
			return syscall.EIO
		}
	}
	return fs.OK
}
