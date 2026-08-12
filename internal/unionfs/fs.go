// Package unionfs presents a unified view (internal/layer) as a
// read-only FUSE filesystem. It consumes the view as-is — entries
// arrive cleaned, sorted, complete, and with hardlinks already
// resolved to their content digests — and resolves regular-file
// content through a caller-supplied digest-to-path resolver, keeping
// the package ignorant of store layout.
package unionfs

import (
	"archive/tar"
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"path"
	"strings"
	"syscall"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/layer"
)

type ociFS struct {
	fs.Inode
	view      *layer.View
	blobPath  func(v1.Hash) string
	extraDirs []string
}

// Init builds the FUSE root for a unified view. blobPath resolves a
// regular-file entry's content digest to its on-disk blob.
func Init(view *layer.View, blobPath func(v1.Hash) string, extraDirs []string) fs.InodeEmbedder {
	return &ociFS{
		view:      view,
		blobPath:  blobPath,
		extraDirs: extraDirs,
	}
}

// headerToFileInfo fills a fuse.Attr struct from a tar.Header.
func headerToFileInfo(out *fuse.Attr, h *tar.Header) {
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
}

var _ = (fs.NodeOnAdder)((*ociFS)(nil))

func (ofs *ociFS) OnAdd(ctx context.Context) {
	for _, entry := range ofs.view.Entries() {
		fileName := entry.Header.Name
		if fileName == "." {
			// Root attributes are not yet projected onto the FUSE
			// root; the entry is positional, not a child.
			continue
		}
		dir, base := path.Split(fileName)
		p := ofs.ensureDir(ctx, dir)

		hdr := entry.Header

		attr := fuse.Attr{}
		headerToFileInfo(&attr, &hdr)

		switch hdr.Typeflag {

		case tar.TypeDir:
			ch := p.GetChild(base)
			if ch == nil {
				ch = p.NewPersistentInode(ctx, &fs.Inode{}, fs.StableAttr{Mode: fuse.S_IFDIR})
				p.AddChild(base, ch, true)
			}

		case tar.TypeSymlink:
			l := &fs.MemSymlink{
				Data: []byte(hdr.Linkname),
			}
			l.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, l, fs.StableAttr{Mode: syscall.S_IFLNK}), false)

		case tar.TypeReg, tar.TypeLink:
			// Hardlinks arrive pre-resolved: Digest and Size carry
			// the content captured at the link's position.
			ch := p.NewPersistentInode(ctx, &ociFile{
				path:     fileName,
				attr:     attr,
				fullPath: ofs.blobPath(entry.Digest),
			}, fs.StableAttr{})
			p.AddChild(base, ch, true)

		case tar.TypeChar:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFCHR}), false)

		case tar.TypeBlock:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFBLK}), false)

		case tar.TypeFifo:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFIFO}), false)

		default:
			slog.Debug("Unsupported file type", "path", fileName, "type", hdr.Typeflag)
		}

	}

	for _, d := range ofs.extraDirs {
		ofs.ensureDir(ctx, d)
	}
}

// ensureDir walks a slash-separated path from the root, creating
// plain directory inodes as needed, and returns the final inode.
func (ofs *ociFS) ensureDir(ctx context.Context, dir string) *fs.Inode {
	p := &ofs.Inode
	for part := range strings.SplitSeq(dir, "/") {
		if len(part) == 0 {
			continue
		}
		ch := p.GetChild(part)
		if ch == nil {
			ch = p.NewPersistentInode(ctx, &fs.Inode{}, fs.StableAttr{Mode: fuse.S_IFDIR})
			p.AddChild(part, ch, true)
		}
		p = ch
	}
	return p
}

type ociFile struct {
	fs.Inode
	path     string
	fullPath string
	attr     fuse.Attr
}

var _ = (fs.NodeOpener)((*ociFile)(nil))

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("Open", "path", of.path, "flags", openFlags, "blobPath", of.fullPath, "size", of.attr.Size)

	f, err := os.Open(of.fullPath)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return nil, 0, syscall.EIO
	}

	return &ociFileHandle{f: f, size: of.attr.Size}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

type ociFileHandle struct {
	f    *os.File
	size uint64
}

var _ = (fs.NodeReader)((*ociFile)(nil))

func (gf *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	slog.Debug("Read", "path", gf.path, "offset", off, "lendest", len(dest))

	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", gf.path, "offset", off)
		return nil, syscall.EIO
	}

	n, err := ofh.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		slog.Error("Error reading file", "path", gf.path, "offset", off, "error", err)
		return nil, syscall.EIO
	}

	slog.Debug("Read", "path", gf.path, "offset", off, "n", n)

	return fuse.ReadResultData(dest), fs.OK
}

var _ = (fs.NodeGetattrer)((*ociFile)(nil))

func (f *ociFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = f.attr
	return fs.OK
}

var _ = (fs.NodeReleaser)((*ociFile)(nil))

func (f *ociFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	slog.Debug("Release", "path", f.path)
	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", f.path)
		return syscall.EIO
	}
	err := ofh.f.Close()
	if err != nil {
		slog.Error("Error closing file", "path", f.path, "error", err)
		return syscall.EIO
	}
	return fs.OK
}
