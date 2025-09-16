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

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type ociFS struct {
	fs.Inode
	img       *store.Image
	files     []*store.File
	lookup    map[string]*store.File
	extraDirs []string
}

func Init(img *store.Image, extraDirs []string) fs.InodeEmbedder {
	files := Unify(img.Layers())
	lookup := make(map[string]*store.File, len(files))
	for _, f := range files {
		lookup[f.Hdr.Name] = f
	}
	return &ociFS{
		img:       img,
		files:     files,
		lookup:    lookup,
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
	for _, file := range ofs.files {
		fileName := file.Hdr.Name
		dir, base := path.Split(fileName)

		// create parent directories as needed. TODO: we might not need this since we sort on
		// unifications
		p := &ofs.Inode
		for _, part := range strings.Split(dir, "/") {
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

		hdr := file.Hdr

		attr := fuse.Attr{}
		headerToFileInfo(&attr, hdr)

		switch hdr.Typeflag {

		case tar.TypeSymlink:
			l := &fs.MemSymlink{
				Data: []byte(hdr.Linkname),
			}
			l.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, l, fs.StableAttr{Mode: syscall.S_IFLNK}), false)

		// for hardlinks we create an inode pointing to the link file in it's layer whith it's size
		case tar.TypeLink:
			linkEntry, ok := ofs.lookup[hdr.Linkname]
			if !ok {
				slog.Debug("Missing link", "path", hdr.Linkname, "filepath", hdr.Name)
				break
			}
			attr.Size = uint64(linkEntry.Hdr.Size)
			ch := p.NewPersistentInode(ctx, &ociFile{
				path:     hdr.Linkname,
				attr:     attr,
				fullPath: linkEntry.Path,
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

		case tar.TypeReg:
			ch := p.NewPersistentInode(ctx, &ociFile{
				path:     fileName,
				attr:     attr,
				fullPath: file.Path,
			}, fs.StableAttr{})
			p.AddChild(base, ch, true)
		default:
			slog.Debug("Unsupported file type", "path", fileName, "type", hdr.Typeflag)
		}

	}

	for _, d := range ofs.extraDirs {
		p := &ofs.Inode
		for _, part := range strings.Split(d, "/") {
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
	}
}

type ociFile struct {
	fs.Inode
	path     string
	fullPath string
	attr     fuse.Attr
}

var _ = (fs.NodeOpener)((*ociFile)(nil))

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("Open", "path", of.path, "flags", openFlags, "layerPath", of.fullPath, "size", of.attr.Size)

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
