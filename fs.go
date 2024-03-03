package ocifs

import (
	"archive/tar"
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type ociFS struct {
	fs.Inode
	files     map[string]unionFile
	fileNames []string
}

func NewOciFS(store *Store, h v1.Hash) (fs.InodeEmbedder, error) {
	fls, unf, err := unify(store, h)
	if err != nil {
		return nil, err
	}

	return &ociFS{
		files:     unf,
		fileNames: fls,
	}, nil
}

var _ = (fs.NodeOnAdder)((*ociFS)(nil))

// HeaderToFileInfo fills a fuse.Attr struct from a tar.Header.
func HeaderToFileInfo(out *fuse.Attr, h *tar.Header) {
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
}

func (ofs *ociFS) OnAdd(ctx context.Context) {
	slog.Info("OnAdd")

	for _, f := range ofs.fileNames {
		dir, base := filepath.Split(filepath.Clean(f))
		entry := ofs.files[f]

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
		attr := &fuse.Attr{}
		HeaderToFileInfo(attr, entry.entry)
		ch := p.NewPersistentInode(ctx, &ociFile{
			path:      f,
			attr:      attr,
			layerPath: entry.root,
		}, fs.StableAttr{})
		slog.Info("Added file", "path", f, "layer", entry.layer)
		p.AddChild(base, ch, true)

	}
}

type ociFile struct {
	fs.Inode
	attr      *fuse.Attr
	path      string
	layerPath string
}

var (
	_ = (fs.NodeOpener)((*ociFile)(nil))
	_ = (fs.NodeReader)((*ociFile)(nil))
)

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Info("Open", "path", of.path)

	filePath := filepath.Join(of.layerPath, of.path)

	f, err := os.Open(filePath)
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

func (gf *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	slog.Info("Read", "path", gf.path, "offset", off)

	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", gf.path, "offset", off)
		return nil, syscall.EIO
	}

	_, err := ofh.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		slog.Error("Error reading file", "path", gf.path, "offset", off, "error", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest), fs.OK
}

var _ = (fs.NodeGetattrer)((*ociFile)(nil))

func (f *ociFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = *f.attr
	return fs.OK
}
