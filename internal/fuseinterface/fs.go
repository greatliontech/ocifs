package fuseinterface

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"log"
	"log/slog"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/greatliontech/ocifs/internal/layerdb"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/quay/claircore/pkg/tarfs"
)

type ociFS struct {
	fs.Inode
	files     map[string]unionFile
	lp        layout.Path
	fss       []*tarfs.FS
	fileNames []string
	fssrcs    []layerdb.ReaderAtCloser
}

func NewOciFS(imageRef string) (fs.InodeEmbedder, error) {
	lp := layout.Path("./layout")

	ref, err := name.ParseReference(imageRef)
	if err != nil {
		fmt.Printf("Error parsing reference: %v", err)
		return nil, err
	}

	head, err := remote.Head(ref)
	if err != nil {
		fmt.Printf("Error getting head: %v", err)
		return nil, err
	}

	dgst := head.Digest
	var img v1.Image

	img, err = lp.Image(dgst)
	if err != nil {
		fmt.Printf("Local image not found: %v", err)
		rmtImg, err := remote.Image(ref)
		if err != nil {
			fmt.Printf("Error getting remote image: %v", err)
			return nil, err
		}

		if err := lp.AppendImage(rmtImg); err != nil {
			fmt.Printf("Error appending image: %v", err)
			return nil, err
		}
		img, err = lp.Image(dgst)
		if err != nil {
			fmt.Printf("Error getting local image: %v", err)
			return nil, err
		}
	}

	layers, err := img.Layers()
	if err != nil {
		fmt.Printf("Error getting layers: %v", err)
		return nil, err
	}

	ldb := layerdb.NewLayerDB("./layers")
	var fssrcs []layerdb.ReaderAtCloser
	var fss []*tarfs.FS
	var files [][]entry

	for _, layer := range layers {
		rc, err := ldb.Get(layer)
		if err != nil {
			fmt.Printf("Error getting layer from db: %v", err)
			return nil, err
		}
		fssrcs = append(fssrcs, rc)
		fs, err := tarfs.New(rc)
		if err != nil {
			fmt.Printf("Error creating filesystem: %v", err)
			return nil, err
		}
		fss = append(fss, fs)
		layerFiles := []entry{}
		if err := iofs.WalkDir(fs, ".", func(path string, d iofs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			layerFiles = append(layerFiles, entry{d, path})
			return nil
		}); err != nil {
			return nil, err
		}
		files = append(files, layerFiles)
	}

	fls, unf := unify(files)

	return &ociFS{
		lp:        lp,
		fss:       fss,
		fssrcs:    fssrcs,
		files:     unf,
		fileNames: fls,
	}, nil
}

var _ = (fs.NodeOnAdder)((*ociFS)(nil))

func fileInfoToAttr(fi iofs.FileInfo) *fuse.Attr {
	return &fuse.Attr{
		Mode:   uint32(fi.Mode().Perm()),
		Size:   uint64(fi.Size()),
		Blocks: uint64(fi.Size() / 512),
		Atime:  uint64(fi.ModTime().Unix()),
		Mtime:  uint64(fi.ModTime().Unix()),
		Ctime:  uint64(fi.ModTime().Unix()),
	}
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
		finfo, err := entry.entry.Info()
		if err != nil {
			fmt.Printf("Error getting file info: %v", err)
			continue
		}
		attr := fileInfoToAttr(finfo)
		ch := p.NewPersistentInode(ctx, &ociFile{
			layer: ofs.fss[entry.layer],
			entry: entry.entry,
			path:  f,
			attr:  attr,
		}, fs.StableAttr{})
		slog.Info("Added file", "path", f, "layer", entry.layer)
		p.AddChild(base, ch, true)

	}
}

type ociFile struct {
	fs.Inode
	entry iofs.DirEntry
	layer *tarfs.FS
	attr  *fuse.Attr
	path  string
}

var (
	_ = (fs.NodeOpener)((*ociFile)(nil))
	_ = (fs.NodeReader)((*ociFile)(nil))
)

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Info("Open", "path", of.path)

	f, err := of.layer.Open(of.path)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return nil, 0, syscall.EIO
	}
	return &ociFileHandle{f: f, size: of.attr.Size}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

type ociFileHandle struct {
	f    iofs.File
	size uint64
}

func (gf *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	slog.Info("Read", "path", gf.path, "offset", off)

	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", gf.path, "offset", off)
		return nil, syscall.EIO
	}

	data := make([]byte, ofh.size)

	end := int(off) + len(dest)
	if end > len(data) {
		end = len(data)
	}

	_, err := ofh.f.Read(data)
	if err != nil && err != io.EOF {
		slog.Error("Error reading file", "path", gf.path, "offset", off, "error", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data[off:end]), fs.OK
}

var _ = (fs.NodeGetattrer)((*ociFile)(nil))

func (f *ociFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = *f.attr
	return fs.OK
}

// var _ = (fs.NodeWriter)((*ociFile)(nil))
//
// func (f *ociFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
// 	slog.Info("Write", "path", f.path, "offset", off, "data", data)
//
// 	return 0, syscall.EIO
// }
