package fuseinterface

import (
	"context"
	"fmt"
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
	var fss []*tarfs.FS
	var files [][]entry

	for i, layer := range layers {
		rc, err := ldb.Get(layer)
		if err != nil {
			fmt.Printf("Error getting layer from db: %v", err)
			return nil, err
		}
		fs, err := tarfs.New(rc)
		if err != nil {
			fmt.Printf("Error creating filesystem: %v", err)
			return nil, err
		}
		fss = append(fss, fs)
		layerFiles := []entry{}
		if err := iofs.WalkDir(fs, ".", func(path string, d iofs.DirEntry, err error) error {
			fmt.Printf("path: %v, layer: %v\n", path, i)
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
		files:     unf,
		fileNames: fls,
	}, nil
}

var _ = (fs.NodeOnAdder)((*ociFS)(nil))

func (ofs *ociFS) OnAdd(ctx context.Context) {
	slog.Info("OnAdd")

	for _, f := range ofs.fileNames {
		dir, base := filepath.Split(filepath.Clean(f))

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
		entry := ofs.files[f]
		ch := p.NewPersistentInode(ctx, &ociFile{layer: ofs.fss[entry.layer], entry: entry.entry, path: f}, fs.StableAttr{})
		p.AddChild(base, ch, true)

	}
}

type ociFile struct {
	fs.Inode
	entry iofs.DirEntry
	layer *tarfs.FS
	path  string
}

var (
	_ = (fs.NodeOpener)((*ociFile)(nil))
	_ = (fs.NodeReader)((*ociFile)(nil))
)

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f, err := of.layer.Open(of.path)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return nil, 0, syscall.EIO
	}
	return &ociFileHandle{f: f}, 0, fs.OK
}

type ociFileHandle struct {
	f iofs.File
}

func (gf *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		return nil, syscall.EIO
	}
	_, err := ofh.f.Read(dest)
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest), fs.OK
}
