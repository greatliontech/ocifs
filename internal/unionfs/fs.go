package unionfs

import (
	"archive/tar"
	"log/slog"
	"path"
	"time"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// UnionFS is the root of our filesystem. It holds all top-level configuration.
type UnionFS struct {
	unionDir // Embed the directory logic
}

// Option is a function that configures the ociFS.
type Option func(*unionDir) error

// WithWritableLayer enables read-write mode by providing a path for the upper layer.
func WithWritableLayer(writablePath string) Option {
	return func(od *unionDir) error {
		if writablePath == "" {
			return nil // No-op if path is empty
		}
		slog.Info("Configuring filesystem with a writable layer", "path", writablePath)
		writableLayer, err := store.NewWritableLayer(writablePath)
		if err != nil {
			return err
		}
		od.writableLayer = writableLayer
		return nil
	}
}

// WithExtraDirs ensures a list of directories are present in the filesystem.
func WithExtraDirs(dirs []string) Option {
	return func(od *unionDir) error {
		slog.Info("Configuring filesystem with extra directories", "dirs", dirs)
		for _, dir := range dirs {
			// Ensure we have all parent directories as well.
			d := dir
			for d != "/" && d != "." {
				od.extraDirs[d] = true
				d = path.Dir(d)
			}
		}
		return nil
	}
}

// Init sets up the union filesystem using functional options.
func Init(img *store.Image, opts ...Option) (*UnionFS, error) {
	files := img.Unify()
	roLookup := make(map[string]*store.File, len(files))
	roDirs := make(map[string]bool)

	roDirs[""] = true // Root is always a dir
	for _, f := range files {
		roLookup[f.Hdr.Name] = f
		dir := path.Dir(f.Hdr.Name)
		for dir != "." && dir != "/" {
			roDirs[dir] = true
			dir = path.Dir(dir)
		}
	}

	// Setup the root directory node with defaults.
	rootDir := &UnionFS{unionDir: unionDir{
		isRoot:    true,
		pathInFs:  "",
		roLookup:  roLookup,
		roDirs:    roDirs,
		extraDirs: make(map[string]bool),
	}}

	// Apply all the provided options.
	for _, opt := range opts {
		if err := opt(&rootDir.unionDir); err != nil {
			return nil, err
		}
	}

	if rootDir.writableLayer == nil {
		slog.Info("Initializing filesystem in read-only mode")
	} else {
		slog.Info("Initializing filesystem in read-write mode")
	}

	return rootDir, nil
}

func (u *UnionFS) PersistWritable() error {
	if u.writableLayer != nil {
		return u.writableLayer.Persist()
	}
	return nil
}

// headerToAttr fills a fuse.Attr struct from a tar.Header.
func headerToAttr(out *fuse.Attr, h *tar.Header) {
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
}

// attrToHeader creates a new tar.Header for a new file or directory.
func attrToHeader(name string, attr *fuse.Attr, typeflag byte) *tar.Header {
	now := time.Now()
	return &tar.Header{
		Name:       name,
		Mode:       int64(attr.Mode),
		Uid:        int(attr.Uid),
		Gid:        int(attr.Gid),
		Size:       int64(attr.Size),
		ModTime:    now,
		AccessTime: now,
		ChangeTime: now,
		Typeflag:   typeflag,
	}
}

// NOTE: Remember to call `writableLayer.Persist()` on unmount to save changes!
// You can hook into the Unmount call on the fuse.Server.
// server.Unmount()
// if writableLayer != nil { writableLayer.Persist() }
