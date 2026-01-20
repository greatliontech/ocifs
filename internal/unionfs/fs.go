package unionfs

import (
	"archive/tar"
	"log/slog"
	"path"

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
// Optional WritableLayerOptions can be passed to configure auto-persist behavior.
func WithWritableLayer(writablePath string, opts ...store.WritableLayerOption) Option {
	return func(od *unionDir) error {
		if writablePath == "" {
			return nil // No-op if path is empty
		}
		slog.Info("Configuring filesystem with a writable layer", "path", writablePath)
		writableLayer, err := store.NewWritableLayer(writablePath, opts...)
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

// WithBlobStore provides a BlobStore for reading content by reference.
// This enables content-addressed access to read-only layer content.
func WithBlobStore(bs store.BlobStore) Option {
	return func(od *unionDir) error {
		od.blobs = bs
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

// PersistWritable saves metadata to disk without stopping auto-persist.
// Use Close() instead when unmounting.
func (u *UnionFS) PersistWritable() error {
	if u.writableLayer != nil {
		return u.writableLayer.Persist()
	}
	return nil
}

// Close stops auto-persist and performs a final persist.
// Call this when unmounting the filesystem.
func (u *UnionFS) Close() error {
	if u.writableLayer != nil {
		return u.writableLayer.Close()
	}
	return nil
}

// WritableLayer returns the writable layer, or nil if in read-only mode.
// Use this to access the layer for committing changes.
func (u *UnionFS) WritableLayer() *store.WritableLayer {
	return u.writableLayer
}

// headerToAttr fills a fuse.Attr struct from a tar.Header.
func headerToAttr(h tar.Header) fuse.Attr {
	out := fuse.Attr{}
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
	return out
}

// NOTE: Remember to call `writableLayer.Persist()` on unmount to save changes!
// You can hook into the Unmount call on the fuse.Server.
// server.Unmount()
// if writableLayer != nil { writableLayer.Persist() }
