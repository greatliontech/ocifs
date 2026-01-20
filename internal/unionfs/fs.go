package unionfs

import (
	"archive/tar"
	"context"
	"log/slog"
	"path"
	"syscall"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Ensure UnionFS implements Statfs
var _ = (fs.NodeStatfser)((*UnionFS)(nil))

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

// Statfs returns filesystem statistics for df command.
func (u *UnionFS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Get underlying filesystem stats if we have a writable layer
	if u.writableLayer != nil {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(u.writableLayer.ContentPath(""), &stat); err == nil {
			out.Blocks = stat.Blocks
			out.Bfree = stat.Bfree
			out.Bavail = stat.Bavail
			out.Bsize = uint32(stat.Bsize)
			out.Files = stat.Files
			out.Ffree = stat.Ffree
			out.NameLen = uint32(stat.Namelen)
			return fs.OK
		}
	}

	// Default values for read-only mode or if statfs fails
	out.Blocks = 1 << 20 // ~1TB at 1MB blocks
	out.Bfree = 1 << 19
	out.Bavail = 1 << 19
	out.Bsize = 4096
	out.Files = 1 << 16
	out.Ffree = 1 << 15
	out.NameLen = 255
	return fs.OK
}

// headerToAttr fills a fuse.Attr struct from a tar.Header.
func headerToAttr(h tar.Header) fuse.Attr {
	out := fuse.Attr{}
	// Start with permission bits from Mode
	out.Mode = uint32(h.Mode) & 0777

	// Set file type bits based on Typeflag
	switch h.Typeflag {
	case tar.TypeDir:
		out.Mode |= fuse.S_IFDIR
	case tar.TypeSymlink:
		out.Mode |= fuse.S_IFLNK
	case tar.TypeLink: // hard link - treated as regular file
		out.Mode |= fuse.S_IFREG
	case tar.TypeChar:
		out.Mode |= syscall.S_IFCHR
	case tar.TypeBlock:
		out.Mode |= syscall.S_IFBLK
	case tar.TypeFifo:
		out.Mode |= syscall.S_IFIFO
	default:
		// Regular file or unknown - treat as regular
		out.Mode |= fuse.S_IFREG
	}

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
