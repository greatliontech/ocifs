package unionfs

import (
	"archive/tar"
	"context"
	"log/slog"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Ensure ociDir implements all necessary interfaces
var (
	_ = (fs.NodeLookuper)((*unionDir)(nil))
	_ = (fs.NodeReaddirer)((*unionDir)(nil))
	_ = (fs.NodeMkdirer)((*unionDir)(nil))
	_ = (fs.NodeCreater)((*unionDir)(nil))
	_ = (fs.NodeUnlinker)((*unionDir)(nil))
)

// unionDir handles operations for a directory in the filesystem.
type unionDir struct {
	fs.Inode
	isRoot        bool
	pathInFs      string
	writableLayer *store.WritableLayer
	roLookup      map[string]*store.File
	roDirs        map[string]bool
	extraDirs     map[string]bool // Directories to ensure exist
	blobs         store.BlobStore // Optional: for reading content by reference
}

func (od *unionDir) OnAdd(ctx context.Context) {
	// If this is the root node and we are in read-write mode,
	// ensure the root directory exists in our metadata.
	if od.isRoot && od.writableLayer != nil {
		if !od.writableLayer.Exists("") {
			od.writableLayer.Create("", 0755, true)
		}
	}
}

func (od *unionDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := path.Join(od.pathInFs, name)

	// Precedence 1: Writable layer has the final say.
	if od.writableLayer != nil {
		if file := od.writableLayer.Get(childPath); file != nil {
			return od.newInodeFromFile(ctx, file, true), fs.OK
		}
		if od.writableLayer.IsWhiteout(childPath) {
			return nil, syscall.ENOENT
		}
	}

	// Precedence 2: Read-only OCI layers.
	if roFile, ok := od.roLookup[childPath]; ok {
		return od.newInodeFromFile(ctx, roFile, false), fs.OK
	}
	if _, ok := od.roDirs[childPath]; ok {
		return od.newDirInode(ctx, childPath), fs.OK
	}

	// Precedence 3: Virtual extra directories.
	if _, ok := od.extraDirs[childPath]; ok {
		return od.newDirInode(ctx, childPath), fs.OK
	}

	return nil, syscall.ENOENT
}

func (od *unionDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	merged := make(map[string]fuse.DirEntry)
	prefix := od.pathInFs
	if prefix != "" {
		prefix += "/"
	}

	// 1. Add children from read-only layers.
	for p, f := range od.roLookup {
		if strings.HasPrefix(p, prefix) {
			childName := strings.TrimPrefix(p, prefix)
			if !strings.Contains(childName, "/") {
				merged[childName] = fuse.DirEntry{Name: childName, Mode: uint32(f.Hdr.Mode)}
			}
		}
	}
	for p := range od.roDirs {
		if strings.HasPrefix(p, prefix) {
			childName := strings.TrimPrefix(p, prefix)
			if childName != "" && !strings.Contains(childName, "/") {
				merged[childName] = fuse.DirEntry{Name: childName, Mode: fuse.S_IFDIR}
			}
		}
	}

	// 2. Add virtual extra directories.
	for p := range od.extraDirs {
		if strings.HasPrefix(p, prefix) {
			childName := strings.TrimPrefix(p, prefix)
			if childName != "" && !strings.Contains(childName, "/") {
				merged[childName] = fuse.DirEntry{Name: childName, Mode: fuse.S_IFDIR}
			}
		}
	}

	// 3. Overlay changes from the writable layer.
	if od.writableLayer != nil {
		// Add files from writable layer
		for _, file := range od.writableLayer.List(od.pathInFs) {
			baseName := path.Base(file.Hdr.Name)
			merged[baseName] = fuse.DirEntry{Name: baseName, Mode: uint32(file.Hdr.Mode)}
		}
		// Remove whited-out files
		for _, name := range od.writableLayer.Whiteouts(od.pathInFs) {
			delete(merged, name)
		}
	}

	var entries []fuse.DirEntry
	for _, entry := range merged {
		entries = append(entries, entry)
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (od *unionDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if od.writableLayer == nil {
		return nil, syscall.EROFS // Read-only file system
	}

	childPath := path.Join(od.pathInFs, name)
	if _, err := od.writableLayer.Create(childPath, os.FileMode(mode), true); err != nil {
		return nil, fs.ToErrno(err)
	}

	return od.newDirInode(ctx, childPath), fs.OK
}

func (od *unionDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if od.writableLayer == nil {
		return nil, nil, 0, syscall.EROFS // Read-only file system
	}

	childPath := path.Join(od.pathInFs, name)
	file, err := od.writableLayer.Create(childPath, os.FileMode(mode), false)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	f, err := od.writableLayer.OpenContent(childPath, int(flags)|os.O_CREATE, os.FileMode(mode))
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	fileNode := od.newInodeFromFile(ctx, file, true)
	handle := &unionFileHandle{f: f}
	return fileNode, handle, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (od *unionDir) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(od.pathInFs, name)
	slog.Debug("Unlink called", "path", childPath)

	if od.writableLayer == nil {
		return syscall.EROFS // Read-only file system
	}

	// If the file exists in the writable layer, delete it.
	if od.writableLayer.Exists(childPath) {
		slog.Debug("Unlinking from writable layer", "path", childPath)
		if err := od.writableLayer.Remove(childPath); err != nil {
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	// If it exists in the read-only layer, create a whiteout.
	if _, ok := od.roLookup[childPath]; ok {
		slog.Debug("Creating whiteout for read-only layer file", "path", childPath)
		if err := od.writableLayer.Whiteout(childPath); err != nil {
			slog.Error("Failed to create whiteout", "error", err, "path", childPath)
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	return syscall.ENOENT
}

func (od *unionDir) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
	slog.Debug("Truncate called on directory", "path", od.pathInFs, "name", name, "offset", offset)
	return fuse.ENOSYS
}

// newInodeFromHeader decides whether to create a file or directory Inode.
func (od *unionDir) newInodeFromFile(ctx context.Context, file *store.File, isWritable bool) *fs.Inode {
	isDir := file.Hdr.Typeflag == tar.TypeDir || (file.Hdr.Mode&syscall.S_IFMT) == syscall.S_IFDIR
	if isDir {
		return od.newDirInode(ctx, file.Hdr.Name)
	}

	fileNode := &unionFile{
		pathInFs:      file.Hdr.Name,
		file:          file,
		isWritable:    isWritable,
		roLookup:      od.roLookup,
		writableLayer: od.writableLayer,
		blobs:         od.blobs,
	}
	return od.NewPersistentInode(ctx, fileNode, fs.StableAttr{})
}

// newDirInode creates a directory Inode.
func (od *unionDir) newDirInode(ctx context.Context, path string) *fs.Inode {
	dirNode := &unionDir{
		pathInFs:      path,
		writableLayer: od.writableLayer,
		roLookup:      od.roLookup,
		roDirs:        od.roDirs,
		blobs:         od.blobs,
	}
	return od.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: fuse.S_IFDIR})
}
