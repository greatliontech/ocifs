package unionfs

import (
	"archive/tar"
	"context"
	"log/slog"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

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
}

func (od *unionDir) OnAdd(ctx context.Context) {
	// If this is the root node and we are in read-write mode,
	// ensure the root directory exists in our metadata.
	if od.isRoot && od.writableLayer != nil {
		if hdr := od.writableLayer.GetFile(""); hdr == nil {
			rootAttr := fuse.Attr{Mode: fuse.S_IFDIR | 0755}
			file := &store.File{Hdr: attrToHeader("", &rootAttr, tar.TypeDir)}
			od.writableLayer.SetFile(file)
		}
	}
}

func (od *unionDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := path.Join(od.pathInFs, name)

	// Precedence 1: Writable layer has the final say.
	if od.writableLayer != nil {
		if file := od.writableLayer.GetFile(childPath); file != nil {
			return od.newInodeFromFile(ctx, file, true), fs.OK
		}
		whiteoutPath := path.Join(od.pathInFs, store.WhiteoutPrefix+name)
		if od.writableLayer.GetFile(whiteoutPath) != nil {
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
		writableChildren := od.writableLayer.ListChildren(od.pathInFs)
		for _, file := range writableChildren {
			baseName := path.Base(file.Hdr.Name)
			if strings.HasPrefix(baseName, store.WhiteoutPrefix) {
				originalName := strings.TrimPrefix(baseName, store.WhiteoutPrefix)
				delete(merged, originalName)
			} else {
				merged[baseName] = fuse.DirEntry{Name: baseName, Mode: uint32(file.Hdr.Mode)}
			}
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
	now := time.Now()
	attr := fuse.Attr{
		Mode:  fuse.S_IFDIR | mode,
		Atime: uint64(now.Unix()),
		Mtime: uint64(now.Unix()),
		Ctime: uint64(now.Unix()),
	}
	hdr := attrToHeader(childPath, &attr, tar.TypeDir)
	file := &store.File{Hdr: hdr}
	if err := od.writableLayer.SetFile(file); err != nil {
		return nil, fs.ToErrno(err)
	}

	return od.newDirInode(ctx, childPath), fs.OK
}

func (od *unionDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if od.writableLayer == nil {
		return nil, nil, 0, syscall.EROFS // Read-only file system
	}

	childPath := path.Join(od.pathInFs, name)
	now := time.Now()
	attr := fuse.Attr{
		Mode:  fuse.S_IFREG | mode,
		Atime: uint64(now.Unix()),
		Mtime: uint64(now.Unix()),
		Ctime: uint64(now.Unix()),
	}
	hdr := attrToHeader(childPath, &attr, tar.TypeReg)
	file := &store.File{Hdr: hdr}
	if err := od.writableLayer.SetFile(file); err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	contentPath := file.Path
	f, err := os.OpenFile(contentPath, int(flags), os.FileMode(mode))
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	fileNode := od.newInodeFromFile(ctx, file, true)
	handle := &unionFileHandle{f: f}
	return fileNode, handle, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (od *unionDir) Unlink(ctx context.Context, name string) syscall.Errno {
	slog.Debug("Unlink called", "path", path.Join(od.pathInFs, name))
	if od.writableLayer == nil {
		return syscall.EROFS // Read-only file system
	}

	childPath := path.Join(od.pathInFs, name)

	// If the file exists in the writable layer, just delete its metadata.
	// The content in the `content` dir becomes garbage, can be collected later.
	if od.writableLayer.GetFile(childPath) != nil {
		slog.Debug("Unlinking from writable layer", "path", childPath)
		if err := od.writableLayer.DeleteFile(childPath); err != nil {
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	// If it exists in the read-only layer, create a whiteout file.
	if _, ok := od.roLookup[childPath]; ok {
		slog.Debug("Creating whiteout for read-only layer file", "path", childPath)
		whiteoutPath := path.Join(od.pathInFs, store.WhiteoutPrefix+name)
		hdr := &tar.Header{Name: whiteoutPath, Mode: 0, Size: 0}
		file := &store.File{Hdr: hdr}
		if err := od.writableLayer.SetFile(file); err != nil {
			slog.Error("Failed to set whiteout file in writable layer", "error", err, "path", whiteoutPath)
			return fs.ToErrno(err)
		}
		slog.Debug("Creating whiteout file on disk", "path", file.Path)
		touch, err := os.Create(file.Path)
		if err != nil {
			slog.Error("Failed to create whiteout file", "error", err, "path", file.Path)
			return fs.ToErrno(err)
		}
		if err := touch.Close(); err != nil {
			slog.Error("Failed to close whiteout file", "error", err, "path", file.Path)
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	return syscall.ENOENT
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
	}
	return od.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: fuse.S_IFDIR})
}
