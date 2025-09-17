package unionfs

import (
	"archive/tar"
	"context"
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
	_ = (fs.NodeLookuper)((*ociDir)(nil))
	_ = (fs.NodeReaddirer)((*ociDir)(nil))
	_ = (fs.NodeMkdirer)((*ociDir)(nil))
	_ = (fs.NodeCreater)((*ociDir)(nil))
	_ = (fs.NodeUnlinker)((*ociDir)(nil))
)

// ociDir handles operations for a directory in the filesystem.
type ociDir struct {
	fs.Inode
	isRoot        bool
	pathInFs      string
	writableLayer *store.WritableLayer
	roLookup      map[string]*store.File
	roDirs        map[string]bool
	extraDirs     map[string]bool // Directories to ensure exist
}

func (od *ociDir) OnAdd(ctx context.Context) {
	// If this is the root node and we are in read-write mode,
	// ensure the root directory exists in our metadata.
	if od.isRoot && od.writableLayer != nil {
		if hdr := od.writableLayer.GetHeader(""); hdr == nil {
			rootAttr := fuse.Attr{Mode: fuse.S_IFDIR | 0755}
			od.writableLayer.SetHeader(attrToHeader("", &rootAttr, tar.TypeDir))
		}
	}
}

func (od *ociDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := path.Join(od.pathInFs, name)

	// In read-write mode, check the writable layer first.
	if od.writableLayer != nil {
		if hdr := od.writableLayer.GetHeader(childPath); hdr != nil {
			return od.newInodeFromHeader(ctx, hdr, true), fs.OK
		}
		// Check for whiteout
		whiteoutPath := path.Join(od.pathInFs, store.WhiteoutPrefix+name)
		if od.writableLayer.GetHeader(whiteoutPath) != nil {
			return nil, syscall.ENOENT
		}
	}

	// Fallback to read-only layers.
	if roFile, ok := od.roLookup[childPath]; ok {
		return od.newInodeFromHeader(ctx, roFile.Hdr, false), fs.OK
	}
	if _, ok := od.roDirs[childPath]; ok {
		return od.newDirInode(ctx, childPath), fs.OK
	}

	return nil, syscall.ENOENT
}

func (od *ociDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
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

	// 2. In read-write mode, overlay changes from the writable layer.
	if od.writableLayer != nil {
		writableChildren := od.writableLayer.ListChildren(od.pathInFs)
		for _, hdr := range writableChildren {
			baseName := path.Base(hdr.Name)
			if strings.HasPrefix(baseName, store.WhiteoutPrefix) {
				originalName := strings.TrimPrefix(baseName, store.WhiteoutPrefix)
				delete(merged, originalName)
			} else {
				merged[baseName] = fuse.DirEntry{Name: baseName, Mode: uint32(hdr.Mode)}
			}
		}
	}

	var entries []fuse.DirEntry
	for _, entry := range merged {
		entries = append(entries, entry)
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (od *ociDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
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
	od.writableLayer.SetHeader(hdr)

	return od.newDirInode(ctx, childPath), fs.OK
}

func (od *ociDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
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
	od.writableLayer.SetHeader(hdr)

	contentPath := od.writableLayer.GetContentPath(childPath)
	f, err := os.OpenFile(contentPath, int(flags), os.FileMode(mode))
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	fileNode := od.newInodeFromHeader(ctx, hdr, true)
	handle := &ociFileHandle{f: f}
	return fileNode, handle, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (od *ociDir) Unlink(ctx context.Context, name string) syscall.Errno {
	if od.writableLayer == nil {
		return syscall.EROFS // Read-only file system
	}

	childPath := path.Join(od.pathInFs, name)

	// If the file exists in the writable layer, just delete its metadata.
	// The content in the `content` dir becomes garbage, can be collected later.
	if od.writableLayer.GetHeader(childPath) != nil {
		od.writableLayer.DeleteHeader(childPath)
		return fs.OK
	}

	// If it exists in the read-only layer, create a whiteout file.
	if _, ok := od.roLookup[childPath]; ok {
		whiteoutPath := path.Join(od.pathInFs, store.WhiteoutPrefix+name)
		hdr := &tar.Header{Name: whiteoutPath, Mode: 0, Size: 0}
		od.writableLayer.SetHeader(hdr)
		return fs.OK
	}

	return syscall.ENOENT
}

// newInodeFromHeader decides whether to create a file or directory Inode.
func (od *ociDir) newInodeFromHeader(ctx context.Context, hdr *tar.Header, isWritable bool) *fs.Inode {
	isDir := hdr.Typeflag == tar.TypeDir || (hdr.Mode&syscall.S_IFMT) == syscall.S_IFDIR
	if isDir {
		return od.newDirInode(ctx, hdr.Name)
	}

	fileNode := &ociFile{
		pathInFs:      hdr.Name,
		hdr:           hdr,
		isWritable:    isWritable,
		roLookup:      od.roLookup,
		writableLayer: od.writableLayer,
	}
	return od.NewPersistentInode(ctx, fileNode, fs.StableAttr{})
}

// newDirInode creates a directory Inode.
func (od *ociDir) newDirInode(ctx context.Context, path string) *fs.Inode {
	dirNode := &ociDir{
		pathInFs:      path,
		writableLayer: od.writableLayer,
		roLookup:      od.roLookup,
		roDirs:        od.roDirs,
	}
	return od.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: fuse.S_IFDIR})
}
