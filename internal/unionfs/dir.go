package unionfs

import (
	"archive/tar"
	"context"
	"io"
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
	_ = (fs.NodeRmdirer)((*unionDir)(nil))
	_ = (fs.NodeRenamer)((*unionDir)(nil))
	_ = (fs.NodeSymlinker)((*unionDir)(nil))
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

func (od *unionDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(od.pathInFs, name)
	slog.Debug("Rmdir called", "path", childPath)

	if od.writableLayer == nil {
		return syscall.EROFS
	}

	// Check if directory is empty by looking at all layers
	prefix := childPath + "/"

	// Check writable layer for children
	if od.writableLayer != nil {
		children := od.writableLayer.List(childPath)
		if len(children) > 0 {
			slog.Debug("Rmdir: directory not empty (writable layer)", "path", childPath, "children", len(children))
			return syscall.ENOTEMPTY
		}
	}

	// Check read-only layers for children (excluding whiteouts)
	whiteouts := make(map[string]bool)
	if od.writableLayer != nil {
		for _, name := range od.writableLayer.Whiteouts(childPath) {
			whiteouts[name] = true
		}
	}
	for p := range od.roLookup {
		if strings.HasPrefix(p, prefix) {
			relPath := strings.TrimPrefix(p, prefix)
			if !strings.Contains(relPath, "/") {
				// This is a direct child - check if it's whited out
				if !whiteouts[relPath] {
					slog.Debug("Rmdir: directory not empty (RO layer)", "path", childPath, "child", relPath)
					return syscall.ENOTEMPTY
				}
			}
		}
	}
	for p := range od.roDirs {
		if strings.HasPrefix(p, prefix) {
			relPath := strings.TrimPrefix(p, prefix)
			if relPath != "" && !strings.Contains(relPath, "/") {
				if !whiteouts[relPath] {
					slog.Debug("Rmdir: directory not empty (RO subdir)", "path", childPath, "child", relPath)
					return syscall.ENOTEMPTY
				}
			}
		}
	}

	// Directory is empty - now check if it exists and remove it

	// If it exists in the writable layer, remove it
	if od.writableLayer.Exists(childPath) {
		slog.Debug("Rmdir: removing from writable layer", "path", childPath)
		if err := od.writableLayer.Remove(childPath); err != nil {
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	// If it exists in the read-only layer, create a whiteout
	if _, ok := od.roDirs[childPath]; ok {
		slog.Debug("Rmdir: creating whiteout for RO directory", "path", childPath)
		if err := od.writableLayer.Whiteout(childPath); err != nil {
			return fs.ToErrno(err)
		}
		return fs.OK
	}

	return syscall.ENOENT
}

func (od *unionDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := path.Join(od.pathInFs, name)

	// Get the target unionDir - could be UnionFS (root) or unionDir
	var newDir *unionDir
	switch v := newParent.(type) {
	case *unionDir:
		newDir = v
	case *UnionFS:
		newDir = &v.unionDir
	default:
		// Try to get the embedded inode's operations
		ops := newParent.EmbeddedInode().Operations()
		switch v := ops.(type) {
		case *unionDir:
			newDir = v
		case *UnionFS:
			newDir = &v.unionDir
		default:
			slog.Error("Rename: newParent is not a unionDir or UnionFS", "type", ops)
			return syscall.EINVAL
		}
	}
	newPath := path.Join(newDir.pathInFs, newName)

	slog.Debug("Rename called", "oldPath", oldPath, "newPath", newPath)

	if od.writableLayer == nil {
		return syscall.EROFS
	}

	// Check if source exists in any layer
	var srcFile *store.File
	var srcIsWritable bool
	var srcIsDir bool

	// Check writable layer first
	if od.writableLayer != nil {
		if file := od.writableLayer.Get(oldPath); file != nil {
			srcFile = file
			srcIsWritable = true
			srcIsDir = file.Hdr.Typeflag == tar.TypeDir
		}
	}

	// Check read-only layers
	if srcFile == nil {
		if roFile, ok := od.roLookup[oldPath]; ok {
			srcFile = roFile
			srcIsWritable = false
			srcIsDir = roFile.Hdr.Typeflag == tar.TypeDir
		} else if _, ok := od.roDirs[oldPath]; ok {
			srcIsDir = true
			srcIsWritable = false
			// Create a minimal File for the directory
			srcFile = &store.File{
				Hdr: tar.Header{
					Name:     oldPath,
					Typeflag: tar.TypeDir,
					Mode:     0755,
				},
			}
		}
	}

	if srcFile == nil {
		return syscall.ENOENT
	}

	// For directories, check if we're trying to move into a subdirectory of itself
	if srcIsDir && strings.HasPrefix(newPath+"/", oldPath+"/") {
		return syscall.EINVAL
	}

	// Check if destination exists
	destExists := false
	var destIsDir bool

	if od.writableLayer != nil {
		if file := od.writableLayer.Get(newPath); file != nil {
			destExists = true
			destIsDir = file.Hdr.Typeflag == tar.TypeDir
		}
	}
	if !destExists {
		if _, ok := od.roLookup[newPath]; ok {
			destExists = true
			destIsDir = false
		} else if _, ok := od.roDirs[newPath]; ok {
			destExists = true
			destIsDir = true
		}
	}

	// Handle RENAME_NOREPLACE flag
	if flags&1 != 0 && destExists { // RENAME_NOREPLACE = 1
		return syscall.EEXIST
	}

	// If dest exists, handle based on types
	if destExists {
		if srcIsDir && !destIsDir {
			return syscall.ENOTDIR
		}
		if !srcIsDir && destIsDir {
			return syscall.EISDIR
		}
		if destIsDir {
			// Destination is a directory - check if it's empty
			// (for simplicity, just remove it if it exists in writable layer)
			if od.writableLayer.Exists(newPath) {
				children := od.writableLayer.List(newPath)
				if len(children) > 0 {
					return syscall.ENOTEMPTY
				}
				if err := od.writableLayer.Remove(newPath); err != nil {
					return fs.ToErrno(err)
				}
			}
		} else {
			// Destination is a file - unlink it
			if od.writableLayer.Exists(newPath) {
				if err := od.writableLayer.Remove(newPath); err != nil {
					return fs.ToErrno(err)
				}
			}
			if _, ok := od.roLookup[newPath]; ok {
				if err := od.writableLayer.Whiteout(newPath); err != nil {
					return fs.ToErrno(err)
				}
			}
		}
	}

	// Perform the rename
	if srcIsWritable {
		// Source is in writable layer - need to move content and metadata
		slog.Debug("Rename: moving within writable layer", "oldPath", oldPath, "newPath", newPath)

		if srcIsDir {
			// For directories, create new dir and whiteout old
			if _, err := od.writableLayer.Create(newPath, os.FileMode(srcFile.Hdr.Mode), true); err != nil {
				return fs.ToErrno(err)
			}
			if err := od.writableLayer.Remove(oldPath); err != nil {
				return fs.ToErrno(err)
			}
		} else {
			// For files, we need to move the content and update metadata
			oldContent := od.writableLayer.ContentPath(oldPath)
			newContent := od.writableLayer.ContentPath(newPath)

			// Create parent directory for new path
			if err := os.MkdirAll(path.Dir(newContent), 0755); err != nil {
				return fs.ToErrno(err)
			}

			// Rename the content file
			if err := os.Rename(oldContent, newContent); err != nil {
				return fs.ToErrno(err)
			}

			// Create new metadata entry
			newFile := &store.File{
				Hdr: tar.Header{
					Name:       newPath,
					Mode:       srcFile.Hdr.Mode,
					Uid:        srcFile.Hdr.Uid,
					Gid:        srcFile.Hdr.Gid,
					Size:       srcFile.Hdr.Size,
					Typeflag:   srcFile.Hdr.Typeflag,
					ModTime:    srcFile.Hdr.ModTime,
					AccessTime: srcFile.Hdr.AccessTime,
					ChangeTime: srcFile.Hdr.ChangeTime,
				},
				Path: newContent,
			}
			if _, err := od.writableLayer.Create(newPath, os.FileMode(srcFile.Hdr.Mode), false); err != nil {
				return fs.ToErrno(err)
			}
			if err := od.writableLayer.Update(newFile); err != nil {
				return fs.ToErrno(err)
			}

			// Remove old metadata entry
			if err := od.writableLayer.Remove(oldPath); err != nil {
				return fs.ToErrno(err)
			}
		}
	} else {
		// Source is in read-only layer - copy to new location and whiteout old
		slog.Debug("Rename: copying from RO to writable layer", "oldPath", oldPath, "newPath", newPath)

		if srcIsDir {
			// For directories from RO layer, create in writable and whiteout
			if _, err := od.writableLayer.Create(newPath, os.FileMode(srcFile.Hdr.Mode), true); err != nil {
				return fs.ToErrno(err)
			}
		} else {
			// For files, copy content to new location
			roFile := od.roLookup[oldPath]
			if roFile == nil {
				return syscall.ENOENT
			}

			src, err := os.Open(roFile.Path)
			if err != nil {
				return fs.ToErrno(err)
			}
			defer src.Close()

			// Create with new path using CopyUp-like logic
			newContent := od.writableLayer.ContentPath(newPath)
			if err := os.MkdirAll(path.Dir(newContent), 0755); err != nil {
				return fs.ToErrno(err)
			}

			dest, err := os.Create(newContent)
			if err != nil {
				return fs.ToErrno(err)
			}

			written, err := io.Copy(dest, src)
			dest.Close()
			if err != nil {
				os.Remove(newContent)
				return fs.ToErrno(err)
			}

			// Create metadata
			newFile, err := od.writableLayer.Create(newPath, os.FileMode(srcFile.Hdr.Mode), false)
			if err != nil {
				return fs.ToErrno(err)
			}
			newFile.Hdr.Size = written
			newFile.Hdr.Uid = srcFile.Hdr.Uid
			newFile.Hdr.Gid = srcFile.Hdr.Gid
			if err := od.writableLayer.Update(newFile); err != nil {
				return fs.ToErrno(err)
			}
		}

		// Whiteout the old path
		if err := od.writableLayer.Whiteout(oldPath); err != nil {
			return fs.ToErrno(err)
		}
	}

	return fs.OK
}

func (od *unionDir) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if od.writableLayer == nil {
		return nil, syscall.EROFS
	}

	childPath := path.Join(od.pathInFs, name)
	slog.Debug("Symlink called", "path", childPath, "target", target)

	// Create the symlink in the writable layer
	file, err := od.writableLayer.CreateSymlink(childPath, target)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	// Create inode for the symlink
	return od.newInodeFromFile(ctx, file, true), fs.OK
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

	// Determine the mode for the stable attribute
	var mode uint32
	if file.Hdr.Typeflag == tar.TypeSymlink {
		mode = fuse.S_IFLNK
	}

	return od.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: mode})
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
