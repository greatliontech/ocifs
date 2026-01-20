package unionfs

import (
	"archive/tar"
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"syscall"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Ensure unionFile implements all necessary interfaces
var (
	_ = (fs.NodeGetattrer)((*unionFile)(nil))
	_ = (fs.NodeSetattrer)((*unionFile)(nil))
	_ = (fs.NodeOpener)((*unionFile)(nil))
	_ = (fs.NodeReader)((*unionFile)(nil))
	_ = (fs.NodeWriter)((*unionFile)(nil))
	_ = (fs.NodeReleaser)((*unionFile)(nil))
	_ = (fs.NodeReadlinker)((*unionFile)(nil))
	_ = (fs.NodeFsyncer)((*unionFile)(nil))
	_ = (fs.NodeGetxattrer)((*unionFile)(nil))
	_ = (fs.NodeSetxattrer)((*unionFile)(nil))
	_ = (fs.NodeListxattrer)((*unionFile)(nil))
	_ = (fs.NodeRemovexattrer)((*unionFile)(nil))
)

// unionFile represents a file in the filesystem.
type unionFile struct {
	fs.Inode
	mu            sync.Mutex // Protects fields below from concurrent access
	pathInFs      string
	file          *store.File
	isWritable    bool // Does this file exist in the writable layer?
	writableLayer *store.WritableLayer
	roLookup      map[string]*store.File
	blobs         store.BlobStore // Optional: for reading content by reference
}

// unionFileHandle holds the open file descriptor.
type unionFileHandle struct {
	f *os.File
}

func (uf *unionFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	uf.mu.Lock()
	defer uf.mu.Unlock()
	out.Attr = headerToAttr(uf.file.Hdr)
	return fs.OK
}

func (uf *unionFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Setattr called", "path", uf.pathInFs, "valid", in.Valid)

	// Handle truncate (size change)
	if sz, ok := in.GetSize(); ok {
		if errno := uf.truncateLocked(int64(sz)); errno != fs.OK {
			return errno
		}
	}

	// Handle mode change
	if mode, ok := in.GetMode(); ok {
		if uf.writableLayer == nil {
			return syscall.EROFS
		}
		if errno := uf.ensureWritableLocked(); errno != fs.OK {
			return errno
		}
		uf.file.Hdr.Mode = int64(mode)
		if err := uf.writableLayer.Update(uf.file); err != nil {
			return fs.ToErrno(err)
		}
	}

	// Handle uid/gid changes
	if uid, ok := in.GetUID(); ok {
		if uf.writableLayer == nil {
			return syscall.EROFS
		}
		if errno := uf.ensureWritableLocked(); errno != fs.OK {
			return errno
		}
		uf.file.Hdr.Uid = int(uid)
		if err := uf.writableLayer.Update(uf.file); err != nil {
			return fs.ToErrno(err)
		}
	}
	if gid, ok := in.GetGID(); ok {
		if uf.writableLayer == nil {
			return syscall.EROFS
		}
		if errno := uf.ensureWritableLocked(); errno != fs.OK {
			return errno
		}
		uf.file.Hdr.Gid = int(gid)
		if err := uf.writableLayer.Update(uf.file); err != nil {
			return fs.ToErrno(err)
		}
	}

	out.Attr = headerToAttr(uf.file.Hdr)
	return fs.OK
}

func (uf *unionFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	isWR := (flags&syscall.O_RDWR != 0) || (flags&syscall.O_WRONLY != 0)
	slog.Debug("Open called", "path", uf.pathInFs, "flags", flags, "isWritable", uf.isWritable, "isWR", isWR)

	var pathOnDisk string
	if uf.isWritable {
		pathOnDisk = uf.file.Path
	} else {
		// This is a read-only file from a base layer.
		roFile, ok := uf.roLookup[uf.pathInFs]
		if !ok {
			return nil, 0, syscall.ENOENT
		}
		pathOnDisk = roFile.Path
	}

	f, err := os.OpenFile(pathOnDisk, int(flags), os.FileMode(uf.file.Hdr.Mode))
	if err != nil {
		slog.Error("Open error", "path", uf.pathInFs, "error", err)
		return nil, 0, fs.ToErrno(err)
	}
	slog.Debug("File opened", "path", uf.pathInFs, "fd", f.Fd())
	return &unionFileHandle{f: f}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

// ensureWritableLocked copies the file to the writable layer if needed.
// Caller must hold uf.mu.
func (uf *unionFile) ensureWritableLocked() syscall.Errno {
	if uf.isWritable {
		return fs.OK
	}

	if uf.writableLayer == nil {
		return syscall.EROFS
	}

	slog.Debug("Copy-on-write triggered", "path", uf.pathInFs)

	// Get source from read-only layer
	roFile, ok := uf.roLookup[uf.pathInFs]
	if !ok {
		return syscall.ENOENT
	}

	// Open source content
	src, err := os.Open(roFile.Path)
	if err != nil {
		return fs.ToErrno(err)
	}
	defer src.Close()

	// Use CopyUp to copy both metadata and content
	destFile, err := uf.writableLayer.CopyUp(uf.file, src)
	if err != nil {
		return fs.ToErrno(err)
	}

	// Update our file reference to point to writable layer
	uf.file = destFile
	uf.isWritable = true

	slog.Debug("Copy-on-write completed", "path", uf.pathInFs, "size", uf.file.Hdr.Size)
	return fs.OK
}

// truncateLocked changes the file size. Caller must hold uf.mu.
func (uf *unionFile) truncateLocked(size int64) syscall.Errno {
	if uf.writableLayer == nil {
		return syscall.EROFS
	}

	slog.Debug("Truncate called", "path", uf.pathInFs, "size", size)

	// Ensure file is in writable layer
	if errno := uf.ensureWritableLocked(); errno != fs.OK {
		return errno
	}

	// Truncate the actual file
	if err := os.Truncate(uf.file.Path, size); err != nil {
		return fs.ToErrno(err)
	}

	// Update metadata
	uf.file.Hdr.Size = size
	if err := uf.writableLayer.Update(uf.file); err != nil {
		return fs.ToErrno(err)
	}

	return fs.OK
}

func (uf *unionFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h, ok := fh.(*unionFileHandle)
	if !ok {
		return nil, syscall.EBADF
	}

	n, err := h.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		slog.Error("Read error", "path", uf.pathInFs, "error", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (uf *unionFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Write called", "path", uf.pathInFs, "offset", off, "length", len(data))

	if uf.writableLayer == nil {
		return 0, syscall.EROFS
	}

	h, ok := fh.(*unionFileHandle)
	if !ok {
		return 0, syscall.EBADF
	}

	// Ensure file is in writable layer (triggers CoW if needed)
	if !uf.isWritable {
		if errno := uf.ensureWritableLocked(); errno != fs.OK {
			return 0, errno
		}

		// Reopen file handle with writable path
		h.f.Close()
		newF, err := os.OpenFile(uf.file.Path, os.O_RDWR, os.FileMode(uf.file.Hdr.Mode))
		if err != nil {
			return 0, fs.ToErrno(err)
		}
		h.f = newF
	}

	// Perform the write
	n, err := h.f.WriteAt(data, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}

	// Update size: new size is max(current size, offset + bytes written)
	newEnd := off + int64(n)
	if newEnd > uf.file.Hdr.Size {
		uf.file.Hdr.Size = newEnd
		if err := uf.writableLayer.Update(uf.file); err != nil {
			return 0, fs.ToErrno(err)
		}
	}

	return uint32(n), fs.OK
}

func (uf *unionFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	h, ok := fh.(*unionFileHandle)
	if !ok {
		return syscall.EBADF
	}
	return fs.ToErrno(h.f.Close())
}

func (uf *unionFile) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Readlink called", "path", uf.pathInFs)

	// Check if this is actually a symlink
	if uf.file.Hdr.Typeflag != tar.TypeSymlink {
		return nil, syscall.EINVAL
	}

	// The target is stored in the header's Linkname field
	return []byte(uf.file.Hdr.Linkname), fs.OK
}

func (uf *unionFile) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Fsync called", "path", uf.pathInFs, "flags", flags)

	h, ok := fh.(*unionFileHandle)
	if !ok {
		return syscall.EBADF
	}

	// Sync the file data to disk
	if err := h.f.Sync(); err != nil {
		return fs.ToErrno(err)
	}

	// Also persist metadata if we have a writable layer
	if uf.writableLayer != nil && uf.isWritable {
		if err := uf.writableLayer.Persist(); err != nil {
			slog.Error("Fsync: failed to persist metadata", "error", err)
			return fs.ToErrno(err)
		}
	}

	return fs.OK
}

func (uf *unionFile) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Getxattr called", "path", uf.pathInFs, "attr", attr)

	// Use PAXRecords with SCHILY.xattr. prefix (standard for xattrs in tar)
	paxKey := "SCHILY.xattr." + attr
	if uf.file.Hdr.PAXRecords != nil {
		if val, ok := uf.file.Hdr.PAXRecords[paxKey]; ok {
			if len(dest) == 0 {
				return uint32(len(val)), fs.OK
			}
			if len(dest) < len(val) {
				return 0, syscall.ERANGE
			}
			copy(dest, val)
			return uint32(len(val)), fs.OK
		}
	}

	return 0, syscall.ENODATA
}

func (uf *unionFile) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Setxattr called", "path", uf.pathInFs, "attr", attr, "size", len(data))

	if uf.writableLayer == nil {
		return syscall.EROFS
	}

	// Ensure file is in writable layer
	if errno := uf.ensureWritableLocked(); errno != fs.OK {
		return errno
	}

	// Initialize PAXRecords map if needed
	if uf.file.Hdr.PAXRecords == nil {
		uf.file.Hdr.PAXRecords = make(map[string]string)
	}

	// Use PAXRecords with SCHILY.xattr. prefix
	paxKey := "SCHILY.xattr." + attr
	_, exists := uf.file.Hdr.PAXRecords[paxKey]

	// Handle flags
	if flags&0x1 != 0 && exists { // XATTR_CREATE
		return syscall.EEXIST
	}
	if flags&0x2 != 0 && !exists { // XATTR_REPLACE
		return syscall.ENODATA
	}

	uf.file.Hdr.PAXRecords[paxKey] = string(data)

	if err := uf.writableLayer.Update(uf.file); err != nil {
		return fs.ToErrno(err)
	}

	return fs.OK
}

func (uf *unionFile) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Listxattr called", "path", uf.pathInFs)

	// Collect all xattr names from PAXRecords
	var names []string
	if uf.file.Hdr.PAXRecords != nil {
		prefix := "SCHILY.xattr."
		for key := range uf.file.Hdr.PAXRecords {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				attrName := key[len(prefix):]
				names = append(names, attrName)
			}
		}
	}

	// Calculate total size (names are null-terminated)
	var totalSize uint32
	for _, name := range names {
		totalSize += uint32(len(name) + 1) // +1 for null terminator
	}

	if len(dest) == 0 {
		return totalSize, fs.OK
	}

	if uint32(len(dest)) < totalSize {
		return 0, syscall.ERANGE
	}

	// Copy names with null terminators
	offset := 0
	for _, name := range names {
		copy(dest[offset:], name)
		offset += len(name)
		dest[offset] = 0
		offset++
	}

	return totalSize, fs.OK
}

func (uf *unionFile) Removexattr(ctx context.Context, attr string) syscall.Errno {
	uf.mu.Lock()
	defer uf.mu.Unlock()

	slog.Debug("Removexattr called", "path", uf.pathInFs, "attr", attr)

	if uf.writableLayer == nil {
		return syscall.EROFS
	}

	// Ensure file is in writable layer
	if errno := uf.ensureWritableLocked(); errno != fs.OK {
		return errno
	}

	// Check if attribute exists in PAXRecords
	paxKey := "SCHILY.xattr." + attr
	if uf.file.Hdr.PAXRecords == nil {
		return syscall.ENODATA
	}
	if _, ok := uf.file.Hdr.PAXRecords[paxKey]; !ok {
		return syscall.ENODATA
	}

	delete(uf.file.Hdr.PAXRecords, paxKey)

	if err := uf.writableLayer.Update(uf.file); err != nil {
		return fs.ToErrno(err)
	}

	return fs.OK
}
