package unionfs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"syscall"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Ensure ociFile implements all necessary interfaces
var (
	_ = (fs.NodeGetattrer)((*unionFile)(nil))
	_ = (fs.NodeOpener)((*unionFile)(nil))
	_ = (fs.NodeReader)((*unionFile)(nil))
	_ = (fs.NodeWriter)((*unionFile)(nil))
	_ = (fs.NodeReleaser)((*unionFile)(nil))
)

// unionFile represents a file in the filesystem.
type unionFile struct {
	fs.Inode
	pathInFs      string
	file          *store.File
	isWritable    bool // Does this file exist in the writable layer?
	writableLayer *store.WritableLayer
	roLookup      map[string]*store.File
}

// unionFileHandle holds the open file descriptor.
type unionFileHandle struct {
	f *os.File
}

func (uf *unionFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	headerToAttr(&out.Attr, uf.file.Hdr)
	return fs.OK
}

func (uf *unionFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
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
		return nil, 0, fs.ToErrno(err)
	}
	return &unionFileHandle{f: f}, fuse.FOPEN_KEEP_CACHE, fs.OK
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
	slog.Debug("Write called", "path", uf.pathInFs, "offset", off, "length", len(data))
	if uf.writableLayer == nil {
		return 0, syscall.EROFS // Read-only file system
	}

	h, ok := fh.(*unionFileHandle)
	if !ok {
		return 0, syscall.EBADF
	}

	// This is the copy-on-write (CoW) logic.
	if !uf.isWritable {
		// The file is currently from a read-only layer. We need to copy it up.
		slog.Debug("Copy-on-write triggered", "path", uf.pathInFs)

		// Get source and destination paths
		roFile := uf.roLookup[uf.pathInFs]
		srcPath := roFile.Path
		dstFile := &store.File{Hdr: uf.file.Hdr} // Create a new file metadata for writable layer
		if err := uf.writableLayer.SetFile(dstFile); err != nil {
			return 0, fs.ToErrno(err)
		}

		destPath := dstFile.Path

		// Copy the content
		src, err := os.Open(srcPath)
		if err != nil {
			return 0, fs.ToErrno(err)
		}
		defer src.Close()

		dest, err := os.Create(destPath)
		if err != nil {
			return 0, fs.ToErrno(err)
		}
		if _, err := io.Copy(dest, src); err != nil {
			dest.Close()
			return 0, fs.ToErrno(err)
		}
		dest.Close()

		// Now, reopen the file handle with the new writable file
		h.f.Close()
		newF, err := os.OpenFile(destPath, os.O_RDWR, os.FileMode(uf.file.Hdr.Mode))
		if err != nil {
			return 0, fs.ToErrno(err)
		}
		h.f = newF
		uf.isWritable = true
	}

	n, err := h.f.WriteAt(data, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}

	// Update the size in our metadata
	uf.file.Hdr.Size = uf.file.Hdr.Size + int64(n) // This is a simplification; a full stat is better
	uf.writableLayer.SetFile(uf.file)

	return uint32(n), fs.OK
}

func (uf *unionFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	h, ok := fh.(*unionFileHandle)
	if !ok {
		return syscall.EBADF
	}
	return fs.ToErrno(h.f.Close())
}
