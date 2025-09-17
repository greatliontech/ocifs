package unionfs

import (
	"archive/tar"
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
	_ = (fs.NodeGetattrer)((*ociFile)(nil))
	_ = (fs.NodeOpener)((*ociFile)(nil))
	_ = (fs.NodeReader)((*ociFile)(nil))
	_ = (fs.NodeWriter)((*ociFile)(nil))
	_ = (fs.NodeReleaser)((*ociFile)(nil))
)

// ociFile represents a file in the filesystem.
type ociFile struct {
	fs.Inode
	pathInFs      string
	hdr           *tar.Header
	isWritable    bool // Does this file exist in the writable layer?
	writableLayer *store.WritableLayer
	roLookup      map[string]*store.File
}

// ociFileHandle holds the open file descriptor.
type ociFileHandle struct {
	f *os.File
}

func (of *ociFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	headerToAttr(&out.Attr, of.hdr)
	return fs.OK
}

func (of *ociFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	var pathOnDisk string
	if of.isWritable {
		pathOnDisk = of.writableLayer.GetContentPath(of.pathInFs)
	} else {
		// This is a read-only file from a base layer.
		roFile, ok := of.roLookup[of.pathInFs]
		if !ok {
			return nil, 0, syscall.ENOENT
		}
		pathOnDisk = roFile.Path
	}

	f, err := os.OpenFile(pathOnDisk, int(flags), os.FileMode(of.hdr.Mode))
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return &ociFileHandle{f: f}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (of *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h, ok := fh.(*ociFileHandle)
	if !ok {
		return nil, syscall.EBADF
	}

	n, err := h.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		slog.Error("Read error", "path", of.pathInFs, "error", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (of *ociFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if of.writableLayer == nil {
		return 0, syscall.EROFS // Read-only file system
	}

	h, ok := fh.(*ociFileHandle)
	if !ok {
		return 0, syscall.EBADF
	}

	// This is the copy-on-write (CoW) logic.
	if !of.isWritable {
		// The file is currently from a read-only layer. We need to copy it up.
		slog.Debug("Copy-on-write triggered", "path", of.pathInFs)

		// Get source and destination paths
		roFile := of.roLookup[of.pathInFs]
		srcPath := roFile.Path
		destPath := of.writableLayer.GetContentPath(of.pathInFs)

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
		newF, err := os.OpenFile(destPath, os.O_RDWR, os.FileMode(of.hdr.Mode))
		if err != nil {
			return 0, fs.ToErrno(err)
		}
		h.f = newF
		of.isWritable = true
	}

	n, err := h.f.WriteAt(data, off)
	if err != nil {
		return 0, fs.ToErrno(err)
	}

	// Update the size in our metadata
	of.hdr.Size = of.hdr.Size + int64(n) // This is a simplification; a full stat is better
	of.writableLayer.SetHeader(of.hdr)

	return uint32(n), fs.OK
}

func (of *ociFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	h, ok := fh.(*ociFileHandle)
	if !ok {
		return syscall.EBADF
	}
	return fs.ToErrno(h.f.Close())
}
