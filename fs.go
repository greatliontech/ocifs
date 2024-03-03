package ocifs

import (
	"archive/tar"
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func (o *OCIFS) Mount(h *v1.Hash, path string) (*fuse.Server, error) {
	root, err := o.initFS(h)
	if err != nil {
		return nil, err
	}

	// Create a FUSE server
	return fs.Mount(path, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  true,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false, // Set to true for debugging
		},
	})
}

type ociFS struct {
	fs.Inode
	files     map[string]unionFile
	fileNames []string
}

func (o *OCIFS) initFS(h *v1.Hash) (fs.InodeEmbedder, error) {
	fls, unf, err := o.unify(h)
	if err != nil {
		return nil, err
	}

	return &ociFS{
		files:     unf,
		fileNames: fls,
	}, nil
}

// headerToFileInfo fills a fuse.Attr struct from a tar.Header.
func headerToFileInfo(out *fuse.Attr, h *tar.Header) {
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
}

var _ = (fs.NodeOnAdder)((*ociFS)(nil))

func (ofs *ociFS) OnAdd(ctx context.Context) {
	for _, f := range ofs.fileNames {
		dir, base := filepath.Split(filepath.Clean(f))
		entry := ofs.files[f]

		p := &ofs.Inode
		for _, part := range strings.Split(dir, "/") {
			if len(part) == 0 {
				continue
			}
			ch := p.GetChild(part)
			if ch == nil {
				ch = p.NewPersistentInode(ctx, &fs.Inode{}, fs.StableAttr{Mode: fuse.S_IFDIR})
				p.AddChild(part, ch, true)
			}
			p = ch
		}

		attr := fuse.Attr{}
		headerToFileInfo(&attr, entry.entry)

		hdr := entry.entry

		switch hdr.Typeflag {

		case tar.TypeSymlink:
			l := &fs.MemSymlink{
				Data: []byte(hdr.Linkname),
			}
			l.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, l, fs.StableAttr{Mode: syscall.S_IFLNK}), false)

		// for hardlinks we create an inode pointing to the link file in it's layer whith it's size
		case tar.TypeLink:
			linkEntry, ok := ofs.files[hdr.Linkname]
			if !ok {
				slog.Info("Missing link", "path", hdr.Linkname, "layer", entry.layer)
				continue
			}
			attr.Size = uint64(linkEntry.entry.Size)
			ch := p.NewPersistentInode(ctx, &ociFile{
				path:      hdr.Linkname,
				attr:      attr,
				layerPath: linkEntry.root,
			}, fs.StableAttr{})
			p.AddChild(base, ch, true)

		case tar.TypeChar:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFCHR}), false)

		case tar.TypeBlock:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFBLK}), false)

		case tar.TypeFifo:
			rf := &fs.MemRegularFile{}
			rf.Attr = attr
			p.AddChild(base, p.NewPersistentInode(ctx, rf, fs.StableAttr{Mode: syscall.S_IFIFO}), false)

		case tar.TypeReg:
			ch := p.NewPersistentInode(ctx, &ociFile{
				path:      f,
				attr:      attr,
				layerPath: entry.root,
			}, fs.StableAttr{})
			p.AddChild(base, ch, true)
		default:
			slog.Info("Unsupported file type", "path", f, "type", hdr.Typeflag)
		}

	}
}

type ociFile struct {
	fs.Inode
	path      string
	layerPath string
	attr      fuse.Attr
}

var _ = (fs.NodeOpener)((*ociFile)(nil))

func (of *ociFile) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Info("Open", "path", of.path, "layer", of.layerPath)

	filePath := filepath.Join(of.layerPath, of.path)

	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return nil, 0, syscall.EIO
	}
	return &ociFileHandle{f: f, size: of.attr.Size}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

type ociFileHandle struct {
	f    *os.File
	size uint64
}

var _ = (fs.NodeReader)((*ociFile)(nil))

func (gf *ociFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	slog.Info("Read", "path", gf.path, "offset", off)

	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", gf.path, "offset", off)
		return nil, syscall.EIO
	}

	_, err := ofh.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		slog.Error("Error reading file", "path", gf.path, "offset", off, "error", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest), fs.OK
}

var _ = (fs.NodeGetattrer)((*ociFile)(nil))

func (f *ociFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = f.attr
	return fs.OK
}

var _ = (fs.NodeReleaser)((*ociFile)(nil))

func (f *ociFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	slog.Info("Release", "path", f.path)
	ofh, ok := fh.(*ociFileHandle)
	if !ok {
		slog.Error("Error getting file handle", "path", f.path)
		return syscall.EIO
	}
	err := ofh.f.Close()
	if err != nil {
		slog.Error("Error closing file", "path", f.path, "error", err)
		return syscall.EIO
	}
	return fs.OK
}

type unionFile struct {
	entry *tar.Header
	root  string
	layer int
}

func (o *OCIFS) unify(dgst *v1.Hash) ([]string, map[string]unionFile, error) {
	layers, err := o.getLayerIndexes(dgst)
	if err != nil {
		return nil, nil, err
	}

	var filesList []string
	unifiedMap := make(map[string]unionFile)
	whiteoutRecords := make(map[string]bool) // Record paths marked by whiteout

	// Process from the last layer to the first
	for layerIndex := len(layers) - 1; layerIndex >= 0; layerIndex-- {
		for _, e := range layers[layerIndex].Files() {
			// Determine the original path affected by a whiteout marker, if any
			fileName := filepath.Base(e.Name)
			// is whiteout file
			if strings.HasPrefix(fileName, ".wh.") {
				originalPath := strings.TrimPrefix(fileName, ".wh.")
				originalFullPath := filepath.Join(filepath.Dir(e.Name), originalPath)
				whiteoutRecords[originalFullPath] = true

				// Remove affected paths from unifiedMap and filesList
				delete(unifiedMap, originalFullPath)
				for i, filePath := range filesList {
					if filePath == originalFullPath {
						filesList = append(filesList[:i], filesList[i+1:]...)
						break
					}
				}
				continue
			}

			// Check if path or any parent path has been marked by a whiteout
			if _, marked := whiteoutRecords[e.Name]; marked {
				continue // Skip adding this path as it's marked for deletion
			}

			// Check for directories in the path that may have been marked by a whiteout
			if isPathOrParentMarked(e.Name, whiteoutRecords) {
				continue
			}

			// For regular files not marked by a whiteout, add them to the map and list
			if e.Typeflag != tar.TypeDir {
				unifiedMap[e.Name] = unionFile{
					entry: e,
					layer: layerIndex,
					root:  o.unpackedPath(layers[layerIndex].Hash()),
				}
				filesList = append(filesList, e.Name)
			}
		}
	}
	sort.Strings(filesList)
	return filesList, unifiedMap, nil
}

// Checks if the file or any of its parent directories have been marked by a whiteout
func isPathOrParentMarked(path string, whiteoutRecords map[string]bool) bool {
	for {
		if _, exists := whiteoutRecords[path]; exists {
			return true
		}
		parentPath := filepath.Dir(path)
		if parentPath == path { // Reached the root without finding a whiteout marker
			break
		}
		path = parentPath
	}
	return false
}
