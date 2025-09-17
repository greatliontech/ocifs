package store

import (
	"archive/tar"
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

const (
	whiteoutPrefix = ".wh."
	whiteoutOpaque = whiteoutPrefix + whiteoutPrefix + "opq"
)

type Image struct {
	h      v1.Hash
	img    v1.Image
	conf   *v1.ConfigFile
	layers []*Layer
}

func (i *Image) Hash() v1.Hash {
	return i.h
}

func (i *Image) Image() v1.Image {
	return i.img
}

func (i *Image) ConfigFile() *v1.ConfigFile {
	return i.conf
}

func (i *Image) Layers() []*Layer {
	return i.layers
}

// adapted from https://github.com/google/go-containerregistry/blob/v0.20.6/pkg/v1/mutate/mutate.go#L265
// to also respect opaque whiteouts

// Unify takes a slice of layers, ordered from base to top, and flattens them
// into a single, unified list of files representing the final filesystem view.
// It correctly processes file overrides, standard whiteouts (.wh.), and
// opaque whiteouts (.wh..wh..opq).
func (i *Image) Unify() []*File {
	// fileMap tracks the status of all paths encountered so far. The meaning of the
	// boolean value is crucial:
	// - true: The path is "final". It's either a regular file or has been explicitly
	//   deleted by a whiteout. No entry from a lower layer can modify or add to it.
	// - false: The path is an existing directory. Files can still be added inside it
	//   from lower layers.
	fileMap := map[string]bool{}

	// opaqueDirs tracks directories whose contents from lower layers should be ignored.
	// This is kept separate from fileMap because an opaque directory must still exist in
	// the final output, whereas a deleted directory (marked in fileMap) must not.
	opaqueDirs := map[string]bool{}

	layers := i.Layers()
	out := []*File{}

	// Iterate through layers from top to bottom (reverse order).
	for i := len(layers) - 1; i >= 0; i-- {
		layer := layers[i]

		// Collect opaque markers for the current layer separately. Their effect should only
		// apply to *lower* layers, not files within this same layer.
		newOpaqueDirs := map[string]bool{}

		for _, file := range layer.Files() {
			header := file.Hdr
			header.Name = filepath.Clean(header.Name)

			baseName := filepath.Base(header.Name)
			dirName := filepath.Dir(header.Name)

			// Handle opaque whiteout markers first.
			if baseName == whiteoutOpaque {
				newOpaqueDirs[dirName] = true
				continue // The marker itself is not included in the filesystem.
			}

			// Check for and process standard ".wh." whiteout markers (tombstones).
			isTombstone := strings.HasPrefix(baseName, whiteoutPrefix)
			if isTombstone {
				baseName = baseName[len(whiteoutPrefix):]
			}

			// Reconstruct the "real" path of the file, without any whiteout prefix.
			var finalPath string
			if header.Typeflag == tar.TypeDir {
				finalPath = header.Name
			} else {
				finalPath = filepath.Join(dirName, baseName)
			}

			// --- Filter out files that should be ignored ---

			// 1. If we have already processed and finalized this path, skip.
			if _, exists := fileMap[finalPath]; exists {
				continue
			}

			// 2. If the file is inside a directory that was deleted or made opaque
			//    by a higher layer, skip.
			if isFinalized(fileMap, finalPath) || inOpaqueDir(opaqueDirs, finalPath) {
				continue
			}

			// --- Process the file ---

			// Mark this path as seen and record its status. A path becomes "final" (true)
			// if it's a tombstone or a regular file. It remains "not final" (false) only
			// if it's a directory, allowing lower layers to add files inside it.
			fileMap[finalPath] = isTombstone || (header.Typeflag != tar.TypeDir)

			// Only add actual files (not tombstones) to the final output list.
			if !isTombstone {
				out = append(out, file)
			}
		}

		// After processing all files in the layer, merge this layer's opaque markers
		// into the main map so they apply to all subsequent (lower) layers.
		for dir := range newOpaqueDirs {
			opaqueDirs[dir] = true
		}
	}

	// Sort the final list of files by path name for predictable output.
	sort.Slice(out, func(i, j int) bool {
		// Use the cleaned header name for consistent sorting.
		return out[i].Hdr.Name < out[j].Hdr.Name
	})

	return out
}

// isFinalized checks if a file is inside a directory that has been finalized
// by a higher layer. A parent is "finalized" if it was either replaced by a regular
// file or explicitly deleted with a whiteout marker.
func isFinalized(fileMap map[string]bool, path string) bool {
	// Walk up the directory tree towards the root.
	for path != "" && path != "." && path != "/" {
		parent := filepath.Dir(path)
		if path == parent {
			break // Reached the root.
		}
		// If the parent exists in the map with a 'true' value, it means it's final
		// and cannot have children from lower layers.
		if isFinal, exists := fileMap[parent]; exists && isFinal {
			return true
		}
		path = parent
	}
	return false
}

// inOpaqueDir checks if a file is inside a directory marked as opaque by a higher layer.
func inOpaqueDir(opaqueDirs map[string]bool, path string) bool {
	// Walk up the directory tree towards the root.
	for path != "" && path != "." && path != "/" {
		parent := filepath.Dir(path)
		if path == parent {
			break // Reached the root.
		}
		if opaqueDirs[parent] {
			return true
		}
		path = parent
	}
	return false
}
