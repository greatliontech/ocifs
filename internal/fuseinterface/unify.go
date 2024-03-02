package fuseinterface

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

type unionFile struct {
	entry fs.DirEntry
	layer int
}

type entry struct {
	entry fs.DirEntry
	path  string
}

func unify(layers [][]entry) ([]string, map[string]unionFile) {
	var filesList []string
	unifiedMap := make(map[string]unionFile)
	whiteoutRecords := make(map[string]bool) // Record paths marked by whiteout

	// Process from the last layer to the first
	for layerIndex := len(layers) - 1; layerIndex >= 0; layerIndex-- {
		for _, e := range layers[layerIndex] {
			// Determine the original path affected by a whiteout marker, if any
			if isWhiteout(e.entry) {
				originalPath := strings.TrimPrefix(e.entry.Name(), ".wh.")
				originalFullPath := filepath.Join(filepath.Dir(e.path), originalPath)
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
			if _, marked := whiteoutRecords[e.path]; marked {
				continue // Skip adding this path as it's marked for deletion
			}

			// Check for directories in the path that may have been marked by a whiteout
			if isPathOrParentMarked(e.path, whiteoutRecords) {
				continue
			}

			// For regular files not marked by a whiteout, add them to the map and list
			if !e.entry.IsDir() {
				unifiedMap[e.path] = unionFile{entry: e.entry, layer: layerIndex}
				filesList = append(filesList, e.path)
			}
		}
	}
	sort.Strings(filesList)
	return filesList, unifiedMap
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

// Determines if an fs.DirEntry represents a whiteout file
func isWhiteout(entry fs.DirEntry) bool {
	return strings.HasPrefix(entry.Name(), ".wh.")
}
