package ocifs

import (
	"archive/tar"
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type unionFile struct {
	entry *tar.Header
	layer int
	root  string
}

func unify(store *Store, dgst v1.Hash) ([]string, map[string]unionFile, error) {
	layers, err := store.Index(dgst)
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
			if isWhiteout(fileName) {
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
					root:  store.UnpackedPath(layers[layerIndex].Hash()),
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

// Determines if an fs.DirEntry represents a whiteout file
func isWhiteout(entry string) bool {
	return strings.HasPrefix(entry, ".wh.")
}
