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
	finalMap := make(map[string]unionFile)
	deletedFiles := make(map[string]bool)
	opaqueDirs := make(map[string]int)
	files := []string{}

	for layer := len(layers) - 1; layer >= 0; layer-- {
		for _, dirEntry := range layers[layer] {

			filePath := dirEntry.path

			dirPath, fileName := filepath.Split(filePath)

			if strings.HasPrefix(fileName, ".wh.") {
				if fileName == ".wh..wh..opq" {
					// Mark the current directory as opaque from this layer
					opaqueDirs[dirPath] = layer
				} else {
					// Mark the file as deleted within its directory
					deletedPath := filepath.Join(dirPath, strings.TrimPrefix(fileName, ".wh."))
					deletedFiles[deletedPath] = true
				}
				continue
			}

			if isDeletedOrUnderOpaqueDir(filePath, deletedFiles, opaqueDirs, layer) {
				continue
			}

			// ignore directories
			if dirEntry.entry.Type().IsDir() {
				continue
			}

			// The file is not deleted and not under an opaque directory, so add it to the map
			finalMap[filePath] = unionFile{dirEntry.entry, layer}
			files = append(files, filePath)
		}
	}

	sort.Strings(files)

	return files, finalMap
}

func isDeletedOrUnderOpaqueDir(filePath string, deletedFiles map[string]bool, opaqueDirs map[string]int, layer int) bool {
	if deletedFiles[filePath] {
		return true
	}

	pathParts := strings.Split(filePath, "/")
	for i := len(pathParts); i >= 0; i-- {
		checkPath := strings.Join(pathParts[:i], "/")
		if opaqueLayer, exists := opaqueDirs[checkPath]; exists {
			// The file is under an opaque directory. It should be included only if it's in the same layer as the opaque marker.
			return opaqueLayer > layer
		}
	}

	return false
}
