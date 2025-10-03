package store

import (
	"archive/tar"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	WhiteoutPrefix   = ".wh."
	metadataFileName = "metadata.json"
	contentDirName   = "content"
)

// WritableLayer manages the upper, writable directory and its in-memory metadata.
type WritableLayer struct {
	path  string
	mutex sync.RWMutex
	files map[string]*File // In-memory store for metadata
}

// NewWritableLayer creates and initializes a new writable layer.
// It will try to load existing metadata from metadata.json.
func NewWritableLayer(path string) (*WritableLayer, error) {
	if err := os.MkdirAll(filepath.Join(path, contentDirName), 0755); err != nil {
		return nil, err
	}

	wl := &WritableLayer{
		path:  path,
		files: make(map[string]*File),
	}

	if err := wl.Load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return wl, nil
}

// GetFile retrieves the tar.Header for a given path from memory.
func (wl *WritableLayer) GetFile(path string) *File {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()
	// Return a copy to prevent race conditions on the header fields
	if file, ok := wl.files[path]; ok {
		fileCopy := *file
		return &fileCopy
	}
	return nil
}

// SetFile stores a tar.Header in memory.
func (wl *WritableLayer) SetFile(hdr tar.Header) (*File, error) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()

	filePath := wl.getContentPath(hdr.Name)
	dir := filePath
	if hdr.Typeflag != tar.TypeDir {
		dir = filepath.Dir(filePath)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	file := &File{
		Hdr:  hdr,
		Path: filePath,
	}
	wl.files[file.Hdr.Name] = file
	fileCopy := *file
	return &fileCopy, nil
}

// DeleteFile removes a tar.Header from memory.
func (wl *WritableLayer) DeleteFile(path string) error {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()

	f, ok := wl.files[path]
	if !ok {
		return nil // Nothing to delete
	}

	if err := os.Remove(wl.getContentPath(f.Hdr.Name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	delete(wl.files, path)
	return nil
}

// ListChildren returns all immediate children for a given directory path from memory.
func (wl *WritableLayer) ListChildren(dirPath string) []*File {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	var children []*File
	if dirPath != "" && !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	for key, file := range wl.files {
		if strings.HasPrefix(key, dirPath) {
			childPath := strings.TrimPrefix(key, dirPath)
			if !strings.Contains(childPath, "/") {
				children = append(children, file)
			}
		}
	}
	return children
}

// Load reads the metadata.json file into the in-memory map.
func (wl *WritableLayer) Load() error {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()

	f, err := os.Open(filepath.Join(wl.path, metadataFileName))
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &wl.files)
}

// Persist writes the in-memory map to the metadata.json file.
func (wl *WritableLayer) Persist() error {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	data, err := json.MarshalIndent(wl.files, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(wl.path, metadataFileName), data, 0644)
}

// getContentPath returns the path where a file's content should be stored.
func (wl *WritableLayer) getContentPath(name string) string {
	// Note: You might want to use a hash of the name to avoid deep directory structures
	return filepath.Join(wl.path, contentDirName, name)
}
