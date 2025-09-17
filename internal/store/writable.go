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
	path    string
	mutex   sync.RWMutex
	headers map[string]*tar.Header // In-memory store for metadata
}

// NewWritableLayer creates and initializes a new writable layer.
// It will try to load existing metadata from metadata.json.
func NewWritableLayer(path string) (*WritableLayer, error) {
	if err := os.MkdirAll(filepath.Join(path, contentDirName), 0755); err != nil {
		return nil, err
	}

	wl := &WritableLayer{
		path:    path,
		headers: make(map[string]*tar.Header),
	}

	if err := wl.Load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return wl, nil
}

// GetContentPath returns the path where a file's content should be stored.
func (wl *WritableLayer) GetContentPath(name string) string {
	// Note: You might want to use a hash of the name to avoid deep directory structures
	return filepath.Join(wl.path, contentDirName, name)
}

// GetHeader retrieves the tar.Header for a given path from memory.
func (wl *WritableLayer) GetHeader(path string) *tar.Header {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()
	// Return a copy to prevent race conditions on the header fields
	if hdr, ok := wl.headers[path]; ok {
		hdrCopy := *hdr
		return &hdrCopy
	}
	return nil
}

// SetHeader stores a tar.Header in memory.
func (wl *WritableLayer) SetHeader(hdr *tar.Header) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	wl.headers[hdr.Name] = hdr
}

// DeleteHeader removes a tar.Header from memory.
func (wl *WritableLayer) DeleteHeader(path string) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	delete(wl.headers, path)
}

// ListChildren returns all immediate children for a given directory path from memory.
func (wl *WritableLayer) ListChildren(dirPath string) []*tar.Header {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	var children []*tar.Header
	if dirPath != "" && !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	for key, hdr := range wl.headers {
		if strings.HasPrefix(key, dirPath) {
			childPath := strings.TrimPrefix(key, dirPath)
			if !strings.Contains(childPath, "/") {
				children = append(children, hdr)
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

	return json.Unmarshal(data, &wl.headers)
}

// Persist writes the in-memory map to the metadata.json file.
func (wl *WritableLayer) Persist() error {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	data, err := json.MarshalIndent(wl.headers, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(wl.path, metadataFileName), data, 0644)
}
