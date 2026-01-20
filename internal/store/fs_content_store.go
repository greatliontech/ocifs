package store

import (
	"fmt"
	"os"
	"path/filepath"
)

// Ensure fsContentStore implements ContentStore.
var _ ContentStore = (*fsContentStore)(nil)

// fsContentStore implements ContentStore using the local filesystem.
// Content is stored in a regular directory hierarchy.
type fsContentStore struct {
	basePath string
}

// NewFSContentStore creates a new filesystem-based content store.
func NewFSContentStore(basePath string) (ContentStore, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create content store dir: %w", err)
	}
	return &fsContentStore{basePath: basePath}, nil
}

// Open opens a file with the given flags.
func (s *fsContentStore) Open(path string, flags int, mode os.FileMode) (*os.File, error) {
	fullPath := s.ContentPath(path)

	// Create parent directories if O_CREATE is set
	if flags&os.O_CREATE != 0 {
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return nil, fmt.Errorf("create parent dirs: %w", err)
		}
	}

	return os.OpenFile(fullPath, flags, mode)
}

// Create creates or truncates a file.
func (s *fsContentStore) Create(path string) (*os.File, error) {
	fullPath := s.ContentPath(path)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, fmt.Errorf("create parent dirs: %w", err)
	}

	return os.Create(fullPath)
}

// Remove deletes a file or empty directory.
func (s *fsContentStore) Remove(path string) error {
	fullPath := s.ContentPath(path)
	err := os.Remove(fullPath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Stat returns file info for a path.
func (s *fsContentStore) Stat(path string) (os.FileInfo, error) {
	fullPath := s.ContentPath(path)
	return os.Stat(fullPath)
}

// MkdirAll creates a directory and all parents.
func (s *fsContentStore) MkdirAll(path string, mode os.FileMode) error {
	fullPath := s.ContentPath(path)
	return os.MkdirAll(fullPath, mode)
}

// ContentPath returns the absolute filesystem path for a given logical path.
func (s *fsContentStore) ContentPath(path string) string {
	return filepath.Join(s.basePath, path)
}
