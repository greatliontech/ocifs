package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Ensure fsBlobStore implements BlobStore.
var _ BlobStore = (*fsBlobStore)(nil)

// fsBlobStore implements BlobStore using the local filesystem.
// Blobs are stored in a content-addressed layout: basePath/<algorithm>/<hex>
type fsBlobStore struct {
	basePath string
}

// NewFSBlobStore creates a new filesystem-based blob store.
func NewFSBlobStore(basePath string) (BlobStore, error) {
	// Ensure the base path and sha256 directory exist
	sha256Dir := filepath.Join(basePath, "sha256")
	if err := os.MkdirAll(sha256Dir, 0755); err != nil {
		return nil, fmt.Errorf("create blob store dir: %w", err)
	}
	return &fsBlobStore{basePath: basePath}, nil
}

// Get returns a reader for the blob content.
func (s *fsBlobStore) Get(ref string) (io.ReadCloser, error) {
	path := s.refToPath(ref)
	return os.Open(path)
}

// Put stores content and returns its hash reference.
func (s *fsBlobStore) Put(r io.Reader) (string, error) {
	// Write to temp file while computing hash
	tmpFile, err := os.CreateTemp(s.basePath, "blob-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	hasher := sha256.New()
	mw := io.MultiWriter(tmpFile, hasher)

	if _, err := io.Copy(mw, r); err != nil {
		return "", fmt.Errorf("write content: %w", err)
	}

	ref := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	destPath := s.refToPath(ref)

	// Check if blob already exists
	if _, err := os.Stat(destPath); err == nil {
		return ref, nil // Already exists
	}

	// Move temp file to final location
	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpFile.Name(), destPath); err != nil {
		return "", fmt.Errorf("move blob: %w", err)
	}

	return ref, nil
}

// Exists checks if a blob exists.
func (s *fsBlobStore) Exists(ref string) bool {
	path := s.refToPath(ref)
	_, err := os.Stat(path)
	return err == nil
}

// Delete removes a blob.
func (s *fsBlobStore) Delete(ref string) error {
	path := s.refToPath(ref)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// refToPath converts a ref like "sha256:abc123" to a filesystem path.
func (s *fsBlobStore) refToPath(ref string) string {
	// ref format: "algorithm:hex"
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) != 2 {
		// Assume sha256 if no algorithm specified
		return filepath.Join(s.basePath, "sha256", ref)
	}
	return filepath.Join(s.basePath, parts[0], parts[1])
}

// PathToRef converts a filesystem path back to a ref.
// Useful when migrating from Path-based File to BlobRef-based File.
func (s *fsBlobStore) PathToRef(path string) string {
	rel, err := filepath.Rel(s.basePath, path)
	if err != nil {
		return ""
	}
	// rel should be like "sha256/abc123..."
	parts := strings.SplitN(rel, string(filepath.Separator), 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[0] + ":" + parts[1]
}
