package store

import (
	"io"
	"os"
)

// BlobStore provides access to content-addressed immutable blobs.
// Blobs are identified by their content hash (e.g., "sha256:abc123...").
type BlobStore interface {
	// Get returns a reader for the blob content.
	// Returns os.ErrNotExist if the blob doesn't exist.
	Get(ref string) (io.ReadCloser, error)

	// Put stores content and returns its hash reference.
	Put(r io.Reader) (ref string, err error)

	// Exists checks if a blob exists.
	Exists(ref string) bool

	// Delete removes a blob. No error if blob doesn't exist.
	Delete(ref string) error
}

// ContentStore provides access to mutable content (for writable layers).
// Paths are relative to the store root.
type ContentStore interface {
	// Open opens a file with the given flags.
	Open(path string, flags int, mode os.FileMode) (*os.File, error)

	// Create creates or truncates a file.
	Create(path string) (*os.File, error)

	// Remove deletes a file or empty directory.
	Remove(path string) error

	// Stat returns file info for a path.
	Stat(path string) (os.FileInfo, error)

	// MkdirAll creates a directory and all parents.
	MkdirAll(path string, mode os.FileMode) error

	// ContentPath returns the absolute filesystem path for a given logical path.
	// This is needed for direct filesystem access (e.g., truncate, chmod).
	ContentPath(path string) string
}

// Opener is a function that returns a reader for some content.
// Used for lazy content access.
type Opener func() (io.ReadCloser, error)
