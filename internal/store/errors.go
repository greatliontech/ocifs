package store

import (
	"errors"
	"fmt"
)

// Sentinel errors for common conditions.
var (
	// ErrNotFound indicates a requested resource was not found.
	ErrNotFound = errors.New("not found")

	// ErrReadOnly indicates an operation was attempted on a read-only resource.
	ErrReadOnly = errors.New("read-only")

	// ErrCorrupted indicates data integrity issues.
	ErrCorrupted = errors.New("corrupted")
)

// PullError represents an error that occurred while pulling an image.
type PullError struct {
	Ref string
	Op  string // "parse", "fetch", "resolve", "store"
	Err error
}

func (e *PullError) Error() string {
	return fmt.Sprintf("pull %s: %s: %v", e.Ref, e.Op, e.Err)
}

func (e *PullError) Unwrap() error {
	return e.Err
}

// LayerError represents an error that occurred during layer operations.
type LayerError struct {
	Digest string // layer digest if known
	Op     string // "load", "persist", "extract", "unpack"
	Err    error
}

func (e *LayerError) Error() string {
	if e.Digest != "" {
		return fmt.Sprintf("layer %s: %s: %v", e.Digest, e.Op, e.Err)
	}
	return fmt.Sprintf("layer: %s: %v", e.Op, e.Err)
}

func (e *LayerError) Unwrap() error {
	return e.Err
}

// BlobError represents an error that occurred during blob operations.
type BlobError struct {
	Ref string
	Op  string // "get", "put", "delete", "exists"
	Err error
}

func (e *BlobError) Error() string {
	return fmt.Sprintf("blob %s: %s: %v", e.Ref, e.Op, e.Err)
}

func (e *BlobError) Unwrap() error {
	return e.Err
}

// PersistError represents an error that occurred during metadata persistence.
type PersistError struct {
	Path string
	Op   string // "marshal", "write", "rename", "load"
	Err  error
}

func (e *PersistError) Error() string {
	return fmt.Sprintf("persist %s: %s: %v", e.Path, e.Op, e.Err)
}

func (e *PersistError) Unwrap() error {
	return e.Err
}

// CommitError represents an error that occurred during image commit.
type CommitError struct {
	Op  string // "create_layer", "append", "config", "store", "unpack"
	Err error
}

func (e *CommitError) Error() string {
	return fmt.Sprintf("commit: %s: %v", e.Op, e.Err)
}

func (e *CommitError) Unwrap() error {
	return e.Err
}
