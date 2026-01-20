package store

import (
	"archive/tar"
)

type PullPolicy int

func (s PullPolicy) String() string {
	switch s {
	case PullIfNotPresent:
		return "IfNotPresent"
	case PullAlways:
		return "Always"
	case PullNever:
		return "Never"
	default:
		return "Unknown"
	}
}

const (
	PullIfNotPresent PullPolicy = iota
	PullAlways
	PullNever
)

// File represents a file in the filesystem with metadata and content location.
type File struct {
	Hdr tar.Header

	// Path is the absolute filesystem path to the content (legacy field).
	// Used for direct filesystem access. Prefer BlobRef when possible.
	Path string `json:",omitempty"`

	// BlobRef is a content-addressed reference (e.g., "sha256:abc123").
	// When set, content should be accessed through a BlobStore.
	// This field takes precedence over Path when both are set.
	BlobRef string `json:",omitempty"`
}

// ContentRef returns the content reference for this file.
// Returns BlobRef if set, otherwise derives a ref from Path.
func (f *File) ContentRef() string {
	if f.BlobRef != "" {
		return f.BlobRef
	}
	// For backward compatibility, derive ref from path
	// Path format: .../blobs/sha256/hexdigest
	// We don't have enough context to convert, so return empty
	return ""
}

// HasContent returns true if this file has content (regular file with size > 0).
func (f *File) HasContent() bool {
	return f.Hdr.Typeflag == tar.TypeReg && f.Hdr.Size > 0
}
