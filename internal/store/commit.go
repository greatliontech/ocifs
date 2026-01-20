package store

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// CommitOptions configures how a writable layer is committed.
type CommitOptions struct {
	Author    string    // Author of the commit (e.g., "user@example.com")
	Comment   string    // Commit message
	CreatedBy string    // Tool that created this layer (e.g., "ocifs commit")
	Timestamp time.Time // Commit timestamp (defaults to now if zero)
}

// ToLayer creates an OCI v1.Layer from the writable layer's current state.
// This includes all files and whiteout markers.
func (wl *WritableLayer) ToLayer() (v1.Layer, error) {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	// Create a tarball buffer
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Sort paths for reproducible layers
	paths := make([]string, 0, len(wl.files))
	for p := range wl.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, path := range paths {
		file := wl.files[path]

		// Clone the header to avoid modifying the original
		hdr := file.Hdr

		// Write header
		if err := tw.WriteHeader(&hdr); err != nil {
			return nil, fmt.Errorf("write header %s: %w", path, err)
		}

		// Write content for regular files with content
		if hdr.Typeflag == tar.TypeReg && hdr.Size > 0 && file.Path != "" {
			content, err := os.Open(file.Path)
			if err != nil {
				return nil, fmt.Errorf("open content %s: %w", path, err)
			}
			written, err := io.Copy(tw, content)
			content.Close()
			if err != nil {
				return nil, fmt.Errorf("copy content %s: %w", path, err)
			}
			if written != hdr.Size {
				return nil, fmt.Errorf("size mismatch for %s: expected %d, wrote %d", path, hdr.Size, written)
			}
		}
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar writer: %w", err)
	}

	// Create layer from tarball using LayerFromOpener (non-deprecated)
	data := buf.Bytes()
	opener := func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return tarball.LayerFromOpener(opener)
}

// Commit creates a new image by appending the writable layer's changes to a base image.
// The new image is stored in the local OCI layout.
func (s *Store) Commit(ctx context.Context, base *Image, wl *WritableLayer, opts CommitOptions) (*Image, error) {
	// Set default timestamp
	if opts.Timestamp.IsZero() {
		opts.Timestamp = time.Now()
	}

	// Create layer from writable changes
	layer, err := wl.ToLayer()
	if err != nil {
		return nil, fmt.Errorf("create layer: %w", err)
	}

	// Append layer to base image
	newImg, err := mutate.AppendLayers(base.img, layer)
	if err != nil {
		return nil, fmt.Errorf("append layer: %w", err)
	}

	// Get current config and add history entry
	cfg, err := newImg.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}

	// Clone config to avoid modifying the original
	newCfg := cfg.DeepCopy()
	newCfg.History = append(newCfg.History, v1.History{
		Author:    opts.Author,
		Created:   v1.Time{Time: opts.Timestamp},
		CreatedBy: opts.CreatedBy,
		Comment:   opts.Comment,
	})

	// Apply the new config
	newImg, err = mutate.ConfigFile(newImg, newCfg)
	if err != nil {
		return nil, fmt.Errorf("update config: %w", err)
	}

	// Store in local OCI layout
	if err := s.lp.AppendImage(newImg); err != nil {
		return nil, fmt.Errorf("store image: %w", err)
	}

	// Unpack the new layer to our blob store
	// This extracts file contents and creates layer metadata
	if err := s.unpackLayer(ctx, layer); err != nil {
		return nil, fmt.Errorf("unpack layer: %w", err)
	}

	// Get the new image's digest
	h, err := newImg.Digest()
	if err != nil {
		return nil, fmt.Errorf("get digest: %w", err)
	}

	// Return wrapped Image
	return s.getImage(h)
}

// Push uploads an image to a remote registry.
func (s *Store) Push(ctx context.Context, img *Image, ref string) error {
	dest, err := name.ParseReference(ref)
	if err != nil {
		return fmt.Errorf("parse ref: %w", err)
	}

	return remote.Write(dest, img.img,
		remote.WithAuthFromKeychain(s.auth),
		remote.WithContext(ctx),
	)
}

// Tag associates a reference with an image in the local store.
// This allows the image to be retrieved later by the given reference.
func (s *Store) Tag(img *Image, ref string) error {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return fmt.Errorf("parse ref: %w", err)
	}

	h, err := img.img.Digest()
	if err != nil {
		return fmt.Errorf("get digest: %w", err)
	}

	return s.refs.Put(parsed, h)
}
