package store

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
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

	slog.Debug("creating layer from writable layer", "files", len(wl.files))

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
			return nil, &CommitError{Op: "write_header", Err: fmt.Errorf("%s: %w", path, err)}
		}

		// Write content for regular files with content
		if hdr.Typeflag == tar.TypeReg && hdr.Size > 0 && file.Path != "" {
			content, err := os.Open(file.Path)
			if err != nil {
				return nil, &CommitError{Op: "open_content", Err: fmt.Errorf("%s: %w", path, err)}
			}
			written, err := io.Copy(tw, content)
			content.Close()
			if err != nil {
				return nil, &CommitError{Op: "copy_content", Err: fmt.Errorf("%s: %w", path, err)}
			}
			if written != hdr.Size {
				return nil, &CommitError{Op: "verify_size", Err: fmt.Errorf("%s: expected %d, wrote %d", path, hdr.Size, written)}
			}
		}
	}

	if err := tw.Close(); err != nil {
		return nil, &CommitError{Op: "close_tar", Err: err}
	}

	slog.Debug("layer tarball created", "size", buf.Len())

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
	slog.Info("committing writable layer", "base", base.h.String())

	// Set default timestamp
	if opts.Timestamp.IsZero() {
		opts.Timestamp = time.Now()
	}

	// Create layer from writable changes
	layer, err := wl.ToLayer()
	if err != nil {
		return nil, err // ToLayer already wraps errors
	}

	layerDigest, _ := layer.Digest()
	slog.Debug("layer created", "digest", layerDigest.String())

	// Append layer to base image
	newImg, err := mutate.AppendLayers(base.img, layer)
	if err != nil {
		return nil, &CommitError{Op: "append", Err: err}
	}

	// Get current config and add history entry
	cfg, err := newImg.ConfigFile()
	if err != nil {
		return nil, &CommitError{Op: "config", Err: err}
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
		return nil, &CommitError{Op: "update_config", Err: err}
	}

	// Store in local OCI layout
	if err := s.lp.AppendImage(newImg); err != nil {
		return nil, &CommitError{Op: "store", Err: err}
	}

	// Unpack the new layer to our blob store
	// This extracts file contents and creates layer metadata
	if err := s.unpackLayer(ctx, layer); err != nil {
		return nil, &CommitError{Op: "unpack", Err: err}
	}

	// Get the new image's digest
	h, err := newImg.Digest()
	if err != nil {
		return nil, &CommitError{Op: "digest", Err: err}
	}

	slog.Info("commit complete", "digest", h.String())

	// Return wrapped Image
	return s.getImage(h)
}

// Push uploads an image to a remote registry.
func (s *Store) Push(ctx context.Context, img *Image, ref string) error {
	slog.Info("pushing image", "ref", ref, "digest", img.h.String())

	dest, err := name.ParseReference(ref)
	if err != nil {
		return &PullError{Ref: ref, Op: "parse", Err: err}
	}

	if err := remote.Write(dest, img.img,
		remote.WithAuthFromKeychain(s.auth),
		remote.WithContext(ctx),
	); err != nil {
		return &PullError{Ref: ref, Op: "push", Err: err}
	}

	slog.Info("image pushed", "ref", ref)
	return nil
}

// Tag associates a reference with an image in the local store.
// This allows the image to be retrieved later by the given reference.
func (s *Store) Tag(img *Image, ref string) error {
	slog.Debug("tagging image", "ref", ref, "digest", img.h.String())

	parsed, err := name.ParseReference(ref)
	if err != nil {
		return &PullError{Ref: ref, Op: "parse", Err: err}
	}

	h, err := img.img.Digest()
	if err != nil {
		return &PullError{Ref: ref, Op: "digest", Err: err}
	}

	return s.refs.Put(parsed, h)
}
