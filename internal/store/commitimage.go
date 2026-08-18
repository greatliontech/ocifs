//go:build linux || darwin

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"

	"github.com/greatliontech/ocifs/internal/commit"
	"github.com/greatliontech/ocifs/internal/upper"
)

// NewUpper creates (or reopens) the store-managed upper named name,
// binding it to base on first use: the binding record is created
// atomically without replacement, and the loser of a creation race
// reads the winner's binding and validates against it
// (REQ-writable-base-binding). The returned directory is the dialect
// tree root.
func (s *Store) NewUpper(name string, base v1.Hash) (string, error) {
	if !validMountID(name) {
		return "", fmt.Errorf("upper name %q is not a single path element", name)
	}
	root := upperDirOf(s.path, name)
	dir := filepath.Join(root, "upper")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	recorded, err := s.bk.UpperBind(context.Background(), name, base)
	if err != nil {
		return "", err
	}
	if recorded != base {
		return "", fmt.Errorf("upper %q is bound to base %s; refusing base %s — a whiteout set produced over one base applied to another materializes a tree nobody wrote", name, recorded, base)
	}
	return dir, nil
}

// UpperDir returns the named upper's dialect root without creating
// or validating anything.
func (s *Store) UpperDir(name string) string {
	return filepath.Join(s.path, "uppers", name, "upper")
}

// CommitUpper serializes the canonical diff of img and the upper at
// upperRoot, assembles the committed image — base layers plus the
// new uncompressed layer, config and manifest extended — under the
// store's ingest ordering, and returns the new image's digest,
// acquirable under the local namespace (REQ-writable-commit-image).
// No live mount is involved: the upper is read from disk as-is.
func (s *Store) CommitUpper(ctx context.Context, img *Image, upperRoot string) (v1.Hash, error) {
	// A commit is an ingest under the lease, row-last
	// (REQ-store-single-writer): content blobs, descriptor, then the
	// localimages root row, all inside the leased span.
	tok, err := s.AcquireIngestLease(ctx)
	if err != nil {
		return v1.Hash{}, err
	}
	defer func() { _ = s.ReleaseIngestLease(context.WithoutCancel(ctx), tok) }()
	view, err := img.Unify()
	if err != nil {
		return v1.Hash{}, err
	}
	st, err := upper.Walk(upperRoot)
	if err != nil {
		return v1.Hash{}, err
	}
	layerBytes, layerDigest, err := commit.LayerBytes(view, st)
	if err != nil {
		return v1.Hash{}, err
	}

	baseManifestRaw, err := os.ReadFile(s.ociBlobPath(img.Hash()))
	if err != nil {
		return v1.Hash{}, fmt.Errorf("base manifest %s not retained: %w", img.Hash(), err)
	}
	baseManifest, err := v1.ParseManifest(bytes.NewReader(baseManifestRaw))
	if err != nil {
		return v1.Hash{}, err
	}

	// Config: diff IDs and history extended; the uncompressed
	// layer's blob digest is its diff ID. No commit-time values —
	// a committed image is as deterministic as its layer.
	conf := img.ConfigFile().DeepCopy()
	conf.RootFS.DiffIDs = append(conf.RootFS.DiffIDs, layerDigest)
	conf.History = append(conf.History, v1.History{CreatedBy: "ocifs commit"})
	confBytes, err := json.Marshal(conf)
	if err != nil {
		return v1.Hash{}, err
	}
	confDigest, _, err := v1.SHA256(bytes.NewReader(confBytes))
	if err != nil {
		return v1.Hash{}, err
	}

	manifest := v1.Manifest{
		SchemaVersion: 2,
		MediaType:     types.OCIManifestSchema1,
		Config: v1.Descriptor{
			MediaType: types.OCIConfigJSON,
			Size:      int64(len(confBytes)),
			Digest:    confDigest,
		},
		Layers: append(append([]v1.Descriptor{}, baseManifest.Layers...), v1.Descriptor{
			MediaType: types.OCIUncompressedLayer,
			Size:      int64(len(layerBytes)),
			Digest:    layerDigest,
		}),
		Annotations: baseManifest.Annotations,
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return v1.Hash{}, err
	}
	manifestDigest, _, err := v1.SHA256(bytes.NewReader(manifestBytes))
	if err != nil {
		return v1.Hash{}, err
	}

	// Ingest order (REQ-store-ingest-order): every blob durable
	// before the descriptor names the manifest.
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	for _, blob := range []struct {
		h v1.Hash
		b []byte
	}{
		{layerDigest, layerBytes},
		{confDigest, confBytes},
		{manifestDigest, manifestBytes},
	} {
		if err := s.writeOCIBlob(blob.h, func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(blob.b)), nil
		}); err != nil {
			return v1.Hash{}, err
		}
	}
	if err := s.appendDescriptor(v1.Descriptor{
		MediaType: types.OCIManifestSchema1,
		Size:      int64(len(manifestBytes)),
		Digest:    manifestDigest,
	}); err != nil {
		return v1.Hash{}, err
	}
	// The localimages row is the committed image's root
	// (REQ-store-gc-roots), written last like any root
	// (REQ-store-single-writer's row-last ordering).
	if err := s.bk.LocalImagePut(ctx, manifestDigest, time.Now().Unix()); err != nil {
		return v1.Hash{}, err
	}
	return manifestDigest, nil
}

// CommitNamedUpper is CommitUpper against a store-managed upper,
// validating its base binding first (REQ-writable-base-binding).
func (s *Store) CommitNamedUpper(ctx context.Context, img *Image, name string) (v1.Hash, error) {
	recorded, err := s.bk.UpperBinding(ctx, name)
	if err != nil {
		return v1.Hash{}, err
	}
	if recorded != img.Hash() {
		return v1.Hash{}, fmt.Errorf("upper %q is bound to base %s; refusing commit over %s", name, recorded, img.Hash())
	}
	return s.CommitUpper(ctx, img, s.UpperDir(name))
}
