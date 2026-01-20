package store

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
)

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithDefaultPlatform sets the default platform for pulling multi-arch images.
// If not specified, defaults to the current runtime platform (runtime.GOOS/runtime.GOARCH).
func WithDefaultPlatform(platform v1.Platform) StoreOption {
	return func(s *Store) {
		s.platform = &platform
	}
}

type Store struct {
	path       string
	auth       authn.Keychain
	pullPolicy PullPolicy
	refs       referenceStore
	lp         layout.Path
	blobs      BlobStore
	platform   *v1.Platform // Target platform for multi-arch images
}

func NewStore(path string, auth authn.Keychain, pullPolicy PullPolicy, opts ...StoreOption) (*Store, error) {
	// if dir does not exist, create it
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	dirs := []string{"refs", "blobs/sha256", "oci", "mounts"}
	for _, dir := range dirs {
		if err := os.MkdirAll(filepath.Join(path, dir), 0755); err != nil {
			return nil, err
		}
	}

	// creat index.json for oci layout if it does not exist
	ociDir := filepath.Join(path, "oci")
	idxFilePath := filepath.Join(ociDir, "index.json")
	if _, err := os.Stat(idxFilePath); os.IsNotExist(err) {
		// create index.json
		if err := os.WriteFile(idxFilePath, []byte("{}"), 0644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// Initialize blob store
	blobStore, err := NewFSBlobStore(filepath.Join(path, "blobs"))
	if err != nil {
		return nil, fmt.Errorf("create blob store: %w", err)
	}

	// Default platform is the current runtime
	defaultPlatform := &v1.Platform{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
	}

	s := &Store{
		path:       path,
		auth:       auth,
		pullPolicy: pullPolicy,
		refs:       referenceStore(filepath.Join(path, "refs")),
		lp:         layout.Path(ociDir),
		blobs:      blobStore,
		platform:   defaultPlatform,
	}

	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (s *Store) NewMountDir(id string) (string, error) {
	if id == "" {
		uid, err := uuid.NewRandom()
		if err != nil {
			return "", err
		}
		id = uid.String()
	}
	path := filepath.Join(s.path, "mounts", id)
	if err := os.Mkdir(path, 0755); err != nil {
		return "", err
	}
	return path, nil
}

// BlobStore returns the store's blob storage interface.
// This can be used to access content by reference without knowing the physical path.
func (s *Store) BlobStore() BlobStore {
	return s.blobs
}

// OpenBlob opens a blob by its reference (e.g., "sha256:abc123...").
// This is a convenience method that delegates to the BlobStore.
func (s *Store) OpenBlob(ref string) (io.ReadCloser, error) {
	return s.blobs.Get(ref)
}

// Platform returns the target platform used for pulling multi-arch images.
func (s *Store) Platform() v1.Platform {
	if s.platform != nil {
		return *s.platform
	}
	return v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

// ListPlatforms queries a remote image reference and returns available platforms.
// For single-arch images, returns a slice with one platform.
// For multi-arch images (indexes), returns all available platforms.
func (s *Store) ListPlatforms(ctx context.Context, imageRef string) ([]v1.Platform, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parse reference: %w", err)
	}

	desc, err := remote.Get(ref, remote.WithAuthFromKeychain(s.auth), remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("get descriptor: %w", err)
	}

	// Check if it's an index (multi-arch)
	if desc.MediaType.IsIndex() {
		idx, err := desc.ImageIndex()
		if err != nil {
			return nil, fmt.Errorf("get index: %w", err)
		}
		manifest, err := idx.IndexManifest()
		if err != nil {
			return nil, fmt.Errorf("get index manifest: %w", err)
		}

		var platforms []v1.Platform
		for _, m := range manifest.Manifests {
			if m.Platform != nil {
				platforms = append(platforms, *m.Platform)
			}
		}
		return platforms, nil
	}

	// Single-arch image - get platform from config
	img, err := desc.Image()
	if err != nil {
		return nil, fmt.Errorf("get image: %w", err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}

	return []v1.Platform{{
		OS:           cfg.OS,
		Architecture: cfg.Architecture,
		Variant:      cfg.Variant,
	}}, nil
}

func (s *Store) Image(ctx context.Context, imageRef string) (*Image, error) {
	slog.Debug("loading image", "ref", imageRef)

	h, err := s.pullImage(ctx, imageRef)
	if err != nil {
		return nil, err // pullImage already wraps errors
	}

	img, err := s.getImage(h)
	if err != nil {
		return nil, &PullError{Ref: imageRef, Op: "load", Err: err}
	}

	slog.Debug("image loaded", "ref", imageRef, "digest", h.String())
	return img, nil
}

func (s *Store) getImage(h v1.Hash) (*Image, error) {
	img, err := s.lp.Image(h)
	if err != nil {
		return nil, &LayerError{Digest: h.String(), Op: "open", Err: err}
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, &LayerError{Digest: h.String(), Op: "list", Err: err}
	}

	outLayers := make([]*Layer, len(layers))

	for i, layer := range layers {
		lh, err := layer.Digest()
		if err != nil {
			return nil, &LayerError{Op: "digest", Err: err}
		}
		blobPath := s.blobPath(lh)
		outLayer := &Layer{
			path: blobPath,
		}
		if err := outLayer.Load(); err != nil {
			return nil, &LayerError{Digest: lh.String(), Op: "load", Err: err}
		}
		outLayers[i] = outLayer
	}

	conf, err := img.ConfigFile()
	if err != nil {
		return nil, &LayerError{Digest: h.String(), Op: "config", Err: err}
	}

	return &Image{
		h:      h,
		img:    img,
		layers: outLayers,
		conf:   conf,
	}, nil
}

func (s *Store) pullImage(ctx context.Context, imageRef string) (v1.Hash, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "parse", Err: err}
	}

	// Check cache
	h, refFound, err := s.refs.Get(ref)
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "cache_lookup", Err: err}
	}

	if !refFound && s.pullPolicy == PullNever {
		return emptyHash, &PullError{Ref: imageRef, Op: "cache_lookup", Err: ErrNotFound}
	}

	// Return cached if policy allows
	if refFound {
		if s.pullPolicy == PullIfNotPresent {
			slog.Debug("using cached image", "ref", imageRef, "digest", h.String())
			return h, nil
		}
		desc, err := remote.Head(ref, remote.WithAuthFromKeychain(s.auth))
		if err != nil {
			return emptyHash, &PullError{Ref: imageRef, Op: "check_remote", Err: err}
		}
		if desc.Digest == h {
			slog.Debug("cached image is up to date", "ref", imageRef, "digest", h.String())
			return h, nil
		}
		slog.Debug("cached image is stale, pulling", "ref", imageRef, "cached", h.String(), "remote", desc.Digest.String())
	}

	// Pull from remote
	slog.Info("pulling image", "ref", imageRef, "platform", s.Platform().String())

	remoteOpts := []remote.Option{remote.WithAuthFromKeychain(s.auth)}
	if s.platform != nil {
		remoteOpts = append(remoteOpts, remote.WithPlatform(*s.platform))
	}
	rmtImg, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "fetch", Err: err}
	}

	if err := s.lp.AppendImage(rmtImg); err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "store", Err: err}
	}

	h, err = rmtImg.Digest()
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "digest", Err: err}
	}

	img, err := s.lp.Image(h)
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "load", Err: err}
	}

	layers, err := img.Layers()
	if err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "layers", Err: err}
	}

	slog.Debug("extracting layers", "ref", imageRef, "count", len(layers))
	for i, layer := range layers {
		if err := s.unpackLayer(ctx, layer); err != nil {
			return emptyHash, err // unpackLayer already wraps errors
		}
		slog.Debug("layer extracted", "ref", imageRef, "layer", i+1, "total", len(layers))
	}

	if err := s.refs.Put(ref, h); err != nil {
		return emptyHash, &PullError{Ref: imageRef, Op: "save_ref", Err: err}
	}

	slog.Info("image pulled", "ref", imageRef, "digest", h.String())
	return h, nil
}

func (s *Store) unpackLayer(ctx context.Context, layer v1.Layer) error {
	h, err := layer.Digest()
	if err != nil {
		return &LayerError{Op: "digest", Err: err}
	}
	digest := h.String()

	rc, err := layer.Uncompressed()
	if err != nil {
		return &LayerError{Digest: digest, Op: "decompress", Err: err}
	}
	defer rc.Close()

	files, err := s.extractTar(ctx, rc)
	if err != nil {
		return &LayerError{Digest: digest, Op: "extract", Err: err}
	}

	blobPath := s.blobPath(h)
	intLayer := &Layer{
		files: files,
		path:  blobPath,
	}

	if err := intLayer.Persist(); err != nil {
		return &LayerError{Digest: digest, Op: "persist", Err: err}
	}

	return nil
}

func (s *Store) extractTar(ctx context.Context, rc io.ReadCloser) ([]*File, error) {
	tr := tar.NewReader(rc)
	var ret []*File
	buf := make([]byte, 256*1024)
	blobsDir := filepath.Join(s.path, "blobs")

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar header: %w", err)
		}

		outFile := &File{Hdr: *hdr}
		ret = append(ret, outFile)

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		tf, err := os.CreateTemp(blobsDir, "blob-*")
		if err != nil {
			return nil, fmt.Errorf("create temp file: %w", err)
		}
		defer tf.Close()
		defer os.Remove(tf.Name())

		hasher := sha256.New()
		mw := io.MultiWriter(tf, hasher)
		if _, err := io.CopyBuffer(mw, tr, buf); err != nil {
			return nil, fmt.Errorf("extract %s: %w", hdr.Name, err)
		}

		h := v1.Hash{
			Algorithm: "sha256",
			Hex:       hex.EncodeToString(hasher.Sum(make([]byte, 0, hasher.Size()))),
		}
		blobPath := s.blobPath(h)

		outFile.Path = blobPath
		outFile.BlobRef = h.Algorithm + ":" + h.Hex

		if _, err := os.Stat(blobPath); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat blob %s: %w", blobPath, err)
		}

		if err := os.Rename(tf.Name(), blobPath); err != nil {
			return nil, fmt.Errorf("store blob %s: %w", hdr.Name, err)
		}
	}

	return ret, nil
}

func (s *Store) blobPath(h v1.Hash) string {
	return filepath.Join(s.path, "blobs", h.Algorithm, h.Hex)
}
