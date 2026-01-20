package store

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	// pull image if needed
	h, err := s.pullImage(ctx, imageRef)
	if err != nil {
		return nil, err
	}

	// get image from store
	return s.getImage(h)
}

func (s *Store) getImage(h v1.Hash) (*Image, error) {
	img, err := s.lp.Image(h)
	if err != nil {
		return nil, err
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, err
	}

	outLayers := make([]*Layer, len(layers))

	// loop through layers to get their hashes
	for i, layer := range layers {
		lh, err := layer.Digest()
		if err != nil {
			return nil, err
		}
		blobPath := s.blobPath(lh)
		outLayer := &Layer{
			path: blobPath,
		}
		if err := outLayer.Load(); err != nil {
			return nil, err
		}
		outLayers[i] = outLayer
	}

	// read the config file here to avoid exposing a method
	// that will return (Conf, error)
	conf, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}

	return &Image{
		h:      h,
		img:    img,
		layers: outLayers,
		conf:   conf,
	}, nil
}

func (s *Store) pullImage(ctx context.Context, imageRef string) (v1.Hash, error) {
	// parse reference string
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return emptyHash, err
	}

	// check in cache
	h, refFound, err := s.refs.Get(ref)
	if err != nil {
		return emptyHash, err
	}

	// no ref found, only matters if pull policy is never
	if !refFound && s.pullPolicy == PullNever {
		return emptyHash, fmt.Errorf("image %s not found in cache and pull policy is 'Never'", imageRef)
	}

	// ref found, return hash if no pull needed
	if refFound {
		if s.pullPolicy == PullIfNotPresent {
			return h, nil
		}
		desc, err := remote.Head(ref, remote.WithAuthFromKeychain(s.auth))
		if err != nil {
			return emptyHash, err
		}
		if desc.Digest == h {
			return h, nil
		}
	}

	// at this point, we need to pull the image
	// Use the configured platform for multi-arch image resolution
	remoteOpts := []remote.Option{remote.WithAuthFromKeychain(s.auth)}
	if s.platform != nil {
		remoteOpts = append(remoteOpts, remote.WithPlatform(*s.platform))
	}
	rmtImg, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return emptyHash, err
	}

	// store in local oci layout
	if err := s.lp.AppendImage(rmtImg); err != nil {
		return emptyHash, err
	}

	// get the image hash to query local layout
	h, err = rmtImg.Digest()
	if err != nil {
		return emptyHash, err
	}

	// find local image
	img, err := s.lp.Image(h)
	if err != nil {
		return emptyHash, err
	}

	// get layers and unpack them
	layers, err := img.Layers()
	if err != nil {
		return emptyHash, err
	}
	for _, layer := range layers {
		if err := s.unpackLayer(ctx, layer); err != nil {
			return emptyHash, err
		}
	}

	// store ref
	if err := s.refs.Put(ref, h); err != nil {
		return emptyHash, err
	}

	return h, nil
}

func (s *Store) unpackLayer(ctx context.Context, layer v1.Layer) error {
	// tar reader
	rc, err := layer.Uncompressed()
	if err != nil {
		return err
	}
	defer rc.Close()

	// get unpacked layer files
	files, err := s.extractTar(ctx, rc)
	if err != nil {
		return err
	}

	// layer hash
	h, err := layer.Digest()
	if err != nil {
		return err
	}
	blobPath := s.blobPath(h)

	intLayer := &Layer{
		files: files,
		path:  blobPath,
	}

	// persist layer data
	if err := intLayer.Persist(); err != nil {
		return err
	}

	return nil
}

func (s *Store) extractTar(ctx context.Context, rc io.ReadCloser) ([]*File, error) {
	tr := tar.NewReader(rc)
	ret := []*File{}
	buf := make([]byte, 256*1024)
	blobsDir := filepath.Join(s.path, "blobs")

	// iterate through entries in the tar archive
	for {
		// check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// get next hdr
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, err
		}

		outFile := &File{
			Hdr: *hdr,
		}

		// we add this erly
		ret = append(ret, outFile)

		// we only care about regular files
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		// temp file
		tf, err := os.CreateTemp(blobsDir, "blob-*")
		if err != nil {
			return nil, err
		}
		defer tf.Close()
		defer os.Remove(tf.Name())

		hasher := sha256.New()

		// stream file bytes -> [temp file, hasher]
		mw := io.MultiWriter(tf, hasher)
		if _, err := io.CopyBuffer(mw, tr, buf); err != nil {
			return nil, err
		}

		h := v1.Hash{
			Algorithm: "sha256",
			Hex:       hex.EncodeToString(hasher.Sum(make([]byte, 0, hasher.Size()))),
		}
		blobPath := s.blobPath(h)

		outFile.Path = blobPath
		outFile.BlobRef = h.Algorithm + ":" + h.Hex // Set content-addressed reference

		// check if blob already exists
		if _, err := os.Stat(blobPath); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return nil, err
		}

		// move temp file to final location
		if err := os.Rename(tf.Name(), blobPath); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (s *Store) blobPath(h v1.Hash) string {
	return filepath.Join(s.path, "blobs", h.Algorithm, h.Hex)
}
