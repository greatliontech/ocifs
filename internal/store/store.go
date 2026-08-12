// Package store is ocifs's on-disk home for pulled OCI images
// (docs/specs/store.md): the retained OCI content under oci/, the
// content CAS under blobs/, layer indexes under layers/, the
// reference cache under refs/, and per-mount state under mounts/.
// Content tiers are a cache — re-derivable from a registry, or from
// the retained OCI content for the extraction tiers.
//
// The OCI append is written here rather than through
// layout.AppendImage: the library call appends a duplicate index
// descriptor on every ingest and rewrites index.json in place
// (non-atomic, never fsynced), which would break both ingest
// idempotence and the crash story — the reference-cache entry is
// only a valid completion barrier if everything written before it is
// durable in order. Blobs and index.json go through
// internal/atomicfile instead, and the descriptor append deduplicates
// by digest.
package store

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/google/uuid"

	"github.com/greatliontech/ocifs/internal/atomicfile"
	"github.com/greatliontech/ocifs/internal/cas"
	"github.com/greatliontech/ocifs/internal/layer"
)

// ErrPreLayoutStore reports a work directory written by an ocifs
// version predating the tiered store layout; its blobs/ tier mixed
// layer indexes into the content keyspace, so it cannot be adopted.
// The store is a cache: delete the directory (safe with no live
// mounts) and re-pull.
var ErrPreLayoutStore = errors.New("work directory holds a pre-layout ocifs store; delete it and re-pull")

type Store struct {
	path       string
	auth       authn.Keychain
	pullPolicy PullPolicy
	refs       referenceStore
	cas        *cas.CAS
	layers     layerIndexes
	lp         layout.Path
	// transport overrides the registry transport when non-nil; the
	// injection seam that lets the test harness serve a registry
	// in-process with no sockets.
	transport http.RoundTripper

	// ingestMu serializes writers to the content tiers within this
	// process (REQ-store-single-writer): index.json is a
	// read-modify-write document, and idempotence checks assume no
	// concurrent mutation between check and write. Shared per store
	// root, not per Store instance — two instances over one root are
	// still one ingesting process.
	ingestMu *sync.Mutex
}

// ingestLocks maps a cleaned store root to its in-process ingest
// mutex. Path aliases (symlinks, relative vs absolute spellings) map
// to distinct keys — the same residual the spec accepts for
// cross-process writers, resolved when bookkeeping moves to shared
// storage.
var ingestLocks sync.Map

func ingestLockFor(root string) *sync.Mutex {
	m, _ := ingestLocks.LoadOrStore(filepath.Clean(root), &sync.Mutex{})
	return m.(*sync.Mutex)
}

func NewStore(path string, auth authn.Keychain, pullPolicy PullPolicy) (*Store, error) {
	ociDir := filepath.Join(path, "oci")
	idxPath := filepath.Join(ociDir, "index.json")
	markerPath := filepath.Join(ociDir, "oci-layout")

	if _, err := os.Stat(idxPath); err == nil {
		if _, err := os.Stat(markerPath); errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s: %w", path, ErrPreLayoutStore)
		} else if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	for _, dir := range []string{"refs", "blobs", "layers", "oci", "mounts"} {
		if err := os.MkdirAll(filepath.Join(path, dir), 0o755); err != nil {
			return nil, err
		}
	}

	// The layout's two files are written individually, atomically,
	// and without replacement, marker first: index.json-without-marker
	// stays the unambiguous pre-layout signature above, a crash after
	// the marker leaves marker-without-index — uniquely a crashed
	// first creation, completed by the index write on the next open —
	// and no-replace publication means a concurrent opener that
	// already populated index.json can never be clobbered.
	if err := atomicfile.WriteNew(markerPath, strings.NewReader(`{"imageLayoutVersion": "1.0.0"}`), 0o644); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	emptyIdx, err := json.MarshalIndent(v1.IndexManifest{
		SchemaVersion: 2,
		MediaType:     types.OCIImageIndex,
		Manifests:     []v1.Descriptor{},
	}, "", "   ")
	if err != nil {
		return nil, err
	}
	if err := atomicfile.WriteNew(idxPath, bytes.NewReader(emptyIdx), 0o644); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	lp := layout.Path(ociDir)

	contentCAS, err := cas.New(filepath.Join(path, "blobs"))
	if err != nil {
		return nil, err
	}

	return &Store{
		path:       path,
		auth:       auth,
		pullPolicy: pullPolicy,
		refs:       referenceStore(filepath.Join(path, "refs")),
		cas:        contentCAS,
		layers:     layerIndexes{root: filepath.Join(path, "layers")},
		lp:         lp,
		ingestMu:   ingestLockFor(path),
	}, nil
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
	if err := os.Mkdir(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

// BlobPath returns the on-disk path of the content-CAS blob a
// unified-view entry names by digest. The blob may not exist if the
// store was damaged; consumers surface read errors.
func (s *Store) BlobPath(h v1.Hash) string {
	return s.cas.Path(h)
}

func (s *Store) Image(ctx context.Context, imageRef string) (*Image, error) {
	h, err := s.pullImage(ctx, imageRef)
	if err != nil {
		return nil, err
	}
	return s.getImage(ctx, h)
}

func (s *Store) getImage(ctx context.Context, h v1.Hash) (*Image, error) {
	img, err := s.lp.Image(h)
	if err != nil {
		return nil, err
	}

	v1Layers, err := img.Layers()
	if err != nil {
		return nil, err
	}

	layers := make([]layer.Layer, len(v1Layers))
	for i, vl := range v1Layers {
		ld, err := vl.Digest()
		if err != nil {
			return nil, err
		}
		l, err := s.layers.Get(ld)
		if err != nil || !s.blobsPresent(l) {
			// Missing or unreadable index, or an index naming a
			// content blob that is gone: re-derive both from the
			// retained compressed layer, no network involved
			// (REQ-store-self-heal).
			s.ingestMu.Lock()
			l, err = s.unpackLayer(ctx, vl)
			s.ingestMu.Unlock()
			if err != nil {
				return nil, fmt.Errorf("self-heal of layer %s: %w", ld, err)
			}
		}
		layers[i] = l
	}

	conf, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}

	return &Image{
		h:      h,
		img:    img,
		layers: layers,
		conf:   conf,
	}, nil
}

func (s *Store) pullImage(ctx context.Context, imageRef string) (v1.Hash, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return emptyHash, err
	}

	h, refFound, err := s.refs.Get(ref)
	if err != nil {
		return emptyHash, err
	}

	if !refFound && s.pullPolicy == PullNever {
		return emptyHash, fmt.Errorf("image %s not found in cache and pull policy is 'Never'", imageRef)
	}

	if refFound {
		if s.pullPolicy == PullIfNotPresent || s.pullPolicy == PullNever {
			return h, nil
		}
		desc, err := remote.Head(ref, append(s.remoteOpts(), remote.WithContext(ctx))...)
		if err != nil {
			return emptyHash, err
		}
		if desc.Digest == h {
			return h, nil
		}
	}

	return s.ingest(ctx, ref)
}

func (s *Store) remoteOpts() []remote.Option {
	opts := []remote.Option{remote.WithAuthFromKeychain(s.auth)}
	if s.transport != nil {
		opts = append(opts, remote.WithTransport(s.transport))
	}
	return opts
}

// ingest pulls ref and materializes it across the content tiers in
// the spec's order (REQ-store-ingest-order): OCI content first, then
// unpacked layers (content blobs, then the layer index), and the
// reference-cache entry strictly last — a crash at any earlier point
// leaves no ref entry, and the next pull re-runs ingest, which is
// idempotent over whatever survived (REQ-store-ingest-idempotent).
func (s *Store) ingest(ctx context.Context, ref name.Reference) (v1.Hash, error) {
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()

	rmtImg, err := remote.Image(ref, append(s.remoteOpts(), remote.WithContext(ctx))...)
	if err != nil {
		return emptyHash, err
	}
	h, err := rmtImg.Digest()
	if err != nil {
		return emptyHash, err
	}

	if err := s.appendImage(rmtImg, h); err != nil {
		return emptyHash, err
	}

	// Unpack from the retained layout, not the remote: the same code
	// path self-heal uses, so ingest proves the retained content is
	// sufficient to derive the extraction tiers.
	img, err := s.lp.Image(h)
	if err != nil {
		return emptyHash, err
	}
	v1Layers, err := img.Layers()
	if err != nil {
		return emptyHash, err
	}
	for _, vl := range v1Layers {
		ld, err := vl.Digest()
		if err != nil {
			return emptyHash, err
		}
		if l, err := s.layers.Get(ld); err == nil && s.blobsPresent(l) {
			continue // already fully unpacked
		}
		if _, err := s.unpackLayer(ctx, vl); err != nil {
			return emptyHash, err
		}
	}

	if err := s.refs.Put(ref, h); err != nil {
		return emptyHash, err
	}
	return h, nil
}

// appendImage retains img's manifest, config, and compressed layers
// in oci/ and appends its index.json descriptor, deduplicating by
// digest. Blobs are written before the descriptor that names them,
// each atomically, so a descriptor never dangles.
func (s *Store) appendImage(img v1.Image, h v1.Hash) error {
	layers, err := img.Layers()
	if err != nil {
		return err
	}
	for _, l := range layers {
		ld, err := l.Digest()
		if err != nil {
			return err
		}
		if err := s.writeOCIBlob(ld, l.Compressed); err != nil {
			return err
		}
	}

	cfgName, err := img.ConfigName()
	if err != nil {
		return err
	}
	rawCfg, err := img.RawConfigFile()
	if err != nil {
		return err
	}
	if err := s.writeOCIBlob(cfgName, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(rawCfg)), nil
	}); err != nil {
		return err
	}

	rawManifest, err := img.RawManifest()
	if err != nil {
		return err
	}
	if err := s.writeOCIBlob(h, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(rawManifest)), nil
	}); err != nil {
		return err
	}

	mt, err := img.MediaType()
	if err != nil {
		return err
	}
	return s.appendDescriptor(v1.Descriptor{
		MediaType: mt,
		Size:      int64(len(rawManifest)),
		Digest:    h,
	})
}

// writeOCIBlob publishes one oci/blobs entry atomically, skipping
// blobs already present (their content is fixed by their digest).
func (s *Store) writeOCIBlob(h v1.Hash, open func() (io.ReadCloser, error)) error {
	path := filepath.Join(string(s.lp), "blobs", h.Algorithm, h.Hex)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	rc, err := open()
	if err != nil {
		return err
	}
	defer rc.Close()
	return atomicfile.Write(path, rc, 0o644)
}

// appendDescriptor adds desc to oci/index.json unless a descriptor
// with the same digest is already listed, and publishes the new
// index atomically.
func (s *Store) appendDescriptor(desc v1.Descriptor) error {
	idxPath := filepath.Join(string(s.lp), "index.json")
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return err
	}
	var idx v1.IndexManifest
	if err := json.Unmarshal(data, &idx); err != nil {
		return fmt.Errorf("oci/index.json: %w", err)
	}
	for _, d := range idx.Manifests {
		if d.Digest == desc.Digest {
			return nil
		}
	}
	idx.Manifests = append(idx.Manifests, desc)
	out, err := json.MarshalIndent(idx, "", "   ")
	if err != nil {
		return err
	}
	return atomicfile.Write(idxPath, bytes.NewReader(out), 0o644)
}

// blobsPresent reports whether every content blob a layer index
// names exists in the CAS; a false answer is a self-heal trigger.
func (s *Store) blobsPresent(l layer.Layer) bool {
	for _, e := range l {
		if e.Digest == (v1.Hash{}) {
			continue
		}
		if _, err := os.Stat(s.cas.Path(e.Digest)); err != nil {
			return false
		}
	}
	return true
}

// unpackLayer extracts vl into the content CAS and publishes its
// layer index, returning the recorded entries. Serves both ingest
// and self-heal; the input is always a layer backed by the retained
// oci/ content or a verified remote.
func (s *Store) unpackLayer(ctx context.Context, vl v1.Layer) (layer.Layer, error) {
	ld, err := vl.Digest()
	if err != nil {
		return nil, err
	}
	rc, err := vl.Uncompressed()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	l, err := s.extractTar(ctx, rc)
	if err != nil {
		return nil, err
	}
	if err := s.layers.Put(ld, l); err != nil {
		return nil, err
	}
	return l, nil
}

// extractTar records every tar entry in order and streams
// regular-file bytes into the content CAS. Each entry's temporary
// lives only for that entry (cas.Put closes and removes it before
// returning), so archives of any size hold one temporary at a time.
func (s *Store) extractTar(ctx context.Context, r io.Reader) (layer.Layer, error) {
	tr := tar.NewReader(r)
	var l layer.Layer

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
			return nil, err
		}

		e := layer.Entry{Header: *hdr}

		if hdr.Typeflag == tar.TypeReg {
			key, _, err := s.cas.Put(tr)
			if err != nil {
				return nil, err
			}
			e.Digest = key
		}

		l = append(l, e)
	}

	return l, nil
}
