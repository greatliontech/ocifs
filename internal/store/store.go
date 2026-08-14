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
//
// Materialization runs one code path for fresh pulls, cached serves,
// and self-heal: a read-only assembly pass first (no lock, no
// mutation), and on incomplete local state a mutating pass under the
// per-root ingest lock that fetches exactly what is missing — always
// by digest, never by tag re-resolution. The platform-selecting
// child walk is the same function in both passes, so a cached
// reference can never select a different child than the pull that
// recorded it.
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
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
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

// errIncomplete classifies the read-only assembly pass finding local
// state missing or unreadable; the caller retries with a mutating
// pass that may fetch and unpack. Never returned by a mutating pass.
var errIncomplete = errors.New("store state incomplete")

type Store struct {
	path            string
	auth            authn.Keychain
	pullPolicy      PullPolicy
	defaultPlatform v1.Platform
	refs            referenceStore
	cas             *cas.CAS
	layers          layerIndexes
	ociDir          string
	// transport overrides the registry transport when non-nil; the
	// injection seam that lets the test harness serve a registry
	// in-process with no sockets.
	transport http.RoundTripper
	// verifier is the consumer's verification hook; nil means no
	// verification and every resolvable image is served
	// (verification-seam.md REQ-seam-optional).
	verifier Verifier

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

// NewStore opens or creates the store at path. A zero defaultPlatform
// falls back to the host-derived platform — host os/arch, linux on
// darwin (REQ-store-platform-default). A nil verifier disables the
// verification seam (verification-seam.md REQ-seam-optional).
func NewStore(path string, auth authn.Keychain, pullPolicy PullPolicy, defaultPlatform v1.Platform, verifier Verifier) (*Store, error) {
	switch pullPolicy {
	case PullIfNotPresent, PullAlways, PullNever:
	default:
		// An unvalidated policy would fall through resolveTop's
		// switch into an unconditional pull.
		return nil, fmt.Errorf("unknown pull policy %v", pullPolicy)
	}
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

	if defaultPlatform.Equals(v1.Platform{}) {
		defaultPlatform = hostPlatform()
	}

	contentCAS, err := cas.New(filepath.Join(path, "blobs"))
	if err != nil {
		return nil, err
	}

	return &Store{
		path:            path,
		auth:            auth,
		pullPolicy:      pullPolicy,
		defaultPlatform: defaultPlatform,
		refs:            referenceStore(filepath.Join(path, "refs")),
		cas:             contentCAS,
		layers:          layerIndexes{root: filepath.Join(path, "layers")},
		ociDir:          ociDir,
		verifier:        verifier,
		ingestMu:        ingestLockFor(path),
	}, nil
}

// NewMountState creates the per-mount state directory mounts/<id> —
// the mount's bookkeeping beside its mnt/ mountpoint subdirectory
// (REQ-store-layout), a sibling layout so a live mount cannot shadow
// its own state. An empty id draws a random one; a supplied id must
// be a single path element — no separators, not "." or ".." — so no
// id can place the state outside the mounts/ tier
// (api.md REQ-api-mount-id).
func (s *Store) NewMountState(id string) (stateDir, mountDir string, err error) {
	if id == "" {
		uid, err := uuid.NewRandom()
		if err != nil {
			return "", "", err
		}
		id = uid.String()
	}
	if !validMountID(id) {
		return "", "", fmt.Errorf("mount id %q is not a single path element", id)
	}
	stateDir = filepath.Join(s.path, "mounts", id)
	if err := os.Mkdir(stateDir, 0o755); err != nil {
		return "", "", err
	}
	mountDir = filepath.Join(stateDir, "mnt")
	if err := os.Mkdir(mountDir, 0o755); err != nil {
		os.Remove(stateDir)
		return "", "", err
	}
	return stateDir, mountDir, nil
}

func validMountID(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, `/\`)
}

// BlobPath returns the on-disk path of the content-CAS blob a
// unified-view entry names by digest. The blob may not exist if the
// store was damaged; consumers surface read errors.
func (s *Store) BlobPath(h v1.Hash) string {
	return s.cas.Path(h)
}

// request carries one materialization request: the reference as
// given, its digest identity when the reference is digest-form (the
// digest is then the resolution — no policy, no network), and the
// platform with its explicitness (nil platform at the API boundary
// means the configured default; only explicit requests constrain a
// direct manifest).
type request struct {
	// ref parses the reference as the consumer requested it;
	// ref.String() returns that original spelling — the seam's
	// Reference input (verification-seam.md REQ-seam-input) — while
	// name resolution uses the parsed form.
	ref      name.Reference
	digest   *v1.Hash
	platform v1.Platform
	explicit bool
}

// Image materializes imageRef for the requested platform (nil: the
// store's default) and returns the platform-selected image. The
// reference may be tag- or digest-form; digest-form requests never
// re-resolve a tag (REQ-store-digest-entry).
func (s *Store) Image(ctx context.Context, imageRef string, platform *v1.Platform) (*Image, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, err
	}
	req := request{ref: ref, platform: s.defaultPlatform, explicit: platform != nil}
	if platform != nil {
		req.platform = *platform
	}
	if d, ok := ref.(name.Digest); ok {
		h, err := v1.NewHash(d.DigestStr())
		if err != nil {
			return nil, err
		}
		req.digest = &h
	}

	top, needRecord, err := s.resolveTop(ctx, req)
	if err != nil {
		return nil, err
	}

	// The verification seam sits between top-level resolution and any
	// materialization for this request (REQ-seam-position); a
	// rejection returns before assemble touches layer content and
	// before the reference-cache record below (REQ-seam-abort).
	if err := s.verify(ctx, req, top); err != nil {
		return nil, err
	}

	img, err := s.assemble(ctx, req, top, nil)
	if errors.Is(err, errIncomplete) {
		f := &fetcher{store: s, repo: ref.Context(), allowed: s.pullPolicy != PullNever}
		s.ingestMu.Lock()
		img, err = s.assemble(ctx, req, top, f)
		s.ingestMu.Unlock()
	}
	if err != nil {
		return nil, err
	}

	// The reference-cache entry is ingest's completion barrier: it is
	// recorded only after the artifact and the requested platform are
	// fully materialized (REQ-store-ingest-order).
	if needRecord {
		if err := s.refs.Put(req.ref, top); err != nil {
			return nil, err
		}
	}
	return img, nil
}

// resolveTop resolves the request to its top-level digest per pull
// policy, retaining a remotely fetched top-level artifact in oci/
// before returning. The second result reports whether the
// reference-cache entry must still be recorded after materialization.
func (s *Store) resolveTop(ctx context.Context, req request) (v1.Hash, bool, error) {
	cached, found, err := s.refs.Get(req.ref)
	if err != nil {
		return emptyHash, false, err
	}

	// Digest-form: the digest is the identity — no resolution, no
	// revalidation (an immutable binding cannot move), no network.
	if req.digest != nil {
		return *req.digest, !found || cached != *req.digest, nil
	}

	switch s.pullPolicy {
	case PullNever:
		if !found {
			return emptyHash, false, fmt.Errorf("image %s not found in cache and pull policy is 'Never'", req.ref)
		}
		return cached, false, nil
	case PullIfNotPresent:
		if found {
			return cached, false, nil
		}
	case PullAlways:
		if found {
			desc, err := remote.Head(req.ref, s.remoteOpts(ctx)...)
			if err != nil {
				return emptyHash, false, err
			}
			// Top-level to top-level: a HEAD on a multi-platform
			// reference returns the index digest, and the ref cache
			// records exactly that (REQ-store-pull-policy).
			if desc.Digest == cached {
				return cached, false, nil
			}
		}
	}

	desc, err := remote.Get(req.ref, s.remoteOpts(ctx)...)
	if err != nil {
		return emptyHash, false, err
	}
	if err := s.writeOCIBlob(desc.Digest, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(desc.Manifest)), nil
	}); err != nil {
		return emptyHash, false, err
	}
	return desc.Digest, true, nil
}

func (s *Store) remoteOpts(ctx context.Context) []remote.Option {
	opts := []remote.Option{remote.WithAuthFromKeychain(s.auth), remote.WithContext(ctx)}
	if s.transport != nil {
		opts = append(opts, remote.WithTransport(s.transport))
	}
	return opts
}

// fetcher fetches store content by digest from the request's
// repository — the only network access materialization performs, so
// no code path can re-resolve a tag. allowed is false under
// PullNever: local state is still consulted (and locally re-derived)
// but a missing oci/ blob fails identifying itself
// (REQ-store-self-heal).
type fetcher struct {
	store   *Store
	repo    name.Repository
	allowed bool
}

func (f *fetcher) manifest(ctx context.Context, h v1.Hash) ([]byte, error) {
	desc, err := remote.Get(f.repo.Digest(h.String()), f.store.remoteOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return desc.Manifest, nil
}

func (f *fetcher) blob(ctx context.Context, h v1.Hash) (io.ReadCloser, error) {
	l, err := remote.Layer(f.repo.Digest(h.String()), f.store.remoteOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return l.Compressed()
}

// assemble materializes the top-level artifact for req's platform
// and returns the platform-selected image. With f == nil it is a
// read-only pass: missing or unreadable local state returns
// errIncomplete and nothing is written. With f it is the mutating
// pass (caller holds the ingest lock): exactly the missing pieces
// are fetched by digest, retention is re-completed, and layers are
// unpacked — in the spec's ingest order (REQ-store-ingest-order).
func (s *Store) assemble(ctx context.Context, req request, top v1.Hash, f *fetcher) (*Image, error) {
	topRaw, err := s.ensureManifest(ctx, f, top)
	if err != nil {
		return nil, err
	}
	topKind, topMediaType, err := manifestKind(topRaw)
	if err != nil {
		return nil, fmt.Errorf("artifact %s: %w", top, err)
	}

	child := top
	childRaw := topRaw
	if topKind == kindIndex {
		idx, err := v1.ParseIndexManifest(bytes.NewReader(topRaw))
		if err != nil {
			return nil, fmt.Errorf("index %s: %w", top, err)
		}
		desc, err := selectChild(idx, req.platform)
		if err != nil {
			return nil, fmt.Errorf("index %s: %w", top, err)
		}
		// Same hazard as the config/layer check below: a digest-less
		// descriptor would resolve to the blob tier's directory.
		if desc.Digest.Algorithm == "" || desc.Digest.Hex == "" {
			return nil, fmt.Errorf("index %s: selected child descriptor has no digest", top)
		}
		child = desc.Digest
		childRaw, err = s.ensureManifest(ctx, f, child)
		if err != nil {
			return nil, err
		}
		childKind, _, err := manifestKind(childRaw)
		if err != nil {
			return nil, fmt.Errorf("manifest %s: %w", child, err)
		}
		if childKind == kindIndex {
			return nil, fmt.Errorf("platform-selected child %s of index %s is itself an index; nested indexes are not supported", child, top)
		}
	}

	m, err := v1.ParseManifest(bytes.NewReader(childRaw))
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", child, err)
	}
	// A descriptor without a digest would resolve to the blob tier's
	// directory itself; reject the malformed manifest outright.
	if m.Config.Digest.Algorithm == "" || m.Config.Digest.Hex == "" {
		return nil, fmt.Errorf("manifest %s: config descriptor has no digest", child)
	}
	for _, ld := range m.Layers {
		if ld.Digest.Algorithm == "" || ld.Digest.Hex == "" {
			return nil, fmt.Errorf("manifest %s: layer descriptor has no digest", child)
		}
	}

	cfgRaw, err := s.readBlob(ctx, f, m.Config.Digest)
	if err != nil {
		return nil, err
	}
	conf, err := v1.ParseConfigFile(bytes.NewReader(cfgRaw))
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", m.Config.Digest, err)
	}

	// Only an explicit platform constrains a direct manifest
	// (REQ-store-platform-strict); an index child was already
	// selected by its index descriptor.
	if topKind == kindManifest && req.explicit {
		if !configMatchesPlatform(req.platform, conf) {
			return nil, fmt.Errorf("manifest %s is for platform %s, not the requested %s",
				child, (&v1.Platform{OS: conf.OS, Architecture: conf.Architecture, Variant: conf.Variant, OSVersion: conf.OSVersion}).String(),
				req.platform.String())
		}
	}

	// Retain every compressed layer before unpacking any, and append
	// the top-level descriptor only after every blob it transitively
	// names is durable, so a descriptor never dangles
	// (REQ-store-ingest-order). The read-only pass conversely demands
	// the descriptor already be listed: content can pre-exist through
	// a shared route (a digest pull of the child, an overlapping
	// image), and completing without the append would record a ref
	// whose top-level artifact oci/index.json never enumerates.
	for _, ld := range m.Layers {
		if err := s.ensureBlob(ctx, f, ld.Digest); err != nil {
			return nil, err
		}
	}
	if f == nil {
		listed, err := s.descriptorListed(top)
		if err != nil {
			return nil, err
		}
		if !listed {
			return nil, errIncomplete
		}
	} else {
		if err := s.appendDescriptor(v1.Descriptor{
			MediaType: topMediaType,
			Size:      int64(len(topRaw)),
			Digest:    top,
		}); err != nil {
			return nil, err
		}
	}

	layers := make([]layer.Layer, len(m.Layers))
	for i, ld := range m.Layers {
		l, err := s.layers.Get(ld.Digest)
		if err != nil || !s.blobsPresent(l) {
			// Missing or unreadable index, or an index naming a
			// content blob that is gone: re-derive both from the
			// retained compressed layer (REQ-store-self-heal).
			if f == nil {
				return nil, errIncomplete
			}
			l, err = s.unpackLayer(ctx, ld.Digest)
			if err != nil {
				return nil, fmt.Errorf("unpack layer %s: %w", ld.Digest, err)
			}
		}
		layers[i] = l
	}

	return &Image{h: child, conf: conf, layers: layers}, nil
}

func (s *Store) ociBlobPath(h v1.Hash) string {
	return filepath.Join(s.ociDir, "blobs", h.Algorithm, h.Hex)
}

// ensureManifest returns the manifest-or-index bytes at h from oci/,
// fetching and retaining them by digest when absent and the pass may
// mutate. The registry's manifest endpoint serves them; the fetch is
// digest-verified before anything is written
// (REQ-store-ingest-verified).
func (s *Store) ensureManifest(ctx context.Context, f *fetcher, h v1.Hash) ([]byte, error) {
	raw, err := os.ReadFile(s.ociBlobPath(h))
	if err == nil {
		return raw, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if f == nil {
		return nil, errIncomplete
	}
	if !f.allowed {
		return nil, fmt.Errorf("artifact %s is not retained in oci/ and pull policy %s forbids fetching it", h, s.pullPolicy)
	}
	raw, err = f.manifest(ctx, h)
	if err != nil {
		return nil, err
	}
	if err := s.writeOCIBlob(h, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}); err != nil {
		return nil, err
	}
	return raw, nil
}

// ensureBlob guarantees the config or compressed-layer blob h exists
// in oci/, fetching it by digest from the blob endpoint when absent
// and the pass may mutate.
func (s *Store) ensureBlob(ctx context.Context, f *fetcher, h v1.Hash) error {
	if _, err := os.Stat(s.ociBlobPath(h)); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.fetchMissingBlob(ctx, f, h)
}

// readBlob returns the blob bytes at h, fetching a missing blob under
// ensureBlob's rules first — one classification for a missing blob,
// whether the caller wants bytes or existence.
func (s *Store) readBlob(ctx context.Context, f *fetcher, h v1.Hash) ([]byte, error) {
	raw, err := os.ReadFile(s.ociBlobPath(h))
	if err == nil {
		return raw, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := s.fetchMissingBlob(ctx, f, h); err != nil {
		return nil, err
	}
	return os.ReadFile(s.ociBlobPath(h))
}

// fetchMissingBlob classifies a blob found absent: the read-only pass
// reports incomplete state, PullNever fails naming the blob
// (REQ-store-self-heal), and otherwise the blob is fetched by digest
// and published.
func (s *Store) fetchMissingBlob(ctx context.Context, f *fetcher, h v1.Hash) error {
	if f == nil {
		return errIncomplete
	}
	if !f.allowed {
		return fmt.Errorf("blob %s is missing from oci/ and pull policy %s forbids fetching it", h, s.pullPolicy)
	}
	return s.writeOCIBlob(h, func() (io.ReadCloser, error) { return f.blob(ctx, h) })
}

type artifactKind int

const (
	kindManifest artifactKind = iota
	kindIndex
)

// manifestKind classifies raw manifest bytes as an image index or an
// image manifest, tolerating an absent mediaType field (legal in OCI)
// by structure. Anything else — including docker schema 1 — is
// rejected explicitly rather than half-parsed.
func manifestKind(raw []byte) (artifactKind, types.MediaType, error) {
	var probe struct {
		MediaType types.MediaType `json:"mediaType"`
		Manifests json.RawMessage `json:"manifests"`
		Config    json.RawMessage `json:"config"`
		FSLayers  json.RawMessage `json:"fsLayers"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return 0, "", err
	}
	switch {
	case probe.MediaType.IsIndex():
		return kindIndex, probe.MediaType, nil
	case probe.MediaType.IsImage():
		return kindManifest, probe.MediaType, nil
	case probe.MediaType != "":
		return 0, "", fmt.Errorf("unsupported media type %q", probe.MediaType)
	case probe.FSLayers != nil:
		return 0, "", errors.New("docker schema 1 manifests are not supported")
	case probe.Manifests != nil:
		return kindIndex, types.OCIImageIndex, nil
	case probe.Config != nil:
		return kindManifest, types.OCIManifestSchema1, nil
	default:
		return 0, "", errors.New("unrecognized manifest: neither an index nor an image manifest")
	}
}

// writeOCIBlob publishes one oci/blobs entry atomically, skipping
// blobs already present (their content is fixed by their digest).
func (s *Store) writeOCIBlob(h v1.Hash, open func() (io.ReadCloser, error)) error {
	path := s.ociBlobPath(h)
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

// descriptorListed reports whether oci/index.json lists a descriptor
// with digest h. Reads race appendDescriptor's atomic replacement
// safely: a reader sees a complete old or new document, never a torn
// one.
func (s *Store) descriptorListed(h v1.Hash) (bool, error) {
	data, err := os.ReadFile(filepath.Join(s.ociDir, "index.json"))
	if err != nil {
		return false, err
	}
	var idx v1.IndexManifest
	if err := json.Unmarshal(data, &idx); err != nil {
		return false, fmt.Errorf("oci/index.json: %w", err)
	}
	for _, d := range idx.Manifests {
		if d.Digest == h {
			return true, nil
		}
	}
	return false, nil
}

// appendDescriptor adds desc to oci/index.json unless a descriptor
// with the same digest is already listed, and publishes the new
// index atomically.
func (s *Store) appendDescriptor(desc v1.Descriptor) error {
	idxPath := filepath.Join(s.ociDir, "index.json")
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

// unpackLayer extracts the retained compressed layer at ld into the
// content CAS and publishes its layer index, returning the recorded
// entries. Serves both ingest and self-heal; the caller has ensured
// the oci/ blob exists. Compression is detected from the bytes
// (gzip, zstd, or none), not the manifest's media type — the digest
// already fixed the bytes, and the bytes carry their own framing.
func (s *Store) unpackLayer(ctx context.Context, ld v1.Hash) (layer.Layer, error) {
	blobPath := s.ociBlobPath(ld)
	vl, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return os.Open(blobPath)
	})
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
