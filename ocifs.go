// Package ocifs acquires, verifies, caches, exports, archives, and
// commits OCI images over a crash-safe local store. Live filesystem
// projection mounting (FUSE, FSKit, ProjFS) lives in the sibling
// github.com/greatliontech/ocifs/mount module so consumers of the
// store and export surface carry zero mount-backend dependencies.
//
// The public surface is pinned by docs/specs/api.md; construction
// configures the store root, credentials, pull policy, default
// platform, and an optional verification hook
// (docs/specs/verification-seam.md), acquisition works by reference
// string (tag or digest form) with an optional explicit platform, and
// the credential resolution the store uses is available to the
// consumer for round trips it makes itself.
package ocifs

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/store"
)

// PullPolicy controls when acquisition consults the registry;
// semantics are pinned by docs/specs/store.md (REQ-store-pull-policy).
type PullPolicy = store.PullPolicy

const (
	PullIfNotPresent = store.PullIfNotPresent
	PullAlways       = store.PullAlways
	PullNever        = store.PullNever
)

// settings holds construction-only configuration populated by Option
// values inside New, making post-construction option mutation
// unrepresentable (REQ-api-construction).
type settings struct {
	workDir                string
	extraDirs              []string
	creds                  map[string]authn.AuthConfig
	includeDefaultKeychain bool
	transport              http.RoundTripper
	pullPolicy             PullPolicy
	defaultPlatform        v1.Platform
	verifier               Verifier
	autoGC                 bool
	gcGrace                time.Duration
}

// Option configures an OCIFS instance at construction time.
type Option func(*settings)

func WithWorkDir(workDir string) Option {
	return func(s *settings) {
		s.workDir = filepath.Clean(workDir)
	}
}

func WithExtraDirs(extraDirs []string) Option {
	cloned := slices.Clone(extraDirs)
	return func(s *settings) {
		s.extraDirs = slices.Clone(cloned)
	}
}

func WithAuthSource(prefix string, auth authn.AuthConfig) Option {
	return func(s *settings) {
		s.creds[prefix] = auth
	}
}

// WithTransport routes every registry round trip through rt in place
// of the registry client's own transport, which wraps a given one
// for its retries as well: a consumer that serves a registry
// in-process, or pins its client, hands it here (api.md
// REQ-api-construction).
func WithTransport(rt http.RoundTripper) Option {
	return func(s *settings) {
		s.transport = rt
	}
}

func WithEnableDefaultKeychain() Option {
	return func(s *settings) {
		s.includeDefaultKeychain = true
	}
}

// WithPullPolicy sets the pull policy (default PullIfNotPresent).
func WithPullPolicy(p PullPolicy) Option {
	return func(s *settings) {
		s.pullPolicy = p
	}
}

// WithDefaultPlatform sets the platform used when an acquisition
// names none. Unset, the default derives from the host: its os/arch,
// except on darwin, where it is linux with the host's architecture
// (docs/specs/store.md REQ-store-platform-default).
func WithDefaultPlatform(p v1.Platform) Option {
	return func(s *settings) {
		s.defaultPlatform = p
	}
}

// Verifier is the verification seam's consumer-supplied hook
// (docs/specs/verification-seam.md): it judges every acquisition's
// resolved identity against the consumer's trust policy, after
// top-level resolution and before any content is materialized for
// the request — cached content included. ocifs ships no
// implementation and depends on no signature tooling; without a
// configured Verifier, no verification occurs.
type Verifier = store.Verifier

// ResolvedIdentity is what a Verifier receives: the reference as
// requested, the resolved top-level digest, and the top-level
// artifact's bytes (the image index for a multi-platform image —
// platform selection happens only after the seam passes).
type ResolvedIdentity = store.ResolvedIdentity

// VerificationError is the error an acquisition or a resolution
// returns when the configured Verifier rejects it; match with
// errors.As to distinguish verification failure from resolution
// failure. Nothing is served and the reference cache records nothing
// for the failed resolution.
type VerificationError = store.VerificationError

// WithVerifier configures the verification seam's hook
// (docs/specs/verification-seam.md).
func WithVerifier(v Verifier) Option {
	return func(s *settings) {
		s.verifier = v
	}
}

// WithAutoGC turns automatic collection at garbage-creating
// transitions on or off (default on — store.md
// REQ-store-gc-collect).
func WithAutoGC(on bool) Option {
	return func(s *settings) {
		s.autoGC = on
	}
}

// WithGCGrace sets the retention grace for unreachable content
// (default 24h): blobs are content-addressed and shared, so
// unreachable content still deduplicates a future pull. Pure
// retention policy, never load-bearing for safety.
func WithGCGrace(d time.Duration) Option {
	return func(s *settings) {
		s.gcGrace = d
	}
}

type OCIFS struct {
	workDir    string
	extraDirs  []string
	authn      *ocifsKeychain
	pullPolicy PullPolicy
	store      *store.Store
	gcGrace    time.Duration
}

func New(opts ...Option) (*OCIFS, error) {
	cfg := settings{
		workDir:    filepath.Join(os.TempDir(), "ocifs"),
		creds:      make(map[string]authn.AuthConfig),
		pullPolicy: PullIfNotPresent,
		autoGC:     true,
		gcGrace:    24 * time.Hour,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	kc := &ocifsKeychain{
		creds:                  cfg.creds,
		includeDefaultKeychain: cfg.includeDefaultKeychain,
	}
	s, err := store.NewStore(store.Config{
		Path:            cfg.workDir,
		Auth:            kc,
		PullPolicy:      cfg.pullPolicy,
		DefaultPlatform: cfg.defaultPlatform,
		Verifier:        cfg.verifier,
		AutoGC:          cfg.autoGC,
		GCGrace:         cfg.gcGrace,
		Transport:       cfg.transport,
	})
	if err != nil {
		return nil, err
	}

	return &OCIFS{
		workDir:    cfg.workDir,
		extraDirs:  slices.Clone(cfg.extraDirs),
		authn:      kc,
		pullPolicy: cfg.pullPolicy,
		store:      s,
		gcGrace:    cfg.gcGrace,
	}, nil
}

// MountState exposes the underlying store handle, workDir, and
// extraDirs to github.com/greatliontech/ocifs/mount without leaking
// internal types to external consumers.
func (o *OCIFS) MountState(store.Internal) (st *store.Store, workDir string, extraDirs []string) {
	return o.store, o.workDir, slices.Clone(o.extraDirs)
}

// Image is a materialized image: the platform-selected manifest and
// its config, ready to export or archive. It remembers the store that
// materialized it, so an export of it is the same acquisition
// continued — no second resolution, no second seam run. Every Image
// is made by newImage, so none exists without its store.
type Image struct {
	ofs *OCIFS
	img *store.Image
}

func (o *OCIFS) newImage(img *store.Image) *Image {
	return &Image{ofs: o, img: img}
}

// Digest returns the digest of the platform-selected manifest — the
// identity of the materialized image (REQ-store-platform-serves-child).
func (i *Image) Digest() v1.Hash {
	return i.img.Hash()
}

func (i *Image) ConfigFile() *v1.ConfigFile {
	return i.img.ConfigFile()
}

type PullOption func(*pullReq)

type pullReq struct {
	platform *v1.Platform
}

// PullWithPlatform requests an explicit platform: index selection is
// strict, and a direct manifest must match (REQ-store-platform-strict).
func PullWithPlatform(p v1.Platform) PullOption {
	return func(r *pullReq) {
		r.platform = &p
	}
}

// Pull materializes imageRef — tag or digest form, resolved per the
// pull policy — and returns the image. Digest-form references are
// materialized without any tag re-resolution
// (REQ-store-digest-entry).
func (o *OCIFS) Pull(ctx context.Context, imageRef string, opts ...PullOption) (*Image, error) {
	var r pullReq
	for _, opt := range opts {
		if opt != nil {
			opt(&r)
		}
	}
	img, err := o.store.Image(ctx, imageRef, r.platform)
	if err != nil {
		return nil, err
	}
	return o.newImage(img), nil
}

// Resolved is what a resolution yields: the reference as requested
// and the top-level digest it resolved to, admitted by the
// verification seam where one is configured.
type Resolved struct {
	// Reference is the reference exactly as requested.
	Reference string
	// Digest is the top-level digest the reference resolved to: the
	// index of a multi-platform image, the manifest otherwise.
	Digest v1.Hash
}

// ResolveOption shapes one resolution.
type ResolveOption func(*resolveReq)

type resolveReq struct {
	policy PullPolicy
}

// ResolveUnder resolves under the given pull policy in place of the
// store's, for this call alone (api.md REQ-api-resolve): a consumer
// re-resolving a reference it holds cached asks Always of the one
// call. A store held at Never grants no call a policy past it.
func ResolveUnder(p PullPolicy) ResolveOption {
	return func(r *resolveReq) {
		r.policy = p
	}
}

// Resolve resolves imageRef — tag or digest form, per the store's
// pull policy or the one an option states for this call alone
// (ResolveUnder) — to its top-level digest and runs the verification
// seam on it exactly as an acquisition would, materializing nothing
// (api.md REQ-api-resolve): for a consumer whose substrate fetches
// the content itself by that digest, this is the whole of the
// acquisition. A digest-form reference resolves to itself and still
// runs the seam.
func (o *OCIFS) Resolve(ctx context.Context, imageRef string, opts ...ResolveOption) (*Resolved, error) {
	r := resolveReq{policy: o.pullPolicy}
	for _, opt := range opts {
		if opt != nil {
			opt(&r)
		}
	}
	h, err := o.store.ResolveUnder(ctx, imageRef, r.policy)
	if err != nil {
		return nil, err
	}
	return &Resolved{Reference: imageRef, Digest: h}, nil
}

// Keychain is the store's credential resolution (api.md
// REQ-api-keychain), for round trips the consumer makes itself: what
// the store sends for a registry, the consumer sends.
func (o *OCIFS) Keychain() authn.Keychain { return o.authn }

// Close releases the store's coordination resources — the
// bookkeeping database's reader slots, heartbeat, and writer
// coordination (docs/specs/store.md REQ-store-bookkeeping). Live
// mounts are unaffected by construction-state release; a consumer
// done with the handle closes it, and a process-lifetime handle may
// equally leave it to exit — the database is crash-safe either way.
func (o *OCIFS) Close() error {
	return o.store.Close()
}

// GCOption configures an explicit collection.
type GCOption func(*store.CollectOpts)

// GCIgnoreGrace collects unreachable content regardless of age —
// the wipe-now operator intent automatic collection deliberately
// does not serve (api.md REQ-api-gc).
func GCIgnoreGrace() GCOption {
	return func(o *store.CollectOpts) {
		o.Grace = -1
	}
}

// GCResult is what a collection pass reports: the blobs and paths
// collected, the dead mounts reclaimed, the live rows of another
// version that halted image-tier collection, and the items whose
// deletion is deferred to the next pass (api.md REQ-api-gc). A
// consumer holding a collection to its word reads it by this name.
type GCResult = store.GCResult

// GC runs one explicit collection pass, honoring the configured
// retention grace unless overridden, and reports what was
// collected, what was deferred, and the live rows of another version
// that halted image-tier collection (api.md REQ-api-gc).
func (o *OCIFS) GC(ctx context.Context, opts ...GCOption) (*GCResult, error) {
	co := store.CollectOpts{Grace: o.gcGrace}
	for _, opt := range opts {
		if opt != nil {
			opt(&co)
		}
	}
	return o.store.Collect(ctx, co)
}

// RemoveRef deletes the cached resolution for a reference — the
// root, not the content; collection reclaims what nothing else
// roots (api.md REQ-api-remove).
func (o *OCIFS) RemoveRef(ctx context.Context, ref string) error {
	return o.store.RemoveRef(ctx, ref)
}

// RemoveImage deletes a committed image's root. An image a live
// mount serves is refused.
func (o *OCIFS) RemoveImage(ctx context.Context, digest string) error {
	h, err := v1.NewHash(digest)
	if err != nil {
		return err
	}
	return o.store.RemoveImage(ctx, h)
}

// RemoveUpper deletes a named upper — dialect tree and base
// binding. An upper a live mount serves is refused.
func (o *OCIFS) RemoveUpper(ctx context.Context, name string) error {
	return o.store.RemoveUpper(ctx, name)
}

// RemoveAll severs every root the store holds at once — every cached
// reference and every local image no live mount serves — and returns
// what stays kept, sorted: the local images live mounts serve and the
// images live in-flight operations pin (a hold among them), each by
// the digest the store materialized — the platform child of an
// index-resolved reference, not the index; collection reclaims what
// nothing roots any more (api.md REQ-api-remove).
func (o *OCIFS) RemoveAll(ctx context.Context) ([]string, error) {
	kept, err := o.store.RemoveAll(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(kept))
	for _, h := range kept {
		out = append(out, h.String())
	}
	return out, nil
}

// Hold is a consumer's claim over an acquired image: the image and
// its export stay until Release, whatever removes the reference
// meanwhile, and a crashed holder releases with its process (api.md
// REQ-api-hold).
type Hold struct {
	ofs   *OCIFS
	claim *store.OpClaim
}

// ErrGone refuses a hold over an image a collection took before it,
// and ErrCondemned one over an image a live sweep has condemned: the
// consumer re-acquires and holds again (api.md REQ-api-hold).
var (
	ErrGone      = store.ErrGone
	ErrCondemned = store.ErrCondemned
)

// Hold takes a hold over the image (api.md REQ-api-hold): an
// in-flight operation pinning its manifest, a collection root while
// the holder lives. The hold comes before the export it vouches for:
// an export materialized earlier is the snapshot rule's, collectible
// meanwhile, so a consumer holds, then exports. A refused hold wraps
// ErrGone or ErrCondemned.
func (o *OCIFS) Hold(ctx context.Context, img *Image) (*Hold, error) {
	if img == nil {
		return nil, errors.New("ocifs: hold over no image")
	}
	claim, err := o.store.Hold(ctx, img.Digest())
	if err != nil {
		return nil, err
	}
	return &Hold{ofs: o, claim: claim}, nil
}

// Release ends the hold; the image is garbage unless something else
// roots it. A second Release is nothing. A hold outliving the store's
// Close releases with an error, its row left as a dead claim the next
// sweep reclaims.
func (h *Hold) Release(ctx context.Context) error {
	if h.claim == nil {
		return nil
	}
	claim := h.claim
	h.claim = nil
	return h.ofs.store.EndOp(ctx, claim)
}
