// Package ocifs mounts OCI images as read-only filesystems. The
// public surface is pinned by docs/specs/api.md; construction
// configures the store root, credentials, pull policy, default
// platform, and an optional verification hook
// (docs/specs/verification-seam.md), and acquisition works by
// reference string (tag or digest form) with an optional explicit
// platform.
package ocifs

import (
	"context"
	"os"
	"path/filepath"

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

type Option func(*OCIFS)

var WithWorkDir = func(workDir string) Option {
	return func(o *OCIFS) {
		o.workDir = filepath.Clean(workDir)
	}
}

var WithExtraDirs = func(extraDirs []string) Option {
	return func(o *OCIFS) {
		o.extraDirs = extraDirs
	}
}

var WithAuthSource = func(prefix string, auth authn.AuthConfig) Option {
	return func(o *OCIFS) {
		o.authn.creds[prefix] = auth
	}
}

var WithEnableDefaultKeychain = func() Option {
	return func(o *OCIFS) {
		o.authn.includeDefaultKeychain = true
	}
}

// WithPullPolicy sets the pull policy (default PullIfNotPresent).
var WithPullPolicy = func(p PullPolicy) Option {
	return func(o *OCIFS) {
		o.pullPolicy = p
	}
}

// WithDefaultPlatform sets the platform used when an acquisition
// names none. Unset, the default derives from the host: its os/arch,
// except on darwin, where it is linux with the host's architecture
// (docs/specs/store.md REQ-store-platform-default).
var WithDefaultPlatform = func(p v1.Platform) Option {
	return func(o *OCIFS) {
		o.defaultPlatform = p
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

// VerificationError is the error an acquisition returns when the
// configured Verifier rejects it; match with errors.As to
// distinguish verification failure from resolution failure. Nothing
// is served and the reference cache records nothing for the failed
// resolution.
type VerificationError = store.VerificationError

// WithVerifier configures the verification seam's hook
// (docs/specs/verification-seam.md).
var WithVerifier = func(v Verifier) Option {
	return func(o *OCIFS) {
		o.verifier = v
	}
}

type OCIFS struct {
	workDir         string
	extraDirs       []string
	authn           *ocifsKeychain
	pullPolicy      PullPolicy
	defaultPlatform v1.Platform
	verifier        Verifier
	store           *store.Store
}

func New(opts ...Option) (*OCIFS, error) {
	// default values
	ofs := &OCIFS{
		workDir: filepath.Join(os.TempDir(), "ocifs"),
		authn: &ocifsKeychain{
			creds: make(map[string]authn.AuthConfig),
		},
		pullPolicy: PullIfNotPresent,
	}

	// apply options
	for _, opt := range opts {
		opt(ofs)
	}

	// initialize store
	s, err := store.NewStore(ofs.workDir, ofs.authn, ofs.pullPolicy, ofs.defaultPlatform, ofs.verifier)
	if err != nil {
		return nil, err
	}
	ofs.store = s

	return ofs, nil
}

// Image is a materialized image: the platform-selected manifest and
// its config, ready to mount or export.
type Image struct {
	img *store.Image
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
var PullWithPlatform = func(p v1.Platform) PullOption {
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
		opt(&r)
	}
	img, err := o.store.Image(ctx, imageRef, r.platform)
	if err != nil {
		return nil, err
	}
	return &Image{img: img}, nil
}

// mountServer is what a platform backend returns from platformMount:
// a live projection that serves until unmounted.
type mountServer interface {
	Wait()
	Unmount() error
}

type ImageMount struct {
	ofs        *OCIFS
	server     mountServer
	img        *store.Image
	mountPoint string
	id         string
	ctx        context.Context
	platform   *v1.Platform
}

func (im *ImageMount) ConfigFile() *v1.ConfigFile {
	return im.img.ConfigFile()
}

func (im *ImageMount) Wait() {
	im.server.Wait()
}

func (im *ImageMount) Unmount() error {
	return im.server.Unmount()
}

func (im *ImageMount) MountPoint() string {
	return im.mountPoint
}

type MountOption func(*ImageMount)

var MountWithTargetPath = func(targetPath string) MountOption {
	return func(im *ImageMount) {
		im.mountPoint = targetPath
	}
}

var MountWithID = func(id string) MountOption {
	return func(im *ImageMount) {
		im.id = id
	}
}

var MountWithContext = func(ctx context.Context) MountOption {
	return func(im *ImageMount) {
		im.ctx = ctx
	}
}

// MountWithPlatform mounts an explicit platform; selection semantics
// match PullWithPlatform.
var MountWithPlatform = func(p v1.Platform) MountOption {
	return func(im *ImageMount) {
		im.platform = &p
	}
}

func (o *OCIFS) Mount(imgRef string, opts ...MountOption) (*ImageMount, error) {
	im := &ImageMount{
		ofs: o,
		ctx: context.Background(),
	}
	for _, opt := range opts {
		opt(im)
	}

	// Acquire the image before creating any per-mount state, so a
	// failed pull leaves no orphan under mounts/.
	img, err := o.store.Image(im.ctx, imgRef, im.platform)
	if err != nil {
		return nil, err
	}
	im.img = img

	view, err := img.Unify()
	if err != nil {
		return nil, err
	}

	// Every mount owns per-mount state — the projection report's home
	// (REQ-proj-report) — whether or not the caller supplied a
	// mountpoint. Removed again on any later failure (im.server is
	// set only on success); a caller-supplied target is caller-owned
	// and never touched.
	stateDir, mountDir, err := o.store.NewMountState(im.id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if im.server == nil {
			os.RemoveAll(stateDir)
		}
	}()
	if im.mountPoint == "" {
		im.mountPoint = mountDir
	}

	im.mountPoint = filepath.Clean(im.mountPoint)
	if !filepath.IsAbs(im.mountPoint) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		im.mountPoint = filepath.Clean(filepath.Join(cwd, im.mountPoint))
	}

	srv, err := platformMount(o, imgRef, img, view, stateDir, im.mountPoint)
	if err != nil {
		return nil, err
	}
	im.server = srv

	return im, nil
}
