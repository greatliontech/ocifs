// Package mount serves OCI images acquired by github.com/greatliontech/ocifs
// as live read-only or writable filesystem projections (FUSE on Linux,
// appex-mediated FSKit on macOS, ProjFS on Windows; docs/specs/projection.md,
// docs/specs/writable.md, docs/specs/api.md).
package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb/oslock"
	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/store"
)

// Report, ReportEntry, Disposition, and Reason expose the projection
// report surface so consumers can switch exhaustively on dispositions
// and symbolic reasons (projection.md REQ-proj-report).
type (
	Report      = projection.Report
	ReportEntry = projection.ReportEntry
	Disposition = projection.Disposition
	Reason      = projection.Reason
)

const (
	DispositionOmitted  = projection.DispositionOmitted
	DispositionAltered  = projection.DispositionAltered
	DispositionResidual = projection.DispositionResidual
)

const (
	ReasonSymlinkUnsupported  = projection.ReasonSymlinkUnsupported
	ReasonFIFOUnsupported     = projection.ReasonFIFOUnsupported
	ReasonDeviceUnsupported   = projection.ReasonDeviceUnsupported
	ReasonCaseCollision       = projection.ReasonCaseCollision
	ReasonKindUnknown         = projection.ReasonKindUnknown
	ReasonNameUnrepresentable = projection.ReasonNameUnrepresentable
	ReasonResidualForeignFile = projection.ReasonResidualForeignFile
)

// mountServer is what a platform backend returns from platformMount:
// a live projection that serves until unmounted.
type mountServer interface {
	Wait()
	Unmount() error
}

// ImageMount is a live filesystem projection over an acquired image.
type ImageMount struct {
	st         *store.Store
	workDir    string
	extraDirs  []string
	server     mountServer
	img        *store.Image
	mountPoint string
	id         string
	ctx        context.Context
	platform   *v1.Platform
	upperDir   string
	upperName  string
	// upperRoot is the resolved upper the mount serves ("" for a
	// read-only mount); the named upper's one-writable-mount
	// arbitration is the upper claim lock inside claim.
	upperRoot string
	// claim is the mount's held claim (store.md held-lock
	// liveness): acquired before the registry row, held for the
	// serve, retired at deregistration.
	claim *store.MountClaim
}

// Digest returns the materialized child manifest digest served by the mount.
func (im *ImageMount) Digest() v1.Hash {
	return im.img.Hash()
}

func (im *ImageMount) ConfigFile() *v1.ConfigFile {
	return im.img.ConfigFile()
}

func (im *ImageMount) Wait() {
	im.server.Wait()
}

func (im *ImageMount) Unmount() error {
	err := im.server.Unmount()
	// Deregistration only when the mount actually stopped serving —
	// a failed unmount keeps the row and the held claim, and with
	// them the named upper's one-writable-mount refusal, in force
	// (REQ-store-mount-registry).
	if err == nil {
		if derr := im.st.DeregisterMount(context.Background(), im.id, im.claim); derr != nil {
			return derr
		}
		// Unmount is a garbage-creating transition
		// (REQ-store-gc-collect).
		im.st.AutoCollect(context.Background())
	}
	return err
}

func (im *ImageMount) MountPoint() string {
	return im.mountPoint
}

// UpperPath returns the upper directory a writable mount serves
// ("" for a read-only mount) — the path to hand to Commit.
func (im *ImageMount) UpperPath() string { return im.upperRoot }

type mountSettings struct {
	mountPoint string
	id         string
	ctx        context.Context
	platform   *v1.Platform
	upperDir   string
	upperName  string
}

// Option configures a single Mount invocation.
type Option func(*mountSettings)

func WithTargetPath(targetPath string) Option {
	return func(s *mountSettings) {
		s.mountPoint = targetPath
	}
}

func WithID(id string) Option {
	return func(s *mountSettings) {
		s.id = id
	}
}

func WithContext(ctx context.Context) Option {
	return func(s *mountSettings) {
		s.ctx = ctx
	}
}

// WithPlatform mounts an explicit platform; selection semantics
// match ocifs.PullWithPlatform.
func WithPlatform(p v1.Platform) Option {
	return func(s *mountSettings) {
		s.platform = &p
	}
}

// WithUpperDir serves the writable merged projection over a
// caller-supplied upper directory (writable.md); the upper outlives
// the mount — unmount leaves it intact for remounting or commit
// (REQ-api-mount-writable).
func WithUpperDir(dir string) Option {
	return func(s *mountSettings) {
		s.upperDir = dir
	}
}

// WithNamedUpper serves the writable merged projection over a
// store-managed upper, created on first use and bound to the mounted
// image's digest; a mount against a different base is refused
// (REQ-api-mount-writable, REQ-writable-base-binding).
func WithNamedUpper(name string) Option {
	return func(s *mountSettings) {
		s.upperName = name
	}
}

// Mount acquires imgRef through ofs and serves a filesystem projection.
func Mount(ofs *ocifs.OCIFS, imgRef string, opts ...Option) (*ImageMount, error) {
	if ofs == nil {
		return nil, errors.New("mount: nil OCIFS store")
	}
	cfg := mountSettings{ctx: context.Background()}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.ctx == nil {
		cfg.ctx = context.Background()
	}
	if cfg.upperDir != "" && cfg.upperName != "" {
		return nil, fmt.Errorf("mount takes at most one upper: WithUpperDir or WithNamedUpper")
	}

	st, workDir, extraDirs := ofs.MountState(store.Internal{})
	im := &ImageMount{
		st:         st,
		workDir:    workDir,
		extraDirs:  extraDirs,
		mountPoint: cfg.mountPoint,
		id:         cfg.id,
		ctx:        cfg.ctx,
		platform:   cfg.platform,
		upperDir:   cfg.upperDir,
		upperName:  cfg.upperName,
	}

	// Acquire the image before creating any per-mount state, so a
	// failed pull leaves no orphan under mounts/.
	img, err := st.Image(im.ctx, imgRef, im.platform)
	if err != nil {
		return nil, err
	}
	im.img = img

	// A named upper's serve claim comes FIRST — before the upper's
	// tree or base binding is touched (writable.md
	// REQ-writable-base-binding: the lock is acquired before any
	// bookkeeping write) — so a concurrent RemoveUpper can never
	// interleave with an upper being brought up. Ownership passes
	// to the registration below, which releases it on any failure.
	var upperClaim *oslock.Lock
	if im.upperName != "" {
		upperClaim, err = st.ClaimUpper(im.upperName)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if im.claim == nil && upperClaim != nil {
			upperClaim.Close()
		}
	}()

	if err := platformResolveUpper(im, img); err != nil {
		return nil, err
	}

	view, err := img.Unify()
	if err != nil {
		return nil, err
	}

	resolvedID, mountLock, err := st.ClaimMount(im.id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if mountLock != nil {
			st.AbortMountClaim(context.Background(), resolvedID, mountLock)
		}
	}()

	stateDir, mountDir, err := st.NewMountStateClaimed(resolvedID)
	if err != nil {
		return nil, err
	}
	if im.mountPoint == "" {
		im.mountPoint = mountDir
	}

	im.mountPoint = filepath.Clean(im.mountPoint)
	if !filepath.IsAbs(im.mountPoint) {
		cwd, err := os.Getwd()
		if err != nil {
			os.RemoveAll(stateDir)
			return nil, err
		}
		im.mountPoint = filepath.Clean(filepath.Join(cwd, im.mountPoint))
	}

	im.id = resolvedID
	claim, err := st.RegisterMountRecordClaimed(context.Background(), im.id, img.Hash(), im.upperName, im.mountPoint, stateDir, mountLock, upperClaim)
	mountLock = nil  // ownership transferred, success or failure
	upperClaim = nil // ownership transferred, success or failure
	if err != nil {
		return nil, err
	}
	im.claim = claim
	defer func() {
		if im.server == nil {
			// Remove stateDir while locks/mount-<id> is still held so
			// a concurrent remount of the same id cannot adopt stateDir
			// and have it deleted out from under it.
			os.RemoveAll(stateDir)
			_ = st.DeregisterMount(context.Background(), im.id, im.claim)
		}
	}()

	srv, err := platformMount(im, imgRef, img, view, im.id, stateDir, im.mountPoint, im.upperRoot)
	if err != nil {
		return nil, err
	}
	im.server = srv

	return im, nil
}

// ReportOf reads a mount's projection report from its bookkeeping
// record (projection.md REQ-proj-report). The second return reports
// whether the report has been published (store.md
// REQ-store-bookkeeping): false means the mount is registered but
// its projection has not yet published.
func ReportOf(ofs *ocifs.OCIFS, id string) (Report, bool, error) {
	if ofs == nil {
		return Report{}, false, errors.New("mount: nil OCIFS store")
	}
	st, _, _ := ofs.MountState(store.Internal{})
	rec, err := st.MountRecord(context.Background(), id)
	if err != nil {
		return Report{}, false, err
	}
	return rec.Report, rec.Published, nil
}
