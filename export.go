package ocifs

import (
	"context"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type ExportOption func(*exportReq)

type exportReq struct {
	target   string
	platform *v1.Platform
}

// ExportWithTargetPath exports into the caller's own directory,
// which must not exist yet, instead of the store-managed export
// cache.
var ExportWithTargetPath = func(dir string) ExportOption {
	return func(r *exportReq) {
		r.target = dir
	}
}

// ExportWithPlatform exports an explicit platform; selection
// semantics match PullWithPlatform.
var ExportWithPlatform = func(p v1.Platform) ExportOption {
	return func(r *exportReq) {
		r.platform = &p
	}
}

// Export materializes imageRef's unified view into a real directory
// tree per docs/specs/export.md — acquisition follows the pull
// policy and runs the verification seam like any acquisition — and
// returns the export root: the caller's target when one was given,
// otherwise the store-managed cache entry for the materialized
// manifest digest, served as-is when it already exists. Cached
// exports are shared: treat them as read-only.
func (o *OCIFS) Export(ctx context.Context, imageRef string, opts ...ExportOption) (string, error) {
	var r exportReq
	for _, opt := range opts {
		opt(&r)
	}
	img, err := o.store.Image(ctx, imageRef, r.platform)
	if err != nil {
		return "", err
	}
	if r.target == "" {
		return o.newImage(img).Export(ctx)
	}
	return o.newImage(img).ExportTo(ctx, r.target)
}

// Export materializes this image's unified view into the
// store-managed export cache and returns the export root (served
// as-is when the entry already exists, REQ-export-cache). The image
// was resolved and verified when it was acquired; nothing is
// resolved or verified again (REQ-api-export). The handle is a
// snapshot of that acquisition: content collected from under it
// since fails the export rather than being fetched again.
func (i *Image) Export(ctx context.Context) (string, error) {
	return i.ofs.store.Export(ctx, i.img)
}

// ExportTo materializes this image's unified view into the caller's
// own directory, which must not exist yet (REQ-export-atomic), and
// returns that directory. As for Export, the acquisition that
// produced the image — a pull or a commit — is the only one.
func (i *Image) ExportTo(ctx context.Context, dir string) (string, error) {
	view, err := i.img.Unify()
	if err != nil {
		return "", err
	}
	if err := i.ofs.store.ExportTo(ctx, view, dir, i.img.Hash()); err != nil {
		return "", err
	}
	return dir, nil
}
