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
		return o.store.Export(img)
	}
	view, err := img.Unify()
	if err != nil {
		return "", err
	}
	if err := o.store.ExportTo(view, r.target); err != nil {
		return "", err
	}
	return r.target, nil
}
