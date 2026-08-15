//go:build linux || darwin

package ocifs

import (
	"context"
	"fmt"
)

// Commit serializes an upper's canonical diff over its base image
// into a new image in the store (docs/specs/writable.md
// REQ-writable-commit-image): the base resolves per the pull policy
// with the verification seam governing like any acquisition, the
// upper is read from disk — no live mount is involved — and the
// result is returned materialized, acquirable at
// LocalRef(result.Digest()). The returned image is re-acquired
// through the standard acquisition path, so a configured verifier
// runs a second time against the committed image's ocifs.local
// identity — a verifier that only accepts registry-anchored
// identities will reject every commit result. Committing an upper
// under concurrent writes yields a point-in-time result; quiescence
// is the caller's to arrange.
func (o *OCIFS) Commit(ctx context.Context, baseRef string, opts ...CommitOption) (*Image, error) {
	var r commitReq
	for _, opt := range opts {
		opt(&r)
	}
	if (r.upperDir == "") == (r.upperName == "") {
		return nil, fmt.Errorf("commit needs exactly one upper: CommitWithUpperDir or CommitWithNamedUpper")
	}
	base, err := o.store.Image(ctx, baseRef, r.platform)
	if err != nil {
		return nil, err
	}
	var digest = base.Hash()
	if r.upperName != "" {
		digest, err = o.store.CommitNamedUpper(base, r.upperName)
	} else {
		digest, err = o.store.CommitUpper(base, r.upperDir)
	}
	if err != nil {
		return nil, err
	}
	img, err := o.store.Image(ctx, LocalRef(digest), nil)
	if err != nil {
		return nil, fmt.Errorf("committed image %s does not materialize: %w", digest, err)
	}
	return &Image{img: img}, nil
}
