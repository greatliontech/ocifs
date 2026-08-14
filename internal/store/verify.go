package store

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ResolvedIdentity is the verification seam's input
// (docs/specs/verification-seam.md REQ-seam-input): the reference as
// the consumer requested it, the top-level digest it resolved to,
// and the top-level artifact's bytes. For a multi-platform image the
// bytes are the image index — the artifact signatures are made over;
// platform selection happens only after the seam passes, so a
// verifier targeting per-platform children derives their digests
// from these bytes.
type ResolvedIdentity struct {
	// Reference is the reference string as requested — tag or digest
	// form, unresolved and unnormalized.
	Reference string
	// Digest is the resolved top-level digest.
	Digest v1.Hash
	// Artifact is the top-level artifact's bytes as retained: an
	// image index for a multi-platform image, otherwise the image
	// manifest. Network-fetched bytes were digest-verified on
	// arrival, but the store's integrity boundary is the local
	// filesystem (store.md REQ-store-ingest-verified) — a verifier
	// whose policy requires digest–byte correspondence hashes
	// Artifact itself.
	Artifact []byte
}

// Verifier is the consumer-supplied verification hook
// (docs/specs/verification-seam.md): it judges a resolved identity
// against the consumer's trust policy. ocifs never ships an
// implementation and never interprets the policy — a nil Verifier
// means every resolvable image is served (REQ-seam-optional). It
// runs on every acquisition request, cached or not; caching verdicts
// is the verifier's own concern. A non-nil error aborts the request
// (REQ-seam-abort).
type Verifier func(ctx context.Context, id ResolvedIdentity) error

// VerificationError reports a Verifier rejection, distinguishing
// verification failure from resolution failure (REQ-seam-abort):
// errors.As against *VerificationError matches exactly the requests
// a verifier rejected. Err is the verifier's own error, reachable
// through Unwrap.
type VerificationError struct {
	// Reference is the rejected request's reference as requested.
	Reference string
	// Digest is the resolved top-level digest the verifier rejected.
	Digest v1.Hash
	// Err is the verifier's error.
	Err error
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("image %s (resolved to %s) failed verification: %v", e.Reference, e.Digest, e.Err)
}

func (e *VerificationError) Unwrap() error { return e.Err }

// verify runs the seam for one request after top-level resolution
// and before any materialization for the request
// (REQ-seam-position). The top-level artifact is read from the
// retained oci/ tier: tag-form network resolutions retain it in
// resolveTop, cached content's copy is retained by ingest order, and
// the fetch branch below covers the rest — a digest-form request's
// first contact as much as a damaged retained copy — by digest under
// the ingest lock.
func (s *Store) verify(ctx context.Context, req request, top v1.Hash) error {
	if s.verifier == nil {
		return nil
	}
	raw, err := s.ensureManifest(ctx, nil, top)
	if errors.Is(err, errIncomplete) {
		f := &fetcher{store: s, repo: req.ref.Context(), allowed: s.pullPolicy != PullNever}
		s.ingestMu.Lock()
		raw, err = s.ensureManifest(ctx, f, top)
		s.ingestMu.Unlock()
	}
	if err != nil {
		return err
	}
	if err := s.verifier(ctx, ResolvedIdentity{Reference: req.ref.String(), Digest: top, Artifact: raw}); err != nil {
		return &VerificationError{Reference: req.ref.String(), Digest: top, Err: err}
	}
	return nil
}
