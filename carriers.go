package ocifs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/greatliontech/ocifs/internal/store"
)

const (
	sigstoreBundleMediaType = "application/vnd.dev.sigstore.bundle.v0.3+json"
	sigstorePredicateKey    = "dev.sigstore.bundle.predicateType"
	cosignSignPredicate     = "https://sigstore.dev/cosign/sign/v1"
	legacySigConfigType     = "application/vnd.dev.cosign.artifact.sig.v1+json"
	simpleSigningMediaType  = "application/vnd.dev.cosign.simplesigning.v1+json"
	annotationSignature     = "dev.cosignproject.cosign/signature"
	annotationCertificate   = "dev.sigstore.cosign/certificate"
	annotationChain         = "dev.sigstore.cosign/chain"
	annotationRekorBundle   = "dev.sigstore.cosign/bundle"
	annotationTimestamp     = "dev.sigstore.cosign/rfc3161timestamp"
	signatureTagSuffix      = ".sig"
)

// MaxCarrierBytes bounds the byte size of any single discovered
// signature carrier layer (4 MiB).
const MaxCarrierBytes = 4 << 20

// ErrRejectedCarrier classifies a discovered signature carrier whose
// structure or size violates the carrier format (such as an invalid
// referrer digest, an unexpected index manifest, a Sigstore bundle
// referrer with other than one layer, or a carrier exceeding
// MaxCarrierBytes), distinguishing malformed carrier evidence from
// registry transport failures.
var ErrRejectedCarrier = errors.New("ocifs: malformed or oversized signature carrier")

// CarrierKind identifies the wire format of a discovered OCI signature
// carrier.
type CarrierKind uint8

const (
	// CarrierSigstoreBundle is an OCI referrer carrying a Sigstore
	// bundle JSON document (application/vnd.dev.sigstore.bundle.v0.3+json).
	CarrierSigstoreBundle CarrierKind = iota + 1
	// CarrierSimpleSigning is a cosign simple-signing layer with its
	// signature, certificate, chain, Rekor bundle, and RFC 3161
	// timestamp annotations.
	CarrierSimpleSigning
)

// SimpleSigning holds the payload bytes and envelope annotations of a
// cosign simple-signing layer.
type SimpleSigning struct {
	Payload          []byte
	Signature        string
	Certificate      string
	Chain            string
	RekorBundle      string
	RFC3161Timestamp string
}

// Carrier is one signature carrier discovered for an OCI artifact
// digest.
type Carrier struct {
	Where         string
	Kind          CarrierKind
	BundleJSON    []byte
	SimpleSigning *SimpleSigning
}

// DiscoverCarriers yields signature carriers for digest from the
// repository named by imageRefOrRepo (either a full tag/digest
// reference or a repository path), using the store's configured
// transport and ContextKeychain (api.md REQ-api-carriers).
func (o *OCIFS) DiscoverCarriers(ctx context.Context, imageRefOrRepo string, digest v1.Hash) iter.Seq2[Carrier, error] {
	if o == nil {
		return singleCarrierError(errors.New("ocifs: nil OCIFS store"))
	}
	repo, err := parseCarrierRepository(imageRefOrRepo)
	if err != nil {
		return singleCarrierError(err)
	}
	return DiscoverCarriers(ctx, repo, digest, o.store.RemoteOpts(ctx)...)
}

func parseCarrierRepository(raw string) (name.Repository, error) {
	if strings.TrimSpace(raw) == "" {
		return name.Repository{}, errors.New("ocifs: empty repository or image reference")
	}
	if ref, err := name.ParseReference(raw); err == nil {
		return ref.Context(), nil
	}
	repo, err := name.NewRepository(raw)
	if err != nil {
		return name.Repository{}, fmt.Errorf("ocifs: invalid repository %q: %w", raw, err)
	}
	return repo, nil
}

func singleCarrierError(err error) iter.Seq2[Carrier, error] {
	return func(yield func(Carrier, error) bool) {
		yield(Carrier{}, err)
	}
}

// DiscoverCarriers yields signature carriers for digest from repo in
// deterministic order — Sigstore bundle referrers (sorted by referrer
// digest), legacy cosign signature referrers (sorted by referrer
// digest, layers in manifest order), and finally the legacy
// `<algorithm>-<hex>.sig` signature tag's layers — fetching each
// lazily so an existential verifier stops at the first accepted
// carrier without downloading later ones (api.md REQ-api-carriers).
func DiscoverCarriers(ctx context.Context, repo name.Repository, digest v1.Hash, opts ...remote.Option) iter.Seq2[Carrier, error] {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.EqualFold(repo.RegistryStr(), store.LocalRegistry) {
		return singleCarrierError(fmt.Errorf("ocifs: %s uses reserved local namespace %s", repo.Name(), store.LocalRegistry))
	}
	if digest.Algorithm == "" || digest.Hex == "" {
		return singleCarrierError(fmt.Errorf("ocifs: invalid digest %q", digest))
	}
	nameOpts := repoNameOpts(repo)
	subjectRef, err := name.NewDigest(repo.Name()+"@"+digest.String(), nameOpts...)
	if err != nil {
		return singleCarrierError(fmt.Errorf("ocifs: invalid digest %q: %w", digest, err))
	}
	opts = append(slices.Clone(opts), remote.WithContext(ctx))
	return func(yield func(Carrier, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(Carrier{}, err)
			return
		}
		idx, err := remote.Referrers(subjectRef, opts...)
		if err != nil {
			yield(Carrier{}, fmt.Errorf("discover: referrers of %s: %w", digest, err))
			return
		}
		im, err := idx.IndexManifest()
		if err != nil {
			yield(Carrier{}, fmt.Errorf("discover: referrers of %s: %w", digest, err))
			return
		}
		referrers := slices.Clone(im.Manifests)
		slices.SortFunc(referrers, func(a, b v1.Descriptor) int {
			return strings.Compare(a.Digest.String(), b.Digest.String())
		})

		for _, d := range referrers {
			if err := ctx.Err(); err != nil {
				yield(Carrier{}, err)
				return
			}
			if d.ArtifactType != sigstoreBundleMediaType {
				continue
			}
			if pred, ok := d.Annotations[sigstorePredicateKey]; ok && pred != cosignSignPredicate {
				continue
			}
			c, evidence, err := fetchBundleReferrer(repo, d, opts)
			if !evidence {
				continue
			}
			if !yield(c, err) {
				return
			}
			if err != nil && !errors.Is(err, ErrRejectedCarrier) {
				return
			}
		}

		for _, d := range referrers {
			if err := ctx.Err(); err != nil {
				yield(Carrier{}, err)
				return
			}
			if d.ArtifactType != legacySigConfigType {
				continue
			}
			where := "referrer " + d.Digest.String()
			refDigest, err := parseReferrerDigest(repo, d.Digest)
			if err != nil {
				if !yield(Carrier{}, fmt.Errorf("discover: %s: %w", where, err)) {
					return
				}
				continue
			}
			img, m, present, err := fetchCarrierImage(refDigest, opts)
			if err != nil {
				if !yield(Carrier{}, fmt.Errorf("discover: %s: %w", where, err)) {
					return
				}
				if !errors.Is(err, ErrRejectedCarrier) {
					return
				}
				continue
			}
			if !present {
				continue
			}
			if !yieldSimpleSigningLayers(ctx, img, m, where, yield) {
				return
			}
		}

		if err := ctx.Err(); err != nil {
			yield(Carrier{}, err)
			return
		}
		tagStr := digest.Algorithm + "-" + digest.Hex + signatureTagSuffix
		sigTag, err := name.NewTag(repo.Name()+":"+tagStr, nameOpts...)
		if err != nil {
			// Overlong non-sha256 digests (e.g. sha512:<128hex> -> 139
			// chars) cannot form a valid 128-char OCI tag, so no .sig
			// fallback tag can exist in any compliant registry.
			return
		}
		where := "tag " + sigTag.TagStr()
		img, m, present, err := fetchCarrierImage(sigTag, opts)
		if err != nil {
			yield(Carrier{}, fmt.Errorf("discover: %s: %w", where, err))
			return
		}
		if !present {
			return
		}
		yieldSimpleSigningLayers(ctx, img, m, where, yield)
	}
}

func repoNameOpts(repo name.Repository) []name.Option {
	if repo.Scheme() == "http" {
		return []name.Option{name.StrictValidation, name.Insecure}
	}
	return []name.Option{name.StrictValidation}
}

func parseReferrerDigest(repo name.Repository, d v1.Hash) (name.Digest, error) {
	if d.Algorithm == "" || d.Hex == "" {
		return name.Digest{}, fmt.Errorf("empty referrer digest %q: %w", d, ErrRejectedCarrier)
	}
	ref, err := name.NewDigest(repo.Name()+"@"+d.String(), repoNameOpts(repo)...)
	if err != nil {
		return name.Digest{}, fmt.Errorf("invalid referrer digest %q: %w: %v", d, ErrRejectedCarrier, err)
	}
	return ref, nil
}

// fetchCarrierImage fetches an optional carrier image manifest,
// treating 404/MANIFEST_UNKNOWN as absent (present == false, err == nil)
// and classifying structural manifest violations (index media types,
// corrupt manifest JSON, missing config descriptor) as ErrRejectedCarrier
// so malformed carriers never poison subsequent valid carriers.
func fetchCarrierImage(ref name.Reference, opts []remote.Option) (v1.Image, *v1.Manifest, bool, error) {
	desc, err := remote.Get(ref, opts...)
	if isRegistryNotFound(err) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	if desc.MediaType.IsIndex() {
		return nil, nil, true, fmt.Errorf("unexpected index media type %q: %w", desc.MediaType, ErrRejectedCarrier)
	}
	img, err := desc.Image()
	if err != nil {
		return nil, nil, true, fmt.Errorf("invalid carrier image: %w: %v", ErrRejectedCarrier, err)
	}
	m, err := img.Manifest()
	if err != nil {
		return nil, nil, true, fmt.Errorf("invalid carrier manifest: %w: %v", ErrRejectedCarrier, err)
	}
	if m.Config.Digest == (v1.Hash{}) {
		return nil, nil, true, fmt.Errorf("carrier manifest missing config descriptor: %w", ErrRejectedCarrier)
	}
	return img, m, true, nil
}

func fetchBundleReferrer(repo name.Repository, d v1.Descriptor, opts []remote.Option) (Carrier, bool, error) {
	where := "referrer " + d.Digest.String()
	refDigest, err := parseReferrerDigest(repo, d.Digest)
	if err != nil {
		return Carrier{}, true, fmt.Errorf("discover: %s: %w", where, err)
	}
	img, m, present, err := fetchCarrierImage(refDigest, opts)
	if err != nil {
		return Carrier{}, true, fmt.Errorf("discover: %s: %w", where, err)
	}
	if !present {
		return Carrier{}, false, nil
	}
	if m.Annotations[sigstorePredicateKey] != cosignSignPredicate {
		return Carrier{}, false, nil
	}
	if len(m.Layers) != 1 {
		return Carrier{}, true, fmt.Errorf("discover: %s: %d layers, want 1: %w", where, len(m.Layers), ErrRejectedCarrier)
	}
	if m.Layers[0].Size < 0 || m.Layers[0].Size > MaxCarrierBytes {
		return Carrier{}, true, fmt.Errorf("discover: %s: layer size %d out of bounds [0, %d]: %w", where, m.Layers[0].Size, MaxCarrierBytes, ErrRejectedCarrier)
	}
	layers, err := img.Layers()
	if err != nil {
		return Carrier{}, true, fmt.Errorf("discover: %s: %w: %v", where, ErrRejectedCarrier, err)
	}
	if len(layers) != 1 {
		return Carrier{}, true, fmt.Errorf("discover: %s: %d layers, want 1: %w", where, len(layers), ErrRejectedCarrier)
	}
	raw, err := readBoundedCarrierLayer(layers[0])
	if err != nil {
		return Carrier{}, true, fmt.Errorf("discover: %s: %w", where, err)
	}
	return Carrier{
		Where:      where,
		Kind:       CarrierSigstoreBundle,
		BundleJSON: raw,
	}, true, nil
}

func yieldSimpleSigningLayers(ctx context.Context, img v1.Image, m *v1.Manifest, where string, yield func(Carrier, error) bool) bool {
	layers, err := img.Layers()
	if err != nil {
		return yield(Carrier{}, fmt.Errorf("discover: %s: %w: %v", where, ErrRejectedCarrier, err))
	}
	if len(layers) != len(m.Layers) {
		return yield(Carrier{}, fmt.Errorf("discover: %s: %d layers for %d descriptors: %w", where, len(layers), len(m.Layers), ErrRejectedCarrier))
	}
	for i, d := range m.Layers {
		if err := ctx.Err(); err != nil {
			yield(Carrier{}, err)
			return false
		}
		if string(d.MediaType) != simpleSigningMediaType {
			continue
		}
		at := fmt.Sprintf("%s layer %d", where, i)
		if d.Size < 0 || d.Size > MaxCarrierBytes {
			if !yield(Carrier{}, fmt.Errorf("discover: %s: layer size %d out of bounds [0, %d]: %w", at, d.Size, MaxCarrierBytes, ErrRejectedCarrier)) {
				return false
			}
			continue
		}
		payload, err := readBoundedCarrierLayer(layers[i])
		if err != nil {
			if !yield(Carrier{}, fmt.Errorf("discover: %s: %w", at, err)) {
				return false
			}
			if !errors.Is(err, ErrRejectedCarrier) {
				return false
			}
			continue
		}
		c := Carrier{
			Where: at,
			Kind:  CarrierSimpleSigning,
			SimpleSigning: &SimpleSigning{
				Payload:          payload,
				Signature:        d.Annotations[annotationSignature],
				Certificate:      d.Annotations[annotationCertificate],
				Chain:            d.Annotations[annotationChain],
				RekorBundle:      d.Annotations[annotationRekorBundle],
				RFC3161Timestamp: d.Annotations[annotationTimestamp],
			},
		}
		if !yield(c, nil) {
			return false
		}
	}
	return true
}

func readBoundedCarrierLayer(l v1.Layer) ([]byte, error) {
	rc, err := l.Compressed()
	if err != nil {
		if isMissingCarrierBlob(err) {
			return nil, fmt.Errorf("missing carrier layer blob: %w: %v", ErrRejectedCarrier, err)
		}
		return nil, err
	}
	defer rc.Close()
	raw, err := io.ReadAll(io.LimitReader(rc, MaxCarrierBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxCarrierBytes {
		return nil, fmt.Errorf("carrier exceeds %d bytes: %w", MaxCarrierBytes, ErrRejectedCarrier)
	}
	return raw, nil
}

func isMissingCarrierBlob(err error) bool {
	if err == nil {
		return false
	}
	var te *transport.Error
	if !errors.As(err, &te) {
		return false
	}
	if te.StatusCode == http.StatusNotFound {
		return true
	}
	for _, d := range te.Errors {
		if d.Code == transport.BlobUnknownErrorCode {
			return true
		}
	}
	return false
}

func isRegistryNotFound(err error) bool {
	if err == nil {
		return false
	}
	var te *transport.Error
	if !errors.As(err, &te) {
		return false
	}
	if te.StatusCode == http.StatusNotFound {
		return true
	}
	for _, d := range te.Errors {
		if d.Code == transport.ManifestUnknownErrorCode || d.Code == transport.NameUnknownErrorCode {
			return true
		}
	}
	return false
}
