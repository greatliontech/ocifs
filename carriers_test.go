package ocifs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

var (
	testManifestPath  = regexp.MustCompile(`^/v2/(.+)/manifests/([^/]+)$`)
	testReferrersPath = regexp.MustCompile(`^/v2/(.+)/referrers/([^/]+)$`)
	testFallbackTag   = regexp.MustCompile(`^sha256-[0-9a-f]{64}$`)
)

type testReferrersRegistry struct {
	inner   http.Handler
	api     bool
	sigHits *atomic.Int64
	mu      sync.Mutex
	by      map[string][]v1.Descriptor
}

func newTestReferrersRegistry(api bool, sigHits *atomic.Int64) http.Handler {
	return &testReferrersRegistry{
		inner:   registry.New(registry.Logger(log.New(io.Discard, "", 0))),
		api:     api,
		sigHits: sigHits,
		by:      map[string][]v1.Descriptor{},
	}
}

func (r *testReferrersRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasSuffix(req.URL.Path, ".sig") && req.Method != http.MethodPut {
		r.sigHits.Add(1)
	}
	if m := testReferrersPath.FindStringSubmatch(req.URL.Path); m != nil && req.Method == http.MethodGet && r.api {
		r.serveIndex(w, m[1]+"@"+m[2], false)
		return
	}
	m := testManifestPath.FindStringSubmatch(req.URL.Path)
	if m != nil && !r.api && testFallbackTag.MatchString(m[2]) {
		key := m[1] + "@sha256:" + strings.TrimPrefix(m[2], "sha256-")
		switch req.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
			return
		case http.MethodGet, http.MethodHead:
			r.mu.Lock()
			has := len(r.by[key]) > 0
			r.mu.Unlock()
			if has {
				r.serveIndex(w, key, true)
				return
			}
		}
	}
	if m == nil || req.Method != http.MethodPut {
		r.inner.ServeHTTP(w, req)
		return
	}
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	var mf struct {
		ArtifactType string                     `json:"artifactType"`
		Config       struct{ MediaType string } `json:"config"`
		Subject      *struct{ Digest string }   `json:"subject"`
		Annotations  map[string]string          `json:"annotations"`
	}
	_ = json.Unmarshal(raw, &mf)
	if mf.Subject == nil {
		r.inner.ServeHTTP(w, req)
		return
	}
	sum := sha256.Sum256(raw)
	artType := mf.ArtifactType
	if artType == "" {
		artType = mf.Config.MediaType
	}
	desc := v1.Descriptor{
		MediaType:    types.MediaType(req.Header.Get("Content-Type")),
		Size:         int64(len(raw)),
		Digest:       v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])},
		ArtifactType: artType,
		Annotations:  mf.Annotations,
	}
	if r.api {
		w.Header().Set("OCI-Subject", mf.Subject.Digest)
	}
	r.inner.ServeHTTP(w, req)
	key := m[1] + "@" + mf.Subject.Digest
	r.mu.Lock()
	r.by[key] = append(r.by[key], desc)
	r.mu.Unlock()
}

func (r *testReferrersRegistry) serveIndex(w http.ResponseWriter, key string, stripAnnotations bool) {
	r.mu.Lock()
	list := append([]v1.Descriptor(nil), r.by[key]...)
	r.mu.Unlock()
	if list == nil {
		list = []v1.Descriptor{}
	}
	if stripAnnotations {
		for i := range list {
			list[i].Annotations = nil
		}
	}
	body, _ := json.Marshal(v1.IndexManifest{SchemaVersion: 2, MediaType: types.OCIImageIndex, Manifests: list})
	w.Header().Set("Content-Type", string(types.OCIImageIndex))
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

type rawTestManifest struct {
	raw  []byte
	desc v1.Descriptor
}

func (m *rawTestManifest) RawManifest() ([]byte, error)        { return m.raw, nil }
func (m *rawTestManifest) MediaType() (types.MediaType, error) { return m.desc.MediaType, nil }
func (m *rawTestManifest) Digest() (v1.Hash, error)            { return m.desc.Digest, nil }
func (m *rawTestManifest) Size() (int64, error)                { return m.desc.Size, nil }

func putRawReferrer(t *testing.T, repo name.Repository, subject v1.Descriptor, artifactType, configMediaType string, anns map[string]string, layers []v1.Descriptor, blobs []v1.Layer, tag string) v1.Hash {
	t.Helper()
	cfgLayer := static.NewLayer([]byte("{}"), types.MediaType(configMediaType))
	cfgDigest, _ := cfgLayer.Digest()
	cfgSize, _ := cfgLayer.Size()
	blobs = append([]v1.Layer{cfgLayer}, blobs...)
	for _, b := range blobs {
		if err := remote.WriteLayer(repo, b); err != nil {
			t.Fatal(err)
		}
	}
	mf := &v1.Manifest{
		SchemaVersion: 2,
		MediaType:     types.OCIManifestSchema1,
		ArtifactType:  artifactType,
		Config:        v1.Descriptor{MediaType: types.MediaType(configMediaType), Size: cfgSize, Digest: cfgDigest},
		Layers:        layers,
		Annotations:   anns,
	}
	if subject.Digest != (v1.Hash{}) {
		s := subject
		mf.Subject = &s
	}
	raw, err := json.Marshal(mf)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	h := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
	rm := &rawTestManifest{
		raw:  raw,
		desc: v1.Descriptor{MediaType: types.OCIManifestSchema1, Size: int64(len(raw)), Digest: h},
	}
	var ref name.Reference = repo.Digest(h.String())
	if tag != "" {
		ref = repo.Tag(tag)
	}
	if err := remote.Put(ref, rm); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestDiscoverCarriersDeterministicOrderLazyFetchAndBounds(t *testing.T) {
	for _, mode := range []struct {
		name string
		api  bool
	}{
		{name: "referrers-api", api: true},
		{name: "fallback-tag", api: false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			var sigTagHits atomic.Int64
			srv := httptest.NewServer(newTestReferrersRegistry(mode.api, &sigTagHits))
			t.Cleanup(srv.Close)

			u, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			repoStr := u.Host + "/team/plugin"
			repo, err := name.NewRepository(repoStr)
			if err != nil {
				t.Fatal(err)
			}

			subjectImg := testPlatformImage(t, v1.Platform{OS: "linux", Architecture: "amd64"}, "bin/p", "hello")
			subjectDigest, err := subjectImg.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if err := remote.Write(repo.Tag("v1.0.0"), subjectImg); err != nil {
				t.Fatal(err)
			}
			subjectRaw, err := subjectImg.RawManifest()
			if err != nil {
				t.Fatal(err)
			}
			subjectDesc := v1.Descriptor{
				MediaType: types.OCIManifestSchema1,
				Size:      int64(len(subjectRaw)),
				Digest:    subjectDigest,
			}

			pushBundle := func(bundleJSON, predicate string, count int) v1.Hash {
				var descs []v1.Descriptor
				var blobs []v1.Layer
				for range count {
					b := static.NewLayer([]byte(bundleJSON), types.MediaType(sigstoreBundleMediaType))
					bh, _ := b.Digest()
					bs, _ := b.Size()
					descs = append(descs, v1.Descriptor{MediaType: types.MediaType(sigstoreBundleMediaType), Size: bs, Digest: bh})
					blobs = append(blobs, b)
				}
				return putRawReferrer(
					t, repo, subjectDesc,
					sigstoreBundleMediaType, "application/vnd.oci.empty.v1+json",
					map[string]string{sigstorePredicateKey: predicate},
					descs, blobs, "",
				)
			}

			bundleA := pushBundle(`{"bundle":"alpha"}`, cosignSignPredicate, 1)
			bundleB := pushBundle(`{"bundle":"beta"}`, cosignSignPredicate, 1)
			_ = pushBundle(`{"bundle":"slsa"}`, "https://slsa.dev/provenance/v1", 1)
			badBundle := pushBundle(`{"bundle":"two-layers"}`, cosignSignPredicate, 2)

			// Legacy signature referrer.
			refPayload := static.NewLayer([]byte(`{"critical":"ref"}`), types.MediaType(simpleSigningMediaType))
			rph, _ := refPayload.Digest()
			rps, _ := refPayload.Size()
			_ = putRawReferrer(
				t, repo, subjectDesc,
				"", legacySigConfigType, nil,
				[]v1.Descriptor{{
					MediaType: types.MediaType(simpleSigningMediaType),
					Size:      rps,
					Digest:    rph,
					Annotations: map[string]string{
						annotationSignature:   "sig-ref",
						annotationCertificate: "cert-ref",
					},
				}},
				[]v1.Layer{refPayload}, "",
			)

			// Legacy .sig tag with an oversized first layer, a non-signature layer, and a valid third layer.
			oversized := static.NewLayer(bytes.Repeat([]byte("Z"), MaxCarrierBytes+16), types.MediaType(simpleSigningMediaType))
			ovh, _ := oversized.Digest()
			ovs, _ := oversized.Size()
			ignored := static.NewLayer([]byte("other"), types.OCIUncompressedLayer)
			igh, _ := ignored.Digest()
			igs, _ := ignored.Size()
			tagPayload := static.NewLayer([]byte(`{"critical":"tag"}`), types.MediaType(simpleSigningMediaType))
			tph, _ := tagPayload.Digest()
			tps, _ := tagPayload.Size()
			sigTagName := subjectDigest.Algorithm + "-" + subjectDigest.Hex + signatureTagSuffix
			_ = putRawReferrer(
				t, repo, v1.Descriptor{},
				"", "application/vnd.oci.image.config.v1+json", nil,
				[]v1.Descriptor{
					{
						MediaType:   types.MediaType(simpleSigningMediaType),
						Size:        ovs,
						Digest:      ovh,
						Annotations: map[string]string{annotationSignature: "sig-too-big"},
					},
					{
						MediaType: types.OCIUncompressedLayer,
						Size:      igs,
						Digest:    igh,
					},
					{
						MediaType: types.MediaType(simpleSigningMediaType),
						Size:      tps,
						Digest:    tph,
						Annotations: map[string]string{
							annotationSignature: "sig-tag-ok",
							annotationChain:     "chain-pem",
						},
					},
				},
				[]v1.Layer{oversized, ignored, tagPayload}, sigTagName,
			)

			ofs, err := New(WithWorkDir(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer ofs.Close()

			// 1. Lazy early stop: stopping at the first valid bundle carrier must NOT fetch .sig tag.
			sigTagHits.Store(0)
			gotFirst := false
			for c, cerr := range ofs.DiscoverCarriers(context.Background(), repoStr+":v1.0.0", subjectDigest) {
				if cerr != nil {
					if errors.Is(cerr, ErrRejectedCarrier) {
						continue
					}
					t.Fatal(cerr)
				}
				if c.Kind != CarrierSigstoreBundle {
					t.Fatalf("first valid carrier kind = %v, want CarrierSigstoreBundle", c.Kind)
				}
				gotFirst = true
				break
			}
			if !gotFirst {
				t.Fatal("expected at least one valid bundle carrier")
			}
			if sigTagHits.Load() != 0 {
				t.Fatalf("early stop still queried .sig tag (%d hits)", sigTagHits.Load())
			}

			// 2. Full traversal: deterministic digest order for bundles, rejected 2-layer bundle
			//    and oversized .sig layer surfaced as ErrRejectedCarrier without poisoning valid siblings.
			var valid []Carrier
			var rejected int
			for c, cerr := range ofs.DiscoverCarriers(context.Background(), repoStr, subjectDigest) {
				if cerr != nil {
					if errors.Is(cerr, ErrRejectedCarrier) {
						rejected++
						continue
					}
					t.Fatalf("unexpected transport error: %v", cerr)
				}
				valid = append(valid, c)
			}
			if rejected != 2 {
				t.Fatalf("rejected carriers = %d, want 2 (2-layer bundle %s + oversized .sig layer)", rejected, badBundle)
			}
			if len(valid) != 4 {
				t.Fatalf("valid carriers = %d (%+v), want 4", len(valid), valid)
			}

			wantFirstBundle := `{"bundle":"alpha"}`
			wantSecondBundle := `{"bundle":"beta"}`
			if bundleB.String() < bundleA.String() {
				wantFirstBundle, wantSecondBundle = wantSecondBundle, wantFirstBundle
			}
			if string(valid[0].BundleJSON) != wantFirstBundle || string(valid[1].BundleJSON) != wantSecondBundle {
				t.Fatalf("bundle order = [%s, %s], want [%s, %s]", valid[0].BundleJSON, valid[1].BundleJSON, wantFirstBundle, wantSecondBundle)
			}
			if valid[2].Kind != CarrierSimpleSigning || valid[2].SimpleSigning.Signature != "sig-ref" {
				t.Fatalf("third carrier = %+v, want legacy referrer sig-ref", valid[2])
			}
			if valid[3].Kind != CarrierSimpleSigning || valid[3].SimpleSigning.Signature != "sig-tag-ok" || valid[3].SimpleSigning.Chain != "chain-pem" {
				t.Fatalf("fourth carrier = %+v, want legacy .sig tag sig-tag-ok", valid[3])
			}
		})
	}
}

type failOnDialTransport struct {
	t *testing.T
}

func (f failOnDialTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.t.Fatalf("unexpected network round trip to %s", req.URL)
	return nil, errors.New("unexpected network round trip")
}

type rewriteHostTransport struct {
	t          *testing.T
	wantScheme string
	targetHost string
	base       http.RoundTripper
}

func (rt rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != rt.wantScheme {
		return nil, errors.New("test registry speaks " + rt.wantScheme + " only, not " + req.URL.Scheme)
	}
	cloned := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = "http"
	u.Host = rt.targetHost
	cloned.URL = &u
	cloned.Host = rt.targetHost
	base := rt.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(cloned)
}

func TestDiscoverCarriersRejectsMalformedInputsWithoutPanicOrPoisoning(t *testing.T) {
	// 1. Reserved ocifs.local namespace must fail before dialing.
	ofsLocal, err := New(WithWorkDir(t.TempDir()), WithTransport(failOnDialTransport{t: t}))
	if err != nil {
		t.Fatal(err)
	}
	defer ofsLocal.Close()

	validHash := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}
	var gotLocalErr error
	for _, cerr := range ofsLocal.DiscoverCarriers(context.Background(), "ocifs.local/commits:v1", validHash) {
		gotLocalErr = cerr
	}
	if gotLocalErr == nil || !strings.Contains(gotLocalErr.Error(), "ocifs.local") {
		t.Fatalf("ocifs.local DiscoverCarriers = %v, want reserved local namespace error", gotLocalErr)
	}

	// 2. Invalid or unsupported caller digests (short sha256 or sha512) must return
	//    errors cleanly without panicking or dialing.
	repoValid, err := name.NewRepository("ghcr.io/org/plugin")
	if err != nil {
		t.Fatal(err)
	}
	for _, badSubj := range []v1.Hash{
		{Algorithm: "sha256", Hex: "abcd"},
		{Algorithm: "sha512", Hex: strings.Repeat("e", 128)},
	} {
		var badDigestErr error
		for _, cerr := range DiscoverCarriers(context.Background(), repoValid, badSubj, remote.WithTransport(failOnDialTransport{t: t})) {
			badDigestErr = cerr
		}
		if badDigestErr == nil {
			t.Fatalf("bad subject digest %s succeeded unexpectedly", badSubj)
		}
	}

	// 3. Malformed referrers (bad digest, OCI index carrier, corrupt manifest JSON,
	//    oversized declared descriptor, and extra index descriptor annotations
	//    without predicateType) must yield ErrRejectedCarrier without panicking or
	//    poisoning subsequent valid carriers.
	var sigHits atomic.Int64
	reg := newTestReferrersRegistry(true, &sigHits).(*testReferrersRegistry)
	corruptRaw := []byte("{not-valid-manifest-json")
	corruptDigest := sha256HashOf(corruptRaw)
	rawIdx, _ := json.Marshal(v1.IndexManifest{SchemaVersion: 2, MediaType: types.OCIImageIndex})
	indexDigest := sha256HashOf(rawIdx)
	sentinelZeroDigest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0", 64)}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/manifests/"+corruptDigest.String()) {
			w.Header().Set("Content-Type", string(types.OCIManifestSchema1))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(corruptRaw)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/manifests/"+indexDigest.String()) {
			w.Header().Set("Content-Type", string(types.OCIImageIndex))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(rawIdx)
			return
		}
		if testReferrersPath.MatchString(r.URL.Path) && r.Method == http.MethodGet {
			rec := httptest.NewRecorder()
			reg.ServeHTTP(rec, r)
			// Strip the "digest" field from sentinelZeroDigest's JSON object so
			// Unmarshal produces d.Digest == v1.Hash{} on an untrusted /referrers/ response.
			body := bytes.ReplaceAll(rec.Body.Bytes(), []byte(`"digest":"`+sentinelZeroDigest.String()+`",`), nil)
			w.Header().Set("Content-Type", rec.Header().Get("Content-Type"))
			w.WriteHeader(rec.Code)
			_, _ = w.Write(body)
			return
		}
		reg.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	repoStr := u.Host + "/team/resilient"
	repo, err := name.NewRepository(repoStr)
	if err != nil {
		t.Fatal(err)
	}

	subjectImg := testPlatformImage(t, v1.Platform{OS: "linux", Architecture: "amd64"}, "bin/p", "resilient")
	subjectDigest, err := subjectImg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(repo.Tag("v1.0.0"), subjectImg); err != nil {
		t.Fatal(err)
	}
	subjectRaw, _ := subjectImg.RawManifest()
	subjectDesc := v1.Descriptor{MediaType: types.OCIManifestSchema1, Size: int64(len(subjectRaw)), Digest: subjectDigest}

	// Push a valid Sigstore bundle whose index descriptor later has ONLY an
	// unrelated annotation (no predicateType on the index descriptor, only on the manifest).
	bundleLayer := static.NewLayer([]byte(`{"bundle":"survives-extra-descriptor-annotation"}`), types.MediaType(sigstoreBundleMediaType))
	blh, _ := bundleLayer.Digest()
	bls, _ := bundleLayer.Size()
	goodBundleHash := putRawReferrer(
		t, repo, subjectDesc,
		sigstoreBundleMediaType, "application/vnd.oci.empty.v1+json",
		map[string]string{sigstorePredicateKey: cosignSignPredicate},
		[]v1.Descriptor{{MediaType: types.MediaType(sigstoreBundleMediaType), Size: bls, Digest: blh}},
		[]v1.Layer{bundleLayer}, "",
	)

	// Push a referrer whose manifest declares a layer larger than MaxCarrierBytes (without uploading that layer blob).
	declaredHugeHash := putRawReferrer(
		t, repo, subjectDesc,
		sigstoreBundleMediaType, "application/vnd.oci.empty.v1+json",
		map[string]string{sigstorePredicateKey: cosignSignPredicate},
		[]v1.Descriptor{{MediaType: types.MediaType(sigstoreBundleMediaType), Size: MaxCarrierBytes + 1024, Digest: v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("f", 64)}}},
		nil, "",
	)

	// Push a referrer with a valid small layer size whose layer blob is missing (404 / BLOB_UNKNOWN).
	missingBlobHash := putRawReferrer(
		t, repo, subjectDesc,
		sigstoreBundleMediaType, "application/vnd.oci.empty.v1+json",
		map[string]string{sigstorePredicateKey: cosignSignPredicate},
		[]v1.Descriptor{{MediaType: types.MediaType(sigstoreBundleMediaType), Size: 64, Digest: v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("9", 64)}}},
		nil, "",
	)

	// Mutate the registry's referrer index to inject:
	// - an untrusted malformed referrer digest ("sha256:bad")
	// - an OCI index masquerading as a bundle referrer
	// - a corrupt-JSON legacy signature referrer
	// - an unrelated annotation (without predicateType) on goodBundleHash's descriptor
	subjectKey := "team/resilient@" + subjectDigest.String()
	reg.mu.Lock()
	for i := range reg.by[subjectKey] {
		if reg.by[subjectKey][i].Digest == goodBundleHash {
			reg.by[subjectKey][i].Annotations = map[string]string{
				"org.opencontainers.image.created": "2026-01-01T00:00:00Z",
			}
		}
	}
	reg.by[subjectKey] = append(reg.by[subjectKey],
		v1.Descriptor{
			MediaType:    types.OCIManifestSchema1,
			Size:         100,
			Digest:       sentinelZeroDigest,
			ArtifactType: sigstoreBundleMediaType,
		},
		v1.Descriptor{
			MediaType:    types.OCIImageIndex,
			Size:         100,
			Digest:       indexDigest,
			ArtifactType: sigstoreBundleMediaType,
		},
		v1.Descriptor{
			MediaType:    types.OCIManifestSchema1,
			Size:         100,
			Digest:       corruptDigest,
			ArtifactType: legacySigConfigType,
		},
	)
	reg.mu.Unlock()
	_ = declaredHugeHash
	_ = missingBlobHash

	// Query through a non-loopback name.Insecure repository ("http" scheme) so we
	// also verify DiscoverCarriers preserves name.Insecure across referrers,
	// manifest fetches, and .sig fallback tag checks.
	insecureRepo, err := name.NewRepository("registry.example.invalid:5000/team/resilient", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}

	var valid []Carrier
	var rejected int
	for c, cerr := range DiscoverCarriers(
		context.Background(),
		insecureRepo,
		subjectDigest,
		remote.WithTransport(rewriteHostTransport{t: t, wantScheme: "http", targetHost: u.Host}),
	) {
		if cerr != nil {
			if errors.Is(cerr, ErrRejectedCarrier) {
				rejected++
				continue
			}
			t.Fatalf("unexpected non-rejected error: %v", cerr)
		}
		valid = append(valid, c)
	}
	if rejected != 5 {
		t.Fatalf("rejected carriers = %d, want 5 (bad digest + index mediaType + declared huge + missing 404 blob + corrupt JSON)", rejected)
	}
	if len(valid) != 1 || string(valid[0].BundleJSON) != `{"bundle":"survives-extra-descriptor-annotation"}` {
		t.Fatalf("surviving valid carriers = %+v, want 1 surviving bundle", valid)
	}
}
