package ocifs

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func testPlatformImage(t *testing.T, p v1.Platform, file, content string) v1.Image {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := tar.Header{Name: file, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}
	if err := tw.WriteHeader(&hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	tarData := buf.Bytes()
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(tarData)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, l)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cf = cf.DeepCopy()
	cf.OS = p.OS
	cf.Architecture = p.Architecture
	cf.Variant = p.Variant
	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

// TestPullSurface pins REQ-api-acquire and the construction options
// of REQ-api-construction: acquisition by reference string under the
// configured default platform, acquisition by digest with explicit
// platform, an accessible config file on both, and the configured
// pull policy governing.
func TestPullSurface(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	arm64v8 := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	amdImg := testPlatformImage(t, amd64, "plat", "amd64")
	armImg := testPlatformImage(t, arm64v8, "plat", "arm64")
	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amdImg, Descriptor: v1.Descriptor{Platform: &amd64}},
		mutate.IndexAddendum{Add: armImg, Descriptor: v1.Descriptor{Platform: &arm64v8}},
	)
	refStr := u.Host + "/test/pullapi:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, idx); err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-pull"))

	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithPullPolicy(PullIfNotPresent),
		WithDefaultPlatform(amd64),
	)
	if err != nil {
		t.Fatal(err)
	}

	img, err := ofs.Pull(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	amdDigest, err := amdImg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if img.Digest() != amdDigest {
		t.Fatalf("default-platform pull digest = %s, want %s", img.Digest(), amdDigest)
	}
	cf := img.ConfigFile()
	if cf == nil || cf.Architecture != "amd64" {
		t.Fatalf("config file not accessible or wrong: %+v", cf)
	}

	idxDigest, err := idx.Digest()
	if err != nil {
		t.Fatal(err)
	}
	img2, err := ofs.Pull(context.Background(), u.Host+"/test/pullapi@"+idxDigest.String(),
		PullWithPlatform(v1.Platform{OS: "linux", Architecture: "arm64"}))
	if err != nil {
		t.Fatal(err)
	}
	armDigest, err := armImg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if img2.Digest() != armDigest {
		t.Fatalf("digest+platform pull digest = %s, want %s", img2.Digest(), armDigest)
	}
	if cf := img2.ConfigFile(); cf == nil || cf.Architecture != "arm64" {
		t.Fatalf("config file not accessible or wrong: %+v", cf)
	}

	// The configured pull policy governs acquisition.
	never, err := New(
		WithWorkDir(filepath.Join(scratch, "never-work")),
		WithPullPolicy(PullNever),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := never.Pull(context.Background(), refStr); err == nil {
		t.Fatal("uncached pull succeeded under PullNever")
	} else if !strings.Contains(err.Error(), "Never") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFailedMountLeavesNoMountDir: a mount that fails at acquisition
// must not strand an orphan directory under the store's mounts/
// tier.
func TestFailedMountLeavesNoMountDir(t *testing.T) {
	skipUnderMutationCampaign(t)
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-mountfail"))

	workDir := filepath.Join(scratch, "work")
	ofs, err := New(WithWorkDir(workDir), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ofs.Mount("registry.invalid/absent:v1"); err == nil {
		t.Fatal("mount of an uncached image under PullNever succeeded")
	}
	entries, err := os.ReadDir(filepath.Join(workDir, "mounts"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed mount left %d orphan entries under mounts/", len(entries))
	}
}

// TestConstructionFailsOnUninitializableStore pins the construction
// clause of REQ-api-construction: New fails when the store cannot be
// initialized (here: a pre-layout work directory).
func TestConstructionFailsOnUninitializableStore(t *testing.T) {
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-badstore"))
	if err := os.MkdirAll(filepath.Join(scratch, "oci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "oci", "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(WithWorkDir(scratch)); err == nil {
		t.Fatal("construction succeeded over an unrecognized store layout")
	}
}

// TestVerifierSeamOnAcquisition pins REQ-api-acquire's seam clause on
// the public surface: with WithVerifier configured, both acquisition
// arms — by reference string and by digest with explicit platform —
// run the verification seam, a rejection surfaces as a
// VerificationError carrying the resolved identity, and an accepting
// verifier leaves acquisition unchanged.
func TestVerifierSeamOnAcquisition(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	arm64v8 := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: testPlatformImage(t, amd64, "seam", "amd64"), Descriptor: v1.Descriptor{Platform: &amd64}},
		mutate.IndexAddendum{Add: testPlatformImage(t, arm64v8, "seam", "arm64"), Descriptor: v1.Descriptor{Platform: &arm64v8}},
	)
	refStr := u.Host + "/test/seam:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, idx); err != nil {
		t.Fatal(err)
	}
	idxDigest, err := idx.Digest()
	if err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-seam"))

	rejected := errors.New("untrusted")
	var seen []ResolvedIdentity
	admit := false
	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithDefaultPlatform(amd64),
		WithVerifier(func(ctx context.Context, id ResolvedIdentity) error {
			seen = append(seen, id)
			if !admit {
				return rejected
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ofs.Pull(context.Background(), refStr)
	var verr *VerificationError
	if !errors.As(err, &verr) || !errors.Is(err, rejected) {
		t.Fatalf("rejected tag pull returned %v, want VerificationError wrapping the verifier's error", err)
	}
	if verr.Reference != refStr || verr.Digest != idxDigest {
		t.Fatalf("VerificationError identity = (%q, %s), want (%q, %s)", verr.Reference, verr.Digest, refStr, idxDigest)
	}

	digestRef := u.Host + "/test/seam@" + idxDigest.String()
	if _, err := ofs.Pull(context.Background(), digestRef, PullWithPlatform(v1.Platform{OS: "linux", Architecture: "arm64"})); !errors.As(err, &verr) {
		t.Fatalf("rejected digest pull returned %v, want VerificationError", err)
	}
	if len(seen) != 2 {
		t.Fatalf("verifier ran %d times for two acquisitions", len(seen))
	}
	if seen[1].Reference != digestRef || seen[1].Digest != idxDigest {
		t.Fatalf("digest-arm identity = (%q, %s), want (%q, %s)", seen[1].Reference, seen[1].Digest, digestRef, idxDigest)
	}

	admit = true
	img, err := ofs.Pull(context.Background(), refStr)
	if err != nil {
		t.Fatalf("admitted pull failed: %v", err)
	}
	if cf := img.ConfigFile(); cf == nil || cf.Architecture != "amd64" {
		t.Fatalf("admitted pull served wrong image: %+v", cf)
	}
}

// Resolution runs the seam and yields the digest without materializing:
// a rejected resolution is the seam's refusal; an admitted one leaves
// no layer content and no reference-cache entry behind, so a
// pull-never acquisition afterwards still finds nothing (api.md
// REQ-api-resolve).
func TestResolveRunsSeamWithoutMaterializing(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: testPlatformImage(t, amd64, "resolve", "amd64"), Descriptor: v1.Descriptor{Platform: &amd64}},
	)
	refStr := u.Host + "/test/resolve:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, idx); err != nil {
		t.Fatal(err)
	}
	idxDigest, err := idx.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-resolve"))
	rejected := errors.New("untrusted")
	admit := false
	var seen []ResolvedIdentity
	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithDefaultPlatform(amd64),
		WithVerifier(func(ctx context.Context, id ResolvedIdentity) error {
			seen = append(seen, id)
			if !admit {
				return rejected
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	var verr *VerificationError
	if _, err := ofs.Resolve(context.Background(), refStr); !errors.As(err, &verr) || verr.Digest != idxDigest {
		t.Fatalf("rejected resolution returned %v, want VerificationError at %s", err, idxDigest)
	}
	admit = true
	res, err := ofs.Resolve(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Digest != idxDigest || res.Reference != refStr {
		t.Fatalf("resolved %+v, want %s", res, idxDigest)
	}
	if len(seen) != 2 || seen[1].Digest != idxDigest || len(seen[1].Artifact) == 0 {
		t.Fatalf("the seam saw %d identities, last %+v", len(seen), seen[len(seen)-1])
	}
	// Nothing materialized, nothing recorded: a pull that may not
	// reach the network finds no image.
	never, err := New(WithWorkDir(filepath.Join(scratch, "work")), WithDefaultPlatform(amd64), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := never.Pull(context.Background(), refStr); err == nil {
		t.Fatal("a resolution recorded the reference as acquired")
	}
	digestRef := u.Host + "/test/resolve@" + idxDigest.String()
	if _, err := never.Pull(context.Background(), digestRef); err == nil {
		t.Fatal("a resolution materialized the image")
	}
	// The digest form resolves to itself, runs the seam, and dials no
	// network: the retained top-level artifact is the seam's input
	// after the registry is gone.
	srv.Close()
	before := len(seen)
	if res, err := ofs.Resolve(context.Background(), digestRef); err != nil || res.Digest != idxDigest || len(seen) != before+1 {
		t.Fatalf("digest-form resolution without a registry: %+v %v, seam runs %d", res, err, len(seen)-before)
	}
}

// handlerRoundTripper serves a registry handler as the transport:
// every registry round trip is an in-process call, no socket. It
// keeps the RoundTripper contract — the caller's request is not
// mutated, and its body is closed.
type handlerRoundTripper struct{ h http.Handler }

func (t handlerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	served := req.Clone(req.Context())
	if served.Body == nil {
		served.Body = http.NoBody
	}
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, served)
	if req.Body != nil {
		req.Body.Close()
	}
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

// bearerGate fronts a registry handler with a token challenge: every
// request but the token endpoint's needs a bearer the endpoint minted
// for the fixture's one user — the round trips a consumer's
// credentials ride, all through the same transport.
func bearerGate(inner http.Handler, host, user, pass string) http.Handler {
	const token = "fixture-bearer"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			u, p, ok := r.BasicAuth()
			if !ok || u != user || p != pass {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"token":"`+token+`"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("WWW-Authenticate", `Bearer realm="https://`+host+`/token",service="fixture"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	})
}

// The transport a consumer hands at construction carries every
// registry round trip: an image is pulled from a registry that is a
// handler in this process, no socket bound, through the token
// challenge and exchange the consumer's credentials answer; without
// the transport the same host is a dial that resolves nowhere
// (REQ-api-construction).
func TestTransportServesRegistryInProcess(t *testing.T) {
	const host = "inprocess.invalid"
	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	img := testPlatformImage(t, amd64, "hello", "through the transport")
	want, err := img.Digest()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := name.ParseReference(host + "/test/transport:v1")
	if err != nil {
		t.Fatal(err)
	}
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	open := handlerRoundTripper{h: inner}
	if err := remote.Write(ref, img, remote.WithTransport(open)); err != nil {
		t.Fatal(err)
	}
	gated := handlerRoundTripper{h: bearerGate(inner, host, "user", "pass")}
	ofs, err := New(WithWorkDir(filepath.Join(t.TempDir(), "work")), WithDefaultPlatform(amd64), WithTransport(gated), WithAuthSource(host, authn.AuthConfig{Username: "user", Password: "pass"}))
	if err != nil {
		t.Fatal(err)
	}
	defer ofs.Close()
	pulled, err := ofs.Pull(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("pull through the in-process transport: %v", err)
	}
	if pulled.Digest() != want {
		t.Fatalf("pulled %s, want %s", pulled.Digest(), want)
	}
	// The wrong credentials are refused by the same challenge, through
	// the same transport.
	wrong, err := New(WithWorkDir(filepath.Join(t.TempDir(), "wrong")), WithDefaultPlatform(amd64), WithTransport(gated), WithAuthSource(host, authn.AuthConfig{Username: "user", Password: "nope"}))
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	if _, err := wrong.Pull(context.Background(), ref.String()); err == nil {
		t.Fatal("a pull with the wrong credentials was served")
	}
	// Without the transport the same host is a dial, and there is
	// nothing to dial; the attempt is bounded so a proxy in the
	// environment cannot stretch it.
	bare, err := New(WithWorkDir(filepath.Join(t.TempDir(), "bare")), WithDefaultPlatform(amd64))
	if err != nil {
		t.Fatal(err)
	}
	defer bare.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := bare.Pull(ctx, ref.String()); err == nil {
		t.Fatal("a pull with no transport reached an in-process handler")
	}
}

// A resolution under a policy the call states — Always over a store
// held at IfNotPresent — asks the registry for the reference it
// holds cached, the store's own policy answering from the cache
// (REQ-api-resolve, REQ-store-pull-policy); a policy the store does
// not know is refused.
func TestResolveUnderPolicy(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	refStr := u.Host + "/test/moving:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	push := func(marker string) v1.Hash {
		idx := mutate.AppendManifests(empty.Index,
			mutate.IndexAddendum{Add: testPlatformImage(t, amd64, marker, "amd64"), Descriptor: v1.Descriptor{Platform: &amd64}},
		)
		if err := remote.WriteIndex(ref, idx); err != nil {
			t.Fatal(err)
		}
		h, err := idx.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	first := push("first")
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-resolve-under"))
	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")), WithDefaultPlatform(amd64))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ofs.Close() })
	if _, err := ofs.Pull(context.Background(), refStr); err != nil {
		t.Fatal(err)
	}
	second := push("second")
	if res, err := ofs.Resolve(context.Background(), refStr); err != nil || res.Digest != first {
		t.Fatalf("the store's policy re-resolved a cached tag: %+v %v", res, err)
	}
	if res, err := ofs.Resolve(context.Background(), refStr, ResolveUnder(PullAlways)); err != nil || res.Digest != second {
		t.Fatalf("Always for the call alone: %+v %v, want %s", res, err, second)
	}
	// The call's policy is the call's: the store still answers from
	// the cache afterwards, nothing recorded by the resolution.
	if res, err := ofs.Resolve(context.Background(), refStr); err != nil || res.Digest != first {
		t.Fatalf("the store's policy after the call: %+v %v", res, err)
	}
	if _, err := ofs.Resolve(context.Background(), u.Host+"/test/unknown:v1", ResolveUnder(PullNever)); err == nil {
		t.Fatal("Never resolved an uncached reference")
	}
	for _, p := range []PullPolicy{PullPolicy(42), PullPolicy(0)} {
		if _, err := ofs.Resolve(context.Background(), refStr, ResolveUnder(p)); err == nil {
			t.Fatalf("policy %d resolved", p)
		}
	}
	// The call's Always materialized nothing and recorded nothing: a
	// store that may not dial finds no image at the new digest.
	never, err := New(WithWorkDir(filepath.Join(scratch, "work")), WithDefaultPlatform(amd64), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { never.Close() })
	if _, err := never.Pull(context.Background(), u.Host+"/test/moving@"+second.String()); err == nil {
		t.Fatal("a resolution under Always materialized the image")
	}
	// A store held at Never grants no call a policy past it.
	if _, err := never.Resolve(context.Background(), refStr, ResolveUnder(PullAlways)); err == nil || !strings.Contains(err.Error(), "held at Never") {
		t.Fatalf("Always asked of a store held at Never: %v", err)
	}
	if res, err := never.Resolve(context.Background(), refStr, ResolveUnder(PullNever)); err != nil || res.Digest != first {
		t.Fatalf("Never asked of a store held at Never: %+v %v", res, err)
	}
}
