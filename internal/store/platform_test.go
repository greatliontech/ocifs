package store

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// --- multi-platform fixtures and transport seams ---

var (
	linuxAMD64   = v1.Platform{OS: "linux", Architecture: "amd64"}
	linuxARM64   = v1.Platform{OS: "linux", Architecture: "arm64"}
	linuxARM64v8 = v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
)

func pp(p v1.Platform) *v1.Platform { return &p }

// imageWithPlatform stamps the image's config with a platform, for
// direct-manifest platform checks.
func imageWithPlatform(t *testing.T, p v1.Platform, layers ...v1.Layer) v1.Image {
	t.Helper()
	img := makeImage(t, layers...)
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cf = cf.DeepCopy()
	cf.OS = p.OS
	cf.Architecture = p.Architecture
	cf.Variant = p.Variant
	cf.OSVersion = p.OSVersion
	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

type platformImage struct {
	plat v1.Platform
	img  v1.Image
}

func makeIndex(t *testing.T, children ...platformImage) v1.ImageIndex {
	t.Helper()
	adds := make([]mutate.IndexAddendum, len(children))
	for i, c := range children {
		p := c.plat
		adds[i] = mutate.IndexAddendum{Add: c.img, Descriptor: v1.Descriptor{Platform: &p}}
	}
	return mutate.AppendManifests(empty.Index, adds...)
}

func pushIndex(t *testing.T, rt http.RoundTripper, refStr string, idx v1.ImageIndex) {
	t.Helper()
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, idx, remote.WithTransport(rt)); err != nil {
		t.Fatal(err)
	}
}

func mustDigest(t *testing.T, d interface{ Digest() (v1.Hash, error) }) v1.Hash {
	t.Helper()
	h, err := d.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func newStoreAt(t *testing.T, dir string, policy PullPolicy, platform v1.Platform, rt http.RoundTripper) *Store {
	t.Helper()
	s, err := NewStore(dir, anonKeychain{}, policy, platform)
	if err != nil {
		t.Fatal(err)
	}
	s.transport = rt
	return s
}

// recordingTransport records every round trip as "METHOD path".
type recordingTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	reqs  []string
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	rt.reqs = append(rt.reqs, req.Method+" "+req.URL.Path)
	rt.mu.Unlock()
	return rt.inner.RoundTrip(req)
}

func (rt *recordingTransport) requests() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return append([]string(nil), rt.reqs...)
}

// tagGuardTransport fails the test on any manifest request that is
// not digest-addressed: the seam pinning "never by tag
// re-resolution". Digests carry ':' in the final path element; tags
// cannot.
type tagGuardTransport struct {
	t     *testing.T
	inner http.RoundTripper
}

func (g tagGuardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/manifests/") && !strings.Contains(path.Base(req.URL.Path), ":") {
		g.t.Errorf("tag re-resolution: %s %s", req.Method, req.URL.Path)
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusNotFound)
		resp := rec.Result()
		resp.Request = req
		return resp, nil
	}
	return g.inner.RoundTrip(req)
}

// cutTransport fails the test on any network access.
func cutTransport(t *testing.T) handlerTransport {
	return handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected network access: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusServiceUnavailable)
	})}
}

// offlineTransport serves 404 for everything without failing the
// test: for asserting that an operation errors rather than pulls.
func offlineTransport() handlerTransport {
	return handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})}
}

// --- platform selection ---

// TestIndexPullServesHostDefaultChild pins REQ-store-platform-default
// (the host-derived fallback with no explicit request and no
// configured default),
// REQ-store-platform-serves-child (the child digest names the image),
// and the ingest-order retention shape: refs hold the index digest,
// oci/ retains the index alongside the platform-selected child only.
func TestIndexPullServesHostDefaultChild(t *testing.T) {
	reg := newTestRegistry()
	host := hostPlatform()
	hostImg := imageWithPlatform(t, host, newRawLayer(t, tarBytes(t, tfile("plat", "host"))))
	otherImg := imageWithPlatform(t, v1.Platform{OS: "plan9", Architecture: "mips"}, newRawLayer(t, tarBytes(t, tfile("plat", "other"))))
	idx := makeIndex(t,
		platformImage{host, hostImg},
		platformImage{v1.Platform{OS: "plan9", Architecture: "mips"}, otherImg},
	)
	refStr := testHost + "/test/hostdefault:v1"
	pushIndex(t, reg, refStr, idx)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "host" {
		t.Fatalf("plat = %q, want the host-platform child", got)
	}

	idxDigest := mustDigest(t, idx)
	hostChild := mustDigest(t, hostImg)
	otherChild := mustDigest(t, otherImg)
	if img.Hash() != hostChild {
		t.Fatalf("image digest = %s, want the platform-selected child %s (REQ-store-platform-serves-child)", img.Hash(), hostChild)
	}

	// The ref records the top-level (index) digest.
	files := refFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("%d ref files, want 1", len(files))
	}
	refContent, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(refContent) != idxDigest.String() {
		t.Fatalf("ref holds %q, want the index digest %s (REQ-store-ref-complete)", refContent, idxDigest)
	}

	// oci/ retains the index itself alongside the selected child; the
	// unselected child is not fetched.
	for _, h := range []v1.Hash{idxDigest, hostChild} {
		if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", h.Algorithm, h.Hex)); err != nil {
			t.Fatalf("top-level retention: %s missing from oci/: %v", h, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", otherChild.Algorithm, otherChild.Hex)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected child %s fetched: %v", otherChild, err)
	}

	// index.json records the top-level descriptor, once.
	if n := descriptorCount(t, dir); n != 1 {
		t.Fatalf("%d descriptors, want 1 (the index)", n)
	}
}

// TestExplicitPlatformStrict pins REQ-store-platform-strict: an
// unspecified request field constrains nothing (linux/arm64 uniquely
// matches the arm64/v8 child), and zero matches fail with no
// fallback.
func TestExplicitPlatformStrict(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "arm64"))))
	idx := makeIndex(t,
		platformImage{linuxAMD64, amdImg},
		platformImage{linuxARM64v8, armImg},
	)
	refStr := testHost + "/test/strict:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)

	img, err := s.Image(context.Background(), refStr, pp(linuxARM64))
	if err != nil {
		t.Fatalf("subset match linux/arm64 against sole arm64/v8 child: %v", err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "arm64" {
		t.Fatalf("plat = %q", got)
	}
	if img.Hash() != mustDigest(t, armImg) {
		t.Fatalf("wrong child selected")
	}

	if _, err := s.Image(context.Background(), refStr, pp(v1.Platform{OS: "linux", Architecture: "riscv64"})); err == nil {
		t.Fatal("absent platform served (fallback?)")
	} else if !strings.Contains(err.Error(), "no manifest for platform") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := s.Image(context.Background(), refStr, pp(v1.Platform{OS: "windows", Architecture: "amd64"})); err == nil {
		t.Fatal("absent os served")
	}
}

// TestExplicitPlatformAmbiguous pins the several-matches clause of
// REQ-store-platform-strict: choosing among multiple matching
// children would be a fallback.
func TestExplicitPlatformAmbiguous(t *testing.T) {
	reg := newTestRegistry()
	plain := imageWithPlatform(t, linuxARM64, newRawLayer(t, tarBytes(t, tfile("plat", "plain"))))
	v8 := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "v8"))))
	idx := makeIndex(t,
		platformImage{linuxARM64, plain},
		platformImage{linuxARM64v8, v8},
	)
	refStr := testHost + "/test/ambiguous:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)

	if _, err := s.Image(context.Background(), refStr, pp(linuxARM64)); err == nil {
		t.Fatal("ambiguous platform served")
	} else if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("unexpected error: %v", err)
	}

	// A fully specified request disambiguates.
	img, err := s.Image(context.Background(), refStr, pp(linuxARM64v8))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "v8" {
		t.Fatalf("plat = %q", got)
	}
}

// TestConfiguredDefaultPlatform pins the configured-default clause of
// REQ-store-platform-default: with no explicit request the configured
// platform selects, under the same match rule.
func TestConfiguredDefaultPlatform(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "arm64"))))
	idx := makeIndex(t,
		platformImage{linuxAMD64, amdImg},
		platformImage{linuxARM64v8, armImg},
	)
	refStr := testHost + "/test/confdefault:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxARM64, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "arm64" {
		t.Fatalf("plat = %q, want the configured-default child", got)
	}
}

// TestDirectManifestExplicitPlatformChecked pins the manifest clause
// of REQ-store-platform-strict: an explicit platform constrains a
// direct manifest via its config, and only an explicit one.
func TestDirectManifestExplicitPlatformChecked(t *testing.T) {
	reg := newTestRegistry()
	img := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "direct"))))
	refStr := testHost + "/test/direct:v1"
	push(t, reg, refStr, img)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)

	// Default request: served as-is, no config check — the store's
	// default platform (amd64) differs from the config (arm64/v8).
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatalf("direct manifest with default request: %v", err)
	}

	// Explicit matching request (subset: variant unspecified).
	if _, err := s.Image(context.Background(), refStr, pp(linuxARM64)); err != nil {
		t.Fatalf("direct manifest with matching explicit platform: %v", err)
	}

	// Explicit mismatch fails.
	if _, err := s.Image(context.Background(), refStr, pp(linuxAMD64)); err == nil {
		t.Fatal("mismatched explicit platform served")
	} else if !strings.Contains(err.Error(), "not the requested") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- cached-index platform materialization ---

// TestSecondPlatformPulledByDigestThroughCachedIndex pins the
// IfNotPresent platform clause of REQ-store-pull-policy: a platform
// whose child is not yet materialized is pulled by digest through
// the cached index — no tag re-resolution — and one cached
// resolution then serves both platforms offline.
func TestSecondPlatformPulledByDigestThroughCachedIndex(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "arm64"))))
	idx := makeIndex(t,
		platformImage{linuxAMD64, amdImg},
		platformImage{linuxARM64v8, armImg},
	)
	refStr := testHost + "/test/secondplat:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	// Second platform: every request from here on must be
	// digest-addressed.
	rec := &recordingTransport{inner: reg}
	s.transport = tagGuardTransport{t: t, inner: rec}
	img, err := s.Image(context.Background(), refStr, pp(linuxARM64))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "arm64" {
		t.Fatalf("plat = %q", got)
	}
	if img.Hash() != mustDigest(t, armImg) {
		t.Fatalf("wrong child")
	}
	var manifestGets int
	for _, r := range rec.requests() {
		if strings.Contains(r, "/manifests/") {
			manifestGets++
		}
	}
	if manifestGets == 0 {
		t.Fatal("second platform served without fetching its child (test harness broken?)")
	}

	// Both platforms now serve fully offline from one cached
	// resolution.
	s.transport = cutTransport(t)
	for _, p := range []v1.Platform{linuxAMD64, linuxARM64v8} {
		if _, err := s.Image(context.Background(), refStr, pp(p)); err != nil {
			t.Fatalf("offline serve of %s: %v", p.String(), err)
		}
	}
	if n := len(refFiles(t, dir)); n != 1 {
		t.Fatalf("%d ref files, want 1 (one resolution serves all platforms)", n)
	}
}

// TestNeverPlatformNotMaterializedFails pins the Never platform
// clause of REQ-store-pull-policy.
func TestNeverPlatformNotMaterializedFails(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "arm64"))))
	idx := makeIndex(t,
		platformImage{linuxAMD64, amdImg},
		platformImage{linuxARM64v8, armImg},
	)
	refStr := testHost + "/test/neverplat:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	never := newStoreAt(t, dir, PullNever, linuxAMD64, cutTransport(t))
	if _, err := never.Image(context.Background(), refStr, nil); err != nil {
		t.Fatalf("materialized platform under Never: %v", err)
	}
	if _, err := never.Image(context.Background(), refStr, pp(linuxARM64)); err == nil {
		t.Fatal("unmaterialized platform served under Never")
	} else if !strings.Contains(err.Error(), "forbids fetching") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- digest-addressed entry ---

// TestDigestEntry pins REQ-store-digest-entry: entry by (repository,
// digest, platform) with no tag interaction at all, completing under
// Never once cached.
func TestDigestEntry(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "arm64"))))
	idx := makeIndex(t,
		platformImage{linuxAMD64, amdImg},
		platformImage{linuxARM64v8, armImg},
	)
	repo := testHost + "/test/digentry"
	pushIndex(t, reg, repo+":v1", idx)
	idxDigest := mustDigest(t, idx)
	digestRef := repo + "@" + idxDigest.String()

	// Fresh store: the whole materialization is digest-addressed.
	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, tagGuardTransport{t: t, inner: reg})
	img, err := s.Image(context.Background(), digestRef, pp(linuxARM64))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img, "plat")); got != "arm64" {
		t.Fatalf("plat = %q", got)
	}

	// Fully cached: completes under Never with no network access —
	// and with no reference-cache entry: the digest is the identity,
	// so the resolution must not route through the refs tier.
	for _, f := range refFiles(t, dir) {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	never := newStoreAt(t, dir, PullNever, linuxAMD64, cutTransport(t))
	img2, err := never.Image(context.Background(), digestRef, pp(linuxARM64))
	if err != nil {
		t.Fatalf("digest entry against cached content under Never: %v", err)
	}
	if img2.Hash() != img.Hash() {
		t.Fatalf("cached digest entry selected a different child")
	}
}

// TestDigestEntryDirectManifest pins the direct-manifest arm of
// REQ-store-digest-entry: the digest can name a manifest, and the
// explicit-platform config check applies.
func TestDigestEntryDirectManifest(t *testing.T) {
	reg := newTestRegistry()
	img := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("plat", "direct"))))
	repo := testHost + "/test/digdirect"
	push(t, reg, repo+":v1", img)
	childDigest := mustDigest(t, img)
	digestRef := repo + "@" + childDigest.String()

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, tagGuardTransport{t: t, inner: reg})

	got, err := s.Image(context.Background(), digestRef, pp(linuxARM64))
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash() != childDigest {
		t.Fatalf("digest entry hash = %s, want %s", got.Hash(), childDigest)
	}
	if _, err := s.Image(context.Background(), digestRef, pp(linuxAMD64)); err == nil {
		t.Fatal("mismatched explicit platform served on direct-manifest digest entry")
	}
}

// --- pull policies over multi-platform content ---

// TestPullAlwaysRevalidatesTopLevel pins the Always clause of
// REQ-store-pull-policy for a multi-platform reference: HEAD
// compares top-level to top-level (index digest), cached content
// serves on a match, and a moved tag re-ingests and re-records.
func TestPullAlwaysRevalidatesTopLevel(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("v", "one"))))
	armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("v", "arm"))))
	idx := makeIndex(t, platformImage{linuxAMD64, amdImg}, platformImage{linuxARM64v8, armImg})
	refStr := testHost + "/test/alwaysidx:v1"
	pushIndex(t, reg, refStr, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullAlways, linuxAMD64, reg)
	img1, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Unchanged remote: exactly one HEAD, no GETs — the cached index
	// digest matches the served top-level digest.
	rec := &recordingTransport{inner: reg}
	s.transport = rec
	img2, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if img2.Hash() != img1.Hash() {
		t.Fatal("digest changed without remote change")
	}
	for _, r := range rec.requests() {
		if strings.HasPrefix(r, "GET ") && (strings.Contains(r, "/manifests/") || strings.Contains(r, "/blobs/")) {
			t.Fatalf("content re-fetched despite matching HEAD: %s", r)
		}
	}

	// Tag moved: Always serves the new content and re-records the ref.
	amdImg2 := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("v", "two"))))
	idx2 := makeIndex(t, platformImage{linuxAMD64, amdImg2}, platformImage{linuxARM64v8, armImg})
	pushIndex(t, reg, refStr, idx2)
	s.transport = reg
	img3, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img3, "v")); got != "two" {
		t.Fatalf("v = %q after tag move", got)
	}
	files := refFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("%d ref files", len(files))
	}
	refContent, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(refContent) != mustDigest(t, idx2).String() {
		t.Fatalf("ref not re-recorded to the new index digest")
	}
}

// --- self-heal pull-through ---

// TestPullThroughHealsMissingOCIBlob pins the pull-through clause of
// REQ-store-self-heal: content absent from oci/ under an intact ref
// is re-fetched exactly, by digest through the cached resolution,
// when the policy permits network access.
func TestPullThroughHealsMissingOCIBlob(t *testing.T) {
	reg := newTestRegistry()
	l := newRawLayer(t, tarBytes(t, tfile("data", "precious")))
	img := makeImage(t, l)
	refStr := testHost + "/test/pullthrough:v1"
	push(t, reg, refStr, img)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	// Erase the compressed layer from oci/ AND its layer index, so
	// re-serving needs the compressed bytes back.
	ld := mustDigest(t, l)
	for _, p := range []string{
		filepath.Join(dir, "oci", "blobs", ld.Algorithm, ld.Hex),
		filepath.Join(dir, "layers", ld.Algorithm, ld.Hex),
	} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}

	rec := &recordingTransport{inner: reg}
	s.transport = tagGuardTransport{t: t, inner: rec}
	healed, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("pull-through heal: %v", err)
	}
	if got := string(readEntry(t, s, healed, "data")); got != "precious" {
		t.Fatalf("data = %q after pull-through heal", got)
	}

	// Exactly the missing blob was fetched: digest-addressed, no
	// manifest requests at all.
	var blobGets, manifestReqs int
	for _, r := range rec.requests() {
		if strings.Contains(r, "/manifests/") {
			manifestReqs++
		}
		if strings.HasPrefix(r, "GET ") && strings.Contains(r, "/blobs/") {
			blobGets++
			if !strings.Contains(r, ld.Hex) {
				t.Fatalf("fetched a blob that was not missing: %s", r)
			}
		}
	}
	if manifestReqs != 0 {
		t.Fatalf("%d manifest requests during blob pull-through, want 0", manifestReqs)
	}
	if blobGets != 1 {
		t.Fatalf("%d blob GETs, want exactly the missing one", blobGets)
	}
}

// TestPullThroughHealsMissingTopArtifact: the retained top-level
// artifact itself is re-fetched by digest through the cached
// resolution.
func TestPullThroughHealsMissingTopArtifact(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	idx := makeIndex(t, platformImage{linuxAMD64, amdImg})
	refStr := testHost + "/test/pullthroughtop:v1"
	pushIndex(t, reg, refStr, idx)
	idxDigest := mustDigest(t, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, "oci", "blobs", idxDigest.Algorithm, idxDigest.Hex)); err != nil {
		t.Fatal(err)
	}
	s.transport = tagGuardTransport{t: t, inner: reg}
	healed, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("pull-through heal of the top-level artifact: %v", err)
	}
	if got := string(readEntry(t, s, healed, "plat")); got != "amd64" {
		t.Fatalf("plat = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", idxDigest.Algorithm, idxDigest.Hex)); err != nil {
		t.Fatalf("top-level artifact not re-retained: %v", err)
	}
}

// TestPullThroughUnderNeverFailsNamingBlob pins the Never arm of the
// pull-through clause: the heal fails identifying the missing blob.
func TestPullThroughUnderNeverFailsNamingBlob(t *testing.T) {
	reg := newTestRegistry()
	l := newRawLayer(t, tarBytes(t, tfile("data", "precious")))
	img := makeImage(t, l)
	refStr := testHost + "/test/pullthroughnever:v1"
	push(t, reg, refStr, img)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	ld := mustDigest(t, l)
	for _, p := range []string{
		filepath.Join(dir, "oci", "blobs", ld.Algorithm, ld.Hex),
		filepath.Join(dir, "layers", ld.Algorithm, ld.Hex),
	} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}

	never := newStoreAt(t, dir, PullNever, v1.Platform{}, cutTransport(t))
	_, err := never.Image(context.Background(), refStr, nil)
	if err == nil {
		t.Fatal("served with a missing oci/ blob under Never")
	}
	if !strings.Contains(err.Error(), ld.Hex) {
		t.Fatalf("heal failure does not identify the missing blob: %v", err)
	}
	if !strings.Contains(err.Error(), "forbids fetching") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestTagPullAfterDigestPullListsTopDescriptor: content can
// pre-exist through a shared route (here: the child was pulled by
// digest first); a later tag pull that completes over that content
// must still leave its top-level artifact listed in oci/index.json —
// a ref must never name an artifact the layout cannot enumerate
// (REQ-store-ingest-order retention).
func TestTagPullAfterDigestPullListsTopDescriptor(t *testing.T) {
	reg := newTestRegistry()
	amdImg := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("plat", "amd64"))))
	idx := makeIndex(t, platformImage{linuxAMD64, amdImg})
	repo := testHost + "/test/shared-route"
	pushIndex(t, reg, repo+":v1", idx)
	childDigest := mustDigest(t, amdImg)
	idxDigest := mustDigest(t, idx)

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)

	// Materialize the child fully via its own digest identity.
	if _, err := s.Image(context.Background(), repo+"@"+childDigest.String(), nil); err != nil {
		t.Fatal(err)
	}
	// The tag pull now finds every blob it needs already present.
	if _, err := s.Image(context.Background(), repo+":v1", nil); err != nil {
		t.Fatal(err)
	}

	listed, err := s.descriptorListed(idxDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !listed {
		t.Fatalf("ref recorded for index %s but oci/index.json does not list it", idxDigest)
	}
}

// TestDigestlessIndexChildRejected: an index whose platform-matching
// child carries no digest must fail with a classified error, never
// resolve the blob tier's directory as a path.
func TestDigestlessIndexChildRejected(t *testing.T) {
	rawIdx := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"linux","architecture":"amd64"}}]}`)
	rt := handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/":
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
			w.Write(rawIdx)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})}

	dir := scratchDir(t)
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, rt)
	_, err := s.Image(context.Background(), testHost+"/test/digestless:v1", nil)
	if err == nil {
		t.Fatal("digest-less index child accepted")
	}
	if !strings.Contains(err.Error(), "has no digest") {
		t.Fatalf("unclassified failure: %v", err)
	}
}

// TestFallbackPlatform pins the built-in default derivation
// (REQ-store-platform-default): the host's os/arch, except darwin
// falls back to linux — an os=darwin request could never match
// published images.
func TestFallbackPlatform(t *testing.T) {
	for _, tc := range []struct {
		goos, goarch string
		want         v1.Platform
	}{
		{"linux", "amd64", v1.Platform{OS: "linux", Architecture: "amd64"}},
		{"linux", "arm64", v1.Platform{OS: "linux", Architecture: "arm64"}},
		{"windows", "amd64", v1.Platform{OS: "windows", Architecture: "amd64"}},
		{"darwin", "arm64", v1.Platform{OS: "linux", Architecture: "arm64"}},
		{"darwin", "amd64", v1.Platform{OS: "linux", Architecture: "amd64"}},
	} {
		if got := fallbackPlatform(tc.goos, tc.goarch); !got.Equals(tc.want) {
			t.Fatalf("fallbackPlatform(%s, %s) = %s, want %s", tc.goos, tc.goarch, got.String(), tc.want.String())
		}
	}
}
