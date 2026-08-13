package store

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/cas"
)

// TestPropertyTierKeyspacesDisjoint pins REQ-store-ns as a for-all
// over keys: no digest resolves to the same path in the layer-index
// tier and the content CAS, and each stays under its own root.
func TestPropertyTierKeyspacesDisjoint(t *testing.T) {
	dir := scratchDir(t)
	contentCAS, err := cas.New(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	li := layerIndexes{root: filepath.Join(dir, "layers")}
	hexRunes := rapid.RuneFrom([]rune("0123456789abcdef"))
	rapid.Check(t, func(rt *rapid.T) {
		h := v1.Hash{
			Algorithm: "sha256",
			Hex:       rapid.StringOfN(hexRunes, 64, 64, -1).Draw(rt, "hex"),
		}
		indexPath, blobPath := li.path(h), contentCAS.Path(h)
		if indexPath == blobPath {
			rt.Fatalf("colliding path %s", indexPath)
		}
		if !strings.HasPrefix(indexPath, filepath.Join(dir, "layers")+string(os.PathSeparator)) {
			rt.Fatalf("index path %s outside layers/", indexPath)
		}
		if !strings.HasPrefix(blobPath, filepath.Join(dir, "blobs")+string(os.PathSeparator)) {
			rt.Fatalf("blob path %s outside blobs/", blobPath)
		}
	})
}

// TestPropertyTamperRejected pins REQ-store-ingest-verified as a
// for-all over corruption position: however the network flips one
// byte of a layer blob, ingest fails and neither the blob nor a ref
// is persisted.
func TestPropertyTamperRejected(t *testing.T) {
	l := newRawLayer(t, tarBytes(t, tfile("x", "trustworthy")))
	ld, err := l.Digest()
	if err != nil {
		t.Fatal(err)
	}
	img := makeImage(t, l)

	rapid.Check(t, func(rt *rapid.T) {
		pos := rapid.IntRange(0, len(l.compressed)-1).Draw(rt, "pos")

		transport := tamperTransport(newTestRegistry(), ld, pos, l.compressed)
		refStr := testHost + "/test/tampered:v1"
		push(t, transport, refStr, img)

		s, dir := newTestStore(t, PullIfNotPresent, transport)
		if _, err := s.Image(context.Background(), refStr, nil); err == nil {
			rt.Fatalf("ingest succeeded with byte %d flipped", pos)
		}
		if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", ld.Algorithm, ld.Hex)); !errors.Is(err, os.ErrNotExist) {
			rt.Fatalf("tampered blob persisted (flip at %d): %v", pos, err)
		}
		if files := refFiles(t, dir); len(files) != 0 {
			rt.Fatalf("ref written despite failed verification: %v", files)
		}
	})
}

// failAfterTransport serves n round trips, then answers everything
// 404 (a terminal registry error — no client retries): the fault
// injection for crash-shaped interrupted ingests.
type failAfterTransport struct {
	inner     http.RoundTripper
	mu        sync.Mutex
	remaining int
}

func (ft *failAfterTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ft.mu.Lock()
	ok := ft.remaining > 0
	if ok {
		ft.remaining--
	}
	ft.mu.Unlock()
	if !ok {
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusNotFound)
		resp := rec.Result()
		resp.Request = req
		return resp, nil
	}
	return ft.inner.RoundTrip(req)
}

// TestPropertyRefImpliesOfflineComplete pins REQ-store-ref-complete
// as a for-all over ingest interruption points: however many round
// trips an ingest survives before the registry vanishes, a recorded
// reference-cache entry implies the requested platform materializes
// fully offline — and a pull that never recorded a ref must have
// failed.
func TestPropertyRefImpliesOfflineComplete(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		reg := newTestRegistry()
		refStr := testHost + "/prop/complete:v1"

		nLayers := rapid.IntRange(1, 2).Draw(rt, "layers")
		imgLayers := make([]v1.Layer, nLayers)
		for i := range imgLayers {
			content := rapid.StringMatching(`[a-z]{1,8}`).Draw(rt, "content"+strconv.Itoa(i))
			imgLayers[i] = newRawLayer(t, tarBytes(t, tfile("f"+strconv.Itoa(i), content)))
		}
		img := imageWithPlatform(t, linuxAMD64, imgLayers...)
		if rapid.Bool().Draw(rt, "index") {
			armImg := imageWithPlatform(t, linuxARM64v8, newRawLayer(t, tarBytes(t, tfile("arm", "arm"))))
			pushIndex(t, reg, refStr, makeIndex(t,
				platformImage{linuxAMD64, img},
				platformImage{linuxARM64v8, armImg},
			))
		} else {
			push(t, reg, refStr, img)
		}

		dir := scratchDir(t)
		budget := rapid.IntRange(0, 14).Draw(rt, "budget")
		s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, &failAfterTransport{inner: reg, remaining: budget})
		_, pullErr := s.Image(context.Background(), refStr, nil)

		// Whatever the interruption point, no oci/index.json
		// descriptor dangles: blobs are durable before the descriptor
		// that names them (REQ-store-ingest-order).
		idxData, err := os.ReadFile(filepath.Join(dir, "oci", "index.json"))
		if err != nil {
			rt.Fatalf("oci/index.json unreadable: %v", err)
		}
		var oidx v1.IndexManifest
		if err := json.Unmarshal(idxData, &oidx); err != nil {
			rt.Fatalf("oci/index.json corrupt: %v", err)
		}
		for _, d := range oidx.Manifests {
			if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", d.Digest.Algorithm, d.Digest.Hex)); err != nil {
				rt.Fatalf("descriptor %s dangles (budget %d): %v", d.Digest, budget, err)
			}
		}

		if len(refFiles(t, dir)) == 0 {
			if pullErr == nil {
				rt.Fatalf("pull succeeded without recording a ref (budget %d)", budget)
			}
			return
		}
		offline := newStoreAt(t, dir, PullNever, linuxAMD64, offlineTransport())
		if _, err := offline.Image(context.Background(), refStr, nil); err != nil {
			rt.Fatalf("ref recorded (pull err: %v, budget %d) but offline materialization failed: %v", pullErr, budget, err)
		}
	})
}

// TestPropertySelectChildExactlyOneMatch pins the
// REQ-store-platform-strict match rule as a for-all over index
// platform sets and requests: selection succeeds exactly when one
// child satisfies every request-specified field, and then returns
// that child — zero and several matches always fail.
func TestPropertySelectChildExactlyOneMatch(t *testing.T) {
	osGen := rapid.SampledFrom([]string{"linux", "windows", "plan9"})
	archGen := rapid.SampledFrom([]string{"amd64", "arm64", "arm"})
	variantGen := rapid.SampledFrom([]string{"", "v7", "v8"})
	osvGen := rapid.SampledFrom([]string{"", "10.0.20348"})
	platGen := rapid.Custom(func(rt *rapid.T) v1.Platform {
		return v1.Platform{
			OS:           osGen.Draw(rt, "os"),
			Architecture: archGen.Draw(rt, "arch"),
			Variant:      variantGen.Draw(rt, "variant"),
			OSVersion:    osvGen.Draw(rt, "osversion"),
		}
	})
	reqGen := rapid.Custom(func(rt *rapid.T) v1.Platform {
		p := platGen.Draw(rt, "base")
		// A request may leave any field unspecified.
		if rapid.Bool().Draw(rt, "dropOS") {
			p.OS = ""
		}
		if rapid.Bool().Draw(rt, "dropArch") {
			p.Architecture = ""
		}
		if rapid.Bool().Draw(rt, "dropVariant") {
			p.Variant = ""
		}
		if rapid.Bool().Draw(rt, "dropOSV") {
			p.OSVersion = ""
		}
		return p
	})

	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 5).Draw(rt, "children")
		idx := &v1.IndexManifest{}
		for i := 0; i < n; i++ {
			d := v1.Descriptor{Digest: v1.Hash{Algorithm: "sha256", Hex: strings.Repeat(strconv.Itoa(i%10), 64)}}
			if !rapid.Bool().Draw(rt, "platformless"+strconv.Itoa(i)) {
				p := platGen.Draw(rt, "plat"+strconv.Itoa(i))
				d.Platform = &p
			}
			idx.Manifests = append(idx.Manifests, d)
		}
		req := reqGen.Draw(rt, "request")

		// Independent statement of the rule: every specified request
		// field equals the child's; platform-less children never match.
		var want []v1.Descriptor
		for _, d := range idx.Manifests {
			if d.Platform == nil {
				continue
			}
			eq := func(want, got string) bool { return want == "" || want == got }
			if eq(req.OS, d.Platform.OS) && eq(req.Architecture, d.Platform.Architecture) &&
				eq(req.Variant, d.Platform.Variant) && eq(req.OSVersion, d.Platform.OSVersion) {
				want = append(want, d)
			}
		}

		got, err := selectChild(idx, req)
		if len(want) == 1 {
			if err != nil {
				rt.Fatalf("unique match rejected: %v", err)
			}
			if got.Digest != want[0].Digest {
				rt.Fatalf("selected %s, want %s", got.Digest, want[0].Digest)
			}
		} else if err == nil {
			rt.Fatalf("%d matches but selection succeeded with %s", len(want), got.Digest)
		}
	})
}
