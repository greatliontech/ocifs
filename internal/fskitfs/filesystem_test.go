package fskitfs

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	fskit "github.com/greatliontech/fskit-go"

	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/store"
)

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig("file:///stores/s1", []string{"image=r.io/a@sha256:abc,platform=linux/arm64", "extra=proc,extra=sys/x", "state=/stores/s1/mounts/m1"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store != "/stores/s1" || cfg.Image != "r.io/a@sha256:abc" || cfg.Platform != "linux/arm64" || cfg.State != "/stores/s1/mounts/m1" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if strings.Join(cfg.ExtraDirs, "|") != "proc|sys/x" {
		t.Fatalf("extras = %v", cfg.ExtraDirs)
	}

	// Percent-encoded values survive the option syntax: the darwin
	// app-group container path always carries a space.
	spacey := "/Users/u/Library/Group Containers/g.ocifs/store/mounts/m1"
	cfg, err = ParseConfig("/stores/s1", []string{"image=r.io/a@sha256:abc,state=" + url.QueryEscape(spacey)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.State != spacey {
		t.Fatalf("space-bearing state = %q, want %q", cfg.State, spacey)
	}

	// A plain path resource works like a file URL.
	cfg, err = ParseConfig("/stores/s2", []string{"image=r.io/a@sha256:abc"})
	if err != nil || cfg.Store != "/stores/s2" {
		t.Fatalf("plain path: %+v, %v", cfg, err)
	}

	if _, err := ParseConfig("/stores/s", nil); err == nil {
		t.Fatal("missing image accepted")
	}
	// The appex never re-resolves tags (REQ-store-digest-entry).
	if _, err := ParseConfig("/stores/s", []string{"image=r.io/a:latest"}); err == nil {
		t.Fatal("tag-form image accepted")
	}
	if _, err := ParseConfig("", nil); err == nil {
		t.Fatal("empty resource accepted")
	}
}

// TestLoadVolumeEndToEnd exercises the whole appex serving path on
// linux: a store populated through a loopback registry, then
// loadVolume under PullNever with the network gone — declarative
// config in, serving volume and persisted report out
// (REQ-proj-server, REQ-proj-report).
func TestLoadVolumeEndToEnd(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := tar.Header{Name: "hello.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5}
	if err := tw.WriteHeader(&hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, "world"); err != nil {
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
	refStr := u.Host + "/test/fskit:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	// The orchestrator's side: populate the store (scratchtest keeps
	// every write inside the campaign's observation bracket).
	scratch := scratchtest.Dir(t, "fskitfs")
	storeDir := filepath.Join(scratch, "store")
	s, err := store.NewStore(storeDir, anonKeychain{}, store.PullIfNotPresent, v1.Platform{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	simg, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	digestRef := u.Host + "/test/fskit@" + simg.Hash().String()

	// The appex's side: the registry is gone; everything is cached.
	srv.Close()

	stateDir := filepath.Join(scratch, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Store: storeDir, Image: digestRef, ExtraDirs: []string{"anchor"}, State: stateDir}
	vol, id, err := loadVolume(cfg)
	if err != nil {
		t.Fatalf("loadVolume offline: %v", err)
	}

	root, err := vol.Activate(fskit.TaskOptions{})
	if err != nil {
		t.Fatal(err)
	}
	item, _, err := vol.Lookup(root, "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	buf2 := make([]byte, 16)
	n, err := vol.Read(item, 0, buf2)
	if err != nil || string(buf2[:n]) != "world" {
		t.Fatalf("served content = %q, %v", buf2[:n], err)
	}
	if _, _, err := vol.Lookup(root, "anchor"); err != nil {
		t.Fatalf("extra dir: %v", err)
	}

	rep, err := projection.ReadReportFile(filepath.Join(stateDir, projection.ReportFileName))
	if err != nil || rep.Entries == nil {
		t.Fatalf("appex-side report: %+v, %v", rep, err)
	}

	// Identity is stable across loads (VolumeIdentity contract).
	_, id2, err := loadVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if id != id2 {
		t.Fatalf("volume identity unstable: %v vs %v", id, id2)
	}
	if id.UUID == (fskit.UUID{}) || id.Name == "" {
		t.Fatalf("identity unset: %+v", id)
	}

	// A missing state directory fails loudly: the report must have a
	// per-mount home, and silent self-registration would leak state
	// (docs/issues/mount-state-lifecycle.md).
	cfg2 := cfg
	cfg2.State = ""
	if _, _, err := loadVolume(cfg2); err == nil {
		t.Fatal("state-less load accepted")
	}
}

// TestEnumerateCookieWrapDoesNotPanic: the kernel forwards cookies
// verbatim; a wrapped value must end the enumeration, never index.
func TestEnumerateCookieWrapDoesNotPanic(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)
	p := &collectPacker{}
	v, err := vol.Enumerate(root, fskit.DirCookie(1<<63), 0, 0, p)
	if err != nil || v != readOnlyVerifier || len(p.names) != 0 {
		t.Fatalf("wrapped cookie: v=%d err=%v names=%v", v, err, p.names)
	}
}

// TestProbeRecognizesOnlyStoreLayouts pins recognition by the
// store's layout signature (REQ-store-adopt).
func TestProbeRecognizesOnlyStoreLayouts(t *testing.T) {
	scratch := scratchtest.Dir(t, "fskitfs")
	storeDir := filepath.Join(scratch, "probe-store")
	if _, err := store.NewStore(storeDir, anonKeychain{}, store.PullNever, v1.Platform{}, nil); err != nil {
		t.Fatal(err)
	}
	res, err := FileSystem{}.Probe(fakeResource(storeDir))
	if err != nil || res.Match != fskit.MatchUsable {
		t.Fatalf("store probe = %+v, %v", res, err)
	}
	res, err = FileSystem{}.Probe(fakeResource(scratch))
	if err != nil || res.Match != fskit.MatchNotRecognized {
		t.Fatalf("non-store probe = %+v, %v", res, err)
	}
	// The bridge delivers file:// absolute-string URLs; the marker
	// check must see through the scheme. Scratch paths stay relative
	// (observation-bracket hygiene), so the file-URL branch is pinned
	// against a deterministic absolute non-store: the scheme parses,
	// the marker stat misses.
	res, err = FileSystem{}.Probe(fakeResource("file:///"))
	if err != nil || res.Match != fskit.MatchNotRecognized {
		t.Fatalf("file-URL non-store probe = %+v, %v", res, err)
	}
}

// fakeResource is a URL-backed test resource.
type fakeResource string

func (r fakeResource) Revoked() bool                          { return false }
func (r fakeResource) URL() (string, bool)                    { return string(r), true }
func (r fakeResource) BlockDevice() (fskit.BlockDevice, bool) { return nil, false }
