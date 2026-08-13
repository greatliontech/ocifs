package ocifs

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

	scratch := filepath.Join(".scratch", "ocifs-pull")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })

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
	scratch := filepath.Join(".scratch", "ocifs-mountfail")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })

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
	scratch := filepath.Join(".scratch", "ocifs-badstore")
	if err := os.MkdirAll(filepath.Join(scratch, "oci"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })
	if err := os.WriteFile(filepath.Join(scratch, "oci", "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(WithWorkDir(scratch)); err == nil {
		t.Fatal("construction succeeded over an unrecognized store layout")
	}
}
