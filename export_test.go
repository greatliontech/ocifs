package ocifs

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// TestExportSurface pins REQ-api-export: export by reference into
// the store-managed cache (digest-keyed, served as-is on repeat) and
// into a caller-supplied target, with platform selection and the
// verification seam governing acquisition exactly as for Pull.
func TestExportSurface(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	arm64v8 := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	amdImg := testPlatformImage(t, amd64, "which", "amd")
	armImg := testPlatformImage(t, arm64v8, "which", "arm")
	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amdImg, Descriptor: v1.Descriptor{Platform: &amd64}},
		mutate.IndexAddendum{Add: armImg, Descriptor: v1.Descriptor{Platform: &arm64v8}},
	)
	refStr := u.Host + "/test/export:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, idx); err != nil {
		t.Fatal(err)
	}

	scratch := filepath.Join(".scratch", "ocifs-export")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })

	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithDefaultPlatform(amd64),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Store-managed cache, keyed by the platform-selected child.
	out, err := ofs.Export(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	amdDigest, err := amdImg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(out) != amdDigest.Hex {
		t.Fatalf("cache export at %q, want keyed by the served child %s", out, amdDigest.Hex)
	}
	if b, err := os.ReadFile(filepath.Join(out, "which")); err != nil || string(b) != "amd" {
		t.Fatalf("exported content = %q, %v", b, err)
	}
	out2, err := ofs.Export(context.Background(), refStr)
	if err != nil || out2 != out {
		t.Fatalf("repeat export = %q, %v; want the same cache entry", out2, err)
	}

	// Explicit platform keys its own entry.
	armOut, err := ofs.Export(context.Background(), refStr, ExportWithPlatform(v1.Platform{OS: "linux", Architecture: "arm64"}))
	if err != nil {
		t.Fatal(err)
	}
	if armOut == out {
		t.Fatal("distinct platforms share one export entry")
	}
	if b, err := os.ReadFile(filepath.Join(armOut, "which")); err != nil || string(b) != "arm" {
		t.Fatalf("arm export content = %q, %v", b, err)
	}

	// Caller-supplied target.
	target := filepath.Join(scratch, "rootfs")
	got, err := ofs.Export(context.Background(), refStr, ExportWithTargetPath(target))
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Fatalf("target export returned %q, want %q", got, target)
	}
	if b, err := os.ReadFile(filepath.Join(target, "which")); err != nil || string(b) != "amd" {
		t.Fatalf("target export content = %q, %v", b, err)
	}

	// The seam governs export acquisition like any acquisition.
	rejected := errors.New("untrusted")
	guarded, err := New(
		WithWorkDir(filepath.Join(scratch, "guarded")),
		WithDefaultPlatform(amd64),
		WithVerifier(func(ctx context.Context, id ResolvedIdentity) error { return rejected }),
	)
	if err != nil {
		t.Fatal(err)
	}
	var verr *VerificationError
	if _, err := guarded.Export(context.Background(), refStr); !errors.As(err, &verr) || !errors.Is(err, rejected) {
		t.Fatalf("rejected export returned %v, want VerificationError", err)
	}
}
