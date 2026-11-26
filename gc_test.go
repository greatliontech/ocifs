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
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// The collection's report is nameable at the library surface, and GC
// returns it by that name: a consumer holds GC to its word through
// the type (REQ-api-gc).
func TestGCResultNameable(t *testing.T) {
	var gc func(*OCIFS, context.Context, ...GCOption) (*GCResult, error) = (*OCIFS).GC
	if gc == nil {
		t.Fatal("GC does not return the report by its name")
	}
}

// A hold keeps an acquired image and its export through an emptying
// and a grace-ignored collection, reported among the kept; released,
// the next collection reclaims it (REQ-api-hold).
func TestHoldKeepsTheExportThroughEmptying(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	refStr := u.Host + "/test/hold:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, testPlatformImage(t, amd64, "held", "ok")); err != nil {
		t.Fatal(err)
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-hold"))
	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")), WithDefaultPlatform(amd64))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ofs.Close() })

	img, err := ofs.Pull(t.Context(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	// The hold comes first; the export under it is the one vouched for.
	hold, err := ofs.Hold(t.Context(), img)
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := img.Export(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	kept, err := ofs.RemoveAll(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0] != img.Digest().String() {
		t.Fatalf("kept = %v, want the held image", kept)
	}
	if _, err := ofs.GC(t.Context(), GCIgnoreGrace()); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(rootfs); err != nil || len(entries) == 0 {
		t.Fatalf("the held image's export after the emptying: %v, %v", entries, err)
	}
	if _, err := img.Export(t.Context()); err != nil {
		t.Fatalf("the held image exports after the emptying: %v", err)
	}
	if err := hold.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := hold.Release(t.Context()); err != nil {
		t.Fatalf("a second release: %v", err)
	}
	if _, err := ofs.GC(t.Context(), GCIgnoreGrace()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rootfs); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the export survived the release and a collection: %v", err)
	}
	if _, err := ofs.Hold(t.Context(), img); !errors.Is(err, ErrGone) {
		t.Fatalf("a hold over a collected image: %v, want ErrGone", err)
	}
	if _, err := ofs.Hold(t.Context(), nil); err == nil {
		t.Fatal("a hold over no image was granted")
	}
}
