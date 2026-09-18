//go:build linux

package ocifs

import (
	"context"
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

// TestExportFromCommittedImage pins that a committed image is an
// acquired image (REQ-api-export): it exports from its own handle, the
// upper's content included, with no second acquisition. Linux-gated
// like every commit test: the commit arm is not built elsewhere.
func TestExportFromCommittedImage(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	img := testPlatformImage(t, amd64, "which", "amd")
	refStr := u.Host + "/test/export-committed:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-export-committed"))
	seamRuns := 0
	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithDefaultPlatform(amd64),
		WithVerifier(func(ctx context.Context, id ResolvedIdentity) error { seamRuns++; return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	upDir := filepath.Join(scratch, "upper")
	if err := os.MkdirAll(upDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upDir, "added"), []byte("later"), 0o644); err != nil {
		t.Fatal(err)
	}
	committed, err := ofs.Commit(context.Background(), refStr, CommitWithUpperDir(upDir))
	if err != nil {
		t.Fatal(err)
	}
	runsAfterCommit := seamRuns
	cout, err := committed.Export(context.Background())
	if err != nil {
		t.Fatalf("committed image export: %v", err)
	}
	if seamRuns != runsAfterCommit {
		t.Fatalf("export from the committed image ran the seam (%d → %d)", runsAfterCommit, seamRuns)
	}
	if b, err := os.ReadFile(filepath.Join(cout, "added")); err != nil || string(b) != "later" {
		t.Fatalf("committed export content = %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(cout, "which")); err != nil || string(b) != "amd" {
		t.Fatalf("committed export base content = %q, %v", b, err)
	}
	if filepath.Base(cout) != committed.Digest().Hex {
		t.Fatalf("export keyed by %s, committed digest %s", filepath.Base(cout), committed.Digest().Hex)
	}
}
