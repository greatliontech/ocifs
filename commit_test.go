//go:build linux

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
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/greatliontech/ocifs/internal/upper"
)

// TestCommitSurface pins REQ-api-commit: base by reference with the
// seam governing, upper in either form, no live mount; the result
// materializes at LocalRef and exports like any image.
func TestCommitSurface(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	refStr := u.Host + "/test/commitbase:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, testPlatformImage(t, v1.Platform{OS: "linux", Architecture: "amd64"}, "hello", "world")); err != nil {
		t.Fatal(err)
	}

	scratch := filepath.Join(".scratch", "ocifs-commit")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })

	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")))
	if err != nil {
		t.Fatal(err)
	}

	// Caller-supplied upper.
	upDir := filepath.Join(scratch, "up")
	if err := os.MkdirAll(upDir, 0o755); err != nil {
		t.Fatal(err)
	}
	w := upper.NewWriter(upDir)
	if err := w.PublishFile("hello", strings.NewReader("rewritten"), 0o644, time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatal(err)
	}

	img, err := ofs.Commit(context.Background(), refStr, CommitWithUpperDir(upDir))
	if err != nil {
		t.Fatal(err)
	}

	// The committed image exports like any image, offline.
	offline, err := New(WithWorkDir(filepath.Join(scratch, "work")), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	out, err := offline.Export(context.Background(), LocalRef(img.Digest()))
	if err != nil {
		t.Fatalf("committed image did not export offline: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(out, "hello")); err != nil || string(b) != "rewritten" {
		t.Fatalf("committed content = %q, %v", b, err)
	}

	// Named upper: created against the resolved base, then committed.
	base, err := ofs.Pull(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	namedDir, err := ofs.store.NewUpper("session", base.Digest())
	if err != nil {
		t.Fatal(err)
	}
	nw := upper.NewWriter(namedDir)
	if err := nw.Whiteout("hello"); err != nil {
		t.Fatal(err)
	}
	img2, err := ofs.Commit(context.Background(), refStr, CommitWithNamedUpper("session"))
	if err != nil {
		t.Fatal(err)
	}
	out2, err := offline.Export(context.Background(), LocalRef(img2.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out2, "hello")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("whited-out entry survived: %v", err)
	}

	// Option misuse.
	if _, err := ofs.Commit(context.Background(), refStr); err == nil {
		t.Fatal("commit with no upper accepted")
	}
	if _, err := ofs.Commit(context.Background(), refStr, CommitWithUpperDir(upDir), CommitWithNamedUpper("session")); err == nil {
		t.Fatal("commit with two uppers accepted")
	}

	// The seam governs the base acquisition.
	rejected := errors.New("untrusted")
	guarded, err := New(
		WithWorkDir(filepath.Join(scratch, "guarded")),
		WithVerifier(func(ctx context.Context, id ResolvedIdentity) error { return rejected }),
	)
	if err != nil {
		t.Fatal(err)
	}
	var verr *VerificationError
	if _, err := guarded.Commit(context.Background(), refStr, CommitWithUpperDir(upDir)); !errors.As(err, &verr) {
		t.Fatalf("rejected commit returned %v, want VerificationError", err)
	}
}
