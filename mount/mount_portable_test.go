package mount

import (
	"archive/tar"
	"bytes"
	"errors"
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
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestReportTypesNameable pins that Report, ReportEntry, Disposition,
// and Reason (plus their constants) are exported at the mount library
// boundary on all platforms so consumers can switch exhaustively on
// projection reports.
func TestReportTypesNameable(t *testing.T) {
	var fn func(*ocifs.OCIFS, string) (Report, bool, error) = ReportOf
	if fn == nil {
		t.Fatal("ReportOf signature mismatch")
	}
	disps := []Disposition{DispositionOmitted, DispositionAltered, DispositionResidual}
	reasons := []Reason{
		ReasonSymlinkUnsupported,
		ReasonFIFOUnsupported,
		ReasonDeviceUnsupported,
		ReasonCaseCollision,
		ReasonKindUnknown,
		ReasonNameUnrepresentable,
		ReasonResidualForeignFile,
	}
	if len(disps) != 3 || len(reasons) != 7 {
		t.Fatalf("unexpected exported constant count: %d %d", len(disps), len(reasons))
	}
	entry := ReportEntry{Path: "x", Disposition: DispositionOmitted, Reason: ReasonFIFOUnsupported}
	if (Report{Entries: []ReportEntry{entry}}).Entries[0].Reason != ReasonFIFOUnsupported {
		t.Fatal("ReportEntry round-trip mismatch")
	}
}

func TestMountAndReportValidationGuards(t *testing.T) {
	if _, err := Mount(nil, "registry.invalid/x:v1"); err == nil || !strings.Contains(err.Error(), "nil OCIFS store") {
		t.Fatalf("Mount(nil, ...) = %v, want nil OCIFS store error", err)
	}
	if _, _, err := ReportOf(nil, "id"); err == nil || !strings.Contains(err.Error(), "nil OCIFS store") {
		t.Fatalf("ReportOf(nil, ...) = %v, want nil OCIFS store error", err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "mount-portable"))
	ofs, err := ocifs.New(ocifs.WithWorkDir(filepath.Join(scratch, "work")), ocifs.WithPullPolicy(ocifs.PullNever))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ofs.Close() })

	if _, err := Mount(ofs, "registry.invalid/x:v1", nil, WithUpperDir("a"), WithNamedUpper("b")); err == nil || !strings.Contains(err.Error(), "takes at most one upper") {
		t.Fatalf("Mount with both WithUpperDir and WithNamedUpper = %v, want upper-conflict error", err)
	}
	if _, err := Mount(ofs, "registry.invalid/x:v1", nil, WithContext(nil)); err == nil || strings.Contains(err.Error(), "takes at most one upper") {
		t.Fatalf("Mount of absent image under PullNever = %v, want acquisition failure", err)
	}

	// Verify ClaimUpper rejects path traversal before creating any lock files.
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, "x"); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	layerBytes := buf.Bytes()
	lyr, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(layerBytes)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, lyr)
	if err != nil {
		t.Fatal(err)
	}
	refStr := u.Host + "/test/upperguard:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	liveWork := filepath.Join(scratch, "livework")
	liveOfs, err := ocifs.New(ocifs.WithWorkDir(liveWork))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { liveOfs.Close() })

	escaped := filepath.Join(scratch, "escaped-upper-lock")
	for _, bad := range []string{"a/../forged", "a/../ingest", "a/../../escaped-upper-lock", "..", `a\b`} {
		if _, err := Mount(liveOfs, refStr, WithNamedUpper(bad)); err == nil || !strings.Contains(err.Error(), "not a single path element") {
			t.Fatalf("Mount WithNamedUpper(%q) = %v, want single-path-element error", bad, err)
		}
	}
	ents, err := os.ReadDir(filepath.Join(liveWork, "locks"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.Name() != "ingest" {
			t.Fatalf("rejected WithNamedUpper left unexpected locks entry %q", e.Name())
		}
	}
	if _, err := os.Stat(escaped); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected WithNamedUpper created %s outside store: %v", escaped, err)
	}
}
