//go:build windows && amd64

package ocifs

import (
	"archive/tar"
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	"github.com/greatliontech/ocifs/internal/projection"
)

// TestMountWindowsEndToEnd exercises the full windows path — pull
// through the store, ProjFS mount via the library surface, read,
// report persistence, unmount residue (REQ-api-mountpoint) — on a
// real machine with ProjFS enabled.
func TestMountWindowsEndToEnd(t *testing.T) {
	skipUnderMutationCampaign(t)
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "docs", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "docs/hello.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Name == "docs/hello.txt" {
			if _, err := io.WriteString(tw, "world"); err != nil {
				t.Fatal(err)
			}
		}
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
	refStr := u.Host + "/test/winmount:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	work := filepath.Join(t.TempDir(), "work")
	ofs, err := New(WithWorkDir(work), WithExtraDirs([]string{"anchor"}))
	if err != nil {
		t.Fatal(err)
	}
	im, err := ofs.Mount(refStr, MountWithID("win-e2e"))
	if err != nil {
		t.Fatal(err)
	}
	defer im.Unmount()

	got, err := os.ReadFile(filepath.Join(im.MountPoint(), "docs", "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "world" {
		t.Fatalf("content = %q", got)
	}
	if fi, err := os.Stat(filepath.Join(im.MountPoint(), "anchor")); err != nil || !fi.IsDir() {
		t.Fatalf("extra dir: %v %v", fi, err)
	}

	rep, err := projection.ReadReportFile(filepath.Join(work, "mounts", "win-e2e", projection.ReportFileName))
	if err != nil || rep.Entries == nil {
		t.Fatalf("report: %+v %v", rep, err)
	}

	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(im.MountPoint())
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("mountpoint not empty after unmount: %v", ents)
	}
}
