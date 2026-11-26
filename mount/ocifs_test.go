//go:build linux

package mount

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
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestMountLocalImage mounts an image served by an in-process
// registry on a loopback socket and reads it back through the FUSE
// mountpoint: the full pull -> store -> unify -> mount path with no
// external network.
func TestMountLocalImage(t *testing.T) {
	skipUnderMutationCampaign(t)
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	writeEntry := func(hdr tar.Header, content string) {
		hdr.Size = int64(len(content))
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatal(err)
		}
		if content != "" {
			if _, err := io.WriteString(tw, content); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeEntry(tar.Header{Name: "etc", Typeflag: tar.TypeDir, Mode: 0o755}, "")
	writeEntry(tar.Header{Name: "etc/motd", Typeflag: tar.TypeReg, Mode: 0o644}, "hello from ocifs\n")
	writeEntry(tar.Header{Name: "motd", Typeflag: tar.TypeSymlink, Linkname: "etc/motd", Mode: 0o777}, "")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	tarData := buf.Bytes()

	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(tarData)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	refStr := u.Host + "/test/mount:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs"))
	workDir := filepath.Join(scratch, "work")
	mnt := filepath.Join(scratch, "mnt")
	for _, d := range []string{workDir, mnt} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	ofs, err := ocifs.New(ocifs.WithWorkDir(workDir))
	if err != nil {
		t.Fatal(err)
	}
	im, err := Mount(ofs, refStr, WithTargetPath(mnt))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := im.Unmount(); err != nil {
			t.Fatal(err)
		}
	}()

	got, err := os.ReadFile(filepath.Join(mnt, "etc", "motd"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello from ocifs\n" {
		t.Fatalf("etc/motd = %q", got)
	}
	target, err := os.Readlink(filepath.Join(mnt, "motd"))
	if err != nil {
		t.Fatal(err)
	}
	if target != "etc/motd" {
		t.Fatalf("symlink target = %q", target)
	}
	if _, err := os.ReadFile(filepath.Join(mnt, "motd")); err != nil {
		t.Fatalf("read through symlink: %v", err)
	}
	if cf := im.ConfigFile(); cf == nil {
		t.Fatal("config file not accessible")
	}

	var hash v1.Hash = im.Digest()
	if hash == (v1.Hash{}) {
		t.Fatal("empty image hash")
	}
}

// TestFailedMountLeavesNoMountDir: a mount that fails at acquisition
// must not strand an orphan directory under the store's mounts/ tier.
func TestFailedMountLeavesNoMountDir(t *testing.T) {
	skipUnderMutationCampaign(t)
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-mountfail"))

	workDir := filepath.Join(scratch, "work")
	ofs, err := ocifs.New(ocifs.WithWorkDir(workDir), ocifs.WithPullPolicy(ocifs.PullNever))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Mount(ofs, "registry.invalid/absent:v1"); err == nil {
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
