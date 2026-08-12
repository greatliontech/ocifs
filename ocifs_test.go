package ocifs

import (
	"archive/tar"
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// TestMountLocalImage mounts an image served by an in-process
// registry on a loopback socket and reads it back through the FUSE
// mountpoint: the full pull → store → unify → mount path with no
// external network.
func TestMountLocalImage(t *testing.T) {
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

	workDir := filepath.Join(".scratch", "ocifs", "work")
	mnt := filepath.Join(".scratch", "ocifs", "mnt")
	for _, d := range []string{workDir, mnt} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			// A prior run killed mid-mount (mutation testing does
			// this) leaves a disconnected FUSE endpoint here; detach
			// it and retry once.
			exec.Command("fusermount3", "-u", mnt).Run()
			exec.Command("fusermount", "-u", mnt).Run()
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Join(".scratch", "ocifs")) })

	ofs, err := New(WithWorkDir(workDir))
	if err != nil {
		t.Fatal(err)
	}
	im, err := ofs.Mount(refStr, MountWithTargetPath(mnt))
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

	var hash v1.Hash = im.img.Hash()
	if hash == (v1.Hash{}) {
		t.Fatal("empty image hash")
	}
}
