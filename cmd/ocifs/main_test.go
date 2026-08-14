//go:build linux

package main

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
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// TestCLISmoke pins REQ-api-cli network-free: the built binary
// mounts an image from a loopback registry at the required
// mountpoint, serves until signalled, unmounts on SIGTERM, and exits
// cleanly — the CLI as a consumer of the library surface.
func TestCLISmoke(t *testing.T) {
	// Excluded from mutation campaigns: the CLI mounts FUSE, and
	// mount(2) escapes the campaign's observation bracket.
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("mount-performing test skipped under mutation campaign")
	}
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := tar.Header{Name: "greeting", Typeflag: tar.TypeReg, Mode: 0o644, Size: 3}
	if err := tw.WriteHeader(&hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, "hey"); err != nil {
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
	refStr := u.Host + "/test/cli:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	scratch := filepath.Join(".scratch", "cli")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Join(".scratch")) })

	bin := filepath.Join(scratch, "ocifs-test-bin")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	mnt := filepath.Join(scratch, "mnt")
	work := filepath.Join(scratch, "work")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "-i", refStr, "-m", mnt, "-w", work)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
		// A failure between mount-up and clean exit leaves a
		// disconnected endpoint; detach it so scratch cleanup works.
		exec.Command("fusermount3", "-u", mnt).Run()
		exec.Command("fusermount", "-u", mnt).Run()
	}()

	// The mount is up when the projected file is readable.
	target := filepath.Join(mnt, "greeting")
	deadline := time.Now().Add(15 * time.Second)
	var content []byte
	for {
		content, err = os.ReadFile(target)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount never served %s: %v", target, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if string(content) != "hey" {
		t.Fatalf("greeting = %q", content)
	}

	// SIGTERM triggers unmount and a clean exit (REQ-api-cli).
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CLI exited uncleanly after SIGTERM: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("CLI did not exit after SIGTERM")
	}
	cmd.Process = nil

	// Unmounted: the projected file is gone, the mountpoint directory
	// remains (REQ-api-mountpoint).
	if _, err := os.Stat(target); err == nil {
		t.Fatal("mount still serving after SIGTERM unmount")
	}
	if fi, err := os.Stat(mnt); err != nil || !fi.IsDir() {
		t.Fatalf("mountpoint directory gone after unmount: %v", err)
	}
}
