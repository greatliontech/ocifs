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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/gmdb/oslock"
	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

var wfixMtime = time.Date(2022, 5, 6, 7, 8, 9, 0, time.UTC)

func writableFixtureEnv(t *testing.T, scratchName string, extra ...ocifs.Option) (*ocifs.OCIFS, string) {
	t.Helper()
	skipUnderMutationCampaign(t)
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Getuid(), os.Getgid()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "./", Typeflag: tar.TypeDir, Mode: 0o755, Uid: uid, Gid: gid, ModTime: wfixMtime},
		{Name: "etc", Typeflag: tar.TypeDir, Mode: 0o755, Uid: uid, Gid: gid, ModTime: wfixMtime},
		{Name: "etc/conf", Typeflag: tar.TypeReg, Mode: 0o644, Uid: uid, Gid: gid, ModTime: wfixMtime, Size: 8},
		{Name: "etc/keep", Typeflag: tar.TypeReg, Mode: 0o644, Uid: uid, Gid: gid, ModTime: wfixMtime, Size: 4},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		switch h.Name {
		case "etc/conf":
			io.WriteString(tw, "old-conf")
		case "etc/keep":
			io.WriteString(tw, "keep")
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
	refStr := u.Host + "/test/" + scratchName + ":v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-"+scratchName))
	opts := append([]ocifs.Option{ocifs.WithWorkDir(filepath.Join(scratch, "work"))}, extra...)
	ofs, err := ocifs.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return ofs, refStr
}

func TestWritableMountSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wsurf")
	upperDir := filepath.Join(".scratch", "ocifs-wsurf", "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := Mount(ofs, refStr, WithUpperDir(upperDir), WithNamedUpper("x")); err == nil ||
		!strings.Contains(err.Error(), "at most one upper") {
		t.Fatalf("double upper: %v", err)
	}

	im, err := Mount(ofs, refStr, WithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	mnt := im.MountPoint()
	if im.UpperPath() != upperDir {
		t.Fatalf("UpperPath %q", im.UpperPath())
	}

	if err := os.WriteFile(filepath.Join(mnt, "etc", "new"), []byte("fresh"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "etc", "conf"), []byte("new-conf"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mnt, "etc", "keep")); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}

	im2, err := Mount(ofs, refStr, WithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	mnt = im2.MountPoint()
	b, err := os.ReadFile(filepath.Join(mnt, "etc", "new"))
	if err != nil || string(b) != "fresh" {
		t.Fatalf("remount new: %q %v", b, err)
	}
	b, err = os.ReadFile(filepath.Join(mnt, "etc", "conf"))
	if err != nil || string(b) != "new-conf" {
		t.Fatalf("remount conf: %q %v", b, err)
	}
	if _, err := os.Lstat(filepath.Join(mnt, "etc", "keep")); !os.IsNotExist(err) {
		t.Fatalf("deleted file after remount: %v", err)
	}
	if err := im2.Unmount(); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	committed, err := ofs.Commit(ctx, refStr, ocifs.CommitWithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	cim, err := Mount(ofs, ocifs.LocalRef(committed.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	cmnt := cim.MountPoint()
	b, err = os.ReadFile(filepath.Join(cmnt, "etc", "new"))
	if err != nil || string(b) != "fresh" {
		t.Fatalf("committed new: %q %v", b, err)
	}
	b, err = os.ReadFile(filepath.Join(cmnt, "etc", "conf"))
	if err != nil || string(b) != "new-conf" {
		t.Fatalf("committed conf: %q %v", b, err)
	}
	if _, err := os.Lstat(filepath.Join(cmnt, "etc", "keep")); !os.IsNotExist(err) {
		t.Fatalf("deleted file in committed image: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cmnt, "etc", "denied"), nil, 0o600); err == nil {
		t.Fatal("committed mount is writable")
	}
	if err := cim.Unmount(); err != nil {
		t.Fatal(err)
	}
}

func TestWritableNamedUpper(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wnamed")

	im, err := Mount(ofs, refStr, WithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Mount(ofs, refStr, WithNamedUpper("scratchpad")); err == nil ||
		!strings.Contains(err.Error(), "already serves") {
		t.Fatalf("second writable mount: %v", err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "note"), []byte("n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	im2, err := Mount(ofs, refStr, WithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatalf("remount after unmount: %v", err)
	}
	if err := im2.Unmount(); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	committed, err := ofs.Commit(ctx, refStr, ocifs.CommitWithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Mount(ofs, ocifs.LocalRef(committed.Digest()), WithNamedUpper("scratchpad")); err == nil ||
		!strings.Contains(err.Error(), "bound to base") {
		t.Fatalf("foreign base accepted: %v", err)
	}
	im3, err := Mount(ofs, refStr, WithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatalf("upper claim leaked by refused mount: %v", err)
	}
	if err := im3.Unmount(); err != nil {
		t.Fatal(err)
	}
}

func TestMountIDReusableAfterUnmount(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wreuse")
	im, err := Mount(ofs, refStr, WithID("reused-id"))
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReportOf(ofs, "reused-id"); err == nil {
		t.Fatal("mount row survived clean unmount")
	}
	im2, err := Mount(ofs, refStr, WithID("reused-id"))
	if err != nil {
		t.Fatalf("id reuse after clean unmount: %v", err)
	}
	if err := im2.Unmount(); err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateLiveMountIDRefusedWithoutDetachingLiveMount(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wduplive")
	im, err := Mount(ofs, refStr, WithID("live-id"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if im != nil {
			if err := im.Unmount(); err != nil {
				t.Fatal(err)
			}
		}
	}()

	if got, err := os.ReadFile(filepath.Join(im.MountPoint(), "etc", "keep")); err != nil || string(got) != "keep" {
		t.Fatalf("initial live mount content = %q, %v; want keep", got, err)
	}

	// Sequential duplicate mount with the same ID must fail without
	// detaching the live mount's kernel mountpoint or deleting mounts/live-id.
	if _, err := Mount(ofs, refStr, WithID("live-id")); err == nil || !strings.Contains(err.Error(), "in use by a live mount") {
		t.Fatalf("sequential duplicate live ID = %v, want in-use error", err)
	}

	// Concurrent duplicate mounts with the same ID must also all fail cleanly.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m, err := Mount(ofs, refStr, WithID("live-id")); err == nil {
				_ = m.Unmount()
				t.Errorf("concurrent duplicate live ID succeeded")
			} else if !strings.Contains(err.Error(), "in use by a live mount") {
				t.Errorf("concurrent duplicate live ID = %v, want in-use error", err)
			}
		}()
	}
	wg.Wait()

	// The original live mount must still serve its files intact.
	got, err := os.ReadFile(filepath.Join(im.MountPoint(), "etc", "keep"))
	if err != nil {
		t.Fatalf("live mount detached or stateDir deleted by duplicate ID attempt: %v", err)
	}
	if string(got) != "keep" {
		t.Fatalf("live mount content = %q, want keep", got)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	im = nil
}

func TestNamedUpperClaimPrecedesUpperCreation(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wupperclaim")
	work := filepath.Join(".scratch", "ocifs-wupperclaim", "work")
	holder, err := oslock.TryAcquire(filepath.Join(work, "locks", "upper-ux"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Mount(ofs, refStr, WithNamedUpper("ux")); err == nil ||
		!strings.Contains(err.Error(), "already serves") {
		t.Fatalf("mount against a held upper claim: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "uppers", "ux")); !os.IsNotExist(err) {
		t.Fatalf("refused mount created the upper tree: %v", err)
	}
	holder.Close()
	im, err := Mount(ofs, refStr, WithNamedUpper("ux"))
	if err != nil {
		t.Fatalf("mount after release: %v", err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(work, "locks", "upper-ux")); !os.IsNotExist(err) {
		t.Fatalf("upper claim file survived unmount: %v", err)
	}
}
