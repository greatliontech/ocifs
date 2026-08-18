//go:build linux

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
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/gmdb/oslock"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

var wfixMtime = time.Date(2022, 5, 6, 7, 8, 9, 0, time.UTC)

// writableFixtureEnv pushes an image whose entries the invoking user
// owns — default_permissions enforces PRESENTED attributes, so an
// unprivileged caller can only write beneath entries it could own
// (the uid-0 image case is the user-namespace deployment).
func writableFixtureEnv(t *testing.T, scratchName string, extra ...Option) (*OCIFS, string) {
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
	opts := append([]Option{WithWorkDir(filepath.Join(scratch, "work"))}, extra...)
	ofs, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return ofs, refStr
}

// TestWritableMountSurface pins REQ-api-mount-writable end to end:
// mount over an upper, mutate through the mount, unmount leaving the
// upper intact, remount observing the changes, commit, and mount the
// committed image read-only observing the same tree.
func TestWritableMountSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wsurf")
	upperDir := filepath.Join(".scratch", "ocifs-wsurf", "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Both upper forms at once: refused.
	if _, err := ofs.Mount(refStr, MountWithUpperDir(upperDir), MountWithNamedUpper("x")); err == nil ||
		!strings.Contains(err.Error(), "at most one upper") {
		t.Fatalf("double upper: %v", err)
	}

	im, err := ofs.Mount(refStr, MountWithUpperDir(upperDir))
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

	// The upper outlives the mount; a remount presents the changes.
	im2, err := ofs.Mount(refStr, MountWithUpperDir(upperDir))
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

	// Commit the upper and mount the result read-only.
	ctx := t.Context()
	committed, err := ofs.Commit(ctx, refStr, CommitWithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	cim, err := ofs.Mount(LocalRef(committed.Digest()))
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

// TestWritableNamedUpper pins the store-managed arm: creation on
// first use, base binding refusing a different image, and commit by
// name.
func TestWritableNamedUpper(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wnamed")

	im, err := ofs.Mount(refStr, MountWithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatal(err)
	}
	// One writable mount at a time per named upper: a second is
	// refused while the first serves, admitted after unmount.
	if _, err := ofs.Mount(refStr, MountWithNamedUpper("scratchpad")); err == nil ||
		!strings.Contains(err.Error(), "already serves") {
		t.Fatalf("second writable mount: %v", err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "note"), []byte("n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	im2, err := ofs.Mount(refStr, MountWithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatalf("remount after unmount: %v", err)
	}
	if err := im2.Unmount(); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	committed, err := ofs.Commit(ctx, refStr, CommitWithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatal(err)
	}
	// The same named upper refuses a different base: the committed
	// image is a different digest.
	if _, err := ofs.Mount(LocalRef(committed.Digest()), MountWithNamedUpper("scratchpad")); err == nil ||
		!strings.Contains(err.Error(), "bound to base") {
		t.Fatalf("foreign base accepted: %v", err)
	}
	// The refusal released the upper claim it took before the
	// binding check — a leaked claim would refuse this remount (and
	// RemoveUpper) forever in this process.
	im3, err := ofs.Mount(refStr, MountWithNamedUpper("scratchpad"))
	if err != nil {
		t.Fatalf("upper claim leaked by refused mount: %v", err)
	}
	if err := im3.Unmount(); err != nil {
		t.Fatal(err)
	}
}

// TestMountIDReusableAfterUnmount pins the reuse clause of
// REQ-store-mount-registry: a clean unmount releases the id — the
// rowless state directory is adopted by the next mount, not
// refused.
func TestMountIDReusableAfterUnmount(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wreuse")
	im, err := ofs.Mount(refStr, MountWithID("reused-id"))
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	// The row is gone with the unmount (deregistration).
	if _, _, err := ofs.MountReport("reused-id"); err == nil {
		t.Fatal("mount row survived clean unmount")
	}
	im2, err := ofs.Mount(refStr, MountWithID("reused-id"))
	if err != nil {
		t.Fatalf("id reuse after clean unmount: %v", err)
	}
	if err := im2.Unmount(); err != nil {
		t.Fatal(err)
	}
}

// TestNamedUpperClaimPrecedesUpperCreation pins writable.md
// REQ-writable-base-binding's ordering: the upper's serve claim is
// acquired BEFORE any bookkeeping write for the upper, so a mount
// refused by a held claim leaves no upper tree and no binding —
// nothing a concurrent RemoveUpper holding that same claim could
// race against. The holder is simulated directly on the claim file
// the spec names (locks/upper-<name>).
func TestNamedUpperClaimPrecedesUpperCreation(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wupperclaim")
	work := filepath.Join(".scratch", "ocifs-wupperclaim", "work")
	holder, err := oslock.TryAcquire(filepath.Join(work, "locks", "upper-ux"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ofs.Mount(refStr, MountWithNamedUpper("ux")); err == nil ||
		!strings.Contains(err.Error(), "already serves") {
		// The substring binds the refusal to the upper-claim
		// mechanism: a mis-derived work path would fail some other
		// way and pass vacuously.
		t.Fatalf("mount against a held upper claim: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "uppers", "ux")); !os.IsNotExist(err) {
		t.Fatalf("refused mount created the upper tree: %v", err)
	}
	holder.Close()
	im, err := ofs.Mount(refStr, MountWithNamedUpper("ux"))
	if err != nil {
		t.Fatalf("mount after release: %v", err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	// The claim retired with the unmount: the file is gone and the
	// name is immediately claimable.
	if _, err := os.Stat(filepath.Join(work, "locks", "upper-ux")); !os.IsNotExist(err) {
		t.Fatalf("upper claim file survived unmount: %v", err)
	}
}
