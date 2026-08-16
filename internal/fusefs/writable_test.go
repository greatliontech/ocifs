//go:build linux

package fusefs

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// mountWritable builds a small base, a writable merge over a fresh
// upper, and a live private FUSE mount serving it. Mount-performing
// tests skip under mutation campaigns: mount(2) is a system-global
// side effect the observation bracket cannot contain.
func mountWritable(t *testing.T) (mnt, upperRoot string) {
	t.Helper()
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("mount-performing test under a mutation campaign")
	}
	dir := scratchtest.Dir(t, "fusefs")
	content := []byte("base-content")
	sum := sha256.Sum256(content)
	digest := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
	blobPath := filepath.Join(dir, digest.Hex)
	if err := os.WriteFile(blobPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	// Entries carry the invoking user's ids: default_permissions
	// enforces the PRESENTED attributes, and an unprivileged caller
	// can only write beneath entries it could own (the uid-0 image
	// case is the user-namespace deployment, exercised by the
	// acceptance workloads).
	uid, gid := os.Getuid(), os.Getgid()
	view, err := layer.Unify([]layer.Layer{{
		{Header: tar.Header{Name: ".", Typeflag: tar.TypeDir, Mode: 0o755, Uid: uid, Gid: gid}},
		{Header: tar.Header{Name: "d", Typeflag: tar.TypeDir, Mode: 0o755, Uid: uid, Gid: gid}},
		{Header: tar.Header{Name: "d/f", Typeflag: tar.TypeReg, Mode: 0o644, Uid: uid, Gid: gid, Size: int64(len(content))}, Digest: digest},
	}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := projection.New(view, nil, Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	upperRoot = filepath.Join(dir, "u")
	if err := os.Mkdir(upperRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	m, err := projection.NewMergedWritable(p, upperRoot, func(h v1.Hash) (io.ReadCloser, error) {
		return os.Open(blobPath)
	})
	if err != nil {
		t.Fatal(err)
	}
	root := NewWritable(m, func(v1.Hash) string { return blobPath })
	mnt = filepath.Join(dir, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	server, err := fs.Mount(mnt, root, &fs.Options{
		// Zero modes are presented truth, never rewritten to 644/755.
		NullPermissions: true,
		MountOptions: fuse.MountOptions{
			FsName: "ocifs-writable-test",
			Name:   "ocifs",
			// The kernel enforces the PRESENTED modes — host upper
			// bits are machinery (the mode fidelity override).
			Options: []string{"default_permissions"},
		},
	})
	if err != nil {
		t.Skipf("cannot mount fuse here: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Unmount()
	})
	return mnt, upperRoot
}

// TestWritableMountCycle drives real syscalls against a live
// writable mount: create/write/read-back, copy-up on write with
// handle continuity, mkdir/symlink, unlink/rmdir semantics, chmod,
// truncate, utimes.
func TestWritableMountCycle(t *testing.T) {
	mnt, _ := mountWritable(t)

	// Create + write + read back.
	if err := os.WriteFile(filepath.Join(mnt, "new.txt"), []byte("hello"), 0o640); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(mnt, "new.txt"))
	if err != nil || string(b) != "hello" {
		t.Fatalf("read back: %q %v", b, err)
	}
	fi, err := os.Stat(filepath.Join(mnt, "new.txt"))
	if err != nil || fi.Mode().Perm() != 0o640 {
		t.Fatalf("created mode: %v %v", fi.Mode(), err)
	}

	// Copy-up with handle continuity: a read fd opened on the base
	// file observes the copied-up object after a write.
	bf := filepath.Join(mnt, "d", "f")
	rf, err := os.Open(bf)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	wf, err := os.OpenFile(bf, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteAt([]byte("MOD!"), 0); err != nil {
		t.Fatal(err)
	}
	wf.Close()
	got := make([]byte, 4)
	if _, err := rf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "MOD!" {
		t.Fatalf("pre-copy-up handle reads %q", got)
	}

	// mkdir + symlink.
	if err := os.Mkdir(filepath.Join(mnt, "nd"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(mnt, "nd", "sl")); err != nil {
		t.Fatal(err)
	}
	tgt, err := os.Readlink(filepath.Join(mnt, "nd", "sl"))
	if err != nil || tgt != "target" {
		t.Fatalf("readlink: %q %v", tgt, err)
	}

	// rmdir non-empty refused, then emptied and removed.
	if err := syscall.Rmdir(filepath.Join(mnt, "nd")); err != syscall.ENOTEMPTY {
		t.Fatalf("rmdir non-empty: %v", err)
	}
	if err := os.Remove(filepath.Join(mnt, "nd", "sl")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Rmdir(filepath.Join(mnt, "nd")); err != nil {
		t.Fatalf("rmdir: %v", err)
	}

	// Unlink a base file: gone from the presentation.
	if err := os.Remove(bf); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(bf); !os.IsNotExist(err) {
		t.Fatalf("unlinked base file still there: %v", err)
	}

	// chmod + truncate + utimes.
	nt := filepath.Join(mnt, "new.txt")
	if err := os.Chmod(nt, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(nt, 2); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := os.Chtimes(nt, when, when); err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat(nt)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || fi.Size() != 2 || !fi.ModTime().Equal(when) {
		t.Fatalf("attrs: %v %d %v", fi.Mode(), fi.Size(), fi.ModTime())
	}

	// Reserved names refused.
	if err := os.Mkdir(filepath.Join(mnt, ".wh.evil"), 0o755); err == nil {
		t.Fatal("reserved mkdir accepted")
	}

	// Enumeration reflects everything.
	ents, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	want := map[string]bool{"d": true, "new.txt": true}
	if len(names) != len(want) {
		t.Fatalf("readdir %v", names)
	}
	for _, n := range names {
		if !want[n] {
			t.Fatalf("readdir %v", names)
		}
	}
}

// TestWritableMountModeEnforced pins the presented-mode enforcement
// seam: with the mode record in play (mode 0), the kernel — not the
// laundered host bits — denies access.
func TestWritableMountModeEnforced(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: mode denial does not bind root")
	}
	mnt, upperRoot := mountWritable(t)
	p := filepath.Join(mnt, "secret")
	if err := os.WriteFile(p, []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(p); !os.IsPermission(err) {
		t.Fatalf("mode-0 file readable: %v", err)
	}
	fi, err := os.Stat(p)
	if err != nil || fi.Mode().Perm() != 0 {
		t.Fatalf("presented mode: %v %v", fi.Mode(), err)
	}
	// The upper keeps provider access underneath.
	hfi, err := os.Stat(filepath.Join(upperRoot, "secret"))
	if err != nil {
		t.Fatal(err)
	}
	if hfi.Mode().Perm()&0o600 != 0o600 {
		t.Fatalf("host lost provider access: %v", hfi.Mode())
	}
	// And chmod back restores readability.
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(p); err != nil || string(b) != "s" {
		t.Fatalf("restored read: %q %v", b, err)
	}
}

// TestWritableUnlinkedHandleOps pins the mkstemp pattern: attribute
// and size operations through a handle whose path was unlinked
// succeed via the descriptor.
func TestWritableUnlinkedHandleOps(t *testing.T) {
	mnt, _ := mountWritable(t)
	p := filepath.Join(mnt, "tmpfile")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("scratch"); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3); err != nil {
		t.Fatalf("ftruncate on unlinked: %v", err)
	}
	if err := f.Chmod(0o400); err != nil {
		t.Fatalf("fchmod on unlinked: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	if st.Size != 3 {
		t.Fatalf("size after ftruncate: %d", st.Size)
	}
	b := make([]byte, 8)
	n, err := f.ReadAt(b, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(b[:n]) != "scr" {
		t.Fatalf("content: %q", b[:n])
	}
}
