//go:build linux

package store

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"pgregory.net/rapid"
)

// exportFixture pushes an image whose layer exercises the fidelity
// surface the store path must not disturb: a setuid binary sharing
// CAS content with a plain file, and a subdirectory.
func exportFixture(t *testing.T, rt handlerTransport, refStr string) {
	t.Helper()
	l := newRawLayer(t, tarBytes(t,
		tdir("bin"),
		tarEntry{
			hdr:     tar.Header{Name: "bin/su", Typeflag: tar.TypeReg, Mode: 0o4755, Size: int64(len("shared bits"))},
			content: []byte("shared bits"),
		},
		tfile("copy", "shared bits"),
		tfile("plain", "other"),
	))
	push(t, rt, refStr, makeImage(t, l))
}

// snapshotModes maps every file under root to its full mode.
func snapshotModes(t *testing.T, root string) map[string]fs.FileMode {
	t.Helper()
	out := map[string]fs.FileMode{}
	err := filepath.Walk(root, func(p string, fi fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		out[rel] = fi.Mode()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestExportCachedServedAsIs pins REQ-export-cache: the first export
// materializes under exports/<algo>/<hex> keyed by the served child
// digest, and a second export of the same digest returns the same
// tree without re-materialization — proven by a marker planted in
// the cached tree surviving the second call.
func TestExportCachedServedAsIs(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/export/cache:v1"
	exportFixture(t, reg, refStr)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	path1, err := s.Export(img)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "exports", img.Hash().Algorithm, img.Hash().Hex)
	if path1 != want {
		t.Fatalf("export path = %q, want digest-keyed %q", path1, want)
	}
	if b, err := os.ReadFile(filepath.Join(path1, "copy")); err != nil || string(b) != "shared bits" {
		t.Fatalf("exported content = %q, %v", b, err)
	}

	marker := filepath.Join(path1, "bin", ".cache-marker")
	if err := os.WriteFile(marker, []byte("m"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A cached serve happens without re-materialization: with the
	// CAS damaged, only a serve that touches no blob can succeed.
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := view.Lookup("plain")
	if !ok {
		t.Fatal("fixture entry missing")
	}
	blob := s.BlobPath(e.Digest)
	if err := os.Rename(blob, blob+".away"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(blob+".away", blob)

	path2, err := s.Export(img)
	if err != nil {
		t.Fatalf("cached export touched the store: %v", err)
	}
	if path2 != path1 {
		t.Fatalf("second export path %q differs from first %q", path2, path1)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("second export re-materialized the cached tree: %v", err)
	}
}

// TestExportImmutableStore pins REQ-export-immutable: CAS bytes,
// modes, and link counts are identical around an export whose
// headers carry setuid bits — the hostile-header chmod must land on
// the exported copy, never the shared blob.
func TestExportImmutableStore(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/export/immutable:v1"
	exportFixture(t, reg, refStr)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	blobs := filepath.Join(dir, "blobs")
	before := snapshotModes(t, blobs)

	out, err := s.Export(img)
	if err != nil {
		t.Fatal(err)
	}
	if !sameModeMap(before, snapshotModes(t, blobs)) {
		t.Fatal("export changed CAS modes or population")
	}
	// The suid file's CAS blob keeps link count 1: the export copied,
	// never linked (REQ-export-copy).
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := view.Lookup("bin/su")
	if !ok {
		t.Fatal("fixture entry missing")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(s.BlobPath(e.Digest), &st); err != nil {
		t.Fatal(err)
	}
	if st.Nlink != 1 {
		t.Fatalf("CAS blob link count = %d after export, want 1", st.Nlink)
	}
	exp, _ := os.Stat(filepath.Join(out, "bin", "su"))
	blob, _ := os.Stat(s.BlobPath(e.Digest))
	if os.SameFile(exp, blob) {
		t.Fatal("exported file shares the CAS blob's inode")
	}
	if exp.Mode()&fs.ModeSetuid == 0 {
		t.Fatal("exported file lost its setuid bit")
	}
	if blob.Mode()&fs.ModeSetuid != 0 {
		t.Fatal("CAS blob gained the header's setuid bit")
	}
}

func sameModeMap(a, b map[string]fs.FileMode) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestExportAtomicOnFailure pins REQ-export-atomic: an export
// interrupted by a damaged CAS leaves nothing at the final path —
// never a partial tree — and a later repaired attempt still succeeds.
func TestExportAtomicOnFailure(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/export/atomic:v1"
	exportFixture(t, reg, refStr)

	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := view.Lookup("plain")
	if !ok {
		t.Fatal("fixture entry missing")
	}
	blob := s.BlobPath(e.Digest)
	damaged := blob + ".away"
	if err := os.Rename(blob, damaged); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Export(img); err == nil {
		t.Fatal("export succeeded with a damaged CAS")
	}
	final := filepath.Join(dir, "exports", img.Hash().Algorithm, img.Hash().Hex)
	if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("failed export left state at the final path: %v", err)
	}

	if err := os.Rename(damaged, blob); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Export(img); err != nil {
		t.Fatalf("repaired export failed: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(final, "plain")); err != nil || string(b) != "other" {
		t.Fatalf("repaired export content = %q, %v", b, err)
	}
}

// TestExportToCallerTarget pins the caller-supplied arm: an absent
// target is created atomically, and any existing target — empty or
// populated — refuses undisturbed (no-replace rename).
func TestExportToCallerTarget(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/export/target:v1"
	exportFixture(t, reg, refStr)

	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}

	scratch := scratchDir(t)
	absent := filepath.Join(scratch, "fresh")
	if err := s.ExportTo(view, absent); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(absent, "copy")); err != nil || string(b) != "shared bits" {
		t.Fatalf("caller-target content = %q, %v", b, err)
	}

	empty := filepath.Join(scratch, "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.ExportTo(view, empty); err == nil {
		t.Fatal("export replaced an existing (empty) target")
	}

	fileTarget := filepath.Join(scratch, "occupied")
	if err := os.WriteFile(fileTarget, []byte("caller data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.ExportTo(view, fileTarget); err == nil {
		t.Fatal("export replaced an existing file target")
	}
	if b, err := os.ReadFile(fileTarget); err != nil || string(b) != "caller data" {
		t.Fatalf("failed export disturbed the file target: %q, %v", b, err)
	}

	populated := filepath.Join(scratch, "populated")
	if err := os.MkdirAll(filepath.Join(populated, "keep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.ExportTo(view, populated); err == nil {
		t.Fatal("export replaced a populated target")
	}
	if _, err := os.Stat(filepath.Join(populated, "keep")); err != nil {
		t.Fatalf("failed export disturbed the populated target: %v", err)
	}
}

// TestExportOwnershipUnprivileged pins REQ-export-ownership: without
// privilege, foreign uid/gid headers do not fail the export and the
// tree belongs to the invoking user.
func TestExportOwnershipUnprivileged(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: the unprivileged arm is the one under test")
	}
	reg := newTestRegistry()
	refStr := testHost + "/export/owner:v1"
	exportFixture(t, reg, refStr)

	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.Export(img)
	if err != nil {
		t.Fatalf("unprivileged export failed: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(out, "bin", "su"), &st); err != nil {
		t.Fatal(err)
	}
	if int(st.Uid) != os.Getuid() {
		t.Fatalf("exported file uid = %d, want the invoking user %d", st.Uid, os.Getuid())
	}
}

// TestPropertyExportAtomicOnDamage pins REQ-export-atomic as a
// for-all over interruption points: whichever blob the damage
// removes, a failed export leaves nothing at the final path, and a
// repaired retry serves a complete tree.
func TestPropertyExportAtomicOnDamage(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		reg := newTestRegistry()
		refStr := testHost + "/export/atomicprop:v1"
		files := rapid.IntRange(1, 4).Draw(rt, "files")
		entries := []tarEntry{tdir("d")}
		names := make([]string, files)
		for i := 0; i < files; i++ {
			names[i] = fmt.Sprintf("d/f%d", i)
			entries = append(entries, tfile(names[i], fmt.Sprintf("content-%d", i)))
		}
		push(t, reg, refStr, makeImage(t, newRawLayer(t, tarBytes(t, entries...))))

		s, dir := newTestStore(t, PullIfNotPresent, reg)
		img, err := s.Image(context.Background(), refStr, nil)
		if err != nil {
			rt.Fatal(err)
		}
		view, err := img.Unify()
		if err != nil {
			rt.Fatal(err)
		}
		victim := names[rapid.IntRange(0, files-1).Draw(rt, "victim")]
		e, ok := view.Lookup(victim)
		if !ok {
			rt.Fatalf("victim %q missing from view", victim)
		}
		blob := s.BlobPath(e.Digest)
		if err := os.Rename(blob, blob+".away"); err != nil {
			rt.Fatal(err)
		}

		if _, err := s.Export(img); err == nil {
			rt.Fatal("export succeeded with a damaged CAS")
		}
		final := filepath.Join(dir, "exports", img.Hash().Algorithm, img.Hash().Hex)
		if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
			rt.Fatalf("failed export observable at the final path: %v", err)
		}

		if err := os.Rename(blob+".away", blob); err != nil {
			rt.Fatal(err)
		}
		if _, err := s.Export(img); err != nil {
			rt.Fatalf("repaired export failed: %v", err)
		}
		for i, n := range names {
			b, err := os.ReadFile(filepath.Join(final, filepath.FromSlash(n)))
			if err != nil || string(b) != fmt.Sprintf("content-%d", i) {
				rt.Fatalf("entry %q = %q, %v", n, b, err)
			}
		}
	})
}
