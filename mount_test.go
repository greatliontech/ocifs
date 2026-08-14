//go:build linux

package ocifs

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
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/projection"
)

var fixtureMtime = time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)

// fixtureEnv pushes a crafted image to a loopback registry and
// returns a library instance over a scratch work directory plus the
// image reference and the work dir. The image exercises the linux
// fidelity envelope: a directory with distinctive mode/owner/time,
// file content, a verbatim symlink, and a FIFO.
func fixtureEnv(t *testing.T, scratchName string) (*OCIFS, string, string) {
	t.Helper()
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
		{Name: "docs", Typeflag: tar.TypeDir, Mode: 0o710, Uid: 12, Gid: 34, ModTime: fixtureMtime},
		{Name: "docs/a.txt", Typeflag: tar.TypeReg, Mode: 0o604, Uid: 12, Gid: 34, ModTime: fixtureMtime, Size: 11},
		{Name: "docs/hard", Typeflag: tar.TypeLink, Linkname: "docs/a.txt"},
		{Name: "docs/link", Typeflag: tar.TypeSymlink, Linkname: "a.txt", Mode: 0o777},
		{Name: "null", Typeflag: tar.TypeChar, Mode: 0o666, Devmajor: 1, Devminor: 3},
		{Name: "pipe", Typeflag: tar.TypeFifo, Mode: 0o644},
		{Name: "shadow", Typeflag: tar.TypeReg, Mode: 0, ModTime: fixtureMtime},
		{Name: "suid", Typeflag: tar.TypeReg, Mode: 0o4755, ModTime: fixtureMtime},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Name == "docs/a.txt" {
			if _, err := io.WriteString(tw, "hello mount"); err != nil {
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
	refStr := u.Host + "/test/" + scratchName + ":v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	scratch := filepath.Join(".scratch", "ocifs-"+scratchName)
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })
	work := filepath.Join(scratch, "work")

	ofs, err := New(WithWorkDir(work), WithExtraDirs([]string{"proc", "x/y"}))
	if err != nil {
		t.Fatal(err)
	}
	return ofs, refStr, work
}

func mountFixture(t *testing.T, scratchName string, opts ...MountOption) (*ImageMount, string, string) {
	t.Helper()
	ofs, refStr, work := fixtureEnv(t, scratchName)
	im, err := ofs.Mount(refStr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { im.Unmount() })
	return im, refStr, work
}

func ino(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("no Stat_t")
	}
	return st.Ino
}

// TestMountFidelityEnvelope pins the FUSE column of
// REQ-proj-fidelity end to end: recorded modes, ownership,
// timestamps, sizes, verbatim symlinks, typed FIFO nodes — on files,
// directories, and content alike.
func TestMountFidelityEnvelope(t *testing.T) {
	im, _, _ := mountFixture(t, "fidelity")
	mnt := im.MountPoint()

	fi, err := os.Lstat(filepath.Join(mnt, "docs"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0o710 {
		t.Fatalf("docs mode = %v, want dir 0710 (directory attributes projected)", fi.Mode())
	}
	st := fi.Sys().(*syscall.Stat_t)
	if st.Uid != 12 || st.Gid != 34 {
		t.Fatalf("docs ownership = %d:%d, want 12:34", st.Uid, st.Gid)
	}
	if !fi.ModTime().Equal(fixtureMtime) {
		t.Fatalf("docs mtime = %v, want %v", fi.ModTime(), fixtureMtime)
	}

	ffi, err := os.Lstat(filepath.Join(mnt, "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if ffi.Mode().Perm() != 0o604 || ffi.Size() != 11 {
		t.Fatalf("a.txt mode %v size %d", ffi.Mode(), ffi.Size())
	}

	target, err := os.Readlink(filepath.Join(mnt, "docs", "link"))
	if err != nil {
		t.Fatal(err)
	}
	if target != "a.txt" {
		t.Fatalf("symlink target = %q", target)
	}

	pfi, err := os.Lstat(filepath.Join(mnt, "pipe"))
	if err != nil {
		t.Fatal(err)
	}
	if pfi.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("pipe mode = %v, want a typed FIFO node", pfi.Mode())
	}

	got, err := os.ReadFile(filepath.Join(mnt, "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello mount" {
		t.Fatalf("content = %q", got)
	}

	// A recorded 0000 mode serves verbatim, never rewritten to a
	// default (the /etc/shadow class).
	sfi, err := os.Lstat(filepath.Join(mnt, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	if sfi.Mode().Perm() != 0 {
		t.Fatalf("shadow mode = %v, want 0000 served verbatim", sfi.Mode())
	}
	// Suid/sgid/sticky bits are within the FUSE envelope.
	ufi, err := os.Lstat(filepath.Join(mnt, "suid"))
	if err != nil {
		t.Fatal(err)
	}
	if ufi.Mode()&os.ModeSetuid == 0 || ufi.Mode().Perm() != 0o755 {
		t.Fatalf("suid mode = %v, want setuid|0755", ufi.Mode())
	}
	// Devices are typed nodes without device numbers.
	dfi, err := os.Lstat(filepath.Join(mnt, "null"))
	if err != nil {
		t.Fatal(err)
	}
	if dfi.Mode()&os.ModeCharDevice == 0 {
		t.Fatalf("null mode = %v, want a typed char device", dfi.Mode())
	}
	if rdev := dfi.Sys().(*syscall.Stat_t).Rdev; rdev != 0 {
		t.Fatalf("null rdev = %d, want 0 (device numbers dropped)", rdev)
	}
	// A hardlink presents as an independent regular node with the
	// target's content and nlink 1.
	hfi, err := os.Lstat(filepath.Join(mnt, "docs", "hard"))
	if err != nil {
		t.Fatal(err)
	}
	if hfi.Mode()&os.ModeType != 0 {
		t.Fatalf("hard mode = %v, want a regular file", hfi.Mode())
	}
	if nlink := hfi.Sys().(*syscall.Stat_t).Nlink; nlink != 1 {
		t.Fatalf("hard nlink = %d, want 1", nlink)
	}
	hgot, err := os.ReadFile(filepath.Join(mnt, "docs", "hard"))
	if err != nil || string(hgot) != "hello mount" {
		t.Fatalf("hard content = %q, %v", hgot, err)
	}
	// Unrecorded atime/ctime slots fall back to the modification
	// time, never a year-one artifact.
	ast := ffi.Sys().(*syscall.Stat_t)
	if ast.Atim.Sec != fixtureMtime.Unix() {
		t.Fatalf("a.txt atime sec = %d, want mtime fallback %d", ast.Atim.Sec, fixtureMtime.Unix())
	}

	// Enumeration arrives in byte order from the backend
	// (REQ-proj-enumeration FUSE arm): read raw directory order,
	// unsorted.
	df, err := os.Open(mnt)
	if err != nil {
		t.Fatal(err)
	}
	names, err := df.Readdirnames(-1)
	df.Close()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("readdir order not byte-sorted: %q before %q", names[i-1], names[i])
		}
	}

	// Extra directories appear as empty directories
	// (REQ-api-extra-dirs), and their unrecorded modification time
	// presents as the Unix epoch — never a year-one zero-time
	// artifact (REQ-proj-fidelity).
	for _, p := range []string{"proc", "x/y"} {
		efi, err := os.Lstat(filepath.Join(mnt, p))
		if err != nil || !efi.IsDir() {
			t.Fatalf("extra dir %q: %v", p, err)
		}
		ents, err := os.ReadDir(filepath.Join(mnt, p))
		if err != nil || len(ents) != 0 {
			t.Fatalf("extra dir %q not empty: %v %v", p, ents, err)
		}
		est := efi.Sys().(*syscall.Stat_t)
		if est.Mtim.Sec != 0 || est.Ctim.Sec != 0 {
			t.Fatalf("extra dir %q times = m%d c%d, want epoch fallback", p, est.Mtim.Sec, est.Ctim.Sec)
		}
	}
}

// TestMountInodesAreKernelIDs pins REQ-proj-identity end to end: the
// root is inode 2, view entries carry view-range inodes, extra
// directories carry synthetic-range inodes, and a remount of the
// same image — through a fresh instance, offline — projects
// identical numbers.
func TestMountInodesAreKernelIDs(t *testing.T) {
	im, refStr, work := mountFixture(t, "inodes")
	mnt := im.MountPoint()

	if got := ino(t, mnt); got != 2 {
		t.Fatalf("root inode = %d, want 2", got)
	}
	paths := []string{"docs", "docs/a.txt", "docs/link", "pipe", "proc", "x", "x/y"}
	first := map[string]uint64{}
	for _, p := range paths {
		first[p] = ino(t, filepath.Join(mnt, p))
	}
	for _, p := range []string{"docs", "docs/a.txt", "pipe"} {
		if first[p] < 16 || first[p] >= 1<<62 {
			t.Fatalf("view entry %q inode %d outside the view range", p, first[p])
		}
	}
	for _, p := range []string{"proc", "x", "x/y"} {
		if first[p] < 1<<62 {
			t.Fatalf("extra dir %q inode %d inside the synthetic range", p, first[p])
		}
	}

	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	ofs2, err := New(WithWorkDir(work), WithExtraDirs([]string{"proc", "x/y"}), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	im2, err := ofs2.Mount(refStr, MountWithID("re"))
	if err != nil {
		t.Fatal(err)
	}
	defer im2.Unmount()
	for _, p := range paths {
		if got := ino(t, filepath.Join(im2.MountPoint(), p)); got != first[p] {
			t.Fatalf("remount inode of %q = %d, want %d (REQ-proj-identity)", p, got, first[p])
		}
	}
}

// TestMountReadOnlyKernelEnforced pins REQ-api-mount-ro /
// REQ-proj-ro on FUSE: the kernel denies every mutation class with
// EROFS.
func TestMountReadOnlyKernelEnforced(t *testing.T) {
	im, _, _ := mountFixture(t, "ro")
	mnt := im.MountPoint()

	if err := os.WriteFile(filepath.Join(mnt, "new"), []byte("x"), 0o644); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("create: err = %v, want EROFS", err)
	}
	if err := os.Mkdir(filepath.Join(mnt, "newdir"), 0o755); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("mkdir: err = %v, want EROFS", err)
	}
	if err := os.Remove(filepath.Join(mnt, "pipe")); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("remove: err = %v, want EROFS", err)
	}
	if err := os.Rename(filepath.Join(mnt, "pipe"), filepath.Join(mnt, "pipe2")); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("rename: err = %v, want EROFS", err)
	}
	if err := os.Chmod(filepath.Join(mnt, "docs"), 0o777); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("chmod: err = %v, want EROFS", err)
	}
	if err := os.Symlink("t", filepath.Join(mnt, "s")); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("symlink: err = %v, want EROFS", err)
	}
}

// TestMountShortReadOnlyAtEOF pins REQ-proj-content's read contract:
// a read crossing EOF returns exactly the remaining bytes, and reads
// at every offset equal the recorded content.
func TestMountShortReadOnlyAtEOF(t *testing.T) {
	im, _, _ := mountFixture(t, "shortread")
	f, err := os.Open(filepath.Join(im.MountPoint(), "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	content := "hello mount"
	buf := make([]byte, 64)
	n, err := f.ReadAt(buf, 6)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(content)-6 || string(buf[:n]) != content[6:] {
		t.Fatalf("ReadAt(6) = %d %q, want %q (short read only at EOF)", n, buf[:n], content[6:])
	}
	for off := 0; off <= len(content); off++ {
		n, err := f.ReadAt(buf[:3], int64(off))
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		want := content[off:min(off+3, len(content))]
		if string(buf[:n]) != want {
			t.Fatalf("ReadAt(%d) = %q, want %q", off, buf[:n], want)
		}
	}
}

// TestMountReportPersisted pins the per-mount arm of
// REQ-proj-report: every mount writes its projection report into the
// state directory beside the mnt/ mountpoint, readable while the
// mount is live.
func TestMountReportPersisted(t *testing.T) {
	im, _, work := mountFixture(t, "report", MountWithID("withreport"))

	r, err := projection.ReadReportFile(filepath.Join(work, "mounts", "withreport", projection.ReportFileName))
	if err != nil {
		t.Fatalf("report not persisted: %v", err)
	}
	if r.Entries == nil || len(r.Entries) != 0 {
		t.Fatalf("full-envelope mount report = %+v, want present empty entries", r)
	}
	if im.MountPoint() != mustAbs(t, filepath.Join(work, "mounts", "withreport", "mnt")) {
		t.Fatalf("mountpoint = %q, want the state dir's mnt/ sibling", im.MountPoint())
	}
}

func mustAbs(t *testing.T, p string) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(cwd, p))
}

// TestMountIDValidation pins REQ-api-mount-id: an id with separators
// or dot-elements is rejected and creates no state, so no id can
// address anything outside the mounts/ tier.
func TestMountIDValidation(t *testing.T) {
	ofs, refStr, work := fixtureEnv(t, "mountid")

	for _, bad := range []string{"../evil", "a/b", `a\b`, ".", ".."} {
		if _, err := ofs.Mount(refStr, MountWithID(bad)); err == nil {
			t.Fatalf("mount id %q accepted", bad)
		}
	}
	entries, err := os.ReadDir(filepath.Join(work, "mounts"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid ids left state behind: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(work, "evil")); err == nil {
		t.Fatal("traversal id escaped the mounts tier")
	}
}

// TestPropertyMountMutationsDenied pins REQ-proj-ro / REQ-api-mount-ro
// as a for-all over mutation operations and target paths against a
// live kernel-ro FUSE mount: every mutation class is denied with
// EROFS, existing or new path alike.
func TestPropertyMountMutationsDenied(t *testing.T) {
	im, _, _ := mountFixture(t, "roprop")
	mnt := im.MountPoint()

	paths := []string{".", "docs", "docs/a.txt", "docs/link", "pipe", "proc", "absent", "docs/absent"}
	ops := []string{"create", "mkdir", "remove", "rename", "chmod", "chtimes", "truncate", "symlink", "link", "openwrite"}

	rapid.Check(t, func(rt *rapid.T) {
		p := filepath.Join(mnt, rapid.SampledFrom(paths).Draw(rt, "path"))
		// New-name targets stay strictly inside the mount: a fresh
		// name within a drawn projected directory.
		fresh := filepath.Join(mnt, rapid.SampledFrom([]string{"", "docs", "proc"}).Draw(rt, "dir"), "zz-new")
		var err error
		switch op := rapid.SampledFrom(ops).Draw(rt, "op"); op {
		case "create":
			err = os.WriteFile(fresh, []byte("x"), 0o644)
		case "mkdir":
			err = os.Mkdir(fresh, 0o755)
		case "remove":
			err = os.Remove(p)
		case "rename":
			err = os.Rename(p, fresh)
		case "chmod":
			err = os.Chmod(p, 0o777)
		case "chtimes":
			err = os.Chtimes(p, time.Now(), time.Now())
		case "truncate":
			err = os.Truncate(p, 0)
		case "symlink":
			err = os.Symlink("t", fresh)
		case "link":
			err = os.Link(p, fresh)
		case "openwrite":
			// O_NONBLOCK: a read-only filesystem still permits FIFO
			// opens (IPC, not fs mutation); without a reader the
			// nonblocking writer-open fails ENXIO instead of blocking
			// the test forever.
			var f *os.File
			f, err = os.OpenFile(p, os.O_WRONLY|syscall.O_NONBLOCK, 0)
			if err == nil {
				f.Close()
				err = errors.New("open for write succeeded")
			}
		}
		if err == nil {
			rt.Fatalf("mutation succeeded on %s", p)
		}
		// EROFS is the kernel-level denial; ENOENT/EINVAL/ENOTDIR on
		// absent or mistyped paths and EBUSY on the mountpoint itself
		// (rmdir/rename of a live mount) still never mutate, but an
		// existing target must fail with a denial, never succeed.
		if !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOTDIR) && !errors.Is(err, syscall.EBUSY) && !errors.Is(err, syscall.ENXIO) && !errors.Is(err, syscall.EISDIR) && !errors.Is(err, syscall.EXDEV) {
			rt.Fatalf("mutation on %s failed with %v, want EROFS-class denial", p, err)
		}
	})

	// Spot-check the tree survived (op success was already fatal
	// above; this catches only gross damage, not a full equality
	// sweep).
	if _, err := os.Lstat(filepath.Join(mnt, "docs", "a.txt")); err != nil {
		t.Fatalf("tree damaged by denied mutations: %v", err)
	}
}
