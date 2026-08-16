//go:build linux

package ocifs

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"golang.org/x/sys/unix"
)

// acceptanceMount builds a writable mount over the caller-owned
// fixture image plus a fresh upper.
func acceptanceMount(t *testing.T, name string) (*ImageMount, *OCIFS, string) {
	t.Helper()
	ofs, refStr := writableFixtureEnv(t, name)
	upperDir := filepath.Join(".scratch", "ocifs-"+name, "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	im, err := ofs.Mount(refStr, MountWithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { im.Unmount() })
	return im, ofs, refStr
}

// TestAcceptanceExecAndMmap pins the exec and mmap paths: a binary
// written through the mount executes from it, and mapped pages read
// back written content (REQ-proj-content over the write path).
func TestAcceptanceExecAndMmap(t *testing.T) {
	im, ofs, refStr := acceptanceMount(t, "wexec")
	mnt := im.MountPoint()

	// Exec: a shell script written and chmodded through the mount.
	script := filepath.Join(mnt, "hello.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho exec-ok\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(script).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "exec-ok") {
		t.Fatalf("exec from mount: %q %v", out, err)
	}

	// Exec a real ELF copied through the mount (the mmap-exec path).
	shPath, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh binary to copy: %v", err)
	}
	elf, err := os.ReadFile(shPath)
	if err != nil {
		t.Fatal(err)
	}
	binPath := filepath.Join(mnt, "shcopy")
	if err := os.WriteFile(binPath, elf, 0o755); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(mnt, "mapped")
	want := []byte("mmap-content-roundtrip")
	if err := os.WriteFile(dataPath, want, 0o644); err != nil {
		t.Fatal(err)
	}

	// Remount before every read assertion: the kernel page cache
	// could otherwise serve just-written pages without a FUSE read —
	// the fresh mount forces the provider's read and mmap paths.
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	im2, err := ofs.Mount(refStr, MountWithUpperDir(im.UpperPath()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { im2.Unmount() })
	mnt = im2.MountPoint()
	script = filepath.Join(mnt, "hello.sh")
	binPath = filepath.Join(mnt, "shcopy")
	dataPath = filepath.Join(mnt, "mapped")

	out, err = exec.Command(script).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "exec-ok") {
		t.Fatalf("script exec after remount: %q %v", out, err)
	}
	out, err = exec.Command(binPath, "-c", "echo elf-ok").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "elf-ok") {
		t.Fatalf("ELF exec from mount: %q %v", out, err)
	}

	// mmap read-back of written content through the fresh mount.
	f, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mem, err := unix.Mmap(int(f.Fd()), 0, len(want), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer unix.Munmap(mem)
	if !bytes.Equal(mem, want) {
		t.Fatalf("mapped bytes %q", mem)
	}
}

// TestAcceptanceConcurrentBuild runs a parallel make on the mount —
// deferring loudly where the toolchain is absent.
func TestAcceptanceConcurrentBuild(t *testing.T) {
	makeBin, err := exec.LookPath("make")
	if err != nil {
		t.Skip("deferred: no make on this machine")
	}
	ccBin, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("deferred: no cc on this machine")
	}
	im, _, _ := acceptanceMount(t, "wbuild")
	mnt := im.MountPoint()

	src := map[string]string{
		"Makefile": "all: app\napp: a.o b.o main.o\n\t$(CC) -o app a.o b.o main.o\n%.o: %.c\n\t$(CC) -c -o $@ $<\n",
		"a.c":      "int a(void){return 20;}\n",
		"b.c":      "int b(void){return 22;}\n",
		"main.c":   "extern int a(void); extern int b(void);\n#include <stdio.h>\nint main(void){printf(\"%d\\n\", a()+b()); return 0;}\n",
	}
	proj := filepath.Join(mnt, "proj")
	if err := os.Mkdir(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range src {
		if err := os.WriteFile(filepath.Join(proj, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(makeBin, "-j4")
	cmd.Dir = proj
	cmd.Env = append(os.Environ(), "CC="+ccBin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -j4 on the mount: %v\n%s", err, out)
	}
	run, err := exec.Command(filepath.Join(proj, "app")).Output()
	if err != nil || strings.TrimSpace(string(run)) != "42" {
		t.Fatalf("built app: %q %v", run, err)
	}
}

// usernsSupported probes unprivileged user-namespace creation,
// once; a false answer may also mean an LSM denial — either way the
// workload cannot run here.
var usernsOnce = struct {
	done bool
	ok   bool
}{}

func usernsSupported() bool {
	if usernsOnce.done {
		return usernsOnce.ok
	}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	usernsOnce.done, usernsOnce.ok = true, cmd.Run() == nil
	return usernsOnce.ok
}

// TestAcceptanceContainerExec chroots into the writable mount inside
// an unprivileged user namespace — the dominant single-uid
// deployment (container root mapped to the caller) — writes as
// ns-root, verifying install-like writes land through the mount
// (REQ-writable-fidelity's user-namespace case). Hermetic: the
// payload is a static binary compiled on the spot.
func TestAcceptanceContainerExec(t *testing.T) {
	if !usernsSupported() {
		t.Skip("deferred: unprivileged user namespaces unavailable")
	}
	im, _, _ := acceptanceMount(t, "wctr")
	mnt := im.MountPoint()

	// Provision the "container": a static payload binary compiled on
	// the spot (the Go toolchain runs this test, so it is present)
	// that behaves install-like — mkdir, write, chmod, read back.
	payloadSrc := `package main

import (
	"fmt"
	"os"
)

func main() {
	if err := os.MkdirAll("/usr/pkg", 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile("/usr/pkg/manifest", []byte("v1"), 0o644); err != nil {
		panic(err)
	}
	if err := os.Chmod("/usr/pkg/manifest", 0o600); err != nil {
		panic(err)
	}
	b, err := os.ReadFile("/usr/pkg/manifest")
	if err != nil || string(b) != "v1" {
		panic(fmt.Sprintf("readback %q %v", b, err))
	}
	if fi, err := os.Stat("/etc"); err != nil || !fi.IsDir() {
		panic("no /etc in container")
	}
	fmt.Println("container-ok")
}
`
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "main.go"), []byte(payloadSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "go.mod"), []byte("module payload\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", filepath.Join(tmp, "payload"), ".")
	build.Dir = tmp
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("payload build: %v\n%s", err, out)
	}
	elf, err := os.ReadFile(filepath.Join(tmp, "payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mnt, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "bin", "payload"), elf, 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("/bin/payload")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		Chroot:      mnt,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "container-ok") {
		t.Fatalf("container exec: %v\n%s", err, out)
	}
	// The install landed on the upper through the mount.
	b, err := os.ReadFile(filepath.Join(mnt, "usr", "pkg", "manifest"))
	if err != nil || string(b) != "v1" {
		t.Fatalf("installed file: %q %v", b, err)
	}
}

// TestAcceptanceCapabilityXattr stores security.capability through
// the mount as user-namespace root — the kernel checks CAP_SETFCAP
// before any filesystem sees the store, so the realistic caller IS
// ns-root — reads it back in-namespace, and commits it into a
// genuine layer record (REQ-writable-fidelity end to end; the
// unprivileged provider escapes what its own host uid cannot store).
func TestAcceptanceCapabilityXattr(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: the escape arm needs a refused store")
	}
	if !usernsSupported() {
		t.Skip("deferred: unprivileged user namespaces unavailable")
	}
	im, ofs, refStr := acceptanceMount(t, "wcaps")
	mnt := im.MountPoint()

	p := filepath.Join(mnt, "capbin")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Store as ns-root: setcap holds CAP_SETFCAP in its namespace.
	if _, err := exec.LookPath("setcap"); err != nil {
		t.Skip("deferred: no setcap binary on this machine")
	}
	if _, err := exec.LookPath("getcap"); err != nil {
		t.Skip("deferred: no getcap binary on this machine")
	}
	cmd := exec.Command("setcap", "cap_net_raw+p", p)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("setcap in userns through the mount: %v\n%s", err, out)
	}
	// Read back the way the deployment does: getcap inside the same
	// namespace mapping (the kernel converts namespaced capability
	// blobs per-reader; raw init-ns reads are not the contract).
	getcapInNS := func(path string) string {
		cmd := exec.Command("getcap", path)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Cloneflags:  syscall.CLONE_NEWUSER,
			UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
			GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("getcap %s: %v\n%s", path, err, out)
		}
		return string(out)
	}
	if got := getcapInNS(p); !strings.Contains(got, "cap_net_raw") {
		t.Fatalf("capability read-back through the mount: %q", got)
	}

	// Commit: the capability lands as a genuine layer record and
	// serves from the committed image identically.
	committed, err := ofs.Commit(t.Context(), refStr, CommitWithUpperDir(im.UpperPath()))
	if err != nil {
		t.Fatal(err)
	}
	cim, err := ofs.Mount(LocalRef(committed.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	if got := getcapInNS(filepath.Join(cim.MountPoint(), "capbin")); !strings.Contains(got, "cap_net_raw") {
		t.Fatalf("committed capability: %q", got)
	}
	if err := cim.Unmount(); err != nil {
		t.Fatal(err)
	}
	// A SECOND mount serves the layer through the persisted index
	// document — the binary-safe encoding, not the first mount's
	// in-memory parse.
	cim2, err := ofs.Mount(LocalRef(committed.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	defer cim2.Unmount()
	if got := getcapInNS(filepath.Join(cim2.MountPoint(), "capbin")); !strings.Contains(got, "cap_net_raw") {
		t.Fatalf("index-served capability: %q", got)
	}
}

// TestAcceptancePackageManager runs a real package-manager install
// in a container rooted on the mount — env-gated: it needs a
// network-pulled image and explicit opt-in.
func TestAcceptancePackageManager(t *testing.T) {
	img := os.Getenv("OCIFS_ACCEPTANCE_IMAGE")
	if img == "" {
		t.Skip("deferred: set OCIFS_ACCEPTANCE_IMAGE (e.g. alpine:3) and network access to run the package-manager workload")
	}
	if !usernsSupported() {
		t.Skip("deferred: unprivileged user namespaces unavailable")
	}
	scratch := filepath.Join(".scratch", "ocifs-wpkg")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(scratch) })
	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")))
	if err != nil {
		t.Fatal(err)
	}
	upperDir := filepath.Join(scratch, "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	im, err := ofs.Mount(img, MountWithUpperDir(upperDir))
	if err != nil {
		t.Fatalf("mounting %s: %v", img, err)
	}
	defer im.Unmount()

	cmd := exec.Command("/bin/sh", "-c", "exec /sbin/apk add --no-network --force-broken-world busybox-suid")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		Chroot:      im.MountPoint(),
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("package manager in container: %v\n%s", err, out)
	}
	t.Logf("apk: %s", out)
}

// TestExportPrivilegedOwnership exercises export's privileged chown
// arm inside a WIDE user namespace (subuid-delegated via
// unshare --map-auto): the mapped root applies DISTINCT recorded
// ownership natively, and recorded setuid survives — pinning both
// the chown itself and the chown-before-chmod ordering (a
// wrongly-ordered chown by root would clear the bit). The folded
// issue's third pin (chown to ids outside the mapping is EINVAL) is
// structural under any mapping and needs no test.
func TestExportPrivilegedOwnership(t *testing.T) {
	if os.Getenv("OCIFS_USERNS_EXPORT_CHILD") != "" {
		exportPrivilegedChild(t)
		return
	}
	if err := exec.Command("unshare", "--user", "--map-root-user", "--map-auto", "true").Run(); err != nil {
		t.Skipf("deferred: wide user-namespace mappings unavailable (unshare --map-auto: %v)", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("unshare", "--user", "--map-root-user", "--map-auto", exe,
		"-test.run", "^TestExportPrivilegedOwnership$", "-test.v")
	cmd.Env = append(os.Environ(), "OCIFS_USERNS_EXPORT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "child-ownership-ok") {
		t.Fatalf("userns export child: %v\n%s", err, out)
	}
}

// exportPrivilegedChild runs as wide-mapped ns-root: exports an
// image with distinct recorded ownership and a setuid entry,
// asserting both are applied natively.
func exportPrivilegedChild(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("child expected to run as ns-root")
	}
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "owned", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 12, Gid: 34, Size: 2},
		{Name: "suidbin", Typeflag: tar.TypeReg, Mode: 0o4755, Uid: 12, Gid: 34, Size: 2},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		io.WriteString(tw, "xx")
	}
	tw.Close()
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
	refStr := u.Host + "/test/uns-export:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(".scratch", "ocifs-wexport-uns")
	os.RemoveAll(scratch)
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(scratch)
	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ofs.Pull(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(scratch, "out")
	if _, err := ofs.Export(t.Context(), refStr, ExportWithTargetPath(target)); err != nil {
		t.Fatalf("export as wide ns-root: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(filepath.Join(target, "owned"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != 12 || st.Gid != 34 {
		t.Fatalf("recorded ownership not applied natively: %d:%d", st.Uid, st.Gid)
	}
	if err := syscall.Lstat(filepath.Join(target, "suidbin"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != 12 || st.Mode&0o6777 != 0o4755 {
		t.Fatalf("setuid ordering: uid %d mode %o", st.Uid, st.Mode&0o7777)
	}
	fmt.Println("child-ownership-ok")
}
