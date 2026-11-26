//go:build linux

package mount

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"golang.org/x/sys/unix"
)

func acceptanceMount(t *testing.T, name string) (*ImageMount, *ocifs.OCIFS, string) {
	t.Helper()
	ofs, refStr := writableFixtureEnv(t, name)
	upperDir := filepath.Join(".scratch", "ocifs-"+name, "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	im, err := Mount(ofs, refStr, WithUpperDir(upperDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { im.Unmount() })
	return im, ofs, refStr
}

func TestAcceptanceExecAndMmap(t *testing.T) {
	if os.Getenv("OCIFS_MMAP_CHILD_PATH") != "" {
		mmapChild(t)
		return
	}
	im, ofs, refStr := acceptanceMount(t, "wexec")
	mnt := im.MountPoint()

	script := filepath.Join(mnt, "hello.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho exec-ok\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := execViaHostShell(script).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "exec-ok") {
		t.Fatalf("exec from mount: %q %v", out, err)
	}

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

	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	im2, err := Mount(ofs, refStr, WithUpperDir(im.UpperPath()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { im2.Unmount() })
	mnt = im2.MountPoint()
	script = filepath.Join(mnt, "hello.sh")
	binPath = filepath.Join(mnt, "shcopy")
	dataPath = filepath.Join(mnt, "mapped")

	out, err = execViaHostShell(script).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "exec-ok") {
		t.Fatalf("script exec after remount: %q %v", out, err)
	}
	out, err = execViaHostShell(binPath, "-c", "echo elf-ok").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "elf-ok") {
		t.Fatalf("ELF exec from mount: %q %v", out, err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(exe, "-test.run", "^TestAcceptanceExecAndMmap$")
	child.Env = append(os.Environ(),
		"OCIFS_MMAP_CHILD_PATH="+dataPath,
		"OCIFS_MMAP_CHILD_WANT="+string(want))
	out, err = child.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "mmap-ok") {
		t.Fatalf("mmap child: %q %v", out, err)
	}
}

func execViaHostShell(path string, args ...string) *exec.Cmd {
	argv := append([]string{"-c", `exec "$0" "$@"`, path}, args...)
	return exec.Command("/bin/sh", argv...)
}

func mmapChild(t *testing.T) {
	p := os.Getenv("OCIFS_MMAP_CHILD_PATH")
	want := []byte(os.Getenv("OCIFS_MMAP_CHILD_WANT"))
	f, err := os.Open(p)
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
		t.Fatalf("mapped bytes %q, want %q", mem, want)
	}
	fmt.Println("mmap-ok")
}

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
	cmd := exec.Command("/bin/sh", "-c", `CDPATH= cd "$0" && exec "$1" -j4`, proj, makeBin)
	cmd.Env = append(os.Environ(), "CC="+ccBin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -j4 on the mount: %v\n%s", err, out)
	}
	run, err := execViaHostShell(filepath.Join(proj, "app")).Output()
	if err != nil || strings.TrimSpace(string(run)) != "42" {
		t.Fatalf("built app: %q %v", run, err)
	}
}

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

func TestAcceptanceContainerExec(t *testing.T) {
	if !usernsSupported() {
		t.Skip("deferred: unprivileged user namespaces unavailable")
	}
	im, _, _ := acceptanceMount(t, "wctr")
	mnt := im.MountPoint()

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
	b, err := os.ReadFile(filepath.Join(mnt, "usr", "pkg", "manifest"))
	if err != nil || string(b) != "v1" {
		t.Fatalf("installed file: %q %v", b, err)
	}
}

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

	committed, err := ofs.Commit(t.Context(), refStr, ocifs.CommitWithUpperDir(im.UpperPath()))
	if err != nil {
		t.Fatal(err)
	}
	cim, err := Mount(ofs, ocifs.LocalRef(committed.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	if got := getcapInNS(filepath.Join(cim.MountPoint(), "capbin")); !strings.Contains(got, "cap_net_raw") {
		t.Fatalf("committed capability: %q", got)
	}
	if err := cim.Unmount(); err != nil {
		t.Fatal(err)
	}
	cim2, err := Mount(ofs, ocifs.LocalRef(committed.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	defer cim2.Unmount()
	if got := getcapInNS(filepath.Join(cim2.MountPoint(), "capbin")); !strings.Contains(got, "cap_net_raw") {
		t.Fatalf("index-served capability: %q", got)
	}
}

func TestAcceptancePackageManager(t *testing.T) {
	img := os.Getenv("OCIFS_ACCEPTANCE_IMAGE")
	if img == "" {
		t.Skip("deferred: set OCIFS_ACCEPTANCE_IMAGE (e.g. alpine:3) and network access to run the package-manager workload")
	}
	if !usernsSupported() {
		t.Skip("deferred: unprivileged user namespaces unavailable")
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-wpkg"))
	ofs, err := ocifs.New(ocifs.WithWorkDir(filepath.Join(scratch, "work")))
	if err != nil {
		t.Fatal(err)
	}
	upperDir := filepath.Join(scratch, "up")
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		t.Fatal(err)
	}
	im, err := Mount(ofs, img, WithUpperDir(upperDir))
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
