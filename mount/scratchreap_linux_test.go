//go:build linux

package mount

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/hanwen/go-fuse/v2/fs"
)

type wedgeRoot struct{ fs.Inode }

func wedgeChild() {
	mnt := os.Getenv("SCRATCHTEST_WEDGE_MNT")
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		fmt.Println("child:", err)
		os.Exit(1)
	}
	if _, err := fs.Mount(mnt, &wedgeRoot{}, &fs.Options{}); err != nil {
		fmt.Println("child:", err)
		os.Exit(1)
	}
	fmt.Println("ready")
	syscall.Kill(syscall.Getpid(), syscall.SIGSTOP)
	select {}
}

func wedgeSetup(t *testing.T, dir string) {
	t.Helper()
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("mount-performing test skipped under mutation campaign")
	}
	if err := scratchtest.ReapMounts(dir); err != nil {
		t.Fatalf("self-heal of prior run's residue: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		scratchtest.ReapMounts(dir)
		os.RemoveAll(dir)
	})
}

func wedgeAt(t *testing.T, mnt string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", "^TestReapRecoversWedgedMount$")
	cmd.Env = append(os.Environ(), "SCRATCHTEST_WEDGE_CHILD=1", "SCRATCHTEST_WEDGE_MNT="+mnt)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Kill(cmd.Process.Pid, syscall.SIGCONT)
		cmd.Process.Kill()
		cmd.Wait()
	})
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || !strings.Contains(line, "ready") {
		t.Fatalf("wedge child: %q %v", line, err)
	}
	for range 100 {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", cmd.Process.Pid))
		if err == nil && strings.Contains(string(b), ") T ") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("wedge child never stopped")
}

func mountsUnder(t *testing.T, dir string) []string {
	t.Helper()
	base, err := scratchtest.CanonicalBase(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := scratchtest.MountTable(base)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestReapRecoversWedgedMount(t *testing.T) {
	if os.Getenv("SCRATCHTEST_WEDGE_CHILD") != "" {
		wedgeChild()
		return
	}
	dir := filepath.Join(".scratch", "scratchtest-wedge", "reap")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if len(mountsUnder(t, dir)) == 0 {
		t.Fatal("fixture: no mount appeared under scratch dir")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- scratchtest.ReapMounts(dir) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("reap: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("reap hung on a wedged mount — it must never stat the tree")
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mounts survived reap: %v", got)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("tree not removable after reap: %v", err)
	}

	mnt := filepath.Join(dir, "mnt")
	wedgeAt(t, mnt)
	if err := scratchtest.ReapMounts(mnt); err != nil {
		t.Fatalf("reap of the mountpoint itself: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mountpoint-path reap left mounts: %v", got)
	}
}

func TestReapRecoversNestedMounts(t *testing.T) {
	dir := filepath.Join(".scratch", "scratchtest-wedge", "nested")
	wedgeSetup(t, dir)
	inner := filepath.Join(dir, "m", "sub")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	wedgeAt(t, inner)
	wedgeAt(t, filepath.Join(dir, "m"))
	if got := mountsUnder(t, dir); len(got) != 2 {
		t.Fatalf("fixture: want 2 stacked mounts, have %v", got)
	}
	if err := scratchtest.ReapMounts(dir); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mounts survived nested reap: %v", got)
	}
}

func TestReapThroughSymlinkedPath(t *testing.T) {
	dir := filepath.Join(".scratch", "scratchtest-wedge", "real")
	wedgeSetup(t, dir)
	link := filepath.Join(".scratch", "scratchtest-wedge", "link")
	os.Remove(link)
	if err := os.Symlink("real", link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(link) })
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if err := scratchtest.ReapMounts(filepath.Join(link, "mnt")); err != nil {
		t.Fatalf("reap via symlinked ancestors: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mount survived symlink-path reap: %v", got)
	}
}

func TestInRecoversWedgedMount(t *testing.T) {
	dir := filepath.Join(".scratch", "scratchtest-wedge", "in")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))

	got := scratchtest.In(t, dir)
	if len(mountsUnder(t, got)) != 0 {
		t.Fatal("In returned a directory with live mounts")
	}
	entries, err := os.ReadDir(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("In returned a non-empty directory: %v", entries)
	}
}

func TestReapLeavesSiblings(t *testing.T) {
	dir := filepath.Join(".scratch", "scratchtest-wedge", "reap-sib")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if err := scratchtest.ReapMounts(filepath.Join(".scratch", "scratchtest-wedge", "reap-si")); err != nil {
		t.Fatal(err)
	}
	if len(mountsUnder(t, dir)) == 0 {
		t.Fatal("reap of a name-prefix path detached a sibling's mount")
	}
}
