package scratchtest

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

	"github.com/hanwen/go-fuse/v2/fs"
)

type wedgeRoot struct{ fs.Inode }

// wedgeChild mounts an empty FUSE filesystem and freezes its own
// server with SIGSTOP: every subsequent request on the mount parks
// in the kernel's request wait — the exact state a test process
// killed mid-mount leaves behind.
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

// wedgeSetup prepares a wedge test: campaign guard (mount(2) escapes
// the mutation observation bracket, and a killed mutant leaks the
// mount), then self-healing — the reap under test is also this
// fixture's own recovery from a killed predecessor, consulted
// before any path operation on the tree.
func wedgeSetup(t *testing.T, dir string) {
	t.Helper()
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("mount-performing test skipped under mutation campaign")
	}
	if err := reapMounts(dir); err != nil {
		t.Fatalf("self-heal of prior run's residue: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reapMounts(dir)
		os.RemoveAll(dir)
	})
}

// wedgeAt spawns a child serving a FUSE mount at mnt and freezes it,
// returning once the mount is live. The child carries
// PR_SET_PDEATHSIG=SIGKILL, so even a SIGKILLed test run cannot
// leak a frozen child (SIGKILL is delivered to stopped processes);
// only the mount-table entry survives, which the reap recovers.
// Caveat: PDEATHSIG fires if the forking OS thread exits, not only
// the process — the runtime rarely retires threads mid-test, and
// the state-T poll below re-verifies the freeze regardless.
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
	// SIGSTOP is the child's own last act; wait for it to take.
	for range 100 {
		// The child is not traced; poll its stat state instead.
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
	base, err := canonicalBase(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := mountTable(base)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestReapRecoversWedgedMount pins the recovery invariant: scratch
// preparation consults only the mount table before touching the
// tree, so a mount whose frozen server would hang any stat is
// lazily detached and the directory comes back fresh. The second
// arm reaps the mountpoint path itself — recovery must not depend
// on the mount sitting strictly below the requested directory.
func TestReapRecoversWedgedMount(t *testing.T) {
	if os.Getenv("SCRATCHTEST_WEDGE_CHILD") != "" {
		wedgeChild()
		return
	}
	dir := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "reap")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if len(mountsUnder(t, dir)) == 0 {
		t.Fatal("fixture: no mount appeared under scratch dir")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- reapMounts(dir) }()
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

	// Equality arm: the reaped path IS the mountpoint.
	mnt := filepath.Join(dir, "mnt")
	wedgeAt(t, mnt)
	if err := reapMounts(mnt); err != nil {
		t.Fatalf("reap of the mountpoint itself: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mountpoint-path reap left mounts: %v", got)
	}
}

// TestReapRecoversNestedMounts pins recovery of a mount stacked
// inside another dead mount's subtree: the inner mount's table
// entry survives the outer overmount, and the reap ends with zero
// mounts regardless of the order individual detaches resolve in.
func TestReapRecoversNestedMounts(t *testing.T) {
	dir := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "nested")
	wedgeSetup(t, dir)
	inner := filepath.Join(dir, "m", "sub")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	wedgeAt(t, inner)
	// Overmount the parent: the inner entry stays in the table,
	// shadowed beneath the outer wedge.
	wedgeAt(t, filepath.Join(dir, "m"))
	if got := mountsUnder(t, dir); len(got) != 2 {
		t.Fatalf("fixture: want 2 stacked mounts, have %v", got)
	}
	if err := reapMounts(dir); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mounts survived nested reap: %v", got)
	}
}

// TestReapThroughSymlinkedPath pins canonicalization: the reap is
// asked for a path whose ancestors contain a symlink, while the
// mount table reports the kernel-canonical path — the stale mount
// must still be found and detached.
func TestReapThroughSymlinkedPath(t *testing.T) {
	dir := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "real")
	wedgeSetup(t, dir)
	link := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "link")
	os.Remove(link)
	if err := os.Symlink("real", link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(link) })
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if err := reapMounts(filepath.Join(link, "mnt")); err != nil {
		t.Fatalf("reap via symlinked ancestors: %v", err)
	}
	if got := mountsUnder(t, dir); len(got) != 0 {
		t.Fatalf("mount survived symlink-path reap: %v", got)
	}
}

// TestInRecoversWedgedMount pins the integration: In itself clears a
// wedged mount and hands back a fresh directory. A regression that
// drops or reorders the reap either fails In or hangs the suite —
// both loud.
func TestInRecoversWedgedMount(t *testing.T) {
	dir := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "in")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))

	got := In(t, dir)
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

// TestReapLeavesSiblings pins the boundary: a mount under a
// directory whose path merely extends the requested prefix as a
// name (…/reap-sib vs …/reap-si) is not this scratch dir's mount
// and survives the reap.
func TestReapLeavesSiblings(t *testing.T) {
	dir := filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "reap-sib")
	wedgeSetup(t, dir)
	wedgeAt(t, filepath.Join(dir, "mnt"))
	if err := reapMounts(filepath.Join("..", "..", ".scratch", "scratchtest-wedge", "reap-si")); err != nil {
		t.Fatal(err)
	}
	if len(mountsUnder(t, dir)) == 0 {
		t.Fatal("reap of a name-prefix path detached a sibling's mount")
	}
}

func TestUnescapeMountPath(t *testing.T) {
	for in, want := range map[string]string{
		`/plain/path`:    "/plain/path",
		`/with\040space`: "/with space",
		`/tab\011nl\012`: "/tab\tnl\n",
		`/back\134slash`: `/back\slash`,
		`/trailing\04`:   `/trailing\04`,
		`/not\999octal`:  `/not\999octal`,
	} {
		if got := unescapeMountPath(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}
