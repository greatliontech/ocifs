//go:build linux

package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

const (
	crossNSChildEnv = "OCIFS_STORE_CROSSNS_CHILD"
	crossNSStoreEnv = "OCIFS_STORE_CROSSNS_DIR"
)

// crossNSChild registers a mount in the shared store and parks until
// killed. It runs inside a fresh PID (and user) namespace — see the
// parent test — so every process-identity signal the retired
// liveness design consulted is meaningless from the parent's side.
func crossNSChild() {
	dir := os.Getenv(crossNSStoreEnv)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		fmt.Println("child:", err)
		os.Exit(1)
	}
	defer s.Close()
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("cd", 32)}
	if _, err := s.RegisterMountRecordArbitrated(context.Background(), "ns-mount", h, "", "/x", nil); err != nil {
		fmt.Println("child register:", err)
		os.Exit(1)
	}
	fmt.Println("REGISTERED")
	// Park on stdin rather than a timer: the parent holds the write
	// end for its lifetime, so a parent killed without running its
	// deferred cleanup (a go-test timeout) EOFs the pipe and the
	// child exits instead of surviving as an hour-long orphan holding
	// a mount claim in the shared scratch tree.
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// TestCrossNamespaceMountReclaimedAfterKill pins the headline
// capability of held-lock liveness end-to-end on the real kernel: a
// mount registered by a process in a SIBLING PID namespace — where
// kill(0) answers ESRCH for live and dead alike and every identity
// field is unjudgeable — is protected while its owner lives (the
// held mount lock) and reclaimed the moment the owner is SIGKILLed
// (the kernel releases the lock at death; no namespace
// classification, no aging window). The retired identity design
// documented exactly this corpse as its unjudgeable residual class.
func TestCrossNamespaceMountReclaimedAfterKill(t *testing.T) {
	if os.Getenv(crossNSChildEnv) != "" {
		crossNSChild()
		return
	}
	if testing.Short() {
		t.Skip("subprocess test skipped in short mode")
	}

	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", "^TestCrossNamespaceMountReclaimedAfterKill$")
	cmd.Env = append(os.Environ(), crossNSChildEnv+"=1", crossNSStoreEnv+"="+dir)
	// A fresh PID namespace needs a fresh user namespace for an
	// unprivileged runner; map our own uid/gid so the child keeps
	// filesystem access to the shared store.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWPID,
		UidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
		GidMappingsEnableSetgroups: false,
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		// Unprivileged user namespaces can be administratively
		// disabled; that is an environment limit, not a store defect.
		// EPERM/ENOSYS = disabled or unsupported; EUSERS =
		// user.max_user_namespaces exhausted or zeroed.
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EUSERS) {
			t.Skipf("cannot create user+pid namespace: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	lineCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			if line == "REGISTERED" || strings.HasPrefix(line, "child") {
				lineCh <- line
				return
			}
		}
		lineCh <- fmt.Sprintf("child exited silently: %v", sc.Err())
	}()
	select {
	case line := <-lineCh:
		if line != "REGISTERED" {
			t.Fatalf("child: %s", line)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for the child to register its mount")
	}

	// Owner alive in its foreign namespace: the held mount lock is
	// the protection — reclamation must leave the row alone.
	reclaimed, err := s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(reclaimed, "ns-mount") {
		t.Fatal("live cross-namespace mount reclaimed (its lock is held)")
	}
	if _, err := s.MountRecord(context.Background(), "ns-mount"); err != nil {
		t.Fatalf("live cross-namespace row missing: %v", err)
	}

	// SIGKILL: no cleanup path runs in the child; the kernel drops
	// its locks at death, across the namespace boundary.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	reclaimed, err = s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(reclaimed, "ns-mount") {
		t.Fatalf("killed cross-namespace mount not reclaimed: %v", reclaimed)
	}
	if _, err := s.MountRecord(context.Background(), "ns-mount"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead cross-namespace row survived: %v", err)
	}
}
