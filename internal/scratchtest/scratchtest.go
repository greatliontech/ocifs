// Package scratchtest hands out repo-local test scratch directories
// under <repo>/.scratch/<tier>/<seq> instead of the OS temp
// directory: the paths sit inside the mutation/witness observation
// bracket, names are deterministic, and nothing machine-local (no
// /tmp, no absolute paths) enters a test's input surface — the
// stale-mount reap resolves absolute paths internally for
// mount-table comparison only, never handing them to a test.
// Dir serves the internal package test suites, which all live two
// levels below the repo root; suites elsewhere choose their own
// relative path through In.
package scratchtest

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greatliontech/gmdb/oslock"
)

var seq atomic.Uint64

// forceRemoveAll removes a tree that residue may have left
// non-traversable (restricted directory modes from killed mutants
// or mode-mutation tests): a top-down best-effort chmod pass makes
// each directory listable as the walk reaches it, then RemoveAll
// finishes.
func forceRemoveAll(p string) error {
	_ = filepath.WalkDir(p, func(q string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			_ = os.Chmod(q, 0o700)
		}
		return nil
	})
	return os.RemoveAll(p)
}

// Dir creates and returns a fresh scratch directory under the named
// tier, removed when the test ends. The path is relative to the
// calling test's working directory (its package dir); the sequence
// is the process's own, so the names are deterministic and In's
// hold keeps a second process of the same suite off them.
func Dir(t testing.TB, tier string) string {
	t.Helper()
	return In(t, filepath.Join("..", "..", ".scratch", tier, strconv.FormatUint(seq.Add(1), 10)))
}

// holdBound is how long a process waits for a scratch directory
// another holds before failing by name: a sibling suite's whole run
// at the outside, a hung one named rather than waited on forever.
const holdBound = 15 * time.Minute

// holds are the process's directory holds, by the hold file's path;
// each is taken once, the first caller acquiring outside the map's
// mutex so a hold never stalls the others' lookups.
var (
	holdsMu sync.Mutex
	holds   = map[string]*hold{}
)

type hold struct {
	once sync.Once
	err  error
}

// holdDirectory takes the hold of the directory's parent for the
// process, once: a lock file in the parent, held for the process's
// lifetime and released by the kernel at its exit, so a killed
// process never holds past its death and a waiter follows a release
// within the lock's poll. The directory's names being deterministic,
// two processes of one suite in one checkout would otherwise wipe
// each other's directories underfoot. The hold is the parent's, not
// the directory's: a suite's directories share one parent (a tier,
// the root's scratch directory), so a process holds one lock there
// however many directories it takes, in whatever order, and two
// processes can never wait on each other's second hold.
func holdDirectory(t testing.TB, dir string) {
	t.Helper()
	path := filepath.Join(filepath.Dir(dir), ".held")
	holdsMu.Lock()
	h, ok := holds[path]
	if !ok {
		h = &hold{}
		holds[path] = h
	}
	holdsMu.Unlock()
	h.once.Do(func() {
		parent, err := filepath.Abs(filepath.Dir(dir))
		if err != nil {
			h.err = err
			return
		}
		if err := os.MkdirAll(parent, 0o755); err != nil {
			h.err = err
			return
		}
		if inheritedHold(parent) {
			// Told of a hold by its own parent, the process checks it
			// is held: by that parent, as told, or by no one (the
			// parent gone, or the variable set by hand), in which
			// case the hold is this process's own from here; held by
			// anyone else, the telling was no inheritance.
			l, err := oslock.TryAcquire(path)
			switch {
			case errors.Is(err, oslock.ErrHeld):
				return
			case err == nil:
				pinned.Store(path, l)
				h.err = bequeathHold(parent)
				return
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), holdBound)
		defer cancel()
		l, err := oslock.Acquire(ctx, path)
		if err != nil {
			h.err = fmt.Errorf("scratch directory %s held by another process: %w", dir, err)
			return
		}
		pinned.Store(path, l)
		h.err = bequeathHold(parent)
	})
	if h.err != nil {
		t.Fatal(h.err)
	}
}

// heldEnv names, for the process's children, the parents it holds:
// entries `<pid>|<absolute path, hex-encoded>` in the platform's
// list spelling (the encoding keeping a path holding the list
// separator one entry), the pid the holder's own. A child re-executing the test binary (a
// user-namespace arm, a helper) works inside its parent's
// directories under the parent's own hold, which outlives the
// child, and waits on nothing; an entry naming any other process
// than the child's parent is no inheritance (a variable left by a
// holder since gone, or set by hand, with a stranger holding).
const heldEnv = "OCIFS_SCRATCH_HELD"

// inheritedHeld is what the process was told at its start, read
// once: its own bequests, written to the same variable for its
// children, never read as an inheritance.
var inheritedHeld = filepath.SplitList(os.Getenv(heldEnv))

// inheritedHold reports whether the process's parent told it that
// it holds the parent directory.
func inheritedHold(parent string) bool {
	me := heldEntry(os.Getppid(), parent)
	for _, held := range inheritedHeld {
		if held == me {
			return true
		}
	}
	return false
}

// bequeathHold names the held parent directory, with this process
// as its holder, to the process's children through the environment.
func bequeathHold(parent string) error {
	held := os.Getenv(heldEnv)
	if held != "" {
		held += string(os.PathListSeparator)
	}
	return os.Setenv(heldEnv, held+heldEntry(os.Getpid(), parent))
}

// heldEntry spells a hold's entry for the environment.
func heldEntry(pid int, parent string) string {
	return strconv.Itoa(pid) + "|" + hex.EncodeToString([]byte(parent))
}

// pinned keeps each hold's lock reachable for the process's lifetime
// (a dropped Lock stays held anyway; the map is for the test of the
// hold, which releases through it).
var pinned sync.Map

// releaseHoldForTest hands a directory's hold back, for the test of
// the hold itself.
func releaseHoldForTest(dir string) {
	path := filepath.Join(filepath.Dir(dir), ".held")
	holdsMu.Lock()
	delete(holds, path)
	holdsMu.Unlock()
	if l, ok := pinned.LoadAndDelete(path); ok {
		l.(*oslock.Lock).Close()
	}
}

// In prepares the caller-chosen scratch directory and registers the
// same teardown: stale mounts reaped, residue removed, directory
// created fresh. Freshness is enforced, not assumed: a killed test
// process (a SIGKILLed run, a mutation campaign's timed-out mutant)
// skips Cleanup and leaves residue — restrictive modes and live
// kernel mounts included — exactly where the next process starts.
// Mount reaping precedes any path operation on the tree: a dead
// FUSE mountpoint blocks stats indefinitely, so recovery is driven
// by the mount table alone. The directory is held for the process
// first (holdDirectory), so the residue cleared is never a live
// sibling process's.
func In(t testing.TB, dir string) string {
	t.Helper()
	holdDirectory(t, dir)
	clear := func() error {
		if err := reapMounts(dir); err != nil {
			return err
		}
		return forceRemoveAll(dir)
	}
	if err := clear(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clear() })
	return dir
}
