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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
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
// calling test's working directory (its package dir).
func Dir(t testing.TB, tier string) string {
	t.Helper()
	return In(t, filepath.Join("..", "..", ".scratch", tier, strconv.FormatUint(seq.Add(1), 10)))
}

// In prepares the caller-chosen scratch directory and registers the
// same teardown: stale mounts reaped, residue removed, directory
// created fresh. Freshness is enforced, not assumed: a killed test
// process (a SIGKILLed run, a mutation campaign's timed-out mutant)
// skips Cleanup and leaves residue — restrictive modes and live
// kernel mounts included — exactly where the next process starts.
// Mount reaping precedes any path operation on the tree: a dead
// FUSE mountpoint blocks stats indefinitely, so recovery is driven
// by the mount table alone.
func In(t testing.TB, dir string) string {
	t.Helper()
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
