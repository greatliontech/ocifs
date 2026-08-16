// Package scratchtest hands out repo-local test scratch directories
// under <repo>/.scratch/<tier>/<seq> instead of the OS temp
// directory: the paths sit inside the mutation/witness observation
// bracket, names are deterministic, and nothing machine-local (no
// /tmp, no PWD via filepath.Abs) enters a test's input surface.
// Callers are the internal package test suites, which all live two
// levels below the repo root.
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
	dir := filepath.Join("..", "..", ".scratch", tier, strconv.FormatUint(seq.Add(1), 10))
	// Freshness is enforced, not assumed: a killed test process (a
	// mutation campaign's timed-out mutant) skips Cleanup and leaves
	// residue exactly where the next process's sequence restarts.
	if err := forceRemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = forceRemoveAll(dir) })
	return dir
}
