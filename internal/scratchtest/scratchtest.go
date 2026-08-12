// Package scratchtest hands out repo-local test scratch directories
// under <repo>/.scratch/<tier>/<seq> instead of the OS temp
// directory: the paths sit inside the mutation/witness observation
// bracket, names are deterministic, and nothing machine-local (no
// /tmp, no PWD via filepath.Abs) enters a test's input surface.
// Callers are the internal package test suites, which all live two
// levels below the repo root.
package scratchtest

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

var seq atomic.Uint64

// Dir creates and returns a fresh scratch directory under the named
// tier, removed when the test ends. The path is relative to the
// calling test's working directory (its package dir).
func Dir(t testing.TB, tier string) string {
	t.Helper()
	dir := filepath.Join("..", "..", ".scratch", tier, strconv.FormatUint(seq.Add(1), 10))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
