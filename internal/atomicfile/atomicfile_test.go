package atomicfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func scratchDir(t *testing.T) string {
	t.Helper()
	return scratchtest.Dir(t, "atomicfile")
}

func listNames(t *testing.T, dir string) []string {
	t.Helper()
	var names []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			names = append(names, d.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestWritePublishesExactBytes(t *testing.T) {
	dir := scratchDir(t)
	path := filepath.Join(dir, "sub", "f")
	if err := Write(path, strings.NewReader("payload"), 0o640); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "payload" {
		t.Fatalf("content %q", got)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// windows keeps no mode but the read-only attribute: a 0640 file
	// is writable there.
	if runtime.GOOS == "windows" {
		if fi.Mode().Perm()&0o200 == 0 {
			t.Fatalf("mode %v, want writable", fi.Mode().Perm())
		}
	} else if fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v, want 0640", fi.Mode().Perm())
	}
	if names := listNames(t, dir); len(names) != 1 {
		t.Fatalf("unexpected files: %v", names)
	}
}

type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }

func TestWriteErrorPublishesNothing(t *testing.T) {
	dir := scratchDir(t)
	path := filepath.Join(dir, "f")
	boom := errors.New("boom")
	if err := Write(path, failReader{boom}, 0o644); !errors.Is(err, boom) {
		t.Fatalf("err %v, want boom", err)
	}
	// Neither the destination nor a stray temporary exists.
	if names := listNames(t, dir); len(names) != 0 {
		t.Fatalf("residue after failed write: %v", names)
	}
}

func TestAbortDiscards(t *testing.T) {
	dir := scratchDir(t)
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	w.Abort()
	if names := listNames(t, dir); len(names) != 0 {
		t.Fatalf("residue after abort: %v", names)
	}
}

func TestWriteNewRefusesExisting(t *testing.T) {
	dir := scratchDir(t)
	path := filepath.Join(dir, "f")
	if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := WriteNew(path, strings.NewReader("usurper"), 0o644)
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("err %v, want ErrExist", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The existing file survives byte-for-byte and the temporary is
	// gone.
	if string(got) != "original" {
		t.Fatalf("existing file clobbered: %q", got)
	}
	if names := listNames(t, dir); len(names) != 1 {
		t.Fatalf("residue: %v", names)
	}
}

func TestWriteNewCreates(t *testing.T) {
	dir := scratchDir(t)
	path := filepath.Join(dir, "sub", "f")
	if err := WriteNew(path, strings.NewReader("fresh"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "fresh" {
		t.Fatalf("content %q", got)
	}
	if names := listNames(t, dir); len(names) != 1 {
		t.Fatalf("residue: %v", names)
	}
}

func TestCommitIntoUncreatablePathCleansUp(t *testing.T) {
	dir := scratchDir(t)
	// A regular file where a parent directory is needed: MkdirAll
	// fails, and the temporary must not leak.
	obstacle := filepath.Join(dir, "obstacle")
	if err := os.WriteFile(obstacle, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(filepath.Join(obstacle, "child"), 0o644); err == nil {
		t.Fatal("commit under a file succeeded")
	}
	if names := listNames(t, dir); len(names) != 1 || names[0] != "obstacle" {
		t.Fatalf("residue after failed commit: %v", names)
	}
}

// WriteNew publishes where nothing stands and refuses where a file
// does, the first bytes kept and the temporary gone either way.
func TestWriteNewNeverReplaces(t *testing.T) {
	dir := scratchDir(t)
	path := filepath.Join(dir, "blob")
	if err := WriteNew(path, strings.NewReader("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := WriteNew(path, strings.NewReader("second"), 0o644)
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("a second write: %v, want ErrExist", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "first" {
		t.Fatalf("content %q", b)
	}
	if names := listNames(t, dir); len(names) != 1 {
		t.Fatalf("unexpected files: %v", names)
	}
}
