// Package atomicfile publishes files by writing a temporary in the
// destination directory, fsyncing it, and renaming it into place. A
// reader therefore never observes a partially written file, and a
// file that survives a crash carries exactly the bytes that were
// fsynced before its rename.
//
// Directories are deliberately not fsynced: on a journaled
// filesystem metadata operations commit in order, so a later rename
// (the store's reference-cache entry, written last) cannot become
// durable while an earlier one is lost — and a rename lost from the
// journal tail merely leaves the entry absent, which every consumer
// treats as not-yet-derived state. The hazard atomicity must close
// is file *data* lagging its rename, and the pre-rename fsync closes
// it.
package atomicfile

import (
	"io"
	"os"
	"path/filepath"
)

// Writer accumulates a pending file. Exactly one of Commit or Abort
// must be called; both close the temporary.
type Writer struct {
	f *os.File
}

// NewWriter creates a pending temporary in dir. The eventual Commit
// path must lie on the same filesystem (same store tier root).
func NewWriter(dir string) (*Writer, error) {
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

func (w *Writer) Write(p []byte) (int, error) { return w.f.Write(p) }

// Commit fsyncs the temporary, sets its permissions, and renames it
// to path, creating parent directories as needed. An existing file
// at path is replaced.
func (w *Writer) Commit(path string, perm os.FileMode) error {
	if err := w.prepare(path, perm); err != nil {
		return err
	}
	if err := os.Rename(w.f.Name(), path); err != nil {
		os.Remove(w.f.Name())
		return err
	}
	return nil
}

// CommitNew is Commit that never replaces: if path already exists the
// temporary is discarded and the error satisfies
// errors.Is(err, fs.ErrExist). Link is the no-replace publication
// primitive — unlike a stat-then-rename it has no window in which a
// concurrent writer's file can be clobbered.
func (w *Writer) CommitNew(path string, perm os.FileMode) error {
	if err := w.prepare(path, perm); err != nil {
		return err
	}
	linkErr := os.Link(w.f.Name(), path)
	if err := os.Remove(w.f.Name()); linkErr == nil && err != nil {
		return err
	}
	return linkErr
}

// prepare fsyncs, sets permissions, closes the temporary, and creates
// path's parent directories; on failure the temporary is removed.
func (w *Writer) prepare(path string, perm os.FileMode) error {
	if err := w.f.Sync(); err != nil {
		w.Abort()
		return err
	}
	if err := w.f.Chmod(perm); err != nil {
		w.Abort()
		return err
	}
	if err := w.f.Close(); err != nil {
		os.Remove(w.f.Name())
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		os.Remove(w.f.Name())
		return err
	}
	return nil
}

// Abort discards the temporary.
func (w *Writer) Abort() {
	w.f.Close()
	os.Remove(w.f.Name())
}

// Write publishes r's bytes at path atomically, replacing any
// existing file. The temporary lives in path's directory, which is
// created if absent.
func Write(path string, r io.Reader, perm os.FileMode) error {
	return write(path, r, perm, (*Writer).Commit)
}

// WriteNew publishes r's bytes at path atomically unless path
// already exists; then nothing is written and the error satisfies
// errors.Is(err, fs.ErrExist).
func WriteNew(path string, r io.Reader, perm os.FileMode) error {
	return write(path, r, perm, (*Writer).CommitNew)
}

func write(path string, r io.Reader, perm os.FileMode, commit func(*Writer, string, os.FileMode) error) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	w, err := NewWriter(dir)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		w.Abort()
		return err
	}
	return commit(w, path, perm)
}
