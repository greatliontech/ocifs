package export

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
)

// ErrDirtyExport classifies any discrepancy between an on-disk export
// tree and its unified view (missing or extra entries, altered node
// types, symlink targets, permissions, sizes, or regular-file SHA-256
// content digests).
var ErrDirtyExport = errors.New("cached export does not match unified view")

// Verify checks that root holds an exact materialization of view:
// every materializable entry in view exists at its path with matching
// kind, permissions (on Unix), symlink target, size, and SHA-256
// content digest, and zero extra entries exist anywhere in root. All
// filesystem inspection runs through root (*os.Root) without following
// symlinks.
func Verify(ctx context.Context, root *os.Root, view *layer.View) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries := view.Entries()
	expected := make(map[string]*layer.Entry, len(entries))
	var dotEntry *layer.Entry
	for i := range entries {
		e := &entries[i]
		if e.Header.Typeflag == tar.TypeChar || e.Header.Typeflag == tar.TypeBlock {
			continue
		}
		if e.Header.Name == "." {
			dotEntry = e
			continue
		}
		expected[e.Header.Name] = e
	}

	rootInfo, err := root.Lstat(".")
	if err != nil {
		return fmt.Errorf("%w: stat root: %v", ErrDirtyExport, err)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("%w: root is not a directory", ErrDirtyExport)
	}
	if dotEntry != nil && runtime.GOOS != "windows" {
		wantMode := dotEntry.Header.FileInfo().Mode().Perm() | modeSpecial(dotEntry)
		gotMode := rootInfo.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
		if gotMode != wantMode {
			return fmt.Errorf("%w: root mode %v, want %v", ErrDirtyExport, gotMode, wantMode)
		}
	}

	visited := make(map[string]struct{}, len(expected))
	var walkDir func(dir string) error
	walkDir = func(dir string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		df, err := root.Open(dir)
		if err != nil {
			return fmt.Errorf("%w: open dir %q: %v", ErrDirtyExport, dir, err)
		}
		dirEntries, readErr := df.ReadDir(-1)
		_ = df.Close()
		if readErr != nil {
			return fmt.Errorf("%w: readdir %q: %v", ErrDirtyExport, dir, readErr)
		}
		for _, de := range dirEntries {
			if err := ctx.Err(); err != nil {
				return err
			}
			rel := de.Name()
			if dir != "." {
				rel = dir + "/" + rel
			}
			exp, ok := expected[rel]
			if !ok {
				return fmt.Errorf("%w: unexpected entry %q", ErrDirtyExport, rel)
			}
			visited[rel] = struct{}{}
			if err := verifyEntry(ctx, root, rel, exp); err != nil {
				return err
			}
			if exp.Header.Typeflag == tar.TypeDir {
				if err := walkDir(rel); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walkDir("."); err != nil {
		return err
	}
	if len(visited) != len(expected) {
		for rel := range expected {
			if _, ok := visited[rel]; !ok {
				return fmt.Errorf("%w: missing entry %q", ErrDirtyExport, rel)
			}
		}
	}
	return nil
}

func verifyEntry(ctx context.Context, root *os.Root, rel string, exp *layer.Entry) error {
	fi, err := root.Lstat(rel)
	if err != nil {
		return fmt.Errorf("%w: lstat %q: %v", ErrDirtyExport, rel, err)
	}
	mode := fi.Mode()

	switch exp.Header.Typeflag {
	case tar.TypeDir:
		if !mode.IsDir() {
			return fmt.Errorf("%w: %q is not a directory", ErrDirtyExport, rel)
		}
		if runtime.GOOS != "windows" {
			want := exp.Header.FileInfo().Mode().Perm() | modeSpecial(exp)
			got := mode & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
			if got != want {
				return fmt.Errorf("%w: dir %q mode %v, want %v", ErrDirtyExport, rel, got, want)
			}
		}
		return nil

	case tar.TypeSymlink:
		if mode&fs.ModeSymlink == 0 {
			return fmt.Errorf("%w: %q is not a symlink", ErrDirtyExport, rel)
		}
		if runtime.GOOS == "darwin" {
			want := exp.Header.FileInfo().Mode().Perm()
			if mode.Perm() != want {
				return fmt.Errorf("%w: symlink %q mode %v, want %v", ErrDirtyExport, rel, mode.Perm(), want)
			}
		}
		target, err := root.Readlink(rel)
		if err != nil {
			return fmt.Errorf("%w: readlink %q: %v", ErrDirtyExport, rel, err)
		}
		if target != exp.Header.Linkname {
			return fmt.Errorf("%w: symlink %q target %q, want %q", ErrDirtyExport, rel, target, exp.Header.Linkname)
		}
		return nil

	case tar.TypeFifo:
		if mode&fs.ModeNamedPipe == 0 {
			return fmt.Errorf("%w: %q is not a FIFO", ErrDirtyExport, rel)
		}
		if runtime.GOOS != "windows" {
			want := exp.Header.FileInfo().Mode().Perm() | modeSpecial(exp)
			got := mode & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
			if got != want {
				return fmt.Errorf("%w: fifo %q mode %v, want %v", ErrDirtyExport, rel, got, want)
			}
		}
		return nil

	case tar.TypeReg, tar.TypeLink:
		if !mode.IsRegular() {
			return fmt.Errorf("%w: %q is not a regular file", ErrDirtyExport, rel)
		}
		if fi.Size() != exp.Header.Size {
			return fmt.Errorf("%w: %q size %d, want %d", ErrDirtyExport, rel, fi.Size(), exp.Header.Size)
		}
		if runtime.GOOS != "windows" {
			want := exp.Header.FileInfo().Mode().Perm() | modeSpecial(exp)
			got := mode & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
			if got != want {
				return fmt.Errorf("%w: file %q mode %v, want %v", ErrDirtyExport, rel, got, want)
			}
		}
		return verifyRootFileDigest(ctx, root, rel, exp.Header.Size, exp.Digest)

	default:
		return fmt.Errorf("%w: unsupported entry type %q for %q", ErrDirtyExport, exp.Header.Typeflag, rel)
	}
}

func verifyRootFileDigest(ctx context.Context, root *os.Root, rel string, wantSize int64, want v1.Hash) error {
	if want.Algorithm != "sha256" || want.Hex == "" {
		return fmt.Errorf("%w: missing or unsupported sha256 digest %q for %q", ErrDirtyExport, want, rel)
	}
	f, err := root.Open(rel)
	if err != nil {
		return fmt.Errorf("%w: open %q: %v", ErrDirtyExport, rel, err)
	}
	defer f.Close()

	h := sha256.New()
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := io.CopyN(h, f, copyChunk)
		total += n
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("%w: read %q: %v", ErrDirtyExport, rel, err)
		}
	}
	if total != wantSize {
		return fmt.Errorf("%w: %q read %d bytes, want %d", ErrDirtyExport, rel, total, wantSize)
	}
	gotHex := hex.EncodeToString(h.Sum(nil))
	if gotHex != want.Hex {
		return fmt.Errorf("%w: %q digest sha256:%s, want %s", ErrDirtyExport, rel, gotHex, want)
	}
	return nil
}
