//go:build linux

package export

import (
	"archive/tar"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func TestPropertyMaterializePassesVerifyAndRejectsMutations(t *testing.T) {
	scratch := scratchtest.Dir(t, "export-verify-prop")
	var iter int
	rapid.Check(t, func(rt *rapid.T) {
		iter++
		dir := filepath.Join(scratch, strconv.Itoa(iter))
		_ = os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			rt.Fatal(err)
		}
		defer os.RemoveAll(dir)

		contents := map[string][]byte{}
		stack := genStack(rt, contents)
		view, err := layer.Unify(stack)
		if err != nil {
			rt.Fatal(err)
		}
		blobs := blobDir(t, dir, contents)
		outDir := filepath.Join(dir, "out")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			rt.Fatal(err)
		}
		root, err := os.OpenRoot(outDir)
		if err != nil {
			rt.Fatal(err)
		}
		defer root.Close()

		if err := Materialize(context.Background(), root, view, blobs); err != nil {
			rt.Fatal(err)
		}
		if err := Verify(context.Background(), root, view); err != nil {
			rt.Fatalf("freshly materialized tree failed Verify: %v", err)
		}

		// Plant an extra rogue entry at the root and verify failure.
		rogue := filepath.Join(outDir, ".unexpected-rogue")
		if err := os.WriteFile(rogue, []byte("x"), 0o644); err != nil {
			rt.Fatal(err)
		}
		if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
			rt.Fatalf("Verify with rogue file = %v, want ErrDirtyExport", err)
		}
		if err := os.Remove(rogue); err != nil {
			rt.Fatal(err)
		}
		if err := Verify(context.Background(), root, view); err != nil {
			rt.Fatalf("Verify after removing rogue file: %v", err)
		}

		// If the view has a regular file, mutate a byte in place (same size) and verify failure.
		for _, e := range view.Entries() {
			if e.Header.Typeflag == tar.TypeReg {
				target := filepath.Join(outDir, filepath.FromSlash(e.Header.Name))
				_ = os.Chmod(filepath.Dir(target), 0o755)
				_ = os.Chmod(target, 0o644)
				orig, rerr := os.ReadFile(target)
				if rerr != nil {
					rt.Fatal(rerr)
				}
				mutated := append([]byte(nil), orig...)
				if len(mutated) == 0 {
					mutated = []byte{'!'}
				} else {
					mutated[0] ^= 0xff
				}
				if werr := os.WriteFile(target, mutated, 0o644); werr != nil {
					rt.Fatal(werr)
				}
				if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
					rt.Fatalf("Verify after mutating %q = %v, want ErrDirtyExport", e.Header.Name, err)
				}
				break
			}
		}
	})
}
