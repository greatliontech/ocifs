package export

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
)

func TestVerifyPortableDetectsAllTamperingClasses(t *testing.T) {
	tmp := t.TempDir()
	casDir := filepath.Join(tmp, "cas")
	if err := os.MkdirAll(casDir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("verified-payload")
	sum := sha256.Sum256(payload)
	dig := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
	if err := os.WriteFile(filepath.Join(casDir, dig.Hex), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	blobs := func(h v1.Hash) string { return filepath.Join(casDir, h.Hex) }

	entries := layer.Layer{
		{Header: tar.Header{Name: "bin", Typeflag: tar.TypeDir, Mode: 0o755}},
		{Header: tar.Header{Name: "bin/tool", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(payload))}, Digest: dig},
	}
	if runtime.GOOS != "windows" {
		entries = append(entries, layer.Entry{
			Header: tar.Header{Name: "bin/current", Typeflag: tar.TypeSymlink, Linkname: "tool", Mode: 0o777},
		})
	}
	view, err := layer.Unify([]layer.Layer{entries})
	if err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tmp, "export")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	if err := Materialize(context.Background(), root, view, blobs); err != nil {
		t.Fatal(err)
	}
	if err := Verify(context.Background(), root, view); err != nil {
		t.Fatalf("clean export failed Verify: %v", err)
	}

	// 1. Same-size content corruption.
	toolPath := filepath.Join(outDir, "bin", "tool")
	corrupt := append([]byte(nil), payload...)
	corrupt[0] ^= 0x01
	if err := os.WriteFile(toolPath, corrupt, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
		t.Fatalf("same-size byte flip = %v, want ErrDirtyExport", err)
	}
	if err := os.WriteFile(toolPath, payload, 0o755); err != nil {
		t.Fatal(err)
	}

	// 2. Permission tampering on Unix.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(toolPath, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
			t.Fatalf("chmod drift = %v, want ErrDirtyExport", err)
		}
		if err := os.Chmod(toolPath, 0o755); err != nil {
			t.Fatal(err)
		}

		// 3. Symlink target tampering on Unix.
		symPath := filepath.Join(outDir, "bin", "current")
		if err := os.Remove(symPath); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("wrong", symPath); err != nil {
			t.Fatal(err)
		}
		if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
			t.Fatalf("symlink drift = %v, want ErrDirtyExport", err)
		}
		_ = os.Remove(symPath)
		if err := os.Symlink("tool", symPath); err != nil {
			t.Fatal(err)
		}
	}

	// 4. Missing entry.
	if err := os.Remove(toolPath); err != nil {
		t.Fatal(err)
	}
	if err := Verify(context.Background(), root, view); !errors.Is(err, ErrDirtyExport) {
		t.Fatalf("missing entry = %v, want ErrDirtyExport", err)
	}
	if err := os.WriteFile(toolPath, payload, 0o755); err != nil {
		t.Fatal(err)
	}

	// 5. Missing/zero or non-sha256 digest in view must fail closed on the digest guard itself.
	for _, badDigest := range []v1.Hash{
		{},
		{Algorithm: "blake2b", Hex: dig.Hex},
	} {
		badEntries := layer.Layer{
			{Header: tar.Header{Name: "bin", Typeflag: tar.TypeDir, Mode: 0o755}},
			{Header: tar.Header{Name: "bin/tool", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(payload))}, Digest: badDigest},
		}
		if runtime.GOOS != "windows" {
			badEntries = append(badEntries, layer.Entry{
				Header: tar.Header{Name: "bin/current", Typeflag: tar.TypeSymlink, Linkname: "tool", Mode: 0o777},
			})
		}
		badView, err := layer.Unify([]layer.Layer{badEntries})
		if err != nil {
			t.Fatal(err)
		}
		err = Verify(context.Background(), root, badView)
		if !errors.Is(err, ErrDirtyExport) || !strings.Contains(err.Error(), "missing or unsupported sha256 digest") {
			t.Fatalf("bad digest %q Verify = %v, want ErrDirtyExport with unsupported-digest message", badDigest, err)
		}
	}
}
