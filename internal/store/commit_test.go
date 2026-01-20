package store

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"testing"
)

func TestWritableLayer_ToLayer(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Create some files
	f1, _ := wl.Create("file1.txt", 0644, false)
	f1Content := []byte("content of file 1")

	// Write content to file1
	contentPath := wl.ContentPath("file1.txt")
	if err := os.WriteFile(contentPath, f1Content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	f1.Hdr.Size = int64(len(f1Content))
	wl.Update(f1)

	// Create a directory
	wl.Create("mydir", 0755, true)

	// Create a file in the directory
	f2, _ := wl.Create("mydir/file2.txt", 0644, false)
	f2Content := []byte("nested file content")
	contentPath2 := wl.ContentPath("mydir/file2.txt")
	if err := os.WriteFile(contentPath2, f2Content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	f2.Hdr.Size = int64(len(f2Content))
	wl.Update(f2)

	// Create a whiteout
	wl.Whiteout("deleted.txt")

	// Convert to layer
	layer, err := wl.ToLayer()
	if err != nil {
		t.Fatalf("ToLayer failed: %v", err)
	}

	// Verify the layer
	digest, err := layer.Digest()
	if err != nil {
		t.Fatalf("Digest failed: %v", err)
	}
	t.Logf("Layer digest: %s", digest)

	// Read the layer content
	rc, err := layer.Compressed()
	if err != nil {
		t.Fatalf("Compressed failed: %v", err)
	}
	defer rc.Close()

	// Decompress and read tar
	gzr, err := gzip.NewReader(rc)
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	files := make(map[string]*tar.Header)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next failed: %v", err)
		}
		files[hdr.Name] = hdr

		// Read content for regular files
		if hdr.Typeflag == tar.TypeReg && hdr.Size > 0 {
			content, _ := io.ReadAll(tr)
			t.Logf("File %s: %d bytes, content: %q", hdr.Name, hdr.Size, string(content))
		}
	}

	// Verify expected files
	expectedFiles := []string{".wh.deleted.txt", "file1.txt", "mydir", "mydir/file2.txt"}
	for _, name := range expectedFiles {
		if _, ok := files[name]; !ok {
			t.Errorf("Expected file %q in layer", name)
		}
	}

	// Verify file1.txt content size
	if hdr, ok := files["file1.txt"]; ok {
		if hdr.Size != int64(len(f1Content)) {
			t.Errorf("file1.txt size mismatch: got %d, want %d", hdr.Size, len(f1Content))
		}
	}

	// Verify whiteout is a regular file
	if hdr, ok := files[".wh.deleted.txt"]; ok {
		if hdr.Typeflag != tar.TypeReg {
			t.Errorf("Whiteout should be regular file, got %d", hdr.Typeflag)
		}
	}

	// Verify mydir is a directory
	if hdr, ok := files["mydir"]; ok {
		if hdr.Typeflag != tar.TypeDir {
			t.Errorf("mydir should be directory, got %d", hdr.Typeflag)
		}
	}
}

func TestWritableLayer_ToLayer_Empty(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Create layer from empty writable
	layer, err := wl.ToLayer()
	if err != nil {
		t.Fatalf("ToLayer failed: %v", err)
	}

	// Should have a valid digest
	digest, err := layer.Digest()
	if err != nil {
		t.Fatalf("Digest failed: %v", err)
	}
	t.Logf("Empty layer digest: %s", digest)

	// Size should be minimal (just tar EOF markers)
	size, err := layer.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	t.Logf("Empty layer size: %d", size)
}
