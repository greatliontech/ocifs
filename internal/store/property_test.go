package store

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// Phase 7.1: Property-Based Tests
// =============================================================================

// TestProperty_UnifyIdempotent tests that Unify(Unify(x)) == Unify(x)
// Note: Unify operates on an Image, so this tests multiple calls
func TestProperty_UnifyIdempotent(t *testing.T) {
	tempDir := t.TempDir()

	layers := []*Layer{
		{files: []*File{
			makeDir("/etc"),
			makeFile(t, tempDir, "/etc/hosts", "localhost"),
			makeFile(t, tempDir, "/etc/passwd", "root:x:0:0"),
		}},
		{files: []*File{
			makeFile(t, tempDir, "/etc/hosts", "127.0.0.1 localhost"),
		}},
	}

	img := &Image{layers: layers}

	// First Unify
	result1 := img.Unify()

	// The result is a slice of Files, not a new Image
	// To test idempotency, we create a new image from the result
	// and unify again
	img2 := &Image{layers: []*Layer{{files: result1}}}
	result2 := img2.Unify()

	// Results should be identical
	if len(result1) != len(result2) {
		t.Fatalf("Different lengths: first=%d, second=%d", len(result1), len(result2))
	}

	for i, f1 := range result1 {
		f2 := result2[i]
		if f1.Hdr.Name != f2.Hdr.Name {
			t.Errorf("Path mismatch at %d: %s vs %s", i, f1.Hdr.Name, f2.Hdr.Name)
		}
	}
}

// TestProperty_PersistLoadRoundtrip tests that Persist then Load preserves all data
func TestProperty_PersistLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create files with various attributes
	testFiles := []struct {
		name    string
		mode    os.FileMode
		isDir   bool
		content []byte
		uid     int
		gid     int
	}{
		{"file1.txt", 0644, false, []byte("content1"), 1000, 2000},
		{"file2.txt", 0755, false, []byte("content2"), 0, 0},
		{"subdir", 0755, true, nil, 1000, 1000},
		{"subdir/nested.txt", 0600, false, []byte("nested"), 500, 500},
	}

	for _, tf := range testFiles {
		f, _ := wl.Create(tf.name, tf.mode, tf.isDir)
		f.Hdr.Uid = tf.uid
		f.Hdr.Gid = tf.gid
		if tf.content != nil {
			os.WriteFile(wl.ContentPath(tf.name), tf.content, 0644)
			f.Hdr.Size = int64(len(tf.content))
		}
		wl.Update(f)
	}

	// Persist
	if err := wl.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	wl.Close()

	// Load in new instance
	wl2, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl2.Close()

	// Verify all files preserved
	for _, tf := range testFiles {
		loaded := wl2.Get(tf.name)
		if loaded == nil {
			t.Errorf("File %s not found after reload", tf.name)
			continue
		}

		if loaded.Hdr.Uid != tf.uid {
			t.Errorf("%s: Uid mismatch: got %d, want %d", tf.name, loaded.Hdr.Uid, tf.uid)
		}
		if loaded.Hdr.Gid != tf.gid {
			t.Errorf("%s: Gid mismatch: got %d, want %d", tf.name, loaded.Hdr.Gid, tf.gid)
		}
	}
}

// TestProperty_CommitPreservesContent tests that Commit preserves file content
func TestProperty_CommitPreservesContent(t *testing.T) {
	// This is tested in store_test.go but we add a property-style test here
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	// Create files with known content
	contents := map[string][]byte{
		"file1.txt":     []byte("content one"),
		"file2.txt":     []byte("content two"),
		"dir/nested.txt": []byte("nested content"),
	}

	for name, content := range contents {
		f, _ := wl.Create(name, 0644, false)
		os.WriteFile(wl.ContentPath(name), content, 0644)
		f.Hdr.Size = int64(len(content))
		wl.Update(f)
	}

	// Convert to layer and verify content is still accessible
	layer, err := wl.ToLayer()
	if err != nil {
		t.Fatalf("ToLayer failed: %v", err)
	}

	// Open the layer to verify content
	rc, err := layer.Uncompressed()
	if err != nil {
		t.Fatalf("Uncompressed failed: %v", err)
	}
	defer rc.Close()

	tr := tar.NewReader(rc)
	foundContent := make(map[string][]byte)

	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Size > 0 {
			content := make([]byte, hdr.Size)
			tr.Read(content)
			foundContent[hdr.Name] = content
		}
	}

	// Verify content matches
	for name, expected := range contents {
		got, ok := foundContent[name]
		if !ok {
			t.Errorf("File %s not found in layer", name)
			continue
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("Content mismatch for %s: got %q, want %q", name, got, expected)
		}
	}
}

// TestProperty_SortedOutputDeterministic tests that Unify always produces sorted output
func TestProperty_SortedOutputDeterministic(t *testing.T) {
	tempDir := t.TempDir()

	// Create layers with files in random order
	layers := []*Layer{
		{files: []*File{
			makeFile(t, tempDir, "/z.txt", "z"),
			makeFile(t, tempDir, "/a.txt", "a"),
			makeFile(t, tempDir, "/m.txt", "m"),
		}},
		{files: []*File{
			makeFile(t, tempDir, "/b.txt", "b"),
			makeDir("/dir"),
			makeFile(t, tempDir, "/dir/x.txt", "x"),
		}},
	}

	// Run multiple times
	for i := 0; i < 10; i++ {
		img := &Image{layers: layers}
		result := img.Unify()

		// Verify sorted
		for j := 1; j < len(result); j++ {
			if result[j-1].Hdr.Name >= result[j].Hdr.Name {
				t.Errorf("Iteration %d: Output not sorted at index %d: %s >= %s",
					i, j, result[j-1].Hdr.Name, result[j].Hdr.Name)
			}
		}
	}
}

// TestProperty_WhiteoutRemovesFile tests that whiteouts always remove the target
func TestProperty_WhiteoutRemovesFile(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple files
	fileNames := []string{"file1.txt", "file2.txt", "dir/nested.txt", "deep/path/file.txt"}

	for _, name := range fileNames {
		t.Run(name, func(t *testing.T) {
			layers := []*Layer{
				{files: []*File{
					makeFile(t, tempDir, "/"+name, "content"),
				}},
				{files: []*File{
					makeWhiteout("/" + name),
				}},
			}

			img := &Image{layers: layers}
			result := img.Unify()

			// File should not appear in result
			for _, f := range result {
				cleanName := filepath.Clean(f.Hdr.Name)
				if cleanName == "/"+name || cleanName == name {
					t.Errorf("File %s should be removed by whiteout", name)
				}
			}
		})
	}
}

// TestProperty_LayerOrderMatters tests that layer order affects the result
func TestProperty_LayerOrderMatters(t *testing.T) {
	tempDir := t.TempDir()

	// Same file with different content in different layers
	layer1 := &Layer{files: []*File{
		makeFile(t, tempDir, "/config.txt", "version=1"),
	}}
	layer2 := &Layer{files: []*File{
		makeFile(t, tempDir, "/config.txt", "version=2"),
	}}

	// Order 1: layer1 (base), layer2 (top)
	img1 := &Image{layers: []*Layer{layer1, layer2}}
	result1 := img1.Unify()

	// Order 2: layer2 (base), layer1 (top)
	img2 := &Image{layers: []*Layer{layer2, layer1}}
	result2 := img2.Unify()

	// Both should have one file, but with different content
	if len(result1) != 1 || len(result2) != 1 {
		t.Fatalf("Expected 1 file each, got %d and %d", len(result1), len(result2))
	}

	// Content should differ based on order
	content1, _ := os.ReadFile(result1[0].Path)
	content2, _ := os.ReadFile(result2[0].Path)

	if string(content1) != "version=2" {
		t.Errorf("img1 (layer2 on top) should have version=2, got %q", content1)
	}
	if string(content2) != "version=1" {
		t.Errorf("img2 (layer1 on top) should have version=1, got %q", content2)
	}
}

// Helper for creating symlink File entries for tests
func makeSymlink(name, target string) *File {
	return &File{
		Hdr: tar.Header{
			Name:     name,
			Typeflag: tar.TypeSymlink,
			Linkname: target,
			ModTime:  time.Now(),
		},
	}
}

// TestProperty_SymlinksPreserved tests that symlinks are preserved through unification
func TestProperty_SymlinksPreserved(t *testing.T) {
	tempDir := t.TempDir()

	layers := []*Layer{
		{files: []*File{
			makeFile(t, tempDir, "/target", "content"),
			makeSymlink("/link", "/target"),
		}},
	}

	img := &Image{layers: layers}
	result := img.Unify()

	// Should have both file and symlink
	hasFile := false
	hasSymlink := false
	for _, f := range result {
		if f.Hdr.Name == "/target" {
			hasFile = true
		}
		if f.Hdr.Typeflag == tar.TypeSymlink && f.Hdr.Linkname == "/target" {
			hasSymlink = true
		}
	}

	if !hasFile {
		t.Error("Target file missing")
	}
	if !hasSymlink {
		t.Error("Symlink missing")
	}
}
