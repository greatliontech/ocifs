package store

import (
	"archive/tar"
	"bytes"
	"testing"
	"time"
)

// =============================================================================
// Phase 7.2: Fuzz Tests (Go 1.18+)
// =============================================================================

// FuzzUnify_RandomLayers fuzzes the Unify function with random layer structures
func FuzzUnify_RandomLayers(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("/file.txt"), []byte("content"), byte(tar.TypeReg))
	f.Add([]byte("/dir/"), []byte(""), byte(tar.TypeDir))
	f.Add([]byte("/.wh.deleted"), []byte(""), byte(tar.TypeReg))
	f.Add([]byte("/unicode_\u4e2d\u6587.txt"), []byte("unicode"), byte(tar.TypeReg))

	f.Fuzz(func(t *testing.T, name []byte, content []byte, typeflag byte) {
		// Skip if name is empty or invalid
		if len(name) == 0 {
			return
		}

		// Create a file entry
		file := &File{
			Hdr: tar.Header{
				Name:     string(name),
				Typeflag: typeflag,
				Size:     int64(len(content)),
				Mode:     0644,
				ModTime:  time.Now(),
			},
		}

		// Create single-layer image and unify
		layer := &Layer{files: []*File{file}}
		img := &Image{layers: []*Layer{layer}}

		// Should not panic
		result := img.Unify()

		// Basic sanity checks
		for _, f := range result {
			// Name should not be empty after cleaning
			if f.Hdr.Name == "" {
				t.Error("Result contains file with empty name")
			}
		}
	})
}

// FuzzWritableLayer_RandomOps fuzzes WritableLayer with random operations
func FuzzWritableLayer_RandomOps(f *testing.F) {
	// Add seed corpus
	f.Add("file.txt", []byte("content"), uint8(0), false)
	f.Add("dir", []byte(""), uint8(1), true)
	f.Add("nested/path/file.txt", []byte("nested content"), uint8(0), false)
	f.Add(".wh.whiteout", []byte(""), uint8(2), false)

	f.Fuzz(func(t *testing.T, name string, content []byte, op uint8, isDir bool) {
		if name == "" {
			return
		}

		dir := t.TempDir()
		wl, err := NewWritableLayer(dir)
		if err != nil {
			return // Skip if we can't create writable layer
		}
		defer wl.Close()

		// Perform operation based on op byte
		switch op % 4 {
		case 0: // Create
			wl.Create(name, 0644, isDir)
		case 1: // Whiteout
			wl.Whiteout(name)
		case 2: // CopyUp
			srcFile := &File{
				Hdr: tar.Header{
					Name:     name,
					Typeflag: tar.TypeReg,
					Size:     int64(len(content)),
				},
			}
			wl.CopyUp(srcFile, bytes.NewReader(content))
		case 3: // Remove
			wl.Remove(name)
		}

		// Should be able to persist without panic
		wl.Persist()
	})
}

// FuzzTarHeader_Parsing fuzzes tar header handling
func FuzzTarHeader_Parsing(f *testing.F) {
	// Add seed corpus with various header configurations
	f.Add("normal.txt", int64(100), int64(0644), 1000, 2000)
	f.Add("", int64(0), int64(0), 0, 0) // Empty name
	f.Add("very_long_filename_that_might_cause_issues_in_some_systems.txt", int64(1<<20), int64(0755), 65534, 65534)
	f.Add("spaces in name.txt", int64(50), int64(0600), 1, 1)

	f.Fuzz(func(t *testing.T, name string, size int64, mode int64, uid, gid int) {
		// Skip negative sizes
		if size < 0 {
			return
		}

		// Create a file with the fuzzed header
		file := &File{
			Hdr: tar.Header{
				Name:     name,
				Size:     size,
				Mode:     mode,
				Uid:      uid,
				Gid:      gid,
				Typeflag: tar.TypeReg,
				ModTime:  time.Now(),
			},
		}

		// Operations that use the header should not panic
		_ = file.ContentRef()
		_ = file.HasContent()
	})
}

// FuzzWhiteoutPath_Conversion fuzzes the whiteout path conversion
func FuzzWhiteoutPath_Conversion(f *testing.F) {
	// Add seed corpus
	f.Add("file.txt")
	f.Add("dir/file.txt")
	f.Add("a/b/c/d/e/file.txt")
	f.Add(".dotfile")
	f.Add("unicode_\u4e2d\u6587.txt")

	f.Fuzz(func(t *testing.T, path string) {
		if path == "" {
			return
		}

		// toWhiteoutPath should not panic
		whPath := toWhiteoutPath(path)

		// Result should contain the whiteout prefix
		if whPath != "" && !bytes.Contains([]byte(whPath), []byte(WhiteoutPrefix)) {
			t.Errorf("Whiteout path missing prefix: %s -> %s", path, whPath)
		}
	})
}

// FuzzBlobRef_Handling fuzzes blob reference handling
func FuzzBlobRef_Handling(f *testing.F) {
	// Add seed corpus
	f.Add("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	f.Add("sha256:abc123")
	f.Add("")
	f.Add("/path/to/file")

	f.Fuzz(func(t *testing.T, blobRef string) {
		file := &File{
			BlobRef: blobRef,
			Hdr: tar.Header{
				Name: "test.txt",
			},
		}

		// Should not panic
		ref := file.ContentRef()

		// If BlobRef is set, ContentRef should return it
		if blobRef != "" && ref != blobRef {
			t.Errorf("ContentRef mismatch: got %q, want %q", ref, blobRef)
		}
	})
}
