package store

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// expectedFile holds the path and content for a file we expect to see in the output.
type expectedFile struct {
	path    string
	content string // Empty for directories.
}

// --- Test Helper Functions ---

// mockFile creates a File for testing, writing content to a temp file on disk.
func mockFile(t *testing.T, tempDir, name string, flag int64, content string) *File {
	t.Helper()
	var onDiskPath string
	if flag == tar.TypeReg {
		// Create a temp file to store the mock content.
		f, err := os.CreateTemp(tempDir, "mockfile-")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		f.Close()
		onDiskPath = f.Name()
	}

	return &File{
		Hdr: tar.Header{
			Name:       name,
			Typeflag:   byte(flag),
			Size:       int64(len(content)),
			ModTime:    time.Now(),
			AccessTime: time.Now(),
			ChangeTime: time.Now(),
		},
		Path: onDiskPath,
	}
}

// makeFile creates a regular file entry.
func makeFile(t *testing.T, tempDir, name, content string) *File {
	t.Helper()
	return mockFile(t, tempDir, name, tar.TypeReg, content)
}

// makeDir creates a directory entry. The Path field is empty for directories.
func makeDir(name string) *File {
	p := name
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	// A temp dir and t are not needed as no content is written to disk.
	return &File{
		Hdr: tar.Header{Name: p, Typeflag: tar.TypeDir, ModTime: time.Now()},
	}
}

// makeWhiteout creates a standard whiteout entry. No content on disk.
func makeWhiteout(name string) *File {
	whPath := filepath.Join(filepath.Dir(name), whiteoutPrefix+filepath.Base(name))
	return &File{
		Hdr: tar.Header{Name: whPath, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
}

// makeOpaque creates an opaque whiteout entry. No content on disk.
func makeOpaque(dirName string) *File {
	opqPath := filepath.Join(dirName, whiteoutOpaque)
	return &File{
		Hdr: tar.Header{Name: opqPath, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
}

// --- Main Test Function ---

func TestUnify(t *testing.T) {
	testCases := []struct {
		name          string
		layerFunc     func(t *testing.T, tempDir string) []*Layer
		expectedFiles []expectedFile
	}{
		{
			name: "Single layer with one file",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{makeFile(t, tempDir, "/hello.txt", "world")}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/hello.txt", content: "world"},
			},
		},
		// === Phase 1.1: Layer Unification Edge Cases ===
		{
			name: "Opaque whiteout hides all lower files in directory",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/data"),
						makeFile(t, tempDir, "/data/file1.txt", "content1"),
						makeFile(t, tempDir, "/data/file2.txt", "content2"),
						makeFile(t, tempDir, "/data/file3.txt", "content3"),
					}},
					{files: []*File{
						makeOpaque("/data"),
						// No new files - just opaque marker
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/data"},
			},
		},
		{
			name: "Nested opaque whiteouts - deeper opaque doesn't affect shallower",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/a"),
						makeDir("/a/b"),
						makeFile(t, tempDir, "/a/file.txt", "a-content"),
						makeFile(t, tempDir, "/a/b/file.txt", "b-content"),
					}},
					{files: []*File{
						makeOpaque("/a/b"),
						makeFile(t, tempDir, "/a/b/new.txt", "new-in-b"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/a"},
				{path: "/a/b"},
				{path: "/a/b/new.txt", content: "new-in-b"},
				{path: "/a/file.txt", content: "a-content"}, // not affected by /a/b opaque
			},
		},
		{
			name: "File replaces directory - upper layer file hides lower layer directory",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/config"),
						makeFile(t, tempDir, "/config/setting.json", `{"key":"value"}`),
					}},
					{files: []*File{
						// Upper layer has a file where directory used to be
						makeFile(t, tempDir, "/config", "now-a-file"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/config", content: "now-a-file"},
			},
		},
		{
			name: "Directory replaces file - upper layer directory hides lower layer file",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeFile(t, tempDir, "/item", "was-a-file"),
					}},
					{files: []*File{
						makeDir("/item"),
						makeFile(t, tempDir, "/item/child.txt", "child-content"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/item"},
				{path: "/item/child.txt", content: "child-content"},
			},
		},
		{
			name: "Whiteout then recreate - delete and create same path in upper layer",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeFile(t, tempDir, "/config.yaml", "old-config"),
					}},
					{files: []*File{
						makeWhiteout("/config.yaml"),
					}},
					{files: []*File{
						makeFile(t, tempDir, "/config.yaml", "brand-new-config"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/config.yaml", content: "brand-new-config"},
			},
		},
		{
			name: "Deep directory deletion via whiteout",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/deep"),
						makeDir("/deep/nested"),
						makeDir("/deep/nested/path"),
						makeFile(t, tempDir, "/deep/nested/path/file.txt", "deep-file"),
						makeFile(t, tempDir, "/other.txt", "other"),
					}},
					{files: []*File{
						makeWhiteout("/deep"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/other.txt", content: "other"},
			},
		},
		{
			name: "Mixed whiteout types - standard and opaque in same layer",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/dir1"),
						makeFile(t, tempDir, "/dir1/a.txt", "a"),
						makeFile(t, tempDir, "/dir1/b.txt", "b"),
						makeDir("/dir2"),
						makeFile(t, tempDir, "/dir2/x.txt", "x"),
						makeFile(t, tempDir, "/dir2/y.txt", "y"),
						makeFile(t, tempDir, "/standalone.txt", "standalone"),
					}},
					{files: []*File{
						makeWhiteout("/standalone.txt"), // standard whiteout
						makeOpaque("/dir1"),             // opaque whiteout
						makeFile(t, tempDir, "/dir1/c.txt", "c-new"),
						// dir2 untouched
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/dir1"},
				{path: "/dir1/c.txt", content: "c-new"},
				{path: "/dir2"},
				{path: "/dir2/x.txt", content: "x"},
				{path: "/dir2/y.txt", content: "y"},
			},
		},
		{
			name: "Empty layers are handled correctly",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{}},
					{files: []*File{makeFile(t, tempDir, "/file.txt", "content")}},
					{files: []*File{}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/file.txt", content: "content"},
			},
		},
		{
			name: "Single layer preserves all files",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/etc"),
						makeFile(t, tempDir, "/etc/hosts", "127.0.0.1 localhost"),
						makeFile(t, tempDir, "/etc/passwd", "root:x:0:0"),
						makeDir("/var"),
						makeDir("/var/log"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/etc"},
				{path: "/etc/hosts", content: "127.0.0.1 localhost"},
				{path: "/etc/passwd", content: "root:x:0:0"},
				{path: "/var"},
				{path: "/var/log"},
			},
		},
		{
			name: "Top layer file overrides lower layer file",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/app"),
						makeFile(t, tempDir, "/app/config.txt", "old_version"),
					}},
					{files: []*File{
						makeFile(t, tempDir, "/app/config.txt", "new_version"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/app"},
				{path: "/app/config.txt", content: "new_version"},
			},
		},
		{
			name: "Regular whiteout deletes a directory and its contents",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/app"),
						makeFile(t, tempDir, "/app/main.go", "package main"),
					}},
					{files: []*File{makeWhiteout("/app")}},
				}
			},
			expectedFiles: []expectedFile{},
		},
		{
			name: "Opaque whiteout removes subdirectories from lower layers",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/app"),
						makeDir("/app/migrations"),
						makeFile(t, tempDir, "/app/migrations/001.sql", "CREATE TABLE..."),
					}},
					{files: []*File{
						makeOpaque("/app"),
						makeFile(t, tempDir, "/app/new_file.txt", "This should be kept."),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/app"},
				{path: "/app/new_file.txt", content: "This should be kept."},
			},
		},
		{
			name: "Complex three-layer interaction with content check",
			layerFunc: func(t *testing.T, tempDir string) []*Layer {
				return []*Layer{
					{files: []*File{
						makeDir("/var"), makeDir("/var/log"), makeDir("/etc"),
						makeFile(t, tempDir, "/var/log/dmesg", "kernel boot messages"),
						makeFile(t, tempDir, "/etc/hostname", "host-from-base"),
					}},
					{files: []*File{
						makeWhiteout("/var/log/dmesg"),
						makeFile(t, tempDir, "/var/log/app.log", "app started"),
						makeFile(t, tempDir, "/etc/hostname", "host-from-middle"),
					}},
					{files: []*File{
						makeOpaque("/var/log"),
						makeFile(t, tempDir, "/var/log/new.log", "fresh content"),
					}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/etc"},
				{path: "/etc/hostname", content: "host-from-middle"},
				{path: "/var"},
				{path: "/var/log"},
				{path: "/var/log/new.log", content: "fresh content"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			layers := tc.layerFunc(t, tempDir)

			// Deep copy the layers to prevent modification during test.
			layersCopy := make([]*Layer, len(layers))
			for i, l := range layers {
				filesCopy := make([]*File, len(l.Files()))
				for j, f := range l.Files() {
					filesCopy[j] = &File{Hdr: f.Hdr, Path: f.Path}
				}
				layersCopy[i] = &Layer{files: filesCopy}
			}

			imgCopy := &Image{layers: layersCopy}

			// === Execution ===
			resultFiles := imgCopy.Unify()

			// === Verification ===
			if len(resultFiles) != len(tc.expectedFiles) {
				// To aid debugging, print the resulting paths
				var resultPaths []string
				for _, f := range resultFiles {
					resultPaths = append(resultPaths, f.Hdr.Name)
				}
				t.Fatalf("Unify() returned %d files, but expected %d.\nGot paths: %v", len(resultFiles), len(tc.expectedFiles), resultPaths)
			}

			for i, expected := range tc.expectedFiles {
				result := resultFiles[i]
				resultPath := filepath.Clean(result.Hdr.Name)

				// 1. Verify path and order
				if resultPath != expected.path {
					t.Errorf("File at index %d: expected path '%s', got '%s'", i, expected.path, resultPath)
					continue
				}

				// 2. Verify content for regular files
				if result.Hdr.Typeflag == tar.TypeReg {
					content, err := os.ReadFile(result.Path)
					if err != nil {
						t.Fatalf("Failed to read result file content for '%s': %v", resultPath, err)
					}
					if string(content) != expected.content {
						t.Errorf("File '%s': content mismatch.\nExpected: %q\nGot:      %q", resultPath, expected.content, string(content))
					}
				}
			}
		})
	}
}

// TestUnify_HundredsOfLayers tests that Unify handles many layers efficiently
func TestUnify_HundredsOfLayers(t *testing.T) {
	tempDir := t.TempDir()
	const numLayers = 100

	layers := make([]*Layer, numLayers)
	for i := 0; i < numLayers; i++ {
		// Each layer adds a unique file
		fileName := filepath.Join("/layer", string(rune('a'+i%26)), "file.txt")
		content := filepath.Join("content-from-layer-", string(rune('0'+i%10)))
		layers[i] = &Layer{
			files: []*File{makeFile(t, tempDir, fileName, content)},
		}
	}

	img := &Image{layers: layers}
	result := img.Unify()

	// Should have files from all unique paths
	if len(result) == 0 {
		t.Error("Expected non-empty result from many layers")
	}

	// Verify output is sorted
	for i := 1; i < len(result); i++ {
		if result[i-1].Hdr.Name >= result[i].Hdr.Name {
			t.Errorf("Output not sorted: %s >= %s", result[i-1].Hdr.Name, result[i].Hdr.Name)
		}
	}
}

// TestUnify_DeterministicOutput ensures that Unify produces the same output for the same input
func TestUnify_DeterministicOutput(t *testing.T) {
	tempDir := t.TempDir()

	createLayers := func() []*Layer {
		return []*Layer{
			{files: []*File{
				makeDir("/etc"),
				makeFile(t, tempDir, "/etc/a.conf", "a"),
				makeFile(t, tempDir, "/etc/z.conf", "z"),
				makeFile(t, tempDir, "/etc/m.conf", "m"),
			}},
			{files: []*File{
				makeFile(t, tempDir, "/bin/cmd", "binary"),
				makeDir("/var"),
			}},
		}
	}

	// Run Unify multiple times
	const iterations = 10
	var firstResult []*File

	for i := 0; i < iterations; i++ {
		img := &Image{layers: createLayers()}
		result := img.Unify()

		if i == 0 {
			firstResult = result
		} else {
			// Compare with first result
			if len(result) != len(firstResult) {
				t.Fatalf("Iteration %d: different number of files: %d vs %d", i, len(result), len(firstResult))
			}
			for j, f := range result {
				if f.Hdr.Name != firstResult[j].Hdr.Name {
					t.Errorf("Iteration %d, index %d: different path: %s vs %s", i, j, f.Hdr.Name, firstResult[j].Hdr.Name)
				}
			}
		}
	}

	// Verify the result is sorted
	for i := 1; i < len(firstResult); i++ {
		if firstResult[i-1].Hdr.Name >= firstResult[i].Hdr.Name {
			t.Errorf("Output not sorted: %s >= %s", firstResult[i-1].Hdr.Name, firstResult[i].Hdr.Name)
		}
	}
}

// TestUnify_NoLayers handles the edge case of an image with no layers
func TestUnify_NoLayers(t *testing.T) {
	img := &Image{layers: []*Layer{}}
	result := img.Unify()

	if len(result) != 0 {
		t.Errorf("Expected empty result for no layers, got %d files", len(result))
	}
}

// TestUnify_SymlinkHandling tests that symlinks are properly preserved in unification
func TestUnify_SymlinkHandling(t *testing.T) {
	tempDir := t.TempDir()

	// Create a symlink entry
	symlinkFile := &File{
		Hdr: tar.Header{
			Name:     "/link",
			Typeflag: tar.TypeSymlink,
			Linkname: "/target",
			ModTime:  time.Now(),
		},
	}

	layers := []*Layer{
		{files: []*File{
			makeFile(t, tempDir, "/target", "target-content"),
			symlinkFile,
		}},
	}

	img := &Image{layers: layers}
	result := img.Unify()

	if len(result) != 2 {
		t.Fatalf("Expected 2 files, got %d", len(result))
	}

	// Find the symlink
	var foundSymlink bool
	for _, f := range result {
		if f.Hdr.Typeflag == tar.TypeSymlink {
			foundSymlink = true
			if f.Hdr.Linkname != "/target" {
				t.Errorf("Symlink linkname wrong: got %s, want /target", f.Hdr.Linkname)
			}
		}
	}
	if !foundSymlink {
		t.Error("Symlink not found in result")
	}
}

// TestUnify_HardlinkHandling tests that hardlinks are properly preserved in unification
func TestUnify_HardlinkHandling(t *testing.T) {
	tempDir := t.TempDir()

	// Create a hardlink entry
	hardlinkFile := &File{
		Hdr: tar.Header{
			Name:     "/hardlink",
			Typeflag: tar.TypeLink,
			Linkname: "/original",
			ModTime:  time.Now(),
		},
	}

	layers := []*Layer{
		{files: []*File{
			makeFile(t, tempDir, "/original", "original-content"),
			hardlinkFile,
		}},
	}

	img := &Image{layers: layers}
	result := img.Unify()

	if len(result) != 2 {
		t.Fatalf("Expected 2 files, got %d", len(result))
	}

	// Find the hardlink
	var foundHardlink bool
	for _, f := range result {
		if f.Hdr.Typeflag == tar.TypeLink {
			foundHardlink = true
			if f.Hdr.Linkname != "/original" {
				t.Errorf("Hardlink linkname wrong: got %s, want /original", f.Hdr.Linkname)
			}
		}
	}
	if !foundHardlink {
		t.Error("Hardlink not found in result")
	}
}
