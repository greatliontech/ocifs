package unionfs

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/greatliontech/ocifs/internal/store" // Assuming this is the correct import path
)

// expectedFile holds the path and content for a file we expect to see in the output.
type expectedFile struct {
	path    string
	content string // Empty for directories.
}

// --- Test Helper Functions ---

// mockFile creates a store.File for testing, writing content to a temp file on disk.
func mockFile(t *testing.T, tempDir, name string, flag int64, content string) *store.File {
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

	return &store.File{
		Hdr: &tar.Header{
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
func makeFile(t *testing.T, tempDir, name, content string) *store.File {
	t.Helper()
	return mockFile(t, tempDir, name, tar.TypeReg, content)
}

// makeDir creates a directory entry. The Path field is empty for directories.
func makeDir(name string) *store.File {
	p := name
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	// A temp dir and t are not needed as no content is written to disk.
	return &store.File{
		Hdr: &tar.Header{Name: p, Typeflag: tar.TypeDir, ModTime: time.Now()},
	}
}

// makeWhiteout creates a standard whiteout entry. No content on disk.
func makeWhiteout(name string) *store.File {
	whPath := filepath.Join(filepath.Dir(name), whiteoutPrefix+filepath.Base(name))
	return &store.File{
		Hdr: &tar.Header{Name: whPath, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
}

// makeOpaque creates an opaque whiteout entry. No content on disk.
func makeOpaque(dirName string) *store.File {
	opqPath := filepath.Join(dirName, whiteoutOpaque)
	return &store.File{
		Hdr: &tar.Header{Name: opqPath, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
}

// --- Main Test Function ---

func TestUnify(t *testing.T) {
	testCases := []struct {
		name          string
		layerFunc     func(t *testing.T, tempDir string) []*store.Layer
		expectedFiles []expectedFile
	}{
		{
			name: "Single layer with one file",
			layerFunc: func(t *testing.T, tempDir string) []*store.Layer {
				return []*store.Layer{
					{Files: []*store.File{makeFile(t, tempDir, "/hello.txt", "world")}},
				}
			},
			expectedFiles: []expectedFile{
				{path: "/hello.txt", content: "world"},
			},
		},
		{
			name: "Top layer file overrides lower layer file",
			layerFunc: func(t *testing.T, tempDir string) []*store.Layer {
				return []*store.Layer{
					{Files: []*store.File{
						makeDir("/app"),
						makeFile(t, tempDir, "/app/config.txt", "old_version"),
					}},
					{Files: []*store.File{
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
			layerFunc: func(t *testing.T, tempDir string) []*store.Layer {
				return []*store.Layer{
					{Files: []*store.File{
						makeDir("/app"),
						makeFile(t, tempDir, "/app/main.go", "package main"),
					}},
					{Files: []*store.File{makeWhiteout("/app")}},
				}
			},
			expectedFiles: []expectedFile{},
		},
		{
			name: "Opaque whiteout removes subdirectories from lower layers",
			layerFunc: func(t *testing.T, tempDir string) []*store.Layer {
				return []*store.Layer{
					{Files: []*store.File{
						makeDir("/app"),
						makeDir("/app/migrations"),
						makeFile(t, tempDir, "/app/migrations/001.sql", "CREATE TABLE..."),
					}},
					{Files: []*store.File{
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
			layerFunc: func(t *testing.T, tempDir string) []*store.Layer {
				return []*store.Layer{
					{Files: []*store.File{
						makeDir("/var"), makeDir("/var/log"), makeDir("/etc"),
						makeFile(t, tempDir, "/var/log/dmesg", "kernel boot messages"),
						makeFile(t, tempDir, "/etc/hostname", "host-from-base"),
					}},
					{Files: []*store.File{
						makeWhiteout("/var/log/dmesg"),
						makeFile(t, tempDir, "/var/log/app.log", "app started"),
						makeFile(t, tempDir, "/etc/hostname", "host-from-middle"),
					}},
					{Files: []*store.File{
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
			layersCopy := make([]*store.Layer, len(layers))
			for i, l := range layers {
				filesCopy := make([]*store.File, len(l.Files))
				for j, f := range l.Files {
					hdrCopy := *f.Hdr
					filesCopy[j] = &store.File{Hdr: &hdrCopy, Path: f.Path}
				}
				layersCopy[i] = &store.Layer{Files: filesCopy}
			}

			// === Execution ===
			resultFiles := Unify(layersCopy)

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
