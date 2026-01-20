package unionfs

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/greatliontech/ocifs/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// testEnv sets up a complete test environment with mock layers and optional writable layer
type testEnv struct {
	t          *testing.T
	tempDir    string
	mountPoint string
	server     *fuse.Server
	ufs        *UnionFS
	roFiles    map[string]*store.File // path -> File for verification
	blobDir    string
	writeDir   string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	tempDir := t.TempDir()

	return &testEnv{
		t:          t,
		tempDir:    tempDir,
		mountPoint: filepath.Join(tempDir, "mnt"),
		blobDir:    filepath.Join(tempDir, "blobs"),
		writeDir:   filepath.Join(tempDir, "writable"),
		roFiles:    make(map[string]*store.File),
	}
}

// addROFile adds a file to the read-only layer
func (e *testEnv) addROFile(name string, content []byte, mode int64) {
	e.t.Helper()

	// Create blob file
	if err := os.MkdirAll(e.blobDir, 0755); err != nil {
		e.t.Fatalf("Failed to create blob dir: %v", err)
	}

	blobPath := filepath.Join(e.blobDir, name)
	if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
		e.t.Fatalf("Failed to create blob parent dir: %v", err)
	}

	if err := os.WriteFile(blobPath, content, 0644); err != nil {
		e.t.Fatalf("Failed to write blob: %v", err)
	}

	now := time.Now()
	e.roFiles[name] = &store.File{
		Hdr: tar.Header{
			Name:       name,
			Mode:       mode,
			Size:       int64(len(content)),
			Typeflag:   tar.TypeReg,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
		Path: blobPath,
	}
}

// addRODir adds a directory to the read-only layer
func (e *testEnv) addRODir(name string, mode int64) {
	e.t.Helper()

	now := time.Now()
	e.roFiles[name] = &store.File{
		Hdr: tar.Header{
			Name:       name,
			Mode:       mode | int64(syscall.S_IFDIR),
			Typeflag:   tar.TypeDir,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
	}
}

// mount mounts the filesystem
func (e *testEnv) mount(writable bool) {
	e.t.Helper()

	if err := os.MkdirAll(e.mountPoint, 0755); err != nil {
		e.t.Fatalf("Failed to create mount point: %v", err)
	}

	// Build roLookup and roDirs
	roLookup := make(map[string]*store.File)
	roDirs := make(map[string]bool)
	roDirs[""] = true // Root

	for path, file := range e.roFiles {
		roLookup[path] = file
		// Add parent directories
		dir := filepath.Dir(path)
		for dir != "." && dir != "/" && dir != "" {
			roDirs[dir] = true
			dir = filepath.Dir(dir)
		}
	}

	rootDir := &UnionFS{unionDir: unionDir{
		isRoot:    true,
		pathInFs:  "",
		roLookup:  roLookup,
		roDirs:    roDirs,
		extraDirs: make(map[string]bool),
	}}

	if writable {
		if err := os.MkdirAll(e.writeDir, 0755); err != nil {
			e.t.Fatalf("Failed to create write dir: %v", err)
		}
		wl, err := store.NewWritableLayer(e.writeDir)
		if err != nil {
			e.t.Fatalf("Failed to create writable layer: %v", err)
		}
		rootDir.writableLayer = wl
	}

	e.ufs = rootDir

	server, err := fs.Mount(e.mountPoint, rootDir, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  false,
			Name:        "test-ocifs",
			DirectMount: true,
		},
	})
	if err != nil {
		e.t.Fatalf("Mount failed: %v", err)
	}
	e.server = server

	// Wait for mount to be ready
	time.Sleep(100 * time.Millisecond)
}

// unmount unmounts the filesystem
func (e *testEnv) unmount() {
	e.t.Helper()
	if e.server != nil {
		if err := e.server.Unmount(); err != nil {
			e.t.Logf("Unmount warning: %v", err)
		}
		e.server = nil
	}
}

// cleanup unmounts and removes temp files
func (e *testEnv) cleanup() {
	e.unmount()
	// TempDir is cleaned up automatically by testing
}

// path returns the full path within the mount
func (e *testEnv) path(p string) string {
	return filepath.Join(e.mountPoint, p)
}

// ==================== READ-ONLY TESTS ====================

func TestReadOnly_ReadFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	content := []byte("Hello, OCIFS!")
	env.addROFile("hello.txt", content, 0644)
	env.mount(false)

	// Read the file
	got, err := os.ReadFile(env.path("hello.txt"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("Content mismatch: got %q, want %q", got, content)
	}
}

func TestReadOnly_ReadLargeFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Create a 1MB file
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	env.addROFile("large.bin", content, 0644)
	env.mount(false)

	got, err := os.ReadFile(env.path("large.bin"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("Large file content mismatch")
	}
}

func TestReadOnly_Stat(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	content := []byte("test content")
	env.addROFile("stat.txt", content, 0755)
	env.mount(false)

	info, err := os.Stat(env.path("stat.txt"))
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size() != int64(len(content)) {
		t.Errorf("Size mismatch: got %d, want %d", info.Size(), len(content))
	}

	// Check mode (mask off type bits)
	gotMode := info.Mode().Perm()
	if gotMode != 0755 {
		t.Errorf("Mode mismatch: got %o, want %o", gotMode, 0755)
	}
}

func TestReadOnly_ReadDir(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addRODir("dir", 0755)
	env.addROFile("dir/file1.txt", []byte("1"), 0644)
	env.addROFile("dir/file2.txt", []byte("2"), 0644)
	env.addROFile("dir/file3.txt", []byte("3"), 0644)
	env.mount(false)

	entries, err := os.ReadDir(env.path("dir"))
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}

	for _, expected := range []string{"file1.txt", "file2.txt", "file3.txt"} {
		if !names[expected] {
			t.Errorf("Missing expected entry: %s", expected)
		}
	}
}

func TestReadOnly_NestedDirectories(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addROFile("a/b/c/deep.txt", []byte("deep content"), 0644)
	env.mount(false)

	// Verify all levels exist
	paths := []string{"a", "a/b", "a/b/c", "a/b/c/deep.txt"}
	for _, p := range paths {
		if _, err := os.Stat(env.path(p)); err != nil {
			t.Errorf("Path %q should exist: %v", p, err)
		}
	}

	// Read the deep file
	content, err := os.ReadFile(env.path("a/b/c/deep.txt"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != "deep content" {
		t.Errorf("Content mismatch")
	}
}

func TestReadOnly_WriteRejected(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addROFile("readonly.txt", []byte("original"), 0644)
	env.mount(false) // Read-only mount

	// Try to write - should fail with EROFS
	err := os.WriteFile(env.path("readonly.txt"), []byte("modified"), 0644)
	if err == nil {
		t.Error("Write to read-only filesystem should fail")
	}

	// Try to create - should fail
	err = os.WriteFile(env.path("newfile.txt"), []byte("new"), 0644)
	if err == nil {
		t.Error("Create on read-only filesystem should fail")
	}
}

func TestReadOnly_ConcurrentReads(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	content := []byte("concurrent test content")
	env.addROFile("concurrent.txt", content, 0644)
	env.mount(false)

	const numReaders = 20
	const readsPerGoroutine = 50

	var wg sync.WaitGroup
	errors := make(chan error, numReaders*readsPerGoroutine)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readsPerGoroutine; j++ {
				got, err := os.ReadFile(env.path("concurrent.txt"))
				if err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(got, content) {
					errors <- io.ErrUnexpectedEOF
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent read error: %v", err)
	}
}

// ==================== WRITABLE TESTS ====================

func TestWritable_CreateNewFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true) // Writable mount

	content := []byte("new file content")
	err := os.WriteFile(env.path("created.txt"), content, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read it back
	got, err := os.ReadFile(env.path("created.txt"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("Content mismatch: got %q, want %q", got, content)
	}
}

func TestWritable_CreateInNewDirectory(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create directory
	if err := os.Mkdir(env.path("newdir"), 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create file in new directory
	content := []byte("file in new dir")
	if err := os.WriteFile(env.path("newdir/file.txt"), content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify
	got, _ := os.ReadFile(env.path("newdir/file.txt"))
	if !bytes.Equal(got, content) {
		t.Errorf("Content mismatch")
	}
}

func TestWritable_CopyOnWrite(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	original := []byte("original content")
	env.addROFile("cow.txt", original, 0644)
	env.mount(true)

	// Read original
	got, _ := os.ReadFile(env.path("cow.txt"))
	if !bytes.Equal(got, original) {
		t.Errorf("Original content mismatch")
	}

	// Modify the file
	f, err := os.OpenFile(env.path("cow.txt"), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	modified := []byte("MODIFIED content")
	if _, err := f.WriteAt(modified[:8], 0); err != nil { // Write "MODIFIED" at start
		t.Fatalf("WriteAt failed: %v", err)
	}
	f.Close()

	// Read back - should see modification
	got, _ = os.ReadFile(env.path("cow.txt"))
	expected := []byte("MODIFIED content")
	if !bytes.Equal(got, expected) {
		t.Errorf("Modified content mismatch: got %q, want %q", got, expected)
	}

	// Original blob should be unchanged
	blobContent, _ := os.ReadFile(env.roFiles["cow.txt"].Path)
	if !bytes.Equal(blobContent, original) {
		t.Errorf("Original blob was modified!")
	}
}

func TestWritable_WriteAtOffset(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create a file with known content
	initial := []byte("AAAAAAAAAA") // 10 A's
	if err := os.WriteFile(env.path("offset.txt"), initial, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Write at offset 5
	f, _ := os.OpenFile(env.path("offset.txt"), os.O_RDWR, 0)
	f.WriteAt([]byte("BBBBB"), 5)
	f.Close()

	got, _ := os.ReadFile(env.path("offset.txt"))
	expected := []byte("AAAAABBBBB")
	if !bytes.Equal(got, expected) {
		t.Errorf("WriteAt offset result: got %q, want %q", got, expected)
	}
}

func TestWritable_WriteExtendFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create small file
	if err := os.WriteFile(env.path("extend.txt"), []byte("small"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Write beyond current size
	f, _ := os.OpenFile(env.path("extend.txt"), os.O_RDWR, 0)
	f.WriteAt([]byte("END"), 100)
	f.Close()

	// Check size
	info, _ := os.Stat(env.path("extend.txt"))
	if info.Size() != 103 { // 100 + 3
		t.Errorf("Extended size: got %d, want 103", info.Size())
	}
}

func TestWritable_Truncate(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	content := []byte("1234567890")
	env.addROFile("truncate.txt", content, 0644)
	env.mount(true)

	// Truncate to 5 bytes
	if err := os.Truncate(env.path("truncate.txt"), 5); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	got, _ := os.ReadFile(env.path("truncate.txt"))
	if string(got) != "12345" {
		t.Errorf("Truncated content: got %q, want %q", got, "12345")
	}
}

func TestWritable_TruncateGrow(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	if err := os.WriteFile(env.path("grow.txt"), []byte("abc"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Truncate to grow
	if err := os.Truncate(env.path("grow.txt"), 10); err != nil {
		t.Fatalf("Truncate (grow) failed: %v", err)
	}

	info, _ := os.Stat(env.path("grow.txt"))
	if info.Size() != 10 {
		t.Errorf("Size after grow: got %d, want 10", info.Size())
	}

	// First 3 bytes should be "abc", rest should be null
	got, _ := os.ReadFile(env.path("grow.txt"))
	if string(got[:3]) != "abc" {
		t.Errorf("Content after grow: got %q for first 3 bytes", got[:3])
	}
}

func TestWritable_DeleteFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create and delete a new file
	os.WriteFile(env.path("todelete.txt"), []byte("delete me"), 0644)

	if err := os.Remove(env.path("todelete.txt")); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, err := os.Stat(env.path("todelete.txt")); !os.IsNotExist(err) {
		t.Error("File still exists after delete")
	}
}

func TestWritable_DeleteROFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addROFile("ro_delete.txt", []byte("read-only"), 0644)
	env.mount(true)

	// Delete read-only file (should create whiteout)
	if err := os.Remove(env.path("ro_delete.txt")); err != nil {
		t.Fatalf("Remove RO file failed: %v", err)
	}

	// File should not be visible
	if _, err := os.Stat(env.path("ro_delete.txt")); !os.IsNotExist(err) {
		t.Error("RO file still visible after delete")
	}

	// Original blob should still exist
	if _, err := os.Stat(env.roFiles["ro_delete.txt"].Path); err != nil {
		t.Error("Original blob was removed (should only have whiteout)")
	}
}

func TestWritable_ConcurrentWrites(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	const numWriters = 10
	const writesPerGoroutine = 20

	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				filename := env.path(filepath.Join("concurrent", string(rune('a'+id)), "file.txt"))
				os.MkdirAll(filepath.Dir(filename), 0755)
				content := []byte("content from writer")
				os.WriteFile(filename, content, 0644)
				os.ReadFile(filename)
			}
		}(i)
	}

	wg.Wait()
	// Success = no panics or deadlocks
}

func TestWritable_ConcurrentWritesToSameFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addROFile("shared.txt", bytes.Repeat([]byte("X"), 1000), 0644)
	env.mount(true)

	const numWriters = 5
	const writesPerGoroutine = 50

	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				f, err := os.OpenFile(env.path("shared.txt"), os.O_RDWR, 0)
				if err != nil {
					continue // CoW might be in progress
				}
				offset := int64((id*writesPerGoroutine + j) % 1000)
				f.WriteAt([]byte{byte('A' + id)}, offset)
				f.Close()
			}
		}(i)
	}

	wg.Wait()

	// File should still be readable
	_, err := os.ReadFile(env.path("shared.txt"))
	if err != nil {
		t.Errorf("File unreadable after concurrent writes: %v", err)
	}
}

func TestWritable_SizeCalculationBug(t *testing.T) {
	// This tests the fix for BUG-001: size calculation
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create a file
	if err := os.WriteFile(env.path("size.txt"), []byte(""), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, _ := os.OpenFile(env.path("size.txt"), os.O_RDWR, 0)

	// Write 100 bytes at offset 0
	f.WriteAt(bytes.Repeat([]byte("A"), 100), 0)

	// Write 50 bytes at offset 200
	f.WriteAt(bytes.Repeat([]byte("B"), 50), 200)

	f.Close()

	// Size should be 250 (200 + 50), not 150 (100 + 50)
	info, _ := os.Stat(env.path("size.txt"))
	if info.Size() != 250 {
		t.Errorf("Size calculation bug: got %d, want 250", info.Size())
	}
}

func TestWritable_CoWPreservesSize(t *testing.T) {
	// This tests the fix for BUG-002: stale file reference after CoW
	env := newTestEnv(t)
	defer env.cleanup()

	// RO file with known size
	original := bytes.Repeat([]byte("X"), 500)
	env.addROFile("cowsize.txt", original, 0644)
	env.mount(true)

	// Trigger CoW by writing
	f, _ := os.OpenFile(env.path("cowsize.txt"), os.O_RDWR, 0)
	f.WriteAt([]byte("Y"), 0) // Write 1 byte at start
	f.Close()

	// Size should still be 500, not 1
	info, _ := os.Stat(env.path("cowsize.txt"))
	if info.Size() != 500 {
		t.Errorf("CoW size bug: got %d, want 500", info.Size())
	}

	// Content should be "Y" + 499 "X"s
	got, _ := os.ReadFile(env.path("cowsize.txt"))
	if got[0] != 'Y' {
		t.Errorf("First byte should be Y, got %c", got[0])
	}
	if len(got) != 500 {
		t.Errorf("Content length should be 500, got %d", len(got))
	}
}

func TestWritable_PersistAndReload(t *testing.T) {
	tempDir := t.TempDir()
	mountPoint := filepath.Join(tempDir, "mnt")
	writeDir := filepath.Join(tempDir, "writable")
	blobDir := filepath.Join(tempDir, "blobs")

	os.MkdirAll(mountPoint, 0755)
	os.MkdirAll(blobDir, 0755)

	// First mount: create and write files
	{
		roLookup := make(map[string]*store.File)
		roDirs := map[string]bool{"": true}

		wl, _ := store.NewWritableLayer(writeDir)

		ufs := &UnionFS{unionDir: unionDir{
			isRoot:        true,
			pathInFs:      "",
			roLookup:      roLookup,
			roDirs:        roDirs,
			extraDirs:     make(map[string]bool),
			writableLayer: wl,
		}}

		server, err := fs.Mount(mountPoint, ufs, &fs.Options{
			MountOptions: fuse.MountOptions{DirectMount: true},
		})
		if err != nil {
			t.Fatalf("Mount failed: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Create files
		os.WriteFile(filepath.Join(mountPoint, "persist1.txt"), []byte("content1"), 0644)
		os.WriteFile(filepath.Join(mountPoint, "persist2.txt"), []byte("content2"), 0644)

		server.Unmount()
		ufs.PersistWritable()
	}

	// Second mount: verify files exist
	{
		roLookup := make(map[string]*store.File)
		roDirs := map[string]bool{"": true}

		wl, _ := store.NewWritableLayer(writeDir)

		ufs := &UnionFS{unionDir: unionDir{
			isRoot:        true,
			pathInFs:      "",
			roLookup:      roLookup,
			roDirs:        roDirs,
			extraDirs:     make(map[string]bool),
			writableLayer: wl,
		}}

		server, err := fs.Mount(mountPoint, ufs, &fs.Options{
			MountOptions: fuse.MountOptions{DirectMount: true},
		})
		if err != nil {
			t.Fatalf("Remount failed: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Verify files
		content1, err := os.ReadFile(filepath.Join(mountPoint, "persist1.txt"))
		if err != nil || string(content1) != "content1" {
			t.Errorf("persist1.txt not preserved: %v, %q", err, content1)
		}

		content2, err := os.ReadFile(filepath.Join(mountPoint, "persist2.txt"))
		if err != nil || string(content2) != "content2" {
			t.Errorf("persist2.txt not preserved: %v, %q", err, content2)
		}

		server.Unmount()
	}
}

// ==================== EDGE CASES ====================

func TestEdge_EmptyFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.addROFile("empty.txt", []byte{}, 0644)
	env.mount(false)

	content, err := os.ReadFile(env.path("empty.txt"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(content) != 0 {
		t.Errorf("Empty file has content: %q", content)
	}

	info, _ := os.Stat(env.path("empty.txt"))
	if info.Size() != 0 {
		t.Errorf("Empty file size: %d", info.Size())
	}
}

func TestEdge_SpecialCharactersInFilename(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Note: avoiding truly problematic characters like / and null
	specialNames := []string{
		"file with spaces.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.multiple.dots.txt",
	}

	for _, name := range specialNames {
		env.addROFile(name, []byte("content"), 0644)
	}
	env.mount(false)

	for _, name := range specialNames {
		if _, err := os.Stat(env.path(name)); err != nil {
			t.Errorf("Special filename %q not accessible: %v", name, err)
		}
	}
}

func TestEdge_ManyFiles(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		name := filepath.Join("manyfiles", string(rune('a'+i%26)), "file"+string(rune('0'+i%10))+".txt")
		env.addROFile(name, []byte("content"), 0644)
	}
	env.mount(false)

	// Just verify we can list the root
	entries, err := os.ReadDir(env.path("manyfiles"))
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 26 {
		t.Errorf("Expected 26 subdirectories, got %d", len(entries))
	}
}

// ==================== RMDIR TESTS ====================

func TestWritable_Rmdir_EmptyDir(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create a directory
	if err := os.Mkdir(env.path("emptydir"), 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Remove it
	if err := os.Remove(env.path("emptydir")); err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	// Verify it's gone
	if _, err := os.Stat(env.path("emptydir")); !os.IsNotExist(err) {
		t.Errorf("Directory should not exist after rmdir")
	}
}

func TestWritable_Rmdir_NonEmpty(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create a directory with a file
	os.Mkdir(env.path("nonempty"), 0755)
	os.WriteFile(env.path("nonempty/file.txt"), []byte("content"), 0644)

	// Try to remove it - should fail
	err := os.Remove(env.path("nonempty"))
	if err == nil {
		t.Errorf("Rmdir should fail on non-empty directory")
	}
}

func TestWritable_Rmdir_RODir(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Add a RO directory (implicitly by adding a file)
	env.addROFile("rodir/file.txt", []byte("content"), 0644)
	env.mount(true)

	// Delete the file first
	if err := os.Remove(env.path("rodir/file.txt")); err != nil {
		t.Fatalf("Remove file failed: %v", err)
	}

	// Now remove the directory (should create whiteout)
	if err := os.Remove(env.path("rodir")); err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	// Verify it's gone
	if _, err := os.Stat(env.path("rodir")); !os.IsNotExist(err) {
		t.Errorf("Directory should not exist after rmdir")
	}
}

// ==================== RENAME TESTS ====================

func TestWritable_Rename_SameDir(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	content := []byte("rename me")
	os.WriteFile(env.path("original.txt"), content, 0644)

	// Rename
	if err := os.Rename(env.path("original.txt"), env.path("renamed.txt")); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Old name should not exist
	if _, err := os.Stat(env.path("original.txt")); !os.IsNotExist(err) {
		t.Errorf("Original file should not exist")
	}

	// New name should exist with correct content
	got, err := os.ReadFile(env.path("renamed.txt"))
	if err != nil {
		t.Fatalf("Read renamed file failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Renamed file content mismatch: got %q, want %q", got, content)
	}
}

func TestWritable_Rename_CrossDir(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	os.Mkdir(env.path("srcdir"), 0755)
	os.Mkdir(env.path("dstdir"), 0755)

	content := []byte("move me")
	os.WriteFile(env.path("srcdir/file.txt"), content, 0644)

	// Move file to different directory
	if err := os.Rename(env.path("srcdir/file.txt"), env.path("dstdir/file.txt")); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Old path should not exist
	if _, err := os.Stat(env.path("srcdir/file.txt")); !os.IsNotExist(err) {
		t.Errorf("Original file should not exist")
	}

	// New path should exist with correct content
	got, err := os.ReadFile(env.path("dstdir/file.txt"))
	if err != nil {
		t.Fatalf("Read moved file failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Moved file content mismatch: got %q, want %q", got, content)
	}
}

func TestWritable_Rename_ROFile(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	content := []byte("read-only content")
	env.addROFile("rofile.txt", content, 0644)
	env.mount(true)

	// Rename the RO file
	if err := os.Rename(env.path("rofile.txt"), env.path("moved.txt")); err != nil {
		t.Fatalf("Rename RO file failed: %v", err)
	}

	// Old path should not exist
	if _, err := os.Stat(env.path("rofile.txt")); !os.IsNotExist(err) {
		t.Errorf("Original file should not exist")
	}

	// New path should exist with correct content
	got, err := os.ReadFile(env.path("moved.txt"))
	if err != nil {
		t.Fatalf("Read moved file failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Moved file content mismatch: got %q, want %q", got, content)
	}

	// Original blob should be unchanged
	blobContent, _ := os.ReadFile(env.roFiles["rofile.txt"].Path)
	if !bytes.Equal(blobContent, content) {
		t.Errorf("Original blob was modified!")
	}
}

func TestWritable_Rename_Overwrite(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	os.WriteFile(env.path("src.txt"), []byte("source"), 0644)
	os.WriteFile(env.path("dst.txt"), []byte("destination"), 0644)

	// Rename should overwrite destination
	if err := os.Rename(env.path("src.txt"), env.path("dst.txt")); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	got, _ := os.ReadFile(env.path("dst.txt"))
	if string(got) != "source" {
		t.Errorf("Rename didn't overwrite: got %q", got)
	}
}

// ==================== SYMLINK TESTS ====================

func TestWritable_Symlink_Create(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	content := []byte("target content")
	os.WriteFile(env.path("target.txt"), content, 0644)

	// Create symlink
	if err := os.Symlink("target.txt", env.path("link.txt")); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	// Verify symlink exists
	info, err := os.Lstat(env.path("link.txt"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("link.txt should be a symlink")
	}

	// Read through symlink
	got, err := os.ReadFile(env.path("link.txt"))
	if err != nil {
		t.Fatalf("ReadFile through symlink failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Symlink content mismatch: got %q, want %q", got, content)
	}
}

func TestWritable_Symlink_Readlink(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	target := "../some/relative/path"
	if err := os.Symlink(target, env.path("relative-link")); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	// Read the link target
	got, err := os.Readlink(env.path("relative-link"))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}
	if got != target {
		t.Errorf("Readlink: got %q, want %q", got, target)
	}
}

func TestWritable_Symlink_Absolute(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	target := "/absolute/path/to/target"
	if err := os.Symlink(target, env.path("abs-link")); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	got, err := os.Readlink(env.path("abs-link"))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}
	if got != target {
		t.Errorf("Readlink: got %q, want %q", got, target)
	}
}
