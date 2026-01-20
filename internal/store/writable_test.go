package store

import (
	"archive/tar"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWritableLayer_NewAndPersist(t *testing.T) {
	dir := t.TempDir()

	// Create a new writable layer
	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Verify content directory was created
	contentDir := filepath.Join(dir, contentDirName)
	if _, err := os.Stat(contentDir); os.IsNotExist(err) {
		t.Errorf("Content directory was not created: %s", contentDir)
	}

	// Add a file
	hdr := tar.Header{
		Name:     "test.txt",
		Mode:     0644,
		Size:     100,
		Typeflag: tar.TypeReg,
		ModTime:  time.Now(),
	}
	file, err := wl.SetFile(hdr)
	if err != nil {
		t.Fatalf("SetFile failed: %v", err)
	}

	// Verify file path is correct
	expectedPath := filepath.Join(dir, contentDirName, "test.txt")
	if file.Path != expectedPath {
		t.Errorf("File path mismatch: got %s, want %s", file.Path, expectedPath)
	}

	// Persist and reload
	if err := wl.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Create a new writable layer from same path (should load existing)
	wl2, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer (reload) failed: %v", err)
	}

	// Verify the file was reloaded
	reloaded := wl2.GetFile("test.txt")
	if reloaded == nil {
		t.Fatal("File was not reloaded after Persist/Load")
	}
	if reloaded.Hdr.Name != "test.txt" {
		t.Errorf("Reloaded file name mismatch: got %s, want test.txt", reloaded.Hdr.Name)
	}
	if reloaded.Hdr.Size != 100 {
		t.Errorf("Reloaded file size mismatch: got %d, want 100", reloaded.Hdr.Size)
	}
}

func TestWritableLayer_SetFileCreatesDirectories(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Set a file in a nested directory
	hdr := tar.Header{
		Name:     "a/b/c/deep.txt",
		Mode:     0644,
		Size:     0,
		Typeflag: tar.TypeReg,
	}
	file, err := wl.SetFile(hdr)
	if err != nil {
		t.Fatalf("SetFile failed: %v", err)
	}

	// Verify parent directories were created
	parentDir := filepath.Dir(file.Path)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		t.Errorf("Parent directories were not created: %s", parentDir)
	}
}

func TestWritableLayer_GetFileReturnsCopy(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	hdr := tar.Header{Name: "test.txt", Mode: 0644, Size: 100}
	wl.SetFile(hdr)

	// Get file twice
	f1 := wl.GetFile("test.txt")
	f2 := wl.GetFile("test.txt")

	// Modify f1
	f1.Hdr.Size = 999

	// f2 should be unchanged (it's a copy)
	if f2.Hdr.Size != 100 {
		t.Errorf("GetFile did not return a copy: f2.Size = %d, want 100", f2.Hdr.Size)
	}
}

func TestWritableLayer_DeleteFile(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Create a file with content
	hdr := tar.Header{Name: "todelete.txt", Mode: 0644, Size: 5}
	file, _ := wl.SetFile(hdr)

	// Write actual content
	if err := os.WriteFile(file.Path, []byte("hello"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Delete it
	if err := wl.DeleteFile("todelete.txt"); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	// Verify it's gone from metadata
	if wl.GetFile("todelete.txt") != nil {
		t.Error("File still exists in metadata after delete")
	}

	// Verify content file was removed
	if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
		t.Error("Content file still exists after delete")
	}
}

func TestWritableLayer_ListChildren(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Create directory structure
	files := []string{
		"root.txt",
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/subdir/deep.txt",
		"dir2/other.txt",
	}

	for _, f := range files {
		hdr := tar.Header{Name: f, Mode: 0644}
		wl.SetFile(hdr)
	}

	// List root children
	rootChildren := wl.ListChildren("")
	rootNames := make(map[string]bool)
	for _, c := range rootChildren {
		rootNames[c.Hdr.Name] = true
	}
	if !rootNames["root.txt"] {
		t.Error("root.txt not found in root children")
	}
	if len(rootChildren) != 1 {
		t.Errorf("Expected 1 root child, got %d", len(rootChildren))
	}

	// List dir1 children
	dir1Children := wl.ListChildren("dir1")
	if len(dir1Children) != 2 { // file1.txt and file2.txt (not subdir/deep.txt)
		t.Errorf("Expected 2 dir1 children, got %d", len(dir1Children))
	}
}

func TestWritableLayer_Whiteout(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Create a whiteout file
	whiteoutPath := WhiteoutPrefix + "deleted.txt"
	hdr := tar.Header{Name: whiteoutPath, Mode: 0, Size: 0}
	_, err = wl.SetFile(hdr)
	if err != nil {
		t.Fatalf("SetFile for whiteout failed: %v", err)
	}

	// Verify whiteout exists
	wh := wl.GetFile(whiteoutPath)
	if wh == nil {
		t.Error("Whiteout file not found")
	}
}

func TestWritableLayer_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	const numGoroutines = 50
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				name := filepath.Join("concurrent", string(rune('a'+id)), "file.txt")
				hdr := tar.Header{Name: name, Mode: 0644, Size: int64(j)}

				// SetFile
				wl.SetFile(hdr)

				// GetFile
				wl.GetFile(name)

				// ListChildren
				wl.ListChildren("concurrent")
			}
		}(i)
	}

	wg.Wait()

	// Should not panic or deadlock - if we get here, concurrency is working
}

func TestWritableLayer_PersistAndLoadPreservesAllFields(t *testing.T) {
	dir := t.TempDir()

	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	now := time.Now().Truncate(time.Second) // JSON doesn't preserve nanoseconds

	hdr := tar.Header{
		Name:       "full.txt",
		Mode:       0755,
		Uid:        1000,
		Gid:        1000,
		Size:       12345,
		ModTime:    now,
		AccessTime: now,
		ChangeTime: now,
		Typeflag:   tar.TypeReg,
	}

	wl.SetFile(hdr)
	wl.Persist()

	// Reload
	wl2, _ := NewWritableLayer(dir)
	reloaded := wl2.GetFile("full.txt")

	if reloaded == nil {
		t.Fatal("File not found after reload")
	}

	// Check all fields
	if reloaded.Hdr.Name != hdr.Name {
		t.Errorf("Name mismatch: got %s", reloaded.Hdr.Name)
	}
	if reloaded.Hdr.Mode != hdr.Mode {
		t.Errorf("Mode mismatch: got %o, want %o", reloaded.Hdr.Mode, hdr.Mode)
	}
	if reloaded.Hdr.Uid != hdr.Uid {
		t.Errorf("Uid mismatch: got %d", reloaded.Hdr.Uid)
	}
	if reloaded.Hdr.Gid != hdr.Gid {
		t.Errorf("Gid mismatch: got %d", reloaded.Hdr.Gid)
	}
	if reloaded.Hdr.Size != hdr.Size {
		t.Errorf("Size mismatch: got %d", reloaded.Hdr.Size)
	}
}
