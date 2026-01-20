package store

import (
	"archive/tar"
	"bytes"
	"io"
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

	// Create a file using the new API
	file, err := wl.Create("test.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify file path is correct
	expectedPath := filepath.Join(dir, contentDirName, "test.txt")
	if file.Path != expectedPath {
		t.Errorf("File path mismatch: got %s, want %s", file.Path, expectedPath)
	}

	// Write some content
	if err := os.WriteFile(file.Path, []byte("hello"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Update the size
	file.Hdr.Size = 5
	if err := wl.Update(file); err != nil {
		t.Fatalf("Update failed: %v", err)
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
	reloaded := wl2.Get("test.txt")
	if reloaded == nil {
		t.Fatal("File was not reloaded after Persist/Load")
	}
	if reloaded.Hdr.Name != "test.txt" {
		t.Errorf("Reloaded file name mismatch: got %s, want test.txt", reloaded.Hdr.Name)
	}
	if reloaded.Hdr.Size != 5 {
		t.Errorf("Reloaded file size mismatch: got %d, want 5", reloaded.Hdr.Size)
	}
}

func TestWritableLayer_CreateAndExists(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// File shouldn't exist yet
	if wl.Exists("newfile.txt") {
		t.Error("File should not exist before creation")
	}

	// Create it
	_, err := wl.Create("newfile.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Now it should exist
	if !wl.Exists("newfile.txt") {
		t.Error("File should exist after creation")
	}
}

func TestWritableLayer_CreateDirectory(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a directory
	dirFile, err := wl.Create("mydir", 0755, true)
	if err != nil {
		t.Fatalf("Create directory failed: %v", err)
	}

	if dirFile.Hdr.Typeflag != tar.TypeDir {
		t.Errorf("Expected TypeDir, got %d", dirFile.Hdr.Typeflag)
	}
}

func TestWritableLayer_CreateNestedPath(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a file in a nested directory
	file, err := wl.Create("a/b/c/deep.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify parent directories were created for content
	parentDir := filepath.Dir(file.Path)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		t.Errorf("Parent directories were not created: %s", parentDir)
	}
}

func TestWritableLayer_GetReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	wl.Create("test.txt", 0644, false)

	// Get file twice
	f1 := wl.Get("test.txt")
	f2 := wl.Get("test.txt")

	// Modify f1
	f1.Hdr.Size = 999

	// f2 should be unchanged (it's a copy)
	if f2.Hdr.Size != 0 {
		t.Errorf("Get did not return a copy: f2.Size = %d, want 0", f2.Hdr.Size)
	}
}

func TestWritableLayer_Update(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	file, _ := wl.Create("test.txt", 0644, false)

	// Update the file
	file.Hdr.Size = 100
	file.Hdr.Uid = 1000
	if err := wl.Update(file); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Get and verify
	updated := wl.Get("test.txt")
	if updated.Hdr.Size != 100 {
		t.Errorf("Size not updated: got %d, want 100", updated.Hdr.Size)
	}
	if updated.Hdr.Uid != 1000 {
		t.Errorf("Uid not updated: got %d, want 1000", updated.Hdr.Uid)
	}
}

func TestWritableLayer_UpdateNonexistent(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	file := &File{Hdr: tar.Header{Name: "nonexistent.txt"}}
	err := wl.Update(file)
	if err == nil {
		t.Error("Update should fail for nonexistent file")
	}
}

func TestWritableLayer_Remove(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a file with content
	file, _ := wl.Create("todelete.txt", 0644, false)
	os.WriteFile(file.Path, []byte("hello"), 0644)

	// Remove it
	if err := wl.Remove("todelete.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone from metadata
	if wl.Get("todelete.txt") != nil {
		t.Error("File still exists in metadata after remove")
	}

	// Verify content file was removed
	if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
		t.Error("Content file still exists after remove")
	}
}

func TestWritableLayer_List(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create directory structure
	wl.Create("root.txt", 0644, false)
	wl.Create("dir1/file1.txt", 0644, false)
	wl.Create("dir1/file2.txt", 0644, false)
	wl.Create("dir1/subdir/deep.txt", 0644, false)
	wl.Create("dir2/other.txt", 0644, false)

	// List root children
	rootChildren := wl.List("")
	if len(rootChildren) != 1 {
		t.Errorf("Expected 1 root child (root.txt), got %d", len(rootChildren))
	}

	// List dir1 children (should not include subdir/deep.txt)
	dir1Children := wl.List("dir1")
	if len(dir1Children) != 2 {
		t.Errorf("Expected 2 dir1 children, got %d", len(dir1Children))
	}
}

func TestWritableLayer_ListExcludesWhiteouts(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create some files
	wl.Create("dir/file1.txt", 0644, false)
	wl.Create("dir/file2.txt", 0644, false)

	// Create a whiteout
	wl.Whiteout("dir/deleted.txt")

	// List should not include the whiteout
	children := wl.List("dir")
	for _, c := range children {
		if c.Hdr.Name == ".wh.deleted.txt" || c.Hdr.Name == "dir/.wh.deleted.txt" {
			t.Error("List should not return whiteout markers")
		}
	}
	if len(children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(children))
	}
}

func TestWritableLayer_Whiteout(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a whiteout
	if err := wl.Whiteout("deleted.txt"); err != nil {
		t.Fatalf("Whiteout failed: %v", err)
	}

	// Check IsWhiteout
	if !wl.IsWhiteout("deleted.txt") {
		t.Error("IsWhiteout should return true")
	}

	// The actual whiteout file should exist
	whPath := toWhiteoutPath("deleted.txt")
	if wl.Get(whPath) == nil {
		t.Error("Whiteout marker should exist in metadata")
	}
}

func TestWritableLayer_WhiteoutInSubdir(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a whiteout in a subdirectory
	if err := wl.Whiteout("subdir/deleted.txt"); err != nil {
		t.Fatalf("Whiteout failed: %v", err)
	}

	if !wl.IsWhiteout("subdir/deleted.txt") {
		t.Error("IsWhiteout should return true for nested path")
	}
}

func TestWritableLayer_Whiteouts(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create some whiteouts
	wl.Whiteout("dir/deleted1.txt")
	wl.Whiteout("dir/deleted2.txt")
	wl.Create("dir/realfile.txt", 0644, false)

	// Get whiteouts
	whiteouts := wl.Whiteouts("dir")
	if len(whiteouts) != 2 {
		t.Errorf("Expected 2 whiteouts, got %d", len(whiteouts))
	}

	// Verify original names are returned
	names := make(map[string]bool)
	for _, n := range whiteouts {
		names[n] = true
	}
	if !names["deleted1.txt"] || !names["deleted2.txt"] {
		t.Error("Whiteouts should return original file names")
	}
}

func TestWritableLayer_RemoveWhiteout(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	wl.Whiteout("deleted.txt")

	if !wl.IsWhiteout("deleted.txt") {
		t.Fatal("Whiteout should exist")
	}

	if err := wl.RemoveWhiteout("deleted.txt"); err != nil {
		t.Fatalf("RemoveWhiteout failed: %v", err)
	}

	if wl.IsWhiteout("deleted.txt") {
		t.Error("Whiteout should be removed")
	}
}

func TestWritableLayer_CopyUp(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create a mock source file (simulating a RO layer file)
	srcContent := []byte("original content from RO layer")
	srcFile := &File{
		Hdr: tar.Header{
			Name:     "copied.txt",
			Mode:     0755,
			Uid:      1000,
			Gid:      1000,
			Size:     int64(len(srcContent)),
			Typeflag: tar.TypeReg,
		},
	}

	// CopyUp
	copied, err := wl.CopyUp(srcFile, bytes.NewReader(srcContent))
	if err != nil {
		t.Fatalf("CopyUp failed: %v", err)
	}

	// Verify metadata was copied
	if copied.Hdr.Mode != srcFile.Hdr.Mode {
		t.Errorf("Mode not preserved: got %o, want %o", copied.Hdr.Mode, srcFile.Hdr.Mode)
	}
	if copied.Hdr.Uid != srcFile.Hdr.Uid {
		t.Errorf("Uid not preserved: got %d, want %d", copied.Hdr.Uid, srcFile.Hdr.Uid)
	}

	// Verify content was copied
	gotContent, err := os.ReadFile(copied.Path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(gotContent, srcContent) {
		t.Errorf("Content not copied correctly")
	}

	// Verify size was set correctly
	if copied.Hdr.Size != int64(len(srcContent)) {
		t.Errorf("Size mismatch: got %d, want %d", copied.Hdr.Size, len(srcContent))
	}
}

func TestWritableLayer_ContentPath(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	path := wl.ContentPath("some/file.txt")
	expected := filepath.Join(dir, contentDirName, "some/file.txt")
	if path != expected {
		t.Errorf("ContentPath mismatch: got %s, want %s", path, expected)
	}
}

func TestWritableLayer_OpenContent(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Open for creation
	f, err := wl.OpenContent("newfile.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenContent failed: %v", err)
	}

	f.WriteString("hello")
	f.Close()

	// Verify content was written
	content, _ := os.ReadFile(wl.ContentPath("newfile.txt"))
	if string(content) != "hello" {
		t.Errorf("Content mismatch: got %q", content)
	}
}

func TestWritableLayer_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	const numGoroutines = 50
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				name := filepath.Join("concurrent", string(rune('a'+id)), "file.txt")

				// Create
				wl.Create(name, 0644, false)

				// Get
				wl.Get(name)

				// Exists
				wl.Exists(name)

				// List
				wl.List("concurrent")
			}
		}(i)
	}

	wg.Wait()
	// Success = no panics or deadlocks
}

func TestWritableLayer_PersistPreservesAllFields(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create with specific attributes
	file, _ := wl.Create("full.txt", 0755, false)
	file.Hdr.Uid = 1000
	file.Hdr.Gid = 1000
	file.Hdr.Size = 12345
	wl.Update(file)

	wl.Persist()

	// Reload
	wl2, _ := NewWritableLayer(dir)
	reloaded := wl2.Get("full.txt")

	if reloaded == nil {
		t.Fatal("File not found after reload")
	}
	if reloaded.Hdr.Name != "full.txt" {
		t.Errorf("Name mismatch")
	}
	if reloaded.Hdr.Uid != 1000 {
		t.Errorf("Uid mismatch: got %d", reloaded.Hdr.Uid)
	}
	if reloaded.Hdr.Gid != 1000 {
		t.Errorf("Gid mismatch: got %d", reloaded.Hdr.Gid)
	}
	if reloaded.Hdr.Size != 12345 {
		t.Errorf("Size mismatch: got %d", reloaded.Hdr.Size)
	}
}

// =============================================================================
// Deprecated API Tests (ensure backward compatibility)
// =============================================================================

func TestWritableLayer_DeprecatedSetFile(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	hdr := tar.Header{
		Name:     "legacy.txt",
		Mode:     0644,
		Size:     100,
		Typeflag: tar.TypeReg,
		ModTime:  time.Now(),
	}

	file, err := wl.SetFile(hdr)
	if err != nil {
		t.Fatalf("SetFile failed: %v", err)
	}

	if file.Hdr.Name != "legacy.txt" {
		t.Errorf("Name mismatch")
	}

	// GetFile should also work
	got := wl.GetFile("legacy.txt")
	if got == nil {
		t.Error("GetFile returned nil")
	}
}

func TestWritableLayer_DeprecatedDeleteFile(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	wl.Create("todelete.txt", 0644, false)

	if err := wl.DeleteFile("todelete.txt"); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	if wl.Exists("todelete.txt") {
		t.Error("File should be deleted")
	}
}

func TestWritableLayer_DeprecatedListChildren(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	wl.Create("dir/file1.txt", 0644, false)
	wl.Create("dir/file2.txt", 0644, false)
	wl.Whiteout("dir/deleted.txt")

	// ListChildren includes whiteouts (unlike List)
	children := wl.ListChildren("dir")
	if len(children) != 3 {
		t.Errorf("ListChildren should include whiteouts: got %d, want 3", len(children))
	}
}

// =============================================================================
// Helper function tests
// =============================================================================

func TestToWhiteoutPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"file.txt", ".wh.file.txt"},
		{"dir/file.txt", "dir/.wh.file.txt"},
		{"a/b/c/file.txt", "a/b/c/.wh.file.txt"},
	}

	for _, tc := range tests {
		got := toWhiteoutPath(tc.input)
		if got != tc.expected {
			t.Errorf("toWhiteoutPath(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

// =============================================================================
// Integration-style tests
// =============================================================================

func TestWritableLayer_FullWorkflow(t *testing.T) {
	dir := t.TempDir()

	// Session 1: Create files and whiteouts
	{
		wl, _ := NewWritableLayer(dir)

		// Create some files
		f1, _ := wl.Create("file1.txt", 0644, false)
		os.WriteFile(f1.Path, []byte("content1"), 0644)
		f1.Hdr.Size = 8
		wl.Update(f1)

		wl.Create("dir/file2.txt", 0644, false)

		// Create a whiteout (simulating delete of RO file)
		wl.Whiteout("deleted_from_ro.txt")

		wl.Persist()
	}

	// Session 2: Reload and verify
	{
		wl, _ := NewWritableLayer(dir)

		// Verify files exist
		if !wl.Exists("file1.txt") {
			t.Error("file1.txt should exist")
		}
		if !wl.Exists("dir/file2.txt") {
			t.Error("dir/file2.txt should exist")
		}

		// Verify whiteout
		if !wl.IsWhiteout("deleted_from_ro.txt") {
			t.Error("deleted_from_ro.txt should be whited out")
		}

		// Verify content
		content, _ := os.ReadFile(wl.ContentPath("file1.txt"))
		if string(content) != "content1" {
			t.Errorf("Content mismatch: got %q", content)
		}

		// Remove a file
		wl.Remove("file1.txt")

		// Remove the whiteout
		wl.RemoveWhiteout("deleted_from_ro.txt")

		wl.Persist()
	}

	// Session 3: Verify removals persisted
	{
		wl, _ := NewWritableLayer(dir)

		if wl.Exists("file1.txt") {
			t.Error("file1.txt should be removed")
		}
		if wl.IsWhiteout("deleted_from_ro.txt") {
			t.Error("whiteout should be removed")
		}
		if !wl.Exists("dir/file2.txt") {
			t.Error("dir/file2.txt should still exist")
		}
	}
}

func TestWritableLayer_CopyUpThenModify(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Simulate CopyUp from RO layer
	srcContent := []byte("original RO content")
	srcFile := &File{
		Hdr: tar.Header{
			Name:     "copied.txt",
			Mode:     0644,
			Typeflag: tar.TypeReg,
		},
	}

	copied, _ := wl.CopyUp(srcFile, bytes.NewReader(srcContent))

	// Modify the copied file
	f, _ := os.OpenFile(copied.Path, os.O_WRONLY|os.O_APPEND, 0)
	f.WriteString(" + modifications")
	f.Close()

	// Update metadata
	fi, _ := os.Stat(copied.Path)
	copied.Hdr.Size = fi.Size()
	wl.Update(copied)

	// Verify
	got := wl.Get("copied.txt")
	if got.Hdr.Size != int64(len(srcContent)+len(" + modifications")) {
		t.Errorf("Size mismatch after modification")
	}

	content, _ := os.ReadFile(copied.Path)
	if string(content) != "original RO content + modifications" {
		t.Errorf("Content mismatch: got %q", content)
	}
}

func BenchmarkWritableLayer_Create(b *testing.B) {
	dir := b.TempDir()
	wl, _ := NewWritableLayer(dir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wl.Create("file.txt", 0644, false)
		wl.Remove("file.txt")
	}
}

func BenchmarkWritableLayer_CopyUp(b *testing.B) {
	dir := b.TempDir()
	wl, _ := NewWritableLayer(dir)

	content := bytes.Repeat([]byte("x"), 1024) // 1KB
	srcFile := &File{Hdr: tar.Header{Name: "bench.txt", Typeflag: tar.TypeReg}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wl.CopyUp(srcFile, bytes.NewReader(content))
		wl.Remove("bench.txt")
	}
}

func BenchmarkWritableLayer_ConcurrentGet(b *testing.B) {
	dir := b.TempDir()
	wl, _ := NewWritableLayer(dir)
	wl.Create("test.txt", 0644, false)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wl.Get("test.txt")
		}
	})
}

// =============================================================================
// Phase 1.2: WritableLayer Error Handling Tests
// =============================================================================

// TestWritableLayer_LoadCorruptedJSON tests handling of corrupted metadata JSON
func TestWritableLayer_LoadCorruptedJSON(t *testing.T) {
	dir := t.TempDir()

	// Create a corrupted metadata file
	metadataPath := dir + "/metadata.json"
	if err := os.WriteFile(metadataPath, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("Failed to write corrupted metadata: %v", err)
	}

	// Create content directory (required by NewWritableLayer)
	if err := os.MkdirAll(dir+"/content", 0755); err != nil {
		t.Fatalf("Failed to create content dir: %v", err)
	}

	// NewWritableLayer should fail on corrupted JSON
	_, err := NewWritableLayer(dir)
	if err == nil {
		t.Error("Expected error when loading corrupted JSON")
	}
}

// TestWritableLayer_LoadMissingFile tests that missing metadata is handled gracefully
func TestWritableLayer_LoadMissingFile(t *testing.T) {
	dir := t.TempDir()

	// NewWritableLayer should succeed with no existing metadata (fresh start)
	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed for fresh directory: %v", err)
	}
	defer wl.Close()

	// Should have no files
	if wl.Exists("anything") {
		t.Error("Fresh writable layer should have no files")
	}
}

// TestWritableLayer_CopyUpSourceMissing tests CopyUp with nil content reader
func TestWritableLayer_CopyUpSourceMissing(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	srcFile := &File{
		Hdr: tar.Header{
			Name:     "missing.txt",
			Typeflag: tar.TypeReg,
		},
	}

	// CopyUp with a reader that returns an error
	errReader := &errorReader{err: io.ErrUnexpectedEOF}
	_, err := wl.CopyUp(srcFile, errReader)
	if err == nil {
		t.Error("Expected error when CopyUp with failing reader")
	}

	// File should not exist in the layer
	if wl.Exists("missing.txt") {
		t.Error("File should not exist after failed CopyUp")
	}
}

// errorReader is an io.Reader that always returns an error
type errorReader struct {
	err error
}

func (e *errorReader) Read(p []byte) (int, error) {
	return 0, e.err
}

// TestWritableLayer_ConcurrentPersist tests multiple goroutines calling Persist
func TestWritableLayer_ConcurrentPersist(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	// Create some files
	for i := 0; i < 10; i++ {
		name := "file" + string(rune('a'+i)) + ".txt"
		wl.Create(name, 0644, false)
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := wl.Persist(); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent Persist error: %v", err)
	}
}

// TestWritableLayer_CloseWhilePersisting tests Close during active Persist
func TestWritableLayer_CloseWhilePersisting(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// Create many files to make Persist take longer
	for i := 0; i < 100; i++ {
		name := "file" + string(rune('a'+i%26)) + string(rune('0'+i/26)) + ".txt"
		wl.Create(name, 0644, false)
	}

	// Start Persist in background
	done := make(chan error)
	go func() {
		done <- wl.Persist()
	}()

	// Immediately call Close
	closeErr := wl.Close()

	// Wait for Persist to complete
	persistErr := <-done

	// Neither should panic; errors are acceptable
	if closeErr != nil {
		t.Logf("Close error (may be expected): %v", closeErr)
	}
	if persistErr != nil {
		t.Logf("Persist error (may be expected): %v", persistErr)
	}
}

// TestWritableLayer_AutoPersistRace tests mutations during auto-persist
func TestWritableLayer_AutoPersistRace(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWritableLayer(dir, WithAutoPersist(10*time.Millisecond))
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Perform many mutations while auto-persist is running
	const numMutations = 200
	for i := 0; i < numMutations; i++ {
		name := "file" + string(rune('a'+i%26)) + ".txt"
		wl.Create(name, 0644, false)
		time.Sleep(time.Millisecond) // Give auto-persist a chance to run
	}

	// Let auto-persist catch up
	time.Sleep(50 * time.Millisecond)

	// Verify no panics and data is consistent
	if !wl.Exists("filea.txt") {
		t.Error("Expected filea.txt to exist")
	}
}

// TestWritableLayer_PersistThreshold tests mutation-based auto-persist
func TestWritableLayer_PersistThreshold(t *testing.T) {
	dir := t.TempDir()
	wl, err := NewWritableLayer(dir, WithPersistAfterMutations(5))
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Perform mutations
	for i := 0; i < 10; i++ {
		name := "file" + string(rune('a'+i)) + ".txt"
		wl.Create(name, 0644, false)
	}

	// Give threshold persist time to complete
	time.Sleep(50 * time.Millisecond)

	// Reload to verify persist happened
	wl2, _ := NewWritableLayer(dir)
	defer wl2.Close()

	// At least some files should be persisted
	found := 0
	for i := 0; i < 10; i++ {
		name := "file" + string(rune('a'+i)) + ".txt"
		if wl2.Exists(name) {
			found++
		}
	}

	if found == 0 {
		t.Error("Expected some files to be persisted by threshold")
	}
}

// TestWritableLayer_CreateInvalidPath tests creating files with edge-case paths
func TestWritableLayer_CreateInvalidPath(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	testCases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty path", "", false}, // Empty is allowed, creates at content root
		{"dot path", ".", false},
		{"relative with dots", "../escape", false}, // Will be created but within content dir
		{"deeply nested", "a/b/c/d/e/f/g/file.txt", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := wl.Create(tc.path, 0644, false)
			if tc.wantErr && err == nil {
				t.Errorf("Expected error for path %q", tc.path)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected error for path %q: %v", tc.path, err)
			}
		})
	}
}

// TestWritableLayer_DoubleClose tests calling Close multiple times
func TestWritableLayer_DoubleClose(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)

	// First close should succeed
	if err := wl.Close(); err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	// Second close should be idempotent (no error)
	if err := wl.Close(); err != nil {
		t.Errorf("Second Close should be idempotent, got: %v", err)
	}
}

// TestWritableLayer_IsDirty tests the dirty flag tracking
func TestWritableLayer_IsDirty(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	// Fresh layer should not be dirty
	if wl.IsDirty() {
		t.Error("Fresh layer should not be dirty")
	}

	// Create a file - should become dirty
	wl.Create("file.txt", 0644, false)
	if !wl.IsDirty() {
		t.Error("Layer should be dirty after Create")
	}

	// Persist - should clear dirty flag
	wl.Persist()
	if wl.IsDirty() {
		t.Error("Layer should not be dirty after Persist")
	}

	// Update - should become dirty again
	f := wl.Get("file.txt")
	f.Hdr.Size = 100
	wl.Update(f)
	if !wl.IsDirty() {
		t.Error("Layer should be dirty after Update")
	}
}

// TestWritableLayer_RemoveNonexistent tests removing a file that doesn't exist
func TestWritableLayer_RemoveNonexistent(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	// Remove should be idempotent - no error for nonexistent file
	err := wl.Remove("nonexistent.txt")
	if err != nil {
		t.Errorf("Remove nonexistent should not error: %v", err)
	}
}

// TestWritableLayer_CopyUpLargeFile tests CopyUp with a larger file
func TestWritableLayer_CopyUpLargeFile(t *testing.T) {
	dir := t.TempDir()
	wl, _ := NewWritableLayer(dir)
	defer wl.Close()

	// Create 1MB of content
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	srcFile := &File{
		Hdr: tar.Header{
			Name:     "large.bin",
			Mode:     0644,
			Typeflag: tar.TypeReg,
		},
	}

	copied, err := wl.CopyUp(srcFile, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("CopyUp large file failed: %v", err)
	}

	// Verify size
	if copied.Hdr.Size != int64(len(content)) {
		t.Errorf("Size mismatch: got %d, want %d", copied.Hdr.Size, len(content))
	}

	// Verify content
	got, err := os.ReadFile(copied.Path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("Content mismatch for large file")
	}
}
