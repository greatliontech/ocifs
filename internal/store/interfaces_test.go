package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestFSBlobStore_PutGet(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Put some content
	content := []byte("hello world")
	ref, err := bs.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify ref format
	if !strings.HasPrefix(ref, "sha256:") {
		t.Errorf("Expected ref to start with 'sha256:', got %q", ref)
	}

	// Get the content back
	rc, err := bs.Get(ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("Content mismatch: got %q, want %q", got, content)
	}
}

func TestFSBlobStore_Exists(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Non-existent blob
	if bs.Exists("sha256:nonexistent") {
		t.Error("Expected Exists to return false for non-existent blob")
	}

	// Put a blob
	ref, _ := bs.Put(bytes.NewReader([]byte("test")))

	// Now it should exist
	if !bs.Exists(ref) {
		t.Error("Expected Exists to return true after Put")
	}
}

func TestFSBlobStore_Delete(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Put a blob
	ref, _ := bs.Put(bytes.NewReader([]byte("test")))

	// Delete it
	if err := bs.Delete(ref); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Should no longer exist
	if bs.Exists(ref) {
		t.Error("Expected blob to not exist after Delete")
	}

	// Delete non-existent should not error
	if err := bs.Delete("sha256:nonexistent"); err != nil {
		t.Errorf("Delete non-existent should not error: %v", err)
	}
}

func TestFSBlobStore_Deduplication(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	content := []byte("duplicate content")

	// Put same content twice
	ref1, _ := bs.Put(bytes.NewReader(content))
	ref2, _ := bs.Put(bytes.NewReader(content))

	// Should get the same ref
	if ref1 != ref2 {
		t.Errorf("Expected same ref for identical content: %q vs %q", ref1, ref2)
	}
}

func TestFSContentStore_CreateOpen(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewFSContentStore(dir)
	if err != nil {
		t.Fatalf("NewFSContentStore failed: %v", err)
	}

	// Create a file
	f, err := cs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	f.Write([]byte("hello"))
	f.Close()

	// Open it for reading
	f, err = cs.Open("test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	content, _ := io.ReadAll(f)
	if string(content) != "hello" {
		t.Errorf("Content mismatch: got %q, want %q", content, "hello")
	}
}

func TestFSContentStore_NestedPaths(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewFSContentStore(dir)
	if err != nil {
		t.Fatalf("NewFSContentStore failed: %v", err)
	}

	// Create nested file - should auto-create parents
	f, err := cs.Create("a/b/c/file.txt")
	if err != nil {
		t.Fatalf("Create nested failed: %v", err)
	}
	f.Close()

	// Verify file exists
	info, err := cs.Stat("a/b/c/file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.IsDir() {
		t.Error("Expected file, got directory")
	}
}

func TestFSContentStore_MkdirAll(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewFSContentStore(dir)
	if err != nil {
		t.Fatalf("NewFSContentStore failed: %v", err)
	}

	// Create nested directories
	if err := cs.MkdirAll("x/y/z", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Verify directory exists
	info, err := cs.Stat("x/y/z")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if !info.IsDir() {
		t.Error("Expected directory")
	}
}

func TestFSContentStore_Remove(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewFSContentStore(dir)
	if err != nil {
		t.Fatalf("NewFSContentStore failed: %v", err)
	}

	// Create and remove file
	f, _ := cs.Create("toremove.txt")
	f.Close()

	if err := cs.Remove("toremove.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Should not exist
	_, err = cs.Stat("toremove.txt")
	if !os.IsNotExist(err) {
		t.Error("Expected file to not exist after Remove")
	}

	// Remove non-existent should not error
	if err := cs.Remove("nonexistent"); err != nil {
		t.Errorf("Remove non-existent should not error: %v", err)
	}
}

func TestFSContentStore_ContentPath(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewFSContentStore(dir)
	if err != nil {
		t.Fatalf("NewFSContentStore failed: %v", err)
	}

	path := cs.ContentPath("some/nested/file.txt")
	expected := filepath.Join(dir, "some/nested/file.txt")

	if path != expected {
		t.Errorf("ContentPath mismatch: got %q, want %q", path, expected)
	}
}

// =============================================================================
// Phase 1.3: BlobStore Robustness Tests
// =============================================================================

// TestBlobStore_ConcurrentPut tests concurrent puts of the same content
func TestBlobStore_ConcurrentPut(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	content := []byte("concurrent content test")
	const numGoroutines = 20

	refs := make(chan string, numGoroutines)
	errs := make(chan error, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref, err := bs.Put(bytes.NewReader(content))
			if err != nil {
				errs <- err
				return
			}
			refs <- ref
		}()
	}

	wg.Wait()
	close(refs)
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf("Concurrent Put error: %v", err)
	}

	// All refs should be the same (content-addressed)
	var firstRef string
	for ref := range refs {
		if firstRef == "" {
			firstRef = ref
		} else if ref != firstRef {
			t.Errorf("Expected all refs to be same, got %s vs %s", firstRef, ref)
		}
	}
}

// TestBlobStore_ConcurrentPutDifferent tests concurrent puts of different content
func TestBlobStore_ConcurrentPutDifferent(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	const numGoroutines = 20
	refs := make(chan string, numGoroutines)
	errs := make(chan error, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			content := []byte("content-" + string(rune('A'+id)))
			ref, err := bs.Put(bytes.NewReader(content))
			if err != nil {
				errs <- err
				return
			}
			refs <- ref
		}(i)
	}

	wg.Wait()
	close(refs)
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf("Concurrent Put different error: %v", err)
	}

	// Count unique refs - should have multiple unique refs
	refSet := make(map[string]bool)
	for ref := range refs {
		refSet[ref] = true
	}

	if len(refSet) < numGoroutines/2 {
		t.Errorf("Expected many unique refs, got only %d", len(refSet))
	}
}

// TestBlobStore_GetNonexistent tests getting a blob that doesn't exist
func TestBlobStore_GetNonexistent(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Try to get a non-existent blob
	_, err = bs.Get("sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Error("Expected error when getting non-existent blob")
	}
}

// TestBlobStore_DeleteNonexistent tests that deleting non-existent blob is idempotent
func TestBlobStore_DeleteNonexistent(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Delete non-existent should not error (idempotent)
	err = bs.Delete("sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Errorf("Delete non-existent should be idempotent: %v", err)
	}
}

// TestBlobStore_HashVerification tests that content matches its hash
func TestBlobStore_HashVerification(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	content := []byte("verify hash content")
	ref, err := bs.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify hash format
	if !strings.HasPrefix(ref, "sha256:") {
		t.Errorf("Expected sha256: prefix, got %s", ref)
	}

	// Get content and verify it matches
	rc, err := bs.Get(ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Error("Retrieved content doesn't match original")
	}
}

// TestBlobStore_LargeBlob tests putting and getting a large blob (10MB)
func TestBlobStore_LargeBlob(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Create 10MB content
	const size = 10 * 1024 * 1024
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}

	ref, err := bs.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put large blob failed: %v", err)
	}

	// Get and verify
	rc, err := bs.Get(ref)
	if err != nil {
		t.Fatalf("Get large blob failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(got) != size {
		t.Errorf("Size mismatch: got %d, want %d", len(got), size)
	}

	if !bytes.Equal(got, content) {
		t.Error("Large blob content mismatch")
	}
}

// TestBlobStore_EmptyBlob tests handling of empty content
func TestBlobStore_EmptyBlob(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Put empty content
	ref, err := bs.Put(bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("Put empty blob failed: %v", err)
	}

	// Verify it exists
	if !bs.Exists(ref) {
		t.Error("Empty blob should exist after Put")
	}

	// Get and verify
	rc, err := bs.Get(ref)
	if err != nil {
		t.Fatalf("Get empty blob failed: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if len(got) != 0 {
		t.Errorf("Expected empty content, got %d bytes", len(got))
	}
}

// TestBlobStore_ConcurrentReadWrite tests reading while writing
func TestBlobStore_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Pre-create a blob
	content := []byte("pre-existing content for read test")
	ref, _ := bs.Put(bytes.NewReader(content))

	const numReaders = 10
	const numWriters = 5

	var wg sync.WaitGroup
	errors := make(chan error, numReaders+numWriters)

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				rc, err := bs.Get(ref)
				if err != nil {
					errors <- err
					return
				}
				io.ReadAll(rc)
				rc.Close()
			}
		}()
	}

	// Start writers (different content each time)
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				newContent := []byte("writer-" + string(rune('0'+id)) + "-" + string(rune('0'+j)))
				_, err := bs.Put(bytes.NewReader(newContent))
				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent read/write error: %v", err)
	}
}

// TestBlobStore_DoubleDelete tests deleting the same blob twice
func TestBlobStore_DoubleDelete(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFSBlobStore failed: %v", err)
	}

	// Create a blob
	ref, _ := bs.Put(bytes.NewReader([]byte("delete me twice")))

	// First delete
	if err := bs.Delete(ref); err != nil {
		t.Errorf("First delete failed: %v", err)
	}

	// Second delete should be idempotent
	if err := bs.Delete(ref); err != nil {
		t.Errorf("Second delete should be idempotent: %v", err)
	}
}
