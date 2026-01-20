package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
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
