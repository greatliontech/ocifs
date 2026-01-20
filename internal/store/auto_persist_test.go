package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAutoPersist_Interval(t *testing.T) {
	dir := t.TempDir()

	// Create writable layer with auto-persist every 100ms
	wl, err := NewWritableLayer(dir, WithAutoPersist(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Create a file
	_, err = wl.Create("test.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify dirty flag is set
	if !wl.IsDirty() {
		t.Error("Expected dirty flag to be set after Create")
	}

	// Wait for auto-persist to trigger
	time.Sleep(150 * time.Millisecond)

	// Verify dirty flag is cleared
	if wl.IsDirty() {
		t.Error("Expected dirty flag to be cleared after auto-persist")
	}

	// Verify metadata was persisted
	metaPath := filepath.Join(dir, metadataFileName)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("Expected metadata file to exist after auto-persist")
	}
}

func TestAutoPersist_ThresholdMutations(t *testing.T) {
	dir := t.TempDir()

	// Create writable layer that persists after 3 mutations
	wl, err := NewWritableLayer(dir, WithPersistAfterMutations(3))
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Create 2 files - should not trigger persist
	wl.Create("file1.txt", 0644, false)
	wl.Create("file2.txt", 0644, false)

	// Give async persist time to run (if it were triggered)
	time.Sleep(50 * time.Millisecond)

	// Should still be dirty
	if !wl.IsDirty() {
		t.Error("Expected dirty flag to still be set after 2 mutations")
	}

	// Third mutation should trigger persist
	wl.Create("file3.txt", 0644, false)

	// Give async persist time to run
	time.Sleep(50 * time.Millisecond)

	// Should no longer be dirty
	if wl.IsDirty() {
		t.Error("Expected dirty flag to be cleared after threshold persist")
	}
}

func TestAutoPersist_Close(t *testing.T) {
	dir := t.TempDir()

	// Create writable layer with auto-persist
	wl, err := NewWritableLayer(dir, WithAutoPersist(1*time.Hour)) // Long interval
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}

	// Create a file
	_, err = wl.Create("test.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Close should persist
	if err := wl.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify metadata was persisted
	metaPath := filepath.Join(dir, metadataFileName)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("Expected metadata file to exist after Close")
	}

	// Double close should be safe
	if err := wl.Close(); err != nil {
		t.Errorf("Double Close should be safe: %v", err)
	}
}

func TestAutoPersist_NoAutoWithoutOption(t *testing.T) {
	dir := t.TempDir()

	// Create writable layer without auto-persist
	wl, err := NewWritableLayer(dir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Create a file
	_, err = wl.Create("test.txt", 0644, false)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Should still be dirty (no auto-persist)
	if !wl.IsDirty() {
		t.Error("Expected dirty flag to still be set without auto-persist")
	}

	// Metadata file should not exist yet
	metaPath := filepath.Join(dir, metadataFileName)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("Expected no metadata file without manual persist")
	}
}
