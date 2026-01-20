package store

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
)

// =============================================================================
// Phase 4.1: Full Workflow Integration Tests
// =============================================================================

// TestIntegration_PullMountModifyCommitPush tests the full workflow
func TestIntegration_PullMountModifyCommitPush(t *testing.T) {
	// Setup registry
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)

	// Create initial image
	initialFiles := map[string][]byte{
		"config.json": []byte(`{"version": 1}`),
		"data.txt":    []byte("initial data"),
	}
	testImg := createTestImage(t, initialFiles)
	ref := pushTestImage(t, server.URL, testImg, "test/workflow", "v1")

	// Step 1: Pull
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	ctx := context.Background()
	img, err := store.Image(ctx, ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Step 2: Create writable layer and modify
	wlDir := t.TempDir()
	wl, err := NewWritableLayer(wlDir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Add new file
	newFile, _ := wl.Create("new.txt", 0644, false)
	os.WriteFile(wl.ContentPath("new.txt"), []byte("new content"), 0644)
	newFile.Hdr.Size = 11
	wl.Update(newFile)

	// Delete existing file
	wl.Whiteout("data.txt")

	// Modify existing file
	configFile, _ := wl.Create("config.json", 0644, false)
	os.WriteFile(wl.ContentPath("config.json"), []byte(`{"version": 2}`), 0644)
	configFile.Hdr.Size = 14
	wl.Update(configFile)

	// Step 3: Commit
	newImg, err := store.Commit(ctx, img, wl, CommitOptions{
		Author:  "test@example.com",
		Comment: "Modified files",
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Step 4: Push
	host := strings.TrimPrefix(server.URL, "http://")
	newRef := host + "/test/workflow:v2"
	if err := store.Push(ctx, newImg, newRef); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Step 5: Verify by pulling fresh
	store2Dir := t.TempDir()
	store2, _ := NewStore(store2Dir, nil, PullAlways)
	pulledImg, err := store2.Image(ctx, newRef)
	if err != nil {
		t.Fatalf("Pull v2 failed: %v", err)
	}

	// Verify content
	unified := pulledImg.Unify()
	files := make(map[string]struct{})
	for _, f := range unified {
		files[f.Hdr.Name] = struct{}{}
	}

	// Check new.txt exists
	if _, ok := files["new.txt"]; !ok {
		t.Error("new.txt should exist")
	}

	// Check data.txt is deleted
	if _, ok := files["data.txt"]; ok {
		t.Error("data.txt should be deleted")
	}

	// Check config.json exists
	if _, ok := files["config.json"]; !ok {
		t.Error("config.json should exist")
	}
}

// TestIntegration_MountUnmountRemount tests that state is preserved across unmount/remount
func TestIntegration_MountUnmountRemount(t *testing.T) {
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)

	testImg := createTestImage(t, map[string][]byte{
		"file.txt": []byte("content"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/remount", "v1")

	storeDir := t.TempDir()
	wlDir := t.TempDir()

	// First "mount" - pull and modify
	{
		store, _ := NewStore(storeDir, nil, PullAlways)
		img, _ := store.Image(context.Background(), ref.String())
		wl, _ := NewWritableLayer(wlDir)

		// Make changes
		newFile, _ := wl.Create("added.txt", 0644, false)
		os.WriteFile(wl.ContentPath("added.txt"), []byte("added"), 0644)
		newFile.Hdr.Size = 5
		wl.Update(newFile)
		wl.Persist()
		wl.Close()

		// Verify image is available
		if img == nil {
			t.Fatal("Image should not be nil")
		}
	}

	// Second "mount" - verify changes persisted
	{
		wl, err := NewWritableLayer(wlDir)
		if err != nil {
			t.Fatalf("NewWritableLayer failed on remount: %v", err)
		}
		defer wl.Close()

		// Verify the file we added is still there
		if !wl.Exists("added.txt") {
			t.Error("added.txt should exist after remount")
		}
	}
}

// TestIntegration_MultipleSimultaneousMounts tests multiple stores
func TestIntegration_MultipleSimultaneousMounts(t *testing.T) {
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)

	testImg := createTestImage(t, map[string][]byte{
		"shared.txt": []byte("shared content"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/multi", "v1")

	ctx := context.Background()

	// Create multiple stores
	const numStores = 3
	stores := make([]*Store, numStores)
	for i := 0; i < numStores; i++ {
		storeDir := t.TempDir()
		store, err := NewStore(storeDir, nil, PullAlways)
		if err != nil {
			t.Fatalf("NewStore %d failed: %v", i, err)
		}
		stores[i] = store
	}

	// Pull same image from all stores
	for i, store := range stores {
		img, err := store.Image(ctx, ref.String())
		if err != nil {
			t.Fatalf("Image %d failed: %v", i, err)
		}
		if len(img.Layers()) == 0 {
			t.Errorf("Store %d: expected layers", i)
		}
	}
}

// TestIntegration_CommitThenMountNewImage tests committing and then using the new image
func TestIntegration_CommitThenMountNewImage(t *testing.T) {
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)

	baseImg := createTestImage(t, map[string][]byte{
		"base.txt": []byte("base"),
	})
	ref := pushTestImage(t, server.URL, baseImg, "test/chain", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)
	ctx := context.Background()

	// Pull base
	img1, _ := store.Image(ctx, ref.String())

	// Create commit chain
	currentImg := img1
	for i := 0; i < 3; i++ {
		wlDir := t.TempDir()
		wl, _ := NewWritableLayer(wlDir)

		// Add file
		name := "file" + string(rune('1'+i)) + ".txt"
		f, _ := wl.Create(name, 0644, false)
		content := []byte("content " + string(rune('1'+i)))
		os.WriteFile(wl.ContentPath(name), content, 0644)
		f.Hdr.Size = int64(len(content))
		wl.Update(f)

		// Commit
		newImg, err := store.Commit(ctx, currentImg, wl, CommitOptions{})
		wl.Close()
		if err != nil {
			t.Fatalf("Commit %d failed: %v", i, err)
		}
		currentImg = newImg
	}

	// Verify final image has all files
	unified := currentImg.Unify()
	expected := []string{"base.txt", "file1.txt", "file2.txt", "file3.txt"}
	for _, name := range expected {
		found := false
		for _, f := range unified {
			if f.Hdr.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected %s in final image", name)
		}
	}
}

// TestIntegration_LargeImageWorkflow tests workflow with larger content
func TestIntegration_LargeImageWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large image test in short mode")
	}

	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)

	// Create image with larger files
	largeContent := bytes.Repeat([]byte("x"), 5*1024*1024) // 5MB
	testImg := createTestImage(t, map[string][]byte{
		"large.bin":  largeContent,
		"small.txt":  []byte("small"),
		"medium.bin": bytes.Repeat([]byte("m"), 1024*1024), // 1MB
	})
	ref := pushTestImage(t, server.URL, testImg, "test/large", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)

	// Pull
	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Verify files
	unified := img.Unify()
	for _, f := range unified {
		if f.Hdr.Name == "large.bin" {
			if f.Hdr.Size != int64(len(largeContent)) {
				t.Errorf("large.bin size mismatch: got %d, want %d", f.Hdr.Size, len(largeContent))
			}
		}
	}
}
