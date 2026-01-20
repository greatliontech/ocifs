package store

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/stream"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// setupTestRegistry creates an in-memory registry and returns the server URL.
func setupTestRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	r := registry.New()
	s := httptest.NewServer(r)
	t.Cleanup(s.Close)
	return s
}

// createLayerFromFiles creates a proper v1.Layer from a map of filename -> content.
func createLayerFromFiles(t *testing.T, files map[string][]byte) v1.Layer {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data := buf.Bytes()
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil {
		t.Fatalf("LayerFromOpener failed: %v", err)
	}

	return layer
}

// createTestImage creates a simple test image with files using proper go-containerregistry methods.
func createTestImage(t *testing.T, files map[string][]byte) v1.Image {
	t.Helper()

	// Start with empty image
	img := empty.Image

	if len(files) > 0 {
		layer := createLayerFromFiles(t, files)
		var err error
		img, err = mutate.AppendLayers(img, layer)
		if err != nil {
			t.Fatalf("AppendLayers failed: %v", err)
		}
	}

	// Set config
	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile failed: %v", err)
	}
	cfg = cfg.DeepCopy()
	cfg.Architecture = "amd64"
	cfg.OS = "linux"

	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		t.Fatalf("mutate.ConfigFile failed: %v", err)
	}

	return img
}

// pushTestImage pushes an image to the test registry.
func pushTestImage(t *testing.T, serverURL string, img v1.Image, repo, tag string) name.Reference {
	t.Helper()

	// Parse reference (strip http:// from server URL)
	host := strings.TrimPrefix(serverURL, "http://")
	refStr := host + "/" + repo + ":" + tag
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatalf("ParseReference failed: %v", err)
	}

	// Push the image
	if err := remote.Write(ref, img); err != nil {
		t.Fatalf("remote.Write failed: %v", err)
	}

	return ref
}

func TestStore_PullImage(t *testing.T) {
	// Setup in-memory registry
	server := setupTestRegistry(t)

	// Create and push a test image
	files := map[string][]byte{
		"hello.txt": []byte("Hello, World!"),
		"data.bin":  []byte{0x00, 0x01, 0x02, 0x03},
	}
	testImg := createTestImage(t, files)
	ref := pushTestImage(t, server.URL, testImg, "test/myimage", "v1")

	// Create store
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Pull the image
	ctx := context.Background()
	img, err := store.Image(ctx, ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Verify image was pulled
	if img == nil {
		t.Fatal("Expected image to be non-nil")
	}

	// Verify layers
	layers := img.Layers()
	if len(layers) != 1 {
		t.Errorf("Expected 1 layer, got %d", len(layers))
	}

	// Verify files from unified view
	unified := img.Unify()
	fileNames := make(map[string]bool)
	for _, f := range unified {
		fileNames[f.Hdr.Name] = true
	}

	if !fileNames["hello.txt"] {
		t.Error("Expected hello.txt in unified view")
	}
	if !fileNames["data.bin"] {
		t.Error("Expected data.bin in unified view")
	}
}

func TestStore_PullImageWithMultipleLayers(t *testing.T) {
	server := setupTestRegistry(t)

	// Create base image with one file
	baseImg := createTestImage(t, map[string][]byte{
		"base.txt": []byte("base content"),
	})

	// Add another layer
	layer2 := createLayerFromFiles(t, map[string][]byte{
		"layer2.txt": []byte("layer 2 content"),
	})
	multiLayerImg, err := mutate.AppendLayers(baseImg, layer2)
	if err != nil {
		t.Fatalf("AppendLayers failed: %v", err)
	}

	ref := pushTestImage(t, server.URL, multiLayerImg, "test/multilayer", "latest")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Should have 2 layers
	if len(img.Layers()) != 2 {
		t.Errorf("Expected 2 layers, got %d", len(img.Layers()))
	}

	// Unified view should have both files
	unified := img.Unify()
	files := make(map[string]bool)
	for _, f := range unified {
		files[f.Hdr.Name] = true
	}

	if !files["base.txt"] {
		t.Error("Expected base.txt")
	}
	if !files["layer2.txt"] {
		t.Error("Expected layer2.txt")
	}
}

func TestStore_PullPolicy_IfNotPresent(t *testing.T) {
	server := setupTestRegistry(t)

	testImg := createTestImage(t, map[string][]byte{
		"test.txt": []byte("test"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/cached", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullIfNotPresent)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	ctx := context.Background()

	// First pull should fetch from registry
	img1, err := store.Image(ctx, ref.String())
	if err != nil {
		t.Fatalf("First Image call failed: %v", err)
	}
	hash1 := img1.Hash()

	// Second pull should use cache
	img2, err := store.Image(ctx, ref.String())
	if err != nil {
		t.Fatalf("Second Image call failed: %v", err)
	}
	hash2 := img2.Hash()

	if hash1 != hash2 {
		t.Errorf("Expected same hash for cached image: %v vs %v", hash1, hash2)
	}
}

func TestStore_PullPolicy_Never(t *testing.T) {
	server := setupTestRegistry(t)

	testImg := createTestImage(t, map[string][]byte{
		"test.txt": []byte("test"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/never", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullNever)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Should fail because image not in cache
	_, err = store.Image(context.Background(), ref.String())
	if err == nil {
		t.Error("Expected error with PullNever and uncached image")
	}
}

func TestStore_Push(t *testing.T) {
	server := setupTestRegistry(t)

	// Create and push a base image
	baseImg := createTestImage(t, map[string][]byte{
		"original.txt": []byte("original content"),
	})
	ref := pushTestImage(t, server.URL, baseImg, "test/base", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	ctx := context.Background()

	// Pull the base image
	img, err := store.Image(ctx, ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Create a writable layer with changes
	wlDir := t.TempDir()
	wl, err := NewWritableLayer(wlDir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	// Add a new file
	newFile, _ := wl.Create("newfile.txt", 0644, false)
	newContent := []byte("new content added")
	contentPath := wl.ContentPath("newfile.txt")
	os.WriteFile(contentPath, newContent, 0644)
	newFile.Hdr.Size = int64(len(newContent))
	wl.Update(newFile)

	// Commit changes
	newImg, err := store.Commit(ctx, img, wl, CommitOptions{
		Author:  "test@example.com",
		Comment: "Added newfile.txt",
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Push to a new tag
	host := strings.TrimPrefix(server.URL, "http://")
	newRef := host + "/test/modified:v2"
	if err := store.Push(ctx, newImg, newRef); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Verify by pulling from the registry
	pulledRef, _ := name.ParseReference(newRef)
	pulledImg, err := remote.Image(pulledRef)
	if err != nil {
		t.Fatalf("remote.Image failed: %v", err)
	}

	// Should have one more layer than the base
	baseLayers, _ := baseImg.Layers()
	pulledLayers, _ := pulledImg.Layers()
	if len(pulledLayers) != len(baseLayers)+1 {
		t.Errorf("Expected %d layers, got %d", len(baseLayers)+1, len(pulledLayers))
	}
}

func TestStore_Commit_PreservesHistory(t *testing.T) {
	server := setupTestRegistry(t)

	// Create base image
	baseImg := createTestImage(t, map[string][]byte{
		"base.txt": []byte("base"),
	})
	ref := pushTestImage(t, server.URL, baseImg, "test/history", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Create empty writable layer
	wlDir := t.TempDir()
	wl, _ := NewWritableLayer(wlDir)
	defer wl.Close()

	// Commit with metadata
	newImg, err := store.Commit(context.Background(), img, wl, CommitOptions{
		Author:    "author@test.com",
		Comment:   "Test commit",
		CreatedBy: "ocifs test",
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check history
	cfg := newImg.ConfigFile()
	if len(cfg.History) == 0 {
		t.Fatal("Expected history entries")
	}

	lastHistory := cfg.History[len(cfg.History)-1]
	if lastHistory.Author != "author@test.com" {
		t.Errorf("Author mismatch: got %q", lastHistory.Author)
	}
	if lastHistory.Comment != "Test commit" {
		t.Errorf("Comment mismatch: got %q", lastHistory.Comment)
	}
	if lastHistory.CreatedBy != "ocifs test" {
		t.Errorf("CreatedBy mismatch: got %q", lastHistory.CreatedBy)
	}
}

func TestStore_Tag(t *testing.T) {
	server := setupTestRegistry(t)

	testImg := createTestImage(t, map[string][]byte{
		"test.txt": []byte("test"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/tag", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullIfNotPresent)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Tag with a new reference
	host := strings.TrimPrefix(server.URL, "http://")
	newRef := host + "/test/tag:latest"
	if err := store.Tag(img, newRef); err != nil {
		t.Fatalf("Tag failed: %v", err)
	}

	// Should be able to get the image by the new tag (from local cache)
	img2, err := store.Image(context.Background(), newRef)
	if err != nil {
		t.Fatalf("Image by new tag failed: %v", err)
	}

	if img.Hash() != img2.Hash() {
		t.Error("Tagged image should have same hash")
	}
}

func TestStore_BlobStore(t *testing.T) {
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullIfNotPresent)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	bs := store.BlobStore()
	if bs == nil {
		t.Fatal("BlobStore should not be nil")
	}

	// Put a blob
	content := []byte("test blob content")
	ref, err := bs.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get it back
	rc, err := bs.Get(ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, content) {
		t.Errorf("Content mismatch: got %q, want %q", got, content)
	}

	// OpenBlob convenience method
	rc2, err := store.OpenBlob(ref)
	if err != nil {
		t.Fatalf("OpenBlob failed: %v", err)
	}
	defer rc2.Close()

	got2, _ := io.ReadAll(rc2)
	if !bytes.Equal(got2, content) {
		t.Errorf("OpenBlob content mismatch")
	}
}

func TestStore_VerifyBlobRefPopulated(t *testing.T) {
	server := setupTestRegistry(t)

	// Create image with a file that has content
	testImg := createTestImage(t, map[string][]byte{
		"content.txt": []byte("some content here"),
	})
	ref := pushTestImage(t, server.URL, testImg, "test/blobref", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Check that files have BlobRef populated
	for _, layer := range img.Layers() {
		for _, f := range layer.Files() {
			if f.Hdr.Size > 0 {
				if f.BlobRef == "" {
					t.Errorf("File %s has size %d but no BlobRef", f.Hdr.Name, f.Hdr.Size)
				}
				if !strings.HasPrefix(f.BlobRef, "sha256:") {
					t.Errorf("BlobRef should start with sha256:, got %q", f.BlobRef)
				}
				// Path should also be set for backward compat
				if f.Path == "" {
					t.Errorf("File %s should have Path set", f.Hdr.Name)
				}
			}
		}
	}
}

func TestStore_CommitWithWhiteouts(t *testing.T) {
	server := setupTestRegistry(t)

	// Base image with files to delete
	baseImg := createTestImage(t, map[string][]byte{
		"keep.txt":   []byte("keep this"),
		"delete.txt": []byte("delete this"),
	})
	ref := pushTestImage(t, server.URL, baseImg, "test/whiteout", "v1")

	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Create writable layer and delete a file
	wlDir := t.TempDir()
	wl, _ := NewWritableLayer(wlDir)
	defer wl.Close()

	// Create whiteout for delete.txt
	wl.Whiteout("delete.txt")

	// Commit
	newImg, err := store.Commit(context.Background(), img, wl, CommitOptions{})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Push and pull back
	host := strings.TrimPrefix(server.URL, "http://")
	newRef := host + "/test/whiteout:v2"
	store.Push(context.Background(), newImg, newRef)

	// Create a new store to pull fresh
	storeDir2 := t.TempDir()
	store2, _ := NewStore(storeDir2, nil, PullAlways)
	pulledImg, err := store2.Image(context.Background(), newRef)
	if err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	// Unified view should not have delete.txt
	unified := pulledImg.Unify()
	for _, f := range unified {
		if f.Hdr.Name == "delete.txt" {
			t.Error("delete.txt should be removed by whiteout")
		}
	}

	// But keep.txt should still exist
	hasKeep := false
	for _, f := range unified {
		if f.Hdr.Name == "keep.txt" {
			hasKeep = true
			break
		}
	}
	if !hasKeep {
		t.Error("keep.txt should still exist")
	}
}

func TestStore_RoundTrip_FullWorkflow(t *testing.T) {
	server := setupTestRegistry(t)

	// Step 1: Create initial image
	initialImg := createTestImage(t, map[string][]byte{
		"config.json": []byte(`{"version": 1}`),
		"data/a.txt":  []byte("file a"),
		"data/b.txt":  []byte("file b"),
	})
	ref := pushTestImage(t, server.URL, initialImg, "test/workflow", "v1")

	// Step 2: Pull with store
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	t.Logf("Pulled image with %d layers", len(img.Layers()))

	// Step 3: Make modifications in writable layer
	wlDir := t.TempDir()
	wl, _ := NewWritableLayer(wlDir)
	defer wl.Close()

	// Add new file
	newFile, _ := wl.Create("data/c.txt", 0644, false)
	os.WriteFile(wl.ContentPath("data/c.txt"), []byte("file c"), 0644)
	newFile.Hdr.Size = 6
	wl.Update(newFile)

	// Delete file b
	wl.Whiteout("data/b.txt")

	// Modify config (create new version)
	configFile, _ := wl.Create("config.json", 0644, false)
	newConfig := []byte(`{"version": 2}`)
	os.WriteFile(wl.ContentPath("config.json"), newConfig, 0644)
	configFile.Hdr.Size = int64(len(newConfig))
	wl.Update(configFile)

	// Step 4: Commit
	newImg, err := store.Commit(context.Background(), img, wl, CommitOptions{
		Author:  "workflow@test.com",
		Comment: "Modified config, added c.txt, removed b.txt",
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Step 5: Push
	host := strings.TrimPrefix(server.URL, "http://")
	newRef := host + "/test/workflow:v2"
	if err := store.Push(context.Background(), newImg, newRef); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Step 6: Pull in a fresh store and verify
	storeDir2 := t.TempDir()
	store2, _ := NewStore(storeDir2, nil, PullAlways)
	finalImg, err := store2.Image(context.Background(), newRef)
	if err != nil {
		t.Fatalf("Pull v2 failed: %v", err)
	}

	unified := finalImg.Unify()
	files := make(map[string]struct{})
	for _, f := range unified {
		files[f.Hdr.Name] = struct{}{}
		t.Logf("Final file: %s (size=%d)", f.Hdr.Name, f.Hdr.Size)
	}

	// Verify final state
	if _, ok := files["data/a.txt"]; !ok {
		t.Error("data/a.txt should exist")
	}
	if _, ok := files["data/b.txt"]; ok {
		t.Error("data/b.txt should be deleted")
	}
	if _, ok := files["data/c.txt"]; !ok {
		t.Error("data/c.txt should exist")
	}
	if _, ok := files["config.json"]; !ok {
		t.Error("config.json should exist")
	}

	// Verify config content was updated
	for _, f := range unified {
		if f.Hdr.Name == "config.json" {
			content, _ := os.ReadFile(f.Path)
			if !bytes.Contains(content, []byte(`"version": 2`)) {
				t.Errorf("config.json should have version 2, got: %s", content)
			}
		}
	}
}

func TestStore_RandomImage(t *testing.T) {
	server := setupTestRegistry(t)

	// Use random.Image to generate a reproducible test image
	img, err := random.Image(256, 2) // 256 bytes per layer, 2 layers
	if err != nil {
		t.Fatalf("random.Image failed: %v", err)
	}

	// Push to registry
	host := strings.TrimPrefix(server.URL, "http://")
	refStr := host + "/test/random:v1"
	ref, _ := name.ParseReference(refStr)
	if err := remote.Write(ref, img); err != nil {
		t.Fatalf("remote.Write failed: %v", err)
	}

	// Pull with store
	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)
	pulledImg, err := store.Image(context.Background(), refStr)
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	if len(pulledImg.Layers()) != 2 {
		t.Errorf("Expected 2 layers, got %d", len(pulledImg.Layers()))
	}
}

func TestStore_ImageMediaTypes(t *testing.T) {
	server := setupTestRegistry(t)

	// Create OCI image (default)
	ociImg := createTestImage(t, map[string][]byte{
		"oci.txt": []byte("oci"),
	})
	ociRef := pushTestImage(t, server.URL, ociImg, "test/oci", "v1")

	// Create Docker image
	dockerImg := createTestImage(t, map[string][]byte{
		"docker.txt": []byte("docker"),
	})
	dockerImg = mutate.MediaType(dockerImg, types.DockerManifestSchema2)
	pushTestImage(t, server.URL, dockerImg, "test/docker", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)

	// Pull OCI
	img1, err := store.Image(context.Background(), ociRef.String())
	if err != nil {
		t.Fatalf("Pull OCI failed: %v", err)
	}
	t.Logf("OCI image layers: %d", len(img1.Layers()))

	// Pull Docker
	host := strings.TrimPrefix(server.URL, "http://")
	img2, err := store.Image(context.Background(), host+"/test/docker:v1")
	if err != nil {
		t.Fatalf("Pull Docker failed: %v", err)
	}
	t.Logf("Docker image layers: %d", len(img2.Layers()))
}

func TestStore_DirectoryStructure(t *testing.T) {
	storeDir := t.TempDir()
	_, err := NewStore(storeDir, nil, PullIfNotPresent)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Verify expected directories were created
	expectedDirs := []string{"refs", "blobs/sha256", "oci", "mounts"}
	for _, dir := range expectedDirs {
		path := filepath.Join(storeDir, dir)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("Expected directory %s to exist: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Expected %s to be a directory", dir)
		}
	}

	// Verify index.json exists
	indexPath := filepath.Join(storeDir, "oci", "index.json")
	if _, err := os.Stat(indexPath); err != nil {
		t.Errorf("Expected index.json to exist: %v", err)
	}
}

func TestStore_StreamingLayer(t *testing.T) {
	server := setupTestRegistry(t)

	// Create a streaming layer (tests that we handle different layer types)
	content := []byte("streaming layer content repeated many times" + strings.Repeat("x", 1000))

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{
		Name: "stream.txt",
		Mode: 0644,
		Size: int64(len(content)),
	})
	tw.Write(content)
	tw.Close()

	data := buf.Bytes()
	layer := stream.NewLayer(io.NopCloser(bytes.NewReader(data)))

	img, _ := mutate.AppendLayers(empty.Image, layer)
	ref := pushTestImage(t, server.URL, img, "test/stream", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)
	pulledImg, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	if len(pulledImg.Layers()) != 1 {
		t.Errorf("Expected 1 layer, got %d", len(pulledImg.Layers()))
	}
}

func TestStore_EmptyImage(t *testing.T) {
	server := setupTestRegistry(t)

	// Push an empty image (no layers)
	img := empty.Image
	ref := pushTestImage(t, server.URL, img, "test/empty", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)
	pulledImg, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	if len(pulledImg.Layers()) != 0 {
		t.Errorf("Expected 0 layers, got %d", len(pulledImg.Layers()))
	}

	unified := pulledImg.Unify()
	if len(unified) != 0 {
		t.Errorf("Expected empty unified view, got %d files", len(unified))
	}
}

func TestStore_LargeFile(t *testing.T) {
	server := setupTestRegistry(t)

	// Create a 1MB file
	largeContent := bytes.Repeat([]byte("abcdefghij"), 100*1024) // 1MB

	img := createTestImage(t, map[string][]byte{
		"large.bin": largeContent,
	})
	ref := pushTestImage(t, server.URL, img, "test/large", "v1")

	storeDir := t.TempDir()
	store, _ := NewStore(storeDir, nil, PullAlways)
	pulledImg, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Verify the file
	unified := pulledImg.Unify()
	var found bool
	for _, f := range unified {
		if f.Hdr.Name == "large.bin" {
			found = true
			if f.Hdr.Size != int64(len(largeContent)) {
				t.Errorf("Size mismatch: got %d, want %d", f.Hdr.Size, len(largeContent))
			}
			// Read content and verify
			content, err := os.ReadFile(f.Path)
			if err != nil {
				t.Fatalf("ReadFile failed: %v", err)
			}
			if !bytes.Equal(content, largeContent) {
				t.Error("Content mismatch for large file")
			}
		}
	}
	if !found {
		t.Error("large.bin not found in unified view")
	}
}
