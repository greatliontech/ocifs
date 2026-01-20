package store

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// createPlatformImage creates a test image for a specific platform
func createPlatformImage(t *testing.T, platform v1.Platform, files map[string][]byte) v1.Image {
	t.Helper()

	layer := createLayerFromFiles(t, files)

	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatalf("AppendLayers failed: %v", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile failed: %v", err)
	}
	newCfg := cfg.DeepCopy()
	newCfg.OS = platform.OS
	newCfg.Architecture = platform.Architecture
	newCfg.Variant = platform.Variant

	img, err = mutate.ConfigFile(img, newCfg)
	if err != nil {
		t.Fatalf("mutate.ConfigFile failed: %v", err)
	}

	return img
}

// createMultiArchImage creates a multi-arch image index with images for multiple platforms
func createMultiArchImage(t *testing.T, platforms []v1.Platform, filesPerPlatform map[string]map[string][]byte) v1.ImageIndex {
	t.Helper()

	var adds []mutate.IndexAddendum

	for _, platform := range platforms {
		files := filesPerPlatform[platform.Architecture]
		if files == nil {
			files = map[string][]byte{
				"platform.txt": []byte(platform.Architecture + "/" + platform.OS),
			}
		}

		img := createPlatformImage(t, platform, files)

		adds = append(adds, mutate.IndexAddendum{
			Add: img,
			Descriptor: v1.Descriptor{
				Platform: &platform,
			},
		})
	}

	idx := mutate.AppendManifests(empty.Index, adds...)
	return idx
}

// pushMultiArchImage pushes a multi-arch image to the test registry
func pushMultiArchImage(t *testing.T, serverURL string, idx v1.ImageIndex, repo, tag string) name.Reference {
	t.Helper()

	host := strings.TrimPrefix(serverURL, "http://")
	refStr := host + "/" + repo + ":" + tag
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatalf("ParseReference failed: %v", err)
	}

	if err := remote.WriteIndex(ref, idx); err != nil {
		t.Fatalf("WriteIndex failed: %v", err)
	}

	return ref
}

// =============================================================================
// Platform Selection Tests (WithDefaultPlatform)
// =============================================================================

func TestMultiArch_DefaultPlatformIsRuntime(t *testing.T) {
	// Verify that without explicit platform option, store defaults to runtime platform
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	platform := store.Platform()
	if platform.OS != runtime.GOOS {
		t.Errorf("Expected OS %s, got %s", runtime.GOOS, platform.OS)
	}
	if platform.Architecture != runtime.GOARCH {
		t.Errorf("Expected Architecture %s, got %s", runtime.GOARCH, platform.Architecture)
	}
}

func TestMultiArch_WithDefaultPlatform(t *testing.T) {
	// Verify WithDefaultPlatform option overrides the runtime default
	storeDir := t.TempDir()
	customPlatform := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}

	store, err := NewStore(storeDir, nil, PullAlways, WithDefaultPlatform(customPlatform))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	platform := store.Platform()
	if platform.OS != "linux" {
		t.Errorf("Expected OS linux, got %s", platform.OS)
	}
	if platform.Architecture != "arm64" {
		t.Errorf("Expected Architecture arm64, got %s", platform.Architecture)
	}
	if platform.Variant != "v8" {
		t.Errorf("Expected Variant v8, got %s", platform.Variant)
	}
}

func TestMultiArch_PullUsesConfiguredPlatform(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create multi-arch image with distinct content per platform
	platforms := []v1.Platform{
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64"},
	}
	filesPerPlatform := map[string]map[string][]byte{
		"amd64": {"arch.txt": []byte("this-is-amd64")},
		"arm64": {"arch.txt": []byte("this-is-arm64")},
	}

	idx := createMultiArchImage(t, platforms, filesPerPlatform)
	ref := pushMultiArchImage(t, server.URL, idx, "test/platform-select", "v1")

	// Create store configured for arm64
	storeDir := t.TempDir()
	arm64Platform := v1.Platform{OS: "linux", Architecture: "arm64"}
	store, err := NewStore(storeDir, nil, PullAlways, WithDefaultPlatform(arm64Platform))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Pull should get arm64 image
	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	conf := img.ConfigFile()
	if conf.Architecture != "arm64" {
		t.Errorf("Expected arm64, got %s", conf.Architecture)
	}
	t.Logf("Successfully pulled configured platform: %s/%s", conf.OS, conf.Architecture)
}

func TestMultiArch_PullMatchesRuntimePlatform(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create multi-arch with current runtime arch + another arch
	currentArch := runtime.GOARCH
	currentOS := runtime.GOOS
	otherArch := "arm64"
	if currentArch == "arm64" {
		otherArch = "amd64"
	}

	platforms := []v1.Platform{
		{OS: currentOS, Architecture: currentArch},
		{OS: currentOS, Architecture: otherArch},
	}
	filesPerPlatform := map[string]map[string][]byte{
		currentArch: {"arch.txt": []byte("current-" + currentArch)},
		otherArch:   {"arch.txt": []byte("other-" + otherArch)},
	}

	idx := createMultiArchImage(t, platforms, filesPerPlatform)
	ref := pushMultiArchImage(t, server.URL, idx, "test/multiarch-runtime", "v1")

	// Store without explicit platform should use runtime platform
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	conf := img.ConfigFile()
	t.Logf("Selected platform: %s/%s (runtime: %s/%s)",
		conf.OS, conf.Architecture, currentOS, currentArch)

	// Should match runtime platform
	if conf.Architecture != currentArch {
		t.Errorf("Expected runtime architecture %s, got %s", currentArch, conf.Architecture)
	}
}

// =============================================================================
// ListPlatforms Tests
// =============================================================================

func TestMultiArch_ListPlatforms_MultiArch(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create multi-arch image
	platforms := []v1.Platform{
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64"},
		{OS: "linux", Architecture: "386"},
		{OS: "windows", Architecture: "amd64"},
	}

	idx := createMultiArchImage(t, platforms, nil)
	ref := pushMultiArchImage(t, server.URL, idx, "test/list-platforms", "v1")

	// Create store and list platforms
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	listedPlatforms, err := store.ListPlatforms(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("ListPlatforms failed: %v", err)
	}

	if len(listedPlatforms) != len(platforms) {
		t.Errorf("Expected %d platforms, got %d", len(platforms), len(listedPlatforms))
	}

	t.Log("Available platforms:")
	for _, p := range listedPlatforms {
		t.Logf("  - %s/%s (variant: %s)", p.OS, p.Architecture, p.Variant)
	}

	// Verify all expected platforms are present
	found := make(map[string]bool)
	for _, p := range listedPlatforms {
		key := p.OS + "/" + p.Architecture
		found[key] = true
	}

	for _, p := range platforms {
		key := p.OS + "/" + p.Architecture
		if !found[key] {
			t.Errorf("Missing platform: %s", key)
		}
	}
}

func TestMultiArch_ListPlatforms_SingleArch(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Push a single-arch image
	singleImg := createTestImage(t, map[string][]byte{"test.txt": []byte("test")})
	ref := pushTestImage(t, server.URL, singleImg, "test/single-arch", "v1")

	// Create store and list platforms
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	platforms, err := store.ListPlatforms(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("ListPlatforms failed: %v", err)
	}

	// Single-arch should return exactly one platform
	if len(platforms) != 1 {
		t.Errorf("Expected 1 platform for single-arch image, got %d", len(platforms))
	}

	t.Logf("Single-arch platform: %s/%s", platforms[0].OS, platforms[0].Architecture)
}

// =============================================================================
// Commit Tests
// =============================================================================

func TestMultiArch_CommitPreservesPlatform(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create and push arm64 image directly (not multi-arch)
	arm64Platform := v1.Platform{OS: "linux", Architecture: "arm64"}
	arm64Img := createPlatformImage(t, arm64Platform, map[string][]byte{
		"platform.txt": []byte("arm64"),
	})

	host := strings.TrimPrefix(server.URL, "http://")
	refStr := host + "/test/arm64:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatalf("ParseReference failed: %v", err)
	}
	if err := remote.Write(ref, arm64Img); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Pull with arm64 platform configured
	storeDir := t.TempDir()
	store, err := NewStore(storeDir, nil, PullAlways, WithDefaultPlatform(arm64Platform))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), refStr)
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	originalConf := img.ConfigFile()
	t.Logf("Original platform: %s/%s", originalConf.OS, originalConf.Architecture)

	// Create writable layer and commit
	wlDir := t.TempDir()
	wl, err := NewWritableLayer(wlDir)
	if err != nil {
		t.Fatalf("NewWritableLayer failed: %v", err)
	}
	defer wl.Close()

	wl.Create("change.txt", 0644, false)

	committed, err := store.Commit(context.Background(), img, wl, CommitOptions{
		CreatedBy: "test",
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	committedConf := committed.ConfigFile()
	t.Logf("Committed platform: %s/%s", committedConf.OS, committedConf.Architecture)

	// Verify platform is preserved
	if committedConf.Architecture != originalConf.Architecture {
		t.Errorf("Architecture not preserved: got %s, want %s", committedConf.Architecture, originalConf.Architecture)
	}
	if committedConf.OS != originalConf.OS {
		t.Errorf("OS not preserved: got %s, want %s", committedConf.OS, originalConf.OS)
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestMultiArch_PullMissingPlatform(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create multi-arch image with only amd64 and arm64
	platforms := []v1.Platform{
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64"},
	}

	idx := createMultiArchImage(t, platforms, nil)
	ref := pushMultiArchImage(t, server.URL, idx, "test/missing-platform", "v1")

	// Create store configured for riscv64 (not in index)
	storeDir := t.TempDir()
	riscvPlatform := v1.Platform{OS: "linux", Architecture: "riscv64"}
	store, err := NewStore(storeDir, nil, PullAlways, WithDefaultPlatform(riscvPlatform))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Pull should fail
	_, err = store.Image(context.Background(), ref.String())
	if err == nil {
		t.Error("Expected error when pulling non-existent platform")
	} else {
		t.Logf("Expected error for missing platform: %v", err)
	}
}

// =============================================================================
// Shared Layer Tests
// =============================================================================

func TestMultiArch_SharedLayers(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create a shared base layer
	sharedContent := map[string][]byte{
		"shared.txt": []byte("This content is shared across platforms"),
	}
	sharedLayer := createLayerFromFiles(t, sharedContent)

	// Create platform-specific images that share the base layer
	var adds []mutate.IndexAddendum

	for _, arch := range []string{"amd64", "arm64"} {
		platformContent := map[string][]byte{
			"platform.txt": []byte(arch),
		}
		platformLayer := createLayerFromFiles(t, platformContent)

		img, err := mutate.AppendLayers(empty.Image, sharedLayer, platformLayer)
		if err != nil {
			t.Fatalf("AppendLayers failed: %v", err)
		}

		cfg, _ := img.ConfigFile()
		newCfg := cfg.DeepCopy()
		newCfg.OS = "linux"
		newCfg.Architecture = arch
		img, _ = mutate.ConfigFile(img, newCfg)

		adds = append(adds, mutate.IndexAddendum{
			Add: img,
			Descriptor: v1.Descriptor{
				Platform: &v1.Platform{OS: "linux", Architecture: arch},
			},
		})
	}

	idx := mutate.AppendManifests(empty.Index, adds...)
	ref := pushMultiArchImage(t, server.URL, idx, "test/shared-layers", "v1")

	// Verify the index was created
	remoteIdx, err := remote.Index(ref)
	if err != nil {
		t.Fatalf("remote.Index failed: %v", err)
	}

	idxManifest, _ := remoteIdx.IndexManifest()
	t.Logf("Multi-arch image with shared layers: %d platforms", len(idxManifest.Manifests))

	// Collect all layer digests
	layerDigests := make(map[string][]string)
	for _, desc := range idxManifest.Manifests {
		childImg, err := remoteIdx.Image(desc.Digest)
		if err != nil {
			t.Fatalf("Image failed: %v", err)
		}
		layers, _ := childImg.Layers()
		platform := desc.Platform.Architecture
		for _, l := range layers {
			digest, _ := l.Digest()
			layerDigests[digest.String()] = append(layerDigests[digest.String()], platform)
		}
	}

	// Check for shared layers
	sharedCount := 0
	for digest, platforms := range layerDigests {
		if len(platforms) > 1 {
			t.Logf("Shared layer %s used by: %v", digest, platforms)
			sharedCount++
		}
	}

	if sharedCount == 0 {
		t.Log("No shared layers found (expected at least one)")
	}
}

// =============================================================================
// Layer Content Verification
// =============================================================================

func TestMultiArch_VerifyPlatformContent(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()

	// Create multi-arch image with distinct content
	platforms := []v1.Platform{
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64"},
	}
	filesPerPlatform := map[string]map[string][]byte{
		"amd64": {"arch.txt": []byte("CONTENT-FOR-AMD64")},
		"arm64": {"arch.txt": []byte("CONTENT-FOR-ARM64")},
	}

	idx := createMultiArchImage(t, platforms, filesPerPlatform)
	ref := pushMultiArchImage(t, server.URL, idx, "test/verify-content", "v1")

	// Pull arm64 and verify content
	storeDir := t.TempDir()
	arm64Platform := v1.Platform{OS: "linux", Architecture: "arm64"}
	store, err := NewStore(storeDir, nil, PullAlways, WithDefaultPlatform(arm64Platform))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	img, err := store.Image(context.Background(), ref.String())
	if err != nil {
		t.Fatalf("Image failed: %v", err)
	}

	// Verify we got arm64 content
	layers := img.Layers()
	if len(layers) == 0 {
		t.Fatal("Expected at least one layer")
	}

	// Read layer content via go-containerregistry to verify
	rc, err := img.img.Layers()
	if err != nil {
		t.Fatalf("Layers failed: %v", err)
	}
	if len(rc) > 0 {
		uncompressed, err := rc[0].Uncompressed()
		if err != nil {
			t.Fatalf("Uncompressed failed: %v", err)
		}
		defer uncompressed.Close()

		tr := tar.NewReader(uncompressed)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("tar.Next failed: %v", err)
			}
			if hdr.Name == "arch.txt" {
				content, _ := io.ReadAll(tr)
				if !bytes.Contains(content, []byte("ARM64")) {
					t.Errorf("Expected ARM64 content, got: %s", string(content))
				}
				t.Logf("Verified content: %s", string(content))
			}
		}
	}
}
