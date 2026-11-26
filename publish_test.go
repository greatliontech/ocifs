package ocifs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func sha256HashOf(b []byte) v1.Hash {
	sum := sha256.Sum256(b)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// assertValidOCIIndex verifies every OCI image-index, manifest, config,
// RootFS DiffID, and compressed/uncompressed layer digest invariant on idx
// without adding third-party test dependencies to go.mod.
func assertValidOCIIndex(t *testing.T, idx v1.ImageIndex, wantLayersPerImage int) {
	t.Helper()
	mt, err := idx.MediaType()
	if err != nil || mt != types.OCIImageIndex {
		t.Fatalf("index MediaType = %q (%v), want %q", mt, err, types.OCIImageIndex)
	}
	rawIdx, err := idx.RawManifest()
	if err != nil {
		t.Fatal(err)
	}
	idxDig, err := idx.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256HashOf(rawIdx); got != idxDig {
		t.Fatalf("index raw digest = %s, want %s", got, idxDig)
	}
	im, err := idx.IndexManifest()
	if err != nil {
		t.Fatal(err)
	}
	for _, md := range im.Manifests {
		if md.Platform == nil {
			t.Fatalf("index child %s missing Platform descriptor", md.Digest)
		}
		img, err := idx.Image(md.Digest)
		if err != nil {
			t.Fatal(err)
		}
		imt, err := img.MediaType()
		if err != nil || imt != types.OCIManifestSchema1 {
			t.Fatalf("child %s MediaType = %q (%v), want %q", md.Digest, imt, err, types.OCIManifestSchema1)
		}
		rawMan, err := img.RawManifest()
		if err != nil {
			t.Fatal(err)
		}
		if got := sha256HashOf(rawMan); got != md.Digest || int64(len(rawMan)) != md.Size {
			t.Fatalf("child manifest %s raw hash/size = %s/%d, want %s/%d", md.Digest, got, len(rawMan), md.Digest, md.Size)
		}
		m, err := img.Manifest()
		if err != nil {
			t.Fatal(err)
		}
		if m.Config.MediaType != types.OCIConfigJSON {
			t.Fatalf("child %s config MediaType = %q, want %q", md.Digest, m.Config.MediaType, types.OCIConfigJSON)
		}
		rawCfg, err := img.RawConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		if got := sha256HashOf(rawCfg); got != m.Config.Digest || int64(len(rawCfg)) != m.Config.Size {
			t.Fatalf("child %s config hash/size = %s/%d, want %s/%d", md.Digest, got, len(rawCfg), m.Config.Digest, m.Config.Size)
		}
		cfg, err := img.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OS != md.Platform.OS || cfg.Architecture != md.Platform.Architecture || cfg.Variant != md.Platform.Variant || cfg.OSVersion != md.Platform.OSVersion {
			t.Fatalf("child %s config platform (%s/%s/%s/%s) != descriptor (%+v)", md.Digest, cfg.OS, cfg.Architecture, cfg.Variant, cfg.OSVersion, md.Platform)
		}
		if cfg.RootFS.Type != "layers" {
			t.Fatalf("child %s RootFS.Type = %q, want \"layers\"", md.Digest, cfg.RootFS.Type)
		}
		layers, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Layers) != wantLayersPerImage || len(layers) != wantLayersPerImage || len(cfg.RootFS.DiffIDs) != wantLayersPerImage {
			t.Fatalf("child %s layer counts: manifest=%d img=%d diffIDs=%d, want %d", md.Digest, len(m.Layers), len(layers), len(cfg.RootFS.DiffIDs), wantLayersPerImage)
		}
		for i, l := range layers {
			compRC, err := l.Compressed()
			if err != nil {
				t.Fatal(err)
			}
			compBytes, err := io.ReadAll(compRC)
			_ = compRC.Close()
			if err != nil {
				t.Fatal(err)
			}
			if got := sha256HashOf(compBytes); got != m.Layers[i].Digest || int64(len(compBytes)) != m.Layers[i].Size {
				t.Fatalf("child %s layer %d compressed hash/size = %s/%d, want %s/%d", md.Digest, i, got, len(compBytes), m.Layers[i].Digest, m.Layers[i].Size)
			}
			uncompRC, err := l.Uncompressed()
			if err != nil {
				t.Fatal(err)
			}
			uncompBytes, err := io.ReadAll(uncompRC)
			_ = uncompRC.Close()
			if err != nil {
				t.Fatal(err)
			}
			if got := sha256HashOf(uncompBytes); got != cfg.RootFS.DiffIDs[i] {
				t.Fatalf("child %s layer %d uncompressed DiffID = %s, want %s", md.Digest, i, got, cfg.RootFS.DiffIDs[i])
			}
		}
		if !bytes.Contains(rawCfg, []byte(`"type":"layers"`)) {
			t.Fatalf("child %s raw config JSON missing rootfs.type=layers: %s", md.Digest, rawCfg)
		}
	}
}

func writeTree(t *testing.T, dir string, files map[string]string, mode os.FileMode, mtime time.Time) {
	t.Helper()
	for rel, body := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(full, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBuildIndexDeterminismAndPublishLifecycle(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-publish"))
	t1Linux := filepath.Join(scratch, "t1-linux")
	t1Darwin := filepath.Join(scratch, "t1-darwin")
	t2Linux := filepath.Join(scratch, "t2-linux")
	t2Darwin := filepath.Join(scratch, "t2-darwin")

	filesLinux := map[string]string{
		"bin/plugin":  "linux-amd64-elf",
		"bin/helper":  "helper-script",
		"etc/cfg.txt": "config-v1",
	}
	filesDarwin := map[string]string{
		"bin/plugin":  "darwin-arm64-macho",
		"bin/helper":  "helper-script",
		"etc/cfg.txt": "config-v1",
	}

	// Different host permissions and modification times must yield identical OCI digests.
	writeTree(t, t1Linux, filesLinux, 0o600, time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC))
	writeTree(t, t1Darwin, filesDarwin, 0o600, time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC))
	writeTree(t, t2Linux, filesLinux, 0o777, time.Date(2026, 9, 8, 7, 6, 5, 0, time.UTC))
	writeTree(t, t2Darwin, filesDarwin, 0o777, time.Date(2026, 9, 8, 7, 6, 5, 0, time.UTC))

	ofs, err := New(
		WithWorkDir(filepath.Join(scratch, "work")),
		WithDefaultPlatform(v1.Platform{OS: "linux", Architecture: "amd64"}),
		WithVerifyCachedExports(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ofs.Close()

	req1 := BuildRequest{
		Entrypoint:  []string{"/bin/plugin", "--serve"},
		Executables: []string{"/bin/helper"},
		Platforms: map[string]PlatformTree{
			"darwin/arm64": {Dir: t1Darwin},
			"linux/amd64":  {Dir: t1Linux},
		},
	}
	req2 := BuildRequest{
		Entrypoint:  []string{"/bin/plugin", "--serve"},
		Executables: []string{"/bin/helper"},
		Platforms: map[string]PlatformTree{
			"linux/amd64":  {Dir: t2Linux},
			"darwin/arm64": {Dir: t2Darwin},
		},
	}

	b1, err := ofs.BuildIndex(context.Background(), req1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := ofs.BuildIndex(context.Background(), req2)
	if err != nil {
		t.Fatal(err)
	}
	if b1.Digest != b2.Digest {
		t.Fatalf("non-deterministic index digest: %s vs %s", b1.Digest, b2.Digest)
	}
	if len(b1.Images) != 2 || b1.Images[0].Platform != "darwin/arm64" || b1.Images[1].Platform != "linux/amd64" {
		t.Fatalf("unexpected platform order: %+v", b1.Images)
	}
	if b1.Images[0].Digest != b2.Images[0].Digest || b1.Images[1].Digest != b2.Images[1].Digest {
		t.Fatalf("non-deterministic child digests: %+v vs %+v", b1.Images, b2.Images)
	}
	assertValidOCIIndex(t, b1.Index, 1)

	// First publish to explicit tag succeeds with Unchanged=false.
	tagRef := u.Host + "/plugins/protoc-gen-demo:v1.0.0"
	pub1, err := ofs.Publish(context.Background(), PublishRequest{
		Reference:    tagRef,
		BuildRequest: req1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pub1.Unchanged || pub1.Digest != b1.Digest {
		t.Fatalf("first Publish = %+v, want Unchanged=false Digest=%s", pub1, b1.Digest)
	}

	// Idempotent re-publish of identical content returns Unchanged=true.
	pub2, err := ofs.Publish(context.Background(), PublishRequest{
		Reference:    tagRef,
		BuildRequest: req2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !pub2.Unchanged || pub2.Digest != b1.Digest {
		t.Fatalf("idempotent Publish = %+v, want Unchanged=true", pub2)
	}

	// Pull and verified-export the published image through OCIFS.
	pulled, err := ofs.Pull(context.Background(), tagRef)
	if err != nil {
		t.Fatal(err)
	}
	cf := pulled.ConfigFile()
	if cf.Config.User != "65534:65534" || cf.Config.WorkingDir != "/" {
		t.Fatalf("unexpected scratch config: user=%q workdir=%q", cf.Config.User, cf.Config.WorkingDir)
	}
	if len(cf.Config.Entrypoint) != 2 || cf.Config.Entrypoint[0] != "/bin/plugin" || cf.Config.Entrypoint[1] != "--serve" {
		t.Fatalf("unexpected entrypoint: %v", cf.Config.Entrypoint)
	}
	expRoot, err := pulled.ExportVerified(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		for rel, wantPerm := range map[string]os.FileMode{
			"bin/plugin":  0o755,
			"bin/helper":  0o755,
			"etc/cfg.txt": 0o644,
		} {
			fi, err := os.Stat(filepath.Join(expRoot, filepath.FromSlash(rel)))
			if err != nil || fi.Mode().Perm() != wantPerm {
				t.Fatalf("%s perm = %v (%v), want %o", rel, fi, err, wantPerm)
			}
		}
	}

	// Mutating content at the same tag fails with ErrTagTaken unless Force=true.
	t3Linux := filepath.Join(scratch, "t3-linux")
	writeTree(t, t3Linux, map[string]string{
		"bin/plugin":  "linux-amd64-elf-v2",
		"bin/helper":  "helper-script",
		"etc/cfg.txt": "config-v2",
	}, 0o644, time.Unix(0, 0))
	reqChanged := BuildRequest{
		Entrypoint:  []string{"/bin/plugin"},
		Executables: []string{"/bin/helper"},
		Platforms: map[string]PlatformTree{
			"linux/amd64": {Dir: t3Linux},
		},
	}
	if _, err := ofs.Publish(context.Background(), PublishRequest{
		Reference:    tagRef,
		BuildRequest: reqChanged,
	}); !errors.Is(err, ErrTagTaken) {
		t.Fatalf("conflicting Publish without Force = %v, want ErrTagTaken", err)
	}

	pubForced, err := ofs.Publish(context.Background(), PublishRequest{
		Reference:    tagRef,
		BuildRequest: reqChanged,
		Force:        true,
	})
	if err != nil {
		t.Fatalf("Publish with Force=true failed: %v", err)
	}
	if pubForced.Unchanged || pubForced.Digest == b1.Digest {
		t.Fatalf("forced Publish = %+v, want updated digest", pubForced)
	}

	// Build over a digest-pinned base index preserves descriptor-only OSVersion,
	// arm64 Variant ("v8"), RootFS DiffIDs, and base layers.
	baseCfg := &v1.ConfigFile{
		OS:           "windows",
		Architecture: "amd64",
		RootFS:       v1.RootFS{Type: "layers"},
	}
	baseWin, err := mutate.ConfigFile(empty.Image, baseCfg)
	if err != nil {
		t.Fatal(err)
	}
	baseWin = testAppendFileLayer(t, baseWin, "Windows/System32/ntdll.dll", "base-dll")
	winPl := v1.Platform{OS: "windows", Architecture: "amd64", OSVersion: "10.0.20348.1"}

	armBaseCfg := &v1.ConfigFile{
		OS:           "linux",
		Architecture: "arm64",
		Variant:      "v8",
		RootFS:       v1.RootFS{Type: "layers"},
	}
	baseArm, err := mutate.ConfigFile(empty.Image, armBaseCfg)
	if err != nil {
		t.Fatal(err)
	}
	baseArm = testAppendFileLayer(t, baseArm, "lib/ld-musl-aarch64.so.1", "musl-loader")
	armPl := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}

	baseIdx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{
			Add:        baseWin,
			Descriptor: v1.Descriptor{Platform: &winPl},
		},
		mutate.IndexAddendum{
			Add:        baseArm,
			Descriptor: v1.Descriptor{Platform: &armPl},
		},
	)
	baseIdxDigest, err := baseIdx.Digest()
	if err != nil {
		t.Fatal(err)
	}
	baseRepoRef, err := name.ParseReference(u.Host + "/bases/multi:v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(baseRepoRef, baseIdx); err != nil {
		t.Fatal(err)
	}

	winTree := filepath.Join(scratch, "win-tree")
	writeTree(t, winTree, map[string]string{"bin/protoc-gen-demo": "pe-exe"}, 0o644, time.Unix(0, 0))
	armTree := filepath.Join(scratch, "arm-tree")
	writeTree(t, armTree, map[string]string{"bin/protoc-gen-demo": "arm64-elf"}, 0o644, time.Unix(0, 0))
	baseDigestRef := u.Host + "/bases/multi@" + baseIdxDigest.String()

	builtBase, err := ofs.BuildIndex(context.Background(), BuildRequest{
		Entrypoint: []string{"/bin/protoc-gen-demo"},
		Platforms: map[string]PlatformTree{
			"linux/arm64":   {Dir: armTree, Base: baseDigestRef},
			"windows/amd64": {Dir: winTree, Base: baseDigestRef},
		},
	})
	if err != nil {
		t.Fatalf("BuildIndex over digest-pinned base index: %v", err)
	}
	assertValidOCIIndex(t, builtBase.Index, 2)
	im, err := builtBase.Index.IndexManifest()
	if err != nil || len(im.Manifests) != 2 {
		t.Fatalf("unexpected base-derived index manifest: %+v (%v)", im, err)
	}
	if im.Manifests[0].Platform.Variant != "v8" {
		t.Fatalf("linux/arm64 Variant = %q, want v8", im.Manifests[0].Platform.Variant)
	}
	if im.Manifests[1].Platform.OSVersion != "10.0.20348.1" {
		t.Fatalf("windows/amd64 descriptor OSVersion = %q, want 10.0.20348.1", im.Manifests[1].Platform.OSVersion)
	}

	basePubTag := u.Host + "/plugins/protoc-gen-based:v1.0.0"
	if _, err := ofs.Publish(context.Background(), PublishRequest{
		Reference: basePubTag,
		BuildRequest: BuildRequest{
			Entrypoint: []string{"/bin/protoc-gen-demo"},
			Platforms: map[string]PlatformTree{
				"linux/arm64":   {Dir: armTree, Base: baseDigestRef},
				"windows/amd64": {Dir: winTree, Base: baseDigestRef},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	pulledArm, err := ofs.Pull(context.Background(), basePubTag, PullWithPlatform(v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}))
	if err != nil {
		t.Fatalf("strict PullWithPlatform(linux/arm64/v8) failed: %v", err)
	}
	if pulledArm.ConfigFile().Variant != "v8" {
		t.Fatalf("pulled arm64 config Variant = %q, want v8", pulledArm.ConfigFile().Variant)
	}
}

func testAppendFileLayer(t *testing.T, base v1.Image, file, content string) v1.Image {
	t.Helper()
	tmp := testPlatformImage(t, v1.Platform{OS: "windows", Architecture: "amd64"}, file, content)
	layers, err := tmp.Layers()
	if err != nil {
		t.Fatal(err)
	}
	out, err := mutate.AppendLayers(base, layers[0])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBuildAndPublishValidationErrors(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"bin/app": "ok"}, 0o644, time.Unix(0, 0))

	for name, req := range map[string]BuildRequest{
		"no-entrypoint": {
			Platforms: map[string]PlatformTree{"linux/amd64": {Dir: dir}},
		},
		"rel-entrypoint": {
			Entrypoint: []string{"bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir}},
		},
		"dotdot-entrypoint": {
			Entrypoint: []string{"/bin/../bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir}},
		},
		"missing-entrypoint": {
			Entrypoint: []string{"/bin/missing"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir}},
		},
		"missing-executable": {
			Entrypoint:  []string{"/bin/app"},
			Executables: []string{"/bin/missing-helper"},
			Platforms:   map[string]PlatformTree{"linux/amd64": {Dir: dir}},
		},
		"bad-platform": {
			Entrypoint: []string{"/bin/app"},
			Platforms:  map[string]PlatformTree{"LINUX/amd64": {Dir: dir}},
		},
		"unpinned-base": {
			Entrypoint: []string{"/bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir, Base: "ghcr.io/org/base:v1"}},
		},
		"local-namespace-base": {
			Entrypoint: []string{"/bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir, Base: "ocifs.local/org/base@sha256:" + strings.Repeat("a", 64)}},
		},
	} {
		if _, err := BuildIndex(context.Background(), req); err == nil {
			t.Fatalf("case %s succeeded unexpectedly", name)
		}
	}

	if runtime.GOOS != "windows" {
		symDir := t.TempDir()
		writeTree(t, symDir, map[string]string{"bin/app": "ok"}, 0o644, time.Unix(0, 0))
		if err := os.Symlink("app", filepath.Join(symDir, "bin", "link")); err != nil {
			t.Fatal(err)
		}
		if _, err := BuildIndex(context.Background(), BuildRequest{
			Entrypoint: []string{"/bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: symDir}},
		}); err == nil || !strings.Contains(err.Error(), "symbolic link") {
			t.Fatalf("symlink in tree = %v, want symlink rejection", err)
		}
	}

	// Implicit tag, implicit registry, or reserved ocifs.local namespace rejected.
	for _, badRef := range []string{
		"ghcr.io/org/plugin",
		"ocifs.local/org/plugin:v1.0.0",
	} {
		if _, err := Publish(context.Background(), PublishRequest{
			Reference: badRef,
			BuildRequest: BuildRequest{
				Entrypoint: []string{"/bin/app"},
				Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: dir}},
			},
		}); err == nil {
			t.Fatalf("Publish(%q) succeeded unexpectedly", badRef)
		}
	}

	// Case-colliding paths inside a platform tree are rejected on case-sensitive hosts.
	caseDir := t.TempDir()
	writeTree(t, caseDir, map[string]string{
		"bin/app": "lower",
		"bin/App": "upper",
	}, 0o644, time.Unix(0, 0))
	if entries, err := os.ReadDir(filepath.Join(caseDir, "bin")); err == nil && len(entries) == 2 {
		if _, err := BuildIndex(context.Background(), BuildRequest{
			Entrypoint: []string{"/bin/app"},
			Platforms:  map[string]PlatformTree{"linux/amd64": {Dir: caseDir}},
		}); err == nil || !strings.Contains(err.Error(), "case-colliding") {
			t.Fatalf("case-colliding BuildIndex = %v, want case-colliding rejection", err)
		}
	}
}
