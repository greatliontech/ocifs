package ocifs

import (
	"archive/tar"
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
	"sync"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func TestVerifiedCachedExportHealsTamperingAndRefusesCorruptCAS(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	const cleanContent = "clean-plugin-binary-v1"
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "bin", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "bin/plugin", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(cleanContent))},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := io.WriteString(tw, cleanContent); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	rawTar := buf.Bytes()

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	lyr, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(rawTar)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfgFile := &v1.ConfigFile{OS: amd64.OS, Architecture: amd64.Architecture}
	baseImg, err := mutate.ConfigFile(empty.Image, cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(baseImg, lyr)
	if err != nil {
		t.Fatal(err)
	}
	refStr := u.Host + "/test/verified-export:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-verified-export"))
	workDir := filepath.Join(scratch, "work")

	// Session 1: Pull and export under WithVerifyCachedExports.
	ofs1, err := New(
		WithWorkDir(workDir),
		WithDefaultPlatform(amd64),
		WithVerifyCachedExports(),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstRoot, err := ofs1.Export(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := ofs1.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2 (reopen offline): pristine cached export is served in place.
	ofs2, err := New(
		WithWorkDir(workDir),
		WithDefaultPlatform(amd64),
		WithPullPolicy(PullNever),
		WithVerifyCachedExports(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ofs2.Close()

	secondRoot, err := ofs2.Export(context.Background(), refStr)
	if err != nil {
		t.Fatalf("pristine cached export failed verification: %v", err)
	}
	if secondRoot != firstRoot {
		t.Fatalf("secondRoot = %q, want %q", secondRoot, firstRoot)
	}

	// Tamper 1: overwrite exported binary with same-length malicious bytes AND plant a rogue file.
	pluginPath := filepath.Join(firstRoot, "bin", "plugin")
	tampered := strings.Repeat("X", len(cleanContent))
	if err := os.WriteFile(pluginPath, []byte(tampered), 0o755); err != nil {
		t.Fatal(err)
	}
	roguePath := filepath.Join(firstRoot, "bin", "rogue")
	if err := os.WriteFile(roguePath, []byte("rogue"), 0o644); err != nil {
		t.Fatal(err)
	}

	healedRoot, err := ofs2.Export(context.Background(), refStr)
	if err != nil {
		t.Fatalf("self-healing dirtied cached export failed: %v", err)
	}
	if healedRoot != firstRoot {
		t.Fatalf("healedRoot = %q, want %q", healedRoot, firstRoot)
	}
	gotBytes, err := os.ReadFile(pluginPath)
	if err != nil || string(gotBytes) != cleanContent {
		t.Fatalf("healed plugin content = %q, %v; want %q", gotBytes, err, cleanContent)
	}
	if _, err := os.Stat(roguePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rogue file survived self-heal: %v", err)
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(pluginPath)
		if err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("healed plugin mode = %v, %v; want 0755", fi, err)
		}
	}
	// Ensure no .export-* or .export-condemned-* siblings remain.
	siblings, err := os.ReadDir(filepath.Dir(firstRoot))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range siblings {
		if strings.HasPrefix(s.Name(), ".export-") {
			t.Fatalf("leftover export temporary %q", s.Name())
		}
	}

	// Tamper 2: corrupt BOTH the cached export AND the underlying CAS blob.
	// Verified export must fail closed with *IntegrityError (ErrIntegrity) and remove the dirtied export.
	sum := sha256.Sum256([]byte(cleanContent))
	casBlob := filepath.Join(workDir, "blobs", "sha256", hex.EncodeToString(sum[:]))
	if err := os.WriteFile(casBlob, []byte("corrupted-cas-bytes!!!"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pluginPath, []byte("dirtied-again"), 0o755); err != nil {
		t.Fatal(err)
	}

	var ierr *IntegrityError
	_, err = ofs2.Export(context.Background(), refStr)
	if !errors.Is(err, ErrIntegrity) || !errors.As(err, &ierr) {
		t.Fatalf("export over corrupt CAS = %v, want *IntegrityError / ErrIntegrity", err)
	}
	if _, statErr := os.Stat(firstRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("dirtied export survived failed integrity check at %s: %v", firstRoot, statErr)
	}

	// Per-call ExportWithVerify to a target directory also fails closed with ErrIntegrity.
	targetDir := filepath.Join(scratch, "verified-target")
	if _, err := ofs2.Export(context.Background(), refStr, ExportWithTargetPath(targetDir), ExportWithVerify()); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("ExportWithVerify target over corrupt CAS = %v, want ErrIntegrity", err)
	}
}

func TestConcurrentExportVerifiedAndReadOnlyTempCleanup(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	const body = "concurrent-verified-payload"
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "bin", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "bin/plugin", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(body))},
		{Name: "ro", Typeflag: tar.TypeDir, Mode: 0o555},
		{Name: "ro/conf", Typeflag: tar.TypeReg, Mode: 0o444, Size: int64(len(body))},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := io.WriteString(tw, body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	rawTar := buf.Bytes()

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	lyr, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(rawTar)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, lyr)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cf = cf.DeepCopy()
	cf.OS = amd64.OS
	cf.Architecture = amd64.Architecture
	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		t.Fatal(err)
	}

	refStr := u.Host + "/test/concurrent-verified:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}

	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-concurrent-verified"))
	workDir := filepath.Join(scratch, "work")
	ofs, err := New(
		WithWorkDir(workDir),
		WithDefaultPlatform(amd64),
		WithVerifyCachedExports(),
	)
	if err != nil {
		t.Fatal(err)
	}
	pulled, err := ofs.Pull(context.Background(), refStr)
	if err != nil {
		t.Fatal(err)
	}

	// 1. 8-way cold-cache concurrent ExportVerified race: winners must never be
	//    condemned or unlinked out from under concurrent callers.
	const workers = 8
	runRace := func(label string) string {
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		roots := make(chan string, workers)
		start := make(chan struct{})
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				r, e := pulled.ExportVerified(context.Background())
				if e != nil {
					errs <- e
					return
				}
				got, rerr := os.ReadFile(filepath.Join(r, "bin", "plugin"))
				if rerr != nil {
					errs <- rerr
					return
				}
				if string(got) != body {
					errs <- errors.New("unexpected content: " + string(got))
					return
				}
				roots <- r
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		close(roots)
		for e := range errs {
			t.Fatalf("%s concurrent ExportVerified error: %v", label, e)
		}
		var out string
		for r := range roots {
			out = r
		}
		return out
	}

	finalRoot := runRace("cold-cache")

	// 2. Dirtied cache concurrent heal race.
	_ = os.Chmod(filepath.Join(finalRoot, "bin", "plugin"), 0o755)
	if err := os.WriteFile(filepath.Join(finalRoot, "bin", "plugin"), []byte(strings.Repeat("B", len(body))), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = runRace("dirty-heal")

	// 3. Canceled context must NEVER condemn or delete a live cached export.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pulled.ExportVerified(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ExportVerified = %v, want context.Canceled", err)
	}
	if got, err := os.ReadFile(filepath.Join(finalRoot, "bin", "plugin")); err != nil || string(got) != body {
		t.Fatalf("live export damaged by canceled ExportVerified: %q, %v", got, err)
	}
	if err := ofs.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. Unowned crashed .export-* temporary containing a 0o500 read-only directory
	//    must be swept cleanly at store startup (DebrisSweep -> forceRemoveTree).
	crashedTmp := filepath.Join(filepath.Dir(finalRoot), ".export-crashed-ro")
	roSub := filepath.Join(crashedTmp, "ro", "nested")
	if err := os.MkdirAll(roSub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(roSub, "locked"), []byte("x"), 0o400); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(roSub, 0o500)
	_ = os.Chmod(filepath.Join(crashedTmp, "ro"), 0o500)

	ofsReopen, err := New(WithWorkDir(workDir), WithDefaultPlatform(amd64), WithPullPolicy(PullNever))
	if err != nil {
		t.Fatal(err)
	}
	defer ofsReopen.Close()

	if _, err := os.Stat(crashedTmp); !errors.Is(err, os.ErrNotExist) {
		_ = os.Chmod(filepath.Join(crashedTmp, "ro"), 0o700)
		_ = os.Chmod(roSub, 0o700)
		t.Fatalf("crashed read-only .export-* temp survived DebrisSweep: %v", err)
	}
}
