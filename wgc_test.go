//go:build linux

package ocifs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestGCPublicSurface pins api.md REQ-api-remove and REQ-api-gc end
// to end: severing a reference through the public surface and
// collecting with the grace ignored reclaims the content, reported
// in the result; a second image stays rooted and servable.
func TestGCPublicSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wgc")
	scratch := filepath.Join(".scratch", "ocifs-wgc")

	// A second acquisition roots independently via commit.
	im, err := ofs.Mount(refStr, MountWithUpperDir(scratchtest.In(t, filepath.Join(scratch, "up"))))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "delta"), []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	committed, err := ofs.Commit(t.Context(), refStr, CommitWithUpperDir(filepath.Join(scratch, "up")))
	if err != nil {
		t.Fatal(err)
	}

	// Sever the pulled ref; the committed image's root keeps the
	// base layers reachable (they are its layers too).
	if err := ofs.RemoveRef(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	res, err := ofs.GC(t.Context(), GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	// The committed image still mounts.
	cim, err := ofs.Mount(LocalRef(committed.Digest()))
	if err != nil {
		t.Fatalf("committed image unmountable after GC: %v", err)
	}
	if err := cim.Unmount(); err != nil {
		t.Fatal(err)
	}

	// Remove the committed image too; a grace-ignored pass reclaims
	// everything and reports it.
	if err := ofs.RemoveImage(t.Context(), committed.Digest().String()); err != nil {
		t.Fatal(err)
	}
	res, err = ofs.GC(t.Context(), GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatalf("nothing reported collected after removing the last root: %+v", res)
	}
	if _, err := ofs.Mount(LocalRef(committed.Digest())); err == nil {
		t.Fatal("removed image still mounts")
	}
}

// TestRemoveAllPublicSurface pins REQ-api-remove's emptying at the
// library surface: a pulled reference and a committed image are
// severed at once, a grace-ignored collection reclaims their content,
// and neither mounts afterwards.
func TestRemoveAllPublicSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wremoveall")
	scratch := filepath.Join(".scratch", "ocifs-wremoveall")
	im, err := ofs.Mount(refStr, MountWithUpperDir(scratchtest.In(t, filepath.Join(scratch, "up"))))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "delta"), []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	committed, err := ofs.Commit(t.Context(), refStr, CommitWithUpperDir(filepath.Join(scratch, "up")))
	if err != nil {
		t.Fatal(err)
	}
	kept, err := ofs.RemoveAll(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 0 {
		t.Fatalf("kept = %v with no live mount", kept)
	}
	res, err := ofs.GC(t.Context(), GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatalf("nothing reported collected after emptying: %+v", res)
	}
	if _, err := ofs.Mount(LocalRef(committed.Digest())); err == nil {
		t.Fatal("the committed image still mounts after emptying")
	}
	if _, err := ofs.Resolve(t.Context(), refStr, ResolveUnder(PullNever)); err == nil {
		t.Fatal("the pulled reference still resolves from the store after emptying")
	}
}

// The collection's report is nameable at the library surface, and GC
// returns it by that name: a consumer holds GC to its word through
// the type (REQ-api-gc).
func TestGCResultNameable(t *testing.T) {
	var gc func(*OCIFS, context.Context, ...GCOption) (*GCResult, error) = (*OCIFS).GC
	if gc == nil {
		t.Fatal("GC does not return the report by its name")
	}
}
