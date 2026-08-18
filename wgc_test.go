//go:build linux

package ocifs

import (
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
