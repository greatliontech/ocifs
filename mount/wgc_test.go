//go:build linux

package mount

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func TestGCPublicSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wgc")
	scratch := filepath.Join(".scratch", "ocifs-wgc")

	im, err := Mount(ofs, refStr, WithUpperDir(scratchtest.In(t, filepath.Join(scratch, "up"))))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "delta"), []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	committed, err := ofs.Commit(t.Context(), refStr, ocifs.CommitWithUpperDir(filepath.Join(scratch, "up")))
	if err != nil {
		t.Fatal(err)
	}

	if err := ofs.RemoveRef(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	res, err := ofs.GC(t.Context(), ocifs.GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	cim, err := Mount(ofs, ocifs.LocalRef(committed.Digest()))
	if err != nil {
		t.Fatalf("committed image unmountable after GC: %v", err)
	}
	if err := cim.Unmount(); err != nil {
		t.Fatal(err)
	}

	if err := ofs.RemoveImage(t.Context(), committed.Digest().String()); err != nil {
		t.Fatal(err)
	}
	res, err = ofs.GC(t.Context(), ocifs.GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatalf("nothing reported collected after removing the last root: %+v", res)
	}
	if _, err := Mount(ofs, ocifs.LocalRef(committed.Digest())); err == nil {
		t.Fatal("removed image still mounts")
	}
}

func TestRemoveAllPublicSurface(t *testing.T) {
	ofs, refStr := writableFixtureEnv(t, "wremoveall")
	scratch := filepath.Join(".scratch", "ocifs-wremoveall")
	im, err := Mount(ofs, refStr, WithUpperDir(scratchtest.In(t, filepath.Join(scratch, "up"))))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(im.MountPoint(), "delta"), []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}
	committed, err := ofs.Commit(t.Context(), refStr, ocifs.CommitWithUpperDir(filepath.Join(scratch, "up")))
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
	res, err := ofs.GC(t.Context(), ocifs.GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatalf("nothing reported collected after emptying: %+v", res)
	}
	if _, err := Mount(ofs, ocifs.LocalRef(committed.Digest())); err == nil {
		t.Fatal("the committed image still mounts after emptying")
	}
	if _, err := ofs.Resolve(t.Context(), refStr, ocifs.ResolveUnder(ocifs.PullNever)); err == nil {
		t.Fatal("the pulled reference still resolves from the store after emptying")
	}
}
