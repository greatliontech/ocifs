//go:build linux

package mount

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/ocifs"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func TestChurnSoak(t *testing.T) {
	skipUnderMutationCampaign(t)
	ofs, refStr := writableFixtureEnv(t, "wchurn", ocifs.WithAutoGC(true), ocifs.WithGCGrace(0))
	scratch := filepath.Join(".scratch", "ocifs-wchurn")

	var lastCommitted v1.Hash
	for cycle := 0; cycle < 5; cycle++ {
		upDir := scratchtest.In(t, filepath.Join(scratch, fmt.Sprintf("up%d", cycle)))
		im, err := Mount(ofs, refStr, WithUpperDir(upDir))
		if err != nil {
			t.Fatalf("cycle %d mount: %v", cycle, err)
		}
		name := fmt.Sprintf("cycle-%d", cycle)
		if err := os.WriteFile(filepath.Join(im.MountPoint(), name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := im.Unmount(); err != nil {
			t.Fatalf("cycle %d unmount: %v", cycle, err)
		}
		committed, err := ofs.Commit(t.Context(), refStr, ocifs.CommitWithUpperDir(upDir))
		if err != nil {
			t.Fatalf("cycle %d commit: %v", cycle, err)
		}
		cim, err := Mount(ofs, ocifs.LocalRef(committed.Digest()))
		if err != nil {
			t.Fatalf("cycle %d committed mount: %v", cycle, err)
		}
		b, err := os.ReadFile(filepath.Join(cim.MountPoint(), name))
		if err != nil || string(b) != name {
			t.Fatalf("cycle %d delta: %q %v", cycle, b, err)
		}
		if err := cim.Unmount(); err != nil {
			t.Fatal(err)
		}
		if lastCommitted != (v1.Hash{}) {
			if err := ofs.RemoveImage(t.Context(), lastCommitted.String()); err != nil {
				t.Fatalf("cycle %d remove prior: %v", cycle, err)
			}
			if _, err := Mount(ofs, ocifs.LocalRef(lastCommitted)); err == nil {
				t.Fatalf("cycle %d: removed prior commit still mounts", cycle)
			}
		}
		lastCommitted = committed.Digest()
	}

	if err := ofs.RemoveImage(t.Context(), lastCommitted.String()); err != nil {
		t.Fatal(err)
	}
	if err := ofs.RemoveRef(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	res, err := ofs.GC(t.Context(), ocifs.GCIgnoreGrace())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deferred) != 0 || len(res.ForeignVersionRows) != 0 {
		t.Fatalf("teardown pass not clean: %+v", res)
	}
	work := filepath.Join(scratch, "work")
	if _, err := os.Stat(work); err != nil {
		t.Fatalf("fixture layout drifted; the tier-empty assertion would be vacuous: %v", err)
	}
	for _, tier := range []string{"blobs", filepath.Join("oci", "blobs")} {
		empty, err := tierEmpty(filepath.Join(work, tier))
		if err != nil {
			t.Fatal(err)
		}
		if !empty {
			t.Fatalf("tier %s not empty after full teardown", tier)
		}
	}
}

func tierEmpty(root string) (bool, error) {
	algos, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, a := range algos {
		if !a.IsDir() {
			return false, nil
		}
		entries, err := os.ReadDir(filepath.Join(root, a.Name()))
		if err != nil {
			return false, err
		}
		if len(entries) != 0 {
			return false, nil
		}
	}
	return true, nil
}
