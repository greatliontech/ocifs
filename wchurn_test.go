//go:build linux

package ocifs

import (
	"errors"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"os"
	"path/filepath"
	"testing"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestChurnSoak runs the full lifecycle loop with automatic
// collection on and zero grace — pull, writable mount, write,
// unmount, commit, remove, collect — and requires every cycle's
// image to serve whole while the store never accretes: the plan's
// churn deliverable (docs/plans/store-gc.md chunk 7).
func TestChurnSoak(t *testing.T) {
	skipUnderMutationCampaign(t)
	ofs, refStr := writableFixtureEnv(t, "wchurn", WithAutoGC(true), WithGCGrace(0))
	scratch := filepath.Join(".scratch", "ocifs-wchurn")

	var lastCommitted v1.Hash
	for cycle := 0; cycle < 5; cycle++ {
		upDir := scratchtest.In(t, filepath.Join(scratch, fmt.Sprintf("up%d", cycle)))
		im, err := ofs.Mount(refStr, MountWithUpperDir(upDir))
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
		committed, err := ofs.Commit(t.Context(), refStr, CommitWithUpperDir(upDir))
		if err != nil {
			t.Fatalf("cycle %d commit: %v", cycle, err)
		}
		// The fresh commit mounts and serves its delta.
		cim, err := ofs.Mount(LocalRef(committed.Digest()))
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
		// The previous cycle's commit is removed; zero grace plus
		// the removal transition reclaims it while THIS cycle's
		// commit and the base stay rooted and serving.
		if lastCommitted != (v1.Hash{}) {
			if err := ofs.RemoveImage(t.Context(), lastCommitted.String()); err != nil {
				t.Fatalf("cycle %d remove prior: %v", cycle, err)
			}
			if _, err := ofs.Mount(LocalRef(lastCommitted)); err == nil {
				t.Fatalf("cycle %d: removed prior commit still mounts", cycle)
			}
		}
		lastCommitted = committed.Digest()
	}

	// Teardown: remove the last commit and the base ref; a
	// grace-ignored pass leaves the content tiers empty.
	if err := ofs.RemoveImage(t.Context(), lastCommitted.String()); err != nil {
		t.Fatal(err)
	}
	if err := ofs.RemoveRef(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	res, err := ofs.GC(t.Context(), GCIgnoreGrace())
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

// tierEmpty reports whether a digest tier holds no blobs.
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
