//go:build linux

package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/upper"
)

var commitTime = time.Date(2024, 7, 1, 8, 0, 0, 0, time.UTC)

// commitFixture pulls a base and prepares an upper with one change,
// one deletion, and one addition.
func commitFixture(t *testing.T) (*Store, string, *Image, string) {
	t.Helper()
	reg := newTestRegistry()
	refStr := testHost + "/commit/base:v1"
	l := newRawLayer(t, tarBytes(t,
		tdir("app"),
		tfile("app/keep", "kept"),
		tfile("app/gone", "bye"),
		tfile("app/change", "old"),
	))
	push(t, reg, refStr, makeImage(t, l))
	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}

	upperDir, err := s.NewUpper("work", img.Hash())
	if err != nil {
		t.Fatal(err)
	}
	w := upper.NewWriter(upperDir)
	if err := w.Mkdir("app", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("app/change", strings.NewReader("new"), 0o644, commitTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("app/gone"); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("added", strings.NewReader("fresh"), 0o600, commitTime, nil); err != nil {
		t.Fatal(err)
	}
	return s, dir, img, upperDir
}

// TestCommitUpperMaterializesLocally pins REQ-writable-commit-image
// and REQ-store-local-images: the committed digest acquires under
// the local namespace with the network gone, unifies to the merged
// truth, and never dials.
func TestCommitUpperMaterializesLocally(t *testing.T) {
	s, dir, img, upperDir := commitFixture(t)
	digest, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}

	// A fresh store instance over the same root, network cut: the
	// committed image is fully local.
	offline := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, cutTransport(t))
	got, err := offline.Image(context.Background(), LocalRef(digest), nil)
	if err != nil {
		t.Fatalf("committed image did not materialize offline: %v", err)
	}
	view, err := got.Unify()
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := view.Lookup("app/change"); !ok || string(readCAS(t, offline, e.Digest)) != "new" {
		t.Fatalf("changed entry wrong: %v", ok)
	}
	if _, ok := view.Lookup("app/gone"); ok {
		t.Fatal("deleted entry survived commit")
	}
	if e, ok := view.Lookup("app/keep"); !ok || string(readCAS(t, offline, e.Digest)) != "kept" {
		t.Fatal("base entry lost")
	}
	if e, ok := view.Lookup("added"); !ok || string(readCAS(t, offline, e.Digest)) != "fresh" {
		t.Fatal("added entry lost")
	}
	if cf := got.ConfigFile(); len(cf.RootFS.DiffIDs) != 2 || cf.History[len(cf.History)-1].CreatedBy != "ocifs commit" {
		t.Fatalf("config not extended: %+v", cf.RootFS)
	}
}

func readCAS(t *testing.T, s *Store, h interface{ String() string }) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.path, "blobs", strings.ReplaceAll(h.String(), ":", "/")))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCommitDeterministic: committing the same (base, upper) twice
// yields the same digest — layer, config, manifest all.
func TestCommitDeterministic(t *testing.T) {
	s, _, img, upperDir := commitFixture(t)
	d1, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatalf("repeat commit digest %s != %s", d2, d1)
	}
}

// TestBaseBinding pins REQ-writable-base-binding: first use records,
// re-use validates, a different base refuses for mount and commit
// alike, and the record lives beside the dialect tree, not inside.
func TestBaseBinding(t *testing.T) {
	s, _, img, _ := commitFixture(t)

	if _, err := s.NewUpper("work", img.Hash()); err != nil {
		t.Fatalf("re-open with the bound base failed: %v", err)
	}
	other := img.Hash()
	other.Hex = strings.Repeat("0", 64)
	if _, err := s.NewUpper("work", other); err == nil || !strings.Contains(err.Error(), "bound to base") {
		t.Fatalf("mismatched base accepted: %v", err)
	}
	if recorded, err := s.bk.UpperBinding(context.Background(), "work"); err != nil || recorded != img.Hash() {
		t.Fatalf("binding row: %s, %v", recorded, err)
	}
	// Nothing beside or inside the dialect tree carries the binding
	// (REQ-writable-base-binding: bookkeeping only).
	for _, p := range []string{
		filepath.Join(s.path, "uppers", "work", "base"),
		filepath.Join(s.path, "uppers", "work", "upper", "base"),
	} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("binding file present at %s; the record lives in bookkeeping", p)
		}
	}

	if _, err := s.CommitNamedUpper(context.Background(), img, "work"); err != nil {
		t.Fatalf("bound commit failed: %v", err)
	}
	fake := &Image{h: other, conf: img.ConfigFile()}
	if _, err := s.CommitNamedUpper(context.Background(), fake, "work"); err == nil || !strings.Contains(err.Error(), "bound to base") {
		t.Fatalf("commit over foreign base accepted: %v", err)
	}
	if _, err := s.CommitNamedUpper(context.Background(), img, "unbound"); err == nil {
		t.Fatal("commit of unbound upper succeeded")
	}
	if _, err := s.NewUpper("bad/name", img.Hash()); err == nil {
		t.Fatal("upper name with separator accepted")
	}
}

// TestLocalNamespaceNeverDials pins REQ-store-local-images: tags
// under ocifs.local resolve to nothing under every policy, and a
// missing piece fails offline-style — the cut transport proves no
// dial happens.
func TestLocalNamespaceNeverDials(t *testing.T) {
	s, dir, img, upperDir := commitFixture(t)
	digest, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, policy := range []PullPolicy{PullIfNotPresent, PullAlways, PullNever} {
		cut := newStoreAt(t, dir, policy, linuxAMD64, cutTransport(t))
		if _, err := cut.Image(context.Background(), LocalRegistry+"/commits:latest", nil); err == nil {
			t.Fatalf("local tag resolved under %v", policy)
		}
	}

	// Damage the committed layer blob: acquisition must fail as
	// store damage without dialing. The victim is targeted by
	// digest — the last layer the committed manifest names.
	blob := filepath.Join(dir, "oci", "blobs", "sha256")
	raw, err := os.ReadFile(filepath.Join(blob, digest.Hex))
	if err != nil {
		t.Fatal(err)
	}
	var man v1.Manifest
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatal(err)
	}
	committedLayer := man.Layers[len(man.Layers)-1].Digest
	deleteLayerIdxRow(t, dir, committedLayer)
	if err := os.Remove(filepath.Join(blob, committedLayer.Hex)); err != nil {
		t.Fatal(err)
	}
	cut := newStoreAt(t, dir, PullAlways, linuxAMD64, cutTransport(t))
	if _, err := cut.Image(context.Background(), LocalRef(digest), nil); err == nil {
		t.Fatal("damaged local image materialized")
	}
}

// TestCommitRecordsLocalImageRoot pins the localimages row
// (REQ-store-gc-roots): commit records the root that keeps the
// committed image reachable.
func TestCommitRecordsLocalImageRoot(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/commit/root:v1"
	l := newRawLayer(t, tarBytes(t, tfile("f", "base")))
	push(t, reg, refStr, makeImage(t, l))
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	up := filepath.Join(scratchDir(t), "up")
	if err := os.MkdirAll(up, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(up, "new"), []byte("delta"), 0o644); err != nil {
		t.Fatal(err)
	}
	digest, err := s.CommitUpper(context.Background(), img, up)
	if err != nil {
		t.Fatal(err)
	}
	present, err := s.bk.LocalImagePresent(context.Background(), digest)
	if err != nil || !present {
		t.Fatalf("localimages root after commit: present=%v err=%v", present, err)
	}
	absent, err := s.bk.LocalImagePresent(context.Background(), img.Hash())
	if err != nil || absent {
		t.Fatalf("uncommitted digest has a root row: %v %v", absent, err)
	}
}
