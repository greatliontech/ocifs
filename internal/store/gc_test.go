//go:build linux

package store

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"
	"github.com/greatliontech/ocifs/internal/layer"
	"pgregory.net/rapid"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// pullTwo pushes and pulls two distinct images, returning their
// refs and the store.
func pullTwo(t *testing.T) (*Store, string, string, string) {
	t.Helper()
	reg := newTestRegistry()
	ref1 := testHost + "/gc/keep:v1"
	ref2 := testHost + "/gc/drop:v1"
	push(t, reg, ref1, makeImage(t, newRawLayer(t, tarBytes(t, tfile("keep", "keep-bytes")))))
	push(t, reg, ref2, makeImage(t, newRawLayer(t, tarBytes(t, tfile("drop", "drop-bytes")))))
	s, dir := newTestStore(t, PullIfNotPresent, reg)
	if _, err := s.Image(context.Background(), ref1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Image(context.Background(), ref2, nil); err != nil {
		t.Fatal(err)
	}
	return s, dir, ref1, ref2
}

// TestCollectReclaimsUnreachable pins REQ-store-gc-roots/collect:
// severing a ref makes its content collectable; rooted content and
// shared reachability survive, and the survivor still serves.
func TestCollectReclaimsUnreachable(t *testing.T) {
	s, dir, ref1, ref2 := pullTwo(t)
	if err := s.RemoveRef(context.Background(), ref2); err != nil {
		t.Fatal(err)
	}
	res, err := s.Collect(context.Background(), CollectOpts{Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatal("nothing collected after severing the ref")
	}
	// The dropped image's content-CAS entry is gone.
	dropDigest := digestOfBytes("drop-bytes")
	if _, err := os.Stat(s.BlobPath(dropDigest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("severed image's CAS blob survived: %v", err)
	}
	// The kept image survives whole and still serves offline.
	keepDigest := digestOfBytes("keep-bytes")
	if _, err := os.Stat(s.BlobPath(keepDigest)); err != nil {
		t.Fatalf("rooted image's CAS blob collected: %v", err)
	}
	never := newStoreAt(t, dir, PullNever, v1.Platform{}, cutTransport(t))
	img, err := never.Image(context.Background(), ref1, nil)
	if err != nil {
		t.Fatalf("rooted image no longer serves offline: %v", err)
	}
	if got := string(readEntry(t, never, img, "keep")); got != "keep-bytes" {
		t.Fatalf("kept content = %q", got)
	}
}

// TestGraceRetainsYoungGarbage pins the retention grace: young
// unreachable content is retained (first-seen recorded), and the
// same pass with the grace ignored collects it.
func TestGraceRetainsYoungGarbage(t *testing.T) {
	s, _, _, ref2 := pullTwo(t)
	if err := s.RemoveRef(context.Background(), ref2); err != nil {
		t.Fatal(err)
	}
	res, err := s.Collect(context.Background(), CollectOpts{Grace: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CollectedBlobs) != 0 || len(res.CollectedPaths) != 0 {
		t.Fatalf("grace-protected garbage collected: %+v", res)
	}
	dropDigest := digestOfBytes("drop-bytes")
	if _, err := os.Stat(s.BlobPath(dropDigest)); err != nil {
		t.Fatalf("blob gone under grace: %v", err)
	}
	res2, err := s.Collect(context.Background(), CollectOpts{Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.CollectedBlobs) == 0 {
		t.Fatal("grace-ignored pass collected nothing")
	}
}

// TestSharedBlobSurvivesPartialRemoval pins reachability sharing:
// two images sharing content, one severed — the shared blob stays.
func TestSharedBlobSurvivesPartialRemoval(t *testing.T) {
	reg := newTestRegistry()
	shared := tfile("shared", "common-bytes")
	refA := testHost + "/gc/a:v1"
	refB := testHost + "/gc/b:v1"
	push(t, reg, refA, makeImage(t, newRawLayer(t, tarBytes(t, shared, tfile("only-a", "a")))))
	push(t, reg, refB, makeImage(t, newRawLayer(t, tarBytes(t, shared, tfile("only-b", "b")))))
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	if _, err := s.Image(context.Background(), refA, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Image(context.Background(), refB, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(context.Background(), refA); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.BlobPath(digestOfBytes("common-bytes"))); err != nil {
		t.Fatalf("shared blob collected while still reachable: %v", err)
	}
	if _, err := os.Stat(s.BlobPath(digestOfBytes("a"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("severed-only blob survived: %v", err)
	}
}

// TestCondemnedConsultRefusesUnleasedPublish pins
// REQ-store-gc-safe's fence for unleased root publishes: a digest
// condemned by a LIVE sweeper refuses registration, ref rows,
// bindings, and op pins; a dead or finished sweeper binds nobody.
func TestCondemnedConsultRefusesUnleasedPublish(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ee", 32)}

	sweepOp, err := s.BeginOp(context.Background(), "sweep", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.condemn(context.Background(), sweepOp, []gcItem{{tier: "oci", digest: h}}); err != nil {
		t.Fatal(err)
	}

	upClaim, err := s.ClaimUpper("u-cond")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterMountRecordArbitrated(context.Background(), "m1", h, "u-cond", "/x", upClaim); !errors.Is(err, ErrCondemned) {
		t.Fatalf("registration over a condemned digest: %v", err)
	}
	// The refused registration released BOTH claims (a leaked upper
	// would refuse every later writable mount of the name forever).
	if reup, err := s.ClaimUpper("u-cond"); err != nil {
		t.Fatalf("upper claim leaked by refused registration: %v", err)
	} else {
		reup.Close()
	}
	if ml, err := oslock.TryAcquire(s.mountLockPath("m1")); err != nil {
		t.Fatalf("mount claim leaked by refused registration: %v", err)
	} else {
		ml.Close()
	}
	if err := s.bk.RefPut(context.Background(), mustRef(t, "r.io/xx:yy"), h); !errors.Is(err, ErrCondemned) {
		t.Fatalf("ref row over a condemned digest: %v", err)
	}
	if _, err := s.bk.UpperBind(context.Background(), "u1", h); !errors.Is(err, ErrCondemned) {
		t.Fatalf("binding over a condemned digest: %v", err)
	}
	if _, err := s.BeginOp(context.Background(), opKindExport, []v1.Hash{h}, nil); !errors.Is(err, ErrCondemned) {
		t.Fatalf("op pin over a condemned digest: %v", err)
	}

	// The sweeper finishing (op row gone) unbinds the condemned row.
	if err := s.EndOp(context.Background(), sweepOp); err != nil {
		t.Fatal(err)
	}
	if claim, err := s.RegisterMountRecordArbitrated(context.Background(), "m1", h, "", "/x", nil); err != nil {
		t.Fatalf("finished sweeper still binds: %v", err)
	} else {
		claim.release()
	}
}

// TestRemovalRefusedWhileServed pins REQ-api-remove's refusals.
func TestRemovalRefusedWhileServed(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("dd", 32)}
	upClaim, err := s.ClaimUpper("up1")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.RegisterMountRecordArbitrated(context.Background(), "served", h, "up1", "/m", upClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveImage(context.Background(), h); err == nil {
		t.Fatal("served image removed")
	}
	if err := s.RemoveUpper(context.Background(), "up1"); err == nil {
		t.Fatal("served upper removed")
	}
	if err := s.DeregisterMount(context.Background(), "served", claim); err != nil {
		t.Fatal(err)
	}
	// Deregistration retired both claim files (row removed, unlink
	// while held, then released — REQ-store-mount-registry).
	if _, err := os.Stat(s.mountLockPath("served")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mount claim file survived deregistration: %v", err)
	}
	if _, err := os.Stat(s.upperLockPath("up1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upper claim file survived deregistration: %v", err)
	}
	if err := s.RemoveImage(context.Background(), h); err != nil {
		t.Fatalf("unserved image removal: %v", err)
	}
	if err := s.RemoveUpper(context.Background(), "up1"); err != nil {
		t.Fatalf("unserved upper removal: %v", err)
	}
	if _, err := s.bk.UpperBinding(context.Background(), "up1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upper binding survived removal: %v", err)
	}
}

// TestAutoCollectOnRemoval pins the automatic transition
// (REQ-store-gc-collect): with automatic collection on and zero
// grace, severing the ref alone reclaims the content — no explicit
// pass.
func TestAutoCollectOnRemoval(t *testing.T) {
	reg := newTestRegistry()
	ref := testHost + "/gc/auto:v1"
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "auto-bytes")))))
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.transport = reg
	if _, err := s.Image(context.Background(), ref, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.BlobPath(digestOfBytes("auto-bytes"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("automatic collection did not run on removal: %v", err)
	}
}

// TestDebrisSweepAtInit pins the initialization transition: dead
// ops rows' temporaries (wherever they live), dead mounts, and
// unowned export temporaries go at open.
func TestDebrisSweepAtInit(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	callerTemp := filepath.Join(scratchDir(t), ".export-orphan")
	if err := os.MkdirAll(callerTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeOpRow(t, dir, "export-dead", OpRecord{
		Kind: opKindExport, Owner: deadIdentity(), Temps: []string{callerTemp},
	}); err != nil {
		t.Fatal(err)
	}
	storeTemp := filepath.Join(dir, "exports", "sha256", ".export-unowned")
	if err := os.MkdirAll(storeTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	// A stranded residue-free lock file: the init debris sweep's own
	// lock-tier pass must dispose of it.
	if l, err := oslock.TryAcquire(s.mountLockPath("init-stranded")); err != nil {
		t.Fatal(err)
	} else {
		l.Close()
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, true, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	if _, err := os.Stat(callerTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead op's caller-side temporary survived init: %v", err)
	}
	if _, err := os.Stat(storeTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned export temporary survived init: %v", err)
	}
	if n := len(opsRows(t, dir)); n != 0 {
		t.Fatalf("%d dead ops rows survived init", n)
	}
	if _, err := os.Stat(s2.mountLockPath("init-stranded")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stranded lock file survived the init sweep: %v", err)
	}
}

// TestOwnedTempExemptFromCollection pins ops-row temp ownership:
// a live op's temporary survives even a grace-ignored pass.
func TestOwnedTempExemptFromCollection(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	tmp := filepath.Join(dir, "exports", "sha256", ".export-live")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	id, err := s.BeginOp(context.Background(), opKindExport, nil, []string{tmp})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("live op's temporary collected: %v", err)
	}
	if err := s.EndOp(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned temporary survived: %v", err)
	}
}

func digestOfBytes(content string) v1.Hash {
	sum := sha256.Sum256([]byte(content))
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

func mustRef(t *testing.T, s string) name.Reference {
	t.Helper()
	r, err := name.ParseReference(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestRootArms pins each non-ref root of REQ-store-gc-roots: a live
// mount's image, an upper binding's base, a live op's pins, and a
// localimages row each keep content collectable-not-collected.
func TestRootArms(t *testing.T) {
	for _, arm := range []string{"mount", "upper", "oppin", "localimage"} {
		t.Run(arm, func(t *testing.T) {
			reg := newTestRegistry()
			ref := testHost + "/gc/rootarm:v1"
			push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "rooted-"+arm)))))
			s, _ := newTestStore(t, PullIfNotPresent, reg)
			img, err := s.Image(context.Background(), ref, nil)
			if err != nil {
				t.Fatal(err)
			}
			top := img.Hash()
			switch arm {
			case "mount":
				claim, err := s.RegisterMountRecordArbitrated(context.Background(), "root-m", top, "", "/m", nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(claim.release)
			case "upper":
				if _, err := s.bk.UpperBind(context.Background(), "root-u", top); err != nil {
					t.Fatal(err)
				}
			case "oppin":
				if _, err := s.BeginOp(context.Background(), opKindExport, []v1.Hash{top}, nil); err != nil {
					t.Fatal(err)
				}
			case "localimage":
				if err := s.bk.LocalImagePut(context.Background(), top, time.Now().Unix()); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.RemoveRef(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(s.BlobPath(digestOfBytes("rooted-" + arm))); err != nil {
				t.Fatalf("%s-rooted content collected: %v", arm, err)
			}
		})
	}
}

// TestCondemnReVerifiesRoots pins REQ-store-gc-safe's re-check: an
// item rooted between the mark and the condemn transaction is
// dropped from the condemned set untouched.
func TestCondemnReVerifiesRoots(t *testing.T) {
	reg := newTestRegistry()
	ref := testHost + "/gc/reverify:v1"
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "re-rooted")))))
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The item was marked unreachable (say, by an earlier pass);
	// it is rooted NOW — condemn must drop it.
	op, err := s.BeginOp(context.Background(), "sweep", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.EndOp(context.Background(), op)
	out, err := s.condemn(context.Background(), op, []gcItem{{tier: "oci", digest: img.Hash()}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("condemn kept a rooted item: %+v", out)
	}
}

// TestAutoCollectDisabled pins the construction knob: with
// automatic collection off, severing a ref reclaims nothing until
// an explicit pass.
func TestAutoCollectDisabled(t *testing.T) {
	reg := newTestRegistry()
	ref := testHost + "/gc/manual:v1"
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "manual-bytes")))))
	s, _ := newTestStore(t, PullIfNotPresent, reg) // autoGC=false in fixtures
	if _, err := s.Image(context.Background(), ref, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.BlobPath(digestOfBytes("manual-bytes"))); err != nil {
		t.Fatalf("disabled auto-collect still collected: %v", err)
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.BlobPath(digestOfBytes("manual-bytes"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("explicit pass did not collect")
	}
}

// TestForeignMountRowHaltsImageCollection pins the one
// irreducible foreign-version conservatism
// (REQ-store-bookkeeping): a LIVE foreign-version mounts row — its
// claim lock held, its image unreadable — halts image-tier
// collection visibly and refuses image removal; dead, the same row
// is ordinary debris: reclaimed, halting nothing.
func TestForeignMountRowHaltsImageCollection(t *testing.T) {
	reg := newTestRegistry()
	ref := testHost + "/gc/foreignrow:v1"
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "foreign-guard")))))
	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreign := append([]byte{mountRecVersion + 1}, encodeMountRecord(MountRecord{Owner: deadIdentity()})[1:]...)
	if err := writeRawMountRow(t, dir, "future", foreign); err != nil {
		t.Fatal(err)
	}
	// The future-version mount is ALIVE: its claim lock is held.
	holder, err := oslock.TryAcquire(s.mountLockPath("future"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	res, err := s.Collect(context.Background(), CollectOpts{Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ForeignVersionRows) == 0 {
		t.Fatal("live foreign row not reported")
	}
	if len(res.CollectedBlobs) != 0 {
		t.Fatalf("image-tier collection proceeded under a live foreign row: %+v", res.CollectedBlobs)
	}
	if err := s.RemoveImage(context.Background(), img.Hash()); err == nil {
		t.Fatal("removal proceeded under a live unreadable mount row")
	}

	// Dead — the lock released — the same row halts nothing: image
	// removal proceeds past it (the narrowing to LIVE foreign rows
	// only), and the sweep reclaims it while collection proceeds.
	holder.Close()
	if err := s.RemoveImage(context.Background(), img.Hash()); err != nil {
		t.Fatalf("dead foreign row refused image removal: %v", err)
	}
	res, err = s.Collect(context.Background(), CollectOpts{Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ForeignVersionRows) != 0 {
		t.Fatalf("dead foreign row still reported live: %v", res.ForeignVersionRows)
	}
	found := false
	for _, id := range res.ReclaimedMounts {
		found = found || id == "future"
	}
	if !found {
		t.Fatalf("dead foreign row not reclaimed: %v", res.ReclaimedMounts)
	}
	if len(res.CollectedBlobs) == 0 {
		t.Fatal("collection did not proceed after the foreign mount died")
	}
}

func writeRawMountRow(t testing.TB, storeDir, id string, raw []byte) error {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		return ks.Put([]byte(id), raw)
	})
}

// TestMediaTypelessIndexWalks pins the reachability walk's index
// detection: OCI 1.0 allows an index document without a mediaType
// body field, and its children must still mark — misreading it as a
// manifest would collect a live root's children.
func TestMediaTypelessIndexWalks(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	child := []byte(`{"schemaVersion":2,"config":{"digest":"sha256:` + strings.Repeat("11", 32) + `","size":2},"layers":[]}`)
	childDigest := digestOfBytes(string(child))
	idx := []byte(`{"schemaVersion":2,"manifests":[{"digest":"` + childDigest.String() + `","size":` + fmt.Sprint(len(child)) + `}]}`)
	idxDigest := digestOfBytes(string(idx))
	for _, b := range []struct {
		h v1.Hash
		d []byte
	}{{childDigest, child}, {idxDigest, idx}} {
		p := s.ociBlobPath(b.h)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b.d, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reachable, err := s.reachableFrom(context.Background(), []v1.Hash{idxDigest})
	if err != nil {
		t.Fatal(err)
	}
	if !reachable["oci\x00"+childDigest.Algorithm+"\x00"+childDigest.Hex] {
		t.Fatal("mediaType-less index's child not marked reachable")
	}
}

// TestCASRootTempCollected pins the tier-root temporary sweep: a
// crashed cas.Put leaves its temp at blobs/ itself, not under the
// algorithm directory.
func TestCASRootTempCollected(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	rootTemp := filepath.Join(dir, "blobs", ".tmp-crashed")
	if err := os.WriteFile(rootTemp, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rootTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CAS-root temporary survived: %v", err)
	}
}

// TestReclaimDeferralKeepsRowAndFile pins the deferral branch
// (REQ-store-mount-registry): a dead mount whose state-directory
// removal fails keeps BOTH its row and its lock file for a later
// sweep — released without unlink, never a half-reclaimed id — and
// the retry reclaims once removal works again. The failure is
// injected at the removal seam; nothing on a healthy filesystem
// reaches this branch.
func TestReclaimDeferralKeepsRowAndFile(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("d4", 32)}
	if _, _, err := s.NewMountState("deferred"); err != nil {
		t.Fatal(err)
	}
	if err := s.bk.MountPut(context.Background(), "deferred", MountRecord{Owner: deadIdentity(), Image: h}); err != nil {
		t.Fatal(err)
	}
	orig := removeMountState
	removeMountState = func(string) error { return errors.New("injected removal failure") }
	defer func() { removeMountState = orig }()

	reclaimed, err := s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range reclaimed {
		if id == "deferred" {
			t.Fatal("deferral reported as reclaimed")
		}
	}
	if _, err := s.MountRecord(context.Background(), "deferred"); err != nil {
		t.Fatalf("deferral lost the row: %v", err)
	}
	if _, err := os.Stat(s.mountLockPath("deferred")); err != nil {
		t.Fatalf("deferral lost the lock file: %v", err)
	}

	// Removal healed: the retry reclaims row, state, and lock file.
	removeMountState = orig
	if _, err := s.ReclaimDeadMounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MountRecord(context.Background(), "deferred"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry left the row: %v", err)
	}
	if _, err := os.Stat(s.mountLockPath("deferred")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry left the lock file: %v", err)
	}
}

// TestMidRegistrationStateSurvivesSweep pins the lock-before-row
// window (REQ-store-mount-registry): a registrant holds its claim
// before its row exists, and its freshly created state directory —
// rowless but claimed — must survive even a grace-ignoring sweep.
func TestMidRegistrationStateSurvivesSweep(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	// The mid-registration state: claim held, directory minted, row
	// not yet written.
	held, err := oslock.TryAcquire(s.mountLockPath("midreg"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	stateDir := filepath.Join(dir, "mounts", "midreg", "mnt")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A stranded residue-free lock file rides along: Collect's own
	// lock-tier sweep — not just a direct sweepLockTier call — must
	// dispose of it (the delivery path of the disposer).
	if l, err := oslock.TryAcquire(s.mountLockPath("stranded")); err != nil {
		t.Fatal(err)
	} else {
		l.Close()
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stateDir); err != nil {
		t.Fatalf("claimed mid-registration state collected: %v", err)
	}
	if _, err := os.Stat(s.mountLockPath("stranded")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stranded lock file survived Collect: %v", err)
	}
}

// TestMountIDRejectsControlBytes pins the id hygiene rule: API ids
// never carry control bytes — they name registry rows, state
// directories, and claim lock files (api.md REQ-api-mount-id).
func TestMountIDRejectsControlBytes(t *testing.T) {
	for _, bad := range []string{"\x00x", "a\x00b", "a\tb", "a\nb"} {
		if validMountID(bad) {
			t.Fatalf("control-byte id %q accepted", bad)
		}
	}
	if !validMountID("ordinary-id.v1") {
		t.Fatal("ordinary id rejected")
	}
}

// TestDeadSweeperCondemnedRowBindsNobody pins REQ-store-gc-safe's
// crash clause deterministically: a condemned row whose sweeper
// died mid-sweep refuses nothing, and the next pass drops it as
// debris.
func TestDeadSweeperCondemnedRowBindsNobody(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("99", 32)}

	// The dead sweeper's lease, op row, and condemned row — exactly
	// as a kill between condemn and delete leaves them (Collect
	// acquires the lease before its sweep op).
	if err := writeOpRow(t, dir, ingestLeaseKey, OpRecord{Kind: opKindIngest, Owner: deadIdentity(), Nonce: "corpse"}); err != nil {
		t.Fatal(err)
	}
	if err := writeOpRow(t, dir, "sweep-corpse", OpRecord{Kind: "sweep", Owner: deadIdentity()}); err != nil {
		t.Fatal(err)
	}
	if err := writeGCRow(t, dir, gcCondemnedPrefix+"oci\x00"+h.Algorithm+"\x00"+h.Hex, []byte("sweep-corpse")); err != nil {
		t.Fatal(err)
	}

	// Publication proceeds: the dead sweeper binds nobody.
	if err := s.bk.RefPut(context.Background(), mustRef(t, "r.io/xx:zz"), h); err != nil {
		t.Fatalf("dead sweeper's condemned row refused publication: %v", err)
	}
	// The next pass drops the stale row as debris.
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if gcRowExists(t, dir, gcCondemnedPrefix+"oci\x00"+h.Algorithm+"\x00"+h.Hex) {
		t.Fatal("dead sweeper's condemned row survived the next pass")
	}
}

func writeGCRow(t testing.TB, storeDir, key string, val []byte) error {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		return ks.Put([]byte(key), val)
	})
}

func gcRowExists(t testing.TB, storeDir, key string) bool {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exists := false
	_ = db.View(context.Background(), func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksGC)
		if err != nil {
			return err
		}
		if _, err := ks.Get([]byte(key)); err == nil {
			exists = true
		}
		return nil
	})
	return exists
}

// TestPropertyCollectNeverEatsRooted is REQ-store-gc-safe's for-all
// witness: over random sequences of pull, remove, and collect (any
// grace, ignore included), every currently-rooted reference always
// serves offline — collection never takes what a root reaches,
// whatever the interleaving of transitions.
func TestPropertyCollectNeverEatsRooted(t *testing.T) {
	reg := newTestRegistry()
	tags := make([]string, 4)
	contents := make([]string, 4)
	for i := range tags {
		tags[i] = fmt.Sprintf("%s/gcprop/img%d:v1", testHost, i)
		contents[i] = fmt.Sprintf("gcprop-payload-%d", i)
		push(t, reg, tags[i], makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", contents[i])))))
	}
	rapid.Check(t, func(rt *rapid.T) {
		dir := scratchDir(t)
		s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, false, 0)
		if err != nil {
			rt.Fatal(err)
		}
		defer s.Close()
		s.transport = reg
		reader := newStoreAt(t, dir, PullNever, v1.Platform{}, cutTransport(t))

		rooted := map[int]bool{}
		nOps := rapid.IntRange(3, 10).Draw(rt, "ops")
		for op := 0; op < nOps; op++ {
			i := rapid.IntRange(0, 3).Draw(rt, "tag")
			switch rapid.IntRange(0, 2).Draw(rt, "kind") {
			case 0:
				if _, err := s.Image(context.Background(), tags[i], nil); err != nil {
					rt.Fatalf("pull %d: %v", i, err)
				}
				rooted[i] = true
			case 1:
				if err := s.RemoveRef(context.Background(), tags[i]); err != nil {
					rt.Fatalf("remove %d: %v", i, err)
				}
				delete(rooted, i)
			case 2:
				grace := rapid.SampledFrom([]time.Duration{-1, 0, time.Hour}).Draw(rt, "grace")
				if _, err := s.Collect(context.Background(), CollectOpts{Grace: grace}); err != nil {
					rt.Fatalf("collect: %v", err)
				}
			}
			// The invariant, after every operation: rooted content
			// serves offline — no heal-by-refetch can mask a wrong
			// deletion.
			for j := range rooted {
				img, err := reader.Image(context.Background(), tags[j], nil)
				if err != nil {
					rt.Fatalf("op %d: rooted tag %d lost: %v", op, j, err)
				}
				if got := string(readEntry(t, reader, img, "f")); got != contents[j] {
					rt.Fatalf("op %d: rooted tag %d content %q", op, j, got)
				}
			}
		}
	})
}

// TestPropertyTierKeyspacesDisjoint is REQ-store-ns's for-all
// witness under the bookkeeping layout: whatever the digest, a
// layer-index row and a content-CAS entry under the SAME key
// coexist byte-intact — the structural split holds over the whole
// keyspace, not just one collision fixture.
func TestPropertyTierKeyspacesDisjoint(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	hexRunes := rapid.RuneFrom([]rune("0123456789abcdef"))
	rapid.Check(t, func(rt *rapid.T) {
		h := v1.Hash{Algorithm: "sha256", Hex: rapid.StringOfN(hexRunes, 64, 64, -1).Draw(rt, "hex")}
		idxName := rapid.StringOfN(rapid.RuneFrom([]rune("abc\x00\xff/")), 1, 12, -1).Draw(rt, "name")
		blobBytes := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "blob")

		idx := layer.Layer{{Header: tar.Header{Name: idxName, Typeflag: tar.TypeReg}}}
		if err := s.bk.LayerIdxPut(context.Background(), h, idx); err != nil {
			rt.Fatal(err)
		}
		blobPath := s.cas.Path(h)
		if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
			rt.Fatal(err)
		}
		if err := os.WriteFile(blobPath, blobBytes, 0o644); err != nil {
			rt.Fatal(err)
		}

		got, err := s.bk.LayerIdxGet(context.Background(), h)
		if err != nil || len(got) != 1 || got[0].Header.Name != idxName {
			rt.Fatalf("index under colliding key: %v %v", got, err)
		}
		b, err := os.ReadFile(blobPath)
		if err != nil || !bytes.Equal(b, blobBytes) {
			rt.Fatalf("blob under colliding key: %v", err)
		}
	})
}
