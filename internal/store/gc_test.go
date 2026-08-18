//go:build linux

package store

import (
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

	if err := s.RegisterMountRecordArbitrated(context.Background(), "m1", h, "", "/x"); !errors.Is(err, ErrCondemned) {
		t.Fatalf("registration over a condemned digest: %v", err)
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
	if err := s.RegisterMountRecordArbitrated(context.Background(), "m1", h, "", "/x"); err != nil {
		t.Fatalf("finished sweeper still binds: %v", err)
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
	if err := s.RegisterMountRecordArbitrated(context.Background(), "served", h, "up1", "/m"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveImage(context.Background(), h); err == nil {
		t.Fatal("served image removed")
	}
	if err := s.RemoveUpper(context.Background(), "up1"); err == nil {
		t.Fatal("served upper removed")
	}
	if err := s.DeregisterMount(context.Background(), "served"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveImage(context.Background(), h); err != nil {
		t.Fatalf("unserved image removal: %v", err)
	}
	if err := s.RemoveUpper(context.Background(), "up1"); err != nil {
		t.Fatalf("unserved upper removal: %v", err)
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
				if err := s.RegisterMountRecordArbitrated(context.Background(), "root-m", top, "", "/m"); err != nil {
					t.Fatal(err)
				}
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

// TestForeignMountRowHaltsImageCollection pins the M-class rule: an
// unreadable mounts row stops image-tier collection visibly and
// refuses removals.
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
	if err := s.RemoveRef(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	res, err := s.Collect(context.Background(), CollectOpts{Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ForeignVersionRows) == 0 {
		t.Fatal("foreign row not reported")
	}
	if len(res.CollectedBlobs) != 0 {
		t.Fatalf("image-tier collection proceeded under a foreign row: %+v", res.CollectedBlobs)
	}
	if err := s.RemoveImage(context.Background(), img.Hash()); err == nil {
		t.Fatal("removal proceeded under an unreadable mount row")
	}
	if err := s.RemoveUpper(context.Background(), "any"); err == nil {
		t.Fatal("upper removal proceeded under an unreadable mount row")
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

// TestMountIDRejectsControlBytes pins the guard-namespace
// reservation: API ids never carry control bytes, so internal
// registry ids are unrepresentable (api.md REQ-api-mount-id).
func TestMountIDRejectsControlBytes(t *testing.T) {
	for _, bad := range []string{"\x00upper-removal\x00x", "a\x00b", "a\tb", "a\nb"} {
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
