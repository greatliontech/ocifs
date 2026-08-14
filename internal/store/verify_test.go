package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"
)

var errRejected = errors.New("policy says no")

func rejectAll(ctx context.Context, id ResolvedIdentity) error { return errRejected }

// treeFiles snapshots one store tier as relative path -> size; the
// no-trace property compares snapshots around a rejected request.
func treeFiles(t testing.TB, root string) map[string]int64 {
	t.Helper()
	files := map[string]int64{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[rel] = info.Size()
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return files
}

// refsContent snapshots the reference cache by full content: a
// rejection must not record anything, and a rewritten ref entry is
// size-identical to the one it clobbers.
func refsContent(t testing.TB, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	root := filepath.Join(dir, "refs")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[rel] = string(data)
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return files
}

func sameContent(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func sameTree(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestPropertyVerifierRejectionLeavesNoTrace pins REQ-seam-position
// and REQ-seam-abort as a for-all over request shapes: whether the
// content was already cached or not, whether the reference is tag- or
// digest-form, and whether the artifact is an index or a direct
// manifest, a rejecting verifier aborts with a VerificationError and
// the request materializes nothing — no layer unpacked into the CAS
// or layer-index tier, no descriptor appended, no reference-cache
// entry recorded. The one write resolution itself performs — retaining
// the top-level artifact in oci/blobs, the seam's own input — is the
// only new object tolerated.
func TestPropertyVerifierRejectionLeavesNoTrace(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cached := rapid.Bool().Draw(rt, "cached")
		digestForm := rapid.Bool().Draw(rt, "digestForm")
		indexed := rapid.Bool().Draw(rt, "indexed")

		reg := newTestRegistry()
		refStr := testHost + "/seam/notrace:v1"
		var top v1.Hash
		if indexed {
			idx := makeIndex(t,
				platformImage{plat: linuxAMD64, img: imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("a", "amd"))))},
				platformImage{plat: linuxARM64, img: imageWithPlatform(t, linuxARM64, newRawLayer(t, tarBytes(t, tfile("a", "arm"))))},
			)
			pushIndex(t, reg, refStr, idx)
			top = mustDigest(t, idx)
		} else {
			img := imageWithPlatform(t, linuxAMD64,
				newRawLayer(t, tarBytes(t, tfile("a", "solo"))))
			push(t, reg, refStr, img)
			top = mustDigest(t, img)
		}
		reqRef := refStr
		if digestForm {
			reqRef = testHost + "/seam/notrace@" + top.String()
		}

		dir := scratchDir(t)
		linux := linuxAMD64
		if cached {
			// Populate through a verifier-less store over the same
			// root: the rejected request below then judges fully
			// cached content (REQ-seam-position's cached clause).
			pre := newStoreAt(t, dir, PullIfNotPresent, linux, reg)
			if _, err := pre.Image(context.Background(), reqRef, nil); err != nil {
				rt.Fatalf("pre-populate: %v", err)
			}
		}
		// Snapshot after construction: NewStore itself lays down the
		// oci-layout marker and empty index — initialization, not the
		// request under test.
		s := newStoreAt(t, dir, PullIfNotPresent, linux, reg)
		s.verifier = rejectAll
		before := map[string]map[string]int64{}
		for _, tier := range []string{"refs", "blobs", "layers", "oci"} {
			before[tier] = treeFiles(t, filepath.Join(dir, tier))
		}
		beforeRefs := refsContent(t, dir)
		_, err := s.Image(context.Background(), reqRef, nil)
		var verr *VerificationError
		if !errors.As(err, &verr) {
			rt.Fatalf("rejected request returned %v, want VerificationError", err)
		}
		if !errors.Is(err, errRejected) {
			rt.Fatalf("VerificationError does not wrap the verifier's error: %v", err)
		}

		for _, tier := range []string{"refs", "blobs", "layers"} {
			after := treeFiles(t, filepath.Join(dir, tier))
			if !sameTree(before[tier], after) {
				rt.Fatalf("rejected request mutated %s/: before %v, after %v", tier, before[tier], after)
			}
		}
		// Size equality is too coarse for the reference cache — an
		// overwritten digest is digest-sized — so refs/ is compared
		// by content.
		if afterRefs := refsContent(t, dir); !sameContent(beforeRefs, afterRefs) {
			rt.Fatalf("rejected request rewrote refs/: before %v, after %v", beforeRefs, afterRefs)
		}
		// oci/ may gain exactly the retained top-level artifact — the
		// seam's input — and nothing else; index.json never lists it.
		afterOCI := treeFiles(t, filepath.Join(dir, "oci"))
		topRel := filepath.Join("blobs", top.Algorithm, top.Hex)
		for rel, size := range afterOCI {
			if prev, ok := before["oci"][rel]; ok {
				if prev != size {
					rt.Fatalf("rejected request rewrote oci/%s", rel)
				}
				continue
			}
			if rel != topRel {
				rt.Fatalf("rejected request wrote oci/%s (only the top-level artifact %s is resolution's own retention)", rel, topRel)
			}
		}
		listed, lerr := s.descriptorListed(top)
		if lerr != nil {
			rt.Fatal(lerr)
		}
		if !cached && listed {
			rt.Fatalf("rejected request appended the top-level descriptor")
		}
	})
}

// TestVerifierReceivesResolvedIdentity pins REQ-seam-input: the hook
// receives the reference exactly as requested, the resolved top-level
// digest, and artifact bytes hashing to that digest — for a
// multi-platform image the index bytes, judged before platform
// selection ever picks a child.
func TestVerifierReceivesResolvedIdentity(t *testing.T) {
	reg := newTestRegistry()
	// Tagless on purpose: parsing normalizes it to :latest, so the
	// pin distinguishes the reference as requested from its
	// normalized form.
	refStr := testHost + "/seam/input"
	idx := makeIndex(t,
		platformImage{plat: linuxAMD64, img: imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("a", "amd"))))},
		platformImage{plat: linuxARM64, img: imageWithPlatform(t, linuxARM64, newRawLayer(t, tarBytes(t, tfile("a", "arm"))))},
	)
	pushIndex(t, reg, refStr, idx)
	indexDigest := mustDigest(t, idx)

	var got []ResolvedIdentity
	s := newStoreAt(t, scratchDir(t), PullIfNotPresent, linuxAMD64, reg)
	s.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		got = append(got, id)
		return nil
	}

	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("verifier called %d times for one request", len(got))
	}
	id := got[0]
	if id.Reference != refStr {
		t.Fatalf("verifier saw reference %q, want the request's own %q", id.Reference, refStr)
	}
	if id.Digest != indexDigest {
		t.Fatalf("verifier saw digest %s, want the index digest %s (signatures are made over the index)", id.Digest, indexDigest)
	}
	sum := sha256.Sum256(id.Artifact)
	if hex.EncodeToString(sum[:]) != indexDigest.Hex {
		t.Fatalf("artifact bytes do not hash to the resolved digest")
	}
	// Platform selection happened only after the seam: the served
	// child is a manifest the index names, not the index itself.
	if img.Hash() == indexDigest {
		t.Fatalf("served image is the index itself")
	}
}

// TestVerifierRunsPerRequestCachedIncluded pins REQ-seam-position's
// cached clause: the seam runs on every acquisition request — repeat
// requests over fully-materialized content included, offline
// included — and a store whose verifier rejects serves nothing even
// when everything is cached.
func TestVerifierRunsPerRequestCachedIncluded(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/seam/percall:v1"
	push(t, reg, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "content")))))

	dir := scratchDir(t)
	calls := 0
	s := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	s.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		calls++
		return nil
	}
	for i := 1; i <= 3; i++ {
		if _, err := s.Image(context.Background(), refStr, nil); err != nil {
			t.Fatal(err)
		}
		if calls != i {
			t.Fatalf("after %d requests the verifier ran %d times", i, calls)
		}
	}

	// Offline over the same root: cached content is not exempt, and
	// the retained top-level artifact feeds the seam with no network
	// (REQ-seam-position's availability clause).
	off := newStoreAt(t, dir, PullNever, linuxAMD64, offlineTransport())
	offCalls := 0
	off.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		offCalls++
		return nil
	}
	if _, err := off.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	if offCalls != 1 {
		t.Fatalf("offline cached request ran the verifier %d times", offCalls)
	}

	rej := newStoreAt(t, dir, PullNever, linuxAMD64, offlineTransport())
	rej.verifier = rejectAll
	var verr *VerificationError
	if _, err := rej.Image(context.Background(), refStr, nil); !errors.As(err, &verr) {
		t.Fatalf("fully-cached rejected request returned %v, want VerificationError", err)
	}
	if msg := verr.Error(); !strings.Contains(msg, refStr) || !strings.Contains(msg, verr.Digest.String()) {
		t.Fatalf("VerificationError message %q does not name the rejected identity", msg)
	}
}

// TestResolutionFailureIsNotVerificationError pins REQ-seam-abort's
// distinguishability clause from the other side: a request that fails
// to resolve — even with a verifier configured — never reports a
// VerificationError, and the verifier never runs.
func TestResolutionFailureIsNotVerificationError(t *testing.T) {
	s := newStoreAt(t, scratchDir(t), PullNever, linuxAMD64, offlineTransport())
	calls := 0
	s.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		calls++
		return nil
	}
	_, err := s.Image(context.Background(), testHost+"/seam/absent:v1", nil)
	if err == nil {
		t.Fatal("absent image resolved offline")
	}
	var verr *VerificationError
	if errors.As(err, &verr) {
		t.Fatalf("resolution failure reported as verification failure: %v", err)
	}
	if calls != 0 {
		t.Fatalf("verifier ran %d times for a request that never resolved", calls)
	}
}

// TestVerifierSelfHealsDamagedRetention pins REQ-seam-position's
// availability clause against a damaged store: with the retained
// top-level artifact deleted, a pull-permitted request re-fetches it
// by digest and the seam still runs on bytes hashing to the resolved
// digest, while a PullNever request fails naming the artifact — a
// resolution-side failure, not a VerificationError — without the
// verifier ever running on absent bytes. The second PullNever
// request pins the self-heal branch's lock hygiene: a held ingest
// lock would deadlock it.
func TestVerifierSelfHealsDamagedRetention(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/seam/heal:v1"
	img := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("f", "content"))))
	push(t, reg, refStr, img)
	top := mustDigest(t, img)

	dir := scratchDir(t)
	pre := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	if _, err := pre.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	topPath := filepath.Join(dir, "oci", "blobs", top.Algorithm, top.Hex)

	// PullNever with the retention damaged: the seam's input cannot
	// be self-healed, the request fails as resolution/materialization
	// failure, and the verifier never sees the request.
	if err := os.Remove(topPath); err != nil {
		t.Fatal(err)
	}
	never := newStoreAt(t, dir, PullNever, linuxAMD64, offlineTransport())
	neverCalls := 0
	never.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		neverCalls++
		return nil
	}
	_, err := never.Image(context.Background(), refStr, nil)
	if err == nil {
		t.Fatal("damaged retention served under PullNever")
	}
	var verr *VerificationError
	if errors.As(err, &verr) {
		t.Fatalf("availability failure reported as verification failure: %v", err)
	}
	if neverCalls != 0 {
		t.Fatalf("verifier ran %d times without its input available", neverCalls)
	}
	// A second failing request must behave identically — in
	// particular the self-heal branch must have released the ingest
	// lock (a held lock deadlocks here).
	if _, err := never.Image(context.Background(), refStr, nil); err == nil {
		t.Fatal("damaged retention served on retry under PullNever")
	}

	// Pull-permitted: the artifact self-heals by digest and the seam
	// judges bytes hashing to the resolved digest.
	heal := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	var healed []ResolvedIdentity
	heal.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		healed = append(healed, id)
		return nil
	}
	if _, err := heal.Image(context.Background(), refStr, nil); err != nil {
		t.Fatalf("self-heal request failed: %v", err)
	}
	if len(healed) != 1 {
		t.Fatalf("verifier ran %d times", len(healed))
	}
	sum := sha256.Sum256(healed[0].Artifact)
	if hex.EncodeToString(sum[:]) != top.Hex {
		t.Fatalf("self-healed artifact does not hash to the resolved digest")
	}
	if _, err := os.Stat(topPath); err != nil {
		t.Fatalf("self-healed artifact not retained: %v", err)
	}
}

// TestRejectionPreservesPriorReferenceRecord pins REQ-seam-abort
// against the stale-entry arm: a tag's reference-cache record from an
// earlier successful resolution survives a later rejected resolution
// unchanged — the rejection records nothing, not even by overwriting
// the same-sized prior entry.
func TestRejectionPreservesPriorReferenceRecord(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/seam/stale:v1"
	v1img := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("f", "one"))))
	push(t, reg, refStr, v1img)

	dir := scratchDir(t)
	pre := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, reg)
	if _, err := pre.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	recorded := refsContent(t, dir)

	// The tag moves; PullAlways revalidates, resolves the new top,
	// and the verifier rejects it.
	v2img := imageWithPlatform(t, linuxAMD64, newRawLayer(t, tarBytes(t, tfile("f", "two"))))
	push(t, reg, refStr, v2img)
	s := newStoreAt(t, dir, PullAlways, linuxAMD64, reg)
	s.verifier = rejectAll
	var verr *VerificationError
	if _, err := s.Image(context.Background(), refStr, nil); !errors.As(err, &verr) {
		t.Fatalf("rejected moved-tag request returned %v, want VerificationError", err)
	}
	if verr.Digest != mustDigest(t, v2img) {
		t.Fatalf("rejection judged %s, want the moved tag's new top %s", verr.Digest, mustDigest(t, v2img))
	}
	if after := refsContent(t, dir); !sameContent(recorded, after) {
		t.Fatalf("rejected resolution touched the reference cache: before %v, after %v", recorded, after)
	}

	// The surviving record still serves the old image — through the
	// seam, which now admits it.
	admit := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, offlineTransport())
	var seen []ResolvedIdentity
	admit.verifier = func(ctx context.Context, id ResolvedIdentity) error {
		seen = append(seen, id)
		return nil
	}
	img, err := admit.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if img.Hash() != mustDigest(t, v1img) {
		t.Fatalf("surviving record served %s, want the previously admitted %s", img.Hash(), mustDigest(t, v1img))
	}
	if len(seen) != 1 || seen[0].Digest != mustDigest(t, v1img) {
		t.Fatalf("serve through the surviving record did not re-run the seam on the old identity: %v", seen)
	}
}
