package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"
)

// assertOnlyProbeResidue fails on any locks-tier entry that is not
// an ACQUIRABLE soundness-probe leftover: claim files must not
// exist, and a probe file may remain only as the deferral branch's
// promise — an unheld, acquirable dead entry (REQ-store-layout).
// A held leftover means a probe that never retired: a leaked lock
// the sweep would judge live forever. The check retires what it
// acquires, so the directory is clean afterwards — a second call
// sees an empty tier, not the same leftovers.
func assertOnlyProbeResidue(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("locks tier unreadable: %v", err)
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "probe-") {
			t.Fatalf("non-probe residue in locks tier: %q", e.Name())
		}
		l, err := oslock.TryAcquire(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("probe leftover %q not acquirable: %v", e.Name(), err)
		}
		l.Retire()
	}
}

// TestLocksTierCreatedAtInit pins the locks tier (REQ-store-layout):
// store initialization creates locks/ and the soundness probe
// retires its own file — at most a deferred probe leftover remains.
func TestLocksTierCreatedAtInit(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	assertOnlyProbeResidue(t, filepath.Join(dir, "locks"))
}

// TestClaimLockPaths pins the claim-file naming (held-lock
// liveness): one file per claim under locks/, names as specced,
// kind prefixes injective.
func TestClaimLockPaths(t *testing.T) {
	s := &Store{path: "/store"}
	for want, got := range map[string]string{
		"/store/locks/mount-m1": s.mountLockPath("m1"),
		"/store/locks/upper-u1": s.upperLockPath("u1"),
		"/store/locks/op-op-x":  s.opLockPath("op-x"),
		"/store/locks/ingest":   s.ingestLockPath(),
		"/store/locks":          s.locksDir(),
	} {
		if got != filepath.FromSlash(want) {
			t.Fatalf("path %q, want %q", got, want)
		}
	}
}

// TestProbeSoundFilesystem pins the probe's pass arm on a real
// filesystem: soundness verified, probe file retired (at most a
// deferred leftover).
func TestProbeSoundFilesystem(t *testing.T) {
	dir := scratchDir(t)
	if err := probeLockingSoundness(dir); err != nil {
		t.Fatal(err)
	}
	assertOnlyProbeResidue(t, dir)
}

// TestProbeRefusesGrantedSecondHolder pins the refusal arm
// (REQ-store-adopt): a filesystem that grants two holders is
// refused with ErrLockingUnsound. The broken filesystem is
// simulated faithfully — every acquisition returns a genuinely
// held lock, each on a distinct file, so no two ever conflict.
func TestProbeRefusesGrantedSecondHolder(t *testing.T) {
	backing := scratchDir(t)
	grant := func(string) (*oslock.Lock, error) {
		return oslock.TryAcquire(filepath.Join(backing, "second-holder"))
	}
	if err := probeLockingSoundnessWith(scratchDir(t), grant); !errors.Is(err, ErrLockingUnsound) {
		t.Fatalf("two-holder filesystem: %v, want ErrLockingUnsound", err)
	}
}

// TestProbeUndecidedSurfaces pins the probe's third arm: a second
// try that can neither hold nor observe contention surfaces its
// error — never a soundness verdict in either direction.
func TestProbeUndecidedSurfaces(t *testing.T) {
	errWeird := errors.New("filesystem hiccup")
	try := func(string) (*oslock.Lock, error) { return nil, errWeird }
	dir := scratchDir(t)
	err := probeLockingSoundnessWith(dir, try)
	if !errors.Is(err, errWeird) {
		t.Fatalf("undecided probe: %v, want the underlying error", err)
	}
	if errors.Is(err, ErrLockingUnsound) {
		t.Fatal("undecided probe judged unsound")
	}
	if !strings.Contains(err.Error(), "locking-soundness probe") {
		t.Fatalf("undecided probe not labeled: %v", err)
	}
	// The undecided arm disposes its probe lock like every other
	// arm — an undecided refusal must not accumulate held files
	// across retries.
	assertOnlyProbeResidue(t, dir)

	// The first arm's failure carries its own label naming the
	// acquisition step: an unopenable locks directory exhausts the
	// bounded open budget and surfaces there.
	err = probeLockingSoundnessWith(filepath.Join(scratchDir(t), "absent"), try)
	if err == nil || !strings.Contains(err.Error(), "acquiring the probe lock") {
		t.Fatalf("first-arm failure mislabeled: %v", err)
	}
}

// TestProbeAcquireWaits pins the first position's primitive: the
// probe's first acquisition WAITS on a briefly held lock rather
// than misreading it — a bare try in that position would return
// ErrHeld immediately, refusing a healthy store over a transient.
func TestProbeAcquireWaits(t *testing.T) {
	p := filepath.Join(scratchDir(t), "claim")
	held, err := oslock.TryAcquire(p)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		held.Close()
	}()
	start := time.Now()
	l, err := probeAcquire(p)
	if err != nil {
		t.Fatalf("probe acquisition did not wait out a transient holder: %v", err)
	}
	l.Retire()
	// A non-vacuous run waited for the release (~100ms); an instant
	// success means the holder was already gone before the
	// acquisition started and the wait was never exercised.
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("vacuous run: the holder released before the acquisition began")
	}
}

// TestInitRefusesUnsoundLocking pins the probe's wiring
// (REQ-store-adopt): a failing probe refuses store initialization
// outright. The refusal arm itself is unreachable on a working
// filesystem, so the probe is swapped at its seam.
func TestInitRefusesUnsoundLocking(t *testing.T) {
	orig := newStoreProbe
	newStoreProbe = func(string) error { return ErrLockingUnsound }
	defer func() { newStoreProbe = orig }()
	_, err := NewStore(scratchDir(t), anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if !errors.Is(err, ErrLockingUnsound) {
		t.Fatalf("unsound filesystem adopted: %v", err)
	}
}

// TestProbePathsUnique pins the uniqueness invariant
// deterministically: two probe runs never share a probe path, so
// concurrent openers cannot read each other's live probe as their
// own filesystem's verdict.
func TestProbePathsUnique(t *testing.T) {
	paths := map[string]bool{}
	recordRefuse := func(p string) (*oslock.Lock, error) {
		paths[p] = true
		return nil, fmt.Errorf("%w: test", oslock.ErrHeld)
	}
	dir := scratchDir(t)
	for i := 0; i < 2; i++ {
		if err := probeLockingSoundnessWith(dir, recordRefuse); err != nil {
			t.Fatal(err)
		}
	}
	if len(paths) != 2 {
		t.Fatalf("probe paths not unique per run: %v", paths)
	}
	// The sound arm retired both probe files on the way out.
	assertOnlyProbeResidue(t, dir)
	for p := range paths {
		if !strings.HasPrefix(filepath.Base(p), "probe-") {
			t.Fatalf("probe path %q lacks the probe- prefix", p)
		}
	}
}

// TestLockTierSweep pins the locks/ disposal rule
// (REQ-store-gc-roots): unheld files with no surviving residue — a
// crashed registration's claim, a crashed opener's probe, a
// retired serve's upper file — retire at the sweep; a held file is
// untouched; a mount file whose row survives keeps its file (the
// reclamation deferral shape).
func TestLockTierSweep(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	// Stranded, residue-free files: acquire-then-close leaves the
	// unheld file a crash or refused registration would.
	for _, p := range []string{s.mountLockPath("gone"), s.upperLockPath("gone"), filepath.Join(s.locksDir(), "probe-stranded")} {
		l, err := oslock.TryAcquire(p)
		if err != nil {
			t.Fatal(err)
		}
		l.Close()
	}
	// A mount file whose ROW survives: the deferral shape — kept.
	img := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("f0", 32)}
	if err := s.bk.MountPut(ctx, "deferred", newMountRecord(img, "", "/d")); err != nil {
		t.Fatal(err)
	}
	if l, err := oslock.TryAcquire(s.mountLockPath("deferred")); err != nil {
		t.Fatal(err)
	} else {
		l.Close()
	}
	// A FOREIGN-version row's deferred file: the row exists but is
	// undecodable to this binary — existence, not decodability,
	// keeps the file.
	foreign := append([]byte{mountRecVersion + 1}, encodeMountRecord(MountRecord{})[1:]...)
	if err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		return ks.Put([]byte("foreigndefer"), foreign)
	}); err != nil {
		t.Fatal(err)
	}
	if l, err := oslock.TryAcquire(s.mountLockPath("foreigndefer")); err != nil {
		t.Fatal(err)
	} else {
		l.Close()
	}
	// A held file: live, untouched.
	held, err := oslock.TryAcquire(s.mountLockPath("livehold"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	s.sweepLockTier(ctx)

	for _, gone := range []string{s.mountLockPath("gone"), s.upperLockPath("gone"), filepath.Join(s.locksDir(), "probe-stranded")} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stranded lock file %s survived the sweep: %v", gone, err)
		}
	}
	if _, err := os.Stat(s.mountLockPath("deferred")); err != nil {
		t.Fatalf("deferred mount's lock file swept despite surviving row: %v", err)
	}
	if _, err := os.Stat(s.mountLockPath("foreigndefer")); err != nil {
		t.Fatalf("foreign row's lock file swept despite surviving row: %v", err)
	}
	if _, err := os.Stat(s.mountLockPath("livehold")); err != nil {
		t.Fatalf("held lock file swept: %v", err)
	}
}

// TestProbesDoNotCrossTrip exercises the uniqueness property
// end-to-end: concurrent openers probing one locks directory all
// pass.
func TestProbesDoNotCrossTrip(t *testing.T) {
	dir := scratchDir(t)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = probeLockingSoundness(dir)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("prober %d: %v", i, err)
		}
	}
}
