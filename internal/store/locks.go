package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"
)

// The locks tier (docs/specs/store.md REQ-store-layout, held-lock
// liveness): one empty lock file per claim under locks/, whose
// advisory lock is the store's only liveness authority, plus the
// transient soundness-probe file of a store open. Claim names are
// single path elements by construction — mount ids and upper names
// pass the mount-id rule (REQ-api-mount-id) before reaching a path
// helper, and op ids are generated — and the kind set is
// prefix-free (no kind equals another kind followed by "-"; the
// property injectivity rests on), so no id can forge another
// kind's claim file.

// ErrLockingUnsound reports a store filesystem whose advisory locks
// do not conflict between two open file descriptions. Held-lock
// liveness is the store's only liveness authority, and a filesystem
// that grants two holders would let a sweeper judge every live
// claim dead (REQ-store-adopt) — the store refuses rather than
// destroys.
var ErrLockingUnsound = errors.New("store filesystem's advisory locking is unsound: a second try-lock on a held lock did not conflict; use a filesystem with working advisory locks")

// locksDirOf is the tier's one name site.
func locksDirOf(path string) string { return filepath.Join(path, "locks") }

func (s *Store) locksDir() string { return locksDirOf(s.path) }

// claimLockPath is the single constructor of claim-file names: the
// kind prefix carries the injectivity, the name its identity.
func (s *Store) claimLockPath(kind, name string) string {
	return filepath.Join(s.locksDir(), kind+"-"+name)
}

// mountLockPath is the claim lock file for mount id (held for the
// serve; acquirable = the mount is dead).
func (s *Store) mountLockPath(id string) string {
	return s.claimLockPath("mount", id)
}

// upperLockPath is the claim lock file for a store-managed upper's
// writable serve (writable.md REQ-writable-base-binding).
func (s *Store) upperLockPath(name string) string {
	return s.claimLockPath("upper", name)
}

// opLockPath is the claim lock file for an in-flight operation's
// row and owned temporaries (REQ-store-bookkeeping ops clause).
func (s *Store) opLockPath(id string) string {
	return s.claimLockPath("op", id)
}

// ingestLockPath is the ingest lease itself — not a row at all
// (REQ-store-single-writer).
func (s *Store) ingestLockPath() string {
	return filepath.Join(s.locksDir(), "ingest")
}

// upperDirOf is the uppers tier's one name site for a named upper's
// dialect-tree root.
func upperDirOf(path, name string) string {
	return filepath.Join(path, "uppers", name)
}

// claimVerdict is the three-valued judgment of a claim's lock file
// (held-lock liveness, store.md). judgeClaim is the ONLY derivation
// site: call sites branch on these named values, never on raw error
// shapes — parallel derivations are how one arm drifts when the
// verdict rules change.
type claimVerdict int

const (
	// claimLive: the try-lock blocked — a live holder, frozen
	// processes included. Nothing is held by the judge.
	claimLive claimVerdict = iota
	// claimDead: the try-lock acquired — the previous holder is
	// gone and the acquisition IS the claim: the caller is now the
	// claim's holder and must end it with exactly one of
	// Lock.Retire (final-holder disposal — unlink while held, then
	// release) or Lock.Close (deferral — release without unlink;
	// row and file persist for retry).
	claimDead
	// claimUndecided: any other outcome (open failure, permission
	// problem) — never death. The caller treats the claim as live
	// and retries later.
	claimUndecided
)

// judgeClaim renders the verdict on a claim lock file. For
// claimDead the returned Lock is non-nil and owned by the caller
// (see claimDead's disposal contract); for every other verdict it
// is nil.
func judgeClaim(path string) (claimVerdict, *oslock.Lock) {
	l, err := oslock.TryAcquire(path)
	switch {
	case err == nil:
		return claimDead, l
	case errors.Is(err, oslock.ErrHeld):
		return claimLive, nil
	default:
		return claimUndecided, nil
	}
}

// claimHeld is the judge-only consult: a dead claim's accidental
// acquisition is released immediately without unlink (the deferral
// shape, leaving disposal to the sweep), and both live and
// UNDECIDED read as held. Callers that must KEEP a dead claim's
// acquisition (reclamation, the sweep) use judgeClaim directly.
func claimHeld(path string) bool {
	v, l := judgeClaim(path)
	if v == claimDead {
		l.Close()
		return false
	}
	return true
}

// mountClaimHeld is claimHeld over a mount id's claim.
func (s *Store) mountClaimHeld(id string) bool {
	return claimHeld(s.mountLockPath(id))
}

// rowExists reports whether ANY row occupies the id in the named
// keyspace — decodable or foreign — with an undecided read counting
// as existing (never a destruction verdict). Deliberately RAW
// presence, not decodeMountRow/decodeOpRow classification: its one
// consumer (sweepLockTier) asks "does residue exist", and a foreign
// row is residue exactly like a native one.
func (s *Store) rowExists(ctx context.Context, keyspace, id string) bool {
	exists := true
	err := dbView(ctx, s.bk.db, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(keyspace)
		if err != nil {
			return err
		}
		_, err = ks.Get([]byte(id))
		if errors.Is(err, gmdb.ErrNotFound) {
			exists = false
			return nil
		}
		return err
	})
	return exists || err != nil
}

// sweepLockTier disposes of dead claims' lock files
// (REQ-store-gc-roots): for each locks/ file, try-lock — blocked
// means live, untouched; acquired means the claim is dead, and the
// sweep unlinks as its final holder once no residue survives. A
// mount claim whose row still exists keeps its file with the row
// (the reclamation deferral shape — the row-driven sweep owns that
// disposal); every other unheld file (a crashed registration's
// claim, a crashed opener's probe, a retired serve's upper file)
// has no residue and retires here.
func (s *Store) sweepLockTier(ctx context.Context) {
	ents, err := os.ReadDir(s.locksDir())
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		v, l := judgeClaim(filepath.Join(s.locksDir(), name))
		if v != claimDead {
			continue // live or undecided: untouched
		}
		// RAW row existence, never decodability: a foreign-version
		// row deliberately reads as absent through the decoding
		// getters (heal-as-absent), but its FILE must stay with the
		// row (the deferral shape). Only a proven-absent row makes
		// the file residue-free; an undecided read keeps it too.
		if id, ok := strings.CutPrefix(name, "mount-"); ok {
			if s.rowExists(ctx, ksMounts, id) {
				l.Close()
				continue
			}
		}
		if id, ok := strings.CutPrefix(name, "op-"); ok {
			if s.rowExists(ctx, ksOps, id) {
				l.Close()
				continue
			}
		}
		l.Retire()
	}
}

// probeAcquireTimeout bounds the probe's first acquisition: on a
// fresh unique path nothing can legitimately hold the lock, so the
// budget only covers transient open failures — and turns a
// filesystem reporting phantom contention into a loud, bounded
// refusal instead of a hung store open.
const probeAcquireTimeout = 5 * time.Second

// newStoreProbe is NewStore's soundness probe — a variable only so
// the refusal wiring is testable on a filesystem whose locks work.
// Never reassigned outside tests.
var newStoreProbe = probeLockingSoundness

// probeLockingSoundness refuses a filesystem whose advisory locks
// are broken (REQ-store-adopt): while a probe lock is held, a
// second open file description's try-lock on the same file must
// observe contention. The probe file's name is unique per opener,
// so concurrent store opens never read each other's live probe as
// their own filesystem's verdict; the file is retired
// (unlink-while-held) on the way out, and a crash-orphaned probe
// file is an ordinary unheld locks/ entry for the sweep.
func probeLockingSoundness(locksDir string) error {
	return probeLockingSoundnessWith(locksDir, oslock.TryAcquire)
}

// probeAcquire takes the probe lock: blocking with the bounded
// budget, never a bare try — a transient open failure must not
// refuse a healthy store, and a filesystem reporting phantom
// contention must surface as a bounded timeout, not a misdirecting
// held-by-a-live-process error.
func probeAcquire(p string) (*oslock.Lock, error) {
	ctx, cancel := context.WithTimeout(context.Background(), probeAcquireTimeout)
	defer cancel()
	return oslock.Acquire(ctx, p)
}

// probeLockingSoundnessWith is the probe with the second
// description's try injectable — the only seam through which the
// refusal and undecided arms are reachable on a filesystem whose
// locks actually work. The first acquisition is probeAcquire by
// construction (no parameter to miswire); only the verdict-
// rendering try varies.
func probeLockingSoundnessWith(locksDir string, try func(string) (*oslock.Lock, error)) error {
	p := filepath.Join(locksDir, "probe-"+uuid.NewString())
	held, err := probeAcquire(p)
	if err != nil {
		return fmt.Errorf("locking-soundness probe: acquiring the probe lock: %w", err)
	}
	second, err := try(p)
	if errors.Is(err, oslock.ErrHeld) {
		// Sound: the held lock excluded the second description. The
		// verdict is already rendered; retirement is best-effort
		// disposal (an unlink deferral or close error leaves an
		// unheld probe file — an ordinary acquirable dead entry,
		// never grounds to refuse a proven-sound store).
		_ = held.Retire()
		return nil
	}
	if err == nil {
		second.Close()
	}
	_ = held.Retire()
	if err != nil {
		// Neither held nor granted: the probe could not judge —
		// surfaced as undecided, never a soundness verdict.
		return fmt.Errorf("locking-soundness probe: %w", err)
	}
	return ErrLockingUnsound
}
