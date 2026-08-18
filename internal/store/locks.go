package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
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
