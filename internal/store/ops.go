package store

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"
)

// The ops keyspace (docs/specs/store.md REQ-store-bookkeeping)
// holds one row per in-flight extra-transactional operation: export
// materializations, commits, sweeps. A row's claim lock (held-lock
// liveness) is its life: while held, its pinned digests are
// collection roots and its recorded temporaries are sweep-exempt;
// unheld, the row is debris. The ingest lease is not a row at all —
// it is the locks/ingest lock itself (REQ-store-single-writer).

const opRecVersion = 1

// Operation kinds.
const (
	opKindExport = "export"
	opKindSweep  = "sweep"
)

// OpRecord is the ops-row value. Owner is diagnostic identity only
// (held-lock liveness): it never decides the op's liveness — the
// claim lock does.
type OpRecord struct {
	Kind  string
	Owner LivenessIdentity
	Pins  []v1.Hash
	Temps []string
}

func encodeOpRecord(rec OpRecord) []byte {
	w := &binWriter{}
	w.byteVal(opRecVersion)
	w.str(rec.Kind)
	w.i64(rec.Owner.Pid)
	w.u64(rec.Owner.StartTime)
	w.str(rec.Owner.PidNS)
	w.str(rec.Owner.BootID)
	w.u64(uint64(len(rec.Pins)))
	for _, p := range rec.Pins {
		w.str(p.Algorithm)
		w.str(p.Hex)
	}
	w.u64(uint64(len(rec.Temps)))
	for _, t := range rec.Temps {
		w.str(t)
	}
	return w.buf
}

// errForeignOpVersion marks an ops row written by a different ocifs
// version: handled as the absent-row case for the record's CONTENT
// (pins and temps unreadable), while the row's liveness stays
// judgeable through its claim lock (REQ-store-bookkeeping).
var errForeignOpVersion = errors.New("foreign op record version")

func decodeOpRecord(data []byte) (OpRecord, error) {
	var rec OpRecord
	r := &binReader{buf: data}
	ver, err := r.byteVal()
	if err != nil {
		return rec, err
	}
	if ver != opRecVersion {
		return rec, fmt.Errorf("%w: op record version %d (want %d)", errForeignOpVersion, ver, opRecVersion)
	}
	if rec.Kind, err = r.str(); err != nil {
		return rec, err
	}
	if rec.Owner.Pid, err = r.i64(); err != nil {
		return rec, err
	}
	if rec.Owner.StartTime, err = r.u64(); err != nil {
		return rec, err
	}
	if rec.Owner.PidNS, err = r.str(); err != nil {
		return rec, err
	}
	if rec.Owner.BootID, err = r.str(); err != nil {
		return rec, err
	}
	n, err := r.u64()
	if err != nil {
		return rec, err
	}
	// Each pin costs at least 2 bytes (two length prefixes).
	if n > uint64(len(r.buf))/2 {
		return rec, errBinTruncated
	}
	for i := uint64(0); i < n; i++ {
		var h v1.Hash
		if h.Algorithm, err = r.str(); err != nil {
			return rec, err
		}
		if h.Hex, err = r.str(); err != nil {
			return rec, err
		}
		rec.Pins = append(rec.Pins, h)
	}
	n, err = r.u64()
	if err != nil {
		return rec, err
	}
	if n > uint64(len(r.buf)) {
		return rec, errBinTruncated
	}
	for i := uint64(0); i < n; i++ {
		t, err := r.str()
		if err != nil {
			return rec, err
		}
		rec.Temps = append(rec.Temps, t)
	}
	if err := r.done(); err != nil {
		return rec, err
	}
	return rec, nil
}

// AcquireIngestLease takes the cross-process ingest lease
// (REQ-store-single-writer): the held locks/ingest claim lock
// itself — no row, no nonce, no in-process slot. Distinct open file
// descriptions exclude each other in-process exactly as across
// processes, a crashed holder's lease releases with its process,
// and a waiter proceeds promptly once the kernel frees the lock,
// cancellably and without leaving abandoned waiters. The lease
// spans content writes through the root-row commit; the caller
// releases with ReleaseIngestLease.
func (s *Store) AcquireIngestLease(ctx context.Context) (*oslock.Lock, error) {
	l, err := oslock.Acquire(ctx, s.ingestLockPath())
	if err != nil {
		return nil, fmt.Errorf("ingest lease: %w", err)
	}
	return l, nil
}

// ReleaseIngestLease drops the lease after the root row committed:
// release without unlink — the lease's claim name is the permanent
// singleton the next acquirer reuses (the lock-tier sweep retires
// an unheld leftover, and acquisition recreates the file).
func (s *Store) ReleaseIngestLease(ctx context.Context, l *oslock.Lock) error {
	if l == nil {
		return nil
	}
	return l.Close()
}

// OpClaim is an in-flight operation's held claim: its id names the
// row and the lock file, and the held lock is the op's liveness
// (held-lock liveness).
type OpClaim struct {
	ID   string
	lock *oslock.Lock
}

// BeginOp registers an in-flight operation: claim lock first
// (lock-before-row — the id is freshly generated, so the
// acquisition cannot meet a live holder), then the row, whose pins
// become collection roots and whose temporaries are sweep-exempt
// while the lock is held (REQ-store-gc-roots). The write
// transaction consults the condemned set — op registration runs
// unleased (REQ-store-gc-safe).
func (s *Store) BeginOp(ctx context.Context, kind string, pins []v1.Hash, temps []string) (*OpClaim, error) {
	id := kind + "-" + uuid.NewString()
	l, err := oslock.TryAcquire(s.opLockPath(id))
	if err != nil {
		return nil, fmt.Errorf("op %s claim: %w", id, err)
	}
	err = s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		for _, p := range pins {
			if cond, cerr := s.condemnedByLiveSweep(ctx, tx, p); cerr != nil {
				return cerr
			} else if cond {
				return ErrCondemned
			}
		}
		ks, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		return ks.Insert([]byte(id), encodeOpRecord(OpRecord{
			Kind:  kind,
			Owner: selfIdentity(),
			Pins:  pins,
			Temps: temps,
		}))
	})
	if err != nil {
		// The claim never vouched for a row: release without
		// unlink — the leftover is an acquirable dead entry for the
		// lock-tier sweep.
		l.Close()
		return nil, err
	}
	return &OpClaim{ID: id, lock: l}, nil
}

// EndOp removes a completed operation's row and retires its claim,
// in that order (row removed, lock file unlinked while held, then
// released).
func (s *Store) EndOp(ctx context.Context, claim *OpClaim) error {
	if claim == nil {
		return nil
	}
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		err = ks.Delete([]byte(claim.ID))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	})
	if err != nil {
		// The row survives: the claim must stay its witness —
		// release without unlink, exactly the deferral shape.
		claim.lock.Close()
		return err
	}
	return claim.lock.Retire()
}

// opClaimHeld is the judge-only three-valued verdict on an op id's
// claim (see claimHeld).
func (s *Store) opClaimHeld(id string) bool {
	return claimHeld(s.opLockPath(id))
}
