package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"
	"github.com/greatliontech/gmdb"
)

// The ops keyspace (docs/specs/store.md REQ-store-bookkeeping)
// holds one row per in-flight extra-transactional operation: the
// ingest lease, export materializations, commits. A live row's
// pinned digests are collection roots and its recorded temporaries
// are exempt from sweeps; a dead row is debris, its temporaries
// swept wherever they live.

const opRecVersion = 1

// Operation kinds. The ingest lease is the distinguished singleton
// op enforcing REQ-store-single-writer.
const (
	opKindIngest = "ingest"
	opKindExport = "export"
)

// ingestLeaseKey is the lease's fixed row key: one ingesting
// process at a time means one row to contend on.
const ingestLeaseKey = "ingest-lease"

// OpRecord is the ops-row value.
type OpRecord struct {
	Kind  string
	Owner LivenessIdentity
	// Nonce distinguishes acquisitions within one process: release
	// matches identity AND nonce, so an unpaired release can never
	// drop another acquisition's lease.
	Nonce string
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
	w.str(rec.Nonce)
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
	if rec.Nonce, err = r.str(); err != nil {
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
// (REQ-store-single-writer): one write transaction claims the
// fixed lease row — absent or dead-held rows claim immediately, a
// live-held row waits and retries until the context ends. The lease
// spans content writes through the root-row commit; the caller
// releases with ReleaseIngestLease.
// errForeignOpVersion marks an ops row written by a different ocifs
// version. For the LEASE it means wait, never claim: an older
// binary treating a newer one's live lease as absent would put two
// writers on the content tiers (REQ-store-single-writer overrides
// the general foreign-version-as-absent rule here —
// REQ-store-bookkeeping).
var errForeignOpVersion = errors.New("foreign op record version")

// processLease is the in-process half of the ingest lease: a
// per-store-root slot serializing this process's acquisitions
// (ctx-aware, unlike a bare mutex), so a self-owned lease ROW seen
// while holding the slot is unambiguous residue of a failed
// release — reclaimable — never a sibling goroutine's live hold.
type processLease struct {
	sem    chan struct{}
	mu     sync.Mutex
	holder string
}

var leaseSlots sync.Map // cleaned store root -> *processLease

func leaseSlotFor(root string) *processLease {
	v, _ := leaseSlots.LoadOrStore(filepath.Clean(root), &processLease{sem: make(chan struct{}, 1)})
	return v.(*processLease)
}

func (s *Store) AcquireIngestLease(ctx context.Context) (string, error) {
	nonce := fmt.Sprintf("%d-%s", selfIdentity().Pid, uuid.NewString())
	slot := leaseSlotFor(s.path)
	select {
	case slot.sem <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	release := func() { <-slot.sem }
	backoff := 10 * time.Millisecond
	for {
		claimed := false
		err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
			ks, err := tx.OpenKeyspace(ksOps)
			if err != nil {
				return err
			}
			v, err := ks.Get([]byte(ingestLeaseKey))
			if err == nil {
				rec, derr := decodeOpRecord(v)
				switch {
				case errors.Is(derr, errForeignOpVersion):
					return nil // unknown holder: wait, never claim
				case derr == nil && rec.Owner == selfIdentity():
					// This process's own residue (a failed release):
					// reclaimable — in-process content safety rests
					// on the shared ingest mutex, not the lease.
				case derr == nil && !rec.Owner.Dead():
					return nil // live foreign holder: wait
				default:
					// Dead holder, or a torn current-version row
					// (transactionally impossible; debris either
					// way): claimable.
				}
			} else if !errors.Is(err, gmdb.ErrNotFound) {
				return err
			}
			if err := ks.Put([]byte(ingestLeaseKey), encodeOpRecord(OpRecord{
				Kind:  opKindIngest,
				Owner: selfIdentity(),
				Nonce: nonce,
			})); err != nil {
				return err
			}
			claimed = true
			return nil
		})
		if err != nil {
			release()
			return "", err
		}
		if claimed {
			slot.mu.Lock()
			slot.holder = nonce
			slot.mu.Unlock()
			return nonce, nil
		}
		select {
		case <-ctx.Done():
			release()
			return "", ctx.Err()
		case <-time.After(backoff):
		}
		// Bounded exponential backoff; sustained contention is
		// ingest-vs-ingest, where fairness matters less than
		// progress (the holder's span is one image's ingest).
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

// ReleaseIngestLease drops the lease after the root row committed.
// Only this acquisition's own row — identity AND nonce — is
// deleted; a successor's or sibling acquisition's lease is never
// clobbered. A failed release is retried once; residue beyond that
// is reclaimed by this process's next acquisition (never by the
// clock).
func (s *Store) ReleaseIngestLease(ctx context.Context, nonce string) error {
	self := selfIdentity()
	release := func() error {
		return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
			ks, err := tx.OpenKeyspace(ksOps)
			if err != nil {
				return err
			}
			v, err := ks.Get([]byte(ingestLeaseKey))
			if errors.Is(err, gmdb.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			rec, derr := decodeOpRecord(v)
			if derr != nil || rec.Owner != self || rec.Nonce != nonce {
				return nil
			}
			return ks.Delete([]byte(ingestLeaseKey))
		})
	}
	err := release()
	if err != nil {
		err = release()
	}
	slot := leaseSlotFor(s.path)
	slot.mu.Lock()
	if slot.holder == nonce {
		slot.holder = ""
		slot.mu.Unlock()
		<-slot.sem
	} else {
		slot.mu.Unlock()
	}
	return err
}

// BeginOp registers an in-flight operation: its pins are collection
// roots and its temporaries sweep-exempt while this process lives
// (REQ-store-gc-roots). Returns the op id for EndOp.
func (s *Store) BeginOp(ctx context.Context, kind string, pins []v1.Hash, temps []string) (string, error) {
	id := kind + "-" + uuid.NewString()
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		// Pins become roots: consult the condemned set — op
		// registration runs unleased (REQ-store-gc-safe).
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
		return "", err
	}
	return id, nil
}

// EndOp removes a completed operation's row.
func (s *Store) EndOp(ctx context.Context, id string) error {
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		err = ks.Delete([]byte(id))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	})
}
