package store

import (
	"context"
	"errors"
	"fmt"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"

	"github.com/greatliontech/ocifs/internal/projection"
)

// The mounts keyspace (docs/specs/store.md REQ-store-bookkeeping)
// holds one record per mount, keyed by the mount id: the serving
// process's diagnostic identity, the image digest served, the
// upper name for a writable mount over a store-managed upper, the
// mountpoint path, the projection report (`projection.md`
// REQ-proj-report) — paths as exact bytes — and the report's
// publication flag. Liveness is never read from the record:
// a mount is alive exactly while its claim lock is held
// (held-lock liveness), judged by try-acquisition.

const mountRecVersion = 1

// LivenessIdentity is a recorded process's DIAGNOSTIC identity
// (REQ-store-bookkeeping): it rides rows for inspection and never
// decides liveness — every claim's claim lock does.
type LivenessIdentity struct {
	Pid       int64
	StartTime uint64 // kernel start-time ticks; zero where unavailable
	PidNS     string // PID-namespace identity; empty where unavailable
	BootID    string // boot identity; empty where unavailable
}

// MountRecord is the mounts-row value.
type MountRecord struct {
	Owner      LivenessIdentity
	Image      v1.Hash
	UpperName  string // empty for read-only and caller-upper mounts
	Mountpoint string
	Report     projection.Report
	// Published distinguishes a registered-but-not-yet-published
	// report from a published clean one (docs/specs/store.md
	// REQ-store-bookkeeping):
	// without it a reader mid-mount takes "no omissions" from a
	// report that does not exist yet. Fresh registrations start
	// unpublished; publication sets it atomically with the report.
	Published bool
}

func encodeMountRecord(rec MountRecord) []byte {
	w := &binWriter{}
	w.byteVal(mountRecVersion)
	w.i64(rec.Owner.Pid)
	w.u64(rec.Owner.StartTime)
	w.str(rec.Owner.PidNS)
	w.str(rec.Owner.BootID)
	w.str(rec.Image.Algorithm)
	w.str(rec.Image.Hex)
	w.str(rec.UpperName)
	w.str(rec.Mountpoint)
	pub := byte(0)
	if rec.Published {
		pub = 1
	}
	w.byteVal(pub)
	w.u64(uint64(len(rec.Report.Entries)))
	for _, e := range rec.Report.Entries {
		w.str(e.Path)
		w.str(string(e.Disposition))
		w.str(string(e.Reason))
		w.str(e.Detail)
	}
	return w.buf
}

func decodeMountRecord(data []byte) (MountRecord, error) {
	var rec MountRecord
	r := &binReader{buf: data}
	ver, err := r.byteVal()
	if err != nil {
		return rec, err
	}
	if ver != mountRecVersion {
		return rec, fmt.Errorf("mount record version %d (want %d): %w", ver, mountRecVersion, os.ErrNotExist)
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
	if rec.Image.Algorithm, err = r.str(); err != nil {
		return rec, err
	}
	if rec.Image.Hex, err = r.str(); err != nil {
		return rec, err
	}
	if rec.UpperName, err = r.str(); err != nil {
		return rec, err
	}
	if rec.Mountpoint, err = r.str(); err != nil {
		return rec, err
	}
	pub, err := r.byteVal()
	if err != nil {
		return rec, err
	}
	rec.Published = pub != 0
	n, err := r.u64()
	if err != nil {
		return rec, err
	}
	// A corrupt count must error, never allocate: each encoded
	// report entry costs at least 4 bytes (four length prefixes).
	if n > uint64(len(r.buf))/4 {
		return rec, errBinTruncated
	}
	// A present record always carries a present entries list — the
	// empty report stays distinguishable from an absent row
	// (REQ-proj-report).
	rec.Report.Entries = make([]projection.ReportEntry, 0, n)
	for i := uint64(0); i < n; i++ {
		var e projection.ReportEntry
		if e.Path, err = r.str(); err != nil {
			return rec, err
		}
		d, err := r.str()
		if err != nil {
			return rec, err
		}
		e.Disposition = projection.Disposition(d)
		reason, err := r.str()
		if err != nil {
			return rec, err
		}
		e.Reason = projection.Reason(reason)
		if e.Detail, err = r.str(); err != nil {
			return rec, err
		}
		rec.Report.Entries = append(rec.Report.Entries, e)
	}
	if err := r.done(); err != nil {
		return rec, err
	}
	return rec, nil
}

// MountPut writes the mount's record.
func (b *bookkeeping) MountPut(ctx context.Context, id string, rec MountRecord) error {
	return b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		return ks.Put([]byte(id), encodeMountRecord(rec))
	})
}

// MountGet loads the mount's record; a missing row is
// os.ErrNotExist.
func (b *bookkeeping) MountGet(ctx context.Context, id string) (MountRecord, error) {
	var rec MountRecord
	err := b.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksMounts)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(id))
		if errors.Is(err, gmdb.ErrNotFound) {
			return fmt.Errorf("mount record %q: %w", id, os.ErrNotExist)
		}
		if err != nil {
			return err
		}
		rec, err = decodeMountRecord(v)
		if err != nil {
			return fmt.Errorf("mount record %q: %w", id, err)
		}
		return nil
	})
	return rec, err
}

// MountUpdateReport replaces the report inside the mount's record
// and marks it published — read-modify-write in one write
// transaction, so the flag is atomic with the report it marks
// (REQ-store-bookkeeping) and a backend republishing accumulated
// residuals never races another field; republication is an
// idempotent set.
func (b *bookkeeping) MountUpdateReport(ctx context.Context, id string, rep projection.Report) error {
	return b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(id))
		if errors.Is(err, gmdb.ErrNotFound) {
			return fmt.Errorf("mount record %q: %w", id, os.ErrNotExist)
		}
		if err != nil {
			return err
		}
		rec, err := decodeMountRecord(v)
		if err != nil {
			return fmt.Errorf("mount record %q: %w", id, err)
		}
		rec.Report = rep
		rec.Published = true
		return ks.Put([]byte(id), encodeMountRecord(rec))
	})
}

// newMountRecord is the single source of every fresh registration's
// row: this process's liveness identity, an empty report, and —
// structurally, by zero value — unpublished (store.md
// REQ-store-bookkeeping: every fresh registration starts
// unpublished; a remount over a dead row must never inherit the
// dead mount's published report as its own).
func newMountRecord(image v1.Hash, upperName, mountpoint string) MountRecord {
	return MountRecord{
		Owner:      selfIdentity(),
		Image:      image,
		UpperName:  upperName,
		Mountpoint: mountpoint,
	}
}

// DeleteMountRecord removes the mount's record — a failed mount
// attempt leaves no row (REQ-store-mount-registry).
func (s *Store) DeleteMountRecord(ctx context.Context, id string) error {
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
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

// MountRecord loads a mount's record — the read surface for
// consumers, orchestrators, and inspection (projection.md
// REQ-proj-report reads through the store).
func (s *Store) MountRecord(ctx context.Context, id string) (MountRecord, error) {
	return s.bk.MountGet(ctx, id)
}

// PublishMountReport replaces the report in the mount's record.
func (s *Store) PublishMountReport(ctx context.Context, id string, rep projection.Report) error {
	return s.bk.MountUpdateReport(ctx, id, rep)
}

// MountClaim is a live mount's held claim: the mount-id lock and,
// for a writable mount over a store-managed upper, the upper lock —
// acquired before the row they vouch for (held-lock liveness), held
// for the serve, retired at deregistration.
type MountClaim struct {
	mount *oslock.Lock
	upper *oslock.Lock // nil unless writable over a store-managed upper
}

// release closes both locks without unlinking — the deferral shape:
// the claim files persist as acquirable entries.
func (c *MountClaim) release() {
	if c.upper != nil {
		c.upper.Close()
	}
	c.mount.Close()
}

// retire ends both claims: unlink-while-held, then release, per the
// lock-file discipline.
func (c *MountClaim) retire() {
	if c.upper != nil {
		c.upper.Retire()
	}
	c.mount.Retire()
}

// ClaimUpper acquires the named upper's serve claim
// (writable.md REQ-writable-base-binding): the lock is taken
// BEFORE any bookkeeping write for the upper — tree creation and
// the base binding happen under the held claim, so a concurrent
// RemoveUpper (which holds the same lock across its row delete and
// tree removal) can never interleave with an upper being brought
// up. ErrHeld refuses as already-serving; ownership passes to the
// registration, which folds it into the mount's claim.
func (s *Store) ClaimUpper(name string) (*oslock.Lock, error) {
	l, err := oslock.TryAcquire(s.upperLockPath(name))
	if errors.Is(err, oslock.ErrHeld) {
		return nil, fmt.Errorf("upper %q already serves a writable mount", name)
	}
	if err != nil {
		return nil, fmt.Errorf("upper %q claim: %w", name, err)
	}
	return l, nil
}

// RegisterMountRecordArbitrated acquires the mount's claim and
// registers its row (REQ-store-mount-registry: lock before row).
// The mount-id lock arbitrates id reuse — a held lock is a live
// mount or a mid-reclamation sweep, refused as in-use either way.
// The named upper's serve claim arrives pre-acquired (ClaimUpper,
// taken before the upper's own bookkeeping writes) and its
// ownership transfers here unconditionally: it folds into the
// returned claim on success and is released on failure. The write
// transaction consults the condemned set (registration runs
// unleased — REQ-store-gc-safe) and overwrites any dead leftover
// row. On failure nothing is held and no row is written.
func (s *Store) RegisterMountRecordArbitrated(ctx context.Context, id string, image v1.Hash, upperName, mountpoint string, upper *oslock.Lock) (*MountClaim, error) {
	mountLock, err := oslock.TryAcquire(s.mountLockPath(id))
	if errors.Is(err, oslock.ErrHeld) {
		if upper != nil {
			upper.Close()
		}
		return nil, fmt.Errorf("mount id %q is in use by a live mount", id)
	}
	if err != nil {
		if upper != nil {
			upper.Close()
		}
		return nil, fmt.Errorf("mount id %q claim: %w", id, err)
	}
	claim := &MountClaim{mount: mountLock, upper: upper}
	err = s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		// The image digest becomes a root: consult the condemned
		// set — registration runs unleased (REQ-store-gc-safe).
		if cond, cerr := s.condemnedByLiveSweep(ctx, tx, image); cerr != nil {
			return cerr
		} else if cond {
			return ErrCondemned
		}
		return ks.Put([]byte(id), encodeMountRecord(newMountRecord(image, upperName, mountpoint)))
	})
	if err != nil {
		claim.release()
		return nil, err
	}
	return claim, nil
}

// DeregisterMount removes the mount's row and retires its claim, in
// that order (REQ-store-mount-registry: row removed, lock file
// unlinked while held, then released) — on clean unmount and on a
// failed mount attempt alike; the store-managed mountpoint
// directory remains for the consumer (api.md REQ-api-mountpoint)
// as collectible scaffolding. A nil claim removes the row only.
func (s *Store) DeregisterMount(ctx context.Context, id string, claim *MountClaim) error {
	if err := s.DeleteMountRecord(ctx, id); err != nil {
		return err
	}
	if claim != nil {
		claim.retire()
	}
	return nil
}
