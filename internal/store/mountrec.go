package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"

	"github.com/greatliontech/ocifs/internal/projection"
)

// The mounts keyspace (docs/specs/store.md REQ-store-bookkeeping)
// holds one record per mount, keyed by the mount id: the serving
// process's liveness identity, the image digest served, the upper
// name for a writable mount over a store-managed upper, the
// mountpoint path, and the projection report (`projection.md`
// REQ-proj-report) — paths as exact bytes.

const mountRecVersion = 1

// LivenessIdentity discriminates a recorded process's liveness
// (REQ-store-bookkeeping): dead iff the boot id differs, or — same
// boot, same PID namespace — the pid is gone or its start time
// differs. A same-boot row from a foreign PID namespace is
// unjudgeable and treated as live.
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

// MountUpdateReport replaces the report inside the mount's record —
// read-modify-write in one write transaction, so a backend
// republishing accumulated residuals never races another field.
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
		return ks.Put([]byte(id), encodeMountRecord(rec))
	})
}

// RegisterMountRecord writes the mount's record with this process's
// liveness identity and an empty report; the projection's report
// arrives through PublishMountReport once built.
func (s *Store) RegisterMountRecord(ctx context.Context, id string, image v1.Hash, upperName, mountpoint string) error {
	return s.bk.MountPut(ctx, id, MountRecord{
		Owner:      selfIdentity(),
		Image:      image,
		UpperName:  upperName,
		Mountpoint: mountpoint,
	})
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
// consumers, orchestrators, and inspection (REQ-proj-report reads
// through the store).
func (s *Store) MountRecord(ctx context.Context, id string) (MountRecord, error) {
	return s.bk.MountGet(ctx, id)
}

// PublishMountReport replaces the report in the mount's record.
func (s *Store) PublishMountReport(ctx context.Context, id string, rep projection.Report) error {
	return s.bk.MountUpdateReport(ctx, id, rep)
}

// RegisterMountRecordArbitrated is RegisterMountRecord with the
// named-upper arbitration (writable.md REQ-writable-base-binding):
// in the same write transaction, a LIVE row already holding
// upperName refuses the registration — cross-process, by the same
// liveness rules as any registry row; a dead holder never blocks.
func (s *Store) RegisterMountRecordArbitrated(ctx context.Context, id string, image v1.Hash, upperName, mountpoint string) error {
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		// A live row under this id is an active mount: registration
		// must refuse it in the same transaction — the registry is
		// the serialization point (REQ-store-mount-registry). A dead
		// row is overwritten.
		if v, err := ks.Get([]byte(id)); err == nil {
			if rec, derr := decodeMountRecord(v); derr == nil && !rec.Owner.Dead() {
				return fmt.Errorf("mount id %q is in use by a live mount", id)
			}
		} else if !errors.Is(err, gmdb.ErrNotFound) {
			return err
		}
		// The image digest becomes a root: consult the condemned
		// set — registration runs unleased (REQ-store-gc-safe).
		if cond, cerr := s.condemnedByLiveSweep(ctx, tx, image); cerr != nil {
			return cerr
		} else if cond {
			return ErrCondemned
		}
		if upperName != "" {
			for k, v := range ks.All() {
				if string(k) == id {
					continue
				}
				rec, err := decodeMountRecord(v)
				if err != nil {
					continue // undecodable rows never arbitrate
				}
				if rec.UpperName == upperName && !rec.Owner.Dead() {
					return fmt.Errorf("upper %q already serves a writable mount (mount %q)", upperName, string(k))
				}
			}
		}
		return ks.Put([]byte(id), encodeMountRecord(MountRecord{
			Owner:      selfIdentity(),
			Image:      image,
			UpperName:  upperName,
			Mountpoint: mountpoint,
		}))
	})
}

// ReclaimDeadMounts removes dead mounts' rows and state directories
// (REQ-store-mount-registry): best-effort per row — a mountpoint
// that cannot be detached or removed defers, keeping its row for a
// later sweep, never a half-reclaimed id. Returns the reclaimed
// ids.
func (s *Store) ReclaimDeadMounts(ctx context.Context) ([]string, error) {
	var candidates []string
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksMounts)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, err := decodeMountRecord(v)
			if err != nil {
				// A foreign-version row is a future ocifs's — its
				// mount may be live. Handled as the absent-row case
				// (REQ-store-bookkeeping): nothing here to reclaim.
				continue
			}
			if rec.Owner.Dead() {
				candidates = append(candidates, string(k))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var reclaimed []string
	for _, id := range candidates {
		// Claim first: the write transaction re-verifies still-dead
		// and rewrites the row's owner to this sweeper — a revived
		// row skips untouched, and the claimed row stays live so a
		// racing remount refuses instead of serving paths
		// mid-deletion. Deletion follows only successful path
		// teardown; failure restores the original dead owner, and a
		// sweeper crash re-deadens the row through this process's
		// own identity — deferral either way.
		orig, claimed, err := s.claimDeadMount(ctx, id)
		if err != nil || !claimed {
			continue
		}
		stateDir := filepath.Join(s.path, "mounts", id)
		acted := true
		// The err == nil gate is load-bearing beyond ENOENT: a
		// NUL-bearing internal guard id makes Stat fail EINVAL, and
		// its reclamation must stay row-only.
		if _, err := os.Stat(stateDir); err == nil {
			detachStaleMount(filepath.Join(stateDir, "mnt"))
			if err := os.RemoveAll(stateDir); err != nil {
				acted = false
			}
		}
		if !acted {
			// Deferral: restore the original dead owner so the row
			// stays sweepable — never a half-reclaimed id
			// (REQ-store-mount-registry).
			_ = s.restoreMountOwner(ctx, id, orig)
			continue
		}
		if err := s.DeleteMountRecord(ctx, id); err != nil {
			continue
		}
		reclaimed = append(reclaimed, id)
	}
	return reclaimed, nil
}

// DeregisterMount removes the mount's row on clean unmount; the
// store-managed mountpoint directory remains for the consumer
// (api.md REQ-api-mountpoint) as collectible scaffolding.
func (s *Store) DeregisterMount(ctx context.Context, id string) error {
	return s.DeleteMountRecord(ctx, id)
}

// claimDeadMount is the sweep's verdict-then-act boundary: one
// write transaction re-verifies the row is STILL dead and rewrites
// its owner to the sweeper's own identity — a row revived by a
// concurrent remount fails the verify untouched, and a claimed row
// stays LIVE (under the sweeper) so a racing remount of the id
// refuses rather than serving from paths the sweep is deleting.
// The row is deleted only after the paths are gone; a sweeper
// crash re-deadens the row (its identity dies), restoring the
// deferral. Returns the original owner for restoration on
// deferral.
func (s *Store) claimDeadMount(ctx context.Context, id string) (LivenessIdentity, bool, error) {
	var orig LivenessIdentity
	claimed := false
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(id))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		rec, derr := decodeMountRecord(v)
		if derr != nil || !rec.Owner.Dead() {
			return nil
		}
		orig = rec.Owner
		rec.Owner = selfIdentity()
		if err := ks.Put([]byte(id), encodeMountRecord(rec)); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return orig, claimed, err
}

// restoreMountOwner puts a deferred row back under its original
// dead owner, re-arming later sweeps.
func (s *Store) restoreMountOwner(ctx context.Context, id string, owner LivenessIdentity) error {
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(id))
		if err != nil {
			return err
		}
		rec, derr := decodeMountRecord(v)
		if derr != nil {
			return derr
		}
		rec.Owner = owner
		return ks.Put([]byte(id), encodeMountRecord(rec))
	})
}
