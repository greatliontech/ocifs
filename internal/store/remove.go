package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"
)

// Removal severs roots (api.md REQ-api-remove): content becomes
// garbage for collection rather than being deleted inline.

// RemoveRef deletes the cached resolution for ref — the cached
// row, not the remote. A missing row is not an error: the root is
// equally severed.
func (s *Store) RemoveRef(ctx context.Context, refStr string) error {
	ref, err := name.ParseReference(refStr)
	if err != nil {
		return err
	}
	err = s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksRefs)
		if err != nil {
			return err
		}
		err = ks.Delete(refKey(ref))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	s.AutoCollect(ctx)
	return nil
}

// RemoveImage deletes a committed image's root row. A digest a
// live mount serves is refused.
func (s *Store) RemoveImage(ctx context.Context, manifest v1.Hash) error {
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		mounts, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		for k, v := range mounts.All() {
			id := string(k)
			rec, derr := decodeMountRecord(v)
			if derr == nil && rec.Image != manifest {
				continue
			}
			// A row naming this image — or a foreign-version row,
			// whose image this binary cannot read — blocks removal
			// exactly while its mount is alive: liveness by
			// try-lock, non-blocking inside this write transaction
			// (held-lock liveness ordering). Undecided is never
			// read as dead.
			if !s.mountClaimHeld(id) {
				continue
			}
			if derr != nil {
				return fmt.Errorf("image %s removal refused: live mount row %q is unreadable to this version", manifest, id)
			}
			return fmt.Errorf("image %s is served by live mount %q", manifest, id)
		}
		ks, err := tx.OpenKeyspace(ksLocalImages)
		if err != nil {
			return err
		}
		err = ks.Delete(layerKey(manifest))
		if err != nil && !errors.Is(err, gmdb.ErrNotFound) {
			return err
		}
		// Acquisition of a committed image records a local-namespace
		// refs row (REQ-store-gc-roots digest-form rooting); removal
		// severs those resolution rows too, or the image would stay
		// rooted by its own past mounts (api.md REQ-api-remove).
		refs, err := tx.OpenKeyspace(ksRefs)
		if err != nil {
			return err
		}
		var stale [][]byte
		for k, v := range refs.All() {
			if strings.HasPrefix(string(k), LocalRegistry+"\x00") && string(v) == manifest.String() {
				stale = append(stale, append([]byte(nil), k...))
			}
		}
		for _, k := range stale {
			if err := refs.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.AutoCollect(ctx)
	return nil
}

// RemoveUpper deletes a named upper — its base-binding row and its
// dialect tree — the explicit act REQ-api-mount-writable names. An
// upper a live writable mount serves is refused by its claim lock
// (writable.md REQ-writable-base-binding): the removal holds
// locks/upper-<name> from before the row transaction through the
// tree removal — the kernel's own arbitration, cross-process and
// crash-released, so no fresh writable mount can race in and serve
// a tree the removal is destroying. Acquisition precedes the write
// transaction per the claim-lock ordering (held-lock liveness).
func (s *Store) RemoveUpper(ctx context.Context, upperName string) error {
	if !validMountID(upperName) {
		return fmt.Errorf("upper name %q is not a single path element", upperName)
	}
	l, err := oslock.TryAcquire(s.upperLockPath(upperName))
	if errors.Is(err, oslock.ErrHeld) {
		return fmt.Errorf("upper %q is served by a live writable mount", upperName)
	}
	if err != nil {
		return fmt.Errorf("upper %q claim: %w", upperName, err)
	}
	err = s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksUppers)
		if err != nil {
			return err
		}
		err = ks.Delete([]byte(upperName))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	})
	if err != nil {
		l.Close()
		return err
	}
	rmErr := forceRemoveTree(upperDirOf(s.path, upperName))
	// The upper is gone (or its remainder is rowless debris): the
	// claim name retires with it, as its final holder.
	l.Retire()
	if rmErr != nil {
		return rmErr
	}
	s.AutoCollect(ctx)
	return nil
}
