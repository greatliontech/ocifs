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

// liveMounts is what the mounts keyspace serves right now: the image
// each live mount serves, by the mount's id, and the ids of live rows
// this version cannot read, whose image is unknown. Liveness is the
// claim's, by try-lock, non-blocking inside the write transaction
// (held-lock liveness ordering); undecided is never read as dead.
type liveMounts struct {
	served  map[v1.Hash][]string
	foreign []string
}

func (s *Store) liveMounts(tx *gmdb.Tx) (liveMounts, error) {
	live := liveMounts{served: map[v1.Hash][]string{}}
	mounts, err := tx.OpenKeyspace(ksMounts)
	if err != nil {
		return live, err
	}
	for k, v := range mounts.All() {
		id := string(k)
		rec, foreign := decodeMountRow(v)
		if !s.mountClaimHeld(id) {
			continue
		}
		if foreign {
			live.foreign = append(live.foreign, id)
			continue
		}
		live.served[rec.Image] = append(live.served[rec.Image], id)
	}
	return live, nil
}

// RemoveImage deletes a committed image's root row. A digest a
// live mount serves is refused, as is one a live mount row this
// version cannot read may serve.
func (s *Store) RemoveImage(ctx context.Context, manifest v1.Hash) error {
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		live, err := s.liveMounts(tx)
		if err != nil {
			return err
		}
		if ids := live.served[manifest]; len(ids) > 0 {
			return fmt.Errorf("image %s is served by live mount %q", manifest, ids[0])
		}
		if len(live.foreign) > 0 {
			return fmt.Errorf("image %s removal refused: live mount row %q is unreadable to this version", manifest, live.foreign[0])
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

// RemoveAll severs every root at once — every refs row and every
// localimages row no live mount serves — in one write transaction,
// returning the local images live mounts kept, sorted by digest
// (api.md REQ-api-remove: the operator's emptying of the store). A
// live mount row this version cannot read may serve any local image,
// so under one every local image is kept. A kept image stays rooted
// by its mounts row regardless, so severing nothing of it changes no
// reachability. Named uppers keep their base bindings: they are the
// explicit act's to remove.
func (s *Store) RemoveAll(ctx context.Context) ([]v1.Hash, error) {
	var kept []v1.Hash
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		kept = nil
		live, err := s.liveMounts(tx)
		if err != nil {
			return err
		}
		served := func(h v1.Hash) bool { return len(live.foreign) > 0 || len(live.served[h]) > 0 }
		refs, err := tx.OpenKeyspace(ksRefs)
		if err != nil {
			return err
		}
		var keys [][]byte
		for k := range refs.All() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := refs.Delete(k); err != nil {
				return err
			}
		}
		images, err := tx.OpenKeyspace(ksLocalImages)
		if err != nil {
			return err
		}
		keys = keys[:0]
		for k := range images.All() {
			if h, ok := digestFromKey(k); ok && served(h) {
				kept = append(kept, h)
				continue
			}
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := images.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.AutoCollect(ctx)
	return kept, nil
}
