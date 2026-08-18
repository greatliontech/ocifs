package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
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
			rec, derr := decodeMountRecord(v)
			if derr != nil {
				// A foreign-version row may be a live mount serving
				// this image; refusal is the only safe verdict.
				return fmt.Errorf("image %s removal refused: mount row %q is unreadable to this version", manifest, string(k))
			}
			if rec.Image == manifest && !rec.Owner.Dead() {
				return fmt.Errorf("image %s is served by live mount %q", manifest, string(k))
			}
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
// upper a live mount serves is refused.
func (s *Store) RemoveUpper(ctx context.Context, upperName string) error {
	if !validMountID(upperName) {
		return fmt.Errorf("upper name %q is not a single path element", upperName)
	}
	// The guard row holds the upper name through the tree removal:
	// without it, a fresh writable mount racing in after the row
	// delete would serve a dialect tree the RemoveAll below is
	// destroying. The guard is an ordinary mounts row under this
	// process's identity — arbitration refuses new writable mounts,
	// and a crash leaves a dead row the sweep reclaims.
	guardID := "\x00upper-removal\x00" + upperName
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		mounts, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		for k, v := range mounts.All() {
			rec, derr := decodeMountRecord(v)
			if derr != nil {
				// A foreign-version row may be a live mount serving
				// this upper; refusal is the only safe verdict.
				return fmt.Errorf("upper %q removal refused: mount row %q is unreadable to this version", upperName, string(k))
			}
			if rec.UpperName == upperName && !rec.Owner.Dead() {
				return fmt.Errorf("upper %q is served by live mount %q", upperName, string(k))
			}
		}
		if err := mounts.Put([]byte(guardID), encodeMountRecord(MountRecord{
			Owner:     selfIdentity(),
			UpperName: upperName,
		})); err != nil {
			return err
		}
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
		return err
	}
	rmErr := forceRemoveTree(filepath.Join(s.path, "uppers", upperName))
	_ = s.DeleteMountRecord(ctx, guardID)
	if rmErr != nil {
		return rmErr
	}
	s.AutoCollect(ctx)
	return nil
}
