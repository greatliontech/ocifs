//go:build linux

package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestOverwriteTransitionCollects pins the ref-overwrite collection
// transition (REQ-store-gc-collect) AND its non-deadlock: the
// auto-collect must run after the request's lease is released — a
// collect under the held slot self-blocks forever.
func TestOverwriteTransitionCollects(t *testing.T) {
	reg := newTestRegistry()
	ref := testHost + "/dbg/move:v1"
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "one")))))
	dir := scratchDir(t)
	s, err := NewStore(Config{Path: dir, Auth: anonKeychain{}, PullPolicy: PullAlways, AutoGC: true, Transport: reg})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Image(context.Background(), ref, nil); err != nil {
		t.Fatal(err)
	}
	// Move the tag; the next Image overwrites the row (prevMoved)
	// and fires the auto-collect transition while holding the lease.
	push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", "two")))))
	done := make(chan error, 1)
	go func() {
		_, err := s.Image(context.Background(), ref, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DEADLOCK: overwrite auto-collect self-blocked on the lease slot")
	}
	// Zero grace: the transition itself reclaimed the old content.
	if _, err := os.Stat(s.BlobPath(digestOfBytes("one"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old content survived the overwrite transition: %v", err)
	}
}
