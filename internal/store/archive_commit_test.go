//go:build linux

package store

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// TestArchiveCommittedImage pins REQ-api-archive for a committed
// image: its archive carries the base's layers as retained and the
// committed layer's plain tar last, the identity under each form
// the committed image's own.
func TestArchiveCommittedImage(t *testing.T) {
	s, dir, img, upperDir := commitFixture(t)
	digest, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}
	offline := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, cutTransport(t))
	committed, err := offline.Image(context.Background(), LocalRef(digest), nil)
	if err != nil {
		t.Fatal(err)
	}
	var a bytes.Buffer
	id, err := offline.Archive(context.Background(), committed, &a, DockerArchive)
	if err != nil {
		t.Fatal(err)
	}
	names, entries := readArchive(t, a.Bytes())
	var manifest []struct {
		Config string
		Layers []string
	}
	if err := json.Unmarshal(entries["manifest.json"], &manifest); err != nil || len(manifest) != 1 || len(manifest[0].Layers) != 2 {
		t.Fatalf("manifest.json: %s %v", entries["manifest.json"], err)
	}
	if id.Hex+".json" != manifest[0].Config || len(names) != 4 {
		t.Errorf("the identity %s against the manifest's %s; entries %v", id, manifest[0].Config, names)
	}
	// The committed layer is a plain tar: its first entry readable.
	last := entries[manifest[0].Layers[1]]
	if h, err := tar.NewReader(bytes.NewReader(last)).Next(); err != nil || h.Name == "" {
		t.Errorf("the committed layer as retained: %v %v", h, err)
	}
	var o bytes.Buffer
	if id, err := offline.Archive(context.Background(), committed, &o, OCILayout); err != nil || id != digest {
		t.Errorf("the committed image's OCI layout: %v, identity %s, want %s", err, id, digest)
	}
}
