package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/cas"
)

// TestPropertyTierKeyspacesDisjoint pins REQ-store-ns as a for-all
// over keys: no digest resolves to the same path in the layer-index
// tier and the content CAS, and each stays under its own root.
func TestPropertyTierKeyspacesDisjoint(t *testing.T) {
	dir := scratchDir(t)
	contentCAS, err := cas.New(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	li := layerIndexes{root: filepath.Join(dir, "layers")}
	hexRunes := rapid.RuneFrom([]rune("0123456789abcdef"))
	rapid.Check(t, func(rt *rapid.T) {
		h := v1.Hash{
			Algorithm: "sha256",
			Hex:       rapid.StringOfN(hexRunes, 64, 64, -1).Draw(rt, "hex"),
		}
		indexPath, blobPath := li.path(h), contentCAS.Path(h)
		if indexPath == blobPath {
			rt.Fatalf("colliding path %s", indexPath)
		}
		if !strings.HasPrefix(indexPath, filepath.Join(dir, "layers")+string(os.PathSeparator)) {
			rt.Fatalf("index path %s outside layers/", indexPath)
		}
		if !strings.HasPrefix(blobPath, filepath.Join(dir, "blobs")+string(os.PathSeparator)) {
			rt.Fatalf("blob path %s outside blobs/", blobPath)
		}
	})
}

// TestPropertyTamperRejected pins REQ-store-ingest-verified as a
// for-all over corruption position: however the network flips one
// byte of a layer blob, ingest fails and neither the blob nor a ref
// is persisted.
func TestPropertyTamperRejected(t *testing.T) {
	l := newRawLayer(t, tarBytes(t, tfile("x", "trustworthy")))
	ld, err := l.Digest()
	if err != nil {
		t.Fatal(err)
	}
	img := makeImage(t, l)

	rapid.Check(t, func(rt *rapid.T) {
		pos := rapid.IntRange(0, len(l.compressed)-1).Draw(rt, "pos")

		transport := tamperTransport(newTestRegistry(), ld, pos, l.compressed)
		refStr := testHost + "/test/tampered:v1"
		push(t, transport, refStr, img)

		s, dir := newTestStore(t, PullIfNotPresent, transport)
		if _, err := s.Image(context.Background(), refStr); err == nil {
			rt.Fatalf("ingest succeeded with byte %d flipped", pos)
		}
		if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", ld.Algorithm, ld.Hex)); !errors.Is(err, os.ErrNotExist) {
			rt.Fatalf("tampered blob persisted (flip at %d): %v", pos, err)
		}
		if files := refFiles(t, dir); len(files) != 0 {
			rt.Fatalf("ref written despite failed verification: %v", files)
		}
	})
}
