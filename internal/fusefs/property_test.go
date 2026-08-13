package fusefs

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestPropertyReadEqualsBlobBytes pins REQ-proj-content as a for-all
// over content, offset, and length: the bytes a node read returns
// equal the content-CAS blob at that offset exactly — short reads
// only at EOF, never a stale buffer tail.
func TestPropertyReadEqualsBlobBytes(t *testing.T) {
	dir := scratchtest.Dir(t, "fusefs")

	rapid.Check(t, func(rt *rapid.T) {
		content := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "content")
		sum := sha256.Sum256(content)
		digest := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
		blobPath := filepath.Join(dir, digest.Hex)
		if err := os.WriteFile(blobPath, content, 0o644); err != nil {
			rt.Fatal(err)
		}

		view, err := layer.Unify([]layer.Layer{{
			{Header: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}, Digest: digest},
		}})
		if err != nil {
			rt.Fatal(err)
		}
		p, err := projection.New(view, nil, Capabilities())
		if err != nil {
			rt.Fatal(err)
		}
		root := New(p, func(v1.Hash) string { return blobPath }).(*node)
		entry, ok := p.Lookup(p.Root(), "f")
		if !ok {
			rt.Fatal("f not projected")
		}
		fnode := &node{s: root.s, e: entry}

		fh, _, errno := fnode.Open(context.Background(), 0)
		if errno != 0 {
			rt.Fatalf("open: %v", errno)
		}
		defer fnode.Release(context.Background(), fh)

		off := rapid.Int64Range(0, int64(len(content))+8).Draw(rt, "off")
		length := rapid.IntRange(0, 80).Draw(rt, "len")
		dest := make([]byte, length)
		// Poison the buffer: any stale tail leaking into the result is
		// visible.
		for i := range dest {
			dest[i] = 0xEE
		}
		res, errno := fnode.Read(context.Background(), fh, dest, off)
		if errno != 0 {
			rt.Fatalf("read(off=%d,len=%d): %v", off, length, errno)
		}
		got, status := res.Bytes(make([]byte, length))
		if status != fuse.OK {
			rt.Fatalf("result bytes: %v", status)
		}

		want := []byte{}
		if off < int64(len(content)) {
			end := off + int64(length)
			if end > int64(len(content)) {
				end = int64(len(content))
			}
			want = content[off:end]
		}
		if string(got) != string(want) {
			rt.Fatalf("read(off=%d,len=%d) = %x, want %x", off, length, got, want)
		}
	})
}
