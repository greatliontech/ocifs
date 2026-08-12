// Package cas is the store's content CAS (docs/specs/store.md): a
// directory of immutable blobs at <root>/<algorithm>/<hex>, each
// keyed by the digest of its own bytes. The key is derived from the
// bytes as they stream through Put — there is no other write path —
// so a published entry hashing to its key (REQ-store-cas-content)
// holds by construction, and reads are trusted without
// re-verification (the store's integrity boundary is the local
// filesystem). Publication is atomic and durable
// (internal/atomicfile), and an existing entry is never rewritten by
// a Put that observes it: concurrent Puts of identical content that
// both miss the existence check rename over each other, last one
// winning — byte-identical either way, and readers holding the
// replaced inode still see complete content.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/atomicfile"
)

// CAS is a content-addressed blob store rooted at one directory.
type CAS struct {
	root string
}

// New opens (creating if needed) a CAS rooted at root.
func New(root string) (*CAS, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &CAS{root: root}, nil
}

// Put streams r into the CAS and returns the key its bytes hash to,
// along with the byte count. If the key already exists the existing
// entry is kept untouched and the temporary is discarded.
func (c *CAS) Put(r io.Reader) (v1.Hash, int64, error) {
	w, err := atomicfile.NewWriter(c.root)
	if err != nil {
		return v1.Hash{}, 0, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), r)
	if err != nil {
		w.Abort()
		return v1.Hash{}, 0, err
	}
	key := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(h.Sum(nil))}
	path := c.Path(key)
	if _, err := os.Stat(path); err == nil {
		w.Abort()
		return key, n, nil
	} else if !os.IsNotExist(err) {
		w.Abort()
		return v1.Hash{}, 0, err
	}
	if err := w.Commit(path, 0o644); err != nil {
		return v1.Hash{}, 0, err
	}
	return key, n, nil
}

// Open opens the blob at key for reading.
func (c *CAS) Open(key v1.Hash) (*os.File, error) {
	return os.Open(c.Path(key))
}

// Path returns the on-disk path of the blob at key. The blob may or
// may not exist.
func (c *CAS) Path(key v1.Hash) string {
	return filepath.Join(c.root, key.Algorithm, key.Hex)
}
