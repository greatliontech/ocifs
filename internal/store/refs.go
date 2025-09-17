package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type referenceStore string

var emptyHash = v1.Hash{}

// Get returns (digest, true, nil) if present; ("", false, nil) if missing.
func (rc referenceStore) Get(ref name.Reference) (v1.Hash, bool, error) {
	b, err := os.ReadFile(rc.pathForRef(ref))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return emptyHash, false, nil
		}
		return emptyHash, false, err
	}
	h, err := v1.NewHash(string(b))
	if err != nil {
		return emptyHash, false, fmt.Errorf("invalid hash in ref store %q: %w", ref, err)
	}
	return h, true, nil
}

// Put writes/overwrites the ref -> hash
func (rc referenceStore) Put(ref name.Reference, hash v1.Hash) error {
	p := rc.pathForRef(ref)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	// atomic write
	return os.WriteFile(p, []byte(hash.String()), 0644)
}

func (rc referenceStore) pathForRef(ref name.Reference) string {
	return filepath.Join(string(rc), ref.Context().Name(), ref.Identifier())
}
