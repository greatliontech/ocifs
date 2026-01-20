package store

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type referenceStore string

var emptyHash = v1.Hash{}

// Get returns (digest, true, nil) if present; ("", false, nil) if missing.
func (rc referenceStore) Get(ref name.Reference) (v1.Hash, bool, error) {
	path := rc.pathForRef(ref)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			slog.Debug("ref not found in cache", "ref", ref.String())
			return emptyHash, false, nil
		}
		return emptyHash, false, fmt.Errorf("read ref %s: %w", ref.String(), err)
	}
	h, err := v1.NewHash(string(b))
	if err != nil {
		return emptyHash, false, fmt.Errorf("invalid hash in ref store %q: %w", ref, err)
	}
	slog.Debug("ref found in cache", "ref", ref.String(), "digest", h.String())
	return h, true, nil
}

// Put writes/overwrites the ref -> hash
func (rc referenceStore) Put(ref name.Reference, hash v1.Hash) error {
	p := rc.pathForRef(ref)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return fmt.Errorf("create ref dir: %w", err)
	}

	// Atomic write: write to temp then rename
	tmpPath := p + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(hash.String()), 0644); err != nil {
		return fmt.Errorf("write ref: %w", err)
	}
	if err := os.Rename(tmpPath, p); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename ref: %w", err)
	}

	slog.Debug("ref stored", "ref", ref.String(), "digest", hash.String())
	return nil
}

func (rc referenceStore) pathForRef(ref name.Reference) string {
	return filepath.Join(string(rc), ref.Context().Name(), ref.Identifier())
}
