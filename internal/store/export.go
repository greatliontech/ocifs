package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"

	"github.com/greatliontech/ocifs/internal/export"
	"github.com/greatliontech/ocifs/internal/layer"
)

// Export materializes img into the store-managed export cache and
// returns the export root. Entries are keyed by the digest of the
// manifest actually materialized — the platform-selected child — and
// images are immutable, so an existing entry is served as-is without
// re-materialization; cached exports are shared and read-only
// (REQ-export-cache).
func (s *Store) Export(ctx context.Context, img *Image) (string, error) {
	h := img.Hash()
	final := filepath.Join(s.path, "exports", h.Algorithm, h.Hex)
	if _, err := os.Stat(final); err == nil {
		return final, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	view, err := img.Unify()
	if err != nil {
		return "", err
	}
	if err := s.materializeAt(ctx, view, final, img.Hash()); err != nil {
		// Two exporters of one digest race benignly: the loser's
		// rename fails against the winner's complete directory, and
		// immutability makes the winner's tree the same tree.
		if _, statErr := os.Stat(final); statErr == nil {
			return final, nil
		}
		return "", err
	}
	return final, nil
}

// ExportTo materializes view into targetDir, which must not exist:
// the completed tree is renamed into place without replacing caller
// state, and a target observable at its path is complete. The
// existence check up front spares a doomed materialization; the
// rename's own guard still governs the race window.
func (s *Store) ExportTo(ctx context.Context, view *layer.View, targetDir string, pin v1.Hash) error {
	if _, err := os.Lstat(targetDir); err == nil {
		return fmt.Errorf("export target %s already exists", targetDir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.materializeAt(ctx, view, targetDir, pin)
}

// materializeAt runs the materializer in a temporary sibling of
// final and renames it into place, so a directory observable at the
// final path is complete — a crash leaves no directory or a stale
// temporary, never a partial tree (REQ-export-atomic). os.Rename
// refuses an existing target directory via its lstat guard
// (long-standing os semantics, not atomic: a bare kernel rename
// could still replace an empty directory racing into the window —
// benign for the digest-keyed cache, whose racers carry identical
// trees).
func (s *Store) materializeAt(ctx context.Context, view *layer.View, final string, pin v1.Hash) error {
	parent := filepath.Dir(final)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	uid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	tmp := filepath.Join(parent, ".export-"+uid.String())
	if err := os.Mkdir(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	// The op row pins the source image (a collection root while
	// live) and owns the temporary — sweep-exempt while this
	// process lives, debris when dead (REQ-store-gc-roots).
	opID, err := s.BeginOp(ctx, opKindExport, []v1.Hash{pin}, []string{tmp})
	if err != nil {
		return err
	}
	defer func() { _ = s.EndOp(context.WithoutCancel(ctx), opID) }()
	root, err := os.OpenRoot(tmp)
	if err != nil {
		return err
	}
	if err := export.Materialize(ctx, root, view, s.BlobPath); err != nil {
		root.Close()
		return fmt.Errorf("export: %w", err)
	}
	if err := root.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}
