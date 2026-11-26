package store

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"

	"github.com/greatliontech/ocifs/internal/export"
	"github.com/greatliontech/ocifs/internal/layer"
)

// ErrIntegrity reports unrecoverable corruption in content-addressed
// store blobs detected during verified export.
var ErrIntegrity = errors.New("ocifs: store content integrity failure")

// IntegrityError identifies the corrupted content digest and on-disk
// path when a content-CAS blob fails SHA-256 verification.
type IntegrityError struct {
	Digest v1.Hash
	Actual string
	Path   string
	Detail string
}

func (e *IntegrityError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("ocifs: integrity check failed for %s (%s): %s", e.Digest, e.Path, e.Detail)
	}
	return fmt.Sprintf("ocifs: integrity check failed for %s (%s): got sha256:%s", e.Digest, e.Path, e.Actual)
}

func (e *IntegrityError) Is(target error) bool {
	return target == ErrIntegrity
}

// Export materializes img into the store-managed export cache and
// returns the export root. Entries are keyed by the digest of the
// manifest actually materialized — the platform-selected child — and
// images are immutable, so by default an existing entry is served
// as-is without re-materialization (REQ-export-cache). When the store
// was constructed with VerifyCachedExports, Export delegates to
// ExportVerified.
func (s *Store) Export(ctx context.Context, img *Image) (string, error) {
	if s.verifyCachedExports {
		return s.ExportVerified(ctx, img)
	}
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

// verifyExistingExport checks whether final exists and passes
// export.Verify: present && clean means final is a pristine cache hit;
// present && !clean && err == nil means final is dirtied and eligible
// for condemnation/replacement; a non-nil err is a context
// cancellation or filesystem stat failure that must propagate without
// condemning final.
func verifyExistingExport(ctx context.Context, final string, view *layer.View) (present, clean bool, err error) {
	if _, statErr := os.Lstat(final); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return false, false, nil
		}
		return false, false, statErr
	}
	root, openErr := os.OpenRoot(final)
	if openErr != nil {
		if errors.Is(openErr, fs.ErrNotExist) {
			return false, false, nil
		}
		return true, false, nil
	}
	vErr := export.Verify(ctx, root, view)
	_ = root.Close()
	if vErr == nil {
		return true, true, nil
	}
	if ctx.Err() != nil && errors.Is(vErr, ctx.Err()) {
		return true, false, ctx.Err()
	}
	return true, false, nil
}

// ExportVerified materializes or verifies img's store-managed export
// cache entry against img.Unify():
//   - If exports/<algo>/<hex> already exists and passes export.Verify,
//     it is returned in place with zero writes.
//   - If exports/<algo>/<hex> is absent or dirtied (modified, missing,
//     or unexpected entries), all referenced content-CAS blobs are
//     SHA-256 verified first (failing closed with *IntegrityError if
//     any CAS blob's bytes do not hash to its key), a clean sibling
//     tree is materialized, any still-dirtied directory at final is
//     moved to an ops-owned .export-condemned-<uuid> sibling before
//     removal (never disturbing a pristine tree published by a
//     concurrent racer), and the clean tree is renamed into place.
func (s *Store) ExportVerified(ctx context.Context, img *Image) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	view, err := img.Unify()
	if err != nil {
		return "", err
	}
	h := img.Hash()
	final := filepath.Join(s.path, "exports", h.Algorithm, h.Hex)

	presentInitial, cleanInitial, err := verifyExistingExport(ctx, final, view)
	if err != nil {
		return "", err
	}
	if presentInitial && cleanInitial {
		return final, nil
	}
	dirtyInitial := presentInitial && !cleanInitial

	parent := filepath.Dir(final)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	uid1, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	uid2, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	tmp := filepath.Join(parent, ".export-"+uid1.String())
	condemned := filepath.Join(parent, ".export-condemned-"+uid2.String())

	opClaim, err := s.BeginOp(ctx, opKindExport, []v1.Hash{h}, []string{tmp, condemned})
	if err != nil {
		return "", err
	}
	defer func() { _ = s.EndOp(context.WithoutCancel(ctx), opClaim) }()
	defer func() {
		_ = forceRemoveTree(tmp)
		_ = forceRemoveTree(condemned)
	}()

	if err := s.verifyCASBlobs(ctx, view); err != nil {
		// Condemn final ONLY if it was already dirty AND CAS failed
		// SHA-256 integrity (never on ctx cancellation or transient
		// errors, and never if a concurrent racer healed final).
		if dirtyInitial && errors.Is(err, ErrIntegrity) {
			if pNow, cleanNow, verr := verifyExistingExport(context.WithoutCancel(ctx), final, view); verr == nil && pNow && !cleanNow {
				_ = os.Rename(final, condemned)
			}
		}
		return "", err
	}

	if err := os.Mkdir(tmp, 0o755); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(tmp)
	if err != nil {
		return "", err
	}
	if err := export.Materialize(ctx, root, view, s.BlobPath); err != nil {
		_ = root.Close()
		return "", fmt.Errorf("export: %w", err)
	}
	if err := root.Close(); err != nil {
		return "", err
	}

	// Fast publication when final was absent: if a concurrent racer
	// won os.Rename(tmp, final) ahead of us, verify the winner's tree
	// in place without ever condemning or unlinking it.
	if !dirtyInitial {
		if err := os.Rename(tmp, final); err == nil {
			return final, nil
		}
	}

	// Either final was initially dirty or a racer published at final:
	// verify final first — if a concurrent racer already healed/published
	// a pristine tree, serve it untouched!
	presentNow, cleanNow, verr := verifyExistingExport(ctx, final, view)
	if verr != nil {
		return "", verr
	}
	if presentNow && cleanNow {
		return final, nil
	}
	if presentNow {
		if rerr := os.Rename(final, condemned); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
			return "", rerr
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		if pWin, cleanWin, verr := verifyExistingExport(ctx, final, view); verr == nil && pWin && cleanWin {
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
	return s.exportTo(ctx, view, targetDir, pin, s.verifyCachedExports)
}

// ExportVerifiedTo materializes view into targetDir after verifying
// that every referenced content-CAS blob hashes to its digest.
func (s *Store) ExportVerifiedTo(ctx context.Context, view *layer.View, targetDir string, pin v1.Hash) error {
	return s.exportTo(ctx, view, targetDir, pin, true)
}

func (s *Store) exportTo(ctx context.Context, view *layer.View, targetDir string, pin v1.Hash, verify bool) error {
	if _, err := os.Lstat(targetDir); err == nil {
		return fmt.Errorf("export target %s already exists", targetDir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if verify {
		if err := s.verifyCASBlobs(ctx, view); err != nil {
			return err
		}
	}
	return s.materializeAt(ctx, view, targetDir, pin)
}

func (s *Store) verifyCASBlobs(ctx context.Context, view *layer.View) error {
	entries := view.Entries()
	checked := make(map[v1.Hash]struct{}, len(entries))
	for i := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		e := &entries[i]
		if e.Header.Typeflag != tar.TypeReg && e.Header.Typeflag != tar.TypeLink {
			continue
		}
		if _, ok := checked[e.Digest]; ok {
			continue
		}
		if err := verifyCASFile(ctx, s.BlobPath(e.Digest), e.Digest); err != nil {
			return err
		}
		checked[e.Digest] = struct{}{}
	}
	return nil
}

func verifyCASFile(ctx context.Context, path string, want v1.Hash) error {
	if want.Algorithm != "sha256" || want.Hex == "" {
		return &IntegrityError{Digest: want, Path: path, Detail: fmt.Sprintf("missing or unsupported digest %q", want)}
	}
	f, err := os.Open(path)
	if err != nil {
		// Missing blob (e.g. collected out from under an unpinned Image
		// handle per REQ-api-export) surfaces as the underlying open
		// error rather than an IntegrityError.
		return fmt.Errorf("content %s: %w", want, err)
	}
	defer f.Close()

	h := sha256.New()
	const chunk = 32 << 20
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.CopyN(h, f, chunk); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("content %s: %w", want, err)
		}
	}
	gotHex := hex.EncodeToString(h.Sum(nil))
	if gotHex != want.Hex {
		return &IntegrityError{Digest: want, Actual: gotHex, Path: path}
	}
	return nil
}

// materializeAt runs the materializer in a temporary sibling of
// final and renames it into place, so a directory observable at the
// final path is complete — a crash leaves no directory or a stale
// temporary, never a partial tree (REQ-export-atomic).
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
	// The op row pins the source image (a collection root while
	// live) and owns the temporary — registered BEFORE the
	// directory exists, so no window shows an unowned temporary to
	// a concurrent debris sweep (REQ-store-gc-collect: every window
	// sits inside a fence or a live ops row).
	opClaim, err := s.BeginOp(ctx, opKindExport, []v1.Hash{pin}, []string{tmp})
	if err != nil {
		return err
	}
	defer func() { _ = s.EndOp(context.WithoutCancel(ctx), opClaim) }()
	if err := os.Mkdir(tmp, 0o755); err != nil {
		return err
	}
	defer func() { _ = forceRemoveTree(tmp) }()
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
