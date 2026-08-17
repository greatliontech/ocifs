package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
)

// bookkeeping is the store's gmdb database (docs/specs/store.md
// REQ-store-bookkeeping): every record only ocifs interprets, keys
// and values byte-exact, transactional and cross-process. The
// database lives under bookkeeping/ and is owned entirely by gmdb;
// ocifs never touches the files directly.
type bookkeeping struct {
	db  *gmdb.DB
	dir string // the bookkeeping/ tier, for concurrent test handles
}

// Keyspace names are wire contract (REQ-store-bookkeeping). Later
// plan chunks add layeridx, mounts, uppers, localimages, ops, gc.
const (
	ksRefs        = "refs"
	ksLayerIdx    = "layeridx"
	ksMounts      = "mounts"
	ksUppers      = "uppers"
	ksLocalImages = "localimages"
)

var emptyHash = v1.Hash{}

// refSep joins reference components in a refs key: a byte no
// registry, repository, or identifier can carry, so the layout is
// injective without any escaping.
const refSep = 0x00

func openBookkeeping(root string) (*bookkeeping, error) {
	dir := filepath.Join(root, "bookkeeping")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Construction is synchronous with no caller deadline surface;
	// gmdb's open-time lock waits are short by its own contract.
	ctx := context.Background()
	db, err := gmdb.Open(ctx, filepath.Join(dir, "db"), gmdb.Options{})
	if err != nil {
		return nil, fmt.Errorf("bookkeeping database: %w", err)
	}
	if err := db.Update(ctx, func(tx *gmdb.Tx) error {
		for _, name := range []string{ksRefs, ksLayerIdx, ksMounts, ksUppers, ksLocalImages} {
			if _, err := tx.CreateKeyspaceIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("bookkeeping keyspaces: %w", err)
	}
	return &bookkeeping{db: db, dir: dir}, nil
}

func (b *bookkeeping) Close() error { return b.db.Close() }

// refKey lays a reference out as
// registry 0x00 repository 0x00 identifier — the registry
// lowercased (DNS names are case-insensitive), everything else
// byte-exact (REQ-store-bookkeeping).
func refKey(ref name.Reference) []byte {
	var k bytes.Buffer
	k.WriteString(strings.ToLower(ref.Context().RegistryStr()))
	k.WriteByte(refSep)
	k.WriteString(ref.Context().RepositoryStr())
	k.WriteByte(refSep)
	k.WriteString(ref.Identifier())
	return k.Bytes()
}

// RefGet returns (digest, true, nil) if present; (zero, false, nil)
// if missing. A per-operation read transaction, per
// REQ-store-single-writer's long-lived-reader rule.
func (b *bookkeeping) RefGet(ctx context.Context, ref name.Reference) (v1.Hash, bool, error) {
	var h v1.Hash
	found := false
	err := b.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksRefs)
		if err != nil {
			return err
		}
		v, err := ks.Get(refKey(ref))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		// The value is borrowed until transaction close; parse
		// inside the transaction.
		parsed, err := v1.NewHash(string(v))
		if err != nil {
			return fmt.Errorf("invalid hash in refs row %q: %w", ref, err)
		}
		h, found = parsed, true
		return nil
	})
	return h, found, err
}

// RefPut publishes the ref → digest row, written last in ingest
// (REQ-store-ingest-order): the transactional commit is never
// observable half-written. Durability ORDER against the content
// renames before it is not guaranteed — a power cut can persist
// the row while an un-journaled rename is lost — and does not need
// to be: a row over a missing blob is the absence self-heal
// re-derives or re-fetches (REQ-store-self-heal).
func (b *bookkeeping) RefPut(ctx context.Context, ref name.Reference, hash v1.Hash) error {
	return b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksRefs)
		if err != nil {
			return err
		}
		return ks.Put(refKey(ref), []byte(hash.String()))
	})
}

// UpperBind records the named upper's base binding transactionally
// without replacement (writable.md REQ-writable-base-binding): the
// first binder wins, and every caller gets the recorded base back
// for validation.
func (b *bookkeeping) UpperBind(ctx context.Context, name string, base v1.Hash) (v1.Hash, error) {
	var recorded v1.Hash
	err := b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksUppers)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(name))
		if err == nil {
			recorded, err = v1.NewHash(string(v))
			return err
		}
		if !errors.Is(err, gmdb.ErrNotFound) {
			return err
		}
		recorded = base
		return ks.Insert([]byte(name), []byte(base.String()))
	})
	return recorded, err
}

// UpperBinding reads the named upper's base binding; a missing row
// is os.ErrNotExist.
func (b *bookkeeping) UpperBinding(ctx context.Context, name string) (v1.Hash, error) {
	var recorded v1.Hash
	err := b.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksUppers)
		if err != nil {
			return err
		}
		v, err := ks.Get([]byte(name))
		if errors.Is(err, gmdb.ErrNotFound) {
			return fmt.Errorf("upper %q has no base binding: %w", name, os.ErrNotExist)
		}
		if err != nil {
			return err
		}
		recorded, err = v1.NewHash(string(v))
		return err
	})
	return recorded, err
}

const localImageVersion = 1

// LocalImagePut records a committed image — the root that keeps it
// reachable (REQ-store-gc-roots) until explicitly removed.
func (b *bookkeeping) LocalImagePut(ctx context.Context, manifest v1.Hash, createdUnix int64) error {
	return b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksLocalImages)
		if err != nil {
			return err
		}
		w := &binWriter{}
		w.byteVal(localImageVersion)
		w.i64(createdUnix)
		return ks.Put(layerKey(manifest), w.buf)
	})
}

// LocalImagePresent reports whether a committed image's root row
// exists.
func (b *bookkeeping) LocalImagePresent(ctx context.Context, manifest v1.Hash) (bool, error) {
	present := false
	err := b.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksLocalImages)
		if err != nil {
			return err
		}
		v, err := ks.Get(layerKey(manifest))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		// Version discipline: a foreign version is the absent-row
		// case (REQ-store-bookkeeping).
		present = len(v) > 0 && v[0] == localImageVersion
		return nil
	})
	return present, err
}
