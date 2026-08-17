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
	db *gmdb.DB
}

// Keyspace names are wire contract (REQ-store-bookkeeping). Later
// plan chunks add layeridx, mounts, uppers, localimages, ops, gc.
const ksRefs = "refs"

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
		_, err := tx.CreateKeyspaceIfNotExists(ksRefs)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("bookkeeping keyspaces: %w", err)
	}
	return &bookkeeping{db: db}, nil
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
