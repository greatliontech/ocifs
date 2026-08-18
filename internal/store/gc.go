package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"

	"github.com/greatliontech/ocifs/internal/atomicfile"
)

// Collection (docs/specs/store.md REQ-store-gc-roots/safe/collect):
// mark from the transactional root snapshot, walk the OCI graph,
// condemn what is unreachable and past the retention grace inside
// the sweeper's write transaction, then delete files and rows. The
// whole sweep runs under the ingest lease, so every leased writer
// (ingest, commit) is excluded structurally; unleased root
// publishes consult the condemned set (REQ-store-gc-safe).

// gc keyspace rows. First-seen rows ground the retention grace:
// losing them re-derives conservatively later. Condemned rows carry
// the sweeping op's id; a dead sweeper's condemned rows bind
// nobody.
const (
	gcFirstSeenPrefix = "seen\x00"
	gcCondemnedPrefix = "cond\x00"
)

// GCResult reports a collection pass (api.md REQ-api-gc).
type GCResult struct {
	// CollectedBlobs lists removed digests (oci and content-CAS
	// keys), CollectedPaths removed files and directories outside
	// the digest keyspaces (export temporaries, mount state).
	CollectedBlobs []v1.Hash
	CollectedPaths []string
	// ReclaimedMounts lists dead mount ids reclaimed.
	ReclaimedMounts []string
	// ForeignVersionRows lists LIVE mounts rows written by a
	// different ocifs version: their liveness is judged by lock
	// like any row, but a live one's image cannot be read by this
	// binary, so image-tier collection halts visibly — the one
	// irreducible foreign-version conservatism
	// (REQ-store-bookkeeping). Dead foreign rows reclaim normally
	// and are never listed.
	ForeignVersionRows []string
	// Deferred lists items whose deletion failed this pass; they
	// stay condemnation-eligible and retry next pass.
	Deferred []string
}

// CollectOpts configures one pass.
type CollectOpts struct {
	// Grace overrides the store's configured retention grace;
	// negative means ignore the grace entirely (the wipe-now
	// operator intent).
	Grace time.Duration
}

// Collect runs one full collection pass.
func (s *Store) Collect(ctx context.Context, opts CollectOpts) (*GCResult, error) {
	res := &GCResult{}

	// The sweep is itself an operation, and it runs under the
	// ingest lease: leased writers and the sweep exclude each other
	// structurally (REQ-store-gc-safe).
	tok, err := s.AcquireIngestLease(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.ReleaseIngestLease(context.WithoutCancel(ctx), tok) }()
	sweepOp, err := s.BeginOp(ctx, "sweep", nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.EndOp(context.WithoutCancel(ctx), sweepOp) }()

	// Debris first: dead mounts (rows + state), dead ops rows and
	// their recorded temporaries wherever they live
	// (REQ-store-gc-collect).
	reclaimed, err := s.ReclaimDeadMounts(ctx)
	if err != nil {
		return nil, err
	}
	res.ReclaimedMounts = reclaimed
	if err := s.reclaimDeadOps(ctx, res); err != nil {
		return nil, err
	}
	s.sweepLockTier(ctx)

	// Mark: the transactional root snapshot and the reachable set.
	roots, foreignRows, err := s.rootSet(ctx)
	if err != nil {
		return nil, err
	}
	if len(foreignRows) > 0 {
		// A foreign-version mounts row makes the mark unjudgeable:
		// its mount may be live and its image unreadable to this
		// binary. Debris was reclaimed above; image-tier collection
		// stops here, visibly (REQ-store-gc-safe: false-dead — and
		// false-unrooted — never destroys served content).
		res.ForeignVersionRows = foreignRows
		return res, nil
	}
	reachable, err := s.reachableFrom(ctx, roots)
	if err != nil {
		return nil, err
	}

	// Candidates: everything on disk or in rows the reachable set
	// does not name.
	candidates, err := s.unreachableItems(ctx, reachable)
	if err != nil {
		return nil, err
	}

	// gc-keyspace hygiene: first-seen rows for items that became
	// reachable again are dropped — a later re-orphaning starts its
	// grace fresh, and the keyspace stays bounded — and condemned
	// rows of dead sweepers are debris (REQ-store-gc-roots).
	if err := s.gcRowHygiene(ctx, candidates); err != nil {
		return nil, err
	}

	// Grace: an unreachable item younger than the grace is retained
	// (its first-seen row recorded); older items proceed to
	// condemnation. Grace is pure retention policy —
	// REQ-store-gc-collect.
	due, err := s.applyGrace(ctx, candidates, opts.Grace)
	if err != nil {
		return nil, err
	}
	if len(due) == 0 {
		return res, nil
	}

	// Condemn inside one write transaction, re-verifying roots
	// (REQ-store-gc-safe); then delete files; then clear rows.
	condemned, err := s.condemn(ctx, sweepOp, due)
	if err != nil {
		return nil, err
	}
	for _, item := range condemned {
		if err := s.deleteItem(ctx, item, res); err != nil {
			// Deferral, visibly: clear the condemned row, keep the
			// first-seen row — retried next pass.
			res.Deferred = append(res.Deferred, item.key())
			_ = s.clearCondemned(ctx, item.key())
			continue
		}
		_ = s.clearCondemned(ctx, item.key())
		_ = s.clearFirstSeen(ctx, item.key())
	}
	return res, nil
}

// gcItem is one collectible: a digest in a tier, or a path.
type gcItem struct {
	// digest items: tier is "oci" or "cas" or "layeridx" or
	// "export"; path items: tier is "path", path set.
	tier   string
	digest v1.Hash
	path   string
}

func (it gcItem) key() string {
	if it.tier == "path" {
		return "path\x00" + it.path
	}
	return it.tier + "\x00" + it.digest.Algorithm + "\x00" + it.digest.Hex
}

// rootSet snapshots every root transactionally
// (REQ-store-gc-roots).
// markedMount is one mounts row awaiting its liveness verdict —
// snapshotted transactionally, judged by lock OUTSIDE the read
// transaction (a reader slot is a finite coordination resource,
// and a read transaction should not create lock files).
type markedMount struct {
	id      string
	image   v1.Hash
	foreign bool
}

func (s *Store) rootSet(ctx context.Context) ([]v1.Hash, []string, error) {
	var roots []v1.Hash
	var foreignRows []string
	var mountRows []markedMount
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		refs, err := rtx.OpenKeyspaceReadOnly(ksRefs)
		if err != nil {
			return err
		}
		for _, v := range refs.All() {
			h, err := v1.NewHash(string(v))
			if err == nil {
				roots = append(roots, h)
			}
		}
		local, err := rtx.OpenKeyspaceReadOnly(ksLocalImages)
		if err != nil {
			return err
		}
		for k := range local.All() {
			if h, ok := digestFromKey(k); ok {
				roots = append(roots, h)
			}
		}
		mounts, err := rtx.OpenKeyspaceReadOnly(ksMounts)
		if err != nil {
			return err
		}
		for k, v := range mounts.All() {
			rec, derr := decodeMountRecord(v)
			mountRows = append(mountRows, markedMount{
				id: string(k), image: rec.Image, foreign: derr != nil,
			})
		}
		uppers, err := rtx.OpenKeyspaceReadOnly(ksUppers)
		if err != nil {
			return err
		}
		for _, v := range uppers.All() {
			h, err := v1.NewHash(string(v))
			if err == nil {
				roots = append(roots, h)
			}
		}
		ops, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for _, v := range ops.All() {
			rec, derr := decodeOpRecord(v)
			if derr != nil || rec.Owner.Dead() {
				continue
			}
			roots = append(roots, rec.Pins...)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	// Liveness by held lock, judgeable for every row —
	// foreign-version rows included (the lock has no format). A
	// dead row is no root (the judge releases without unlink,
	// leaving disposal to the sweep); held and UNDECIDED both read
	// live — for a readable row its image roots, for a foreign row
	// it halts image-tier collection visibly
	// (REQ-store-bookkeeping's one irreducible foreign-version
	// conservatism).
	for _, m := range mountRows {
		if !s.mountClaimHeld(m.id) {
			continue
		}
		if m.foreign {
			foreignRows = append(foreignRows, m.id)
			continue
		}
		roots = append(roots, m.image)
	}
	return roots, foreignRows, nil
}

func digestFromKey(k []byte) (v1.Hash, bool) {
	parts := strings.SplitN(string(k), "\x00", 2)
	if len(parts) != 2 {
		return v1.Hash{}, false
	}
	return v1.Hash{Algorithm: parts[0], Hex: parts[1]}, true
}

// reachableFrom walks the OCI graph: index → child manifests →
// config and layers → layer indexes → content-CAS entries
// (REQ-store-gc-roots). Unreadable pieces terminate their branch —
// what cannot be enumerated cannot mark, and self-heal territory
// stays collectable only when unrooted.
func (s *Store) reachableFrom(ctx context.Context, roots []v1.Hash) (map[string]bool, error) {
	reachable := map[string]bool{}
	var walk func(h v1.Hash)
	walk = func(h v1.Hash) {
		key := h.Algorithm + "\x00" + h.Hex
		if reachable["oci\x00"+key] {
			return
		}
		reachable["oci\x00"+key] = true
		raw, err := os.ReadFile(s.ociBlobPath(h))
		if err != nil {
			return
		}
		// Index detection must not depend on the OPTIONAL mediaType
		// field (OCI 1.0 indexes may omit it in the body): a
		// document with a manifests array and no config is an
		// index; misreading one as a manifest would strand a live
		// root's children unmarked and collect them.
		var probe struct {
			MediaType string          `json:"mediaType"`
			Config    json.RawMessage `json:"config"`
			Manifests json.RawMessage `json:"manifests"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			return
		}
		if len(probe.Manifests) > 0 && len(probe.Config) == 0 {
			var idx v1.IndexManifest
			if err := json.Unmarshal(raw, &idx); err == nil {
				for _, d := range idx.Manifests {
					walk(d.Digest)
				}
			}
			return
		}
		var m v1.Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return
		}
		if m.Config.Digest.Hex != "" {
			reachable["oci\x00"+m.Config.Digest.Algorithm+"\x00"+m.Config.Digest.Hex] = true
		}
		for _, ld := range m.Layers {
			lkey := ld.Digest.Algorithm + "\x00" + ld.Digest.Hex
			reachable["oci\x00"+lkey] = true
			reachable["layeridx\x00"+lkey] = true
			l, err := s.bk.LayerIdxGet(ctx, ld.Digest)
			if err != nil {
				continue
			}
			for i := range l {
				if l[i].Digest != (v1.Hash{}) {
					reachable["cas\x00"+l[i].Digest.Algorithm+"\x00"+l[i].Digest.Hex] = true
				}
			}
		}
		// The manifest digest also keys the exports tier.
		reachable["export\x00"+key] = true
	}
	for _, r := range roots {
		walk(r)
	}
	return reachable, nil
}

// unreachableItems enumerates disk and row state the reachable set
// does not name (REQ-store-gc-roots).
func (s *Store) unreachableItems(ctx context.Context, reachable map[string]bool) ([]gcItem, error) {
	var items []gcItem
	// oci blobs.
	if err := walkDigestTier(filepath.Join(s.path, "oci", "blobs"), func(h v1.Hash) {
		if !reachable["oci\x00"+h.Algorithm+"\x00"+h.Hex] {
			items = append(items, gcItem{tier: "oci", digest: h})
		}
	}); err != nil {
		return nil, err
	}
	// content CAS.
	if err := walkDigestTier(filepath.Join(s.path, "blobs"), func(h v1.Hash) {
		if !reachable["cas\x00"+h.Algorithm+"\x00"+h.Hex] {
			items = append(items, gcItem{tier: "cas", digest: h})
		}
	}); err != nil {
		return nil, err
	}
	// exports tier + stray temporaries.
	exportsRoot := filepath.Join(s.path, "exports")
	if algos, err := os.ReadDir(exportsRoot); err == nil {
		for _, a := range algos {
			entries, err := os.ReadDir(filepath.Join(exportsRoot, a.Name()))
			if err != nil {
				continue
			}
			for _, e := range entries {
				p := filepath.Join(exportsRoot, a.Name(), e.Name())
				if strings.HasPrefix(e.Name(), ".export-") {
					if !s.tempOwnedByLiveOp(ctx, p) {
						items = append(items, gcItem{tier: "path", path: p})
					}
					continue
				}
				h := v1.Hash{Algorithm: a.Name(), Hex: e.Name()}
				if !reachable["export\x00"+h.Algorithm+"\x00"+h.Hex] {
					items = append(items, gcItem{tier: "path", path: p})
				}
			}
		}
	}
	// layeridx rows.
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksLayerIdx)
		if err != nil {
			return err
		}
		for k := range ks.All() {
			if h, ok := digestFromKey(k); ok {
				if !reachable["layeridx\x00"+h.Algorithm+"\x00"+h.Hex] {
					items = append(items, gcItem{tier: "layeridx", digest: h})
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Write temporaries in the content tiers: the sweep holds the
	// ingest lease, so no live writer exists — every dot-prefixed
	// temporary here is a crashed write's orphan.
	for _, tier := range []string{filepath.Join(s.path, "oci", "blobs"), filepath.Join(s.path, "blobs")} {
		algos, err := os.ReadDir(tier)
		if err != nil {
			continue
		}
		for _, a := range algos {
			// The CAS writes its temporaries at the TIER ROOT
			// (cas.Put creates in blobs/ and renames into
			// blobs/<algo>/), so orphans appear both here and one
			// level down.
			if !a.IsDir() && strings.HasPrefix(a.Name(), ".") {
				items = append(items, gcItem{tier: "path", path: filepath.Join(tier, a.Name())})
				continue
			}
			entries, err := os.ReadDir(filepath.Join(tier, a.Name()))
			if err != nil {
				continue
			}
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), ".") {
					items = append(items, gcItem{tier: "path", path: filepath.Join(tier, a.Name(), e.Name())})
				}
			}
		}
	}
	// Rowless mount scaffolding — skipping any id whose claim is
	// held: registration acquires the lock BEFORE its row exists
	// (lock-before-row), so a mid-registration state directory is
	// rowless yet claimed, and a grace-ignoring sweep must not eat
	// it from under the registrant.
	if entries, err := os.ReadDir(filepath.Join(s.path, "mounts")); err == nil {
		for _, e := range entries {
			if s.mountClaimHeld(e.Name()) {
				continue
			}
			if _, err := s.bk.MountGet(ctx, e.Name()); errors.Is(err, os.ErrNotExist) {
				items = append(items, gcItem{tier: "path", path: filepath.Join(s.path, "mounts", e.Name())})
			}
		}
	}
	return items, nil
}

func walkDigestTier(root string, fn func(v1.Hash)) error {
	algos, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, a := range algos {
		entries, err := os.ReadDir(filepath.Join(root, a.Name()))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
				fn(v1.Hash{Algorithm: a.Name(), Hex: e.Name()})
			}
		}
	}
	return nil
}

// tempOwnedByLiveOp reports whether any live ops row records p.
func (s *Store) tempOwnedByLiveOp(ctx context.Context, p string) bool {
	owned := false
	_ = s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for _, v := range ks.All() {
			rec, derr := decodeOpRecord(v)
			if derr != nil || rec.Owner.Dead() {
				continue
			}
			for _, t := range rec.Temps {
				if t == p {
					owned = true
					return nil
				}
			}
		}
		return nil
	})
	return owned
}

// applyGrace records first-seen times for new candidates and
// returns those past the grace (REQ-store-gc-collect). A negative
// grace collects everything now.
func (s *Store) applyGrace(ctx context.Context, candidates []gcItem, grace time.Duration) ([]gcItem, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	now := time.Now().Unix()
	var due []gcItem
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		for _, c := range candidates {
			// Any non-positive grace collects on first sight — the
			// zero-configured store and the ignore-grace override
			// behave identically by design.
			if grace <= 0 {
				due = append(due, c)
				continue
			}
			key := []byte(gcFirstSeenPrefix + c.key())
			v, err := ks.Get(key)
			if errors.Is(err, gmdb.ErrNotFound) {
				w := &binWriter{}
				w.i64(now)
				if err := ks.Put(key, w.buf); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			r := &binReader{buf: v}
			seen, derr := r.i64()
			if derr != nil {
				// Unreadable first-seen: re-derive conservatively
				// later (REQ-store-bookkeeping's gc note).
				w := &binWriter{}
				w.i64(now)
				if err := ks.Put(key, w.buf); err != nil {
					return err
				}
				continue
			}
			if time.Duration(now-seen)*time.Second >= grace {
				due = append(due, c)
			}
		}
		return nil
	})
	return due, err
}

// condemn re-verifies roots and writes the condemned set inside one
// write transaction (REQ-store-gc-safe). Items reachable under the
// fresh snapshot are dropped.
func (s *Store) condemn(ctx context.Context, sweepOp string, due []gcItem) ([]gcItem, error) {
	roots, foreignRows, err := s.rootSet(ctx)
	if err != nil {
		return nil, err
	}
	if len(foreignRows) > 0 {
		return nil, nil // unjudgeable: condemn nothing
	}
	reachable, err := s.reachableFrom(ctx, roots)
	if err != nil {
		return nil, err
	}
	var out []gcItem
	err = s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		for _, c := range due {
			if c.tier != "path" && reachable[c.tier+"\x00"+c.digest.Algorithm+"\x00"+c.digest.Hex] {
				continue
			}
			if err := ks.Put([]byte(gcCondemnedPrefix+c.key()), []byte(sweepOp)); err != nil {
				return err
			}
			out = append(out, c)
		}
		return nil
	})
	return out, err
}

// condemnedByLiveSweep reports whether the digest is condemned by a
// LIVE sweeper — the consult every unleased root-publishing write
// runs (REQ-store-gc-safe).
func (s *Store) condemnedByLiveSweep(rtxCtx context.Context, tx *gmdb.Tx, h v1.Hash) (bool, error) {
	gcks, err := tx.OpenKeyspace(ksGC)
	if err != nil {
		return false, err
	}
	ops, err := tx.OpenKeyspace(ksOps)
	if err != nil {
		return false, err
	}
	for _, tier := range []string{"oci", "cas", "layeridx", "export"} {
		v, err := gcks.Get([]byte(gcCondemnedPrefix + tier + "\x00" + h.Algorithm + "\x00" + h.Hex))
		if errors.Is(err, gmdb.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		ov, err := ops.Get(v)
		if errors.Is(err, gmdb.ErrNotFound) {
			continue // sweeper's op row gone: binds nobody
		}
		if err != nil {
			return false, err
		}
		rec, derr := decodeOpRecord(ov)
		if derr != nil || rec.Owner.Dead() {
			continue // dead sweeper binds nobody
		}
		return true, nil
	}
	return false, nil
}

func (s *Store) deleteItem(ctx context.Context, it gcItem, res *GCResult) error {
	switch it.tier {
	case "oci":
		if err := os.Remove(s.ociBlobPath(it.digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := s.dropDescriptor(it.digest); err != nil {
			return err
		}
		res.CollectedBlobs = append(res.CollectedBlobs, it.digest)
	case "cas":
		if err := os.Remove(s.cas.Path(it.digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		res.CollectedBlobs = append(res.CollectedBlobs, it.digest)
	case "layeridx":
		if err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
			ks, err := tx.OpenKeyspace(ksLayerIdx)
			if err != nil {
				return err
			}
			err = ks.Delete(layerKey(it.digest))
			if errors.Is(err, gmdb.ErrNotFound) {
				return nil
			}
			return err
		}); err != nil {
			return err
		}
		res.CollectedBlobs = append(res.CollectedBlobs, it.digest)
	case "path":
		if err := forceRemoveTree(it.path); err != nil {
			return err
		}
		res.CollectedPaths = append(res.CollectedPaths, it.path)
	}
	return nil
}

// forceRemoveTree removes a tree whose recorded modes may make
// directories untraversable (an exported image's read-only dirs): a
// best-effort top-down chmod makes each directory deletable, then
// RemoveAll finishes.
func forceRemoveTree(p string) error {
	_ = filepath.WalkDir(p, func(q string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			_ = os.Chmod(q, 0o700)
		}
		return nil
	})
	return os.RemoveAll(p)
}

func (s *Store) clearCondemned(ctx context.Context, key string) error {
	return s.gcDelete(ctx, gcCondemnedPrefix+key)
}

func (s *Store) clearFirstSeen(ctx context.Context, key string) error {
	return s.gcDelete(ctx, gcFirstSeenPrefix+key)
}

func (s *Store) gcDelete(ctx context.Context, key string) error {
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		err = ks.Delete([]byte(key))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	})
}

// removeMountState removes a dead mount's state directory — a
// variable only so the reclamation deferral branch (removal
// failure ⇒ row and lock file both survive) is testable on a
// filesystem where removal works. Never reassigned outside tests.
var removeMountState = os.RemoveAll

// ReclaimDeadMounts reclaims dead mounts' rows, state directories,
// and claim files (REQ-store-mount-registry): a row whose claim
// lock a try-acquisition takes is a dead mount — the verdict for
// every row, foreign-version rows included (the lock has no
// format) — and holding that acquisition the sweep runs the
// fallible steps first (detach, state-directory removal); only on
// full success the row goes and the lock file is retired as the
// claim's final holder. Failure releases without unlink — row and
// file persist for retry, never a half-reclaimed id. A racing
// remount of the id meets the held lock and refuses as in-use.
// Returns the reclaimed ids.
func (s *Store) ReclaimDeadMounts(ctx context.Context) ([]string, error) {
	var candidates []string
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksMounts)
		if err != nil {
			return err
		}
		for k := range ks.All() {
			candidates = append(candidates, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var reclaimed []string
	for _, id := range candidates {
		l, err := oslock.TryAcquire(s.mountLockPath(id))
		if err != nil {
			// Held (live) or undecided: nothing to reclaim now.
			continue
		}
		stateDir := filepath.Join(s.path, "mounts", id)
		acted := true
		if _, err := os.Stat(stateDir); err == nil {
			detachStaleMount(filepath.Join(stateDir, "mnt"))
			if err := removeMountState(stateDir); err != nil {
				acted = false
			}
		}
		if !acted {
			// Deferral: release without unlink — row and lock file
			// stay for a later sweep (REQ-store-mount-registry).
			l.Close()
			continue
		}
		if err := s.DeleteMountRecord(ctx, id); err != nil {
			l.Close()
			continue
		}
		l.Retire()
		reclaimed = append(reclaimed, id)
	}
	return reclaimed, nil
}

// reclaimDeadOps removes dead ops rows and the temporaries they
// own, wherever they live (REQ-store-gc-collect).
func (s *Store) reclaimDeadOps(ctx context.Context, res *GCResult) error {
	type deadOp struct {
		id    string
		temps []string
	}
	var dead []deadOp
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, derr := decodeOpRecord(v)
			if derr != nil {
				continue // foreign rows: never acted on
			}
			if rec.Owner.Dead() {
				dead = append(dead, deadOp{id: string(k), temps: rec.Temps})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, d := range dead {
		for _, tmp := range d.temps {
			if err := os.RemoveAll(tmp); err == nil {
				res.CollectedPaths = append(res.CollectedPaths, tmp)
			}
		}
		if err := s.EndOp(ctx, d.id); err != nil {
			continue
		}
	}
	return nil
}

// DebrisSweep is the initialization sweep (REQ-store-gc-collect):
// dead mount and ops rows, dead leases, unowned export
// temporaries, orphaned tier files — no reachability mark, no
// grace.
func (s *Store) DebrisSweep(ctx context.Context) (*GCResult, error) {
	res := &GCResult{}
	reclaimed, err := s.ReclaimDeadMounts(ctx)
	if err != nil {
		return nil, err
	}
	res.ReclaimedMounts = reclaimed
	if err := s.reclaimDeadOps(ctx, res); err != nil {
		return nil, err
	}
	s.sweepLockTier(ctx)
	// Unowned export temporaries inside the store.
	exportsRoot := filepath.Join(s.path, "exports")
	if algos, err := os.ReadDir(exportsRoot); err == nil {
		for _, a := range algos {
			entries, err := os.ReadDir(filepath.Join(exportsRoot, a.Name()))
			if err != nil {
				continue
			}
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), ".export-") {
					p := filepath.Join(exportsRoot, a.Name(), e.Name())
					if !s.tempOwnedByLiveOp(ctx, p) {
						if err := os.RemoveAll(p); err == nil {
							res.CollectedPaths = append(res.CollectedPaths, p)
						}
					}
				}
			}
		}
	}
	return res, nil
}

// AutoCollect runs a collection pass at a garbage-creating
// transition when automatic collection is enabled
// (REQ-store-gc-collect). Failures are deliberately swallowed: the
// transition's own outcome must not depend on collection, and the
// garbage stays for the next pass.
func (s *Store) AutoCollect(ctx context.Context) {
	if !s.autoGC {
		return
	}
	_, _ = s.Collect(context.WithoutCancel(ctx), CollectOpts{Grace: s.gcGrace})
}

// ErrCondemned reports a root publication refused because a live
// sweep is deleting the digest (REQ-store-gc-safe): the caller
// backs out to its acquisition and retries — presence re-verifies
// and re-ingest happens under the lease.
var ErrCondemned = errors.New("digest condemned by a live sweep; retry from acquisition")

// dropDescriptor removes h from oci/index.json's descriptor list.
func (s *Store) dropDescriptor(h v1.Hash) error {
	idxPath := filepath.Join(s.path, "oci", "index.json")
	raw, err := os.ReadFile(idxPath)
	if err != nil {
		return err
	}
	var idx v1.IndexManifest
	if err := json.Unmarshal(raw, &idx); err != nil {
		return err
	}
	kept := idx.Manifests[:0]
	for _, d := range idx.Manifests {
		if d.Digest != h {
			kept = append(kept, d)
		}
	}
	idx.Manifests = kept
	out, err := json.MarshalIndent(idx, "", "   ")
	if err != nil {
		return err
	}
	return atomicfile.Write(idxPath, bytes.NewReader(out), 0o644)
}

// gcRowHygiene drops first-seen rows whose item is no longer a
// candidate (it re-rooted) and condemned rows whose sweeper is dead
// or gone.
func (s *Store) gcRowHygiene(ctx context.Context, candidates []gcItem) error {
	cand := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		cand[c.key()] = true
	}
	return s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		ops, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		var drop [][]byte
		for k, v := range ks.All() {
			key := string(k)
			switch {
			case strings.HasPrefix(key, gcFirstSeenPrefix):
				if !cand[strings.TrimPrefix(key, gcFirstSeenPrefix)] {
					drop = append(drop, append([]byte(nil), k...))
				}
			case strings.HasPrefix(key, gcCondemnedPrefix):
				ov, err := ops.Get(v)
				if errors.Is(err, gmdb.ErrNotFound) {
					drop = append(drop, append([]byte(nil), k...))
					continue
				}
				if err != nil {
					return err
				}
				rec, derr := decodeOpRecord(ov)
				if derr != nil || rec.Owner.Dead() {
					drop = append(drop, append([]byte(nil), k...))
				}
			}
		}
		for _, k := range drop {
			if err := ks.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}
