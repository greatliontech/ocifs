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
	// ForeignVersionRows lists LIVE mounts or ops rows written by
	// a different ocifs version: their liveness is judged by lock
	// like any row, but a live one's image or pins cannot be read
	// by this binary, so image-tier collection halts visibly — the
	// one irreducible foreign-version conservatism
	// (REQ-store-bookkeeping). Dead foreign rows halt nothing and
	// are never listed.
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
	lease, err := s.AcquireIngestLease(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.ReleaseIngestLease(context.WithoutCancel(ctx), lease) }()
	sweepOp, err := s.BeginOp(ctx, opKindSweep, nil, nil)
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
		// A live foreign-version mounts or ops row makes the mark
		// unjudgeable: its image or pins are unreadable to this
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
	condemned, deferredPaths, err := s.condemn(ctx, sweepOp.ID, due)
	if err != nil {
		return nil, err
	}
	res.Deferred = append(res.Deferred, deferredPaths...)
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

// markedOp is an ops row awaiting the same treatment.
type markedOp struct {
	id      string
	pins    []v1.Hash
	foreign bool
}

func (s *Store) rootSet(ctx context.Context) ([]v1.Hash, []string, error) {
	if markHook != nil {
		markHook()
	}
	var roots []v1.Hash
	var foreignRows []string
	var mountRows []markedMount
	var opRows []markedOp
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
			rec, foreign := decodeMountRow(v)
			mountRows = append(mountRows, markedMount{
				id: string(k), image: rec.Image, foreign: foreign,
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
		for k, v := range ops.All() {
			rec, foreign := decodeOpRow(v)
			opRows = append(opRows, markedOp{
				id: string(k), pins: rec.Pins, foreign: foreign,
			})
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
	// Ops rows: identical treatment — a LIVE foreign-version op row
	// pins digests this binary cannot read, so it halts image-tier
	// collection exactly like a live foreign mounts row
	// (REQ-store-bookkeeping); dead rows pin nothing.
	for _, o := range opRows {
		if !s.opClaimHeld(o.id) {
			continue
		}
		if o.foreign {
			foreignRows = append(foreignRows, o.id)
			continue
		}
		roots = append(roots, o.pins...)
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
	type opTemps struct {
		id    string
		temps []string
	}
	var rows []opTemps
	verr := dbView(ctx, s.bk.db, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, foreign := decodeOpRow(v)
			if foreign {
				continue // foreign temps unreadable: not matchable
			}
			rows = append(rows, opTemps{id: string(k), temps: rec.Temps})
		}
		return nil
	})
	if verr != nil {
		// An undecided read is never a destruction verdict
		// (held-lock liveness): an unreadable ops keyspace reads as
		// "owned" and the path survives this pass.
		return true
	}
	for _, r := range rows {
		for _, t := range r.temps {
			if t == p && s.opClaimHeld(r.id) {
				return true
			}
		}
	}
	return false
}

// pathReclaimed reports whether a path candidate has been re-owned
// since enumeration: a mounts state directory whose id's claim is
// now held, or a temporary an op row records (ownedTemps, one
// snapshot per condemnation — never one read transaction per
// candidate under the write grant; the caller defers everything
// when the snapshot is undecided). Condemnation re-checks path
// items with this exactly as it re-marks digests
// (REQ-store-gc-safe).
func (s *Store) pathReclaimed(p string, ownedTemps map[string]string) bool {
	mountsRoot := filepath.Join(s.path, "mounts") + string(filepath.Separator)
	if rest, ok := strings.CutPrefix(p, mountsRoot); ok {
		id := rest
		if i := strings.IndexByte(rest, filepath.Separator); i >= 0 {
			id = rest[:i]
		}
		if s.mountClaimHeld(id) {
			return true
		}
	}
	if op, ok := ownedTemps[p]; ok && s.opClaimHeld(op) {
		return true
	}
	return false
}

// opsTempsSnapshot maps every recorded temporary to its op id —
// one read pass, judged lazily per hit. A nil return means the
// read was undecided.
func (s *Store) opsTempsSnapshot(ctx context.Context) map[string]string {
	owned := map[string]string{}
	err := dbView(ctx, s.bk.db, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, foreign := decodeOpRow(v)
			if foreign {
				continue // foreign temps unreadable: not matchable
			}
			for _, t := range rec.Temps {
				owned[t] = string(k)
			}
		}
		return nil
	})
	if err != nil {
		// Reachable only on a transient: rootSet reads this same
		// keyspace moments earlier in the same condemnation and
		// propagates ITS failure as a hard error of the pass — that
		// ordering is what bounds the deferral below; a reordering
		// that drops the rootSet read would silently unbound it.
		return nil
	}
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

// markHook fires at the top of every mark (rootSet) — the seam
// that lets a test assert condemn's re-mark runs UNDER the write
// grant: hoisting the mark out of the transaction moves this hook
// with it, and the test's probe write then succeeds where it must
// fail. Never set outside tests.
var markHook func()

// condemn re-marks and writes the condemned set INSIDE one write
// transaction (REQ-store-gc-safe): the re-mark runs while this
// transaction holds the database's write grant, so every root
// publish is serialized either before it — a fresh read snapshot
// under the grant sees all committed roots, and nothing new can
// commit while it is held — or after the condemned rows exist,
// where the publisher's consult refuses. No fence reasoning about
// the acquisition-to-row span is needed: the ordering is the
// writer's own serialization. Items reachable under the re-mark
// are dropped.
func (s *Store) condemn(ctx context.Context, sweepOp string, due []gcItem) ([]gcItem, []string, error) {
	var out []gcItem
	var deferredPaths []string
	err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
		roots, foreignRows, err := s.rootSet(ctx)
		if err != nil {
			return err
		}
		if len(foreignRows) > 0 {
			return nil // unjudgeable: condemn nothing
		}
		reachable, err := s.reachableFrom(ctx, roots)
		if err != nil {
			return err
		}
		ownedTemps := s.opsTempsSnapshot(ctx)
		ks, err := tx.OpenKeyspace(ksGC)
		if err != nil {
			return err
		}
		for _, c := range due {
			if c.tier != "path" && reachable[c.tier+"\x00"+c.digest.Algorithm+"\x00"+c.digest.Hex] {
				continue
			}
			// Path items get the same fresh re-check as digests:
			// ownership may have appeared since enumeration (a
			// registrant claiming the id, an op adopting the temp).
			// An UNDECIDED snapshot defers them VISIBLY: a re-owned
			// path is not garbage and stays silent, but a skip the
			// operator did not cause must never be one they cannot
			// see (Deferred is the vocabulary for exactly this).
			if c.tier == "path" {
				if ownedTemps == nil {
					// The keyed form, matching every other Deferred
					// element (one parse rule for the whole list).
					deferredPaths = append(deferredPaths, c.key())
					continue
				}
				if s.pathReclaimed(c.path, ownedTemps) {
					continue
				}
			}
			if err := ks.Put([]byte(gcCondemnedPrefix+c.key()), []byte(sweepOp)); err != nil {
				return err
			}
			out = append(out, c)
		}
		return nil
	})
	return out, deferredPaths, err
}

// condemnedByLiveSweep reports whether the digest is condemned by a
// LIVE sweeper — the consult every unleased root-publishing write
// runs (REQ-store-gc-safe).
func (s *Store) condemnedByLiveSweep(_ context.Context, tx *gmdb.Tx, h v1.Hash) (bool, error) {
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
		sweeper := string(v)
		if _, err := ops.Get(v); errors.Is(err, gmdb.ErrNotFound) {
			// Row gone: a finished sweeper binds nobody, and its
			// stale condemned rows are provably clearable without
			// any lock — the same clearing rule as the dead arm.
			if err := clearCondemnedOf(gcks, sweeper); err != nil {
				return false, err
			}
			continue
		} else if err != nil {
			return false, err
		}
		// The sweeper's liveness is its claim lock — one probe,
		// held through the clearing: a dead sweeper binds nobody,
		// and the judge clears its debris rows before releasing
		// (REQ-store-gc-safe) so later consults meet nothing. The
		// release is WITHOUT unlink: the sweeper's row still exists
		// and its file follows the row's own reclamation path
		// (deferral shape — foreign rows defer indefinitely).
		verdict, l := judgeClaim(s.opLockPath(sweeper))
		if verdict != claimDead {
			return true, nil // live or undecided: a live sweeper binds
		}
		if err := clearCondemnedOf(gcks, sweeper); err != nil {
			l.Close()
			return false, err
		}
		l.Close()
	}
	return false, nil
}

// sweeperVerdict is a condemned-row judge's three-valued reading.
// The zero value is sweeperLive DELIBERATELY: an error path that
// discards the verdict falls on the safe, binding side.
type sweeperVerdict int

const (
	sweeperLive sweeperVerdict = iota // row present, claim held or undecided
	sweeperDead                       // row present, claim acquirable
	sweeperGone                       // row absent: the sweeper finished
)

// sweeperLiveness is the judge-only reading of a condemned row's
// sweeper (gcRowHygiene): a finished sweeper (row gone) binds
// nobody without any lock probe; otherwise the claim lock decides,
// non-blocking (this runs inside write transactions). The consult
// probes-and-HOLDS instead, so it does not share this.
func (s *Store) sweeperLiveness(ops *gmdb.Keyspace, sweeper string) (sweeperVerdict, error) {
	if _, err := ops.Get([]byte(sweeper)); errors.Is(err, gmdb.ErrNotFound) {
		return sweeperGone, nil
	} else if err != nil {
		return sweeperLive, err
	}
	verdict, l := judgeClaim(s.opLockPath(sweeper))
	if verdict == claimDead {
		l.Close() // judge-only consult: the deferral shape
		return sweeperDead, nil
	}
	return sweeperLive, nil // live or undecided both bind
}

// clearCondemnedOf deletes every condemned row a sweeper wrote —
// the debris a dead sweeper leaves (its final holder disposes it).
func clearCondemnedOf(gcks *gmdb.Keyspace, sweeper string) error {
	var drop [][]byte
	for k, v := range gcks.All() {
		if strings.HasPrefix(string(k), gcCondemnedPrefix) && string(v) == sweeper {
			drop = append(drop, append([]byte(nil), k...))
		}
	}
	for _, k := range drop {
		if err := gcks.Delete(k); err != nil {
			return err
		}
	}
	return nil
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

// removeOpTemp is removeMountState's sibling for a dead op's
// recorded temporaries. Never reassigned outside tests.
var removeOpTemp = os.RemoveAll

// dbView is the read-transaction entry every liveness-adjacent
// verdict read goes through — a variable only so the
// undecided-read arms (a failed View must never become a
// destruction verdict) are testable on a healthy database. Never
// reassigned outside tests.
var dbView = func(ctx context.Context, db *gmdb.DB, fn func(*gmdb.ReadTx) error) error {
	return db.View(ctx, fn)
}

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
		verdict, l := judgeClaim(s.mountLockPath(id))
		if verdict != claimDead {
			// Live or undecided: nothing to reclaim now.
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
	type opRow struct {
		id      string
		temps   []string
		foreign bool
	}
	var rows []opRow
	err := s.bk.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, foreign := decodeOpRow(v)
			rows = append(rows, opRow{id: string(k), temps: rec.Temps, foreign: foreign})
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, d := range rows {
		// The verdict IS the claim (held-lock liveness): held or
		// undecided rows are live and untouched; an acquired row is
		// dead — temporaries first (fallible), then the row, then
		// the lock file as its final holder. Failure releases
		// without unlink: row and file persist for retry. A dead
		// FOREIGN row defers wholesale: its recorded temporaries
		// are unreadable to this binary and a caller-target
		// temporary is reachable through nothing else, so the row
		// and file stay for a binary that can read them — reclaim
		// what the key alone names, defer what needs the value
		// (in-store dot-temporaries still fall to the orphan sweep
		// as unowned debris).
		if d.foreign {
			continue
		}
		verdict, l := judgeClaim(s.opLockPath(d.id))
		if verdict != claimDead {
			continue
		}
		acted := true
		for _, tmp := range d.temps {
			if err := removeOpTemp(tmp); err != nil {
				acted = false
				continue
			}
			res.CollectedPaths = append(res.CollectedPaths, tmp)
		}
		if !acted {
			l.Close()
			continue
		}
		err := s.bk.db.Update(ctx, func(tx *gmdb.Tx) error {
			ks, err := tx.OpenKeyspace(ksOps)
			if err != nil {
				return err
			}
			err = ks.Delete([]byte(d.id))
			if errors.Is(err, gmdb.ErrNotFound) {
				return nil
			}
			return err
		})
		if err != nil {
			l.Close()
			continue
		}
		l.Retire()
	}
	return nil
}

// DebrisSweep is the initialization sweep (REQ-store-gc-collect):
// dead mount and ops rows, stranded lock files, unowned export
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
				verdict, err := s.sweeperLiveness(ops, string(v))
				if err != nil {
					return err
				}
				if verdict != sweeperLive {
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
