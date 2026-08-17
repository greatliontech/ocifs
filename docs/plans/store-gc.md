# Plan: store bookkeeping on gmdb + garbage collection

Derived from `docs/specs/store.md` as amended (bookkeeping database,
mount registry, GC contract), `api.md` (removal/collection surface,
construction knobs), `projection.md` (report re-homed), and
`writable.md` (binding re-homed). Deliverable: all ocifs-interpreted
records in one transactional cross-process gmdb database, dead state
reclaimed, unreachable content collected automatically under a
retention grace, and a deletion surface that severs roots. Clean
break: the old file tiers (`refs/`, `layers/`, per-mount state
files, binding siblings) are refused by adoption, not migrated.

- [x] 1. Spec work: amend store.md (bookkeeping database, keyspace
      contract, mount registry, GC roots/safety/collection,
      single-writer resolution), api.md (knobs, remove, gc),
      projection.md (report re-homed), writable.md (binding
      re-homed); compile the corpus, declare gaps; this plan.
- [x] 2. Bookkeeping seam: gmdb dependency and lifecycle in store
      init (`bookkeeping/` tier); `refs` keyspace replaces the file
      tier and its percent-encoding machinery; adoption signature
      updated to refuse pre-database layouts.
- [x] 3. Regenerable records into the database: `layeridx` keyspace
      replaces the `layers/` file tier and its base64 encoding
      (self-heal semantics unchanged); mount records into `mounts`
      carrying the full wire contract (liveness identity, upper
      name, report) from the start; base bindings into `uppers`;
      `localimages` rows written by commit. Folds
      docs/issues/projection-report-binary-names.md.
- [x] 4. Mount registry liveness and reclamation: registration
      before serve with the full liveness identity, deregistration
      on unmount,
      dead-row recognition, mount-id reuse. Folds
      docs/issues/mount-state-lifecycle.md.
- [ ] 5. Cross-process ingest lease and ops rows: single ingesting
      process enforced through a live `ops` row spanning content
      writes through root publication (commit included); export
      and commit ops pin their digests and record their
      temporaries; lease death by liveness identity; concurrent
      same-image pulls race benignly across processes.
- [ ] 6. Removal and collection: RemoveRef/RemoveImage/RemoveUpper
      severing roots; mark-and-sweep with transactional root
      snapshot, re-check before delete, ingest-lease fencing,
      retention grace and condemned-set fencing; automatic
      collection at garbage-creating transitions plus the debris
      sweep (dead ops' temporaries wherever they live — the
      caller-target export litter included); explicit GC API + CLI
      verb with grace override. Folds
      docs/issues/export-temp-cleanup.md.
- [ ] 7. Churn soak and crash harness: pull/mount/commit/remove/
      collect cycles with automatic collection on, kill storms
      mid-sweep and mid-ingest, cross-process readers during
      collection; the crash model pinned end-to-end.
