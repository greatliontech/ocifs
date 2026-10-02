# Issues

- `docs/issues/fskit-darwin-validation.md` — the FSKit backend's
  portable core is linux-pinned; the platform half (signed appex,
  bridge dispatch, orchestrated mount) awaits a darwin Tier-2 run.
  Lands: when the darwin mount validation reports back and its
  findings are dispositioned.
- `docs/issues/projfs-filtered-pagination-untested.md` — a wildcard
  search over a paginating ProjFS directory has no dedicated test.
  Lands: when the windows row witnesses a filtered enumeration that
  pages.
- `docs/issues/projfs-content-id-by-review.md` — the placeholder
  ContentID digest is pinned by review alone, ProjFS exposing no
  read-back. Lands: when a read-back of a placeholder's ContentID
  exists (projfs-go or the platform) and the windows row compares it.
- `docs/issues/layer-suite-linux-gated.md` — the layer suite's
  extraction oracle needs mkfifo and the property tests share its
  linux-gated file, so three oracle-free property tests and the
  oracle run on linux alone. Lands: when the three run on every row
  and the oracle builds its FIFO cases without mkfifo or states the
  cap.
- `docs/issues/projfs-foreign-held-placeholder.md` — a placeholder a
  foreign process holds open at unmount survives the residue sweep
  unreported. Lands: when the windows row probes it and the sweep's
  answer is dispositioned.
- `docs/issues/store-tests-linux-gated.md` — the store's collection,
  lock-retirement-under-collection and export tests are linux-gated.
  Lands: when the three files run on the windows and darwin rows.
- `docs/issues/projfs-fskit-write-arms.md` — writable mounts are
  FUSE-only; the ProjFS/FSKit backends need write-engine glue plus
  platform fidelity mechanics. Lands: per backend, when its platform
  validation findings are dispositioned and a writable mount there
  is first needed.
- `docs/issues/liveness-locks-mutation-campaign.md` — the held-lock
  rework's close-out ran targeted hand probes instead of a full
  gomutant campaign (tool defects make long runs lose incremental
  cache state). Lands: when a campaign over the rework's delta
  (changed vs a5c0f9d) completes and its survivors are
  dispositioned.
- `docs/issues/concurrent-test-runs-collide-on-scratch-paths.md` —
  deterministic scratch paths make concurrent same-package test runs
  mutually destructive (tree removal and stale-mount reaping hit the
  other run's live state); two incidents recorded, oslock now offers
  the fix mechanism. Lands: user decision.
- `docs/issues/options-apply-after-construction.md` — `Option` values
  write a constructed store's fields, so one applied after New races
  the store's and a consumer's credential resolution; the collapse is
  options onto a settings value only New holds. Lands: when an option
  is next added, removed or changed.
- `docs/issues/mount-report-types-internal.md` — MountReport returns
  an internal type, so a consumer cannot name a report's disposition
  or reason. Lands: when a consumer first switches on them
  exhaustively.
