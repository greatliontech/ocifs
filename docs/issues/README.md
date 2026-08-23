# Issues

- `docs/issues/fskit-darwin-validation.md` — the FSKit backend's
  portable core is linux-pinned; the platform half (signed appex,
  bridge dispatch, orchestrated mount) awaits a darwin Tier-2 run.
  Lands: when the darwin mount validation reports back and its
  findings are dispositioned.
- `docs/issues/projfs-windows-validation.md` — the ProjFS backend and
  its windows test suite are authored and cross-compiled but
  unexecuted. Lands: when the windows test run reports back and its
  findings are dispositioned.
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
