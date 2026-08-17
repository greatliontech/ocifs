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
- `docs/issues/export-temp-cleanup.md` — crashed exports leave inert
  `.export-*` temporaries nothing sweeps; caller-target temporaries
  land outside the store. Lands: with store GC, or earlier if
  accumulation surfaces in practice.
- `docs/issues/mount-state-lifecycle.md` — per-mount state is never
  reclaimed and mount ids are single-use; reclamation design shares
  the dead-state recognition problem with store GC. Lands: when
  mount-state reclamation or mount-id reuse is first needed — at the
  latest with store GC.
- `docs/issues/projection-report-binary-names.md` — projection report
  persists entry paths as plain JSON strings, mangling non-UTF-8
  names (same encoding-fault class the layer index fixed). Lands:
  when the projection report gains a consumer resolving paths against
  image entries, or the next change set touching
  `internal/projection/report.go`.
- `docs/issues/projfs-fskit-write-arms.md` — writable mounts are
  FUSE-only; the ProjFS/FSKit backends need write-engine glue plus
  platform fidelity mechanics. Lands: per backend, when its platform
  validation findings are dispositioned and a writable mount there
  is first needed.
- `docs/issues/concurrent-test-runs-collide-on-scratch-paths.md` —
  deterministic scratch paths make concurrent same-package test runs
  mutually destructive (tree removal and stale-mount reaping hit the
  other run's live state). Lands: when concurrent same-package runs
  become a supported flow, or the next collision incident.
