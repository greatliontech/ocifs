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
- `docs/issues/export-cancellation.md` — materialization ignores
  context cancellation once started. Lands: when consumer-driven
  cancellation of a running export is first needed.
- `docs/issues/mount-state-lifecycle.md` — per-mount state is never
  reclaimed and mount ids are single-use; reclamation design shares
  the dead-state recognition problem with store GC. Lands: when
  mount-state reclamation or mount-id reuse is first needed — at the
  latest with store GC.
- `docs/issues/platform-default-variant-hosts.md` — default-platform
  pulls on 32-bit arm hosts are variant-ambiguous; host variant
  detection for the default request. Lands: when default-platform
  pulls on 32-bit arm hosts are first needed.
- `docs/issues/store-metadata-gmdb.md` — store bookkeeping (refs, image
  records, mount registry, blob refcounts/GC) on gmdb. Lands: when
  store GC or cross-process store inspection is first needed; may
  precede other gmdb consumers.
- `docs/issues/projection-report-binary-names.md` — projection report
  persists entry paths as plain JSON strings, mangling non-UTF-8
  names (same encoding-fault class the layer index fixed). Lands:
  when the projection report gains a consumer resolving paths against
  image entries, or the next change set touching
  `internal/projection/report.go`.
- `docs/issues/test-fixture-stale-mount-recovery.md` — test fixtures
  hang on FUSE mounts leaked by a killed prior run; setup should
  lazy-unmount stale scratch mounts before removing. Lands: next
  change set touching the shared test fixture helpers, or the next
  stale-mount incident.
