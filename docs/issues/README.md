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
- `docs/issues/mount-state-lifecycle.md` — per-mount state is never
  reclaimed and mount ids are single-use; reclamation design shares
  the dead-state recognition problem with store GC. Lands: when
  mount-state reclamation or mount-id reuse is first needed — at the
  latest with store GC.
- `docs/issues/platform-default-variant-hosts.md` — default-platform
  pulls on 32-bit arm hosts are variant-ambiguous; host variant
  detection for the default request. Lands: when default-platform
  pulls on 32-bit arm hosts are first needed.
- `docs/issues/writable-upper-fs-native.md` — writable layer: fs-native
  upper dir as truth; metadata.json removed. Lands: before the writable
  layer serves as a live container root.
- `docs/issues/writable-unprivileged-fidelity.md` — xattr presentation
  for ownership and special files; commit serializes the presented
  truth. Lands: with the fs-native upper.
- `docs/issues/writable-acceptance-workloads.md` — real-workload
  acceptance and crash-consistency tests. Lands: before the writable
  layer is declared production-ready.
- `docs/issues/store-metadata-gmdb.md` — store bookkeeping (refs, image
  records, mount registry, blob refcounts/GC) on gmdb. Lands: when
  store GC or cross-process store inspection is first needed; may
  precede other gmdb consumers.
