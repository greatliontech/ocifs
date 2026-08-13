# Issues

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
