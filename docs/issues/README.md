# Issues

- `docs/issues/store-oci-absent-pull-through.md` — self-heal cannot
  recover a missing `oci/` blob under an intact ref; digest
  pull-through per policy. Lands: 4.
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
