# Store bookkeeping on gmdb

The store's cold-path metadata is file-based today (refs under
`refs/`, ad-hoc layout elsewhere). Move it to gmdb (the
`greatliontech/gmdb` repository; import path per its go.mod when
adopted), which fits this exactly:

- ref → digest cache, image records, mount registry as keyspaces;
- **blob refcounts for GC** — the need that grows teeth once
  environments churn (attach/detach/commit cycles create garbage
  blobs); "which blobs are reachable from which images and writable
  layers" is a transactional refcount table with prefix scans;
- cross-process readers: an `ocifs` CLI inspecting the store while a
  daemon holds mounts, without an RPC surface.

Scope boundary: store bookkeeping only. The writable layer's
per-syscall metadata stays fs-native (see
writable-upper-fs-native.md); a database belongs under the store, not
under the filesystem.

Lands: when store GC or cross-process store inspection is first
needed; may precede other gmdb consumers — consumer ordering is the
constellation's call, not this repo's.
