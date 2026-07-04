# Writable layer: fs-native upper dir as truth

The `writable` branch stores writable-layer state in a `files` map
persisted to `metadata.json` with auto-persist (interval / mutation
threshold). That design has a corruption window: content can land while
metadata is lost on crash, and the whole map must round-trip through
one JSON file.

Replace it with the upper directory as the single source of truth:

- Written files are real files in the writable dir — real names, real
  modes, real timestamps. Whiteouts are literal `.wh.` marker files,
  opaque directories are `.wh..wh..opq` — the upper dir is stored in
  OCI layer layout directly, the same format the read-side store
  unpacks.
- The in-memory map becomes a cache, rebuilt by walking the dir on
  mount. Nothing else is persisted: no metadata.json, no auto-persist
  ticker, no dirty accounting.
- Crash consistency is inherited from the kernel filesystem,
  per-operation. A killed process must leave a consistent layer
  (possibly missing the in-flight file), never a corrupt one.
- Commit becomes a walk-and-tar of a dir that is already OCI-shaped
  (attribute translation per the unprivileged-fidelity issue).

Rule of thumb this encodes: per-syscall metadata belongs to the
filesystem; a database under a filesystem is indirection unless the
filesystem genuinely cannot hold the data — and with the xattr escape
hatch, it can.

Lands: before the writable layer serves as a live container root
(weaver consumes it as the environment mechanism).
