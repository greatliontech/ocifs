# Per-mount state is never reclaimed and mount ids are single-use

`mounts/<id>` (bookkeeping beside the `mnt/` mountpoint,
`docs/specs/store.md` REQ-store-layout) is created at mount time and
never removed: unmount leaves the state directory, its projection
report, and the empty `mnt/` behind, and a later mount reusing the
same id fails with EEXIST. Auto-generated ids accumulate one
directory per mount for the store's lifetime.

The store spec's overview describes per-mount state as "ephemeral
bookkeeping that dies with its mount", but no requirement pins a
reclamation point, and REQ-api-mountpoint requires the mountpoint
directory to remain (empty) after a successful unmount — so naive
removal of the whole state directory on unmount would violate it for
store-managed mountpoints. Reclamation wants a deliberate design:
what unmount removes (the report? nothing?), when ids become
reusable, and how dead state from crashed serving processes is
recognized — the same recognition problem store GC and the mount
registry face (`docs/issues/store-metadata-gmdb.md`).

Lands: when mount-state reclamation or mount-id reuse is first
needed — at the latest with store GC.
