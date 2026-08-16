# Plan: writable projections (provider-mediated, linux-first)

Derived from `docs/specs/writable.md` plus the amended
`projection.md` / `api.md` / `store.md` (chunk 1 authored them;
later chunks conform). Deliverable: writable FUSE mounts over a
POSIX upper with unprivileged fidelity, crash-consistent by
construction, and a dialect-neutral commit producing store images —
the ProjFS/FSKit write arms are explicit non-goals, gated on their
platform validation runs.

- [x] 1. Author `docs/specs/writable.md`; amend projection.md
      (upper-born ID derivation, forward contract resolved), api.md
      (writable mount, commit), store.md (uppers tier, local image
      namespace); compile the corpus, declare gaps; this plan.
- [x] 2. `internal/upper` — the POSIX upper dialect: walker/reader
      (dialect state → abstract upper entries: content, whiteouts,
      opaque, stand-ins, fidelity overrides), writer primitives
      (atomic create-via-rename, markers, stand-in conversion,
      override records), index rebuild from a walk; property tests
      over generated dialect states including every crash prefix.
- [x] 3. Commit: canonical (base view, upper) diff → deterministic
      uncompressed layer tar; store image assembly (manifest/config
      extension, diff IDs, ingest ordering) and the `ocifs.local`
      digest namespace; base binding for store-managed uppers;
      public Commit API (offline, no mount). Commit-neutrality
      property: equal states → byte-identical layers, independent of
      write history.
- [x] 4. Projection kernel merge — view ⊎ upper resolution behind
      the existing kernel surface: shadowing, whiteout/opaque
      occlusion, upper-born ino-derived IDs, merged enumeration
      snapshots with upper-derived change verifiers, invalidation on
      upper mutation; kernel-level tests, no backend.
- [x] 5. FUSE write path A — copy-up engine (atomic, ancestors with
      presented attrs) and the core mutating ops (create, write,
      truncate, setattr, mkdir, unlink, rmdir, symlink) with
      whiteout/opaque production; writable mount API (caller dir +
      store-managed named upper), reserved-name refusal; store
      uppers tier.
- [ ] 6. FUSE write path B — rename semantics (EXDEV for
      base-visible directories), hardlinks, mknod/FIFO/socket
      stand-ins, xattr surface with fidelity overrides and
      stand-in conversion; presented-truth attribute merging.
- [ ] 7. Crash-consistency harness — kill storms at arbitrary points
      under write/rename/whiteout load; on remount the dialect is
      valid, the rebuilt index matches the directory, and commit of
      the survived state is correct.
- [ ] 8. Acceptance workloads — exec-from-written-file, mmap,
      concurrent build (`make -j`), package-manager install in a
      container rooted on the mount, `security.capability` xattrs;
      environment-dependent pieces run where the machine allows and
      defer loudly where it does not.
