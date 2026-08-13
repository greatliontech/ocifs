# Plan: specs + read path (cross-platform)

Derived from `docs/specs/` (chunk 1 authored them; later chunks
conform to them). Deliverable: the read path rebuilt on the
first-principles package structure — pure `layer` core, `cas`,
reworked `store`, portable `projection` kernel with FUSE, ProjFS,
and FSKit backends — plus a digest-addressed platform-explicit
entry, the verification seam, and Export.

- [x] 1. Author `docs/specs/` in stipulator markup; compile the
      corpus, declare gaps, bind what exists; fix stale gmdb cite.
- [x] 2. `internal/layer` — the pure semantic core: value-typed
      entries addressing content by digest; bottom-up two-pass
      unification (whiteouts-first and tar-order-independent,
      intra-layer last-wins, non-directory-ancestor discard,
      leading-slash unification, containment failure, no input
      mutation); hardlink resolution; sorted View with path lookup.
      Layer-stack generator, filesystem-extraction oracle, property
      tests.
- [x] 3. `internal/cas` + `internal/store` rework: tier split
      (`layers/` out of the content CAS, per-mount `mounts/<id>`
      state), layer indexes record CAS keys (relocatable store),
      ingest idempotence (no duplicate `index.json` descriptors),
      in-process ingest locking, self-heal re-unpack from the
      retained layout, per-entry temp-file lifecycle,
      ref-written-last pinned by test; fixture-based network-free
      store harness.
- [x] 4. Platform + digest-addressed entry: retain and record the
      top-level artifact (refs hold the top-level digest),
      host-default platform, strict explicit platform (index and
      direct-manifest cases), entry by (repo, digest, platform) with
      no tag re-resolution, pull-policy and default-platform library
      options, longest-prefix keychain resolution.
- [x] 5. `internal/projection` kernel (portable, no backend): stable
      identity scheme, sorted snapshots + resumable enumeration,
      case-collision policy, fidelity mapping, store-persisted
      projection report, symbolic error space; kernel-level tests.
- [ ] 6. FUSE backend: glue replacing `internal/unionfs` on the
      kernel (short-read fix, directory attributes, mount-id
      validation); unionfs-equivalent tests over fixtures plus a
      network-free CLI smoke test.
- [ ] 7. ProjFS backend (windows): projfs-go glue — per-enumID
      cursors over comparator-sorted snapshots, placeholder
      ContentID = content digest, aligned-buffer reads, symlink
      feature probe with omit+report fallback, read-only pre-op
      vetoes with the two declared residuals; windows CI run.
- [ ] 8. FSKit backend (darwin): portable Volume implementation in
      the kernel's terms (tested on linux), darwin registration
      glue, `cmd/ocifs-fskit` appex main, `ocifs.app` host-bundle
      recipe (Tier-2 mount validation runs when the appex is signed
      and enabled — user-side act).
- [ ] 9. Verification seam: consumer-supplied verifier hook between
      top-level resolution and content materialization.
- [ ] 10. Export: materialize the unified view into a directory per
      `export.md` on `os.Root` (write-time symlink containment,
      copy-not-hardlink, modes, atomic completion, store-managed
      cache).
