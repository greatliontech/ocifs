# Plan: specs + read path

Derived from `docs/specs/` (chunk 1 authors them; later chunks conform
to them). Deliverable of the plan: specs exist as the compiled
stipulator corpus, the read path (ingest → unify → mount/export) is
correct and tested, and a digest-addressed, platform-explicit entry
plus Export exist for library consumers.

- [x] 1. Author `docs/specs/` (`store.md`, `layer-semantics.md`,
      `export.md`, `api.md`, `verification-seam.md`) in stipulator
      requirement markup; compile the corpus, declare gaps for
      unbuilt requirements, bind what exists. Fix stale gmdb repo
      cite in `docs/issues/store-metadata-gmdb.md`.
- [ ] 2. Store ingest correctness: split layer-index namespace out of
      the content CAS (`layers/` vs `blobs/`, retiring the shared
      path helper), layer indexes record content-CAS keys instead of
      absolute host paths (relocatable store), ingest idempotence (no
      duplicate `index.json` descriptors on re-pull), in-process
      ingest locking (concurrent pulls no longer race the
      `index.json` read-modify-write), self-heal re-unpack from the
      retained local layout, per-entry temp-file lifecycle in tar
      extraction, ref-written-last ordering pinned by test;
      fixture-based (network-free) store test harness.
- [ ] 3. Unification hardening + mount presentation: tar-order-independent
      whiteout application (dir-before-marker no longer neutralizes
      the delete), discard descendants of non-directory entries,
      intra-layer last-wins, leading-slash path unification, hardlink
      linkname cleaning and non-regular-target omission, unify no
      longer mutates its input headers, short-read result trimming in
      FUSE read, directory attributes applied from tar headers,
      mount-id validation (single path element); unionfs tests over
      fixtures plus a network-free CLI smoke test.
- [ ] 4. Platform + digest-addressed entry: retain and record the
      top-level artifact (index) with refs holding the top-level
      digest, host-default platform, strict explicit platform, entry
      by (repo, digest, platform) with no tag re-resolution,
      pull-policy and default-platform exposed as library options,
      deterministic longest-prefix keychain resolution; port/adapt
      multiarch machinery and tests from the `writable` branch.
- [ ] 5. Verification seam: consumer-supplied verifier hook invoked
      between top-level resolution and content materialization.
- [ ] 6. Export: materialize unified view into a directory per
      `export.md` (containment, copy-not-hardlink, modes, atomic
      completion, store-managed export cache).
