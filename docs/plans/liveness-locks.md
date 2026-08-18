# Plan: liveness by held lock

Derived from the store.md liveness contract as it will be amended
in chunk 1: cross-process liveness verdicts move from process
identity (a namespace-scoped name proxy) to kernel-held file locks
(fd-lifetime, filesystem-observable, try-lock verdicts where
acquisition IS the claim). Spans ocifs and gmdb (gmdb commits
authorized as part of this change set). Includes the agreed mount-
report publication marker.

- [x] 1. Spec work: rewrite store.md's liveness clauses — the
      `locks/` tier, one lock file per claim (mount, upper, op,
      ingest lease), acquire-before-row ordering, the CLOEXEC and
      inode-verify disciplines, the init locking-soundness probe
      (refuse a filesystem whose advisory locks are broken), the
      foreign-version lease carve-out deleted, foreign-version
      mounts conservatism narrowed to live rows; writable.md
      arbitration wording; api.md GC-report fields (unjudgeable
      deletes); the publication-marker sentences; compile, gaps,
      pins; this plan.
- [x] 2. Publication marker: mounts-record published flag, set by
      report publication, exposed to readers; closes
      docs/issues/mount-report-publication-marker.md.
- [ ] 3. gmdb: export the portable lock layer as a public package
      (try/exclusive/unlock with inode-verified acquire), adopted
      internally; tag; ocifs go.mod bump. Committed in gmdb.
- [ ] 4. ocifs lock plumbing: the locks tier, the init probe, and
      the claim helpers over the gmdb lock package.
- [ ] 5. Mounts and uppers on locks: registration acquires
      mount-<id>, writable serve holds upper-<name>, reclamation
      claims by acquisition, RemoveUpper takes the upper lock; the
      identity Dead() verdicts, guard rows, and claim-rewrite
      machinery delete; identity fields become diagnostic only.
- [ ] 6. Lease and ops on locks: locks/ingest replaces the ops-row
      lease (nonce, in-process slot, self-reclaim delete);
      op-<id> locks replace op-owner verdicts; sweep's dead-sweeper
      rule becomes a try-lock; condemned-set fencing unchanged.
- [ ] 7. gmdb: reader-slot liveness reworked onto OFD byte-range
      locks (heartbeat and PID-namespace machinery deletes);
      cross-process.md rewritten; gmdb's own crash/soak suites are
      the net. Committed in gmdb; tag; ocifs bump.
- [ ] 8. Revalidation and close-out: the existing storm, soak,
      reader, and property harnesses green over the swap; the new
      cross-namespace test (mount registered in a child PID
      namespace, killed, judged and reclaimed from the parent);
      probe the try-lock verdict arms, inode-verify retry, and
      init probe; plan close-out.
