# Full mutation campaign over the liveness-locks delta

**Lands: when a gomutant campaign over the liveness rework's delta
(changed vs a5c0f9d) completes and its survivors are dispositioned**
— blocked today by gomutant defects the user is fixing (exhaustive
runs over heavyweight oracles take tens of hours, and useful cache
state is invalidated between runs, so long campaigns lose their
incremental progress).

## What ran instead

The close-out ran targeted hand probes via `gomutant ephemeral`
over the delta's load-bearing liveness symbols — the try-lock
verdict arms (`claimHeld` both arms, root-set consult, registration
and upper arbitration, ingest-lease blocking semantics), the
soundness probe (both verdict arms), the lock-tier sweep (unlink
and deferral disciplines), mount/op reclamation (liveness gate,
deferral, final-holder retire, foreign-defer), the sweeper-liveness
dead arm, the publication flag, and deregistration disposal. Two
survivors became tests (`TestCollectHygieneDropsDeadSweeperCondemnedRows`,
`TestDeregisterHoldsClaimUntilRowGone`); the rest were killed by
behavior-named tests. The plan's third probe item — oslock's
inode-verify retry — lives in the gmdb dependency and cannot be
mutated from this tree; its probes ran (and killed) in gmdb's own
per-chunk review loops. The gmdb side likewise ran its own
hand-probe sets there.

One recorded equivalence for the campaign's survivor triage: a
mutant collapsing `claimLive` into `claimUndecided` (internal/store
locks.go, claimVerdict) is equivalent by construction today — every
call site branches on `!= claimDead` — and the split is kept as
contract-naming (store.md's verdict is three-valued) and as the
attachment point for a future undecided counter.

## What the campaign adds over the probes

Breadth: the hand probes are one-to-a-few chosen mutations per
symbol; a campaign measures the operator set exhaustively (or
budgeted) across all 59 changed symbols, including the ones judged
low-risk and left unprobed (path helpers, encoders, snapshot
plumbing). Survivors there are findings to disposition, not
assumed-covered.
