# Store: one three-valued judge for claim-lock verdicts

**Lands: user decision** (a cross-cutting refactor of the store's
reclamation and root-set paths; scheduling is the user's call).

## The duplication

The try-lock verdict is spelled three ways:

- `claimHeld` (locks.go) — judge-only: acquire ⇒ dead, release
  immediately; held/undecided ⇒ alive.
- `sweeperLiveness`'s claim leg (gc.go) — the same judgment
  reshaped into a three-state enum for hygiene.
- The acquire-and-keep reclamation paths (`ReclaimDeadMounts`,
  `reclaimDeadOps`) — the same verdict where an acquisition must be
  RETAINED as the reclamation claim rather than released.

Adjacent: "is this row foreign" is likewise decided in more than
one place (decode-error in `rootSet` and `reclaimDeadOps`, raw
presence in `rowExists`).

## Sketch of the collapse

One judge — `judgeClaim(path) (verdict, release)` — with an
explicit keep/release choice at the call site, replacing the three
spellings; every arm keeps its current semantics (undecided is
never death; a kept acquisition is the reclamation claim). One
`decodeRowClass` helper owning the foreign/native/absent
classification. Invariants preserved: the three-valued verdict
contract of store.md, the deferral shape, foreign-defer. What
deletes: the enum-vs-bool-vs-inline divergence that lets one arm
drift when the verdict rules change.
