# Unleased publishes can root a digest severed mid-acquisition

REQ-store-gc-safe's fences leave one window open (stated in the
spec): a mount registration, upper binding, export pin, or the
reference-cache row of a digest-form acquisition over fully
materialized content names a digest that was rooted when its
acquisition resolved, but the acquisition-to-row span holds no
pin — a concurrent RemoveRef plus a grace-ignoring sweep can
condemn and delete the content inside that span, after which the
unleased row publishes a root over vanished blobs. Reachable paths
cited by review: (1) Mount resolves image A (rooted by refs);
RemoveRef severs A; `ocifs gc --ignore-grace` marks and condemns A
in the window before the registration's row write; the condemned
consult passes because the condemned rows do not exist yet at the
consult — the mount serves missing layers. (2) Worse and
persisting: a digest-form acquisition over fully materialized
content takes NO lease (nothing to fetch, nothing to write) and
its RefPut lands in the same window — leaving a durable refs row
naming a top-level artifact whose oci/ content is gone, a direct
REQ-store-ref-complete violation that local self-heal cannot
repair (the retained blobs are gone; under Never it fails naming
the missing blob). Pre-existing at the identity-based HEAD — the
window is in the two-phase design, not the lock rework.

Fix (review's preferred shape, concurred): root the acquisition —
every acquisition that will publish an unleased root (mount
registration; upper binding; digest-form RefPut; the
resolution-to-BeginOp prologue of an export pin) holds an op claim
pinning its digest from resolution to row write, exactly what
REQ-store-gc-roots' op pins exist for. Then "already rooted" holds
by construction and the spec's open-window sentence deletes.

Lands: 6 (liveness-locks plan — lease and ops on locks; op claims
become cheap enough to hold per-acquisition).
