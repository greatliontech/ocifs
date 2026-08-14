# ocifs — verification seam

ocifs never verifies signatures. Trust decisions belong to
consumers; ocifs offers a **seam** where a consumer-supplied
verifier plugs in. This keeps signature machinery (sigstore, TUF
roots, policy) out of ocifs and its dependency graph permanently.

**verifier** (term): A consumer-supplied hook that judges a resolved
image identity against the consumer's trust policy.

## Contract

**REQ-seam-optional** (behavior): A consumer MAY configure a
verifier at construction. Without one, no verification occurs and
every resolvable image is served — ocifs is a trust-neutral
transport (digest integrity per `store.md` REQ-store-ingest-verified
holds unconditionally).

**REQ-seam-position** (invariant): The verifier MUST run after
manifest resolution and before content materialization: at the
point where the reference (or digest) has resolved to a concrete
top-level artifact, and no layer content has yet been unpacked,
served, mounted, or exported for this request. Cached content is
not exempt: the seam runs per request, whether or not the content
was already materialized (verifier-side caching is the verifier's
own concern). The store retains the resolved top-level artifact
(`store.md` REQ-store-ingest-order), so the seam's inputs are
available for cached content without network access. The seam
governs consumer acquisition through the library surface (`api.md`
REQ-api-acquire); platform-initiated serving of already-materialized
content — the darwin app extension — is downstream of an admitted
acquisition and runs no seam of its own.

**REQ-seam-input** (behavior): The verifier MUST receive the
resolved identity: the reference as requested, the resolved
top-level digest, and the top-level artifact's bytes. For a
multi-platform image this is the **image index** — the digest
signatures are made over; platform selection happens only after the
seam passes. A verifier whose policy targets per-platform child
manifests derives the child digests from the index bytes it is
given. The bytes are served from the store's retained copy under the
store's integrity boundary (`store.md` REQ-store-ingest-verified:
network arrivals are digest-verified, local reads are not
re-verified); a verifier whose policy requires digest–byte
correspondence hashes the bytes itself.

**REQ-seam-abort** (behavior): A verifier failure MUST abort the
request: no content is served and the reference cache records
nothing for the failed resolution. A reference-cache entry recorded
by an earlier successful resolution survives a later rejected
resolution unchanged; any serve through the surviving entry runs the
seam per REQ-seam-position. Verification failure is distinguishable
from resolution failure in the returned error.

## Non-goals

ocifs never ships a verifier implementation in its core module and
never depends on signature tooling; verifier implementations live
with consumers (or as separate modules) and satisfy the seam
structurally. The seam does not cover layer-content policy
(per-blob scanning) — the digest chain from the verified top-level
artifact already pins every blob (`store.md`
REQ-store-ingest-verified).
