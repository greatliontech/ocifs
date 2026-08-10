# ocifs — projection

A **projection** is a live filesystem presentation of a unified view
(`layer-semantics.md`): the same logical tree served through an
OS-specific backend — FUSE on linux, FSKit on macOS, ProjFS on
windows. This spec pins what all projections have in common and
exactly where they are allowed to differ; it is the contract that
keeps three backends one product. Export (`export.md`) is not a
projection — it is a one-shot materialization with its own contract.

**projection** (term): A live, backend-served filesystem presentation
of a unified view.

**backend** (term): The OS-specific mechanism serving a projection:
FUSE (linux), FSKit (darwin), ProjFS (windows).

**fidelity envelope** (term): The subset of recorded entry attributes
and entry kinds a given backend is capable of presenting.

**projection report** (term): The per-projection record of every
entry the projection omitted or altered relative to the unified view,
with the reason.

**upper** (term): The on-disk directory holding a writable
projection's local modifications; the unified view is its **lower**.

## One tree, declared differences

**REQ-proj-model** (behavior): Every backend MUST present the same
logical tree — the unified view, with implied directories
synthesized (as plain directories, `layer-semantics.md`) and any
consumer-configured extra directories added
(`api.md`) — differing from another backend only in ways the
fidelity envelope table below declares. An entry visible on one
platform and missing on another without a matrix row saying so is a
defect, not a platform quirk.

**REQ-proj-report** (behavior): Every omission or alteration a
projection makes relative to the unified view (unsupported symlinks,
devices, FIFOs, unresolvable hardlinks, case collisions, read-only
residual foreign files) MUST be recorded in the projection report,
enumerable by the consumer — never only logged, never silent. The
report is persisted in the store's per-mount state for every
projection, so any process — the in-process consumer, the
orchestrator of an out-of-process backend (REQ-proj-server), or an
inspecting CLI — reads the same record.

## Out-of-process serving

**REQ-proj-server** (behavior): A backend whose server runs as its
own OS-managed process (the FSKit appex, spawned by the platform's
extension machinery when the volume is mounted) MUST receive its
entire configuration — store location,
image identity, platform — declaratively at mount time through the
platform's mount/activate parameters; no live control channel
exists, unmount is the only control verb, and coordination with
orchestrating processes happens through shared store state (the
mount registry and projection report), never an RPC surface. On
darwin the appex runs app-sandboxed and can only open paths inside
its app-group container, so an appex-served projection requires the
store to live there — a location the sandboxed server and
unsandboxed consumers can both access.

## Identity

**REQ-proj-identity** (invariant): The numeric identity (inode
number, item ID) of every entry *present in the unified view* MUST
be a deterministic function of the view alone — never of visit
order, allocation history, or backend state — with the root at ID 2
and view entries assigned from 16 upward. In a writable projection,
a view path keeps its view-derived ID while the upper shadows it in
place — content or metadata replacing the same logical object
(identity follows the logical path-entry, not the storage) — while
a view path that is deleted in the upper and later recreated is a
new object and draws upper-born identity, exactly like paths absent
from the view, whose IDs come from a disjoint upper-born range. Because images are
immutable, the same image projects the same IDs across remounts;
unstable IDs break resumable enumeration cookies and any consumer
caching by inode. Path-addressed backends (ProjFS) have no IDs and
ignore this scheme.

## Enumeration

**REQ-proj-enumeration** (behavior): Directory enumeration MUST be
served from immutable sorted snapshots with resumable positions:
concurrent enumerations of one directory never disturb each other,
and a paused enumeration resumes exactly where it stopped. In a
read-only projection — the view being immutable — any
change-verifier a backend exposes is constant for the projection's
lifetime; a writable projection's verifiers change when the upper
mutates the directory. The sort comparator is the backend's own
(byte order on FUSE/FSKit; the platform comparator on ProjFS).

**REQ-proj-case** (behavior): On a backend whose namespace is
case-insensitive (ProjFS), view entries that collide under the
backend comparator MUST resolve deterministically — the first entry
in unified-view order wins, every loser goes in the projection
report. On a backend that can declare case sensitivity (FSKit), the
projection declares case-sensitive and no collision handling applies.

## Content

**REQ-proj-content** (invariant): The bytes readable from a projected
regular file MUST equal the content-CAS blob named by its view entry
— regardless of backend chunking, buffer alignment, or read offset
(short reads only at EOF). Backends with a content-versioning slot
(ProjFS placeholder ContentID) carry the entry's content digest in
it, so content-addressed refresh compares digests, never times.

## Read-only enforcement

**REQ-proj-ro** (invariant): A read-only projection MUST deny
mutation of every projected entry to the strongest degree its
backend affords: kernel `ro` on FUSE (full denial), a read-only
error from every mutating operation on FSKit (full denial), and
pre-operation vetoes on ProjFS — content (convert-to-full), name
(rename), existence (delete), hardlinking — which leave exactly two
declared residuals, both recorded in the projection report: creation
of *new* files inside the virtualization root, and metadata changes
(attributes, timestamps, ACLs) on projected placeholders — neither
has a deniable pre-operation on the platform. Residual foreign
files and metadata dirt never alter projected entries' *content* as
served.

## Fidelity envelopes

**REQ-proj-fidelity** (behavior, supersedes REQ-api-mount-attrs):
Each backend MUST present recorded
entry attributes and kinds exactly to the extent of its envelope in
the table below — outside the envelope, the table's stated omission
or default applies, and nothing is silently approximated beyond it.

| Aspect | FUSE (linux) | FSKit (darwin) | ProjFS (windows) |
|---|---|---|---|
| mode bits (incl. suid/sgid/sticky) | recorded | recorded | not representable — platform defaults |
| ownership (uid/gid) | recorded | recorded | not representable |
| timestamps | recorded | recorded | recorded (platform's four slots) |
| size | recorded | recorded | recorded |
| symlinks | verbatim target | verbatim target | only where the platform feature probe passes (2004+, NTFS root); otherwise omitted + reported |
| hardlinks | independent node, target's content (`layer-semantics.md`) | independent node | independent node |
| FIFOs | typed node | typed node | omitted + reported |
| char/block devices | typed node, no device numbers | typed node, no device numbers | omitted + reported |
| case | sensitive | declared sensitive | insensitive (REQ-proj-case) |
| ro enforcement | kernel | error from every mutating op | pre-op vetoes + two declared residuals (REQ-proj-ro) |

## Writable projections (forward contract)

Two write models exist, dictated by the backends' natures; the
detailed writable design (fs-native upper as truth, unprivileged
fidelity via extended attributes) is deferred work and becomes spec
when that stage starts. Two invariants are pinned now because the
projection architecture must not foreclose them:

**REQ-proj-upper-truth** (invariant): In both write models —
provider-mediated (FUSE, FSKit: the projection applies writes to a
POSIX upper directory itself) and OS-native (ProjFS: the
virtualization root *is* the upper; the OS writes to it directly and
the projection observes) — the on-disk upper MUST be the single
source of truth for local modifications, with any in-memory overlay
state a cache rebuildable from disk alone. An authoritative
in-memory record is unimplementable on ProjFS (the OS writes behind
the provider), so violating this forecloses windows. Lands: when the
writable stage first lands a write path.

**REQ-proj-commit-neutral** (invariant): Committing a writable
projection MUST produce a layer that is a pure function of the upper
state — the upper diff of added, modified, deleted, and
metadata-changed entries, read through each platform's dialect
(literal whiteout markers and fidelity xattrs on POSIX uppers;
tombstones and full files on ProjFS) — never of which backend or
write model produced that state. Equal upper states commit to
equal layers, up to each platform's declared fidelity envelope.
Lands: with the commit path of the writable stage.
