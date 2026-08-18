# ocifs — store

The store is ocifs's on-disk home for pulled OCI images: the original
OCI content, per-file extracted contents, and everything ocifs
records about them. State divides by consumer: **filesystem tiers**
hold bytes the kernel or foreign tools consume directly (extracted
contents served to mounts, export trees, upper dialect trees,
mountpoint directories) or whose format a wire contract pins (the
OCI image layout); the **bookkeeping database** holds every record
only ocifs interprets — reference cache, layer indexes, mount
registry and per-mount records, upper base bindings, local-image
records, and collection state. Content tiers are a cache — every
byte re-derivable from a registry, or from retained OCI content —
and every bookkeeping record except upper base bindings is a cache
or derivable (references re-resolve, indexes re-unpack, registry
rows are liveness-scoped, collection state re-marks); the binding
is the store's one irreplaceable record. Wiping the store is safe
whenever no mounts are live and no upper's future commit matters
(live mounts serve reads from the store and would see I/O errors).

**store root** (term): The configured work directory under which all
store state lives.

**top-level artifact** (term): The artifact a reference resolves to
before platform selection — an image index for a multi-platform
image, an image manifest otherwise.

**content CAS** (term): The store tier holding extracted regular-file
contents, keyed by the digest of the file's own bytes.

**layer index** (term): A bookkeeping record listing, in tar order,
every entry of a layer's uncompressed tar: the tar header's
metadata fields plus, for regular files, the content-CAS key of the
entry's bytes. Header strings are arbitrary bytes (names, link
targets, xattr keys and values may be non-UTF-8 or binary) and
round-trip byte-exactly.

**bookkeeping database** (term): The single gmdb database under the
store root holding every ocifs-interpreted record, transactional
and cross-process (one writer, any readers, per gmdb's own
coordination).

## Disk layout

**REQ-store-layout** (wire): The store root MUST contain exactly
these persisted tiers: `oci/` — an OCI image layout per the OCI
image-layout spec (`index.json`, `oci-layout`, `blobs/` holding
indexes, manifests, configs, and layer blobs as distributed —
compressed for pulled images, uncompressed tar for locally committed
layers (`writable.md` REQ-writable-commit) — addressed by
their OCI digests), append-only except for garbage collection;
`blobs/<algorithm>/<hex>` — the content CAS, entries immutable once
written and shared freely across layers and images;
`mounts/<id>/mnt` — the store-managed mountpoint directory when the
caller supplies none, and nothing else under `mounts/<id>` (every
mount record lives in the bookkeeping database);
`exports/<algorithm>/<hex>` — materialized root filesystems keyed
by the digest of the manifest actually materialized (behavioral
contract in `export.md`); `uppers/<name>/upper` — store-managed
writable uppers in the POSIX upper dialect (`writable.md`), the
name a single path element under the mount-id rule
(REQ-api-mount-id), nothing beside the dialect tree (the base
binding lives in the bookkeeping database); `locks/` — the claim
lock files of held-lock liveness, one per claim, empty files whose
advisory locks are the store's only liveness authority; and
`bookkeeping/` — the bookkeeping database file and its
coordination artifacts, owned entirely by gmdb. A tier earns
filesystem residence only by
direct kernel/foreign-tool consumption or a wire contract; every
record only ocifs interprets lives in the bookkeeping database,
whose keys and values carry arbitrary bytes exactly.

**REQ-store-bookkeeping** (wire): The bookkeeping database MUST hold
exactly these keyspaces, keys and values byte-exact: `refs` — key:
registry (lowercased — DNS names are case-insensitive), repository, and
identifier (tag or digest), joined by `0x00` (a byte no reference
component can carry); value: the digest string of the resolved top-level
artifact.
`layeridx` — key: the layer digest the manifest lists, as `<algorithm>
0x00 <hex>`; value: the layer index, a versioned binary record
round-tripping every header string byte-exactly; a value whose version
is foreign to the reader is unparseable state healing as an absent index
(REQ-store-self-heal).
`mounts` — key: the mount id; value: a versioned record of the serving
process's diagnostic identity, the image digest served, the upper name
when the mount is writable over a store-managed upper (the arbitration
and removal-refusal witness — `writable.md` REQ-writable-base-binding,
`api.md` REQ-api-remove), the mountpoint path, the projection report
(`projection.md` REQ-proj-report), and a publication flag distinguishing
a registered-but-not-yet-published report from a published clean one —
without it an inspector reading the row mid-mount takes "no omissions"
from a report that does not exist yet, the silent-wrong answer
REQ-proj-report forbids. Every fresh registration starts unpublished (a
remount is a fresh registration and publishes anew); publication sets
the flag atomically with the report it marks and is idempotent, and
within one registration the flag is monotone — only a fresh
registration of the id clears it.
`ops` — key: an operation id; value: a versioned record of an in-flight
extra-transactional operation (an export materialization, a commit, a
sweep) — the owner's diagnostic identity, the digests the operation pins
(roots while the op's claim lock is held — REQ-store-gc-roots), and the
temporary paths it owns (exempt from sweeps while the lock is held,
swept as debris once it is not, wherever they live — including a
caller-target export's temporary in the caller's own parent directory).
The ingest lease is not a row at all: it is the `locks/ingest` lock
itself (REQ-store-single-writer).
`uppers` — key: the upper name; value: the base binding — the digest of
the image the upper was first mounted over (`writable.md`
REQ-writable-base-binding).
`localimages` — key: `<algorithm> 0x00 <hex>` of a committed manifest;
value: a versioned creation record. Commit writes the row; it is the
root that keeps a committed image reachable (REQ-store-gc-roots) until
explicitly removed (`api.md` REQ-api-remove).
`gc` — collection bookkeeping: first-seen times grounding the retention
grace, and the condemned set (REQ-store-gc-safe). Losing this keyspace
is safe in one direction only: a re-mark re-derives first-seen
conservatively later (extending retention, never shrinking safety), and
a lost condemned set merely aborts an in-progress sweep.
**Held-lock liveness** (every claim: `mounts` and `ops` rows, the ingest
lease, a named upper's writable serve): a claim is alive exactly while
its holder keeps an advisory file lock on the claim's lock file under
`locks/` — `mount-<id>`, `upper-<name>`, `op-<id>`, `ingest`. The kernel
releases the lock at process death (SIGKILL included), and any
participant in the store's locking domain — the set of openers among
whom the filesystem makes advisory locks conflict; one host's processes
opening the same filesystem are one domain whatever their namespaces
(locks are inode-scoped, so bind mounts and container volumes share
them), but a stacked view of the store — an overlay upper, a passthrough
FUSE view — has its own inodes and is a different filesystem, out of
contract exactly like the network split below — can pass verdict by
try-lock: blocked means live — a frozen process still holds and is still
live; acquired means dead, and the acquisition IS the claim, one atomic
act, so no verdict can go stale between judging and acting. A store
shared beyond one locking domain (a network filesystem whose locks are
client-local) is out of contract: two domains never see each other's
locks, and each would judge the other's live claims dead. Process
identity (pid and friends) may ride rows as diagnostic data but never
decides. No clock, no heartbeat, no name ever decides death: names are
namespace-scoped and clocks misjudge frozen processes — both yield the
false-dead verdict that deletes content a live process serves, the
unrecoverable direction; a held lock yields neither. Lock-file
discipline: claim fds are close-on-exec and never placed on a child's
inheritance list, so no exec'd child ever carries a claim past its
parent (a fork that never execs shares the open description until it
exits — a bounded false-live, the safe direction); acquisition verifies
identity after locking (the locked descriptor's file identity compared
against the path's — a mismatch means the file was unlinked and
recreated underfoot: close and retry); and a lock file is unlinked only
while its lock is still held, by a holder whose claim has semantically
ended, and always BEFORE release — an unlink after release races a fresh
acquirer of the old inode into a second-holder state the identity verify
cannot catch, and an unheld file is never unlinked by anyone. A dead
claim's acquirer is that claim's final holder: it disposes the claim's
residue and unlinks the lock file before releasing (deferring a
reclamation instead releases without unlink — its file and row persist
for retry; a platform that refuses unlinking open files merely defers
the unlink step — the lock, not the file's absence, is the authority).
Rows are written only AFTER their vouching lock is held, and blocking
claim acquisition happens only outside bookkeeping write transactions —
inside a write transaction a claim is consulted by non-blocking try-lock
only, because a holder may be waiting on that very transaction's write
grant to record its row, and a blocking wait under the grant would cycle
with it.
Version discipline: every value whose shape can evolve carries a version
discriminator, and a foreign version is handled exactly as that
keyspace's absent-row case — never a hard failure for regenerable
records. A foreign-version `mounts` row's LIVENESS is still judgeable
(the lock has no format): dead, it reclaims like any dead row; live, it
is a live mount whose image this binary cannot read, and image-tier
collection halts visibly (REQ-store-gc-collect) — the one irreducible
foreign-version conservatism.

**REQ-store-adopt** (behavior): Store initialization MUST refuse a
work directory holding store state it does not recognize as this
layout — including stores written by ocifs versions predating the
bookkeeping database — with an error directing deletion, and
refusing a filesystem whose advisory locking is unsound (probed at
open: a second open file description's try-lock against a held
lock conflicts, or the store is refused) — held-lock liveness is
the store's only liveness authority, and a filesystem that grants
two holders would let a sweeper judge every live claim dead. The
probe establishes local soundness only; it cannot see other hosts,
so it never certifies a multi-host locking domain — single-domain
sharing is the stated precondition, not a probed one.
Unrecognized state is never adopted, migrated, or deleted, because
the store destroys nothing it cannot prove is its own cache
(wiping is the user's
documented remedy). Recognition is by layout signature and therefore
best-effort: state that carries the signature is trusted, consistent
with the local-filesystem integrity boundary
(REQ-store-ingest-verified). The one incomplete signature treated as
ocifs's own is the OCI layout marker with no `index.json` beside it —
an interrupted first creation — which is completed in place with an
empty index; content tiers are never touched by the completion.

**REQ-store-ns** (invariant): Layer indexes and content-CAS entries
MUST occupy disjoint keyspaces; no key is ever interpreted as both.
A layer whose compressed bytes also occur as a regular file
*inside* some image (airgap bundles, embedded image tarballs)
produces the same hex digest for a layer index and a content blob,
and the two have different content — a shared keyspace corrupts
every consumer of the colliding key. The split is structural: the
index lives in the `layeridx` bookkeeping keyspace, the blob in the
`blobs/` tier.

**REQ-store-cas-content** (invariant): The bytes stored at content-CAS
key `h` MUST hash to `h`. A corrupted or misplaced write would serve
wrong file content to every image sharing the blob.

## Ingest

**REQ-store-ingest-order** (behavior): Ingest MUST proceed: resolve
the reference per pull policy; fetch and retain the top-level
artifact in `oci/` (for an index: the index itself alongside the
platform-selected child); append manifest(s), config, and compressed
layers to `oci/`; unpack every layer of the selected manifest
(regular-file bytes into the content CAS, then the layer index); and
record the `refs` row **last**. A crash at any earlier point
leaves no ref row, and the next pull re-runs ingest.

**REQ-store-ingest-idempotent** (behavior): Re-running ingest for
already-present content MUST be a no-op: appending retained OCI
content never duplicates `index.json` descriptors, and rewriting
CAS or layer-index entries with identical content is harmless.

**REQ-store-ref-complete** (invariant): A reference-cache entry MUST
name a top-level digest whose artifact is retained in `oci/`, with
every platform served through the entry fully materialized: child
manifest, config, and compressed layers present in `oci/`, and layer
indexes and content blobs derivable locally (possibly via
self-heal). A ref written before unpack completes would serve a
mount that hits a missing blob mid-read.

**REQ-store-self-heal** (behavior): A missing or unreadable layer
index (or missing content blob) for an image whose compressed layers
`oci/` retains MUST NOT be fatal: the store re-derives it by
re-unpacking the retained layer, with no network access. Content
absent from `oci/` itself is beyond local re-derivation: when the
pull policy permits network access, the store re-fetches exactly the
missing blobs by digest through the cached resolution — never by tag
re-resolution — and resumes the heal; under `Never`, the heal fails
identifying the missing blob.

## Trust model

**REQ-store-ingest-verified** (invariant): Content arriving from the
network MUST be digest-verified before it is persisted: indexes,
manifests, configs, and layers are validated against the digests
that name them, and an object failing verification is never written
to the store. Content already in the store is trusted; local reads
are not re-verified — the store's integrity boundary is the local
filesystem. The store performs no signature verification
(`verification-seam.md`).

## Pull policies

**REQ-store-pull-policy** (behavior): The store MUST implement three
pull policies. `IfNotPresent`: a cached ref resolution is used
without re-resolving the reference; a requested platform whose child
is not yet materialized is pulled by digest through the cached index
(no tag re-resolution); an uncached reference is pulled. `Always`: a
HEAD request revalidates the cached resolution; cached content is
used iff the remote digest matches the cached **top-level** digest
(top-level to top-level — a HEAD on a multi-platform reference
returns the index digest); otherwise pull. `Never`: cached content
only, no network access; a reference with no cached resolution, or a
platform not materialized locally, is an error.

## Digest-addressed entry

**REQ-store-digest-entry** (behavior): Given (repository, digest,
platform), the store MUST materialize the image without any tag
re-resolution — the digest is the identity. The digest can name an
index or a manifest; selection and the direct-manifest platform
check follow REQ-store-platform-strict and
REQ-store-platform-default. A digest-addressed request against fully
cached content
completes under `Never` with no network access. Fetching by digest
needs no signature machinery: every fetched byte is verified against
the requested digest (REQ-store-ingest-verified).

**REQ-store-local-images** (behavior): Images the store itself
produces (committed writable uppers — `writable.md`
REQ-writable-commit-image) MUST be acquirable by digest under the
local repository namespace `ocifs.local/…`, whose content is by
construction fully materialized. The reservation is behavioral:
ocifs never consults the network for a reference under this
namespace, whatever the surrounding DNS makes of the name (a missing
piece is store damage, failing as under `Never`) — so a local
identity can never be served, or shadowed, by a remote registry.
The rule overrides the pull policy wholesale: under `Always` a local
reference undergoes no revalidation — there is no remote to
revalidate against.

## Platform selection

**REQ-store-platform-strict** (behavior): When an explicit platform
is requested, selection MUST be strict. Against an index, a child
matches when it carries a platform whose value equals the request's
in every field the request specifies (os, architecture, variant,
os.version; an unspecified field constrains nothing), and exactly
one child must match: zero matching children fail the operation, and
more than one fails it as underspecified — choosing among them would
be a fallback. Children carrying no platform (attestation and other
non-platform entries) never match. When the top-level artifact is a
manifest, the manifest's config platform is checked against the
request by the same field rule and a mismatch fails the operation.
No fallback, no closest match, no normalization of platform names.

**REQ-store-platform-default** (behavior): When no explicit platform
is requested, the request MUST use the configured default platform
(REQ-api-construction), which itself defaults to the host's os/arch
— except on darwin, where the fallback is `linux` with the host's
architecture: an `os=darwin` request could never match published
images (no darwin container-image ecosystem exists), and darwin
mounts serve linux root filesystems. On a 32-bit arm host the
built-in default additionally carries the host's detected CPU
variant (`v5`/`v6`/`v7`, from the kernel's reported CPU
architecture; 32-bit userland on v8 hardware detects as `v7`):
without it, `linux/arm` matches both `arm/v6` and `arm/v7` children
of standard indexes and every default pull fails as ambiguous,
while any variant preference chosen without host knowledge would
silently select binaries that trap on older hardware. Detection
failing yields no variant — the strict rule's loud ambiguity
failure, never a guess. Explicit and configured platforms are used
exactly as given. Selection against an index
follows the same match rule as an explicit request; a top-level
manifest is served as-is — only an explicit platform constrains a
direct manifest.

**REQ-store-platform-serves-child** (behavior): The platform-selected
child manifest's digest — not the index digest — MUST name the
materialized image: mounts, exports, and the config file all come
from the child. The reference cache records the top-level digest;
with the index retained in `oci/`, platform selection for a cached
reference is a local operation, so one cached resolution serves any
platform whose child is materialized.

## Concurrency

**REQ-store-single-writer** (behavior): One ingesting process
mutates the content tiers (`oci/`, `blobs/`) at a time, enforced
through the ingest lease — the held `locks/ingest` claim lock —
held from the first content-tier write through the commit of the
root row (the `refs` row for a pull, the `localimages` row for a
commit: a commit is an ingest under the lease, row-last, exactly
like a pull). The span matters: a root published outside the lease
would leave a window where the just-written content is unrooted
and a fenceless sweep could collect it. A crashed holder's lease
releases with its process (held-lock liveness); a second
acquisition waits, cancellably, and proceeds the moment the kernel
frees the lock — distinct open file descriptions exclude each
other in-process exactly as across processes. Any number of
processes read every tier and keyspace
concurrently (projection servers are ordinary readers —
`projection.md` REQ-proj-server), each through per-operation read
transactions — a held long-lived snapshot obstructs database
maintenance, and reader slots are a finite coordination resource.
Concurrent pulls of the same image MUST NOT corrupt any tier —
in-process or across processes: identical content races benignly
in the CAS, and a CAS entry is published only by atomic rename of
a fully written temporary. Bookkeeping writes are transactional
per the database's own single-writer coordination.

## Mount registry and reclamation

**REQ-store-mount-registry** (behavior): Every mount MUST acquire its
claim lock and register in the `mounts` keyspace before serving, and
deregister on unmount — row removed, lock file unlinked while the lock
is still held, then the lock released, in that order per the lock-file
discipline. A row whose claim lock a sweep can acquire is a dead mount,
and holding that acquisition the sweep reclaims — fallible steps first:
for a store-managed mountpoint, best-effort detaching any stale kernel
mount and removing the `mounts/<id>` directory; only on full success
removing the row, then unlinking the claim's lock file while held and
releasing, as the claim's final holder. Where detach or removal fails (a
foreign-user FUSE mount, a busy mountpoint) the sweep releases without
unlink — row and lock file stay intact — and reclamation retries later:
deferral, never a half-reclaimed id. A racing remount of the id blocks
on the very lock the sweep holds, so nothing can serve paths
mid-reclamation. On clean unmount the row and report go; the
store-managed mountpoint directory remains for the consumer that just
held it (`api.md` REQ-api-mountpoint) and is thereafter store
scaffolding owned by no row — collectible like any orphaned tier file
once the retention grace passes. A caller-supplied mountpoint is the
caller's property; no sweep touches it. A mount id is reusable once its
row is gone: a rowless state directory is store scaffolding the next
mount of that id adopts (its mountpoint verified empty), never a refusal
— waiting for collection to remove an empty directory would block
remounting an id its own unmount just released. A dead row's id becomes
reusable through reclamation; a caller-supplied id whose mount attempt
failed before serving leaves no row and is immediately reusable.

## Garbage collection

**REQ-store-gc-roots** (behavior): The reachable set MUST be computed
from exactly these roots: every `refs` row's top-level digest —
digest-addressed acquisition records a `refs` row (identifier = the
digest) like any acquisition, so every acquired image is rooted; every
`localimages` row's manifest digest; every live `mounts` row's image
digest; every `uppers` row's base-binding digest; and every live `ops`
row's pinned digests (an in-flight export or commit roots the content it
reads). From a root, reachability follows the OCI graph: index → child
manifests → config and layers → layer indexes → content-CAS entries.
Everything else is garbage: unreferenced `oci/` blobs, content-CAS
entries, `layeridx` rows, dead `mounts` and `ops` rows (a dead op's row
and its owned temporaries go together), stale condemned rows of dead
sweepers, exports tier entries whose manifest digest is unreachable,
temporaries and `mnt` directories owned by no live row, and orphaned
tier files no bookkeeping row names — except files under `locks/`, which
are never unlinked unheld: a lock file is legitimately rowless while
held (the ingest lease has no row at all, and lock-before-row means
every claim is rowless mid-acquire), and unlinking does not release a
held lock — a recreated path would grant a second holder over a live
claim. Lock files leave the store only through the holder-unlink
discipline (held-lock liveness): a sweep disposes of a dead claim's lock
file exactly by acquiring it first — for each `locks/` file, try-lock;
acquired means the claim is dead, and once its residue is disposed the
sweep, as final holder, unlinks before releasing (the ingest file
included — the next acquirer recreates it; a dead claim whose
reclamation defers keeps its file with its surviving row); blocked means
live, untouched. A temporary owned by a live `ops` row is never garbage
— REQ-export-atomic's "stale temporaries are inert" holds only for
temporaries whose owner is dead.

**REQ-store-gc-safe** (invariant): Collection MUST be safe at any
moment, whoever triggers it. The mark reads roots transactionally.
Deletion is two-phase through the condemned set: the sweep is
itself an `ops` operation, and inside one write transaction it
re-checks reachability against fresh roots and records the digests
it will delete in `gc`, each condemned row carrying the sweeping
op's id — a condemned row whose sweeper's claim lock is
acquirable binds nobody and is debris (a crashed sweeper must not
wedge publication forever; the try-lock verdict is held-lock
liveness like any other), and the judge that acquires it clears
those debris rows and unlinks the dead sweeper's lock file before
releasing (its final holder — held-lock liveness) — while the
probe is held, a concurrent consult reads the dead sweeper as live
and backs out needlessly, so the first judge leaves nothing for
later consults to meet.
Every root-publishing write (a `refs`, `localimages`, `mounts`,
`uppers`, or digest-pinning `ops` row) consults the live-sweeper
condemned set in its own write transaction and backs out when its
digest is condemned — backing out to the operation's acquisition,
not the row write: re-verify presence and re-ingest under the
lease if the content is gone, because a bare row-retry after the
sweep clears would publish a root over vanished content. The
database's serialized writer makes sweep and publication mutually
exclusive; after the files are gone the sweep clears its condemned
rows. Ingest and collection additionally exclude each
other through the ingest lease, which spans root publication
(REQ-store-single-writer), so no content is ever both written and
unrooted outside a fence. Violation: a sweep racing an ingest,
commit, or mount-start deletes a blob the winner then serves — a
live mount reading vanished content.

**REQ-store-gc-collect** (behavior): Collection MUST run
automatically at the transitions that create garbage — a `refs` row
overwritten by re-resolution, a reference or local image removed
(`api.md` REQ-api-remove), an unmount, and store initialization
(crash debris: dead mount and ops rows, `.export-*` temporaries
owned by no live row, orphaned tier files per
REQ-store-gc-roots) — and on explicit demand
(`api.md` REQ-api-gc). Automatic collection is on by default and
disableable at construction. Unreachable content younger than the
configured retention grace (default 24h) is retained — blobs are
content-addressed and shared, so unreachable content still
deduplicates a future pull; the grace is pure retention policy and
never load-bearing for safety (REQ-store-gc-safe's fences and the
`ops` roots are — every window in which content is legitimately
unrooted sits inside a fence or a live `ops` row, with or without
grace). Explicit collection may ignore the grace on demand. A
transition's collection runs after the transition's own lease span
has ended —
the sweep acquires the lease itself, and a collection started while
its trigger still holds the lease would deadlock on it. The
debris sweep reclaims what dead rows own wherever it lives: a dead
`ops` row's recorded temporaries (including a caller-target
export's temporary outside the store), dead `mounts` rows and
their directories. Collection removes bookkeeping
rows and their files together; a crash between leaves either an
orphaned file (swept as such next collection) or a rowless state
self-heal already treats as absent — never served corruption.
