# ocifs — store

The store is ocifs's on-disk home for pulled OCI images: the original
OCI content, per-file extracted contents, reference cache, and mount
scaffolding. Its content tiers are a cache — every byte re-derivable
from a registry, or from the retained OCI content for the extraction
tiers — while per-mount state is ephemeral bookkeeping that dies with
its mount; wiping the store is safe whenever no mounts are live (live
mounts serve reads from the store and would see I/O errors).

**store root** (term): The configured work directory under which all
store state lives.

**top-level artifact** (term): The artifact a reference resolves to
before platform selection — an image index for a multi-platform
image, an image manifest otherwise.

**content CAS** (term): The store tier holding extracted regular-file
contents, keyed by the digest of the file's own bytes.

**layer index** (term): A JSON document recording, in tar order,
every entry of a layer's uncompressed tar: the full tar header plus,
for regular files, the content-CAS key of the entry's bytes.

## Disk layout

**REQ-store-layout** (wire): The store root MUST contain exactly
these persisted tiers: `oci/` — an OCI image layout per the OCI
image-layout spec (`index.json`, `oci-layout`, `blobs/` holding
indexes, manifests, configs, and layer blobs as distributed —
compressed for pulled images, uncompressed tar for locally committed
layers (`writable.md` REQ-writable-commit) — addressed by
their OCI digests), append-only except for garbage collection;
`blobs/<algorithm>/<hex>` — the content CAS, entries immutable once
written and shared freely across layers and images; `layers/<algorithm>/<hex>`
— layer indexes keyed by the layer digest the manifest lists;
`refs/<registry>/<repository>/<identifier>` — one plain file per
resolved reference at a fixed depth of exactly three encoded path
components: the registry (lowercased — DNS names are
case-insensitive), the whole repository path as a single component
(its `/` separators encoded like any other byte), and the identifier
(tag or digest), the file containing the digest string of the
top-level artifact the reference resolved to. Fixed depth means no
reference's directory chain is a prefix of another reference's file —
variable-depth nesting would let a tag file and a sub-repository
directory claim the same path. Within each component every byte
outside `[a-z0-9._-]` — plus any leading or trailing `.`, plus the
first byte of a Windows reserved device name (`con`, `prn`, `aux`,
`nul`, `com0`–`com9`, `lpt0`–`lpt9` as the first dot-segment) — is
percent-encoded as lowercase `%xx`; a component whose encoding would
exceed 200 bytes is instead stored as `%h` followed by the lowercase
hex SHA-256 of the raw component. Escaping `%` itself makes the plain
encoding injective, and `%h` begins no plain encoding (every plain
escape is followed by two hex digits), so the plain and hashed forms
never collide — hashed-form distinctness rests on the same collision
resistance the content CAS already assumes. Escaping a leading dot
keeps every component an ordinary path element (`.` and `..` never
reach path resolution, so no reference can address another tier);
escaping a trailing dot keeps distinct names distinct under Windows
path normalization, which silently strips them; reserved-name
escaping keeps every element creatable on Windows; the length bound
keeps every element within common 255-byte filesystem name limits;
the all-lowercase result avoids bytes not every supported filesystem
can hold (`:` in digest identifiers and registry ports) and cannot
collide under case folding; `mounts/<id>` —
per-mount state: the mount's bookkeeping (registration, projection
report — `projection.md`) beside a `mnt/` subdirectory serving as
the store-managed mountpoint when the caller supplies none — the
mountpoint is a sibling of the bookkeeping, never its parent, so a
live mount cannot shadow its own state; written only by that mount's
serving and orchestrating processes; `exports/<algorithm>/<hex>` — materialized
root filesystems keyed by the digest of the manifest actually
materialized (behavioral contract in `export.md`);
`uppers/<name>/upper` — store-managed writable uppers in the POSIX
upper dialect (`writable.md`), the name a single path element under
the mount-id rule (REQ-api-mount-id), with the upper's bookkeeping
(the base binding) as sibling files under `uppers/<name>/`, never
inside the dialect tree.

**REQ-store-adopt** (behavior): Store initialization MUST refuse a
work directory holding store state it does not recognize as this
layout — including stores written by ocifs versions predating the
tier split — with an error directing deletion; unrecognized state is
never adopted, migrated, or deleted, because the store destroys
nothing it cannot prove is its own cache (wiping is the user's
documented remedy). Recognition is by layout signature and therefore
best-effort: state that carries the signature is trusted, consistent
with the local-filesystem integrity boundary
(REQ-store-ingest-verified). The one incomplete signature treated as
ocifs's own is the OCI layout marker with no `index.json` beside it —
an interrupted first creation — which is completed in place with an
empty index; content tiers are never touched by the completion.

**REQ-store-ns** (invariant): Layer indexes and content-CAS entries
MUST occupy disjoint on-disk keyspaces; no path is ever interpreted
as both. A layer whose compressed bytes also occur as a regular file
*inside* some image (airgap bundles, embedded image tarballs)
produces the same hex digest for a layer index and a content blob,
and the two have different content — a shared keyspace corrupts every
consumer of the colliding key.

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
record the reference-cache entry **last**. A crash at any earlier
point leaves no ref entry, and the next pull re-runs ingest.

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
mounts serve linux root filesystems. Selection against an index
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

**REQ-store-single-writer** (behavior): The content tiers (`oci/`,
`blobs/`, `layers/`, `refs/`) assume a single ingesting process at a
time; any number of processes read them concurrently (projection
servers are ordinary readers — `projection.md` REQ-proj-server), and
per-mount state under `mounts/<id>` is written only by that mount's
own processes — ownership partitioning, not locking. Cross-process
ingest locking and shared transactional bookkeeping are deferred
until store bookkeeping moves to shared, cross-process-capable
storage. Concurrent pulls of the same image within one process MUST
NOT corrupt any tier: identical content races benignly in the CAS,
and a CAS entry is published only by atomic rename of a fully
written temporary.
