# ocifs — store

The store is ocifs's on-disk home for pulled OCI images: the original
OCI content, per-file extracted contents, reference cache, and mount
scaffolding. It is a cache: every byte in it is re-derivable from a
registry (or from the retained OCI content for the extraction tiers),
and wiping the store is safe whenever no mounts are live (live mounts
serve reads from the store and would see I/O errors).

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
indexes, manifests, configs, and compressed layer blobs, addressed by
their OCI digests), append-only except for garbage collection;
`blobs/<algorithm>/<hex>` — the content CAS, entries immutable once
written and shared freely across layers and images; `layers/<algorithm>/<hex>`
— layer indexes keyed by the layer digest the manifest lists;
`refs/<registry>/<repository>/<identifier>` — one plain file per
resolved reference (identifier = tag or digest) containing the digest
string of the top-level artifact it resolved to; `mounts/<id>` —
store-managed mountpoint directories; `exports/<algorithm>/<hex>` —
materialized root filesystems keyed by the digest of the manifest
actually materialized (behavioral contract in `export.md`).

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
re-unpacking the retained layer, with no network access. Only
content absent from `oci/` requires a pull.

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
index or a manifest; for an index, platform selection picks the
child. When the digest names a manifest directly and an explicit
platform is also requested, the manifest's config platform is
checked against it and a mismatch fails the request (strict, like
REQ-store-platform-strict). A digest-addressed request against fully
cached content
completes under `Never` with no network access. Fetching by digest
needs no signature machinery: every fetched byte is verified against
the requested digest (REQ-store-ingest-verified).

## Platform selection

**REQ-store-platform-strict** (behavior): When an explicit platform
is requested and the top-level artifact is an index, selection MUST
be strict: the index contains an exact match for the requested
platform, or the operation fails. No fallback, no closest match.

**REQ-store-platform-default** (behavior): When no explicit platform
is requested, the default MUST be the host's os/arch.

**REQ-store-platform-serves-child** (behavior): The platform-selected
child manifest's digest — not the index digest — MUST name the
materialized image: mounts, exports, and the config file all come
from the child. The reference cache records the top-level digest;
with the index retained in `oci/`, platform selection for a cached
reference is a local operation, so one cached resolution serves any
platform whose child is materialized.

## Concurrency

**REQ-store-single-writer** (behavior): The store assumes a single
writer process; cross-process locking and shared bookkeeping are out
of scope (`docs/issues/store-metadata-gmdb.md`). Concurrent pulls of
the same image within one process MUST NOT corrupt any tier:
identical content races benignly in the CAS, and a CAS entry is
published only by atomic rename of a fully written temporary.
