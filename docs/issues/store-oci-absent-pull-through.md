# Store: absent oci/ content with an intact ref has no pull path

A reference-cache entry whose compressed layer blob is missing from
`oci/` cannot be served: self-heal re-unpacks only from retained
`oci/` content, and no pull policy re-fetches — `IfNotPresent` and
`Never` return the cached resolution, `Always` HEAD-matches the
cached digest and returns before ingest. `Image()` then fails until
the ref file is deleted by hand or the store is wiped.

`docs/specs/store.md` REQ-store-self-heal pins the contract: when the
pull policy permits network access, the store re-fetches exactly the
missing blobs by digest through the cached resolution — never by tag
re-resolution — and resumes the heal; under `Never`, the heal fails
identifying the missing blob. The clause is spec-tier only until the
digest-addressed fetch machinery exists to implement it.

Lands: 4
