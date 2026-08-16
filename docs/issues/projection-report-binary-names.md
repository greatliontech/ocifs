# Projection report mangles non-UTF-8 entry paths

`internal/projection/report.go` persists entry paths as plain JSON
strings (`Path string \`json:"path"\``). `encoding/json` replaces
invalid-UTF-8 bytes with U+FFFD on marshal, so a report row for an
entry whose tar name is not valid UTF-8 records a path that names no
entry in the image — the same encoding fault class the layer index
carried (fixed there with base64 fields and a format version).

Failure mode: pull an image whose layer contains an entry with a
non-UTF-8 name that the projection dispositions (e.g. an excluded
hardlink target); the written `projection-report.json` row's `path`
differs from the entry's actual bytes, so a consumer resolving the
report against the image misses or mismatches the entry.

Distinct from the layer-index fix: the report is a diagnostic
document for human/tooling consumption, not a serving-path cache —
corruption misleads audit, it does not corrupt served data. Not
covered by the index fix's escalation scope (different document,
different consumer contract; the narrower fix left no demonstrated
fault here on the serving path).

Lands: when the projection report gains a consumer that resolves
paths against image entries, or the next change set touching
`internal/projection/report.go`.
