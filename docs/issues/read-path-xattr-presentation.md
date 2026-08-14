# Read-path xattr presentation is unpinned

The projection fidelity table (docs/specs/projection.md
REQ-proj-fidelity) has no extended-attributes row: whether and how a
read-only projection presents a base entry's xattrs is uncontracted,
and no backend currently serves them. The writable spec's rule that
base-borne `user.ocifs.*` attributes are never presented
(REQ-writable-reserved) creates a read-only/writable asymmetry that
is only expressible once the read side's xattr presentation is
pinned — the reserved-namespace inertness should hold on both.

Lands: when a mount surface first serves extended attributes — the
writable arc's xattr chunk pins the writable side and should land
the fidelity-table row (including reserved-namespace inertness) for
the read side with it.
