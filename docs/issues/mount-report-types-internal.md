# A mount's report is returned by a type a consumer cannot name

Lands: when a consumer first switches on a mount report's
disposition or reason (a caller of MountReport outside this module
that must be exhaustive over them)

`OCIFS.MountReport` returns `projection.Report`, a type of the
internal projection package: a consumer can hold it only by
inference, and cannot name `Report`, `ReportEntry`, `Disposition`,
`Reason` or their constants, so it cannot switch exhaustively on a
disposition or a reason as `projection.md` REQ-proj-report's consumer
contract implies. The collection's report had the same gap and is
now aliased at the root (`GCResult`, api.md REQ-api-gc); the mount
report's types want the same aliasing, with the constants
re-exported beside them. `GCOption` is an opaque option over an
internal type and needs nothing: consumers reach options only through
the constructors.
