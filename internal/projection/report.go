package projection

// ReportSink persists a projection report into the mount's
// bookkeeping record (REQ-proj-report; store.md
// REQ-store-bookkeeping mounts keyspace). Backends that accumulate
// residuals call it again with the grown report; publication is
// transactional at the sink.
type ReportSink func(Report) error

// Disposition classifies a report entry: the projection either
// omitted the view entry or presented it altered.
type Disposition string

const (
	DispositionOmitted Disposition = "omitted"
	DispositionAltered Disposition = "altered"
	// DispositionResidual records a read-only residual REQ-proj-ro
	// declares (ProjFS): the path is the foreign path, not a view
	// entry.
	DispositionResidual Disposition = "residual"
)

// Reason is the symbolic cause of an omission, alteration, or
// residual.
type Reason string

const (
	ReasonSymlinkUnsupported  Reason = "symlink-unsupported"
	ReasonFIFOUnsupported     Reason = "fifo-unsupported"
	ReasonDeviceUnsupported   Reason = "device-unsupported"
	ReasonCaseCollision       Reason = "case-collision"
	ReasonKindUnknown         Reason = "kind-unknown"
	ReasonNameUnrepresentable Reason = "name-unrepresentable"
	ReasonResidualForeignFile Reason = "residual-foreign-file"
)

// ReportEntry records one omission or alteration relative to the
// unified view (REQ-proj-report).
type ReportEntry struct {
	Path        string
	Disposition Disposition
	Reason      Reason
	Detail      string
}

// Report is the per-projection record of every entry omitted or
// altered relative to the unified view — never only logged, never
// silent (REQ-proj-report). It is built alongside the tree by the
// same classification, so the report is exactly the complement of
// what the projection presents.
type Report struct {
	Entries []ReportEntry
}

func (r *Report) add(path string, reason Reason, detail string) {
	r.Entries = append(r.Entries, ReportEntry{
		Path:        path,
		Disposition: DispositionOmitted,
		Reason:      reason,
		Detail:      detail,
	})
}
