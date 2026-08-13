package projection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/greatliontech/ocifs/internal/atomicfile"
)

// ReportFileName is the projection report's file name inside the
// mount's per-mount state directory (REQ-proj-report,
// store.md REQ-store-layout mounts tier).
const ReportFileName = "projection-report.json"

// Disposition classifies a report entry: the projection either
// omitted the view entry or presented it altered.
type Disposition string

const (
	DispositionOmitted Disposition = "omitted"
	DispositionAltered Disposition = "altered"
)

// Reason is the symbolic cause of an omission or alteration.
type Reason string

const (
	ReasonSymlinkUnsupported Reason = "symlink-unsupported"
	ReasonFIFOUnsupported    Reason = "fifo-unsupported"
	ReasonDeviceUnsupported  Reason = "device-unsupported"
	ReasonCaseCollision      Reason = "case-collision"
	ReasonKindUnknown        Reason = "kind-unknown"
)

// ReportEntry records one omission or alteration relative to the
// unified view (REQ-proj-report).
type ReportEntry struct {
	Path        string      `json:"path"`
	Disposition Disposition `json:"disposition"`
	Reason      Reason      `json:"reason"`
	Detail      string      `json:"detail,omitempty"`
}

// Report is the per-projection record of every entry omitted or
// altered relative to the unified view — never only logged, never
// silent (REQ-proj-report). It is built alongside the tree by the
// same classification, so the report is exactly the complement of
// what the projection presents.
type Report struct {
	Entries []ReportEntry `json:"entries"`
}

func (r *Report) add(path string, reason Reason, detail string) {
	r.Entries = append(r.Entries, ReportEntry{
		Path:        path,
		Disposition: DispositionOmitted,
		Reason:      reason,
		Detail:      detail,
	})
}

// WriteFile persists the report atomically at path. The entries
// array is always present — an empty report is `{"entries":[]}`,
// distinguishable from an absent or unwritten file.
func (r Report) WriteFile(path string) error {
	entries := r.Entries
	if entries == nil {
		entries = []ReportEntry{}
	}
	data, err := json.MarshalIndent(Report{Entries: entries}, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(path, bytes.NewReader(data), 0o644)
}

// ReadReportFile loads a persisted projection report — the read
// surface for consumers, orchestrators, and inspecting CLIs
// (REQ-proj-report).
func ReadReportFile(path string) (*Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("projection report %s: %w", path, err)
	}
	return &r, nil
}
