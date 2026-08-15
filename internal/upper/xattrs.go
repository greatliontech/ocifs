package upper

import (
	"archive/tar"
	"strings"
)

// PresentedBaseXattrs returns a base entry header's presented
// extended attributes — PAX SCHILY records and legacy header xattrs
// under their real names — with the reserved machinery namespace
// stripped: reserved names on base content are inert, never
// presented and never read as dialect records
// (docs/specs/writable.md REQ-writable-reserved). Both the merge
// presentation and commit's base comparison read base xattrs
// through this one rule.
func PresentedBaseXattrs(h *tar.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h.PAXRecords {
		if name, ok := strings.CutPrefix(k, "SCHILY.xattr."); ok && !strings.HasPrefix(name, XattrNS) {
			out[name] = v
		}
	}
	for k, v := range h.Xattrs { //nolint:staticcheck // legacy producers
		if !strings.HasPrefix(k, XattrNS) {
			out[k] = v
		}
	}
	return out
}
