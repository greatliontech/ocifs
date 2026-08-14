// Package projfsfs serves a projection kernel tree through Windows
// Projected File System — the windows backend glue
// (docs/specs/projection.md). This file is portable so the name
// rules the backend declares are testable on every platform; the
// provider itself builds only on windows/amd64.
package projfsfs

import (
	"strings"
	"unicode/utf16"
)

// ValidName reports whether the NTFS namespace can hold name as one
// path element (REQ-proj-fidelity: an entry whose name the platform
// cannot hold is omitted and reported). Rejected: NTFS-illegal
// characters (including path separators — kernel names are single
// elements), control bytes, trailing dot or space (silently stripped
// by Windows path normalization, which would fold distinct names),
// components beyond NTFS's 255-UTF-16-unit bound, and reserved
// device names as the first dot-segment (con, prn, aux, nul,
// com0–com9, lpt0–lpt9 — creatable nowhere).
func ValidName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c < 0x20:
			return false
		case c == '<', c == '>', c == ':', c == '"', c == '/', c == '\\', c == '|', c == '?', c == '*':
			return false
		}
	}
	if c := name[len(name)-1]; c == '.' || c == ' ' {
		return false
	}
	if len(utf16.Encode([]rune(name))) > 255 {
		// NTFS bounds one component at 255 UTF-16 units.
		return false
	}
	return !reservedDeviceName(name)
}

// reservedDeviceName reports whether the name's first dot-segment is
// a Windows reserved device name, case-insensitively.
func reservedDeviceName(name string) bool {
	seg, _, _ := strings.Cut(name, ".")
	seg = strings.ToLower(seg)
	switch seg {
	case "con", "prn", "aux", "nul":
		return true
	}
	if len(seg) == 4 && (strings.HasPrefix(seg, "com") || strings.HasPrefix(seg, "lpt")) &&
		seg[3] >= '0' && seg[3] <= '9' {
		return true
	}
	return false
}
