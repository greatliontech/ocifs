// Package upper reads and writes the POSIX upper dialect
// (docs/specs/writable.md): an on-disk directory in OCI layer layout
// that is the single source of truth for a writable projection's
// local modifications. The walker turns a dialect tree into the
// abstract upper state — entries with overrides resolved and the
// machinery invisible — and the writer primitives produce dialect
// mutations as sequences of atomic filesystem steps whose orderings
// keep every crash prefix a valid dialect state
// (REQ-writable-crash). The same abstract state is what an OS-native
// dialect reader (ProjFS tombstones) will produce for the
// dialect-neutral commit.
package upper

import (
	"time"
)

// Dialect name and xattr namespaces (REQ-writable-dialect,
// REQ-writable-reserved).
const (
	// WhiteoutPrefix marks a deletion of the base entry sharing the
	// marker's basename remainder.
	WhiteoutPrefix = ".wh."
	// OpaqueMarker inside a directory hides all base content beneath
	// it.
	OpaqueMarker = ".wh..wh..opq"
	// TempPrefix names publish temporaries — inside the reserved
	// marker namespace the layer dialect drops, so a crash-orphaned
	// temporary is inert to presentation, unification, and commit.
	TempPrefix = ".wh..wh..tmp."

	// XattrNS is the reserved fidelity-override namespace: invisible
	// and unforgeable through the mount, inert on base content,
	// authored only by the provider.
	XattrNS = "user.ocifs."
	// XattrKind marks a stand-in and names its node kind: "char",
	// "block", "fifo", "socket", or "symlink".
	XattrKind = XattrNS + "kind"
	// XattrTarget carries a stand-in symlink's target verbatim.
	XattrTarget = XattrNS + "target"
	// XattrRdev carries a device stand-in's numbers as
	// "major:minor".
	XattrRdev = XattrNS + "rdev"
	// XattrOwner records ownership the host refused, as "uid:gid" —
	// one attribute, so the record lands atomically.
	XattrOwner = XattrNS + "owner"
	// XattrEscapePrefix + <real name> records an extended attribute
	// the host refused to store natively, value verbatim.
	XattrEscapePrefix = XattrNS + "xattr."
)

// Kind is an abstract upper entry's node kind.
type Kind uint8

const (
	KindFile Kind = iota
	KindDir
	KindSymlink
	KindFifo
	KindSocket
	KindCharDev
	KindBlockDev
)

// Rdev is a device stand-in's numbers.
type Rdev struct {
	Major uint32
	Minor uint32
}

// Entry is one abstract upper entry: the presented truth of a
// dialect node, overrides resolved, machinery stripped
// (REQ-writable-fidelity). HostPath locates the backing node for
// content access; for a stand-in it backs nothing but the record.
type Entry struct {
	// Path is the cleaned upper-root-relative path.
	Path string
	Kind Kind
	// Mode holds permission and setuid/setgid/sticky bits in unix
	// layout (0o7777 mask) — the dialect and tar are unix-native.
	Mode uint32
	// UID and GID are the presented owner — the override when one is
	// recorded, the host owner otherwise.
	UID int
	GID int
	// Size is the content size for files (0 for stand-ins), the
	// target length for symlinks.
	Size    int64
	ModTime time.Time
	// Target is a symlink's target verbatim (native or stand-in).
	Target string
	// Rdev is set for device kinds.
	Rdev Rdev
	// Nlink is the host link count — upper hardlinks share storage
	// and identity (REQ-writable-hardlink).
	Nlink uint64
	// Ino is the host inode number, the upper-born identity source
	// (projection.md REQ-proj-identity).
	Ino uint64
	// Xattrs are the presented extended attributes: native ones plus
	// escaped ones under their real names; never the machinery
	// namespace.
	Xattrs map[string]string
	// HostPath is the node's absolute path in the upper.
	HostPath string
	// StandIn reports that the node is a dialect stand-in rather
	// than a native node of its kind.
	StandIn bool
}

// State is the walked abstract upper: the dialect tree resolved into
// entries, whiteouts, and opaque directories, keyed by cleaned
// upper-root-relative path. It is the provider's rebuildable cache
// and commit's input — never authoritative (REQ-proj-upper-truth).
type State struct {
	// Entries maps path to the abstract entry.
	Entries map[string]Entry
	// Whiteouts holds every path a whiteout marker deletes.
	Whiteouts map[string]bool
	// Opaque holds every directory path carrying the opaque marker.
	Opaque map[string]bool
}
