// Package layer is the semantic core of ocifs: it turns a stack of
// OCI layers into the unified view every consumer (mount backends,
// export, commit) presents. The contract it implements is
// docs/specs/layer-semantics.md; this comment records the mechanism
// choices the spec deliberately leaves to the code.
//
// Unification is implemented bottom-up, base layer first, as a direct
// transcription of sequential tar extraction: each layer is applied
// to a mutable tree in two passes — whiteout markers first (scoped to
// the state built from lower layers), then content entries in tar
// order. The two-pass shape makes whiteouts-first structural: a
// marker can never observe or affect its own layer's entries, so
// marker/content ordering inside a layer cannot matter. Earlier
// implementations were top-down with shadow-masking — an optimization
// of these semantics — and every defect found in them lived in the
// masking bookkeeping; at the entry counts of real images the
// optimization bought nothing, so it is gone.
//
// The package is pure: no filesystem, no network, no store. Entries
// name regular-file content by digest, and resolving a digest to
// bytes is the consumer's concern. Headers are held by value; input
// slices are never mutated.
//
// The unified view is complete: directories a layer only implied
// (by placing entries beneath them) are materialized as synthesized
// 0755 directory entries, because extraction makes implied
// directories real and one emptied by a later marker still exists —
// no consumer could reconstruct that from surviving entries. The
// root-entry ("." in the view) and sort-order contracts live in the
// spec, not here.
package layer

import (
	"archive/tar"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Entry is one filesystem object in a layer or in the unified view.
// Header is held by value, so unification never mutates caller
// memory — but tar.Header contains maps (PAXRecords, Xattrs) that
// value copies share; the package treats them as read-only. Name is
// the entry's cleaned, root-relative path once the entry is part of
// a unified view.
type Entry struct {
	Header tar.Header
	// Digest is the content-CAS key of the entry's bytes; zero for
	// anything but regular files.
	Digest v1.Hash
}

// Layer is one layer's recorded filesystem entries in tar order.
// Tar metadata entries (PAX extended/global headers, GNU long-name
// records) are not filesystem entries and are ignored if present.
type Layer []Entry
