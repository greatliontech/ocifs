package ocifs

import (
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/store"
)

// LocalRef renders a committed image's digest as its acquirable
// reference under the store's local namespace (docs/specs/store.md
// REQ-store-local-images) — usable with Pull, Mount, and Export like
// any reference, never touching the network.
func LocalRef(h v1.Hash) string {
	return store.LocalRef(h)
}

type CommitOption func(*commitReq)

type commitReq struct {
	upperDir  string
	upperName string
	platform  *v1.Platform
}

// CommitWithUpperDir commits the caller-supplied upper directory;
// the caller owns the base pairing (docs/specs/writable.md
// REQ-writable-base-binding).
var CommitWithUpperDir = func(dir string) CommitOption {
	return func(r *commitReq) {
		r.upperDir = dir
	}
}

// CommitWithNamedUpper commits a store-managed upper; its base
// binding is validated against the resolved base image.
var CommitWithNamedUpper = func(name string) CommitOption {
	return func(r *commitReq) {
		r.upperName = name
	}
}

// CommitWithPlatform resolves the base with an explicit platform;
// selection semantics match PullWithPlatform.
var CommitWithPlatform = func(p v1.Platform) CommitOption {
	return func(r *commitReq) {
		r.platform = &p
	}
}
