package store

import (
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// LocalRegistry is the reserved local repository namespace: ocifs
// never consults the network for a reference under it, whatever the
// surrounding DNS makes of the name (REQ-store-local-images).
const LocalRegistry = "ocifs.local"

// LocalRef renders a committed image's digest as an acquirable
// reference under the local namespace.
func LocalRef(h v1.Hash) string {
	return LocalRegistry + "/commits@" + h.String()
}

// isLocalRef reports whether the registry is the reserved local
// namespace.
func isLocalRef(registry string) bool {
	return registry == LocalRegistry
}
