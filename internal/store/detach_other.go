//go:build !linux

package store

// detachStaleMount is a no-op off linux: the platforms' mount
// teardown is orchestrated by their own services.
func detachStaleMount(string) {}
