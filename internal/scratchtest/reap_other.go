//go:build !linux

package scratchtest

// reapMounts is a no-op off linux: the FUSE test mounts this sweep
// recovers from are created only by the linux suites.
func reapMounts(string) error { return nil }
