//go:build !linux

package store

import "os"

// selfIdentity collects what this platform exposes of the liveness
// identity (REQ-store-bookkeeping); absent discriminators are zero
// and dead-detection treats them conservatively.
func selfIdentity() LivenessIdentity {
	return LivenessIdentity{Pid: int64(os.Getpid())}
}
