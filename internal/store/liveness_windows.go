//go:build windows

package store

import "os"

// Dead evaluates recorded liveness with what windows exposes: the
// handle probe — an unopenable pid is absent. With no start-time or
// boot discriminators, PID reuse yields a false-live row (a leak,
// the conservative direction), never a false-dead one.
func (li LivenessIdentity) Dead() bool {
	p, err := os.FindProcess(int(li.Pid))
	if err != nil {
		return true
	}
	_ = p.Release()
	return false
}
