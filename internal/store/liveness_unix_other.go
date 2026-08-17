//go:build !linux && !windows

package store

import "golang.org/x/sys/unix"

// Dead evaluates recorded liveness on non-linux unix (darwin): the
// signal probe — ESRCH is the only death verdict, EPERM is
// exists-conservative. With no start-time or boot discriminators,
// PID reuse yields a false-live row (a leak, the conservative
// direction), never a false-dead one.
func (li LivenessIdentity) Dead() bool {
	err := unix.Kill(int(li.Pid), 0)
	return err == unix.ESRCH
}
