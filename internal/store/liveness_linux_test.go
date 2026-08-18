//go:build linux

package store

import (
	"testing"

	"golang.org/x/sys/unix"
)

// TestLivenessVerdicts pins each arm of the Dead() rule
// (REQ-store-bookkeeping): live self; PID reuse (same pid, foreign
// start time) dead; absent pid dead; foreign boot dead; same-boot
// foreign namespace conservatively live.
func TestLivenessVerdicts(t *testing.T) {
	self := selfIdentity()
	if self.Dead() {
		t.Fatal("own identity judged dead")
	}
	reused := self
	reused.StartTime = self.StartTime + 1
	if !reused.Dead() {
		t.Fatal("PID reuse (start-time mismatch) judged live")
	}
	absent := self
	absent.Pid = 1<<30 - 3
	if !absent.Dead() {
		t.Fatal("absent pid judged live")
	}
	otherBoot := self
	otherBoot.BootID = "not-this-boot"
	if !otherBoot.Dead() {
		t.Fatal("foreign boot judged live")
	}
	foreignNS := absent
	foreignNS.PidNS = "pid:[1]"
	if foreignNS.Dead() {
		t.Fatal("same-boot foreign-namespace row judged dead")
	}
	// A live foreign-user pid answers the signal probe with EPERM —
	// exists, unjudgeable further, conservative live. pid 1 is the
	// canonical foreign-user process for an unprivileged run.
	if err := unix.Kill(1, 0); err == unix.EPERM {
		init := self
		init.Pid = 1
		init.StartTime = 0
		if init.Dead() {
			t.Fatal("EPERM (live foreign-user pid) judged dead")
		}
	} else {
		t.Log("EPERM arm skipped: kill(1,0) did not return EPERM here")
	}
}
