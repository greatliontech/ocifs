package store

import (
	"golang.org/x/sys/unix"

	"fmt"
	"os"
	"strings"
)

// Dead evaluates the recorded identity against the running system
// (REQ-store-bookkeeping): dead iff the boot id differs, or — same
// boot, same PID namespace — the pid is gone or its start time
// differs (PID reuse). A same-boot row from a foreign PID namespace
// is unjudgeable and reported live; a false-dead verdict deletes
// content a live process serves — the unrecoverable direction. No
// clock ever decides.
func (li LivenessIdentity) Dead() bool {
	self := selfIdentity()
	if li.BootID != "" && self.BootID != "" && li.BootID != self.BootID {
		return true
	}
	if li.PidNS != "" && self.PidNS != "" && li.PidNS != self.PidNS {
		return false // foreign namespace: unjudgeable, conservative
	}
	// Existence via the signal probe: ESRCH is the only "gone"
	// verdict — a stat read failure also occurs for LIVE
	// foreign-user pids under hidepid procfs, and misreading that
	// as death deletes content a live process serves.
	if err := unix.Kill(int(li.Pid), 0); err != nil {
		if err == unix.ESRCH {
			return true
		}
		return false // EPERM: exists, foreign user — conservative
	}
	if li.StartTime == 0 {
		return false // discriminator absent: pid presence must suffice
	}
	statPath := fmt.Sprintf("/proc/%d/stat", li.Pid)
	b, err := os.ReadFile(statPath)
	if err != nil {
		return false // exists but unreadable (hidepid): conservative
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return false
	}
	fields := strings.Fields(string(b[i+1:]))
	if len(fields) < 20 {
		return false
	}
	return fields[19] != fmt.Sprintf("%d", li.StartTime)
}
