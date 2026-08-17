package store

import (
	"os"
	"strings"
)

// selfIdentity collects this process's liveness identity
// (REQ-store-bookkeeping): pid, kernel start-time ticks (field 22
// of /proc/self/stat — the PID-reuse discriminator), the PID
// namespace, and the boot id. Any unavailable component stays zero;
// dead-detection treats absent discriminators conservatively.
func selfIdentity() LivenessIdentity {
	id := LivenessIdentity{Pid: int64(os.Getpid())}
	if b, err := os.ReadFile("/proc/self/stat"); err == nil {
		// comm may carry spaces/parens; fields count from after the
		// closing paren.
		if i := strings.LastIndexByte(string(b), ')'); i >= 0 {
			fields := strings.Fields(string(b[i+1:]))
			// fields[0] is state (field 3); start-time is field 22.
			if len(fields) >= 20 {
				var st uint64
				for _, c := range []byte(fields[19]) {
					if c < '0' || c > '9' {
						st = 0
						break
					}
					st = st*10 + uint64(c-'0')
				}
				id.StartTime = st
			}
		}
	}
	if ns, err := os.Readlink("/proc/self/ns/pid"); err == nil {
		id.PidNS = ns
	}
	if b, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
		id.BootID = strings.TrimSpace(string(b))
	}
	return id
}
