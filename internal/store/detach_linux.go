package store

import (
	"os/exec"

	"golang.org/x/sys/unix"
)

// detachStaleMount lazily detaches whatever is mounted at mnt — a
// dead mount's kernel state (REQ-store-mount-registry). Best
// effort: reclamation defers on failure.
func detachStaleMount(mnt string) {
	if unix.Unmount(mnt, unix.MNT_DETACH) == nil {
		return
	}
	for _, helper := range []string{"fusermount3", "fusermount"} {
		if exec.Command(helper, "-uz", mnt).Run() == nil {
			return
		}
	}
}
