//go:build windows

package export

import (
	"fmt"
	"os"
	"time"
)

// mkfifo refuses: windows filesystems hold no FIFO nodes, and a
// rootfs is exact or refused, never silently thinned
// (REQ-export-fidelity's exact-or-refused principle).
func mkfifo(root *os.Root, name string) error {
	return fmt.Errorf("entry %q: FIFO nodes cannot be materialized on windows", name)
}

// lchtimes is a no-op: symlink timestamps are not separately
// settable through a no-follow path on windows, and the export's
// fidelity surface for symlinks there is the target reference
// itself.
func lchtimes(root *os.Root, name string, mtime time.Time) error {
	return nil
}
