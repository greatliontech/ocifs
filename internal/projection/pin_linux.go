package projection

import (
	"os"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// openPin pins an upper node's inode open so the host cannot
// recycle its number while the projected object lives
// (REQ-proj-identity). O_PATH holds any node kind — including FIFOs
// without blocking and sockets — without read access, and the
// descriptor reopens content via /proc/self/fd when needed.
func openPin(hostPath string, _ upper.Kind) (*os.File, error) {
	fd, err := unix.Open(hostPath, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: hostPath, Err: err}
	}
	return os.NewFile(uintptr(fd), hostPath), nil
}
