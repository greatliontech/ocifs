package projection

import (
	"os"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// openPin pins an upper node's inode open (REQ-proj-identity).
// darwin has no O_PATH: regular files, directories, and (with
// O_NONBLOCK) FIFOs pin through a read-only descriptor, symlinks
// through O_SYMLINK. A NATIVE socket cannot be opened with open(2)
// on darwin (stand-ins are regular files and pin normally — the
// caller passes their host kind); the native-socket pin is deferred
// with the FSKit write arm — an explicit non-goal of the current
// writable stage — and returns no handle.
func openPin(hostPath string, kind upper.Kind) (*os.File, error) {
	if kind == upper.KindSocket {
		return nil, nil
	}
	flags := unix.O_RDONLY | unix.O_NONBLOCK | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if kind == upper.KindSymlink {
		flags = unix.O_SYMLINK | unix.O_CLOEXEC
	}
	fd, err := unix.Open(hostPath, flags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: hostPath, Err: err}
	}
	return os.NewFile(uintptr(fd), hostPath), nil
}
