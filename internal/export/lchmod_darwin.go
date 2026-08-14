package export

import (
	"io/fs"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

// lchmod applies permission bits to a symlink itself: darwin
// filesystems store real symlink permissions and fchmodat honors
// AT_SYMLINK_NOFOLLOW, anchored on the traversal-safe parent handle
// like the other no-follow calls (REQ-export-fidelity's
// permission-bits clause on a platform that holds them).
func lchmod(root *os.Root, name string, perm fs.FileMode) error {
	dir, err := root.Open(dirOf(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	err = unix.Fchmodat(int(dir.Fd()), path.Base(name), uint32(perm), unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		return &os.PathError{Op: "lchmod", Path: name, Err: err}
	}
	return nil
}
