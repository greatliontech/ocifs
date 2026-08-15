package upper

import (
	"os"

	"golang.org/x/sys/unix"
)

// renameNoReplace publishes old at new, refusing an existing
// destination — kernel-atomic on linux (RENAME_NOREPLACE), so a
// create-class publish (Mkdir, Mkfifo) keeps POSIX EEXIST semantics
// with no check-then-rename window.
func renameNoReplace(oldPath, newPath string) error {
	if err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE); err != nil {
		return &os.PathError{Op: "rename", Path: newPath, Err: err}
	}
	return nil
}
