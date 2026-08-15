package upper

import (
	"errors"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

// renameNoReplace publishes old at new, refusing an existing
// destination. darwin's renameatx_np(RENAME_EXCL) is not wrapped by
// x/sys, so the guard is an lstat-then-rename pair — a window only
// this process could race, and the writer model is single-writer
// per upper (REQ-writable-base-binding).
func renameNoReplace(oldPath, newPath string) error {
	if _, err := os.Lstat(newPath); err == nil {
		return &os.PathError{Op: "rename", Path: newPath, Err: unix.EEXIST}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return os.Rename(oldPath, newPath)
}
