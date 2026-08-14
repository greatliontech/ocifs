package export

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// mkfifo creates a FIFO node at name inside root. x/sys wraps no
// mkfifoat on darwin, so the call is path-based — sound here because
// every ancestor of name is a real directory this export created
// inside its private temporary root (the view admits no symlink
// ancestors, and nothing else writes the temporary), so the path
// cannot traverse a symlink. Linux keeps the dirfd-anchored form.
func mkfifo(root *os.Root, name string) error {
	// Existence through the root handle first: path-based mkfifo
	// reports EEXIST too, but the check keeps the collision
	// diagnosis on the traversal-safe handle.
	if _, err := root.Lstat(name); err == nil {
		return &os.PathError{Op: "mkfifo", Path: name, Err: os.ErrExist}
	}
	if err := unix.Mkfifo(filepath.Join(root.Name(), name), 0o600); err != nil {
		return &os.PathError{Op: "mkfifo", Path: name, Err: err}
	}
	return nil
}
