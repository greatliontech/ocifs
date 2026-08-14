package export

import (
	"os"
	"path"

	"golang.org/x/sys/unix"
)

// mkfifo creates a FIFO node at name inside root. os.Root offers no
// FIFO constructor, so the parent directory — itself reached through
// the traversal-safe root handle — anchors an *at call; the final
// component cannot traverse anything because mkfifoat never follows
// it (it creates or fails).
func mkfifo(root *os.Root, name string) error {
	dir, err := root.Open(dirOf(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := unix.Mkfifoat(int(dir.Fd()), path.Base(name), 0o600); err != nil {
		return &os.PathError{Op: "mkfifo", Path: name, Err: err}
	}
	return nil
}
