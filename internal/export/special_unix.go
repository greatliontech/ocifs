//go:build linux || darwin

package export

import (
	"os"
	"path"
	"time"

	"golang.org/x/sys/unix"
)

// lchtimes applies the recorded modification time to a symlink
// itself. os.Root.Chtimes would follow the link, so the parent
// directory — reached through the traversal-safe root handle —
// anchors a no-follow *at call.
func lchtimes(root *os.Root, name string, mtime time.Time) error {
	if mtime.IsZero() {
		return nil
	}
	dir, err := root.Open(dirOf(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	ts := unix.NsecToTimespec(mtime.UnixNano())
	err = unix.UtimesNanoAt(int(dir.Fd()), path.Base(name), []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		return &os.PathError{Op: "lchtimes", Path: name, Err: err}
	}
	return nil
}
