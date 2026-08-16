package upper

import (
	"errors"

	"golang.org/x/sys/unix"
)

// xattrAbsent reports the platform's missing-attribute error from
// removexattr.
func xattrAbsent(err error) bool { return errors.Is(err, unix.ENODATA) }
