//go:build !darwin

package export

import (
	"io/fs"
	"os"
)

// lchmod is a no-op away from darwin: linux fixes symlink
// permissions at 0777 in the kernel, and windows symlinks carry no
// unix permission bits — on neither platform is there a symlink
// permission to apply.
func lchmod(root *os.Root, name string, perm fs.FileMode) error {
	return nil
}
