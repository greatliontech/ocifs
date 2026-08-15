//go:build !linux && !darwin

package ocifs

import (
	"context"
	"fmt"
)

// Commit requires the POSIX upper dialect reader; the windows write
// arm lands with the ProjFS dialect (docs/specs/writable.md
// non-goals).
func (o *OCIFS) Commit(ctx context.Context, baseRef string, opts ...CommitOption) (*Image, error) {
	return nil, fmt.Errorf("commit is not available on this platform: the POSIX upper dialect reader is linux/darwin; the windows arm lands with the ProjFS dialect")
}
