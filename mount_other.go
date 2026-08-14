//go:build !linux && !(windows && amd64)

package ocifs

import (
	"errors"

	"github.com/greatliontech/ocifs/internal/layer"
)

// In-process mounting exists on linux (FUSE) and windows/amd64
// (ProjFS); darwin mounting is appex-mediated and lands with the
// FSKit backend (api.md REQ-api-mount-darwin).
func platformMount(o *OCIFS, view *layer.View, stateDir, mountPoint string) (mountServer, error) {
	return nil, errors.New("in-process mounting is not supported on this platform")
}
