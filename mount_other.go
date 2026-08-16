//go:build !linux && !(windows && amd64) && !darwin

package ocifs

import (
	"fmt"
	"errors"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/store"
)

// In-process mounting exists on linux (FUSE) and windows/amd64
// (ProjFS); darwin mounting is appex-mediated (mount_darwin.go).
func platformResolveUpper(o *OCIFS, im *ImageMount, img *store.Image) error {
	if im.upperDir != "" || im.upperName != "" {
		return fmt.Errorf("writable mounts are not served here: no mount backend exists on this platform")
	}
	return nil
}

func platformMount(o *OCIFS, imgRef string, img *store.Image, view *layer.View, stateDir, mountPoint, upperRoot string) (mountServer, error) {
	return nil, errors.New("in-process mounting is not supported on this platform")
}
