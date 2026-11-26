//go:build !linux && !(windows && amd64) && !darwin

package mount

import (
	"errors"
	"fmt"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/store"
)

func platformResolveUpper(im *ImageMount, _ *store.Image) error {
	if im.upperDir != "" || im.upperName != "" {
		return fmt.Errorf("writable mounts are not served here: no mount backend exists on this platform")
	}
	return nil
}

func platformMount(_ *ImageMount, _ string, _ *store.Image, _ *layer.View, _, _, _, _ string) (mountServer, error) {
	return nil, errors.New("in-process mounting is not supported on this platform")
}
