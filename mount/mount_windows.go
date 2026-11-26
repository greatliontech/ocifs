//go:build windows && amd64

package mount

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/store"
	"github.com/greatliontech/ocifs/mount/internal/projfsfs"
)

func platformResolveUpper(im *ImageMount, _ *store.Image) error {
	if im.upperDir != "" || im.upperName != "" {
		return fmt.Errorf("writable mounts are not served here: the ProjFS write arm is an explicit non-goal of this stage")
	}
	return nil
}

func platformMount(im *ImageMount, _ string, _ *store.Image, view *layer.View, mountID, stateDir, mountPoint, _ string) (mountServer, error) {
	symlinks, err := projfsfs.ProbeSymlinkSupport(filepath.Join(stateDir, "probe"))
	if err != nil {
		return nil, err
	}
	proj, err := projection.New(view, im.extraDirs, projfsfs.Capabilities(symlinks))
	if err != nil {
		return nil, err
	}
	sink := projection.ReportSink(func(rep projection.Report) error {
		return im.st.PublishMountReport(context.Background(), mountID, rep)
	})
	if err := sink(proj.Report()); err != nil {
		return nil, err
	}
	return projfsfs.Serve(proj, im.st.BlobPath, sink, mountPoint)
}
