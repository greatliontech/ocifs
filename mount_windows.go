//go:build windows && amd64

package ocifs

import (
	"path/filepath"

	"github.com/greatliontech/ocifs/internal/store"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/projfsfs"
)

// platformMount builds the windows projection under the ProjFS
// envelope — symlink capability settled by a throwaway-instance
// probe before the projection exists, so the declared envelope never
// changes under a live mount — persists its report, and serves the
// mountpoint as the virtualization root.
func platformMount(o *OCIFS, imgRef string, img *store.Image, view *layer.View, stateDir, mountPoint string) (mountServer, error) {
	symlinks, err := projfsfs.ProbeSymlinkSupport(filepath.Join(stateDir, "probe"))
	if err != nil {
		return nil, err
	}
	proj, err := projection.New(view, o.extraDirs, projfsfs.Capabilities(symlinks))
	if err != nil {
		return nil, err
	}
	reportPath := filepath.Join(stateDir, projection.ReportFileName)
	if err := proj.Report().WriteFile(reportPath); err != nil {
		return nil, err
	}
	return projfsfs.Serve(proj, o.store.BlobPath, reportPath, mountPoint)
}
