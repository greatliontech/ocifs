//go:build linux

package ocifs

import (
	"path/filepath"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/fusefs"
	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
)

// platformMount builds the linux projection under the FUSE envelope,
// persists its report beside the mountpoint, and serves it kernel-ro
// (REQ-api-mount-ro, REQ-proj-ro).
func platformMount(o *OCIFS, view *layer.View, stateDir, mountPoint string) (mountServer, error) {
	proj, err := projection.New(view, o.extraDirs, fusefs.Capabilities())
	if err != nil {
		return nil, err
	}
	if err := proj.Report().WriteFile(filepath.Join(stateDir, projection.ReportFileName)); err != nil {
		return nil, err
	}
	srv, err := fs.Mount(mountPoint, fusefs.New(proj, o.store.BlobPath), &fs.Options{
		RootStableAttr: fusefs.RootStableAttr(),
		// Recorded modes serve verbatim: without this, go-fuse
		// rewrites a recorded 0000 mode (/etc/shadow-class entries)
		// to 0644 (REQ-proj-fidelity).
		NullPermissions: true,
		MountOptions: fuse.MountOptions{
			// Kernel-level read-only is FUSE's strongest denial
			// (REQ-api-mount-ro, REQ-proj-ro); the mount is private
			// to the invoking user (no allow_other).
			Options:     []string{"ro"},
			AllowOther:  false,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false, // Set to true for debugging
		},
	})
	if err != nil {
		return nil, err
	}
	return srv, nil
}
