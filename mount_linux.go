//go:build linux

package ocifs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/fusefs"
	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/upper"
)

// platformResolveUpper resolves a requested upper on linux: the
// caller's directory as given, or the store-managed named upper —
// created on first use, its base binding validated
// (REQ-api-mount-writable).
func platformResolveUpper(o *OCIFS, im *ImageMount, img *store.Image) error {
	switch {
	case im.upperDir != "":
		fi, err := os.Stat(im.upperDir)
		if err != nil {
			return fmt.Errorf("upper directory: %w", err)
		}
		if !fi.IsDir() {
			return fmt.Errorf("upper %q is not a directory", im.upperDir)
		}
		im.upperRoot = im.upperDir
	case im.upperName != "":
		root, err := o.store.NewUpper(im.upperName, img.Hash())
		if err != nil {
			return err
		}
		// One writable mount at a time per named upper
		// (REQ-writable-base-binding): a process-lifetime flock,
		// released at unmount.
		lock, err := o.store.LockUpper(im.upperName)
		if err != nil {
			return err
		}
		im.upperRoot, im.upperLock = root, lock
	}
	return nil
}

// platformMount builds the linux projection under the FUSE envelope,
// persists its report beside the mountpoint, and serves it: kernel-ro
// without an upper (REQ-api-mount-ro, REQ-proj-ro), the writable
// merge with one (REQ-api-mount-writable) — presented modes enforced
// by the kernel (default_permissions; the upper's host bits are
// machinery under the mode fidelity override).
func platformMount(o *OCIFS, imgRef string, img *store.Image, view *layer.View, stateDir, mountPoint, upperRoot string) (mountServer, error) {
	proj, err := projection.New(view, o.extraDirs, fusefs.Capabilities())
	if err != nil {
		return nil, err
	}
	if err := proj.Report().WriteFile(filepath.Join(stateDir, projection.ReportFileName)); err != nil {
		return nil, err
	}
	opts := &fs.Options{
		RootStableAttr: fusefs.RootStableAttr(),
		// Recorded modes serve verbatim: without this, go-fuse
		// rewrites a recorded 0000 mode (/etc/shadow-class entries)
		// to 0644 (REQ-proj-fidelity).
		NullPermissions: true,
		MountOptions: fuse.MountOptions{
			AllowOther:  false,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false, // Set to true for debugging
		},
	}
	var root fs.InodeEmbedder
	if upperRoot == "" {
		// Kernel-level read-only is FUSE's strongest denial
		// (REQ-api-mount-ro, REQ-proj-ro); the mount is private
		// to the invoking user (no allow_other).
		opts.MountOptions.Options = []string{"ro"}
		root = fusefs.New(proj, o.store.BlobPath)
	} else {
		// The serving provider sweeps crash-orphaned temporaries on
		// mount (REQ-writable-dialect), then rebuilds its state from
		// a walk alone (REQ-proj-upper-truth).
		if err := upper.Sweep(upperRoot); err != nil {
			return nil, err
		}
		m, err := projection.NewMergedWritable(proj, upperRoot, func(h v1.Hash) (io.ReadCloser, error) {
			return os.Open(o.store.BlobPath(h))
		})
		if err != nil {
			return nil, err
		}
		opts.MountOptions.Options = []string{"default_permissions"}
		root = fusefs.NewWritable(m, o.store.BlobPath)
	}
	srv, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		return nil, err
	}
	return srv, nil
}
