//go:build linux

package mount

import (
	"context"
	"fmt"
	"io"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/store"
	"github.com/greatliontech/ocifs/internal/upper"
	"github.com/greatliontech/ocifs/mount/internal/fusefs"
)

func platformResolveUpper(im *ImageMount, img *store.Image) error {
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
		root, err := im.st.NewUpper(im.upperName, img.Hash())
		if err != nil {
			return err
		}
		im.upperRoot = root
	}
	return nil
}

func platformMount(im *ImageMount, _ string, _ *store.Image, view *layer.View, mountID, _, mountPoint, upperRoot string) (mountServer, error) {
	proj, err := projection.New(view, im.extraDirs, fusefs.Capabilities())
	if err != nil {
		return nil, err
	}
	if err := im.st.PublishMountReport(context.Background(), mountID, proj.Report()); err != nil {
		return nil, err
	}
	opts := &fs.Options{
		RootStableAttr:  fusefs.RootStableAttr(),
		NullPermissions: true,
		MountOptions: fuse.MountOptions{
			AllowOther:  false,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false,
		},
	}
	var root fs.InodeEmbedder
	if upperRoot == "" {
		opts.MountOptions.Options = []string{"ro"}
		root = fusefs.New(proj, im.st.BlobPath)
	} else {
		if err := upper.Sweep(upperRoot); err != nil {
			return nil, err
		}
		m, err := projection.NewMergedWritable(proj, upperRoot, func(h v1.Hash) (io.ReadCloser, error) {
			return os.Open(im.st.BlobPath(h))
		})
		if err != nil {
			return nil, err
		}
		opts.MountOptions.Options = []string{"default_permissions"}
		root = fusefs.NewWritable(m, im.st.BlobPath)
	}
	srv, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		return nil, err
	}
	return srv, nil
}
