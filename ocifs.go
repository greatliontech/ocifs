package ocifs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/ocifs/internal/store"
	"github.com/greatliontech/ocifs/internal/unionfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Option func(*OCIFS)

var WithWorkDir = func(workDir string) Option {
	return func(o *OCIFS) {
		o.workDir = filepath.Clean(workDir)
	}
}

var WithAuthSource = func(prefix string, auth authn.AuthConfig) Option {
	return func(o *OCIFS) {
		o.authn.creds[prefix] = auth
	}
}

var WithEnableDefaultKeychain = func() Option {
	return func(o *OCIFS) {
		o.authn.includeDefaultKeychain = true
	}
}

type OCIFS struct {
	workDir string
	authn   *ocifsKeychain
	store   *store.Store
}

func New(opts ...Option) (*OCIFS, error) {
	// default values
	ofs := &OCIFS{
		workDir: filepath.Join(os.TempDir(), "ocifs"),
		authn: &ocifsKeychain{
			creds: make(map[string]authn.AuthConfig),
		},
	}

	// apply options
	for _, opt := range opts {
		opt(ofs)
	}

	// initialize store
	s, err := store.NewStore(ofs.workDir, ofs.authn, store.PullIfNotPresent)
	if err != nil {
		return nil, err
	}
	ofs.store = s

	return ofs, nil
}

type ImageMount struct {
	srv              *fuse.Server
	img              *store.Image
	mountPoint       string
	id               string
	ctx              context.Context
	extraDirs        []string
	writeDir         string
	writableLayerOpts []store.WritableLayerOption
	ufs              *unionfs.UnionFS
}

func (im *ImageMount) ConfigFile() *v1.ConfigFile {
	return im.img.ConfigFile()
}

func (im *ImageMount) Wait() error {
	im.srv.Wait()
	if im.writeDir != "" {
		return im.ufs.Close()
	}
	return nil
}

func (im *ImageMount) Unmount() error {
	return im.srv.Unmount()
}

func (im *ImageMount) MountPoint() string {
	return im.mountPoint
}

// Image returns the underlying store.Image for this mount.
func (im *ImageMount) Image() *store.Image {
	return im.img
}

// WritableLayer returns the writable layer, or nil if in read-only mode.
func (im *ImageMount) WritableLayer() *store.WritableLayer {
	return im.ufs.WritableLayer()
}

type MountOption func(*ImageMount)

var MountWithTargetPath = func(targetPath string) MountOption {
	return func(im *ImageMount) {
		im.mountPoint = targetPath
	}
}

var MountWithID = func(id string) MountOption {
	return func(im *ImageMount) {
		im.id = id
	}
}

var MountWithContext = func(ctx context.Context) MountOption {
	return func(im *ImageMount) {
		im.ctx = ctx
	}
}

var MountWithExtraDirs = func(dirs []string) MountOption {
	return func(im *ImageMount) {
		im.extraDirs = dirs
	}
}

var MountWithWritableDir = func(dir string) MountOption {
	return func(im *ImageMount) {
		im.writeDir = dir
	}
}

// MountWithWritableLayerOptions configures the writable layer with options like auto-persist.
var MountWithWritableLayerOptions = func(opts ...store.WritableLayerOption) MountOption {
	return func(im *ImageMount) {
		im.writableLayerOpts = append(im.writableLayerOpts, opts...)
	}
}

func (o *OCIFS) Mount(imgRef string, opts ...MountOption) (*ImageMount, error) {
	im := &ImageMount{
		ctx: context.Background(),
	}
	for _, opt := range opts {
		opt(im)
	}

	if im.mountPoint == "" {
		path, err := o.store.NewMountDir(im.id)
		if err != nil {
			return nil, err
		}
		im.mountPoint = path
	}

	im.mountPoint = filepath.Clean(im.mountPoint)
	if !filepath.IsAbs(im.mountPoint) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		im.mountPoint = filepath.Clean(filepath.Join(cwd, im.mountPoint))
	}

	img, err := o.store.Image(im.ctx, imgRef)
	if err != nil {
		return nil, err
	}
	im.img = img

	uopts := []unionfs.Option{
		unionfs.WithExtraDirs(im.extraDirs),
		unionfs.WithWritableLayer(im.writeDir, im.writableLayerOpts...),
		unionfs.WithBlobStore(o.store.BlobStore()),
	}

	root, err := unionfs.Init(img, uopts...)
	if err != nil {
		return nil, err
	}
	im.ufs = root

	// Create a FUSE server
	srv, err := fs.Mount(im.mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  false,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       true, // Set to true for debugging
		},
	})
	if err != nil {
		return nil, err
	}
	im.srv = srv

	return im, nil
}

// CommitOptions configures how a writable layer is committed.
// Re-exported from store for convenience.
type CommitOptions = store.CommitOptions

// Commit creates a new image by appending the writable layer's changes to a base image.
// The mount should have a writable layer configured.
// The new image is stored in the local OCI layout and can be pushed or tagged.
func (o *OCIFS) Commit(ctx context.Context, mount *ImageMount, opts CommitOptions) (*store.Image, error) {
	wl := mount.WritableLayer()
	if wl == nil {
		return nil, fmt.Errorf("mount has no writable layer")
	}
	return o.store.Commit(ctx, mount.img, wl, opts)
}

// Push uploads an image to a remote registry.
func (o *OCIFS) Push(ctx context.Context, img *store.Image, ref string) error {
	return o.store.Push(ctx, img, ref)
}

// Tag associates a reference with an image in the local store.
func (o *OCIFS) Tag(img *store.Image, ref string) error {
	return o.store.Tag(img, ref)
}
