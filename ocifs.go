package ocifs

import (
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type cacheEntry struct {
	hash *v1.Hash
	exp  time.Time
}

type Option func(*OCIFS)

var WithWorkDir = func(workDir string) Option {
	return func(o *OCIFS) {
		o.workDir = filepath.Clean(workDir)
	}
}

var WithCacheExpiration = func(exp time.Duration) Option {
	return func(o *OCIFS) {
		o.exp = exp
	}
}

var WithExtraDirs = func(extraDirs []string) Option {
	return func(o *OCIFS) {
		o.extraDirs = extraDirs
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
	cache     map[string]*cacheEntry
	workDir   string
	lp        layout.Path
	mountDir  string
	extraDirs []string
	exp       time.Duration
	authn     *ocifsKeychain
}

func New(opts ...Option) (*OCIFS, error) {
	// default values
	ofs := &OCIFS{
		workDir: filepath.Join(os.TempDir(), "ocifs"),
		cache:   make(map[string]*cacheEntry),
		exp:     24 * time.Hour,
		authn: &ocifsKeychain{
			creds: make(map[string]authn.AuthConfig),
		},
	}

	// apply options
	for _, opt := range opts {
		opt(ofs)
	}

	// if dir does not exist, create it
	if _, err := os.Stat(ofs.workDir); os.IsNotExist(err) {
		if err := os.MkdirAll(ofs.workDir, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// creat config.json if it does not exist
	idxFilePath := filepath.Join(ofs.workDir, "index.json")
	if _, err := os.Stat(idxFilePath); os.IsNotExist(err) {
		// create index.json
		if err := os.WriteFile(idxFilePath, []byte("{}"), 0644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// create mount dir if it does not exist
	mountDir := filepath.Join(ofs.workDir, "mounts")
	if _, err := os.Stat(mountDir); os.IsNotExist(err) {
		if err := os.MkdirAll(mountDir, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	ofs.mountDir = mountDir

	// at this point, if the directory exists, it should be a valid layout
	lp, err := layout.FromPath(ofs.workDir)
	if err != nil {
		return nil, err
	}

	ofs.lp = lp

	return ofs, nil
}

type ImageMount struct {
	ofs        *OCIFS
	srv        *fuse.Server
	h          v1.Hash
	mountPoint string
	id         string
}

func (im *ImageMount) ConfigFile() (*v1.ConfigFile, error) {
	img, err := im.ofs.lp.Image(im.h)
	if err != nil {
		return nil, err
	}

	return img.ConfigFile()
}

func (im *ImageMount) Wait() {
	im.srv.Wait()
}

func (im *ImageMount) Unmount() error {
	return im.srv.Unmount()
}

func (im *ImageMount) MountPoint() string {
	return im.mountPoint
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

func (o *OCIFS) Mount(imgRef string, opts ...MountOption) (*ImageMount, error) {
	im := &ImageMount{
		ofs: o,
	}
	for _, opt := range opts {
		opt(im)
	}

	if im.mountPoint == "" {
		id := im.id
		if id == "" {
			uid, err := uuid.NewRandom()
			if err != nil {
				return nil, err
			}
			id = uid.String()
		}
		path := filepath.Join(o.mountDir, id)
		if err := os.Mkdir(path, 0755); err != nil {
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

	h, err := o.pullImage(imgRef)
	if err != nil {
		return nil, err
	}
	im.h = *h

	root, err := o.initFS(h, o.extraDirs)
	if err != nil {
		return nil, err
	}

	// Create a FUSE server
	srv, err := fs.Mount(im.mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  false,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false, // Set to true for debugging
		},
	})
	if err != nil {
		return nil, err
	}
	im.srv = srv

	return im, nil
}
