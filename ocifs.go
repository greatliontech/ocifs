package ocifs

import (
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type cacheEntry struct {
	hash v1.Hash
	exp  time.Time
}

type OCIFS struct {
	store *Store
	cache map[string]*cacheEntry
}

func New() (*OCIFS, error) {
	store, err := NewStore("/tmp/ocifs")
	if err != nil {
		return nil, err
	}

	return &OCIFS{
		store: store,
		cache: make(map[string]*cacheEntry),
	}, nil
}

func (o *OCIFS) Mount(img, path string) (*fuse.Server, error) {
	var h v1.Hash

	// look in cache first
	if ce, ok := o.cache[img]; ok && ce.exp.After(time.Now()) {
		h = ce.hash
	} else {
		// pull image
		ph, err := o.store.Pull(img)
		if err != nil {
			return nil, err
		}
		h = *ph
		// add to cache
		o.cache[img] = &cacheEntry{
			hash: h,
			exp:  time.Now().Add(10 * time.Minute),
		}
	}

	root, err := NewOciFS(o.store, h)
	if err != nil {
		return nil, err
	}

	// Create a FUSE server
	return fs.Mount(path, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  true,
			Name:        "ocifs",
			DirectMount: true,
			Debug:       false, // Set to true for debugging
		},
	})
}
