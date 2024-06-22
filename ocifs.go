package ocifs

import (
	"os"
	"path/filepath"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

type cacheEntry struct {
	hash *v1.Hash
	exp  time.Time
}

type OCIFS struct {
	cache     map[string]*cacheEntry
	workDir   string
	lp        layout.Path
	extraDirs []string
	exp       time.Duration
}

func New(opts ...Option) (*OCIFS, error) {
	// default values
	ofs := &OCIFS{
		workDir: filepath.Join(os.TempDir(), "ocifs"),
		cache:   make(map[string]*cacheEntry),
		exp:     24 * time.Hour,
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
		// create index.json
		idxFilePath := filepath.Join(ofs.workDir, "index.json")
		if err := os.WriteFile(idxFilePath, []byte("{}"), 0644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// at this point, if the directory exists, it should be a valid layout
	lp, err := layout.FromPath(ofs.workDir)
	if err != nil {
		return nil, err
	}

	ofs.lp = lp

	return ofs, nil
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
