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
	cache map[string]*cacheEntry
	lp    layout.Path
	exp   time.Duration
}

func New(workDir string) (*OCIFS, error) {
	workDir = filepath.Clean(workDir)

	// if dir does not exist, create it
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		if err := os.MkdirAll(workDir, 0755); err != nil {
			return nil, err
		}
		// create index.json
		idxFilePath := filepath.Join(workDir, "index.json")
		if err := os.WriteFile(idxFilePath, []byte("{}"), 0644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// at this point, if the directory exists, it should be a valid layout
	lp, err := layout.FromPath(workDir)
	if err != nil {
		return nil, err
	}

	return &OCIFS{
		cache: make(map[string]*cacheEntry),
		lp:    lp,
	}, nil
}
