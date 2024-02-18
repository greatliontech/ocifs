package layerdb

import (
	"io"
	"os"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type LayerDB struct {
	path string
}

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
	io.Seeker
}

func NewLayerDB(path string) *LayerDB {
	return &LayerDB{path: path}
}

func (db *LayerDB) Get(layer v1.Layer) (ReaderAtCloser, error) {
	dgst, err := layer.Digest()
	if err != nil {
		return nil, err
	}

	n := filepath.Join(db.path, dgst.String())

	if _, err := os.Stat(n); err == nil {
		return os.Open(filepath.Join(db.path, dgst.String()))
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	r, err := layer.Uncompressed()
	if err != nil {
		return nil, err
	}

	f, err := os.Create(n)
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(f, r); err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}
