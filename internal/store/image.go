package store

import (
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
)

// Image is a materialized image: its manifest digest, config, and
// recorded layer entries, base to top.
type Image struct {
	h      v1.Hash
	img    v1.Image
	conf   *v1.ConfigFile
	layers []layer.Layer
}

func (i *Image) Hash() v1.Hash {
	return i.h
}

func (i *Image) Image() v1.Image {
	return i.img
}

func (i *Image) ConfigFile() *v1.ConfigFile {
	return i.conf
}

// Layers returns the image's recorded layer entries, base to top.
func (i *Image) Layers() []layer.Layer {
	return i.layers
}

// Unify resolves the image's layers into the unified view
// (docs/specs/layer-semantics.md).
func (i *Image) Unify() (*layer.View, error) {
	return layer.Unify(i.layers)
}
