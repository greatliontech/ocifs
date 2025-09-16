package store

import (
	"archive/tar"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type PullPolicy int

func (s PullPolicy) String() string {
	switch s {
	case PullIfNotPresent:
		return "IfNotPresent"
	case PullAlways:
		return "Always"
	case PullNever:
		return "Never"
	default:
		return "Unknown"
	}
}

const (
	PullIfNotPresent PullPolicy = iota
	PullAlways
	PullNever
)

type File struct {
	Hdr  *tar.Header
	Path string
}

type Layer struct {
	Files []*File
}

type Image struct {
	h      v1.Hash
	img    v1.Image
	conf   *v1.ConfigFile
	layers []*Layer
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

func (i *Image) Layers() []*Layer {
	return i.layers
}
