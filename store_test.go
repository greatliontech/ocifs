package ocifs

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestMultiArch(t *testing.T) {
	imageRef := "rust:1.42-slim-buster"

	ref, err := name.ParseReference(imageRef)
	if err != nil {
		t.Error(err)
	}

	s := runtime.GOOS + "/" + runtime.GOARCH

	p, err := v1.ParsePlatform(s)
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Platform", p)

	img, err := remote.Image(ref, remote.WithPlatform(*p))
	if err != nil {
		t.Error(err)
	}

	mani, err := img.Manifest()
	if err != nil {
		t.Error(err)
	}

	fmt.Println("MultiArch", mani.MediaType, mani.Config.Platform)
}

func TestNoMultiArch(t *testing.T) {
	imageRef := "rustlang/rust:nightly-slim"

	ref, err := name.ParseReference(imageRef)
	if err != nil {
		t.Error(err)
	}

	desc, err := remote.Get(ref)
	if err != nil {
		t.Error(err)
	}

	fmt.Println("No MultiArch", desc)

	idx, err := remote.Index(ref)
	if err != nil {
		t.Error(err)
	}

	idxmani, err := idx.IndexManifest()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("No MultiArch", idxmani.MediaType)
	fmt.Println(idxmani.Subject, len(idxmani.Manifests))
	for _, m := range idxmani.Manifests {
		fmt.Println(m.Platform.Architecture, m.ArtifactType)
	}

	img, err := remote.Image(ref)
	if err != nil {
		t.Error(err)
	}

	mani, err := img.Manifest()
	if err != nil {
		t.Error(err)
	}

	fmt.Println("MultiArch", mani.MediaType, mani.Config.Platform)
}
