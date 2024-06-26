package ocifs

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

type unpackedLayer struct {
	hash  v1.Hash
	path  string
	files []*tar.Header
}

func (l *unpackedLayer) Hash() v1.Hash {
	return l.hash
}

func (l *unpackedLayer) Files() []*tar.Header {
	return l.files
}

func (l *unpackedLayer) Path() string {
	return l.path
}

func (s *OCIFS) getUnpackedLayers(h *v1.Hash) ([]*unpackedLayer, error) {
	// get image by hash
	img, err := s.lp.Image(*h)
	if err != nil {
		return nil, err
	}

	// get layers
	layers, err := img.Layers()
	if err != nil {
		slog.Error("get image layers", "error", err)
		return nil, err
	}

	idx := make([]*unpackedLayer, len(layers))

	for i, layer := range layers {
		lh, err := layer.Digest()
		if err != nil {
			slog.Error("get layer digest", "error", err)
			return nil, err
		}
		slog.Debug("layer digest", "digest", lh)

		targetDir := filepath.Join(string(s.lp), "unpacked", h.Algorithm, lh.Hex)
		idxName := targetDir + ".json"

		data, err := os.ReadFile(idxName)
		if err != nil {
			return nil, err
		}

		lidx := &unpackedLayer{
			files: []*tar.Header{},
			hash:  lh,
			path:  targetDir,
		}
		if err := json.Unmarshal(data, &lidx.files); err != nil {
			return nil, err
		}

		idx[i] = lidx
	}

	return idx, nil
}

func (s *OCIFS) pullImage(imageRef string) (*v1.Hash, error) {
	// look in cache first
	if ce, ok := s.cache[imageRef]; ok && ce.exp.After(time.Now()) {
		slog.Debug("cache hit", "image", imageRef, "hash", ce.hash)
		return ce.hash, nil
	}

	ref, err := name.ParseReference(imageRef)
	if err != nil {
		slog.Error("parse reference", "error", err)
		return nil, err
	}

	rmtImg, err := remote.Image(ref, remote.WithAuthFromKeychain(s.authn))
	if err != nil {
		slog.Error("get remote image", "error", err)
		return nil, err
	}

	dgst, err := rmtImg.Digest()
	if err != nil {
		slog.Error("get image digest", "error", err)
		return nil, err
	}

	h := &v1.Hash{}
	*h = dgst

	img, err := s.lp.Image(*h)
	if err != nil {

		if err := s.lp.AppendImage(rmtImg); err != nil {
			slog.Error("append image", "error", err)
			return nil, err
		}

		slog.Debug("getting local image", "hash", h)
		img, err = s.lp.Image(*h)
		if err != nil {
			slog.Error("get local image", "error", err)
			return nil, err
		}
	}

	layers, err := img.Layers()
	if err != nil {
		slog.Error("get image layers", "error", err)
		return nil, err
	}

	for _, layer := range layers {
		if err := s.unpackLayer(layer); err != nil {
			slog.Error("unpack layer", "error", err)
			return nil, err
		}
	}

	// add to cache
	s.cache[imageRef] = &cacheEntry{
		hash: h,
		exp:  time.Now().Add(s.exp),
	}

	return h, nil
}

func (s *OCIFS) unpackLayer(layer v1.Layer) error {
	h, err := layer.Digest()
	if err != nil {
		slog.Error("get layer digest", "error", err)
		return err
	}

	targetDir := filepath.Join(string(s.lp), "unpacked", h.Algorithm, h.Hex)

	if _, err := os.Stat(targetDir); err == nil {
		// if index file exists, we assume the layer has already been unpacked
		if _, err := os.Stat(targetDir + ".json"); err == nil {
			return nil
		}
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			slog.Error("create target dir", "error", err)
			return err
		}
	} else {
		return err
	}

	rc, err := layer.Uncompressed()
	if err != nil {
		return err
	}
	defer rc.Close()

	idx, err := extractTar(rc, targetDir)
	if err != nil {
		slog.Error("extract tar.gz", "error", err)
		return err
	}

	idxName := targetDir + ".json"
	// marshal index to json
	data, err := json.Marshal(idx)
	if err != nil {
		slog.Error("marshal index", "error", err)
		return err
	}

	if err := os.WriteFile(idxName, data, 0644); err != nil {
		slog.Error("write index", "error", err)
		return err
	}

	return nil
}

func extractTar(rc io.ReadCloser, target string) ([]*tar.Header, error) {
	// Create a tar reader
	tarReader := tar.NewReader(rc)

	// Create a map to store the headers
	idx := []*tar.Header{}

	// Iterate through entries in the tar archive
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, err
		}

		// Determine the target file path
		targetFilePath := filepath.Join(target, header.Name)

		// Handle different file types
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetFilePath, 0755); err != nil {
				return nil, err
			}

		case tar.TypeReg:
			slog.Debug("file", "name", header.Name)
			dir := filepath.Dir(targetFilePath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, err
			}
			outFile, err := os.Create(targetFilePath)
			if err != nil {
				return nil, err
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, tarReader); err != nil {
				return nil, err
			}

		case tar.TypeSymlink:
			slog.Debug("symlink", "linkname", header.Linkname, "target", targetFilePath)

		case tar.TypeLink:
			slog.Debug("hardlink", "linkname", header.Linkname, "target", targetFilePath)

		case tar.TypeBlock, tar.TypeChar, tar.TypeFifo:

		default:
			fmt.Printf("Unsupported file type: %c in %s\n", header.Typeflag, header.Name)
			continue
		}

		// Add the header to the index
		idx = append(idx, header)
	}

	return idx, nil
}
