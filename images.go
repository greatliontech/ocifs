package ocifs

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

type Store struct {
	workDir string
	lp      layout.Path
}

type LayerIndex struct {
	files map[string]*tar.Header
	h     v1.Hash
}

func (l *LayerIndex) Hash() v1.Hash {
	return l.h
}

func (l *LayerIndex) Files() map[string]*tar.Header {
	return l.files
}

func NewStore(workDir string) (*Store, error) {
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

	return &Store{
		workDir: workDir,
		lp:      lp,
	}, nil
}

func (s *Store) Path() string {
	return s.workDir
}

func (s *Store) UnpackedPath(h v1.Hash) string {
	return filepath.Join(s.workDir, "unpacked", h.Algorithm, h.Hex)
}

func (s *Store) Index(h v1.Hash) ([]*LayerIndex, error) {
	// get image by hash
	img, err := s.lp.Image(h)
	if err != nil {
		return nil, err
	}

	// get layers
	layers, err := img.Layers()
	if err != nil {
		slog.Error("get image layers", "error", err)
		return nil, err
	}

	slog.Info("image len layers", "layers", len(layers))

	idx := make([]*LayerIndex, len(layers))

	for i, layer := range layers {
		lh, err := layer.Digest()
		if err != nil {
			slog.Error("get layer digest", "error", err)
			return nil, err
		}
		slog.Info("layer digest", "digest", lh)

		targetDir := filepath.Join(s.workDir, "unpacked", h.Algorithm, lh.Hex)
		idxName := targetDir + ".json"

		data, err := os.ReadFile(idxName)
		if err != nil {
			return nil, err
		}

		lidx := &LayerIndex{
			files: map[string]*tar.Header{},
			h:     lh,
		}
		if err := json.Unmarshal(data, &lidx.files); err != nil {
			return nil, err
		}

		idx[i] = lidx
	}

	return idx, nil
}

func (s *Store) Pull(imageRef string) (*v1.Hash, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		slog.Error("parse reference", "error", err)
		return nil, err
	}

	head, err := remote.Head(ref)
	if err != nil {
		slog.Error("get remote head", "error", err)
		return nil, err
	}

	slog.Info("head", "digest", head.Digest)

	h := &v1.Hash{}
	*h = head.Digest
	var img v1.Image

	img, err = s.lp.Image(*h)
	if err != nil {
		slog.Info("local image not found, pulling", "digest", h, "error", err)
		rmtImg, err := remote.Image(ref)
		if err != nil {
			slog.Error("get remote image", "error", err)
			return nil, err
		}

		if err := s.lp.AppendImage(rmtImg); err != nil {
			slog.Error("append image", "error", err)
			return nil, err
		}
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

	return h, nil
}

func (s *Store) unpackLayer(layer v1.Layer) error {
	h, err := layer.Digest()
	if err != nil {
		slog.Error("get layer digest", "error", err)
		return err
	}

	targetDir := filepath.Join(s.workDir, "unpacked", h.Algorithm, h.Hex)

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
	// marshal index to json pretty
	data, err := json.MarshalIndent(idx, "", "  ")
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

func extractTar(rc io.ReadCloser, target string) (map[string]*tar.Header, error) {
	// Create a tar reader
	tarReader := tar.NewReader(rc)

	// Create a map to store the headers
	idx := make(map[string]*tar.Header)

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
			slog.Info("file", "name", header.Name)
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
			slog.Info("symlink", "linkname", header.Linkname, "target", targetFilePath)

		case tar.TypeLink:
			slog.Info("hardlink", "linkname", header.Linkname, "target", targetFilePath)

		case tar.TypeBlock, tar.TypeChar, tar.TypeFifo:

		default:
			fmt.Printf("Unsupported file type: %c in %s\n", header.Typeflag, header.Name)
			continue
		}

		// Add the header to the index
		idx[header.Name] = header
	}

	return idx, nil
}
