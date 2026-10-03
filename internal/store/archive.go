package store

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// ArchiveForm is the form of an archive a daemon's image store loads.
type ArchiveForm int

const (
	// DockerArchive is the form the daemon's classic image store
	// loads: a `manifest.json` naming the image's configuration at
	// `<hex>.json` and each layer at `<hex>/layer.tar`, no tag; the
	// image's identity there is its configuration's digest.
	DockerArchive ArchiveForm = iota
	// OCILayout is the form the daemon's containerd image store
	// loads: an OCI image layout, `oci-layout` and an `index.json`
	// naming the image's manifest, the manifest, configuration and
	// layers under `blobs/`; the image's identity there is the
	// manifest's digest, the acquisition's own.
	OCILayout
)

// Archive writes img as a daemon's image store loads it, in the form
// (REQ-api-archive): the configuration's and each layer's bytes as
// retained under oci/ — a pulled image's as the registry served
// them, compressed, which the daemon reads by the bytes' framing as
// the store's own unpack does; a committed image's its plain tar —
// and the identity the daemon reports for the form returned. The
// op row pins the image for the archive's span, a collection root
// while live (REQ-store-gc-collect), so every blob found before the
// first byte is written is read whole; an image a collection took
// before the pin, or condemned by a live sweep, fails the archive
// as gone (ErrGone), nothing written. The tar is the same bytes for
// the same image: entries in manifest order, root-owned, at the
// epoch. A failure after the first byte — the context's end, the
// writer's refusal — leaves the writer holding a partial archive,
// as any stream does.
func (s *Store) Archive(ctx context.Context, img *Image, w io.Writer, form ArchiveForm) (v1.Hash, error) {
	opClaim, err := s.BeginOp(ctx, opKindArchive, []v1.Hash{img.h}, nil)
	if err != nil {
		if errors.Is(err, ErrCondemned) {
			return v1.Hash{}, fmt.Errorf("archive %s: %w", img.h, ErrGone)
		}
		return v1.Hash{}, err
	}
	defer func() { _ = s.EndOp(context.WithoutCancel(ctx), opClaim) }()
	m, raw, err := s.retainedManifest(img.h)
	if err != nil {
		return v1.Hash{}, fmt.Errorf("archive %s: the manifest: %w", img.h, err)
	}
	blobs := []v1.Hash{m.Config.Digest}
	for _, l := range m.Layers {
		blobs = append(blobs, l.Digest)
	}
	for _, h := range blobs {
		if _, err := os.Stat(s.ociBlobPath(h)); err != nil {
			return v1.Hash{}, fmt.Errorf("archive %s: blob %s: %w", img.h, h, blobErr(err))
		}
	}
	tw := tar.NewWriter(&ctxWriter{ctx: ctx, w: w})
	var id v1.Hash
	switch form {
	case DockerArchive:
		entry := struct {
			Config   string   `json:"Config"`
			RepoTags []string `json:"RepoTags"`
			Layers   []string `json:"Layers"`
		}{Config: m.Config.Digest.Hex + ".json"}
		for _, l := range m.Layers {
			entry.Layers = append(entry.Layers, l.Digest.Hex+"/layer.tar")
		}
		manifest, err := json.Marshal([]any{entry})
		if err != nil {
			return v1.Hash{}, err
		}
		if err := writeBytes(tw, "manifest.json", manifest); err != nil {
			return v1.Hash{}, err
		}
		if err := s.writeBlob(tw, entry.Config, m.Config.Digest); err != nil {
			return v1.Hash{}, fmt.Errorf("archive %s: blob %s: %w", img.h, m.Config.Digest, err)
		}
		for i, l := range m.Layers {
			if err := s.writeBlob(tw, entry.Layers[i], l.Digest); err != nil {
				return v1.Hash{}, fmt.Errorf("archive %s: blob %s: %w", img.h, l.Digest, err)
			}
		}
		id = m.Config.Digest
	case OCILayout:
		mediaType := string(m.MediaType)
		if mediaType == "" {
			mediaType = string(types.OCIManifestSchema1)
		}
		// The descriptor names the image's platform from its
		// configuration, which a store filtering by platform reads.
		platform := map[string]any{"os": img.conf.OS, "architecture": img.conf.Architecture}
		if img.conf.Variant != "" {
			platform["variant"] = img.conf.Variant
		}
		index, err := json.Marshal(map[string]any{
			"schemaVersion": 2,
			"manifests":     []map[string]any{{"mediaType": mediaType, "digest": img.h.String(), "size": len(raw), "platform": platform}},
		})
		if err != nil {
			return v1.Hash{}, err
		}
		if err := writeBytes(tw, "oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)); err != nil {
			return v1.Hash{}, err
		}
		if err := writeBytes(tw, "index.json", index); err != nil {
			return v1.Hash{}, err
		}
		if err := writeBytes(tw, "blobs/"+img.h.Algorithm+"/"+img.h.Hex, raw); err != nil {
			return v1.Hash{}, err
		}
		for _, h := range blobs {
			if err := s.writeBlob(tw, "blobs/"+h.Algorithm+"/"+h.Hex, h); err != nil {
				return v1.Hash{}, fmt.Errorf("archive %s: blob %s: %w", img.h, h, err)
			}
		}
		id = img.h
	default:
		return v1.Hash{}, fmt.Errorf("archive %s: no archive form %d", img.h, form)
	}
	if err := tw.Close(); err != nil {
		return v1.Hash{}, err
	}
	return id, nil
}

// retainedManifest reads the manifest retained under oci/ at h, the
// bytes and their parse.
func (s *Store) retainedManifest(h v1.Hash) (*v1.Manifest, []byte, error) {
	raw, err := os.ReadFile(s.ociBlobPath(h))
	if err != nil {
		return nil, nil, blobErr(err)
	}
	m, err := v1.ParseManifest(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	return m, raw, nil
}

// writeBlob writes the retained blob at h as the archive entry name.
func (s *Store) writeBlob(tw *tar.Writer, name string, h v1.Hash) error {
	f, err := os.Open(s.ociBlobPath(h))
	if err != nil {
		return blobErr(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return writeEntry(tw, name, fi.Size(), func(w io.Writer) error {
		_, err := io.Copy(w, f)
		return err
	})
}

// writeBytes writes b as the archive entry name.
func writeBytes(tw *tar.Writer, name string, b []byte) error {
	return writeEntry(tw, name, int64(len(b)), func(w io.Writer) error {
		_, err := w.Write(b)
		return err
	})
}

// writeEntry writes one regular file of the archive, its directory
// implied: a mode of 0644, root-owned, no timestamp.
func writeEntry(tw *tar.Writer, name string, size int64, body func(io.Writer) error) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: size, Format: tar.FormatPAX}); err != nil {
		return err
	}
	return body(tw)
}

// ctxWriter is a writer that refuses once the context ends, before
// the first byte and between a blob's writes alike.
type ctxWriter struct {
	ctx context.Context
	w   io.Writer
}

func (c *ctxWriter) Write(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.w.Write(p)
}

// blobErr names an absent blob as gone: the acquisition's content
// collected from under the image, which a re-acquisition restores.
func blobErr(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w (collected since the acquisition)", ErrGone)
	}
	return err
}
