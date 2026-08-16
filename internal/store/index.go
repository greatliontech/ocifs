package store

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/atomicfile"
	"github.com/greatliontech/ocifs/internal/layer"
)

// layerIndexes is the layers/ tier (docs/specs/store.md
// REQ-store-layout): one JSON document per unpacked layer, keyed by
// the layer digest the manifest lists, recording the layer's entries
// in tar order with content-CAS keys for regular files. The tier is
// disjoint from the content CAS by construction — it has its own
// root — so a layer digest colliding with a content digest names two
// different files (REQ-store-ns).
//
// Header strings are arbitrary bytes (names, link targets, xattr
// keys and values — a security.capability blob is binary), and JSON
// strings are not: every string field persists base64-encoded.
// Documents carry a format version; any other version is
// unparseable state and heals by re-unpacking, exactly like a
// missing document (REQ-store-self-heal) — the index is a cache
// regenerable from the retained blob.
type layerIndexes struct {
	root string
}

const indexFormatVersion = 2

// indexEntry is the persisted form of one layer entry. It carries no
// filesystem paths: content is named by CAS key only, so the store
// stays relocatable. An empty digest means the entry has no content
// (directories, links, devices).
type indexEntry struct {
	Typeflag byte              `json:"typeflag"`
	Name     string            `json:"name_b64"`
	Linkname string            `json:"linkname_b64,omitempty"`
	Mode     int64             `json:"mode"`
	UID      int               `json:"uid"`
	GID      int               `json:"gid"`
	Uname    string            `json:"uname_b64,omitempty"`
	Gname    string            `json:"gname_b64,omitempty"`
	Size     int64             `json:"size,omitempty"`
	ModTime  time.Time         `json:"mtime"`
	Atime    time.Time         `json:"atime,omitzero"`
	Ctime    time.Time         `json:"ctime,omitzero"`
	Devmajor int64             `json:"devmajor,omitempty"`
	Devminor int64             `json:"devminor,omitempty"`
	PAX      map[string]string `json:"pax_b64,omitempty"`
	Xattrs   map[string]string `json:"xattrs_b64,omitempty"`
	Digest   string            `json:"digest,omitempty"`
}

type indexDoc struct {
	Version int          `json:"version"`
	Entries []indexEntry `json:"entries"`
}

func (li layerIndexes) path(layerDigest v1.Hash) string {
	return filepath.Join(li.root, layerDigest.Algorithm, layerDigest.Hex)
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func unb64(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	return string(b), err
}

func b64Map(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[b64(k)] = b64(v)
	}
	return out
}

func unb64Map(m map[string]string) (map[string]string, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		dk, err := unb64(k)
		if err != nil {
			return nil, err
		}
		dv, err := unb64(v)
		if err != nil {
			return nil, err
		}
		out[dk] = dv
	}
	return out, nil
}

// Get loads the index for layerDigest. A missing document,
// unparseable bytes, or a foreign format version all return errors —
// heal triggers for the caller (REQ-store-self-heal), never served
// state.
func (li layerIndexes) Get(layerDigest v1.Hash) (layer.Layer, error) {
	data, err := os.ReadFile(li.path(layerDigest))
	if err != nil {
		return nil, err
	}
	var doc indexDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("layer index %s: %w", layerDigest, err)
	}
	if doc.Version != indexFormatVersion {
		return nil, fmt.Errorf("layer index %s: format version %d (want %d): %w", layerDigest, doc.Version, indexFormatVersion, os.ErrNotExist)
	}
	l := make(layer.Layer, len(doc.Entries))
	for i, e := range doc.Entries {
		name, err := unb64(e.Name)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		link, err := unb64(e.Linkname)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		uname, err := unb64(e.Uname)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		gname, err := unb64(e.Gname)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		pax, err := unb64Map(e.PAX)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		xattrs, err := unb64Map(e.Xattrs)
		if err != nil {
			return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
		}
		l[i] = layer.Entry{Header: tar.Header{
			Typeflag:   e.Typeflag,
			Name:       name,
			Linkname:   link,
			Mode:       e.Mode,
			Uid:        e.UID,
			Gid:        e.GID,
			Uname:      uname,
			Gname:      gname,
			Size:       e.Size,
			ModTime:    e.ModTime,
			AccessTime: e.Atime,
			ChangeTime: e.Ctime,
			Devmajor:   e.Devmajor,
			Devminor:   e.Devminor,
			PAXRecords: pax,
			Xattrs:     xattrs, //nolint:staticcheck // legacy producers round-trip
		}}
		if e.Digest != "" {
			h, err := v1.NewHash(e.Digest)
			if err != nil {
				return nil, fmt.Errorf("layer index %s entry %d: %w", layerDigest, i, err)
			}
			l[i].Digest = h
		}
	}
	return l, nil
}

// Put publishes the index for layerDigest atomically.
func (li layerIndexes) Put(layerDigest v1.Hash, l layer.Layer) error {
	doc := indexDoc{Version: indexFormatVersion, Entries: make([]indexEntry, len(l))}
	for i, e := range l {
		h := e.Header
		doc.Entries[i] = indexEntry{
			Typeflag: h.Typeflag,
			Name:     b64(h.Name),
			Linkname: b64(h.Linkname),
			Mode:     h.Mode,
			UID:      h.Uid,
			GID:      h.Gid,
			Uname:    b64(h.Uname),
			Gname:    b64(h.Gname),
			Size:     h.Size,
			ModTime:  h.ModTime,
			Atime:    h.AccessTime,
			Ctime:    h.ChangeTime,
			Devmajor: h.Devmajor,
			Devminor: h.Devminor,
			PAX:      b64Map(h.PAXRecords),
			Xattrs:   b64Map(h.Xattrs), //nolint:staticcheck // legacy producers round-trip
		}
		if e.Digest != (v1.Hash{}) {
			doc.Entries[i].Digest = e.Digest.String()
		}
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return atomicfile.Write(li.path(layerDigest), bytes.NewReader(data), 0o644)
}
