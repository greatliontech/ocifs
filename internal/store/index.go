package store

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

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
type layerIndexes struct {
	root string
}

// indexEntry is the persisted form of one layer entry. It carries no
// filesystem paths: content is named by CAS key only, so the store
// stays relocatable. An empty digest means the entry has no content
// (directories, links, devices).
type indexEntry struct {
	Header tar.Header `json:"header"`
	Digest string     `json:"digest,omitempty"`
}

type indexDoc struct {
	Entries []indexEntry `json:"entries"`
}

func (li layerIndexes) path(layerDigest v1.Hash) string {
	return filepath.Join(li.root, layerDigest.Algorithm, layerDigest.Hex)
}

// Get loads the index for layerDigest. A missing or unparseable
// document returns os.ErrNotExist-classed or decode errors; both are
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
	l := make(layer.Layer, len(doc.Entries))
	for i, e := range doc.Entries {
		l[i] = layer.Entry{Header: e.Header}
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
	doc := indexDoc{Entries: make([]indexEntry, len(l))}
	for i, e := range l {
		doc.Entries[i] = indexEntry{Header: e.Header}
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
