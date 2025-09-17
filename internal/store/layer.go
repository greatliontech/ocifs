package store

import (
	"encoding/json"
	"os"
)

type Layer struct {
	files []*File
	path  string
}

func (l *Layer) Files() []*File {
	return l.files
}

// layerMetadata is used for persisting layer metadata to a JSON file.
type layerMetadata struct {
	Files []*File
}

func (l *Layer) Load() error {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return err
	}
	meta := &layerMetadata{}
	if err := json.Unmarshal(data, meta); err != nil {
		return err
	}
	l.files = meta.Files
	return nil
}

func (l *Layer) Persist() error {
	meta := &layerMetadata{
		Files: l.files,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(l.path, data, 0644)
}
