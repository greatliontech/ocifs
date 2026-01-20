package store

import (
	"encoding/json"
	"log/slog"
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
	slog.Debug("loading layer metadata", "path", l.path)

	data, err := os.ReadFile(l.path)
	if err != nil {
		return &PersistError{Path: l.path, Op: "read", Err: err}
	}
	meta := &layerMetadata{}
	if err := json.Unmarshal(data, meta); err != nil {
		return &PersistError{Path: l.path, Op: "unmarshal", Err: err}
	}
	l.files = meta.Files

	slog.Debug("layer metadata loaded", "path", l.path, "files", len(l.files))
	return nil
}

func (l *Layer) Persist() error {
	slog.Debug("persisting layer metadata", "path", l.path, "files", len(l.files))

	meta := &layerMetadata{
		Files: l.files,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return &PersistError{Path: l.path, Op: "marshal", Err: err}
	}

	// Atomic write: write to temp file then rename
	tmpPath := l.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return &PersistError{Path: l.path, Op: "write", Err: err}
	}
	if err := os.Rename(tmpPath, l.path); err != nil {
		os.Remove(tmpPath)
		return &PersistError{Path: l.path, Op: "rename", Err: err}
	}

	slog.Debug("layer metadata persisted", "path", l.path)
	return nil
}
