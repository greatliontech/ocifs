package store

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// WhiteoutPrefix is the OCI standard prefix for whiteout files.
	WhiteoutPrefix = ".wh."

	metadataFileName = "metadata.json"
	contentDirName   = "content"
)

// WritableLayer manages the upper (writable) layer of an overlay filesystem.
// It provides a clean API for file operations, whiteout handling, and persistence.
type WritableLayer struct {
	basePath string
	mu       sync.RWMutex
	files    map[string]*File

	// Content storage (optional - uses direct filesystem if nil)
	content ContentStore

	// Auto-persist fields
	dirty            bool          // Has metadata changed since last persist?
	mutations        int           // Count of mutations since last persist
	persistThreshold int           // Persist after this many mutations (0 = disabled)
	persistInterval  time.Duration // Auto-persist interval (0 = disabled)
	persistTicker    *time.Ticker
	persistDone      chan struct{}
	closed           bool
}

// WritableLayerOption configures a WritableLayer.
type WritableLayerOption func(*WritableLayer)

// WithAutoPersist enables automatic periodic persistence at the given interval.
// A reasonable default is 30 seconds.
func WithAutoPersist(interval time.Duration) WritableLayerOption {
	return func(wl *WritableLayer) {
		wl.persistInterval = interval
	}
}

// WithPersistAfterMutations triggers a persist after the given number of mutations.
// This provides durability proportional to write activity.
func WithPersistAfterMutations(n int) WritableLayerOption {
	return func(wl *WritableLayer) {
		wl.persistThreshold = n
	}
}

// WithContentStore injects a ContentStore for managing file content.
// If not provided, the WritableLayer uses direct filesystem operations.
func WithContentStore(cs ContentStore) WritableLayerOption {
	return func(wl *WritableLayer) {
		wl.content = cs
	}
}

// NewWritableLayer creates a new writable layer at the given path.
// If metadata exists from a previous session, it will be loaded.
func NewWritableLayer(basePath string, opts ...WritableLayerOption) (*WritableLayer, error) {
	contentDir := filepath.Join(basePath, contentDirName)
	if err := os.MkdirAll(contentDir, 0755); err != nil {
		return nil, fmt.Errorf("create content dir: %w", err)
	}

	wl := &WritableLayer{
		basePath: basePath,
		files:    make(map[string]*File),
	}

	// Apply options
	for _, opt := range opts {
		opt(wl)
	}

	if err := wl.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	// Start auto-persist if configured
	if wl.persistInterval > 0 {
		wl.startAutoPersist()
	}

	return wl, nil
}

// startAutoPersist begins the background persistence goroutine.
func (wl *WritableLayer) startAutoPersist() {
	wl.persistTicker = time.NewTicker(wl.persistInterval)
	wl.persistDone = make(chan struct{})

	go func() {
		for {
			select {
			case <-wl.persistTicker.C:
				wl.mu.RLock()
				dirty := wl.dirty
				wl.mu.RUnlock()

				if dirty {
					if err := wl.Persist(); err != nil {
						slog.Error("auto-persist failed", "error", err)
					} else {
						slog.Debug("auto-persist completed")
					}
				}
			case <-wl.persistDone:
				return
			}
		}
	}()
}

// markDirtyLocked marks the layer as having unsaved changes.
// Caller must hold wl.mu. Returns true if threshold persist should be triggered.
func (wl *WritableLayer) markDirtyLocked() bool {
	wl.dirty = true
	wl.mutations++
	return wl.persistThreshold > 0 && wl.mutations >= wl.persistThreshold
}

// triggerThresholdPersist triggers an async persist if needed.
// Call this after releasing the lock if markDirtyLocked returned true.
func (wl *WritableLayer) triggerThresholdPersist() {
	go func() {
		if err := wl.Persist(); err != nil {
			slog.Error("threshold-persist failed", "error", err)
		} else {
			slog.Debug("threshold-persist completed", "threshold", wl.persistThreshold)
		}
	}()
}

// Close stops auto-persist and performs a final persist.
// After Close, the WritableLayer should not be used.
func (wl *WritableLayer) Close() error {
	wl.mu.Lock()
	if wl.closed {
		wl.mu.Unlock()
		return nil
	}
	wl.closed = true
	wl.mu.Unlock()

	// Stop the ticker if running
	if wl.persistTicker != nil {
		wl.persistTicker.Stop()
		close(wl.persistDone)
	}

	// Final persist
	return wl.Persist()
}

// IsDirty returns true if there are unsaved changes.
func (wl *WritableLayer) IsDirty() bool {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	return wl.dirty
}

// =============================================================================
// Lookup Operations
// =============================================================================

// Get returns the file at the given path, or nil if not found.
// The returned File is a copy safe for modification.
func (wl *WritableLayer) Get(path string) *File {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	if f, ok := wl.files[path]; ok {
		copy := *f
		return &copy
	}
	return nil
}

// Exists returns true if a file exists at the given path.
func (wl *WritableLayer) Exists(path string) bool {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	_, ok := wl.files[path]
	return ok
}

// IsWhiteout returns true if the given path has been marked as deleted.
func (wl *WritableLayer) IsWhiteout(path string) bool {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	whPath := toWhiteoutPath(path)
	_, ok := wl.files[whPath]
	return ok
}

// =============================================================================
// Directory Operations
// =============================================================================

// List returns immediate children of the directory at dirPath.
// Whiteout markers are excluded from the result.
func (wl *WritableLayer) List(dirPath string) []*File {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	prefix := dirPath
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var children []*File
	for key, file := range wl.files {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		relPath := strings.TrimPrefix(key, prefix)
		// Only immediate children (no "/" in relative path)
		if relPath != "" && !strings.Contains(relPath, "/") {
			// Skip whiteout markers
			if !strings.HasPrefix(filepath.Base(key), WhiteoutPrefix) {
				copy := *file
				children = append(children, &copy)
			}
		}
	}
	return children
}

// Whiteouts returns the names of files that have been whited out in dirPath.
// These are the original file names (without the .wh. prefix).
func (wl *WritableLayer) Whiteouts(dirPath string) []string {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	prefix := dirPath
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var names []string
	for key := range wl.files {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		relPath := strings.TrimPrefix(key, prefix)
		if strings.Contains(relPath, "/") {
			continue
		}
		baseName := filepath.Base(key)
		if strings.HasPrefix(baseName, WhiteoutPrefix) {
			originalName := strings.TrimPrefix(baseName, WhiteoutPrefix)
			names = append(names, originalName)
		}
	}
	return names
}

// =============================================================================
// Mutation Operations
// =============================================================================

// Create creates a new file or directory and returns its metadata.
// For files, use OpenContent() to write the actual content.
func (wl *WritableLayer) Create(path string, mode os.FileMode, isDir bool) (*File, error) {
	wl.mu.Lock()

	contentPath := wl.contentPath(path)

	// Create parent directories for content
	parentDir := contentPath
	if !isDir {
		parentDir = filepath.Dir(contentPath)
	}
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create parent dirs: %w", err)
	}

	now := time.Now()
	typeflag := tar.TypeReg
	if isDir {
		typeflag = tar.TypeDir
		mode |= os.ModeDir
	}

	f := &File{
		Hdr: tar.Header{
			Name:       path,
			Mode:       int64(mode.Perm()),
			Typeflag:   byte(typeflag),
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
		Path: contentPath,
	}

	if isDir {
		f.Hdr.Mode |= int64(os.ModeDir)
	}

	wl.files[path] = f
	shouldPersist := wl.markDirtyLocked()

	copy := *f
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return &copy, nil
}

// Update updates the metadata for an existing file.
// The file must already exist in the writable layer.
func (wl *WritableLayer) Update(f *File) error {
	wl.mu.Lock()

	if _, ok := wl.files[f.Hdr.Name]; !ok {
		wl.mu.Unlock()
		return fmt.Errorf("file not found: %s", f.Hdr.Name)
	}

	// Update modification time
	f.Hdr.ModTime = time.Now()
	f.Hdr.ChangeTime = time.Now()

	copy := *f
	wl.files[f.Hdr.Name] = &copy
	shouldPersist := wl.markDirtyLocked()
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return nil
}

// Remove deletes a file from the writable layer.
// This removes both metadata and content.
func (wl *WritableLayer) Remove(path string) error {
	wl.mu.Lock()

	f, ok := wl.files[path]
	if !ok {
		wl.mu.Unlock()
		return nil // Nothing to remove
	}

	// Remove content file if it exists
	if f.Path != "" {
		if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			wl.mu.Unlock()
			return fmt.Errorf("remove content: %w", err)
		}
	}

	delete(wl.files, path)
	shouldPersist := wl.markDirtyLocked()
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return nil
}

// Whiteout marks a path as deleted by creating a whiteout marker.
// This is used when deleting files that exist in read-only layers.
func (wl *WritableLayer) Whiteout(path string) error {
	wl.mu.Lock()

	whPath := toWhiteoutPath(path)
	contentPath := wl.contentPath(whPath)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(contentPath), 0755); err != nil {
		wl.mu.Unlock()
		return fmt.Errorf("create whiteout parent dirs: %w", err)
	}

	// Create empty whiteout file on disk
	f, err := os.Create(contentPath)
	if err != nil {
		wl.mu.Unlock()
		return fmt.Errorf("create whiteout file: %w", err)
	}
	f.Close()

	// Add to metadata
	wl.files[whPath] = &File{
		Hdr: tar.Header{
			Name:     whPath,
			Mode:     0,
			Size:     0,
			Typeflag: tar.TypeReg,
		},
		Path: contentPath,
	}

	shouldPersist := wl.markDirtyLocked()
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return nil
}

// RemoveWhiteout removes a whiteout marker, making the underlying file visible again.
func (wl *WritableLayer) RemoveWhiteout(path string) error {
	whPath := toWhiteoutPath(path)
	return wl.Remove(whPath)
}

// CreateHardlink creates a hard link at linkPath pointing to targetPath.
// Both paths must be in the writable layer.
func (wl *WritableLayer) CreateHardlink(linkPath, targetPath string) (*File, error) {
	wl.mu.Lock()

	// Get target file metadata
	targetFile, ok := wl.files[targetPath]
	if !ok {
		wl.mu.Unlock()
		return nil, fmt.Errorf("target not found: %s", targetPath)
	}

	linkContentPath := wl.contentPath(linkPath)
	targetContentPath := wl.contentPath(targetPath)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(linkContentPath), 0755); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create parent dirs: %w", err)
	}

	// Create the actual hard link on disk
	if err := os.Link(targetContentPath, linkContentPath); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create hardlink: %w", err)
	}

	now := time.Now()
	f := &File{
		Hdr: tar.Header{
			Name:       linkPath,
			Mode:       targetFile.Hdr.Mode,
			Uid:        targetFile.Hdr.Uid,
			Gid:        targetFile.Hdr.Gid,
			Size:       targetFile.Hdr.Size,
			Typeflag:   tar.TypeLink,
			Linkname:   targetPath,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
		Path: linkContentPath,
	}

	wl.files[linkPath] = f
	shouldPersist := wl.markDirtyLocked()

	copy := *f
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return &copy, nil
}

// CreateSymlink creates a symbolic link at path pointing to target.
func (wl *WritableLayer) CreateSymlink(path, target string) (*File, error) {
	wl.mu.Lock()

	contentPath := wl.contentPath(path)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(contentPath), 0755); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create parent dirs: %w", err)
	}

	// Remove any existing file at the content path
	os.Remove(contentPath)

	// Create the actual symlink on disk
	if err := os.Symlink(target, contentPath); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create symlink: %w", err)
	}

	now := time.Now()
	f := &File{
		Hdr: tar.Header{
			Name:       path,
			Mode:       int64(os.ModeSymlink | 0777),
			Typeflag:   tar.TypeSymlink,
			Linkname:   target,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
		Path: contentPath,
	}

	wl.files[path] = f
	shouldPersist := wl.markDirtyLocked()

	copy := *f
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return &copy, nil
}

// =============================================================================
// Copy-on-Write Support
// =============================================================================

// CopyUp copies a file from a read-only layer to the writable layer.
// It copies both content and metadata, returning the new writable File.
func (wl *WritableLayer) CopyUp(srcFile *File, srcContent io.Reader) (*File, error) {
	wl.mu.Lock()

	path := srcFile.Hdr.Name
	contentPath := wl.contentPath(path)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(contentPath), 0755); err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create parent dirs: %w", err)
	}

	// Copy content
	dest, err := os.Create(contentPath)
	if err != nil {
		wl.mu.Unlock()
		return nil, fmt.Errorf("create dest file: %w", err)
	}

	written, err := io.Copy(dest, srcContent)
	if err != nil {
		dest.Close()
		os.Remove(contentPath)
		wl.mu.Unlock()
		return nil, fmt.Errorf("copy content: %w", err)
	}
	dest.Close()

	// Create new file with copied metadata
	now := time.Now()
	f := &File{
		Hdr: tar.Header{
			Name:       srcFile.Hdr.Name,
			Mode:       srcFile.Hdr.Mode,
			Uid:        srcFile.Hdr.Uid,
			Gid:        srcFile.Hdr.Gid,
			Size:       written,
			Typeflag:   srcFile.Hdr.Typeflag,
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		},
		Path: contentPath,
	}

	wl.files[path] = f
	shouldPersist := wl.markDirtyLocked()

	copy := *f
	wl.mu.Unlock()

	if shouldPersist {
		wl.triggerThresholdPersist()
	}

	return &copy, nil
}

// =============================================================================
// Content Access
// =============================================================================

// ContentPath returns the on-disk path where the file's content is stored.
func (wl *WritableLayer) ContentPath(path string) string {
	return wl.contentPath(path)
}

// OpenContent opens the content file with the given flags.
// Creates parent directories if needed.
func (wl *WritableLayer) OpenContent(path string, flags int, mode os.FileMode) (*os.File, error) {
	contentPath := wl.contentPath(path)

	// Create parent directories if creating
	if flags&os.O_CREATE != 0 {
		if err := os.MkdirAll(filepath.Dir(contentPath), 0755); err != nil {
			return nil, fmt.Errorf("create parent dirs: %w", err)
		}
	}

	return os.OpenFile(contentPath, flags, mode)
}

// =============================================================================
// Persistence
// =============================================================================

// Persist saves all metadata to disk.
// Content is already on disk; this only persists the metadata index.
func (wl *WritableLayer) Persist() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	return wl.persistLocked()
}

// persistLocked performs the actual persist. Caller must hold wl.mu.
// Uses atomic write (write to temp file, then rename) to prevent corruption on crash.
func (wl *WritableLayer) persistLocked() error {
	data, err := json.MarshalIndent(wl.files, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	metaPath := filepath.Join(wl.basePath, metadataFileName)
	tmpPath := metaPath + ".tmp"

	// Write to temp file first
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp metadata: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, metaPath); err != nil {
		os.Remove(tmpPath) // Clean up temp file on failure
		return fmt.Errorf("rename metadata: %w", err)
	}

	// Reset dirty state after successful persist
	wl.dirty = false
	wl.mutations = 0

	return nil
}

// =============================================================================
// Deprecated Methods (for backward compatibility during migration)
// =============================================================================

// GetFile is deprecated. Use Get instead.
func (wl *WritableLayer) GetFile(path string) *File {
	return wl.Get(path)
}

// SetFile is deprecated. Use Create or Update instead.
func (wl *WritableLayer) SetFile(hdr tar.Header) (*File, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	contentPath := wl.contentPath(hdr.Name)

	// Create parent directories
	dir := contentPath
	if hdr.Typeflag != tar.TypeDir {
		dir = filepath.Dir(contentPath)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	f := &File{
		Hdr:  hdr,
		Path: contentPath,
	}
	wl.files[hdr.Name] = f

	copy := *f
	return &copy, nil
}

// DeleteFile is deprecated. Use Remove instead.
func (wl *WritableLayer) DeleteFile(path string) error {
	return wl.Remove(path)
}

// ListChildren is deprecated. Use List and Whiteouts instead.
func (wl *WritableLayer) ListChildren(dirPath string) []*File {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	prefix := dirPath
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var children []*File
	for key, file := range wl.files {
		if strings.HasPrefix(key, prefix) {
			relPath := strings.TrimPrefix(key, prefix)
			if !strings.Contains(relPath, "/") {
				copy := *file
				children = append(children, &copy)
			}
		}
	}
	return children
}

// =============================================================================
// Internal Helpers
// =============================================================================

func (wl *WritableLayer) contentPath(name string) string {
	return filepath.Join(wl.basePath, contentDirName, name)
}

func (wl *WritableLayer) load() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	metaPath := filepath.Join(wl.basePath, metadataFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &wl.files)
}

// toWhiteoutPath converts a regular path to its whiteout marker path.
func toWhiteoutPath(path string) string {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if dir == "." {
		return WhiteoutPrefix + base
	}
	return filepath.Join(dir, WhiteoutPrefix+base)
}
