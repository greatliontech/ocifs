# Store Package Architecture

The `store` package manages OCI container images locally, providing a content-addressed storage system with support for pulling, caching, modifying, and pushing images.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Store                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │  BlobStore   │  │ ContentStore │  │    WritableLayer       │ │
│  │  (CAS)       │  │  (files)     │  │    (CoW upper)         │ │
│  └──────────────┘  └──────────────┘  └────────────────────────┘ │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │   Image      │  │    Layer     │  │   referenceStore       │ │
│  │  (unified)   │  │  (metadata)  │  │   (tag→digest)         │ │
│  └──────────────┘  └──────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Disk Layout                                   │
│                                                                  │
│  workdir/                                                        │
│  ├── oci/                    # OCI layout (go-containerregistry) │
│  │   ├── index.json                                              │
│  │   └── blobs/sha256/...    # Compressed layer tarballs         │
│  ├── blobs/sha256/...        # Extracted file content (CAS)      │
│  ├── refs/                   # Reference → digest mappings       │
│  └── mounts/                 # Mount point directories           │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### Store

The main entry point for image operations.

```go
store, err := NewStore(workDir, keychain, PullIfNotPresent,
    WithDefaultPlatform(v1.Platform{OS: "linux", Architecture: "arm64"}),
)
```

**Options:**
- `WithDefaultPlatform(platform)` - Set target platform for multi-arch images (defaults to `runtime.GOOS/runtime.GOARCH`)

**Methods:**
- `Image(ctx, ref)` - Pull and load an image
- `Commit(ctx, base, writableLayer, opts)` - Create new image from changes
- `Push(ctx, image, ref)` - Push image to remote registry
- `Tag(image, ref)` - Associate a reference with an image locally
- `ListPlatforms(ctx, ref)` - List available platforms for an image
- `Platform()` - Get configured target platform
- `BlobStore()` - Access the content-addressed blob storage

### WritableLayer

Copy-on-write layer for tracking filesystem modifications.

```go
wl, err := NewWritableLayer(dir,
    WithAutoPersist(5*time.Minute),       // Auto-save every 5 minutes
    WithPersistAfterMutations(100),       // Auto-save after 100 changes
)
defer wl.Close()
```

**Options:**
- `WithAutoPersist(interval)` - Periodic metadata persistence
- `WithPersistAfterMutations(n)` - Persist after N mutations
- `WithContentStore(cs)` - Custom content storage

**Key Methods:**
- `Create(path, mode, isDir)` - Create file or directory
- `Get(path)` - Get file metadata (returns copy)
- `Update(file)` - Update file metadata
- `Remove(path)` - Remove file
- `Whiteout(path)` - Mark file as deleted (for lower layers)
- `CopyUp(path, hdr)` - Copy file from read-only layer for modification
- `ToLayer()` - Convert to OCI v1.Layer for commit
- `ContentPath(path)` - Get filesystem path for file content
- `IsDirty()` - Check if there are unsaved changes

### BlobStore Interface

Content-addressed storage for file contents.

```go
type BlobStore interface {
    Get(ref string) (io.ReadCloser, error)  // ref = "sha256:abc123..."
    Put(r io.Reader) (ref string, error)
    Exists(ref string) bool
    Delete(ref string) error
}
```

### ContentStore Interface

Simple filesystem abstraction for writable content.

```go
type ContentStore interface {
    Open(path string, flags int, mode os.FileMode) (*os.File, error)
    Create(path string) (*os.File, error)
    Remove(path string) error
    Stat(path string) (os.FileInfo, error)
    MkdirAll(path string, mode os.FileMode) error
    ContentPath(path string) string
}
```

## Multi-Architecture Support

### Default Platform Selection

The store defaults to the current runtime platform (`runtime.GOOS/runtime.GOARCH`), not the go-containerregistry default of `amd64/linux`.

```go
// Uses current machine's platform
store, _ := NewStore(dir, auth, policy)

// Explicitly set platform for cross-platform work
store, _ := NewStore(dir, auth, policy,
    WithDefaultPlatform(v1.Platform{OS: "linux", Architecture: "arm64"}),
)
```

### Listing Available Platforms

Query platforms before pulling:

```go
platforms, err := store.ListPlatforms(ctx, "docker.io/library/alpine:latest")
for _, p := range platforms {
    fmt.Printf("%s/%s\n", p.OS, p.Architecture)
}
// linux/amd64
// linux/arm64
// linux/arm/v7
// ...
```

### Platform Preserved on Commit

When committing changes to an image, the platform information is preserved automatically.

## Commit Workflow

```go
// 1. Pull base image
img, _ := store.Image(ctx, "alpine:latest")

// 2. Create writable layer
wl, _ := NewWritableLayer(dir)

// 3. Make modifications
f, _ := wl.Create("app/config.json", 0644, false)
// Write content to wl.ContentPath("app/config.json")
wl.Update(f)

// 4. Commit changes
newImg, _ := store.Commit(ctx, img, wl, CommitOptions{
    Author:    "user@example.com",
    Comment:   "Add configuration",
    CreatedBy: "ocifs commit",
})

// 5. Push to registry
store.Push(ctx, newImg, "myregistry.com/myimage:v1")
```

## Whiteout Handling

Whiteouts follow the OCI image spec for marking deletions in overlay filesystems:

- File deletion: Creates `.wh.<filename>` marker
- Directory deletion: Creates `.wh.<dirname>` marker
- Opaque directory: Creates `.wh..wh..opq` inside directory

```go
// Mark file as deleted in lower layers
wl.Whiteout("etc/passwd")  // Creates .wh.passwd entry

// Check whiteouts
whiteouts := wl.Whiteouts()  // Returns list of whiteout paths
```

## Pull Policies

| Policy | Behavior |
|--------|----------|
| `PullAlways` | Check remote for updates, pull if changed |
| `PullIfNotPresent` | Use cached image if available |
| `PullNever` | Only use cached images, error if not present |

## Out of Scope

The following features are intentionally not implemented as they don't fit the FUSE mount use case:

| Feature | Reason |
|---------|--------|
| `IsMultiArch()` method | Use `len(ListPlatforms()) > 1` instead |
| Image index creation/push | FUSE mount modifies single images, not multi-arch builds |
| Per-call platform selection | Store-level default sufficient; use separate stores if needed |
| Shared layer deduplication | Typically only one platform pulled at a time |

## Test Coverage

The store package has comprehensive test coverage (79 tests):

- Pull operations with all policies
- Multi-arch platform selection and listing
- Commit with history preservation
- Push to remote registries
- Whiteout handling
- Auto-persist functionality
- BlobStore and ContentStore interfaces
- Layer content verification

Tests use go-containerregistry's in-memory registry (`pkg/registry`) for isolated, fast testing without network dependencies.
