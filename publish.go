package ocifs

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"

	"github.com/greatliontech/ocifs/internal/store"
)

// ErrTagTaken refuses publishing to a tag that already names a
// different artifact unless PublishRequest.Force is true.
var ErrTagTaken = errors.New("ocifs: tag already names another artifact")

// PlatformTree specifies one platform's root filesystem directory and
// an optional digest-pinned base image or index reference
// (`<repository>@sha256:<hex>`).
type PlatformTree struct {
	Dir  string
	Base string
}

// BuildRequest specifies a deterministic multi-platform OCI image
// index build without requiring a container engine.
type BuildRequest struct {
	// Entrypoint is the image's process argv; Entrypoint[0] must be a
	// clean absolute slash path ("/...") naming a regular file present
	// in every platform tree.
	Entrypoint []string
	// Executables lists additional clean absolute slash paths ("/...")
	// marked 0755 in every platform tree alongside Entrypoint[0]. Host
	// permission bits, ownership, and timestamps are never consulted.
	Executables []string
	// Platforms maps `<os>/<arch>` to its PlatformTree.
	Platforms map[string]PlatformTree
	// Transport and Keychain reach base registries when PlatformTree.Base
	// is set; nil uses the default transport and ocifs's ctx-cancellable
	// ambient keychain.
	Transport http.RoundTripper
	Keychain  authn.Keychain
}

// PlatformImage records a single platform's built manifest digest.
type PlatformImage struct {
	Platform string
	Digest   v1.Hash
}

// BuiltIndex holds a deterministically assembled OCI image index and
// its per-platform child digests in sorted `<os>/<arch>` order.
type BuiltIndex struct {
	Index  v1.ImageIndex
	Digest v1.Hash
	Images []PlatformImage
}

// PublishRequest builds and publishes an OCI image index to an
// explicit tag reference.
type PublishRequest struct {
	// Reference is a full explicit tag reference (e.g.
	// `ghcr.io/org/plugin:v1.0.0`); implicit registries or implicit
	// `:latest` tags are rejected.
	Reference string
	BuildRequest
	// Force permits overwriting an existing tag that points to a
	// different digest; when false (default), tag collisions fail with
	// ErrTagTaken.
	Force bool
}

// Published reports the result of Publish.
type Published struct {
	Reference string
	Digest    v1.Hash
	Images    []PlatformImage
	// Unchanged is true when Reference already pointed to the exact
	// built Digest, in which case no upload was performed.
	Unchanged bool
}

// BuildIndex assembles a deterministic multi-platform OCI image index
// using the store's configured transport and keychain unless
// overridden on req (api.md REQ-api-publish).
func (o *OCIFS) BuildIndex(ctx context.Context, req BuildRequest) (*BuiltIndex, error) {
	if o == nil {
		return nil, errors.New("ocifs: nil OCIFS store")
	}
	if req.Transport == nil {
		req.Transport = o.store.Transport()
	}
	if req.Keychain == nil {
		req.Keychain = o.Keychain()
	}
	return BuildIndex(ctx, req)
}

// Publish builds and pushes a deterministic multi-platform OCI image
// index using the store's configured transport and keychain unless
// overridden on req (api.md REQ-api-publish).
func (o *OCIFS) Publish(ctx context.Context, req PublishRequest) (*Published, error) {
	if o == nil {
		return nil, errors.New("ocifs: nil OCIFS store")
	}
	if req.Transport == nil {
		req.Transport = o.store.Transport()
	}
	if req.Keychain == nil {
		req.Keychain = o.Keychain()
	}
	return Publish(ctx, req)
}

// BuildIndex validates req and assembles the deterministic OCI image
// index in memory without pushing to a registry (api.md REQ-api-publish).
func BuildIndex(ctx context.Context, req BuildRequest) (*BuiltIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := publishRemoteOpts(ctx, req.Transport, req.Keychain)
	idx, images, err := assembleIndex(ctx, req, opts)
	if err != nil {
		return nil, err
	}
	digest, err := idx.Digest()
	if err != nil {
		return nil, err
	}
	return &BuiltIndex{
		Index:  idx,
		Digest: digest,
		Images: images,
	}, nil
}

// Publish validates req, builds the deterministic OCI image index, and
// pushes it to req.Reference, refusing to overwrite a tag naming a
// different digest with ErrTagTaken unless req.Force is true (api.md
// REQ-api-publish).
func Publish(ctx context.Context, req PublishRequest) (*Published, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tag, err := name.NewTag(req.Reference, name.StrictValidation)
	if err != nil {
		return nil, fmt.Errorf("publish: reference %q: %w", req.Reference, err)
	}
	if strings.EqualFold(tag.Context().RegistryStr(), store.LocalRegistry) {
		return nil, fmt.Errorf("publish: reference %q uses reserved local namespace %s", req.Reference, store.LocalRegistry)
	}
	opts := publishRemoteOpts(ctx, req.Transport, req.Keychain)

	idx, images, err := assembleIndex(ctx, req.BuildRequest, opts)
	if err != nil {
		return nil, err
	}
	digest, err := idx.Digest()
	if err != nil {
		return nil, err
	}
	out := &Published{
		Reference: req.Reference,
		Digest:    digest,
		Images:    images,
	}

	held, err := remote.Head(tag, opts...)
	switch {
	case err == nil && held.Digest == digest:
		out.Unchanged = true
		return out, nil
	case err == nil && !req.Force:
		return nil, fmt.Errorf("publish: %s holds %s, build is %s: %w", req.Reference, held.Digest, digest, ErrTagTaken)
	case err != nil && !isRegistryNotFound(err):
		return nil, fmt.Errorf("publish: checking %s: %w", req.Reference, err)
	}

	if err := remote.WriteIndex(tag, idx, opts...); err != nil {
		return nil, fmt.Errorf("publish: pushing %s: %w", req.Reference, err)
	}
	return out, nil
}

func publishRemoteOpts(ctx context.Context, rt http.RoundTripper, kc authn.Keychain) []remote.Option {
	if kc == nil {
		kc = &ocifsKeychain{includeDefaultKeychain: true}
	}
	opts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(kc),
	}
	if rt != nil {
		opts = append(opts, remote.WithTransport(rt))
	}
	return opts
}

func assembleIndex(ctx context.Context, req BuildRequest, opts []remote.Option) (v1.ImageIndex, []PlatformImage, error) {
	if len(req.Entrypoint) == 0 {
		return nil, nil, errors.New("publish: entrypoint is required")
	}
	wantEntry, err := cleanContainerAbsPath(req.Entrypoint[0])
	if err != nil {
		return nil, nil, fmt.Errorf("publish: entrypoint %q: %w", req.Entrypoint[0], err)
	}
	executables := map[string]bool{wantEntry: true}
	for _, rawExec := range req.Executables {
		cleaned, err := cleanContainerAbsPath(rawExec)
		if err != nil {
			return nil, nil, fmt.Errorf("publish: executable %q: %w", rawExec, err)
		}
		executables[cleaned] = true
	}
	if len(req.Platforms) == 0 {
		return nil, nil, errors.New("publish: at least one platform tree is required")
	}

	platforms := make([]string, 0, len(req.Platforms))
	parsedPlatforms := make(map[string]v1.Platform, len(req.Platforms))
	for p := range req.Platforms {
		pl, err := parsePlatformKey(p)
		if err != nil {
			return nil, nil, fmt.Errorf("publish: platform %q: %w", p, err)
		}
		platforms = append(platforms, p)
		parsedPlatforms[p] = pl
	}
	sort.Strings(platforms)

	idx := mutate.IndexMediaType(v1.ImageIndex(empty.Index), types.OCIImageIndex)
	images := make([]PlatformImage, 0, len(platforms))
	for _, p := range platforms {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		tree := req.Platforms[p]
		pl := parsedPlatforms[p]
		lyr, err := canonicalLayerOf(ctx, tree.Dir, wantEntry, executables)
		if err != nil {
			return nil, nil, fmt.Errorf("publish: platform %s: %w", p, err)
		}
		img, err := assemblePlatformImage(lyr, tree.Base, &pl, req.Entrypoint, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("publish: platform %s: %w", p, err)
		}
		d, err := img.Digest()
		if err != nil {
			return nil, nil, err
		}
		images = append(images, PlatformImage{Platform: p, Digest: d})
		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add:        img,
			Descriptor: v1.Descriptor{Platform: &pl},
		})
	}
	return idx, images, nil
}

func cleanContainerAbsPath(raw string) (string, error) {
	if raw == "" || !strings.HasPrefix(raw, "/") {
		return "", errors.New("must be an absolute slash path")
	}
	if strings.HasSuffix(raw, "/") {
		return "", errors.New("must name a file, not a directory")
	}
	if strings.ContainsAny(raw, `\`+"\x00") {
		return "", errors.New("contains invalid path character")
	}
	trimmed := strings.TrimPrefix(raw, "/")
	if trimmed == "" {
		return "", errors.New("must name a file inside the root")
	}
	for seg := range strings.SplitSeq(trimmed, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", errors.New("must be clean without empty, '.', or '..' segments")
		}
	}
	return path.Clean(trimmed), nil
}

func parsePlatformKey(raw string) (v1.Platform, error) {
	osName, arch, ok := strings.Cut(raw, "/")
	if !ok || osName == "" || arch == "" || strings.Contains(arch, "/") {
		return v1.Platform{}, errors.New("must have form <os>/<arch>")
	}
	if !validPlatformToken(osName) || !validPlatformToken(arch) {
		return v1.Platform{}, errors.New("contains invalid platform characters")
	}
	return v1.Platform{OS: osName, Architecture: arch}, nil
}

func validPlatformToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		isLower := c >= 'a' && c <= 'z'
		isDigit := c >= '0' && c <= '9'
		if !isLower && !isDigit && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func assemblePlatformImage(layer v1.Layer, base string, pl *v1.Platform, entrypoint []string, opts []remote.Option) (v1.Image, error) {
	img := v1.Image(empty.Image)
	var descPlatform *v1.Platform
	if base != "" {
		ref, err := name.NewDigest(base, name.StrictValidation)
		if err != nil {
			return nil, fmt.Errorf("base %q must be an explicit digest reference: %w", base, err)
		}
		if strings.EqualFold(ref.Context().RegistryStr(), store.LocalRegistry) {
			return nil, fmt.Errorf("base %q uses reserved local namespace %s", base, store.LocalRegistry)
		}
		fetched, dp, err := fetchBaseImage(ref, pl, opts)
		if err != nil {
			return nil, fmt.Errorf("base %s: %w", base, err)
		}
		baseCfg, err := fetched.ConfigFile()
		if err != nil {
			return nil, fmt.Errorf("base %s: %w", base, err)
		}
		if baseCfg.OS != pl.OS || baseCfg.Architecture != pl.Architecture {
			return nil, fmt.Errorf("base %s is %s/%s, want %s/%s", base, baseCfg.OS, baseCfg.Architecture, pl.OS, pl.Architecture)
		}
		img = fetched
		descPlatform = dp
	}

	// Append the canonical layer BEFORE reading ConfigFile so
	// cfg.RootFS.Type ("layers") and cfg.RootFS.DiffIDs include the
	// appended layer's uncompressed DiffID.
	img, err := mutate.AppendLayers(img, layer)
	if err != nil {
		return nil, err
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cfg = cfg.DeepCopy()
	if cfg.RootFS.Type == "" {
		cfg.RootFS.Type = "layers"
	}

	cfg.OS = pl.OS
	cfg.Architecture = pl.Architecture
	if base != "" {
		osVer := cfg.OSVersion
		if osVer == "" && descPlatform != nil {
			osVer = descPlatform.OSVersion
		}
		variant := cfg.Variant
		if variant == "" && descPlatform != nil {
			variant = descPlatform.Variant
		}
		cfg.OSVersion = osVer
		cfg.Variant = variant
		pl.OSVersion = osVer
		pl.Variant = variant
	} else {
		cfg.OSVersion = ""
		cfg.Variant = ""
		cfg.Config.User = "65534:65534"
		cfg.Config.WorkingDir = "/"
	}
	cfg.Config.Entrypoint = slices.Clone(entrypoint)
	cfg.Config.Cmd = nil
	cfg.Created = v1.Time{}
	cfg.History = nil
	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		return nil, err
	}
	return mutate.MediaType(mutate.ConfigMediaType(img, types.OCIConfigJSON), types.OCIManifestSchema1), nil
}

func fetchBaseImage(ref name.Digest, pl *v1.Platform, opts []remote.Option) (v1.Image, *v1.Platform, error) {
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, nil, err
	}
	if !desc.MediaType.IsIndex() {
		img, err := desc.Image()
		return img, desc.Platform, err
	}
	idx, err := desc.ImageIndex()
	if err != nil {
		return nil, nil, err
	}
	man, err := idx.IndexManifest()
	if err != nil {
		return nil, nil, err
	}
	type matched struct {
		digest   v1.Hash
		platform *v1.Platform
	}
	var matches []matched
	for _, m := range man.Manifests {
		if m.Platform != nil && m.Platform.OS == pl.OS && m.Platform.Architecture == pl.Architecture {
			pCopy := *m.Platform
			matches = append(matches, matched{digest: m.Digest, platform: &pCopy})
		}
	}
	if len(matches) != 1 {
		return nil, nil, fmt.Errorf("index carries %d entries for %s/%s, want 1", len(matches), pl.OS, pl.Architecture)
	}
	img, err := idx.Image(matches[0].digest)
	if err != nil {
		return nil, nil, err
	}
	return img, matches[0].platform, nil
}

func canonicalLayerOf(ctx context.Context, givenDir, wantEntry string, executables map[string]bool) (v1.Layer, error) {
	if givenDir == "" {
		return nil, errors.New("empty tree directory")
	}
	dir, err := filepath.EvalSymlinks(givenDir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", givenDir)
	}
	treeRoot, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer treeRoot.Close()

	type treeEntry struct {
		rel  string
		size int64
		dir  bool
	}
	var entries []treeEntry
	if err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if p == dir {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%s is a symbolic link; platform trees must contain only directories and regular files", filepath.ToSlash(rel))
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() && !fi.Mode().IsDir() {
			return fmt.Errorf("%s is neither a regular file nor a directory", filepath.ToSlash(rel))
		}
		name := filepath.ToSlash(rel)
		if fi.IsDir() {
			name += "/"
		}
		entries = append(entries, treeEntry{
			rel:  name,
			size: fi.Size(),
			dir:  fi.IsDir(),
		})
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].rel < entries[j].rel
	})

	// Reject case-colliding slash paths up front so images built on
	// case-sensitive Linux hosts remain Portable and materializable on
	// case-insensitive macOS/Windows filesystems (export.md REQ-export-fidelity).
	folded := make(map[string]string, len(entries))
	for _, e := range entries {
		clean := strings.TrimSuffix(e.rel, "/")
		key := strings.ToLower(clean)
		if prev, dup := folded[key]; dup && prev != clean {
			return nil, fmt.Errorf("case-colliding paths %q and %q in platform tree", prev, clean)
		}
		folded[key] = clean
	}

	epoch := time.Unix(0, 0).UTC()
	foundEntrypoint := false
	markedExec := make(map[string]bool, len(executables))
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h := &tar.Header{
			Name:    e.rel,
			Uid:     0,
			Gid:     0,
			ModTime: epoch,
			Format:  tar.FormatPAX,
		}
		if e.dir {
			h.Typeflag = tar.TypeDir
			h.Mode = 0o755
		} else {
			h.Typeflag = tar.TypeReg
			h.Size = e.size
			h.Mode = 0o644
			if executables[e.rel] {
				h.Mode = 0o755
				markedExec[e.rel] = true
			}
			if e.rel == wantEntry {
				foundEntrypoint = true
			}
		}
		if err := tw.WriteHeader(h); err != nil {
			return nil, err
		}
		if !e.dir {
			if err := copyExactFile(ctx, tw, treeRoot, e.rel, e.size); err != nil {
				return nil, fmt.Errorf("copying %s: %w", e.rel, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}

	if !foundEntrypoint {
		return nil, fmt.Errorf("entrypoint /%s names no regular file in tree", wantEntry)
	}
	sortedExecs := make([]string, 0, len(executables))
	for k := range executables {
		sortedExecs = append(sortedExecs, k)
	}
	sort.Strings(sortedExecs)
	for _, k := range sortedExecs {
		if !markedExec[k] {
			return nil, fmt.Errorf("executable /%s names no regular file in tree", k)
		}
	}

	return static.NewLayer(buf.Bytes(), types.OCIUncompressedLayer), nil
}

func copyExactFile(ctx context.Context, dst io.Writer, treeRoot *os.Root, relSlash string, wantSize int64) error {
	f, err := treeRoot.Open(filepath.FromSlash(relSlash))
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", relSlash)
	}

	const chunk = 4 << 20
	var copied int64
	for copied < wantSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		step := min(int64(chunk), wantSize-copied)
		n, err := io.CopyN(dst, f, step)
		copied += n
		if err != nil {
			return err
		}
	}
	// Detect concurrent file growth after Lstat and surface any
	// non-EOF read error so mismatched reads never succeed silently.
	var extra [1]byte
	n, rerr := f.Read(extra[:])
	if n > 0 {
		return fmt.Errorf("file grew beyond expected %d bytes during packaging", wantSize)
	}
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return rerr
	}
	return nil
}
