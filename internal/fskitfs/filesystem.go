package fskitfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	fskit "github.com/greatliontech/fskit-go"

	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/store"
)

// Config is the appex's entire declarative configuration
// (REQ-proj-server): the store location arrives as the mount
// resource (a path or file URL), and image identity, platform, and
// presentation options arrive as mount options — no live control
// channel exists.
type Config struct {
	// Store is the store root. On darwin it must live inside the
	// app-group container the sandboxed extension can open
	// (api.md REQ-api-mount-darwin).
	Store string
	// Image is the digest-form reference (repository@digest) of the
	// image to serve; the appex resolves it against cached content
	// only, never the network (REQ-store-pull-policy Never).
	Image string
	// Platform optionally names an explicit platform.
	Platform string
	// ExtraDirs are the consumer-configured anchor directories.
	ExtraDirs []string
	// State names the per-mount state directory the projection
	// report is written into. Required: the report must have a
	// per-mount home.
	State string
}

// ParseConfig assembles the configuration from the mount resource
// URL and the option vector. Options are comma-separated k=v tokens
// (mount -o style): image=…, platform=…, extra=… (repeatable),
// state=…. Values are percent-encoded by the orchestrator
// (EncodeMountOptions), so paths carrying spaces or commas — the
// app-group container path always carries a space — survive the
// option syntax; unencoded values without reserved bytes parse
// identically for hand-written mounts.
func ParseConfig(resourceURL string, args []string) (Config, error) {
	var cfg Config
	storePath := resourceURL
	if u, err := url.Parse(resourceURL); err == nil && u.Scheme == "file" {
		storePath = u.Path
	}
	if storePath == "" {
		return cfg, fmt.Errorf("mount resource names no store root")
	}
	cfg.Store = filepath.Clean(storePath)

	for _, arg := range args {
		for _, tok := range strings.Split(arg, ",") {
			k, v, ok := strings.Cut(strings.TrimSpace(tok), "=")
			if !ok {
				continue
			}
			if dec, err := url.QueryUnescape(v); err == nil {
				v = dec
			}
			switch k {
			case "image":
				cfg.Image = v
			case "platform":
				cfg.Platform = v
			case "extra":
				cfg.ExtraDirs = append(cfg.ExtraDirs, v)
			case "state":
				cfg.State = v
			}
		}
	}
	if cfg.Image == "" {
		return cfg, fmt.Errorf("mount options name no image (image=repository@digest)")
	}
	if !strings.Contains(cfg.Image, "@") {
		// The appex never re-resolves tags: identity must be a
		// digest reference (REQ-store-digest-entry).
		return cfg, fmt.Errorf("image %q is not digest-form", cfg.Image)
	}
	return cfg, nil
}

// FileSystem is the appex-side entry point: Probe recognizes a
// store-root resource, Load materializes the volume from cached
// content and persists the projection report (REQ-proj-report).
type FileSystem struct{}

var _ fskit.FileSystem = FileSystem{}

func (FileSystem) Probe(res fskit.Resource) (fskit.ProbeResult, error) {
	u, ok := res.URL()
	if !ok {
		return fskit.ProbeResult{Match: fskit.MatchNotRecognized}, nil
	}
	storePath := u
	if parsed, err := url.Parse(u); err == nil && parsed.Scheme == "file" {
		storePath = parsed.Path
	}
	// Recognition is by the store's layout signature (store.md
	// REQ-store-adopt): a resource without the OCI layout marker is
	// not an ocifs store.
	if _, err := os.Stat(filepath.Join(storePath, "oci", "oci-layout")); err != nil {
		return fskit.ProbeResult{Match: fskit.MatchNotRecognized}, nil
	}
	return fskit.ProbeResult{
		Match:       fskit.MatchUsable,
		Name:        "ocifs",
		ContainerID: containerID(u),
	}, nil
}

func containerID(s string) fskit.UUID {
	return uuidFrom("ocifs-container:" + s)
}

// uuidFrom derives a stable 16-byte identifier from a name, stamped
// as an RFC 4122 name-derived UUID (version 4 bits, RFC variant).
func uuidFrom(name string) fskit.UUID {
	sum := sha256.Sum256([]byte(name))
	var u fskit.UUID
	copy(u[:], sum[:16])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// Load opens the store named by the resource under PullNever — the
// sandboxed server never touches the network — materializes the
// configured digest identity, builds the projection under the FSKit
// envelope, persists its report into the per-mount state, and
// returns the serving volume. The volume identity derives from the
// platform-selected manifest digest, stable across remounts.
func (FileSystem) Load(res fskit.Resource, opts fskit.TaskOptions) (fskit.Volume, fskit.VolumeIdentity, error) {
	u, ok := res.URL()
	if !ok {
		return nil, fskit.VolumeIdentity{}, fskit.EINVAL
	}
	cfg, err := ParseConfig(u, opts.Args)
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}
	vol, id, err := loadVolume(cfg)
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}
	return vol, id, nil
}

func (FileSystem) Unload(res fskit.Resource, opts fskit.TaskOptions) error { return nil }

// loadVolume is the portable heart of Load, shared with the linux
// tests.
func loadVolume(cfg Config) (*Volume, fskit.VolumeIdentity, error) {
	var platform *v1.Platform
	if cfg.Platform != "" {
		p, err := v1.ParsePlatform(cfg.Platform)
		if err != nil {
			return nil, fskit.VolumeIdentity{}, fmt.Errorf("platform %q: %w", cfg.Platform, err)
		}
		platform = p
	}

	s, err := store.NewStore(cfg.Store, anonKeychain{}, store.PullNever, v1.Platform{})
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}
	img, err := s.Image(context.Background(), cfg.Image, platform)
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}
	view, err := img.Unify()
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}
	proj, err := projection.New(view, cfg.ExtraDirs, Capabilities())
	if err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}

	if cfg.State == "" {
		// The report must have a per-mount home (REQ-proj-report);
		// silently self-registering one would leak state directories
		// nothing reclaims.
		return nil, fskit.VolumeIdentity{}, fmt.Errorf("mount options name no per-mount state directory (state=…)")
	}
	if err := proj.Report().WriteFile(filepath.Join(cfg.State, projection.ReportFileName)); err != nil {
		return nil, fskit.VolumeIdentity{}, err
	}

	child := img.Hash()
	return New(proj, s.BlobPath), fskit.VolumeIdentity{
		// Stable across remounts: the platform-selected manifest
		// digest is the image identity
		// (REQ-store-platform-serves-child).
		UUID: uuidFrom("ocifs-volume:" + child.String()),
		Name: "ocifs-" + child.Hex[:12],
	}, nil
}

// anonKeychain resolves everything anonymously: the appex serves
// cached content only and never authenticates.
type anonKeychain struct{}

func (anonKeychain) Resolve(authn.Resource) (authn.Authenticator, error) {
	return authn.Anonymous, nil
}
