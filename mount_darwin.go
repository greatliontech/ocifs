//go:build darwin

package ocifs

import (
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/store"
)

// fskitShortName is the FSShortName the appex Info.plist declares —
// the `mount -F -t` type.
const fskitShortName = "OcifsFS"

// platformMount orchestrates the appex-mediated darwin mount
// (api.md REQ-api-mount-darwin): the library never serves in
// process. The image was acquired into the store by this process;
// the platform mount command carries the appex's entire declarative
// configuration (REQ-proj-server) — the store root as the resource,
// and the digest-form image identity, per-mount state directory,
// and extra directories as options. The signed, enabled ocifs
// extension is spawned by the platform; the projection report is
// written by the appex into the shared state directory. Tier-2
// validation of this path runs on a real darwin machine with a
// signed extension — a user-side act.
func platformResolveUpper(o *OCIFS, im *ImageMount, img *store.Image) error {
	if im.upperDir != "" || im.upperName != "" {
		return fmt.Errorf("writable mounts are not served here: darwin mounts are appex-mediated; the FSKit write arm is an explicit non-goal of this stage")
	}
	return nil
}

func platformMount(o *OCIFS, imgRef string, img *store.Image, view *layer.View, stateDir, mountPoint, upperRoot string) (mountServer, error) {
	ref, err := name.ParseReference(imgRef)
	if err != nil {
		return nil, err
	}
	// The platform-selected manifest digest is the identity the
	// appex materializes — platform selection already happened, so
	// no platform option travels (a direct-manifest digest entry is
	// unconstrained by the default platform).
	digestRef := ref.Context().Name() + "@" + img.Hash().String()

	// Option values are percent-encoded so paths carrying spaces or
	// commas — the app-group container path always carries a space —
	// survive the -o syntax (fskitfs.ParseConfig decodes).
	opts := []string{
		"image=" + url.QueryEscape(digestRef),
		"state=" + url.QueryEscape(stateDir),
	}
	for _, d := range o.extraDirs {
		opts = append(opts, "extra="+url.QueryEscape(d))
	}
	cmd := exec.Command("mount", "-F", "-t", fskitShortName,
		"-o", strings.Join(opts, ","), o.workDir, mountPoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		// An inaccessible store (outside the app-group container the
		// sandboxed extension can open) surfaces here as a failed
		// platform mount (REQ-api-mount-darwin).
		return nil, fmt.Errorf("appex-mediated mount failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	// The kernel reports mountpoints symlink-resolved (Mntonname);
	// canonicalize once so the poller compares like with like — /tmp
	// and $TMPDIR traverse symlinks on darwin.
	canonical := mountPoint
	if resolved, err := filepath.EvalSymlinks(mountPoint); err == nil {
		canonical = resolved
	}
	return &darwinMount{mountPoint: mountPoint, canonical: canonical, done: make(chan struct{})}, nil
}

// darwinMount tracks a platform-managed mount: the serving process
// is the appex, so unmount is the platform's verb — and it can fire
// externally (umount, Finder eject) without this process
// (REQ-proj-server).
type darwinMount struct {
	mountPoint string
	// canonical is the symlink-resolved mountpoint the kernel's
	// mount table reports.
	canonical string
	done      chan struct{}
	stop      sync.Once
}

// Wait returns when the mount is gone — through this process's
// Unmount or an external one, observed by polling the mount table.
func (m *darwinMount) Wait() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-t.C:
			if !isMountpoint(m.canonical) {
				m.stop.Do(func() { close(m.done) })
				return
			}
		}
	}
}

// isMountpoint reports whether path is currently a mount root.
func isMountpoint(path string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false
	}
	mntOn := unix.ByteSliceToString(st.Mntonname[:])
	return mntOn == path
}

func (m *darwinMount) Unmount() error {
	if out, err := exec.Command("umount", m.mountPoint).CombinedOutput(); err != nil {
		// Already unmounted externally: the desired state holds.
		if isMountpoint(m.canonical) {
			return fmt.Errorf("umount %s: %w: %s", m.mountPoint, err, strings.TrimSpace(string(out)))
		}
	}
	m.stop.Do(func() { close(m.done) })
	return nil
}
