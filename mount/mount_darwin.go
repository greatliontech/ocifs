//go:build darwin

package mount

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

const fskitShortName = "OcifsFS"

func platformResolveUpper(im *ImageMount, _ *store.Image) error {
	if im.upperDir != "" || im.upperName != "" {
		return fmt.Errorf("writable mounts are not served here: darwin mounts are appex-mediated; the FSKit write arm is an explicit non-goal of this stage")
	}
	return nil
}

func platformMount(im *ImageMount, imgRef string, img *store.Image, _ *layer.View, _, stateDir, mountPoint, _ string) (mountServer, error) {
	ref, err := name.ParseReference(imgRef)
	if err != nil {
		return nil, err
	}
	digestRef := ref.Context().Name() + "@" + img.Hash().String()

	opts := []string{
		"image=" + url.QueryEscape(digestRef),
		"state=" + url.QueryEscape(stateDir),
	}
	for _, d := range im.extraDirs {
		opts = append(opts, "extra=" + url.QueryEscape(d))
	}
	cmd := exec.Command("mount", "-F", "-t", fskitShortName,
		"-o", strings.Join(opts, ","), im.workDir, mountPoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("appex-mediated mount failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	canonical := mountPoint
	if resolved, err := filepath.EvalSymlinks(mountPoint); err == nil {
		canonical = resolved
	}
	return &darwinMount{mountPoint: mountPoint, canonical: canonical, done: make(chan struct{})}, nil
}

type darwinMount struct {
	mountPoint string
	canonical  string
	done       chan struct{}
	stop       sync.Once
}

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
		if isMountpoint(m.canonical) {
			return fmt.Errorf("umount %s: %w: %s", m.mountPoint, err, strings.TrimSpace(string(out)))
		}
	}
	m.stop.Do(func() { close(m.done) })
	return nil
}
