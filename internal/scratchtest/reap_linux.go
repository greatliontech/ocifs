package scratchtest

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// reapMounts lazily detaches every mount whose mountpoint lies under
// (or is) dir, then verifies none remain. A test process killed
// mid-mount leaves kernel FUSE mounts whose server is gone; any path
// operation on such a mountpoint blocks in the kernel until the
// connection dies (a leaked fd holder can keep it alive
// indefinitely), so the sweep consults only the mount table — it
// must never stat the tree it is about to clear.
func reapMounts(dir string) error {
	base, err := canonicalBase(dir)
	if err != nil {
		return err
	}
	targets, err := mountTable(base)
	if err != nil {
		return err
	}
	// Parents first — the one addressable order for stacks: a mount
	// nested inside a dead mount is unreachable by path (resolution
	// dies at the dead ancestor with ENOTCONN), while the
	// shallowest target's path traverses only plain directories,
	// and its lazy detach takes the whole subtree with it. The
	// tolerance below then treats detached-with-parent children as
	// success; the post-loop verification is the contract.
	sort.Slice(targets, func(i, j int) bool { return len(targets[i]) < len(targets[j]) })
	for _, mp := range targets {
		derr := detach(mp)
		if derr == nil {
			continue
		}
		// A child already taken out by its parent's recursive lazy
		// detach is success; only a mount still in the table is a
		// failure.
		still, err := mountTable(base)
		if err != nil {
			return err
		}
		if slices.Contains(still, mp) {
			return fmt.Errorf("reap stale mount %s: %w", mp, derr)
		}
	}
	if left, err := mountTable(base); err != nil {
		return err
	} else if len(left) != 0 {
		return fmt.Errorf("mounts survived reap under %s: %v", dir, left)
	}
	return nil
}

// detach lazily unmounts mp: umount2 directly (privileged or own
// namespace), else the setuid fusermount helper — fusermount3 first,
// fusermount for fuse2-only hosts.
func detach(mp string) error {
	uerr := unix.Unmount(mp, unix.MNT_DETACH)
	if uerr == nil {
		return nil
	}
	var lastOut []byte
	for _, helper := range []string{"fusermount3", "fusermount"} {
		out, err := exec.Command(helper, "-uz", mp).CombinedOutput()
		if err == nil {
			return nil
		}
		lastOut = out
	}
	return fmt.Errorf("umount2: %v; fusermount: %s", uerr, strings.TrimSpace(string(lastOut)))
}

// canonicalBase resolves dir to the kernel-canonical absolute path
// the mount table reports. Symlinks are resolved on the ancestors
// only — resolving dir itself would stat inside the tree the reap
// exists to avoid touching.
func canonicalBase(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	parent, base := filepath.Split(abs)
	rp, err := filepath.EvalSymlinks(filepath.Clean(parent))
	if err != nil {
		// A missing parent means no mount can be listed under the
		// base (a dir holding a mount is not removable) — the
		// unresolved path is then safely equivalent. Any other
		// resolution failure would silently forgo canonicalization
		// and could miss a stale mount; surface it instead.
		if os.IsNotExist(err) {
			return abs, nil
		}
		return "", err
	}
	return filepath.Join(rp, base), nil
}

// mountTable returns the mountpoints currently at or under base.
func mountTable(base string) ([]string, error) {
	f, err := os.Open("/proc/self/mounts")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var targets []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		mp := unescapeMountPath(fields[1])
		if mp == base || strings.HasPrefix(mp, base+string(filepath.Separator)) {
			targets = append(targets, mp)
		}
	}
	return targets, sc.Err()
}

// unescapeMountPath decodes the octal escapes (\040 space, \011 tab,
// \012 newline, \134 backslash) /proc/self/mounts applies to
// mountpoint paths.
func unescapeMountPath(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
