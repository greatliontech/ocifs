//go:build linux || darwin

package upper

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Walk reads the dialect tree at root into its abstract state
// (REQ-writable-dialect). Reserved temporaries are ignored — they
// are inert garbage, not state — and never removed here: sweeping is
// the serving provider's act (Sweep), and a reader (commit) must not
// mutate an upper it may not own. A dialect impossibility — an
// unknown machinery xattr, a malformed stand-in, a degenerate
// marker — is damage and fails loudly: the provider authors every
// record, so no reachable state contains one (REQ-writable-crash).
func Walk(root string) (*State, error) {
	st := &State{
		Entries:   map[string]Entry{},
		Whiteouts: map[string]bool{},
		Opaque:    map[string]bool{},
	}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		base := path.Base(rel)
		dir := path.Dir(rel)
		if dir == "." {
			dir = ""
		}

		switch {
		case base == OpaqueMarker:
			if dir == "" {
				return fmt.Errorf("upper: opaque marker at the root has no directory to mark")
			}
			st.Opaque[dir] = true
			return nil
		case strings.HasPrefix(base, TempPrefix):
			// Inert orphan; skip its subtree if a directory somehow
			// bears the name (temps are files, but inertness is by
			// name).
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		case strings.HasPrefix(base, WhiteoutPrefix):
			target := base[len(WhiteoutPrefix):]
			if target == "" || target == "." || target == ".." || strings.HasPrefix(target, WhiteoutPrefix) {
				return fmt.Errorf("upper: degenerate whiteout marker %q — the provider never writes one", rel)
			}
			if d.IsDir() {
				return fmt.Errorf("upper: whiteout marker %q is a directory — markers are files", rel)
			}
			st.Whiteouts[path.Join(dir, target)] = true
			return nil
		}

		e, err := readEntry(root, rel, p)
		if err != nil {
			return err
		}
		st.Entries[rel] = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	r, err := StatRoot(root)
	if err != nil {
		return nil, err
	}
	st.Root = r
	return st, nil
}

// StatRoot reads the upper root's record (REQ-writable-dialect): nil
// when no record exists — the root then presents the base root.
// Machinery on the root without the record is damage — the provider
// stamps the record before any other root machinery.
func StatRoot(root string) (*Entry, error) {
	names, err := listXattrs(root)
	if err != nil {
		return nil, err
	}
	machinery, recorded := false, false
	for _, name := range names {
		if strings.HasPrefix(name, XattrNS) {
			machinery = true
			if name == XattrOwner {
				recorded = true
			}
		}
	}
	if !machinery {
		return nil, nil
	}
	if !recorded {
		return nil, fmt.Errorf("upper: root carries machinery without a root record — the provider never writes one")
	}
	e, err := readEntry(root, ".", root)
	if err != nil {
		return nil, err
	}
	if e.Kind != KindDir {
		return nil, fmt.Errorf("upper: root is not a directory")
	}
	return &e, nil
}

// Stat reads one dialect node's presented entry — the walker's
// per-path arm, for a provider maintaining its index incrementally
// (the index stays a cache rebuildable by Walk,
// REQ-proj-upper-truth). ok=false when no node exists at rel.
func Stat(root, rel string) (Entry, bool, error) {
	hostPath := filepath.Join(root, filepath.FromSlash(rel))
	if _, err := os.Lstat(hostPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}
	e, err := readEntry(root, rel, hostPath)
	if err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

// readEntry resolves one non-marker node into its presented truth.
func readEntry(root, rel, hostPath string) (Entry, error) {
	var lst unix.Stat_t
	if err := unix.Lstat(hostPath, &lst); err != nil {
		return Entry{}, &os.PathError{Op: "lstat", Path: hostPath, Err: err}
	}
	e := Entry{
		Path:     rel,
		Mode:     uint32(lst.Mode) & 0o7777,
		UID:      int(lst.Uid),
		GID:      int(lst.Gid),
		Size:     lst.Size,
		ModTime:  time.Unix(lst.Mtim.Sec, lst.Mtim.Nsec),
		Nlink:    uint64(lst.Nlink),
		Ino:      lst.Ino,
		HostPath: hostPath,
	}

	switch uint32(lst.Mode) & unix.S_IFMT {
	case unix.S_IFDIR:
		e.Kind = KindDir
	case unix.S_IFREG:
		e.Kind = KindFile
	case unix.S_IFLNK:
		target, err := os.Readlink(hostPath)
		if err != nil {
			return Entry{}, err
		}
		e.Kind, e.Target = KindSymlink, target
		// The kernel refuses user xattrs on symlinks; nothing more
		// to resolve (an overridden symlink is a stand-in, which is
		// a regular file). A privileged writer could set security.*
		// xattrs on these kinds natively — outside the unprivileged
		// model this dialect serves; they would not present.
		return e, nil
	case unix.S_IFIFO:
		e.Kind = KindFifo
		return e, nil
	case unix.S_IFSOCK:
		e.Kind = KindSocket
		return e, nil
	default:
		return Entry{}, fmt.Errorf("upper: %q has host node type %#o the dialect never stores", rel, uint32(lst.Mode)&unix.S_IFMT)
	}

	names, err := listXattrs(hostPath)
	if err != nil {
		return Entry{}, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, XattrNS) {
			val, err := getXattr(hostPath, name)
			if err != nil {
				return Entry{}, err
			}
			if e.Xattrs == nil {
				e.Xattrs = map[string]string{}
			}
			e.Xattrs[name] = val
			continue
		}
		val, err := getXattr(hostPath, name)
		if err != nil {
			return Entry{}, err
		}
		switch {
		case name == XattrKind:
			if e.Kind != KindFile {
				return Entry{}, fmt.Errorf("upper: stand-in record on non-regular %q", rel)
			}
			kind, err := standInKind(val)
			if err != nil {
				return Entry{}, fmt.Errorf("upper: %q: %w", rel, err)
			}
			e.Kind, e.StandIn = kind, true
		case name == XattrTarget:
			e.Target = val
		case name == XattrRdev:
			maj, min, ok := strings.Cut(val, ":")
			if !ok {
				return Entry{}, fmt.Errorf("upper: %q: malformed rdev record %q", rel, val)
			}
			ma, err1 := strconv.ParseUint(maj, 10, 32)
			mi, err2 := strconv.ParseUint(min, 10, 32)
			if err1 != nil || err2 != nil {
				return Entry{}, fmt.Errorf("upper: %q: malformed rdev record %q", rel, val)
			}
			e.Rdev = Rdev{Major: uint32(ma), Minor: uint32(mi)}
		case name == XattrOwner:
			u, g, ok := strings.Cut(val, ":")
			uid, err1 := strconv.Atoi(u)
			gid, err2 := strconv.Atoi(g)
			if !ok || err1 != nil || err2 != nil {
				return Entry{}, fmt.Errorf("upper: %q: malformed owner record %q", rel, val)
			}
			e.UID, e.GID = uid, gid
		case name == XattrMode:
			mv, err := strconv.ParseUint(val, 8, 32)
			if err != nil || mv > 0o7777 {
				return Entry{}, fmt.Errorf("upper: %q: malformed mode record %q", rel, val)
			}
			// The record is the presented mode; the host bits are
			// machinery (REQ-writable-fidelity's mode override).
			e.Mode = uint32(mv)
		case strings.HasPrefix(name, XattrEscapePrefix):
			real := name[len(XattrEscapePrefix):]
			if real == "" {
				return Entry{}, fmt.Errorf("upper: %q: escape record with empty name", rel)
			}
			if e.Xattrs == nil {
				e.Xattrs = map[string]string{}
			}
			e.Xattrs[real] = val
		default:
			return Entry{}, fmt.Errorf("upper: %q: unknown machinery xattr %q — the provider never writes one", rel, name)
		}
	}
	if e.StandIn {
		if lst.Size != 0 {
			return Entry{}, fmt.Errorf("upper: stand-in %q carries content — stand-ins are empty records", rel)
		}
		switch e.Kind {
		case KindSymlink:
			if e.Target == "" {
				return Entry{}, fmt.Errorf("upper: symlink stand-in %q records no target", rel)
			}
			e.Size = int64(len(e.Target))
		case KindCharDev, KindBlockDev:
			// Rdev may legitimately be 0:0; nothing to validate.
			e.Size = 0
		default:
			e.Size = 0
		}
	} else if e.Target != "" || e.Rdev != (Rdev{}) {
		// A native node may not carry stand-in fields — files and
		// directories alike (the other kinds return before the xattr
		// loop).
		return Entry{}, fmt.Errorf("upper: %q carries stand-in fields without a kind record", rel)
	}
	return e, nil
}

func standInKind(v string) (Kind, error) {
	switch v {
	case "char":
		return KindCharDev, nil
	case "block":
		return KindBlockDev, nil
	case "fifo":
		return KindFifo, nil
	case "socket":
		return KindSocket, nil
	case "symlink":
		return KindSymlink, nil
	}
	return 0, fmt.Errorf("unknown stand-in kind %q", v)
}

// listXattrs returns the node's xattr names, tolerating filesystems
// without xattr support (no names, not an error).
func listXattrs(p string) ([]string, error) {
	buf := make([]byte, 1024)
	for {
		n, err := unix.Llistxattr(p, buf)
		if errors.Is(err, unix.ERANGE) {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			return nil, nil
		}
		if err != nil {
			return nil, &os.PathError{Op: "llistxattr", Path: p, Err: err}
		}
		var names []string
		for _, s := range strings.Split(string(buf[:n]), "\x00") {
			if s != "" {
				names = append(names, s)
			}
		}
		return names, nil
	}
}

func getXattr(p, name string) (string, error) {
	buf := make([]byte, 256)
	for {
		n, err := unix.Lgetxattr(p, name, buf)
		if errors.Is(err, unix.ERANGE) {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if err != nil {
			return "", &os.PathError{Op: "lgetxattr", Path: p + ":" + name, Err: err}
		}
		return string(buf[:n]), nil
	}
}
