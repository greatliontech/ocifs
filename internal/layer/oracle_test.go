//go:build linux

package layer

// The extraction oracle is an independent reference implementation
// of layer application: it applies layers to a real temporary
// directory with ordinary filesystem operations, letting the
// filesystem itself decide placement semantics (last-wins overwrite,
// directory merge, non-directory ancestors rejecting descendants,
// hardlinks binding to the inode existing at link time). Unify's
// output must present the same tree. The oracle only runs on
// generator-produced input: safe path alphabet, no escapes, no
// device nodes.

import (
	"archive/tar"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

// contentFor reproduces the test convention that a digest is the
// sha256 of the content string carried by the generator.
type oracleEntry struct {
	flag    byte
	content string // regular files (and hardlinks, via inode)
	link    string // symlink target
	mode    os.FileMode
}

// chainIsDirs reports whether every *existing* component of the
// slash path dir under root is a real directory (Lstat, so symlinks
// count as non-directories). This encodes the spec's one deliberate
// divergence from raw tar extraction: entries and markers whose
// parent chain passes through a non-directory are discarded, never
// resolved through it (REQ-unify-clean; raw extraction would follow
// a symlink component — the exact attack export must exclude).
func chainIsDirs(root, dir string) bool {
	if dir == "." || dir == "" {
		return true
	}
	cur := root
	for c := range strings.SplitSeq(dir, "/") {
		cur = filepath.Join(cur, c)
		fi, err := os.Lstat(cur)
		if err != nil {
			return true // rest of the chain doesn't exist yet
		}
		if !fi.IsDir() {
			return false
		}
	}
	return true
}

// mkParents creates missing parent components one by one, never
// following a non-directory (symlink included); it reports whether
// the full chain is directories. Prefixes created before an
// obstruction persist — physical extraction leaves them behind even
// when the entry itself then fails.
func mkParents(root, dir string) bool {
	if dir == "." || dir == "" {
		return true
	}
	cur := root
	for c := range strings.SplitSeq(dir, "/") {
		cur = filepath.Join(cur, c)
		fi, err := os.Lstat(cur)
		if err != nil {
			if os.Mkdir(cur, 0o755) != nil {
				return false
			}
			continue
		}
		if !fi.IsDir() {
			return false
		}
	}
	return true
}

// applyOracle applies layers to root per the spec's two passes,
// using real filesystem operations. contents maps digest hex to the
// bytes behind regular-file entries.
func applyOracle(root string, layers []Layer, contents map[string]string) {
	for _, l := range layers {
		// Pass 1: markers against lower state.
		for _, e := range l {
			if metaType(e.Header.Typeflag) {
				continue
			}
			name := cleanName(e.Header.Name)
			d, base := path.Split(name)
			d = strings.TrimSuffix(d, "/")
			if base == opaqueMarker {
				if !chainIsDirs(root, d) {
					continue
				}
				ents, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(d)))
				if err == nil {
					for _, de := range ents {
						os.RemoveAll(filepath.Join(root, filepath.FromSlash(d), de.Name()))
					}
				}
				continue
			}
			if !strings.HasPrefix(base, whiteoutPrefix) {
				continue
			}
			target := base[len(whiteoutPrefix):]
			if target == "" || target == "." || target == ".." || strings.HasPrefix(target, whiteoutPrefix) {
				continue
			}
			if !chainIsDirs(root, d) {
				continue
			}
			os.RemoveAll(filepath.Join(root, filepath.FromSlash(d), target))
		}
		// Pass 2: content entries in tar order.
		for _, e := range l {
			if metaType(e.Header.Typeflag) {
				continue
			}
			name := cleanName(e.Header.Name)
			base := path.Base(name)
			if strings.HasPrefix(base, whiteoutPrefix) {
				continue
			}
			if reservedComponent(name) {
				continue // unrepresentable path: inert, like the model
			}
			full := filepath.Join(root, filepath.FromSlash(name))
			mode := os.FileMode(e.Header.Mode & 0o777)
			if name == "." {
				os.Chmod(root, mode)
				continue
			}
			// Implied parents are created component by component and
			// persist even when the entry is then discarded or
			// omitted — matching both the model and raw extraction.
			if !mkParents(root, path.Dir(name)) {
				continue
			}
			flag := e.Header.Typeflag
			if flag == 0 {
				flag = tar.TypeReg
			}
			// Hardlinks capture the target's content and mode at
			// resolution, before the destination is replaced — the
			// destination may be an ancestor of the target, and the
			// inode outlives the path exactly like capture-first.
			var linkContent []byte
			var linkMode os.FileMode
			if flag == tar.TypeLink {
				tname := cleanName(e.Header.Linkname)
				if tname == name {
					continue // self-link: no-op
				}
				if !chainIsDirs(root, path.Dir(tname)) {
					continue
				}
				linkTarget := filepath.Join(root, filepath.FromSlash(tname))
				fi, err := os.Lstat(linkTarget)
				if err != nil || !fi.Mode().IsRegular() {
					continue
				}
				linkMode = fi.Mode().Perm()
				if linkContent, err = os.ReadFile(linkTarget); err != nil {
					continue
				}
			}
			switch flag {
			case tar.TypeDir:
				if fi, err := os.Lstat(full); err == nil && !fi.IsDir() {
					os.RemoveAll(full)
				}
				if err := os.MkdirAll(full, 0o755); err != nil {
					continue
				}
				os.Chmod(full, mode)
			case tar.TypeReg:
				os.RemoveAll(full)
				if err := os.WriteFile(full, []byte(contents[e.Digest.Hex]), mode); err != nil {
					continue
				}
				os.Chmod(full, mode)
			case tar.TypeSymlink:
				os.RemoveAll(full)
				os.Symlink(e.Header.Linkname, full)
			case tar.TypeFifo:
				os.RemoveAll(full)
				syscall.Mkfifo(full, uint32(mode))
			case tar.TypeLink:
				os.RemoveAll(full)
				os.WriteFile(full, linkContent, linkMode)
				os.Chmod(full, linkMode)
			}
		}
	}
}

// collectOracle walks the oracle tree into path → entry.
func collectOracle(t reporter, root string) map[string]oracleEntry {
	out := map[string]oracleEntry{}
	err := filepath.Walk(root, func(p string, fi fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		oe := oracleEntry{mode: fi.Mode().Perm()}
		switch {
		case fi.IsDir():
			oe.flag = tar.TypeDir
		case fi.Mode()&os.ModeSymlink != 0:
			oe.flag = tar.TypeSymlink
			oe.link, _ = os.Readlink(p)
		case fi.Mode()&os.ModeNamedPipe != 0:
			oe.flag = tar.TypeFifo
		case fi.Mode().IsRegular():
			oe.flag = tar.TypeReg
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			oe.content = string(b)
		default:
			t.Fatalf("oracle produced unexpected mode at %s: %v", rel, fi.Mode())
		}
		out[rel] = oe
		return nil
	})
	if err != nil {
		t.Fatalf("walk oracle: %v", err)
	}
	return out
}

// presentView expands a unified view the way a presentation would:
// hardlinks become regular entries with their captured content. The
// view is complete (implied directories included), so nothing else
// is synthesized.
func presentView(v *View, contents map[string]string) map[string]oracleEntry {
	out := map[string]oracleEntry{}
	for _, e := range v.Entries() {
		name := e.Header.Name
		if name == "." {
			continue
		}
		oe := oracleEntry{mode: os.FileMode(e.Header.Mode & 0o777)}
		switch e.Header.Typeflag {
		case tar.TypeDir:
			oe.flag = tar.TypeDir
		case tar.TypeSymlink:
			oe.flag = tar.TypeSymlink
			oe.link = e.Header.Linkname
		case tar.TypeFifo:
			oe.flag = tar.TypeFifo
		case tar.TypeReg, tar.TypeLink:
			oe.flag = tar.TypeReg
			oe.content = contents[e.Digest.Hex]
		}
		out[name] = oe
	}
	return out
}

func compareOracle(t reporter, got, want map[string]oracleEntry) {
	var paths []string
	for p := range want {
		paths = append(paths, p)
	}
	for p := range got {
		if _, ok := want[p]; !ok {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		g, gok := got[p]
		w, wok := want[p]
		switch {
		case !gok:
			t.Errorf("path %q: in oracle, missing from view presentation", p)
		case !wok:
			t.Errorf("path %q: in view presentation, missing from oracle", p)
		case g != w:
			t.Errorf("path %q: view %+v, oracle %+v", p, g, w)
		}
	}
}

// scratchSeq numbers oracle trees within one test process so their
// paths are deterministic: randomized temp paths (OS temp dir or
// MkdirTemp suffixes) poison mutation-campaign observation sealing
// as machine-local runtime inputs, while a stable module-relative
// sequence observes identically on every run.
var scratchSeq atomic.Uint64

// scratchCase creates a per-case oracle tree under the repository
// root's git-ignored scratch directory — outside the package
// directory, whose listing is part of the mutation campaign's
// built-in observation bracket and must not churn during a run.
func scratchCase(t reporter) (string, func()) {
	d := filepath.Join("..", "..", ".scratch", "layer", strconv.FormatUint(scratchSeq.Add(1), 10))
	if err := os.RemoveAll(d); err != nil {
		t.Fatalf("scratch reset: %v", err)
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatalf("scratch case: %v", err)
	}
	return d, func() { os.RemoveAll(d) }
}

// registerContent stores content bytes under their digest so both
// the oracle and comparisons can materialize them.
func registerContent(contents map[string]string, content string) {
	contents[digestOf(content).Hex] = content
}

// runOracleCase unifies and extracts the same stack and compares.
func runOracleCase(t reporter, root string, layers []Layer, contents map[string]string) {
	v, err := Unify(layers)
	if err != nil {
		t.Fatalf("Unify: %v", err)
	}
	checkInvariants(t, v)
	applyOracle(root, layers, contents)
	compareOracle(t, presentView(v, contents), collectOracle(t, root))
}

func TestOracleHandPicked(t *testing.T) {
	contents := map[string]string{}
	mk := func(name, c string) Entry {
		registerContent(contents, c)
		return file(name, c)
	}
	cases := map[string][]Layer{
		"whiteout orders": {
			{dir("app"), mk("app/secret", "s")},
			{wh("app"), dir("app"), mk("app/new", "n")},
		},
		"marker after content": {
			{dir("app"), mk("app/secret", "s")},
			{dir("app"), mk("app/new", "n"), wh("app")},
		},
		"hardlink then replace": {
			{mk("bin/busybox", "v1"), hardlink("bin/sh", "bin/busybox")},
			{mk("bin/busybox", "v2")},
		},
		"file over dir": {
			{dir("a"), mk("a/f", "x")},
			{mk("a", "flat")},
		},
		"dir over file": {
			{mk("a", "flat")},
			{dir("a"), mk("a/f", "x")},
		},
		"opaque": {
			{dir("d"), mk("d/old", "1"), dir("d/sub"), mk("d/sub/x", "2")},
			{opq("d"), mk("d/new", "3")},
		},
		"same layer shadow": {
			{mk("a", "v1"), mk("a", "v2"), symlink("s", "a"), mk("s/under", "gone")},
		},
	}
	for name, layers := range cases {
		t.Run(name, func(t *testing.T) {
			root, cleanup := scratchCase(t)
			defer cleanup()
			runOracleCase(t, root, layers, contents)
		})
	}
}
