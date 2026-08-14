//go:build linux

package export

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// The materializer is pinned against the unified view directly:
// REQ-export-fidelity's contract is "every unified entry at its
// path", and the view's own equivalence to sequential extraction is
// already pinned by the layer package's oracle property.

var baseTime = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

func digestOf(content []byte) v1.Hash {
	sum := sha256.Sum256(content)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// blobDir writes every registered content into a flat fake CAS and
// returns the resolver the materializer uses.
func blobDir(t testing.TB, dir string, contents map[string][]byte) func(v1.Hash) string {
	t.Helper()
	casDir := filepath.Join(dir, "cas")
	if err := os.MkdirAll(casDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for hexKey, data := range contents {
		if err := os.WriteFile(filepath.Join(casDir, hexKey), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return func(h v1.Hash) string { return filepath.Join(casDir, h.Hex) }
}

func entryFile(name string, content []byte, mode int64, mt time.Time) layer.Entry {
	return layer.Entry{
		Header: tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: mode, Size: int64(len(content)), ModTime: mt, Uid: 12345, Gid: 12345},
		Digest: digestOf(content),
	}
}

func entryDir(name string, mode int64, mt time.Time) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: mode, ModTime: mt}}
}

func entrySymlink(name, target string, mt time.Time) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777, ModTime: mt}}
}

func entryHardlink(name, target string) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeLink, Linkname: target}}
}

func entryFifo(name string, mode int64, mt time.Time) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeFifo, Mode: mode, ModTime: mt}}
}

func entryDevice(name string, flag byte) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: flag, Mode: 0o644, ModTime: baseTime, Devmajor: 1, Devminor: 3}}
}

// genStack draws a random layer stack over a tiny path alphabet —
// files (setuid modes included), directories, symlinks (relative and
// absolute, dangling included), hardlinks, FIFOs, devices, whiteouts
// and opaques, root-attribute entries — registering regular-file
// content in contents.
func genStack(rt *rapid.T, contents map[string][]byte) []layer.Layer {
	comp := rapid.SampledFrom([]string{"a", "b", "c", "d"})
	genPath := func(label string) string {
		n := rapid.IntRange(1, 3).Draw(rt, label+"-depth")
		parts := make([]string, n)
		for i := range parts {
			parts[i] = comp.Draw(rt, label+"-comp")
		}
		return strings.Join(parts, "/")
	}
	mt := func(label string) time.Time {
		return baseTime.Add(time.Duration(rapid.IntRange(0, 5).Draw(rt, label+"-mt"))*time.Minute +
			time.Duration(rapid.IntRange(0, 999_999_999).Draw(rt, label+"-ns")))
	}
	var linkNames []string
	nLayers := rapid.IntRange(1, 3).Draw(rt, "layers")
	stack := make([]layer.Layer, nLayers)
	for li := range stack {
		nEntries := rapid.IntRange(0, 8).Draw(rt, "entries")
		var l layer.Layer
		for i := 0; i < nEntries; i++ {
			switch rapid.IntRange(0, 9).Draw(rt, "kind") {
			case 0, 1, 2:
				content := []byte(rapid.StringOfN(rapid.RuneFrom([]rune("xyz")), 0, 3, -1).Draw(rt, "content"))
				contents[digestOf(content).Hex] = content
				mode := rapid.SampledFrom([]int64{0o600, 0o644, 0o755, 0o4755, 0o2755, 0o1777}).Draw(rt, "fmode")
				l = append(l, entryFile(genPath("f"), content, mode, mt("f")))
			case 3, 4:
				l = append(l, entryDir(genPath("d"), rapid.SampledFrom([]int64{0o700, 0o755, 0o750, 0o2755, 0o1777}).Draw(rt, "dmode"), mt("d")))
			case 5:
				target := genPath("st")
				if rapid.Bool().Draw(rt, "abs") {
					target = "/" + target
				}
				l = append(l, entrySymlink(genPath("s"), target, mt("s")))
			case 6:
				target := genPath("ht")
				// Chains: sometimes target an earlier link.
				if len(linkNames) > 0 && rapid.Bool().Draw(rt, "chain") {
					target = rapid.SampledFrom(linkNames).Draw(rt, "chaintarget")
				}
				name := genPath("h")
				linkNames = append(linkNames, name)
				l = append(l, entryHardlink(name, target))
			case 7:
				l = append(l, entryFifo(genPath("p"), rapid.SampledFrom([]int64{0o644, 0o600, 0o4644}).Draw(rt, "pmode"), mt("p")))
			case 8:
				flag := byte(tar.TypeChar)
				if rapid.Bool().Draw(rt, "blk") {
					flag = tar.TypeBlock
				}
				l = append(l, entryDevice(genPath("dev"), flag))
			case 9:
				l = append(l, layer.Entry{Header: tar.Header{
					Name:     ".wh." + comp.Draw(rt, "whtarget"),
					Typeflag: tar.TypeReg,
				}})
			}
		}
		stack[li] = l
	}
	return stack
}

type exported struct {
	flag    byte
	content string
	link    string
	mode    fs.FileMode // permissions plus setuid/setgid/sticky
	mtime   time.Time
}

func collectExport(t testing.TB, root string) map[string]exported {
	t.Helper()
	out := map[string]exported{}
	err := filepath.Walk(root, func(p string, fi fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		e := exported{
			mode:  fi.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky),
			mtime: fi.ModTime(),
		}
		switch {
		case fi.IsDir():
			e.flag = tar.TypeDir
		case fi.Mode()&fs.ModeSymlink != 0:
			e.flag = tar.TypeSymlink
			e.link, _ = os.Readlink(p)
		case fi.Mode()&fs.ModeNamedPipe != 0:
			e.flag = tar.TypeFifo
		case fi.Mode().IsRegular():
			e.flag = tar.TypeReg
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			e.content = string(b)
		default:
			t.Fatalf("export holds unexpected mode at %s: %v", rel, fi.Mode())
		}
		out[rel] = e
		return nil
	})
	if err != nil {
		t.Fatalf("walk export: %v", err)
	}
	return out
}

// wantFromView is the expectation REQ-export-fidelity states: every
// view entry at its path — hardlinks as regular files with their
// captured content, devices omitted.
func wantFromView(v *layer.View, contents map[string][]byte) map[string]exported {
	out := map[string]exported{}
	for _, e := range v.Entries() {
		name := e.Header.Name
		if name == "." {
			continue
		}
		fm := e.Header.FileInfo().Mode()
		w := exported{
			mode:  fm & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky),
			mtime: e.Header.ModTime,
		}
		switch e.Header.Typeflag {
		case tar.TypeDir:
			w.flag = tar.TypeDir
		case tar.TypeSymlink:
			w.flag = tar.TypeSymlink
			w.link = e.Header.Linkname
			w.mode = fs.ModePerm & 0o777
		case tar.TypeFifo:
			w.flag = tar.TypeFifo
		case tar.TypeReg, tar.TypeLink:
			w.flag = tar.TypeReg
			w.content = string(contents[e.Digest.Hex])
		case tar.TypeChar, tar.TypeBlock:
			continue // omitted by contract
		}
		out[name] = w
	}
	return out
}

// TestPropertyExportMatchesView pins REQ-export-fidelity (and, by
// the layer oracle's transitivity, extraction equivalence) and
// REQ-export-immutable as a for-all over generated stacks: the exported tree holds exactly the
// view's entries — types, content, symlink targets verbatim,
// permission bits including setuid/setgid/sticky, and recorded
// modification times — with device nodes omitted and nothing else
// present. A canary sibling pins that materialization never writes
// outside its root (REQ-export-contained's observable half).
func TestPropertyExportMatchesView(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string][]byte{}
		stack := genStack(rt, contents)
		view, err := layer.Unify(stack)
		if err != nil {
			rt.Fatalf("Unify: %v", err)
		}

		dir := scratchtest.Dir(t, "export")
		blobPath := blobDir(t, dir, contents)
		canary := filepath.Join(dir, "canary")
		if err := os.MkdirAll(canary, 0o755); err != nil {
			rt.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(canary, "sentinel"), []byte("untouched"), 0o644); err != nil {
			rt.Fatal(err)
		}

		casBefore := collectExport(t, filepath.Join(dir, "cas"))

		rootDir := filepath.Join(dir, "root")
		if err := os.Mkdir(rootDir, 0o755); err != nil {
			rt.Fatal(err)
		}
		root, err := os.OpenRoot(rootDir)
		if err != nil {
			rt.Fatal(err)
		}
		merr := Materialize(root, view, blobPath)
		root.Close()
		if merr != nil {
			rt.Fatalf("Materialize: %v", merr)
		}

		// REQ-export-immutable, for-all: the CAS is byte-, mode-, and
		// population-identical around every export.
		casAfter := collectExport(t, filepath.Join(dir, "cas"))
		if len(casBefore) != len(casAfter) {
			rt.Fatalf("export changed CAS population: %d -> %d", len(casBefore), len(casAfter))
		}
		for p, b := range casBefore {
			if a, ok := casAfter[p]; !ok || a != b {
				rt.Fatalf("export mutated CAS entry %q: %+v -> %+v", p, b, casAfter[p])
			}
		}

		got := collectExport(t, rootDir)
		want := wantFromView(view, contents)
		for p, w := range want {
			g, ok := got[p]
			if !ok {
				rt.Fatalf("path %q: in view, missing from export", p)
			}
			if g.flag != w.flag || g.content != w.content || g.link != w.link || g.mode != w.mode {
				rt.Fatalf("path %q: export %+v, view %+v", p, g, w)
			}
			if !w.mtime.IsZero() && !g.mtime.Equal(w.mtime) {
				rt.Fatalf("path %q: export mtime %v, view %v", p, g.mtime, w.mtime)
			}
		}
		for p := range got {
			if _, ok := want[p]; !ok {
				rt.Fatalf("path %q: in export, not in view", p)
			}
		}

		if b, err := os.ReadFile(filepath.Join(canary, "sentinel")); err != nil || string(b) != "untouched" {
			rt.Fatalf("materialization reached outside its root: %v %q", err, b)
		}
	})
}

// TestRootEntryAttributesApply pins the `.` entry arm: a view
// carrying the root directory's own attributes applies them to the
// export root itself.
func TestRootEntryAttributesApply(t *testing.T) {
	stack := []layer.Layer{{
		entryDir(".", 0o700, baseTime),
		entryDir("sub", 0o755, baseTime.Add(time.Minute)),
	}}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Materialize(root, view, blobDir(t, dir, nil)); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Fatalf("root mode = %v, want 0700", fi.Mode().Perm())
	}
	if !fi.ModTime().Equal(baseTime) {
		t.Fatalf("root mtime = %v, want %v", fi.ModTime(), baseTime)
	}
}

// TestHardlinkStaleCaptureCopies pins REQ-export-copy's stale arm: a
// link whose captured content the target no longer holds
// materializes as an independent copy, while a fresh capture links
// to the exported target.
func TestHardlinkStaleCaptureCopies(t *testing.T) {
	contents := map[string][]byte{}
	reg := func(c string) []byte {
		b := []byte(c)
		contents[digestOf(b).Hex] = b
		return b
	}
	v1c, v2c := reg("v1"), reg("v2")
	stack := []layer.Layer{
		{entryFile("busybox", v1c, 0o755, baseTime), entryHardlink("sh", "busybox"), entryFile("cp", v2c, 0o755, baseTime), entryHardlink("cpln", "cp")},
		{entryFile("busybox", v2c, 0o755, baseTime)},
	}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Materialize(root, view, blobDir(t, dir, contents)); err != nil {
		t.Fatal(err)
	}

	// sh captured v1; busybox now holds v2 → independent copy.
	sh, _ := os.Stat(filepath.Join(rootDir, "sh"))
	bb, _ := os.Stat(filepath.Join(rootDir, "busybox"))
	if os.SameFile(sh, bb) {
		t.Fatal("stale-capture link shares the replaced target's inode")
	}
	if b, _ := os.ReadFile(filepath.Join(rootDir, "sh")); string(b) != "v1" {
		t.Fatalf("stale-capture link content = %q, want the captured v1", b)
	}
	// cpln captured cp's current content → a real link.
	cpln, _ := os.Stat(filepath.Join(rootDir, "cpln"))
	cp, _ := os.Stat(filepath.Join(rootDir, "cp"))
	if !os.SameFile(cpln, cp) {
		t.Fatal("fresh-capture link is not a hardlink to its exported target")
	}
}

// TestHardlinkSameBytesNewAttrsCopies is the named anchor for the
// seed-found corner: a target replaced by a same-content file with
// different attributes is a new inode — the link keeps its captured
// mode through an independent copy, never the replacement's.
func TestHardlinkSameBytesNewAttrsCopies(t *testing.T) {
	contents := map[string][]byte{}
	b := []byte("")
	contents[digestOf(b).Hex] = b
	stack := []layer.Layer{
		{entryFile("f", b, 0o600, baseTime), entryHardlink("ln", "f")},
		{entryFile("f", b, 0o644, baseTime)},
	}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Materialize(root, view, blobDir(t, dir, contents)); err != nil {
		t.Fatal(err)
	}
	ln, _ := os.Stat(filepath.Join(rootDir, "ln"))
	f, _ := os.Stat(filepath.Join(rootDir, "f"))
	if os.SameFile(ln, f) {
		t.Fatal("link shares an inode with the same-bytes replacement")
	}
	if ln.Mode().Perm() != 0o600 || f.Mode().Perm() != 0o644 {
		t.Fatalf("modes = link %v, target %v; want captured 0600 and replacement 0644", ln.Mode().Perm(), f.Mode().Perm())
	}
}

// TestHardlinkChainMaterializes: a link whose target is itself a
// link (in-view chains resolve to captured content) materializes
// regardless of creation order — all chain members share one inode
// when their identities agree.
func TestHardlinkChainMaterializes(t *testing.T) {
	contents := map[string][]byte{}
	b := []byte("chained")
	contents[digestOf(b).Hex] = b
	stack := []layer.Layer{{
		entryFile("z", b, 0o755, baseTime),
		entryHardlink("m", "z"),
		entryHardlink("a", "m"),
	}}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Materialize(root, view, blobDir(t, dir, contents)); err != nil {
		t.Fatalf("chain export failed: %v", err)
	}
	a, _ := os.Stat(filepath.Join(rootDir, "a"))
	z, _ := os.Stat(filepath.Join(rootDir, "z"))
	if !os.SameFile(a, z) {
		t.Fatal("chain link does not share the plain target's inode")
	}
	if bts, _ := os.ReadFile(filepath.Join(rootDir, "a")); string(bts) != "chained" {
		t.Fatalf("chain link content = %q", bts)
	}
}

// TestCollisionErrorNamesBothPaths pins REQ-export-fidelity's
// refusal clause at the unit seam: an EEXIST whose case-folded
// spelling was already created is diagnosed naming both paths, while
// other errors pass through untouched.
func TestCollisionErrorNamesBothPaths(t *testing.T) {
	folded := map[string]string{"etc/config": "etc/Config"}
	err := collisionError(folded, "etc/config", fs.ErrExist)
	if err == nil || !strings.Contains(err.Error(), `"etc/Config"`) || !strings.Contains(err.Error(), `"etc/config"`) {
		t.Fatalf("collision diagnosis = %v, want both spellings named", err)
	}
	if err := collisionError(folded, "etc/other", fs.ErrExist); !errors.Is(err, fs.ErrExist) || strings.Contains(err.Error(), "distinct") {
		t.Fatalf("non-colliding EEXIST rewritten: %v", err)
	}
	sentinel := errors.New("io broke")
	if err := collisionError(folded, "etc/config", sentinel); !errors.Is(err, sentinel) || errors.Is(err, fs.ErrExist) {
		t.Fatalf("unrelated error rewritten: %v", err)
	}
}

// TestMissingBlobFailsNamingEntry: a damaged CAS fails the export
// identifying the entry and digest rather than materializing a hole.
func TestMissingBlobFailsNamingEntry(t *testing.T) {
	contents := map[string][]byte{}
	b := []byte("present")
	contents[digestOf(b).Hex] = b
	stack := []layer.Layer{{entryFile("keep", b, 0o644, baseTime), entryFile("gone", []byte("absent"), 0o644, baseTime)}}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = Materialize(root, view, blobDir(t, dir, contents))
	if err == nil {
		t.Fatal("export succeeded with a missing blob")
	}
	if !strings.Contains(err.Error(), `"gone"`) || !strings.Contains(err.Error(), digestOf([]byte("absent")).Hex) {
		t.Fatalf("error %v does not identify the entry and digest", err)
	}
}

// TestHardlinkCycleMaterializes: a later layer can re-link a chain
// member back onto another, so a view can hold a link cycle; the
// materializer breaks it with an independent copy of the captured
// content and links the rest onto it — both entries present, content
// faithful.
func TestHardlinkCycleMaterializes(t *testing.T) {
	contents := map[string][]byte{}
	b := []byte("cyclic")
	contents[digestOf(b).Hex] = b
	stack := []layer.Layer{
		{entryFile("b", b, 0o644, baseTime), entryHardlink("a", "b")},
		{entryHardlink("b", "a")},
	}
	view, err := layer.Unify(stack)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratchtest.Dir(t, "export")
	rootDir := filepath.Join(dir, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Materialize(root, view, blobDir(t, dir, contents)); err != nil {
		t.Fatalf("cyclic-view export failed: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if got, err := os.ReadFile(filepath.Join(rootDir, name)); err != nil || string(got) != "cyclic" {
			t.Fatalf("cycle member %q = %q, %v", name, got, err)
		}
	}
}
