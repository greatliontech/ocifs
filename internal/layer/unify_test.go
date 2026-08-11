package layer

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"
	"strings"
	"testing"
	"time"
)

// --- entry construction helpers ---

func digestOf(content string) (d struct {
	Algorithm string
	Hex       string
}) {
	sum := sha256.Sum256([]byte(content))
	d.Algorithm = "sha256"
	d.Hex = hex.EncodeToString(sum[:])
	return
}

func file(name, content string) Entry {
	e := Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeReg, Size: int64(len(content)), Mode: 0o644}}
	d := digestOf(content)
	e.Digest.Algorithm = d.Algorithm
	e.Digest.Hex = d.Hex
	return e
}

func dir(name string) Entry {
	return dirMode(name, 0o755)
}

func dirMode(name string, mode int64) Entry {
	return Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: mode}}
}

func symlink(name, target string) Entry {
	return Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777}}
}

func hardlink(name, target string) Entry {
	return Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeLink, Linkname: target}}
}

func fifo(name string) Entry {
	return Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeFifo, Mode: 0o644}}
}

func wh(target string) Entry {
	d, b := path.Split(target)
	return Entry{Header: tar.Header{Name: d + ".wh." + b, Typeflag: tar.TypeReg}}
}

func opq(dirName string) Entry {
	return Entry{Header: tar.Header{Name: dirName + "/.wh..wh..opq", Typeflag: tar.TypeReg}}
}

// want is one expected view entry: path, typeflag, and — per type —
// content (regular/hardlink, compared by digest), or symlink target.
type want struct {
	name    string
	flag    byte
	content string
	link    string
}

func checkView(t *testing.T, v *View, wants []want) {
	t.Helper()
	checkInvariants(t, v)
	got := v.Entries()
	if len(got) != len(wants) {
		names := make([]string, len(got))
		for i, e := range got {
			names[i] = e.Header.Name
		}
		t.Fatalf("view has %d entries, want %d\ngot: %v", len(got), len(wants), names)
	}
	for i, w := range wants {
		e := got[i]
		if e.Header.Name != w.name {
			t.Errorf("entry %d: name %q, want %q", i, e.Header.Name, w.name)
			continue
		}
		if e.Header.Typeflag != w.flag {
			t.Errorf("entry %q: typeflag %q, want %q", w.name, e.Header.Typeflag, w.flag)
		}
		if w.flag == tar.TypeReg || w.flag == tar.TypeLink {
			wd := digestOf(w.content)
			if e.Digest.Hex != wd.Hex {
				t.Errorf("entry %q: content digest mismatch (want content %q)", w.name, w.content)
			}
			if e.Header.Size != int64(len(w.content)) {
				t.Errorf("entry %q: size %d, want %d", w.name, e.Header.Size, len(w.content))
			}
		}
		if w.flag == tar.TypeSymlink && e.Header.Linkname != w.link {
			t.Errorf("entry %q: symlink target %q, want %q", w.name, e.Header.Linkname, w.link)
		}
	}
}

// reporter is the subset of testing.TB that rapid.T also satisfies,
// letting the invariant and oracle helpers serve example tests and
// property runs alike.
type reporter interface {
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// checkInvariants asserts the REQ-unify-clean / -contained / -sorted
// output invariants on any view.
func checkInvariants(t reporter, v *View) {
	entries := v.Entries()
	if v.Len() != len(entries) {
		t.Errorf("Len() = %d, want %d", v.Len(), len(entries))
	}
	nonDirs := map[string]bool{}
	for i, e := range entries {
		name := e.Header.Name
		if i > 0 && entries[i-1].Header.Name >= name {
			t.Errorf("view not strictly sorted at %q", name)
		}
		if cleanName(name) != name || escapes(name) {
			t.Errorf("view contains uncleaned or escaping path %q", name)
		}
		base := path.Base(name)
		if strings.HasPrefix(base, ".wh.") {
			t.Errorf("view contains whiteout marker %q", name)
		}
		if e.Header.Typeflag != tar.TypeDir {
			nonDirs[name] = true
		}
	}
	dirs := map[string]bool{".": true}
	for _, e := range entries {
		if e.Header.Typeflag == tar.TypeDir {
			dirs[e.Header.Name] = true
		}
	}
	for _, e := range entries {
		for c := range strings.SplitSeq(e.Header.Name, "/") {
			if e.Header.Name != "." && strings.HasPrefix(c, ".wh.") {
				t.Errorf("entry %q has reserved-namespace component %q", e.Header.Name, c)
			}
		}
		for p := path.Dir(e.Header.Name); p != "." && p != "/"; p = path.Dir(p) {
			if nonDirs[p] {
				t.Errorf("entry %q has non-directory ancestor %q", e.Header.Name, p)
			}
			if !dirs[p] {
				t.Errorf("view incomplete: entry %q has no directory entry for ancestor %q", e.Header.Name, p)
			}
		}
	}
}

func mustUnify(t *testing.T, layers ...Layer) *View {
	t.Helper()
	v, err := Unify(layers)
	if err != nil {
		t.Fatalf("Unify: %v", err)
	}
	return v
}

// --- precedence and override ---

func TestSingleLayer(t *testing.T) {
	v := mustUnify(t, Layer{dir("app"), file("app/cfg", "x")})
	checkView(t, v, []want{
		{name: "app", flag: tar.TypeDir},
		{name: "app/cfg", flag: tar.TypeReg, content: "x"},
	})
}

func TestTopLayerOverrides(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("app"), file("app/cfg", "old")},
		Layer{file("app/cfg", "new")},
	)
	checkView(t, v, []want{
		{name: "app", flag: tar.TypeDir},
		{name: "app/cfg", flag: tar.TypeReg, content: "new"},
	})
}

func TestIntraLayerLastWins(t *testing.T) {
	v := mustUnify(t, Layer{file("a", "v1"), file("a", "v2")})
	checkView(t, v, []want{{name: "a", flag: tar.TypeReg, content: "v2"}})
}

func TestFileReplacesDirectoryDropsSubtree(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("a"), file("a/f", "x"), dir("a/b"), file("a/b/g", "y")},
		Layer{file("a", "now a file")},
	)
	checkView(t, v, []want{{name: "a", flag: tar.TypeReg, content: "now a file"}})
}

func TestDirOverDirKeepsChildrenUpdatesAttrs(t *testing.T) {
	v := mustUnify(t,
		Layer{dirMode("a", 0o700), file("a/f", "x")},
		Layer{dirMode("a", 0o755)},
	)
	checkView(t, v, []want{
		{name: "a", flag: tar.TypeDir},
		{name: "a/f", flag: tar.TypeReg, content: "x"},
	})
	e, _ := v.Lookup("a")
	if e.Header.Mode != 0o755 {
		t.Errorf("dir mode %o, want 0755 (top layer wins)", e.Header.Mode)
	}
}

func TestNonDirAncestorDiscardsDescendants(t *testing.T) {
	// Across layers: lower symlink finalizes the path.
	v := mustUnify(t,
		Layer{symlink("a", "/etc")},
		Layer{file("a/evil", "x")},
	)
	checkView(t, v, []want{{name: "a", flag: tar.TypeSymlink, link: "/etc"}})

	// Same layer, either order.
	v = mustUnify(t, Layer{file("b", "f"), file("b/c", "x")})
	checkView(t, v, []want{{name: "b", flag: tar.TypeReg, content: "f"}})
}

// --- whiteouts ---

func TestWhiteoutDeletesLowerSubtree(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("app"), file("app/secret", "s"), dir("app/d"), file("app/d/x", "y")},
		Layer{wh("app")},
	)
	checkView(t, v, nil)
}

func TestWhiteoutOrderIndependentWithinLayer(t *testing.T) {
	lower := Layer{dir("app"), file("app/secret", "s")}
	orderA := Layer{wh("app"), dir("app"), file("app/new", "n")}
	orderB := Layer{dir("app"), file("app/new", "n"), wh("app")}
	wants := []want{
		{name: "app", flag: tar.TypeDir},
		{name: "app/new", flag: tar.TypeReg, content: "n"},
	}
	checkView(t, mustUnify(t, lower, orderA), wants)
	checkView(t, mustUnify(t, lower, orderB), wants)
}

func TestWhiteoutThenReplacementFile(t *testing.T) {
	for _, l := range []Layer{
		{wh("x"), file("x", "new")},
		{file("x", "new"), wh("x")},
	} {
		v := mustUnify(t, Layer{file("x", "old")}, l)
		checkView(t, v, []want{{name: "x", flag: tar.TypeReg, content: "new"}})
	}
}

func TestWhiteoutUnderImpliedDir(t *testing.T) {
	v := mustUnify(t,
		Layer{file("a/b/c", "x")}, // a and a/b implied
		Layer{wh("a/b")},
	)
	// The implied "a" became real at extraction and survives its
	// child's deletion.
	checkView(t, v, []want{{name: "a", flag: tar.TypeDir}})
}

func TestDegenerateMarkersHaveNoEffect(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("d"), file("d/f", "x"), file("top", "t")},
		Layer{
			{Header: tar.Header{Name: "d/.wh..", Typeflag: tar.TypeReg}},
			{Header: tar.Header{Name: "d/.wh...", Typeflag: tar.TypeReg}},
			{Header: tar.Header{Name: "d/.wh..wh.f", Typeflag: tar.TypeReg}},
			{Header: tar.Header{Name: ".wh.", Typeflag: tar.TypeReg}},
		},
	)
	checkView(t, v, []want{
		{name: "d", flag: tar.TypeDir},
		{name: "d/f", flag: tar.TypeReg, content: "x"},
		{name: "top", flag: tar.TypeReg, content: "t"},
	})
}

// --- opaque whiteouts ---

func TestOpaqueRemovesLowerKeepsSameLayer(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("app"), dir("app/migrations"), file("app/migrations/001", "sql")},
		Layer{opq("app"), file("app/new", "keep")},
	)
	checkView(t, v, []want{
		{name: "app", flag: tar.TypeDir},
		{name: "app/new", flag: tar.TypeReg, content: "keep"},
	})
}

func TestOpaqueOnAbsentDirIsNoop(t *testing.T) {
	v := mustUnify(t, Layer{opq("ghost"), file("f", "x")})
	checkView(t, v, []want{{name: "f", flag: tar.TypeReg, content: "x"}})
}

func TestThreeLayerInteraction(t *testing.T) {
	v := mustUnify(t,
		Layer{dir("var"), dir("var/log"), dir("etc"),
			file("var/log/dmesg", "kernel"), file("etc/hostname", "base")},
		Layer{wh("var/log/dmesg"), file("var/log/app.log", "app"),
			file("etc/hostname", "middle")},
		Layer{opq("var/log"), file("var/log/new.log", "fresh")},
	)
	checkView(t, v, []want{
		{name: "etc", flag: tar.TypeDir},
		{name: "etc/hostname", flag: tar.TypeReg, content: "middle"},
		{name: "var", flag: tar.TypeDir},
		{name: "var/log", flag: tar.TypeDir},
		{name: "var/log/new.log", flag: tar.TypeReg, content: "fresh"},
	})
}

// --- path cleaning and containment ---

func TestSpellingsUnify(t *testing.T) {
	v := mustUnify(t,
		Layer{file("/etc/passwd", "old")},
		Layer{file("./etc//passwd", "new")},
	)
	checkView(t, v, []want{
		{name: "etc", flag: tar.TypeDir},
		{name: "etc/passwd", flag: tar.TypeReg, content: "new"},
	})
}

func TestEscapeFailsUnification(t *testing.T) {
	for _, name := range []string{"../evil", "a/../../evil", ".."} {
		_, err := Unify([]Layer{{{Header: tar.Header{Name: name, Typeflag: tar.TypeReg}}}})
		if !errors.Is(err, ErrPathEscape) {
			t.Errorf("name %q: err = %v, want ErrPathEscape", name, err)
		}
	}
}

func TestNonDirAtRootFails(t *testing.T) {
	_, err := Unify([]Layer{{file(".", "x")}})
	if !errors.Is(err, ErrPathEscape) {
		t.Errorf("err = %v, want ErrPathEscape", err)
	}
}

func TestRootEntryCarriesRootAttrs(t *testing.T) {
	v := mustUnify(t, Layer{dirMode("./", 0o701), file("f", "x"), file("!f", "y")})
	// The root entry participates in plain byte order: '!' (0x21)
	// sorts before '.' (0x2e), so a sibling can precede it.
	checkView(t, v, []want{
		{name: "!f", flag: tar.TypeReg, content: "y"},
		{name: ".", flag: tar.TypeDir},
		{name: "f", flag: tar.TypeReg, content: "x"},
	})
	e, ok := v.Lookup(".")
	if !ok || e.Header.Mode != 0o701 {
		t.Errorf("root entry mode = %o, want 0701", e.Header.Mode)
	}
}

// --- sorting ---

func TestFlatByteSortOrder(t *testing.T) {
	// "a" < "a!" < "a/b" under byte order ('!' 0x21 < '/' 0x2f).
	v := mustUnify(t, Layer{dir("a"), file("a!", "x"), file("a/b", "y")})
	checkView(t, v, []want{
		{name: "a", flag: tar.TypeDir},
		{name: "a!", flag: tar.TypeReg, content: "x"},
		{name: "a/b", flag: tar.TypeReg, content: "y"},
	})
}

// --- hardlinks ---

func TestHardlinkCapturesContentAtPosition(t *testing.T) {
	v := mustUnify(t,
		Layer{file("bin/busybox", "v1"), hardlink("bin/sh", "bin/busybox")},
		Layer{file("bin/busybox", "v2")},
	)
	// The link captured v1; the target was later replaced with v2 —
	// kernel union semantics, not final-view resolution.
	checkView(t, v, []want{
		{name: "bin", flag: tar.TypeDir},
		{name: "bin/busybox", flag: tar.TypeReg, content: "v2"},
		{name: "bin/sh", flag: tar.TypeLink, content: "v1"},
	})
}

func TestHardlinkTargetSpellingCleaned(t *testing.T) {
	v := mustUnify(t, Layer{file("bin/true", "t"), hardlink("bin/false", "./bin//true")})
	e, ok := v.Lookup("bin/false")
	if !ok || e.Header.Linkname != "bin/true" {
		t.Fatalf("linkname = %q, want cleaned bin/true", e.Header.Linkname)
	}
	checkView(t, v, []want{
		{name: "bin", flag: tar.TypeDir},
		{name: "bin/false", flag: tar.TypeLink, content: "t"},
		{name: "bin/true", flag: tar.TypeReg, content: "t"},
	})
}

func TestHardlinkChainsResolve(t *testing.T) {
	v := mustUnify(t, Layer{
		file("a", "x"),
		hardlink("b", "a"),
		hardlink("c", "b"),
	})
	checkView(t, v, []want{
		{name: "a", flag: tar.TypeReg, content: "x"},
		{name: "b", flag: tar.TypeLink, content: "x"},
		{name: "c", flag: tar.TypeLink, content: "x"},
	})
}

func TestReservedComponentInert(t *testing.T) {
	v := mustUnify(t,
		Layer{file("ok", "keep")},
		Layer{
			{Header: tar.Header{Name: ".wh.x/y", Typeflag: tar.TypeReg}},
			{Header: tar.Header{Name: "a/.wh.d/z", Typeflag: tar.TypeDir}},
		},
	)
	checkView(t, v, []want{{name: "ok", flag: tar.TypeReg, content: "keep"}})
}

func TestNameLengthCapFails(t *testing.T) {
	long := strings.Repeat("a/", 3000) + "f"
	_, err := Unify([]Layer{{{Header: tar.Header{Name: long, Typeflag: tar.TypeReg}}}})
	if !errors.Is(err, ErrPathEscape) {
		t.Fatalf("err = %v, want ErrPathEscape for over-long name", err)
	}
}

func TestDiscardedEntryLeavesParents(t *testing.T) {
	v := mustUnify(t, Layer{file("m/n", "x"), file("m/n/o/p", "gone")})
	checkView(t, v, []want{
		{name: "m", flag: tar.TypeDir},
		{name: "m/n", flag: tar.TypeReg, content: "x"},
	})
}

func TestUnresolvableLinkLeavesParents(t *testing.T) {
	v := mustUnify(t, Layer{hardlink("d/l", "missing")})
	checkView(t, v, []want{{name: "d", flag: tar.TypeDir}})
}

func TestHardlinkCapturesInodeAttributes(t *testing.T) {
	target := file("bin/tool", "payload")
	target.Header.Uid, target.Header.Gid = 1234, 5678
	target.Header.Uname, target.Header.Gname = "svc", "grp"
	target.Header.ModTime = time.Unix(1700000000, 0)
	target.Header.AccessTime = time.Unix(1700000001, 0)
	target.Header.ChangeTime = time.Unix(1700000002, 0)
	target.Header.Xattrs = map[string]string{"user.legacy": "v"} //nolint:staticcheck // legacy producers still emit it
	target.Header.PAXRecords = map[string]string{"SCHILY.xattr.security.capability": "cap"}
	v := mustUnify(t, Layer{target, hardlink("bin/alias", "bin/tool")})
	e, ok := v.Lookup("bin/alias")
	if !ok {
		t.Fatal("link missing from view")
	}
	h := e.Header
	if h.Uid != 1234 || h.Gid != 5678 || h.Uname != "svc" || h.Gname != "grp" ||
		!h.ModTime.Equal(time.Unix(1700000000, 0)) ||
		!h.AccessTime.Equal(time.Unix(1700000001, 0)) ||
		!h.ChangeTime.Equal(time.Unix(1700000002, 0)) ||
		h.Xattrs["user.legacy"] != "v" || //nolint:staticcheck // legacy field still carried
		h.PAXRecords["SCHILY.xattr.security.capability"] != "cap" {
		t.Errorf("link did not capture target inode attributes: %+v", h)
	}
}

func TestDeviceEntriesCarried(t *testing.T) {
	v := mustUnify(t, Layer{
		{Header: tar.Header{Name: "dev-null", Typeflag: tar.TypeChar, Devmajor: 1, Devminor: 3, Mode: 0o666}},
		{Header: tar.Header{Name: "disk", Typeflag: tar.TypeBlock, Devmajor: 8, Devminor: 0, Mode: 0o660}},
	})
	c, _ := v.Lookup("dev-null")
	b, _ := v.Lookup("disk")
	if c.Header.Typeflag != tar.TypeChar || c.Header.Devmajor != 1 || c.Header.Devminor != 3 {
		t.Errorf("char device not carried: %+v", c.Header)
	}
	if b.Header.Typeflag != tar.TypeBlock || b.Header.Devmajor != 8 {
		t.Errorf("block device not carried: %+v", b.Header)
	}
}

func TestSelfLinkNoop(t *testing.T) {
	v := mustUnify(t, Layer{file("a", "keep"), hardlink("a", "a")})
	checkView(t, v, []want{{name: "a", flag: tar.TypeReg, content: "keep"}})
}

func TestHardlinkUnresolvableOmitted(t *testing.T) {
	v := mustUnify(t, Layer{
		dir("d"),
		symlink("s", "d"),
		hardlink("l1", "missing"), // absent
		hardlink("l2", "d"),       // directory
		hardlink("l3", "s"),       // symlink
		hardlink("l4", "later"),   // target appears only later
		file("later", "too late"),
	})
	checkView(t, v, []want{
		{name: "d", flag: tar.TypeDir},
		{name: "later", flag: tar.TypeReg, content: "too late"},
		{name: "s", flag: tar.TypeSymlink, link: "d"},
	})
}

// --- symlinks, special types, metadata entries ---

func TestSymlinkTargetVerbatim(t *testing.T) {
	v := mustUnify(t, Layer{symlink("l", "/absolute/../weird/"), fifo("p")})
	checkView(t, v, []want{
		{name: "l", flag: tar.TypeSymlink, link: "/absolute/../weird/"},
		{name: "p", flag: tar.TypeFifo},
	})
}

func TestMetaEntriesIgnored(t *testing.T) {
	v := mustUnify(t, Layer{
		{Header: tar.Header{Name: "pax_global_header", Typeflag: tar.TypeXGlobalHeader}},
		{Header: tar.Header{Name: "ignored", Typeflag: tar.TypeXHeader}},
		file("real", "x"),
	})
	checkView(t, v, []want{{name: "real", flag: tar.TypeReg, content: "x"}})
}

func TestLookupMissing(t *testing.T) {
	v := mustUnify(t, Layer{file("present", "x")})
	if e, ok := v.Lookup("absent"); ok || e.Header.Name != "" {
		t.Fatalf("Lookup(absent) = (%+v, %v), want zero entry and false", e, ok)
	}
	if _, ok := v.Lookup("present"); !ok {
		t.Fatalf("Lookup(present) not found")
	}
}

func TestLegacyRegTypeNormalized(t *testing.T) {
	v := mustUnify(t, Layer{{Header: tar.Header{Name: "old", Typeflag: 0, Size: 1}}})
	e, ok := v.Lookup("old")
	if !ok || e.Header.Typeflag != tar.TypeReg {
		t.Fatalf("legacy typeflag not normalized to TypeReg")
	}
}

func TestInputNotMutated(t *testing.T) {
	in := Layer{
		file("/a//b", "x"),
		hardlink("l", "./a//b"),
		{Header: tar.Header{Name: "legacy", Typeflag: 0, Size: 1}},
	}
	origs := make([]tar.Header, len(in))
	for i := range in {
		origs[i] = in[i].Header
	}
	mustUnify(t, in)
	for i := range in {
		if in[i].Header.Name != origs[i].Name ||
			in[i].Header.Linkname != origs[i].Linkname ||
			in[i].Header.Typeflag != origs[i].Typeflag {
			t.Errorf("input header %d mutated: %+v -> %+v", i, origs[i], in[i].Header)
		}
	}
}
