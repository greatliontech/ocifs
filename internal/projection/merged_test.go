//go:build linux

package projection

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/upper"
)

var upTime = time.Date(2024, 6, 2, 10, 0, 0, 0, time.UTC)

func newUpperFor(t testing.TB) (string, *upper.Writer) {
	t.Helper()
	dir := scratchtest.Dir(t, "projection")
	root := filepath.Join(dir, "u")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	return root, upper.NewWriter(root)
}

func mustMerged(t testing.TB, inner *Projection, root string) *Merged {
	t.Helper()
	m, err := NewMerged(inner, root)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mustLookup(t testing.TB, m *Merged, dir *Node, name string) *Node {
	t.Helper()
	n, ok, err := m.Lookup(dir, name)
	if err != nil {
		t.Fatalf("lookup %q: %v", name, err)
	}
	if !ok {
		t.Fatalf("lookup %q: not presented", name)
	}
	return n
}

func inoOf(t testing.TB, hostPath string) uint64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(hostPath, &st); err != nil {
		t.Fatal(err)
	}
	return st.Ino
}

// TestMergedPresentation pins REQ-writable-presented over one
// fixture: shadow entirely, whiteout occlusion, coexisting
// recreation, opaque, upper-only content, sockets presented live,
// markers invisible.
func TestMergedPresentation(t *testing.T) {
	view := mustView(t,
		ldir("d"),
		lfile("d/gone"),
		lfile("d/keep"),
		lfile("d/keep2"),
		lfile("d/shadow"),
		lfile("re"),
		ldir("wipe"),
		lfile("wipe/x"),
	)
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	if err := w.Mkdir("d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("d/shadow", strings.NewReader("new"), 0o600, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("d/gone"); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("re"); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("re", strings.NewReader("again"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("wipe", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("wipe"); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("wipe/u", strings.NewReader("upper"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("fresh", strings.NewReader("f"), 0o640, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mknod(filepath.Join(root, "sk"), unix.S_IFSOCK|0o755, 0); err != nil {
		t.Fatal(err)
	}

	m := mustMerged(t, inner, root)
	rootN := m.Root()
	if rootN.ID() != RootID || rootN.Kind() != KindDir {
		t.Fatalf("root wrong: %v %v", rootN.ID(), rootN.Kind())
	}

	d := mustLookup(t, m, rootN, "d")
	if !d.UpperBacked() || d.Kind() != KindDir {
		t.Fatalf("shadowed dir wrong: %+v", d)
	}
	keep := mustLookup(t, m, d, "keep")
	if keep.UpperBacked() || keep.ContentDigest() != fakeDigest("d/keep") {
		t.Fatalf("base file wrong: %+v", keep)
	}
	if _, ok, err := m.Lookup(d, "gone"); ok || err != nil {
		t.Fatalf("whited-out base presented: %v %v", ok, err)
	}
	shadow := mustLookup(t, m, d, "shadow")
	if !shadow.UpperBacked() || shadow.Header().Mode != 0o600 {
		t.Fatalf("shadow wrong: %+v", shadow.Header())
	}
	defer shadow.Close()
	re := mustLookup(t, m, rootN, "re")
	defer re.Close()
	if !re.UpperBacked() {
		t.Fatal("recreated entry not upper-backed")
	}
	wipe := mustLookup(t, m, rootN, "wipe")
	defer wipe.Close()
	if _, ok, _ := m.Lookup(wipe, "x"); ok {
		t.Fatal("opaque leaked base child")
	}
	u := mustLookup(t, m, wipe, "u")
	u.Close()
	sk := mustLookup(t, m, rootN, "sk")
	defer sk.Close()
	if sk.Kind() != KindSocket {
		t.Fatalf("socket kind wrong: %v", sk.Kind())
	}
	if _, ok, _ := m.Lookup(rootN, ".wh.re"); ok {
		t.Fatal("marker presented")
	}

	snap, err := m.OpenDir(rootN)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for i := 0; i < snap.Len(); i++ {
		names = append(names, snap.At(i).Name)
	}
	want := []string{"d", "fresh", "re", "sk", "wipe"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("root enumeration %v, want %v", names, want)
	}
	dsnap, err := m.OpenDir(d)
	if err != nil {
		t.Fatal(err)
	}
	names = nil
	for i := 0; i < dsnap.Len(); i++ {
		names = append(names, dsnap.At(i).Name)
	}
	if fmt.Sprint(names) != fmt.Sprint([]string{"keep", "keep2", "shadow"}) {
		t.Fatalf("d enumeration %v", names)
	}
	wsnap, err := m.OpenDir(wipe)
	if err != nil {
		t.Fatal(err)
	}
	if wsnap.Len() != 1 || wsnap.At(0).Name != "u" {
		t.Fatalf("opaque enumeration wrong: %+v", wsnap.Entries())
	}
	d.Close()
	keep.Close()
}

// TestMergedIdentity pins REQ-proj-identity's merge rules:
// shadow-in-place keeps the view ID, deletion+recreation and
// occluded recreations draw ino-derived upper-born identity,
// hardlink migration moves a shadowing target to its group's
// upper-born identity, and identity is stable across remounts of
// the same upper.
func TestMergedIdentity(t *testing.T) {
	view := mustView(t,
		ldir("d"),
		lfile("d/keep2"),
		lfile("d/shadow"),
		lkind("pf", tar.TypeFifo),
		lfile("re"),
	)
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	if err := w.Mkdir("d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("d/shadow", strings.NewReader("new"), 0o600, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("re"); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("re", strings.NewReader("again"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("d/keep2", strings.NewReader("k2"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("d/keep2", "h3"); err != nil {
		t.Fatal(err)
	}
	// A non-file hardlink group: a fifo shadowing base, linked — the
	// one-ID rule has no kind restriction (REQ-writable-hardlink).
	if err := w.Mkfifo("pf", 0o640); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("pf", "pl"); err != nil {
		t.Fatal(err)
	}

	m := mustMerged(t, inner, root)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	viewIDAt := func(p string) ID {
		e, ok := m.viewAt(p)
		if !ok {
			t.Fatalf("view path %q missing", p)
		}
		return e.ID()
	}

	if d.ID() != viewIDAt("d") {
		t.Fatalf("shadowed dir ID %d, want view %d", d.ID(), viewIDAt("d"))
	}
	shadow := mustLookup(t, m, d, "shadow")
	defer shadow.Close()
	if shadow.ID() != viewIDAt("d/shadow") {
		t.Fatalf("shadow-in-place ID %d, want view %d", shadow.ID(), viewIDAt("d/shadow"))
	}
	re := mustLookup(t, m, rootN, "re")
	defer re.Close()
	wantRe := upperIDBase | ID(inoOf(t, filepath.Join(root, "re")))
	if re.ID() != wantRe {
		t.Fatalf("recreated ID %d, want upper-born %d", re.ID(), wantRe)
	}
	k2 := mustLookup(t, m, d, "keep2")
	defer k2.Close()
	h3 := mustLookup(t, m, rootN, "h3")
	defer h3.Close()
	if k2.ID() != h3.ID() {
		t.Fatalf("hardlink group split: %d vs %d", k2.ID(), h3.ID())
	}
	if k2.ID() < upperIDBase || k2.ID() >= syntheticIDBase {
		t.Fatalf("migrated hardlink outside upper-born partition: %d", k2.ID())
	}
	if k2.Nlink() != 2 {
		t.Fatalf("nlink %d, want 2", k2.Nlink())
	}
	pf := mustLookup(t, m, rootN, "pf")
	defer pf.Close()
	pl := mustLookup(t, m, rootN, "pl")
	defer pl.Close()
	if pf.Kind() != KindFIFO || pf.ID() != pl.ID() {
		t.Fatalf("fifo link group split: %v %d vs %d", pf.Kind(), pf.ID(), pl.ID())
	}
	if pf.ID() < upperIDBase || pf.ID() >= syntheticIDBase {
		t.Fatalf("linked fifo outside upper-born partition: %d", pf.ID())
	}

	// Remount the same upper: identical identities (REQ-proj-identity).
	m2 := mustMerged(t, inner, root)
	root2 := m2.Root()
	d2 := mustLookup(t, m2, root2, "d")
	defer d2.Close()
	for _, tc := range []struct {
		dir  *Node
		name string
		want ID
	}{
		{root2, "re", re.ID()},
		{root2, "h3", h3.ID()},
		{d2, "shadow", shadow.ID()},
		{d2, "keep2", k2.ID()},
	} {
		n := mustLookup(t, m2, tc.dir, tc.name)
		if n.ID() != tc.want {
			t.Fatalf("remount changed %q: %d -> %d", tc.name, tc.want, n.ID())
		}
		n.Close()
	}
}

// TestPinnedIdentityAfterRenameOver pins the identity/pin single
// source: when the path was replaced between the walk and the
// lookup, the ID names the inode the pin actually guards — never
// the stale walked number, which would stay unpinned and mintable
// for a second live object.
func TestPinnedIdentityAfterRenameOver(t *testing.T) {
	view := mustView(t, lfile("a"))
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	if err := w.PublishFile("swap", strings.NewReader("v1"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	m := mustMerged(t, inner, root)
	// Replace behind the kernel's back — no Refresh: the index is
	// stale when the lookup runs.
	if err := w.PublishFile("swap", strings.NewReader("v2"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	n := mustLookup(t, m, m.Root(), "swap")
	defer n.Close()
	cur := inoOf(t, filepath.Join(root, "swap"))
	if n.ID() != upperIDBase|ID(cur) {
		t.Fatalf("ID %d does not name the current inode %d", n.ID(), cur)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(n.Pin().Fd()), &st); err != nil {
		t.Fatal(err)
	}
	if st.Ino != cur {
		t.Fatalf("pin holds ino %d, path has %d", st.Ino, cur)
	}
}

// TestMergedRefusesPartialEnvelope pins the construction guard: the
// merge requires the all-presenting byte-order envelope.
func TestMergedRefusesPartialEnvelope(t *testing.T) {
	view := mustView(t, lfile("a"))
	root, _ := newUpperFor(t)
	for _, caps := range []Capabilities{
		{Compare: foldCompare, Symlinks: true, FIFOs: true, Devices: true},
		{ValidName: func(string) bool { return true }, Symlinks: true, FIFOs: true, Devices: true},
		{Symlinks: true, FIFOs: true},
	} {
		inner := mustNew(t, view, nil, caps)
		if _, err := NewMerged(inner, root); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("partial envelope accepted: %v", err)
		}
	}
}

// TestMergedWithExtras pins the extras interplay: synthetic
// directories keep their synthetic IDs when the upper shadows them
// in place, and upper content beneath them is upper-born.
func TestMergedWithExtras(t *testing.T) {
	view := mustView(t, lfile("a"))
	inner := mustNew(t, view, []string{"ex/tra"}, capsFull)
	root, w := newUpperFor(t)
	if err := w.Mkdir("ex", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("ex/tra", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("ex/tra/g", strings.NewReader("g"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	m := mustMerged(t, inner, root)
	ex := mustLookup(t, m, m.Root(), "ex")
	defer ex.Close()
	tra := mustLookup(t, m, ex, "tra")
	defer tra.Close()
	g := mustLookup(t, m, tra, "g")
	defer g.Close()
	exView, _ := m.viewAt("ex")
	traView, _ := m.viewAt("ex/tra")
	if ex.ID() != exView.ID() || tra.ID() != traView.ID() {
		t.Fatalf("shadowed synthetic IDs moved: %d/%d vs %d/%d", ex.ID(), tra.ID(), exView.ID(), traView.ID())
	}
	if ex.ID() < syntheticIDBase {
		t.Fatalf("synthetic dir outside its partition: %d", ex.ID())
	}
	if g.ID() < upperIDBase || g.ID() >= syntheticIDBase {
		t.Fatalf("upper file under extras outside upper-born partition: %d", g.ID())
	}
}

// TestIdentityRangeLoud pins the derivation envelope: an inode at
// or above the partition base fails the operation rather than
// aliasing.
func TestIdentityRangeLoud(t *testing.T) {
	view := mustView(t, lfile("a"))
	inner := mustNew(t, view, nil, capsFull)
	root, _ := newUpperFor(t)
	m := mustMerged(t, inner, root)
	_, _, err := m.presentedID("x", upper.Entry{Kind: upper.KindFile, Ino: 1 << 61}, 0, m.index())
	if !errors.Is(err, ErrIdentityRange) {
		t.Fatalf("ino at partition base: %v", err)
	}
}

// TestEnumerationSnapshotsAndVerifiers pins REQ-proj-enumeration
// for the writable case: snapshots are immutable and resumable
// across upper mutation, and verifiers derive from upper directory
// state — constant without mutation, changed by each mutation
// class independently (entry membership, whiteouts, opaque),
// untouched for unrelated directories.
func TestEnumerationSnapshotsAndVerifiers(t *testing.T) {
	view := mustView(t,
		ldir("d"), lfile("d/a"), lfile("d/b"),
		ldir("dw"), lfile("dw/x"),
		ldir("do"), lfile("do/x"),
		ldir("q"), lfile("q/z"),
	)
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	m := mustMerged(t, inner, root)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")

	verifier := func(name string) uint64 {
		t.Helper()
		n := mustLookup(t, m, rootN, name)
		defer n.Close()
		s, err := m.OpenDir(n)
		if err != nil {
			t.Fatal(err)
		}
		return s.Verifier()
	}

	s1, err := m.OpenDir(d)
	if err != nil {
		t.Fatal(err)
	}
	if v := verifier("d"); v != s1.Verifier() {
		t.Fatal("verifier unstable without mutation")
	}
	vw1 := verifier("dw")
	vo0 := verifier("do")
	qs1 := verifier("q")

	// Prepare the upper dirs; entry membership of the dirs
	// themselves lives in the parent's verifier, not their own.
	for _, dn := range []string{"d", "dw", "do"} {
		if err := w.Mkdir(dn, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Refresh(); err != nil {
		t.Fatal(err)
	}
	vo1 := verifier("do")

	// Entry membership alone.
	if err := w.PublishFile("d/c", strings.NewReader("c"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	// Whiteout alone.
	if err := w.Whiteout("dw/x"); err != nil {
		t.Fatal(err)
	}
	// Opaque alone.
	if err := w.Opaque("do"); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(); err != nil {
		t.Fatal(err)
	}

	// The old snapshot is immutable: same rows, resumable position.
	if s1.Len() != 2 || s1.At(0).Name != "a" || s1.At(1).Name != "b" {
		t.Fatalf("old snapshot disturbed: %+v", s1.Entries())
	}
	if i := s1.Seek("a"); i != 1 {
		t.Fatalf("seek after a = %d", i)
	}

	if v := verifier("d"); v == s1.Verifier() {
		t.Fatal("verifier blind to entry membership")
	}
	if v := verifier("dw"); v == vw1 {
		t.Fatal("verifier blind to whiteouts")
	}
	if v := verifier("do"); v == vo1 {
		t.Fatal("verifier blind to opaque")
	}
	if vo1 != vo0 {
		t.Fatal("shadowing mkdir alone moved the dir's own verifier")
	}
	if v := verifier("q"); v != qs1 {
		t.Fatal("unrelated directory's verifier moved")
	}

	s2, err := m.OpenDir(mustLookup(t, m, rootN, "d"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for i := 0; i < s2.Len(); i++ {
		names = append(names, s2.At(i).Name)
	}
	if fmt.Sprint(names) != fmt.Sprint([]string{"a", "b", "c"}) {
		t.Fatalf("new snapshot wrong: %v", names)
	}

	// Same-name replacement: constant membership, new inode — the
	// entry-identity input the verifier clause names.
	vd2 := verifier("d")
	if err := w.PublishFile("d/c", strings.NewReader("c2"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(); err != nil {
		t.Fatal(err)
	}
	if v := verifier("d"); v == vd2 {
		t.Fatal("verifier blind to same-name entry replacement")
	}
}

// TestPinSurvivesUnlink pins the identity envelope's recycling
// guard: a live node's upper inode stays open — content reachable,
// identity stable — even after the path is unlinked behind the
// kernel and the index refreshed.
func TestPinSurvivesUnlink(t *testing.T) {
	view := mustView(t, lfile("a"))
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	if err := w.PublishFile("pinme", strings.NewReader("alive"), 0o644, upTime, nil); err != nil {
		t.Fatal(err)
	}
	m := mustMerged(t, inner, root)
	n := mustLookup(t, m, m.Root(), "pinme")
	defer n.Close()
	if n.Pin() == nil {
		t.Fatal("upper-backed node not pinned")
	}
	if err := os.Remove(filepath.Join(root, "pinme")); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Lookup(m.Root(), "pinme"); ok {
		t.Fatal("unlinked path still presented")
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(n.Pin().Fd()), &st); err != nil {
		t.Fatalf("pinned inode gone: %v", err)
	}
	if upperIDBase|ID(st.Ino) != n.ID() {
		t.Fatalf("pinned ino %d does not back ID %d", st.Ino, n.ID())
	}
	f, err := os.Open(fmt.Sprintf("/proc/self/fd/%d", n.Pin().Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil || string(b) != "alive" {
		t.Fatalf("pinned content unreadable: %q %v", b, err)
	}
}

// TestMergedRootRecord pins the root presentation rule: base root
// attributes until the record exists, the record's afterwards, ID 2
// throughout (writable.md REQ-writable-presented).
func TestMergedRootRecord(t *testing.T) {
	view := mustView(t, lfile("a"))
	inner := mustNew(t, view, nil, capsFull)
	root, w := newUpperFor(t)
	m := mustMerged(t, inner, root)

	r0 := m.Root()
	if r0.ID() != RootID || r0.UpperBacked() {
		t.Fatalf("unrecorded root wrong: %d %v", r0.ID(), r0.UpperBacked())
	}

	if err := w.RecordRoot(3, 4); err != nil {
		t.Fatal(err)
	}
	if err := w.SetRootMode(0o701); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(); err != nil {
		t.Fatal(err)
	}
	r1 := m.Root()
	if r1.ID() != RootID || !r1.UpperBacked() {
		t.Fatalf("recorded root wrong: %d %v", r1.ID(), r1.UpperBacked())
	}
	h := r1.Header()
	if h.Mode != 0o701 || h.Uid != 3 || h.Gid != 4 {
		t.Fatalf("recorded root attrs wrong: %+v", h)
	}
	// Resolution beneath the recorded root still works.
	a, ok, err := m.Lookup(r1, "a")
	if err != nil || !ok {
		t.Fatalf("lookup under recorded root: %v %v", ok, err)
	}
	a.Close()
}
