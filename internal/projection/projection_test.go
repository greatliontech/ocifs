package projection

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// fakeDigest derives a syntactically valid content key from a path;
// the kernel treats digests as opaque names.
func fakeDigest(p string) v1.Hash {
	sum := sha256.Sum256([]byte(p))
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

func lfile(name string) layer.Entry {
	return layer.Entry{
		Header: tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: 42},
		Digest: fakeDigest(name),
	}
}

func ldir(name string) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755}}
}

func lsymlink(name, target string) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777}}
}

func lkind(name string, flag byte) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: flag, Mode: 0o644}}
}

func mustView(t testing.TB, entries ...layer.Entry) *layer.View {
	t.Helper()
	v, err := layer.Unify([]layer.Layer{layer.Layer(entries)})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// capsFull presents every kind under byte order: the FUSE/FSKit
// shape.
var capsFull = Capabilities{Symlinks: true, FIFOs: true, Devices: true}

// foldCompare is a case-insensitive comparator: the test stand-in
// for a platform comparator like PrjFileNameCompare.
func foldCompare(a, b string) int {
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

// capsFolded is the ProjFS-shaped envelope: case-insensitive
// namespace, no symlinks/FIFOs/devices.
var capsFolded = Capabilities{Compare: foldCompare}

func mustNew(t testing.TB, view *layer.View, extras []string, caps Capabilities) *Projection {
	t.Helper()
	p, err := New(view, extras, caps)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTreeShapeAndLookup(t *testing.T) {
	view := mustView(t,
		ldir("etc"),
		lfile("etc/hosts"),
		lfile("bin"),
		lsymlink("link", "etc/hosts"),
		lkind("fifo", tar.TypeFifo),
		lkind("dev-null", tar.TypeChar),
	)
	p := mustNew(t, view, nil, capsFull)

	if p.Root().ID() != RootID {
		t.Fatalf("root ID = %d, want %d", p.Root().ID(), RootID)
	}
	if len(p.Report().Entries) != 0 {
		t.Fatalf("full-capability projection reported %v", p.Report().Entries)
	}

	etc, ok := p.Lookup(p.Root(), "etc")
	if !ok || etc.Kind() != KindDir {
		t.Fatal("etc not presented as a directory")
	}
	hosts, ok := p.Lookup(etc, "hosts")
	if !ok || hosts.Kind() != KindFile {
		t.Fatal("etc/hosts not presented")
	}
	if hosts.Path() != "etc/hosts" || hosts.Parent() != etc {
		t.Fatalf("hosts path %q parent %v", hosts.Path(), hosts.Parent())
	}
	if hosts.ContentDigest() != fakeDigest("etc/hosts") {
		t.Fatal("content digest not carried")
	}
	link, ok := p.Lookup(p.Root(), "link")
	if !ok || link.Kind() != KindSymlink || link.LinkTarget() != "etc/hosts" {
		t.Fatal("symlink not presented verbatim")
	}

	// Children sorted by the byte comparator.
	root := p.Root()
	for i := 1; i < root.Len(); i++ {
		if strings.Compare(root.At(i-1).Name(), root.At(i).Name()) >= 0 {
			t.Fatalf("root children unsorted: %q before %q", root.At(i-1).Name(), root.At(i).Name())
		}
	}
}

func TestIdentityAssignment(t *testing.T) {
	view := mustView(t,
		lfile("a"),
		ldir("d"),
		lfile("d/x"),
	)
	p := mustNew(t, view, nil, capsFull)

	// IDs follow unified-view order from 16 upward, over the view
	// alone (REQ-proj-identity). The view is path-sorted; "." (if
	// present) projects onto the fixed root identity.
	next := viewIDBase
	for _, ve := range view.Entries() {
		name := ve.Header.Name
		if name == "." {
			continue
		}
		e, ok := p.Lookup(p.Root(), name)
		if strings.Contains(name, "/") {
			dir, base := filepath.Split(name)
			parent, pok := p.Lookup(p.Root(), strings.TrimSuffix(dir, "/"))
			if !pok {
				t.Fatalf("parent of %q missing", name)
			}
			e, ok = p.Lookup(parent, base)
		}
		if !ok {
			t.Fatalf("view entry %q not presented", name)
		}
		if e.ID() != next {
			t.Fatalf("ID(%q) = %d, want %d", name, e.ID(), next)
		}
		got, byOK := p.ByID(e.ID())
		if !byOK || got != e {
			t.Fatalf("ByID(%d) did not round-trip", e.ID())
		}
		next++
	}

	if r, ok := p.ByID(RootID); !ok || r != p.Root() {
		t.Fatal("ByID(RootID) did not return the root")
	}
}

func TestRootRecordProjectsOntoRoot(t *testing.T) {
	view := mustView(t,
		layer.Entry{Header: tar.Header{Name: ".", Typeflag: tar.TypeDir, Mode: 0o700, Uid: 7}},
		lfile("f"),
	)
	p := mustNew(t, view, nil, capsFull)
	if h := p.Root().Header(); h.Uid != 7 || h.Mode != 0o700 {
		t.Fatalf("root header not projected from the view record: %+v", h)
	}
	// The root keeps the fixed identity; the first child still draws
	// the first view ID after the root record.
	f, ok := p.Lookup(p.Root(), "f")
	if !ok {
		t.Fatal("f missing")
	}
	if f.ID() != viewIDBase {
		t.Fatalf("first child ID = %d, want %d", f.ID(), viewIDBase)
	}
}

func TestKindOmissionsReported(t *testing.T) {
	view := mustView(t,
		lfile("keep"),
		lsymlink("sym", "keep"),
		lkind("pipe", tar.TypeFifo),
		lkind("cdev", tar.TypeChar),
		lkind("bdev", tar.TypeBlock),
	)
	p := mustNew(t, view, nil, capsFolded)

	for _, name := range []string{"sym", "pipe", "cdev", "bdev"} {
		if _, ok := p.Lookup(p.Root(), name); ok {
			t.Fatalf("%q presented outside the declared envelope", name)
		}
	}
	if _, ok := p.Lookup(p.Root(), "keep"); !ok {
		t.Fatal("keep omitted")
	}

	want := map[string]Reason{
		"sym":  ReasonSymlinkUnsupported,
		"pipe": ReasonFIFOUnsupported,
		"cdev": ReasonDeviceUnsupported,
		"bdev": ReasonDeviceUnsupported,
	}
	got := map[string]Reason{}
	for _, re := range p.Report().Entries {
		if re.Disposition != DispositionOmitted {
			t.Fatalf("unexpected disposition %q", re.Disposition)
		}
		got[re.Path] = re.Reason
	}
	if len(got) != len(want) {
		t.Fatalf("report = %v, want reasons for %v", p.Report().Entries, want)
	}
	for path, reason := range want {
		if got[path] != reason {
			t.Fatalf("report[%q] = %q, want %q", path, got[path], reason)
		}
	}
}

func TestCaseCollisionFirstInViewOrderWins(t *testing.T) {
	view := mustView(t,
		lfile("README"),
		lfile("readme"),
	)
	p := mustNew(t, view, nil, capsFolded)

	// View order is path order: "README" precedes "readme".
	e, ok := p.Lookup(p.Root(), "ReAdMe")
	if !ok {
		t.Fatal("no entry for folded lookup")
	}
	if e.Name() != "README" {
		t.Fatalf("winner = %q, want the first entry in unified-view order", e.Name())
	}
	if p.Root().Len() != 1 {
		t.Fatalf("%d entries presented for one folded name", p.Root().Len())
	}
	if len(p.Report().Entries) != 1 {
		t.Fatalf("report = %v, want exactly the loser", p.Report().Entries)
	}
	re := p.Report().Entries[0]
	if re.Path != "readme" || re.Reason != ReasonCaseCollision || !strings.Contains(re.Detail, "README") {
		t.Fatalf("loser record = %+v", re)
	}
}

func TestOmittedDirectoryContainment(t *testing.T) {
	view := mustView(t,
		ldir("Data"),
		lfile("Data/keep"),
		ldir("data"),
		lfile("data/lost"),
	)
	p := mustNew(t, view, nil, capsFolded)

	winner, ok := p.Lookup(p.Root(), "DATA")
	if !ok || winner.Name() != "Data" {
		t.Fatal("collision winner wrong")
	}
	if _, ok := p.Lookup(winner, "keep"); !ok {
		t.Fatal("winner's child missing")
	}
	if _, ok := p.Lookup(winner, "lost"); ok {
		t.Fatal("loser's child grafted into the winner")
	}

	reasons := map[string]Reason{}
	for _, re := range p.Report().Entries {
		reasons[re.Path] = re.Reason
	}
	if reasons["data"] != ReasonCaseCollision {
		t.Fatalf("loser dir not reported: %v", p.Report().Entries)
	}
	if reasons["data/lost"] != ReasonCaseCollision {
		t.Fatalf("contained child not reported with the inherited reason: %v", p.Report().Entries)
	}
}

func TestExtraDirs(t *testing.T) {
	view := mustView(t, ldir("etc"), lfile("etc/hosts"))
	extras := []string{"proc", "sys/kernel/debug", "etc"}

	p1 := mustNew(t, view, extras, capsFull)
	p2 := mustNew(t, view, extras, capsFull)

	proc, ok := p1.Lookup(p1.Root(), "proc")
	if !ok || proc.Kind() != KindDir || proc.Len() != 0 {
		t.Fatal("proc not an empty directory")
	}
	if proc.ID() < syntheticIDBase {
		t.Fatalf("synthetic ID %d inside the view range", proc.ID())
	}
	sys, _ := p1.Lookup(p1.Root(), "sys")
	kernel, _ := p1.Lookup(sys, "kernel")
	debug, ok := p1.Lookup(kernel, "debug")
	if !ok || debug.Kind() != KindDir {
		t.Fatal("nested extra dir missing")
	}
	// A view directory named as an extra is reused, not duplicated.
	etc, _ := p1.Lookup(p1.Root(), "etc")
	if etc.ID() >= syntheticIDBase {
		t.Fatal("view directory re-created as synthetic")
	}

	// Deterministic across builds (REQ-proj-identity: same
	// configuration, same IDs).
	for _, path := range []string{"proc", "sys"} {
		a, _ := p1.Lookup(p1.Root(), path)
		b, _ := p2.Lookup(p2.Root(), path)
		if a.ID() != b.ID() {
			t.Fatalf("extra dir %q ID differs across builds: %d vs %d", path, a.ID(), b.ID())
		}
	}
}

func TestExtraDirConflictIsAnError(t *testing.T) {
	view := mustView(t, ldir("etc"), lfile("etc/hosts"))
	if _, err := New(view, []string{"etc/hosts"}, capsFull); err == nil {
		t.Fatal("extra directory over a view file accepted")
	}
}

func TestSeekResumesAfterName(t *testing.T) {
	view := mustView(t, lfile("a"), lfile("b"), lfile("c"), lfile("d"))
	p := mustNew(t, view, nil, capsFull)
	root := p.Root()

	i := p.Seek(root, "b")
	if i != 2 || root.At(i).Name() != "c" {
		t.Fatalf("Seek after b = %d (%q), want index of c", i, root.At(i).Name())
	}
	if got := p.Seek(root, "d"); got != root.Len() {
		t.Fatalf("Seek after last = %d, want %d", got, root.Len())
	}
	if got := p.Seek(root, "0"); got != 0 {
		t.Fatalf("Seek before first = %d, want 0", got)
	}
}

func TestReportRoundtrip(t *testing.T) {
	dir := scratchtest.Dir(t, "projection")
	path := filepath.Join(dir, ReportFileName)

	empty := &Report{}
	if err := empty.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	got, err := ReadReportFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Entries == nil || len(got.Entries) != 0 {
		t.Fatalf("empty report round-trip = %+v, want present empty entries", got)
	}

	r := &Report{}
	r.add("a/b", ReasonCaseCollision, "collides with a/B")
	if err := r.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	got, err = ReadReportFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 1 || got.Entries[0] != r.Entries[0] {
		t.Fatalf("report round-trip = %+v", got.Entries)
	}
}

func TestHardlinkPresentsAsFile(t *testing.T) {
	view := mustView(t,
		lfile("target"),
		layer.Entry{Header: tar.Header{Name: "link", Typeflag: tar.TypeLink, Linkname: "target", Mode: 0o644}},
	)
	p := mustNew(t, view, nil, capsFull)
	l, ok := p.Lookup(p.Root(), "link")
	if !ok || l.Kind() != KindFile {
		t.Fatal("hardlink not presented as an independent file node")
	}
	if l.LinkTarget() != "" {
		t.Fatalf("hardlink-derived file leaks a link target %q", l.LinkTarget())
	}
	if h := l.Header(); h.Typeflag != tar.TypeReg {
		t.Fatalf("hardlink-derived file leaks typeflag %q; backends reading the raw header would misclassify", h.Typeflag)
	}
	if l.ContentDigest() != fakeDigest("target") {
		t.Fatal("hardlink content digest not the resolved target's")
	}
}

func TestUnknownKindReported(t *testing.T) {
	view := mustView(t,
		lfile("keep"),
		lkind("sparse", 'S'),
	)
	p := mustNew(t, view, nil, capsFull)
	if _, ok := p.Lookup(p.Root(), "sparse"); ok {
		t.Fatal("unknown tar type presented")
	}
	rep := p.Report()
	if len(rep.Entries) != 1 || rep.Entries[0].Path != "sparse" || rep.Entries[0].Reason != ReasonKindUnknown {
		t.Fatalf("unknown kind not reported: %v", rep.Entries)
	}
}

func TestDeviceNumbersZeroed(t *testing.T) {
	view := mustView(t,
		layer.Entry{Header: tar.Header{Name: "null", Typeflag: tar.TypeChar, Mode: 0o666, Devmajor: 1, Devminor: 3}},
		layer.Entry{Header: tar.Header{Name: "sda", Typeflag: tar.TypeBlock, Mode: 0o660, Devmajor: 8, Devminor: 1}},
	)
	p := mustNew(t, view, nil, capsFull)
	for name, kind := range map[string]Kind{"null": KindCharDevice, "sda": KindBlockDevice} {
		dev, ok := p.Lookup(p.Root(), name)
		if !ok || dev.Kind() != kind {
			t.Fatalf("%s not presented as a typed node", name)
		}
		if h := dev.Header(); h.Devmajor != 0 || h.Devminor != 0 {
			t.Fatalf("%s device numbers leaked: %d,%d (every envelope presents devices without them)", name, h.Devmajor, h.Devminor)
		}
	}
}

func TestExtraDirEscapeRejected(t *testing.T) {
	view := mustView(t, lfile("f"))
	for _, bad := range []string{"../boot", "..", "a/../../b", "/abs"} {
		if _, err := New(view, []string{bad}, capsFull); err == nil {
			t.Fatalf("escaping extra directory %q accepted", bad)
		}
	}
}

func TestExtraDirConflictWrapsErrNotDir(t *testing.T) {
	view := mustView(t, ldir("etc"), lfile("etc/hosts"))
	_, err := New(view, []string{"etc/hosts"}, capsFull)
	if err == nil {
		t.Fatal("conflict accepted")
	}
	var e Errno
	if !errorsAs(err, &e) || e != ErrNotDir {
		t.Fatalf("conflict error not classified as ErrNotDir: %v", err)
	}
}

func TestHeaderCopyIsolated(t *testing.T) {
	view := mustView(t, layer.Entry{Header: tar.Header{
		Name: "f", Typeflag: tar.TypeReg, Mode: 0o644,
		PAXRecords: map[string]string{"k": "v"},
	}, Digest: fakeDigest("f")})
	p := mustNew(t, view, nil, capsFull)
	f, _ := p.Lookup(p.Root(), "f")
	h := f.Header()
	h.PAXRecords["k"] = "tampered"
	if f.Header().PAXRecords["k"] != "v" {
		t.Fatal("Header copy aliases kernel state")
	}
}

func TestReportCopyIsolated(t *testing.T) {
	view := mustView(t, lsymlink("s", "t"))
	p := mustNew(t, view, nil, capsFolded)
	r := p.Report()
	if len(r.Entries) != 1 {
		t.Fatal("expected one omission")
	}
	r.Entries[0].Path = "tampered"
	r.Entries = append(r.Entries, ReportEntry{Path: "junk"})
	fresh := p.Report()
	if len(fresh.Entries) != 1 || fresh.Entries[0].Path != "s" {
		t.Fatalf("Report copy aliases kernel state: %v", fresh.Entries)
	}
}

func errorsAs(err error, target *Errno) bool { return errors.As(err, target) }

// TestExtraDirConflictIndependentOfCapabilities: conflicts are judged
// against the view (api.md REQ-api-extra-dirs), so the same
// configuration fails identically whether or not the backend's
// envelope presents the conflicting entry.
func TestExtraDirConflictIndependentOfCapabilities(t *testing.T) {
	view := mustView(t, lsymlink("a", "t"))
	for name, caps := range map[string]Capabilities{"full": capsFull, "folded": capsFolded} {
		if _, err := New(view, []string{"a"}, caps); err == nil {
			t.Fatalf("caps=%s: extra directory over a view symlink accepted", name)
		}
	}
	// Deeper anchor through the conflicting component fails too.
	if _, err := New(view, []string{"a/deeper"}, capsFolded); err == nil {
		t.Fatal("extra directory descending through a view non-directory accepted")
	}

	// Fold-spelling conflicts are judged the same way: under a
	// folding comparator, an extra whose spelling fold-matches a view
	// non-directory fails whether or not the envelope presents it.
	foldedView := mustView(t, lsymlink("A", "t"))
	for name, caps := range map[string]Capabilities{
		"folded+symlinks": {Compare: foldCompare, Symlinks: true},
		"folded-symlinks": {Compare: foldCompare},
	} {
		if _, err := New(foldedView, []string{"a"}, caps); err == nil {
			t.Fatalf("caps=%s: extra %q accepted over fold-matching view symlink %q", name, "a", "A")
		}
	}
	// Under the byte comparator the spellings differ, so no conflict.
	if _, err := New(foldedView, []string{"a"}, capsFull); err != nil {
		t.Fatalf("byte comparator: distinct spelling rejected: %v", err)
	}
}
