//go:build linux

package projection

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/upper"
)

// contentStore is the test CAS: digests resolve to fixed bytes.
type contentStore map[string]string

func (c contentStore) open(h v1.Hash) (io.ReadCloser, error) {
	s, ok := c[h.Hex]
	if !ok {
		return nil, errors.New("missing blob")
	}
	return io.NopCloser(strings.NewReader(s)), nil
}

func mustWritable(t testing.TB, inner *Projection, root string, cas contentStore) *Merged {
	t.Helper()
	m, err := NewMergedWritable(inner, root, cas.open)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func baseLayerAndCAS(t testing.TB) (layer.Layer, contentStore) {
	t.Helper()
	cas := contentStore{}
	mk := func(name, content string) struct{} {
		cas[fakeDigest(name).Hex] = content
		return struct{}{}
	}
	mk("d/f", "base-df")
	mk("d/g", "base-dg")
	mk("d/sub/h", "base-h")
	mk("top", "base-top")
	return layer.Layer{
		ldir("d"),
		ldir("emptyd"),
		lfile("d/f"),
		lfile("d/g"),
		lfile("d/sub/h"),
		lsymlink("d/sl", "f"),
		lfile("top"),
	}, cas
}

func baseFixture(t testing.TB) (*Projection, contentStore) {
	t.Helper()
	bl, cas := baseLayerAndCAS(t)
	view, err := layer.Unify([]layer.Layer{bl})
	if err != nil {
		t.Fatal(err)
	}
	return mustNew(t, view, nil, capsFull), cas
}

// TestWriteCreateMkdirSymlink pins creation under base directories:
// spine materialization with presented attributes, natural parent
// mtime, restored ancestor times, reserved-name refusal, EEXIST.
func TestWriteCreateMkdirSymlink(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()

	d := mustLookup(t, m, rootN, "d")
	sub := mustLookup(t, m, d, "sub")

	n, f, err := m.Create(sub, "new", 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer n.Close()
	if !n.UpperBacked() || n.Header().Mode != 0o640 {
		t.Fatalf("created node wrong: %+v", n.Header())
	}
	// Spine: d and d/sub materialized with presented attrs; d's time
	// restored (implementation detail), sub's bumped (logical parent).
	de, ok, err := upper.Stat(root, "d")
	if err != nil || !ok || de.Kind != upper.KindDir {
		t.Fatalf("spine d missing: %v %v", ok, err)
	}
	if de.Mode != 0o755 {
		t.Fatalf("spine d mode %o", de.Mode)
	}
	// d is not the logical parent: its presented time stays the
	// base's (zero-time header presents epoch).
	if !de.ModTime.Equal(time.Unix(0, 0)) {
		t.Fatalf("spine d time not restored: %v", de.ModTime)
	}

	if _, _, err := m.Create(sub, "new", 0o600); !errors.Is(err, ErrExists) {
		t.Fatalf("recreate existing: %v", err)
	}
	if _, _, err := m.Create(sub, ".wh.x", 0o600); !errors.Is(err, ErrReserved) {
		t.Fatalf("reserved name: %v", err)
	}

	nd, err := m.Mkdir(d, "newdir", 0o750)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Kind() != KindDir || nd.Header().Mode != 0o750 {
		t.Fatalf("mkdir wrong: %+v", nd.Header())
	}
	nd.Close()
	ns, err := m.Symlink(d, "newlink", "/tmp/t")
	if err != nil {
		t.Fatal(err)
	}
	if ns.Kind() != KindSymlink || ns.LinkTarget() != "/tmp/t" {
		t.Fatalf("symlink wrong: %+v", ns)
	}
	ns.Close()
	sub.Close()
	d.Close()
}

// TestWriteCopyUpPreservesTruth pins REQ-writable-copyup: opening
// for write copies content and recorded attributes (owner as
// override for a foreign uid), and the copied entry presents
// identically minus the mutation.
func TestWriteCopyUpPreservesTruth(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arm needs refused chowns")
	}
	cas := contentStore{}
	cas[fakeDigest("owned").Hex] = "owned-content"
	view := mustView(t,
		lownedFile("owned", 0, 0),
	)
	inner := mustNew(t, view, nil, capsFull)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)

	n := mustLookup(t, m, m.Root(), "owned")
	nn, f, err := m.OpenWrite(n)
	if err != nil {
		t.Fatal(err)
	}
	defer nn.Close()
	if _, err := f.WriteAt([]byte("MOD"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	b, err := os.ReadFile(filepath.Join(root, "owned"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "MODed-content" {
		t.Fatalf("copy-up content wrong: %q", b)
	}
	h := nn.Header()
	if h.Uid != 0 || h.Gid != 0 {
		t.Fatalf("owner not preserved through copy-up: %d:%d", h.Uid, h.Gid)
	}
	if nn.ID() != n.ID() {
		t.Fatalf("shadow-in-place ID moved: %d -> %d", n.ID(), nn.ID())
	}
	n.Close()
}

// TestWriteUnlinkArms pins REQ-writable-delete for files: marker
// for base-visible content (marker precedes removal), plain removal
// for upper-born entries, marker persistence beside recreation.
func TestWriteUnlinkArms(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	// Base-only: marker.
	if err := m.Unlink(d, "f"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Lookup(d, "f"); ok {
		t.Fatal("unlinked base file still presented")
	}
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["d/f"] {
		t.Fatal("no marker for base-visible unlink")
	}

	// Shadow: marker plus removal.
	g := mustLookup(t, m, d, "g")
	gg, f, err := m.OpenWrite(g)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	g.Close()
	gg.Close()
	if err := m.Unlink(d, "g"); err != nil {
		t.Fatal(err)
	}
	st, _ = upper.Walk(root)
	if !st.Whiteouts["d/g"] {
		t.Fatal("no marker for shadow unlink")
	}
	if _, ok := st.Entries["d/g"]; ok {
		t.Fatal("shadow entry survived unlink")
	}

	// Upper-born: removal alone.
	n, f2, err := m.Create(d, "born", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()
	n.Close()
	if err := m.Unlink(d, "born"); err != nil {
		t.Fatal(err)
	}
	st, _ = upper.Walk(root)
	if st.Whiteouts["d/born"] {
		t.Fatal("marker for upper-born unlink")
	}

	// Recreation beside the marker: marker stays after re-unlink.
	rn, f3, err := m.Create(d, "f", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f3.Close()
	rn.Close()
	if err := m.Unlink(d, "f"); err != nil {
		t.Fatal(err)
	}
	st, _ = upper.Walk(root)
	if !st.Whiteouts["d/f"] {
		t.Fatal("marker lost across recreation cycle")
	}
	if _, ok := st.Entries["d/f"]; ok {
		t.Fatal("recreated entry survived unlink")
	}
}

// TestWriteRmdirArms pins REQ-writable-delete for directories:
// merged-empty enforcement, hide-then-dismantle ordering effects
// (marker present, interior markers gone, upper dir gone), and
// recreation semantics.
func TestWriteRmdirArms(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	if err := m.Rmdir(rootN, "d"); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("non-empty rmdir: %v", err)
	}
	// Empty the merged directory (its children are base content and
	// one symlink).
	for _, name := range []string{"f", "g", "sl"} {
		if err := m.Unlink(d, name); err != nil {
			t.Fatalf("unlink %s: %v", name, err)
		}
	}
	sub := mustLookup(t, m, d, "sub")
	if err := m.Unlink(sub, "h"); err != nil {
		t.Fatal(err)
	}
	sub.Close()
	if err := m.Rmdir(d, "sub"); err != nil {
		t.Fatal(err)
	}
	if err := m.Rmdir(rootN, "d"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Lookup(rootN, "d"); ok {
		t.Fatal("rmdir'd dir still presented")
	}
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["d"] {
		t.Fatal("no marker for base-visible rmdir")
	}
	if _, ok := st.Entries["d"]; ok {
		t.Fatal("upper dir survived rmdir")
	}
	for p := range st.Whiteouts {
		if strings.HasPrefix(p, "d/") {
			t.Fatalf("interior marker %q survived dismantling", p)
		}
	}

	// Recreate over the marker: presented empty (base occluded).
	nd, err := m.Mkdir(rootN, "d", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := m.OpenDir(nd)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Len() != 0 {
		t.Fatalf("recreated dir not empty: %+v", snap.Entries())
	}
	nd.Close()
}

// TestWriteSetattrArms pins the attribute path: chmod, chown (with
// override and stand-in conversion), utimens — and the root record
// dance (first root setattr stamps presented attrs, then applies).
func TestWriteSetattrArms(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	// chmod on a base file: copy-up plus mode.
	fnode := mustLookup(t, m, d, "f")
	if err := m.SetMode(fnode, 0o4711); err != nil {
		t.Fatal(err)
	}
	fnode.Close()
	fn := mustLookup(t, m, d, "f")
	if fn.Header().Mode != 0o4711 {
		t.Fatalf("chmod lost: %o", fn.Header().Mode)
	}
	b, err := os.ReadFile(filepath.Join(root, "d", "f"))
	if err != nil || string(b) != "base-df" {
		t.Fatalf("copy-up content wrong: %q %v", b, err)
	}
	fn.Close()

	// chown to a foreign uid: override records; suid clears like a
	// native chown.
	fn = mustLookup(t, m, d, "f")
	if err := m.SetOwner(fn, 0, 0); err != nil {
		t.Fatal(err)
	}
	fn.Close()
	fn = mustLookup(t, m, d, "f")
	h := fn.Header()
	if h.Uid != 0 || h.Gid != 0 {
		t.Fatalf("chown lost: %d:%d", h.Uid, h.Gid)
	}
	if h.Mode&0o6000 != 0 {
		t.Fatalf("suid survived override chown: %o", h.Mode)
	}
	fn.Close()

	// chown on a symlink: stand-in conversion carries it.
	sl := mustLookup(t, m, d, "sl")
	if err := m.SetOwner(sl, 0, 0); err != nil {
		t.Fatal(err)
	}
	sl.Close()
	sl = mustLookup(t, m, d, "sl")
	if sl.Kind() != KindSymlink || sl.LinkTarget() != "f" || sl.Header().Uid != 0 {
		t.Fatalf("symlink conversion wrong: %v %q %d", sl.Kind(), sl.LinkTarget(), sl.Header().Uid)
	}
	sl.Close()

	// utimens.
	when := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	tn := mustLookup(t, m, rootN, "top")
	if err := m.SetTimes(tn, when); err != nil {
		t.Fatal(err)
	}
	tn.Close()
	tn = mustLookup(t, m, rootN, "top")
	if !tn.Header().ModTime.Equal(when) {
		t.Fatalf("utimens lost: %v", tn.Header().ModTime)
	}
	tn.Close()

	// Root: first chmod stamps the record with presented owner and
	// prior attrs, then applies.
	if err := m.SetMode(rootN, 0o700); err != nil {
		t.Fatal(err)
	}
	r := m.Root()
	if !r.UpperBacked() {
		t.Fatal("root record missing after root chmod")
	}
	rh := r.Header()
	if rh.Mode != 0o700 || rh.Uid != 0 || rh.Gid != 0 {
		t.Fatalf("root attrs wrong: %+v", rh)
	}
	// A second root op applies directly.
	if err := m.SetTimes(m.Root(), when); err != nil {
		t.Fatal(err)
	}
	if got := m.Root().Header().ModTime; !got.Equal(when) {
		t.Fatalf("root time wrong: %v", got)
	}
}

// TestWriteTruncate pins the truncate arms: copy-up bounded by the
// new size, extension with zeros, and upper-file truncation.
func TestWriteTruncate(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()

	n := mustLookup(t, m, rootN, "top")
	if err := m.Truncate(n, 4); err != nil {
		t.Fatal(err)
	}
	n.Close()
	b, err := os.ReadFile(filepath.Join(root, "top"))
	if err != nil || string(b) != "base" {
		t.Fatalf("bounded copy-up wrong: %q %v", b, err)
	}
	n = mustLookup(t, m, rootN, "top")
	if n.Header().Size != 4 {
		t.Fatalf("size %d", n.Header().Size)
	}
	if err := m.Truncate(n, 6); err != nil {
		t.Fatal(err)
	}
	n.Close()
	b, _ = os.ReadFile(filepath.Join(root, "top"))
	if !bytes.Equal(b, []byte("base\x00\x00")) {
		t.Fatalf("extension wrong: %q", b)
	}
}

// TestWriteIncrementalIndexMatchesWalk pins REQ-proj-upper-truth
// across the write path: after a burst of operations the
// incrementally maintained index presents exactly what a fresh walk
// does.
func TestWriteIncrementalIndexMatchesWalk(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	if n, f, err := m.Create(d, "n1", 0o600); err != nil {
		t.Fatal(err)
	} else {
		f.WriteString("x")
		f.Close()
		n.Close()
		if err := m.Flushed("d/n1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Unlink(d, "g"); err != nil {
		t.Fatal(err)
	}
	nd, err := m.Mkdir(d, "nd", 0o750)
	if err != nil {
		t.Fatal(err)
	}
	nd.Close()
	fnode := mustLookup(t, m, d, "f")
	if err := m.SetMode(fnode, 0o600); err != nil {
		t.Fatal(err)
	}
	fnode.Close()
	if err := m.SetMode(rootN, 0o701); err != nil {
		t.Fatal(err)
	}

	m2, err := NewMergedWritable(inner, root, cas.open)
	if err != nil {
		t.Fatal(err)
	}
	collect := func(mm *Merged) map[string]string {
		out := map[string]string{}
		var walk func(dir *Node)
		walk = func(dir *Node) {
			snap, err := mm.OpenDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < snap.Len(); i++ {
				row := snap.At(i)
				n, ok, err := mm.Lookup(dir, row.Name)
				if err != nil || !ok {
					t.Fatalf("resolve %q: %v %v", row.Name, ok, err)
				}
				h := n.Header()
				size := h.Size
				if n.Kind() == KindDir {
					size = 0 // host directory sizes are noise
				}
				out[n.Path()] = fmt.Sprintf("%v/%d/%o/%d:%d/%d/%v", n.Kind(), n.ID(), h.Mode, h.Uid, h.Gid, size, n.UpperBacked())
				if n.Kind() == KindDir {
					walk(n)
				}
				n.Close()
			}
		}
		r := mm.Root()
		rh := r.Header()
		out["."] = fmt.Sprintf("%o/%d:%d/%v", rh.Mode, rh.Uid, rh.Gid, r.UpperBacked())
		walk(r)
		return out
	}
	a, b := collect(m), collect(m2)
	if len(a) != len(b) {
		t.Fatalf("tree size diverged: %d vs %d\n%v\n%v", len(a), len(b), a, b)
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("%q diverged:\n incremental %s\n walked      %s", k, v, b[k])
		}
	}
}

// lownedFile is a base file owned by root (0:0) for override arms.
func lownedFile(name string, uid, gid int) layer.Entry {
	e := lfile(name)
	e.Header.Uid, e.Header.Gid = uid, gid
	e.Header.Mode = 0o644
	return e
}

// TestWriteRootAttrPreservation pins the root record dance against a
// distinctive base root: any first root mutation — chmod, chown, or
// utimens — preserves the other presented root attributes instead of
// flipping them to upper-dir creation noise.
func TestWriteRootAttrPreservation(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	rootTime := time.Date(2019, 9, 8, 7, 6, 5, 0, time.UTC)
	build := func() *Merged {
		e := layer.Entry{Header: tarHeaderDir(".", 0o700, rootTime)}
		view, err := layer.Unify([]layer.Layer{{e, lfile("a")}})
		if err != nil {
			t.Fatal(err)
		}
		inner := mustNew(t, view, nil, capsFull)
		root, _ := newUpperFor(t)
		return mustWritable(t, inner, root, contentStore{})
	}

	// First root op is chmod: times survive.
	m := build()
	if err := m.SetMode(m.Root(), 0o750); err != nil {
		t.Fatal(err)
	}
	h := m.Root().Header()
	if h.Mode != 0o750 || !h.ModTime.Equal(rootTime) {
		t.Fatalf("chmod-first root attrs: %o %v", h.Mode, h.ModTime)
	}

	// First root op is utimens: mode survives.
	m = build()
	when := time.Unix(5000, 0)
	if err := m.SetTimes(m.Root(), when); err != nil {
		t.Fatal(err)
	}
	h = m.Root().Header()
	if h.Mode != 0o700 || !h.ModTime.Equal(when) {
		t.Fatalf("utimens-first root attrs: %o %v", h.Mode, h.ModTime)
	}

	// First root op is chown: mode and times both survive.
	m = build()
	if err := m.SetOwner(m.Root(), 0, 0); err != nil {
		t.Fatal(err)
	}
	h = m.Root().Header()
	if h.Mode != 0o700 || !h.ModTime.Equal(rootTime) || h.Uid != 0 {
		t.Fatalf("chown-first root attrs: %o %v %d", h.Mode, h.ModTime, h.Uid)
	}
}

func tarHeaderDir(name string, mode int64, mt time.Time) tar.Header {
	return tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: mode, ModTime: mt}
}
