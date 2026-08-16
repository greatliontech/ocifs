//go:build linux

package projection

import (
	"errors"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/upper"
)

// TestWriteRenameArms pins REQ-writable-rename: POSIX rename on the
// merged tree across the three source classes, destination
// replacement earning its whiteout, and EXDEV for directories with
// base-visible content.
func TestWriteRenameArms(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	// Base-only source: copy-up at destination, marker at source.
	if err := m.Rename(d, "f", rootN, "moved"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Lookup(d, "f"); ok {
		t.Fatal("renamed source still presented")
	}
	mv := mustLookup(t, m, rootN, "moved")
	b, err := os.ReadFile(mv.HostPath())
	if err != nil || string(b) != "base-df" {
		t.Fatalf("moved content: %q %v", b, err)
	}
	mv.Close()
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["d/f"] {
		t.Fatal("no marker for base-only rename source")
	}

	// Shadowing source: destination-first via link, then marker plus
	// removal.
	g := mustLookup(t, m, d, "g")
	gg, f, err := m.OpenWrite(g)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	g.Close()
	gg.Close()
	if err := m.Rename(d, "g", rootN, "gmoved"); err != nil {
		t.Fatal(err)
	}
	st, _ = upper.Walk(root)
	if !st.Whiteouts["d/g"] {
		t.Fatal("no marker for shadowing rename source")
	}
	if _, ok := st.Entries["d/g"]; ok {
		t.Fatal("shadowing source entry survived")
	}
	if _, ok := st.Entries["gmoved"]; !ok {
		t.Fatal("destination missing")
	}

	// Replacing a base-visible destination: the old destination's
	// marker lands.
	n, f2, err := m.Create(rootN, "src1", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f2.WriteString("v1")
	f2.Close()
	n.Close()
	sub := mustLookup(t, m, d, "sub")
	defer sub.Close()
	if err := m.Rename(rootN, "src1", sub, "h"); err != nil {
		t.Fatal(err)
	}
	st, _ = upper.Walk(root)
	if !st.Whiteouts["d/sub/h"] {
		t.Fatal("replaced base-visible destination has no marker")
	}
	h := mustLookup(t, m, sub, "h")
	b, _ = os.ReadFile(h.HostPath())
	if string(b) != "v1" {
		t.Fatalf("replaced destination content: %q", b)
	}
	h.Close()

	// Directory renames: base-visible refuses EXDEV; upper-born and
	// recreated-over-whiteout rename natively.
	if err := m.Rename(rootN, "d", rootN, "dmoved"); !errors.Is(err, ErrCrossDevice) {
		t.Fatalf("base-visible dir rename: %v", err)
	}
	nd, err := m.Mkdir(rootN, "born", 0o750)
	if err != nil {
		t.Fatal(err)
	}
	if _, fc, err := m.Create(nd, "inner", 0o600); err != nil {
		t.Fatal(err)
	} else {
		fc.Close()
	}
	nd.Close()
	if err := m.Rename(rootN, "born", rootN, "bornmoved"); err != nil {
		t.Fatalf("upper-born dir rename: %v", err)
	}
	bm := mustLookup(t, m, rootN, "bornmoved")
	inner2, ok, err := m.Lookup(bm, "inner")
	if err != nil || !ok {
		t.Fatalf("moved dir child lost: %v %v", ok, err)
	}
	inner2.Close()
	bm.Close()

	// Into own subtree refused; self-rename is a no-op.
	bm = mustLookup(t, m, rootN, "bornmoved")
	if err := m.Rename(rootN, "bornmoved", bm, "loop"); err == nil {
		t.Fatal("rename into own subtree accepted")
	}
	bm.Close()
	if err := m.Rename(rootN, "bornmoved", rootN, "bornmoved"); err != nil {
		t.Fatalf("self rename: %v", err)
	}
}

// TestWriteLinkArms pins REQ-writable-hardlink: a real hardlink with
// one upper inode, one set of attributes, and the one-time identity
// migration of a base-visible target.
func TestWriteLinkArms(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	target := mustLookup(t, m, d, "f")
	oldID := target.ID()
	ln, err := m.Link(target, rootN, "hardlink")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	target.Close()

	// The target migrated to the shared upper-born identity.
	nt := mustLookup(t, m, d, "f")
	defer nt.Close()
	if nt.ID() == oldID {
		t.Fatal("link target kept its view identity")
	}
	if nt.ID() != ln.ID() {
		t.Fatalf("link group split: %d vs %d", nt.ID(), ln.ID())
	}
	if nt.ID() < upperIDBase || nt.ID() >= syntheticIDBase {
		t.Fatalf("migrated identity outside upper-born partition: %d", nt.ID())
	}
	if nt.Nlink() != 2 || ln.Nlink() != 2 {
		t.Fatalf("nlink %d/%d", nt.Nlink(), ln.Nlink())
	}
	// Content flows through one inode.
	b, err := os.ReadFile(ln.HostPath())
	if err != nil || string(b) != "base-df" {
		t.Fatalf("link content: %q %v", b, err)
	}

	// Directories refuse; existing names refuse.
	if _, err := m.Link(d, rootN, "dlink"); !errors.Is(err, ErrNotDir) {
		t.Fatalf("dir link: %v", err)
	}
	if _, err := m.Link(nt, rootN, "hardlink"); !errors.Is(err, ErrExists) {
		t.Fatalf("existing name: %v", err)
	}
}

// TestWriteMknodArms pins the node-creation surface: native FIFOs
// and sockets, device stand-ins presenting recorded numbers.
func TestWriteMknodArms(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()

	fifo, err := m.Mknod(rootN, "pipe", KindFIFO, 0o640, upper.Rdev{})
	if err != nil {
		t.Fatal(err)
	}
	if fifo.Kind() != KindFIFO || fifo.Header().Mode != 0o640 {
		t.Fatalf("fifo wrong: %v %o", fifo.Kind(), fifo.Header().Mode)
	}
	fifo.Close()
	sock, err := m.Mknod(rootN, "sock", KindSocket, 0o755, upper.Rdev{})
	if err != nil {
		t.Fatal(err)
	}
	if sock.Kind() != KindSocket {
		t.Fatalf("socket kind: %v", sock.Kind())
	}
	sock.Close()
	dev, err := m.Mknod(rootN, "null", KindCharDevice, 0o666, upper.Rdev{Major: 1, Minor: 3})
	if err != nil {
		t.Fatal(err)
	}
	h := dev.Header()
	if dev.Kind() != KindCharDevice || h.Devmajor != 1 || h.Devminor != 3 {
		t.Fatalf("device wrong: %v %d:%d", dev.Kind(), h.Devmajor, h.Devminor)
	}
	dev.Close()
}

// TestWriteXattrArms pins the extended-attribute surface: native
// stores, escape on host refusal, caller EINVAL surfacing, the
// unforgeable machinery namespace, stand-in conversion, and removal
// of native and escaped attributes.
func TestWriteXattrArms(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: escape arms need refused stores")
	}
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()

	// Native store on a base file (copy-up implied).
	fn := mustLookup(t, m, d, "f")
	if err := m.SetXattr(fn, "user.k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	fn.Close()
	fn = mustLookup(t, m, d, "f")
	if fn.Xattrs()["user.k"] != "v" {
		t.Fatalf("native xattr lost: %v", fn.Xattrs())
	}

	// Host-refused class records the escape and presents under the
	// real name.
	if err := m.SetXattr(fn, "trusted.overlay.opaque", []byte("y"), 0); err != nil {
		t.Fatal(err)
	}
	fn.Close()
	fn = mustLookup(t, m, d, "f")
	if fn.Xattrs()["trusted.overlay.opaque"] != "y" {
		t.Fatalf("escaped xattr lost: %v", fn.Xattrs())
	}

	// Caller EINVAL surfaces — never escaped (security.capability
	// with a garbage value fails kernel validation).
	if err := m.SetXattr(fn, "security.capability", []byte("garbage"), 0); err == nil {
		t.Fatal("invalid capability value accepted")
	} else if errors.Is(err, ErrReserved) {
		t.Fatalf("wrong class: %v", err)
	}

	// The machinery namespace is unforgeable and invisible.
	if err := m.SetXattr(fn, upper.XattrOwner, []byte("0:0"), 0); !errors.Is(err, ErrReserved) {
		t.Fatalf("machinery store: %v", err)
	}
	for k := range fn.Xattrs() {
		if strings.HasPrefix(k, upper.XattrNS) {
			t.Fatalf("machinery visible: %s", k)
		}
	}

	// Removal: native and escaped; absent names refuse.
	if err := m.RemoveXattr(fn, "user.k"); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveXattr(fn, "trusted.overlay.opaque"); err != nil {
		t.Fatal(err)
	}
	fn.Close()
	fn = mustLookup(t, m, d, "f")
	defer fn.Close()
	if len(fn.Xattrs()) != 0 {
		t.Fatalf("xattrs survived removal: %v", fn.Xattrs())
	}
	if err := m.RemoveXattr(fn, "user.gone"); !errors.Is(err, ErrNoAttr) {
		t.Fatalf("absent removal: %v", err)
	}

	// A native symlink converts to a stand-in to carry its record.
	sl := mustLookup(t, m, d, "sl")
	if err := m.SetXattr(sl, "user.tag", []byte("t"), 0); err != nil {
		t.Fatal(err)
	}
	sl.Close()
	sl = mustLookup(t, m, d, "sl")
	defer sl.Close()
	if sl.Kind() != KindSymlink || sl.LinkTarget() != "f" {
		t.Fatalf("symlink truth lost in conversion: %v %q", sl.Kind(), sl.LinkTarget())
	}
	if sl.Xattrs()["user.tag"] != "t" {
		t.Fatalf("symlink xattr lost: %v", sl.Xattrs())
	}
}


// TestWriteRenameOntoDirAndAliases pins the replacing-directory
// compound (marker, dismantle, swap-beside-marker), the
// two-names-one-inode no-op, and survivor link counts after alias
// removal.
func TestWriteRenameOntoDirAndAliases(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()

	// A dir with content renames onto an empty base-visible dir:
	// marker lands, content follows.
	src, err := m.Mkdir(rootN, "srcdir", 0o750)
	if err != nil {
		t.Fatal(err)
	}
	if _, f, err := m.Create(src, "payload", 0o600); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}
	src.Close()
	if err := m.Rename(rootN, "srcdir", rootN, "emptyd"); err != nil {
		t.Fatalf("rename onto empty base dir: %v", err)
	}
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["emptyd"] {
		t.Fatal("replaced dir destination has no marker")
	}
	moved := mustLookup(t, m, rootN, "emptyd")
	pay, ok, err := m.Lookup(moved, "payload")
	if err != nil || !ok {
		t.Fatalf("moved payload lost: %v %v", ok, err)
	}
	pay.Close()
	moved.Close()

	// Two names of one inode: rename no-ops, both names stay.
	d := mustLookup(t, m, rootN, "d")
	defer d.Close()
	tgt := mustLookup(t, m, d, "f")
	ln, err := m.Link(tgt, rootN, "alias")
	if err != nil {
		t.Fatal(err)
	}
	tgt.Close()
	ln.Close()
	af := mustLookup(t, m, d, "f")
	if err := m.Rename(d, "f", rootN, "alias"); err != nil {
		t.Fatalf("same-inode rename: %v", err)
	}
	af.Close()
	if _, ok, _ := m.Lookup(d, "f"); !ok {
		t.Fatal("same-inode rename removed the source")
	}
	if _, ok, _ := m.Lookup(rootN, "alias"); !ok {
		t.Fatal("same-inode rename removed the destination")
	}

	// Removing one alias updates the survivor's link count.
	if err := m.Unlink(rootN, "alias"); err != nil {
		t.Fatal(err)
	}
	sf := mustLookup(t, m, d, "f")
	defer sf.Close()
	if sf.Nlink() != 1 {
		t.Fatalf("survivor nlink %d, want 1", sf.Nlink())
	}
}

// TestWriteXattrFlagsAndSocketMode pins XATTR_CREATE/REPLACE
// semantics and mknod'd socket mode fidelity under umask.
func TestWriteXattrFlagsAndSocketMode(t *testing.T) {
	inner, cas := baseFixture(t)
	root, _ := newUpperFor(t)
	m := mustWritable(t, inner, root, cas)
	rootN := m.Root()

	n, f, err := m.Create(rootN, "xf", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer n.Close()
	if err := m.SetXattr(n, "user.a", []byte("1"), unix.XATTR_REPLACE); !errors.Is(err, ErrNoAttr) {
		t.Fatalf("REPLACE on absent: %v", err)
	}
	if err := m.SetXattr(n, "user.a", []byte("1"), unix.XATTR_CREATE); err != nil {
		t.Fatal(err)
	}
	if err := m.SetXattr(n, "user.a", []byte("2"), unix.XATTR_CREATE); !errors.Is(err, ErrExists) {
		t.Fatalf("CREATE on present: %v", err)
	}
	if err := m.SetXattr(n, "user.a", []byte("2"), unix.XATTR_REPLACE); err != nil {
		t.Fatal(err)
	}
	nn := mustLookupPath(t, m, "xf")
	if nn.Xattrs()["user.a"] != "2" {
		t.Fatalf("flagged stores wrong: %v", nn.Xattrs())
	}
	nn.Close()

	// Socket mode survives the daemon umask (published via temp).
	oldMask := unix.Umask(0o077)
	sk, err := m.Mknod(rootN, "sock77", KindSocket, 0o775, upper.Rdev{})
	unix.Umask(oldMask)
	if err != nil {
		t.Fatal(err)
	}
	defer sk.Close()
	if sk.Header().Mode != 0o775 {
		t.Fatalf("socket mode %o under umask, want 775", sk.Header().Mode)
	}
}

func mustLookupPath(t testing.TB, m *Merged, p string) *Node {
	t.Helper()
	n, err := m.NodeAt(p)
	if err != nil {
		t.Fatalf("nodeAt %q: %v", p, err)
	}
	return n
}
