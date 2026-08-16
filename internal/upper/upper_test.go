//go:build linux

package upper

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

var fixedTime = time.Date(2024, 5, 1, 10, 0, 0, 123456789, time.UTC)

func newUpper(t testing.TB) (string, *Writer) {
	t.Helper()
	dir := scratchtest.Dir(t, "upper")
	root := filepath.Join(dir, "u")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	return root, NewWriter(root)
}

// view compresses a walked state into a comparable presentation:
// the fields the dialect contract pins, host-volatile ones excluded.
type viewEntry struct {
	kind    Kind
	mode    uint32
	uid     int
	gid     int
	target  string
	standIn bool
	xattrs  string
	content string
	size    int64
}

func viewOf(t testing.TB, st *State) map[string]viewEntry {
	t.Helper()
	out := map[string]viewEntry{}
	for p, e := range st.Entries {
		var xs []string
		for k, v := range e.Xattrs {
			xs = append(xs, k+"="+v)
		}
		// Deterministic order for comparison.
		for i := 0; i < len(xs); i++ {
			for j := i + 1; j < len(xs); j++ {
				if xs[j] < xs[i] {
					xs[i], xs[j] = xs[j], xs[i]
				}
			}
		}
		ve := viewEntry{kind: e.Kind, mode: e.Mode, uid: e.UID, gid: e.GID, target: e.Target, standIn: e.StandIn, xattrs: strings.Join(xs, ",")}
		// Size is contract only where the dialect stores content or a
		// target; directory sizes are host-filesystem noise.
		if e.Kind == KindFile || e.Kind == KindSymlink {
			ve.size = e.Size
		}
		if e.Kind == KindFile && !e.StandIn {
			b, err := os.ReadFile(e.HostPath)
			if err != nil {
				t.Fatalf("read %s: %v", e.HostPath, err)
			}
			ve.content = string(b)
		}
		out[p] = ve
	}
	for p := range st.Whiteouts {
		out["wh:"+p] = viewEntry{}
	}
	for p := range st.Opaque {
		out["opq:"+p] = viewEntry{}
	}
	return out
}

func mustWalkView(t testing.TB, root string) map[string]viewEntry {
	t.Helper()
	st, err := Walk(root)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	return viewOf(t, st)
}

func sameView(a, b map[string]viewEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestWalkReadsEveryDialectClass pins REQ-writable-dialect's reader
// arm over a hand-built tree holding every name and record class.
func TestWalkReadsEveryDialectClass(t *testing.T) {
	root, w := newUpper(t)

	if err := w.Mkdir("etc", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("etc/conf", strings.NewReader("data"), 0o4644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Symlink("/bin/sh", "sh"); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkfifo("pipe", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("etc/old"); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("etc"); err != nil {
		t.Fatal(err)
	}
	if err := w.MakeStandIn("null", KindCharDev, "", Rdev{Major: 1, Minor: 3}, 0o666, 0, 0, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.MakeStandIn("rootlink", KindSymlink, "/root/target", Rdev{}, 0o777, 0, 0, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.SetEscapedXattr("etc/conf", "security.capability", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	// A native non-machinery xattr presents as itself.
	if err := unix.Lsetxattr(filepath.Join(root, "etc/conf"), "user.app", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	// An orphaned temporary is inert.
	if err := os.WriteFile(filepath.Join(root, TempPrefix+"orphan"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["etc/old"] || !st.Opaque["etc"] {
		t.Fatalf("markers misread: %+v %+v", st.Whiteouts, st.Opaque)
	}
	conf := st.Entries["etc/conf"]
	if conf.Kind != KindFile || conf.Mode != 0o4644 || !conf.ModTime.Equal(fixedTime) {
		t.Fatalf("conf misread: %+v", conf)
	}
	if conf.Xattrs["security.capability"] != "\x01\x02\x03" || conf.Xattrs["user.app"] != "v" {
		t.Fatalf("conf xattrs misread: %q", conf.Xattrs)
	}
	for name := range conf.Xattrs {
		if strings.HasPrefix(name, XattrNS) {
			t.Fatalf("machinery leaked into presentation: %q", name)
		}
	}
	if sh := st.Entries["sh"]; sh.Kind != KindSymlink || sh.Target != "/bin/sh" || sh.StandIn {
		t.Fatalf("native symlink misread: %+v", sh)
	}
	if p := st.Entries["pipe"]; p.Kind != KindFifo || p.StandIn {
		t.Fatalf("native fifo misread: %+v", p)
	}
	if n := st.Entries["null"]; n.Kind != KindCharDev || !n.StandIn || n.Rdev != (Rdev{Major: 1, Minor: 3}) || n.Mode != 0o666 {
		t.Fatalf("device stand-in misread: %+v", n)
	}
	if rl := st.Entries["rootlink"]; rl.Kind != KindSymlink || !rl.StandIn || rl.Target != "/root/target" || rl.Size != int64(len("/root/target")) {
		t.Fatalf("symlink stand-in misread: %+v", rl)
	}
	if _, ok := st.Entries[TempPrefix+"orphan"]; ok {
		t.Fatal("orphan temporary presented as state")
	}
}

// TestOwnerOverrideAtomicAndPrivilegeReducing pins the override arm
// of REQ-writable-fidelity: a foreign chown records the single owner
// attribute, presents the recorded owner, and clears setuid/setgid
// clear-first.
func TestOwnerOverrideAtomicAndPrivilegeReducing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: the override path needs a refused chown")
	}
	root, w := newUpper(t)
	if err := w.PublishFile("su", strings.NewReader("x"), 0o4755, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.SetOwner("su", 0, 0); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	e := st.Entries["su"]
	if e.UID != 0 || e.GID != 0 {
		t.Fatalf("override owner not presented: %+v", e)
	}
	if e.Mode != 0o755 {
		t.Fatalf("chown did not clear special bits: mode %o", e.Mode)
	}
	// The record is one attribute (atomic).
	val, err := getXattr(filepath.Join(root, "su"), XattrOwner)
	if err != nil || val != "0:0" {
		t.Fatalf("owner record = %q, %v", val, err)
	}
}

// TestConvertToStandInPreservesTruth: an override landing on a
// native symlink converts it, preserving target and mode, and the
// converted node accepts the owner record.
func TestConvertToStandInPreservesTruth(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: conversion needs a refused chown")
	}
	root, w := newUpper(t)
	if err := w.Symlink("../x", "ln"); err != nil {
		t.Fatal(err)
	}
	if err := w.SetOwner("ln", 12345, 12345); !errors.Is(err, ErrNeedsStandIn) {
		t.Fatalf("symlink chown = %v, want ErrNeedsStandIn", err)
	}
	if err := w.ConvertToStandIn("ln", 12345, 12345); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	e := st.Entries["ln"]
	if !e.StandIn || e.Kind != KindSymlink || e.Target != "../x" || e.UID != 12345 {
		t.Fatalf("conversion lost truth: %+v", e)
	}
}

// TestReservedNamesRefused pins REQ-writable-reserved's name arm at
// the primitive seam.
func TestReservedNamesRefused(t *testing.T) {
	_, w := newUpper(t)
	// Real source and parent, so refusal can only come from the name
	// check, never from ENOENT.
	if err := w.PublishFile("a", strings.NewReader("x"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("d", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{".wh.x", "d/.wh.y", OpaqueMarker, TempPrefix + "z", ".wh..wh..tmp.1.2", "/abs", "..", "../out"} {
		for verb, err := range map[string]error{
			"publish":  w.PublishFile(rel, strings.NewReader(""), 0o644, fixedTime, nil),
			"mkdir":    w.Mkdir(rel, 0o755),
			"rename":   w.Rename("a", rel),
			"link":     w.Link("a", rel),
			"whiteout": w.Whiteout(rel),
		} {
			if err == nil {
				t.Fatalf("%s of reserved name %q accepted", verb, rel)
			}
			if !strings.Contains(err.Error(), "reserved") && !strings.Contains(err.Error(), "clean relative") {
				t.Fatalf("%s of %q refused for the wrong reason: %v", verb, rel, err)
			}
		}
	}
	// Reserved SOURCES are refused too: a live marker cannot be
	// renamed or linked out of its role.
	if err := w.Rename(".wh.a", "out"); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("rename of a marker accepted: %v", err)
	}
	if err := w.Link(".wh.a", "out"); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("link of a marker accepted: %v", err)
	}
}

// TestSweepRemovesOnlyTemps: the provider's sweep removes orphaned
// temporaries and nothing else.
func TestSweepRemovesOnlyTemps(t *testing.T) {
	root, w := newUpper(t)
	if err := w.PublishFile("keep", strings.NewReader("k"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("gone"); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(root, TempPrefix+"1.999")
	if err := os.WriteFile(orphan, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Sweep(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(orphan); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("orphan survived sweep: %v", err)
	}
	v := mustWalkView(t, root)
	if _, ok := v["keep"]; !ok {
		t.Fatal("sweep removed real state")
	}
	if _, ok := v["wh:gone"]; !ok {
		t.Fatal("sweep removed a marker")
	}
}

// TestUnknownMachineryFailsLoudly: a record the provider never
// writes is damage, not tolerated input (REQ-writable-crash's
// no-repair stance).
func TestUnknownMachineryFailsLoudly(t *testing.T) {
	root, w := newUpper(t)
	if err := w.PublishFile("f", strings.NewReader("x"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := unix.Lsetxattr(filepath.Join(root, "f"), XattrNS+"future", []byte("?"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := Walk(root); err == nil || !strings.Contains(err.Error(), "unknown machinery") {
		t.Fatalf("unknown machinery record tolerated: %v", err)
	}
}

// TestStripMachineryXattrsClosesSmuggling pins the copy-up strip:
// base-borne machinery records are removed wholesale, real xattrs
// survive (REQ-writable-reserved's base-content arm).
func TestStripMachineryXattrsClosesSmuggling(t *testing.T) {
	root, w := newUpper(t)
	if err := w.PublishFile("f", strings.NewReader("real content"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	host := filepath.Join(root, "f")
	// A hostile base could carry any machinery record; simulate the
	// copy-up having faithfully copied them.
	for name, val := range map[string]string{
		XattrKind:               "char",
		XattrRdev:               "1:3",
		XattrOwner:              "0:0",
		XattrEscapePrefix + "e": "v",
		"user.legit":            "keep",
	} {
		if err := unix.Lsetxattr(host, name, []byte(val), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := StripMachineryXattrs(host); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatalf("stripped node still unreadable: %v", err)
	}
	e := st.Entries["f"]
	if e.StandIn || e.Kind != KindFile || e.UID == 0 && os.Getuid() != 0 {
		t.Fatalf("machinery survived the strip: %+v", e)
	}
	if e.Xattrs["user.legit"] != "keep" {
		t.Fatalf("legitimate xattr lost: %q", e.Xattrs)
	}
	if len(e.Xattrs) != 1 {
		t.Fatalf("unexpected presented xattrs: %q", e.Xattrs)
	}
}

// TestRmdirDismantleSequence pins REQ-writable-delete's dismantling
// discipline at the primitive seam: whiteout the directory first,
// then remove its interior markers, then remove the directory —
// every prefix keeps deleted base content hidden.
func TestRmdirDismantleSequence(t *testing.T) {
	root, w := newUpper(t)
	if err := w.Mkdir("d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("d/x"); err != nil {
		t.Fatal(err)
	}
	// Hide first: the parent whiteout occludes everything the inner
	// marker did.
	if err := w.Whiteout("d"); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveMarker("d/x"); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveDir("d"); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Whiteouts["d"] {
		t.Fatal("directory whiteout lost")
	}
	if st.Whiteouts["d/x"] {
		t.Fatal("inner marker survived dismantling")
	}
	if len(st.Entries) != 0 {
		t.Fatalf("entries survived dismantling: %v", st.Entries)
	}
	// The primitives are single atomic steps: a marker removal or
	// rmdir aimed at absent state errors rather than pretending.
	if err := w.RemoveDir("d"); err == nil {
		t.Fatal("rmdir of absent dir succeeded")
	}
	if err := w.RemoveMarker("d/x"); err == nil {
		t.Fatal("marker removal of absent marker succeeded")
	}
}

// TestWalkRefusesDamage pins every loud-failure branch: states the
// provider never writes are damage, and the walker names them
// rather than repairing or tolerating (REQ-writable-crash).
func TestWalkRefusesDamage(t *testing.T) {
	cases := map[string]func(root string) error{
		"degenerate marker": func(root string) error {
			return os.WriteFile(filepath.Join(root, ".wh."), nil, 0o644)
		},
		"reserved-target marker": func(root string) error {
			return os.WriteFile(filepath.Join(root, ".wh..wh.x"), nil, 0o644)
		},
		"marker as directory": func(root string) error {
			return os.Mkdir(filepath.Join(root, ".wh.d"), 0o755)
		},
		"opaque at root": func(root string) error {
			return os.WriteFile(filepath.Join(root, OpaqueMarker), nil, 0o644)
		},
		"malformed rdev": func(root string) error {
			p := filepath.Join(root, "dev")
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				return err
			}
			if err := unix.Lsetxattr(p, XattrKind, []byte("char"), 0); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrRdev, []byte("junk"), 0)
		},
		"malformed owner": func(root string) error {
			p := filepath.Join(root, "f")
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrOwner, []byte("root"), 0)
		},
		"stand-in record on a directory": func(root string) error {
			p := filepath.Join(root, "d")
			if err := os.Mkdir(p, 0o755); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrKind, []byte("fifo"), 0)
		},
		"stray stand-in field": func(root string) error {
			p := filepath.Join(root, "d2")
			if err := os.Mkdir(p, 0o755); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrTarget, []byte("/x"), 0)
		},
		"empty escape name": func(root string) error {
			p := filepath.Join(root, "f2")
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrEscapePrefix, []byte("v"), 0)
		},
		"content-bearing stand-in": func(root string) error {
			p := filepath.Join(root, "si")
			if err := os.WriteFile(p, []byte("junk"), 0o644); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrKind, []byte("char"), 0)
		},
		"symlink stand-in without target": func(root string) error {
			p := filepath.Join(root, "sl")
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrKind, []byte("symlink"), 0)
		},
		"unknown stand-in kind": func(root string) error {
			p := filepath.Join(root, "si2")
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				return err
			}
			return unix.Lsetxattr(p, XattrKind, []byte("door"), 0)
		},
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			root, _ := newUpper(t)
			if err := plant(root); err != nil {
				t.Fatalf("plant: %v", err)
			}
			if _, err := Walk(root); err == nil {
				t.Fatalf("damage %q walked clean", name)
			}
		})
	}
}

// TestCreatePublishRefusesExisting pins POSIX create semantics
// through the atomic publish: Mkdir and Mkfifo refuse an existing
// destination rather than silently replacing its inode and records.
func TestCreatePublishRefusesExisting(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: the override arm needs a refused chown")
	}
	root, w := newUpper(t)
	if err := w.Mkdir("d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.SetOwner("d", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("d", 0o700); err == nil {
		t.Fatal("mkdir over existing directory succeeded")
	}
	before, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("d", 0o700); err == nil {
		t.Fatal("second mkdir over existing directory succeeded")
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if e := st.Entries["d"]; e.UID != 0 || e.Mode != 0o755 || e.Ino != before.Entries["d"].Ino {
		t.Fatalf("existing directory disturbed: %+v (ino before %d)", e, before.Entries["d"].Ino)
	}
	if err := w.Mkfifo("d", 0o600); err == nil {
		t.Fatal("mkfifo over existing directory succeeded")
	}
	if err := w.PublishFile("f", strings.NewReader("v1"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkfifo("f", 0o600); err == nil {
		t.Fatal("mkfifo over existing file succeeded")
	}
	// PublishFile deliberately replaces: content writes are updates.
	if err := w.PublishFile("f", strings.NewReader("v2"), 0o600, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
}

// TestRootRecord pins the root-record dialect rule
// (REQ-writable-dialect): no record means no root entry; the record
// makes root attributes deliberate and presented through the walk;
// machinery on the root without the record is damage.
func TestRootRecord(t *testing.T) {
	root, w := newUpper(t)
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Root != nil {
		t.Fatal("unrecorded root surfaced an entry")
	}

	if err := w.RecordRoot(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.SetRootMode(0o700); err != nil {
		t.Fatal(err)
	}
	if err := w.SetRootTimes(fixedTime); err != nil {
		t.Fatal(err)
	}
	st, err = Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Root == nil {
		t.Fatal("recorded root missing")
	}
	if st.Root.Kind != KindDir || st.Root.Mode != 0o700 ||
		st.Root.UID != 0 || st.Root.GID != 0 ||
		!st.Root.ModTime.Equal(fixedTime) {
		t.Fatalf("root record wrong: %+v", st.Root)
	}

	// Re-recording replaces atomically.
	if err := w.RecordRoot(7, 8); err != nil {
		t.Fatal(err)
	}
	st, err = Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Root.UID != 7 || st.Root.GID != 8 {
		t.Fatalf("re-record not presented: %+v", st.Root)
	}
}

// TestRootMachineryWithoutRecordRefused pins the damage arm: any
// reserved xattr on the root without the owner record fails the
// walk loudly.
func TestRootMachineryWithoutRecordRefused(t *testing.T) {
	root, _ := newUpper(t)
	if err := unix.Lsetxattr(root, XattrEscapePrefix+"user.k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := Walk(root); err == nil || !strings.Contains(err.Error(), "root record") {
		t.Fatalf("root machinery without record accepted: %v", err)
	}
}

// TestModeRecord pins the mode fidelity override
// (REQ-writable-fidelity): a provider-denying mode presents and
// round-trips through the record while the host keeps access bits;
// returning to an accessible mode drops the record.
func TestModeRecord(t *testing.T) {
	root, w := newUpper(t)
	if err := w.PublishFile("f", strings.NewReader("x"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.SetMode("f", 0); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatalf("walk with mode-000 entry: %v", err)
	}
	if st.Entries["f"].Mode != 0 {
		t.Fatalf("presented mode %o, want 0", st.Entries["f"].Mode)
	}
	var hst unix.Stat_t
	if err := unix.Lstat(filepath.Join(root, "f"), &hst); err != nil {
		t.Fatal(err)
	}
	if hst.Mode&0o600 != 0o600 {
		t.Fatalf("host lost provider access: %o", hst.Mode&0o7777)
	}

	// Back to accessible: the record drops, host truth resumes.
	if err := w.SetMode("f", 0o640); err != nil {
		t.Fatal(err)
	}
	st, err = Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Entries["f"].Mode != 0o640 {
		t.Fatalf("presented mode %o, want 640", st.Entries["f"].Mode)
	}
	names, err := listXattrs(filepath.Join(root, "f"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if n == XattrMode {
			t.Fatal("stale mode record after accessible chmod")
		}
	}

	// Publishing directly with a denying mode records too — dirs
	// keep owner search.
	if err := w.Mkdir("d", 0o111); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("d/g", strings.NewReader("y"), 0o004, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	st, err = Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Entries["d"].Mode != 0o111 || st.Entries["d/g"].Mode != 0o004 {
		t.Fatalf("published denying modes wrong: %o %o", st.Entries["d"].Mode, st.Entries["d/g"].Mode)
	}
}

// TestChownClearsModeRecordSuid pins REQ-writable-fidelity's
// clearing rule through the mode record: special bits held in the
// record clear on chown exactly as native host bits would.
func TestChownClearsModeRecordSuid(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arm needs refused chowns")
	}
	root, w := newUpper(t)
	if err := w.PublishFile("f", strings.NewReader("x"), 0o644, fixedTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.SetMode("f", 0o4400); err != nil {
		t.Fatal(err)
	}
	if err := w.SetOwner("f", 0, 0); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	e := st.Entries["f"]
	if e.UID != 0 || e.GID != 0 {
		t.Fatalf("owner %d:%d", e.UID, e.GID)
	}
	if e.Mode&0o6000 != 0 {
		t.Fatalf("suid survived chown through the record: %o", e.Mode)
	}
	if e.Mode != 0o400 {
		t.Fatalf("mode %o, want 400", e.Mode)
	}
}

// TestPublishDirAtomic pins directory copy-up atomicity: a
// fully-attributed publish (mode record, owner override, escaped
// xattr, time) appears whole, and an aborted publish leaves nothing
// at the path.
func TestPublishDirAtomic(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arm needs refused chowns")
	}
	root, w := newUpper(t)
	xattrs := map[string]string{"user.k": "v", "security.capability": "caps"}
	if err := w.PublishDir("d", 0o500, 0, 0, fixedTime, xattrs); err != nil {
		t.Fatal(err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	e := st.Entries["d"]
	if e.Kind != KindDir || e.Mode != 0o500 || e.UID != 0 || e.GID != 0 ||
		!e.ModTime.Equal(fixedTime) {
		t.Fatalf("published dir wrong: %+v", e)
	}
	if e.Xattrs["user.k"] != "v" || e.Xattrs["security.capability"] != "caps" {
		t.Fatalf("xattrs wrong: %v", e.Xattrs)
	}

	// Abort at the publish gate: the crash prefix holds no entry —
	// fully copied or untouched.
	w2root, w2 := newUpper(t)
	w2.SetStepHook(func(desc string) error {
		if desc == "dir-publish e" {
			return errors.New("crash")
		}
		return nil
	})
	if err := w2.PublishDir("e", 0o755, 0, 0, fixedTime, nil); err == nil {
		t.Fatal("gate abort did not surface")
	}
	st2, err := Walk(w2root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st2.Entries["e"]; ok {
		t.Fatal("aborted publish left an entry")
	}
}

// TestChmodOnFifoStoresNatively pins the kind switch: FIFOs carry no
// xattr machinery, so any mode — provider-denying included — stores
// natively and round-trips through the walk.
func TestChmodOnFifoStoresNatively(t *testing.T) {
	root, w := newUpper(t)
	if err := w.Mkfifo("p", 0o640); err != nil {
		t.Fatal(err)
	}
	if err := w.SetMode("p", 0o400); err != nil {
		t.Fatalf("denying chmod on fifo: %v", err)
	}
	if err := w.SetMode("p", 0o644); err != nil {
		t.Fatalf("accessible chmod on fifo: %v", err)
	}
	st, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Entries["p"].Mode != 0o644 {
		t.Fatalf("fifo mode %o", st.Entries["p"].Mode)
	}
}
