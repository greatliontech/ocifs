//go:build linux

package commit

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sys/unix"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/upper"
)

var baseTime = time.Date(2024, 6, 1, 9, 0, 0, 500, time.UTC)

func digestOf(content string) v1.Hash {
	sum := sha256.Sum256([]byte(content))
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// Base fixtures carry the invoking user's ids: an unprivileged
// restore can only match a base entry it could own.
func bfile(name, content string, mode int64, mt time.Time) layer.Entry {
	return layer.Entry{
		Header: tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: mode, Size: int64(len(content)), ModTime: mt, Uid: os.Getuid(), Gid: os.Getgid()},
		Digest: digestOf(content),
	}
}

func bdir(name string, mode int64, mt time.Time) layer.Entry {
	return layer.Entry{Header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: mode, ModTime: mt, Uid: os.Getuid(), Gid: os.Getgid()}}
}

func newUpperDir(t testing.TB) (string, *upper.Writer) {
	t.Helper()
	dir := scratchtest.Dir(t, "commit")
	root := filepath.Join(dir, "u")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	return root, upper.NewWriter(root)
}

func mustLayer(t testing.TB, base *layer.View, root string) []byte {
	t.Helper()
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := LayerBytes(base, st)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// tarEntries parses a committed layer into name -> header (+content
// digest for regular files).
type committedEntry struct {
	hdr     tar.Header
	content string
}

func tarEntries(t testing.TB, b []byte) map[string]committedEntry {
	t.Helper()
	out := map[string]committedEntry{}
	tr := tar.NewReader(bytes.NewReader(b))
	var order []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var content []byte
		if hdr.Typeflag == tar.TypeReg {
			content, err = io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
		}
		out[hdr.Name] = committedEntry{hdr: *hdr, content: string(content)}
		order = append(order, hdr.Name)
	}
	for i := 1; i < len(order); i++ {
		if order[i-1] >= order[i] {
			t.Fatalf("emission order not sorted: %q >= %q", order[i-1], order[i])
		}
	}
	return out
}

// TestEmissionRules pins REQ-writable-commit's arms over one
// fixture: emit-iff-differs, restored-entries elided, effective
// markers only, fixed marker headers, machinery never emitted,
// overrides resolved into genuine headers, sockets omitted.
func TestEmissionRules(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	base, err := layer.Unify([]layer.Layer{{
		bdir("etc", 0o755, baseTime),
		bfile("etc/keep", "same", 0o644, baseTime),
		bfile("etc/gone", "bye", 0o644, baseTime),
		bfile("etc/gone2", "old2", 0o644, baseTime),
		bfile("etc/change", "old", 0o644, baseTime),
		bfile("sock", "shadowed", 0o644, baseTime),
		bfile("sock2", "shadowed2", 0o644, baseTime),
		bfile("imp/f", "impcontent", 0o644, baseTime),
		bdir("wipe", 0o755, baseTime),
		bfile("wipe/x", "x", 0o644, baseTime),
		bdir("re", 0o755, baseTime),
		bfile("re/x", "rx", 0o644, baseTime),
		bfile("re/z", "rz", 0o644, baseTime),
		bdir("re/sub", 0o755, baseTime),
		bfile("re/sub/q", "q", 0o644, baseTime),
	}})
	if err != nil {
		t.Fatal(err)
	}

	root, w := newUpperDir(t)
	// The spine dir with base-equal attrs (elides from the diff).
	if err := w.Mkdir("etc", 0o755); err != nil {
		t.Fatal(err)
	}
	// Identical restore of etc/keep: not emitted.
	if err := w.PublishFile("etc/keep", strings.NewReader("same"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	// Content change.
	if err := w.PublishFile("etc/change", strings.NewReader("new!"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	// Deletion of a base file; whiteout of a base-less path (inert).
	if err := w.Whiteout("etc/gone"); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("etc/neverwas"); err != nil {
		t.Fatal(err)
	}
	// Whiteout of a second base-less path.
	if err := w.Whiteout("etc/keep2"); err != nil {
		t.Fatal(err)
	}
	// Deleted base file recreated as a file: the new entry finalizes
	// the path, so the marker is redundant and must not be emitted.
	if err := w.Whiteout("etc/gone2"); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("etc/gone2", strings.NewReader("new2"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	// A socket shadowing a deleted base file: sockets never commit,
	// so the base's whiteout must — and a base-less socket emits
	// nothing at all.
	if err := w.Whiteout("sock"); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mknod(filepath.Join(root, "sock"), unix.S_IFSOCK|0o755, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mknod(filepath.Join(root, "livesock"), unix.S_IFSOCK|0o755, 0); err != nil {
		t.Fatal(err)
	}
	// A bare socket at a base path — no marker in the upper (a
	// caller-built upper can hold one): the whiteout must still
	// commit.
	if err := unix.Mknod(filepath.Join(root, "sock2"), unix.S_IFSOCK|0o755, 0); err != nil {
		t.Fatal(err)
	}
	// An implied base directory (synthesized: 0755, 0:0, epoch
	// mtime) restored to its presented state — not emitted.
	if err := w.Mkdir("imp", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("imp/f", strings.NewReader("impcontent"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.SetOwner("imp", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.SetTimes("imp", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	// Opaque with and without effect.
	if err := w.Mkdir("wipe", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.SetTimes("wipe", baseTime); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("wipe"); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("fresh", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("fresh"); err != nil {
		t.Fatal(err)
	}
	if err := w.SetTimes("etc", baseTime); err != nil {
		t.Fatal(err)
	}
	// A directory whited out and recreated: base beneath is occluded,
	// so an identically-restored child is still new content, an
	// interior marker adds nothing, and an opaque has no effect.
	if err := w.Whiteout("re"); err != nil {
		t.Fatal(err)
	}
	if err := w.Mkdir("re", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFile("re/x", strings.NewReader("rx"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Whiteout("re/z"); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("re"); err != nil {
		t.Fatal(err)
	}
	// An opaque'd subdir beneath the whited-out ancestor: its base
	// children are already dead, so the opaque has no effect either.
	if err := w.Mkdir("re/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Opaque("re/sub"); err != nil {
		t.Fatal(err)
	}
	if err := w.SetTimes("re/sub", baseTime); err != nil {
		t.Fatal(err)
	}
	if err := w.SetTimes("re", baseTime); err != nil {
		t.Fatal(err)
	}
	// Device stand-in with a foreign owner and an escaped xattr.
	if err := w.MakeStandIn("null", upper.KindCharDev, "", upper.Rdev{Major: 1, Minor: 3}, 0o666, 0, 0, baseTime, map[string]string{"security.capability": "caps"}); err != nil {
		t.Fatal(err)
	}

	entries := tarEntries(t, mustLayer(t, base, root))

	if _, ok := entries["etc/keep"]; ok {
		t.Fatal("restored entry emitted")
	}
	if e, ok := entries["etc/change"]; !ok || e.content != "new!" {
		t.Fatalf("changed entry wrong: %+v", e)
	}
	wh, ok := entries["etc/.wh.gone"]
	if !ok {
		t.Fatal("deletion marker missing")
	}
	if wh.hdr.Mode != 0 || wh.hdr.Uid != 0 || wh.hdr.Gid != 0 || !wh.hdr.ModTime.Equal(time.Unix(0, 0)) || len(wh.hdr.PAXRecords) != 0 {
		t.Fatalf("marker header not fixed: %+v", wh.hdr)
	}
	if _, ok := entries["etc/.wh.neverwas"]; ok {
		t.Fatal("ineffective marker emitted")
	}
	if _, ok := entries["etc/.wh.keep2"]; ok {
		t.Fatal("marker under base-less path emitted")
	}
	if e, ok := entries["etc/gone2"]; !ok || e.content != "new2" {
		t.Fatalf("recreated entry wrong: %+v", e)
	}
	if _, ok := entries["etc/.wh.gone2"]; ok {
		t.Fatal("marker beside finalizing entry emitted")
	}
	if _, ok := entries[".wh.sock"]; !ok {
		t.Fatal("socket-shadow marker missing")
	}
	if _, ok := entries["sock"]; ok {
		t.Fatal("socket emitted")
	}
	if _, ok := entries["livesock"]; ok {
		t.Fatal("base-less socket emitted")
	}
	if _, ok := entries[".wh.sock2"]; !ok {
		t.Fatal("marker-less socket shadow: whiteout missing")
	}
	if _, ok := entries["sock2"]; ok {
		t.Fatal("marker-less socket emitted")
	}
	if _, ok := entries["imp"]; ok {
		t.Fatal("restored implied directory emitted")
	}
	if _, ok := entries["imp/f"]; ok {
		t.Fatal("restored file under implied directory emitted")
	}
	if _, ok := entries[".wh.re"]; !ok {
		t.Fatal("recreated directory's own marker missing")
	}
	if e, ok := entries["re/x"]; !ok || e.content != "rx" {
		t.Fatal("identically-restored child under occluded base not emitted")
	}
	if _, ok := entries["re/.wh.z"]; ok {
		t.Fatal("interior marker under the parent's marker emitted")
	}
	if _, ok := entries["re/.wh..wh..opq"]; ok {
		t.Fatal("opaque under the directory's own marker emitted")
	}
	if _, ok := entries["re/sub/.wh..wh..opq"]; ok {
		t.Fatal("opaque under an occluded ancestor emitted")
	}
	if _, ok := entries["re"]; !ok {
		t.Fatal("recreated directory entry missing")
	}
	if _, ok := entries["wipe/.wh..wh..opq"]; !ok {
		t.Fatal("effective opaque missing")
	}
	if _, ok := entries["fresh/.wh..wh..opq"]; ok {
		t.Fatal("ineffective opaque emitted")
	}
	dev, ok := entries["null"]
	if !ok || dev.hdr.Typeflag != tar.TypeChar || dev.hdr.Devmajor != 1 || dev.hdr.Devminor != 3 || dev.hdr.Uid != 0 {
		t.Fatalf("device stand-in wrong: %+v", dev.hdr)
	}
	if dev.hdr.PAXRecords["SCHILY.xattr.security.capability"] != "caps" {
		t.Fatalf("escaped xattr not resolved: %v", dev.hdr.PAXRecords)
	}
	for name, e := range entries {
		for k := range e.hdr.PAXRecords {
			if strings.Contains(k, upper.XattrNS) {
				t.Fatalf("machinery leaked into %q: %s", name, k)
			}
		}
	}
}

// TestHardlinkForceEmit pins the link rule: one content entry at the
// sorted-first path (even when that path alone matches base), links
// targeting it.
func TestHardlinkForceEmit(t *testing.T) {
	base, err := layer.Unify([]layer.Layer{{
		bfile("keep", "shared", 0o644, baseTime),
	}})
	if err != nil {
		t.Fatal(err)
	}
	root, w := newUpperDir(t)
	// Restore keep byte-identically, then link a new path onto it:
	// zzz differs (new), keep alone does not — but the group forces
	// the content entry at the sorted-first path.
	if err := w.PublishFile("keep", strings.NewReader("shared"), 0o644, baseTime, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Link("keep", "zzz"); err != nil {
		t.Fatal(err)
	}
	entries := tarEntries(t, mustLayer(t, base, root))
	k, ok := entries["keep"]
	if !ok || k.hdr.Typeflag != tar.TypeReg || k.content != "shared" {
		t.Fatalf("force-emitted content entry wrong: %+v", k)
	}
	z, ok := entries["zzz"]
	if !ok || z.hdr.Typeflag != tar.TypeLink || z.hdr.Linkname != "keep" {
		t.Fatalf("link entry wrong: %+v", z.hdr)
	}
}

// TestNeutralityAcrossHistories pins REQ-proj-commit-neutral /
// REQ-writable-commit: equal (base, upper) states commit to
// byte-identical layers, whatever order produced them.
func TestNeutralityAcrossHistories(t *testing.T) {
	base, err := layer.Unify([]layer.Layer{{
		bfile("a", "av", 0o644, baseTime),
		bfile("b", "bv", 0o644, baseTime),
	}})
	if err != nil {
		t.Fatal(err)
	}

	build := func(order []func(*upper.Writer) error) ([]byte, string) {
		root, w := newUpperDir(t)
		for _, step := range order {
			if err := step(w); err != nil {
				t.Fatal(err)
			}
		}
		// Normalize directory times: dir mtimes are presented truth
		// and child churn moves them.
		if err := w.SetTimes("d1", baseTime); err != nil {
			t.Fatal(err)
		}
		if err := w.SetTimes("d2", baseTime); err != nil {
			t.Fatal(err)
		}
		return mustLayer(t, base, root), root
	}

	mkd1 := func(w *upper.Writer) error { return w.Mkdir("d1", 0o755) }
	mkd2 := func(w *upper.Writer) error { return w.Mkdir("d2", 0o700) }
	f1 := func(w *upper.Writer) error {
		return w.PublishFile("d1/f", strings.NewReader("one"), 0o600, baseTime, nil)
	}
	f2 := func(w *upper.Writer) error {
		return w.PublishFile("d2/g", strings.NewReader("two"), 0o644, baseTime, nil)
	}
	wh := func(w *upper.Writer) error { return w.Whiteout("a") }
	pb := func(w *upper.Writer) error {
		return w.PublishFile("b", strings.NewReader("bv2"), 0o644, baseTime, nil)
	}
	// Deleting b before recreating it leaves a marker in the upper;
	// the committed layer must be identical to the direct modify.
	whb := func(w *upper.Writer) error { return w.Whiteout("b") }

	l1, root1 := build([]func(*upper.Writer) error{mkd1, mkd2, f1, f2, wh, pb})
	l2, _ := build([]func(*upper.Writer) error{pb, mkd2, wh, mkd1, f2, f1})
	// A detour history: write then overwrite d1/f, delete b then
	// recreate it.
	detour := func(w *upper.Writer) error {
		return w.PublishFile("d1/f", strings.NewReader("scratch"), 0o600, baseTime, nil)
	}
	l3, _ := build([]func(*upper.Writer) error{mkd1, detour, mkd2, whb, pb, f2, f1, wh})

	if !bytes.Equal(l1, l2) {
		t.Fatal("permuted history committed a different layer")
	}
	if !bytes.Equal(l1, l3) {
		t.Fatal("detour history committed a different layer")
	}
	// And re-serializing the same upper reproduces the bytes.
	if !bytes.Equal(l1, mustLayer(t, base, root1)) {
		t.Fatal("re-serialization changed the layer")
	}
}

// TestRootRecordCommits pins the root arm of REQ-writable-commit
// via REQ-writable-dialect: an unrecorded root commits nothing, a
// recorded root restored to the base root's presented attributes
// elides, and a differing recorded root commits the "./" entry with
// its presented attributes.
func TestRootRecordCommits(t *testing.T) {
	base, err := layer.Unify([]layer.Layer{{
		bfile("keep", "same", 0o644, baseTime),
	}})
	if err != nil {
		t.Fatal(err)
	}

	// Unrecorded.
	root, _ := newUpperDir(t)
	if _, ok := tarEntries(t, mustLayer(t, base, root))["./"]; ok {
		t.Fatal("unrecorded root emitted")
	}

	// Recorded, restored to the synthesized base root (0755, 0:0,
	// epoch): elided.
	root2, w2 := newUpperDir(t)
	if err := w2.RecordRoot(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := w2.SetRootMode(0o755); err != nil {
		t.Fatal(err)
	}
	if err := w2.SetRootTimes(time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, ok := tarEntries(t, mustLayer(t, base, root2))["./"]; ok {
		t.Fatal("restored root emitted")
	}

	// Recorded, differing: emitted with presented attributes.
	root3, w3 := newUpperDir(t)
	if err := w3.RecordRoot(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := w3.SetRootMode(0o700); err != nil {
		t.Fatal(err)
	}
	if err := w3.SetRootTimes(baseTime); err != nil {
		t.Fatal(err)
	}
	e, ok := tarEntries(t, mustLayer(t, base, root3))["./"]
	if !ok {
		t.Fatal("differing recorded root not emitted")
	}
	if e.hdr.Typeflag != tar.TypeDir || e.hdr.Mode != 0o700 ||
		e.hdr.Uid != 0 || e.hdr.Gid != 0 || !e.hdr.ModTime.Equal(baseTime) {
		t.Fatalf("root header wrong: %+v", e.hdr)
	}
	for k := range e.hdr.PAXRecords {
		if strings.Contains(k, upper.XattrNS) {
			t.Fatalf("machinery leaked into root: %s", k)
		}
	}
}
