package fskitfs

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	fskit "github.com/greatliontech/fskit-go"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

var fixtureMtime = time.Date(2022, 7, 8, 9, 10, 11, 0, time.UTC)

func digestFor(content []byte) v1.Hash {
	sum := sha256.Sum256(content)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// fixtureVolume builds a volume over a crafted view, backing regular
// files with blobs in a scratch dir — the store's blobPath contract
// without a store.
func fixtureVolume(t *testing.T) *Volume {
	t.Helper()
	blobDir := scratchtest.Dir(t, "fskitfs")
	content := map[string]string{
		"docs/a.txt": "hello fskit",
		"suid":       "",
		"zeta":       "z",
	}
	entries := []layer.Entry{
		{Header: tar.Header{Name: "docs", Typeflag: tar.TypeDir, Mode: 0o710, Uid: 12, Gid: 34, ModTime: fixtureMtime}},
		{Header: tar.Header{Name: "docs/a.txt", Typeflag: tar.TypeReg, Mode: 0o604, Uid: 12, Gid: 34, ModTime: fixtureMtime, Size: 11}},
		{Header: tar.Header{Name: "docs/link", Typeflag: tar.TypeSymlink, Linkname: "a.txt", Mode: 0o777}},
		{Header: tar.Header{Name: "hard", Typeflag: tar.TypeLink, Linkname: "docs/a.txt"}},
		{Header: tar.Header{Name: "null", Typeflag: tar.TypeChar, Mode: 0o666, Devmajor: 1, Devminor: 3}},
		{Header: tar.Header{Name: "pipe", Typeflag: tar.TypeFifo, Mode: 0o10644}},
		{Header: tar.Header{Name: "sda", Typeflag: tar.TypeBlock, Mode: 0o660}},
		{Header: tar.Header{Name: "suid", Typeflag: tar.TypeReg, Mode: 0o4755, Size: 0}},
		{Header: tar.Header{Name: "zeta", Typeflag: tar.TypeReg, Mode: 0, Size: 1}},
	}
	for i := range entries {
		if entries[i].Header.Typeflag == tar.TypeReg {
			c := content[entries[i].Header.Name]
			d := digestFor([]byte(c))
			entries[i].Digest = d
			if err := os.WriteFile(filepath.Join(blobDir, d.Hex), []byte(c), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	view, err := layer.Unify([]layer.Layer{layer.Layer(entries)})
	if err != nil {
		t.Fatal(err)
	}
	proj, err := projection.New(view, []string{"anchor"}, Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	return New(proj, func(h v1.Hash) string { return filepath.Join(blobDir, h.Hex) })
}

func rootOf(t *testing.T, vol *Volume) fskit.Item {
	t.Helper()
	root, err := vol.Activate(fskit.TaskOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func lookup(t *testing.T, vol *Volume, dir fskit.Item, name string) fskit.Item {
	t.Helper()
	item, canonical, err := vol.Lookup(dir, name)
	if err != nil {
		t.Fatalf("Lookup(%s): %v", name, err)
	}
	if canonical != name {
		t.Fatalf("canonical %q for requested %q on a case-sensitive volume", canonical, name)
	}
	return item
}

// collectPacker gathers packed entries with an optional acceptance
// limit — the memfs pagination pattern.
type collectPacker struct {
	limit   int
	names   []string
	types   []fskit.ItemType
	ids     []fskit.ItemID
	cookies []fskit.DirCookie
	attrs   []*fskit.Attributes
}

func (p *collectPacker) PackEntry(name string, typ fskit.ItemType, id fskit.ItemID, next fskit.DirCookie, attrs *fskit.Attributes) bool {
	if p.limit > 0 && len(p.names) >= p.limit {
		return false
	}
	p.names = append(p.names, name)
	p.types = append(p.types, typ)
	p.ids = append(p.ids, id)
	p.cookies = append(p.cookies, next)
	p.attrs = append(p.attrs, attrs)
	return true
}

// TestVolumeIdentityIsKernelIdentity pins REQ-proj-identity on the
// FSKit surface: the root is item ID 2, entries carry view-range
// IDs, ParentID chains correctly.
func TestVolumeIdentityIsKernelIdentity(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)

	ra, err := vol.GetAttributes(root, fskit.GetAttributesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ra.FileID != fskit.ItemIDRoot {
		t.Fatalf("root FileID = %d, want %d", ra.FileID, fskit.ItemIDRoot)
	}
	if ra.ParentID != fskit.ItemIDParentOfRoot {
		t.Fatalf("root ParentID = %d, want %d", ra.ParentID, fskit.ItemIDParentOfRoot)
	}

	docs := lookup(t, vol, root, "docs")
	da, _ := vol.GetAttributes(docs, fskit.GetAttributesRequest{})
	if da.FileID < fskit.ItemIDFirstValid || da.FileID >= 1<<62 {
		t.Fatalf("docs FileID %d outside the view range", da.FileID)
	}
	if da.ParentID != fskit.ItemIDRoot {
		t.Fatalf("docs ParentID = %d, want root", da.ParentID)
	}
	a := lookup(t, vol, docs, "a.txt")
	aa, _ := vol.GetAttributes(a, fskit.GetAttributesRequest{})
	if aa.ParentID != da.FileID {
		t.Fatalf("a.txt ParentID = %d, want docs %d", aa.ParentID, da.FileID)
	}
	anchor := lookup(t, vol, root, "anchor")
	xa, _ := vol.GetAttributes(anchor, fskit.GetAttributesRequest{})
	if xa.FileID < 1<<62 {
		t.Fatalf("extra dir FileID %d inside the view range", xa.FileID)
	}
}

// TestAttributesCarryTheRecordedEnvelope pins the FSKit column of
// REQ-proj-fidelity: modes (including 0000 verbatim), ownership,
// sizes, times with the spec's fallbacks.
func TestAttributesCarryTheRecordedEnvelope(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)

	docs := lookup(t, vol, root, "docs")
	da, _ := vol.GetAttributes(docs, fskit.GetAttributesRequest{})
	if da.Type != fskit.TypeDirectory || da.Mode != 0o710 || da.UID != 12 || da.GID != 34 {
		t.Fatalf("docs attrs = %+v", da)
	}
	if !da.ModifyTime.Equal(fixtureMtime) || !da.AccessTime.Equal(fixtureMtime) || !da.BirthTime.Equal(fixtureMtime) {
		t.Fatalf("docs times = m%v a%v b%v (fallbacks)", da.ModifyTime, da.AccessTime, da.BirthTime)
	}

	a := lookup(t, vol, docs, "a.txt")
	aa, _ := vol.GetAttributes(a, fskit.GetAttributesRequest{})
	if aa.Type != fskit.TypeFile || aa.Mode != 0o604 || aa.Size != 11 {
		t.Fatalf("a.txt attrs = %+v", aa)
	}

	// Mode 0000 serves verbatim; zero mtime presents as epoch.
	zeta := lookup(t, vol, root, "zeta")
	za, _ := vol.GetAttributes(zeta, fskit.GetAttributesRequest{})
	if za.Mode != 0 {
		t.Fatalf("zeta mode = %o, want 0000 verbatim", za.Mode)
	}
	if !za.ModifyTime.Equal(time.Unix(0, 0)) {
		t.Fatalf("zeta mtime = %v, want epoch fallback", za.ModifyTime)
	}

	link := lookup(t, vol, docs, "link")
	la, _ := vol.GetAttributes(link, fskit.GetAttributesRequest{})
	if la.Type != fskit.TypeSymlink || la.Size != uint64(len("a.txt")) {
		t.Fatalf("link attrs = %+v", la)
	}
	target, err := vol.ReadSymlink(link)
	if err != nil || target != "a.txt" {
		t.Fatalf("ReadSymlink = %q, %v", target, err)
	}

	pipe := lookup(t, vol, root, "pipe")
	pa, _ := vol.GetAttributes(pipe, fskit.GetAttributesRequest{})
	if pa.Type != fskit.TypeFIFO {
		t.Fatalf("pipe type = %v, want FIFO (typed node)", pa.Type)
	}
	// Writer-set type bits in the recorded mode are masked off:
	// Attributes.Mode carries permission bits only.
	if pa.Mode != 0o644 {
		t.Fatalf("pipe mode = %o, want type bits masked to 0644", pa.Mode)
	}

	// Devices present as typed nodes (numbers already dropped by the
	// kernel); a hardlink is an independent file node serving the
	// target's content; suid survives the permission mask.
	null := lookup(t, vol, root, "null")
	na, _ := vol.GetAttributes(null, fskit.GetAttributesRequest{})
	if na.Type != fskit.TypeCharDevice {
		t.Fatalf("null type = %v", na.Type)
	}
	sda := lookup(t, vol, root, "sda")
	sa, _ := vol.GetAttributes(sda, fskit.GetAttributesRequest{})
	if sa.Type != fskit.TypeBlockDevice {
		t.Fatalf("sda type = %v", sa.Type)
	}
	hard := lookup(t, vol, root, "hard")
	ha, _ := vol.GetAttributes(hard, fskit.GetAttributesRequest{})
	if ha.Type != fskit.TypeFile || ha.LinkCount != 1 {
		t.Fatalf("hard attrs = %+v (independent node)", ha)
	}
	hbuf := make([]byte, 16)
	n, err := vol.Read(hard, 0, hbuf)
	if err != nil || string(hbuf[:n]) != "hello fskit" {
		t.Fatalf("hard content = %q, %v", hbuf[:n], err)
	}
	suid := lookup(t, vol, root, "suid")
	ua, _ := vol.GetAttributes(suid, fskit.GetAttributesRequest{})
	if ua.Mode != 0o4755 {
		t.Fatalf("suid mode = %o, want setuid preserved by the mask", ua.Mode)
	}
}

func TestLookupErrors(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)

	if _, _, err := vol.Lookup(root, "absent"); !errors.Is(err, fskit.ENOENT) {
		t.Fatalf("absent: %v, want ENOENT", err)
	}
	file := lookup(t, vol, root, "zeta")
	if _, _, err := vol.Lookup(file, "x"); !errors.Is(err, fskit.ENOTDIR) {
		t.Fatalf("lookup in file: %v, want ENOTDIR", err)
	}
	if _, _, err := vol.Lookup("foreign item", "x"); !errors.Is(err, fskit.EINVAL) {
		t.Fatalf("foreign item: %v, want EINVAL", err)
	}
	if err := vol.Reclaim(root); err != nil {
		t.Fatalf("Reclaim(root): %v", err)
	}
	if err := vol.Reclaim(42); !errors.Is(err, fskit.EINVAL) {
		t.Fatalf("Reclaim(foreign): %v, want EINVAL", err)
	}
}

// TestEnumerate pins REQ-proj-enumeration on the FSKit surface:
// byte-sorted entries, no dot entries, positional cookies that
// resume exactly, and the constant read-only verifier.
func TestEnumerate(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)

	full := &collectPacker{}
	v1st, err := vol.Enumerate(root, 0, 0, fskit.AttrType, full)
	if err != nil {
		t.Fatal(err)
	}
	if v1st != readOnlyVerifier {
		t.Fatalf("verifier = %d, want the constant", v1st)
	}
	want := []string{"anchor", "docs", "hard", "null", "pipe", "sda", "suid", "zeta"}
	if strings.Join(full.names, "|") != strings.Join(want, "|") {
		t.Fatalf("enumeration = %v, want byte-sorted %v (no dot entries)", full.names, want)
	}
	for _, a := range full.attrs {
		if a == nil {
			t.Fatal("wanted attrs but got nil")
		}
	}

	// Paginated: accept one entry per call, resume from the returned
	// cookie; the concatenation equals the whole listing and the
	// verifier never changes.
	var got []string
	cookie := fskit.DirCookie(0)
	for iter := 0; ; iter++ {
		if iter > root.(*projection.Entry).Len()+2 {
			t.Fatal("pagination never terminated (cookie not advancing)")
		}
		p := &collectPacker{limit: 1}
		v, err := vol.Enumerate(root, cookie, v1st, 0, p)
		if err != nil {
			t.Fatal(err)
		}
		if v != readOnlyVerifier {
			t.Fatalf("verifier changed mid-enumeration: %d", v)
		}
		if len(p.names) == 0 {
			break
		}
		got = append(got, p.names...)
		if p.attrs[0] != nil {
			t.Fatal("wanted==0 but attrs packed non-nil")
		}
		cookie = p.cookies[len(p.cookies)-1]
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("paginated = %v, want %v", got, want)
	}

	if _, err := vol.Enumerate(lookup(t, vol, root, "zeta"), 0, 0, 0, &collectPacker{}); !errors.Is(err, fskit.ENOTDIR) {
		t.Fatalf("enumerate file: %v, want ENOTDIR", err)
	}
}

// TestReadShortOnlyAtEOF pins REQ-proj-content on the FSKit read
// surface.
func TestReadShortOnlyAtEOF(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)
	docs := lookup(t, vol, root, "docs")
	a := lookup(t, vol, docs, "a.txt")
	content := "hello fskit"

	buf := make([]byte, 64)
	for _, tc := range []struct {
		off  int64
		size int
		want string
	}{
		{0, 64, content},
		{6, 64, content[6:]},
		{0, 5, content[:5]},
		{int64(len(content)), 8, ""},
		{100, 8, ""},
	} {
		n, err := vol.Read(a, tc.off, buf[:tc.size])
		if err != nil {
			t.Fatalf("Read(off=%d): %v", tc.off, err)
		}
		if string(buf[:n]) != tc.want {
			t.Fatalf("Read(off=%d) = %q, want %q", tc.off, buf[:n], tc.want)
		}
	}

	if _, err := vol.Read(docs, 0, buf); !errors.Is(err, fskit.EISDIR) {
		t.Fatalf("read dir: %v, want EISDIR", err)
	}
	link := lookup(t, vol, docs, "link")
	if _, err := vol.Read(link, 0, buf); !errors.Is(err, fskit.EINVAL) {
		t.Fatalf("read symlink: %v, want EINVAL", err)
	}
	if _, err := vol.Read(a, -1, buf); !errors.Is(err, fskit.EINVAL) {
		t.Fatalf("negative offset: %v, want EINVAL", err)
	}
}

// TestEveryMutatingOperationReturnsEROFS pins the FSKit arm of
// REQ-proj-ro: a read-only error from every mutating operation.
func TestEveryMutatingOperationReturnsEROFS(t *testing.T) {
	vol := fixtureVolume(t)
	root := rootOf(t, vol)
	zeta := lookup(t, vol, root, "zeta")

	ops := map[string]error{}
	_, err := vol.SetAttributes(zeta, fskit.SetAttributesRequest{})
	ops["SetAttributes"] = err
	_, _, err = vol.Create(root, "new", fskit.TypeFile, fskit.SetAttributesRequest{})
	ops["Create"] = err
	_, _, err = vol.CreateSymlink(root, "sym", fskit.SetAttributesRequest{}, "t")
	ops["CreateSymlink"] = err
	_, err = vol.CreateLink(zeta, root, "hard")
	ops["CreateLink"] = err
	ops["Remove"] = vol.Remove(root, zeta, "zeta")
	_, err = vol.Rename(zeta, root, "zeta", root, "omega", nil)
	ops["Rename"] = err
	_, err = vol.Write(zeta, 0, []byte("dirt"))
	ops["Write"] = err

	for op, err := range ops {
		if !errors.Is(err, fskit.EROFS) {
			t.Errorf("%s = %v, want EROFS", op, err)
		}
	}

	// Nothing mutated: the entry still serves its content.
	buf := make([]byte, 8)
	n, err := vol.Read(zeta, 0, buf)
	if err != nil || string(buf[:n]) != "z" {
		t.Fatalf("zeta after denied mutations = %q, %v", buf[:n], err)
	}
}

// TestCapabilitiesDeclareCaseSensitive pins the FSKit clause of
// REQ-proj-case: the projection declares case-sensitive, so no
// collision handling applies.
func TestCapabilitiesDeclareCaseSensitive(t *testing.T) {
	vol := fixtureVolume(t)
	caps := vol.Capabilities()
	if caps.CaseFormat != fskit.CaseSensitive {
		t.Fatalf("CaseFormat = %v, want CaseSensitive", caps.CaseFormat)
	}
	if !caps.SymbolicLinks || !caps.HardLinks {
		t.Fatalf("capability flags = %+v", caps)
	}
	if _, err := vol.Statistics(); err != nil {
		t.Fatal(err)
	}
	if pc := vol.PathConf(); pc.MaxNameLength != -1 {
		t.Fatalf("PathConf = %+v", pc)
	}
}
