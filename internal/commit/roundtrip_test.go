//go:build linux

package commit

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/upper"
)

// refEntry is the comparable presentation of one merged path.
type refEntry struct {
	kind   byte // tar typeflag class
	digest string
	target string
	mode   int64
	uid    int
	gid    int
	mtime  int64 // unix nanos, presented (zero-time normalizes to epoch)
	rdev   [2]int64
	xattrs string
}

// presentedNanos renders a header time as its presented truth: an
// unrecorded (zero) modification time presents as the Unix epoch,
// never as a zero-time artifact (projection.md's fidelity rules).
func presentedNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// refMerge computes the presented merge of (base, upper state) per
// REQ-writable-presented, sockets excluded (they never commit).
func refMerge(base *layer.View, up *upper.State) map[string]refEntry {
	occluded := func(p string) bool {
		if up.Whiteouts[p] {
			return true
		}
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			if up.Whiteouts[d] {
				return true
			}
			if up.Opaque[d] {
				if _, ok := up.Entries[d]; ok {
					return true
				}
			}
		}
		return false
	}
	out := map[string]refEntry{}
	for _, be := range base.Entries() {
		p := be.Header.Name
		if p == "." || occluded(p) {
			continue
		}
		if _, shadowed := up.Entries[p]; shadowed {
			continue
		}
		// A base child under an upper NON-directory entry vanishes
		// (the entry finalizes the path).
		covered := false
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			if ue, ok := up.Entries[d]; ok && ue.Kind != upper.KindDir {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		kind := be.Header.Typeflag
		target := be.Header.Linkname
		if kind == tar.TypeLink {
			// A hardlink presents as an independent node; its
			// Linkname is layer bookkeeping, not presentation.
			kind, target = tar.TypeReg, ""
		}
		var xs []string
		for k, v := range baseXattrs(&be) {
			xs = append(xs, k+"="+v)
		}
		out[p] = refEntry{
			kind: kind, digest: be.Digest.Hex, target: target,
			mode: be.Header.Mode & 0o7777, uid: be.Header.Uid, gid: be.Header.Gid,
			mtime: presentedNanos(be.Header.ModTime),
			rdev:  [2]int64{be.Header.Devmajor, be.Header.Devminor},
			xattrs: sortedJoin(xs),
		}
	}
	for p, e := range up.Entries {
		if e.Kind == upper.KindSocket {
			delete(out, p)
			continue
		}
		re := refEntry{
			mode: int64(e.Mode), uid: e.UID, gid: e.GID, mtime: presentedNanos(e.ModTime),
			target: e.Target,
		}
		switch e.Kind {
		case upper.KindFile:
			re.kind = tar.TypeReg
			sum, err := hashFile(e.HostPath)
			if err == nil {
				re.digest = sum
			}
		case upper.KindDir:
			re.kind = tar.TypeDir
		case upper.KindSymlink:
			re.kind = tar.TypeSymlink
		case upper.KindFifo:
			re.kind = tar.TypeFifo
		case upper.KindCharDev:
			re.kind = tar.TypeChar
			re.rdev = [2]int64{int64(e.Rdev.Major), int64(e.Rdev.Minor)}
		case upper.KindBlockDev:
			re.kind = tar.TypeBlock
			re.rdev = [2]int64{int64(e.Rdev.Major), int64(e.Rdev.Minor)}
		}
		var xs []string
		for k, v := range e.Xattrs {
			xs = append(xs, k+"="+v)
		}
		re.xattrs = sortedJoin(xs)
		out[p] = re
	}
	return out
}

func sortedJoin(xs []string) string {
	for i := 0; i < len(xs); i++ {
		for j := i + 1; j < len(xs); j++ {
			if xs[j] < xs[i] {
				xs[i], xs[j] = xs[j], xs[i]
			}
		}
	}
	return strings.Join(xs, ",")
}

// unifiedView applies the committed layer over the base stack by the
// read side's own rules and renders the comparable presentation.
func unifiedView(t testing.TB, baseStack []layer.Layer, committed []byte, contents map[string]string) map[string]refEntry {
	t.Helper()
	var cl layer.Layer
	tr := tar.NewReader(bytes.NewReader(committed))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		e := layer.Entry{Header: *hdr}
		if hdr.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			e.Digest = digestOf(string(b))
			contents[e.Digest.Hex] = string(b)
		}
		cl = append(cl, e)
	}
	v, err := layer.Unify(append(append([]layer.Layer{}, baseStack...), cl))
	if err != nil {
		t.Fatalf("committed layer does not unify: %v", err)
	}
	out := map[string]refEntry{}
	for _, e := range v.Entries() {
		p := e.Header.Name
		if p == "." {
			continue
		}
		kind := e.Header.Typeflag
		target := e.Header.Linkname
		if kind == tar.TypeLink {
			// A resolved hardlink is an independent node with captured
			// content; its Linkname is unification bookkeeping, not
			// presentation.
			kind, target = tar.TypeReg, ""
		}
		re := refEntry{
			kind: kind, digest: e.Digest.Hex, target: target,
			mode: e.Header.Mode & 0o7777, uid: e.Header.Uid, gid: e.Header.Gid,
			mtime: presentedNanos(e.Header.ModTime),
			rdev:  [2]int64{e.Header.Devmajor, e.Header.Devminor},
		}
		if kind == tar.TypeSymlink {
			re.digest = ""
		}
		var xs []string
		for k, val := range e.Header.PAXRecords {
			if name, ok := strings.CutPrefix(k, "SCHILY.xattr."); ok && !strings.HasPrefix(name, upper.XattrNS) {
				// The machinery namespace on base content is inert:
				// presented nowhere (REQ-writable-reserved).
				xs = append(xs, name+"="+val)
			}
		}
		re.xattrs = sortedJoin(xs)
		out[p] = re
	}
	return out
}

// TestPropertyCommitRoundTrip is the oracle for REQ-writable-commit:
// for generated bases and upper mutation sequences, unifying the
// committed layer over the base by the read side's own rules
// reproduces exactly the presented merge of (base, upper).
//
// The oracle compares presented merges, so it proves correctness of
// the committed layer but is blind to marker minimality and byte
// shape — a redundant-but-harmless marker round-trips identically.
// Every minimality and elision rule must be pinned by a named
// TestEmissionRules arm.
func TestPropertyCommitRoundTrip(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string]string{}
		// Base: one layer over a fixed alphabet — files (some with a
		// real or machinery xattr), files under an implied directory,
		// optionally a symlink, a hardlink pair, and a device node.
		var bl layer.Layer
		comp := []string{"a", "b", "c"}
		bl = append(bl, bdir("d0", 0o755, baseTime))
		n := rapid.IntRange(1, 5).Draw(rt, "base-n")
		for i := 0; i < n; i++ {
			name := "d0/" + rapid.SampledFrom(comp).Draw(rt, "bn") + fmt.Sprint(i)
			c := rapid.StringOfN(rapid.RuneFrom([]rune("uv")), 0, 3, -1).Draw(rt, "bc")
			contents[digestOf(c).Hex] = c
			e := bfile(name, c, 0o644, baseTime)
			switch rapid.IntRange(0, 3).Draw(rt, "bx") {
			case 1:
				e.Header.PAXRecords = map[string]string{"SCHILY.xattr.user.b": "bv"}
			case 2:
				// Machinery on base content must stay inert
				// (anti-laundering).
				e.Header.PAXRecords = map[string]string{"SCHILY.xattr." + upper.XattrNS + "kind": "char"}
			}
			bl = append(bl, e)
		}
		subN := rapid.IntRange(0, 2).Draw(rt, "sub-n")
		for i := 0; i < subN; i++ {
			c := rapid.StringOfN(rapid.RuneFrom([]rune("st")), 0, 3, -1).Draw(rt, "sc")
			contents[digestOf(c).Hex] = c
			bl = append(bl, bfile("d0/sub/g"+fmt.Sprint(i), c, 0o644, baseTime))
		}
		hasSub := subN > 0
		if rapid.Bool().Draw(rt, "bsym") {
			bl = append(bl, layer.Entry{Header: tar.Header{
				Name: "d0/sl", Typeflag: tar.TypeSymlink, Linkname: "a0",
				Mode: 0o777, ModTime: baseTime, Uid: os.Getuid(), Gid: os.Getgid(),
			}})
		}
		if rapid.Bool().Draw(rt, "blink") {
			c := "linked"
			contents[digestOf(c).Hex] = c
			bl = append(bl, bfile("d0/h1", c, 0o644, baseTime))
			bl = append(bl, layer.Entry{Header: tar.Header{
				Name: "d0/h2", Typeflag: tar.TypeLink, Linkname: "d0/h1",
				Mode: 0o644, ModTime: baseTime, Uid: os.Getuid(), Gid: os.Getgid(),
			}})
		}
		hasDev := rapid.Bool().Draw(rt, "bdev")
		if hasDev {
			bl = append(bl, layer.Entry{Header: tar.Header{
				Name: "d0/dev", Typeflag: tar.TypeChar, Mode: 0o666,
				Devmajor: 1, Devminor: 3, ModTime: baseTime,
			}})
		}
		baseStack := []layer.Layer{bl}
		base, err := layer.Unify(baseStack)
		if err != nil {
			rt.Fatal(err)
		}

		dir := scratchtest.Dir(t, "commit")
		root := filepath.Join(dir, "u")
		if err := os.Mkdir(root, 0o755); err != nil {
			rt.Fatal(err)
		}
		w := upper.NewWriter(root)
		// The provider's copy-up materializes ancestor spines with
		// their presented attributes; at the primitive level the
		// harness does it, so unchanged spine dirs elide from the
		// diff. Returns false when an ancestor is a non-directory
		// upper entry — the caller skips the op.
		spineTime := func(a string) time.Time {
			if be, ok := base.Lookup(a); ok && !be.Header.ModTime.IsZero() {
				return be.Header.ModTime
			}
			if _, ok := base.Lookup(a); ok {
				return time.Unix(0, 0) // implied dir presents epoch
			}
			return baseTime
		}
		ensureSpine := func(p string) bool {
			d := path.Dir(p)
			if d == "." {
				return true
			}
			var parts []string
			for d != "." {
				parts = append([]string{d}, parts...)
				d = path.Dir(d)
			}
			for _, a := range parts {
				if fi, err := os.Lstat(filepath.Join(root, filepath.FromSlash(a))); err == nil {
					if !fi.IsDir() {
						return false
					}
					continue
				}
				if err := w.Mkdir(a, 0o755); err != nil {
					rt.Fatal(err)
				}
				if be, ok := base.Lookup(a); ok {
					if err := w.SetOwner(a, be.Header.Uid, be.Header.Gid); err != nil {
						rt.Fatal(err)
					}
				}
				if err := w.SetTimes(a, spineTime(a)); err != nil {
					rt.Fatal(err)
				}
			}
			return true
		}
		basePaths := make([]string, 0, base.Len())
		for _, e := range base.Entries() {
			if e.Header.Typeflag == tar.TypeReg {
				basePaths = append(basePaths, e.Header.Name)
			}
		}

		seq := 0
		ops := rapid.IntRange(0, 10).Draw(rt, "ops")
		var files []string
		for i := 0; i < ops; i++ {
			seq++
			fresh := fmt.Sprintf("n%d", seq)
			switch rapid.IntRange(0, 12).Draw(rt, "verb") {
			case 0, 1:
				c := rapid.StringOfN(rapid.RuneFrom([]rune("xy")), 0, 3, -1).Draw(rt, "c")
				target := fresh
				if len(basePaths) > 0 && rapid.Bool().Draw(rt, "shadow") {
					target = rapid.SampledFrom(basePaths).Draw(rt, "sp")
					if !ensureSpine(target) {
						continue
					}
					if err := w.Whiteout(target); err != nil {
						rt.Fatal(err)
					}
				}
				if !ensureSpine(target) {
					continue
				}
				var px map[string]string
				if rapid.Bool().Draw(rt, "px") {
					px = map[string]string{"user.p": "pv"}
				}
				if err := w.PublishFile(target, strings.NewReader(c), 0o600, baseTime, px); err != nil {
					rt.Fatal(err)
				}
				files = append(files, target)
			case 2:
				if len(basePaths) == 0 {
					continue
				}
				whp := rapid.SampledFrom(basePaths).Draw(rt, "whp")
				if !ensureSpine(whp) {
					continue
				}
				if err := w.Whiteout(whp); err != nil {
					rt.Fatal(err)
				}
			case 3:
				if err := w.Mkdir(fresh, 0o750); err != nil {
					rt.Fatal(err)
				}
				if err := w.SetTimes(fresh, baseTime); err != nil {
					rt.Fatal(err)
				}
			case 4:
				if err := w.Symlink("/etc/"+fresh, fresh); err != nil {
					rt.Fatal(err)
				}
			case 5:
				var sx map[string]string
				if rapid.Bool().Draw(rt, "sx") {
					sx = map[string]string{"security.capability": "caps"}
				}
				if err := w.MakeStandIn(fresh, upper.KindBlockDev, "", upper.Rdev{Major: 8, Minor: 1}, 0o660, 0, 0, baseTime, sx); err != nil {
					rt.Fatal(err)
				}
			case 6:
				if len(files) == 0 {
					continue
				}
				if err := w.Link(rapid.SampledFrom(files).Draw(rt, "lnk"), fresh); err != nil {
					rt.Fatal(err)
				}
				files = append(files, fresh)
			case 7:
				// Whiteout d0 wholesale, sometimes recreating.
				if err := w.Whiteout("d0"); err != nil {
					rt.Fatal(err)
				}
				if rapid.Bool().Draw(rt, "recreate") {
					if _, err := os.Lstat(filepath.Join(root, "d0")); err != nil {
						if err := w.Mkdir("d0", 0o755); err != nil {
							rt.Fatal(err)
						}
					}
					if err := w.SetTimes("d0", baseTime); err != nil {
						rt.Fatal(err)
					}
				}
			case 8:
				// Opaque d0 or d0/sub — over live base content it has
				// effect; over a recreated-beside-marker dir or under
				// an occluded ancestor it does not; all must
				// round-trip.
				target := "d0"
				if hasSub && rapid.Bool().Draw(rt, "opq-sub") {
					target = "d0/sub"
				}
				if !ensureSpine(target) {
					continue
				}
				if fi, err := os.Lstat(filepath.Join(root, filepath.FromSlash(target))); err != nil {
					if err := w.Mkdir(target, 0o755); err != nil {
						rt.Fatal(err)
					}
				} else if !fi.IsDir() {
					continue
				}
				if err := w.Opaque(target); err != nil {
					rt.Fatal(err)
				}
				if err := w.SetTimes(target, baseTime); err != nil {
					rt.Fatal(err)
				}
			case 9:
				if err := w.Mkfifo(fresh, 0o640); err != nil {
					rt.Fatal(err)
				}
				if rapid.Bool().Draw(rt, "conv") {
					if err := w.ConvertToStandIn(fresh, 0, 0); err != nil {
						rt.Fatal(err)
					}
				}
				if err := w.SetTimes(fresh, baseTime); err != nil {
					rt.Fatal(err)
				}
			case 10:
				// A socket: fresh, or shadowing a base path — with or
				// without its own marker (a caller-built upper can
				// hold a bare socket at a base path).
				target := fresh
				if len(basePaths) > 0 && rapid.Bool().Draw(rt, "sshadow") {
					target = rapid.SampledFrom(basePaths).Draw(rt, "ssp")
					if !ensureSpine(target) {
						continue
					}
					if rapid.Bool().Draw(rt, "smk") {
						if err := w.Whiteout(target); err != nil {
							rt.Fatal(err)
						}
					}
				}
				if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(target))); err == nil {
					continue
				}
				if err := unix.Mknod(filepath.Join(root, filepath.FromSlash(target)), unix.S_IFSOCK|0o755, 0); err != nil {
					rt.Fatal(err)
				}
			case 11:
				// Replace the implied base directory with a file: the
				// non-directory entry finalizes the path and covers
				// the base children (unify drops the subtree).
				if !hasSub {
					continue
				}
				if _, err := os.Lstat(filepath.Join(root, "d0", "sub")); err == nil {
					continue
				}
				if !ensureSpine("d0/sub") {
					continue
				}
				if rapid.Bool().Draw(rt, "dwh") {
					if err := w.Whiteout("d0/sub"); err != nil {
						rt.Fatal(err)
					}
				}
				if err := w.PublishFile("d0/sub", strings.NewReader("df"), 0o600, baseTime, nil); err != nil {
					rt.Fatal(err)
				}
				files = append(files, "d0/sub")
			case 12:
				// Device stand-in over the base device: identical
				// restore elides, a differing major emits.
				if !hasDev {
					continue
				}
				if !ensureSpine("d0/dev") {
					continue
				}
				if _, err := os.Lstat(filepath.Join(root, "d0", "dev")); err == nil {
					continue
				}
				maj := 1
				if rapid.Bool().Draw(rt, "devm") {
					maj = 4
				}
				if err := w.MakeStandIn("d0/dev", upper.KindCharDev, "", upper.Rdev{Major: uint32(maj), Minor: 3}, 0o666, 0, 0, baseTime, nil); err != nil {
					rt.Fatal(err)
				}
			}
		}
		if _, err := os.Lstat(filepath.Join(root, "d0")); err == nil {
			if err := w.SetTimes("d0", baseTime); err != nil {
				rt.Fatal(err)
			}
		}

		st, err := upper.Walk(root)
		if err != nil {
			rt.Fatal(err)
		}
		committed, _, err := LayerBytes(base, st)
		if err != nil {
			rt.Fatal(err)
		}

		got := unifiedView(t, baseStack, committed, map[string]string{})
		want := refMerge(base, st)
		for p, we := range want {
			ge, ok := got[p]
			if !ok {
				rt.Fatalf("path %q: presented but absent from committed unify\n layer paths: %v", p, keys(got))
			}
			if ge != we {
				rt.Fatalf("path %q:\n committed %+v\n presented %+v", p, ge, we)
			}
		}
		for p := range got {
			if _, ok := want[p]; !ok {
				rt.Fatalf("path %q: in committed unify but not presented", p)
			}
		}
	})
}

func keys(m map[string]refEntry) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
