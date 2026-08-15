//go:build linux

package projection

import (
	"archive/tar"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/upper"
)

// presRow is the comparable presentation of one merged path.
type presRow struct {
	kind     Kind
	upperSrc bool
	target   string
	nlink    uint64
}

func kindFromFlag(flag byte) Kind {
	switch flag {
	case tar.TypeDir:
		return KindDir
	case tar.TypeSymlink:
		return KindSymlink
	case tar.TypeFifo:
		return KindFIFO
	case tar.TypeChar:
		return KindCharDevice
	case tar.TypeBlock:
		return KindBlockDevice
	default:
		return KindFile
	}
}

// refPresent computes the presented merge from first principles —
// an independent formulation of REQ-writable-presented, sockets
// included.
func refPresent(view *layer.View, st *upper.State) map[string]presRow {
	occluded := func(p string) bool {
		if st.Whiteouts[p] {
			return true
		}
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			if st.Whiteouts[d] {
				return true
			}
			if st.Opaque[d] {
				if _, ok := st.Entries[d]; ok {
					return true
				}
			}
		}
		return false
	}
	out := map[string]presRow{}
	for _, be := range view.Entries() {
		p := be.Header.Name
		if p == "." || occluded(p) {
			continue
		}
		if _, shadowed := st.Entries[p]; shadowed {
			continue
		}
		covered := false
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			if ue, ok := st.Entries[d]; ok && ue.Kind != upper.KindDir {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		target := be.Header.Linkname
		if be.Header.Typeflag == tar.TypeLink {
			target = ""
		}
		out[p] = presRow{kind: kindFromFlag(be.Header.Typeflag), target: target, nlink: 1}
	}
	for p, ue := range st.Entries {
		out[p] = presRow{kind: kindFromUpper(ue.Kind), upperSrc: true, target: ue.Target, nlink: ue.Nlink}
	}
	return out
}

// TestPropertyMergedPresentation is the oracle for
// REQ-writable-presented and REQ-proj-identity's merge rules: for
// generated bases and upper mutation sequences, walking the merged
// kernel presents exactly the reference merge, with every identity
// drawn from the rule the spec prescribes — view IDs for
// base-presented and shadow-in-place entries, ino-derived
// upper-born IDs for everything else, unique except across a
// hardlink group.
func TestPropertyMergedPresentation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		var bl layer.Layer
		comp := []string{"a", "b", "c"}
		bl = append(bl, ldir("d0"))
		n := rapid.IntRange(1, 4).Draw(rt, "base-n")
		for i := 0; i < n; i++ {
			bl = append(bl, lfile("d0/"+rapid.SampledFrom(comp).Draw(rt, "bn")+fmt.Sprint(i)))
		}
		subN := rapid.IntRange(0, 2).Draw(rt, "sub-n")
		for i := 0; i < subN; i++ {
			bl = append(bl, lfile("d0/sub/g"+fmt.Sprint(i)))
		}
		hasSub := subN > 0
		if rapid.Bool().Draw(rt, "bsym") {
			bl = append(bl, lsymlink("d0/sl", "a0"))
		}
		view, err := layer.Unify([]layer.Layer{bl})
		if err != nil {
			rt.Fatal(err)
		}
		inner := mustNew(t, view, nil, capsFull)

		dir := scratchtest.Dir(t, "projection")
		root := filepath.Join(dir, "u")
		if err := os.Mkdir(root, 0o755); err != nil {
			rt.Fatal(err)
		}
		w := upper.NewWriter(root)

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
			}
			return true
		}
		var basePaths []string
		for _, e := range view.Entries() {
			if e.Header.Typeflag == tar.TypeReg {
				basePaths = append(basePaths, e.Header.Name)
			}
		}
		// Whiteout targets cover the wider base surface: files,
		// the symlink, and the implied subdirectory.
		whTargets := append([]string{}, basePaths...)
		for _, e := range view.Entries() {
			if e.Header.Typeflag == tar.TypeSymlink || e.Header.Name == "d0/sub" {
				whTargets = append(whTargets, e.Header.Name)
			}
		}

		seq := 0
		ops := rapid.IntRange(0, 8).Draw(rt, "ops")
		var files []string
		for i := 0; i < ops; i++ {
			seq++
			fresh := fmt.Sprintf("n%d", seq)
			switch rapid.IntRange(0, 12).Draw(rt, "verb") {
			case 0, 1:
				target := fresh
				if len(basePaths) > 0 && rapid.Bool().Draw(rt, "shadow") {
					target = rapid.SampledFrom(basePaths).Draw(rt, "sp")
					if !ensureSpine(target) {
						continue
					}
					if rapid.Bool().Draw(rt, "mk") {
						if err := w.Whiteout(target); err != nil {
							rt.Fatal(err)
						}
					}
				}
				if !ensureSpine(target) {
					continue
				}
				if fi, err := os.Lstat(filepath.Join(root, filepath.FromSlash(target))); err == nil && fi.IsDir() {
					continue // rename-over cannot replace a directory
				}
				if err := w.PublishFile(target, strings.NewReader("x"), 0o600, upTime, nil); err != nil {
					rt.Fatal(err)
				}
				files = append(files, target)
			case 2:
				whp := rapid.SampledFrom(whTargets).Draw(rt, "whp")
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
			case 4:
				if err := w.Symlink("/etc/"+fresh, fresh); err != nil {
					rt.Fatal(err)
				}
			case 5:
				if err := w.MakeStandIn(fresh, upper.KindBlockDev, "", upper.Rdev{Major: 8, Minor: 1}, 0o660, 0, 0, upTime, nil); err != nil {
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
				if err := w.Whiteout("d0"); err != nil {
					rt.Fatal(err)
				}
				if rapid.Bool().Draw(rt, "recreate") {
					if _, err := os.Lstat(filepath.Join(root, "d0")); err != nil {
						if err := w.Mkdir("d0", 0o755); err != nil {
							rt.Fatal(err)
						}
					}
				}
			case 8:
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
			case 9:
				if err := w.Mkfifo(fresh, 0o640); err != nil {
					rt.Fatal(err)
				}
				if rapid.Bool().Draw(rt, "fifolink") {
					// A non-file hardlink pair: one inode, one ID
					// (REQ-writable-hardlink has no kind
					// restriction).
					if err := w.Link(fresh, fresh+"l"); err != nil {
						rt.Fatal(err)
					}
				}
			case 10:
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
				// Replace the implied base directory with a file:
				// kind-change shadow, base children covered.
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
				if err := w.PublishFile("d0/sub", strings.NewReader("df"), 0o600, upTime, nil); err != nil {
					rt.Fatal(err)
				}
				files = append(files, "d0/sub")
			case 12:
				// An upper directory over a base FILE path: the other
				// kind-change shadow.
				if len(basePaths) == 0 {
					continue
				}
				target := rapid.SampledFrom(basePaths).Draw(rt, "mdp")
				if !ensureSpine(target) {
					continue
				}
				if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(target))); err == nil {
					continue
				}
				if err := w.Mkdir(target, 0o755); err != nil {
					rt.Fatal(err)
				}
			}
		}

		m, err := NewMerged(inner, root)
		if err != nil {
			rt.Fatal(err)
		}
		st := m.index().st
		want := refPresent(view, st)

		// occludedForID mirrors the oracle's occlusion for the ID
		// class judgment only.
		occludedForID := func(p string) bool {
			if st.Whiteouts[p] {
				return true
			}
			for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
				if st.Whiteouts[d] {
					return true
				}
				if st.Opaque[d] {
					if _, ok := st.Entries[d]; ok {
						return true
					}
				}
			}
			return false
		}
		readIDs := map[string]ID{}
		var walkRead func(e *Entry)
		walkRead = func(e *Entry) {
			for _, c := range e.Children() {
				readIDs[c.Path()] = c.ID()
				walkRead(c)
			}
		}
		walkRead(mustNew(t, view, nil, capsFull).Root())

		collect := func(mm *Merged) (map[string]presRow, map[string]ID) {
			rows := map[string]presRow{}
			ids := map[string]ID{}
			var walk func(dir *Node)
			walk = func(dir *Node) {
				snap, err := mm.OpenDir(dir)
				if err != nil {
					rt.Fatal(err)
				}
				for i := 0; i < snap.Len(); i++ {
					row := snap.At(i)
					n, ok, err := mm.Lookup(dir, row.Name)
					if err != nil || !ok {
						rt.Fatalf("enumerated %q under %q not resolvable: %v %v", row.Name, dir.Path(), ok, err)
					}
					if n.ID() != row.ID || n.Kind() != row.Kind {
						rt.Fatalf("%q: snapshot row (%d,%v) vs node (%d,%v)", n.Path(), row.ID, row.Kind, n.ID(), n.Kind())
					}
					p := n.Path()
					rows[p] = presRow{kind: n.Kind(), upperSrc: n.UpperBacked(), target: n.LinkTarget(), nlink: n.Nlink()}
					ids[p] = n.ID()
					if n.Kind() == KindDir {
						walk(n)
					}
					n.Close()
				}
			}
			walk(mm.Root())
			return rows, ids
		}
		got, ids := collect(m)

		// Identity class and uniqueness (REQ-proj-identity).
		owner := map[ID]string{}
		for p, id := range ids {
			if got[p].upperSrc {
				ue := st.Entries[p]
				upperBorn := occludedForID(p) ||
					(ue.Kind != upper.KindDir && ue.Nlink > 1)
				if _, inView := readIDs[p]; !inView {
					upperBorn = true
				}
				if upperBorn {
					wantID := upperIDBase | ID(ue.Ino)
					if id != wantID {
						rt.Fatalf("%q: upper-born ID %d, want %d", p, id, wantID)
					}
				} else if id != readIDs[p] {
					rt.Fatalf("%q: shadow-in-place ID %d, want view %d", p, id, readIDs[p])
				}
			} else if id != readIDs[p] {
				rt.Fatalf("%q: base ID %d, want view %d", p, id, readIDs[p])
			}
			if prev, taken := owner[id]; taken {
				pue, pok := st.Entries[prev]
				cue, cok := st.Entries[p]
				if !(pok && cok && pue.Ino == cue.Ino) {
					rt.Fatalf("ID %d shared by %q and %q without a shared inode", id, prev, p)
				}
			}
			owner[id] = p
		}

		for p, wr := range want {
			gr, ok := got[p]
			if !ok {
				rt.Fatalf("path %q: presented by oracle, absent from merge", p)
			}
			if gr != wr {
				rt.Fatalf("path %q:\n merged    %+v\n reference %+v", p, gr, wr)
			}
		}
		for p := range got {
			if _, ok := want[p]; !ok {
				rt.Fatalf("path %q: merged presents it, oracle does not", p)
			}
		}

		// Upper truth (REQ-proj-upper-truth): a kernel rebuilt from
		// the on-disk state alone — a fresh walk, no carried memory
		// — presents the identical tree with identical identities.
		m2, err := NewMerged(inner, root)
		if err != nil {
			rt.Fatal(err)
		}
		got2, ids2 := collect(m2)
		if !maps.Equal(got, got2) || !maps.Equal(ids, ids2) {
			rt.Fatalf("kernel rebuilt from disk diverges:\n first %v\n rebuilt %v", ids, ids2)
		}
	})
}
