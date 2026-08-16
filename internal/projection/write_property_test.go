//go:build linux

package projection

import (
	"fmt"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// modelEnt is the plain-filesystem model of one presented entry —
// what a POSIX user sees, with no upper/base distinction: the write
// engine's whole point is that the split is invisible.
type modelEnt struct {
	kind    Kind
	content string
	mode    uint32
	uid     int
	gid     int
	target  string
}

// TestPropertyWriteEngine drives generated operation sequences
// through the write engine and a plain in-memory filesystem model
// in lockstep: the presented merge must equal the model at every
// end state, and a kernel rebuilt from the on-disk upper alone must
// present identically (REQ-writable-copyup/-delete/-presented,
// REQ-proj-upper-truth).
func TestPropertyWriteEngine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	myUID, myGID := os.Getuid(), os.Getgid()
	rapid.Check(t, func(rt *rapid.T) {
		inner, cas := baseFixture(t)
		root, _ := newUpperFor(t)
		m := mustWritable(t, inner, root, cas)

		// Seed the model from the pre-op presentation (already
		// pinned by the merge property).
		model := map[string]modelEnt{}
		var seed func(dir *Node)
		seed = func(dir *Node) {
			snap, err := m.OpenDir(dir)
			if err != nil {
				rt.Fatal(err)
			}
			for i := 0; i < snap.Len(); i++ {
				n, ok, err := m.Lookup(dir, snap.At(i).Name)
				if err != nil || !ok {
					rt.Fatalf("seed resolve: %v %v", ok, err)
				}
				h := n.Header()
				me := modelEnt{kind: n.Kind(), mode: uint32(h.Mode) & 0o7777, uid: h.Uid, gid: h.Gid, target: n.LinkTarget()}
				if n.Kind() == KindFile {
					me.content = cas[n.ContentDigest().Hex]
				}
				model[n.Path()] = me
				if n.Kind() == KindDir {
					seed(n)
				}
				n.Close()
			}
		}
		seed(m.Root())

		presentedDirs := func() []string {
			out := []string{"."}
			for p, e := range model {
				if e.kind == KindDir {
					out = append(out, p)
				}
			}
			return out
		}
		childrenOf := func(dir string) []string {
			var out []string
			for p := range model {
				if d, _ := splitParent(p); d == dir {
					out = append(out, p)
				}
			}
			return out
		}
		dirNode := func(p string) *Node {
			n, err := m.NodeAt(p)
			if err != nil {
				rt.Fatalf("nodeAt %q: %v", p, err)
			}
			return n
		}

		seq := 0
		ops := rapid.IntRange(0, 12).Draw(rt, "ops")
		for i := 0; i < ops; i++ {
			seq++
			fresh := fmt.Sprintf("w%d", seq)
			switch rapid.IntRange(0, 8).Draw(rt, "verb") {
			case 0: // create + write + flush
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "cd")
				dn := dirNode(dp)
				content := rapid.StringOfN(rapid.RuneFrom([]rune("ab")), 0, 4, -1).Draw(rt, "cc")
				n, f, err := m.Create(dn, fresh, 0o640)
				dn.Close()
				if err != nil {
					rt.Fatalf("create: %v", err)
				}
				if _, err := f.WriteString(content); err != nil {
					rt.Fatal(err)
				}
				f.Close()
				p := n.Path()
				n.Close()
				if err := m.Flushed(p); err != nil {
					rt.Fatal(err)
				}
				model[p] = modelEnt{kind: KindFile, content: content, mode: 0o640, uid: myUID, gid: myGID}
			case 1: // mkdir
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "md")
				dn := dirNode(dp)
				n, err := m.Mkdir(dn, fresh, 0o750)
				dn.Close()
				if err != nil {
					rt.Fatalf("mkdir: %v", err)
				}
				model[n.Path()] = modelEnt{kind: KindDir, mode: 0o750, uid: myUID, gid: myGID}
				n.Close()
			case 2: // symlink
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "sd")
				dn := dirNode(dp)
				n, err := m.Symlink(dn, fresh, "/t/"+fresh)
				dn.Close()
				if err != nil {
					rt.Fatalf("symlink: %v", err)
				}
				model[n.Path()] = modelEnt{kind: KindSymlink, target: "/t/" + fresh, mode: 0o777, uid: myUID, gid: myGID}
				n.Close()
			case 3: // unlink a presented non-dir
				var cands []string
				for p, e := range model {
					if e.kind != KindDir {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "up")
				d, b := splitParent(p)
				dn := dirNode(d)
				err := m.Unlink(dn, b)
				dn.Close()
				if err != nil {
					rt.Fatalf("unlink %q: %v", p, err)
				}
				delete(model, p)
			case 4: // rmdir (may be non-empty)
				var cands []string
				for p, e := range model {
					if e.kind == KindDir {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "rp")
				empty := len(childrenOf(p)) == 0
				d, b := splitParent(p)
				dn := dirNode(d)
				err := m.Rmdir(dn, b)
				dn.Close()
				if empty {
					if err != nil {
						rt.Fatalf("rmdir empty %q: %v", p, err)
					}
					delete(model, p)
				} else if err != ErrNotEmpty {
					rt.Fatalf("rmdir non-empty %q: %v", p, err)
				}
			case 5: // overwrite via OpenWrite
				var cands []string
				for p, e := range model {
					if e.kind == KindFile {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "wp")
				n, err := m.NodeAt(p)
				if err != nil {
					rt.Fatal(err)
				}
				nn, f, err := m.OpenWrite(n)
				n.Close()
				if err != nil {
					rt.Fatalf("openwrite %q: %v", p, err)
				}
				add := rapid.StringOfN(rapid.RuneFrom([]rune("xy")), 1, 3, -1).Draw(rt, "wa")
				if _, err := f.WriteAt([]byte(add), 0); err != nil {
					rt.Fatal(err)
				}
				f.Close()
				nn.Close()
				if err := m.Flushed(p); err != nil {
					rt.Fatal(err)
				}
				e := model[p]
				c := []byte(e.content)
				for j := 0; j < len(add); j++ {
					if j < len(c) {
						c[j] = add[j]
					} else {
						c = append(c, add[j])
					}
				}
				e.content = string(c)
				model[p] = e
			case 6: // truncate
				var cands []string
				for p, e := range model {
					if e.kind == KindFile {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "tp")
				size := rapid.IntRange(0, 6).Draw(rt, "ts")
				n, err := m.NodeAt(p)
				if err != nil {
					rt.Fatal(err)
				}
				err = m.Truncate(n, int64(size))
				n.Close()
				if err != nil {
					rt.Fatalf("truncate %q: %v", p, err)
				}
				e := model[p]
				c := e.content
				for len(c) < size {
					c += "\x00"
				}
				e.content = c[:size]
				model[p] = e
			case 7: // chmod or chown
				var cands []string
				for p := range model {
					if model[p].kind != KindSymlink {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "ap")
				n, err := m.NodeAt(p)
				if err != nil {
					rt.Fatal(err)
				}
				e := model[p]
				if rapid.Bool().Draw(rt, "chm") {
					mode := uint32(rapid.IntRange(0, 0o777).Draw(rt, "am"))
					err = m.SetMode(n, mode)
					e.mode = mode
				} else {
					err = m.SetOwner(n, 0, 0)
					e.uid, e.gid = 0, 0
					e.mode &^= 0o6000
				}
				n.Close()
				if err != nil {
					rt.Fatalf("setattr %q: %v", p, err)
				}
				model[p] = e
			case 8: // settimes (values checked by example tests; here
				// it only must not perturb the model-visible fields)
				var cands []string
				for p := range model {
					cands = append(cands, p)
				}
				if len(cands) == 0 {
					continue
				}
				p := rapid.SampledFrom(cands).Draw(rt, "mp")
				if model[p].kind == KindSymlink {
					continue
				}
				n, err := m.NodeAt(p)
				if err != nil {
					rt.Fatal(err)
				}
				err = m.SetTimes(n, time.Unix(1000, 0))
				n.Close()
				if err != nil {
					rt.Fatalf("settimes %q: %v", p, err)
				}
			}
		}

		compare := func(mm *Merged, label string) {
			got := map[string]modelEnt{}
			var walk func(dir *Node)
			walk = func(dir *Node) {
				snap, err := mm.OpenDir(dir)
				if err != nil {
					rt.Fatal(err)
				}
				for i := 0; i < snap.Len(); i++ {
					n, ok, err := mm.Lookup(dir, snap.At(i).Name)
					if err != nil || !ok {
						rt.Fatalf("%s resolve %q: %v %v", label, snap.At(i).Name, ok, err)
					}
					h := n.Header()
					ge := modelEnt{kind: n.Kind(), mode: uint32(h.Mode) & 0o7777, uid: h.Uid, gid: h.Gid, target: n.LinkTarget()}
					if n.Kind() == KindFile {
						if n.UpperBacked() {
							b, err := os.ReadFile(n.HostPath())
							if err != nil {
								rt.Fatal(err)
							}
							ge.content = string(b)
						} else {
							ge.content = cas[n.ContentDigest().Hex]
						}
					}
					got[n.Path()] = ge
					if n.Kind() == KindDir {
						walk(n)
					}
					n.Close()
				}
			}
			walk(mm.Root())
			for p, we := range model {
				g, ok := got[p]
				if !ok {
					rt.Fatalf("%s: %q in model, not presented", label, p)
				}
				if g != we {
					rt.Fatalf("%s: %q diverged:\n presented %+v\n model     %+v", label, p, g, we)
				}
			}
			for p := range got {
				if _, ok := model[p]; !ok {
					rt.Fatalf("%s: %q presented, not in model", label, p)
				}
			}
		}
		compare(m, "live")

		// Upper truth: a kernel rebuilt from disk alone agrees.
		m2, err := NewMergedWritable(inner, root, cas.open)
		if err != nil {
			rt.Fatal(err)
		}
		compare(m2, "rebuilt")
	})
}
