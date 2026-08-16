//go:build linux

package projection

import (
	"errors"
	"sort"
	"fmt"
	"strings"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/upper"
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
	nlink   int
	xattrs  string
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
		model := map[string]*modelEnt{}
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
				me := &modelEnt{kind: n.Kind(), mode: uint32(h.Mode) & 0o7777, uid: h.Uid, gid: h.Gid, target: n.LinkTarget()}
				if n.Kind() != KindDir {
					// Directory link counts are host-filesystem noise
					// (btrfs 1, ext4 2+len); files' counts are
					// presented truth.
					me.nlink = int(n.Nlink())
				}
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
			switch rapid.IntRange(0, 13).Draw(rt, "verb") {
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
				model[p] = &modelEnt{kind: KindFile, content: content, mode: 0o640, uid: myUID, gid: myGID, nlink: 1}
			case 1: // mkdir
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "md")
				dn := dirNode(dp)
				n, err := m.Mkdir(dn, fresh, 0o750)
				dn.Close()
				if err != nil {
					rt.Fatalf("mkdir: %v", err)
				}
				model[n.Path()] = &modelEnt{kind: KindDir, mode: 0o750, uid: myUID, gid: myGID}
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
				model[n.Path()] = &modelEnt{kind: KindSymlink, target: "/t/" + fresh, mode: 0o777, uid: myUID, gid: myGID, nlink: 1}
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
				model[p].nlink--
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
			case 12: // rename ONTO an existing file (replacement)
				var files2, all []string
				for p, e := range model {
					all = append(all, p)
					if e.kind == KindFile {
						files2 = append(files2, p)
					}
				}
				if len(files2) == 0 || len(all) == 0 {
					continue
				}
				sp := rapid.SampledFrom(all).Draw(rt, "osp")
				dp := rapid.SampledFrom(files2).Draw(rt, "odp")
				if sp == dp || strings.HasPrefix(dp, sp+"/") || model[sp].kind == KindDir {
					continue
				}
				sd, sb := splitParent(sp)
				dd, db := splitParent(dp)
				sdn := dirNode(sd)
				ddn := dirNode(dd)
				err := m.Rename(sdn, sb, ddn, db)
				sdn.Close()
				ddn.Close()
				if errors.Is(err, ErrCrossDevice) {
					continue
				}
				if err != nil {
					rt.Fatalf("replace-rename %q->%q: %v", sp, dp, err)
				}
				if model[sp] == model[dp] {
					// Same-inode rename: successful no-op.
					continue
				}
				model[dp].nlink--
				model[dp] = model[sp]
				delete(model, sp)
			case 13: // setxattr / removexattr on a file
				var files3 []string
				for p, e := range model {
					if e.kind == KindFile {
						files3 = append(files3, p)
					}
				}
				if len(files3) == 0 {
					continue
				}
				p := rapid.SampledFrom(files3).Draw(rt, "xp")
				n, err := m.NodeAt(p)
				if err != nil {
					rt.Fatal(err)
				}
				e := model[p]
				if rapid.Bool().Draw(rt, "xset") {
					val := rapid.StringOfN(rapid.RuneFrom([]rune("pq")), 1, 3, -1).Draw(rt, "xv")
					err = m.SetXattr(n, "user.px", []byte(val), 0)
					if err == nil {
						e.xattrs = "user.px=" + val
					}
				} else if e.xattrs != "" {
					err = m.RemoveXattr(n, "user.px")
					if err == nil {
						e.xattrs = ""
					}
				} else {
					err = nil
				}
				n.Close()
				if err != nil {
					rt.Fatalf("xattr %q: %v", p, err)
				}
			case 9: // rename (attempt; EXDEV and subtree refusals skip)
				var cands []string
				for p := range model {
					cands = append(cands, p)
				}
				if len(cands) == 0 {
					continue
				}
				sp := rapid.SampledFrom(cands).Draw(rt, "rsp")
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "rdp")
				if dp == sp || strings.HasPrefix(dp+"/", sp+"/") {
					continue
				}
				target := childPath(dp, fresh)
				sd, sb := splitParent(sp)
				sdn := dirNode(sd)
				ddn := dirNode(dp)
				err := m.Rename(sdn, sb, ddn, fresh)
				sdn.Close()
				ddn.Close()
				if errors.Is(err, ErrCrossDevice) {
					continue
				}
				if err != nil {
					rt.Fatalf("rename %q->%q: %v", sp, target, err)
				}
				if old, had := model[target]; had {
					old.nlink--
				}
				moved := map[string]*modelEnt{}
				for p, e := range model {
					if p == sp {
						moved[target] = e
						continue
					}
					if strings.HasPrefix(p, sp+"/") {
						moved[target+p[len(sp):]] = e
						continue
					}
					moved[p] = e
				}
				model = moved
			case 10: // hardlink a file
				var cands []string
				for p, e := range model {
					if e.kind == KindFile {
						cands = append(cands, p)
					}
				}
				if len(cands) == 0 {
					continue
				}
				tp := rapid.SampledFrom(cands).Draw(rt, "ltp")
				tn, err := m.NodeAt(tp)
				if err != nil {
					rt.Fatal(err)
				}
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "ldp")
				ddn := dirNode(dp)
				ln, err := m.Link(tn, ddn, fresh)
				tn.Close()
				ddn.Close()
				if err != nil {
					rt.Fatalf("link %q: %v", tp, err)
				}
				model[tp].nlink++
				model[ln.Path()] = model[tp]
				ln.Close()
			case 11: // mknod fifo
				dirs := presentedDirs()
				dp := rapid.SampledFrom(dirs).Draw(rt, "fdp")
				ddn := dirNode(dp)
				n, err := m.Mknod(ddn, fresh, KindFIFO, 0o640, upper.Rdev{})
				ddn.Close()
				if err != nil {
					rt.Fatalf("mknod: %v", err)
				}
				model[n.Path()] = &modelEnt{kind: KindFIFO, mode: 0o640, uid: myUID, gid: myGID, nlink: 1}
				n.Close()
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
					if n.Kind() != KindDir {
						ge.nlink = int(n.Nlink())
					}
					if xs := n.Xattrs(); len(xs) > 0 {
						var parts []string
						for k, v := range xs {
							parts = append(parts, k+"="+v)
						}
						sort.Strings(parts)
						ge.xattrs = strings.Join(parts, ",")
					}
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
				if g != *we {
					rt.Fatalf("%s: %q diverged:\n presented %+v\n model     %+v", label, p, g, *we)
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
