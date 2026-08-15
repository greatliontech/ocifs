//go:build linux

package upper

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// A generated mutation, replayable onto any root so the crash
// property can rebuild identical pre-states.
type genOp struct {
	verb    string
	a, b    string // paths / names
	mode    uint32
	kind    Kind
	uid     int
	content string
}

func applyOp(w *Writer, op genOp) error {
	switch op.verb {
	case "pubfile":
		return w.PublishFile(op.a, strings.NewReader(op.content), op.mode, fixedTime, nil)
	case "mkdir":
		return w.Mkdir(op.a, op.mode)
	case "symlink":
		return w.Symlink(op.b, op.a)
	case "mkfifo":
		return w.Mkfifo(op.a, op.mode)
	case "standin":
		return w.MakeStandIn(op.a, op.kind, op.b, Rdev{Major: 8, Minor: 1}, op.mode, op.uid, op.uid, fixedTime, nil)
	case "whiteout":
		return w.Whiteout(op.a)
	case "opaque":
		return w.Opaque(op.a)
	case "remove":
		return w.Remove(op.a)
	case "rename":
		return w.Rename(op.a, op.b)
	case "link":
		return w.Link(op.a, op.b)
	case "chown":
		return w.SetOwner(op.a, op.uid, op.uid)
	case "escxattr":
		return w.SetEscapedXattr(op.a, "security.capability", []byte(op.content))
	case "convert":
		return w.ConvertToStandIn(op.a, op.uid, op.uid)
	}
	panic("unknown verb " + op.verb)
}

// model mirrors the dialect state the op sequence must produce.
type model struct {
	entries   map[string]viewEntry
	whiteouts map[string]bool
	opaque    map[string]bool
	dirs      []string // live dir paths, "" = root
	files     []string
	specials  []string // native symlink/fifo paths
	nameSeq   int      // per-iteration, so names are draw-deterministic
	// group tracks hardlink groups: an inode-affecting mutation on
	// one path lands on every path sharing the inode.
	group   map[string]int
	members map[int][]string
	nextGrp int
}

func newModel() *model {
	return &model{
		entries: map[string]viewEntry{}, whiteouts: map[string]bool{}, opaque: map[string]bool{},
		dirs: []string{""}, group: map[string]int{}, members: map[int][]string{},
	}
}

func (m *model) newGroup(p string) {
	m.nextGrp++
	m.group[p] = m.nextGrp
	m.members[m.nextGrp] = []string{p}
}

func (m *model) joinGroup(of, p string) {
	g := m.group[of]
	m.group[p] = g
	m.members[g] = append(m.members[g], p)
}

func (m *model) leaveGroup(p string) {
	g, ok := m.group[p]
	if !ok {
		return
	}
	delete(m.group, p)
	m.members[g] = remove(m.members[g], p)
}

// eachLinked applies f to p and every path sharing its inode.
func (m *model) eachLinked(p string, f func(q string)) {
	if g, ok := m.group[p]; ok {
		for _, q := range m.members[g] {
			f(q)
		}
		return
	}
	f(p)
}

func (m *model) view() map[string]viewEntry {
	out := map[string]viewEntry{}
	for p, e := range m.entries {
		out[p] = e
	}
	for p := range m.whiteouts {
		out["wh:"+p] = viewEntry{}
	}
	for p := range m.opaque {
		out["opq:"+p] = viewEntry{}
	}
	return out
}

func (m *model) apply(op genOp, myUID, myGID int) {
	switch op.verb {
	case "pubfile":
		// Publishing over a linked path replaces that path's inode.
		m.leaveGroup(op.a)
		m.entries[op.a] = viewEntry{kind: KindFile, mode: op.mode & 0o7777, uid: myUID, gid: myGID, content: op.content, size: int64(len(op.content))}
		if !contains(m.files, op.a) {
			m.files = append(m.files, op.a)
		}
		m.newGroup(op.a)
	case "mkdir":
		m.entries[op.a] = viewEntry{kind: KindDir, mode: op.mode & 0o7777, uid: myUID, gid: myGID}
		m.dirs = append(m.dirs, op.a)
	case "symlink":
		m.entries[op.a] = viewEntry{kind: KindSymlink, mode: 0o777, uid: myUID, gid: myGID, target: op.b, size: int64(len(op.b))}
		m.specials = append(m.specials, op.a)
	case "mkfifo":
		m.entries[op.a] = viewEntry{kind: KindFifo, mode: op.mode & 0o7777, uid: myUID, gid: myGID}
		m.specials = append(m.specials, op.a)
	case "standin":
		e := viewEntry{kind: op.kind, mode: op.mode & 0o7777, uid: op.uid, gid: op.uid, standIn: true}
		if op.kind == KindSymlink {
			e.target = op.b
			e.size = int64(len(op.b))
		}
		m.entries[op.a] = e
	case "whiteout":
		m.whiteouts[op.a] = true
	case "opaque":
		m.opaque[op.a] = true
	case "remove":
		delete(m.entries, op.a)
		m.files = remove(m.files, op.a)
		m.leaveGroup(op.a)
	case "rename":
		// Same-inode rename is a kernel no-op: both paths remain.
		if ga, aok := m.group[op.a]; aok {
			if gb, bok := m.group[op.b]; bok && ga == gb {
				break
			}
		}
		// A replaced destination loses its inode membership first.
		m.leaveGroup(op.b)
		m.entries[op.b] = m.entries[op.a]
		delete(m.entries, op.a)
		m.files = remove(m.files, op.a)
		if !contains(m.files, op.b) {
			m.files = append(m.files, op.b)
		}
		if g, ok := m.group[op.a]; ok {
			m.leaveGroup(op.a)
			m.group[op.b] = g
			m.members[g] = append(m.members[g], op.b)
		}
	case "link":
		m.entries[op.b] = m.entries[op.a]
		m.files = append(m.files, op.b)
		m.joinGroup(op.a, op.b)
	case "chown":
		m.eachLinked(op.a, func(q string) {
			e := m.entries[q]
			e.uid, e.gid = op.uid, op.uid
			e.mode &^= 0o6000
			m.entries[q] = e
		})
	case "escxattr":
		m.eachLinked(op.a, func(q string) {
			e := m.entries[q]
			if e.xattrs == "" {
				e.xattrs = "security.capability=" + op.content
			}
			m.entries[q] = e
		})
	case "convert":
		e := m.entries[op.a]
		e.standIn = true
		e.uid, e.gid = op.uid, op.uid
		if e.kind == KindFifo {
			e.size = 0
		}
		m.entries[op.a] = e
		m.specials = remove(m.specials, op.a)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func remove(s []string, v string) []string {
	out := s[:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func freshName(rt *rapid.T, m *model) (dir, full string) {
	dir = rapid.SampledFrom(m.dirs).Draw(rt, "dir")
	m.nameSeq++
	full = path.Join(dir, fmt.Sprintf("%s%d", rapid.SampledFrom([]string{"a", "b", "c"}).Draw(rt, "stem"), m.nameSeq))
	return dir, full
}

// genOps draws exactly n valid ops against the model, retrying
// draws whose preconditions the model cannot meet.
func genOps(rt *rapid.T, m *model, n int, myUID, myGID int) []genOp {
	var ops []genOp
	for len(ops) < n {
		var op genOp
		switch rapid.IntRange(0, 15).Draw(rt, "verb") {
		case 0, 1:
			_, full := freshName(rt, m)
			op = genOp{verb: "pubfile", a: full, mode: rapid.SampledFrom([]uint32{0o644, 0o600, 0o4755, 0o2755}).Draw(rt, "fm"), content: rapid.StringOfN(rapid.RuneFrom([]rune("xy")), 0, 3, -1).Draw(rt, "c")}
		case 2:
			_, full := freshName(rt, m)
			op = genOp{verb: "mkdir", a: full, mode: rapid.SampledFrom([]uint32{0o755, 0o700, 0o1777}).Draw(rt, "dm")}
		case 3:
			_, full := freshName(rt, m)
			op = genOp{verb: "symlink", a: full, b: "/" + full}
		case 4:
			_, full := freshName(rt, m)
			op = genOp{verb: "mkfifo", a: full, mode: 0o600}
		case 5:
			_, full := freshName(rt, m)
			op = genOp{verb: "standin", a: full, kind: rapid.SampledFrom([]Kind{KindCharDev, KindBlockDev, KindSymlink}).Draw(rt, "sk"), b: "st-target", mode: 0o666, uid: 12345}
		case 6:
			dir, _ := freshName(rt, m)
			op = genOp{verb: "whiteout", a: path.Join(dir, "victim")}
		case 7:
			op = genOp{verb: "opaque", a: rapid.SampledFrom(m.dirs).Draw(rt, "od")}
			if op.a == "" {
				continue // the root has no opaque marker home
			}
		case 8:
			if len(m.files) == 0 {
				continue
			}
			op = genOp{verb: "remove", a: rapid.SampledFrom(m.files).Draw(rt, "rm")}
		case 9:
			if len(m.files) == 0 {
				continue
			}
			_, full := freshName(rt, m)
			op = genOp{verb: "rename", a: rapid.SampledFrom(m.files).Draw(rt, "rn"), b: full}
		case 10:
			if len(m.files) == 0 {
				continue
			}
			_, full := freshName(rt, m)
			op = genOp{verb: "link", a: rapid.SampledFrom(m.files).Draw(rt, "ln"), b: full}
		case 11:
			if len(m.files) == 0 || os.Geteuid() == 0 {
				continue
			}
			op = genOp{verb: "chown", a: rapid.SampledFrom(m.files).Draw(rt, "co"), uid: 10000 + rapid.IntRange(0, 3).Draw(rt, "cu")}
		case 12:
			if len(m.files) == 0 {
				continue
			}
			op = genOp{verb: "escxattr", a: rapid.SampledFrom(m.files).Draw(rt, "ex"), content: "cap"}
		case 13:
			if len(m.specials) == 0 || os.Geteuid() == 0 {
				continue
			}
			op = genOp{verb: "convert", a: rapid.SampledFrom(m.specials).Draw(rt, "cv"), uid: 12345}
		case 14:
			// Rename onto an EXISTING file: the replace arm.
			if len(m.files) < 2 {
				continue
			}
			src := rapid.SampledFrom(m.files).Draw(rt, "rrs")
			dst := rapid.SampledFrom(m.files).Draw(rt, "rrd")
			if src == dst {
				continue
			}
			op = genOp{verb: "rename", a: src, b: dst}
		case 15:
			// Publish over an existing file: the inode-replace arm.
			if len(m.files) == 0 {
				continue
			}
			op = genOp{verb: "pubfile", a: rapid.SampledFrom(m.files).Draw(rt, "pr"), mode: 0o600, content: "replaced"}
		}
		ops = append(ops, op)
		m.apply(op, os.Getuid(), os.Getgid())
	}
	return ops
}

// TestPropertyWriterWalkRoundTrip pins REQ-writable-dialect both
// ways: whatever valid mutation sequence the writer applies, the
// walker reads back exactly the modeled state — machinery invisible,
// overrides resolved, markers in place.
func TestPropertyWriterWalkRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir := scratchtest.Dir(t, "upper")
		root := filepath.Join(dir, "u")
		if err := os.Mkdir(root, 0o755); err != nil {
			rt.Fatal(err)
		}
		w := NewWriter(root)
		m := newModel()
		for _, op := range genOps(rt, m, rapid.IntRange(1, 12).Draw(rt, "n"), os.Getuid(), os.Getgid()) {
			if err := applyOp(w, op); err != nil {
				rt.Fatalf("op %+v: %v", op, err)
			}
		}
		got := mustWalkView(t, root)
		want := m.view()
		if !sameView(got, want) {
			rt.Fatalf("walk disagrees with model:\n got %v\nwant %v", got, want)
		}
	})
}

var errCrash = errors.New("injected crash")

// TestPropertyCrashPrefixIsValidDialect pins REQ-writable-crash at
// the primitive seam: aborting any mutation at any of its real
// atomic steps leaves a tree the walker reads without error, whose
// state is the pre-state, the post-state, or a declared
// intermediate (the privilege-reducing cleared-mode-old-owner arm of
// an override chown; orphaned temporaries are invisible by
// construction).
func TestPropertyCrashPrefixIsValidDialect(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		scratch := scratchtest.Dir(t, "upper")
		rootA := filepath.Join(scratch, "a")
		rootB := filepath.Join(scratch, "b")
		for _, r := range []string{rootA, rootB} {
			if err := os.Mkdir(r, 0o755); err != nil {
				rt.Fatal(err)
			}
		}
		m := newModel()
		prefix := genOps(rt, m, rapid.IntRange(0, 6).Draw(rt, "n"), os.Getuid(), os.Getgid())
		// The final op draws against the advanced model, so its
		// preconditions hold on the prefix state.
		op := genOps(rt, m, 1, os.Getuid(), os.Getgid())[0]

		wA, wB := NewWriter(rootA), NewWriter(rootB)
		for _, p := range prefix {
			if err := applyOp(wA, p); err != nil {
				rt.Fatalf("prefix on A: %v", err)
			}
			if err := applyOp(wB, p); err != nil {
				rt.Fatalf("prefix on B: %v", err)
			}
		}
		preView := mustWalkView(t, rootA)

		// Full run on A gives the post view.
		if err := applyOp(wA, op); err != nil {
			rt.Fatalf("final op %+v: %v", op, err)
		}
		postView := mustWalkView(t, rootA)

		// Abort B's run at a drawn gate; a k beyond the op's real
		// gate count simply completes the op (the uncrashed arm).
		k := rapid.IntRange(0, 3).Draw(rt, "k")
		seen := 0
		wB.SetStepHook(func(string) error {
			seen++
			if seen > k {
				return errCrash
			}
			return nil
		})
		err := applyOp(wB, op)
		crashed := errors.Is(err, errCrash)
		if err != nil && !crashed {
			rt.Fatalf("op %+v failed for a non-crash reason: %v", op, err)
		}

		got := mustWalkView(t, rootB) // Walk must succeed regardless.
		if crashed {
			if sameView(got, preView) || sameView(got, postView) {
				return
			}
			// Declared intermediate: override chown's clear-first —
			// the chmod lands on the shared inode, so every linked
			// path shows the cleared mode.
			if op.verb == "chown" {
				inter := map[string]viewEntry{}
				for k2, v := range preView {
					inter[k2] = v
				}
				m.eachLinked(op.a, func(q string) {
					e := inter[q]
					e.mode &^= 0o6000
					inter[q] = e
				})
				if sameView(got, inter) {
					return
				}
			}
			rt.Fatalf("crash at gate %d of %+v left an undeclared state:\n got %v\n pre %v\npost %v", k, op, got, preView, postView)
		}
		if !sameView(got, postView) {
			rt.Fatalf("uncrashed op %+v diverged from post state", op)
		}
	})
}
