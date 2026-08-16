//go:build linux

package projection

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/commit"
	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
	"github.com/greatliontech/ocifs/internal/upper"
)

// crashGate aborts the writer's step sequence at step k.
type crashGate struct {
	k     int
	count int
}

var errCrash = errors.New("crash")

func (g *crashGate) hook(string) error {
	g.count++
	if g.count == g.k {
		return errCrash
	}
	return nil
}

// presentedTree renders a Merged presentation for coherence
// comparison: path -> kind/mode/uid/gid/target/content.
func presentedTree(t testing.TB, m *Merged, cas contentStore) map[string]string {
	t.Helper()
	out := map[string]string{}
	var walk func(dir *Node)
	walk = func(dir *Node) {
		snap, err := m.OpenDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < snap.Len(); i++ {
			n, ok, err := m.Lookup(dir, snap.At(i).Name)
			if err != nil || !ok {
				t.Fatalf("resolve %q: %v %v", snap.At(i).Name, ok, err)
			}
			h := n.Header()
			content := ""
			if n.Kind() == KindFile {
				if n.UpperBacked() {
					b, err := os.ReadFile(n.HostPath())
					if err != nil {
						t.Fatal(err)
					}
					content = string(b)
				} else {
					content = cas[n.ContentDigest().Hex]
				}
			}
			out[n.Path()] = fmt.Sprintf("%v|%o|%d:%d|%s|%s", n.Kind(), h.Mode&0o7777, h.Uid, h.Gid, n.LinkTarget(), content)
			if n.Kind() == KindDir {
				walk(n)
			}
			n.Close()
		}
	}
	rn := m.Root()
	rh := rn.Header()
	out["."] = fmt.Sprintf("%v|%o|%d:%d||", rn.Kind(), rh.Mode&0o7777, rh.Uid, rh.Gid)
	walk(rn)
	return out
}

func treesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// crashScriptOp is one scripted mutation with its declared crash
// intermediates beyond the old and new trees.
type crashScriptOp struct {
	name string
	// setup runs ungated before the pre-tree capture — state the
	// op mutates FROM, not part of the crash surface.
	setup func(m *Merged) error
	run   func(m *Merged) error
	// intermediates derives the declared intermediate trees from
	// the pre and post trees (REQ-writable-crash/-rename/-fidelity).
	intermediates func(pre, post map[string]string) []map[string]string
}

func noIntermediates(pre, post map[string]string) []map[string]string { return nil }

// bothPaths declares rename's residual: the entry present at both
// paths.
func bothPaths(src, dst string) func(pre, post map[string]string) []map[string]string {
	return func(pre, post map[string]string) []map[string]string {
		mid := map[string]string{}
		for k, v := range pre {
			mid[k] = v
		}
		mid[dst] = post[dst]
		return []map[string]string{mid}
	}
}

// TestCrashPrefixCoherence enumerates every dialect step of a
// scripted op set: aborting at each step must leave a valid dialect
// tree presenting the old tree, the new tree, or a declared
// intermediate — and committing the survived state must unify to
// exactly what it presents (REQ-writable-crash,
// REQ-proj-commit-neutral's dialect neutrality).
//
// The pre/post trees are the implementation's own endpoints, so this
// harness pins ORDERING coherence, never endpoint semantics — a
// mutation changing what an op ultimately does is the semantic
// suites' kill (they pin endpoints against the spec), while a
// mutation reordering steps within an op is this harness's.
func TestCrashPrefixCoherence(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("privileged run: override arms need refused chowns")
	}
	script := []crashScriptOp{
		{
			name: "create",
			run: func(m *Merged) error {
				n, f, err := m.Create(mustRootDir(m, "d"), "made", 0o640)
				if err != nil {
					return err
				}
				f.WriteString("fresh")
				f.Close()
				n.Close()
				return m.Flushed("d/made")
			},
			intermediates: noIntermediates,
		},
		{
			name: "copyup-chmod",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				defer n.Close()
				return m.SetMode(n, 0o400)
			},
			intermediates: noIntermediates,
		},
		{
			name: "chown-with-suid",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				if err := m.SetMode(n, 0o4755); err != nil {
					n.Close()
					return err
				}
				n.Close()
				n2, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				defer n2.Close()
				return m.SetOwner(n2, 0, 0)
			},
			// This script spans two ops. Declared intermediates: the
			// op boundary (suid set, old owner), and chown's
			// clear-first step (suid cleared, old owner) —
			// REQ-writable-fidelity's privilege-reducing residual.
			intermediates: func(pre, post map[string]string) []map[string]string {
				splice := func(mode string) map[string]string {
					mid := map[string]string{}
					for k, v := range pre {
						mid[k] = v
					}
					pf := strings.Split(pre["d/f"], "|")
					mid["d/f"] = fmt.Sprintf("%s|%s|%s|%s|%s", pf[0], mode, pf[2], pf[3], pf[4])
					return mid
				}
				return []map[string]string{splice("4755"), splice("755")}
			},
		},
		{
			name: "unlink-shadow-modified",
			setup: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				nn, f, err := m.OpenWrite(n)
				n.Close()
				if err != nil {
					return err
				}
				f.WriteAt([]byte("CHANGED!"), 0)
				f.Close()
				nn.Close()
				return m.Flushed("d/f")
			},
			run: func(m *Merged) error {
				return m.Unlink(mustRootDir(m, "d"), "f")
			},
			// No declared intermediate: the marker-first ordering
			// means every prefix presents the modified shadow (old)
			// or absence (new); a remove-before-marker mutation
			// would re-expose the BASE content — neither — which is
			// exactly what this arm refuses.
			intermediates: noIntermediates,
		},
		{
			name: "unlink-base",
			run: func(m *Merged) error {
				return m.Unlink(mustRootDir(m, "d"), "g")
			},
			intermediates: noIntermediates,
		},
		{
			name: "rename-shadow",
			setup: func(m *Merged) error {
				return modifyShadow(m, "d/f")
			},
			run: func(m *Merged) error {
				return m.Rename(mustRootDir(m, "d"), "f", mustRootDir(m, "."), "fmoved")
			},
			intermediates: bothPaths("d/f", "fmoved"),
		},
		{
			name: "rename-base-only",
			run: func(m *Merged) error {
				return m.Rename(mustRootDir(m, "d/sub"), "h", mustRootDir(m, "."), "hmoved")
			},
			intermediates: bothPaths("d/sub/h", "hmoved"),
		},
		{
			name: "rmdir-recreated",
			run: func(m *Merged) error {
				root := mustRootDir(m, ".")
				d, err := m.NodeAt("emptyd")
				if err != nil {
					return err
				}
				d.Close()
				if err := m.Rmdir(root, "emptyd"); err != nil {
					return err
				}
				nd, err := m.Mkdir(root, "emptyd", 0o755)
				if err != nil {
					return err
				}
				nd.Close()
				return nil
			},
			intermediates: func(pre, post map[string]string) []map[string]string {
				// rmdir-then-recreate passes through the removed
				// state.
				mid := map[string]string{}
				for k, v := range pre {
					if k == "emptyd" {
						continue
					}
					mid[k] = v
				}
				return []map[string]string{mid}
			},
		},
		{
			name: "rmdir-dismantle",
			setup: func(m *Merged) error {
				// The child is MODIFIED before deletion so any
				// dismantle-before-hide mutant re-exposing base
				// content presents a tree no accepted state matches.
				if err := modifyShadow(m, "d/sub/h"); err != nil {
					return err
				}
				sub := mustRootDir(m, "d/sub")
				defer sub.Close()
				return m.Unlink(sub, "h")
			},
			run: func(m *Merged) error {
				return m.Rmdir(mustRootDir(m, "d"), "sub")
			},
			intermediates: noIntermediates,
		},
		{
			name: "mode-record-drop",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				if err := m.SetMode(n, 0); err != nil {
					n.Close()
					return err
				}
				n.Close()
				n2, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				defer n2.Close()
				return m.SetMode(n2, 0o644)
			},
			// Boundary state (mode 0); the drop's chmod-first
			// intermediate presents the OLD mode over new host bits —
			// the same presentation.
			intermediates: func(pre, post map[string]string) []map[string]string {
				mid := map[string]string{}
				for k, v := range pre {
					mid[k] = v
				}
				pf := strings.Split(pre["d/f"], "|")
				mid["d/f"] = fmt.Sprintf("%s|0|%s|%s|%s", pf[0], pf[2], pf[3], pf[4])
				return []map[string]string{mid}
			},
		},
		{
			name: "chown-clears-record-suid",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				if err := m.SetMode(n, 0o4400); err != nil {
					n.Close()
					return err
				}
				n.Close()
				n2, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				defer n2.Close()
				return m.SetOwner(n2, 0, 0)
			},
			intermediates: func(pre, post map[string]string) []map[string]string {
				splice := func(mode string) map[string]string {
					mid := map[string]string{}
					for k, v := range pre {
						mid[k] = v
					}
					pf := strings.Split(pre["d/f"], "|")
					mid["d/f"] = fmt.Sprintf("%s|%s|%s|%s|%s", pf[0], mode, pf[2], pf[3], pf[4])
					return mid
				}
				return []map[string]string{splice("4400"), splice("400")}
			},
		},
		{
			name: "root-chown",
			run: func(m *Merged) error {
				return m.SetOwner(m.Root(), 0, 0)
			},
			// The stamping sequence's declared intermediate presents
			// the host root's attributes under the recorded owner —
			// with this fixture's default-mode root that equals the
			// post tree.
			intermediates: noIntermediates,
		},
		{
			name: "rename-replace-file",
			setup: func(m *Merged) error {
				return modifyShadow(m, "d/f")
			},
			run: func(m *Merged) error {
				return m.Rename(mustRootDir(m, "d"), "f", mustRootDir(m, "d"), "g")
			},
			intermediates: func(pre, post map[string]string) []map[string]string {
				mid := map[string]string{}
				for k, v := range pre {
					mid[k] = v
				}
				mid["d/g"] = post["d/g"]
				return []map[string]string{mid}
			},
		},
		{
			name: "rename-dir-onto-empty-dir",
			run: func(m *Merged) error {
				nd, err := m.Mkdir(m.Root(), "borndir", 0o750)
				if err != nil {
					return err
				}
				nd.Close()
				return m.Rename(m.Root(), "borndir", m.Root(), "emptyd")
			},
			intermediates: func(pre, post map[string]string) []map[string]string {
				// Op boundary (borndir exists beside emptyd), and the
				// declared destination-absent step of the replacing
				// compound (source still pending in both).
				withBorn := map[string]string{}
				for k, v := range pre {
					withBorn[k] = v
				}
				withBorn["borndir"] = post["emptyd"]
				noDst := map[string]string{}
				for k, v := range withBorn {
					if k == "emptyd" {
						continue
					}
					noDst[k] = v
				}
				return []map[string]string{withBorn, noDst}
			},
		},
		{
			name: "xattr-set-remove",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				if err := m.SetXattr(n, "user.cx", []byte("v"), 0); err != nil {
					n.Close()
					return err
				}
				n.Close()
				n2, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				defer n2.Close()
				return m.RemoveXattr(n2, "user.cx")
			},
			// Xattrs are outside presentedTree, so pre == post under
			// the projection; the arm pins walk validity and commit
			// round-trip across the gate points.
			intermediates: noIntermediates,
		},
		{
			name: "link-and-truncate",
			run: func(m *Merged) error {
				n, err := m.NodeAt("d/f")
				if err != nil {
					return err
				}
				ln, err := m.Link(n, m.Root(), "flink")
				n.Close()
				if err != nil {
					return err
				}
				ln.Close()
				n2, err := m.NodeAt("top")
				if err != nil {
					return err
				}
				defer n2.Close()
				return m.Truncate(n2, 4)
			},
			intermediates: func(pre, post map[string]string) []map[string]string {
				// Op boundary: link landed, truncate pending.
				mid := map[string]string{}
				for k, v := range post {
					mid[k] = v
				}
				mid["top"] = pre["top"]
				return []map[string]string{mid}
			},
		},
	}

	for _, op := range script {
		op := op
		t.Run(op.name, func(t *testing.T) {
			// Count the op's steps on a throwaway upper.
			inner, cas := baseFixture(t)
			root, _ := newUpperFor(t)
			m := mustWritable(t, inner, root, cas)
			if op.setup != nil {
				if err := op.setup(m); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}
			pre := presentedTree(t, m, cas)
			counter := &crashGate{k: -1}
			m.write.w.SetStepHook(counter.hook)
			if err := op.run(m); err != nil {
				t.Fatalf("clean run failed: %v", err)
			}
			post := presentedTree(t, m, cas)
			steps := counter.count
			if steps == 0 {
				t.Fatalf("op %s drove no gated steps", op.name)
			}
			declared := op.intermediates(pre, post)

			for k := 1; k <= steps; k++ {
				root, _ := newUpperFor(t)
				m := mustWritable(t, inner, root, cas)
				if op.setup != nil {
					if err := op.setup(m); err != nil {
						t.Fatalf("setup: %v", err)
					}
				}
				gate := &crashGate{k: k}
				m.write.w.SetStepHook(gate.hook)
				err := op.run(m)
				if err == nil {
					t.Fatalf("step %d/%d: gate did not fire", k, steps)
				}
				if !errors.Is(err, errCrash) {
					t.Fatalf("step %d/%d: op failed before the gate: %v", k, steps, err)
				}

				// (a) The crashed upper is a valid dialect tree.
				st, err := upper.Walk(root)
				if err != nil {
					t.Fatalf("step %d/%d: walk refused the crash state: %v", k, steps, err)
				}
				// The provider's mount-time sweep removes orphans.
				if err := upper.Sweep(root); err != nil {
					t.Fatal(err)
				}

				// (b) A rebuilt kernel presents old, new, or declared.
				m2, err := NewMergedWritable(inner, root, cas.open)
				if err != nil {
					t.Fatalf("step %d/%d: rebuild: %v", k, steps, err)
				}
				got := presentedTree(t, m2, cas)
				ok := treesEqual(got, pre) || treesEqual(got, post)
				for _, mid := range declared {
					ok = ok || treesEqual(got, mid)
				}
				if !ok {
					t.Fatalf("step %d/%d: crash state presents neither old, new, nor declared:\n got  %v\n pre  %v\n post %v", k, steps, got, pre, post)
				}

				// (c) Commit of the survived state unifies to what it
				// presents.
				st, err = upper.Walk(root)
				if err != nil {
					t.Fatal(err)
				}
				_ = st
				verifyCommitMatchesPresentation(t, root, cas, got)
			}
		})
	}
}

// modifyShadow copies p up and overwrites its content — setup
// state making base re-exposure detectably different.
func modifyShadow(m *Merged, p string) error {
	n, err := m.NodeAt(p)
	if err != nil {
		return err
	}
	nn, f, err := m.OpenWrite(n)
	n.Close()
	if err != nil {
		return err
	}
	f.WriteAt([]byte("CHANGED!"), 0)
	f.Close()
	nn.Close()
	return m.Flushed(p)
}

func mustRootDir(m *Merged, p string) *Node {
	if p == "." {
		return m.Root()
	}
	n, err := m.NodeAt(p)
	if err != nil {
		panic(err)
	}
	return n
}

// verifyCommitMatchesPresentation commits the upper and re-unifies
// the layer over the base by the read side's own rules, asserting
// the result presents the same tree (sockets excluded — they never
// commit; root attrs live outside the walk).
func verifyCommitMatchesPresentation(t testing.TB, root string, cas contentStore, presented map[string]string) {
	t.Helper()
	st, err := upper.Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	bl, _ := baseLayerAndCAS(t)
	view, err := layer.Unify([]layer.Layer{bl})
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := commit.LayerBytes(view, st)
	if err != nil {
		t.Fatalf("commit of crash state: %v", err)
	}
	var cl layer.Layer
	contents := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(b))
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
			raw, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			e.Digest = fakeContentDigest(raw)
			contents[e.Digest.Hex] = string(raw)
		}
		cl = append(cl, e)
	}
	v2, err := layer.Unify([]layer.Layer{bl, cl})
	if err != nil {
		t.Fatalf("committed layer does not unify: %v", err)
	}
	got := map[string]string{}
	for _, e := range v2.Entries() {
		p := e.Header.Name
		if p == "." {
			continue
		}
		kind := kindFromFlag(e.Header.Typeflag)
		target := e.Header.Linkname
		if e.Header.Typeflag == tar.TypeLink {
			target = ""
		}
		content := ""
		if kind == KindFile {
			if c, ok := contents[e.Digest.Hex]; ok {
				content = c
			} else {
				content = cas[e.Digest.Hex]
			}
		}
		got[p] = fmt.Sprintf("%v|%o|%d:%d|%s|%s", kind, uint32(e.Header.Mode)&0o7777, e.Header.Uid, e.Header.Gid, target, content)
	}
	for p, v := range presented {
		if p == "." {
			continue // root commit is pinned by TestRootRecordCommits
		}
		if strings.HasPrefix(v, KindSocket.String()+"|") {
			continue // sockets never commit
		}
		if got[p] != v {
			t.Fatalf("committed unify diverges at %q:\n committed %q\n presented %q", p, got[p], v)
		}
	}
	for p, v := range got {
		if _, ok := presented[p]; !ok {
			t.Fatalf("committed unify has %q (%q) the presentation lacks", p, v)
		}
	}
}

func fakeContentDigest(b []byte) v1.Hash {
	sum := sha256.Sum256(b)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// TestCrashStorm SIGKILLs a child hammering the write engine at
// random points, then verifies the survivor: the dialect walks, a
// rebuilt kernel serves it, further writes succeed, and commit
// round-trips (REQ-writable-crash's kill-storm arm).
func TestCrashStorm(t *testing.T) {
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("storm under a mutation campaign")
	}
	if os.Getenv("OCIFS_CRASH_CHILD") != "" {
		crashStormChild(t)
		return
	}
	if testing.Short() {
		t.Skip("short mode")
	}
	rel := scratchtest.Dir(t, "projection")
	dir, err := filepath.Abs(rel)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "u")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	casDir := filepath.Join(dir, "cas")
	if err := os.Mkdir(casDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, cas := baseFixture(t)
	for hex, content := range cas {
		if err := os.WriteFile(filepath.Join(casDir, hex), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	exe, exeErr := os.Executable()
	if exeErr != nil {
		t.Fatal(exeErr)
	}
	for round := 0; round < 12; round++ {
		cmd := exec.Command(exe, "-test.run", "^TestCrashStorm$", "-test.v")
		cmd.Env = append(os.Environ(),
			"OCIFS_CRASH_CHILD=1",
			"OCIFS_CRASH_UPPER="+root,
			"OCIFS_CRASH_CAS="+casDir,
			"OCIFS_CRASH_SEED="+strconv.Itoa(round),
		)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(3+round*4) * time.Millisecond)
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()

		// Survivor verification.
		if _, err := upper.Walk(root); err != nil {
			t.Fatalf("round %d: dialect invalid after kill: %v", round, err)
		}
		if err := upper.Sweep(root); err != nil {
			t.Fatal(err)
		}
		inner, _ := baseFixture(t)
		m, err := NewMergedWritable(inner, root, cas.open)
		if err != nil {
			t.Fatalf("round %d: rebuild: %v", round, err)
		}
		presented := presentedTree(t, m, cas)
		verifyCommitMatchesPresentation(t, root, cas, presented)
		// The survivor still takes writes.
		n, f, err := m.Create(m.Root(), fmt.Sprintf("post%d", round), 0o600)
		if err != nil {
			t.Fatalf("round %d: survivor refuses writes: %v", round, err)
		}
		f.Close()
		n.Close()
	}

	// The storm must have been real: at least one child artifact
	// survived some round — a silently unproductive child would
	// make every verification vacuous.
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	stormed := false
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "s") && strings.Contains(e.Name(), "_") {
			stormed = true
			break
		}
	}
	if !stormed {
		t.Fatal("no child artifact in any round: the storm never mutated")
	}
}

// crashStormChild hammers the engine until killed.
func crashStormChild(t *testing.T) {
	root := os.Getenv("OCIFS_CRASH_UPPER")
	casDir := os.Getenv("OCIFS_CRASH_CAS")
	seed, _ := strconv.Atoi(os.Getenv("OCIFS_CRASH_SEED"))
	inner, cas := baseFixture(t)
	_ = cas
	casFromDir := contentStore{}
	ents, err := os.ReadDir(casDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		b, _ := os.ReadFile(filepath.Join(casDir, e.Name()))
		casFromDir[e.Name()] = string(b)
	}
	m, err := NewMergedWritable(inner, root, casFromDir.open)
	if err != nil {
		t.Fatal(err)
	}
	r := seed
	next := func(n int) int {
		r = (r*1103515245 + 12345) & 0x7fffffff
		return r % n
	}
	i := 0
	for {
		i++
		name := fmt.Sprintf("s%d_%d", seed, i)
		switch next(10) {
		case 0:
			if n, f, err := m.Create(m.Root(), name, 0o600); err == nil {
				f.WriteString(strings.Repeat("x", next(64)))
				f.Close()
				n.Close()
				_ = m.Flushed(name)
			}
		case 1:
			if n, err := m.Mkdir(m.Root(), name, 0o750); err == nil {
				n.Close()
			}
		case 2:
			_ = m.Unlink(mustRootDir(m, "."), fmt.Sprintf("s%d_%d", seed, 1+next(i)))
		case 3:
			_ = m.Rename(mustRootDir(m, "."), fmt.Sprintf("s%d_%d", seed, 1+next(i)),
				mustRootDir(m, "."), name)
		case 4:
			if n, err := m.NodeAt("d/f"); err == nil {
				_ = m.SetMode(n, 0o600)
				n.Close()
			}
		case 5:
			d := mustRootDir(m, "d")
			_ = m.Unlink(d, "g")
			d.Close()
		case 6:
			// Base-visible rename: the riskiest compound; recreate
			// after the first round consumed the base file.
			if _, err := m.NodeAt("d/f"); err == nil {
				d := mustRootDir(m, "d")
				_ = m.Rename(d, "f", mustRootDir(m, "."), name)
				d.Close()
			} else {
				d := mustRootDir(m, "d")
				if n, f, err := m.Create(d, "f", 0o644); err == nil {
					f.WriteString("re")
					f.Close()
					n.Close()
				}
				d.Close()
			}
		case 7:
			// rmdir with dismantle: empty then remove d/sub;
			// recreate the dir for later rounds.
			if sub, err := m.NodeAt("d/sub"); err == nil {
				_ = m.Unlink(sub, "h")
				sub.Close()
				d := mustRootDir(m, "d")
				_ = m.Rmdir(d, "sub")
				d.Close()
			} else {
				d := mustRootDir(m, "d")
				if nd, err := m.Mkdir(d, "sub", 0o755); err == nil {
					if n, f, err := m.Create(nd, "h", 0o644); err == nil {
						f.Close()
						n.Close()
					}
					nd.Close()
				}
				d.Close()
			}
		case 8:
			if n, err := m.NodeAt("top"); err == nil {
				_ = m.SetOwner(n, 0, 0)
				n.Close()
			}
		case 9:
			if n, err := m.NodeAt("top"); err == nil {
				_ = m.SetXattr(n, "user.storm", []byte("s"), 0)
				n.Close()
			}
		}
	}
}

