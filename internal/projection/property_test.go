package projection

import (
	"archive/tar"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/layer"
)

// genView draws a small unified view whose names deliberately mix
// case variants and kinds, so folded comparators collide and
// envelopes omit.
func genView(rt *rapid.T) *layer.View {
	segGen := rapid.SampledFrom([]string{"a", "A", "b", "data", "Data", "DATA", "x1", "X1", "b.", "a:b", "con"})
	// 'S' (GNU sparse) exercises the unknown-typeflag arm; TypeLink
	// exercises hardlink resolution; "b."/"a:b"/"con" exercise the
	// name-validity arm under a validator-bearing capability set.
	kindGen := rapid.SampledFrom([]byte{tar.TypeReg, tar.TypeDir, tar.TypeSymlink, tar.TypeFifo, tar.TypeChar, tar.TypeBlock, tar.TypeLink, 'S'})

	n := rapid.IntRange(0, 10).Draw(rt, "n")
	seen := map[string]bool{}
	var entries []layer.Entry
	var files []string
	if rapid.Bool().Draw(rt, "rootRecord") {
		entries = append(entries, layer.Entry{Header: tar.Header{Name: ".", Typeflag: tar.TypeDir, Mode: 0o755}})
	}
	for i := 0; i < n; i++ {
		depth := rapid.IntRange(1, 4).Draw(rt, "depth")
		parts := make([]string, depth)
		for j := range parts {
			parts[j] = segGen.Draw(rt, "seg")
		}
		name := strings.Join(parts, "/")
		if seen[name] {
			continue
		}
		seen[name] = true
		flag := kindGen.Draw(rt, "kind")
		if flag == tar.TypeLink && len(files) == 0 {
			flag = tar.TypeReg
		}
		e := layer.Entry{Header: tar.Header{Name: name, Typeflag: flag, Mode: 0o644}}
		switch flag {
		case tar.TypeReg:
			e.Header.Size = 7
			e.Digest = fakeDigest(name)
			files = append(files, name)
		case tar.TypeSymlink:
			e.Header.Linkname = "target"
		case tar.TypeLink:
			e.Header.Linkname = files[rapid.IntRange(0, len(files)-1).Draw(rt, "linkTarget")]
		}
		entries = append(entries, e)
	}
	v, err := layer.Unify([]layer.Layer{layer.Layer(entries)})
	if err != nil {
		rt.Fatalf("unify: %v", err)
	}
	return v
}

func genCaps(rt *rapid.T) Capabilities {
	caps := Capabilities{
		Symlinks: rapid.Bool().Draw(rt, "symlinks"),
		FIFOs:    rapid.Bool().Draw(rt, "fifos"),
		Devices:  rapid.Bool().Draw(rt, "devices"),
	}
	if rapid.Bool().Draw(rt, "folded") {
		caps.Compare = foldCompare
	}
	if rapid.Bool().Draw(rt, "validator") {
		caps.ValidName = func(name string) bool {
			return !strings.ContainsAny(name, ":") && !strings.HasSuffix(name, ".") && strings.ToLower(name) != "con"
		}
	}
	return caps
}

// collectPresented walks the tree and returns path -> entry for
// every presented view entry (synthetic extras excluded).
func collectPresented(p *Projection) map[string]*Entry {
	out := map[string]*Entry{}
	var walk func(e *Entry)
	walk = func(e *Entry) {
		for _, c := range e.Children() {
			if c.ID() < syntheticIDBase {
				out[c.Path()] = c
			}
			if c.Kind() == KindDir {
				walk(c)
			}
		}
	}
	walk(p.Root())
	return out
}

// TestPropertyIdentityDeterministic pins REQ-proj-identity: the
// path->ID assignment is a pure function of the view alone —
// recomputable from view order, identical across rebuilds, and
// independent of the capability set.
func TestPropertyIdentityDeterministic(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		view := genView(rt)

		expected := map[string]ID{}
		next := viewIDBase
		for _, ve := range view.Entries() {
			if ve.Header.Name == "." {
				continue
			}
			expected[ve.Header.Name] = next
			next++
		}

		for _, caps := range []Capabilities{capsFull, capsFolded, genCaps(rt)} {
			p, err := New(view, nil, caps)
			if err != nil {
				rt.Fatalf("New: %v", err)
			}
			if p.Root().ID() != RootID {
				rt.Fatalf("root ID %d", p.Root().ID())
			}
			for path, e := range collectPresented(p) {
				if e.ID() != expected[path] {
					rt.Fatalf("ID(%q) = %d, want %d (caps folded=%v)", path, e.ID(), expected[path], caps.Compare != nil)
				}
			}
		}
	})
}

// TestPropertyPresentedOrReported pins the entailed partition of
// REQ-proj-model + REQ-proj-report: every view entry is exactly
// either presented in the tree or reported omitted — never both,
// never neither, whatever the capability set.
func TestPropertyPresentedOrReported(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		view := genView(rt)
		caps := genCaps(rt)
		p, err := New(view, nil, caps)
		if err != nil {
			rt.Fatalf("New: %v", err)
		}

		presented := collectPresented(p)
		reported := map[string]bool{}
		for _, re := range p.Report().Entries {
			if reported[re.Path] {
				rt.Fatalf("%q reported twice", re.Path)
			}
			reported[re.Path] = true
		}

		for _, ve := range view.Entries() {
			name := ve.Header.Name
			if name == "." {
				continue
			}
			_, isPresented := presented[name]
			isReported := reported[name]
			if isPresented == isReported {
				rt.Fatalf("view entry %q: presented=%v reported=%v — the partition is broken", name, isPresented, isReported)
			}
			delete(presented, name)
			delete(reported, name)
		}
		if len(presented) != 0 {
			rt.Fatalf("presented entries not in the view: %v", presented)
		}
		if len(reported) != 0 {
			rt.Fatalf("reported entries not in the view: %v", reported)
		}

		// Presented names are comparator-unique within each directory
		// (REQ-proj-case).
		var walk func(e *Entry)
		walk = func(e *Entry) {
			for i := 1; i < e.Len(); i++ {
				if caps.compare(e.At(i-1).Name(), e.At(i).Name()) >= 0 {
					rt.Fatalf("children of %q unsorted or colliding: %q, %q", e.Path(), e.At(i-1).Name(), e.At(i).Name())
				}
			}
			for _, c := range e.Children() {
				if c.Kind() == KindDir {
					walk(c)
				}
			}
		}
		walk(p.Root())
	})
}

// TestPropertyCollisionWinnerIsFirstInViewOrder pins REQ-proj-case:
// among comparator-equal names in one directory, the presented entry
// is the first in unified-view order and every other is reported as
// a case collision.
func TestPropertyCollisionWinnerIsFirstInViewOrder(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		view := genView(rt)
		p, err := New(view, nil, capsFolded)
		if err != nil {
			rt.Fatalf("New: %v", err)
		}

		collision := map[string]Reason{}
		for _, re := range p.Report().Entries {
			collision[re.Path] = re.Reason
		}

		// Walk view entries in order, simulating the winner rule over
		// presentable entries only (kind omissions and containment
		// are excluded independently of collisions).
		type dirKey struct{ dir, folded string }
		winners := map[dirKey]string{}
		for _, ve := range view.Entries() {
			name := ve.Header.Name
			if name == "." {
				continue
			}
			kind, known := kindOf(&ve.Header)
			if !known {
				continue
			}
			if ok, _ := capsFolded.supported(kind); !ok {
				continue
			}
			dir := "."
			base := name
			if i := strings.LastIndex(name, "/"); i >= 0 {
				dir, base = name[:i], name[i+1:]
			}
			key := dirKey{dir, strings.ToLower(base)}
			if _, taken := winners[key]; taken {
				if collision[name] != ReasonCaseCollision {
					rt.Fatalf("loser %q not reported as case collision (got %q)", name, collision[name])
				}
				continue
			}
			winners[key] = name
		}
	})
}

// TestPropertyEnumerationSplitEqualsWhole pins REQ-proj-enumeration:
// splitting an enumeration at any position and resuming — by held
// index or by Seek after the last returned name — yields exactly the
// unbroken listing, and two interleaved cursors do not disturb each
// other.
func TestPropertyEnumerationSplitEqualsWhole(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		view := genView(rt)
		caps := genCaps(rt)
		p, err := New(view, nil, caps)
		if err != nil {
			rt.Fatalf("New: %v", err)
		}

		var dirs []*Entry
		dirs = append(dirs, p.Root())
		var collect func(e *Entry)
		collect = func(e *Entry) {
			for _, c := range e.Children() {
				if c.Kind() == KindDir {
					dirs = append(dirs, c)
					collect(c)
				}
			}
		}
		collect(p.Root())
		dir := dirs[rapid.IntRange(0, len(dirs)-1).Draw(rt, "dir")]

		whole := make([]string, dir.Len())
		for i := range whole {
			whole[i] = dir.At(i).Name()
		}
		if dir.Len() == 0 {
			return
		}
		split := rapid.IntRange(0, dir.Len()).Draw(rt, "split")

		var got []string
		for i := 0; i < split; i++ {
			got = append(got, dir.At(i).Name())
		}
		// A second cursor running to completion between the first
		// cursor's halves: immutability means neither disturbs the
		// other, and both see the whole listing.
		var second []string
		for i := 0; i < dir.Len(); i++ {
			second = append(second, dir.At(i).Name())
		}
		resume := split
		if split > 0 {
			resume = p.Seek(dir, got[split-1])
		}
		for i := resume; i < dir.Len(); i++ {
			got = append(got, dir.At(i).Name())
		}

		if strings.Join(got, "\x00") != strings.Join(whole, "\x00") {
			rt.Fatalf("split at %d yields %v, whole is %v", split, got, whole)
		}
		if strings.Join(second, "\x00") != strings.Join(whole, "\x00") {
			rt.Fatalf("interleaved cursor saw %v, whole is %v", second, whole)
		}
	})
}

// TestPropertyExtraDirConflictIffViewNonDir pins the view-level
// conflict rule of REQ-api-extra-dirs under the byte comparator:
// construction fails exactly when some component path of an extra
// directory exists in the view as a non-directory — independent of
// what the envelope presents.
func TestPropertyExtraDirConflictIffViewNonDir(t *testing.T) {
	segGen := rapid.SampledFrom([]string{"a", "A", "b", "data", "x1"})
	rapid.Check(t, func(rt *rapid.T) {
		view := genView(rt)
		caps := Capabilities{
			Symlinks: rapid.Bool().Draw(rt, "symlinks"),
			FIFOs:    rapid.Bool().Draw(rt, "fifos"),
			Devices:  rapid.Bool().Draw(rt, "devices"),
		}

		nExtras := rapid.IntRange(1, 2).Draw(rt, "extras")
		extras := make([]string, nExtras)
		for i := range extras {
			depth := rapid.IntRange(1, 2).Draw(rt, "edepth")
			parts := make([]string, depth)
			for j := range parts {
				parts[j] = segGen.Draw(rt, "eseg")
			}
			extras[i] = strings.Join(parts, "/")
		}

		// Oracle straight from the view: every non-directory path,
		// with unknown kinds counting as non-directories.
		nonDir := map[string]bool{}
		for _, ve := range view.Entries() {
			if ve.Header.Name == "." {
				continue
			}
			kind, known := kindOf(&ve.Header)
			if !known || kind != KindDir {
				nonDir[ve.Header.Name] = true
			}
		}
		wantErr := false
		for _, e := range extras {
			parts := strings.Split(e, "/")
			for i := range parts {
				if nonDir[strings.Join(parts[:i+1], "/")] {
					wantErr = true
				}
			}
		}

		_, err := New(view, extras, caps)
		if (err != nil) != wantErr {
			rt.Fatalf("extras %v over view: err=%v, oracle wantErr=%v", extras, err, wantErr)
		}
	})
}
