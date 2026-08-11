package layer

import (
	"archive/tar"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// The generator produces adversarial layer stacks over a small path
// alphabet so paths collide constantly: files, directories with
// varied modes, symlinks (dangling and into the tree), hardlinks
// (resolvable and not), whiteouts, opaques, degenerate markers, and
// mixed path spellings. Everything the generator emits is contained
// (no escapes) so the oracle can run it against a real directory.

// Generators are constructed per call rather than held in package
// variables: package-level mutable state in the mutated package
// downgrades mutation-campaign evidence (shared dynamic state).
func genComp() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"a", "b", "c", "d"})
}

func genFileMode() *rapid.Generator[int64] {
	return rapid.SampledFrom([]int64{0o600, 0o644, 0o755})
}

// Directory modes stay owner-traversable: physical extraction (and
// the oracle) cannot place children under an untraversable directory
// without the deferred-permissions dance that belongs to export, not
// to this model comparison.
func genDirMode() *rapid.Generator[int64] {
	return rapid.SampledFrom([]int64{0o700, 0o755, 0o750})
}

func genPath(t *rapid.T, label string) string {
	n := rapid.IntRange(1, 3).Draw(t, label+"-depth")
	parts := make([]string, n)
	for i := range parts {
		parts[i] = genComp().Draw(t, label+"-comp")
	}
	return strings.Join(parts, "/")
}

// respell rewrites a path in an equivalent spelling (REQ-unify-paths).
func respell(t *rapid.T, p string) string {
	switch rapid.IntRange(0, 4).Draw(t, "spelling") {
	case 1:
		return "./" + p
	case 2:
		return "/" + p
	case 3:
		return strings.ReplaceAll(p, "/", "//")
	case 4:
		return p + "/"
	}
	return p
}

func genLayerStack(t *rapid.T, contents map[string]string) []Layer {
	nLayers := rapid.IntRange(1, 4).Draw(t, "layers")
	stack := make([]Layer, nLayers)
	for li := range stack {
		nEntries := rapid.IntRange(0, 10).Draw(t, "entries")
		l := make(Layer, 0, nEntries)
		for range nEntries {
			p := genPath(t, "p")
			switch rapid.IntRange(0, 13).Draw(t, "kind") {
			case 0, 1, 2, 3: // regular file
				// Not StringMatching: regex generators populate a
				// package-level cache inside rapid at runtime, which
				// both stipulator and gomutant rightly flag as
				// mutated shared dynamic state.
				c := rapid.StringOfN(rapid.RuneFrom([]rune("xyz")), 0, 3, -1).Draw(t, "content")
				registerContent(contents, c)
				e := file(respell(t, p), c)
				e.Header.Mode = genFileMode().Draw(t, "fmode")
				l = append(l, e)
			case 4, 5: // directory
				l = append(l, dirMode(respell(t, p), genDirMode().Draw(t, "dmode")))
			case 6: // symlink: dangling, absolute, or into the tree
				target := genPath(t, "starget")
				if rapid.Bool().Draw(t, "abs") {
					target = "/" + target
				}
				l = append(l, symlink(respell(t, p), target))
			case 7: // hardlink
				l = append(l, hardlink(respell(t, p), respell(t, genPath(t, "ltarget"))))
			case 8: // whiteout (occasionally degenerate)
				if rapid.IntRange(0, 5).Draw(t, "degen") == 0 {
					l = append(l, Entry{Header: tar.Header{
						Name:     rapid.SampledFrom([]string{".wh..", ".wh...", "a/.wh..wh.x", ".wh."}).Draw(t, "dname"),
						Typeflag: tar.TypeReg,
					}})
				} else {
					l = append(l, wh(p))
				}
			case 9: // opaque
				l = append(l, opq(genPath(t, "opq")))
			case 10: // root directory entry
				l = append(l, dirMode(rapid.SampledFrom([]string{".", "./", "/"}).Draw(t, "rootspell"), genDirMode().Draw(t, "rmode")))
			case 11: // root opaque
				l = append(l, Entry{Header: tar.Header{Name: ".wh..wh..opq", Typeflag: tar.TypeReg}})
			case 12: // archive metadata entry
				l = append(l, Entry{Header: tar.Header{
					Name:     "pax_global_header",
					Typeflag: rapid.SampledFrom([]byte{tar.TypeXGlobalHeader, tar.TypeXHeader}).Draw(t, "meta"),
				}})
			case 13: // FIFO, or an inert mid-path reserved-component entry
				if rapid.Bool().Draw(t, "resv") {
					c := genComp().Draw(t, "resvcomp")
					l = append(l, Entry{Header: tar.Header{
						Name:     c + "/.wh." + c + "/" + genComp().Draw(t, "resvleaf"),
						Typeflag: tar.TypeReg,
					}})
				} else {
					l = append(l, fifo(respell(t, p)))
				}
			}
		}
		stack[li] = l
	}
	return stack
}

// TestPropertyOracle: Unify's presentation equals independent
// filesystem extraction for every generated stack (and the view
// invariants hold).
func TestPropertyOracle(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string]string{}
		stack := genLayerStack(rt, contents)
		root, cleanup := scratchCase(rt)
		defer cleanup()
		runOracleCase(rt, root, stack, contents)
	})
}

// TestPropertyMarkerOrderIndependence: moving every marker to the
// front or the back of its layer never changes the view
// (REQ-unify-whiteout: whiteouts-first is structural).
func TestPropertyMarkerOrderIndependence(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string]string{}
		stack := genLayerStack(rt, contents)
		reorder := func(markersFirst bool) []Layer {
			out := make([]Layer, len(stack))
			for i, l := range stack {
				var markers, rest Layer
				for _, e := range l {
					base := e.Header.Name
					if i := strings.LastIndexByte(base, '/'); i >= 0 {
						base = base[i+1:]
					}
					if strings.HasPrefix(base, whiteoutPrefix) {
						markers = append(markers, e)
					} else {
						rest = append(rest, e)
					}
				}
				if markersFirst {
					out[i] = append(append(Layer{}, markers...), rest...)
				} else {
					out[i] = append(append(Layer{}, rest...), markers...)
				}
			}
			return out
		}
		base, err := Unify(stack)
		if err != nil {
			rt.Fatalf("Unify: %v", err)
		}
		for _, variant := range [][]Layer{reorder(true), reorder(false)} {
			v, err := Unify(variant)
			if err != nil {
				rt.Fatalf("Unify variant: %v", err)
			}
			requireViewsEqual(rt, base, v)
		}
	})
}

// TestPropertySingleLayerRoundtrip: a view re-unified as one layer
// reproduces itself, when no hardlinks are present (a sorted view
// may order a link before its target, which extraction order cannot
// resolve).
func TestPropertySingleLayerRoundtrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string]string{}
		stack := genLayerStack(rt, contents)
		v, err := Unify(stack)
		if err != nil {
			rt.Fatalf("Unify: %v", err)
		}
		for _, e := range v.Entries() {
			if e.Header.Typeflag == tar.TypeLink {
				rt.Skip("view contains hardlinks")
			}
		}
		v2, err := Unify([]Layer{Layer(v.Entries())})
		if err != nil {
			rt.Fatalf("re-Unify: %v", err)
		}
		requireViewsEqual(rt, v, v2)
	})
}

// TestPropertyEscapeRejected: any stack containing one escaping
// entry fails unification with ErrPathEscape.
func TestPropertyEscapeRejected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		contents := map[string]string{}
		stack := genLayerStack(rt, contents)
		evil := rapid.SampledFrom([]string{"..", "../x", "a/../../x", "/../x", "a/b/../../../x"}).Draw(rt, "evil")
		li := rapid.IntRange(0, len(stack)-1).Draw(rt, "layer")
		pos := rapid.IntRange(0, len(stack[li])).Draw(rt, "pos")
		l := append(Layer{}, stack[li][:pos]...)
		l = append(l, Entry{Header: tar.Header{Name: evil, Typeflag: tar.TypeReg}})
		l = append(l, stack[li][pos:]...)
		stack[li] = l
		if _, err := Unify(stack); err == nil {
			rt.Fatalf("escaping entry %q accepted", evil)
		}
	})
}

func requireViewsEqual(t reporter, a, b *View) {
	ae, be := a.Entries(), b.Entries()
	if len(ae) != len(be) {
		t.Fatalf("views differ in length: %d vs %d", len(ae), len(be))
	}
	for i := range ae {
		x, y := ae[i], be[i]
		if x.Header.Name != y.Header.Name || x.Header.Typeflag != y.Header.Typeflag ||
			x.Header.Linkname != y.Header.Linkname || x.Header.Mode != y.Header.Mode ||
			x.Header.Size != y.Header.Size || x.Digest != y.Digest ||
			x.Header.Uid != y.Header.Uid || x.Header.Gid != y.Header.Gid {
			t.Fatalf("views differ at %d:\n%+v\nvs\n%+v", i, x, y)
		}
	}
}
