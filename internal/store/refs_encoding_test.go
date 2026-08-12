package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"
)

// TestRefPathsPortable pins the ref-path encoding of
// REQ-store-layout: no byte outside [a-z0-9._-%] (plus the path
// separator) reaches disk, ':' never appears, and distinct
// references — including tags differing only by case, which collide
// on case-folding filesystems — land on distinct paths that
// round-trip.
func TestRefPathsPortable(t *testing.T) {
	rs := referenceStore(scratchDir(t))
	digest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ab", 32)}

	refs := []string{
		"127.0.0.1:5000/test/img:v1",
		"reg.example/repo@sha256:" + strings.Repeat("cd", 32),
		"reg.example/repo:V1",
		"reg.example/repo:v1",
		// Dot components must not collapse in path resolution: a
		// hostile-but-parseable reference could otherwise climb out
		// of the refs tier and address another tier's keyspace.
		"reg.example/repo:.",
		"reg.example/repo:..",
		"reg.example/..:tag",
		// Windows strips trailing dots silently; v1. and v1 must not
		// share a file.
		"reg.example/repo:v1.",
		// Fixed depth: a tag and a same-named sub-repository must
		// coexist (variable-depth nesting made the second one
		// uncacheable).
		"reg.example/repo:sub",
		"reg.example/repo/sub:v1",
		// Windows reserved device names must not appear literally.
		"reg.example/repo:con",
		"reg.example/nul/img:v1",
		"reg.example/repo:com0",
		"reg.example/repo:lpt9.log",
		// A maximal repository path (255 chars) encodes past any
		// filesystem name limit in plain form; the hashed fallback
		// keeps it storable — and two long repos differing in one
		// byte stay distinct.
		"reg.example/" + strings.Repeat("a/", 126) + "aaa:v1",
		"reg.example/" + strings.Repeat("a/", 126) + "aab:v1",
	}
	for _, r := range refs {
		ref, err := name.ParseReference(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := rs.Put(ref, digest); err != nil {
			t.Fatal(err)
		}
		got, ok, err := rs.Get(ref)
		if err != nil || !ok || got != digest {
			t.Fatalf("roundtrip of %q: %v %v %v", r, got, ok, err)
		}
	}

	// Every ref path must resolve inside the refs root: pathForRef
	// with a dot-component reference escaping the tier is the
	// REQ-store-cas-content poisoning vector.
	for _, r := range refs {
		ref, err := name.ParseReference(r)
		if err != nil {
			t.Fatal(err)
		}
		p := rs.pathForRef(ref)
		rel, err := filepath.Rel(string(rs), p)
		if err != nil {
			t.Fatal(err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == "." {
			t.Fatalf("ref %q resolves outside the refs tier: %s", r, p)
		}
	}

	var files []string
	err := filepath.WalkDir(string(rs), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(string(rs), path)
		if rerr != nil {
			return rerr
		}
		for i := 0; i < len(rel); i++ {
			c := rel[i]
			ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
				c == '.' || c == '_' || c == '-' || c == '%' || c == filepath.Separator
			if !ok {
				t.Fatalf("unportable byte %q in ref path %q", c, rel)
			}
		}
		// No element may begin or end with '.': Windows path
		// normalization silently strips trailing dots, folding
		// distinct refs (v1. vs v1) onto one file. Elements must
		// also stay within 255-byte name limits and never be a
		// Windows reserved device name.
		if rel != "." {
			for elem := range strings.SplitSeq(rel, string(filepath.Separator)) {
				if strings.HasPrefix(elem, ".") || strings.HasSuffix(elem, ".") {
					t.Fatalf("dot-led or dot-trailed element %q in ref path %q", elem, rel)
				}
				if len(elem) > 255 {
					t.Fatalf("element of %d bytes in ref path %q", len(elem), rel)
				}
				if windowsReservedName(elem) {
					t.Fatalf("reserved device name element %q in ref path %q", elem, rel)
				}
			}
		}
		if !d.IsDir() {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// All four refs distinct on disk: the V1/v1 pair must not have
	// collapsed.
	if len(files) != len(refs) {
		t.Fatalf("%d ref files for %d distinct refs: %v", len(files), len(refs), files)
	}
}

// TestPropertyRefEncodingInjective pins injectivity as a for-all:
// distinct component strings never encode to the same path name.
func TestPropertyRefEncodingInjective(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.StringOfN(rapid.Rune(), 1, 40, -1).Draw(rt, "a")
		b := rapid.StringOfN(rapid.Rune(), 1, 40, -1).Draw(rt, "b")
		ea, eb := encodeRefComponent(a), encodeRefComponent(b)
		if a != b && ea == eb {
			rt.Fatalf("collision: %q and %q both encode to %q", a, b, ea)
		}
		if a == b && ea != eb {
			rt.Fatalf("nondeterministic encoding of %q", a)
		}
		for i := 0; i < len(ea); i++ {
			c := ea[i]
			ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
				c == '.' || c == '_' || c == '-' || c == '%'
			if !ok {
				rt.Fatalf("unportable byte %q in encoding %q of %q", c, ea, a)
			}
		}
		// No encoding is a path-special element: dot-led components
		// would collapse in filepath.Join and escape the tier, and
		// dot-trailed ones are silently stripped by Windows path
		// normalization.
		if strings.HasPrefix(ea, ".") || strings.HasSuffix(ea, ".") {
			rt.Fatalf("encoding %q of %q is dot-led or dot-trailed", ea, a)
		}
		// Every encoding fits common filesystem name limits: plain
		// form is bounded, hashed form is 66 bytes.
		if len(ea) > 255 {
			rt.Fatalf("encoding of %q is %d bytes", a, len(ea))
		}
	})
}
