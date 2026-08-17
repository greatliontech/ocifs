package store

import (
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"pgregory.net/rapid"

	"github.com/greatliontech/ocifs/internal/scratchtest"
)

var testRefsMap = map[string]string{
	"docker.io/busybox":                   "sha256:ab33eacc8251e3807b85bb6dba570e4698c3998eca6f0fc2ccb60575a563ea74",
	"gcr.io/distroless/base:latest-amd64": "sha256:b14f0d621bdfd1c967bca28f28ae7c1191e216ce0f34977c9f1e1f5081aae047",
	"ghcr.io/greatliontech/pbr:v0.3.9":    "sha256:216497b191d24a7998a65618ef588a18befbd1a721ca0486835d2a75dde930bd",
	// The byte-exact keyspace makes the old path-encoding hazards
	// trivial: reserved device names, case-only distinctions, deep
	// repositories, ports, and 200-plus-byte components need no
	// escaping to round-trip and stay distinct
	// (REQ-store-bookkeeping).
	"localhost:5000/con/nul:aux":     "sha256:ab33eacc8251e3807b85bb6dba570e4698c3998eca6f0fc2ccb60575a563ea74",
	"r.io/team/deep/nested/repo:v1":  "sha256:b14f0d621bdfd1c967bca28f28ae7c1191e216ce0f34977c9f1e1f5081aae047",
	"r.io/" + longComponent + ":v1":  "sha256:216497b191d24a7998a65618ef588a18befbd1a721ca0486835d2a75dde930bd",
	"r.io/case.sensitive/repo:UPPER": "sha256:ab33eacc8251e3807b85bb6dba570e4698c3998eca6f0fc2ccb60575a563ea74",
	"r.io/case.sensitive/repo:upper": "sha256:b14f0d621bdfd1c967bca28f28ae7c1191e216ce0f34977c9f1e1f5081aae047",
	"r.io/trailing.dot/repo:v1.":     "sha256:216497b191d24a7998a65618ef588a18befbd1a721ca0486835d2a75dde930bd",
}

var longComponent = strings.Repeat("l", 220)

func parseTestRefs(t *testing.T) map[name.Reference]v1.Hash {
	t.Helper()
	refs := make(map[name.Reference]v1.Hash, len(testRefsMap))
	for tref, thash := range testRefsMap {
		ref, err := name.ParseReference(tref)
		if err != nil {
			t.Fatalf("%s: %v", tref, err)
		}
		hash, err := v1.NewHash(thash)
		if err != nil {
			t.Fatal(err)
		}
		refs[ref] = hash
	}
	return refs
}

// TestRefsKeyspaceRoundTrip pins the refs rows of
// REQ-store-bookkeeping: absent before Put, byte-exact after, every
// reference distinct — including the shapes the retired file tier
// needed escaping machinery for.
func TestRefsKeyspaceRoundTrip(t *testing.T) {
	bk, err := openBookkeeping(scratchtest.Dir(t, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer bk.Close()

	cases := parseTestRefs(t)
	for ref := range cases {
		if _, ok, err := bk.RefGet(t.Context(), ref); err != nil || ok {
			t.Fatalf("pre-put state for %q: ok=%v err=%v", ref, ok, err)
		}
	}
	for ref, hash := range cases {
		if err := bk.RefPut(t.Context(), ref, hash); err != nil {
			t.Fatalf("put %q: %v", ref, err)
		}
	}
	for ref, hash := range cases {
		got, ok, err := bk.RefGet(t.Context(), ref)
		if err != nil || !ok {
			t.Fatalf("get %q: ok=%v err=%v", ref, ok, err)
		}
		if got != hash {
			t.Fatalf("get %q: %s, want %s", ref, got, hash)
		}
	}

	// Overwrite: re-resolution replaces the row (the old digest
	// becomes garbage for collection, not an error).
	first, err := name.ParseReference("docker.io/busybox")
	if err != nil {
		t.Fatal(err)
	}
	newHash, err := v1.NewHash("sha256:" + strings.Repeat("cd", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := bk.RefPut(t.Context(), first, newHash); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := bk.RefGet(t.Context(), first); got != newHash {
		t.Fatalf("overwrite: %s, want %s", got, newHash)
	}
}

// TestRefsSurviveReopen pins durability across the database
// lifecycle: rows written before Close are served by a fresh open.
func TestRefsSurviveReopen(t *testing.T) {
	dir := scratchtest.Dir(t, "store")
	bk, err := openBookkeeping(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := name.ParseReference("r.io/reopen/repo:v1")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := v1.NewHash("sha256:" + strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := bk.RefPut(t.Context(), ref, hash); err != nil {
		t.Fatal(err)
	}
	if err := bk.Close(); err != nil {
		t.Fatal(err)
	}
	bk2, err := openBookkeeping(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bk2.Close()
	got, ok, err := bk2.RefGet(t.Context(), ref)
	if err != nil || !ok || got != hash {
		t.Fatalf("after reopen: %s ok=%v err=%v, want %s", got, ok, err, hash)
	}
}

// TestRefsKeyInjective pins the 0x00-separated key layout: component
// boundaries survive, so references whose concatenated components
// coincide stay distinct rows.
func TestRefsKeyInjective(t *testing.T) {
	bk, err := openBookkeeping(scratchtest.Dir(t, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer bk.Close()
	a, err := name.ParseReference("r.io/aab:c")
	if err != nil {
		t.Fatal(err)
	}
	b, err := name.ParseReference("r.io/aa:bc")
	if err != nil {
		t.Fatal(err)
	}
	ha, err := v1.NewHash("sha256:" + strings.Repeat("aa", 32))
	if err != nil {
		t.Fatal(err)
	}
	hb, err := v1.NewHash("sha256:" + strings.Repeat("bb", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := bk.RefPut(t.Context(), a, ha); err != nil {
		t.Fatal(err)
	}
	if err := bk.RefPut(t.Context(), b, hb); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := bk.RefGet(t.Context(), a); got != ha {
		t.Fatalf("r.io/aab:c = %s, want %s (component boundary lost)", got, ha)
	}
	if got, _, _ := bk.RefGet(t.Context(), b); got != hb {
		t.Fatalf("r.io/aa:bc = %s, want %s (component boundary lost)", got, hb)
	}
}

// TestRefsRegistryCaseFolds pins the registry lowercasing: DNS names
// are case-insensitive, so differently-cased spellings address one
// row.
func TestRefsRegistryCaseFolds(t *testing.T) {
	bk, err := openBookkeeping(scratchtest.Dir(t, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer bk.Close()
	lower, err := name.ParseReference("r.io/fold/repo:v1")
	if err != nil {
		t.Fatal(err)
	}
	upper, err := name.ParseReference("R.IO/fold/repo:v1")
	if err != nil {
		t.Fatal(err)
	}
	h, err := v1.NewHash("sha256:" + strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := bk.RefPut(t.Context(), lower, h); err != nil {
		t.Fatal(err)
	}
	got, ok, err := bk.RefGet(t.Context(), upper)
	if err != nil || !ok || got != h {
		t.Fatalf("uppercase spelling: %s ok=%v err=%v, want the lowercase row", got, ok, err)
	}
}

// TestPropertyRefKeyInjective pins key injectivity for all parsed
// references (REQ-store-bookkeeping): distinct references yield
// distinct keys, equal references equal keys — the 0x00 separator
// carries component boundaries for every shape the reference
// grammar admits.
func TestPropertyRefKeyInjective(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		genRef := func(label string) name.Reference {
			host := rapid.SampledFrom([]string{"r.io", "R.IO", "localhost:5000", "a.b.c"}).Draw(rt, label+"-host")
			seg := rapid.SampledFrom([]string{"a", "ab", "aab", "repo", "x.y", "x-y", "x__y"})
			n := rapid.IntRange(1, 3).Draw(rt, label+"-depth")
			parts := make([]string, n)
			for i := range parts {
				parts[i] = seg.Draw(rt, label+"-seg")
			}
			tag := rapid.SampledFrom([]string{"v1", "bc", "c", "latest", "UPPER", "v1."}).Draw(rt, label+"-tag")
			ref, err := name.ParseReference(host + "/" + strings.Join(parts, "/") + ":" + tag)
			if err != nil {
				rt.Skip("unparseable draw")
			}
			return ref
		}
		a, b := genRef("a"), genRef("b")
		ka, kb := string(refKey(a)), string(refKey(b))
		sameIdentity := strings.ToLower(a.Context().RegistryStr()) == strings.ToLower(b.Context().RegistryStr()) &&
			a.Context().RepositoryStr() == b.Context().RepositoryStr() &&
			a.Identifier() == b.Identifier()
		if sameIdentity && ka != kb {
			rt.Fatalf("equal references, distinct keys: %q vs %q", ka, kb)
		}
		if !sameIdentity && ka == kb {
			rt.Fatalf("distinct references share key %q: %s vs %s", ka, a, b)
		}

		// Adversarial derivation: shift the repo/identifier boundary
		// of a — the concatenated bytes coincide, so only the
		// separator keeps the keys distinct.
		repo, id := a.Context().RepositoryStr(), a.Identifier()
		if len(id) > 1 {
			shifted, err := name.ParseReference(
				a.Context().RegistryStr() + "/" + repo + id[:1] + ":" + id[1:])
			if err == nil && shifted.Context().RepositoryStr() != repo {
				if string(refKey(shifted)) == ka {
					rt.Fatalf("boundary shift of %s collides: key %q", a, ka)
				}
			}
		}
	})
}
