package ocifs

import (
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"pgregory.net/rapid"
)

// testResource is an authn.Resource with an arbitrary target string,
// so prefix-resolution tests need no reference parsing.
type testResource string

func (r testResource) String() string { return string(r) }
func (r testResource) RegistryStr() string {
	s, _, _ := strings.Cut(string(r), "/")
	return s
}

func resolvedUser(t *testing.T, kc *ocifsKeychain, target string) string {
	t.Helper()
	a, err := kc.Resolve(testResource(target))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := a.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Username
}

// TestKeychainLongestPrefixWins pins REQ-api-keychain's anchor
// cases: the longest configured registry or registry/repository
// prefix wins, and no match resolves anonymously when the default
// keychain is not enabled.
func TestKeychainLongestPrefixWins(t *testing.T) {
	kc := &ocifsKeychain{creds: map[string]authn.AuthConfig{
		"registry.example.com":          {Username: "registry"},
		"registry.example.com/team":     {Username: "team"},
		"registry.example.com/team/app": {Username: "app"},
	}}
	for target, want := range map[string]string{
		"registry.example.com/team/app": "app",
		"registry.example.com/team/web": "team",
		"registry.example.com/other":    "registry",
		// Segment boundary: team's credentials must not leak to the
		// foreign repository teammate; the registry-wide entry applies.
		"registry.example.com/teammate/app": "registry",
		"registry.example.com/team/apps":    "team",
	} {
		if got := resolvedUser(t, kc, target); got != want {
			t.Fatalf("Resolve(%q) = user %q, want %q", target, got, want)
		}
	}

	a, err := kc.Resolve(testResource("other.io/x"))
	if err != nil {
		t.Fatal(err)
	}
	if a != authn.Anonymous {
		t.Fatalf("no-match resolution = %v, want anonymous", a)
	}
}

// TestPropertyKeychainDeterministicLongestPrefix pins
// REQ-api-keychain as a for-all: resolution equals the brute-force
// longest-matching-prefix oracle and never varies across calls,
// whatever overlapping prefix set is configured.
func TestPropertyKeychainDeterministicLongestPrefix(t *testing.T) {
	prefixes := rapid.SampledFrom([]string{
		"r1.io", "r2.io", "r1.io/a", "r1.io/ab", "r1.io/a/b", "r2.io/x", "r2.io/x/y",
	})
	suffixes := rapid.SampledFrom([]string{"", "/z", "c", "/a"})
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 5).Draw(rt, "n")
		creds := map[string]authn.AuthConfig{}
		for i := 0; i < n; i++ {
			p := prefixes.Draw(rt, "prefix"+strconv.Itoa(i))
			creds[p] = authn.AuthConfig{Username: p}
		}
		target := prefixes.Draw(rt, "target") + suffixes.Draw(rt, "suffix")
		kc := &ocifsKeychain{creds: creds}

		// Oracle: longest configured prefix that the target either
		// equals or extends with a '/' — stated directly from
		// REQ-api-keychain's segment-boundary clause.
		want := ""
		for k := range creds {
			boundary := len(target) == len(k) || (len(target) > len(k) && target[len(k)] == '/')
			if strings.HasPrefix(target, k) && boundary && len(k) > len(want) {
				want = k
			}
		}

		for range 3 {
			a, err := kc.Resolve(testResource(target))
			if err != nil {
				rt.Fatal(err)
			}
			if want == "" {
				if a != authn.Anonymous {
					rt.Fatalf("Resolve(%q) matched despite no configured prefix matching", target)
				}
				continue
			}
			cfg, err := a.Authorization()
			if err != nil {
				rt.Fatal(err)
			}
			if cfg.Username != want {
				rt.Fatalf("Resolve(%q) = %q, want longest prefix %q", target, cfg.Username, want)
			}
		}
	})
}
