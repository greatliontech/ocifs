package ocifs

import (
	"encoding/base64"
	"os"
	"path/filepath"
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

func resolvedUser(t *testing.T, kc authn.Keychain, target string) string {
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

// The exported keychain resolves as the store does (REQ-api-keychain):
// the longest configured prefix at a segment boundary, the default
// keychain where enabled for a miss, anonymous otherwise. The
// ambient keychain is a Docker configuration of the test's own, so
// the default arm resolves a credential only that keychain holds.
func TestKeychainExportedResolvesAsTheStore(t *testing.T) {
	dockerConfig := t.TempDir()
	if err := os.WriteFile(filepath.Join(dockerConfig, "config.json"), []byte(`{"auths":{"r.io":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("ambient:secret"))+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", dockerConfig)
	source := WithAuthSource("r.io/team", authn.AuthConfig{Username: "team"})
	miss := "r.io/teammate/plugin"

	withDefault, err := New(WithWorkDir(t.TempDir()), source, WithEnableDefaultKeychain())
	if err != nil {
		t.Fatal(err)
	}
	defer withDefault.Close()
	kc := withDefault.Keychain()
	if got := resolvedUser(t, kc, "r.io/team/plugin"); got != "team" {
		t.Fatalf("prefix hit resolved %q, want team", got)
	}
	if got := resolvedUser(t, kc, miss); got != "ambient" {
		t.Fatalf("a miss under the default keychain resolved %q, want the ambient credential", got)
	}

	without, err := New(WithWorkDir(t.TempDir()), source)
	if err != nil {
		t.Fatal(err)
	}
	defer without.Close()
	a, err := without.Keychain().Resolve(testResource(miss))
	if err != nil {
		t.Fatal(err)
	}
	if a != authn.Anonymous {
		t.Fatalf("a miss without the default keychain resolved %v, want anonymous", a)
	}
}
