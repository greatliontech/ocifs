package ocifs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) {
	base := strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe")
	if helper, ok := strings.CutPrefix(base, "docker-credential-"); ok {
		runFakeCredHelper(helper)
		return
	}
	os.Exit(m.Run())
}

func runFakeCredHelper(mode string) {
	input, _ := io.ReadAll(os.Stdin)
	server := string(input)
	switch mode {
	case "hang":
		time.Sleep(30 * time.Second)
	case "flood":
		chunk := strings.Repeat("A", 64<<10)
		for range 32 {
			if _, err := os.Stdout.WriteString(chunk); err != nil {
				os.Exit(1)
			}
		}
	case "notfound":
		_, _ = os.Stdout.WriteString("credentials not found in native keychain\n")
		os.Exit(1)
	case "token":
		_, _ = fmt.Fprintf(os.Stdout, `{"ServerURL":%q,"Username":"<token>","Secret":"tok-secret-123"}`, server)
	case "echo":
		_, _ = fmt.Fprintf(os.Stdout, `{"ServerURL":%q,"Username":"helper-user","Secret":%q}`, server, "pw:"+server)
	case "chatter":
		// Emit > maxAuthHelperStderrBytes (64 KiB) to stderr before valid JSON on stdout.
		_, _ = os.Stderr.WriteString(strings.Repeat("E", 128<<10))
		_, _ = fmt.Fprintf(os.Stdout, `{"ServerURL":%q,"Username":"chatter-user","Secret":"chatter-pw"}`, server)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown helper %s\n", mode)
		os.Exit(1)
	}
}

func installFakeCredHelpers(t *testing.T, names ...string) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	for _, name := range names {
		dst := filepath.Join(binDir, "docker-credential-"+name+ext)
		if err := os.Symlink(self, dst); err != nil {
			raw, rerr := os.ReadFile(self)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if werr := os.WriteFile(dst, raw, 0o755); werr != nil {
				t.Fatal(werr)
			}
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return binDir
}

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

func isolateAmbientAuthEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DOCKER_AUTH_CONFIG", "")
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("REGISTRY_AUTH_FILE", "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
}

// The exported keychain resolves as the store does (REQ-api-keychain):
// the longest configured prefix at a segment boundary, the default
// keychain where enabled for a miss, anonymous otherwise. The
// ambient keychain is a Docker configuration of the test's own, so
// the default arm resolves a credential only that keychain holds.
func TestKeychainExportedResolvesAsTheStore(t *testing.T) {
	isolateAmbientAuthEnv(t)
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

func TestKeychainContextCancellationAndHelperBounds(t *testing.T) {
	isolateAmbientAuthEnv(t)
	installFakeCredHelpers(t, "hang", "flood", "notfound", "token", "echo", "chatter")

	// 1. Hung credential helper aborts promptly when ctx cancels.
	hangDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hangDir, "config.json"), []byte(`{"credsStore":"hang"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", hangDir)
	kc := &ocifsKeychain{includeDefaultKeychain: true}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := kc.ResolveContext(ctx, testResource("r.io/team/app"))
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung helper err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("hung helper took %s after 120ms deadline", elapsed)
	}

	// 2. Oversized helper stdout (> MaxAuthHelperBytes) fails closed.
	floodDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(floodDir, "config.json"), []byte(`{"credsStore":"flood"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", floodDir)
	if _, err := kc.ResolveContext(context.Background(), testResource("r.io/team/app")); err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("flooding helper err = %v, want output-exceeded error", err)
	}

	// 3. Path-traversal and colon-bearing helper names are refused before exec.
	for _, badHelper := range []string{"../evil", "C:evil", `sub\evil`} {
		travDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(travDir, "config.json"), []byte(`{"credsStore":`+strconv.Quote(badHelper)+`}`), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("DOCKER_CONFIG", travDir)
		if _, err := kc.ResolveContext(context.Background(), testResource("r.io/team/app")); err == nil || !strings.Contains(err.Error(), "invalid credential helper name") {
			t.Fatalf("bad helper %q err = %v, want invalid helper name error", badHelper, err)
		}
	}

	// 4a. credHelpers at registry level MUST override a LIVE global credsStore ("echo"),
	//     and verbose stderr (>64 KiB) must not fail valid helper stdout.
	liveStoreDir := t.TempDir()
	liveStoreJSON := `{
		"credsStore": "echo",
		"credHelpers": {
			"https://tok.io": "token",
			"chat.io": "chatter"
		}
	}`
	if err := os.WriteFile(filepath.Join(liveStoreDir, "config.json"), []byte(liveStoreJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", liveStoreDir)

	tokOverEcho, err := kc.ResolveContext(context.Background(), testResource("tok.io/team/app"))
	if err != nil {
		t.Fatal(err)
	}
	tokOverEchoCfg, err := tokOverEcho.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	if tokOverEchoCfg.IdentityToken != "tok-secret-123" || tokOverEchoCfg.Username != "" {
		t.Fatalf("credHelpers shadowed by live credsStore: got %+v", tokOverEchoCfg)
	}
	if got := resolvedUser(t, kc, "chat.io/team/app"); got != "chatter-user" {
		t.Fatalf("chatter helper (>64 KiB stderr) resolved %q, want chatter-user", got)
	}
	// Docker Hub alias ("docker.io") sent to credsStore normalizes to DefaultAuthKey.
	hubAliasAuth, err := kc.ResolveContext(context.Background(), testResource("docker.io/library/alpine"))
	if err != nil {
		t.Fatal(err)
	}
	hubAliasCfg, err := hubAliasAuth.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	if hubAliasCfg.Password != "pw:"+authn.DefaultAuthKey {
		t.Fatalf("docker.io credsStore serverKey = %q, want %q", hubAliasCfg.Password, "pw:"+authn.DefaultAuthKey)
	}

	// 4b. <token> maps to IdentityToken, and "credentials not found in native keychain"
	//     falls back to inline auths (ignoring empty {} placeholders).
	mixedDir := t.TempDir()
	fallbackAuth := base64.StdEncoding.EncodeToString([]byte("inline-user:inline-pass"))
	mixedJSON := `{
		"credsStore": "notfound",
		"credHelpers": {
			"https://tok.io": "token",
			"index.docker.io": "echo"
		},
		"auths": {
			"fallback.io": {"auth": "` + fallbackAuth + `"}
		}
	}`
	if err := os.WriteFile(filepath.Join(mixedDir, "config.json"), []byte(mixedJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", mixedDir)

	tokAuth, err := kc.ResolveContext(context.Background(), testResource("tok.io/repo"))
	if err != nil {
		t.Fatal(err)
	}
	tokCfg, err := tokAuth.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	if tokCfg.IdentityToken != "tok-secret-123" || tokCfg.Username != "" {
		t.Fatalf("<token> helper resolved %+v, want IdentityToken=tok-secret-123", tokCfg)
	}

	hubAuth, err := kc.ResolveContext(context.Background(), testResource("index.docker.io/library/alpine"))
	if err != nil {
		t.Fatal(err)
	}
	hubCfg, err := hubAuth.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	if hubCfg.Username != "helper-user" || hubCfg.Password != "pw:index.docker.io" {
		t.Fatalf("Docker Hub credHelper resolved %+v", hubCfg)
	}

	if got := resolvedUser(t, kc, "fallback.io/org/img"); got != "inline-user" {
		t.Fatalf("notfound credsStore fallback resolved %q, want inline-user", got)
	}

	noneAuth, err := kc.ResolveContext(context.Background(), testResource("unknown.io/org/img"))
	if err != nil || noneAuth != authn.Anonymous {
		t.Fatalf("notfound credsStore with no inline auth = %v, %v; want Anonymous", noneAuth, err)
	}

	// 5. Multi-file fallthrough (populated DOCKER_CONFIG without match -> REGISTRY_AUTH_FILE),
	//    multi-segment Podman repository prefixes, repo-scoped "/v2" non-widening, empty
	//    placeholder non-shadowing, and fail-closed DOCKER_AUTH_CONFIG JSON parsing.
	dockerNoMatch := t.TempDir()
	hubInline := base64.StdEncoding.EncodeToString([]byte("hub-user:hub-pass"))
	v2RepoAuth := base64.StdEncoding.EncodeToString([]byte("v2-user:v2-pass"))
	dockerNoMatchJSON := `{
		"auths": {
			"https://index.docker.io/v1/": {},
			"index.docker.io": {"auth": "` + hubInline + `"},
			"r.io/v2": {"auth": "` + v2RepoAuth + `"}
		}
	}`
	if err := os.WriteFile(filepath.Join(dockerNoMatch, "config.json"), []byte(dockerNoMatchJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", dockerNoMatch)

	podmanFile := filepath.Join(t.TempDir(), "auth.json")
	podAuth := base64.StdEncoding.EncodeToString([]byte("podman-user:podman-pass"))
	if err := os.WriteFile(podmanFile, []byte(`{"auths":{"quay.io/team":{"auth":"`+podAuth+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REGISTRY_AUTH_FILE", podmanFile)

	// Empty "https://index.docker.io/v1/": {} placeholder must not shadow "index.docker.io".
	if got := resolvedUser(t, kc, "docker.io/library/busybox"); got != "hub-user" {
		t.Fatalf("Docker Hub with empty v1 placeholder resolved %q, want hub-user", got)
	}
	// Scheme-less "r.io/v2" is repository-scoped and must NOT widen to "r.io/other".
	if got := resolvedUser(t, kc, "r.io/v2/app"); got != "v2-user" {
		t.Fatalf("r.io/v2/app resolved %q, want v2-user", got)
	}
	leakAuth, err := kc.ResolveContext(context.Background(), testResource("r.io/other"))
	if err != nil || leakAuth != authn.Anonymous {
		t.Fatalf("r.io/v2 leaked to r.io/other: %v, %v; want Anonymous", leakAuth, err)
	}

	// Miss in DOCKER_CONFIG falls through to XDG_CONFIG_HOME/containers/auth.json when
	// REGISTRY_AUTH_FILE is unset, and REGISTRY_AUTH_FILE suppresses XDG Podman files when set.
	xdgCfg := t.TempDir()
	xdgContainers := filepath.Join(xdgCfg, "containers")
	if err := os.MkdirAll(xdgContainers, 0o700); err != nil {
		t.Fatal(err)
	}
	xdgAuth := base64.StdEncoding.EncodeToString([]byte("xdg-user:xdg-pass"))
	if err := os.WriteFile(filepath.Join(xdgContainers, "auth.json"), []byte(`{"auths":{"xdg.io/ns":{"auth":"`+xdgAuth+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", xdgCfg)
	t.Setenv("REGISTRY_AUTH_FILE", "")
	if got := resolvedUser(t, kc, "xdg.io/ns/app"); got != "xdg-user" {
		t.Fatalf("XDG_CONFIG_HOME/containers/auth.json fallthrough resolved %q, want xdg-user", got)
	}

	t.Setenv("REGISTRY_AUTH_FILE", podmanFile)
	suppressedXDG, err := kc.ResolveContext(context.Background(), testResource("xdg.io/ns/app"))
	if err != nil || suppressedXDG != authn.Anonymous {
		t.Fatalf("REGISTRY_AUTH_FILE did not suppress XDG_CONFIG_HOME/containers/auth.json: %v, %v", suppressedXDG, err)
	}

	// Miss in DOCKER_CONFIG falls through to REGISTRY_AUTH_FILE and matches multi-segment prefix.
	if got := resolvedUser(t, kc, "quay.io/team/sub/app"); got != "podman-user" {
		t.Fatalf("REGISTRY_AUTH_FILE multi-segment repo key resolved %q, want podman-user", got)
	}
	missPodman, err := kc.ResolveContext(context.Background(), testResource("quay.io/teammate/app"))
	if err != nil || missPodman != authn.Anonymous {
		t.Fatalf("quay.io/teammate/app = %v, %v; want Anonymous", missPodman, err)
	}

	envAuth := base64.StdEncoding.EncodeToString([]byte("env-user:env-pass"))
	t.Setenv("DOCKER_AUTH_CONFIG", `{"auths":{"ghcr.io":{"auth":"`+envAuth+`"}}}`)
	if got := resolvedUser(t, kc, "ghcr.io/org/pkg"); got != "env-user" {
		t.Fatalf("DOCKER_AUTH_CONFIG resolved %q, want env-user", got)
	}

	t.Setenv("DOCKER_AUTH_CONFIG", "{malformed-json")
	if _, err := kc.ResolveContext(context.Background(), testResource("ghcr.io/org/pkg")); err == nil {
		t.Fatal("malformed DOCKER_AUTH_CONFIG succeeded unexpectedly")
	}
}
