package ocifs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

const (
	// MaxAuthConfigBytes bounds the size of an ambient Docker/Podman
	// credential configuration file or DOCKER_AUTH_CONFIG payload.
	MaxAuthConfigBytes = 1 << 20

	// MaxAuthHelperBytes bounds stdout read from a docker-credential-*
	// helper process so a runaway helper cannot exhaust memory.
	MaxAuthHelperBytes = 1 << 20

	maxAuthHelperStderrBytes = 64 << 10
	authHelperWaitDelay      = 250 * time.Millisecond
	tokenUsername            = "<token>"
	credsNotFoundMsg         = "credentials not found in native keychain"
)

type ocifsKeychain struct {
	creds                  map[string]authn.AuthConfig
	includeDefaultKeychain bool
}

var (
	_ authn.Keychain        = (*ocifsKeychain)(nil)
	_ authn.ContextKeychain = (*ocifsKeychain)(nil)
)

// Resolve implements authn.Keychain by delegating to ResolveContext
// with context.Background() (REQ-api-keychain).
func (o *ocifsKeychain) Resolve(res authn.Resource) (authn.Authenticator, error) {
	return o.ResolveContext(context.Background(), res)
}

// ResolveContext picks the credential whose configured registry or
// registry/repository prefix is the longest match for the target;
// with no match, the ambient default keychain applies when enabled —
// honoring ctx cancellation and output bounds across credential
// helper subprocesses — else anonymous (REQ-api-keychain).
func (o *ocifsKeychain) ResolveContext(ctx context.Context, res authn.Resource) (authn.Authenticator, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	target := res.String()
	best, found := "", false
	for k := range o.creds {
		if prefixMatches(target, k) && (!found || len(k) > len(best)) {
			best, found = k, true
		}
	}
	if found {
		return authn.FromConfig(o.creds[best]), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.includeDefaultKeychain {
		return resolveAmbientKeychain(ctx, res)
	}
	return authn.Anonymous, nil
}

// prefixMatches reports whether prefix matches target at a
// path-segment boundary: the target is the prefix or continues with
// a path separator. A prefix never matches inside a segment —
// credentials scoped to r.io/team must not reach r.io/teammate
// (REQ-api-keychain).
func prefixMatches(target, prefix string) bool {
	if !strings.HasPrefix(target, prefix) {
		return false
	}
	return len(target) == len(prefix) || target[len(prefix)] == '/'
}

type dockerConfigFile struct {
	Auths       map[string]dockerAuthEntry `json:"auths"`
	CredsStore  string                     `json:"credsStore"`
	CredHelpers map[string]string          `json:"credHelpers"`
}

type dockerAuthEntry struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Auth          string `json:"auth"`
	IdentityToken string `json:"identitytoken"`
	RegistryToken string `json:"registrytoken"`
}

type helperResponse struct {
	ServerURL string `json:"ServerURL"`
	Username  string `json:"Username"`
	Secret    string `json:"Secret"`
}

func resolveAmbientKeychain(ctx context.Context, target authn.Resource) (authn.Authenticator, error) {
	if envRaw := os.Getenv("DOCKER_AUTH_CONFIG"); envRaw != "" {
		if len(envRaw) > MaxAuthConfigBytes {
			return nil, fmt.Errorf("ocifs: DOCKER_AUTH_CONFIG exceeds %d bytes", MaxAuthConfigBytes)
		}
		var envCfg dockerConfigFile
		if err := json.Unmarshal([]byte(envRaw), &envCfg); err != nil {
			return nil, fmt.Errorf("ocifs: parsing DOCKER_AUTH_CONFIG: %w", err)
		}
		auth, err := resolveFromConfig(ctx, &envCfg, target)
		if err != nil {
			return nil, err
		}
		if auth != authn.Anonymous {
			return auth, nil
		}
	}

	for _, cfgPath := range ambientConfigPaths() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cfg, err := loadDockerConfigFile(cfgPath)
		if err != nil {
			return nil, err
		}
		auth, err := resolveFromConfig(ctx, cfg, target)
		if err != nil {
			return nil, err
		}
		if auth != authn.Anonymous {
			return auth, nil
		}
	}
	return authn.Anonymous, nil
}

func ambientConfigPaths() []string {
	home, _ := os.UserHomeDir()
	dockerCfg := os.Getenv("DOCKER_CONFIG")
	regAuth := os.Getenv("REGISTRY_AUTH_FILE")

	var candidates []string
	if dockerCfg != "" {
		candidates = append(candidates, filepath.Join(dockerCfg, "config.json"))
	}
	if regAuth != "" {
		candidates = append(candidates, filepath.Clean(regAuth))
	}
	if dockerCfg == "" && home != "" {
		candidates = append(candidates, filepath.Join(home, ".docker", "config.json"))
	}
	if regAuth == "" {
		if xdgRun := os.Getenv("XDG_RUNTIME_DIR"); xdgRun != "" {
			candidates = append(candidates, filepath.Join(xdgRun, "containers", "auth.json"))
		}
		configDir := os.Getenv("XDG_CONFIG_HOME")
		if configDir == "" && home != "" {
			configDir = filepath.Join(home, ".config")
		}
		if configDir != "" {
			candidates = append(candidates, filepath.Join(configDir, "containers", "auth.json"))
		}
	}

	var existing []string
	seen := make(map[string]bool, len(candidates))
	for _, p := range candidates {
		clean := filepath.Clean(p)
		if seen[clean] {
			continue
		}
		seen[clean] = true
		if regularFileExists(clean) {
			existing = append(existing, clean)
		}
	}
	return existing
}

func regularFileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}

func loadDockerConfigFile(path string) (*dockerConfigFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxAuthConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxAuthConfigBytes {
		return nil, fmt.Errorf("ocifs: auth config %s exceeds %d bytes", path, MaxAuthConfigBytes)
	}
	var cfg dockerConfigFile
	if len(bytes.TrimSpace(raw)) == 0 {
		return &cfg, nil
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("ocifs: parsing auth config %s: %w", path, err)
	}
	return &cfg, nil
}

func resolveFromConfig(ctx context.Context, cfg *dockerConfigFile, target authn.Resource) (authn.Authenticator, error) {
	prefixes := targetSegmentPrefixes(target)

	// 1. Per-registry / per-repository credHelpers (longest segment prefix wins).
	for _, pfx := range prefixes {
		if helper, serverKey := matchCredHelper(cfg.CredHelpers, pfx); helper != "" {
			ac, found, err := execCredentialHelper(ctx, helper, serverKey)
			if err != nil {
				return nil, err
			}
			if found {
				return authn.FromConfig(ac), nil
			}
		}
	}

	// 2. Global credsStore queried once at the registry level.
	if cfg.CredsStore != "" && target.RegistryStr() != "" {
		serverKey := registryServerKey(target.RegistryStr())
		ac, found, err := execCredentialHelper(ctx, cfg.CredsStore, serverKey)
		if err != nil {
			return nil, err
		}
		if found {
			return authn.FromConfig(ac), nil
		}
	}

	// 3. Inline auths (longest segment prefix wins, supporting both
	//    registry-level Docker auths and repository-scoped Podman auths).
	for _, pfx := range prefixes {
		ac, err := matchFileAuth(cfg.Auths, pfx)
		if err != nil {
			return nil, err
		}
		if ac != (authn.AuthConfig{}) {
			return authn.FromConfig(ac), nil
		}
	}
	return authn.Anonymous, nil
}

func targetSegmentPrefixes(target authn.Resource) []string {
	full := strings.Trim(target.String(), "/")
	var out []string
	seen := map[string]bool{}
	for cur := full; cur != ""; {
		norm := normalizeTargetPrefix(cur)
		if !seen[norm] {
			seen[norm] = true
			out = append(out, norm)
		}
		idx := strings.LastIndexByte(cur, '/')
		if idx < 0 {
			break
		}
		cur = cur[:idx]
	}
	if reg := strings.Trim(target.RegistryStr(), "/"); reg != "" {
		normReg := normalizeTargetPrefix(reg)
		if !seen[normReg] {
			out = append(out, normReg)
		}
	}
	return out
}

func normalizeTargetPrefix(s string) string {
	host, rest, hasPath := strings.Cut(s, "/")
	host = canonicalRegistryHost(host)
	if hasPath && rest != "" {
		return host + "/" + rest
	}
	return host
}

func canonicalRegistryHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	switch h {
	case "docker.io", "registry-1.docker.io", name.DefaultRegistry:
		return name.DefaultRegistry
	}
	return h
}

func registryServerKey(reg string) string {
	if canonicalRegistryHost(reg) == name.DefaultRegistry {
		return authn.DefaultAuthKey
	}
	return strings.ToLower(strings.TrimSpace(reg))
}

func matchCredHelper(helpers map[string]string, normTarget string) (helper, serverKey string) {
	if len(helpers) == 0 {
		return "", ""
	}
	bestRaw := ""
	for rawK, h := range helpers {
		if h == "" {
			continue
		}
		if normalizeAuthKey(rawK) == normTarget && (bestRaw == "" || rawK < bestRaw) {
			bestRaw = rawK
		}
	}
	if bestRaw == "" {
		return "", ""
	}
	return helpers[bestRaw], bestRaw
}

func matchFileAuth(auths map[string]dockerAuthEntry, normTarget string) (authn.AuthConfig, error) {
	if len(auths) == 0 {
		return authn.AuthConfig{}, nil
	}
	bestRaw := ""
	for rawK, entry := range auths {
		if entry == (dockerAuthEntry{}) {
			continue
		}
		if normalizeAuthKey(rawK) == normTarget && (bestRaw == "" || rawK < bestRaw) {
			bestRaw = rawK
		}
	}
	if bestRaw == "" {
		return authn.AuthConfig{}, nil
	}
	return decodeAuthEntry(auths[bestRaw])
}

// normalizeAuthKey canonicalizes Docker/Podman config keys:
//   - strips http(s):// schemes (case-insensitively) and trailing slashes;
//   - strips legacy /v1 or /v2 API suffixes ONLY when the key carried an
//     http(s):// scheme or trailing slash (so a scheme-less repository key
//     such as "r.io/v2" never widens to registry-wide "r.io");
//   - lowercases the registry hostname while preserving repository path case;
//   - unifies Docker Hub's historical hostnames onto index.docker.io.
func normalizeAuthKey(k string) string {
	s := strings.TrimSpace(k)
	lower := strings.ToLower(s)
	hadScheme := false
	switch {
	case strings.HasPrefix(lower, "https://"):
		s = s[len("https://"):]
		hadScheme = true
	case strings.HasPrefix(lower, "http://"):
		s = s[len("http://"):]
		hadScheme = true
	}
	hadTrailingSlash := strings.HasSuffix(s, "/")
	s = strings.TrimSuffix(s, "/")
	if hadScheme || hadTrailingSlash {
		for _, apiSuffix := range []string{"/v1", "/v2"} {
			if host, ok := strings.CutSuffix(s, apiSuffix); ok && !strings.Contains(host, "/") {
				s = host
				break
			}
		}
	}
	host, rest, hasPath := strings.Cut(s, "/")
	host = canonicalRegistryHost(host)
	if hasPath && rest != "" {
		return host + "/" + rest
	}
	return host
}

func decodeAuthEntry(e dockerAuthEntry) (authn.AuthConfig, error) {
	username := e.Username
	password := e.Password
	if e.Auth != "" {
		raw, err := base64.StdEncoding.DecodeString(e.Auth)
		if err != nil {
			return authn.AuthConfig{}, fmt.Errorf("ocifs: decoding base64 auth entry: %w", err)
		}
		u, p, ok := strings.Cut(string(raw), ":")
		if !ok {
			return authn.AuthConfig{}, errors.New("ocifs: invalid base64 auth entry (missing ':')")
		}
		username = u
		password = strings.Trim(p, "\x00")
	}
	return authn.AuthConfig{
		Username:      username,
		Password:      password,
		IdentityToken: e.IdentityToken,
		RegistryToken: e.RegistryToken,
	}, nil
}

func execCredentialHelper(ctx context.Context, helper, serverKey string) (authn.AuthConfig, bool, error) {
	if helper == "" || strings.ContainsAny(helper, `/\:`+"\x00") || helper == "." || helper == ".." {
		return authn.AuthConfig{}, false, fmt.Errorf("ocifs: invalid credential helper name %q", helper)
	}
	if err := ctx.Err(); err != nil {
		return authn.AuthConfig{}, false, err
	}

	prog := "docker-credential-" + helper
	cmd := exec.CommandContext(ctx, prog, "get")
	cmd.Stdin = strings.NewReader(serverKey)
	cmd.WaitDelay = authHelperWaitDelay

	stdout := limitedBuffer{limit: MaxAuthHelperBytes}
	stderr := limitedBuffer{limit: maxAuthHelperStderrBytes, discardOnOverflow: true}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return authn.AuthConfig{}, false, fmt.Errorf("ocifs: credential helper %s: %w", prog, ctxErr)
		}
		if stdout.exceeded {
			return authn.AuthConfig{}, false, fmt.Errorf("ocifs: credential helper %s output exceeded %d bytes", prog, MaxAuthHelperBytes)
		}
		msg := strings.TrimSpace(stdout.buf.String())
		if msg == "" {
			msg = strings.TrimSpace(stderr.buf.String())
		}
		if msg == credsNotFoundMsg {
			return authn.AuthConfig{}, false, nil
		}
		if msg != "" {
			return authn.AuthConfig{}, false, fmt.Errorf("ocifs: credential helper %s: %w: %s", prog, err, msg)
		}
		return authn.AuthConfig{}, false, fmt.Errorf("ocifs: credential helper %s: %w", prog, err)
	}
	if stdout.exceeded {
		return authn.AuthConfig{}, false, fmt.Errorf("ocifs: credential helper %s output exceeded %d bytes", prog, MaxAuthHelperBytes)
	}

	raw := bytes.TrimSpace(stdout.buf.Bytes())
	if len(raw) == 0 || string(raw) == credsNotFoundMsg {
		return authn.AuthConfig{}, false, nil
	}
	var resp helperResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return authn.AuthConfig{}, false, fmt.Errorf("ocifs: parsing credential helper %s output: %w", prog, err)
	}
	if resp.Username == "" && resp.Secret == "" {
		return authn.AuthConfig{}, false, nil
	}
	if resp.Username == tokenUsername {
		return authn.AuthConfig{IdentityToken: resp.Secret}, true, nil
	}
	return authn.AuthConfig{Username: resp.Username, Password: resp.Secret}, true, nil
}

type limitedBuffer struct {
	buf               bytes.Buffer
	limit             int
	exceeded          bool
	discardOnOverflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	rem := b.limit - b.buf.Len()
	if len(p) <= rem {
		return b.buf.Write(p)
	}
	if rem > 0 {
		_, _ = b.buf.Write(p[:rem])
	}
	b.exceeded = true
	if b.discardOnOverflow {
		return len(p), nil
	}
	return 0, errors.New("output limit exceeded")
}
