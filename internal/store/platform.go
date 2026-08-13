package store

import (
	"fmt"
	"runtime"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// hostPlatform is the fallback request platform when construction
// configures none (REQ-store-platform-default).
func hostPlatform() v1.Platform {
	return v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

// platformMatches reports whether cand satisfies the requested
// platform: every field the request specifies (os, architecture,
// variant, os.version) equals cand's, an unspecified field
// constrains nothing, and a candidate carrying no platform never
// matches (REQ-store-platform-strict).
func platformMatches(req v1.Platform, cand *v1.Platform) bool {
	if cand == nil {
		return false
	}
	match := func(want, got string) bool { return want == "" || want == got }
	return match(req.OS, cand.OS) &&
		match(req.Architecture, cand.Architecture) &&
		match(req.Variant, cand.Variant) &&
		match(req.OSVersion, cand.OSVersion)
}

// selectChild picks the one index child matching the requested
// platform. Zero matches and multiple matches both fail: choosing
// among several would be a fallback, and the caller's remedy is a
// more specific request (REQ-store-platform-strict).
func selectChild(idx *v1.IndexManifest, req v1.Platform) (v1.Descriptor, error) {
	var matches []v1.Descriptor
	for _, d := range idx.Manifests {
		if platformMatches(req, d.Platform) {
			matches = append(matches, d)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return v1.Descriptor{}, fmt.Errorf("no manifest for platform %s in index (platforms: %s)", req.String(), indexPlatforms(idx))
	default:
		var alts []string
		for _, d := range matches {
			alts = append(alts, d.Platform.String())
		}
		return v1.Descriptor{}, fmt.Errorf("platform %s is ambiguous in index: matches %s; request a more specific platform", req.String(), strings.Join(alts, ", "))
	}
}

func indexPlatforms(idx *v1.IndexManifest) string {
	var ps []string
	for _, d := range idx.Manifests {
		if d.Platform != nil {
			ps = append(ps, d.Platform.String())
		}
	}
	if len(ps) == 0 {
		return "none"
	}
	return strings.Join(ps, ", ")
}

// configMatchesPlatform applies the request-field match rule to a
// direct manifest's config platform (REQ-store-platform-strict's
// manifest clause; only explicit requests are checked).
func configMatchesPlatform(req v1.Platform, cfg *v1.ConfigFile) bool {
	if cfg == nil {
		return false
	}
	return platformMatches(req, &v1.Platform{
		OS:           cfg.OS,
		Architecture: cfg.Architecture,
		Variant:      cfg.Variant,
		OSVersion:    cfg.OSVersion,
	})
}
