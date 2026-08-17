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
	return fallbackPlatform(runtime.GOOS, runtime.GOARCH, hostARMVariant)
}

// fallbackPlatform derives the built-in default platform from the
// host. On darwin the fallback is linux with the host's
// architecture: an os=darwin request could never match published
// images, and darwin mounts serve linux root filesystems. On a
// 32-bit arm host it carries the detected CPU variant — without
// one, linux/arm is ambiguous against standard indexes' arm/v6 and
// arm/v7 children (REQ-store-platform-default).
func fallbackPlatform(goos, goarch string, armVariant func() string) v1.Platform {
	if goos == "darwin" {
		goos = "linux"
	}
	p := v1.Platform{OS: goos, Architecture: goarch}
	if goarch == "arm" {
		p.Variant = armVariant()
	}
	return p
}

// parseARMVariant maps /proc/cpuinfo content to the OCI variant of
// a 32-bit arm host, first core wins. The kernel's "CPU
// architecture" field carries proc_arch strings (5T/5TE/5TEJ,
// 6TEJ, 7, 7M, 8, AArch64 on old arm64 kernels); 8-class values
// mean 32-bit userland on v8 hardware, which runs arm/v7 images.
// Unrecognized content yields no variant
// (REQ-store-platform-default: loud ambiguity over a guess).
func parseARMVariant(cpuinfo []byte) string {
	arch, model := "", ""
	for line := range strings.Lines(string(cpuinfo)) {
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "CPU architecture":
			if arch == "" {
				arch = strings.TrimSpace(val)
			}
		case "model name", "Processor":
			// Pre-3.8 arm kernels title the model line "Processor".
			if model == "" {
				model = strings.TrimSpace(val)
			}
		}
	}
	// ARM1176 kernels report architecture 7 with a v6 model name
	// (Raspberry Pi 1/Zero); the model line is the truth there —
	// v7 binaries trap on this hardware.
	if arch == "7" && strings.HasPrefix(strings.ToLower(model), "armv6-compatible") {
		return "v6"
	}
	switch strings.ToLower(arch) {
	case "5", "5t", "5te", "5tej":
		return "v5"
	case "6", "6tej":
		return "v6"
	case "7", "7m":
		return "v7"
	case "8", "aarch64":
		return "v7"
	}
	return ""
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
