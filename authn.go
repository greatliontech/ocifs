package ocifs

import (
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
)

type ocifsKeychain struct {
	creds                  map[string]authn.AuthConfig
	includeDefaultKeychain bool
}

// Resolve picks the credential whose configured registry or
// registry/repository prefix is the longest match for the target;
// with no match, the ambient default keychain applies when enabled,
// else anonymous. The longest match is unique (prefixes are map
// keys), so overlapping prefixes always resolve the same way
// (REQ-api-keychain).
func (o *ocifsKeychain) Resolve(res authn.Resource) (authn.Authenticator, error) {
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
	if o.includeDefaultKeychain {
		return authn.DefaultKeychain.Resolve(res)
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
