package ocifs

import (
	"log/slog"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
)

type ocifsKeychain struct {
	creds                  map[string]authn.AuthConfig
	includeDefaultKeychain bool
}

// Resolve looks up the most appropriate credential for the specified target.
func (o *ocifsKeychain) Resolve(res authn.Resource) (authn.Authenticator, error) {
	slog.Debug("resolving creds for", "resource", res.String())
	for k, v := range o.creds {
		if strings.HasPrefix(res.String(), k) {
			slog.Debug("found creds for prefix", "prefix", k)
			return authn.FromConfig(v), nil
		}
	}
	if o.includeDefaultKeychain {
		return authn.DefaultKeychain.Resolve(res)
	}
	return authn.Anonymous, nil
}
