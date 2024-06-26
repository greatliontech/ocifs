package ocifs

import (
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
)

type ocifsKeychain struct {
	creds                  map[string]authn.AuthConfig
	includeDefaultKeychain bool
}

// Resolve looks up the most appropriate credential for the specified target.
func (o *ocifsKeychain) Resolve(res authn.Resource) (authn.Authenticator, error) {
	for k, v := range o.creds {
		if strings.HasPrefix(k, res.String()) {
			return authn.FromConfig(v), nil
		}
	}
	if o.includeDefaultKeychain {
		return authn.DefaultKeychain.Resolve(res)
	}
	return authn.Anonymous, nil
}
