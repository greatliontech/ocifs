package store

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

var testRefsMap = map[string]string{
	"docker.io/busybox":                   "sha256:ab33eacc8251e3807b85bb6dba570e4698c3998eca6f0fc2ccb60575a563ea74",
	"gcr.io/distroless/base:latest-amd64": "sha256:b14f0d621bdfd1c967bca28f28ae7c1191e216ce0f34977c9f1e1f5081aae047",
	"ghcr.io/greatliontech/pbr:v0.3.9":    "sha256:216497b191d24a7998a65618ef588a18befbd1a721ca0486835d2a75dde930bd",
}

func pareseTestRefs() (map[name.Reference]v1.Hash, error) {
	refs := make(map[name.Reference]v1.Hash, len(testRefsMap))
	for tref, thash := range testRefsMap {
		ref, err := name.ParseReference(tref)
		if err != nil {
			return nil, err
		}
		hash, err := v1.NewHash(thash)
		if err != nil {
			return nil, err
		}
		refs[ref] = hash
	}
	return refs, nil
}

func TestPrsedRef(t *testing.T) {
	tmpDir := t.TempDir()
	refStore := referenceStore(tmpDir)

	testCases, err := pareseTestRefs()
	if err != nil {
		t.Fatal(err)
	}

	for ref, hash := range testCases {
		t.Run(ref.String(), func(t *testing.T) {
			_, ok, err := refStore.Get(ref)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatalf("expected ref %q to be missing", ref)
			}
			if err := refStore.Put(ref, hash); err != nil {
				t.Fatal(err)
			}
			retHash, ok, err := refStore.Get(ref)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatalf("expected ref %q to be present", ref)
			}
			if retHash != hash {
				t.Fatalf("expected hash %q, got %q", hash, retHash)
			}
		})
	}
}
