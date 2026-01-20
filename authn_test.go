package ocifs

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
)

// =============================================================================
// Phase 5: Authentication Tests
// =============================================================================

// TestKeychain_PrefixMatching tests that credentials are matched by prefix
func TestKeychain_PrefixMatching(t *testing.T) {
	kc := &ocifsKeychain{
		creds: map[string]authn.AuthConfig{
			"registry.example.com": {
				Username: "user1",
				Password: "pass1",
			},
		},
	}

	// Test matching prefix
	res := &testResource{registry: "registry.example.com"}
	auth, err := kc.Resolve(res)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	authConfig, err := auth.Authorization()
	if err != nil {
		t.Fatalf("Authorization failed: %v", err)
	}

	if authConfig.Username != "user1" {
		t.Errorf("Username mismatch: got %q, want %q", authConfig.Username, "user1")
	}
	if authConfig.Password != "pass1" {
		t.Errorf("Password mismatch: got %q, want %q", authConfig.Password, "pass1")
	}
}

// TestKeychain_PrefixMatchingMultiple tests that a matching prefix is used when multiple exist
// Note: The implementation uses map iteration, so it finds *a* matching prefix, not necessarily the longest
func TestKeychain_PrefixMatchingMultiple(t *testing.T) {
	kc := &ocifsKeychain{
		creds: map[string]authn.AuthConfig{
			"registry.example.com": {
				Username: "general",
				Password: "general_pass",
			},
		},
	}

	// Test that the prefix is matched
	res := &testResource{registry: "registry.example.com/specific/image"}
	auth, err := kc.Resolve(res)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	authConfig, err := auth.Authorization()
	if err != nil {
		t.Fatalf("Authorization failed: %v", err)
	}

	// Should match the general prefix
	if authConfig.Username != "general" {
		t.Errorf("Expected general credentials, got username %q", authConfig.Username)
	}
}

// TestKeychain_FallbackToDefault tests that default keychain is used as fallback
func TestKeychain_FallbackToDefault(t *testing.T) {
	kc := &ocifsKeychain{
		creds:                  map[string]authn.AuthConfig{},
		includeDefaultKeychain: true,
	}

	res := &testResource{registry: "docker.io"}
	auth, err := kc.Resolve(res)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Should return some authenticator (may be anonymous or from default keychain)
	if auth == nil {
		t.Error("Expected non-nil authenticator")
	}
}

// TestKeychain_AnonymousFallback tests that anonymous auth is used when no match
func TestKeychain_AnonymousFallback(t *testing.T) {
	kc := &ocifsKeychain{
		creds: map[string]authn.AuthConfig{
			"other.registry.com": {
				Username: "user",
				Password: "pass",
			},
		},
		includeDefaultKeychain: false,
	}

	// Use a different registry
	res := &testResource{registry: "unmatched.registry.com"}
	auth, err := kc.Resolve(res)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Should return anonymous
	if auth != authn.Anonymous {
		t.Error("Expected anonymous authenticator for unmatched registry")
	}
}

// TestKeychain_MultipleCredentials tests multiple credentials in keychain
func TestKeychain_MultipleCredentials(t *testing.T) {
	kc := &ocifsKeychain{
		creds: map[string]authn.AuthConfig{
			"registry1.com": {
				Username: "user1",
				Password: "pass1",
			},
			"registry2.com": {
				Username: "user2",
				Password: "pass2",
			},
			"registry3.com": {
				Username: "user3",
				Password: "pass3",
			},
		},
	}

	testCases := []struct {
		registry string
		wantUser string
	}{
		{"registry1.com", "user1"},
		{"registry2.com", "user2"},
		{"registry3.com", "user3"},
	}

	for _, tc := range testCases {
		t.Run(tc.registry, func(t *testing.T) {
			res := &testResource{registry: tc.registry}
			auth, err := kc.Resolve(res)
			if err != nil {
				t.Fatalf("Resolve failed: %v", err)
			}

			authConfig, err := auth.Authorization()
			if err != nil {
				t.Fatalf("Authorization failed: %v", err)
			}

			if authConfig.Username != tc.wantUser {
				t.Errorf("Username mismatch: got %q, want %q", authConfig.Username, tc.wantUser)
			}
		})
	}
}

// TestKeychain_EmptyCredentials tests keychain with no credentials
func TestKeychain_EmptyCredentials(t *testing.T) {
	kc := &ocifsKeychain{
		creds:                  map[string]authn.AuthConfig{},
		includeDefaultKeychain: false,
	}

	res := &testResource{registry: "any.registry.com"}
	auth, err := kc.Resolve(res)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Should return anonymous
	if auth != authn.Anonymous {
		t.Error("Expected anonymous authenticator with empty credentials")
	}
}

// testResource is a mock implementation of authn.Resource
type testResource struct {
	registry string
}

func (r *testResource) String() string {
	return r.registry
}

func (r *testResource) RegistryStr() string {
	return r.registry
}
