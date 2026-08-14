package ocifs

import (
	"os"
	"testing"
)

// skipUnderMutationCampaign excludes mount-performing tests from
// mutation campaigns: mount(2) escapes the campaign's observation
// bracket — a mutant of mount-orchestration code can mount at any
// path it computes, and a killed mutant process leaks the live
// mount. The path is verified by the normal suite and hand probes.
func skipUnderMutationCampaign(t *testing.T) {
	t.Helper()
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("mount-performing test skipped under mutation campaign")
	}
}
