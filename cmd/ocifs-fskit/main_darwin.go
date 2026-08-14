//go:build darwin

// Command ocifs-fskit is the FSKit app-extension binary: a pure-Go
// FSKit module serving ocifs projections (docs/specs/projection.md,
// docs/specs/api.md REQ-api-mount-darwin). The platform spawns it
// when an ocifs volume is mounted; its entire configuration arrives
// declaratively through the mount resource and options
// (REQ-proj-server), and it serves cached store content only.
//
// The bootstrap mirrors fskit-go's validation appex (itself modeled
// on Apple's zero-Swift msdos module): register the Go-backed
// principal class first, then hand the main thread to Foundation's
// NSExtensionMain, which reads the ExtensionKit plist keys and
// blocks in the extension run loop. Tier-2 validation — a real
// mount through a signed, enabled extension — is a user-side act.
package main

import (
	"fmt"
	"os"

	"github.com/ebitengine/purego"
	fskit "github.com/greatliontech/fskit-go"

	"github.com/greatliontech/ocifs/internal/fskitfs"
)

// principalClass MUST match EXExtensionPrincipalClass in the appex
// Info.plist (nested under EXAppExtensionAttributes), and FSShortName
// there is the `mount -F -t` name.
const principalClass = "OcifsFS"

const foundation = "/System/Library/Frameworks/Foundation.framework/Foundation"

func main() {
	if _, err := fskit.RegisterFileSystemNamed(principalClass, fskitfs.FileSystem{}); err != nil {
		fmt.Fprintf(os.Stderr, "ocifs-fskit: RegisterFileSystemNamed(%q): %v\n", principalClass, err)
		os.Exit(1)
	}
	h, err := purego.Dlopen(foundation, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ocifs-fskit: dlopen Foundation: %v\n", err)
		os.Exit(1)
	}
	// int NSExtensionMain(void): blocks in the extension run loop for
	// the extension's lifetime.
	var nsExtensionMain func() int32
	purego.RegisterLibFunc(&nsExtensionMain, h, "NSExtensionMain")
	rc := nsExtensionMain()
	fmt.Fprintf(os.Stderr, "ocifs-fskit: NSExtensionMain returned %d (unexpected while live)\n", rc)
}
