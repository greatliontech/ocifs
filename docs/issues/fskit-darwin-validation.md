# FSKit backend awaits its darwin Tier-2 mount validation

The darwin backend is authored and its portable core is pinned on
linux: the Volume/FileSystem implementation (`internal/fskitfs`)
runs the full read-only contract in the linux suite — identity,
attributes with fallbacks, enumeration cookies and the constant
verifier, short-read-at-EOF content, EROFS from every mutating
operation, the case-sensitivity declaration, and the whole
declarative Load path end to end against a populated store with the
network gone. `GOOS=darwin` builds are clean.

What no linux test can reach is the platform half: FSKit spawning
the signed extension, NSExtensionMain hosting the Go runtime,
ObjC-bridge dispatch into the Volume, and the orchestrated
`mount -F -t OcifsFS` path in `mount_darwin.go`. fskit-go's own
Tier-2 status is unproven (its validation README says the bootstrap
has never run on a real Mac), so this validation is a double
experiment: the binding's bootstrap and ocifs's module together.

To validate, on macOS 15.4+ with a paid Apple Developer team:

1. Register an app group and adjust the group id in
   `packaging/darwin/*-entitlements.plist`; provision an App ID with
   the FSKit Module capability for `tech.greatlion.ocifs.OcifsFS` —
   and enable the App Groups capability on BOTH App IDs (host and
   extension) and their profiles: the host app carries the group
   entitlement without an embedded profile, so a team whose policy
   enforces group authorization may need a host profile too
   (embed it beside the appex one in build.sh if launch fails with a
   provisioning error).
2. `SIGN_IDENTITY=… PROVISION_PROFILE=… packaging/darwin/build.sh`,
   install per the script's closing instructions, enable the
   extension in System Settings.
3. Pull an image with the CLI using a work directory inside the
   group container, then mount and read through the volume; capture
   the appex's stderr (Console.app) and report everything observed.
4. Also mount once with a deliberately case-mismatched mountpoint
   path (e.g. `MNT` for an on-disk `mnt` on case-insensitive APFS):
   the orchestrator canonicalizes symlinks but not letter case, so a
   case-mismatched caller path may make the mount-liveness poll
   misread the kernel's recorded mountpoint and report an early
   unmount — confirm or clear this on real APFS.

Lands: when the darwin mount validation reports back and its
findings are dispositioned.
