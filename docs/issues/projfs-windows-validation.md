# ProjFS backend awaits its windows validation run

The windows backend (`internal/projfsfs`, `mount_windows.go`) and
its test suite are authored against the projfs-go contract (whose
own integration tests ran on a real windows machine) and
cross-compile-verified (`GOOS=windows GOARCH=amd64 go vet ./...`),
but no test in the suite has executed on windows: the ProjFS columns
of REQ-proj-model/-fidelity/-content/-ro and REQ-proj-enumeration/
-case remain pinned only at kernel level plus linux arms.

To validate, on a windows/amd64 machine with Go ≥1.26:

    Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS -NoRestart
    go test ./...

and report the full output. The suite covers case-folded resolution,
platform-comparator enumeration order and pagination, byte-exact
content including a 1 MiB file and an unaligned tail, unrepresentable
NTFS names omitted-and-reported, the symlink feature probe with its
declared fallback, read-only vetoes from a foreign process (re-exec
helper, raw errnos), foreign-file mutability and residual recording,
placeholder metadata-dirt tolerance, unmount residue removal, and —
through the library surface — a full pull-mount-read-unmount pass
(`TestMountWindowsEndToEnd`). Known caps (stated, not silent): the
placeholder ContentID digest is pinned by review only (no read-back
API is exposed), filtered pagination (a wildcard search over a
paginating directory) has no dedicated test, and the
internal/layer suite (extraction oracle needs mkfifo) is
linux-gated — the windows run exercises no layer-unification tests;
those semantics are platform-independent and pinned on linux.

One condition only the run can probe: a placeholder held open by a
foreign process at unmount survives the residue sweep (sharing
violation) — if the run observes leftovers beyond foreign files and
their spine, capture which and under what handle state.

Lands: when the windows test run reports back and its findings are
dispositioned.
