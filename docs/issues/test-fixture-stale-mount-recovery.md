# Test fixtures do not recover from leaked FUSE mounts

A SIGKILLed test run leaks its kernel FUSE mounts (`t.Cleanup` never
runs and the in-process FUSE server dies without unmounting). The
next run's fixture setup (`writableFixtureEnv`, `mount_test.go`'s
equivalents — every mount-performing test shares the pattern)
touches the dead mountpoint under its `.scratch/ocifs-<name>/` root
and blocks in uninterruptible sleep: any syscall against a FUSE
mount whose server is gone parks in the kernel's request wait until
the connection is aborted.

Demonstrated: a killed witness run left three `ocifs-wexec` mounts
with dead servers; the subsequent full-suite run wedged on them and
was killed at the package timeout, and a forked child stuck exec'ing
a binary off the dead mount held a machine-lock slot for hours.
Recovery required manual `/sys/fs/fuse/connections/<id>/abort`.

Fix shape: fixture setup scans `/proc/self/mounts` for entries under
its scratch root and lazy-unmounts (`MNT_DETACH`, or
`fusermount3 -uz`) each before `os.RemoveAll` — making every run
self-healing against a killed predecessor.

Lands: next change set touching the shared test fixture helpers, or
the next stale-mount incident.
