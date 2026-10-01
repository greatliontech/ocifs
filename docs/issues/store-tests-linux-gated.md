# The store's collection and export tests run on linux alone

`internal/store/gc_test.go`, `export_test.go` and
`gc_transition_test.go` carry `//go:build linux`, so the garbage
collection, the lock tier's retirement under collection (eight
assertions of a lock file's absence) and the export are exercised
on no other row; what the windows row witnesses of the store is the
rest of its suite. The files name no linux-only symbol on a reading;
the gate predates the rows.

Lands: when the three files run on the windows and darwin rows, their
lock-file assertions reading retirement as `requireRetired` does.
