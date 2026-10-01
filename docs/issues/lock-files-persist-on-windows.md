# On windows a retired lock file is never unlinked

The lock-file discipline (store.md, held-lock liveness) unlinks a
claim's file while its lock is held, by the holder, before release.
gmdb's oslock opens the file through Go's os package, which shares
reads and writes but not deletion, so on windows the holder's own
handle refuses its unlink: every retirement defers, and the deferral
never resolves — a later holder's unlink meets its own handle the
same way. The files are unheld, acquirable dead entries, so no
verdict is wrong, but `locks/` grows by one file per claim ever made
(a probe per store open, an op per operation, a mount per id) and
each lock-tier sweep re-retires every one of them.

The fix is the lock library's: open with delete sharing on windows,
so the holder's unlink lands as delete-pending and the name goes at
the last close — the state gmdb's Acquire already retries through.

Lands: when ocifs depends on a gmdb release whose Retire unlinks on
windows (its own test asserting retirement there rather than
skipping), `requireRetired`'s windows branch then narrowed to the
file's absence.
