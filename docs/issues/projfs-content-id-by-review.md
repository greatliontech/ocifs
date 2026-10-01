# The placeholder ContentID digest is pinned by review alone

REQ-proj-content has the ProjFS placeholder carry the entry's content
digest in its ContentID slot, so content-addressed refresh compares
digests. ProjFS exposes no read-back of a placeholder's ContentID,
so the windows row cannot compare what was written; the value is
pinned by reading `internal/projfsfs/provider_windows.go` alone.

Lands: when a read-back of a placeholder's ContentID exists
(projfs-go or the platform) and the windows row compares it.
