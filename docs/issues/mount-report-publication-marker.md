# Mount records cannot distinguish registered-empty from published-clean reports

A mounts row carries an empty projection report both between
registration and the backend's publication (seconds-long on the
appex-mediated darwin path) and after a clean publication with no
omissions. An inspector reading the row mid-mount cannot tell the
two apart. If inspection tooling should distinguish them, the
mounts value wants a publication marker — a wire-contract change
to `docs/specs/store.md` REQ-store-bookkeeping's mounts clause.

Raised by review during the bookkeeping arc; scope-bearing (a new
observable field), so it awaits the owner's call rather than the
spec-amendment authority.

Lands: when the owner decides inspection must distinguish the two
states, or when inspection tooling first needs it.
