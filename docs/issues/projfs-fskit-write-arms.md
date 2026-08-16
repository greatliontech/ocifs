# Writable arms for the ProjFS and FSKit backends

Writable mounts are FUSE-only: the projection kernel's write engine
(`internal/projection` merged view, copy-up, rename, xattr surface)
is backend-neutral, but only the FUSE backend wires the mutating
ops. The ProjFS and FSKit backends remain read-only — each needs
its backend glue for the write engine plus a platform answer for
the upper dialect's fidelity mechanics (xattr escapes, stand-ins,
whiteout markers) on its native filesystem semantics.

Blocked behind the platform validation runs: the read backends
themselves are unexecuted on their target platforms
(`docs/issues/projfs-windows-validation.md`,
`docs/issues/fskit-darwin-validation.md`), and a write arm built on
an unvalidated read arm has no ground to stand on.

Lands: per backend, when its platform validation run's findings are
dispositioned and a writable mount on that platform is first
needed.
