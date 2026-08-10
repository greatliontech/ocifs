# ocifs — export

Export materializes an image's unified view (`layer-semantics.md`)
into a real directory tree — a prepared root filesystem for
consumers that need a plain directory rather than a FUSE mount
(container runtimes, build tooling, CI environments without
`/dev/fuse`).

**export root** (term): The target directory an export materializes
into — caller-supplied, or a store-managed entry under `exports/`.

## Fidelity

**REQ-export-fidelity** (behavior): An export MUST place every
unified entry at its path: directories, regular files, symlinks,
hardlinks, and FIFOs. Implied directories are created with mode
0755. Permission bits — including setuid, setgid, and sticky — are
applied from each entry's recorded header, to files and directories
alike (directory permissions may be applied after the directory's
contents are written, so restrictive modes cannot block the export
itself). Modification times are applied from the recorded headers.
Symlinks are created with their recorded target verbatim, including
absolute and dangling targets (they are interpreted inside the
consumer's root, not the host's). Character and block device nodes
are **omitted**: creating them requires privilege, and rootfs
consumers provide their own `/dev`; export succeeds without them.

**REQ-export-ownership** (behavior): Ownership (uid/gid) MUST be
applied when the process has the privilege to do so; otherwise
entries are owned by the invoking user and export succeeds.
Consumers needing in-container ownership map users at the sandbox
boundary.

**REQ-export-copy** (behavior): Regular-file content MUST be copied
out of the content CAS — never hardlinked to it. Copy-on-write
cloning (reflink) is a permitted copy mechanism where the filesystem
supports it. Hardlinks whose target resolves within the unified view
are created as links to the exported target file — fidelity within
the tree, still no links into the CAS.

**REQ-export-immutable** (invariant): Export MUST NOT mutate store
state: CAS bytes, modes, and link counts are identical before and
after any export. Exporting via hardlink and then applying a hostile
header's `chmod +s` would change the mode every other image sees for
that shared blob.

## Containment

Layer content is untrusted input; export writes at paths derived
from it.

**REQ-export-contained** (invariant): Every filesystem write
performed by an export MUST resolve to a path strictly inside the
export root. An entry whose cleaned path escapes the root (absolute
after cleaning, or containing `..` traversal) fails the export —
it is not skipped: a layer crafted to escape is evidence of hostile
input, not noise. No write traverses a symlink: every directory
component of every created entry's path is a real directory
created by this export (or the root itself), so a layer that plants
a symlink where a later entry needs a directory cannot redirect
writes through it. Hardlink targets resolve strictly within the
export root under the same rules. Without this, a layer entry
`../../home/user/.bashrc`, or a symlink `x → /etc` followed by entry
`x/cron.d/job`, writes to the host.

## Atomicity and caching

**REQ-export-atomic** (invariant): An export directory observable at
its final path MUST be complete: materialization happens in a
temporary sibling directory renamed into place. A crash leaves
either no directory or a stale temporary, never a partial tree at
the final path.

**REQ-export-cache** (behavior): Store-managed exports live under
the store's `exports/` tier, keyed by the digest of the manifest
actually materialized (the platform-selected child, so distinct
platforms of one index key distinct exports); because images are
immutable, an existing entry for a digest MUST be served as-is,
without re-materialization. Cached exports are shared: consumers
treat them as read-only; a consumer needing a writable tree makes
its own copy or uses its own target directory.
