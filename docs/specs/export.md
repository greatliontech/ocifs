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
hardlinks, and FIFOs. Implied directories are created as the shared
implied-directory presentation prescribes (`layer-semantics.md`:
plain directories, mode 0755). On a target filesystem that cannot
hold two of the view's paths as distinct entries (case-insensitive
hosts), the export fails naming the colliding paths — a rootfs is
either exact or refused, never silently substituted (contrast the
live projection's resolve-and-report policy, `projection.md`
REQ-proj-case). Permission bits — including setuid, setgid, and sticky — are
applied from each entry's recorded header, to files and directories
alike (directory permissions may be applied after the directory's
contents are written, so restrictive modes cannot block the export
itself); symlink permission bits are applied where the platform
stores them (darwin), while linux fixes symlink permissions at 0777
in the kernel — a platform semantic, not an export choice.
Modification times are applied from the recorded headers. Symlinks
are created with their recorded target verbatim, including absolute
and dangling targets (they are interpreted inside the consumer's
root, not the host's). Character and block device nodes are
**omitted**: creating them requires privilege, and rootfs consumers
provide their own `/dev`; export succeeds without them. On a
platform whose filesystems hold no FIFO nodes (windows), exporting a
FIFO-bearing view fails naming the entry — exact or refused applies
to node types exactly as to colliding paths.

**REQ-export-ownership** (behavior): Ownership (uid/gid) MUST be
applied when the process has the privilege to do so; otherwise
entries are owned by the invoking user and export succeeds.
Consumers needing in-container ownership map users at the sandbox
boundary.

**REQ-export-copy** (behavior): Regular-file content MUST be copied
out of the content CAS — never hardlinked to it. Copy-on-write
cloning (reflink) is a permitted copy mechanism where the filesystem
supports it. Hardlinks are created as links to the exported target
file only when the target entry carries the same content identity
the link captured (`layer-semantics.md` REQ-unify-hardlink);
a link whose captured content the target no longer holds
materializes as an independent copy of the captured content —
fidelity within the tree, still no links into the CAS.

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
export root. Path escapes cannot reach export — the unified view
rejects them at unification (`layer-semantics.md`
REQ-unify-contained) — so export's own obligation is the write-time
hazards only the host filesystem can express: no write traverses a
symlink (every directory component of every created entry's path is
a real directory created by this export or the root itself, so a
layer that plants a symlink where a later entry needs a directory
cannot redirect writes through it), and hardlink targets resolve
strictly within the export root under the same rules. Without this,
a symlink `x → /etc` followed by entry `x/cron.d/job` writes to the
host.

## Atomicity and caching

**REQ-export-atomic** (invariant): An export directory observable at
its final path MUST be complete: materialization happens in a
temporary sibling directory renamed into place — for a
caller-supplied target, the sibling lives in the target's parent
directory. A caller-supplied target must not already exist; an
existing target — empty or populated — refuses with the target
undisturbed. A crash leaves either no directory or a stale
temporary, never a partial tree at the final path; stale temporaries
are inert and may be deleted freely.

**REQ-export-cache** (behavior): Store-managed exports live under
the store's `exports/` tier, keyed by the digest of the manifest
actually materialized (the platform-selected child, so distinct
platforms of one index key distinct exports); because images are
immutable, an existing entry for a digest MUST be served as-is,
without re-materialization. Cached exports are shared: consumers
treat them as read-only; a consumer needing a writable tree makes
its own copy or uses its own target directory.
