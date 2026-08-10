# ocifs — public API surface

The `ocifs` module exposes one library package (`ocifs`) and a thin
CLI (`cmd/ocifs`). This spec pins the observable behavior of the
public surface; Go shapes (option names, signatures) live in the
code and its doc comments.

## Construction

**REQ-api-construction** (behavior): A consumer MUST be able to
configure at construction: a work directory (store root; default:
`ocifs` under the OS temp directory); extra directories to
synthesize in mounts; registry credentials as a map of registry or
registry/repository prefixes to credentials plus an opt-in to the
ambient default keychain; a pull policy (default `IfNotPresent`,
semantics in `store.md`); and a default platform (default: host
os/arch, semantics in `store.md`). Construction initializes the
store and fails if the store cannot be initialized.

**REQ-api-keychain** (behavior): Credential resolution MUST pick the
longest matching configured prefix; with no match, the default
keychain applies if enabled, else anonymous. Resolution is
deterministic — overlapping prefixes never resolve differently
across calls.

## Image acquisition

**REQ-api-acquire** (behavior): The library MUST offer image
acquisition by reference string (tag or digest form), resolved per
the pull policy and platform rules in `store.md`, and by digest with
explicit platform (`store.md` REQ-store-digest-entry). Both yield a
materialized image whose config file is accessible to the consumer,
and both run the verification seam (`verification-seam.md`) when a
verifier is configured.

## Mount

**REQ-api-mount-ro** (behavior): Mounting a materialized image MUST
produce a read-only FUSE filesystem of the unified view
(`layer-semantics.md`), private to the invoking user (no
`allow_other`), with regular-file reads served directly from
content-CAS blobs.

**REQ-api-mount-attrs** (behavior): The mount MUST present
attributes (mode, uid/gid, size, times) from the recorded tar
headers, for directories as well as files; implied directories
(`layer-semantics.md` REQ-unify-sorted) are synthesized as plain
directories. Symlinks and hardlinks are
presented per `layer-semantics.md`; FIFOs are presented as FIFO
nodes with their recorded attributes. Character and block
device nodes are presented as nodes of the recorded type, but device
numbers are **not** preserved (a mount is not a bootable rootfs;
consumers needing devices provide their own `/dev`).

**REQ-api-mountpoint** (behavior): The mountpoint MUST be the
caller's target path, or a store-managed mount directory when none
is given; relative targets resolve against the process working
directory. After a successful unmount the mountpoint directory
remains (empty).

**REQ-api-mount-id** (behavior): A caller-supplied mount id MUST be
a single path element (no separators, not `.` or `..`) — anything
else is rejected, so an id cannot place the mount directory outside
the store's `mounts/` tier.

**REQ-api-extra-dirs** (behavior): Extra directories configured at
construction MUST appear as empty directories in every mount —
anchor points for consumers that bind or overlay onto the mounted
tree.

## Export

**REQ-api-export** (behavior): The library MUST offer export of a
materialized image into a caller-supplied target directory or the
store-managed export cache, per `export.md`.

## CLI

**REQ-api-cli** (behavior): The `ocifs` CLI MUST mount an image at a
required mountpoint from a required image reference, with optional
work directory and extra directories, using the ambient default
keychain; it serves until unmounted or signalled (SIGINT/SIGTERM
trigger unmount). The CLI is a consumer of the library surface and
adds no semantics of its own.
