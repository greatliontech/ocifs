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
longest matching configured prefix, where a prefix matches only at a
path-segment boundary — the target equals the prefix or continues
with `/`. A prefix never matches inside a segment: credentials
scoped to `r.io/team` must not be sent for `r.io/teammate`, a
foreign repository whose name merely extends the string. With no
match, the default keychain applies if enabled, else anonymous.
Resolution is deterministic — overlapping prefixes never resolve
differently across calls.

## Image acquisition

**REQ-api-acquire** (behavior): The library MUST offer image
acquisition by reference string (tag or digest form), resolved per
the pull policy and platform rules in `store.md`, and by digest with
explicit platform (`store.md` REQ-store-digest-entry). Both yield a
materialized image whose config file is accessible to the consumer,
and both run the verification seam (`verification-seam.md`) when a
verifier is configured.

## Mount

Mounting produces a **projection** — presentation semantics,
per-backend fidelity, identity, enumeration, and read-only
enforcement are pinned in `projection.md`; this section pins only
the consumer-facing surface around it.

**REQ-api-mount-ro** (behavior): Mounting a materialized image MUST
produce a read-only projection (`projection.md`) of its unified
view; on linux the FUSE mount is additionally private to the
invoking user (no `allow_other`).

**REQ-api-mount-darwin** (behavior): On darwin, mounting MUST be
appex-mediated: the library provides the filesystem (volume)
implementation and orchestration, the signed FSKit app extension is
the server the platform spawns when the volume is mounted
(`mount -F -t <type>`), and its configuration arrives per
`projection.md` REQ-proj-server — there is no in-process darwin
mount call. A store the sandboxed extension cannot open (anywhere
outside the app-group container, including the default temp-dir
work directory) cannot serve appex mounts: consumers intending
darwin mounts configure the work directory inside the app-group
container, and mounting against an inaccessible store fails.

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
keychain; on platforms with in-process mounting (linux, windows) it
serves until unmounted or signalled (SIGINT/SIGTERM trigger
unmount), while on darwin it orchestrates the appex-mediated mount
(REQ-api-mount-darwin) and does not serve. The CLI is a consumer of
the library surface and adds no semantics of its own.
