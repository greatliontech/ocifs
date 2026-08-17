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
semantics in `store.md`); a default platform (default derived
from the host — semantics and the darwin fallback in `store.md`);
automatic garbage collection on or off (default on); and the
collection retention grace (default 24h) — both per `store.md`
REQ-store-gc-collect. Construction initializes the store and fails
if the store cannot be initialized.

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

**REQ-api-mount-ro** (behavior): Mounting a materialized image
without an upper MUST produce a read-only projection
(`projection.md`) of its unified view; on linux the FUSE mount is
additionally private to the invoking user (no `allow_other`).

**REQ-api-self-access** (behavior): The process serving a mount
(linux FUSE serves in-process) MUST NOT access that mount through
kernel paths invisible to its language runtime; ocifs documents the
constraint rather than detecting it. Ordinary syscalls (read,
write, stat, readdir) against the own mount are safe — the runtime
parks the thread and the serving goroutines stay schedulable.
Unsafe: memory-mapping a mount-resident file and then faulting its
pages; exec'ing a mount-resident file via a vfork-suspended child
(with current Go runtimes, exec without a new user namespace); a
child's pre-exec working-directory change or chroot into the
mount. Each leaves a thread the
runtime believes is running while it waits — in the kernel — on the
process's own FUSE server; a concurrent garbage-collection
stop-the-world then deadlocks the whole process intermittently.
Consumers needing exec or mmap of mount content do so from a
separate process (with current Go runtimes a plainly-forked
user-namespace child suffices; a fully separate process is the
durable form).

**REQ-api-mount-writable** (behavior): A mount MUST accept an upper
(`writable.md`) — a caller-supplied directory, or a store-managed
named upper created on first use (`store.md`) — and then serve the
writable merged projection. The upper outlives its mount: unmount
leaves it intact for remounting or commit, and deleting an upper is
an explicit act, never a side effect of unmount. A store-managed
upper's base binding is validated at mount (`writable.md`
REQ-writable-base-binding).

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
remains, holding nothing the projection served; a caller-supplied
target is the caller's property forever, while a store-managed
mount directory is store scaffolding the store may later reclaim
(`store.md` REQ-store-mount-registry) — on a backend with
declared read-only residuals (`projection.md` REQ-proj-ro), the
residual foreign files remain, with the directory spine containing
them, and only those.

**REQ-api-mount-id** (behavior): A caller-supplied mount id MUST be
a single path element (no separators, not `.` or `..`) — anything
else is rejected, so an id cannot place the mount directory outside
the store's `mounts/` tier.

**REQ-api-extra-dirs** (behavior): Extra directories configured at
construction MUST appear as directories in every mount — empty
unless the view already provides the path — anchor points for
consumers that bind or overlay onto the mounted tree. A configured
path that exists in the view as a non-directory, escapes the root,
or is absolute is a configuration error and fails mount
construction.

## Commit

**REQ-api-commit** (behavior): The library MUST offer commit of an
upper over its base image (`writable.md`): the base by reference
(resolved per the pull policy, the verification seam governing like
any acquisition), the upper in either mount form, no live mount
required; the result is the committed image, acquirable by its
digest under the store's local namespace (`store.md`).

## Export

**REQ-api-export** (behavior): The library MUST offer export of a
materialized image into a caller-supplied target directory or the
store-managed export cache, per `export.md`.

## Removal and collection

**REQ-api-remove** (behavior): The library MUST offer removal of a
cached reference (the `refs` row — the cached resolution, not the
remote), of a local image (the `localimages` row for a committed
digest), and of a named upper (the upper's dialect tree and its
base binding — the explicit act REQ-api-mount-writable names).
Removal severs the root; content becomes garbage for collection
(`store.md` REQ-store-gc-roots) rather than being deleted inline.
Removing a named upper currently mounted, or a local image a live
mount serves, is refused.

**REQ-api-gc** (behavior): The library MUST offer explicit
collection: honoring the retention grace by default, ignoring it on
demand (the wipe-now operator intent automatic collection
deliberately does not serve), returning what was collected — and
what could not be judged: rows whose liveness is unjudgeable from
this namespace (`store.md` REQ-store-bookkeeping) are reported, so
a foreign-namespace root leak is visible and the reboot-or-wipe
remedy is an informed one. The CLI exposes the same verb.

## CLI

**REQ-api-cli** (behavior): The `ocifs` CLI MUST mount an image at a
required mountpoint from a required image reference, with optional
work directory and extra directories, using the ambient default
keychain; on platforms with in-process mounting (linux, windows) it
serves until unmounted or signalled (SIGINT/SIGTERM trigger
unmount), while on darwin it orchestrates the appex-mediated mount
(REQ-api-mount-darwin) and does not serve. The CLI is a consumer of
the library surface and adds no semantics of its own.
