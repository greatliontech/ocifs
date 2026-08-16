# ocifs — writable projections

A writable projection unions an immutable image view
(`layer-semantics.md`) with an **upper** — the on-disk directory
holding every local modification (`projection.md` REQ-proj-upper-truth:
the upper is the single source of truth; all in-memory overlay state
is a cache rebuildable from disk alone). This spec pins the POSIX
upper dialect the provider-mediated write model (FUSE, FSKit) stores,
the write semantics that produce it, the presented merged truth, and
the commit function. The OS-native model (ProjFS: tombstones and full
files in the virtualization root) shares the abstract upper-diff
contract (`projection.md` REQ-proj-commit-neutral) and lands with its
own arc; nothing here may foreclose it.

**stand-in** (term): A regular file in the upper representing a node
the host filesystem cannot hold with full fidelity for an
unprivileged writer, its kind and attributes carried in fidelity
xattrs.

**fidelity override** (term): A `user.ocifs.*` extended attribute on
an upper entry recording an attribute the host filesystem refused to
store natively (foreign ownership, device numbers); presentation and
commit read the override, never the host value it papers over.

**base binding** (term): The record naming the image a store-managed
upper was first mounted over; commit against a different base is
refused for a bound upper.

## Upper dialect

**REQ-writable-dialect** (wire): A POSIX upper MUST be stored in OCI
layer layout, directly unpackable by the read side's own rules
(`layer-semantics.md`): written files are real files with real
names, modes, and timestamps; directories are real directories; a
deletion of a base-visible path is a literal whiteout marker file
`.wh.<name>` in the parent; a directory that must hide all base
content is a real directory containing the opaque marker
`.wh..wh..opq`. Copy-up and publish temporaries carry reserved
names (`.wh..wh..tmp.` prefixed) in the target's directory — names
the layer dialect already drops as reserved markers
(`layer-semantics.md` REQ-unify-whiteout) — so a crash-orphaned
temporary is inert to presentation, unification, and commit, and the
provider removes any it encounters. Nothing else is stored: no
metadata files, no journals, no state outside the entries themselves
and their extended attributes. Symlinks, FIFOs, and sockets are native nodes
until a fidelity override lands on one (the kernel refuses user
xattrs on those node kinds), at which point the node converts to a
stand-in; character and block devices are always stand-ins (an
unprivileged writer cannot create the real node). A stand-in is an
empty regular file carrying `user.ocifs.kind` and the kind's
attributes (`user.ocifs.target` for symlinks, `user.ocifs.rdev` for
devices). An extended attribute the host refuses to store natively
(`security.*`, `trusted.*` for an unprivileged writer) is recorded
verbatim as `user.ocifs.xattr.<name>` on the entry — the escape that
lets a capability-bearing binary round-trip through an unprivileged
upper. The upper root directory itself is no entry: its host
attributes are creation noise, and the root presents the base root's
attributes until a **root record** exists — the `user.ocifs.owner`
record stamped on the upper root by the first root-mutating
attribute change (stamped with the presented owner even when the
change touches only mode or times), after which the root's mode and
timestamps live in the host root directory's own attributes and its
ownership in the record. A recorded root commits as the layer's
root entry exactly when its presented attributes differ from the
base root's.

**REQ-writable-fidelity** (behavior): The provider MUST present and
commit the recorded intent, not the host state, wherever the two
diverge: `chown` to an id the host refuses records the single
`user.ocifs.owner` record (`uid:gid` — one attribute, so the record
lands atomically) on the real caller-owned entry and getattr reports
the recorded owner; `mknod` creates a stand-in
and getattr reports the device; setuid/setgid the host accepts are
stored natively. `setxattr` of a
host-refused attribute records the `user.ocifs.xattr.<name>` escape,
and the attribute presents under its real name. In the dominant
single-uid user-namespace case (container root mapped to the caller)
ownership round-trips natively and no override is written. A
presented mode that would deny the provider its own dialect access —
owner read and write on files and stand-ins, owner read, write, and
search on directories — is itself a fidelity override: the host node
keeps the presented mode plus the provider-access bits, the
presented mode lands as the single `user.ocifs.mode` record, and
presentation and commit read the record (the host bits are
machinery; the mount surface enforces the presented mode). A mode
returning to provider-accessible values drops the record — host
truth resumes — ordered mode-first, so the one crash intermediate
presents the old mode over the new host bits. Stamping the root
record orders the owner record first — machinery on the root
without it is damage — so the stamping sequence's intermediate
presents the host root's attributes under the recorded owner. A `chown`
recorded as an override clears setuid/setgid in the presented and
stored mode exactly as a native chown would — presented truth
follows POSIX, not the mechanism — ordered clear-first: the one
crash intermediate is the cleared mode with the old owner, so an
interrupted override chown only ever reduces privilege
(REQ-writable-crash's declared intermediate for this mutation). Function is required of no
stand-in beyond what the platform gives every mount (FIFO and
socket semantics live in the kernel's inode layer; device nodes are
non-functional exactly as on any `nodev` mount). Presentation of an
attribute never depends on whether it was stored natively or as an
override.

**REQ-writable-reserved** (behavior): Names in the layer dialect's
reserved whiteout namespace (`layer-semantics.md`: a basename
beginning `.wh.`) MUST be refused at the write path with an
invalid-name error — the upper is stored in the dialect, so such a
name is indistinguishable from a marker; base images cannot contain
them (unification drops them), so refusal forecloses nothing
representable. The `user.ocifs.*` extended-attribute namespace is
reserved identically at the mount surface: invisible to listxattr,
absent to getxattr, refused to setxattr and removexattr — a client
that could touch it directly would mint phantom stand-ins and forge
overrides through ordinary file copies; overridden attributes
present only under their real names. The reservation extends to base
content: a base entry's extended attributes in the namespace are
inert — never presented, stripped by copy-up (recording nothing),
never read as dialect records — so every `user.ocifs.*` attribute in
an upper is provider-authored, and a hostile image cannot smuggle a
forged stand-in or override through copy-up.

## Write semantics

An entry is **base-visible** when the base view holds an entry at
its path that the upper does not occlude: no whiteout at the path
and no whiteout or opaque marker covering an ancestor. A directory
recreated over its whiteout is not base-visible — the marker
occludes the base entirely — and neither is anything beneath it.

**REQ-writable-copyup** (behavior): The first mutation of a
base-visible entry — content write, truncate, attribute or xattr
change, or rename source — MUST copy the entry into the upper
(content from the CAS, recorded attributes preserved, as overrides
where the host refuses them) and apply the mutation there; ancestor
directories materialize in the upper with their presented
attributes. Copy-on-write cloning is a permitted copy mechanism. A
mutation observes either the fully copied-up entry or, on failure,
the untouched base entry — never a partially copied one (copy-up
completes through a temporary name renamed into place). Handles
opened before a copy-up observe the copied-up object afterwards —
one logical object, whatever its storage.

**REQ-writable-delete** (behavior): Deleting a base-visible entry
MUST leave a whiteout marker at its path; deleting an upper-only
entry that shadows nothing removes the entry alone. `rmdir` applies
to the merged directory (it must present empty), whiteouts a
base-visible directory, and dismantles the upper directory only
after its whiteout exists. Markers are monotone while base
occlusion depends on them: a marker is removed only beneath a
directory whose own whiteout already exists (the dismantling of an
`rmdir`'d upper directory's interior — the parent marker occludes
everything the inner markers did), never while it is what hides base
content. Recreating an entry over a whiteout creates it beside the
marker, which the dialect already renders correctly (a same-layer
entry ships over its own whiteout, `layer-semantics.md`
REQ-unify-whiteout), so base content stays hidden under a recreated
directory with no opaque conversion and no marker-removal step.

**REQ-writable-rename** (behavior): Renames of files — base-visible
(copy-up plus source whiteout) or upper-born (native rename plus
source whiteout when the source shadowed base) — MUST behave as
POSIX rename on the merged tree, including replacing targets (the
replaced target's base visibility earns its own whiteout), ordered
destination-first: the destination materializes before the source
whiteout lands, so the one crash residual is the entry present at
both paths, each individually coherent (REQ-writable-crash).
Renaming a directory that holds any base-visible content returns
EXDEV — userspace fallback (copy-and-delete) is universal, and
recursive provider-side copy-up of unbounded trees is not owed;
directories with no base-visible content beneath them (upper-born,
or recreated over a whiteout) rename natively.

**REQ-writable-hardlink** (behavior): `link` MUST produce a real
hardlink in the upper — copying up the target first when it is
base-visible — so linked entries share one upper inode, one set of
attributes, and one identity through the mount. Link creation is not
shadowing-in-place: a base-visible target copied up by `link`
migrates to the upper-born identity it now shares with its link
(`projection.md` REQ-proj-identity) — hardlink-aware tools must see
one inode, and a one-time identity change at link time is the
faithful rendering of an object whose inode genuinely changed.
Commit emits the sorted-first path among an inode's paths as the
content entry and the rest as hardlink entries targeting it, whether
or not the sorted-first path itself differs from base.

## Presentation

**REQ-writable-presented** (behavior): The presented tree MUST be
the merge: an upper entry shadows the base entry at its path
entirely; a whiteout occludes the base subtree at its path — the
path presents its coexisting same-layer entry if one exists, nothing
otherwise; an opaque directory presents only upper content beneath
it; everything else presents the base. The root presents the base
root's attributes until the upper's root record exists
(REQ-writable-dialect), then the record's. Directory enumeration merges the base
snapshot with the upper listing under the backend comparator,
markers and fidelity machinery invisible; enumeration snapshots
remain immutable and resumable (`projection.md`
REQ-proj-enumeration), with change verifiers derived from upper
directory state. Identity follows `projection.md` REQ-proj-identity:
shadowed base paths keep their view-derived IDs (hardlink migration
excepted — REQ-writable-hardlink); upper-born objects draw IDs from
the upper-born partition as the recorded derivation prescribes. Link
counts follow the storage: upper-backed entries report the upper
inode's count, base entries the read path's independent-node rule.

## Crash consistency

**REQ-writable-crash** (invariant): Every reachable on-disk upper
state — including the state after a crash at any point in any write
sequence — MUST be a valid dialect state presenting a coherent tree.
Each dialect step is a single atomic filesystem operation
(create-via-rename, marker create, remove, rename), and compound
mutations order their steps so every prefix presents the old tree,
the new tree, or a declared intermediate: monotone markers mean
deletion-then-recreation never resurrects base content (the marker
outlives every recreation), `rmdir` hides before it dismantles
(whiteout first, so no prefix re-exposes deleted children), and
rename's sole intermediate is the entry at both paths
(REQ-writable-rename), and an orphaned reserved-name temporary is
inert garbage, not state (REQ-writable-dialect). The provider's in-memory state rebuilds from
a walk of the upper alone (`projection.md` REQ-proj-upper-truth); no
recovery, repair, or journal-replay step exists — there is nothing
to repair.

## Commit

**REQ-writable-commit** (invariant): Commit MUST be the canonical
diff of (base view, upper) serialized as one uncompressed OCI tar
layer, deterministically: an entry is emitted exactly when its
presented kind, content, or attributes differ from the base at that
path, or when REQ-writable-hardlink forces it as a link group's
content entry (a copied-up entry otherwise restored to base state is
not emitted). The comparison base at a path is the base entry only
where the upper leaves it visible — no whiteout at the path or an
ancestor, no opaque on an upper-held ancestor directory; an entry
over occluded base content is new content and always emits. A
whiteout marker is emitted exactly where deleted base content's
occlusion depends on it: for each base path the upper deletes,
except where an ancestor marker or opaque already hides the base
entry (a dismantled and an undismantled rmdir interior commit
identically) or where a non-directory, non-socket upper entry at the
same path already replaces the base entry entirely in the dialect
(emitting the marker in either case would re-encode write history);
a directory recreated over its marker keeps the marker — it is what
hides the base children; opaque markers are emitted exactly where
they have effect — the upper holds the directory, the base holds
content beneath it, and that content is not already hidden by the
directory's own whiteout or an ancestor's occlusion; entries are
written in sorted path order with fixed header layout, stable xattr
order, and no fields drawn from commit time — so equal (base, upper)
pairs commit to byte-identical layers with equal digests, regardless
of write history or platform (`projection.md`
REQ-proj-commit-neutral). Headers carry the presented attributes —
fidelity overrides resolved into header fields and real xattr names,
the `user.ocifs.*` namespace itself never emitted — so the layer
holds genuine ownership, device, and capability entries regardless
of host ownership. Marker entries (whiteout, opaque) have no
presented source and emit fixed headers: mode 0, uid and gid 0, the
Unix epoch for every timestamp, no extended attributes — a marker
file's host attributes are pure write history. Socket entries are
omitted from committed layers: the tar dialect has no socket type
(universal tar tooling omits them the same way), and a socket is a
transient endpoint serving the live mount only — but a socket
shadowing a base-visible entry still commits that entry's whiteout,
or the committed image would resurrect what the mount hides.

**REQ-writable-commit-image** (behavior): Committing MUST produce a
complete image in the store — the base's layers plus the committed
layer, a manifest and config extending the base's (diff IDs and
history appended; the uncompressed layer's blob digest is its diff
ID), everything ingested under the store's ordering rules
(`store.md` REQ-store-ingest-order) — returned by digest and
immediately acquirable, mountable, and exportable like any stored
image (`store.md` governs recording and the local reference
namespace). Commit requires no live mount: it reads the upper from
disk. Committing an upper that is concurrently being written
produces a point-in-time result of no guaranteed cut — quiescence is
the caller's to arrange; the store is never corrupted by a racing
commit.

**REQ-writable-base-binding** (behavior): A store-managed upper MUST
record the image digest it is first mounted over, beside the upper
(never inside the dialect tree), created atomically without
replacement — the loser of a first-mount race reads the winner's
binding and validates against it; later writable mounts and commits
against a different base are refused — a whiteout set produced over
one base silently applied to another materializes a tree nobody
wrote. A named upper admits one writable mount at a time within the
store's single-writer scope (`store.md` REQ-store-single-writer's
in-process rule): a second writable mount is refused while the first
serves. A caller-supplied upper directory carries no binding and no
mount arbitration; the caller names the base explicitly and owns
both the pairing and the exclusivity.

## Non-goals

subuid/subgid range mapping (the override mechanism needs no
privileged helpers); pushing committed images (the store is local;
push is a future surface); freezing or snapshotting live mounts for
commit; the ProjFS and FSKit write arms (their dialect reader and
write path land with their platform arcs, gated on the platform
validation runs — the abstract diff and commit core here are the
contract they plug into).
