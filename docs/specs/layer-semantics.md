# ocifs — layer semantics (unification)

ocifs is the one home of OCI layer semantics: consumers (FUSE mount,
export, external libraries) receive a **unified view** — a flat list
of tar entries representing the final filesystem — and never parse a
layer tar themselves.

**unified view** (term): The flat, path-sorted list of tar entries
produced by unifying an image's layers; the single semantic model
every presentation (mount, export) consumes.

**whiteout** (term): A tar entry named `.wh.<name>` in directory `d`,
deleting `d/<name>` as contributed by lower layers.

**opaque whiteout** (term): A tar entry named `.wh..wh..opq` in
directory `d`, discarding all of `d`'s lower-layer contents.

Input: an image's layers, ordered base → top, each presented as its
recorded tar entries in tar order (from the store's layer indexes).

**REQ-unify-paths** (behavior): Entry names MUST be interpreted after
lexical cleaning (`./x`, `x/`, `/x`, and `x` are the same path),
relative to the image root — leading slashes are stripped, exactly as
tar extraction into a root directory treats them.

**REQ-unify-contained** (invariant): The unified view MUST contain
only clean, root-relative paths: an entry whose cleaned path escapes
the image root (`..` traversal, or a path that cleans to the root
itself claiming non-directory type), or whose cleaned path exceeds
4096 bytes (no materializable root filesystem holds longer paths),
fails unification with an error identifying the entry — such an entry cannot exist in any real root
filesystem, and rejecting it at the model means every consumer
(mount, export, commit) inherits containment instead of re-checking
it. Unification has no other failure mode: hostile but representable
input (whiteout games, duplicates, type flips) resolves by the rules
below, never by error.

## Precedence

**REQ-unify-precedence** (behavior): Layers MUST resolve top-down —
for each path, the topmost layer that speaks about it wins: a
non-whiteout entry in a higher layer shadows entries at the same
path in all lower layers; a directory entry does not shadow the
directory's *contents* (lower layers may still contribute entries
inside it); a non-directory entry at path `p` finalizes `p` as a
whole — lower-layer entries at `p` or beneath `p/` are discarded.
Within a single layer, a path occurring more than once resolves to
its **last** occurrence in tar order (sequential-extraction
semantics: later entries overwrite earlier ones).

**REQ-unify-whiteout** (behavior): A whiteout MUST delete its target
path and everything beneath it as contributed by **lower** layers —
and only lower layers: entries at the target path in the *same*
layer as the marker survive regardless of tar order, because
whiteouts apply before the layer's own content (a layer may delete
lower `x` and ship a replacement `x` in either order). An opaque
whiteout discards all of its
directory's contents contributed by lower layers while the directory
itself remains; entries in the *same* layer as the marker are kept —
opacity scopes strictly to lower layers. A degenerate or reserved
marker — one whose stripped name is empty, `.`, `..`, or itself
begins with the whiteout prefix (the reserved `.wh..wh.*` namespace,
excepting the opaque marker defined above) — has no whiteout effect
and is dropped. An entry with a marker-prefixed name in a
*non-basename* component (`.wh.x/y`) is inert and dropped entirely:
a directory literally named with the whiteout prefix is
unrepresentable in the layer dialect — re-serializing it would read
back as markers — so the view never holds one. Neither marker kind, degenerate or well-formed, ever
appears in the unified view.

## Output

**REQ-unify-clean** (invariant): The unified view MUST satisfy all
of: no whiteout markers of either kind; each path at most once; no
entry that a higher layer deleted or shadowed; and no entry whose
ancestor path is a non-directory entry (descendants of a
non-directory are discarded regardless of the tar order that
produced them). A violated view
resurrects deleted content (a secret whited out in a later layer
reappears in the mount) or presents a tree no real filesystem could
hold (children under a symlink), which export must then reject.

Regular-file entries name their content by **digest** — the store's
content-CAS key (`store.md`) — never by a filesystem path: the model
stays pure, the store stays relocatable, and every consumer resolves
content through the same content-addressed primitive.

**REQ-unify-sorted** (behavior): The unified view MUST be sorted by
cleaned path name; because names are relative and cleaned, a
directory's entry sorts before everything beneath it — except the
root entry: when a layer carries the root directory itself, the view
contains a `.` entry holding the root's attributes, which
participates in plain byte order (`!x` sorts before `.`) and is
never a child entry — consumers treat it positionally independent. The view is
**complete**: a directory a layer only *implied* (created because
entries were placed beneath it, with no entry of its own) appears in
the view as a synthesized plain directory entry with mode 0755 —
extraction makes implied directories real, so one that loses all its
children to a later whiteout or opaque still exists, which no
consumer could reconstruct from the surviving entries alone. The
same holds for parents materialized by an entry that was itself
discarded at a deeper obstruction or omitted as an unresolvable
hardlink — extraction creates parents before it can fail, and they
stay. Consumers present the view as-is and synthesize nothing.

## Hardlinks

**REQ-unify-hardlink** (behavior): A hardlink entry MUST resolve at
its own position in extraction order: its target — after the same
lexical cleaning as entry names — is looked up in the state existing
when the link entry is applied, exactly as physical extraction links
to the inode existing then. A target replaced later (by a higher
layer, or by a later entry in the same layer) therefore leaves the
link carrying the **old** content, matching kernel union-filesystem
behavior. A resolved link appears in the unified view as a hardlink
entry carrying its captured content identity (digest and size) and
the target's inode attributes (mode, ownership, times, and extended
attributes — a hardlink shares its target's inode, so the link
header's own attributes are extraction noise); link
identity (shared inode number, `st_nlink`) is not part of the
contract — presentations may render an independent node with the
captured content. A link whose target at that moment does not carry
regular-file content (absent, or a directory, symlink, device, or
FIFO — an earlier resolved hardlink does carry it, so chains
resolve) is omitted from the unified view; it is not an error. A
link whose cleaned target equals its own path changes nothing —
linking a path to its own inode is a no-op, and the existing entry
stands.

## Special file types

**REQ-unify-symlink** (behavior): Symlinks MUST preserve their target
string verbatim — no resolution, no rewriting; dangling and absolute
targets are presented as recorded. Character/block devices and FIFOs
are carried through unification like any entry; what a given
consumer does with them is that consumer's contract (`api.md` for
the mount, `export.md` for export).
