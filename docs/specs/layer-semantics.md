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
opacity scopes strictly to lower layers. Neither marker kind ever
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

**REQ-unify-sorted** (behavior): The unified view MUST be sorted by
cleaned path name; because names are relative and cleaned, a
directory's entry sorts before everything beneath it. A directory
may be *implied* (entries exist beneath a path that has no entry of
its own); each presentation synthesizes implied directories under
its own contract (`api.md` for the mount, `export.md` for export).

## Hardlinks

**REQ-unify-hardlink** (behavior): A hardlink entry's target MUST be
resolved against the unified view after the same lexical cleaning as
entry names. A hardlink whose target resolves to a regular-file
entry denotes the same **content** as the target; link identity
(shared inode number, `st_nlink`) is not part of the contract —
presentations may render the link as an independent node with the
target's content and size. A hardlink whose target is absent from
the unified view or resolves to anything other than a regular-file
entry is omitted from presentations; it is not an error.

## Special file types

**REQ-unify-symlink** (behavior): Symlinks MUST preserve their target
string verbatim — no resolution, no rewriting; dangling and absolute
targets are presented as recorded. Character/block devices and FIFOs
are carried through unification like any entry; what a given
consumer does with them is that consumer's contract (`api.md` for
the mount, `export.md` for export).
