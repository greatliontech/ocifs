# mountrec.go mixes record codec, registry, and reclamation

`internal/store/mountrec.go` carries four concerns at different
altitudes: the mounts-row codec, the bookkeeping row methods, the
Store-level registration surface, and the dead-mount reclamation
sweep. Raised as a consolidation candidate by review during the
publication-marker chunk. The reclamation arc (claim/restore
machinery) is rewritten wholesale by the liveness-locks plan's
mounts-on-locks chunk — that rewrite is the natural moment to
re-home what survives (sweep logic beside the other sweeps,
codec+registry staying together) instead of splitting twice.

Lands: 5 (liveness-locks plan — mounts and uppers on locks).
