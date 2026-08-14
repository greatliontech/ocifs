# Stale export temporaries accumulate with no sweeper

A crashed export leaves its `.export-<uuid>` temporary sibling
behind — permitted by REQ-export-atomic (stale temporaries are
inert) — but nothing ever removes them: repeated crashes accumulate
directories unboundedly under the store's `exports/<algorithm>/`
tier, and for a caller-supplied target the temporary lands in the
caller's own parent directory, littering space ocifs does not manage.
Recognizing a temporary as dead (no live exporter) shares the
dead-state recognition problem with mount-state reclamation and
store GC.

Lands: with store GC (a startup or GC-time sweep of `.export-*`
entries), or earlier if stale-temporary accumulation surfaces in
practice.
