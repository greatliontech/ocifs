# stipulator check red: REQ-unify-clean (fleet sweep 2026-08-27)

The weekly fleet sweep found `stipulator check` failing here:
REQ-unify-clean is red and no gap excuses it, alongside stale content
pins on its bindings (re-consent: `stipulator pin --req
REQ-unify-clean`) and an uncovered REQ-api-mount-darwin. Undiagnosed —
stale pins after the tool-phase releases are the likely proximate, the
red itself needs a look.

Lands: user decision.
