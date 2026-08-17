# Concurrent test runs collide on fixed scratch paths

Test fixtures use deterministic paths under the package-local
`.scratch/` (`ocifs-<name>`, `scratchtest-wedge/<case>`), so two
`go test` invocations of the same package on one machine operate on
the same directories: run B's setup removes run A's live tree, and
with stale-mount recovery in the fixtures, run B's reap lazily
detaches run A's *live* kernel mount mid-test. Demonstrated: a
review agent's package run and a concurrent full-suite run wedged
each other on `scratchtest-wedge/*`.

Determinism is deliberate (mutation/witness observation brackets
want stable inputs), so per-run random roots conflict with that
design. A fix wants a per-run scratch namespace that is still
deterministic within the run (e.g. keyed by test binary PID with
stale-namespace reaping, or an flock serializing suite runs per
package — the machine-sharing `mlock` wrapper already serializes
heavy work but shared holders coexist and collide today).

Lands: when concurrent same-package test runs on one machine are
first needed as a supported flow, or the next collision incident.
