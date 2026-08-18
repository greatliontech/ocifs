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

Second incident: a coordinator's targeted `go test -run` raced its
own backgrounded full-suite run of the same package and failed the
suite's store tests (transient, solo re-run green). The fix now has
a natural mechanism that did not exist at filing: gmdb's oslock
package gives per-run liveness witnesses, so a per-run scratch
namespace (deterministic WITHIN the run) can be reaped exactly when
its run's lock is acquirable — the same held-lock-liveness shape
the store itself adopted, with none of the old staleness
heuristics.

Lands: user decision (the redesign spans the test fixtures of
every package; scheduling is the owner's call — the interim
discipline is one test invocation per package at a time).
