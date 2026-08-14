# Export materialization ignores context cancellation

`OCIFS.Export` honors its context only during acquisition; once
materialization starts, a multi-gigabyte copy loop runs to
completion or error — the caller cannot cancel it. No spec
requirement pins cancellation today, so this is an unpinned
ergonomic gap, not a violation. A fix threads the context into the
materializer's entry loop (a per-entry check suffices; blob copies
are bounded by entry size).

Lands: when consumer-driven cancellation of a running export is
first needed.
