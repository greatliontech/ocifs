# Privileged export-ownership arm has no test

REQ-export-ownership's privileged branch — chown to recorded
uid/gid on every entry type, ordered before chmod so setuid
survives — never executes in the suite, which does not run as root.
A user-namespace harness (re-exec under CLONE_NEWUSER with the
invoking user mapped to 0) can exercise it unprivileged: mapped-root
chown to in-namespace ids, header uid/gid round-trip, and the
chown-clears-setuid ordering. One behavioral note to pin when built:
a header uid/gid of -1 makes chown a no-op (the entry stays
namespace-root-owned) — same outcome an honest uid-0 header
produces, but "applied from header" is not literally true for that
degenerate value.

Lands: when a user-namespace test harness is added, or the
ownership arm next changes.
