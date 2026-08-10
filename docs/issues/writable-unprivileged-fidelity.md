# Writable layer: unprivileged ownership/special-file fidelity via xattrs

An unprivileged process cannot create root-owned files, foreign-uid
files, or device nodes in a real upper dir. The presented view must be
decoupled from stored reality — a FUSE filesystem's reported attributes
are whatever the server says (the fuse-overlayfs precedent, running in
every rootless podman):

- `chown`/`chgrp` to unmapped ids: record intended uid/gid in
  `user.ocifs.*` xattrs on the real (caller-owned) file; getattr
  reports the recorded owner.
- `mknod`: create an empty regular file plus a device-spec xattr;
  getattr reports the device type/major/minor. Function is not
  required — unprivileged FUSE mounts are nodev and container runtimes
  supply real `/dev` nodes; only the record matters, for commit
  fidelity.
- setuid/setgid bits, foreign groups: same mechanism.
- The dominant case needs no xattrs: with a single-uid user namespace
  (container root mapped to the caller), root-in-container writes
  round-trip correctly through the mapping.
- Commit builds tar headers from the xattr-resolved attributes — the
  emitted OCI layer carries genuine uid/device entries regardless of
  host ownership. The presented truth is serialized, never the host
  truth.

Deliberately out: subuid/subgid range support. The xattr mode alone
requires no /etc/subuid, no setuid helpers, and works for a plain user
on any box.

Scope note (from the projection design): this xattr mechanism
belongs to the provider-mediated write model (FUSE/FSKit) — those
backends intercept chown/mknod and can record intent. ProjFS's
OS-native upper has no interception point, so commit-from-windows
carries the platform's fidelity envelope (defaulted POSIX
modes/ownership, like any Windows tar tool) per the fidelity matrix
in `docs/specs/projection.md`; that is a declared envelope, not a
gap this issue must close.

Lands: with the fs-native upper (see writable-upper-fs-native.md).
