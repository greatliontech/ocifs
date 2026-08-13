# Default platform on variant-fragmented and foreign-OS hosts

The default request platform is the host's os/arch with no variant
(`docs/specs/store.md` REQ-store-platform-default), and selection is
strict with a uniqueness requirement (REQ-store-platform-strict).
Two host classes therefore cannot pull standard multi-platform
images without an explicit platform:

- 32-bit arm hosts: the default `linux/arm` matches both `arm/v6`
  and `arm/v7` children of standard indexes (busybox, alpine) —
  ambiguous, fails loudly.
- darwin hosts: the default `darwin/<arch>` matches nothing in
  linux-only indexes — every default pull fails, though mounting
  linux images for inspection is a primary darwin use case.

The failure is loud and actionable (the error lists available
platforms and asks for a more specific request), never silently
wrong. The pre-rework code was silently wrong instead (it always
pulled linux/amd64 regardless of host).

This is a genuine fork for the user: reasonable alternatives —
(a) keep strict defaults, require explicit platforms on such hosts;
(b) give the *default* request (never explicit ones) a documented
resolution order over variants; (c) detect the host variant
(containerd-style /proc/cpuinfo probing); (d) a darwin-specific
default of linux/<arch> for mount-for-inspection — differ in
observable behavior, and the spec-amend authority does not cover
choosing among them.

Lands: user decision on default-platform semantics for
variant-fragmented architectures, at the latest before darwin
mounting (the FSKit backend) serves default-platform pulls.
