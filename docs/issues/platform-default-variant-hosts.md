# Default platform on 32-bit arm hosts needs variant detection

Default requests use the host's os/arch with no variant
(`docs/specs/store.md` REQ-store-platform-default), and selection
requires a unique match (REQ-store-platform-strict). On a 32-bit arm
host the default `linux/arm` matches both `arm/v6` and `arm/v7`
children of standard indexes (busybox, alpine) — ambiguous, so every
default pull fails loudly with the available platforms listed.

A variant preference order without host knowledge would be unsound:
preferring v7 silently selects binaries that trap on armv6 hardware
— the silent-wrong-result class the strict rule exists to prevent.
The correct fix is host variant detection (containerd-style CPU
probing) feeding the *default* request only; explicit requests stay
strict and unresolved ambiguity still fails.

Lands: when default-platform pulls on 32-bit arm hosts are first
needed.
