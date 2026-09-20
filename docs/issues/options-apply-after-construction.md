# Options apply after construction

Lands: when an option is next added, removed or changed — the change
set that touches the option set carries the collapse

`Option` is `func(*OCIFS)`, and every option writes a field of the
store it is handed: `WithAuthSource` writes the credential map,
`WithEnableDefaultKeychain` its flag, the others the pull policy,
platform, verifier, collection settings and transport. New applies
the options to the store it builds, but nothing confines them there:
an option value applied to a constructed store compiles, and writes
state the store and, through Keychain, a consumer read concurrently
from their own goroutines — a credential-map write against a
concurrent Resolve is a fatal concurrent map access. REQ-api-construction
confines configuration to construction; the Go shape does not.

The collapse: options apply to an unexported settings value that only
New holds — `Option func(*settings)` — and New builds the store from
the settled value, so a post-construction application is
unrepresentable rather than a documented misuse. Every option moves
onto the settings value; the store's fields are set once from it;
the construction tests bind unchanged.
