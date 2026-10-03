# The OCI layout's identity under the containerd image store is unwitnessed

Lands: when a consumer's run against a daemon using the containerd
image store (`docker info` naming `driver-type
io.containerd.snapshotter.v1`) loads an archive of the `OCILayout`
form and reports the loaded image's ID: a log of that run, the ID
compared with the manifest digest `Image.Archive` returned, banked
beside this issue.

`Image.Archive` (`docs/specs/api.md` REQ-api-archive) states that the
containerd image store names a loaded image by its manifest's
digest, the one the OCI layout's index names; the classic store's
form and identity were witnessed on a daemon (overlay2, Docker
29.8.1: a hand-made docker-archive loaded, `Loaded image ID:` the
configuration's digest), the containerd store's only reasoned from
how that store imports a layout. The descriptor the index carries
names the image's platform from its configuration; whether that
store's load filters by platform or names the image otherwise is
what the witness settles. A mismatch is no silent fault for a
consumer that holds the reported ID to the digest returned, as pb's
docker runner does: the run refuses, naming both.
