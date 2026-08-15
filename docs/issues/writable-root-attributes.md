# Root-directory attribute changes are uncommittable

A `chmod`/`chown` of the mount root would copy up onto the upper
root directory itself, but the dialect walker surfaces no `.` entry
(the upper root is the container, not a member), so commit cannot
see or emit root-attribute changes — the layer dialect can carry
them (a `./` entry). Unreachable today: no write path exists until
the FUSE write chunk makes root mutation possible. When that chunk
lands the copy-up of the root, the walker grows a root-attribute
surface and commit emits the `.` entry iff it differs from the
base's root attributes.

Lands: with the FUSE write path's setattr arm (the chunk that makes
root mutation reachable).
