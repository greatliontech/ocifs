# Issues

- `docs/issues/fskit-darwin-validation.md` — the FSKit backend's
  portable core is linux-pinned; the platform half (signed appex,
  bridge dispatch, orchestrated mount) awaits a darwin Tier-2 run.
  Lands: when the darwin mount validation reports back and its
  findings are dispositioned.
- `docs/issues/projfs-windows-validation.md` — the ProjFS backend and
  its windows test suite are authored and cross-compiled but
  unexecuted. Lands: when the windows test run reports back and its
  findings are dispositioned.
- `docs/issues/projfs-fskit-write-arms.md` — writable mounts are
  FUSE-only; the ProjFS/FSKit backends need write-engine glue plus
  platform fidelity mechanics. Lands: per backend, when its platform
  validation findings are dispositioned and a writable mount there
  is first needed.
- `docs/issues/concurrent-test-runs-collide-on-scratch-paths.md` —
  deterministic scratch paths make concurrent same-package test runs
  mutually destructive (tree removal and stale-mount reaping hit the
  other run's live state). Lands: when concurrent same-package runs
  become a supported flow, or the next collision incident.
- `docs/issues/mount-report-publication-marker.md` — mounts rows
  cannot distinguish a not-yet-published report from a published
  clean one; a publication marker is a wire-contract change awaiting
  the owner's call. Lands: when inspection must distinguish them.
