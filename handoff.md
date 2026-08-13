# Session handoff — consume, delete this file, proceed

You are resuming a long-running, first-principles rebuild of ocifs.
Read this fully, then `git rm handoff.md` (fold the deletion into
your first commit), then continue the work. The user's global
CLAUDE.md governs everything (gates, adversarial loop, artifact
homes); this file only carries session-specific state.

## Where things stand

- `docs/plans/read-path.md` is the active plan. Chunks 1–3 are done
  and committed (spec corpus + stipulator scaffolding; internal/layer
  unification core with oracle/property tests; cas + store rework on
  the tiered layout with a network-free harness). **Next: chunk 4 —
  platform + digest-addressed entry.** Git history holds the
  per-chunk records; read the last three commit messages
  (`git log -3`) before starting.
- Specs in `docs/specs/` are a stipulator-compiled corpus (54+ REQs).
  `docs/specs/store.md` is the canonical store contract and was
  amended during chunk 3 (refs encoding, REQ-store-adopt, self-heal
  pull-through clause) — re-read it fully before chunk 4; do not
  trust summaries of it.

## Standing user directives (explicit, still in force)

- Race-enabled testing is authorized for this repo (`-race`, policy
  `race: true`).
- Commit on adversarial-loop convergence without asking; small
  conventional commits; full gate lines in the commit message.
- Spec ownership is delegated: reconcile and amend specs from first
  principles as part of correctness; surface only genuine forks
  (decisions where reasonable alternatives differ in observable
  behavior) to the user. One fork was surfaced in chunk 3 (refs
  layout); the user chose fixed depth.
- `pgregory.net/rapid` is an approved dependency; use property tests
  freely.
- Nothing is sacred: rework any package if first principles demand
  it, through the loop.
- Reviewer quality over speed: adversarial reviewers run at full
  capability (no model downgrades to dodge API overload — back off
  and retry instead).

## Chunk 4 scope (from the plan, plus triage obligations)

Retain and record the **top-level artifact** (refs hold the top-level
digest; index retained in oci/ alongside the platform-selected
child), host-default platform, strict explicit platform (index and
direct-manifest cases), entry by (repo, digest, platform) with no tag
re-resolution, pull-policy + default-platform library options
(api.md REQ-api-construction), longest-prefix keychain resolution
(REQ-api-keychain — current authn.go iterates a map,
nondeterministic).

Triage at 4.1 must disposition:
- `docs/issues/store-oci-absent-pull-through.md` (Lands: 4) — the
  spec clause in REQ-store-self-heal is already settled; implement
  digest pull-through with the fetch machinery.
- Corpus gaps that resolve with chunk 4: REQ-store-ingest-order,
  -ref-complete, -pull-policy (platform clauses), -digest-entry,
  -platform-strict/-default/-serves-child, REQ-api-construction,
  -keychain, -acquire; plus the manual gap on REQ-store-self-heal
  (fire it when pull-through lands). REQ-store-layout's manual gap
  (exports/ tier) stays until chunk 10.

## Verification workflow (hard-won; do not rediscover)

- **stipulator** (MCP): `check` is the verdict; `bind` claims
  (implements/tests roles; method symbols like
  `pkg/path.Type.Method` resolve), `gap` declare/fire, `pin` —
  bare `pin` refreshes shape pins after signature changes, `pin
  ids=...` re-consents content pins after spec edits, `prune`
  deletes resolved gaps (prune residue makes check fail). Invariant
  REQs need **property witnesses** (rapid), not example tests.
  Policy vouches rapid's package-level generator state (complete
  var family) — don't add package-level rapid generators in tests;
  keep generators per-call.
- **gomutant**: `task mutate` (brackets `.scratch/{layer,store,cas}`).
  Known upstream limits: storage-layer packages get all survivors
  tagged `unstable-oracle` (bracket model conflicts with mutated
  code writing the surfaces tests read — datapoints #9–#11 relayed
  upstream, plus earlier #1–#8: record-commit loss, dirty-provenance
  on clean trees, etc.). Kills are still valid. The compensating
  protocol used in chunks 2–3: **hand-probe every load-bearing
  invariant** — `git add -A` FIRST (staging is the restore
  checkpoint; a probe before staging destroys unstaged fixes — this
  bit us once), apply one mutation, confirm a *named* test fails,
  `git restore` the file, re-verify green, re-stage. A surviving
  probe means a vacuous test: strengthen it (happened twice —
  same-ref concurrency, trailing-dot property).
- **Test-harness hygiene** (both tools observe runtime inputs):
  never `t.TempDir()`/`/tmp`, never `filepath.Abs` (PWD), never real
  sockets, never `authn.DefaultKeychain` (~/.docker) in
  store/cas/atomicfile/layer tests. Use `internal/scratchtest`
  (repo-bracketed `.scratch/<tier>/<seq>`, relative paths) and the
  in-process registry transport (`handlerTransport` in
  store_test.go — registry served through http.RoundTripper, no
  listener; `Store.transport` is the seam). The root-package FUSE
  test (`TestMountLocalImage`) uses a loopback httptest server and
  self-recovers from stale FUSE endpoints via fusermount.
- Adversarial loop: fresh-eyes subagent per round, brief in a
  scratchpad file, prior dispositions open each round's prompt;
  chunk 3 took 7 rounds to convergence and the H finding (dot-path
  traversal in MY new code) came in round 3 — do not cut rounds
  short.

## Machine prerequisites

Go ≥1.24, `task` (Taskfile), gomutant CLI on PATH, stipulator MCP
server configured in the harness, FUSE (fusermount/fusermount3) for
the mount test, network for real pulls only (test suite is fully
offline). Sibling checkouts expected under the same parent dir
(`~/repos/github.com/greatliontech/`): projfs-go and fskit-go are
read for chunks 7–8 design; gmdb and pb carry related-but-uncommitted
docs (see below); semrel is the CI convention reference.

## Loose ends outside this repo (verify they transferred)

- gmdb repo: `docs/issues/port-darwin.md`, `port-windows.md` +
  README section were written but NOT committed on the old machine.
- pb repo: `docs/notes/ecosystem.md` corrected-chain edits NOT
  committed on the old machine.
- gomutant upstream datapoints #7–#11 were relayed to the user's
  gomutant agent conversationally; if survivor attestation starts
  working, 24 equivalence families from chunk 2 (documented in
  commit cb70e7b) and chunk 3's survivor set are still open in
  tooling.
