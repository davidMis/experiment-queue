# TODO

This file contains forward-looking work only. Current state and risks live in
`llm/status.md`; completed work belongs in `llm/log.md`.

Each item has a stable ID, owner, dependencies, and completion criterion.

## Public Wiki

- [ ] **EQ-WIKI-001 — Codex after David signs in to GitHub:** initialize the
  public repository wiki and publish the six reviewed pages. Depends on the
  published `v0.2.1` behavior now documented by those pages. Complete when the
  wiki home, installation, project setup, daily operations, troubleshooting,
  and sidebar pages are publicly visible.

## Fresh Flowers Startup

- [ ] **EQ-DEPLOY-001 — David with Codex instructions:** update
  `/home/sdm11/experiment-queue` to the published simplified revision, create
  its repository-local Python 3.14 `.venv`, install the clone editable, and
  prepare a minimal committed Flowers Project/v1 and ExperimentCard/v1 with
  `volumes: []`. Depends on published release `v0.2.1`. Complete when
  validation passes and `/home/sdm11/3D_Helmholtz/.venv` exists under a
  committed ignore rule.
- [ ] **EQ-DEPLOY-002 — David with Codex instructions:** register the exact
  Flowers commit into fresh state under `/home/sdm11/srv/experiment-queue`, run
  `project doctor`, add the intended GPU, and initialize web credentials.
  Depends on `EQ-DEPLOY-001`. Complete when all operations succeed without an
  Enrollment file or legacy importer.
- [ ] **EQ-DEPLOY-003 — David with Codex handoff:** start exactly one schema-v5
  scheduler and web service, submit one simple card, and inspect terminal
  evidence. Depends on `EQ-DEPLOY-002`. Complete when the new queue dispatches
  the job and records its expected event, attempt, and receipt evidence.

## Compatibility Deprecation

- [ ] **EQ-DEPRECATE-001 — Codex:** observe the standalone deployment and ship
  at least one release that announces legacy-admission deprecation. Depends on
  `EQ-DEPLOY-003`. Complete when `docs/release-policy.md` permits the next
  compatibility step.
- [ ] **EQ-DEPRECATE-002 — David with Codex verification:** remove duplicated
  Flowers queue code only after proving no current operational dependency.
  Depends on `EQ-DEPRECATE-001`. Complete when Flowers retains historical
  scientific evidence but no longer depends on deprecated queue code.
- [ ] **EQ-DEPRECATE-003 — Codex with David approval:** retire new
  `LegacyMarkdownCard/v0` admission only after every supported project has
  moved and the compatibility policy permits removal. Depends on
  `EQ-DEPRECATE-002`.

## Explicitly Deferred

Named multi-team principals, gang scheduling, non-NVIDIA accelerators,
distributed queue instances, container sandboxing, automatic
priority-triggered preemption, and a general workflow DAG/matrix engine are not
active startup tasks.
