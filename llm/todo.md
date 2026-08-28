# TODO

This file contains forward-looking implementation work only. Current phase,
blockers, and ownership live in `llm/status.md`; completed work is removed from
this queue and summarized in `llm/log.md` or the relevant durable record.

Each item has a stable ID, owner, dependencies, and completion criterion.

## Governance And Release Baseline

- [ ] **EQ-GOV-001 — David:** choose the Git hosting destination and create or
  authorize the publication remote. Depends on no code work. Complete when the
  remote is configured and its visibility/ownership are recorded.
- [ ] **EQ-GOV-002 — Codex:** add Linux Python 3.14 CI after `EQ-GOV-001`.
  Complete when clean install, unit/integration tests, packaging, and CLI smoke
  run on every proposed change without accessing GPUs or operator state.
- [ ] **EQ-GOV-004 — Codex with David approval:** define release, deprecation,
  support, and changelog policy. Depends on the first multi-project release
  shape. Complete when the policy names compatibility guarantees and the legacy
  removal threshold.
- [ ] **EQ-GOV-005 — Codex:** migrate package license metadata to the supported
  PEP 639 SPDX form before setuptools ends TOML-table support on 2027-02-18.
  Depends on no product implementation. Complete when the wheel still contains
  `LICENSE` and builds without the setuptools license deprecation warning.

## Protocol And Receipt Foundation

- [ ] **EQ-PROTO-006 — Codex:** bind continuation identity to resolved spec
  digest, project revision, Git commit, run identity, and prior receipt in the
  scheduler hold/failure-isolation path. Depends on the completed structured
  receipt and typed cooperative-yield foundations. Complete when changed
  inputs/configs or corrupt resume payloads are held without blocking unrelated
  work.

## Versioned Schemas And Card Compiler

- [ ] **EQ-SCHEMA-009 — Codex:** implement `LegacyMarkdownCard/v0` with exactly
  the current parser contract and no broadened heuristics. Depends on the
  completed typed admission foundation. Complete when byte-for-byte legacy
  command fixtures pass and alternate/unresolved cards remain explicitly
  unimportable.
- [ ] **EQ-SCHEMA-010 — Codex:** add card/project `validate`, `explain`, and
  submission `--dry-run` output. Depends on the completed typed admission
  foundation. Complete when an operator can inspect all resolved bindings,
  paths, resources, digests, and preemption policy without mutating state.

## Project Model And Database V5

- [ ] **EQ-PROJECT-001 — Codex:** write an ADR for Project, host Enrollment,
  immutable ProjectRevision, lifecycle, checkout repointing, archive rules,
  state/artifact overlap, and cross-project dependencies. Depends on the
  completed typed Project foundation. Complete when ownership and mutation
  semantics are unambiguous before schema migration code begins.
- [ ] **EQ-PROJECT-002 — Codex:** implement typed registered-project lifecycle,
  host Enrollment, ProjectRevision, logical mount, artifact root, and project
  runtime-state models around the completed portable Project/v1 authoring
  model. Depends on `EQ-PROJECT-001`. Complete when invalid lifecycle or
  project/revision combinations cannot be constructed through public APIs.
- [ ] **EQ-PROJECT-003 — Codex:** add schema-v5 project, revision, mount,
  artifact-root, runtime-state, and job-artifact tables. Depends on
  `EQ-PROJECT-002`. Complete when foreign keys use `RESTRICT` and archival never
  cascades history or artifact deletion.
- [ ] **EQ-PROJECT-004 — Codex:** add non-null queue-item project/revision
  identity with a composite foreign key proving revision ownership. Depends on
  `EQ-PROJECT-003` and `EQ-PROJECT-009`. Complete when every new queue item has
  one immutable project revision and only resolver-authenticated admission
  evidence can be persisted.
- [ ] **EQ-PROJECT-005 — Codex:** replace global experiment-attempt uniqueness
  with `(project_id, experiment_id, attempt)` while keeping queue item IDs
  global. Depends on `EQ-PROJECT-004`. Complete when two projects can admit the
  same experiment ID independently.
- [ ] **EQ-PROJECT-006 — Codex:** add project identity/failure scope to events
  while retaining host-global events. Depends on `EQ-PROJECT-003`. Complete when
  every pause/failure can be classified as project or host scope.
- [ ] **EQ-PROJECT-007 — Codex:** implement active/paused/archived lifecycle and
  immutable revision activation. Depends on `EQ-PROJECT-002` through
  `EQ-PROJECT-006`. Complete when pause stops only new project dispatch,
  archive requires no active work, and history stays readable.
- [ ] **EQ-PROJECT-008 — Codex:** implement `project init/register/list/show/
  validate/doctor/pause/resume/archive`. Depends on `EQ-PROJECT-007` and
  `EQ-SCHEMA-010`. Complete when every command has JSON output and actionable
  path/lifecycle errors.
- [ ] **EQ-PROJECT-009 — Codex:** implement the trusted Git-tree admission
  source resolver that reads Project, card, and optional extension-schema bytes
  from their named paths in the pinned ProjectRevision commit before database
  mutation. Depends on `EQ-PROJECT-002` and the completed typed admission
  compiler. Complete when missing or mismatched paths, project/revision/commit
  identities, and byte claims fail closed, and snapshot names/hashes match Git
  object evidence.

## Offline Migration Infrastructure

- [ ] **EQ-MIGRATE-001 — Codex:** implement explicit v1-v4-to-v5 migration with
  `--dry-run`, SQLite backup/copy input, operator-supplied legacy project key,
  and machine-readable receipt. Depends on `EQ-PROJECT-003` through
  `EQ-PROJECT-006`. Complete when startup never performs this migration.
- [ ] **EQ-MIGRATE-002 — Codex:** preserve IDs, attempts, events, dependencies,
  reservations, process metadata, refs/worktrees, runner paths, and continuation
  data exactly. Depends on `EQ-MIGRATE-001`. Complete when before/after fixture
  comparisons prove equality except intentional new project fields.
- [ ] **EQ-MIGRATE-003 — Codex:** support a grandfathered legacy source state
  inside the Flowers checkout while requiring the v5 destination outside all
  project and artifact roots. Depends on `EQ-MIGRATE-001`. Complete when only
  importer input may use the legacy overlap and new registration rejects it.
- [ ] **EQ-MIGRATE-004 — Codex:** add migration verification for SQLite
  integrity/foreign keys, row/event counts, refs/worktrees, historical process
  metadata, and continuation digests. Depends on `EQ-MIGRATE-002`. Complete when
  any mismatch fails the receipt and leaves the source untouched.
- [ ] **EQ-MIGRATE-005 — Codex:** create v1-v4 fixtures for queued, held,
  blocked, running metadata, yielded, terminal, cleanup-failed, and external
  artifact paths. Depends on `EQ-MIGRATE-001`. Complete when every supported
  source version has success and corruption cases.
- [ ] **EQ-MIGRATE-006 — Codex:** make schema-v4 code refuse schema v5 and
  document rollback through the untouched source rather than downgrade. Depends
  on `EQ-MIGRATE-001`. Complete when compatibility tests prove both directions
  fail safely.

## Project-Aware Execution And Failure Isolation

- [ ] **EQ-EXEC-001 — Codex:** route Git operations, refs, worktrees, identity
  checks, and cleanup through the admitted project revision. Depends on
  `EQ-PROJECT-004`. Complete when two project repositories recover/reclaim
  independently after restart.
- [ ] **EQ-EXEC-002 — Codex:** replace hard-coded Flowers shared paths with
  declared logical mounts. Depends on `EQ-PROJECT-002`. Complete when traversal,
  symlink escape, overlap, missing-required mount, and changed-revision tests
  fail closed.
- [ ] **EQ-EXEC-003 — Codex:** authorize runner, log, checkpoint, manifest, and
  external scratch paths against the admitted revision's roots. Depends on
  `EQ-EXEC-002`. Complete when cross-project and outside-root access is denied
  through both core and web APIs.
- [ ] **EQ-EXEC-004 — Codex:** check disk space on central state and actual
  project artifact filesystems with correct host/project failure scope. Depends
  on `EQ-EXEC-003`. Complete when project artifact pressure pauses only that
  project and central-state pressure pauses all.
- [ ] **EQ-EXEC-005 — Codex:** construct child environments from a declared
  policy and protect reserved variables and GPU assignment. Depends on
  the completed typed admission foundation. Complete when inherited
  secrets/overrides follow explicit policy and cards cannot select GPUs.
- [ ] **EQ-EXEC-006 — Codex:** replace checkout-specific `cd` rewriting and
  nested project runner commands for structured jobs. Depends on the completed
  typed admission and structured-receipt foundations. Complete when new jobs
  execute direct argv in the pinned worktree without Flowers path knowledge.
- [ ] **EQ-EXEC-007 — Codex:** scope repository/card/mount/artifact/child circuit
  failures to a project and keep host failures global. Depends on
  `EQ-PROJECT-006`. Complete when a broken high-priority project cannot
  head-of-line block a healthy project.
- [ ] **EQ-EXEC-008 — Codex:** preserve global ordering
  `priority DESC, resume_front DESC, id ASC` and manual-only preemption across
  projects. Depends on `EQ-PROJECT-004`. Complete when cross-project priority,
  same-band continuation, and no-auto-preemption tests pass.

## CLI, Web, Authorization, And Observability

- [ ] **EQ-UX-001 — Codex:** add project-qualified submission/item references
  and cwd inference only for one unambiguous registered checkout. Depends on
  `EQ-PROJECT-008` and `EQ-SCHEMA-010`. Complete when ambiguous/unregistered cwd
  fails without inference.
- [ ] **EQ-UX-002 — Codex:** add project-filtered status, receipt, event, and
  artifact CLI output. Depends on `EQ-PROJECT-006`. Complete when JSON/text
  output includes project/revision identity and preserves global item IDs.
- [ ] **EQ-UX-003 — Codex:** add web project overview/health, admission
  selector, badges, filters, revision identity, and project run detail. Depends
  on `EQ-UX-001`. Complete when a two-project fixture is fully operable through
  the UI.
- [ ] **EQ-UX-004 — Codex:** move history filtering/pagination server-side and
  preserve filters through live updates. Depends on `EQ-UX-003`. Complete when
  the browser no longer receives unrequested cross-project history.
- [ ] **EQ-UX-005 — Codex:** enforce project-aware authorization on every
  direct job/log/artifact/event/mutation endpoint. Depends on `EQ-EXEC-003` and
  `EQ-UX-003`. Complete when direct-route disclosure and mutation tests pass.
- [ ] **EQ-UX-006 — Codex:** retain host admin/reservation roles for the first
  compatibility release while minimizing reservation information. Depends on
  `EQ-UX-003`. Complete when reservation users learn only required GPU
  availability and their own reservation state.
- [ ] **EQ-UX-007 — Codex with David approval:** design named principals and
  host-admin/project-maintainer/operator/viewer/reserver scopes before
  multi-team delegation. Depends on `EQ-UX-005`. Complete when server-side
  authorization tests cover every role and endpoint.

## Verification And Onboarding

- [ ] **EQ-TEST-001 — Codex:** add two temporary repositories with colliding
  experiment IDs, distinct environments/mounts/artifacts, and independent
  failures. Depends on `EQ-PROJECT-005` and `EQ-EXEC-001`. Complete when this
  fixture gates every multi-project milestone.
- [ ] **EQ-TEST-002 — Codex:** add project revision pinning, archive/no-cascade,
  restart recovery, reserved-env, path traversal, symlink escape, cross-project
  disclosure, and failure-scope tests. Depends on relevant Project/EXEC APIs.
  Complete when each invariant has a regression test.
- [ ] **EQ-TEST-003 — Codex:** add structured/legacy receipt, corrupt
  continuation, preemption/termination race, and package install/entry-point
  tests. Depends on the completed receipt/yield-helper foundations and
  `EQ-PROTO-006`. Complete when both compatibility and new paths pass on Python
  3.14.
- [ ] **EQ-ONBOARD-001 — Codex:** implement `project init`, `card new`, schema
  export, validation, doctor, and submit dry-run scaffolding. Depends on
  `EQ-PROJECT-008` and `EQ-SCHEMA-010`. Complete when generated files validate
  without manual repair.
- [ ] **EQ-ONBOARD-002 — Codex:** provide minimal ordinary, Python-training,
  data-pipeline, and cooperative-preemption examples. Depends on
  `EQ-ONBOARD-001` and the completed yield-helper foundation. Complete when
  examples pass GPU-free CI or conformance tests.
- [ ] **EQ-ONBOARD-003 — Codex:** write the ten-minute operator/project guides,
  mounts/artifacts/provenance/security reference, troubleshooting, and editor
  schema instructions. Depends on stable CLI/schema behavior. Complete when a
  fresh fixture integrates from documentation alone.

## Flowers Compatibility And Production Cutover

- [ ] **EQ-FLOWERS-001 — Codex:** create the portable Flowers Project manifest,
  extension schema, host enrollment fixture, mounts, external scratch roots,
  and runtime policy without changing live Flowers state. Depends on the
  completed typed Project foundation, `EQ-PROJECT-008`, and `EQ-EXEC-003`.
  Complete when local validation/doctor passes.
- [ ] **EQ-FLOWERS-002 — Codex:** inventory active/future versus historical
  Flowers cards and classify parser-compatible, alternate-heading, local,
  unresolved-template, coordinator, worker, and non-runnable records. Depends
  on `EQ-SCHEMA-009`. Complete when every current card has an explicit migration
  disposition without editing historical Markdown.
- [ ] **EQ-FLOWERS-003 — Codex:** convert representative simple,
  W&B/preemptible, and independently elastic SPECFEM jobs. Depends on
  `EQ-FLOWERS-001`, `EQ-PROTO-006`, and the completed typed admission
  foundation. Complete when exact commit/command/artifact/checkpoint/tracker/
  continuation identity is proven.
- [ ] **EQ-FLOWERS-004 — Codex:** prepare but do not activate Flowers
  compatibility wrappers and docs. Depends on `EQ-FLOWERS-003`. Complete when
  local dry-run commands target the installed package and legacy operation is
  unchanged.
- [ ] **EQ-FLOWERS-005 — Codex:** validate the v4 importer against exhaustive
  copied fixtures and run a fresh-state two-project smoke; David waived a
  separate production-state dress rehearsal. Depends on `EQ-MIGRATE-004`,
  `EQ-TEST-001`, and `EQ-FLOWERS-003`. Complete when all fixture
  receipts/counts/refs/paths/continuations/CLI/web checks pass and cutover can
  safely retry from an untouched source copy after any failure.
- [ ] **EQ-CUTOVER-002 — David with Codex instructions:** drain all
  `starting/running/yielding/terminating/force_killing` legacy items and stop
  both legacy database writers. Depends on the completed scientific cutover
  authorization gate recorded in `llm/status.md` and `llm/log.md`, plus
  `EQ-FLOWERS-005`. Complete when David reports an idle writer-free state.
- [ ] **EQ-CUTOVER-003 — David with Codex verification:** make a consistent
  SQLite backup, retain old code/state read-only, and supply the copied state
  and external-path inventory. Depends on `EQ-CUTOVER-002`. Complete when the
  local copy and backup identity are recorded.
- [ ] **EQ-CUTOVER-004 — Codex:** run the importer on the copy with explicit
  project key `flowers-3d-helmholtz` and validate integrity, counts, events,
  refs, worktrees, process metadata, artifacts, and continuation digests.
  Depends on `EQ-CUTOVER-003`. Complete when the migration receipt passes with
  no unexplained difference.
- [ ] **EQ-CUTOVER-005 — David with Codex handoff:** start exactly one
  standalone scheduler/web service and run the approved operational smoke.
  Depends on `EQ-CUTOVER-004`. Complete when dispatch, recovery, logs, receipts,
  GPU controls, priority, and manual preemption are verified and recorded.
- [ ] **EQ-DEPRECATE-001 — Codex:** observe the standalone deployment and emit
  at least one release of legacy-admission deprecation. Depends on
  `EQ-CUTOVER-005`. Complete when the observation window and release evidence
  meet the accepted support policy.
- [ ] **EQ-DEPRECATE-002 — Codex with David approval:** remove duplicated
  Flowers queue source/tests/wrappers only after proving no active card, state,
  receipt, import, or provenance dependency. Depends on `EQ-DEPRECATE-001`.
  Complete when Flowers retains historical evidence but no duplicate product
  implementation or queue-product TODO.
- [ ] **EQ-DEPRECATE-003 — Codex:** retire `LegacyMarkdownCard/v0` only after all
  supported projects have migrated and the compatibility policy permits it.
  Depends on `EQ-DEPRECATE-002` and the release policy. Complete when no
  supported state/card needs legacy admission and migration docs remain usable.

## Explicitly Deferred

These are not active TODOs: gang scheduling and DDP checkpoint coordination,
non-NVIDIA accelerator backends, distributed queue instances, containerization
as a security sandbox, automatic priority-triggered preemption, and a general
workflow DAG/matrix engine beyond explicit jobs and existing dependency links.
