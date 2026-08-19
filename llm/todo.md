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
- [ ] **EQ-GOV-003 — Codex:** create an ADR index and compatibility/version
  matrix covering database, Project, ExperimentCard, runner manifest/receipt,
  queue export, and yield protocols. Complete when every supported major
  version and fallback has one discoverable owner and test fixture.
- [ ] **EQ-GOV-004 — Codex with David approval:** define release, deprecation,
  support, and changelog policy. Depends on the first multi-project release
  shape. Complete when the policy names compatibility guarantees and the legacy
  removal threshold.

## Protocol And Receipt Foundation

- [ ] **EQ-PROTO-001 — Codex:** implement independent protocol version
  constants/types instead of the current shared integer conventions. Depends on
  `EQ-GOV-003`. Complete when every serialized document identifies its kind and
  major version independently.
- [ ] **EQ-PROTO-002 — Codex:** define and implement an atomic structured runner
  receipt containing run directory, manifest, logs, sync instructions, status,
  and segment identity. Depends on `EQ-PROTO-001`. Complete when scheduler tests
  no longer require English stdout for new jobs.
- [ ] **EQ-PROTO-003 — Codex:** retain a narrowly named legacy stdout-receipt
  parser for imported jobs. Depends on `EQ-PROTO-002`. Complete when golden
  legacy fixtures resolve identically and malformed logs fail without guessing.
- [ ] **EQ-PROTO-004 — Codex:** define a generic cooperative-yield request and
  receipt with hashed checkpoint artifacts, typed progress, and opaque
  project-owned resume context. Depends on `EQ-PROTO-001`. Complete when no
  W&B-specific field is required by the generic protocol.
- [ ] **EQ-PROTO-005 — Codex:** provide a dependency-light optional yield helper
  and conformance suite for atomic ready/failed receipts and continuation
  validation. Depends on `EQ-PROTO-004`. Complete when a fixture project can
  implement safe preemption without copying environment constants or hashing
  logic.
- [ ] **EQ-PROTO-006 — Codex:** bind continuation identity to resolved spec
  digest, project revision, Git commit, run identity, and prior receipt. Depends
  on `EQ-PROTO-002` and `EQ-PROTO-004`. Complete when changed inputs/configs or
  corrupt resume payloads are held without blocking unrelated work.

## Versioned Schemas And Card Compiler

- [ ] **EQ-SCHEMA-001 — Codex:** select the exact YAML 1.2 parser, JSON Schema
  implementation, canonical-JSON algorithm, and dependency bounds in a new ADR.
  Depends on no other implementation task. Complete when behavior for duplicate
  keys, aliases, tags, merges, timestamps, floats, and Unicode is explicit and
  testable.
- [ ] **EQ-SCHEMA-002 — Codex:** implement the strict YAML loader and canonical
  source/digest utilities. Depends on `EQ-SCHEMA-001`. Complete when rejected
  YAML constructs and stable canonical hashes have golden tests.
- [ ] **EQ-SCHEMA-003 — Codex:** bundle immutable Draft 2020-12 Project v1 and
  ExperimentCard v1 JSON Schemas with schema digests. Depends on
  `EQ-SCHEMA-001`. Complete when installed-package resource loading and editor
  export are tested.
- [ ] **EQ-SCHEMA-004 — Codex:** model the portable Project contract: immutable
  key, display name, card roots, declared logical volumes, environment policy,
  supported protocols, and optional extension schema. Depends on
  `EQ-SCHEMA-002` and `EQ-SCHEMA-003`. Complete when no host absolute path or
  credential can appear in portable core fields.
- [ ] **EQ-SCHEMA-005 — Codex:** model ExperimentCard scientific identity and
  one-or-more explicit jobs with argv/wrapper execution, parameters, resources,
  artifacts, provenance, and declared capabilities. Depends on
  `EQ-SCHEMA-002` and `EQ-SCHEMA-003`. Complete when a simple job and an
  explicit coordinator/worker card validate without a template engine.
- [ ] **EQ-SCHEMA-006 — Codex:** implement namespaced extension validation.
  Depends on `EQ-SCHEMA-004` and `EQ-SCHEMA-005`. Complete when unknown core
  fields fail, project extensions remain flexible, and a supplied extension
  schema can add project-specific requirements.
- [ ] **EQ-SCHEMA-007 — Codex:** separate immutable cards from mutable
  Submission bindings, priority, holds, dependencies, operator, and preemption
  authorization. Depends on `EQ-SCHEMA-005`. Complete when no runtime scheduling
  state is serialized into a committed card.
- [ ] **EQ-SCHEMA-008 — Codex:** implement admission compilation/snapshots of
  raw bytes/hash, normalized and resolved JSON/hash, schema identity/hash,
  project revision, Git commit, command, and package version. Depends on
  `EQ-SCHEMA-004` through `EQ-SCHEMA-007`. Complete when a later file/config
  edit cannot change admitted execution.
- [ ] **EQ-SCHEMA-009 — Codex:** implement `LegacyMarkdownCard/v0` with exactly
  the current parser contract and no broadened heuristics. Depends on
  `EQ-SCHEMA-008`. Complete when byte-for-byte legacy command fixtures pass and
  alternate/unresolved cards remain explicitly unimportable.
- [ ] **EQ-SCHEMA-010 — Codex:** add card/project `validate`, `explain`, and
  submission `--dry-run` output. Depends on `EQ-SCHEMA-008`. Complete when an
  operator can inspect all resolved bindings, paths, resources, digests, and
  preemption policy without mutating state.

## Project Model And Database V5

- [ ] **EQ-PROJECT-001 — Codex:** write an ADR for Project, host Enrollment,
  immutable ProjectRevision, lifecycle, checkout repointing, archive rules,
  state/artifact overlap, and cross-project dependencies. Depends on
  `EQ-SCHEMA-004`. Complete when ownership and mutation semantics are
  unambiguous before schema migration code begins.
- [ ] **EQ-PROJECT-002 — Codex:** implement typed Project, Enrollment,
  ProjectRevision, logical mount, artifact root, and project runtime-state
  models. Depends on `EQ-PROJECT-001`. Complete when invalid lifecycle or
  project/revision combinations cannot be constructed through public APIs.
- [ ] **EQ-PROJECT-003 — Codex:** add schema-v5 project, revision, mount,
  artifact-root, runtime-state, and job-artifact tables. Depends on
  `EQ-PROJECT-002`. Complete when foreign keys use `RESTRICT` and archival never
  cascades history or artifact deletion.
- [ ] **EQ-PROJECT-004 — Codex:** add non-null queue-item project/revision
  identity with a composite foreign key proving revision ownership. Depends on
  `EQ-PROJECT-003`. Complete when every new queue item has one immutable project
  revision.
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
  `EQ-SCHEMA-008`. Complete when inherited secrets/overrides follow explicit
  policy and cards cannot select GPUs.
- [ ] **EQ-EXEC-006 — Codex:** replace checkout-specific `cd` rewriting and
  nested project runner commands for structured jobs. Depends on
  `EQ-SCHEMA-008` and `EQ-PROTO-002`. Complete when new jobs execute direct argv
  in the pinned worktree without Flowers path knowledge.
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
  tests. Depends on `EQ-PROTO-002` through `EQ-PROTO-006`. Complete when both
  compatibility and new paths pass on Python 3.14.
- [ ] **EQ-ONBOARD-001 — Codex:** implement `project init`, `card new`, schema
  export, validation, doctor, and submit dry-run scaffolding. Depends on
  `EQ-PROJECT-008` and `EQ-SCHEMA-010`. Complete when generated files validate
  without manual repair.
- [ ] **EQ-ONBOARD-002 — Codex:** provide minimal ordinary, Python-training,
  data-pipeline, and cooperative-preemption examples. Depends on
  `EQ-ONBOARD-001` and `EQ-PROTO-005`. Complete when examples pass GPU-free CI
  or conformance tests.
- [ ] **EQ-ONBOARD-003 — Codex:** write the ten-minute operator/project guides,
  mounts/artifacts/provenance/security reference, troubleshooting, and editor
  schema instructions. Depends on stable CLI/schema behavior. Complete when a
  fresh fixture integrates from documentation alone.

## Flowers Compatibility And Production Cutover

- [ ] **EQ-FLOWERS-001 — Codex:** create the portable Flowers Project manifest,
  extension schema, host enrollment fixture, mounts, external scratch roots,
  and runtime policy without changing live Flowers state. Depends on
  `EQ-SCHEMA-004`, `EQ-PROJECT-008`, and `EQ-EXEC-003`. Complete when local
  validation/doctor passes.
- [ ] **EQ-FLOWERS-002 — Codex:** inventory active/future versus historical
  Flowers cards and classify parser-compatible, alternate-heading, local,
  unresolved-template, coordinator, worker, and non-runnable records. Depends
  on `EQ-SCHEMA-009`. Complete when every current card has an explicit migration
  disposition without editing historical Markdown.
- [ ] **EQ-FLOWERS-003 — Codex:** convert representative simple,
  W&B/preemptible, and independently elastic SPECFEM jobs. Depends on
  `EQ-FLOWERS-001`, `EQ-PROTO-006`, and `EQ-SCHEMA-008`. Complete when exact
  commit/command/artifact/checkpoint/tracker/continuation identity is proven.
- [ ] **EQ-FLOWERS-004 — Codex:** prepare but do not activate Flowers
  compatibility wrappers and docs. Depends on `EQ-FLOWERS-003`. Complete when
  local dry-run commands target the installed package and legacy operation is
  unchanged.
- [ ] **EQ-FLOWERS-005 — Codex:** rehearse the v4 importer against a copied
  representative state and run a fresh-state two-project smoke. Depends on
  `EQ-MIGRATE-004`, `EQ-TEST-001`, and `EQ-FLOWERS-003`. Complete when all
  receipts/counts/refs/paths/continuations/CLI/web checks pass.
- [ ] **EQ-CUTOVER-001 — David:** confirm SPECFEM dataset generation and
  evidence closeout are complete and explicitly authorize migration. Depends on
  the Flowers scientific project, not queue implementation. Complete when David
  explicitly reports that gate; do not infer it.
- [ ] **EQ-CUTOVER-002 — David with Codex instructions:** drain all
  `starting/running/yielding/terminating/force_killing` legacy items and stop
  both legacy database writers. Depends on `EQ-CUTOVER-001` and
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
