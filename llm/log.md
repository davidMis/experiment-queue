# Log

This is the concise chronological record of substantive repository sessions.
Live implementation state belongs in `llm/status.md`, and future actions belong
in `llm/todo.md`. Older entries may be moved verbatim to
`llm/session_logs/`.

## 2026-08-19 - Establish Standalone Repository

Goal: extract the experiment runner, queue, and web application from
`flowers-3d-helmholtz` into an independently maintained project without
changing the live production queue.

Result:

- Filtered the relevant history from Flowers source commit
  `0082945b4d2771dcc1ed93de1c55552df5761f72`; the filtered pre-reorganization
  head is `39c29fbc59abe9f71f991a4ced5362024b70a54b`.
- Created `/Users/david/Projects/experiment-queue`, reorganized the code under
  `src/experiment_queue/`, added Python `>=3.14` packaging and the
  `experiment-queue`, `experiment-queue-web`, and `run-experiment` entry points.
- Kept the service standard-library-only and left Flowers-specific scientific
  integration tests in the Flowers repository.
- Required an explicit absolute state directory through `--state-dir` or
  `EXPERIMENT_QUEUE_STATE_DIR` and added the accepted project-key validator.
- Recorded initial architecture, schema, compatibility, platform, trust,
  migration, and onboarding decisions. Committed the baseline as `09dbe41`.
- Verified the package in its own Python 3.14.4 `.venv`: `82` tests and `22`
  subtests pass; all three installed CLI help paths pass; missing state fails
  safely; Git history and both worktrees were clean.

Open:

- The executable remains a single-project schema-v4 compatibility baseline.
- Multi-project implementation and any production migration remain pending.

## 2026-08-19 - Consolidate Plans And Deprecation Ownership

Goal: make this repository the sole home for queue-product planning and Codex
state while clearly retaining the Flowers implementation only for current
SPECFEM production.

Result:

- Added `llm/status.md`, `llm/todo.md`, and `llm/log.md` with explicit ownership
  and session-maintenance rules in `AGENTS.md`.
- Consolidated the phased implementation, acceptance gates, risk controls,
  onboarding work, and Flowers cutover dependency in
  `docs/implementation-plan.md` and the live TODO.
- Recorded that production cutover and legacy removal cannot begin until David
  confirms SPECFEM data generation and evidence closeout are complete.
- Updated Flowers live guidance separately to deprecate new in-repo queue
  development while leaving current scientific production actions and
  historical records intact.

Verification:

- The standalone suite remains green: `82` tests and `22` subtests passed in
  `9.39 s`.
- All `68` live TODO IDs are unique, every dependency reference resolves, every
  task has a completion criterion, all local Markdown links resolve, and both
  repositories pass `git diff --check`.
- Flowers source tests were not rerun because its changes are documentation and
  live-ledger ownership only; no source, wrapper, card, or test changed.

Open:

- Protocol/schema foundations and database v5 are the next code milestones.
- A real production state copy and publication remote have not been supplied;
  neither blocks local implementation.

## 2026-08-27 - Begin Cutover Foundation Implementation

Goal: begin the recorded standalone cutover work after David confirmed the
Flowers scientific gate was complete, without touching live Flowers state or
`mutton2`.

Decisions:

- David explicitly confirmed SPECFEM dataset generation and synchronized
  evidence closeout are complete and authorized work toward cutover.
- David waived a separate production-state dress rehearsal for this
  single-operator queue. Fixture/importer verification and a fresh two-project
  smoke remain mandatory; cutover still migrates only an offline copy with a
  dry run, machine receipt, verification, and untouched v4 rollback source.
- Accepted ADR 0006 for strict YAML/Draft 2020-12/RFC 8785 behavior and ADR
  0007 for bounded trees, direct `referencing` ownership, semantic validation,
  and precise YAML presentation semantics.
- Named the extracted schema-v4 yield request/receipt shapes v0 compatibility;
  typed cooperative-yield v1 remains a distinct wire protocol.

Result:

- Completed `EQ-GOV-003`, `EQ-PROTO-001` through `EQ-PROTO-005`, and
  `EQ-SCHEMA-001` through `EQ-SCHEMA-003`; removed them from the live TODO.
- Added the independent protocol registry, identity fixtures, ADR index, and
  compatibility/ownership matrix.
- Added atomic RunnerManifest/v1 and RunnerReceipt/v1 emission/ingestion,
  restart recovery, signal-exit normalization, strict path authorization, and
  an exact RunnerReceipt/v0 stdout fallback for legacy jobs.
- Added bounded strict YAML, RFC 8785 canonical JSON, digest-authenticated
  Project/v1 and ExperimentCard/v1 schemas, version-owned semantic checks,
  editor export, and installed-wheel schema verification. Runtime dependencies
  are explicitly bounded in `pyproject.toml`.
- Added the dependency-light CooperativeYield/v1 request, receipt, helper, and
  conformance APIs with strict interoperable JSON, typed progress, opaque
  resume bytes, exact admitted checkpoint-name checks, path-bound stable file
  hashing, and continuation identity over spec/revision/Git/run/prior receipt.
- Updated the runner guide, README, implementation plan, migration guide,
  status, and TODO for the completed scientific gate and the no-separate-dress-
  rehearsal decision.

Verification:

- Full Python 3.14.4 suite: `277 passed, 26 subtests passed` in `10.77 s`.
- Final wheel build succeeded; isolated wheel import authenticated both schema
  resources, pinned canonical digests, and editor exports.
- Independent adversarial review reported no remaining actionable code or
  documentation findings; `git diff --check` passed.
- No Flowers source/card/wrapper/live-state file changed, no GPU allowlist or
  production state was used, and no connection to `mutton2` was attempted.

Open:

- The executable remains schema-v4/single-project. `EQ-PROTO-006` stays open
  until typed continuation evidence is wired into scheduler holds and failure
  isolation.
- Typed Project/ExperimentCard models, extensions, Submission/admission
  snapshots, the Project lifecycle ADR, schema v5, offline importer, and
  two-project isolation are next.
- The publication remote is unset. A non-blocking setuptools license-metadata
  deprecation is tracked as `EQ-GOV-005` before its 2027 deadline.

## 2026-08-28 - Implement Typed Authoring And Admission Evidence

Goal: complete the storage-neutral Project/v1, ExperimentCard/v1, extension,
Submission, and admission-snapshot foundation before database-v5 work, without
touching live Flowers state.

Decisions:

- Accepted ADR 0008: validated authoring models are deeply immutable; mutable
  scheduling policy lives in Submission; bindings replace only complete
  declared parameter values; and one offline project-owned schema validates the
  `extensions.<project-key>` envelope.
- Admission compilation is pure. A future trusted ProjectRevision/Git resolver
  must supply exact named blobs from the pinned full commit before persistence.
  Compiler provenance is always read from installed package metadata.

Result:

- Completed `EQ-SCHEMA-004` through `EQ-SCHEMA-008` and removed them from the
  live TODO.
- Added typed Project/card/nested command-resource-artifact models, strict
  cross-document references, service-owned environment protection, placeholder
  rejection, and exact JSON-native round trips.
- Added strict offline Draft 2020-12 extension-schema validation with canonical
  digest authentication, dormant-reference preflight, recursion handling, and
  exact source/canonical evidence.
- Added mutable Submission input and factory-only immutable policy/schema/
  snapshot evidence with detached inputs, selected-job resolution, bounded
  whole-value bindings, full Git identity, and stable resolved execution
  digests. Later caller mutations cannot alter snapshots.
- Hardened wheel verification to require the new authoring modules and recorded
  `EQ-PROJECT-009` as the Git-tree resolver gate before database admission.

Verification:

- Full Python 3.14 suite: `409 passed, 26 subtests passed` in `15.26 s`.
- Focused authoring/extension/admission suite: `132 passed` in `3.72 s`;
  `py_compile` and `git diff --check` passed.
- A fresh wheel built successfully and isolated verification confirmed the
  authoring/admission modules, compiler provenance against wheel metadata, both
  bundled schemas, pinned canonical digests, and editor exports. The known
  setuptools license warning remains tracked as `EQ-GOV-005`.
- No Flowers source/card/wrapper/live-state file changed, no GPU allowlist or
  production state was used, and no connection to `mutton2` was attempted.

Open:

- The executable remains schema-v4/single-project; typed snapshots are not yet
  persisted or executed.
- The Project lifecycle ADR/models, trusted Git resolver, schema-v5 storage,
  typed-yield scheduler integration, offline importer, and two-project
  isolation remain next.
