# Status

This is the sole mutable source of the repository's current implementation
phase, blockers, active risks, ownership, and next authorized actions. Durable
architecture belongs in `docs/`, accepted decisions in `docs/adr/`,
forward-looking work in `llm/todo.md`, and chronological evidence in
`llm/log.md`.

Last updated: 2026-08-28 by Codex from David's decisions and local repository
evidence.

## Current Phase

The project is in the standalone-foundation phase. The history-preserving
extraction from `flowers-3d-helmholtz` is complete at commit `09dbe41`. The
original extracted baseline passed `82` tests plus `22` subtests. The protocol,
structured-receipt, strict-serialization, bundled-schema, and generic
cooperative-yield foundations are now implemented and independently versioned.
Typed immutable Project/v1 and ExperimentCard/v1 models, offline project-owned
extension validation, mutable Submission separation, and immutable admission
snapshot compilation are also implemented as storage-neutral library APIs.

The executable implementation is intentionally still schema-v4,
single-project compatibility code. It accepts an explicit operator-selected
state directory but still binds one database to one `--repo-root`, parses
legacy Flowers Markdown commands, and uses Flowers-specific shared-worktree
paths. New runner segments emit and ingest atomic typed receipts; a narrowly
bounded human-log parser remains only for legacy jobs. The typed yield protocol
is not yet wired into scheduler admission/failure isolation. The executable is
not ready to replace the operational Flowers queue.

The current objective is to accept the Project/Enrollment/ProjectRevision
lifecycle contract, add first-class projects and explicit database-v5 storage,
and bind the pure admission compiler to exact blobs read from a pinned Git tree
before any snapshot can become persistent queue state.

## Verified Baseline

- Source Flowers commit:
  `0082945b4d2771dcc1ed93de1c55552df5761f72`.
- Filtered pre-reorganization head:
  `39c29fbc59abe9f71f991a4ced5362024b70a54b`.
- Standalone extraction commit: `09dbe41`.
- Runtime/development version: Python 3.14.4; declared support is `>=3.14`.
- Package entry points: `experiment-queue`, `experiment-queue-web`, and
  `run-experiment`.
- State selection: absolute `--state-dir` takes precedence over
  `EXPERIMENT_QUEUE_STATE_DIR`; missing/relative state fails safely.
- Current generic suite: `409` tests and `26` subtests pass in the standalone
  `.venv`.
- The current wheel builds successfully; its isolated import includes the typed
  authoring/admission modules, authenticates compiler provenance against wheel
  metadata, and authenticates both bundled schema resources, pinned canonical
  digests, and editor exports.

## Accepted Product Decisions

- Repository/distribution: `experiment-queue`; import package:
  `experiment_queue`; primary CLI: `experiment-queue`.
- Project keys are immutable lowercase hyphenated slugs, at most 63 characters.
- Portable manifests/cards will use a strict YAML 1.2 subset validated by
  bundled JSON Schema plus version-owned semantic checks, with a maximum tree
  depth of 64, and will be stored as canonical JSON at admission.
- New RunnerManifest/v1 and RunnerReceipt/v1 documents carry independent typed
  identities; exact RunnerReceipt/v0 stdout parsing is legacy-only.
- CooperativeYieldRequest/v1 and CooperativeYieldReceipt/v1 use hashed regular
  files, typed progress, opaque resume bytes, and immutable continuation
  evidence. Schema-v4 yield shapes remain explicitly named v0 compatibility.
- Priority is global across projects and mutable, but never causes automatic
  preemption. Manual cooperative preemption remains explicit.
- Initial scheduling supports one NVIDIA GPU per independent job. Gang/DDP
  preemption is out of scope.
- The service stays dependency-light and never imports scientific project code.
- Database, Project, ExperimentCard, runner manifest/receipt, export, and yield
  protocols have independent version lineages.
- ADR 0008 fixes validated-only immutable authoring models, one project-owned
  extension namespace/schema, mutable Submission separation, whole-value
  bindings without interpolation, and frozen admission evidence. Compiler
  provenance comes only from installed package metadata.

## Ownership And Operational Boundary

- This repository owns all future generic queue, runner, web, schema,
  migration, compatibility, and onboarding development.
- `flowers-3d-helmholtz` owns its scientific cards, checkpoint behavior,
  experiment evidence, and the legacy queue operation until cutover.
- David owns remote execution, GPU allowlists, credentials, live-state backup,
  publication-remote selection, and production cutover authorization.
- Codex implements and verifies locally and does not access `mutton2`.

## Blockers And External Gates

- There is no blocker to local protocol, schema, database, scheduler, UX, test,
  or documentation development.
- On 2026-08-27, David explicitly confirmed that SPECFEM dataset generation and
  synchronized evidence closeout are complete and authorized work toward
  cutover. The scientific gate is satisfied.
- Production migration from the Flowers schema-v4 queue remains blocked on the
  standalone implementation, copied-state importer verification, an idle
  legacy queue with both database writers stopped, and a consistent backup plus
  external-path inventory.
- David waived a distinct production-state dress rehearsal because this is a
  single-operator queue. Comprehensive copied fixtures and a fresh two-project
  smoke remain required; the cutover itself still requires a consistent
  backup and offline copy-only, dry-run/receipt-verified migration.
- The repository has no configured publication remote. Local development can
  proceed; remote CI/publication waits for David's choice.

## Active Risks

- A migration could corrupt or reinterpret live history if it is automatic or
  performed in place.
- Legacy and standalone schedulers could contend for the same GPUs if cutover
  does not enforce a single writer.
- Project mounts, scratch roots, logs, or symlinks could disclose or mutate
  another project's files without strict resolved-path authorization.
- Mutable project configuration could change admitted execution unless every
  job pins an immutable revision and normalized specification.
- The storage-neutral admission compiler cannot prove its input bytes came from
  the claimed commit; the trusted ProjectRevision/Git resolver must satisfy that
  contract before database-v5 persistence accepts a snapshot.
- Flowers-specific W&B and checkpoint assumptions could leak into the generic
  core unless continuation context becomes opaque and versioned.
- A broken highest-priority project could head-of-line block healthy work unless
  failure quarantine and candidate selection are project-aware.

## Next Authorized Actions

1. Accept the Project/Enrollment/ProjectRevision lifecycle ADR, then implement
   its typed lifecycle models and schema-v5 project-aware storage.
2. Implement the trusted pinned-Git source resolver and require its evidence at
   the database admission boundary.
3. Wire typed cooperative-yield continuation validation into scheduler
   admission and project-scoped holds without blocking unrelated work.
4. Implement the explicit offline v1-v4 importer and exhaustive fixtures, then
   prove isolation with two temporary repositories before Flowers cutover.

Detailed dependencies and completion gates are in `llm/todo.md`; the durable
phase plan is `docs/implementation-plan.md`.

## Explicitly Out Of Scope

- Scientific experiment status or result interpretation.
- Automatic priority-triggered preemption.
- Gang scheduling and coordinated DDP/multi-GPU preemption.
- Non-NVIDIA accelerators, distributed queue instances, and containerization as
  a security sandbox.
- A general workflow DAG or matrix/template engine before explicit multi-job
  cards are proven.
