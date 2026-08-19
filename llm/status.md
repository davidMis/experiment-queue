# Status

This is the sole mutable source of the repository's current implementation
phase, blockers, active risks, ownership, and next authorized actions. Durable
architecture belongs in `docs/`, accepted decisions in `docs/adr/`,
forward-looking work in `llm/todo.md`, and chronological evidence in
`llm/log.md`.

Last updated: 2026-08-19 by Codex from David's decisions and local repository
evidence.

## Current Phase

The project is in the standalone-foundation phase. The history-preserving
extraction from `flowers-3d-helmholtz` is complete at commit `09dbe41`. The
package installs in its own Python 3.14 environment, and the extracted baseline
passes `82` tests plus `22` subtests.

The executable implementation is intentionally still schema-v4,
single-project compatibility code. It accepts an explicit operator-selected
state directory but still binds one database to one `--repo-root`, parses
legacy Flowers Markdown commands, uses Flowers-specific shared-worktree paths,
and discovers runner paths from human log lines. It is not ready to replace the
operational Flowers queue.

The current objective is to establish protocol/schema foundations, then add
first-class projects and an explicit database-v5 migration without changing or
operating live Flowers state.

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
- Generic suite: `82` tests and `22` subtests pass in the standalone `.venv`.

## Accepted Product Decisions

- Repository/distribution: `experiment-queue`; import package:
  `experiment_queue`; primary CLI: `experiment-queue`.
- Project keys are immutable lowercase hyphenated slugs, at most 63 characters.
- Portable manifests/cards will use a strict YAML 1.2 subset validated by
  bundled JSON Schema and stored as canonical JSON at admission.
- Priority is global across projects and mutable, but never causes automatic
  preemption. Manual cooperative preemption remains explicit.
- Initial scheduling supports one NVIDIA GPU per independent job. Gang/DDP
  preemption is out of scope.
- The service stays dependency-light and never imports scientific project code.
- Database, Project, ExperimentCard, runner manifest/receipt, export, and yield
  protocols have independent version lineages.

## Ownership And Operational Boundary

- This repository owns all future generic queue, runner, web, schema,
  migration, compatibility, and onboarding development.
- `flowers-3d-helmholtz` owns its scientific cards, checkpoint behavior,
  experiment evidence, and current SPECFEM production operation.
- David owns remote execution, GPU allowlists, credentials, live-state backup,
  publication-remote selection, and production cutover authorization.
- Codex implements and verifies locally and does not access `mutton2`.

## Blockers And External Gates

- There is no blocker to local protocol, schema, database, scheduler, UX, test,
  or documentation development.
- Production migration from the Flowers schema-v4 queue is intentionally
  blocked until the SPECFEM dataset has been generated, its synchronized
  evidence has been closed out, active legacy queue work has drained, and David
  explicitly authorizes cutover.
- No real production state copy has been supplied for migration rehearsal.
  Synthetic fixtures and migration tooling can proceed meanwhile.
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
- Flowers-specific W&B and checkpoint assumptions could leak into the generic
  core unless continuation context becomes opaque and versioned.
- A broken highest-priority project could head-of-line block healthy work unless
  failure quarantine and candidate selection are project-aware.

## Next Authorized Actions

1. Establish the independent protocol-version registry and structured runner
   receipt while retaining tested legacy fallback.
2. Select and record exact strict-YAML, JSON Schema, and canonical-JSON behavior;
   implement Project and ExperimentCard schema/model foundations.
3. Design and implement schema-v5 project/enrollment/revision storage and an
   explicit offline migration with synthetic v1-v4 fixtures.
4. Prove multi-project isolation with two temporary repositories before any
   Flowers state migration.

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
