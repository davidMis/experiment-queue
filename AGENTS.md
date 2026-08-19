# Codex Project Instructions

These instructions apply to the `experiment-queue` repository.

## Session Startup

At the beginning of each substantive work session:

1. Read this file completely.
2. Read `llm/status.md` for the canonical current phase, implementation state,
   blockers, ownership, active risks, and next authorized work.
3. Read `llm/todo.md` for forward-looking actions.
4. Read `README.md` and the relevant sections of
   `docs/implementation-plan.md`, `docs/architecture.md`, and accepted ADRs
   before changing architecture, persistent state, protocols, or public APIs.
5. Read the latest entry in `llm/log.md`; consult archived logs only when the
   task needs that history.
6. Check `git status --short` before editing and preserve unrelated work.
7. Summarize the immediate objective and relevant constraints before
   substantial changes.

If a task depends on current external behavior, package versions, platform
support, or another repository's live state, verify the authoritative source or
ask the operator rather than relying on cached assumptions.

## Environment And Supported Platform

- Use the repository-local `.venv` with Python 3.14 or newer.
- Install from `pyproject.toml`; record every durable dependency there.
- Production version-1 support is Linux, SQLite, Git worktrees, POSIX process
  groups/PTYs, and NVIDIA GPUs visible to `nvidia-smi`.
- macOS is supported for development, validation, migration rehearsal, and
  unit tests, not production GPU dispatch.
- Keep the service environment independent of scientific project environments.
  Never import project code or dependencies into the queue service process.

## Architecture And Engineering Standards

- Keep reusable code under `src/experiment_queue/`; keep checkout-development
  wrappers under `scripts/` thin.
- New or substantially edited source, tests, and configuration files should
  begin with a brief purpose docstring or comment.
- Public APIs and non-obvious state transitions need comments describing
  invariants, failure modes, and security assumptions.
- Every CLI option needs actionable help text, including path requirements,
  defaults, and side effects.
- Expected failures must identify the failed input or operation and the likely
  fix.
- Keep scheduler-critical schema fields strict. Project flexibility belongs
  only under explicitly namespaced extension data.
- Prefer structured argv execution. Shell text is an explicit compatibility
  escape hatch, not the default job format.
- Treat database, Project manifest, ExperimentCard, runner manifest, runner
  receipt, queue export, and cooperative-yield versions as independent
  protocols. Never reuse one version number for another.
- Priority changes never authorize automatic preemption. Preemption remains an
  explicit operator action for a job that declared and was admitted with a
  compatible cooperative checkpoint capability.
- Version 1 supports one GPU per independently schedulable job. Do not mark
  tightly coupled multi-GPU/DDP work preemptible without a separately designed
  gang checkpoint and scheduling contract.

## Persistent-State And Security Safety

- Use temporary Git repositories and state directories in tests. Never point a
  test, migration rehearsal, or development server at operator state.
- Never launch development checks against a live GPU allowlist.
- Database migrations must be explicit, offline, dry-run capable, receipt
  producing, and tested against copies. Startup must not silently perform a
  destructive or one-way migration.
- Preserve queue item IDs, event history, commits, worktree/ref identity,
  process metadata, and continuation evidence during migration.
- Do not delete scientific artifacts as part of queue cleanup, project
  archival, database migration, or worktree removal.
- Project registration authorizes committed project code to execute as the
  service account. The queue is orchestration and provenance, not a sandbox.
- Enforce path and role authorization server-side. Browser filters and project
  labels are not security boundaries.
- Scale tests with the blast radius of database, process-control, recovery,
  path-security, preemption, and web-authorization changes.

## Flowers Migration Boundary

- `/Users/david/Projects/flowers-3d-helmholtz` retains its deprecated legacy
  queue as an operational dependency for current SPECFEM dataset generation.
- Future generic queue, runner, web, schema, and migration development belongs
  in this repository.
- Do not change the Flowers queue implementation, wrappers, immutable cards, or
  live state from this repository unless David explicitly requests a scoped
  compatibility change.
- Production cutover and legacy removal are blocked until David reports that
  SPECFEM data generation and its evidence closeout are complete.
- Never run the legacy and standalone schedulers concurrently against the same
  GPU pool or database.
- David is the sole operator of `mutton2`. Codex must not connect to, inspect,
  launch on, monitor, or synchronize with that machine. Provide reproducible
  commands and analyze only user-supplied local artifacts.
- Scientific intent, experiment status, and result interpretation remain in the
  scientific project's records, not this repository's `llm/` files.

## Documentation Ownership

- `llm/status.md` is the sole mutable source of current phase, implementation
  state, blockers, ownership, active risks, and next authorized actions.
- `llm/todo.md` contains forward-looking actions only. Every task needs a
  stable ID, owner, dependencies, and a concrete completion criterion.
- `llm/log.md` is the concise chronological session record. Older entries may
  later move verbatim to `llm/session_logs/`.
- `docs/implementation-plan.md` owns the durable phased roadmap, deliverables,
  dependencies, and exit criteria; it is not a progress tracker.
- `docs/architecture.md` owns the durable component and responsibility model.
- Accepted files under `docs/adr/` are immutable decisions. Supersede a
  decision with a new ADR instead of silently rewriting its rationale.
- `README.md` is the public setup and capability overview.
- `docs/migrations/` owns reproducible migration and rollback procedures.
- `docs/legacy/` is historical compatibility material, not the target API.

Do not scatter live TODOs through README files, ADRs, source comments, or
historical documents. Add them to `llm/todo.md` and link to the durable plan.

## Logging And Session Closeout

For each substantive session, maintain `llm/log.md` under a dated second-level
heading. Keep the entry concise: goal, decisions, material changes,
verification, unresolved risks, and next step.

At the end of a session:

1. Update the current `llm/log.md` entry.
2. Update `llm/status.md` if phase, blockers, ownership, risks, or next actions
   changed.
3. Update `llm/todo.md` only when forward-looking work changed.
4. Update the relevant durable plan, ADR, migration guide, or operator document
   when its contract changed.
5. Run verification proportional to the change and report exact results.
6. State what changed, what remains open, and whether either repository has
   uncommitted work.
