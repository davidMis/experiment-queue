# experiment-queue

`experiment-queue` is a durable, operator-controlled queue for scientific GPU
experiments on a shared unmanaged host. It records exact Git and command
identity, schedules jobs from multiple registered projects on operator-approved
NVIDIA GPUs, preserves run and artifact evidence, and supports explicit
cooperative checkpoint-and-requeue preemption.

## Current status

The primary installed CLI and web application use the project-aware
`Database/v5` implementation. Schema v5 has first-class Projects, immutable
ProjectRevisions, exact Git-tree admission, project-isolated worktrees and
failures, passive GPU reservations, explicit cooperative preemption, and durable
termination/recovery. Startup never converts old state.

The extracted schema-v4 implementation remains available only through the
explicit `experiment-queue-legacy-v4` and
`experiment-queue-web-legacy-v4` compatibility entry points. Do not run a v4
and v5 scheduler concurrently against the same GPU pool.

David confirmed on 2026-08-27 that Flowers SPECFEM generation and synchronized
evidence closeout are complete. He also waived a separate production-state
dress rehearsal for this single-operator queue. That does not authorize a
production migration by itself: cutover still requires an idle writer-free v4
queue, a consistent offline copy, an operator-supplied external-path and card
inventory, successful dry-run and real migration receipts, and David's explicit
cutover authorization. See the exact
[Flowers cutover and rollback checklist](docs/migrations/flowers-v4.md).

Live implementation state belongs in [`llm/status.md`](llm/status.md); the
durable delivery and rollback model belongs in
[`docs/implementation-plan.md`](docs/implementation-plan.md).

## Development setup

Python 3.14 or newer is required.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```

Production version 1 supports Linux, Git worktrees, POSIX process groups and
PTYs, SQLite, and NVIDIA GPUs visible to `nvidia-smi`. macOS supports local
development, validation, migration work, and unit tests, not production GPU
dispatch.

## Explicit state and project selection

Every stateful command requires an operator-selected absolute state directory.
Pass it directly or set it in the service environment:

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/srv/experiment-queue/state
experiment-queue project list
```

There is no current-directory fallback for state. Outside the separate offline
importer, only explicit first-project registration may create fresh schema-v5
state; read, control, scheduler, and web commands require an existing exact-v5
database. A command may infer a
Project from the current directory only when that canonical path is inside
exactly one current registered checkout. Use `--project PROJECT-KEY` whenever
there could be ambiguity.

The [project onboarding guide](docs/project-onboarding.md) walks through a
fresh repository, Enrollment, exact revision registration, validation, dry run,
submission, and dispatch. Runnable portable examples cover
[ordinary](examples/ordinary), [Python training](examples/python-training),
[data pipeline](examples/data-pipeline), and
[cooperative preemption](examples/cooperative-preemption) projects. The
[Flowers compatibility fixture](examples/flowers-compatibility) is local test
material, not a description of live Flowers state.

## Installed commands

- `experiment-queue`: primary project-aware schema-v5 project, card,
  submission, queue, GPU, reservation, preemption, termination, migration, and
  scheduler controls;
- `experiment-queue-web`: authenticated project-aware HTTPS interface and
  minimal-information reservation view;
- `experiment-queue-migrate-v5`: standalone alias for the explicit offline
  copy-only v1-v4 importer;
- `run-experiment`: standalone run provenance, logs, manifests, continuation
  segments, receipts, and synchronization instructions;
- `experiment-queue-legacy-v4` and `experiment-queue-web-legacy-v4`:
  deprecated rollback/compatibility surfaces, never schema-v5 entry points.

The checkout wrappers under `scripts/` select the primary v5 applications.
New integrations should use the installed commands and typed YAML authoring
contracts, not legacy Markdown admission.

## Safety boundary

Registering a scientific project authorizes committed code from admitted Git
objects to execute as the queue service account. The queue is orchestration and
provenance, not a sandbox. Keep project environments separate from the service,
use a dedicated non-root account, keep state private, and read
[`SECURITY.md`](SECURITY.md) before deployment.

Project manifests contain logical names only. Host paths live in immutable
Enrollment evidence, and child jobs receive authorized paths through
`EXPERIMENT_QUEUE_MOUNT_<NAME>` and `EXPERIMENT_QUEUE_ARTIFACT_<NAME>`
variables. Details are in the [operator guide](docs/operator-guide.md).

The original detailed Flowers instructions are retained only as historical
migration reference in
[`docs/legacy/flowers-operator-guide.md`](docs/legacy/flowers-operator-guide.md).

## Documentation

- [`docs/operator-guide.md`](docs/operator-guide.md): schema-v5 lifecycle,
  scheduler, reservation, preemption, termination, web, and recovery operation;
- [`docs/project-onboarding.md`](docs/project-onboarding.md): a fresh Project
  integration from scaffold through submission;
- [`docs/authoring-and-admission.md`](docs/authoring-and-admission.md): strict
  Project/card validation and exact Git-backed admission evidence;
- [`docs/protocol-compatibility.md`](docs/protocol-compatibility.md): independent
  protocol versions, owners, fallbacks, and fixtures;
- [`docs/release-policy.md`](docs/release-policy.md): release, support,
  compatibility, deprecation, and changelog policy;
- [`CHANGELOG.md`](CHANGELOG.md): operator-visible changes by release;
- [`docs/adr/README.md`](docs/adr/README.md): accepted decision index;
- [`AGENTS.md`](AGENTS.md): required Codex repository workflow.
