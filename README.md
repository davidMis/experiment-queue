# experiment-queue

`experiment-queue` is a durable, operator-controlled queue for scientific GPU
experiments on a shared unmanaged host. It records exact Git and command
identity, assigns only operator-approved NVIDIA GPUs, preserves run receipts,
and supports explicit cooperative checkpoint-and-requeue preemption.

## Current status

This repository is the history-preserving standalone extraction of the queue,
runner, and private web application previously maintained inside
`flowers-3d-helmholtz`. The extracted executable is intentionally still the
legacy single-project baseline. Typed Project/ExperimentCard validation and
storage-neutral admission snapshot compilation are available as a library; the
executable does not persist them until database v5 lands. See
[`llm/status.md`](llm/status.md) for current implementation state and
[`docs/implementation-plan.md`](docs/implementation-plan.md) for the durable
multi-project roadmap.

Do not run the standalone scheduler concurrently with the legacy scheduler on
the same GPU host. David confirmed the Flowers SPECFEM generation and evidence
closeout are complete on 2026-08-27. Production cutover still requires the
multi-project implementation, an idle writer-free legacy queue, a consistent
backup and external-path inventory, and an offline copy-only, receipt-verified
state migration.

## Development setup

Python 3.14 or newer is required, matching the originating scientific project.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```

The production scheduler supports Linux, Git worktrees, POSIX process control,
SQLite, and NVIDIA GPUs visible to `nvidia-smi`. macOS is supported for local
development and tests, not GPU dispatch.

## Explicit state directory

Every stateful command requires an operator-selected absolute state directory.
Pass it directly or set it once in the service environment:

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/absolute/path/to/experiment-queue-state
experiment-queue --repo-root /path/to/scientific-project status
```

There is deliberately no current-directory fallback. Until the project-registry
milestone lands, `--repo-root` still selects the one legacy scientific checkout.

## Commands

- `experiment-queue`: queue admission, GPU controls, dispatch, priority, manual
  preemption, termination, receipts, and recovery;
- `experiment-queue-web`: private authenticated control surface;
- `run-experiment`: provenance, logs, manifests, continuation segments, and
  synchronization instructions for one command.

The wrappers under `scripts/` support checkout-based development and historical
command compatibility. New integrations should use the installed commands.

## Safety boundary

Registering a scientific project will authorize its committed jobs to execute
as the queue service account. The queue is an orchestration and provenance
system, not a sandbox. Read [`SECURITY.md`](SECURITY.md) before deployment.

The original detailed Flowers operating instructions are retained only as a
migration reference in
[`docs/legacy/flowers-operator-guide.md`](docs/legacy/flowers-operator-guide.md).

## Planning and Codex records

- [`llm/status.md`](llm/status.md): sole live implementation status, blockers,
  risks, ownership, and next actions;
- [`llm/todo.md`](llm/todo.md): dependency-ordered forward work with owners and
  completion criteria;
- [`llm/log.md`](llm/log.md): concise chronological session evidence;
- [`docs/implementation-plan.md`](docs/implementation-plan.md): durable phases,
  gates, verification, rollback, and risk controls;
- [`docs/adr/README.md`](docs/adr/README.md): accepted decision index;
- [`docs/protocol-compatibility.md`](docs/protocol-compatibility.md): independent
  protocol versions, owners, compatibility fallbacks, and fixtures;
- [`docs/authoring-and-admission.md`](docs/authoring-and-admission.md): typed
  Project/card validation, extension schemas, Submission, and snapshot evidence;
- [`AGENTS.md`](AGENTS.md): required Codex startup, maintenance, and closeout
  workflow for those records.
