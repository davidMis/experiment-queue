# experiment-queue

`experiment-queue` is a durable, operator-controlled queue for scientific GPU
experiments on a shared unmanaged host. It records exact Git and command
identity, assigns only operator-approved NVIDIA GPUs, preserves run receipts,
and supports explicit cooperative checkpoint-and-requeue preemption.

## Current status

This repository is the history-preserving standalone extraction of the queue,
runner, and private web application previously maintained inside
`flowers-3d-helmholtz`. The extracted baseline is intentionally still the
legacy single-project implementation. First-class projects and versioned
experiment cards are the next migration milestone; see
[`docs/architecture.md`](docs/architecture.md).

Do not run the standalone scheduler concurrently with the legacy scheduler on
the same GPU host. The production cutover will use an offline, rehearsed state
migration after active legacy work has drained.

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
