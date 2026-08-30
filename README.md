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

David confirmed that Flowers SPECFEM generation and evidence closeout are
complete, no legacy jobs are running, and the legacy scheduler and web service
are stopped. The production deployment will start with a fresh schema-v5
database; it will not import the legacy database. Follow the
[project onboarding guide](docs/project-onboarding.md) and
[operator guide](docs/operator-guide.md). The old
[Flowers migration checklist](docs/migrations/flowers-v4.md) is retained only
as an inactive reference for the importer capability.

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
Pass it directly or set it in the service environment. The command examples
below assume the queue checkout's environment is active:

```bash
source /home/sdm11/experiment-queue/.venv/bin/activate
export EXPERIMENT_QUEUE_STATE_DIR=/home/sdm11/srv/experiment-queue/state
experiment-queue project list
```

Those two `/home/sdm11` paths are the current `mutton2` deployment layout;
other hosts may use different absolute paths.

There is no current-directory fallback for state. Outside the separate offline
importer, only explicit first-project registration may create fresh schema-v5
state; read, control, scheduler, and web commands require an existing exact-v5
database. A command may infer a
Project from the current directory only when that canonical path is inside
exactly one current registered checkout. Use `--project PROJECT-KEY` whenever
there could be ambiguity.

## Simple trusted-project setup

The queue is not a filesystem sandbox. A normal project does not declare
dataset, output, or scratch directories and does not need a hand-authored
Enrollment file. Initialize a minimal Project with `volumes: []`, keep the
project's existing `.venv` ignored by Git, commit the Project/card files, and
register it directly:

```bash
experiment-queue project init \
  --key my-project --display-name "My Project" --output Project.yaml

git add Project.yaml .gitignore experiments
git commit -m "Add experiment queue configuration"

experiment-queue project register "$PWD" \
  --git-commit "$(git rev-parse HEAD)" \
  --reason "initial registration" --actor "$USER"
```

With `volumes: []` and one declared environment, registration automatically
binds `$PWD/.venv/bin`. A virtual-environment root, bin directory, or Python
executable can be selected with `--environment-bin`. Explicit Enrollment,
logical volumes, and artifact declarations remain available for projects that
want those advanced provenance features.

The [project onboarding guide](docs/project-onboarding.md) walks through exact
revision registration, validation, submission, and dispatch. Runnable portable examples cover
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
provenance, not a sandbox. The scientific environment may be the project's
ordinary checkout-local `.venv`; keep only the queue service's own environment
separate. Use a dedicated non-root account, keep state private, and read
[`SECURITY.md`](SECURITY.md) before deployment.

Project manifests contain logical names only. Optional host paths live in immutable
Enrollment evidence, and child jobs receive declared paths through
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
