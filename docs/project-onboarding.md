# Project onboarding

A trusted scientific repository needs a portable Project/v1 manifest, one or
more ExperimentCard/v1 files, executable committed code, and one host-local
Enrollment. It does not need a scheduler plugin or imports into the queue
service.

This walkthrough uses the
[`ordinary` example](../examples/ordinary). The
[`python-training`](../examples/python-training),
[`data-pipeline`](../examples/data-pipeline), and
[`cooperative-preemption`](../examples/cooperative-preemption) examples show
additional mounts and typed checkpointing. None depends on Flowers, W&B,
`mutton2`, or a GPU for authoring validation.

## 1. Create portable tracked files

Scaffold a new manifest and card:

```bash
experiment-queue project init \
  --key my-project --display-name "My Project" --output Project.yaml

mkdir -p experiments
experiment-queue card new \
  --project-manifest Project.yaml \
  --experiment-id EXP-001 --title "First run" \
  --job-id run --output experiments/EXP-001.yaml
```

Edit the Project to declare logical volumes, environments, supported protocols,
and optional extension-schema identity. Edit the card to select one declared
environment, a direct argv command or committed wrapper, resources, and
declared artifacts. Project-specific data belongs only under
`extensions.<project-key>`.

For a complete starting point, copy the ordinary example into its own
repository:

```bash
cp -R /path/to/experiment-queue/examples/ordinary /srv/projects/ordinary-example
git -C /srv/projects/ordinary-example init
git -C /srv/projects/ordinary-example add .
git -C /srv/projects/ordinary-example commit -m "Add experiment-queue integration"
```

Every admitted source must be committed. Registration and submission read exact
blobs from a full commit; uncommitted working-tree edits are intentionally
ignored.

## 2. Validate without state

```bash
experiment-queue project validate \
  --manifest /srv/projects/ordinary-example/Project.yaml
experiment-queue project explain \
  --manifest /srv/projects/ordinary-example/Project.yaml --json
experiment-queue card validate \
  --project-manifest /srv/projects/ordinary-example/Project.yaml \
  --card /srv/projects/ordinary-example/cards/ORD-001.yaml
experiment-queue card explain \
  --project-manifest /srv/projects/ordinary-example/Project.yaml \
  --card /srv/projects/ordinary-example/cards/ORD-001.yaml --json
```

When `spec.extensionSchema` is declared, add
`--extension-schema /path/to/schema.json` to local validation. Registration
and admission later resolve that schema from the pinned Git tree.

Export authenticated bundled schemas for an editor or project CI:

```bash
mkdir -p .schemas
experiment-queue schema export project \
  --output .schemas/experiment-queue-project-v1.schema.json
experiment-queue schema export card \
  --output .schemas/experiment-queue-card-v1.schema.json
```

These commands do not create or open queue state.

## 3. Create disjoint host roots

Keep state, checkout, mutable data/artifacts, and the project environment in
canonical non-overlapping directories:

```bash
mkdir -p /srv/experiment-queue/state
mkdir -p /srv/experiment-queue/artifacts/ordinary-example
python3.14 -m venv /srv/experiment-queue/environments/ordinary-example
```

Version 1 rejects equality or ancestor/descendant overlap among logical roots,
between Projects, and with queue state. A checkout-local environment or output
directory is accepted only when registration independently proves that the
directory is ignored by committed `.gitignore` rules at the exact pinned commit
and that the commit contains no tracked entry beneath it. Operator-authored
Enrollment path strings, the mutable worktree/index, global excludes, and
`.git/info/exclude` are not proof. Prefer ordinary bindings outside the checkout
unless a checkout-local tool or output directory is genuinely required.

The portable Project names the `artifacts` volume and `python` environment.
Create its strict Enrollment/v1 with the installed library so the normalized
Project digest and constrained artifact-root view are derived rather than
hand-written:

```bash
python - \
  /srv/projects/ordinary-example \
  /srv/experiment-queue/state \
  /srv/experiment-queue/artifacts/ordinary-example \
  /srv/experiment-queue/environments/ordinary-example/bin \
  /srv/experiment-queue/enrollments/ordinary-example.json <<'PY'
from pathlib import Path
import sys

from experiment_queue.authoring import Project
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
)

checkout, state, artifacts, environment_bin = map(
    lambda value: Path(value).resolve(strict=True), sys.argv[1:5]
)
output = Path(sys.argv[5]).resolve()
project = Project.from_yaml(
    (checkout / "Project.yaml").read_bytes(), source_name="Project.yaml"
)
enrollment = Enrollment.create(
    project=project,
    checkout_directory=checkout,
    project_manifest_path="Project.yaml",
    mounts=(
        MountBinding.create(
            name="artifacts", path=artifacts, access="readWrite"
        ),
    ),
    environments=(
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=(environment_bin,),
        ),
    ),
    state_directory=state,
)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_bytes(enrollment.canonical_json)
PY
```

Every required Project volume and every environment must be bound exactly once.
Enrollment may narrow but cannot widen read/write access or ambient-variable
policy. It stores variable names, never values or credentials.

## 4. Register an exact revision

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/srv/experiment-queue/state
git -C /srv/projects/ordinary-example rev-parse HEAD
```

Copy the printed full object ID into the explicit registration command:

```bash
experiment-queue project register /srv/projects/ordinary-example \
  --manifest Project.yaml \
  --enrollment /srv/experiment-queue/enrollments/ordinary-example.json \
  --git-commit FULL_COMMIT_OBJECT_ID \
  --reason "initial ordinary-example enrollment" \
  --actor david

experiment-queue project doctor --project ordinary-example
```

Only explicit registration may initialize a new v5 database. The resolver
authenticates the commit, Project blob, optional extension-schema blob,
Enrollment, paths, and cross-Project separation. Later configuration changes
use `project append-revision`; they never edit this evidence in place.

## 5. Dry-run and submit

```bash
experiment-queue submit --project ordinary-example \
  --card-path cards/ORD-001.yaml --job-id run \
  --operator david --dry-run --json

experiment-queue submit --project ordinary-example \
  --card-path cards/ORD-001.yaml --job-id run \
  --operator david
```

The dry run performs exact Git/blob, schema, binding, environment, resource,
artifact, and preemption resolution without allocating a queue-item ID. Each
card job is submitted independently. Repeated `--dependency ITEM_ID`,
`--priority VALUE`, `--hold-reason TEXT`, and a strict whole-value
`--bindings-json OBJECT` are mutable Submission policy.

A job can be manually preempted only if the Project/card declare
CooperativeYield/v1 and submission adds `--authorize-preemption`. Start with
the cooperative example and its
[`worker.py`](../examples/cooperative-preemption/worker.py) rather than
inventing a checkpoint format.

## 6. Run and inspect

On a Linux/NVIDIA production host, add an observed GPU and start the one
scheduler:

```bash
experiment-queue gpu add 0 --actor david
experiment-queue serve
```

In another shell:

```bash
experiment-queue status --project ordinary-example
experiment-queue item show ITEM_ID --project ordinary-example
experiment-queue artifact --project ordinary-example --item-id ITEM_ID
experiment-queue events --project ordinary-example
```

The ordinary program learns no host path. The queue injects its declared result
as `EXPERIMENT_QUEUE_ARTIFACT_RESULT`. Logical volume names use
`EXPERIMENT_QUEUE_MOUNT_<UPPER_NAME>`; artifact names use
`EXPERIMENT_QUEUE_ARTIFACT_<UPPER_NAME>`; hyphens become underscores. See the
[operator guide](operator-guide.md#child-environment-and-authorized-paths) for
the complete identity/control environment.

## Conformance checklist

Before considering an integration complete:

1. local Project/card validate and explain commands pass from committed bytes;
2. editor schema export and GPU-free project CI pass;
3. Enrollment roots are existing, canonical, least-privilege, and disjoint;
4. registration and `project doctor` authenticate the intended full commit;
5. `submit --dry-run --json` explains the intended argv, cwd, bindings,
   artifacts, revision, commit, and preemption policy;
6. one real item produces required artifacts and a terminal structured receipt;
7. if cooperative preemption is declared, ready, failed, corrupt, stale, and
   resumed paths pass the project-owned conformance tests.

The [Flowers compatibility fixture](../examples/flowers-compatibility) follows
the same portable contract for local migration validation. It was created
without inspecting live Flowers state or `mutton2`; classification of real
current/future/historical Flowers cards requires the operator-supplied offline
inventory in [the migration guide](migrations/flowers-v4.md).
