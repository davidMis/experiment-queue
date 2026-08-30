# Project onboarding

The normal workflow is deliberately small: one committed Project manifest,
one or more committed experiment cards, and the project's existing `.venv`.
The queue runs trusted project code as its service account; it does not require
a filesystem allowlist and it is not a sandbox.

The commands below assume the queue checkout's `.venv` is active, so the
`experiment-queue` executable is on `PATH`.

## 1. Create a minimal Project

From the scientific repository:

```bash
experiment-queue project init \
  --key my-project --display-name "My Project" --output Project.yaml

mkdir -p experiments
experiment-queue card new \
  --project-manifest Project.yaml \
  --experiment-id EXP-001 --title "First run" \
  --job-id run --output experiments/EXP-001.yaml
```

The generated Project uses the trusted-project defaults:

```yaml
apiVersion: experiment-queue/v1
kind: Project
metadata:
  key: my-project
  displayName: "My Project"
spec:
  cardRoots:
    - experiments
  volumes: []
  environments:
    - name: python
  environmentPolicy:
    inherit: none
    allowVariables: []
  supportedProtocols: []
```

`volumes: []` does not restrict what the process can read or modify. Ordinary
Unix permissions remain the boundary. Volumes are optional named-path and
artifact-provenance features.

The minimal environment policy intentionally inherits no ambient variables.
If a job needs names such as `HOME`, `LANG`, `WANDB_API_KEY`, or a library-path
variable, set `inherit: allowlist` and list only those names in
`allowVariables` before registration.

Jobs run from a queue-owned pinned worktree. A relative path such as
`outputs/result.json` is relative to that worktree, not the primary checkout,
and an untracked result can prevent conservative cleanup. Existing programs
should write to their normal absolute output location or derive the primary
checkout from `EXPERIMENT_QUEUE_PRIMARY_REPO`.

Edit the generated card's command to run committed project code. Artifacts are
optional:

```yaml
apiVersion: experiment-queue/v1
kind: ExperimentCard
metadata:
  projectKey: my-project
  experimentId: EXP-001
  title: "First run"
spec:
  parameters: {}
  jobs:
    - id: run
      environment: python
      command:
        type: argv
        argv: [python, experiments/run.py]
      resources:
        gpus: 1
```

Queue-owned variables and `CUDA_VISIBLE_DEVICES` remain reserved.

## 2. Use the project's existing environment

Keep the ordinary project virtual environment in `.venv` and commit its ignore
rule:

```bash
printf '/.venv/\n' >> .gitignore
python3.14 -m venv .venv
```

Install scientific dependencies into that environment as the project normally
does. The queue service has its own `.venv`; it does not import the project's
packages.

Registration automatically uses `<checkout>/.venv/bin`. A committed
`.gitignore` rule is enough; the CLI derives and verifies the checkout-local
evidence without asking the operator to author it.

## 3. Validate and commit

```bash
experiment-queue project validate --manifest Project.yaml
experiment-queue card validate \
  --project-manifest Project.yaml \
  --card experiments/EXP-001.yaml

git add Project.yaml .gitignore experiments/EXP-001.yaml experiments/run.py
git commit -m "Add experiment queue configuration"
```

Registration and submission use exact blobs from the full commit. Uncommitted
changes are intentionally not admitted.

## 4. Register without an Enrollment file

Choose a state directory outside the source checkout, then register the exact
commit:

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/home/sdm11/srv/experiment-queue/state

experiment-queue project register "$PWD" \
  --git-commit "$(git rev-parse HEAD)" \
  --reason "initial registration" \
  --actor "$USER"

experiment-queue project doctor --project my-project
```

If the environment is not `.venv`, select a venv root, bin directory, or
Python executable:

```bash
experiment-queue project register "$PWD" \
  --environment-bin ./another-venv \
  --git-commit "$(git rev-parse HEAD)" \
  --reason "initial registration" \
  --actor "$USER"
```

Only first-Project registration may initialize an absent schema-v5 database.
Later configuration changes use `project append-revision` with the same
automatic-enrollment behavior.

## 5. Submit and run

```bash
experiment-queue submit --project my-project \
  --card-path experiments/EXP-001.yaml \
  --job-id run --operator "$USER"

experiment-queue gpu add 0 --actor "$USER"
experiment-queue serve
```

Inspect from another shell:

```bash
experiment-queue status --project my-project
experiment-queue events --project my-project
```

Jobs execute from a queue-owned pinned worktree. Existing project programs may
continue writing to their normal absolute output paths without declaring them.
If a program needs the primary checkout path, use the injected
`EXPERIMENT_QUEUE_PRIMARY_REPO`. Avoid leaving undeclared relative outputs in
the temporary worktree because they prevent conservative automatic cleanup.

## Advanced Enrollment and artifacts

Use an explicit `--enrollment FILE` only when a project deliberately needs one
of these features:

- multiple named execution environments;
- `EXPERIMENT_QUEUE_MOUNT_<NAME>` path injection;
- queue-observed `EXPERIMENT_QUEUE_ARTIFACT_<NAME>` outputs;
- typed checkpoint artifacts for cooperative preemption; or
- host-specific access narrowing.

The examples for ordinary artifacts, data pipelines, training, and cooperative
preemption demonstrate that advanced contract. An explicit Enrollment remains
immutable revision evidence, but it is no longer a prerequisite for an
ordinary trusted project.

The `examples/flowers-compatibility` directory is advanced local test material,
not the production Flowers setup template.
