# Status

This is the sole mutable source of the repository's current implementation
phase, blockers, ownership, active risks, and next authorized actions. Durable
architecture belongs in `docs/`, accepted decisions in `docs/adr/`,
forward-looking work in `llm/todo.md`, and chronological evidence in
`llm/log.md`.

Last updated: 2026-08-30 by Codex from David's decisions and local repository
evidence.

## Current Phase

Release `v0.2.1` is published from Linux-CI-passing commit
`2eb6028be4e2bd2851d14beccf5c086718615432`. It contains the simplified
trusted-project workflow selected for the fresh `mutton2` startup. Published
tag `v0.2.0` predates that workflow and must not be used for this deployment.

David confirmed that no legacy queue jobs are running, the legacy scheduler and
web service are stopped, and the legacy database does not need to be imported.
The deployment will initialize a fresh schema-v5 database through first-Project
registration. No inventory, database copy, importer, migration receipt, or
rehearsal is part of this deployment.

## Selected Simple Workflow

- The queue is orchestration for trusted code, not a filesystem sandbox.
- A normal `Project.yaml` declares `volumes: []`; jobs retain every host access
  available to the service account without declaring datasets, outputs, or
  scratch paths.
- `project register` and `project append-revision` create Enrollment/v1
  evidence automatically when the Project declares one environment. No
  Enrollment file is needed.
- Automatic enrollment uses the scientific checkout's existing `.venv/bin`.
  The `.venv` stays inside the project root and must be ignored by a committed
  `.gitignore` rule. A venv root, bin directory, or executable override remains
  available through `--environment-bin`.
- Explicit Enrollment, logical mounts, declared artifacts, multiple
  environments, and cooperative checkpointing remain optional advanced
  features.
- Exact committed Project/card inputs and pinned queue worktrees remain. This
  retains useful execution identity without promising perfect experiment
  reproducibility.

## Deployment Layout And Ownership

- Queue source clone: `/home/sdm11/experiment-queue`.
- Mutable service data: `/home/sdm11/srv/experiment-queue`.
- Flowers scientific checkout: `/home/sdm11/3D_Helmholtz`.
- The queue service has its own repository-local `.venv`; Flowers keeps its
  scientific `.venv` inside the Flowers checkout.
- David is the sole operator of `mutton2` and owns remote service control, GPU
  selection, credentials, and scientific Project/card content. Codex does not
  connect to or inspect that host.
- This repository owns the generic queue implementation, release, and public
  documentation. The Flowers repository owns scientific commands and results.

## Verification State

- Remote annotated tag `v0.2.1` resolves through tag object
  `071cd8adad5157a89b5332c1fcb8292f14fc776b` to release commit
  `2eb6028be4e2bd2851d14beccf5c086718615432`.
- GitHub Actions run `33317278646` completed successfully on Linux/Python 3.14:
  clean installation, the complete suite, wheel build/verification, and all
  installed command-help checks passed.
- The simplified code has focused CLI/operator coverage, including automatic
  registration, submission, revision append, whole-venv Git-ignore proof,
  uv-style symlinked Python normalization, parse-time option exclusivity, and
  rejection of volume-bearing Projects in automatic mode.
- Focused operator/example verification passes: `48 passed`.
- The complete Python 3.14.4 suite passes: `1011 passed, 1 skipped, 32 subtests
  passed` in `182.82 s`.
- The candidate wheel passes the complete verifier and has SHA-256
  `8b613662895b2bee5e87b4c16bd02d9eef25513d9da8bdbb8ccdf281babd89f1`.
  This is verification evidence; the deployment remains an editable clone.
- Python compilation, `git diff --check`, and all `80` local Markdown targets
  pass. The six wiki drafts have no unresolved wiki links.
- The GitHub repository and simplified source are public. Its wiki feature is
  enabled but has not yet been initialized. Six concise pages are drafted; the
  available browser is waiting for David to complete GitHub sign-in.

## Active Risks

- Registered code receives the queue account's ordinary filesystem, process,
  network, and credential access. Unix account permissions are the containment
  boundary.
- The minimal environment policy inherits no ambient variables. Flowers must
  declare any needed variable names with `inherit: allowlist` and
  `allowVariables` before registration.
- Jobs run in pinned queue worktrees. Relative output paths land there, not in
  the primary checkout, and untracked results can block conservative cleanup.
  Existing absolute output paths or `EXPERIMENT_QUEUE_PRIMARY_REPO` avoid that
  surprise.
- Only one scheduler may control the queue and GPU pool. Ambiguous process or
  GPU state deliberately pauses rather than guessing.
- Linux/NVIDIA is the production platform; macOS verification cannot exercise
  production GPU dispatch.

## Blocking And Next Authorized Actions

1. After David completes GitHub sign-in in the opened browser panel, initialize
   and publish the six approved wiki pages.
2. Update the `mutton2` clone to `v0.2.1`, create the
   queue clone's editable `.venv`, and prepare a minimal committed Flowers
   Project/card plus its existing project-local `.venv`.
3. Register into fresh state under `/home/sdm11/srv/experiment-queue`, run
   `project doctor`, add the intended GPU, configure web credentials, and start
   exactly one scheduler and web service.
4. Submit one simple typed card and inspect its queue, event, attempt, and
   terminal evidence. Do not run the legacy importer.

## Explicitly Out Of Scope

- Scientific experiment status or result interpretation.
- Automatic priority-triggered preemption.
- Gang scheduling or coordinated multi-GPU/DDP checkpointing.
- Non-NVIDIA accelerators, distributed queue instances, or container isolation.
- General workflow DAG/matrix behavior or named multi-team principals.
