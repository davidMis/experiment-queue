# Remote Experiment Workflow

This document owns the reusable local-to-remote workflow. Current run state
lives only in `../STATUS.md`; exact phase commands belong in immutable run
cards or phase runbooks.

## Define The Experiment Before Preparing It

Before assigning an ID or preparing a command, establish with David:

1. purpose or scientific question;
2. hypothesis, baseline, and intended controlled change;
3. inputs, split, seed, and test-seal implications;
4. expected evidence or success criterion;
5. owner/operator and requested state transition.

If any field is missing, ambiguous, or contradictory, ask David before
recording the experiment. Do not infer purpose from a filename, command, note,
or an earlier plan.

Create a phase-scoped ID and immutable run card under `docs/experiments/`.
Record mutable state, runner identity, blockers, and next actions only in
`../STATUS.md`.

## Intended Loop

1. Develop and verify locally in the repository `.venv`.
2. Commit and push the exact code intended for the run.
3. Prepare one copy-paste command through `scripts/run_experiment.py`.
4. David checks out the intended commit on `mutton2` and launches the command.
5. David reports the launch or completion receipt; Codex records it as
   user-reported with a timestamp.
6. David runs the runner-generated `pull outputs with:` command locally.
7. Codex inspects only synchronized local artifacts, validates provenance, and
   records the result and decision.

## Mutton2 Manual-Execution Boundary

David is the sole operator of `mutton2`. Codex must not connect to, inspect,
launch on, monitor, or synchronize with that machine. Codex supplies commands
and analyzes returned artifacts only.

Every command must:

- start from `~/3D_Helmholtz`;
- use the remote checkout's `.venv`;
- state relevant environment variables, config, seed, and inputs;
- use `scripts/run_experiment.py`;
- include `--remote mutton2`;
- use `--require-clean` when the run must match an exact commit;
- provide useful progress for long child workflows;
- identify expected artifacts and the synchronization procedure.

GPU selection is deliberately outside the command contract. David manages it
before launch, so new commands and templates must not set
`CUDA_VISIBLE_DEVICES` or use an equivalent device-selection option. The
actual device remains a required receipt field.

For explicitly queued work, David may instead manage the mutable runtime GPU
allowlist through `scripts/run_experiment_queue.py gpus`. This is operator
state, not part of any run card. The queue sets child visibility after David's
selection, and every card remains device-neutral. See `experiment_queue.md`
for admission, polling, draining, termination, recovery, and receipt rules.

Unless David explicitly specifies another target, GPU-accelerated runs use
the NVIDIA RTX PRO 6000 Blackwell Server Edition accelerator class. This
default hardware identity is an experiment assumption, not a device selector;
the actual device is still recorded from the run receipt and artifact.

## Command Pattern

```bash
cd ~/3D_Helmholtz

XLA_PYTHON_CLIENT_PREALLOCATE=false \
.venv/bin/python scripts/run_experiment.py \
  --name EXPERIMENT_NAME \
  --config PATH_TO_IMMUTABLE_CONFIG \
  --require-clean \
  --remote mutton2 \
  --local-output-root /Users/david/Projects/flowers-3d-helmholtz/outputs/experiments \
  --notes "User-confirmed purpose and controlled change." \
  -- .venv/bin/python scripts/CHILD_WORKFLOW.py \
    --seed 0
```

Include relevant non-selection environment variables and the seed when they
apply. Never include a GPU device selector. An intentionally dirty run must
explicitly state why exact commit provenance is being waived.

## Run Directory And Receipt

The runner writes under:

```text
outputs/experiments/<UTC>_<name>_<short-commit>/
```

Expected generic contents are `manifest.json`, `stdout.log`, `stderr.log`,
copied configs, and child artifacts written under `EXPERIMENT_OUTPUT_DIR`.
Dirty runs also include Git status and diff snapshots.

David's launch/completion handoff should use:

```text
Experiment ID:
Run ID:
Commit:
Device:
W&B ID or URL:
Startup assertions:
Last event and timestamp:
Synced local path:
Anything unusual:
```

The runner prints an exact `pull outputs with:` command when `--remote
mutton2` is supplied. David runs that command from the local workstation.
Codex must not run it.

## Local Artifact Closeout

Synchronization does not authorize cleanup. Keep the complete local run until
its provenance is verified and its scientific decision is recorded. Later
compaction follows `artifact_retention.md` and the exact registry/receipt
workflow there. Active, ambiguous, or dependency-held runs remain protected,
and root `data/` is never part of experiment-output cleanup.

## Optional Explicit Queue

`scripts/run_experiment_queue.py` can wait for operator-selected GPUs on the
shared unmanaged host and launch exact card commands through the normal
runner. Queue admission is never inferred: David explicitly adds every
experiment ID, and a ready card or `prepared_locally` ledger state does not
create queue membership. The ignored `gpu_scheduler_state/` directory owns the
live SQLite journal, web authentication hashes, reservations, continuation
segments, and receipts. The separate private HTTPS interface in
`scripts/run_experiment_queue_web.py` exposes an administrator dashboard and a
restricted coworker reservation page over that same state. `STATUS.md` remains
the scientific ledger and is updated only from David's reports and synchronized
artifacts.

## Archived Phase Runbooks

- Cross-Flowers exact commands and historical statuses:
  `phases/cross_flowers/runbook.md`
- Cross-Flowers phase index and evidence:
  `phases/cross_flowers/README.md`
