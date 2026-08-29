# Experiment runner

`run-experiment` executes one child command while creating an immutable run
directory, manifest, copied configuration snapshots, Git provenance, streamed
logs, and an optional synchronization command.

```bash
run-experiment \
  --name smoke \
  --output-root outputs/experiments \
  --config config/example.yaml \
  --require-clean \
  -- \
  .venv/bin/python scripts/train.py --steps 10
```

The runner emits independently identified `RunnerManifest/v1` and
`RunnerReceipt/v1` documents. The receipt is atomically published in `running`
state before child launch and replaced with the terminal status, absolute run
and log paths, manifest path, segment identity, and optional typed sync
instruction. Queue-launched runs publish it at the scheduler-supplied control
path; standalone runs keep it beside the manifest.

A continuation may append a new segment only when its manifest protocol, run
name, argv, PTY mode, working directory, and Git commit match the yielded run.
Imported manifests carrying only the exact legacy integer `schema_version: 1`
remain readable. The scheduler uses the narrowly bounded legacy stdout footer
only when a structured receipt is absent; a present malformed receipt fails
closed.

## Queue-launched structured attempts

Database-v5 jobs execute directly from their pinned project worktree; they do
not need a project-local copy of the generic runner. The queue supplies the
atomic receipt path as `EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH` and verifies the
receipt against item, Project, revision, commit, attempt, segment, command, GPU,
and worktree evidence before accepting a terminal transition.

The queue executor sends graceful control to the complete attempt process
group. A queue-launched runner therefore receives the same `SIGINT` or
`SIGTERM` as its child. When the scheduler-supplied receipt path is present and
the child shares the runner's process group, runner cleanup only waits for the
already-signaled child; it does not add another signal or impose its standalone
ten-second escalation, and it preserves the child's actual outcome (including
cooperative-yield exit `75`) in the manifest and receipt. Scheduler-owned
`SIGTERM`/`SIGKILL` deadlines remain authoritative. A standalone runner
signaled only by PID retains its normal child terminate/kill cleanup and records
the runner interruption as `130`.

Portable commands should discover host-local inputs and outputs only through
logical names. A mount `training-data` is injected as
`EXPERIMENT_QUEUE_MOUNT_TRAINING_DATA`; a declared artifact `best-model` is
injected as `EXPERIMENT_QUEUE_ARTIFACT_BEST_MODEL`. Typed-preemptible attempts
also receive `EXPERIMENT_QUEUE_YIELD_REQUEST_PATH` and
`EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH`; resumed segments receive the exact prior
receipt at `EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH`. Projects cannot inherit
or override these service-owned names.

The queue observes general declared artifacts at terminal completion but does
not hash arbitrarily large scientific outputs. Cooperative checkpoint artifacts
are protocol evidence and are always hashed by CooperativeYield/v1. See the
[operator environment reference](operator-guide.md#child-environment-and-authorized-paths)
and [cooperative example](../examples/cooperative-preemption).
