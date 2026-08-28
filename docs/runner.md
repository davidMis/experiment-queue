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
