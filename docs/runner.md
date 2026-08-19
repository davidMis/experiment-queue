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

The current runner manifest is version 1. A continuation may append a new
segment only when run name, argv, PTY mode, working directory, and Git commit
match the yielded run. The multi-project milestone will add an atomic structured
runner receipt so the scheduler no longer discovers paths by scraping logs.
