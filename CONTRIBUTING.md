# Contributing

Use Python 3.14 or newer and an isolated local environment:

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```

Keep generic orchestration in `src/experiment_queue/`. Scientific commands,
checkpoint formats, experiment policies, and project-specific integration tests
belong in their scientific repositories. New source and test files should begin
with a brief purpose docstring.

Changes to database schemas, runner manifests, receipts, state transitions,
process recovery, path authorization, or preemption require focused migration
or integration tests. Never use live operator state as a test fixture.
