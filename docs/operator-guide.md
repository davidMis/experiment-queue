# Standalone baseline operator guide

The current standalone baseline preserves the legacy single-project queue while
the project registry is implemented. Use a clean scientific checkout and an
explicit absolute state directory:

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/srv/experiment-queue/state
experiment-queue --repo-root /srv/projects/my-project status
```

The queue never discovers work automatically. Legacy admission still expects a
tracked Markdown card using the originating command contract:

```bash
experiment-queue --repo-root /srv/projects/my-project add EXP-001 --priority 20
experiment-queue --repo-root /srv/projects/my-project gpus set 0
experiment-queue --repo-root /srv/projects/my-project serve
```

Manual preemption is always explicit:

```bash
experiment-queue --repo-root /srv/projects/my-project preempt ITEM_ID \
  --reason "release one GPU for stakeholder work"
```

Only jobs admitted with `--preemptible` and implementing the cooperative yield
protocol can be preempted. A yielded item retains its priority and returns ahead
of newer items at the same priority; no priority change triggers preemption.

The comprehensive pre-extraction Flowers guide remains under `docs/legacy/`
for migration reference and should not be treated as a generic integration
contract.
