# Explicit Experiment Queue For Unmanaged GPUs

This document defines the operator workflow for the repository's durable GPU
queue. The queue is intentionally explicit: it never discovers experiments
from `STATUS.md`, scans cards for work, or interprets `prepared_locally` as
permission to launch.

David remains the sole `mutton2` operator. Codex prepares and verifies the
tool locally, supplies commands, and analyzes only user-synchronized state and
artifacts. Codex does not run, control, or monitor the remote queue.

## Safety Boundary

The host is shared and unmanaged. GPU polling reduces accidental overlap but
cannot reserve a device against another user. The scheduler records observed
contention and waits for clean idleness before reusing a GPU, but a foreign
process can still start after the final check.

Experiment cards remain device-neutral. David explicitly owns the mutable GPU
allowlist. The scheduler resolves his selected host indices to GPU UUIDs and
sets `CUDA_VISIBLE_DEVICES` only in the launched child's environment.

The ignored root directory `gpu_scheduler_state/` contains the SQLite state,
attempt launcher logs, exit receipts, scheduler identity, and exported queue
receipt. Experiment artifacts remain in their usual `outputs/experiments/`
directories and are not deleted or compacted by the queue.

## Queue Membership Is Explicit

Adding an experiment is the only admission path:

```bash
cd ~/3D_Helmholtz
.venv/bin/python scripts/run_experiment_queue.py add WCG-017 --priority 100
```

This command requires a clean checkout. It reads the explicitly named tracked
card, verifies that its `Exact Manual Command On Mutton2` section invokes
`scripts/run_experiment.py` with `--require-clean` and `--remote mutton2`, and
stores the exact command, card SHA-256, and Git commit. It does not read
`STATUS.md`.

Useful admission controls are:

```bash
# Add held rather than dispatchable.
.venv/bin/python scripts/run_experiment_queue.py add WCG-019 --hold

# Run only after queue item 1 succeeds.
.venv/bin/python scripts/run_experiment_queue.py add WCG-021 --after 1

# A prior launched attempt requires explicit authorization.
.venv/bin/python scripts/run_experiment_queue.py add WCG-017 --new-attempt
```

An experiment can have only one active queue item. Removing a never-launched
item and adding it again creates a new recorded membership. After any launched
attempt, `--new-attempt` is required. The queue never retries automatically.

## GPU Allowlist

David may replace, extend, or reduce the eligible GPU list at any time from a
separate shell. Indices, full UUIDs, unique UUID prefixes, and comma-separated
values are accepted:

```bash
.venv/bin/python scripts/run_experiment_queue.py gpus set 0 2 5
.venv/bin/python scripts/run_experiment_queue.py gpus add GPU-REPLACE_WITH_UUID
.venv/bin/python scripts/run_experiment_queue.py gpus remove 2
.venv/bin/python scripts/run_experiment_queue.py gpus show
```

Calling `gpus set` with no identifiers clears the allowlist; running queue jobs
drain normally.

Removing an idle GPU disables it immediately. Removing a GPU with a queue job
running changes it to `draining`: that job continues, no new job is assigned,
and the device leaves the allowlist after the attempt finishes. A GPU UUID is
the durable identity; host indices are refreshed when their mapping changes.

The default launch gate requires no reported compute process, at least 95%
free device memory, and at most 5% utilization. The scheduler polls every 60
seconds and queries the selected GPU again immediately before launch. It also
uses a same-user advisory lock. None of these mechanisms excludes foreign
users, so the race boundary remains explicit.

## Run The Scheduler

Run the foreground service from the clean checkout, normally inside a terminal
multiplexer selected by David:

```bash
cd ~/3D_Helmholtz
.venv/bin/python scripts/run_experiment_queue.py serve
```

Stopping the scheduler does not stop active experiments. Each attempt runs in
its own process group and writes a durable exit receipt. Starting `serve` again
reconciles those processes and receipts. Only one scheduler may use a state
directory at a time.

Do not update or dirty the shared checkout while queue jobs are running. The
scheduler checks the worktree and commit at launch and during GPU polling. If
it detects repository drift, it records the condition and pauses new dispatch;
it does not terminate active work.

## Operator Controls

These controls may be used while `serve` is running:

```bash
# Inspect all explicit membership and runtime state.
.venv/bin/python scripts/run_experiment_queue.py status

# Remove, hold, release, or reprioritize a pending item by exact queue ID.
.venv/bin/python scripts/run_experiment_queue.py remove 3 --reason "superseded operational order"
.venv/bin/python scripts/run_experiment_queue.py hold 4 --reason "awaiting operator review"
.venv/bin/python scripts/run_experiment_queue.py release 4
.venv/bin/python scripts/run_experiment_queue.py priority 4 200

# Pause or resume only new dispatch. Running jobs are unchanged.
.venv/bin/python scripts/run_experiment_queue.py pause --reason "maintenance window"
.venv/bin/python scripts/run_experiment_queue.py resume
```

`remove` applies only to pending or held work. A running item must be
terminated explicitly.

## Terminate A Running Attempt

Use the exact numeric queue item from `status`:

```bash
.venv/bin/python scripts/run_experiment_queue.py terminate 4 \
  --reason "operator stopped the trajectory"
```

The queue records the request and sends `SIGINT` to the complete attempt
process group. The experiment runner follows its interrupt cleanup path and
marks its manifest `interrupted`. After the default 30-second grace period,
the scheduler escalates an unresponsive process group to `SIGTERM` while still
allowing the runner to update its manifest.

Force-kill is a separate explicit operation and requires acknowledgement:

```bash
.venv/bin/python scripts/run_experiment_queue.py kill 4 \
  --reason "process group remained unresponsive" --yes
```

`SIGKILL` prevents graceful cleanup. The queue records `force_killed`, retains
the launcher log and any existing experiment artifacts, and will not reuse the
GPU until polling observes it idle.

## Failure And Recovery

- Repository, card-hash, disk-space, or GPU-telemetry failures pause dispatch.
- Independent work may continue after one failed child.
- Two consecutive failed children open the circuit breaker and pause dispatch.
- Dependencies dispatch only after every named queue item succeeds.
- User interruption and force-kill are recorded separately from child failure.
- No queue action deletes an experiment artifact.
- Removing membership never erases its event history.

After review, `resume` reopens dispatch. A blocked item can be released only
after its exact identity is again valid; when the required commit changed, the
clear workflow is to remove the old pending membership and explicitly add the
card again from the intended clean commit.

## Receipts And Synchronization

The queue captures the existing runner's final run directory, manifest path,
and generated pull command from each attempt launcher log. Print all available
artifact commands without executing them:

```bash
.venv/bin/python scripts/run_experiment_queue.py pull-commands
```

Export a consistent operational record:

```bash
.venv/bin/python scripts/run_experiment_queue.py receipt
```

The JSON export is written to `gpu_scheduler_state/queue_receipt.json` by
default. David synchronizes experiment outputs with the printed runner
commands and may separately synchronize the ignored `gpu_scheduler_state/`
directory when the queue history itself is needed locally. Codex analyzes only
the synchronized copies and records remote claims as user-reported until then.
