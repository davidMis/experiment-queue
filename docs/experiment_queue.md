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
per-segment launcher logs, yield requests and receipts, detached code
worktrees, web password hashes and session-signing secret, scheduler identity,
and exported queue receipt. Experiment artifacts remain in their usual
`outputs/experiments/` directories and are not deleted or compacted by the
queue.

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
`STATUS.md`. Admission also creates a private
`refs/experiment-queue/items/<queue-id>` Git ref immediately, so rebasing,
deleting a branch, or advancing the primary checkout cannot make the admitted
commit unreachable while it is waiting.

Useful admission controls are:

```bash
# Add held rather than dispatchable.
.venv/bin/python scripts/run_experiment_queue.py add WCG-019 --hold

# Run only after queue item 1 succeeds.
.venv/bin/python scripts/run_experiment_queue.py add WCG-021 --after 1

# A prior launched attempt requires explicit authorization.
.venv/bin/python scripts/run_experiment_queue.py add WCG-017 --new-attempt

# Allow this admitted workflow to checkpoint, yield, and resume.
.venv/bin/python scripts/run_experiment_queue.py add WCG-023 --preemptible
```

`--preemptible` is an explicit operational promise, not a property inferred
from card or ledger status. Use it only for workflows whose trainer implements
the queue yield contract. The frozen WCG wrapper and Flowers trainer do so.

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

At first dispatch, the scheduler materializes a detached worktree for the
item's pinned commit under `gpu_scheduler_state/worktrees/`. It verifies that
checkout's HEAD, cleanliness, and card bytes, and runs the frozen card command
from it. The standard `cd ~/3D_Helmholtz` card line is redirected into the
isolated worktree only for execution; the original command remains unchanged
in the database for audit. Ignored environment, data, output, and run roots are
linked back to the primary repository. A scheduler-only Git excludes file
keeps those runtime links from invalidating `--require-clean`.

This isolates tracked code, not the whole machine. The linked `.venv`, `data/`,
and artifact roots remain shared operational inputs. Updating tracked files in
the primary checkout is safe, but replacing the environment or mutating source
data during an active job is not part of the guarantee; keep those stable and
use the runner manifest's recorded environment/data provenance.

The primary checkout may therefore be committed, pulled, switched, or left
temporarily dirty while an already-running scheduler owns queued or active
isolated items. Each item continues using only its admitted commit. A yielded
item retains the same worktree and pinned ref across every segment so its
runner manifest, checkpoint, optimizer state, and W&B identity can be resumed
without changing code.

To update the scheduler itself, stop `serve`, update the primary checkout to a
clean commit, and start it again. Active experiment executors and their
worktrees continue while the scheduler is stopped; the restarted service
reconciles them from durable PIDs and exit receipts. The private web process is
separate and can be restarted independently when its code changes.

After an item becomes `succeeded`, `failed`, `interrupted`, `force_killed`, or
`removed`, the scheduler removes only that exact detached worktree and its
private pin. Shared data, checkpoints, launcher logs, queue history, runner
artifacts, and W&B records remain. Cleanup failures do not change the terminal
result: they are shown in CLI/web status and retried by later scheduler cycles.

## Run The Private HTTPS Web App

The scheduler and web app are separate processes over the same SQLite state.
The web process records authenticated requests; only the scheduler launches or
reconciles jobs. Configure two passwords interactively: one administrator
password for David and one shared reservation password for coworkers.

```bash
cd ~/3D_Helmholtz
.venv/bin/python scripts/run_experiment_queue_web.py auth-setup
```

Only scrypt password hashes, random salts, an authentication version, and a
session-signing secret are written to
`gpu_scheduler_state/web_auth.json`. The file is owner-only and ignored by
Git. Re-running setup replaces both passwords, rotates the signing secret, and
invalidates existing browser sessions.

Serve on the private network with the certificate and key for its HTTPS
hostname. The key should remain outside Git with owner-only permissions.

```bash
cd ~/3D_Helmholtz
.venv/bin/python scripts/run_experiment_queue_web.py serve \
  --host 0.0.0.0 \
  --port 8443 \
  --tls-cert /absolute/private/path/mutton2.crt \
  --tls-key /absolute/private/path/mutton2.key
```

The two entry points are:

- `https://<private-mutton2-host>:8443/reserve`: restricted coworker GPU
  reservation and early-release page; and
- `https://<private-mutton2-host>:8443/admin`: David's complete queue, GPU
  pool, dispatch, termination, force-kill, reservation, and audit dashboard.

Every experiment name in the administrator queue links to its own
`/admin/runs/<queue-item-id>` page. The page shows the frozen queue identity,
state, timing, paths, dependencies, and item-specific audit history. It also
shows the latest 128 KiB of the runner's `stdout.log` and `stderr.log`, refreshed
about every 10 seconds while the page remains open. Before the runner publishes
its run directory, the stdout panel falls back to the current segment's combined
queue launcher output so startup and setup failures remain visible. The normal
runner default uses a pseudo-terminal and therefore merges child stderr into
`stdout.log`; in that mode `stderr.log` contains the runner's explanatory note.
Runs launched with `--no-pty` retain separate streams.

When the runner has recorded its pull command, the same page displays it with a
**Copy rsync command to clipboard** button. Copying is a browser-only action: the
web service never executes the command. Run pages and their live event streams
require the administrator role; the shared coworker credential cannot read
experiment output or synchronization commands.

The coworker password cannot authorize queue admission, priority, allowlist,
dispatch, termination, or force-kill operations. The required reservation note
is the self-reported identity because coworkers share one credential. Sessions
are signed, secure, HTTP-only, same-site cookies; every mutation also requires
a session-specific CSRF token. Login failures are rate limited. HTTPS is
required except for an explicit loopback-only local-test flag.

Both dashboards stay current without periodically reloading the page. Each
authenticated browser opens a server-sent event stream for its authorized
view. The web service checks the durable queue event sequence twice per second
and pushes changed sections immediately; it also refreshes GPU telemetry every
10 seconds and sends only when the rendered status changed. The browser
reconnects automatically after a brief network interruption, reports its live
connection state, and defers replacement of a section while someone is typing
in one of its controls. The stream is read-only, role-checked on the server,
uses the signed session cookie, and expires with that session.

Every page also offers light and dark appearance modes. The choice is a local
browser preference rather than scheduler state, so coworkers can choose
independently without creating accounts or changing the shared database.

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

## Yield And Resume A Running Job

The coworker page can reserve an idle eligible GPU or yield a running queue job
that was explicitly admitted with `--preemptible`. Reservations accept only
whole-hour durations from 1 through 24 and require a note identifying who the
window is for.

For a running job, the queue first creates a pending reservation so dispatch
cannot refill the device. The Flowers loop observes the durable request only
after completing an optimizer update, atomically serializes the complete Flax
`TrainState` (step, parameters, and optimizer state), records metadata and a
SHA-256 receipt, and exits with the dedicated cooperative-yield status. Its
training batch and dropout randomness are step-derived, so the recorded global
step restores their exact progression without an extra sampler cursor.

The scheduler verifies the checkpoint path, size, digest, queue/request
identity, metadata, and optimizer step before it does either of the following:

1. starts the reservation clock after the GPU-owning process has exited; and
2. returns the same queue item to the front with a new execution segment.

The yielded GPU is excluded while the reservation is pending or active. The
front item may therefore resume on another eligible idle GPU. The experiment
runner reopens the original run directory, appends a segment to its manifest
and logs, and preserves the original pull command. The WCG wrapper reuses the
same training directory, restores the preemption checkpoint, retains earlier
best/milestone checkpoints, and resumes an enabled W&B run using its exact ID
with `resume=must`. A missing W&B identity blocks a W&B-enabled continuation
rather than silently creating another run.

If checkpoint creation fails, the trainer publishes a failure receipt and
continues running. The reservation fails and its timer never starts. The
coworker action never force-kills. David may still use the distinct terminate
or force-kill controls.

An active reservation expires at its exact 1--24 hour deadline or can be
released early. Expiry removes only the temporary exclusion: it does not
reinsert a GPU that David removed from the permanent pool. An expired device
becomes scheduling-eligible at the next polling pass only if it remains in the
allowlist and ordinary telemetry still reports it idle. This preserves the
accepted unmanaged-host race boundary.

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
- A terminal item's exact detached code worktree and private Git ref are
  reclaimed; failed cleanup is visible and retried without broad deletion.

After review, `resume` reopens dispatch. A blocked item can be released only
after its pinned worktree identity is again valid; when the intended commit
changed, the clear workflow is to remove the old pending membership and
explicitly add the card again from the intended clean commit. Updating a branch
does not migrate existing membership to that branch's new commit.

Scheduler versions before the 2026-08-04 connection-lifecycle fix could
eventually stop with `sqlite3.OperationalError: unable to open database file`
after leaking short-lived SQLite handles. Exiting that process releases the
handles; the durable database does not need to be removed or recreated. Update
the checkout and restart `serve` as the same user. If the corrected scheduler
reports a database-path error, preserve `gpu_scheduler_state/` and verify that
the directory still exists and is writable rather than deleting queue state.

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
