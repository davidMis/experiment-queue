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
from card or ledger status. Use it only for workflows that implement the queue
yield contract. The frozen WCG wrapper and Flowers trainer do so; data
workflows may implement the same contract around a durable work-unit boundary.
Do not mark a tightly coupled multi-GPU or distributed-data-parallel job
preemptible unless its complete gang has a separately implemented coordinated
checkpoint-and-exit contract. The current queue preempts one single-GPU item or
one independently elastic worker at a time.

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
shows the latest 128 KiB of the runner's `stdout.log`, refreshed about every 10
seconds while the page remains open. Before the runner publishes its run
directory, the stdout panel falls back to the current segment's combined queue
launcher output so startup and setup failures remain visible. The underlying
runner still retains `stderr.log` where applicable, but job pages present only
stdout.

When the runner has recorded its pull command, the same page displays it with a
**Copy rsync command to clipboard** button. Copying is a browser-only action: the
web service never executes the command. Run pages and their live event streams
require the administrator role; the shared coworker credential cannot read
experiment output or synchronization commands.

The administrator queue table has browser-local view controls. Search matches
queue ID, experiment ID, state, priority, assigned GPU, commit, card path, and
state detail. State filtering supports all, active, finished, or one exact
state; GPU filtering supports one currently represented GPU or unassigned
items. Sorting defaults to queue-item ID in decreasing order, independent of
job priority, and also supports priority in either direction, newest/oldest,
experiment, state, and GPU. The visible-item count and no-match message update
immediately. These controls survive live table replacements for as long as the
page remains open, but they do not alter the database, priority, or scheduler
dispatch order. **Reset** returns to the unfiltered decreasing-ID order. Job
state badges use a consistent, theme-aware color palette while retaining their
text labels, including during live table and run-detail updates.

The coworker password cannot authorize queue admission, priority, manual
preemption, allowlist, dispatch, termination, or force-kill operations. The
required reservation note is the self-reported identity because coworkers
share one credential. Sessions
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
On the administrator dashboard, a paused dispatcher changes the complete page
background to the theme's red pause palette. The background responds to the
same live dispatch update as the status panel and returns to the ordinary theme
background immediately after dispatch resumes.

## Operator Controls

These controls may be used while `serve` is running:

```bash
# Inspect all explicit membership and runtime state.
.venv/bin/python scripts/run_experiment_queue.py status

# Remove, hold, release, or reprioritize an item by exact queue ID.
.venv/bin/python scripts/run_experiment_queue.py remove 3 --reason "superseded operational order"
.venv/bin/python scripts/run_experiment_queue.py hold 4 --reason "awaiting operator review"
.venv/bin/python scripts/run_experiment_queue.py release 4
.venv/bin/python scripts/run_experiment_queue.py priority 4 200

# Safely checkpoint and requeue one running preemptible item.
.venv/bin/python scripts/run_experiment_queue.py preempt 7 \
  --reason "make room for stakeholder overnight run"

# Pause or resume only new dispatch. Running jobs are unchanged.
.venv/bin/python scripts/run_experiment_queue.py pause --reason "maintenance window"
.venv/bin/python scripts/run_experiment_queue.py resume
```

`remove` applies only to pending or held work. A running item must be
preempted cooperatively or terminated explicitly. Priority may be changed for
pending, starting, running, or yielding work; it never preempts another item
automatically.

## Manually Preempt A Running Job

The CLI command above and the administrator dashboard's **Checkpoint &
requeue** action target one exact queue item. The item must be stably `running`
and must have been explicitly admitted with `--preemptible`. Manual preemption
uses no signal and creates no timed GPU reservation: it asks the workflow to
settle its current safe work unit, publish a complete continuation checkpoint,
and exit with the cooperative-yield status. Once the scheduler verifies the
checkpoint and observes process exit, the GPU is immediately eligible for
normal dispatch.

Dispatch order is `priority DESC, resume_front DESC, id ASC`. A yielded item
therefore returns to the front of its current priority band. Every higher-
priority dispatchable item runs first, while the continuation precedes newly
added work at the same priority. The queue never initiates preemption merely
because an item's priority changed or a higher-priority item arrived.

## Cooperative Yield And Resume Contract

Both manual preemption and the coworker reservation page use the same workflow-
facing checkpoint protocol. The coworker page can reserve an idle eligible GPU
or yield a running queue job that was explicitly admitted with
`--preemptible`. Reservations accept only whole-hour durations from 1 through
24 and require a note identifying who the window is for.

For a running job, the queue first creates a pending reservation so dispatch
cannot refill the device. The Flowers loop observes the durable request only
after completing an optimizer update, atomically serializes the complete Flax
`TrainState` (step, parameters, and optimizer state), records metadata and a
SHA-256 receipt, and exits with the dedicated cooperative-yield status. Its
training batch and dropout randomness are step-derived, so the recorded global
step restores their exact progression without an extra sampler cursor.

The scheduler verifies the checkpoint path, size, digest, queue/request
identity, metadata, and continuation `step` before it starts a pending
reservation clock or requeues any yielded item. Legacy training receipts still
require a positive optimizer step; generic progress receipts may use a
nonnegative cursor:

1. a reservation yield starts its clock only after the GPU-owning process has
   exited; and
2. every successful yield returns the same queue item to the front of its
   priority band with a new execution segment.

Both checkpoint and metadata paths and SHA-256 digests are stored in the queue
and revalidated immediately before every continuation launch. Missing, linked,
or changed continuation evidence holds only that item and lets unrelated queued
work continue. A termination or force-kill accepted while yield finalization is
in flight wins atomically; a stale yield receipt cannot requeue the killed job.

Non-training workflows may additionally report generic progress alongside the
existing receipt fields:

```json
{
  "step": 0,
  "progress": {
    "unit": "settled_rows",
    "completed": 0,
    "total": 30000
  }
}
```

`step` remains the nonnegative resume cursor stored by the queue. In
`progress`, `completed` is a nonnegative integer, optional `total` is an
integer no smaller than `completed`, and `unit` is a 1--32 character ASCII
token that starts with a letter and otherwise uses only letters, digits,
underscores, or hyphens. The scheduler records and displays
`completed[/total] unit` when this object is present, including a durable event
`progress_text`. Legacy receipts without it retain the existing `step N` state
and console wording and the existing numeric `step` event field.

A preemptible data workflow may checkpoint a manifest or other durable shared
state after settling one complete work unit, then publish that checkpoint and
its metadata through the same path, size, and SHA-256 receipt fields. If the
number of GPU workers can change, workers should claim units from that shared
durable state instead of relying on a fixed partition made at launch. The
checkpoint must make already settled work and the next safe claims
unambiguous, so a resumed or replacement worker neither skips nor duplicates a
unit.

`scripts/run_hno_specfem_pipeline.py --role consumer` implements this contract
at a safe SPECFEM GPU handoff. In legacy mode, a reservation request waits for
the in-flight row and cleanup receipt. In compact-production mode, it may exit
75 immediately after the immutable raw success terminal, while the CPU-only
assembler still owns compact commitment and cache cleanup. That receipt keeps
the prior `settled_rows` count—possibly zero—because the raw row is not yet
settled. A resumed segment validates the same pipeline/assembly plans and
destination, accepts progress peers made while it was absent, and never repeats
the solve. Under the strict rolling v5/v3 compact policy, that no-resolve barrier is
the immutable success terminal: a resumed worker still accepts it after the
CPU assembler has fully validated the sealed destination shard and retired the
raw NPZ through its authorization/completion chain. If that row completes the
entire plan's GPU work, the consumer exits normally instead of creating an
unnecessary continuation; CPU-only assembly and retirement continue
independently. An explicit queue `terminate` or `kill` is different: it interrupts
the process immediately and does not create an automatic continuation segment.
A handled terminate stops the registered solver child. A force-killed Python
owner cannot do that cleanup, and its separate-session SPECFEM child may remain
alive; released pipeline locks alone must never trigger deletion of that
attempt. The shared ledger can recover the row, but the operator must use the
recorded new-attempt workflow for the terminal queue item and verify child
termination before cleaning its partial work or admitting a replacement for
that worker.

For a reservation yield, the old GPU is excluded while the reservation is
pending or active, so the continuation may resume on another eligible idle
GPU. Manual preemption creates no exclusion; the scheduler assigns the released
device according to current queue priority. In either case, the experiment
runner reopens the original run directory, appends a segment to its manifest
and logs, and preserves the original pull command. The WCG wrapper reuses the
same training directory, restores the preemption checkpoint, retains earlier
best/milestone checkpoints, and resumes an enabled W&B run using its exact ID
with `resume=must`. A missing W&B identity blocks a W&B-enabled continuation
rather than silently creating another run.

If checkpoint creation fails, the workflow publishes a failure receipt and
continues running. A reservation fails and its timer never starts; a manually
preempted item returns to `running`. Neither path force-kills. David may still
use the distinct terminate or force-kill controls.

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
GPU until polling observes it idle. For SPECFEM, the external solver is in a
separate process session and may outlive the force-killed Python worker; verify
that child has ended before removing its preserved partial attempt.

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
