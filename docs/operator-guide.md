# Schema-v5 operator guide

This guide operates the project-aware schema-v5 service. The deprecated v4
rollback surface is named explicitly as `experiment-queue-legacy-v4`; never run
it concurrently with the v5 scheduler on the same GPU pool. Production Flowers
cutover uses the separate
[`migrations/flowers-v4.md`](migrations/flowers-v4.md) checklist.

## State and identity

Select one absolute state directory in the service environment:

```bash
export EXPERIMENT_QUEUE_STATE_DIR=/srv/experiment-queue/state
experiment-queue project list
```

There is no cwd-derived state. Outside the separate offline importer, only
`project register` may initialize absent v5 state. All other CLI and web
commands require an existing database whose exact schema is v5; they neither
initialize nor migrate it.

A newly initialized state leaf is created with mode `0700`, and its database
with mode `0600`. If the leaf already exists, it must be owned by the service
UID and must not be group- or world-writable; intentional read/execute sharing
is preserved. Every ancestor must be root/service-owned and non-writable by
other accounts, except a sticky shared ancestor such as an operator-controlled
temporary parent. Initialization first builds and fsyncs a private candidate
and its directory entry, then atomically publishes `queue.sqlite3` without
overwriting a concurrent creator. It removes the candidate hard link only after
the final link is directory-durable.
Each fresh database has a persisted immutable UUIDv4 instance identity, so a
different database placed at the same path is distinguishable in receipts and
exports.
Opening state also authenticates the exact application-owned SQLite tables,
indexes, and triggers; any missing, changed, or unexpected schema object is a
hard startup error, not an automatic repair opportunity.

Project selectors accept an immutable key or positive database ID. Omission is
allowed only when the canonical cwd lies inside exactly one current registered
checkout. Queue item IDs are global, but item reads and mutations repeat the
selected Project ownership check. Prefer an explicit `--project KEY` in
scripts.

## Project lifecycle

Registration binds a committed Project/v1 manifest to one complete host-local
Enrollment and a full Git commit:

```bash
experiment-queue project register /srv/projects/my-project \
  --manifest Project.yaml \
  --enrollment /srv/experiment-queue/enrollments/my-project.json \
  --git-commit FULL_COMMIT_OBJECT_ID \
  --reason "initial host enrollment" \
  --actor david
```

The resolver reads exact blobs from the commit tree and refuses mutable names,
working-tree-only content, unsupported Git modes, or incomplete Enrollment.
Use the [onboarding guide](project-onboarding.md) to create the portable files
and Enrollment.

Useful read-only checks are:

```bash
experiment-queue project show --project my-project
experiment-queue project doctor --project my-project
experiment-queue events --project my-project --after-id 0 --limit 100
```

Changing a checkout, mount, artifact root, environment binding, Project
manifest, or extension schema appends a revision. Already admitted items remain
pinned to their old revision.

```bash
experiment-queue project append-revision --project my-project \
  /srv/projects/my-project \
  --enrollment /srv/experiment-queue/enrollments/my-project.json \
  --git-commit FULL_NEW_COMMIT_OBJECT_ID \
  --actor david
```

`--no-activate` appends a later revision without selecting it. Activation may
move only forward:

```bash
experiment-queue project activate-revision --project my-project REVISION_ID \
  --actor david
```

Lifecycle and runtime health are separate. Pause blocks only new dispatch for
the Project and leaves running work alone. A project-scoped runtime failure may
open its health circuit; after correcting the cause, close that circuit
explicitly with `repair`. Neither operation changes other Projects.

```bash
experiment-queue project pause --project my-project \
  --reason "maintenance" --actor david
experiment-queue project repair --project my-project \
  --reason "mount restored and verified" --actor david
experiment-queue project resume --project my-project \
  --reason "maintenance complete" --actor david
```

Archive is permanent in version 1. It requires a paused Project, no queued,
held, blocked, or active items, and complete ref/worktree cleanup. It preserves
all database history and scientific artifacts.

```bash
experiment-queue project archive --project my-project \
  --reason "project retired" --actor david
```

## Admission and queue inspection

Always inspect the exact resolved execution before allocating an item ID:

```bash
experiment-queue submit --project my-project \
  --card-path cards/EXP-001.yaml --job-id train \
  --operator david --priority 20 --dry-run --json

experiment-queue submit --project my-project \
  --card-path cards/EXP-001.yaml --job-id train \
  --operator david --priority 20
```

Use repeated `--dependency GLOBAL_ITEM_ID`, `--hold-reason TEXT`, whole-value
`--bindings-json OBJECT`, and `--authorize-preemption` only when required.
Priority is host-global and never triggers preemption.

```bash
experiment-queue status --project my-project --limit 100
experiment-queue item show ITEM_ID --project my-project
experiment-queue artifact --project my-project --item-id ITEM_ID
experiment-queue receipt --project my-project --actor operator:YOUR_NAME --json
```

The receipt command emits exact canonical `QueueExport/v1` JSON containing
package and Database/v5 instance provenance plus persisted
Project/revision/admission/event/artifact and typed continuation evidence. Event
actors and failure scopes remain explicit. The envelope records that exact
ExecutorReceipt bytes are unavailable; it does not reconstruct evidence that
Database/v5 did not retain.

Pending-item controls preserve history and never delete scientific artifacts:

```bash
experiment-queue item hold ITEM_ID --project my-project \
  --reason "awaiting input" --actor david
experiment-queue item release ITEM_ID --project my-project --actor david
experiment-queue item priority ITEM_ID 50 --project my-project --actor david
experiment-queue item remove ITEM_ID --project my-project \
  --reason "superseded" --actor david
```

## GPU and scheduler control

GPU allowlist identity is the full UUID resolved from live `nvidia-smi`
telemetry:

```bash
experiment-queue gpu add 0 --actor david
experiment-queue gpu show
experiment-queue gpu drain GPU-UUID --actor david
experiment-queue gpu undrain GPU-UUID --actor david
```

Enable/disable and drain/undrain affect only future dispatch; they never signal
a running item. Host pause is likewise a dispatch gate:

```bash
experiment-queue host pause --reason "host maintenance" --actor david
experiment-queue host resume --actor david
```

Run the scheduler in the foreground under the host service manager:

```bash
experiment-queue serve
```

The service polls telemetry, reconciles reservations and recovered processes,
dispatches eligible items, validates terminal receipts and required artifacts,
and cleans only authenticated clean queue worktrees. `--once` performs one
reconciliation/recovery-only iteration and never dispatches queued work. It is
safe for a controlled receipt/recovery check, but still must not be pointed at
a live production GPU allowlist during development.

### Terminal state and GPU reuse

`assigned_gpu_uuid` and `assigned_gpu_index` are historical run evidence; they
remain populated after terminal completion and do not mean the GPU is still or
no longer owned. The separate `runtime_gpu_lease_held` field is the resource
barrier. The scheduler records valid terminal evidence first, then—while
holding the host GPU lock—requires fresh telemetry with exactly one record for
that UUID, finite valid metrics, no compute PIDs, and the configured idle
thresholds. Only the resulting `GPU_RUNTIME_LEASE_RELEASED` transaction permits
worktree cleanup, host-lock close, redispatch, or reservation activation.

A valid busy observation leaves the terminal item lease-held for automatic
retry and does not create a new host pause. Missing, duplicate, malformed, or
unavailable telemetry, or loss of the host GPU lock, retains the lease and
pauses host dispatch. The item detail and queue API display both historical GPU
assignment and runtime lease/release time; a terminal-but-held item is not a
reusable GPU. Restore trustworthy telemetry and run the normal service (or a
controlled `--once` recovery pass); do not edit the lease fields manually.

## Passive GPU reservations

A reservation never preempts, terminates, or changes a queue item. Requesting
an idle enabled/undrained GPU activates the reservation immediately. Requesting
a GPU with a held runtime lease creates a pending reservation tied to the item
observed at request time; it blocks later dispatch and activates only after
telemetry-authenticated durable lease release—not merely after the item becomes
terminal. The 1–24 hour duration starts on activation, and scheduler
reconciliation expires it.

```bash
experiment-queue reservation request GPU-UUID \
  --duration-hours 4 --note "interactive analysis" --actor david
experiment-queue reservation list --open-only
experiment-queue reservation release RESERVATION_ID --actor david
```

Release is explicit for pending or active reservations. Completed reservation
rows and host-scoped events remain history.

## Manual cooperative preemption

Preemption is available only to an active ExperimentCard/v1 item whose job
declared CooperativeYield/v1 and whose Submission used
`--authorize-preemption`:

```bash
experiment-queue item preempt ITEM_ID --project my-project \
  --note "release one GPU for higher-priority work" --actor david
```

For a typed admission, the service commits the v1 request before publishing its
control file. Every signal sender first appends a request-bound claim event and
then an outcome event. A running or restarted scheduler retries a missing or
failed outcome only after its bounded claim lease. This sender protocol is
deliberately at-least-once across the unavoidable signal-call crash window;
the executor coalesces retries to at most one scientific-group `SIGINT`
broadcast per segment. A valid ready receipt hashes every
admitted checkpoint artifact and requeues the same item as segment N+1 at the
same priority, ahead of newer equal-priority work. A failed, missing, corrupt,
stale, regressed, or identity-mismatched receipt holds the item and opens only
that Project's health circuit.

If a client dies after request publication but before its signal outcome, leave
the scheduler running (or restart it) so reconciliation can republish exact
missing control bytes and retry after the lease. Inspect the signal claim/result
events. A false or unauditable delivery keeps the live assignment intact and
quarantines the Project; repair it only after resolving the process state.

An imported LegacyMarkdownCard/v0 item does not gain typed-v1 capability, but
an item already recorded as preemptible retains the exact grandfathered v0
request/receipt behavior. The same command selects the adapter from immutable
admission kind. V5 persists before publication, authenticates the recorded
process group, validates the historical runner/checkpoint/progress fields, and
uses a compare-and-set requeue; it does not broaden the legacy parser or admit
new v0 work.

## Termination and force kill

Graceful termination is distinct from cooperative preemption: it ends the item
without a checkpoint-and-requeue promise.

```bash
experiment-queue item terminate ITEM_ID --project my-project \
  --reason "operator cancellation" --actor david
```

The transaction persists intent first, then sends `SIGINT` to the authenticated
executor leader. The executor is the sole graceful-signal broadcaster,
including the launch window before its scientific child exists, and reaches
every existing member of the scientific process group. Durable senders may
retry, but one executor broadcasts each graceful signum at most once. Manual
yield and termination both use `SIGINT`, so they coalesce within one segment;
termination of an already-yielding attempt advances through its durable grace
timer to the single `SIGTERM` stage rather than promising a second `SIGINT`.
`SIGKILL` targets the whole group directly. Process-start evidence prevents
signaling a reused PID; an identity without a stable start token fails closed.

Use force-kill only when immediate `SIGKILL` is intended; the literal
confirmation is deliberate:

```bash
experiment-queue item force-kill ITEM_ID --project my-project \
  --reason "runaway process" --actor david --confirm FORCE-KILL
```

Requests and recovery are at-least-once for a persisted stage because no
process can atomically commit a database outcome and deliver a POSIX signal.
Executor coalescing makes the scientific graceful-signal effect at-most-once
per signum and segment. A scheduler or CLI crash after commit does not lose the
request; a recovery pass replays the recorded stage and finalizes the terminal
receipt/race transactionally.

### Resolve an abandoned active attempt

Recovery deliberately pauses host dispatch and retains the GPU assignment when
either of these crash windows has no terminal executor receipt:

- a `starting` row has no persisted PID, process group, or start token; or
- a `running`, `yielding`, `terminating`, or `force_killing` row retains a
  persisted executor identity whose leader is no longer authenticated.

Do not release or redispatch the GPU based only on a missing PID. Stop every
queue writer, keep host dispatch paused, and externally verify that the exact
recorded process group is absent. Then run the guarded, Project-qualified
resolution with the exact GPU UUID from the item row; the command independently
obtains current GPU telemetry and requires that assigned UUID to be idle:

```bash
experiment-queue item resolve-abandoned-launch ITEM_ID --project my-project \
  --gpu-uuid GPU-UUID --reason "verified executor group and GPU are idle" \
  --actor david --confirm RESOLVE-ABANDONED-LAUNCH
```

The command refuses an active authenticated executor, any extant database- or
sidecar-named process group, an existing exit receipt, incomplete persisted
identity, changed state/GPU evidence, an unpaused host, or a competing queue/GPU
lock. It also refuses unavailable, missing, duplicate, malformed, or busy GPU
telemetry while retaining the assignment, runtime lease, host lock, pause, and
worktree. An absent or rejected `launch.json` is recorded in the audit result but
does not permanently wedge a row after the stronger database-process-group and
current-telemetry checks succeed. Resolution fails the item with either
`ABANDONED_LAUNCH_RESOLVED` or `DEAD_PROCESS_RESOLVED`, preserves process and
scientific evidence, commits `GPU_RUNTIME_LEASE_RELEASED`, and only then
attempts authenticated queue-owned worktree cleanup and host-lock close. A
cleanup refusal is retained and reported rather than forced.

The same fail-closed state is used when launch or launch-recording fails and a
whole-group `SIGKILL` plus process-group absence cannot both be proven. The
starting claim, GPU lease, worktree, and control evidence remain in place under
a host pause for this guarded resolution; no leader-only cleanup fallback is
used.

Launch and exit receipts use durable staging plus an immutable no-clobber final
hard link. On recovery, a same-inode regular staging/final pair is confirmed by
fsyncing the parent directory before the final is trusted; companion cleanup is
then best-effort. Staging without a final, or any symlink, unreadable,
non-regular, or different-inode companion, is preserved and rejected for
operator inspection rather than promoted or overwritten.

The host remains paused and the Project remains quarantined. Inspect the event,
artifacts, and cleanup result; then repair the Project and resume the host only
as separate explicit decisions:

```bash
experiment-queue project repair --project my-project \
  --reason "abandoned attempt reviewed" --actor david
experiment-queue host resume --actor david
```

## Child environment and authorized paths

An ExperimentCard/v1 child begins with an empty environment. Its frozen binding
constructs `PATH`, copies only explicitly allowed ambient names, and then
injects queue-owned values. A portable mount named `training-data` becomes
`EXPERIMENT_QUEUE_MOUNT_TRAINING_DATA`. A declared artifact named
`best-model` becomes `EXPERIMENT_QUEUE_ARTIFACT_BEST_MODEL` and contains its
exact authorized output path.

Imported LegacyMarkdownCard/v0 execution is the deliberate compatibility
exception: it inherits the scheduler service's ambient environment, plus the
frozen legacy `core.excludesFile` binding and queue-owned values. Until all
grandfathered legacy work is retired, start the scheduler with a minimal
non-secret environment; never place credentials or unrelated service secrets
in ambient variables available to that process.

The durable executor itself starts Python in isolated mode, so the project
working directory and `PYTHONPATH`, `PYTHONHOME`, and related interpreter
controls cannot replace queue control code. Those variables are not silently
removed from the admitted scientific environment: the scientific child still
receives the exact typed binding or grandfathered legacy environment.

Every structured attempt also receives:

- `EXPERIMENT_QUEUE_ITEM_ID`, `EXPERIMENT_QUEUE_PROJECT_ID`,
  `EXPERIMENT_QUEUE_PROJECT_KEY`, `EXPERIMENT_QUEUE_PROJECT_REVISION_ID`,
  and `EXPERIMENT_QUEUE_PROJECT_REVISION`;
- `EXPERIMENT_QUEUE_GIT_COMMIT`, `EXPERIMENT_QUEUE_EXPERIMENT_ID`,
  `EXPERIMENT_QUEUE_ATTEMPT`, and `EXPERIMENT_QUEUE_SEGMENT`;
- `EXPERIMENT_QUEUE_GPU_UUID`, `EXPERIMENT_QUEUE_WORKTREE`,
  `EXPERIMENT_QUEUE_PRIMARY_REPO`, and
  `EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH`;
- `EXPERIMENT_QUEUE_YIELD_REQUEST_PATH` and
  `EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH` when cooperative preemption is admitted;
  and
- `EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH` on a resumed typed segment.

`CUDA_VISIBLE_DEVICES` contains the assigned GPU identity. Projects cannot
inherit or override any of these names. Paths are resolved again at use time;
changed symlinks, traversal, overlap, and artifact escapes fail closed.

## Private web application

Initialize owner-only credentials interactively only while the web service is
stopped. The command replaces `web_auth.json` and prompts for four distinct
passwords of at least 12 characters; restart the web service afterward:

```bash
experiment-queue-web auth-setup \
  --operator-project my-project --viewer-project my-project
```

The server loads authentication state once at startup. Replacing the file does
not change a running process's in-memory credentials or sessions; the required
restart activates the new file and invalidates sessions created under the old
configuration.

Roles are:

- host administrator: every Project plus host and reservation mutation;
- Project operator: read/mutate only its signed Project scope and read host
  status;
- viewer: read-only within its signed Project scope;
- reserver: GPU name/index availability and reservation actions, with no
  Project visibility.

Within their signed scope, host administrators and Project operators can apply
Project pause/resume/archive/repair and eligible item hold/release/priority/
remove/terminate/force-kill controls. Cooperative preemption remains an
explicit CLI operation so its admitted capability and note are reviewed at the
operator boundary.

Serve with TLS on a private interface:

```bash
experiment-queue-web serve --host 0.0.0.0 --port 8443 \
  --tls-cert /etc/experiment-queue/tls.crt \
  --tls-key /etc/experiment-queue/tls.key
```

`--insecure-http` is accepted only on loopback for local testing. The web app
uses signed finite sessions, CSRF tokens, bounded forms/queries, server-side
Project authorization, and ID-cursor pagination. Cross-Project direct routes
return a generic not-found response rather than disclosing existence.

## Recovery and failure scope

Restart the same v5 service against the same state directory. Recovery uses
persisted item/segment/GPU/PID/process-start/worktree/control evidence; it does
not infer from a mutable checkout or untrusted PID. A detached live attempt is
observed, a terminal receipt is ingested, and persisted preemption or
termination intent is resumed.

Repository, card, mount, artifact-root, required-artifact, continuation, and
child failures quarantine only the owning Project. Database, scheduler lease,
GPU telemetry, central-state disk, or process-control integrity failures pause
the host. Inspect `project show`, `status`, and `events`; correct the cause;
then use `project repair`, `project resume`, or `host resume` as appropriate.
Never edit queue rows or receipts in place to clear a failure.

Host-wide GPU `flock` files prevent ordinary concurrent v5 ownership only while
their service process is alive. They do not continuously bridge a scheduler
crash and do not coordinate with deprecated v4. The service pauses and
quarantines if it cannot reacquire the lock for a recovered live attempt, but
the service manager remains the authoritative single-writer boundary: disable
legacy automatic restart and run exactly one selected scheduler.
