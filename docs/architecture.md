# Architecture

Current implementation state lives only in [`../llm/status.md`](../llm/status.md).
The durable delivery sequence, gates, and rollback model are in
[`implementation-plan.md`](implementation-plan.md).

## Ownership model

```text
host queue instance
├── global GPU allowlist, reservations, scheduler lease, and host gate
├── registered Projects
│   ├── immutable key and active/paused/archived lifecycle
│   ├── immutable ProjectRevisions with frozen Enrollment
│   ├── logical mounts, artifact roots, and execution environments
│   └── project-scoped health circuit
└── queue items (global IDs)
    ├── immutable Project/revision/admission identity
    ├── mutable priority, hold, dependency, and operator policy
    ├── historical GPU/process assignment plus a distinct runtime GPU lease
    └── attempts, continuation segments, events, and artifacts
```

A portable Project manifest is committed in each scientific repository. Every
revision freezes a host Enrollment, but the ordinary trusted-project path
derives it automatically for `volumes: []`: no mounts and one checkout-local
`.venv/bin`.
Projects may opt into logical mounts and artifact roots when they want named
path injection or queue-observed artifacts. Registration and every later
revision resolve a full Git commit and authenticate exact Project, card,
extension-schema, and wrapper blobs from that tree. Runtime consumes the stored
snapshot; it does not reparse current working files.

Experiment identity and attempt uniqueness are Project-scoped. Queue item IDs,
GPU resources, reservation IDs, priority order, and the scheduler lease remain
host-global. Dispatch order is `priority DESC, resume_front DESC, id ASC`, while
candidate selection skips Projects whose lifecycle or health gate is closed.

## Application boundaries

The CLI and web handler perform input parsing, authentication, authorization,
and presentation. Typed services and repositories own lifecycle, admission,
queue, reservation, continuation, and termination transactions. The web layer
does not issue SQL or dereference stored paths, and direct Project/item routes
repeat ownership checks rather than treating a filter or URL label as
authority.

The foreground scheduler owns telemetry, dispatch, recovery, process-group
control, and reservation reconciliation. Each structured attempt receives a
project-qualified pinned Git worktree, a child environment built only from its
frozen Enrollment, and queue-owned control paths. The executor publishes an
authenticated launch receipt before scientific `Popen`, runs its own Python in
isolated import mode, and publishes immutable terminal evidence. Launch and
exit publication use fsynced staging plus a no-clobber final hard link; any
post-publication durability uncertainty preserves both names. A reader may
confirm a regular same-inode pair by fsyncing the parent before ingesting the
final and then clean the companion best-effort. Staging-only or changed/unsafe
companions fail closed and remain inspection evidence.
Worktree preparation enumerates the pinned tree before materialization,
rejects effective checkout filters, and then verifies every materialized path,
mode, and Git blob object ID directly; mutable clean/smudge configuration and a
misleading clean-index result cannot establish execution provenance. The
factory-only `ExecutionPlan` carries an integrity digest over argv, cwd,
environment, artifact authority, and revision identity, which the attempt
publisher revalidates before creating control evidence.
The scheduler validates the receipt, observes declared artifacts, and records
terminal state while retaining the runtime GPU lease. Under the host-wide GPU
lock it then requires fresh, exact idle telemetry for the assigned UUID, commits
the durable lease-release event, and only afterward removes a verified clean
queue-owned worktree/ref and closes the host lock. A restart explicitly resumes
terminal-but-held lease reconciliation. Scientific artifacts are never
queue-cleanup targets.

The schema-v4 application is a separate compatibility implementation with
separate entry points. V4 and v5 stores inspect and refuse the other version
before mutation. Conversion is an explicit offline import of a copy; startup
never migrates state.

## Execution and environment boundary

For ExperimentCard/v1, the queue starts a child environment empty, constructs
`PATH` from the admitted environment binding, copies only the intersection of portable and host-local
ambient allowlists, and injects service-owned values last. These include exact
Project/revision/Git/item/attempt identities, `CUDA_VISIBLE_DEVICES`, logical
mount paths as `EXPERIMENT_QUEUE_MOUNT_<NAME>`, declared output paths as
`EXPERIMENT_QUEUE_ARTIFACT_<NAME>`, and typed receipt/continuation paths when
applicable. Project input cannot inherit or override `PATH`,
`CUDA_VISIBLE_DEVICES`, or any `EXPERIMENT_QUEUE_*` name.

Grandfathered LegacyMarkdownCard/v0 is the explicit compatibility exception:
its scientific child inherits the scheduler ambient environment plus
queue-owned values. The service must therefore run with a minimal non-secret
ambient environment until imported v0 work is retired. In both paths the
executor interpreter ignores project import controls for loading queue code,
while the scientific child receives its exact admitted environment.

The queue service never imports project Python or other scientific
dependencies. A project owns commands, checkpoints, tracker integration,
scientific status, and result interpretation. Registering it authorizes its
committed code to run as the service account; this is a trust boundary, not a
sandbox.

## Control semantics

- Project pause and a project health circuit block only new dispatch for that
  Project. Host pause, database/lease failures, central-state disk failures, and
  GPU telemetry failures are global.
- Candidate inspection is advisory. The `BEGIN IMMEDIATE` claim transaction
  repeats host, Project, dependency, reservation, enabled/non-draining GPU
  allowlist, and no-other-held-runtime-lease predicates before assigning a GPU.
- Historical GPU assignment is provenance, not availability. Claim sets a
  separate durable runtime lease; terminal state alone does not clear it.
  Current finite, unique telemetry must prove the exact assigned GPU idle before
  a guarded release event, worktree cleanup, host-lock close, redispatch, or
  reservation activation. Valid busy telemetry retains and retries the lease;
  unavailable, missing, duplicate, malformed, or unlocked telemetry pauses the
  host and fails closed.
- GPU reservations are passive. An idle GPU becomes reserved immediately; a
  busy GPU records a pending reservation, never signals its item, blocks later
  dispatch, and activates only after the owning runtime lease is durably
  released. Its duration starts on activation.
- Priority changes never preempt. Typed preemption is an explicit persisted
  CooperativeYield/v1 request followed by authenticated signaling. A ready
  hashed receipt requeues the same item as the next segment at the same
  priority; ambiguous evidence retains the yielding runtime lease, while
  rejection after authenticated executor exit fails the item with that lease
  still held, and either case isolates only its Project.
  An imported item already admitted as preemptible may retain only its exact
  bounded CooperativeYield/v0 behavior; it never gains typed-v1 capability.
- Manual yield and graceful termination persist intent before signaling the
  authenticated executor leader. Durable senders are at-least-once; the
  executor coalesces each graceful signum and broadcasts it once to all current
  scientific process-group members. `SIGINT` shared by yield and termination
  therefore coalesces per segment. Graceful termination advances through one
  `SIGTERM` and then direct whole-group `SIGKILL` on durable deadlines; explicit
  force-kill starts at `SIGKILL`. Recovery replays committed stages without
  trusting a reused PID.

Host GPU locks are process-owned coordination, not a durable cross-version
lease keeper. A restarted v5 scheduler must reacquire the named GPU lock before
reconciling a live attempt or it pauses the host and quarantines the Project.
The database runtime lease remains held across scheduler death and independently
blocks dispatch/reservations; recovery must reacquire the lock and pass the
telemetry release barrier before clearing it.
Service-manager exclusion, including disabled v4 automatic restart, remains a
production invariant across the scheduler crash gap.

## Storage and path boundary

The explicit state directory contains the v5 database, instance identity,
lease, internal worktrees, service logs, and control receipts. It may not
overlap a checkout, declared mount, artifact root, or environment root. Version 1 also
rejects equality or ancestor/descendant overlap between Projects. Authorized
paths are canonicalized at revision creation and resolved again at use time so
traversal and changed symlink targets fail closed.

The selected state leaf is a service-account trust boundary. A freshly created
leaf is mode `0700`; an existing leaf must be owned by the service UID and must
not be group- or world-writable, although intentional group/world read and
execute bits are preserved. The database is always owner-only mode `0600`.
Every canonical ancestor through `/` must be owned by root or the service UID;
group/world-writable ancestors are accepted only with sticky semantics that
protect the root/service-owned child entry. Ancestor device/inode identities
are rechecked across critical publication and open operations.
Fresh initialization builds and validates a private sibling database, fsyncs
the database and its candidate directory entry, and publishes it atomically
without replacing a concurrent creator. The candidate name remains durable
until the final link is directory-fsynced; publication uncertainty preserves
both hard links for inspection.
Every open compares the complete application-owned `sqlite_schema` surface
(tables, indexes, triggers, and any views), plus the exact expected SQLite-owned
internal objects, with the immutable v5 DDL; metadata claiming the expected DDL
digest cannot conceal a missing, changed, or extra schema object.

Artifact declaration is optional. When used, terminal observation records declared name,
type, root, relative and absolute path, presence, and regular-file size without
hashing content. Cooperative checkpoint artifacts are separate protocol
evidence and are always hashed. Archival, migration, queue removal, and
worktree cleanup never delete scientific artifacts.

## Initial resource and authorization scope

Version 1 supports Linux and one independently schedulable NVIDIA GPU per job.
Gang scheduling, tightly coupled DDP preemption, non-NVIDIA accelerators,
distributed queue instances, and container sandboxing are out of scope.

The private web compatibility roles are host administrator, Project operator,
read-only viewer, and GPU reserver. Operator/viewer sessions may be restricted
to finite Project-key sets; reserver sessions receive no Project data. This is
sufficient for the current trusted single-host operation, not a general
multi-team delegation model.
