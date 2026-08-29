# ADR 0009: Project lifecycle, Enrollment, and revision ownership

Status: accepted, 2026-08-28.

## Context

ADR 0008 fixes the portable Project/v1 and ExperimentCard/v1 authoring and
admission contracts. Database v5 now needs a distinct host-local ownership
model. It must preserve immutable execution evidence across checkout changes,
make lifecycle and failure scope unambiguous, and authorize paths and child
environments without placing host paths or secrets in portable documents.

This decision applies to fresh schema-v5 state and to the explicit offline
legacy importer. It does not authorize an in-place or startup migration.

## Decision

### Registered Project identity

A registered Project has a positive database ID and the immutable key defined
by ADR 0001. The display name is presentation data initialized from the active
portable manifest and may change when a new revision is activated. Each
revision retains the exact display name and Project source that it validated.

Every registered Project has one current ProjectRevision, including when it is
paused or archived. Internal Project and ProjectRevision IDs are positive
integers. Revisions also have a positive, gap-tolerant per-project sequence and
the stable operator-facing label `<project-key>:r<sequence>`.

### Frozen Enrollment and append-only ProjectRevision

Enrollment is host-local configuration frozen inside a ProjectRevision. It
contains:

- one canonical absolute checkout directory;
- one portable repository-relative Project-manifest path;
- bindings for declared logical volumes;
- bindings for every declared execution environment; and
- the exact host-resolution JSON and digest used to create the revision.

There is no trusted in-place Enrollment mutation. Registration, refresh, or
checkout repointing resolves a full Git object ID, reads the Project source and
optional extension schema through Git plumbing from that exact tree, validates
the portable Project and all host bindings, appends a revision, and atomically
sets it current. A branch, tag, or working-tree path may select the object but
is never retained as execution identity.

A revision owns its Project source path, exact bytes and hash, canonical JSON
and hash, bundled schema identity and hash, full Git object ID, frozen
Enrollment evidence, creation actor/time, mounts, artifact-root view, and
environment bindings. Revision rows and child binding rows are append-only.
Reverting to earlier content creates a new sequence rather than reactivating or
editing an old row.

Queue items carry `(project_id, project_revision_id)` with a composite foreign
key proving revision ownership. Runtime uses only the item's revision and
stored admission evidence, never the Project's later current revision or live
Enrollment input. Repointing cannot remove an old checkout, ref, binding, or
worktree needed by a nonterminal item; detachment waits until all such items are
terminal and cleanup evidence is complete.

### Volume, artifact, and path authorization

Every required Project volume and every declared environment must be bound.
An optional volume may be absent, but a job that references it cannot be
admitted. Binding access may narrow but never widen the portable declaration.
Writable volume bindings are the source of truth for artifact roots; an
artifact-root table or model is a constrained read-write subset, never a
second independently configurable path.

At revision creation, checkout, volume, environment-search, and artifact roots
must exist as canonical absolute directories. Symlinks are resolved before
comparison and the canonical paths are stored. Use-time authorization resolves
again and rejects traversal or a changed symlink target.

The queue state directory may not equal, contain, or be contained by a
checkout, volume, artifact, or environment root. Scheduler-owned worktrees
beneath state are the only state-containment exception. Version 1 rejects all
cross-project root equality or ancestor/descendant overlap, including apparent
read-only sharing; a future named host-owned shared-volume contract may relax
that rule explicitly. Logical roots within one revision may not overlap.

A volume or environment directory may be a descendant of its own checkout only
when the binding names that path explicitly, the path never contains the
checkout, and Git at the pinned commit proves the descendant is ignored. This
narrow exception supports checkout-local `.venv`, data, and output directories
without treating arbitrary checkout contents as mounted or mutable roots.

### Environment ownership and secrets

The portable Project owns logical environment names and the maximum ambient
inheritance allowlist. Frozen host Environment bindings may only narrow it.
EnvironmentBinding/v1 records:

- the logical environment name;
- canonical executable-search directories;
- an optional structured command-prefix argv whose executable is absolute; and
- the subset of portable allowlisted ambient variable names to inherit.

The child environment starts empty. The queue constructs `PATH` from the frozen
search directories, copies only the intersection of the Project allowlist and
the Enrollment subset, and injects `CUDA_VISIBLE_DEVICES` and all
`EXPERIMENT_QUEUE_*` values last. Queue-owned names can never be inherited or
overridden. Runtime does not consult a current service PATH or Enrollment.

EnvironmentBinding/v1 stores no literal variable values, credentials, or
secret material. Projects needing secrets may use already-authorized ambient
variables named by both allowlists. A future secret-reference mechanism must
be opaque, versioned, and separately accepted before the queue stores or
injects secret values.

### Lifecycle and failure scope

Project lifecycle is:

```text
active <-> paused -> archived
```

Registration creates a complete first revision and starts active unless the
operator explicitly requests paused. Pausing blocks only new dispatch for that
Project. Running work continues; history, admission, and revision creation
remain available; other Projects continue dispatching. Resume changes only an
operator pause.

Archival is permanent in version 1 and is allowed only from paused. It requires
no queue item in `queued`, `held`, `blocked`, `starting`, `running`, `yielding`,
`terminating`, or `force_killing`, and no incomplete ref/worktree cleanup.
Archival blocks admission, revision creation, activation, and dispatch. It
preserves the current revision, every row/event/dependency, and every
scientific artifact. There is no hard-delete or unarchive operation.

Operator lifecycle metadata records reason, actor, and time. Project health is
separate runtime state with a nonnegative circuit-failure count, health reason,
actor, and time. An open Project health circuit also blocks only that Project's
new dispatch. Host database, lease, GPU telemetry, and central-state failures
remain host-global and never masquerade as Project lifecycle.

### Cross-project dependencies

Dependencies continue to use globally unique queue-item IDs and may cross
Projects. Admission may link only to already-existing IDs, keeping ordinary
construction acyclic. A dependency already terminal in a non-success state is
rejected rather than creating permanently blocked work. A dependency that
later fails or is removed holds its dependent item and emits Project-scoped
evidence; it does not pause either Project or the host. A succeeded dependency
is satisfied. Archival preserves incoming dependency targets because history is
never deleted.

Foreign keys use `RESTRICT`/no cascade. Presentation filters are not authority;
a future multi-team authorization model must check access to both Projects.

### Legacy importer boundary

The offline importer may create explicitly marked `legacy-v4` revisions and
`LegacyMarkdownCard/v0` admissions when old rows cannot supply Project/v1 or
ADR-0008 evidence. It must never fabricate typed source or schema evidence.
Legacy revisions still bind the explicit imported Project, full recorded Git
commit where present, frozen host paths, original card path/hash/command, and
all historical runtime data. Structured and legacy admission shapes are
distinguished by database checks and runtime dispatch.

An imported Project remains paused until a valid portable revision is created
and the operator resumes it. Migration preserves old path/ref/worktree values
as evidence; any new project-qualified ref or worktree is created separately
and never deletes or reuses rollback-source state.

## Consequences

Configuration drift, checkout repointing, and Project failure cannot silently
change an admitted job or another Project. The model intentionally duplicates
some immutable evidence across revisions and admissions so verification does
not depend on mutable rows or files.

Version 1 is conservative about path sharing and secret injection. Operators
must make every cross-project root distinct and use explicit ambient-variable
allowlists; broader shared datasets or secret stores require new contracts.

The existing schema-v4 QueueStore remains a compatibility implementation. It
must inspect and reject schema v5 before WAL or DDL changes. Fresh v5 creation
and v1-v4 import are separate explicit code paths.
