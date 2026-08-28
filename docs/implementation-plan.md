# Implementation Plan

This document owns the durable roadmap, dependencies, deliverables, verification
strategy, and exit gates for `experiment-queue`. It is not a progress tracker.
Current state belongs in `../llm/status.md`; granular forward actions belong in
`../llm/todo.md`.

## Product Goal

Provide one independently maintained, dependency-light service that can safely
schedule reproducible jobs from multiple trusted scientific Git repositories on
an unmanaged Linux/NVIDIA host. The service must preserve exact provenance,
manual cooperative preemption, recovery, operator control, and historical queue
evidence without imposing Flowers-specific paths, cards, trackers, or scientific
policies on other projects.

## Non-Negotiable Invariants

1. Every admitted job belongs to one stable Project and one immutable
   ProjectRevision.
2. Queue item IDs remain globally unique operational control targets;
   experiment/job identities and attempts are scoped to their project.
3. Admission snapshots every configuration needed to reproduce execution.
   Runtime never reparses a later mutable Project manifest or ExperimentCard.
4. GPU allowlists, reservations, priority, and the scheduler lease are
   host-global. Repository/configuration/child failures are project-scoped.
5. Dispatch remains `priority DESC, resume_front DESC, id ASC`.
6. Priority changes never trigger preemption. Preemption is a separate explicit
   operator action using a declared, admitted, versioned cooperative protocol.
7. Project paths, mounts, artifacts, logs, and checkpoints are resolved and
   authorized server-side for the admitted revision.
8. The queue owns orchestration and provenance. Projects own scientific code,
   domain checkpoints, experiment status, and result interpretation.
9. Project registration grants code-execution authority to committed project
   code. The queue is not a sandbox.
10. Migrations operate offline on copies, preserve history, produce receipts,
    and roll back through untouched old state rather than downgrade.
11. Project archival, migration, worktree cleanup, and queue cleanup never
    delete scientific artifacts.
12. The first production release supports one independently schedulable NVIDIA
    GPU per job. Tightly coupled multi-GPU work is not preemptible.

## Durable Entity And Lifecycle Model

```text
QueueInstance
├── global GPU allowlist, reservations, scheduler lease, and host events
├── Project (stable immutable key, mutable display name/lifecycle)
│   ├── Enrollment (host-local checkout and environment bindings)
│   ├── ProjectRevision (immutable portable + host-resolved configuration)
│   ├── ExperimentCard (immutable scientific intent and explicit job specs)
│   └── ProjectRuntimeState (health, pause, scoped circuit state)
└── Submission (mutable admission and scheduling policy)
    └── Attempt
        └── Segment (continuation/preemption history and artifacts)
```

Project lifecycle is `active`, `paused`, or `archived`. Pausing prevents new
dispatch for that project and leaves running work alone. Archival requires no
active work or incomplete worktree cleanup and preserves all history. There is
no ordinary hard-delete operation. Changing checkout, mounts, artifact roots,
or environment policy creates a new ProjectRevision; already admitted jobs stay
pinned to their old revision.

Cross-project dependencies remain permitted through globally unique queue item
IDs. Admission may depend only on already existing items, preserving the current
acyclic construction rule. A committed ExperimentCard containing several jobs
does not automatically create a workflow: each job is submitted explicitly,
and operational dependencies remain Submission data.

## Configuration And Secret Boundary

The tracked portable Project manifest contains the project key, display name,
card roots, logical volume declarations, supported protocols, environment
policy, and optional extension-schema reference. It contains no checkout path,
absolute scratch path, sync destination, credential, or secret.

Host enrollment maps logical declarations to canonical absolute paths and a
project runtime environment. Queue-reserved variables and
`CUDA_VISIBLE_DEVICES` cannot be overridden. Cards may refer only to declared
environment names; literal secrets are rejected from core fields. The precise
inheritance/secret-reference mechanism must be fixed by ADR before the service
claims a secure multi-project environment policy.

## Phase 0: Governance And Extracted-Baseline Parity

Objective: establish an independently testable product and durable maintenance
rules without altering live Flowers operation.

Entry conditions:

- the source Flowers queue behavior and relevant history are identified;
- generic versus scientific integration tests are separable.

Deliverables:

- standalone `experiment-queue` package and Python 3.14 environment;
- installed queue, web, and runner entry points;
- explicit user-configured state directory;
- generic extracted tests and preserved filtered history;
- architecture, ADR, security, migration, `llm/`, and maintenance contracts;
- visible deprecation/ownership boundary in Flowers.

Verification:

- editable install and all generic tests in the standalone `.venv`;
- console-entry-point and missing/invalid-state smoke tests;
- Git history/fsck and clean worktrees in both repositories;
- no live state, remote host, or GPU access.

Exit gate: standalone parity is green and future queue-product TODOs exist only
in this repository. No production cutover consequence occurs in this phase.

## Phase 1: Protocol Registry And Structured Receipts

Objective: separate generic process/run/preemption protocols from the legacy
Flowers command parser before introducing new persistent entities.

Entry conditions:

- extracted baseline behavior is frozen by tests;
- independent protocol kinds/version lineages are documented.

Deliverables:

- typed independent version registry;
- atomic structured runner receipt for paths, logs, manifest, sync instruction,
  status, run identity, and segment;
- narrow legacy stdout-receipt fallback;
- generic cooperative-yield request/receipt and opaque project resume payload;
- optional dependency-light project helper and conformance tests;
- continuation identity based on resolved spec/revision/commit/run evidence.

Verification:

- structured receipt success, partial write, malformed/corrupt receipt, and
  restart tests;
- exact legacy-log golden fixtures;
- ready and failed yield receipts, hash changes, stale request IDs, regressed
  progress, and changed continuation identity;
- Flowers W&B and SPECFEM adapters remain project-owned integration tests.

Exit gate: new execution can communicate without scraping human logs and the
generic core has no tracker-specific continuation field. Legacy fallback is
still supported, so rollback is the extracted baseline path.

Explicit exclusions: project storage, UI changes, and live Flowers migration.

## Phase 2: Strict Project And Experiment Schemas

Objective: define portable, versioned authoring contracts before database and
runtime code depend on their shape.

Entry conditions:

- parser, JSON Schema, canonicalization, and dependency choices are accepted by
  ADR;
- protocol identities from Phase 1 can be referenced by schemas.

Deliverables:

- strict YAML 1.2 subset loader and canonical JSON/digest implementation;
- bundled immutable Draft 2020-12 Project v1 and ExperimentCard v1 schemas;
- namespaced extension schema validation;
- immutable ExperimentCard plus mutable Submission model;
- one-or-more explicit jobs per card, direct argv/committed wrappers, parameter
  declarations, resources, artifacts, provenance, and capabilities;
- full admission compilation/snapshot model;
- exact `LegacyMarkdownCard/v0` adapter;
- validate, explain, schema-export, and submit-dry-run services.

Verification:

- duplicate keys, tags, aliases, merges, timestamps/coercions, non-finite
  numbers, unsupported versions, and unknown core fields fail closed;
- canonical digests are stable across supported platforms;
- unresolved parameters, placeholders, path escapes, undeclared resources, and
  unsupported preemption fail before mutation;
- simple single-job and explicit coordinator/worker cards validate;
- legacy commands remain byte-for-byte equivalent.

Exit gate: data models and schemas are stable enough for database v5; admission
can resolve an immutable specification without launching it. Rollback has no
persistent-state consequence because schema-v4 storage remains unchanged.

Explicit exclusions: generalized matrices/templates and automatic workflow
submission.

## Phase 3: Project Model And Database V5

Objective: make Project, revision, lifecycle, and failure scope durable while
preserving exact schema-v4 evidence.

Entry conditions:

- Phase 2 Project and admission snapshot models are accepted;
- lifecycle, checkout repointing, archive, overlap, dependency, and environment
  decisions are recorded in ADRs.

Deliverables:

- Project, Enrollment, ProjectRevision, mount, artifact-root, runtime-state, and
  job-artifact tables/models;
- non-null project/revision queue-item identity with ownership foreign keys;
- project-scoped experiment-attempt uniqueness;
- project or host failure scope in events;
- lifecycle APIs and CLI commands;
- explicit offline v1-v4-to-v5 importer with dry run, backup/copy precondition,
  receipt, integrity verification, and refusal to auto-migrate on startup;
- schema-v4 refusal to write schema v5.

Migration details:

- the legacy source may be grandfathered as importer input even when its state
  directory is inside Flowers; the v5 destination must be external to every
  checkout, mount, and artifact root;
- queue IDs, attempts, events, dependencies, reservations, process metadata,
  refs/worktrees, runner paths, and continuation evidence are preserved;
- migration fixtures may retain historical PID metadata, but production
  cutover requires no item in `starting`, `running`, `yielding`, `terminating`,
  or `force_killing` state. Pending/held work may migrate.

Verification:

- fixtures for every supported source schema and active/history edge case;
- SQLite integrity and foreign-key checks plus before/after semantic counts;
- ownership/revision mismatch and archive/no-cascade tests;
- two projects admit colliding experiment IDs;
- old/new version refusal and source-untouched rollback tests.

Exit gate: synthetic/copy migrations are deterministic, receipt-backed, and
lossless. Rollback always means reopening the untouched old database with old
code; v5 is never downgraded.

Explicit exclusion: production Flowers state migration.

## Phase 4: Project-Aware Execution And Failure Isolation

Objective: remove the global repository assumption from Git, worktrees, paths,
environments, artifacts, scheduling, and health.

Entry conditions:

- schema v5 and immutable project revisions are available;
- structured cards/receipts can launch without legacy checkout commands.

Deliverables:

- project-qualified Git refs, worktrees, identity checks, and cleanup;
- declared logical mounts replacing hard-coded Flowers shared paths;
- resolved path authorization for state, mounts, artifacts, logs, manifests,
  checkpoints, and approved external scratch;
- state and artifact filesystem disk checks with correct failure scope;
- declared child environment construction and reserved-variable protection;
- direct structured execution without `cd ~/3D_Helmholtz` rewriting or a
  project-local generic runner;
- project-scoped repository/card/mount/disk/child failure quarantine;
- global GPU/lease/database/central-state failure handling;
- candidate selection that skips unhealthy projects without violating global
  priority ordering.

Verification:

- two repositories with distinct checkouts, environments, mounts, and outputs;
- traversal, symlink escape, overlap, state nesting, artifact escape, reserved
  environment override, and cleanup safety;
- project A failures while project B continues;
- global pause on telemetry/database/lease/state-disk failure;
- restart/recovery and revision pinning across configuration changes;
- cross-project priority, same-priority continuation, dependencies, and manual
  preemption.

Exit gate: the scheduler has no single-repository execution dependency for new
jobs and no project failure can head-of-line block healthy work. Legacy imported
items retain their recorded compatibility path.

## Phase 5: Project-Aware CLI, Web, Authorization, And Observability

Objective: expose multi-project state safely through every operator surface.

Entry conditions:

- Phase 4 APIs enforce project identity and path authorization independently of
  presentation code.

Deliverables:

- project-qualified submission, status, receipt, event, and artifact CLI;
- unambiguous cwd inference only for one registered checkout;
- web project overview/health, selector, filters, badges, revision identity,
  admission, and run detail;
- server-side filtering/pagination and live-update filter preservation;
- project-aware authorization for all direct and mutation endpoints;
- compatibility admin/reservation roles with minimal information disclosure;
- design for named scoped principals before multi-team delegation;
- package/instance/project/revision/actor/failure-scope provenance in exports.

Verification:

- two-project end-to-end CLI, web, and live-update tests;
- direct-route cross-project disclosure/mutation tests;
- pause/archive and global reservation/manual-preemption behavior;
- large history pagination/filtering without browser-side disclosure;
- authenticated restart/session behavior.

Exit gate: all operator surfaces are project-aware and enforce security in the
server/core. Compatibility roles are sufficient only for the current trusted
host; multi-team use remains gated on scoped principals.

## Phase 6: Onboarding And Conformance

Objective: let a new trusted scientific repository integrate without hidden
Flowers conventions or scheduler-specific project code.

Entry conditions:

- Project/card/CLI behavior is stable enough to teach and scaffold.

Deliverables:

- `project init/register/doctor`, `card new/validate`, schema export, and submit
  dry run;
- minimal ordinary, Python training, data-pipeline, and cooperative-preemption
  examples;
- editor schemas and GPU-free project CI validation;
- cooperative-yield conformance suite;
- ten-minute host and project guides, security/deployment, artifacts/mounts,
  provenance, compatibility, and troubleshooting references.

Verification:

- a fresh fixture project integrates and runs a non-preemptible job using only
  the guide;
- a second fixture implements and safely resumes cooperative preemption;
- examples require no Flowers path, `mutton2`, W&B, GPU, or project import in
  the queue process.

Exit gate: ordinary projects need a manifest, card, and executable command—not
a custom scheduler plugin.

## Phase 7: Flowers Compatibility Integration

Objective: prove the new service against real Flowers contracts without
changing live production state.

Entry conditions:

- Phases 1-6 are green with an independent second project;
- the legacy adapter and v4 importer are stable on synthetic fixtures.

Deliverables:

- portable Flowers Project manifest and extension schema;
- local enrollment fixture for checkout, `.venv`, data/outputs, external
  scratch, sync, and environment policies;
- representative simple, W&B/preemptible, and independently elastic SPECFEM
  job conversions;
- classification of active/future versus historical cards and YAML sidecars
  only where appropriate;
- exhaustive fixture-based importer validation and a fresh two-project
  CLI/web/preemption/recovery smoke; no separate production-state dress
  rehearsal is required;
- compatibility wrappers prepared but not activated.

Verification:

- exact project commit, command, artifact, checkpoint, tracker, and continuation
  identity for representative jobs;
- approved `/scratch` access without broad filesystem access;
- card corpus has explicit dispositions without rewriting immutable history;
- independent second-project tests remain mandatory;
- Flowers live code/state/operation remains unchanged.

Exit gate: local evidence supports a cutover proposal, but no production action
is authorized merely by passing this phase.

## Phase 8: Production Cutover

Objective: replace the deprecated Flowers queue while preserving state,
history, and an untouched rollback source.

Hard entry gate:

- David explicitly confirms the SPECFEM dataset is generated and its
  synchronized scientific evidence is closed out;
- every active legacy item has drained and both legacy database writers are
  stopped;
- David authorizes cutover and supplies a consistent state copy/backup and
  external-path inventory.

Procedure:

1. Record old code, database, WAL/backup, state, and project commit identity.
2. Preserve the source read-only and migrate a copy with explicit project key
   `flowers-3d-helmholtz` to an external v5 state directory.
3. Verify integrity, foreign keys, counts, events, dependencies, reservations,
   refs/worktrees, historical process metadata, artifacts, and continuation
   digests.
4. Verify CLI/web presentation and authorization against migrated state.
5. Start exactly one standalone scheduler/web service and run the approved
   operational smoke.
6. Record cutover and rollback receipts.

Exit gate:

- no old/new concurrent dispatch;
- all retained history and pending work are verified and explainable;
- recovery, logs, receipts, GPU controls, priority, and manual preemption pass;
- rollback remains possible through untouched schema-v4 state.

Failure/rollback: stop the standalone writers, retain failed migration receipts,
fix the standalone importer or runtime against fixtures/a new source copy, and
retry only from the untouched v4 source after David's decision. Never repair
the source in place or attempt a v5-to-v4 downgrade.

## Phase 9: Observation, Deprecation, And Flowers Cleanup

Objective: remove duplication only after the standalone service is proven in
operation.

Entry conditions:

- production cutover is successful and an accepted observation period has no
  unresolved state/protocol regression;
- at least one supported release has warned about legacy admission.

Deliverables:

- redirect remaining Flowers wrappers/docs to the installed package;
- remove duplicate Flowers queue source/tests only after proving no active
  card, state, receipt, import, or provenance dependency;
- retain immutable scientific cards, phase evidence, and migration records;
- publish backup/restore/upgrade/rollback procedures and first supported
  production release;
- eventually retire the legacy adapter under the compatibility policy.

Verification:

- repository-wide dependency/import/card/reference inventory;
- Flowers scientific tests and standalone compatibility tests;
- production backup/restore rehearsal;
- historical records remain interpretable after source removal.

Exit gate: Flowers contains no duplicate queue-product implementation or live
queue-product TODO, and the standalone release owns all supported operation.

## Cross-Cutting Risk Register

- **Live-state corruption:** offline copy-only migration, receipts, integrity
  checks, immutable rollback state, and no startup migration.
- **Two schedulers sharing resources:** explicit drain/stop gate, instance
  lease, single-writer checks, and operator receipt.
- **Cross-project path disclosure or deletion:** canonical allowed roots,
  traversal/symlink tests, server-side authorization, and no artifact deletion
  in lifecycle operations.
- **Configuration drift:** immutable revisions and normalized admission
  snapshots.
- **Head-of-line project failure:** project-scoped quarantine and continued
  candidate scanning.
- **Checkpoint incompatibility:** declared capability, independent protocol
  versions, hashed artifacts, resolved-spec identity, and conformance tests.
- **Schema sprawl:** strict core schemas, namespaced extensions, independent
  versioning, immutable published schemas, and a compatibility matrix.
- **Unsafe onboarding magic:** scaffolding, validation, doctor, explain, and dry
  run instead of implicit path, device, or scientific-intent inference.
- **Scientific/product ownership confusion:** project experiment ledgers remain
  in scientific repositories; `llm/status.md` tracks product implementation
  only.

## Deferred Scope

Gang scheduling, DDP checkpoint coordination, non-NVIDIA accelerator backends,
distributed queue instances, containerization as a security sandbox, automatic
priority-triggered preemption, and a generalized DAG/matrix engine require
separate decisions after the initial multi-project service is proven.
