# Architecture and migration target

## Current baseline

The first standalone release preserves the existing single-project queue so its
packaging and process behavior can be verified independently. One `QueueStore`
is still associated with one repository root, and legacy Markdown cards retain
their existing parser contract.

## Target ownership model

```text
host queue instance
├── global GPU allowlist, reservations, priority order, and scheduler lease
├── projects
│   ├── stable project identity
│   ├── immutable configuration revisions
│   ├── registered checkout, mounts, artifact roots, and runtime policy
│   └── experiment cards and job templates
└── submissions
    └── attempts
        └── continuation segments and artifacts
```

- A portable `Project` manifest is tracked in each scientific repository.
- Host enrollment maps that project to a checkout, logical volumes, artifact
  roots, and synchronization settings without committing host paths or secrets.
- Every submission snapshots one immutable project revision, Git commit, raw
  card digest, normalized card, resolved command, and environment policy.
- Experiment identity is project-scoped. Queue item IDs remain globally unique
  operational control targets.
- Priority is global across projects. Requeued continuations order by
  `priority DESC, resume_front DESC, id ASC`.
- Preemption is manual. A card declares cooperative capability, while the
  submission records whether an operator authorized it.

## Failure isolation

Repository, card, mount, project artifact-disk, and repeated child failures will
pause only the affected project. GPU telemetry, the central database, scheduler
lease, or central-state disk failures remain host-global.

## Execution boundary

The service owns scheduling, worktrees, process identity, logs, and the generic
run envelope. A project owns scientific commands, domain checkpoints, and result
interpretation. The service may launch a project command in its declared
environment but must not import project Python code.

The runner will publish an atomic machine-readable receipt. Human-readable log
scraping and W&B-specific queue columns are compatibility behavior to remove
after structured-card migration.

## Storage boundary

The operator-configured state directory contains the database, instance
identity, lease, internal worktrees, service logs, and control receipts.
Scientific artifacts live only under project-authorized roots and are never
deleted merely because a project is archived.

## Initial resource scope

Version 1 supports Linux hosts and one NVIDIA GPU per job. Gang scheduling,
tightly coupled DDP preemption, non-NVIDIA accelerators, distributed queue
instances, and container sandboxing are out of scope.
