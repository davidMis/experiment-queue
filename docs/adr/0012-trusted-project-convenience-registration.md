# ADR 0012: Trusted-project convenience registration

Status: accepted, 2026-08-29.

## Context

The first schema-v5 workflow exposed the complete Enrollment, logical-volume,
artifact-root, and environment-binding model to every operator. That model is
useful when a project wants named path injection and artifact observation, but
it is unnecessary friction for a trusted single-user host. Registering a
Project already authorizes its committed code to use every filesystem,
process, network, and credential capability available to the service account;
volume declarations are provenance and convenience, not a sandbox.

The initial scaffold and Flowers compatibility helper nevertheless required
operators to invent dataset, output, scratch, and external-environment roots.
That made optional evidence appear to be a prerequisite for dispatch.

## Decision

The primary workflow is a trusted-project convenience path:

- `project init` emits `volumes: []`;
- cards need not declare artifacts;
- `project register` and `project append-revision` accept an omitted
  `--enrollment` when the Project declares `volumes: []` and exactly one
  environment;
- automatic registration freezes an ordinary Enrollment/v1 with no mounts,
  binds the single environment to `<checkout>/.venv/bin` by default, and
  inherits the variable names allowed by the Project;
- `--environment-bin` may override that default and accepts a virtual-
  environment root, executable-search directory, or Python executable; and
- checkout-local environment evidence is derived automatically and
  authenticated against committed `.gitignore` rules at the pinned commit.

The explicit Enrollment file remains supported for projects that deliberately
want named mounts, queue-observed artifacts, multiple environments, or a
different host mapping. Project/v1, ExperimentCard/v1, Enrollment/v1, and
Database/v5 do not change version or schema: the simpler workflow uses shapes
they already support.

Jobs remain trusted service-account processes. Omitting volumes neither grants
nor removes filesystem access; ordinary Unix permissions decide what project
code can read and modify. The queue continues to create a pinned execution
worktree. Jobs that create undeclared relative output inside that worktree can
prevent automatic cleanup, so ordinary project code should write long-lived
results to its existing absolute paths or paths derived from
`EXPERIMENT_QUEUE_PRIMARY_REPO`.

## Consequences

A normal single-environment project registers without an Enrollment generator,
host-path inventory, external virtual environment, or artifact declaration.
Advanced provenance remains opt-in and backward compatible. The committed
`.gitignore` check retains one inexpensive guard against accidentally treating
tracked source as a mutable virtual environment without making the operator
author proof metadata.

This decision supersedes ADR 0009 only where its complete Enrollment model was
presented as required operator-authored input. Its persisted revision ownership
and exact evidence rules remain unchanged.
