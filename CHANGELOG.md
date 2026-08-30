# Changelog

This file records notable operator-visible changes to `experiment-queue`.
Release, compatibility, deprecation, and support rules are defined in
[`docs/release-policy.md`](docs/release-policy.md).

## Unreleased

## 0.2.1 - 2026-08-30

### Changed

- The default Project scaffold now declares no volumes or artifacts. Trusted
  project code retains the service account's normal filesystem access.
- Project registration and revision append can derive Enrollment/v1
  automatically for `volumes: []` from one checkout-local `.venv`, eliminating
  the ordinary host-path inventory and Enrollment-file workflow. Explicit
  Enrollment remains available for advanced mounts, artifacts, and multiple
  environments.
- Flowers deployment now starts with a fresh schema-v5 database because no
  legacy jobs are running, the legacy services are stopped, and the legacy
  database does not need to be imported. The legacy import checklist remains
  available but is not part of the active deployment.

## 0.2.0 - 2026-08-29

### Added

- Project-aware Database/v5 with immutable Projects, ProjectRevisions,
  Enrollments, exact Git/blob admission snapshots, project-scoped attempts,
  events, artifacts, lifecycle, and health circuits.
- Project-qualified CLI for scaffolding, validation, schema export,
  registration, revision activation, dry-run/admission, status, controls,
  receipts, GPUs, passive reservations, explicit cooperative preemption, durable
  termination, and scheduler operation.
- Authenticated project-aware HTTPS application with finite Project scopes,
  server-side ownership enforcement and pagination, and a minimal-information
  reserver role.
- Isolated project worktrees/environments, logical mount and artifact
  variables, structured attempt receipts, required-artifact observation,
  recovery, and Project-scoped failure quarantine.
- CooperativeYieldRequest/v1 and CooperativeYieldReceipt/v1
  checkpoint-and-requeue with immutable continuation identity and hashed
  checkpoint artifacts.
- Explicit copy-only v1-v4→v5 importer with dry run,
  QueueMigrationReceipt/v1, authentic legacy fixtures, field/sequence
  comparison, and atomic destination publication.
- Ordinary, Python-training, data-pipeline, cooperative-preemption, and local
  Flowers compatibility examples.

### Changed

- `experiment-queue`, `python -m experiment_queue`, checkout wrappers, and
  `experiment-queue-web` now select the project-aware schema-v5 applications.
- Project commands may infer identity from cwd only when it belongs to exactly
  one registered current checkout; state always remains explicit.
- Structured jobs execute from pinned project-qualified worktrees and consume
  only stored admission/Enrollment evidence.

### Deprecated

- The extracted schema-v4 applications are available only as
  `experiment-queue-legacy-v4` and
  `experiment-queue-web-legacy-v4`. They remain rollback/compatibility
  surfaces and must never dispatch concurrently with v5.
- LegacyMarkdownCard/v0 is import/runtime compatibility evidence, not a new
  primary admission format. Its removal threshold is documented in
  [the release policy](docs/release-policy.md#legacy-removal-threshold).

### Migration

- Startup never migrates. Production import requires a quiescent complete
  offline copy, an absent external destination, a successful dry-run receipt,
  a successful real receipt, and untouched v4 rollback state.
- David confirmed on 2026-08-27 that Flowers SPECFEM generation and synchronized
  evidence closeout are complete, and waived a separate production-state dress
  rehearsal for this single-operator queue. The idle writer-free copy,
  operator-supplied external-path/card inventory, receipt review, and explicit
  cutover authorization remain required.
- The exact Flowers procedure and rollback boundary are in
  [`docs/migrations/flowers-v4.md`](docs/migrations/flowers-v4.md).

### Security

- V5 validates Project/item ownership server-side, resolves path and symlink
  authority at use time, constructs child environments from explicit
  allowlists, and authenticates process groups before signaling.
- Database identity, exact schema authentication, owner-controlled ancestor
  validation, atomic migration publication, atomic-replace RunnerReceipt state,
  and durable no-clobber executor launch/exit receipts fail closed when
  persistence or identity cannot be proven.
- Runtime GPU leases remain allocated until terminal process-group cleanup and
  fresh idle telemetry are both established; ambiguous release state blocks
  reuse and requires reconciliation.
- Registration remains code-execution authorization, not sandboxing.

## 0.1.0 - 2026-08-19

### Added

- History-preserving standalone extraction of the schema-v4 single-project
  queue, private web application, and experiment runner.
- Explicit absolute state-directory selection, Python 3.14 packaging, baseline
  compatibility tests, architecture records, migration boundaries, and
  deprecated Flowers ownership guidance.
