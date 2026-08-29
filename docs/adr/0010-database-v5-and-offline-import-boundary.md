# ADR 0010: Database v5 and the offline import boundary

Status: accepted, 2026-08-28.

## Context

Schema v4 binds one database to one repository and stores legacy Markdown
command evidence directly on queue-item rows. ADRs 0008 and 0009 require
first-class Projects, immutable revisions, exact admission snapshots, scoped
health, and project-qualified execution. Introducing those constraints by
startup DDL or an in-place migration would risk the only rollback source.

This decision fixes the version boundary and evidence-preservation rules before
schema-v5 storage or the importer becomes an operator command.

## Decision

### Separate stores and version refusal

Schema-v4 and schema-v5 stores are distinct implementations. Opening an
existing database first reads and validates `metadata.schema_version` without a
mutating pragma, DDL statement, sidecar creation, or compatibility migration.
The v4 implementation accepts only versions 1 through 4 and refuses v5. The v5
implementation accepts only v5 and refuses versions 1 through 4. Unsupported,
missing, or malformed metadata fails with an actionable error before mutation.

Fresh v5 initialization occurs only for a nonexistent database through the
explicit v5 store. Conversion from v1-v4 occurs only through the offline
importer. Application startup never chooses or performs the conversion.

### Schema-v5 ownership graph

Schema v5 contains at least these ownership groups:

- `projects`, immutable `project_revisions`, frozen revision Enrollment,
  revision mounts, the constrained read-write artifact-root subset, revision
  environments, and project runtime health;
- immutable structured admission snapshots containing every byte sequence,
  canonical document, schema/extension identity, digest, selected command,
  compiler version, and initial Submission policy fixed by ADR 0008;
- queue items with non-null Project and revision identity, global IDs,
  project-scoped experiment-attempt uniqueness, and a checked structured or
  legacy admission discriminator;
- global dependencies and GPU state, Project/host-scoped events, and declared
  or observed job artifacts; and
- migration-source and receipt evidence sufficient to identify the imported
  state and verifier result.

Every historical relationship uses explicit `ON UPDATE RESTRICT ON DELETE
RESTRICT`. Composite foreign keys prove that a revision belongs to the Project,
that a structured admission belongs to the same Project/revision as its queue
item, and that a Project-scoped queue-item event names the item's Project.
Lifecycle operations never cascade rows or artifact deletion.

Exact source and canonical evidence is stored as SQLite `BLOB`, not a decoded
text surrogate. Application insertion and loading recompute every stored hash.
Schema checks distinguish complete structured evidence from explicitly
grandfathered legacy evidence; nullable columns may not silently create a
partially trusted structured admission.

### Legacy preservation

The importer retains every source queue-item column and value, including IDs,
attempts, state, process identity, Git ref/worktree data, runner paths,
termination fields, cooperative-yield and continuation fields, and historical
tracker compatibility values. It also retains dependency pairs, event IDs and
payload text, GPU allowlist and reservation rows, unknown metadata entries, and
SQLite sequence/high-water values.

New v5 identity, scope, discriminator, and migration-evidence fields are the
only intentional differences. The importer does not reinterpret JSON text,
normalize a historical path, fabricate Project/v1 or AdmissionSnapshot
evidence, or overwrite a legacy field with a new runtime path. Missing columns
in authentic v1-v3 sources receive only the version-owned compatibility default
recorded by the migration receipt.

Legacy queue items use `LegacyMarkdownCard/v0` admission and retain their exact
card path, card hash, command, runner, and recorded commit. If project-qualified
refs or worktrees are needed after import, they are stored as new v5 runtime
identity while original ref/worktree columns remain historical evidence.

### Whole-state, copy-only importer

Importer input is an operator-supplied consistent copy of the complete legacy
state tree, not a live SQLite path. Attempt segments, receipts, logs,
authentication state, and continuation files may be required to verify rows.
The source database is opened read-only and is never checkpointed, migrated,
repaired, or otherwise written. A source with unresolved WAL state or failed
integrity is rejected until the operator supplies a consistent SQLite backup.

The destination must be absent, external to the source, and outside queue
checkouts, mounts, artifact roots, and the Flowers legacy overlap. A real import
builds a temporary sibling destination, verifies it completely, then atomically
publishes it. Failure never replaces a destination and never changes the
source. Dry run performs the same discovery, mapping, and verification plan
without creating or changing the requested destination.

Production import rejects any item in `starting`, `running`, `yielding`,
`terminating`, or `force_killing`. Pending, held, blocked, and terminal history
may migrate. Tests include those running-like shapes as refusal cases and may
retain terminal rows with historical process metadata.

The imported Project is initially paused. Preserved GPU allowlist state cannot
dispatch until a valid portable revision, doctor checks, and an explicit
operator resume occur.

### Migration receipt and verification

Every dry run and real import emits a versioned machine receipt containing:

- source state/database identity, schema version, SQLite integrity result, and
  input file evidence;
- destination schema/package identity and explicit imported Project key;
- row counts, IDs, sequence values, state counts, dependencies, events,
  reservations, and allowlist comparison;
- legacy defaults introduced by source version, path/ref/worktree disposition,
  artifact inventory, and continuation hash results;
- destination integrity and foreign-key results; and
- success/failure, exact failed check, actor, and time.

Verification compares every legacy column field-for-field in addition to
semantic counts. A receipt succeeds only after SQLite integrity, foreign keys,
ownership constraints, referenced local file evidence, Git identity where
available, and continuation digests all pass. A failure receipt is retained
outside the source and requested destination.

QueueMigrationReceipt versions are independent of database versions. Their
protocol identity will be registered before the importer emits one.

## Consequences

Migration requires extra storage and an explicit operator workflow, but the v4
rollback source remains untouched and old/new code cannot silently write the
other schema. Synthetic typed evidence cannot be confused with historical
legacy evidence.

Authentic v1-v4 fixtures, including corruption and active-state refusal cases,
are mandatory. Merely changing a version metadata value on a v4-shaped database
does not qualify as migration coverage.
