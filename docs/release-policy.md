# Release, support, deprecation, and changelog policy

This policy applies to the Python distribution, installed operator surfaces,
and durable protocols owned by `experiment-queue`. It does not change
scientific Project/card ownership or authorize a production migration.

## Package releases and support window

Package versions follow Semantic Versioning. Until 1.0, a minor release may
change Python library or presentation APIs, but it must still honor the durable
state/protocol and deprecation rules below. Patch releases are compatible
corrective releases for their minor line.

Until a 1.0 support window is announced, only the latest published minor line
is supported for production, and its latest patch supersedes earlier patches.
Backports to an older line occur only when a release note explicitly names that
line; there is no implicit LTS promise. A release supports:

- production dispatch on Linux with CPython 3.14+, SQLite, Git worktrees, POSIX
  process groups/PTYs, and NVIDIA GPUs visible to `nvidia-smi`;
- development, authoring validation, importer work, and unit tests on macOS;
  and
- only the dependency ranges and platform scope declared by that release.

Windows, non-NVIDIA accelerators, distributed schedulers, container isolation,
and gang/DDP preemption are unsupported until a later accepted contract says
otherwise.

## Compatibility guarantees

Package SemVer and every durable protocol version evolve independently.

- Published Project/ExperimentCard schemas and serialized protocol majors are
  immutable. A shape change requires a new major owned by that kind.
- A writer emits only versions it explicitly owns. A reader fails closed on
  unknown or malformed identity; it never guesses from a coincident integer.
- A database version changes only through an explicit offline, dry-run-capable,
  receipt-producing migration of a copy. Startup never upgrades, downgrades, or
  repairs persistent state.
- A supported release retains readers/importers needed to interpret state and
  receipts that it promises to support. Removing new writes does not authorize
  deleting historical fixtures or evidence.
- Accepted queue IDs, Project/revision identity, events, dependencies, process
  metadata, refs/worktrees, artifacts, and continuation evidence are never
  renumbered or silently reinterpreted.
- CLI command/option behavior used for production receives at least one
  released-version deprecation warning before removal. During 0.x, documented
  diagnostic text/JSON and private-web presentation may change between minors;
  such changes must appear in the changelog. Diagnostic output is not promoted
  to QueueExport/v1 accidentally.
- A safety or security defect may require immediate refusal of an unsafe write.
  The release notes must identify the refusal and provide a preservation,
  migration, or rollback path; it must not silently reinterpret old state.

The exact per-kind read/write matrix is
[`protocol-compatibility.md`](protocol-compatibility.md).

## Deprecation process

A deprecation announcement names:

1. the affected entry point, option, API, schema, or protocol writer;
2. the supported replacement and conversion/validation procedure;
3. the first release that warns;
4. the earliest release eligible to remove it; and
5. any permanent reader, importer, fixture, or historical-evidence obligation.

Warnings must be visible in CLI help or runtime output where the deprecated
surface is used, and in `CHANGELOG.md`. Documentation may stop teaching the
old writer once the replacement is primary, but rollback instructions remain
until the corresponding operational rollback source is retired.

## Legacy removal threshold

Legacy Markdown admission and schema-v4 executables are separate obligations.
New `LegacyMarkdownCard/v0` admissions may be removed only after all of the
following are recorded:

- no supported state contains a nonterminal legacy item requiring new
  admission or v0 cooperative preemption;
- the operator-supplied live-card inventory classifies every active/future card
  as typed replacement or retired, while historical cards remain immutable;
- production has completed an accepted observation period using typed
  Project/v1 and ExperimentCard/v1 admission;
- at least one released version warned about the exact removal; and
- rollback no longer depends on that writer.

The explicit `experiment-queue-legacy-v4` and
`experiment-queue-web-legacy-v4` entry points may be removed only after the
same conditions, plus an accepted production cutover, verified backup/restore,
and a recorded decision that the untouched v4 rollback source no longer needs
the packaged executable.

Even after writer/entry-point removal, authentic v1-v4 importer fixtures, exact
`LegacyMarkdownCard/v0` parsing fixtures, and protocol identities remain in
the historical regression suite unless a later migration guarantees equivalent
interpretability.

## Release checklist

Every production release must:

1. pass Linux CPython 3.14 clean-install, full test, packaging, and entry-point
   smoke checks without GPUs or operator state;
2. build a wheel whose bundled schemas, canonical digests, modules, license,
   and entry points authenticate in an isolated environment;
3. pass database integrity/version-refusal, migration fixture, two-Project
   isolation, path/authorization, process-control/recovery, reservation,
   preemption, and termination checks proportional to the change;
4. update `CHANGELOG.md`, the compatibility matrix, operator/migration
   guidance, and any accepted ADR supersession;
5. identify persistent-state or protocol changes and their rollback path;
6. contain no credentials, live state, production paths, or scientific
   artifacts; and
7. be tagged from a reviewed clean commit with the package version matching the
   tag.

Production deployment is a separate operator action. A successful package
release never implies authorization to migrate or start a scheduler.

## Changelog format

`CHANGELOG.md` keeps an `Unreleased` section and one date-stamped section per
package release. Operator-visible entries use the categories `Added`,
`Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`, and
`Migration` as applicable. Every entry states operational impact; migration
entries name state/protocol versions, prerequisites, receipt/rollback behavior,
and whether operator action is required.

Commits, `llm/log.md`, and test counts are development evidence, not a
substitute for the changelog. Pure refactors and test-only changes need no
entry unless they alter supported behavior or migration assurance.

Security reports follow [`SECURITY.md`](../SECURITY.md) and should not disclose
credentials, queue state, logs, or private host details in a public issue.
