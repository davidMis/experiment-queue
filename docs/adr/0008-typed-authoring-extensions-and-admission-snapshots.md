# ADR 0008: Typed authoring, extensions, and admission snapshots

Status: accepted, 2026-08-28.

## Context

Project/v1 and ExperimentCard/v1 now have authenticated structural schemas and
version-owned semantic checks. Before database v5 can persist projects or queue
items, the service needs one typed boundary that joins those documents without
letting mutable scheduling state leak into committed scientific intent. It also
needs a deterministic rule for the optional project-owned extension schema and
for the evidence compiled at admission.

This decision completes the authoring semantics left open by ADR 0003. It does
not define host Enrollment or ProjectRevision lifecycle/storage; those remain a
separate decision before database v5.

## Decision

### Immutable typed authoring models

The public Project and ExperimentCard constructors first run the complete
bundled structural and semantic validator. Their typed values are then deeply
immutable and retain the exact validated JSON value so converting a model back
to a document neither inserts defaults nor loses optional-field presence.
Callers receive fresh JSON-native copies rather than references to model state.

Cross-document validation additionally requires:

- the card project key to equal the Project key;
- every job environment to name a declared Project environment;
- every artifact root to name a declared writable Project logical volume;
- each cooperative-yield request and receipt identity to appear in the
  Project's supported-protocol list.

`CUDA_VISIBLE_DEVICES` and every `EXPERIMENT_QUEUE_*` name are service-owned
and cannot appear in the Project environment inheritance allowlist. Read-only
scientific inputs belong in card provenance; declared execution artifacts
cannot target a read-only logical volume.

These are admission rules owned by Project/v1 and ExperimentCard/v1. A caller
must not construct a trusted model by bypassing them.

### Project-owned extension namespace and schema

Within a Project/card admission context, extension data may occur only at
`extensions.<project-key>` in the Project, card, or job. The core schema keeps
that value as an arbitrary JSON object. A different namespace is rejected
rather than treated as an unowned plugin or security boundary.

If `spec.extensionSchema` is absent, the matching namespace remains flexible.
If it is present, admission must be given the exact schema bytes resolved by
the caller from that path in the pinned Git tree. The schema is strict UTF-8
JSON, declares Draft 2020-12 exactly, passes meta-schema checking, and runs with
an offline registry only. Its optional declared SHA-256 authenticates RFC 8785
canonical schema bytes, not presentation whitespace. Admission separately
retains the exact source-byte hash and canonical digest.

One extension schema validates this envelope, omitting locations that have no
payload:

```json
{
  "project": {"...": "extensions.<project-key> from Project"},
  "card": {"...": "extensions.<project-key> from ExperimentCard"},
  "jobs": {
    "job-id": {"...": "extensions.<project-key> from that job"}
  }
}
```

This permits project-specific requirements and cross-location checks without
allowing an extension to relax core validation. Remote retrieval, an unresolved
reference, a different schema dialect, duplicate keys, non-finite values, and
digest mismatches fail closed.

### Mutable Submission and bounded bindings

An ExperimentCard is never modified with priority, hold state, dependencies,
operator identity, device assignment, or manual-preemption authorization.
Those values belong to a mutable Submission and are revalidated whenever an
admission snapshot is compiled.

ExperimentCard/v1 has no string interpolation or template language. Its
`spec.parameters` object declares complete top-level parameter values. A
Submission binding may replace the whole value of an already declared
top-level parameter; it may not introduce a parameter or splice text into a
command, path, or shell script. Reserved placeholder objects such as
`{"$binding": "name"}` and obvious `${...}` or `{{...}}` placeholder tokens
are rejected in core parameters and structured execution fields. Shell scripts
remain the explicit compatibility escape hatch and retain their shell-owned
expansion syntax. A richer declaration, matrix, or interpolation system
requires a future ExperimentCard major.

Each card job is submitted explicitly. Selecting one job does not implicitly
submit its sibling coordinator or worker jobs, and dependencies remain
Submission data referring only to already existing global queue-item IDs.

### Immutable admission evidence

Admission compilation accepts exact Project and card bytes, a mutable
Submission, one immutable ProjectRevision identifier, and one full Git object
ID. It reparses and validates the sources, validates extensions, checks that the
card path is beneath a declared card root, resolves the selected job and
parameter bindings, and then produces a frozen snapshot containing:

- exact Project and card source bytes and SHA-256 hashes;
- RFC 8785 normalized Project and card JSON plus hashes;
- both bundled schema identities and authenticated schema hashes;
- extension-schema source/canonical evidence when applicable;
- canonical resolved execution JSON and its SHA-256;
- the selected typed command, project revision, full Git commit, and package
  version; and
- an immutable copy of the Submission policy as it stood at admission.

The resolved execution JSON pins the Project document, card scientific
identity and provenance, selected job, resolved parameters, environment policy,
extension payloads, revision, Git commit, and preemption authorization.
Priority, hold reason, dependencies, and operator are intentionally excluded
from the execution digest because they remain mutable scheduling/audit state.
Changing them never silently changes executable identity.

Compiler-version evidence is read from installed `experiment-queue` package
metadata. The admission caller cannot override that provenance label.

Runtime execution will consume stored resolved evidence. It must not reopen or
reparse a later Project manifest, ExperimentCard, or extension schema. Database
v5 may decompose this model into strict columns and blobs, but it must preserve
these bytes, identities, and hashes without reinterpretation.

The Phase-2 compiler is deliberately pure and does not itself open a Git
repository. Its caller is a trusted ProjectRevision/Git resolver that must read
the supplied bytes from the named paths in the supplied full commit. Card source
path and Submission card path must agree. A database or remote API must never
forward arbitrary client bytes, revision names, or Git claims directly to the
compiler. ProjectRevision storage and Git-tree verification will enforce that
caller contract before database v5 persists an admission.

## Consequences

Phase 2 can compile deterministic execution evidence without changing schema-v4
state or importing project code. Later host Enrollment and ProjectRevision work
can bind logical names to authorized absolute paths while keeping the portable
authoring and digest rules stable.

Submission parameter overrides are deliberately small and explicit. Projects
that need generated workflows, interpolation, matrices, or scientific-domain
validation keep that logic in committed project code or wait for a separately
versioned authoring contract.
