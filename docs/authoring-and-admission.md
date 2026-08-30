# Typed authoring and admission

Project/v1 and ExperimentCard/v1 are portable authoring documents. They contain
no host checkout, mount, device, or credential values. The typed library turns
their exact bytes into immutable models and compiles one explicitly selected
job into deterministic execution evidence. The production admission service
first authenticates those bytes against a full Git commit and immutable
ProjectRevision, then persists the complete snapshot in database v5.

## Validation pipeline

Admission applies these boundaries in order:

1. parse one strict UTF-8 YAML 1.2 document;
2. validate the bundled Draft 2020-12 schema and version-owned semantic rules;
3. construct a deeply immutable typed Project or ExperimentCard;
4. validate card references against the selected Project;
5. validate the project-owned extension namespace and optional schema;
6. copy and validate mutable Submission policy;
7. resolve the selected job and whole-value parameter bindings; and
8. retain exact source, normalized, schema, extension, and resolved evidence.

The principal APIs are:

```python
from experiment_queue.admission import Submission, compile_admission

submission = Submission(
    project_key="example-project",
    card_path="experiments/EXP-001.yaml",
    job_id="train",
    bindings={"epochs": 20},
    priority=10,
    hold_reason=None,
    dependencies=[],
    operator="operator-name",
    preemption_authorized=False,
)

snapshot = compile_admission(
    project_source=project_bytes,
    card_source=card_bytes,
    submission=submission,
    project_revision="example-project:revision-7",
    git_commit="0123456789abcdef0123456789abcdef01234567",
    extension_schema_source=extension_schema_bytes,
)
```

This pure function does not inspect a repository. Direct library callers must
therefore supply trusted source evidence. Production registration and
submission use `git_resolver`: it accepts only a full 40- or 64-character
commit, proves the object is a commit, reads named blobs with structured Git
plumbing, records each blob object ID/mode/size, and verifies the resulting
ProjectRevision before persistence. A branch, tag, abbreviated object ID,
working-tree file, symlink, submodule, non-regular mode, replacement object, or
unverified API claim cannot become admitted evidence.

The compiler requires the card source path to equal the detached Submission
card path and binds all supplied evidence immutably. The database-v5 repository
accepts only a resolver-created admission wrapper whose Project, revision,
commit, and blob evidence match; it recomputes stored hashes on insertion and
load.

`Project.from_yaml()`, `ExperimentCard.from_yaml()`, and
`validate_card_for_project()` are available when a caller needs validation
without compiling a Submission. Their `to_document()` methods return fresh
JSON-native copies; changing those copies cannot alter the model.

## Cross-document rules

A card must use the selected Project key. Every job environment and artifact
root must name a Project declaration, and artifacts must target a `readWrite`
volume. A cooperative-yield capability may use only protocol identities listed
by the Project. `CUDA_VISIBLE_DEVICES` and all `EXPERIMENT_QUEUE_*` variables
are service-owned and cannot be inherited through Project policy.

Volumes and artifacts are optional. A trusted project may declare
`volumes: []` and omit `job.artifacts`; this does not create a filesystem
sandbox or prevent its process from using ordinary host paths. Declared volumes
exist only for named path injection, observed artifact evidence, and typed
checkpoint contracts.

ExperimentCard/v1 deliberately has no template engine. Submission bindings
replace only complete values of existing top-level `spec.parameters` keys.
They never interpolate argv, wrapper, path, or shell text. `$binding` objects
and obvious `${...}` or `{{...}}` tokens fail in parameters and structured
execution fields; shell scripts retain shell expansion only because they are an
explicit compatibility escape hatch.

## Project extension schema

Only `extensions.<project-key>` is admitted in a Project, card, or job. Without
an extension schema, that value may contain arbitrary JSON-native object data.
When `spec.extensionSchema` is declared, the caller supplies its exact bytes
from the pinned Git revision. The validator accepts strict Draft 2020-12 JSON,
authenticates the optional canonical SHA-256, and never retrieves a reference
from the network.

The project schema validates one envelope containing only locations that are
present:

```json
{
  "project": {"projectSetting": true},
  "card": {"experimentSetting": "value"},
  "jobs": {
    "train": {"jobSetting": 3}
  }
}
```

This lets a project require related fields across its Project, card, and job
payloads without weakening any core schema rule.

## Snapshot boundary

`AdmissionSnapshot` owns immutable exact Project/card bytes and hashes,
canonical normalized documents and hashes, bundled schema identities and
hashes, optional extension-schema evidence, selected command, ProjectRevision,
full Git object ID, installed package version, and canonical resolved execution
JSON. Compiler-version evidence always comes from local installed package
metadata; an admission caller cannot supply its own provenance label.

Bindings and preemption authorization affect the resolved execution digest.
Priority, hold reason, dependencies, and operator remain mutable scheduling or
audit policy, so they are copied into `SubmissionPolicy` but excluded from that
digest. Dependency IDs are a semantic set canonicalized once in ascending
global-item order for snapshot, database edges, events, and export. Database v5
decomposes and stores this evidence without
reinterpretation. Runtime consumes the stored snapshot and pinned worktree; it
does not reopen a later Project manifest, ExperimentCard, extension schema, or
mutable Enrollment file.

The CLI exposes these boundaries without queue mutation through `project
validate`, `project explain`, `card validate`, `card explain`, `schema export`,
and `submit --dry-run`. See the runnable examples and exact host-registration
workflow in [`project-onboarding.md`](project-onboarding.md).
