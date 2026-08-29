# Protocol compatibility and version ownership

This is the discoverable ownership and compatibility record for durable
`experiment-queue` protocols. Current implementation state remains in
[`llm/status.md`](../llm/status.md); release support and removal policy are in
[`release-policy.md`](release-policy.md).

Protocol identity is the pair `apiVersion: experiment-queue/v<major>` and
`kind: <ProtocolKind>`. A major belongs only to its kind:
`RunnerManifest/v1` and `RunnerReceipt/v1` are unrelated versions. New
machine-readable documents carry both fields. Readers fail closed on an unknown
kind or major except for the explicitly bounded v0 representations below.

The typed registry is
[`experiment_queue.protocols`](../src/experiment_queue/protocols.py). The
checked-in [identity fixture](../tests/fixtures/protocol-identities.json) and
[`test_protocols.py`](../tests/test_protocols.py) keep every declared identity
unique and round-trippable.

## Runtime compatibility matrix

“Legacy input” means read/import only in the primary v5 product. “Declared”
reserves an identity but does not claim that the primary executable emits it.
Compatibility readers never broaden their heuristics implicitly.

| Protocol identity | Primary v5 support | Owner | Compatibility and evidence |
| --- | --- | --- | --- |
| `Database/v1` | Offline legacy input | Immutable reader/importer in [`legacy_state.py`](../src/experiment_queue/legacy_state.py) and [`migrate_v5.py`](../src/experiment_queue/migrate_v5.py) | Accepted only as an authentic whole-state offline copy; version-owned missing-column defaults are recorded. The deprecated v4 store retains its historical explicit v1→v4 chain. Exhaustive success/refusal coverage is in [`test_migrate_v5.py`](../tests/test_migrate_v5.py). |
| `Database/v2` | Offline legacy input | Same as v1 | Same copy-only rule; reservations and all historical rows/high-water values are preserved field-for-field. |
| `Database/v3` | Offline legacy input | Same as v1 | Same copy-only rule; continuation defaults/evidence are verified without fabricating typed identity. |
| `Database/v4` | Offline legacy input; explicit legacy read/write | V5 importer plus deprecated `QueueStore` in [`queue.py`](../src/experiment_queue/queue.py) | `experiment-queue-legacy-v4` is the only v4 write surface. Primary startup refuses v4; importer reads a quiescent copy only. V4 startup refuses v5 before mutation. |
| `Database/v5` | Current read/write | [`database_v5.py`](../src/experiment_queue/database_v5.py), typed repositories, and services | Fresh creation occurs only through explicit first Project registration; import is a separate command. V5 refuses v1-v4 and unknown schemas without startup migration. Ownership, no-cascade, trigger, and version-refusal evidence is in [`test_database_v5.py`](../tests/test_database_v5.py). |
| `Project/v1` | Current input and persisted evidence | Strict loader/schema models plus trusted Git resolver and Project repository | Unknown fields/majors, invalid namespaces, environments, logical roots, and extension schemas fail closed. Registration stores exact Git blob and normalized evidence in an immutable ProjectRevision. Tests span [`test_authoring.py`](../tests/test_authoring.py), [`test_git_resolver.py`](../tests/test_git_resolver.py), [`test_project_lifecycle.py`](../tests/test_project_lifecycle.py), and wheel verification. |
| `ExperimentCard/v1` | Current input and persisted admission | Strict loader/schema models, [`admission.py`](../src/experiment_queue/admission.py), Git resolver, and v5 repository | No Markdown fallback, implicit sibling-job submission, or interpolation. Every admission selects one job and stores exact pinned source/resolution evidence. |
| `LegacyMarkdownCard/v0` | Imported compatibility input/runtime | Exact parser in [`legacy.py`](../src/experiment_queue/legacy.py), importer, and v5 compatibility dispatcher | Only the exact `## Exact Manual Command On Mutton2` contract and exact historical worktree transform are accepted. Primary v5 does not create new legacy admissions. Golden parser and importer tests prevent heuristic growth. |
| `RunnerManifest/v1` | Current read/write | [`runner.py`](../src/experiment_queue/runner.py) | New documents carry typed identity. Existing exact untagged `schema_version: 1` manifests remain readable for continuation compatibility. |
| `RunnerReceipt/v1` | Current read/write | Runner writer, typed reader, and scheduler service | Atomic per-segment JSON is authoritative; a present malformed receipt fails closed and never falls back to stdout. Success, partial-write, restart, and end-to-end evidence is in [`test_runner.py`](../tests/test_runner.py) and [`test_scheduler_service_v5.py`](../tests/test_scheduler_service_v5.py). |
| `RunnerReceipt/v0` | Legacy fallback | Narrow human-log parser in the deprecated queue | Read only for exact imported/legacy jobs when a structured receipt is absent. It recognizes only the historical final labeled lines and never guesses. |
| `QueueExport/v0` | Legacy output only | Deprecated v4 `export_receipt` | Historical JSON couples `schema_version` to Database/v4. It remains named v0 so it cannot be mistaken for a database-independent export. |
| `QueueExport/v1` | Current read/write | [`queue_export.py`](../src/experiment_queue/queue_export.py) and `receipt --json` | One bounded RFC 8785 document carries package version, Database/v5 instance identity, export actor/time, Project/revision/item/event/artifact evidence, historical GPU assignment plus `runtimeGpuLeaseHeld`/`runtimeGpuLeaseReleasedAt`, and exact deterministic typed cooperative-yield sources. Typed Project/extension Git blob IDs are recomputed from their exact embedded bytes. Event actor/failure scope remain explicit. Because Database/v5 does not retain exact ExecutorReceipt bytes, the envelope truthfully records their absence and never reconstructs them. |
| `QueueMigrationReceipt/v1` | Current read/write | [`migration_receipt.py`](../src/experiment_queue/migration_receipt.py) and offline importer | One strict receipt identifies a dry run or real copy-only import, source/destination identity, row/sequence comparison, path inventory, checks, and result. A failed receipt never authorizes cutover. See [`test_migration_receipt.py`](../tests/test_migration_receipt.py) and [`test_migrate_v5.py`](../tests/test_migrate_v5.py). |
| `CooperativeYieldRequest/v0` | Imported legacy compatibility only | Deprecated v4 queue and the bounded adapter in [`legacy_continuation_v0.py`](../src/experiment_queue/legacy_continuation_v0.py) | Historical `schema_version: 1` request shape. V5 may emit the exact frozen bytes only for an imported, already-preemptible LegacyMarkdownCard/v0 item; new/typed admissions never use it. |
| `CooperativeYieldReceipt/v0` | Imported legacy compatibility only | Bounded v5 adapter plus project-owned historical writer | Historical generic-progress and Flowers extension shapes are accepted only for the matching persisted imported-v0 request. Checkpoint/metadata roots, bytes, hashes, progress, runner identity, PID/PGID, and compare-and-set requeue are revalidated; the receipt is never treated as typed v1. |
| `CooperativeYieldRequest/v1` | Current read/write | [`cooperative_yield.py`](../src/experiment_queue/cooperative_yield.py) and [`continuation_v5.py`](../src/experiment_queue/continuation_v5.py) | Emitted only for a structured admission with declared capability, explicit operator authorization, and complete spec/revision/Git/run/prior-receipt identity. Request state is persisted before file publication and signaling. |
| `CooperativeYieldReceipt/v1` | Current read/write | Project helper/writer plus typed continuation validator/repository | Strict ready/failed shapes carry typed progress, opaque resume context, and path-bound hashes for every admitted checkpoint artifact. Ready requeues one next segment. Ambiguous evidence retains the yielding runtime lease; rejection after authenticated executor exit records terminal failure with the lease still held, and either case isolates only its Project. Conformance and scheduler coverage is in [`test_cooperative_yield.py`](../tests/test_cooperative_yield.py), [`test_continuation_v5.py`](../tests/test_continuation_v5.py), and the [cooperative example](../examples/cooperative-preemption). |

## Ownership rules

- Database changes belong to the storage/migration layer and never advance
  another protocol automatically.
- Project and ExperimentCard schemas own portable authoring documents. Mutable
  Submission priority, holds, dependencies, operator, and device assignment are
  not part of either document.
- `AdmissionSnapshot` and ExecutorReceipt are internal authenticated evidence
  models, not public protocol identities. Database v5 may decompose them only
  while retaining exact bytes, hashes, and ownership.
- The runner owns RunnerManifest/RunnerReceipt emission. The scheduler owns
  receipt admission, path authorization, process identity, and the bounded v0
  fallback.
- QueueExport versions describe an export envelope, not its database version.
  Diagnostic CLI/web JSON does not acquire a protocol identity accidentally.
- QueueMigrationReceipt versions describe importer/verifier evidence, not the
  source or destination database protocol.
- Cooperative-yield request and receipt versions are independent. The queue
  owns envelope identity and verification; projects own checkpoint semantics
  and opaque resume content.
- A supported major is removed only under
  [the deprecation threshold](release-policy.md#legacy-removal-threshold), and
  its fixtures remain for history/import regression even after new writes end.
