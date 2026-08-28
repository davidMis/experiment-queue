# Protocol compatibility and version ownership

This matrix is the discoverable ownership and compatibility record for durable
`experiment-queue` protocols. Implementation progress remains in
[`llm/status.md`](../llm/status.md); this document records stable protocol
boundaries, not a task ledger.

Protocol identity is the pair `apiVersion: experiment-queue/v<major>` and
`kind: <ProtocolKind>`. A major belongs only to its kind: `RunnerManifest/v1`
and `RunnerReceipt/v1` are unrelated versions. New machine-readable documents
carry both fields. A reader fails closed on an unknown kind or major, except for
the explicitly named legacy representations below.

The typed definitions live in
[`experiment_queue.protocols`](../src/experiment_queue/protocols.py). The
checked-in [identity fixture](../tests/fixtures/protocol-identities.json) and
[`test_protocols.py`](../tests/test_protocols.py) ensure that every declared
identity remains unique, immutable, and round-trippable.

## Runtime compatibility matrix

"Declared" reserves a stable identity for implementation; it does not claim
that the current executable accepts or emits that document. "Legacy input"
means read/migrate only. Compatibility readers must never broaden their
heuristics implicitly.

| Protocol identity | Runtime support | Writer / reader owner | Compatibility rule | Fixture or regression evidence |
| --- | --- | --- | --- | --- |
| `Database/v1` | Legacy input | `QueueStore._migrate_v1_to_v2` in [`queue.py`](../src/experiment_queue/queue.py) | Open only through the explicit v1→v4 compatibility chain; never emit | `ExperimentQueueTests.test_v1_database_migrates_in_place_for_reservations_and_segments` in [`test_queue.py`](../tests/test_queue.py) |
| `Database/v2` | Legacy input | `QueueStore._migrate_v2_to_v3` in [`queue.py`](../src/experiment_queue/queue.py) | Open only through the explicit v2→v4 compatibility chain; never emit | `ExperimentQueueTests.test_v2_migration_pins_an_existing_pending_item` in [`test_queue.py`](../tests/test_queue.py) |
| `Database/v3` | Legacy input | `QueueStore._migrate_v3_to_v4` in [`queue.py`](../src/experiment_queue/queue.py) | Open only through the explicit v3→v4 compatibility chain; never emit | `ExperimentQueueTests.test_v3_migration_binds_legacy_continuation_metadata_or_holds` in [`test_queue.py`](../tests/test_queue.py) |
| `Database/v4` | Current read/write | `QueueStore` in [`queue.py`](../src/experiment_queue/queue.py) | Extracted single-project baseline; reject unknown versions | `TemporaryQueueRepository` plus the queue suite in [`test_queue.py`](../tests/test_queue.py) |
| `Database/v5` | Declared; not yet accepted or emitted | Future offline importer and multi-project store | One-way, offline migration of a copy; v4 code must refuse it | Identity fixture only until the v5 migration fixtures land |
| `Project/v1` | Bundled structural and semantic validation; admission not yet emitted | Strict loader in [`serialization.py`](../src/experiment_queue/serialization.py) and version-owned validator in [`schema_registry.py`](../src/experiment_queue/schema_registry.py) | No compatibility fallback; unknown fields/majors and duplicate logical identities fail closed | Golden parser, schema, digest, offline-reference, editor-export, and packaged-wheel checks in [`test_serialization.py`](../tests/test_serialization.py), [`test_schemas.py`](../tests/test_schemas.py), and [`verify_wheel.py`](../scripts/verify_wheel.py) |
| `ExperimentCard/v1` | Bundled structural and semantic validation; admission not yet emitted | Strict loader in [`serialization.py`](../src/experiment_queue/serialization.py) and version-owned validator in [`schema_registry.py`](../src/experiment_queue/schema_registry.py) | No implicit Markdown fallback; mutable Submission policy is rejected; job/artifact identities and checkpoint references are enforced semantically | Simple and coordinator/worker fixtures plus strict identity, command, resource, reference, and editor-export checks in [`test_schemas.py`](../tests/test_schemas.py) and packaged-wheel verification in [`verify_wheel.py`](../scripts/verify_wheel.py) |
| `LegacyMarkdownCard/v0` | Current compatibility input | `read_card_command` in [`queue.py`](../src/experiment_queue/queue.py) | Exact `## Exact Manual Command On Mutton2` parser only; never emit as a new card | `TemporaryQueueRepository.add_card` and `test_card_command_is_read_only_after_explicit_selection` in [`test_queue.py`](../tests/test_queue.py) |
| `RunnerManifest/v1` | Current read/write | `build_manifest`, `write_manifest`, and continuation validation in [`runner.py`](../src/experiment_queue/runner.py) | Existing untagged `schema_version: 1` manifests remain readable; new manifests carry typed identity | `test_run_experiment_creates_manifest_logs_configs_and_rsync` and `test_yielded_runner_continues_in_same_directory_and_appends_segment` in [`test_runner.py`](../tests/test_runner.py) |
| `RunnerReceipt/v1` | Current read/write | Runner writer in [`runner.py`](../src/experiment_queue/runner.py); scheduler reader in [`queue.py`](../src/experiment_queue/queue.py) | Atomic per-segment JSON is authoritative for new runs; a present malformed v1 receipt fails closed | `test_runner_publishes_complete_running_and_terminal_receipts`, `test_restarted_scheduler_ingests_initial_structured_runner_receipt`, `test_structured_runner_receipt_never_falls_back_when_present_but_invalid`, and the end-to-end runner test |
| `RunnerReceipt/v0` | Current legacy fallback | Narrow legacy stdout parser in [`queue.py`](../src/experiment_queue/queue.py) | Read only for imported/legacy jobs when no v1 receipt exists; exact final labeled lines, no guessing | `test_legacy_stdout_runner_receipt_parser_is_exact_and_nonguessing` and `test_incomplete_atomic_temp_receipt_leaves_legacy_fallback_available` in [`test_queue.py`](../tests/test_queue.py) |
| `QueueExport/v0` | Current compatibility output | `export_receipt` in [`queue.py`](../src/experiment_queue/queue.py) | Historical JSON couples `schema_version` to `Database/v4`; retain only as the named legacy representation | Queue-store fixture and export regression in [`test_queue.py`](../tests/test_queue.py) |
| `QueueExport/v1` | Declared; not yet emitted | Future export writer/reader in [`queue.py`](../src/experiment_queue/queue.py) | Must carry its own identity; database version belongs in separate metadata | Identity fixture only until the v1 export fixture lands |
| `CooperativeYieldRequest/v0` | Current legacy write | Reservation/preemption request writers in [`queue.py`](../src/experiment_queue/queue.py) | Extracted schema-v4 `schema_version: 1` shape; never treat it as typed v1 | Preemption/reservation end-to-end fixtures in [`test_queue.py`](../tests/test_queue.py) |
| `CooperativeYieldRequest/v1` | Library implemented; schema-v5 wiring pending | Typed writer/parser in [`cooperative_yield.py`](../src/experiment_queue/cooperative_yield.py) | Emit only for structured admissions with complete immutable continuation evidence | Request round-trip and helper conformance tests in [`test_cooperative_yield.py`](../tests/test_cooperative_yield.py) |
| `CooperativeYieldReceipt/v0` | Current legacy read | `Scheduler._validated_yield_receipt` in [`queue.py`](../src/experiment_queue/queue.py); legacy project code writes it | Extracted `schema_version: 1` generic-progress and Flowers W&B-extension shapes remain compatibility-only | `test_yield_receipt_rejects_invalid_generic_progress`, `test_yield_checkpoints_reserves_old_gpu_and_resumes_at_priority_front`, and `test_generic_zero_row_progress_yields_and_resumes` in [`test_queue.py`](../tests/test_queue.py) |
| `CooperativeYieldReceipt/v1` | Library implemented; schema-v5 wiring pending | Typed writer/parser and validator in [`cooperative_yield.py`](../src/experiment_queue/cooperative_yield.py) | Strict ready/failed shapes with hashed artifacts, typed progress, opaque resume context, and complete continuation binding | Receipt, corruption, progress, and artifact conformance tests in [`test_cooperative_yield.py`](../tests/test_cooperative_yield.py) |

## Ownership rules

- Database changes belong to the storage/migration layer and never advance
  another protocol automatically.
- Project and ExperimentCard schemas own portable authoring documents; runtime
  Submission state is not part of either document.
- The runner owns RunnerManifest and RunnerReceipt emission. The scheduler owns
  receipt admission, path authorization, and the bounded v0 stdout fallback.
- QueueExport versions describe the export envelope, not the database that was
  exported.
- Cooperative-yield request and receipt versions are separate identities. The
  generic queue owns their envelope and verification; projects own opaque
  checkpoint/resume content.
- A supported major cannot be removed until its compatibility policy permits
  removal and its fixture is retained for migration/regression coverage.
