"""Verify strict, immutable QueueMigrationReceipt/v1 evidence."""

from __future__ import annotations

import json

import pytest

import experiment_queue.migration_receipt as migration_receipt_module
from experiment_queue.migration_receipt import (
    MigrationMode,
    MigrationReceiptError,
    MigrationResult,
    QueueMigrationReceipt,
)
from experiment_queue.protocols import QUEUE_MIGRATION_RECEIPT_V1
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes


_MIGRATED_TABLES = (
    "queue_items",
    "dependencies",
    "gpu_allowlist",
    "events",
    "gpu_reservations",
)


def _successful_comparison() -> dict[str, object]:
    row_counts = {
        table: {"source": 0, "destination": 0} for table in _MIGRATED_TABLES
    }
    table_digests = {table: "4" * 64 for table in _MIGRATED_TABLES}
    return {
        "verified": True,
        "importer_package_version": "0.1.0",
        "source_schema_version": 4,
        "source_state_entry_count": 1,
        "legacy_defaults": {},
        "row_counts": row_counts,
        "state_counts": {},
        "queue_item_ids": [],
        "event_ids": [],
        "reservation_ids": [],
        "dependency_pairs": [],
        "gpu_allowlist_uuids": [],
        "source_sequences": {},
        "destination_sequences": {},
        "source_table_sha256": table_digests,
        "destination_table_sha256": dict(table_digests),
        "event_scope_mapping": {
            "queue_item_event": "project",
            "itemless_event": "host",
        },
        "revision_by_commit": [],
        "project_id": 1,
        "revision_ids": [1],
        "pre_receipt_candidate_database_sha256": "6" * 64,
    }


def _successful_checks() -> list[dict[str, str]]:
    return [
        {
            "name": name,
            "status": (
                "not-applicable"
                if name in {"continuation-evidence", "atomic-publish"}
                else "passed"
            ),
            "detail": "verified fixture evidence",
        }
        for name in (
            "source-identity",
            "source-schema",
            "queue-quiescent",
            "git-evidence",
            "continuation-evidence",
            "destination-integrity",
            "field-comparison",
            "atomic-publish",
        )
    ]


def _values(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "operation_id": "migration-20260828-001",
        "mode": "dry-run",
        "result": "succeeded",
        "project_key": "flowers-3d-helmholtz",
        "actor": "test:operator",
        "started_at": "2026-08-28T12:00:00Z",
        "finished_at": "2026-08-28T12:00:01Z",
        "source": {
            "state_path": "/copied/legacy-state",
            "database_path": "/copied/legacy-state/queue.sqlite3",
            "schema_version": 4,
            "database_sha256": "1" * 64,
            "database_size_bytes": 4096,
            "database_mtime_ns": "1787949790695036929",
            "integrity_check": "ok",
            "sidecars": [],
            "state_identity_sha256": "2" * 64,
        },
        "destination": {
            "state_path": "/new/v5-state",
            "schema_version": 5,
            "schema_identity": "experiment-queue/database-v5",
            "database_instance_id": "e809d520-48af-4c92-a8a1-5d856f6b8959",
            "database_sha256": "3" * 64,
            "integrity_check": "ok",
            "foreign_key_violations": 0,
            "published": False,
        },
        "project": {
            "key": "flowers-3d-helmholtz",
            "id": 1,
            "revision_ids": [1],
            "lifecycle": "paused",
        },
        "comparison": _successful_comparison(),
        "path_inventory": [],
        "continuation_checks": [],
        "checks": _successful_checks(),
        "error": None,
    }
    values.update(changes)
    return values


def _one_item_values(**changes: object) -> dict[str, object]:
    comparison = _successful_comparison()
    row_counts = comparison["row_counts"]
    assert isinstance(row_counts, dict)
    row_counts["queue_items"] = {"source": 1, "destination": 1}
    comparison["state_counts"] = {"succeeded": 1}
    comparison["queue_item_ids"] = ["10"]
    comparison["revision_by_commit"] = [
        {"git_commit": "a" * 40, "revision_id": 1}
    ]
    values = _values(
        comparison=comparison,
        continuation_checks=[
            {"item_id": "10", "status": "not-applicable", "files": []}
        ],
    )
    values.update(changes)
    return values


def test_receipt_round_trips_as_canonical_protocol_evidence() -> None:
    values = _values()
    receipt = QueueMigrationReceipt.create(**values)  # type: ignore[arg-type]

    assert receipt.mode is MigrationMode.DRY_RUN
    assert receipt.result is MigrationResult.SUCCEEDED
    assert receipt.to_document()["apiVersion"] == "experiment-queue/v1"
    assert receipt.to_document()["kind"] == "QueueMigrationReceipt"
    assert receipt.canonical_json == canonical_json_bytes(receipt.to_document())
    assert receipt.sha256 == sha256_bytes(receipt.canonical_json)
    assert QueueMigrationReceipt.from_bytes(receipt.canonical_json) == receipt
    assert QUEUE_MIGRATION_RECEIPT_V1.document_identity() == {
        "apiVersion": "experiment-queue/v1",
        "kind": "QueueMigrationReceipt",
    }

    source = values["source"]
    assert isinstance(source, dict)
    source["schema_version"] = 1
    assert receipt.to_document()["source"]["schema_version"] == 4  # type: ignore[index]
    detached = receipt.to_document()
    detached["checks"] = []
    assert len(receipt.to_document()["checks"]) == 8  # type: ignore[arg-type]


def test_succeeded_import_requires_atomic_publication() -> None:
    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination["published"] = True
    destination["database_sha256"] = None
    checks = _successful_checks()
    checks[-1]["status"] = "passed"
    receipt = QueueMigrationReceipt.create(
        **_values(mode="import", destination=destination, checks=checks)  # type: ignore[arg-type]
    )
    assert receipt.mode is MigrationMode.IMPORT

    destination["published"] = False
    with pytest.raises(MigrationReceiptError, match="published destination"):
        QueueMigrationReceipt.create(
            **_values(mode="import", destination=destination, checks=checks)  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "instance_id",
    [
        None,
        "E809D520-48AF-4C92-A8A1-5D856F6B8959",
        "e809d520-48af-5c92-a8a1-5d856f6b8959",
        "00000000-0000-0000-0000-000000000000",
    ],
)
def test_succeeded_receipt_requires_canonical_database_uuidv4(
    instance_id: str | None,
) -> None:
    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination["database_instance_id"] = instance_id
    with pytest.raises(MigrationReceiptError, match="instance identity|UUIDv4"):
        QueueMigrationReceipt.create(
            **_values(destination=destination)  # type: ignore[arg-type]
        )


def test_failed_receipt_requires_error_and_failed_check() -> None:
    failed_checks = [
        {
            "name": "continuation-digest",
            "status": "failed",
            "detail": "item 7 checkpoint digest changed",
        }
    ]
    receipt = QueueMigrationReceipt.create(
        **_values(
            result="failed",
            error="item 7 checkpoint digest changed",
            checks=failed_checks,
        )  # type: ignore[arg-type]
    )
    assert receipt.result is MigrationResult.FAILED

    with pytest.raises(MigrationReceiptError, match="requires an error"):
        QueueMigrationReceipt.create(
            **_values(result="failed", checks=failed_checks)  # type: ignore[arg-type]
        )
    with pytest.raises(MigrationReceiptError, match="failed check"):
        QueueMigrationReceipt.create(
            **_values(result="failed", error="failure")  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"operation_id": "bad operation"}, "operation_id"),
        ({"finished_at": "2026-08-28T11:59:59Z"}, "must not precede"),
        ({"project_key": "other"}, "project.key"),
    ],
)
def test_receipt_cross_field_invariants_fail_closed(
    change: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(MigrationReceiptError, match=message):
        QueueMigrationReceipt.create(**_values(**change))  # type: ignore[arg-type]


def test_receipt_rejects_unknown_nested_contract_fields() -> None:
    source = dict(_values()["source"])  # type: ignore[arg-type]
    source["guessed"] = True
    with pytest.raises(MigrationReceiptError, match="source.*unknown fields"):
        QueueMigrationReceipt.create(
            **_values(source=source)  # type: ignore[arg-type]
        )

    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination["schema_version"] = 6
    with pytest.raises(MigrationReceiptError, match="exactly 5"):
        QueueMigrationReceipt.create(
            **_values(destination=destination)  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("value", [1787949790695036929, "-1", "+1", "01", ""])
def test_receipt_mtime_nanoseconds_require_exact_decimal_text(value: object) -> None:
    source = dict(_values()["source"])  # type: ignore[arg-type]
    source["database_mtime_ns"] = value
    with pytest.raises(MigrationReceiptError, match="canonical nonnegative decimal"):
        QueueMigrationReceipt.create(
            **_values(source=source)  # type: ignore[arg-type]
        )


def test_receipt_round_trips_zero_mtime_decimal_without_number_coercion() -> None:
    source = dict(_values()["source"])  # type: ignore[arg-type]
    source["database_mtime_ns"] = "0"
    receipt = QueueMigrationReceipt.create(
        **_values(source=source)  # type: ignore[arg-type]
    )
    assert receipt.to_document()["source"]["database_mtime_ns"] == "0"  # type: ignore[index]


def test_receipt_parser_rejects_duplicate_keys_and_protocol_confusion() -> None:
    receipt = QueueMigrationReceipt.create(**_values())  # type: ignore[arg-type]
    duplicated = receipt.canonical_json.replace(
        b'"apiVersion":',
        b'"apiVersion":"experiment-queue/v1","apiVersion":',
        1,
    )
    with pytest.raises(MigrationReceiptError, match="repeats JSON key"):
        QueueMigrationReceipt.from_bytes(duplicated)

    document = receipt.to_document()
    document["kind"] = "RunnerReceipt"
    with pytest.raises(MigrationReceiptError, match="unsupported receipt protocol"):
        QueueMigrationReceipt.from_bytes(json.dumps(document).encode())

    with pytest.raises(MigrationReceiptError, match="canonical JSON"):
        QueueMigrationReceipt.from_bytes(
            json.dumps(receipt.to_document(), indent=2).encode()
        )


def test_receipt_parser_rejects_unknown_top_level_and_nonfinite_json() -> None:
    receipt = QueueMigrationReceipt.create(**_values())  # type: ignore[arg-type]
    document = receipt.to_document()
    document["unexpected"] = True
    with pytest.raises(MigrationReceiptError, match="unknown fields"):
        QueueMigrationReceipt.from_bytes(json.dumps(document).encode())

    nonfinite = receipt.canonical_json.replace(b'"error":null', b'"error":NaN')
    with pytest.raises(MigrationReceiptError, match="unsupported JSON constant"):
        QueueMigrationReceipt.from_bytes(nonfinite)


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("source", "integrity_check", "corrupt", "source integrity"),
        (
            "source",
            "sidecars",
            ["/copied/legacy-state/queue.sqlite3-wal"],
            "no SQLite sidecars",
        ),
        ("destination", "schema_identity", "wrong/database-v5", "schema_identity"),
        ("destination", "integrity_check", "corrupt", "destination integrity"),
        ("destination", "foreign_key_violations", 1, "foreign-key violations"),
    ],
)
def test_succeeded_receipt_rejects_false_integrity_and_identity_claims(
    section: str,
    field: str,
    value: object,
    message: str,
) -> None:
    values = _values()
    nested = dict(values[section])  # type: ignore[arg-type]
    nested[field] = value
    values[section] = nested
    with pytest.raises(MigrationReceiptError, match=message):
        QueueMigrationReceipt.create(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("source", "state_path", "/copied/legacy-state/../legacy-state"),
        (
            "source",
            "database_path",
            "/copied/legacy-state/./queue.sqlite3",
        ),
        ("destination", "state_path", "//new/v5-state"),
        ("destination", "state_path", "/new/v5-state/"),
    ],
)
def test_receipt_paths_require_canonical_nontraversing_posix_syntax(
    section: str,
    field: str,
    value: str,
) -> None:
    values = _values()
    nested = dict(values[section])  # type: ignore[arg-type]
    nested[field] = value
    values[section] = nested
    with pytest.raises(MigrationReceiptError, match="canonical, non-traversing"):
        QueueMigrationReceipt.create(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "destination_state",
    [
        "/copied/legacy-state",
        "/copied/legacy-state/new-v5",
        "/copied",
    ],
)
def test_succeeded_receipt_rejects_overlapping_source_and_destination_roots(
    destination_state: str,
) -> None:
    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination["state_path"] = destination_state
    with pytest.raises(MigrationReceiptError, match="non-overlapping"):
        QueueMigrationReceipt.create(
            **_values(destination=destination)  # type: ignore[arg-type]
        )


def test_result_and_mode_require_exact_destination_evidence() -> None:
    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination["database_sha256"] = None
    with pytest.raises(MigrationReceiptError, match="discarded candidate"):
        QueueMigrationReceipt.create(
            **_values(destination=destination)  # type: ignore[arg-type]
        )

    destination["published"] = True
    with pytest.raises(MigrationReceiptError, match="dry-run receipts"):
        QueueMigrationReceipt.create(
            **_values(destination=destination)  # type: ignore[arg-type]
        )

    failed_checks = [
        {"name": "candidate-build", "status": "failed", "detail": "failed"}
    ]
    with pytest.raises(MigrationReceiptError, match="failed migration receipt"):
        QueueMigrationReceipt.create(
            **_values(
                mode="import",
                result="failed",
                error="failed",
                destination=destination,
                checks=failed_checks,
            )  # type: ignore[arg-type]
        )


def test_success_comparison_is_exact_and_cross_checked() -> None:
    comparison = _successful_comparison()
    comparison["verified"] = False
    with pytest.raises(MigrationReceiptError, match="verified must be true"):
        QueueMigrationReceipt.create(
            **_values(comparison=comparison)  # type: ignore[arg-type]
        )

    comparison = _successful_comparison()
    row_counts = comparison["row_counts"]
    assert isinstance(row_counts, dict)
    row_counts["events"] = {"source": 1, "destination": 0}
    with pytest.raises(MigrationReceiptError, match="source and destination differ"):
        QueueMigrationReceipt.create(
            **_values(comparison=comparison)  # type: ignore[arg-type]
        )

    comparison = _successful_comparison()
    destination_digests = comparison["destination_table_sha256"]
    assert isinstance(destination_digests, dict)
    destination_digests["queue_items"] = "7" * 64
    with pytest.raises(MigrationReceiptError, match="table digests differ"):
        QueueMigrationReceipt.create(
            **_values(comparison=comparison)  # type: ignore[arg-type]
        )

    comparison = _successful_comparison()
    comparison["unversioned_guess"] = True
    receipt = QueueMigrationReceipt.create(**_values())  # type: ignore[arg-type]
    document = receipt.to_document()
    document["comparison"] = comparison
    with pytest.raises(MigrationReceiptError, match="comparison.*unknown fields"):
        QueueMigrationReceipt.from_bytes(json.dumps(document).encode())


def test_success_path_and_continuation_evidence_is_strict_and_linked() -> None:
    QueueMigrationReceipt.create(**_one_item_values())  # type: ignore[arg-type]

    malformed_path = [
        {"item_id": "10", "kind": "runner_run_dir", "path": "/scratch/run-10"}
    ]
    with pytest.raises(MigrationReceiptError, match="path_inventory.*missing fields"):
        QueueMigrationReceipt.create(
            **_one_item_values(path_inventory=malformed_path)  # type: ignore[arg-type]
        )

    inconsistent_status = [
        {"item_id": "10", "status": "verified", "files": []}
    ]
    with pytest.raises(MigrationReceiptError, match="status must be 'not-applicable'"):
        QueueMigrationReceipt.create(
            **_one_item_values(continuation_checks=inconsistent_status)  # type: ignore[arg-type]
        )

    unbound_file = [
        {
            "item_id": "10",
            "status": "verified",
            "files": [
                {
                    "kind": "continuation_checkpoint",
                    "path": "/scratch/checkpoint-10.bin",
                    "sha256": "8" * 64,
                    "size_bytes": "1",
                }
            ],
        }
    ]
    with pytest.raises(MigrationReceiptError, match="does not match path_inventory"):
        QueueMigrationReceipt.create(
            **_one_item_values(continuation_checks=unbound_file)  # type: ignore[arg-type]
        )


def test_failed_receipt_preserves_partial_evidence_without_success_claims() -> None:
    source = dict(_values()["source"])  # type: ignore[arg-type]
    source["integrity_check"] = "database disk image is malformed"
    source["sidecars"] = ["/copied/legacy-state/queue.sqlite3-wal"]
    destination = dict(_values()["destination"])  # type: ignore[arg-type]
    destination.update(
        {
            "database_instance_id": None,
            "database_sha256": None,
            "integrity_check": "not run",
            "published": False,
        }
    )
    project = dict(_values()["project"])  # type: ignore[arg-type]
    project.update({"id": None, "revision_ids": []})
    receipt = QueueMigrationReceipt.create(
        **_values(
            mode="import",
            result="failed",
            error="source failed integrity",
            source=source,
            destination=destination,
            project=project,
            comparison={"verified": False, "source_schema_version": 4},
            path_inventory=[],
            continuation_checks=[],
            checks=[
                {"name": "source-integrity", "status": "failed", "detail": "bad"}
            ],
        )  # type: ignore[arg-type]
    )
    assert receipt.result is MigrationResult.FAILED


def test_factory_and_parser_enforce_the_same_receipt_size_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = QueueMigrationReceipt.create(**_values())  # type: ignore[arg-type]
    monkeypatch.setattr(
        migration_receipt_module,
        "MAX_RECEIPT_BYTES",
        len(receipt.canonical_json) - 1,
    )
    with pytest.raises(MigrationReceiptError, match="exceeds"):
        QueueMigrationReceipt.create(**_values())  # type: ignore[arg-type]
    with pytest.raises(MigrationReceiptError, match="1 through"):
        QueueMigrationReceipt.from_bytes(receipt.canonical_json)


def test_receipt_factory_cannot_be_bypassed() -> None:
    with pytest.raises(TypeError, match="validated-only"):
        QueueMigrationReceipt()  # type: ignore[call-arg]

    class ForgedReceipt(QueueMigrationReceipt):
        pass

    with pytest.raises(TypeError, match="constructs exactly"):
        ForgedReceipt.create(**_values())  # type: ignore[arg-type]
