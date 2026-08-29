"""Define the strict QueueMigrationReceipt/v1 migration-audit protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
import json
from pathlib import PurePath
import re
from typing import Mapping, Self, cast
import uuid

from experiment_queue.protocols import (
    QUEUE_MIGRATION_RECEIPT_V1,
    ProtocolIdentityError,
    ProtocolVersion,
)
from experiment_queue.serialization import (
    CanonicalJSONError,
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


MAX_RECEIPT_BYTES = 16 * 1024 * 1024
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_NONNEGATIVE_DECIMAL_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_POSITIVE_DECIMAL_PATTERN = re.compile(r"[1-9][0-9]*\Z")
_GIT_OBJECT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_FILE_MODE_PATTERN = re.compile(r"[0-7]{4}\Z")
_DATABASE_SCHEMA_IDENTITY = "experiment-queue/database-v5"
_MIGRATED_TABLES = (
    "queue_items",
    "dependencies",
    "gpu_allowlist",
    "events",
    "gpu_reservations",
)
_IMPORTABLE_STATES = frozenset(
    {
        "queued",
        "held",
        "blocked",
        "succeeded",
        "failed",
        "interrupted",
        "force_killed",
        "removed",
    }
)
_SUCCESS_CHECK_NAMES = (
    "source-identity",
    "source-schema",
    "queue-quiescent",
    "git-evidence",
    "continuation-evidence",
    "destination-integrity",
    "field-comparison",
    "atomic-publish",
)
_GIT_DISPOSITIONS = frozenset(
    {
        "removed",
        "live-pending",
        "cleanup-pending-live",
        "pinned-no-worktree",
        "cleanup-pending-worktree-absent",
        "cleanup-pending-resources-absent",
    }
)
_PATH_KIND_ORDER = {
    "runner_run_dir": 0,
    "runner_manifest_path": 1,
    "continuation_checkpoint": 2,
    "continuation_checkpoint_metadata": 3,
    "worktree_path": 4,
    "git_ref": 5,
}
_LEGACY_DEFAULTS_BY_VERSION: dict[int, dict[str, JSONValue]] = {
    1: {
        "preemptible": 0,
        "segment": 1,
        "resume_front": 0,
        "yield_requested_at": None,
        "yield_requested_by": None,
        "yield_request_id": None,
        "yield_note": None,
        "yield_duration_hours": None,
        "continuation_checkpoint": None,
        "continuation_checkpoint_sha256": None,
        "continuation_checkpoint_metadata": None,
        "continuation_checkpoint_metadata_sha256": None,
        "continuation_step": None,
        "continuation_wandb_id": None,
        "git_ref": None,
        "worktree_path": None,
        "worktree_created_at": None,
        "worktree_removed_at": None,
        "worktree_cleanup_error": None,
    },
    2: {
        "continuation_checkpoint_metadata": None,
        "continuation_checkpoint_metadata_sha256": None,
        "git_ref": None,
        "worktree_path": None,
        "worktree_created_at": None,
        "worktree_removed_at": None,
        "worktree_cleanup_error": None,
    },
    3: {
        "continuation_checkpoint_metadata": None,
        "continuation_checkpoint_metadata_sha256": None,
    },
    4: {},
}


class MigrationReceiptError(ValueError):
    """Raised when migration evidence cannot form the v1 receipt protocol."""


class MigrationMode(StrEnum):
    """Whether an importer planned only or atomically published fresh state."""

    DRY_RUN = "dry-run"
    IMPORT = "import"


class MigrationResult(StrEnum):
    """Terminal result recorded even when the importer failed closed."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class MigrationCheckStatus(StrEnum):
    """One named verification outcome in execution order."""

    PASSED = "passed"
    FAILED = "failed"
    NOT_APPLICABLE = "not-applicable"


def _construct(**values: object) -> QueueMigrationReceipt:
    instance = object.__new__(QueueMigrationReceipt)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def _text(value: object, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise MigrationReceiptError(
            f"{field_name} must be a non-empty string without surrounding whitespace"
        )
    if len(value) > maximum:
        raise MigrationReceiptError(
            f"{field_name} must be {maximum} characters or fewer"
        )
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise MigrationReceiptError(
            f"{field_name} must contain valid Unicode scalar text"
        ) from exc
    if any(
        ord(character) < 32
        or ord(character) in {127, 0x85, 0x2028, 0x2029}
        for character in value
    ):
        raise MigrationReceiptError(f"{field_name} must not contain control text")
    return value


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _text(value, field_name=field_name, maximum=16_384)


def _timestamp(value: object, *, field_name: str) -> str:
    text = _text(value, field_name=field_name, maximum=64)
    if "T" not in text:
        raise MigrationReceiptError(
            f"{field_name} must be an RFC 3339 timestamp with an explicit offset"
        )
    try:
        parsed = datetime.fromisoformat(
            f"{text[:-1]}+00:00" if text.endswith("Z") else text
        )
    except ValueError as exc:
        raise MigrationReceiptError(
            f"{field_name} must be a real RFC 3339 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MigrationReceiptError(
            f"{field_name} must include Z or an explicit UTC offset"
        )
    return text


def _sha256(value: object, *, field_name: str, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise MigrationReceiptError(
            f"{field_name} must be a lowercase 64-character SHA-256 digest"
        )
    return value


def _integer(
    value: object,
    *,
    field_name: str,
    minimum: int = 0,
    nullable: bool = False,
) -> int | None:
    if value is None and nullable:
        return None
    if type(value) is not int or value < minimum:
        raise MigrationReceiptError(
            f"{field_name} must be an integer >= {minimum}, got {value!r}"
        )
    return value


def _nonnegative_decimal(value: object, *, field_name: str) -> str:
    """Validate an exact integer too large for interoperable JSON numbers."""

    if (
        type(value) is not str
        or _NONNEGATIVE_DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise MigrationReceiptError(
            f"{field_name} must be a canonical nonnegative decimal string "
            "without a sign or leading zero"
        )
    return value


def _database_instance_id(
    value: object, *, field_name: str, nullable: bool = False
) -> str | None:
    """Validate the canonical UUIDv4 assigned when Database/v5 was created."""

    if value is None and nullable:
        return None
    if type(value) is not str or len(value) != 36:
        raise MigrationReceiptError(
            f"{field_name} must be a canonical lowercase UUIDv4"
        )
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise MigrationReceiptError(
            f"{field_name} must be a canonical lowercase UUIDv4"
        ) from exc
    if (
        str(parsed) != value
        or parsed.version != 4
        or parsed.variant != uuid.RFC_4122
    ):
        raise MigrationReceiptError(
            f"{field_name} must be a canonical lowercase UUIDv4"
        )
    return value


def _absolute_path(value: object, *, field_name: str) -> str:
    text = _text(value, field_name=field_name)
    path = PurePath(text)
    if (
        not path.is_absolute()
        or path.anchor != "/"
        or ".." in path.parts
        or str(path) != text
    ):
        raise MigrationReceiptError(
            f"{field_name} must use canonical, non-traversing absolute POSIX syntax, "
            f"got {text!r}"
        )
    return text


def _exact_fields(
    value: object,
    *,
    field_name: str,
    expected: set[str],
) -> dict[str, JSONValue]:
    if type(value) is not dict:
        raise MigrationReceiptError(f"{field_name} must be a JSON object")
    document = cast(dict[str, JSONValue], value)
    actual = set(document)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields {missing}")
        if unknown:
            details.append(f"unknown fields {unknown}")
        raise MigrationReceiptError(
            f"{field_name} has invalid fields: " + "; ".join(details)
        )
    return document


def _canonical_section(value: object, *, field_name: str) -> bytes:
    if type(value) not in {dict, list}:
        raise MigrationReceiptError(f"{field_name} must be a JSON object or array")
    try:
        canonical = canonical_json_bytes(cast(JSONValue, value))
    except CanonicalJSONError as exc:
        raise MigrationReceiptError(f"{field_name} is not canonical JSON data: {exc}") from exc
    if len(canonical) > MAX_RECEIPT_BYTES:
        raise MigrationReceiptError(
            f"{field_name} exceeds the {MAX_RECEIPT_BYTES}-byte receipt limit"
        )
    return canonical


def _decode_section(source: bytes) -> JSONValue:
    return cast(JSONValue, json.loads(source.decode("utf-8")))


def _validate_source(value: object) -> bytes:
    source = _exact_fields(
        value,
        field_name="source",
        expected={
            "state_path",
            "database_path",
            "schema_version",
            "database_sha256",
            "database_size_bytes",
            "database_mtime_ns",
            "integrity_check",
            "sidecars",
            "state_identity_sha256",
        },
    )
    state_path = _absolute_path(source["state_path"], field_name="source.state_path")
    database_path = _absolute_path(
        source["database_path"], field_name="source.database_path"
    )
    if PurePath(database_path) != PurePath(state_path) / "queue.sqlite3":
        raise MigrationReceiptError(
            "source.database_path must be queue.sqlite3 directly inside source.state_path"
        )
    version = _integer(
        source["schema_version"], field_name="source.schema_version", minimum=1
    )
    if version not in {1, 2, 3, 4}:
        raise MigrationReceiptError("source.schema_version must be one of 1, 2, 3, 4")
    _sha256(source["database_sha256"], field_name="source.database_sha256")
    _integer(
        source["database_size_bytes"],
        field_name="source.database_size_bytes",
        minimum=1,
    )
    _nonnegative_decimal(
        source["database_mtime_ns"],
        field_name="source.database_mtime_ns",
    )
    _text(source["integrity_check"], field_name="source.integrity_check")
    if type(source["sidecars"]) is not list:
        raise MigrationReceiptError("source.sidecars must be an array of path strings")
    sidecars = cast(list[object], source["sidecars"])
    expected_sidecars = [
        f"{database_path}{suffix}" for suffix in ("-wal", "-shm", "-journal")
    ]
    if any(type(item) is not str for item in sidecars) or any(
        item not in expected_sidecars for item in sidecars
    ):
        raise MigrationReceiptError(
            "source.sidecars may contain only absolute SQLite sidecars for the "
            "recorded source database"
        )
    if sidecars != [path for path in expected_sidecars if path in sidecars]:
        raise MigrationReceiptError(
            "source.sidecars must be unique and in WAL, SHM, journal order"
        )
    _sha256(
        source["state_identity_sha256"],
        field_name="source.state_identity_sha256",
    )
    return _canonical_section(source, field_name="source")


def _validate_destination(value: object) -> bytes:
    destination = _exact_fields(
        value,
        field_name="destination",
        expected={
            "state_path",
            "schema_version",
            "schema_identity",
            "database_instance_id",
            "database_sha256",
            "integrity_check",
            "foreign_key_violations",
            "published",
        },
    )
    _absolute_path(destination["state_path"], field_name="destination.state_path")
    if _integer(
        destination["schema_version"],
        field_name="destination.schema_version",
        minimum=5,
    ) != 5:
        raise MigrationReceiptError("destination.schema_version must be exactly 5")
    if destination["schema_identity"] != _DATABASE_SCHEMA_IDENTITY:
        raise MigrationReceiptError(
            "destination.schema_identity must be exactly "
            f"{_DATABASE_SCHEMA_IDENTITY!r}"
        )
    _database_instance_id(
        destination["database_instance_id"],
        field_name="destination.database_instance_id",
        nullable=True,
    )
    _sha256(
        destination["database_sha256"],
        field_name="destination.database_sha256",
        nullable=True,
    )
    _text(destination["integrity_check"], field_name="destination.integrity_check")
    _integer(
        destination["foreign_key_violations"],
        field_name="destination.foreign_key_violations",
    )
    if type(destination["published"]) is not bool:
        raise MigrationReceiptError("destination.published must be a boolean")
    return _canonical_section(destination, field_name="destination")


def _validate_project(value: object, *, project_key: str) -> bytes:
    project = _exact_fields(
        value,
        field_name="project",
        expected={"key", "id", "revision_ids", "lifecycle"},
    )
    if project["key"] != project_key:
        raise MigrationReceiptError(
            f"project.key {project['key']!r} does not match receipt project_key "
            f"{project_key!r}"
        )
    _integer(project["id"], field_name="project.id", minimum=1, nullable=True)
    revision_ids = project["revision_ids"]
    if type(revision_ids) is not list or any(
        type(item) is not int or item <= 0
        for item in cast(list[object], revision_ids)
    ):
        raise MigrationReceiptError(
            "project.revision_ids must be an array of positive integer IDs"
        )
    if len(set(cast(list[int], revision_ids))) != len(cast(list[int], revision_ids)):
        raise MigrationReceiptError("project.revision_ids must not repeat IDs")
    if project["lifecycle"] != "paused":
        raise MigrationReceiptError(
            "project.lifecycle must be 'paused'; imported Projects cannot dispatch"
        )
    return _canonical_section(project, field_name="project")


def _positive_decimal(value: object, *, field_name: str) -> str:
    if type(value) is not str or _POSITIVE_DECIMAL_PATTERN.fullmatch(value) is None:
        raise MigrationReceiptError(
            f"{field_name} must be a canonical positive decimal string"
        )
    return value


def _validate_decimal_id_array(
    value: object, *, field_name: str
) -> tuple[str, ...]:
    if type(value) is not list:
        raise MigrationReceiptError(f"{field_name} must be an array")
    result = tuple(
        _positive_decimal(item, field_name=f"{field_name}[{index}]")
        for index, item in enumerate(cast(list[object], value))
    )
    if len(set(result)) != len(result):
        raise MigrationReceiptError(f"{field_name} must not repeat IDs")
    if tuple(sorted(result, key=int)) != result:
        raise MigrationReceiptError(f"{field_name} must be in increasing numeric order")
    return result


def _validate_digest_map(value: object, *, field_name: str) -> dict[str, str]:
    document = _exact_fields(
        value, field_name=field_name, expected=set(_MIGRATED_TABLES)
    )
    return {
        table: cast(
            str,
            _sha256(document[table], field_name=f"{field_name}.{table}"),
        )
        for table in _MIGRATED_TABLES
    }


def _validate_sequence_map(value: object, *, field_name: str) -> dict[str, str]:
    if type(value) is not dict:
        raise MigrationReceiptError(f"{field_name} must be a JSON object")
    document = cast(dict[str, object], value)
    unknown = sorted(set(document) - {"queue_items", "events", "gpu_reservations"})
    if unknown:
        raise MigrationReceiptError(
            f"{field_name} has unsupported sequence names {unknown}"
        )
    return {
        key: _nonnegative_decimal(item, field_name=f"{field_name}.{key}")
        for key, item in document.items()
    }


def _validate_success_comparison(
    value: object,
    *,
    source_schema_version: int,
    project: Mapping[str, object],
) -> tuple[str, ...]:
    """Validate every field emitted by the successful field comparator."""

    comparison = _exact_fields(
        value,
        field_name="comparison",
        expected={
            "verified",
            "importer_package_version",
            "source_schema_version",
            "source_state_entry_count",
            "legacy_defaults",
            "row_counts",
            "state_counts",
            "queue_item_ids",
            "event_ids",
            "reservation_ids",
            "dependency_pairs",
            "gpu_allowlist_uuids",
            "source_sequences",
            "destination_sequences",
            "source_table_sha256",
            "destination_table_sha256",
            "event_scope_mapping",
            "revision_by_commit",
            "project_id",
            "revision_ids",
            "pre_receipt_candidate_database_sha256",
        },
    )
    if comparison["verified"] is not True:
        raise MigrationReceiptError("comparison.verified must be true on success")
    _text(
        comparison["importer_package_version"],
        field_name="comparison.importer_package_version",
        maximum=128,
    )
    if comparison["source_schema_version"] != source_schema_version:
        raise MigrationReceiptError(
            "comparison.source_schema_version must match source.schema_version"
        )
    _integer(
        comparison["source_state_entry_count"],
        field_name="comparison.source_state_entry_count",
        minimum=1,
    )
    if comparison["legacy_defaults"] != _LEGACY_DEFAULTS_BY_VERSION[source_schema_version]:
        raise MigrationReceiptError(
            "comparison.legacy_defaults does not match the source schema version"
        )

    row_counts = _exact_fields(
        comparison["row_counts"],
        field_name="comparison.row_counts",
        expected=set(_MIGRATED_TABLES),
    )
    counts: dict[str, int] = {}
    for table in _MIGRATED_TABLES:
        pair = _exact_fields(
            row_counts[table],
            field_name=f"comparison.row_counts.{table}",
            expected={"source", "destination"},
        )
        source_count = cast(
            int,
            _integer(
                pair["source"],
                field_name=f"comparison.row_counts.{table}.source",
            ),
        )
        destination_count = cast(
            int,
            _integer(
                pair["destination"],
                field_name=f"comparison.row_counts.{table}.destination",
            ),
        )
        if source_count != destination_count:
            raise MigrationReceiptError(
                f"comparison.row_counts.{table} source and destination differ"
            )
        counts[table] = source_count

    state_counts_value = comparison["state_counts"]
    if type(state_counts_value) is not dict:
        raise MigrationReceiptError("comparison.state_counts must be a JSON object")
    state_counts = cast(dict[str, object], state_counts_value)
    if set(state_counts) - _IMPORTABLE_STATES:
        raise MigrationReceiptError(
            "comparison.state_counts contains a non-importable queue state"
        )
    state_total = 0
    for state, count in state_counts.items():
        state_total += cast(
            int,
            _integer(
                count,
                field_name=f"comparison.state_counts.{state}",
                minimum=1,
            ),
        )
    if state_total != counts["queue_items"]:
        raise MigrationReceiptError(
            "comparison.state_counts total must equal the queue_items row count"
        )

    queue_ids = _validate_decimal_id_array(
        comparison["queue_item_ids"], field_name="comparison.queue_item_ids"
    )
    event_ids = _validate_decimal_id_array(
        comparison["event_ids"], field_name="comparison.event_ids"
    )
    reservation_ids = _validate_decimal_id_array(
        comparison["reservation_ids"], field_name="comparison.reservation_ids"
    )
    for name, ids, table in (
        ("queue_item_ids", queue_ids, "queue_items"),
        ("event_ids", event_ids, "events"),
        ("reservation_ids", reservation_ids, "gpu_reservations"),
    ):
        if len(ids) != counts[table]:
            raise MigrationReceiptError(
                f"comparison.{name} length must equal {table} row count"
            )

    pairs_value = comparison["dependency_pairs"]
    if type(pairs_value) is not list:
        raise MigrationReceiptError("comparison.dependency_pairs must be an array")
    pairs: list[tuple[str, str]] = []
    for index, item in enumerate(cast(list[object], pairs_value)):
        pair = _exact_fields(
            item,
            field_name=f"comparison.dependency_pairs[{index}]",
            expected={"queue_item_id", "dependency_item_id"},
        )
        rendered = (
            _positive_decimal(
                pair["queue_item_id"],
                field_name=f"comparison.dependency_pairs[{index}].queue_item_id",
            ),
            _positive_decimal(
                pair["dependency_item_id"],
                field_name=f"comparison.dependency_pairs[{index}].dependency_item_id",
            ),
        )
        if rendered[0] not in queue_ids or rendered[1] not in queue_ids:
            raise MigrationReceiptError(
                "comparison.dependency_pairs must reference imported queue item IDs"
            )
        pairs.append(rendered)
    if len(pairs) != counts["dependencies"] or len(set(pairs)) != len(pairs):
        raise MigrationReceiptError(
            "comparison.dependency_pairs must uniquely enumerate dependency rows"
        )
    if pairs != sorted(pairs, key=lambda pair: (int(pair[0]), int(pair[1]))):
        raise MigrationReceiptError(
            "comparison.dependency_pairs must use source row order"
        )

    allowlist_value = comparison["gpu_allowlist_uuids"]
    if type(allowlist_value) is not list:
        raise MigrationReceiptError("comparison.gpu_allowlist_uuids must be an array")
    allowlist = tuple(
        _text(item, field_name=f"comparison.gpu_allowlist_uuids[{index}]", maximum=256)
        for index, item in enumerate(cast(list[object], allowlist_value))
    )
    if (
        len(allowlist) != counts["gpu_allowlist"]
        or len(set(allowlist)) != len(allowlist)
        or tuple(sorted(allowlist)) != allowlist
    ):
        raise MigrationReceiptError(
            "comparison.gpu_allowlist_uuids must uniquely enumerate sorted allowlist rows"
        )

    source_sequences = _validate_sequence_map(
        comparison["source_sequences"], field_name="comparison.source_sequences"
    )
    destination_sequences = _validate_sequence_map(
        comparison["destination_sequences"],
        field_name="comparison.destination_sequences",
    )
    if source_sequences != destination_sequences:
        raise MigrationReceiptError(
            "comparison source and destination sequence values differ"
        )
    source_digests = _validate_digest_map(
        comparison["source_table_sha256"],
        field_name="comparison.source_table_sha256",
    )
    destination_digests = _validate_digest_map(
        comparison["destination_table_sha256"],
        field_name="comparison.destination_table_sha256",
    )
    if source_digests != destination_digests:
        raise MigrationReceiptError(
            "comparison source and destination table digests differ"
        )
    if comparison["event_scope_mapping"] != {
        "queue_item_event": "project",
        "itemless_event": "host",
    }:
        raise MigrationReceiptError("comparison.event_scope_mapping is invalid")

    revisions_value = comparison["revision_by_commit"]
    if type(revisions_value) is not list:
        raise MigrationReceiptError("comparison.revision_by_commit must be an array")
    commits: set[str] = set()
    mapped_revision_ids: set[int] = set()
    for index, item in enumerate(cast(list[object], revisions_value)):
        revision = _exact_fields(
            item,
            field_name=f"comparison.revision_by_commit[{index}]",
            expected={"git_commit", "revision_id"},
        )
        commit = revision["git_commit"]
        if type(commit) is not str or _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
            raise MigrationReceiptError(
                f"comparison.revision_by_commit[{index}].git_commit is invalid"
            )
        revision_id = cast(
            int,
            _integer(
                revision["revision_id"],
                field_name=f"comparison.revision_by_commit[{index}].revision_id",
                minimum=1,
            ),
        )
        if commit in commits or revision_id in mapped_revision_ids:
            raise MigrationReceiptError(
                "comparison.revision_by_commit must not repeat commits or revisions"
            )
        commits.add(commit)
        mapped_revision_ids.add(revision_id)

    project_id = cast(
        int,
        _integer(comparison["project_id"], field_name="comparison.project_id", minimum=1),
    )
    if project["id"] != project_id:
        raise MigrationReceiptError("comparison.project_id must match project.id")
    revision_ids_value = comparison["revision_ids"]
    if type(revision_ids_value) is not list or any(
        type(item) is not int or item <= 0
        for item in cast(list[object], revision_ids_value)
    ):
        raise MigrationReceiptError(
            "comparison.revision_ids must be an array of positive integer IDs"
        )
    revision_ids = cast(list[int], revision_ids_value)
    if revision_ids != project["revision_ids"]:
        raise MigrationReceiptError(
            "comparison.revision_ids must match project.revision_ids"
        )
    if mapped_revision_ids - set(revision_ids):
        raise MigrationReceiptError(
            "comparison.revision_by_commit names an unknown revision ID"
        )
    if queue_ids:
        if mapped_revision_ids != set(revision_ids):
            raise MigrationReceiptError(
                "comparison.revision_by_commit must enumerate every imported revision"
            )
    elif revisions_value or len(revision_ids) != 1:
        raise MigrationReceiptError(
            "an empty legacy queue must have one null-commit revision and no commit map"
        )
    _sha256(
        comparison["pre_receipt_candidate_database_sha256"],
        field_name="comparison.pre_receipt_candidate_database_sha256",
    )
    return queue_ids


def _validate_filesystem_identity(entry: Mapping[str, object], *, field_name: str) -> None:
    for name in ("device", "inode", "mtime_ns"):
        _nonnegative_decimal(entry[name], field_name=f"{field_name}.{name}")
    if (
        type(entry["mode"]) is not str
        or _FILE_MODE_PATTERN.fullmatch(cast(str, entry["mode"])) is None
    ):
        raise MigrationReceiptError(f"{field_name}.mode must be four octal digits")


def _validate_success_path_inventory(
    value: object, *, queue_item_ids: tuple[str, ...]
) -> set[tuple[str, str, str]]:
    """Validate exact verified local-path and legacy Git evidence rows."""

    if type(value) is not list:
        raise MigrationReceiptError("path_inventory must be an array")
    known_ids = set(queue_item_ids)
    seen: set[tuple[str, str]] = set()
    continuation_paths: set[tuple[str, str, str]] = set()
    ordering: list[tuple[int, int]] = []
    git_entries: dict[str, dict[str, object]] = {}
    worktree_entries: dict[str, dict[str, object]] = {}
    for index, item in enumerate(cast(list[object], value)):
        field_name = f"path_inventory[{index}]"
        if type(item) is not dict:
            raise MigrationReceiptError(f"{field_name} must be a JSON object")
        entry = cast(dict[str, object], item)
        item_id = _positive_decimal(entry.get("item_id"), field_name=f"{field_name}.item_id")
        if item_id not in known_ids:
            raise MigrationReceiptError(f"{field_name} references an unknown queue item")
        kind = entry.get("kind")
        if kind not in {
            "runner_run_dir",
            "runner_manifest_path",
            "continuation_checkpoint",
            "continuation_checkpoint_metadata",
            "worktree_path",
            "git_ref",
        }:
            raise MigrationReceiptError(f"{field_name}.kind is unsupported")
        assert type(kind) is str
        ordering.append((int(item_id), _PATH_KIND_ORDER[kind]))
        if (item_id, kind) in seen:
            raise MigrationReceiptError(
                f"path_inventory repeats {kind!r} for queue item {item_id}"
            )
        seen.add((item_id, kind))
        base_fields = {
            "item_id", "kind", "path", "scope", "exists", "symlink",
            "expected_type",
        }
        common_evidence = {"disposition", "verified"}
        if kind == "git_ref":
            expected_fields = base_fields | common_evidence | {
                "target", "git_commit"
            }
        elif kind == "worktree_path":
            expected_fields = base_fields | common_evidence | {
                "git_commit", "git_ref", "registered"
            }
            if entry.get("exists") is True:
                expected_fields |= {
                    "actual_type", "device", "inode", "mode", "mtime_ns"
                }
        else:
            expected_fields = base_fields | common_evidence | {
                "actual_type", "device", "inode", "mode", "mtime_ns"
            }
            if kind != "runner_run_dir":
                expected_fields.add("size_bytes")
            if kind == "runner_manifest_path":
                expected_fields.add("sha256")
        _exact_fields(entry, field_name=field_name, expected=expected_fields)
        if type(entry["exists"]) is not bool or type(entry["symlink"]) is not bool:
            raise MigrationReceiptError(
                f"{field_name}.exists and symlink must be booleans"
            )
        if entry["verified"] is not True:
            raise MigrationReceiptError(f"{field_name}.verified must be true")
        if kind in {"git_ref", "worktree_path"} and entry["disposition"] not in _GIT_DISPOSITIONS:
            raise MigrationReceiptError(f"{field_name}.disposition is unsupported")

        if kind == "git_ref":
            expected_ref = f"refs/experiment-queue/items/{item_id}"
            if (
                entry["path"] != expected_ref
                or entry["scope"] != "git-reference"
                or entry["expected_type"] != "git-ref"
                or entry["symlink"] is not False
            ):
                raise MigrationReceiptError(f"{field_name} has invalid Git-ref identity")
            commit = entry["git_commit"]
            target = entry["target"]
            if type(commit) is not str or _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
                raise MigrationReceiptError(f"{field_name}.git_commit is invalid")
            if target is not None and target != commit:
                raise MigrationReceiptError(f"{field_name}.target must equal git_commit")
            if entry["exists"] is not (target is not None):
                raise MigrationReceiptError(f"{field_name}.exists disagrees with target")
            disposition = entry["disposition"]
            if disposition in {"removed", "cleanup-pending-resources-absent"}:
                if entry["exists"] is not False:
                    raise MigrationReceiptError(
                        f"{field_name}.disposition disagrees with ref existence"
                    )
            elif entry["exists"] is not True:
                raise MigrationReceiptError(
                    f"{field_name}.disposition requires an existing ref"
                )
            git_entries[item_id] = entry
            continue

        _absolute_path(entry["path"], field_name=f"{field_name}.path")
        if entry["scope"] not in {"source-state", "legacy-checkout", "external"}:
            raise MigrationReceiptError(f"{field_name}.scope is invalid")
        expected_type = "directory" if kind in {"runner_run_dir", "worktree_path"} else "file"
        if entry["expected_type"] != expected_type or entry["symlink"] is not False:
            raise MigrationReceiptError(f"{field_name} has invalid filesystem type evidence")
        if kind == "worktree_path":
            commit = entry["git_commit"]
            if type(commit) is not str or _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
                raise MigrationReceiptError(f"{field_name}.git_commit is invalid")
            if entry["git_ref"] != f"refs/experiment-queue/items/{item_id}":
                raise MigrationReceiptError(f"{field_name}.git_ref is invalid")
            if type(entry["registered"]) is not bool:
                raise MigrationReceiptError(f"{field_name}.registered must be boolean")
            if entry["exists"] is True:
                if entry["actual_type"] != "directory" or entry["registered"] is not True:
                    raise MigrationReceiptError(f"{field_name} live worktree is not registered")
                _validate_filesystem_identity(entry, field_name=field_name)
            elif entry["registered"] is not False:
                raise MigrationReceiptError(f"{field_name} absent worktree is registered")
            disposition = entry["disposition"]
            if disposition in {"live-pending", "cleanup-pending-live"}:
                if entry["exists"] is not True:
                    raise MigrationReceiptError(
                        f"{field_name}.disposition requires a live worktree"
                    )
            elif disposition in {
                "removed",
                "cleanup-pending-worktree-absent",
                "cleanup-pending-resources-absent",
            }:
                if entry["exists"] is not False:
                    raise MigrationReceiptError(
                        f"{field_name}.disposition requires an absent worktree"
                    )
            else:
                raise MigrationReceiptError(
                    f"{field_name}.disposition cannot describe a recorded worktree path"
                )
            worktree_entries[item_id] = entry
            continue

        if (
            entry["exists"] is not True
            or entry["actual_type"] != expected_type
            or entry["disposition"] != "required-present"
        ):
            raise MigrationReceiptError(f"{field_name} required path is not verified present")
        _validate_filesystem_identity(entry, field_name=field_name)
        if expected_type == "file":
            _nonnegative_decimal(entry["size_bytes"], field_name=f"{field_name}.size_bytes")
        if kind == "runner_manifest_path":
            _sha256(entry["sha256"], field_name=f"{field_name}.sha256")
        if kind in {"continuation_checkpoint", "continuation_checkpoint_metadata"}:
            continuation_paths.add((item_id, kind, cast(str, entry["path"])))
    if ordering != sorted(ordering):
        raise MigrationReceiptError("path_inventory must use queue-item and protocol order")
    for item_id, worktree in worktree_entries.items():
        reference = git_entries.get(item_id)
        if reference is None:
            raise MigrationReceiptError(
                f"path_inventory worktree for queue item {item_id} lacks Git-ref evidence"
            )
        if (
            worktree["git_commit"] != reference["git_commit"]
            or worktree["git_ref"] != reference["path"]
            or worktree["disposition"] != reference["disposition"]
        ):
            raise MigrationReceiptError(
                f"path_inventory Git ref and worktree evidence differ for item {item_id}"
            )
    for item_id in known_ids:
        has_run_dir = (item_id, "runner_run_dir") in seen
        has_manifest = (item_id, "runner_manifest_path") in seen
        if has_run_dir != has_manifest:
            raise MigrationReceiptError(
                f"path_inventory has partial runner evidence for queue item {item_id}"
            )
    return continuation_paths


def _validate_success_continuations(
    value: object,
    *,
    queue_item_ids: tuple[str, ...],
    inventory_paths: set[tuple[str, str, str]],
) -> int:
    if type(value) is not list:
        raise MigrationReceiptError("continuation_checks must be an array")
    checks = cast(list[object], value)
    if len(checks) != len(queue_item_ids):
        raise MigrationReceiptError(
            "continuation_checks must contain exactly one row per queue item"
        )
    file_count = 0
    for index, (item, expected_item_id) in enumerate(zip(checks, queue_item_ids, strict=True)):
        field_name = f"continuation_checks[{index}]"
        check = _exact_fields(
            item,
            field_name=field_name,
            expected={"item_id", "status", "files"},
        )
        if check["item_id"] != expected_item_id:
            raise MigrationReceiptError(
                "continuation_checks must use queue item numeric order"
            )
        files = check["files"]
        if type(files) is not list:
            raise MigrationReceiptError(f"{field_name}.files must be an array")
        expected_kinds = (
            "continuation_checkpoint",
            "continuation_checkpoint_metadata",
        )
        seen_kinds: list[str] = []
        for file_index, item_file in enumerate(cast(list[object], files)):
            file_name = f"{field_name}.files[{file_index}]"
            evidence = _exact_fields(
                item_file,
                field_name=file_name,
                expected={"kind", "path", "sha256", "size_bytes"},
            )
            kind = evidence["kind"]
            if kind not in expected_kinds or cast(str, kind) in seen_kinds:
                raise MigrationReceiptError(f"{file_name}.kind is invalid or repeated")
            seen_kinds.append(cast(str, kind))
            path = _absolute_path(evidence["path"], field_name=f"{file_name}.path")
            _sha256(evidence["sha256"], field_name=f"{file_name}.sha256")
            _nonnegative_decimal(
                evidence["size_bytes"], field_name=f"{file_name}.size_bytes"
            )
            if (expected_item_id, cast(str, kind), path) not in inventory_paths:
                raise MigrationReceiptError(
                    f"{file_name} does not match path_inventory evidence"
                )
            file_count += 1
        if seen_kinds != [kind for kind in expected_kinds if kind in seen_kinds]:
            raise MigrationReceiptError(f"{field_name}.files are not in protocol order")
        expected_status = "verified" if files else "not-applicable"
        if check["status"] != expected_status:
            raise MigrationReceiptError(
                f"{field_name}.status must be {expected_status!r}"
            )
    return file_count


def _validate_succeeded_contract(
    *,
    mode: MigrationMode,
    source: Mapping[str, object],
    destination: Mapping[str, object],
    project: Mapping[str, object],
    comparison: object,
    path_inventory: object,
    continuation_checks: object,
    checks: list[dict[str, object]],
) -> None:
    """Cross-check every exact section emitted for a successful operation."""

    if source["integrity_check"] != "ok" or source["sidecars"] != []:
        raise MigrationReceiptError(
            "a succeeded migration requires source integrity 'ok' and no SQLite sidecars"
        )
    source_state = PurePath(cast(str, source["state_path"]))
    destination_state = PurePath(cast(str, destination["state_path"]))
    if (
        source_state == destination_state
        or source_state in destination_state.parents
        or destination_state in source_state.parents
    ):
        raise MigrationReceiptError(
            "a succeeded migration requires distinct, non-overlapping source and "
            "destination state roots"
        )
    if (
        destination["integrity_check"] != "ok"
        or destination["foreign_key_violations"] != 0
    ):
        raise MigrationReceiptError(
            "a succeeded migration requires destination integrity 'ok' and zero "
            "foreign-key violations"
        )
    if project["id"] is None or not cast(list[object], project["revision_ids"]):
        raise MigrationReceiptError(
            "a succeeded migration requires a Project ID and at least one revision"
        )
    database_sha256 = destination["database_sha256"]
    if mode is MigrationMode.DRY_RUN and database_sha256 is None:
        raise MigrationReceiptError(
            "a succeeded dry run requires the discarded candidate database digest"
        )
    if mode is MigrationMode.IMPORT and database_sha256 is not None:
        raise MigrationReceiptError(
            "a succeeded import database digest must be null because its embedded "
            "receipt is self-referential"
        )

    source_version = cast(int, source["schema_version"])
    queue_item_ids = _validate_success_comparison(
        comparison,
        source_schema_version=source_version,
        project=project,
    )
    inventory_paths = _validate_success_path_inventory(
        path_inventory, queue_item_ids=queue_item_ids
    )
    continuation_file_count = _validate_success_continuations(
        continuation_checks,
        queue_item_ids=queue_item_ids,
        inventory_paths=inventory_paths,
    )
    if continuation_file_count != len(inventory_paths):
        raise MigrationReceiptError(
            "continuation_checks must enumerate every continuation path inventory row"
        )
    if tuple(cast(str, check["name"]) for check in checks) != _SUCCESS_CHECK_NAMES:
        raise MigrationReceiptError(
            "a succeeded migration must contain the exact ordered verification checks"
        )
    statuses = {cast(str, check["name"]): check["status"] for check in checks}
    for name in _SUCCESS_CHECK_NAMES:
        expected_status: str | None = "passed"
        if name == "continuation-evidence" and continuation_file_count == 0:
            expected_status = "not-applicable"
        if name == "atomic-publish" and mode is MigrationMode.DRY_RUN:
            expected_status = "not-applicable"
        if statuses[name] != expected_status:
            raise MigrationReceiptError(
                f"successful check {name!r} must have status {expected_status!r}"
            )


def _validate_checks(value: object) -> bytes:
    if type(value) is not list:
        raise MigrationReceiptError("checks must be a JSON array")
    names: set[str] = set()
    for index, item in enumerate(cast(list[object], value)):
        check = _exact_fields(
            item,
            field_name=f"checks[{index}]",
            expected={"name", "status", "detail"},
        )
        name = _text(check["name"], field_name=f"checks[{index}].name", maximum=256)
        if name in names:
            raise MigrationReceiptError(f"checks repeats check name {name!r}")
        names.add(name)
        try:
            MigrationCheckStatus(check["status"])
        except (TypeError, ValueError) as exc:
            raise MigrationReceiptError(
                f"checks[{index}].status is unsupported: {check['status']!r}"
            ) from exc
        _text(check["detail"], field_name=f"checks[{index}].detail", maximum=16_384)
    return _canonical_section(value, field_name="checks")


@dataclass(frozen=True, slots=True, init=False)
class QueueMigrationReceipt:
    """Immutable, canonical receipt for one dry run, import, or failed attempt."""

    operation_id: str
    mode: MigrationMode
    result: MigrationResult
    project_key: str
    actor: str
    started_at: str
    finished_at: str
    _source_json: bytes = field(repr=False)
    _destination_json: bytes = field(repr=False)
    _project_json: bytes = field(repr=False)
    _comparison_json: bytes = field(repr=False)
    _path_inventory_json: bytes = field(repr=False)
    _continuation_checks_json: bytes = field(repr=False)
    _checks_json: bytes = field(repr=False)
    error: str | None
    canonical_json: bytes = field(repr=False)
    sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "QueueMigrationReceipt is validated-only; use create() or from_bytes()"
        )

    @classmethod
    def create(
        cls,
        *,
        operation_id: str,
        mode: MigrationMode | str,
        result: MigrationResult | str,
        project_key: str,
        actor: str,
        started_at: str,
        finished_at: str,
        source: Mapping[str, object],
        destination: Mapping[str, object],
        project: Mapping[str, object],
        comparison: Mapping[str, object],
        path_inventory: list[object],
        continuation_checks: list[object],
        checks: list[object],
        error: str | None,
    ) -> Self:
        """Validate complete importer evidence and canonicalize it once."""

        if cls is not QueueMigrationReceipt:
            raise TypeError("create() constructs exactly QueueMigrationReceipt")
        identifier = _text(operation_id, field_name="operation_id", maximum=256)
        if _IDENTIFIER_PATTERN.fullmatch(identifier) is None:
            raise MigrationReceiptError("operation_id has invalid syntax")
        try:
            parsed_mode = mode if type(mode) is MigrationMode else MigrationMode(mode)
        except (TypeError, ValueError) as exc:
            raise MigrationReceiptError(f"unsupported migration mode {mode!r}") from exc
        try:
            parsed_result = (
                result if type(result) is MigrationResult else MigrationResult(result)
            )
        except (TypeError, ValueError) as exc:
            raise MigrationReceiptError(f"unsupported migration result {result!r}") from exc
        key = _text(project_key, field_name="project_key", maximum=63)
        receipt_actor = _text(actor, field_name="actor", maximum=256)
        started = _timestamp(started_at, field_name="started_at")
        finished = _timestamp(finished_at, field_name="finished_at")
        if datetime.fromisoformat(started.replace("Z", "+00:00")) > datetime.fromisoformat(
            finished.replace("Z", "+00:00")
        ):
            raise MigrationReceiptError("finished_at must not precede started_at")

        source_json = _validate_source(dict(source))
        destination_json = _validate_destination(dict(destination))
        project_json = _validate_project(dict(project), project_key=key)
        comparison_json = _canonical_section(dict(comparison), field_name="comparison")
        path_json = _canonical_section(list(path_inventory), field_name="path_inventory")
        continuation_json = _canonical_section(
            list(continuation_checks), field_name="continuation_checks"
        )
        checks_json = _validate_checks(list(checks))
        receipt_error = _optional_text(error, field_name="error")
        source_value = cast(dict[str, object], _decode_section(source_json))
        destination_value = cast(dict[str, object], _decode_section(destination_json))
        project_value = cast(dict[str, object], _decode_section(project_json))
        comparison_value = _decode_section(comparison_json)
        path_value = _decode_section(path_json)
        continuation_value = _decode_section(continuation_json)
        check_values = cast(list[dict[str, object]], _decode_section(checks_json))
        has_failed_check = any(
            check["status"] == MigrationCheckStatus.FAILED.value
            for check in check_values
        )
        if parsed_result is MigrationResult.SUCCEEDED:
            if receipt_error is not None or has_failed_check:
                raise MigrationReceiptError(
                    "a succeeded migration receipt cannot contain an error or failed check"
                )
        elif receipt_error is None or not has_failed_check:
            raise MigrationReceiptError(
                "a failed migration receipt requires an error and at least one failed check"
            )
        if (
            parsed_result is MigrationResult.SUCCEEDED
            and destination_value["database_instance_id"] is None
        ):
            raise MigrationReceiptError(
                "a succeeded migration receipt requires a destination database "
                "instance identity"
            )
        if parsed_mode is MigrationMode.DRY_RUN and destination_value["published"] is not False:
            raise MigrationReceiptError("dry-run receipts cannot claim a published destination")
        if (
            parsed_result is MigrationResult.FAILED
            and destination_value["published"] is not False
        ):
            raise MigrationReceiptError(
                "a failed migration receipt cannot claim a published destination"
            )
        if (
            parsed_result is MigrationResult.SUCCEEDED
            and parsed_mode is MigrationMode.IMPORT
            and destination_value["published"] is not True
        ):
            raise MigrationReceiptError(
                "a succeeded import receipt must claim a published destination"
            )
        if parsed_result is MigrationResult.SUCCEEDED:
            _validate_succeeded_contract(
                mode=parsed_mode,
                source=source_value,
                destination=destination_value,
                project=project_value,
                comparison=comparison_value,
                path_inventory=path_value,
                continuation_checks=continuation_value,
                checks=check_values,
            )

        document: dict[str, JSONValue] = {
            **QUEUE_MIGRATION_RECEIPT_V1.document_identity(),
            "operation_id": identifier,
            "mode": parsed_mode.value,
            "result": parsed_result.value,
            "project_key": key,
            "actor": receipt_actor,
            "started_at": started,
            "finished_at": finished,
            "source": _decode_section(source_json),
            "destination": _decode_section(destination_json),
            "project": _decode_section(project_json),
            "comparison": _decode_section(comparison_json),
            "path_inventory": _decode_section(path_json),
            "continuation_checks": _decode_section(continuation_json),
            "checks": _decode_section(checks_json),
            "error": receipt_error,
        }
        canonical = canonical_json_bytes(document)
        if len(canonical) > MAX_RECEIPT_BYTES:
            raise MigrationReceiptError(
                f"receipt exceeds the {MAX_RECEIPT_BYTES}-byte protocol limit"
            )
        return cast(
            Self,
            _construct(
                operation_id=identifier,
                mode=parsed_mode,
                result=parsed_result,
                project_key=key,
                actor=receipt_actor,
                started_at=started,
                finished_at=finished,
                _source_json=source_json,
                _destination_json=destination_json,
                _project_json=project_json,
                _comparison_json=comparison_json,
                _path_inventory_json=path_json,
                _continuation_checks_json=continuation_json,
                _checks_json=checks_json,
                error=receipt_error,
                canonical_json=canonical,
                sha256=sha256_bytes(canonical),
            ),
        )

    @classmethod
    def from_bytes(cls, source: bytes) -> Self:
        """Strictly parse a complete QueueMigrationReceipt/v1 JSON document."""

        if type(source) is not bytes:
            raise TypeError(f"receipt source must be bytes, got {type(source).__name__}")
        if not source or len(source) > MAX_RECEIPT_BYTES:
            raise MigrationReceiptError(
                f"receipt must contain 1 through {MAX_RECEIPT_BYTES} bytes"
            )

        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise MigrationReceiptError(f"receipt repeats JSON key {key!r}")
                result[key] = value
            return result

        def reject_constant(value: str) -> None:
            raise MigrationReceiptError(
                f"receipt contains unsupported JSON constant {value!r}"
            )

        try:
            document = json.loads(
                source.decode("utf-8", errors="strict"),
                object_pairs_hook=reject_duplicates,
                parse_constant=reject_constant,
            )
        except MigrationReceiptError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            raise MigrationReceiptError(f"receipt is not strict UTF-8 JSON: {exc}") from exc
        top = _exact_fields(
            document,
            field_name="receipt",
            expected={
                "apiVersion",
                "kind",
                "operation_id",
                "mode",
                "result",
                "project_key",
                "actor",
                "started_at",
                "finished_at",
                "source",
                "destination",
                "project",
                "comparison",
                "path_inventory",
                "continuation_checks",
                "checks",
                "error",
            },
        )
        try:
            protocol = ProtocolVersion.from_document(top)
        except ProtocolIdentityError as exc:
            raise MigrationReceiptError(f"receipt protocol identity is invalid: {exc}") from exc
        if protocol != QUEUE_MIGRATION_RECEIPT_V1:
            raise MigrationReceiptError(
                f"unsupported receipt protocol {protocol.kind.value}/v{protocol.major}; "
                "expected QueueMigrationReceipt/v1"
            )
        receipt = cls.create(
            operation_id=cast(str, top["operation_id"]),
            mode=cast(str, top["mode"]),
            result=cast(str, top["result"]),
            project_key=cast(str, top["project_key"]),
            actor=cast(str, top["actor"]),
            started_at=cast(str, top["started_at"]),
            finished_at=cast(str, top["finished_at"]),
            source=cast(dict[str, object], top["source"]),
            destination=cast(dict[str, object], top["destination"]),
            project=cast(dict[str, object], top["project"]),
            comparison=cast(dict[str, object], top["comparison"]),
            path_inventory=cast(list[object], top["path_inventory"]),
            continuation_checks=cast(list[object], top["continuation_checks"]),
            checks=cast(list[object], top["checks"]),
            error=cast(str | None, top["error"]),
        )
        if source != receipt.canonical_json:
            raise MigrationReceiptError(
                "receipt bytes must use exact RFC 8785 canonical JSON encoding"
            )
        return receipt

    def to_document(self) -> dict[str, JSONValue]:
        """Return a fresh JSON document detached from immutable receipt evidence."""

        return cast(dict[str, JSONValue], json.loads(self.canonical_json.decode("utf-8")))


__all__ = [
    "MAX_RECEIPT_BYTES",
    "MigrationCheckStatus",
    "MigrationMode",
    "MigrationReceiptError",
    "MigrationResult",
    "QueueMigrationReceipt",
]
