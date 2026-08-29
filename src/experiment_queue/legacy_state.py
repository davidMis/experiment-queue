"""Inspect authentic schema-v1 through v4 queue copies without mutating them.

The offline importer is allowed to consume only a complete, quiescent state
copy.  This module opens its SQLite database in immutable read-only mode,
authenticates the historical table layouts, freezes every row and high-water
value, and verifies Git/card/continuation evidence before mapping begins.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import subprocess
from typing import Final, Mapping, cast

from experiment_queue.legacy import LegacyCardError, LegacyMarkdownCard
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


LEGACY_DATABASE_FILENAME: Final = "queue.sqlite3"
LEGACY_SCHEMA_VERSIONS: Final = frozenset({1, 2, 3, 4})
# Historical DDL/signatures below were extracted locally from these immutable
# repository commits; the importer does not consult Git history at runtime.
LEGACY_SCHEMA_SOURCE_COMMITS: Final[dict[int, str]] = {
    1: "eb7d0c5d16e40643ee2554eaea1970c6217fa126",
    2: "0f8b98d3d0006ae8918d3caf1c518ca72d320178",
    3: "cc68ce9a10b6e9979ebfffedb036be6c502ce36e",
    4: "4569a86a75d559ba99378e54fce301a7415ee57e",
}
IMPORT_BLOCKING_STATES: Final = frozenset(
    {"starting", "running", "yielding", "terminating", "force_killing"}
)
_PENDING_STATES: Final = frozenset({"queued", "held", "blocked"})
_TERMINAL_STATES: Final = frozenset(
    {"succeeded", "failed", "interrupted", "force_killed", "removed"}
)
_FULL_GIT_OBJECT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_GIT_CARD_BYTES: Final = 16 * 1024 * 1024

V1_QUEUE_COLUMNS: Final[tuple[str, ...]] = (
    "id",
    "experiment_id",
    "attempt",
    "state",
    "priority",
    "card_path",
    "card_sha256",
    "command_text",
    "runner_name",
    "git_commit",
    "added_at",
    "added_by",
    "state_detail",
    "assigned_gpu_uuid",
    "assigned_gpu_index",
    "pid",
    "pgid",
    "proc_start_ticks",
    "started_at",
    "finished_at",
    "return_code",
    "terminate_requested_at",
    "terminate_reason",
    "termination_stage",
    "termination_signal_epoch",
    "contention_detected",
    "repo_drift_detected",
    "runner_run_dir",
    "runner_manifest_path",
    "rsync_pull_command",
)

V2_ADDITIONAL_QUEUE_COLUMNS: Final[tuple[str, ...]] = (
    "preemptible",
    "segment",
    "resume_front",
    "yield_requested_at",
    "yield_requested_by",
    "yield_request_id",
    "yield_note",
    "yield_duration_hours",
    "continuation_checkpoint",
    "continuation_checkpoint_sha256",
    "continuation_step",
    "continuation_wandb_id",
)

V3_ADDITIONAL_QUEUE_COLUMNS: Final[tuple[str, ...]] = (
    "git_ref",
    "worktree_path",
    "worktree_created_at",
    "worktree_removed_at",
    "worktree_cleanup_error",
)

V4_ADDITIONAL_QUEUE_COLUMNS: Final[tuple[str, ...]] = (
    "continuation_checkpoint_metadata",
    "continuation_checkpoint_metadata_sha256",
)

LEGACY_QUEUE_COLUMNS: Final[dict[int, tuple[str, ...]]] = {
    1: V1_QUEUE_COLUMNS,
    2: V1_QUEUE_COLUMNS + V2_ADDITIONAL_QUEUE_COLUMNS,
    3: V1_QUEUE_COLUMNS + V2_ADDITIONAL_QUEUE_COLUMNS + V3_ADDITIONAL_QUEUE_COLUMNS,
    4: (
        V1_QUEUE_COLUMNS
        + V2_ADDITIONAL_QUEUE_COLUMNS[:10]
        + V4_ADDITIONAL_QUEUE_COLUMNS
        + V2_ADDITIONAL_QUEUE_COLUMNS[10:]
        + V3_ADDITIONAL_QUEUE_COLUMNS
    ),
}

V4_QUEUE_COLUMNS: Final = LEGACY_QUEUE_COLUMNS[4]

LEGACY_QUEUE_DEFAULTS: Final[dict[str, object]] = {
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
}

_COMMON_TABLE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "metadata": ("key", "value"),
    "dependencies": ("queue_item_id", "dependency_item_id"),
    "gpu_allowlist": (
        "uuid",
        "requested_identifier",
        "last_index",
        "name",
        "enabled",
        "draining",
        "updated_at",
    ),
    "events": (
        "id",
        "created_at",
        "actor",
        "event_type",
        "queue_item_id",
        "payload_json",
    ),
}

_GPU_RESERVATION_COLUMNS: Final[tuple[str, ...]] = (
    "id",
    "gpu_uuid",
    "queue_item_id",
    "status",
    "requested_at",
    "requested_by",
    "note",
    "duration_hours",
    "starts_at",
    "expires_at",
    "released_at",
    "released_by",
    "state_detail",
)

_TABLE_ORDER: Final[dict[str, str]] = {
    "metadata": "key",
    "queue_items": "id",
    "dependencies": "queue_item_id, dependency_item_id",
    "gpu_allowlist": "uuid",
    "events": "id",
    "gpu_reservations": "id",
}

_QUEUE_NOT_NULL: Final = frozenset(
    {
        "experiment_id",
        "attempt",
        "state",
        "priority",
        "card_path",
        "card_sha256",
        "command_text",
        "runner_name",
        "git_commit",
        "added_at",
        "added_by",
        "contention_detected",
        "repo_drift_detected",
        "preemptible",
        "segment",
        "resume_front",
    }
)
_QUEUE_INTEGER_COLUMNS: Final = frozenset(
    {
        "id",
        "attempt",
        "priority",
        "pid",
        "pgid",
        "return_code",
        "contention_detected",
        "repo_drift_detected",
        "preemptible",
        "segment",
        "resume_front",
        "yield_duration_hours",
        "continuation_step",
    }
)
_QUEUE_REAL_COLUMNS: Final = frozenset({"termination_signal_epoch"})
_QUEUE_DEFAULTS_SQL: Final[dict[str, str]] = {
    "priority": "0",
    "contention_detected": "0",
    "repo_drift_detected": "0",
    "preemptible": "0",
    "segment": "1",
    "resume_front": "0",
}

_PATH_COLUMNS: Final[tuple[tuple[str, str, str], ...]] = (
    ("runner_run_dir", "runner_run_dir", "directory"),
    ("runner_manifest_path", "runner_manifest_path", "file"),
    ("continuation_checkpoint", "continuation_checkpoint", "file"),
    (
        "continuation_checkpoint_metadata",
        "continuation_checkpoint_metadata",
        "file",
    ),
    ("worktree_path", "worktree_path", "directory"),
)


class LegacyStateError(RuntimeError):
    """Raised when a legacy state copy cannot be authenticated for import."""

    def __init__(self, message: str, *, probe: LegacySourceProbe | None = None):
        super().__init__(message)
        self.probe = probe


@dataclass(frozen=True, slots=True)
class LegacySourceProbe:
    """Read-only source identity sufficient to produce a failure receipt."""

    state_path: Path
    database_path: Path
    schema_version: int
    database_sha256: str
    database_size_bytes: int
    database_mtime_ns: int
    integrity_check: str
    sidecars: tuple[str, ...]
    state_identity_json: bytes = field(repr=False)
    state_identity_sha256: str

    def receipt_document(self) -> dict[str, JSONValue]:
        """Return the exact QueueMigrationReceipt/v1 source section."""

        return {
            "state_path": str(self.state_path),
            "database_path": str(self.database_path),
            "schema_version": self.schema_version,
            "database_sha256": self.database_sha256,
            "database_size_bytes": self.database_size_bytes,
            "database_mtime_ns": str(self.database_mtime_ns),
            "integrity_check": self.integrity_check,
            "sidecars": list(self.sidecars),
            "state_identity_sha256": self.state_identity_sha256,
        }


@dataclass(frozen=True, slots=True)
class LegacyStateSnapshot:
    """Frozen rows and external evidence from one authentic legacy state copy."""

    probe: LegacySourceProbe
    legacy_checkout: Path
    metadata: tuple[tuple[str, str], ...]
    _queue_items_json: bytes = field(repr=False)
    _dependencies_json: bytes = field(repr=False)
    _gpu_allowlist_json: bytes = field(repr=False)
    _events_json: bytes = field(repr=False)
    _gpu_reservations_json: bytes = field(repr=False)
    sequences: tuple[tuple[str, int], ...]
    _path_inventory_json: bytes = field(repr=False)
    _continuation_checks_json: bytes = field(repr=False)
    commits: tuple[str, ...]
    legacy_defaults: tuple[tuple[str, object], ...]

    @property
    def schema_version(self) -> int:
        return self.probe.schema_version

    @property
    def state_path(self) -> Path:
        return self.probe.state_path

    @property
    def database_path(self) -> Path:
        return self.probe.database_path

    def _rows(self, source: bytes) -> list[dict[str, object]]:
        return _decode_sqlite_rows(source)

    def _json_rows(self, source: bytes) -> list[dict[str, object]]:
        value = json.loads(source.decode("utf-8"))
        assert type(value) is list
        return cast(list[dict[str, object]], value)

    @property
    def queue_items(self) -> list[dict[str, object]]:
        return self._rows(self._queue_items_json)

    @property
    def dependencies(self) -> list[dict[str, object]]:
        return self._rows(self._dependencies_json)

    @property
    def gpu_allowlist(self) -> list[dict[str, object]]:
        return self._rows(self._gpu_allowlist_json)

    @property
    def events(self) -> list[dict[str, object]]:
        return self._rows(self._events_json)

    @property
    def gpu_reservations(self) -> list[dict[str, object]]:
        return self._rows(self._gpu_reservations_json)

    @property
    def path_inventory(self) -> list[dict[str, object]]:
        return self._json_rows(self._path_inventory_json)

    @property
    def continuation_checks(self) -> list[dict[str, object]]:
        return self._json_rows(self._continuation_checks_json)

    def normalized_queue_items(self) -> list[dict[str, object]]:
        """Return source rows expanded only with version-owned compatibility defaults."""

        rows: list[dict[str, object]] = []
        for source in self.queue_items:
            row = dict(source)
            for column in V4_QUEUE_COLUMNS:
                if column not in row:
                    row[column] = LEGACY_QUEUE_DEFAULTS[column]
            rows.append({column: row[column] for column in V4_QUEUE_COLUMNS})
        return rows

    def assert_unchanged(self) -> None:
        """Re-authenticate every frozen source and external evidence identity."""

        identity_json = _state_identity(self.state_path)
        if not hmac.compare_digest(
            sha256_bytes(identity_json), self.probe.state_identity_sha256
        ):
            raise LegacyStateError(
                f"source state copy {self.state_path} changed during migration; "
                "discard the destination and retry from a new consistent copy",
                probe=self.probe,
            )
        queue_items = self.normalized_queue_items()
        try:
            checkout = _canonical_checkout(self.legacy_checkout)
            commits = _verify_git_cards(queue_items, checkout)
            inventory, continuation_checks = _inventory_paths(
                queue_items,
                state_path=self.state_path,
                checkout=checkout,
            )
            inventory_json = canonical_json_bytes(cast(JSONValue, inventory))
            continuation_json = canonical_json_bytes(
                cast(JSONValue, continuation_checks)
            )
        except (LegacyStateError, TypeError, ValueError) as exc:
            raise LegacyStateError(
                f"external migration evidence changed during migration: {exc}",
                probe=self.probe,
            ) from exc
        if (
            commits != self.commits
            or not hmac.compare_digest(inventory_json, self._path_inventory_json)
            or not hmac.compare_digest(
                continuation_json, self._continuation_checks_json
            )
        ):
            raise LegacyStateError(
                "external migration evidence changed during migration; discard the "
                "destination and retry from a new consistent copy",
                probe=self.probe,
            )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        before = path.stat(follow_symlinks=False)
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        after = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise LegacyStateError(f"could not hash source file {path}: {exc}") from exc
    if not stat.S_ISREG(before.st_mode) or not stat.S_ISREG(after.st_mode):
        raise LegacyStateError(f"source file {path} is not a regular file")
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise LegacyStateError(f"source file {path} changed while it was being hashed")
    return digest.hexdigest()


def _sqlite_value_document(value: object) -> list[JSONValue]:
    """Tag one SQLite value so arbitrary 64-bit integers remain exact JSON."""

    if value is None:
        return ["null"]
    if type(value) is int:
        return ["integer", str(value)]
    if type(value) is float:
        if not math.isfinite(value):
            raise LegacyStateError(
                "legacy SQLite contains a non-finite REAL value that cannot be "
                "preserved by schema-v5"
            )
        return ["real", value.hex()]
    if type(value) is str:
        return ["text", value]
    if type(value) is bytes:
        return ["blob", base64.b64encode(value).decode("ascii")]
    raise LegacyStateError(
        f"legacy SQLite returned unsupported value type {type(value).__name__}"
    )


def sqlite_rows_evidence_bytes(rows: list[dict[str, object]]) -> bytes:
    """Canonicalize SQLite rows without narrowing its native value domain."""

    document: list[JSONValue] = [
        {
            key: cast(JSONValue, _sqlite_value_document(value))
            for key, value in row.items()
        }
        for row in rows
    ]
    return canonical_json_bytes(document)


def _decode_sqlite_rows(source: bytes) -> list[dict[str, object]]:
    value = json.loads(source.decode("utf-8"))
    if type(value) is not list:
        raise LegacyStateError("frozen legacy SQLite rows are not an array")
    rows: list[dict[str, object]] = []
    for row_value in value:
        if type(row_value) is not dict:
            raise LegacyStateError("frozen legacy SQLite row is not an object")
        row: dict[str, object] = {}
        for key, tagged in row_value.items():
            if type(key) is not str or type(tagged) is not list or not tagged:
                raise LegacyStateError("frozen legacy SQLite value tag is invalid")
            tag = tagged[0]
            payload = tagged[1] if len(tagged) == 2 else None
            if tag == "null" and len(tagged) == 1:
                decoded: object = None
            elif tag == "integer" and type(payload) is str:
                decoded = int(payload)
            elif tag == "real" and type(payload) is str:
                decoded = float.fromhex(payload)
            elif tag == "text" and type(payload) is str:
                decoded = payload
            elif tag == "blob" and type(payload) is str:
                decoded = base64.b64decode(payload, validate=True)
            else:
                raise LegacyStateError("frozen legacy SQLite value tag is invalid")
            row[key] = decoded
        rows.append(row)
    return rows


def _state_identity(state_path: Path) -> bytes:
    """Hash every state entry without following symlinks or special files."""

    entries: list[dict[str, JSONValue]] = []

    def visit(directory: Path, relative: PurePosixPath) -> None:
        try:
            children = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise LegacyStateError(
                f"could not inventory source state directory {directory}: {exc}"
            ) from exc
        for child in children:
            child_path = directory / child.name
            child_relative = relative / child.name
            try:
                details = child.stat(follow_symlinks=False)
            except OSError as exc:
                raise LegacyStateError(
                    f"could not stat source state entry {child_path}: {exc}"
                ) from exc
            base: dict[str, JSONValue] = {
                "path": child_relative.as_posix(),
                "mode": stat.S_IMODE(details.st_mode),
                # Epoch nanoseconds exceed RFC 8785's interoperable integer
                # domain.  Decimal text retains the exact filesystem value.
                "mtime_ns": str(details.st_mtime_ns),
            }
            if stat.S_ISREG(details.st_mode):
                base.update(
                    {
                        "type": "file",
                        "size": details.st_size,
                        "sha256": _sha256_file(child_path),
                    }
                )
            elif stat.S_ISDIR(details.st_mode):
                base["type"] = "directory"
            elif stat.S_ISLNK(details.st_mode):
                try:
                    target = os.readlink(child_path)
                except OSError as exc:
                    raise LegacyStateError(
                        f"could not read source state symlink {child_path}: {exc}"
                    ) from exc
                base.update({"type": "symlink", "target": target})
            else:
                raise LegacyStateError(
                    f"source state contains unsupported special file {child_path}; "
                    "copy only regular files, directories, and recorded symlinks"
                )
            entries.append(base)
            if stat.S_ISDIR(details.st_mode):
                visit(child_path, child_relative)

    visit(state_path, PurePosixPath("."))
    return canonical_json_bytes(entries)


def _sidecars(database_path: Path) -> tuple[str, ...]:
    candidates = tuple(
        database_path.with_name(database_path.name + suffix)
        for suffix in ("-wal", "-shm", "-journal")
    )
    return tuple(str(path) for path in candidates if path.exists() or path.is_symlink())


def _read_schema_version(database_path: Path) -> tuple[int, str]:
    uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    except sqlite3.Error as exc:
        raise LegacyStateError(
            f"could not open legacy database {database_path} immutable/read-only: {exc}"
        ) from exc
    try:
        try:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise LegacyStateError(
                "legacy database has no readable metadata.schema_version"
            ) from exc
        if row is None or type(row[0]) is not str or not row[0].isdigit():
            raise LegacyStateError(
                f"legacy metadata.schema_version must be text 1 through 4, got "
                f"{None if row is None else row[0]!r}"
            )
        version = int(row[0])
        if version not in LEGACY_SCHEMA_VERSIONS:
            raise LegacyStateError(
                f"legacy database schema v{version} is unsupported; expected 1, 2, 3, or 4"
            )
        try:
            integrity_rows = list(connection.execute("PRAGMA integrity_check"))
        except sqlite3.Error as exc:
            raise LegacyStateError(
                f"could not run source SQLite integrity_check: {exc}"
            ) from exc
        integrity = "; ".join(str(item[0]) for item in integrity_rows) or "no result"
        return version, integrity
    finally:
        connection.close()


def probe_legacy_state(source_state_copy: Path) -> LegacySourceProbe:
    """Capture immutable source identity without accepting it for import yet."""

    supplied = Path(source_state_copy)
    if not supplied.is_absolute():
        raise LegacyStateError(
            f"source state copy must be an absolute directory, got {supplied}"
        )
    if supplied.is_symlink():
        raise LegacyStateError(
            f"source state copy {supplied} must not be a symlink"
        )
    state_path = supplied.resolve()
    if not state_path.is_dir():
        raise LegacyStateError(
            f"source state copy {state_path} is not an existing directory"
        )
    database_path = state_path / LEGACY_DATABASE_FILENAME
    if database_path.is_symlink() or not database_path.is_file():
        raise LegacyStateError(
            f"source state copy must contain regular non-symlink database {database_path}"
        )
    state_identity_json = _state_identity(state_path)
    database_stat = database_path.stat(follow_symlinks=False)
    database_sha256 = _sha256_file(database_path)
    try:
        schema_version, integrity = _read_schema_version(database_path)
    except LegacyStateError as exc:
        raise LegacyStateError(str(exc)) from exc
    return LegacySourceProbe(
        state_path=state_path,
        database_path=database_path,
        schema_version=schema_version,
        database_sha256=database_sha256,
        database_size_bytes=database_stat.st_size,
        database_mtime_ns=database_stat.st_mtime_ns,
        integrity_check=integrity,
        sidecars=_sidecars(database_path),
        state_identity_json=state_identity_json,
        state_identity_sha256=sha256_bytes(state_identity_json),
    )


def _expected_table_columns(version: int) -> dict[str, tuple[str, ...]]:
    expected = dict(_COMMON_TABLE_COLUMNS)
    expected["queue_items"] = LEGACY_QUEUE_COLUMNS[version]
    if version >= 2:
        expected["gpu_reservations"] = _GPU_RESERVATION_COLUMNS
    return expected


def _expected_column_signatures(
    version: int,
) -> dict[str, tuple[tuple[str, str, int, str | None, int], ...]]:
    """Return semantic PRAGMA signatures from the authentic historical DDL."""

    queue: list[tuple[str, str, int, str | None, int]] = []
    for column in LEGACY_QUEUE_COLUMNS[version]:
        declared_type = (
            "INTEGER"
            if column in _QUEUE_INTEGER_COLUMNS
            else "REAL"
            if column in _QUEUE_REAL_COLUMNS
            else "TEXT"
        )
        queue.append(
            (
                column,
                declared_type,
                int(column in _QUEUE_NOT_NULL),
                _QUEUE_DEFAULTS_SQL.get(column),
                int(column == "id"),
            )
        )
    signatures: dict[
        str, tuple[tuple[str, str, int, str | None, int], ...]
    ] = {
        "metadata": (
            ("key", "TEXT", 0, None, 1),
            ("value", "TEXT", 1, None, 0),
        ),
        "queue_items": tuple(queue),
        "dependencies": (
            ("queue_item_id", "INTEGER", 1, None, 1),
            ("dependency_item_id", "INTEGER", 1, None, 2),
        ),
        "gpu_allowlist": (
            ("uuid", "TEXT", 0, None, 1),
            ("requested_identifier", "TEXT", 1, None, 0),
            ("last_index", "TEXT", 1, None, 0),
            ("name", "TEXT", 1, None, 0),
            ("enabled", "INTEGER", 1, None, 0),
            ("draining", "INTEGER", 1, None, 0),
            ("updated_at", "TEXT", 1, None, 0),
        ),
        "events": (
            ("id", "INTEGER", 0, None, 1),
            ("created_at", "TEXT", 1, None, 0),
            ("actor", "TEXT", 1, None, 0),
            ("event_type", "TEXT", 1, None, 0),
            ("queue_item_id", "INTEGER", 0, None, 0),
            ("payload_json", "TEXT", 1, None, 0),
        ),
    }
    if version >= 2:
        signatures["gpu_reservations"] = (
            ("id", "INTEGER", 0, None, 1),
            ("gpu_uuid", "TEXT", 1, None, 0),
            ("queue_item_id", "INTEGER", 0, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("requested_at", "TEXT", 1, None, 0),
            ("requested_by", "TEXT", 1, None, 0),
            ("note", "TEXT", 1, None, 0),
            ("duration_hours", "INTEGER", 1, None, 0),
            ("starts_at", "TEXT", 0, None, 0),
            ("expires_at", "TEXT", 0, None, 0),
            ("released_at", "TEXT", 0, None, 0),
            ("released_by", "TEXT", 0, None, 0),
            ("state_detail", "TEXT", 0, None, 0),
        )
    return signatures


def _authenticate_schema(connection: sqlite3.Connection, version: int) -> None:
    expected = _expected_table_columns(version)
    expected_signatures = _expected_column_signatures(version)
    actual_tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }
    if actual_tables != set(expected):
        raise LegacyStateError(
            f"schema-v{version} tables differ from the authentic historical layout; "
            f"expected {sorted(expected)}, got {sorted(actual_tables)}"
        )
    for table, columns in expected.items():
        details = tuple(connection.execute(f"PRAGMA table_info({table})"))
        actual_columns = tuple(str(row[1]) for row in details)
        actual_signature = tuple(
            (
                str(row[1]),
                str(row[2]).upper(),
                int(row[3]),
                None if row[4] is None else str(row[4]),
                int(row[5]),
            )
            for row in details
        )
        if actual_columns != columns:
            raise LegacyStateError(
                f"schema-v{version} table {table} has columns {actual_columns}, "
                f"expected authentic historical columns {columns}"
            )
        if actual_signature != expected_signatures[table]:
            raise LegacyStateError(
                f"schema-v{version} table {table} declarations differ from the "
                "authentic historical DDL"
            )
    views_or_triggers = list(
        connection.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('view', 'trigger') ORDER BY type, name"
        )
    )
    if views_or_triggers:
        raise LegacyStateError(
            "legacy database contains unsupported views or triggers: "
            + ", ".join(f"{row[0]} {row[1]}" for row in views_or_triggers)
        )


def _read_rows(connection: sqlite3.Connection, table: str) -> list[dict[str, object]]:
    order = _TABLE_ORDER[table]
    return [
        dict(row)
        for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")
    ]


def _git(
    checkout: Path,
    *arguments: str,
    maximum_bytes: int = _MAX_GIT_CARD_BYTES,
) -> bytes:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_")
        and key not in {"SSH_ASKPASS", "GCM_INTERACTIVE"}
    }
    environment.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GCM_INTERACTIVE": "never",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
        }
    )
    try:
        completed = subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            check=False,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30.0,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LegacyStateError(
            f"could not run Git plumbing in legacy checkout {checkout}: {exc}"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise LegacyStateError(
            f"Git {' '.join(arguments)} failed in legacy checkout {checkout}: "
            f"{detail or f'exit {completed.returncode}'}"
        )
    if len(completed.stdout) > maximum_bytes:
        raise LegacyStateError(
            f"Git {' '.join(arguments)} returned {len(completed.stdout)} bytes; "
            f"limit is {maximum_bytes}"
        )
    return completed.stdout


def _canonical_checkout(value: Path) -> Path:
    supplied = Path(value)
    if not supplied.is_absolute():
        raise LegacyStateError(f"legacy checkout must be absolute, got {supplied}")
    if supplied.is_symlink():
        raise LegacyStateError(f"legacy checkout {supplied} must not be a symlink")
    try:
        checkout = supplied.resolve(strict=True)
    except OSError as exc:
        raise LegacyStateError(
            f"legacy checkout {supplied} does not resolve: {exc}"
        ) from exc
    if not checkout.is_dir():
        raise LegacyStateError(f"legacy checkout {checkout} is not a directory")
    _git(checkout, "rev-parse", "--git-dir", maximum_bytes=4096)
    top_level_bytes = _git(
        checkout, "rev-parse", "--show-toplevel", maximum_bytes=16 * 1024
    )
    try:
        top_level = Path(top_level_bytes.decode("utf-8", errors="strict").strip()).resolve(
            strict=True
        )
    except (UnicodeDecodeError, OSError) as exc:
        raise LegacyStateError(
            f"legacy checkout {checkout} has an unreadable Git top-level: {exc}"
        ) from exc
    if top_level != checkout:
        raise LegacyStateError(
            f"legacy checkout {checkout} is not the repository top-level {top_level}"
        )
    return checkout


def _verify_git_cards(
    queue_items: list[dict[str, object]],
    checkout: Path,
) -> tuple[str, ...]:
    commits: list[str] = []
    verified_commits: set[str] = set()
    for item in queue_items:
        item_id = int(item["id"])
        commit_value = item["git_commit"]
        if type(commit_value) is not str or _FULL_GIT_OBJECT.fullmatch(commit_value) is None:
            raise LegacyStateError(
                f"queue item {item_id} git_commit must be a lowercase full 40- or "
                f"64-character object ID, got {commit_value!r}"
            )
        commit = commit_value
        if commit not in verified_commits:
            _git(checkout, "cat-file", "-e", f"{commit}^{{commit}}", maximum_bytes=4096)
            verified_commits.add(commit)
            commits.append(commit)
        card_path_value = item["card_path"]
        if type(card_path_value) is not str:
            raise LegacyStateError(f"queue item {item_id} card_path must be text")
        card_path = PurePosixPath(card_path_value)
        if (
            card_path.is_absolute()
            or "\\" in card_path_value
            or any(part in {"", ".", ".."} for part in card_path_value.split("/"))
            or any(ord(character) < 32 or ord(character) == 127 for character in card_path_value)
        ):
            raise LegacyStateError(
                f"queue item {item_id} card_path {card_path_value!r} is not a "
                "portable repository-relative path"
            )
        source = _git(checkout, "show", f"{commit}:{card_path.as_posix()}")
        try:
            card = LegacyMarkdownCard.from_source(
                source,
                experiment_id=cast(str, item["experiment_id"]),
                source_name=card_path.as_posix(),
            )
        except (LegacyCardError, TypeError) as exc:
            raise LegacyStateError(
                f"queue item {item_id} legacy card evidence is invalid: {exc}"
            ) from exc
        expected_hash = item["card_sha256"]
        if card.source_sha256 != expected_hash:
            raise LegacyStateError(
                f"queue item {item_id} card hash mismatch at {commit}:{card_path}; "
                f"database has {expected_hash!r}, Git bytes hash to {card.source_sha256}"
            )
        if card.command_text != item["command_text"] or card.runner_name != item["runner_name"]:
            raise LegacyStateError(
                f"queue item {item_id} command/runner evidence differs from exact "
                f"legacy card {commit}:{card_path}"
            )
    return tuple(commits)


def _git_text(source: bytes, *, label: str) -> str:
    """Decode bounded Git plumbing output without accepting malformed text."""

    try:
        return source.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError as exc:
        raise LegacyStateError(f"{label} returned non-UTF-8 Git output") from exc


def _git_common_directory(checkout: Path) -> Path:
    """Resolve the repository identity shared by its linked worktrees."""

    value = _git_text(
        _git(checkout, "rev-parse", "--git-common-dir", maximum_bytes=16 * 1024),
        label=f"legacy checkout {checkout} common directory",
    )
    common = Path(value)
    if not common.is_absolute():
        common = checkout / common
    try:
        return common.resolve(strict=True)
    except OSError as exc:
        raise LegacyStateError(
            f"legacy checkout {checkout} Git common directory cannot be resolved: {exc}"
        ) from exc


def _git_ref_target(checkout: Path, reference: str) -> str | None:
    """Read one exact ref without treating an absent historical ref as Git failure."""

    source = _git(
        checkout,
        "for-each-ref",
        "--format=%(refname)%00%(objectname)",
        reference,
        maximum_bytes=64 * 1024,
    )
    matches: list[str] = []
    for line in source.splitlines():
        try:
            name_source, object_source = line.split(b"\x00", 1)
            name = name_source.decode("utf-8", errors="strict")
            object_id = object_source.decode("ascii", errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise LegacyStateError(
                f"Git returned malformed evidence while resolving ref {reference!r}"
            ) from exc
        if name == reference:
            matches.append(object_id)
    if len(matches) > 1:
        raise LegacyStateError(
            f"Git returned repeated exact evidence for legacy ref {reference!r}"
        )
    return matches[0] if matches else None


def _registered_worktrees(checkout: Path) -> dict[Path, str]:
    """Return exact registered worktree paths and HEADs from NUL-safe plumbing."""

    source = _git(
        checkout,
        "worktree",
        "list",
        "--porcelain",
        "-z",
        maximum_bytes=16 * 1024 * 1024,
    )
    registered: dict[Path, str] = {}
    for record in source.split(b"\x00\x00"):
        if not record:
            continue
        fields = record.split(b"\x00")
        if not fields or not fields[0].startswith(b"worktree "):
            raise LegacyStateError("Git returned malformed worktree-list evidence")
        raw_path = os.fsdecode(fields[0][len(b"worktree ") :])
        head_fields = [field for field in fields[1:] if field.startswith(b"HEAD ")]
        if len(head_fields) != 1:
            raise LegacyStateError(
                f"Git worktree record for {raw_path!r} lacks one exact HEAD"
            )
        try:
            head = head_fields[0][len(b"HEAD ") :].decode("ascii", errors="strict")
        except UnicodeDecodeError as exc:
            raise LegacyStateError(
                f"Git worktree record for {raw_path!r} has a malformed HEAD"
            ) from exc
        path = Path(raw_path).resolve(strict=False)
        if path in registered:
            raise LegacyStateError(f"Git repeats registered worktree path {path}")
        registered[path] = head
    return registered


def _verify_live_worktree(
    *,
    item_id: int,
    path: Path,
    commit: str,
    card_path: str,
    card_sha256: object,
    checkout_common: Path,
    registered: Mapping[Path, str],
) -> None:
    """Authenticate one present legacy worktree without requiring it to be clean."""

    if path.is_symlink() or not path.is_dir():
        raise LegacyStateError(
            f"queue item {item_id} recorded worktree {path} must be a present "
            "non-symlink directory"
        )
    try:
        canonical = path.resolve(strict=True)
    except OSError as exc:
        raise LegacyStateError(
            f"queue item {item_id} recorded worktree {path} cannot be resolved: {exc}"
        ) from exc
    if canonical != path:
        raise LegacyStateError(
            f"queue item {item_id} recorded worktree {path} is not its canonical path"
        )
    registered_head = registered.get(path)
    if registered_head is None:
        raise LegacyStateError(
            f"queue item {item_id} recorded worktree {path} is not registered by "
            "the supplied legacy checkout"
        )
    if registered_head != commit:
        raise LegacyStateError(
            f"queue item {item_id} registered worktree {path} HEAD {registered_head} "
            f"differs from recorded commit {commit}"
        )
    top = Path(
        _git_text(
            _git(path, "rev-parse", "--show-toplevel", maximum_bytes=16 * 1024),
            label=f"queue item {item_id} worktree top-level",
        )
    ).resolve(strict=True)
    if top != path:
        raise LegacyStateError(
            f"queue item {item_id} recorded worktree {path} is not exact Git top-level {top}"
        )
    head = _git_text(
        _git(path, "rev-parse", "HEAD", maximum_bytes=4096),
        label=f"queue item {item_id} worktree HEAD",
    )
    if head != commit:
        raise LegacyStateError(
            f"queue item {item_id} worktree HEAD {head} differs from recorded commit {commit}"
        )
    if _git_common_directory(path) != checkout_common:
        raise LegacyStateError(
            f"queue item {item_id} worktree {path} belongs to another Git repository"
        )
    card = path / PurePosixPath(card_path)
    if card.is_symlink() or not card.is_file():
        raise LegacyStateError(
            f"queue item {item_id} committed card is not a regular file in worktree {path}"
        )
    actual_card_sha256 = _sha256_file(card)
    if type(card_sha256) is not str or not hmac.compare_digest(
        actual_card_sha256, card_sha256
    ):
        raise LegacyStateError(
            f"queue item {item_id} card bytes changed inside recorded worktree {path}"
        )


def _verify_legacy_git_runtime(
    item: Mapping[str, object],
    *,
    checkout: Path,
    checkout_common: Path,
    registered: Mapping[Path, str],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Authenticate ref/worktree state according to its recorded cleanup disposition."""

    item_id = int(item["id"])
    state = item.get("state")
    if state not in _PENDING_STATES | _TERMINAL_STATES:
        raise LegacyStateError(
            f"queue item {item_id} has unsupported non-running state {state!r}"
        )
    commit = cast(str, item["git_commit"])
    reference = item.get("git_ref")
    worktree_value = item.get("worktree_path")
    created_at = item.get("worktree_created_at")
    removed_at = item.get("worktree_removed_at")
    cleanup_error = item.get("worktree_cleanup_error")

    if reference is None:
        if any(
            value is not None
            for value in (worktree_value, created_at, removed_at, cleanup_error)
        ):
            raise LegacyStateError(
                f"queue item {item_id} records worktree lifecycle evidence without "
                "its legacy Git ref"
            )
        return None, None
    expected_ref = f"refs/experiment-queue/items/{item_id}"
    if type(reference) is not str or reference != expected_ref:
        raise LegacyStateError(
            f"queue item {item_id} git_ref must be exact historical ref "
            f"{expected_ref!r}, got {reference!r}"
        )
    if (worktree_value is None) != (created_at is None):
        raise LegacyStateError(
            f"queue item {item_id} worktree_path and worktree_created_at must be "
            "recorded together"
        )
    if worktree_value is not None and type(worktree_value) is not str:
        raise LegacyStateError(
            f"queue item {item_id} worktree_path must be absolute text or NULL, "
            f"got {worktree_value!r}"
        )
    if removed_at is not None and cleanup_error is not None:
        raise LegacyStateError(
            f"queue item {item_id} cannot record both completed worktree removal "
            "and a cleanup error"
        )
    if state in _PENDING_STATES and (removed_at is not None or cleanup_error is not None):
        raise LegacyStateError(
            f"pending queue item {item_id} cannot record terminal worktree cleanup evidence"
        )

    target = _git_ref_target(checkout, reference)
    if target is not None and target != commit:
        raise LegacyStateError(
            f"queue item {item_id} legacy ref {reference!r} points to {target}, "
            f"not recorded commit {commit}"
        )
    path = None if worktree_value is None else Path(worktree_value)
    if path is not None and (not path.is_absolute() or str(path) != worktree_value):
        raise LegacyStateError(
            f"queue item {item_id} worktree_path must be an exact absolute path"
        )
    path_exists = path is not None and os.path.lexists(path)
    path_registered = path is not None and path.resolve(strict=False) in registered

    if removed_at is not None:
        if target is not None:
            raise LegacyStateError(
                f"queue item {item_id} records completed cleanup but legacy ref "
                f"{reference!r} still exists"
            )
        if path_exists or path_registered:
            raise LegacyStateError(
                f"queue item {item_id} records completed cleanup but worktree {path} "
                "still exists or remains registered"
            )
        disposition = "removed"
    elif path_exists:
        if target is None:
            raise LegacyStateError(
                f"queue item {item_id} has a live recorded worktree but legacy ref "
                f"{reference!r} is missing"
            )
        assert path is not None
        _verify_live_worktree(
            item_id=item_id,
            path=path,
            commit=commit,
            card_path=cast(str, item["card_path"]),
            card_sha256=item.get("card_sha256"),
            checkout_common=checkout_common,
            registered=registered,
        )
        disposition = (
            "live-pending" if state in _PENDING_STATES else "cleanup-pending-live"
        )
    else:
        if path_registered:
            raise LegacyStateError(
                f"queue item {item_id} worktree path {path} is missing but Git still "
                "registers it"
            )
        if state in _PENDING_STATES and path is not None:
            raise LegacyStateError(
                f"pending queue item {item_id} recorded worktree {path} is missing"
            )
        if state in _PENDING_STATES and target is None:
            raise LegacyStateError(
                f"pending queue item {item_id} legacy ref {reference!r} is missing"
            )
        disposition = (
            "pinned-no-worktree"
            if target is not None and path is None
            else "cleanup-pending-worktree-absent"
            if target is not None
            else "cleanup-pending-resources-absent"
        )

    ref_evidence: dict[str, object] = {
        "item_id": str(item_id),
        "kind": "git_ref",
        "path": reference,
        "scope": "git-reference",
        "exists": target is not None,
        "symlink": False,
        "expected_type": "git-ref",
        "target": target,
        "git_commit": commit,
        "disposition": disposition,
        "verified": True,
    }
    try:
        worktree_identity = (
            _filesystem_identity(path.stat(follow_symlinks=False))
            if path_exists and path is not None
            else {}
        )
    except OSError as exc:
        raise LegacyStateError(
            f"queue item {item_id} could not re-stat recorded worktree {path}: {exc}"
        ) from exc
    worktree_evidence = (
        None
        if path is None
        else {
            **({"actual_type": "directory"} if path_exists else {}),
            **worktree_identity,
            "git_commit": commit,
            "git_ref": reference,
            "registered": path_registered,
            "disposition": disposition,
            "verified": True,
        }
    )
    return ref_evidence, worktree_evidence


def _path_scope(path: Path, *, state_path: Path, checkout: Path) -> str:
    resolved = path.resolve(strict=False)
    if resolved == state_path or state_path in resolved.parents:
        return "source-state"
    if resolved == checkout or checkout in resolved.parents:
        return "legacy-checkout"
    return "external"


def _filesystem_identity(details: os.stat_result) -> dict[str, object]:
    """Return comparison-safe identity fields for one verified filesystem object."""

    return {
        "device": str(details.st_dev),
        "inode": str(details.st_ino),
        "mode": f"{stat.S_IMODE(details.st_mode):04o}",
        "mtime_ns": str(details.st_mtime_ns),
    }


def _verify_continuation_file(
    *,
    item_id: int,
    kind: str,
    path_value: object,
    digest_value: object,
) -> dict[str, object]:
    if type(path_value) is not str or not Path(path_value).is_absolute():
        raise LegacyStateError(
            f"queue item {item_id} {kind} must be an absolute path, got {path_value!r}"
        )
    if type(digest_value) is not str or _SHA256.fullmatch(digest_value) is None:
        raise LegacyStateError(
            f"queue item {item_id} {kind} digest must be lowercase SHA-256"
        )
    path = Path(path_value)
    if path.is_symlink() or not path.is_file():
        raise LegacyStateError(
            f"queue item {item_id} {kind} {path} must be a regular non-symlink file"
        )
    actual = _sha256_file(path)
    if not hmac.compare_digest(actual, digest_value):
        raise LegacyStateError(
            f"queue item {item_id} {kind} digest mismatch: database has "
            f"{digest_value}, file hashes to {actual}"
        )
    return {
        "kind": kind,
        "path": str(path),
        "sha256": actual,
        "size_bytes": str(path.stat().st_size),
    }


def _inventory_paths(
    queue_items: list[dict[str, object]],
    *,
    state_path: Path,
    checkout: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    inventory: list[dict[str, object]] = []
    continuation_checks: list[dict[str, object]] = []
    checkout_common = _git_common_directory(checkout)
    registered_worktrees = _registered_worktrees(checkout)
    for item in queue_items:
        item_id = int(item["id"])
        runner_run_dir = item.get("runner_run_dir")
        runner_manifest = item.get("runner_manifest_path")
        if (runner_run_dir is None) != (runner_manifest is None):
            raise LegacyStateError(
                f"queue item {item_id} has partial runner run-directory/manifest "
                "evidence"
            )
        ref_evidence, worktree_evidence = _verify_legacy_git_runtime(
            item,
            checkout=checkout,
            checkout_common=checkout_common,
            registered=registered_worktrees,
        )
        for column, kind, expected_type in _PATH_COLUMNS:
            value = item.get(column)
            if value is None:
                continue
            if type(value) is not str or not Path(value).is_absolute():
                raise LegacyStateError(
                    f"queue item {item_id} {column} must be an absolute path or NULL, "
                    f"got {value!r}"
                )
            path = Path(value)
            exists = path.exists() or path.is_symlink()
            entry: dict[str, object] = {
                "item_id": str(item_id),
                "kind": kind,
                "path": value,
                "scope": _path_scope(path, state_path=state_path, checkout=checkout),
                "exists": exists,
                "symlink": path.is_symlink(),
                "expected_type": expected_type,
            }
            if kind == "worktree_path" and worktree_evidence is not None:
                entry.update(worktree_evidence)
            elif kind != "worktree_path":
                if not exists:
                    raise LegacyStateError(
                        f"queue item {item_id} required referenced {kind} {path} "
                        "does not exist"
                    )
                if path.is_symlink():
                    raise LegacyStateError(
                        f"queue item {item_id} required referenced {kind} {path} "
                        "must not be a symlink"
                    )
                try:
                    details = path.stat(follow_symlinks=False)
                except OSError as exc:
                    raise LegacyStateError(
                        f"queue item {item_id} could not stat required referenced "
                        f"{kind} {path}: {exc}"
                    ) from exc
                expected_mode = (
                    stat.S_ISDIR(details.st_mode)
                    if expected_type == "directory"
                    else stat.S_ISREG(details.st_mode)
                )
                if not expected_mode:
                    raise LegacyStateError(
                        f"queue item {item_id} required referenced {kind} {path} "
                        f"must be a {expected_type}"
                    )
                entry.update(
                    {
                        "actual_type": expected_type,
                        "disposition": "required-present",
                        "verified": True,
                        **_filesystem_identity(details),
                    }
                )
                if expected_type == "file":
                    entry["size_bytes"] = str(details.st_size)
                if kind == "runner_manifest_path":
                    entry["sha256"] = _sha256_file(path)
            inventory.append(entry)
        if ref_evidence is not None:
            inventory.append(ref_evidence)
        checks: list[dict[str, object]] = []
        checkpoint = item.get("continuation_checkpoint")
        checkpoint_hash = item.get("continuation_checkpoint_sha256")
        if (checkpoint is None) != (checkpoint_hash is None):
            raise LegacyStateError(
                f"queue item {item_id} has partial continuation checkpoint evidence"
            )
        if checkpoint is not None:
            checks.append(
                _verify_continuation_file(
                    item_id=item_id,
                    kind="continuation_checkpoint",
                    path_value=checkpoint,
                    digest_value=checkpoint_hash,
                )
            )
        metadata = item.get("continuation_checkpoint_metadata")
        metadata_hash = item.get("continuation_checkpoint_metadata_sha256")
        if (metadata is None) != (metadata_hash is None):
            raise LegacyStateError(
                f"queue item {item_id} has partial continuation metadata evidence"
            )
        if metadata is not None:
            checks.append(
                _verify_continuation_file(
                    item_id=item_id,
                    kind="continuation_checkpoint_metadata",
                    path_value=metadata,
                    digest_value=metadata_hash,
                )
            )
        continuation_checks.append(
            {
                "item_id": str(item_id),
                "status": "verified" if checks else "not-applicable",
                "files": checks,
            }
        )
    return inventory, continuation_checks


def load_legacy_state(
    source_state_copy: Path,
    *,
    legacy_checkout: Path,
) -> LegacyStateSnapshot:
    """Read and fully verify one quiescent authentic v1-v4 state copy."""

    probe = probe_legacy_state(source_state_copy)
    if probe.sidecars:
        raise LegacyStateError(
            "source state copy contains unresolved SQLite sidecars "
            f"{list(probe.sidecars)}; create a consistent SQLite backup/copy after "
            "both legacy writers stop",
            probe=probe,
        )
    if probe.integrity_check != "ok":
        raise LegacyStateError(
            f"source SQLite integrity_check failed: {probe.integrity_check}",
            probe=probe,
        )
    checkout = _canonical_checkout(legacy_checkout)
    uri = f"{probe.database_path.as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=30.0)
        connection.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        raise LegacyStateError(
            f"could not reopen source database immutable/read-only: {exc}",
            probe=probe,
        ) from exc
    try:
        _authenticate_schema(connection, probe.schema_version)
        violations = list(connection.execute("PRAGMA foreign_key_check"))
        if violations:
            raise LegacyStateError(
                f"source SQLite foreign_key_check found {len(violations)} violation(s)",
                probe=probe,
        )
        metadata_rows = _read_rows(connection, "metadata")
        if any(
            type(row["key"]) is not str or type(row["value"]) is not str
            for row in metadata_rows
        ):
            raise LegacyStateError(
                "legacy metadata keys and values must retain their historical TEXT type",
                probe=probe,
            )
        metadata = tuple(
            (cast(str, row["key"]), cast(str, row["value"]))
            for row in metadata_rows
        )
        metadata_map = dict(metadata)
        if metadata_map.get("schema_version") != str(probe.schema_version):
            raise LegacyStateError(
                "source schema_version changed between immutable inspection passes",
                probe=probe,
            )
        recorded_repo = metadata_map.get("repo_root")
        if recorded_repo is None or not Path(recorded_repo).is_absolute():
            raise LegacyStateError(
                "legacy metadata.repo_root must be an absolute checkout path",
                probe=probe,
            )
        if Path(recorded_repo).resolve(strict=False) != checkout:
            raise LegacyStateError(
                f"legacy metadata.repo_root records {recorded_repo}, not supplied "
                f"checkout {checkout}",
                probe=probe,
            )
        queue_items = _read_rows(connection, "queue_items")
        dependencies = _read_rows(connection, "dependencies")
        allowlist = _read_rows(connection, "gpu_allowlist")
        events = _read_rows(connection, "events")
        reservations = (
            _read_rows(connection, "gpu_reservations")
            if probe.schema_version >= 2
            else []
        )
        sequence_rows = list(
            connection.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        )
        sequences = tuple((str(row[0]), int(row[1])) for row in sequence_rows)
        maxima = {
            "queue_items": max((int(row["id"]) for row in queue_items), default=0),
            "events": max((int(row["id"]) for row in events), default=0),
            "gpu_reservations": max(
                (int(row["id"]) for row in reservations), default=0
            ),
        }
        for name, value in sequences:
            if name not in maxima or value < maxima[name]:
                raise LegacyStateError(
                    f"source sqlite_sequence entry {name!r}={value} is invalid",
                    probe=probe,
                )
    except LegacyStateError as exc:
        if exc.probe is None:
            exc.probe = probe
        raise
    except sqlite3.Error as exc:
        raise LegacyStateError(
            f"could not read authenticated legacy rows: {exc}", probe=probe
        ) from exc
    finally:
        connection.close()

    blocking = [
        (int(item["id"]), str(item["state"]))
        for item in queue_items
        if item["state"] in IMPORT_BLOCKING_STATES
    ]
    if blocking:
        rendered = ", ".join(f"{item_id}:{state}" for item_id, state in blocking)
        raise LegacyStateError(
            "production import requires an idle legacy queue; running-like items "
            f"remain: {rendered}",
            probe=probe,
        )
    try:
        commits = _verify_git_cards(queue_items, checkout)
        normalized = []
        defaults = {
            column: value
            for column, value in LEGACY_QUEUE_DEFAULTS.items()
            if column not in LEGACY_QUEUE_COLUMNS[probe.schema_version]
        }
        for source in queue_items:
            row = dict(source)
            row.update({name: value for name, value in defaults.items() if name not in row})
            normalized.append(row)
        inventory, continuation_checks = _inventory_paths(
            normalized,
            state_path=probe.state_path,
            checkout=checkout,
        )
    except LegacyStateError as exc:
        if exc.probe is None:
            exc.probe = probe
        raise

    try:
        snapshot = LegacyStateSnapshot(
            probe=probe,
            legacy_checkout=checkout,
            metadata=metadata,
            _queue_items_json=sqlite_rows_evidence_bytes(queue_items),
            _dependencies_json=sqlite_rows_evidence_bytes(dependencies),
            _gpu_allowlist_json=sqlite_rows_evidence_bytes(allowlist),
            _events_json=sqlite_rows_evidence_bytes(events),
            _gpu_reservations_json=sqlite_rows_evidence_bytes(reservations),
            sequences=sequences,
            _path_inventory_json=canonical_json_bytes(cast(JSONValue, inventory)),
            _continuation_checks_json=canonical_json_bytes(
                cast(JSONValue, continuation_checks)
            ),
            commits=commits,
            legacy_defaults=tuple(defaults.items()),
        )
    except (LegacyStateError, TypeError, ValueError) as exc:
        raise LegacyStateError(
            f"legacy rows cannot be frozen as exact migration evidence: {exc}",
            probe=probe,
        ) from exc
    snapshot.assert_unchanged()
    return snapshot


__all__ = [
    "IMPORT_BLOCKING_STATES",
    "LEGACY_DATABASE_FILENAME",
    "LEGACY_QUEUE_COLUMNS",
    "LEGACY_QUEUE_DEFAULTS",
    "LEGACY_SCHEMA_SOURCE_COMMITS",
    "LEGACY_SCHEMA_VERSIONS",
    "LegacySourceProbe",
    "LegacyStateError",
    "LegacyStateSnapshot",
    "V4_QUEUE_COLUMNS",
    "load_legacy_state",
    "probe_legacy_state",
    "sqlite_rows_evidence_bytes",
]
