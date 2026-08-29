"""Preserve the frozen schema-v4 cooperative-yield contract after v5 import.

This adapter is intentionally limited to imported ``LegacyMarkdownCard/v0``
items.  It neither guesses at legacy documents nor permits new admissions to
use the untyped v0 protocol.  Request state is persisted before an exact v0
document is atomically published and the authenticated process group is sent
``SIGINT``.  A ready receipt is accepted only after its checkpoint,
continuation, runner, and progress evidence has been revalidated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sqlite3
import stat
import tempfile
import time
from typing import Final, Mapping, cast

from experiment_queue.attempt_runtime import (
    PreparedAttempt,
    process_identity_matches,
    signal_recorded_process,
)
from experiment_queue.protocols import RUNNER_RECEIPT_V1
from experiment_queue.scheduler_v5 import V5SchedulerError, V5SchedulingController
from experiment_queue.serialization import JSONValue, canonical_json_bytes
from experiment_queue.v5_repository import (
    V5ProjectRepository,
    V5QueueItem,
    V5RepositoryError,
)


_ADMISSION_KIND: Final = "LegacyMarkdownCard/v0"
_REQUEST_KIND: Final = "manual_preemption"
_YIELD_EXIT_CODE: Final = 75
_SIGNAL_ATTEMPT_LEASE_SECONDS: Final = 5.0
_MAX_CONTROL_BYTES: Final = 8 * 1024 * 1024
_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_PROGRESS_UNIT_PATTERN: Final = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,31}\Z")
_SHARED_LEGACY_PATHS: Final[tuple[str, ...]] = (
    ".env",
    ".venv",
    "data",
    "experiments",
    "figures",
    "logs",
    "model",
    "out",
    "outputs",
    "oxy_updates",
    "papers",
    "runs",
)


class LegacyV0ContinuationError(RuntimeError):
    """Raised when grandfathered v0 continuation cannot proceed safely."""


class _CoordinatorEvidence:
    """Prevent callers from manufacturing an authenticated pending operation."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is created only by "
            "LegacyV0ContinuationCoordinator"
        )


@dataclass(frozen=True, slots=True, init=False)
class LegacyV0PendingContinuation(_CoordinatorEvidence):
    """Persisted and published v0 request awaiting one exact legacy receipt."""

    project_id: int
    revision_id: int
    queue_item_id: int
    segment: int
    gpu_uuid: str
    request_id: str
    requested_at: str
    requested_by: str
    note: str
    request_path: Path
    receipt_path: Path
    request_source: bytes = field(repr=False)
    request_sha256: str
    runner_run_id: str | None
    runner_run_directory: Path | None
    runner_manifest: Path | None
    rsync_pull_command: str | None = field(repr=False)
    allowed_run_roots: tuple[Path, ...] = field(repr=False)

    @property
    def request_document(self) -> dict[str, JSONValue]:
        """Return a detached representation of the frozen v0 request."""

        return _request_document(
            request_id=self.request_id,
            queue_item_id=self.queue_item_id,
            segment=self.segment,
            gpu_uuid=self.gpu_uuid,
            requested_at=self.requested_at,
            requested_by=self.requested_by,
            note=self.note,
        )


@dataclass(frozen=True, slots=True)
class LegacyV0ContinuationOutcome:
    """Result of accepting one failed or ready v0 cooperative-yield receipt."""

    item: V5QueueItem
    request_id: str
    receipt_sha256: str
    requeued: bool
    resumed_running: bool
    checkpoint: Path | None = None
    checkpoint_sha256: str | None = None
    checkpoint_metadata: Path | None = None
    checkpoint_metadata_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _RunnerEvidence:
    """Strict RunnerReceipt/v1 subset needed to bind legacy continuation."""

    run_id: str
    status: str
    return_code: int | None
    run_directory: Path
    manifest: Path
    rsync_pull_command: str | None


@dataclass(frozen=True, slots=True)
class _ReadyEvidence:
    """Normalized ready receipt and revalidated checkpoint identities."""

    source: bytes = field(repr=False)
    document: dict[str, object] = field(repr=False)
    progress: dict[str, object] | None
    checkpoint: Path
    checkpoint_bytes: int
    checkpoint_sha256: str
    metadata: Path
    metadata_bytes: int
    metadata_sha256: str
    step: int
    wandb_id: str | None


def _construct_pending(**values: object) -> LegacyV0PendingContinuation:
    pending = object.__new__(LegacyV0PendingContinuation)
    for name, value in values.items():
        object.__setattr__(pending, name, value)
    return cast(LegacyV0PendingContinuation, pending)


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise LegacyV0ContinuationError(f"{field_name} must be a positive integer")
    return value


def _text(
    value: object,
    *,
    field_name: str,
    maximum: int = 4096,
) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise LegacyV0ContinuationError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise LegacyV0ContinuationError(
            f"{field_name} must be log-safe text of at most {maximum} characters"
        )
    return value


def _timestamp(value: object, *, field_name: str) -> str:
    timestamp = _text(value, field_name=field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise LegacyV0ContinuationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LegacyV0ContinuationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        )
    return timestamp


def _request_document(
    *,
    request_id: str,
    queue_item_id: int,
    segment: int,
    gpu_uuid: str,
    requested_at: str,
    requested_by: str,
    note: str,
) -> dict[str, JSONValue]:
    """Build the frozen schema-v4 manual-preemption document shape."""

    return {
        "schema_version": 1,
        "request_kind": _REQUEST_KIND,
        "request_id": request_id,
        "queue_item_id": queue_item_id,
        "segment": segment,
        "gpu_uuid": gpu_uuid,
        "requested_at": requested_at,
        "requested_by": requested_by,
        "note": note,
    }


def _v0_wire_bytes(document: Mapping[str, object]) -> bytes:
    """Encode exactly as schema-v4 ``_atomic_write_json`` encoded documents."""

    try:
        return (
            json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise LegacyV0ContinuationError(
            f"legacy v0 document is not finite UTF-8 JSON: {exc}"
        ) from exc


def _read_regular(path: Path, *, label: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        raise LegacyV0ContinuationError(f"{label} is missing: {path}") from exc
    except OSError as exc:
        raise LegacyV0ContinuationError(
            f"could not open {label} {path}: {exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise LegacyV0ContinuationError(
                f"{label} is not a regular file: {path}"
            )
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            source = stream.read(_MAX_CONTROL_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(source) > _MAX_CONTROL_BYTES:
        raise LegacyV0ContinuationError(
            f"{label} exceeds {_MAX_CONTROL_BYTES} bytes: {path}"
        )
    if not source:
        raise LegacyV0ContinuationError(f"{label} is empty: {path}")
    return source


def _decode_document(source: bytes, *, label: str) -> dict[str, object]:
    def without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise LegacyV0ContinuationError(
                    f"{label} repeats JSON object key {key!r}"
                )
            document[key] = value
        return document

    try:
        value = json.loads(
            source.decode("utf-8", errors="strict"),
            object_pairs_hook=without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON token {token}")
            ),
        )
    except LegacyV0ContinuationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise LegacyV0ContinuationError(
            f"{label} is not strict UTF-8 JSON: {exc}"
        ) from exc
    if type(value) is not dict:
        raise LegacyV0ContinuationError(f"{label} must contain one JSON object")
    return cast(dict[str, object], value)


def _atomic_publish(path: Path, source: bytes, *, root: Path) -> None:
    """Atomically replace the one mutable v0 request path under its segment."""

    if not path.is_absolute() or path.parent != root:
        raise LegacyV0ContinuationError(
            f"legacy yield request path {path} is not directly under segment root {root}"
        )
    try:
        resolved_root = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise LegacyV0ContinuationError(
            f"segment root {root} cannot be resolved before publication: {exc}"
        ) from exc
    if resolved_root != root or not root.is_dir():
        raise LegacyV0ContinuationError(
            f"segment root changed canonical identity before publication: {root}"
        )
    if path.is_symlink():
        raise LegacyV0ContinuationError(
            f"refused to replace symlinked legacy yield request {path}"
        )
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=root
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        temporary = None
    except OSError as exc:
        raise LegacyV0ContinuationError(
            f"could not publish legacy yield request {path}: {exc}"
        ) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _unlink_regular_control_file(path: Path, *, label: str) -> None:
    """Remove one authenticated scheduler control file without following links."""

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise LegacyV0ContinuationError(f"could not inspect {label} {path}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise LegacyV0ContinuationError(
            f"refused to remove non-regular {label}: {path}"
        )
    try:
        path.unlink()
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except OSError as exc:
        raise LegacyV0ContinuationError(f"could not remove {label} {path}: {exc}") from exc


def _path_inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _canonical_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise LegacyV0ContinuationError(
            f"{label} must be an absolute non-symlink directory: {path}"
        )
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise LegacyV0ContinuationError(f"{label} cannot be resolved: {exc}") from exc
    if resolved != path or not resolved.is_dir():
        raise LegacyV0ContinuationError(
            f"{label} changed canonical identity or is not a directory: {path}"
        )
    return resolved


def _allowed_run_roots(prepared: PreparedAttempt) -> tuple[Path, ...]:
    roots: list[Path] = [
        _canonical_directory(prepared.worktree, label="legacy execution root")
    ]
    primary_value = prepared.environment.get("EXPERIMENT_QUEUE_PRIMARY_REPO")
    if primary_value:
        primary = _canonical_directory(
            Path(primary_value), label="legacy primary checkout"
        )
        roots.append(primary)
        for name in _SHARED_LEGACY_PATHS:
            candidate = primary / name
            if not candidate.exists() and not candidate.is_symlink():
                continue
            try:
                resolved = candidate.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise LegacyV0ContinuationError(
                    f"legacy shared path {candidate} cannot be resolved: {exc}"
                ) from exc
            if resolved.is_dir():
                roots.append(resolved)
    return tuple(dict.fromkeys(roots))


def _runner_path(value: object, *, field_name: str, receipt_path: Path) -> Path:
    if type(value) is not str or not value or not Path(value).is_absolute():
        raise LegacyV0ContinuationError(
            f"runner receipt {receipt_path} field {field_name} must be an absolute path"
        )
    path = Path(value)
    try:
        return path.resolve()
    except (OSError, RuntimeError) as exc:
        raise LegacyV0ContinuationError(
            f"runner receipt {receipt_path} field {field_name} cannot be resolved: {exc}"
        ) from exc


def _runner_receipt_from_path(
    path: Path,
    *,
    queue_item_id: int,
    segment: int,
    allowed_roots: tuple[Path, ...],
) -> _RunnerEvidence | None:
    """Read the exact structured receipt when the grandfathered runner emits it."""

    if not path.exists() and not path.is_symlink():
        return None
    source = _read_regular(path, label="runner receipt")
    document = _decode_document(source, label="runner receipt")
    required = {
        "apiVersion",
        "kind",
        "run_id",
        "queue_item_id",
        "segment",
        "status",
        "return_code",
        "run_directory",
        "manifest",
        "logs",
        "sync",
        "written_at",
    }
    if set(document) != required:
        raise LegacyV0ContinuationError(
            "runner receipt has invalid fields; expected exact "
            f"{sorted(required)}, got {sorted(document)}"
        )
    identity = RUNNER_RECEIPT_V1.document_identity()
    if any(document[name] != value for name, value in identity.items()):
        raise LegacyV0ContinuationError(
            f"runner receipt {path} has unsupported protocol identity"
        )
    run_id = _text(document["run_id"], field_name="runner receipt run_id", maximum=256)
    if (
        type(document["queue_item_id"]) is not int
        or document["queue_item_id"] != queue_item_id
        or type(document["segment"]) is not int
        or document["segment"] != segment
    ):
        raise LegacyV0ContinuationError(
            "runner receipt is not for this exact queue item and segment"
        )
    status_value = document["status"]
    statuses = {
        "running",
        "succeeded",
        "failed",
        "yielded",
        "interrupted",
        "launch_failed",
    }
    if type(status_value) is not str or status_value not in statuses:
        raise LegacyV0ContinuationError(
            f"runner receipt has invalid status {status_value!r}"
        )
    status = status_value
    return_code_value = document["return_code"]
    if status == "running":
        if return_code_value is not None:
            raise LegacyV0ContinuationError(
                "running runner receipt must have a null return_code"
            )
        return_code = None
    elif type(return_code_value) is not int:
        raise LegacyV0ContinuationError(
            "terminal runner receipt must have an integer return_code"
        )
    else:
        return_code = return_code_value
    expected_codes = {"succeeded": 0, "yielded": 75, "interrupted": 130, "launch_failed": 2}
    if status in expected_codes and return_code != expected_codes[status]:
        raise LegacyV0ContinuationError(
            f"runner receipt status {status!r} requires return_code "
            f"{expected_codes[status]}"
        )
    if status == "failed" and return_code == 0:
        raise LegacyV0ContinuationError(
            "failed runner receipt cannot have return_code zero"
        )

    run_directory = _runner_path(
        document["run_directory"], field_name="run_directory", receipt_path=path
    )
    if not allowed_roots or not any(
        run_directory != root and _path_inside(run_directory, root)
        for root in allowed_roots
    ):
        raise LegacyV0ContinuationError(
            f"runner run directory {run_directory} is outside authorized legacy "
            f"roots {[str(root) for root in allowed_roots]}"
        )
    manifest = _runner_path(
        document["manifest"], field_name="manifest", receipt_path=path
    )
    logs = document["logs"]
    if type(logs) is not dict or set(logs) != {"stdout", "stderr"}:
        raise LegacyV0ContinuationError(
            "runner receipt logs must contain exactly stdout and stderr"
        )
    subordinate = [manifest]
    for name in ("stdout", "stderr"):
        subordinate.append(
            _runner_path(logs[name], field_name=f"logs.{name}", receipt_path=path)
        )
    if any(not _path_inside(candidate, run_directory) for candidate in subordinate):
        raise LegacyV0ContinuationError(
            "runner receipt manifest or log path escapes its run directory"
        )
    sync = document["sync"]
    if sync is None:
        pull_command = None
    elif (
        type(sync) is not dict
        or set(sync) != {"type", "command"}
        or sync.get("type") != "rsync-pull"
    ):
        raise LegacyV0ContinuationError(
            "runner receipt sync must be null or exact rsync-pull evidence"
        )
    else:
        pull_command = _text(
            sync.get("command"),
            field_name="runner receipt sync command",
            maximum=16_384,
        )
    _timestamp(document["written_at"], field_name="runner receipt written_at")
    return _RunnerEvidence(
        run_id=run_id,
        status=status,
        return_code=return_code,
        run_directory=run_directory,
        manifest=manifest,
        rsync_pull_command=pull_command,
    )


def _runner_receipt(
    prepared: PreparedAttempt,
    *,
    allowed_roots: tuple[Path, ...],
) -> _RunnerEvidence | None:
    return _runner_receipt_from_path(
        prepared.paths.segment_root / "runner.json",
        queue_item_id=prepared.queue_item_id,
        segment=prepared.segment,
        allowed_roots=allowed_roots,
    )


def _progress(document: Mapping[str, object]) -> dict[str, object] | None:
    if "progress" not in document:
        return None
    value = document["progress"]
    if type(value) is not dict:
        raise LegacyV0ContinuationError("yield receipt progress must be an object")
    unit = value.get("unit")
    if type(unit) is not str or _PROGRESS_UNIT_PATTERN.fullmatch(unit) is None:
        raise LegacyV0ContinuationError(
            "yield receipt progress unit must be a 1-32 character ASCII token "
            "starting with a letter"
        )
    completed = value.get("completed")
    if type(completed) is not int or completed < 0:
        raise LegacyV0ContinuationError(
            "yield receipt progress completed must be an integer >= 0"
        )
    normalized: dict[str, object] = {"unit": unit, "completed": completed}
    if "total" in value:
        total = value["total"]
        if type(total) is not int or total < completed:
            raise LegacyV0ContinuationError(
                "yield receipt progress total must be an integer >= completed"
            )
        normalized["total"] = total
    if set(value) != set(normalized):
        raise LegacyV0ContinuationError(
            "yield receipt progress contains unsupported fields"
        )
    return normalized


def _file_evidence(
    value: object,
    *,
    field_name: str,
    allowed_roots: tuple[Path, ...],
) -> tuple[Path, int, str]:
    if type(value) is not str or not value or not Path(value).is_absolute():
        raise LegacyV0ContinuationError(
            f"yield receipt {field_name} must be an absolute path"
        )
    supplied = Path(value)
    if supplied.is_symlink():
        raise LegacyV0ContinuationError(
            f"yield receipt {field_name} must not be a symlink: {supplied}"
        )
    try:
        resolved = supplied.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise LegacyV0ContinuationError(
            f"yield receipt {field_name} cannot be resolved: {exc}"
        ) from exc
    if resolved != supplied:
        raise LegacyV0ContinuationError(
            f"yield receipt {field_name} changed canonical target: {supplied}"
        )
    if not any(_path_inside(resolved, root) for root in allowed_roots):
        raise LegacyV0ContinuationError(
            f"yield receipt {field_name} {resolved} is outside authorized roots "
            f"{[str(root) for root in allowed_roots]}"
        )
    try:
        descriptor = os.open(resolved, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise LegacyV0ContinuationError(
            f"could not open yield receipt {field_name} {resolved}: {exc}"
        ) from exc
    digest = hashlib.sha256()
    size = 0
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise LegacyV0ContinuationError(
                f"yield receipt {field_name} is not a regular file: {resolved}"
            )
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (metadata.st_dev, metadata.st_ino, metadata.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ):
            raise LegacyV0ContinuationError(
                f"yield receipt {field_name} changed while it was authenticated"
            )
    finally:
        os.close(descriptor)
    return resolved, size, digest.hexdigest()


def _validate_receipt_identity(
    document: Mapping[str, object],
    pending: LegacyV0PendingContinuation,
) -> str:
    if document.get("schema_version") != 1:
        raise LegacyV0ContinuationError(
            "legacy yield receipt schema_version must be exactly 1"
        )
    status_value = document.get("status")
    if status_value not in {"ready", "failed"}:
        raise LegacyV0ContinuationError(
            f"legacy yield receipt status must be 'ready' or 'failed', got {status_value!r}"
        )
    if document.get("request_id") != pending.request_id:
        raise LegacyV0ContinuationError(
            "legacy yield receipt request identity does not match the persisted request"
        )
    if (
        type(document.get("queue_item_id")) is not int
        or document["queue_item_id"] != pending.queue_item_id
    ):
        raise LegacyV0ContinuationError(
            "legacy yield receipt queue-item identity does not match"
        )
    return cast(str, status_value)


def _validate_step(
    document: Mapping[str, object],
    *,
    progress: Mapping[str, object] | None,
) -> int:
    step = document.get("step")
    label = "continuation" if progress is not None else "optimizer"
    if type(step) is not int:
        raise LegacyV0ContinuationError(
            f"legacy yield receipt has no valid {label} step"
        )
    if step < 0 or (progress is None and step < 1):
        raise LegacyV0ContinuationError(
            f"legacy yield receipt has invalid {label} step {step}"
        )
    return step


def _ready_evidence(
    *,
    source: bytes,
    document: dict[str, object],
    pending: LegacyV0PendingContinuation,
) -> _ReadyEvidence:
    required = {
        "schema_version",
        "status",
        "request_id",
        "queue_item_id",
        "step",
        "checkpoint",
        "checkpoint_metadata",
        "checkpoint_bytes",
        "checkpoint_sha256",
        "wandb",
    }
    allowed = required | {"progress"}
    if not required.issubset(document) or not set(document).issubset(allowed):
        raise LegacyV0ContinuationError(
            "ready legacy yield receipt has invalid fields; expected exact v0 "
            f"fields {sorted(required)} with optional progress"
        )
    progress = _progress(document)
    step = _validate_step(document, progress=progress)
    roots = (
        (pending.runner_run_directory,)
        if pending.runner_run_directory is not None
        else pending.allowed_run_roots
    )
    checkpoint, checkpoint_bytes, checkpoint_sha256 = _file_evidence(
        document["checkpoint"],
        field_name="checkpoint",
        allowed_roots=roots,
    )
    metadata, metadata_bytes, metadata_sha256 = _file_evidence(
        document["checkpoint_metadata"],
        field_name="checkpoint_metadata",
        allowed_roots=roots,
    )
    claimed_bytes = document["checkpoint_bytes"]
    if type(claimed_bytes) is not int or claimed_bytes < 0:
        raise LegacyV0ContinuationError(
            "legacy yield receipt checkpoint_bytes must be an integer >= 0"
        )
    if claimed_bytes != checkpoint_bytes:
        raise LegacyV0ContinuationError(
            "legacy yield checkpoint size differs from its receipt"
        )
    claimed_sha256 = document["checkpoint_sha256"]
    if (
        type(claimed_sha256) is not str
        or _SHA256_PATTERN.fullmatch(claimed_sha256) is None
        or not hmac.compare_digest(claimed_sha256, checkpoint_sha256)
    ):
        raise LegacyV0ContinuationError(
            "legacy yield checkpoint SHA-256 differs from its receipt"
        )
    wandb = document["wandb"]
    if wandb is None:
        wandb_id = None
    elif type(wandb) is not dict or set(wandb) != {"id"}:
        raise LegacyV0ContinuationError(
            "legacy yield receipt wandb must be null or contain exactly id"
        )
    else:
        wandb_id = _text(
            wandb["id"], field_name="legacy yield receipt wandb.id", maximum=256
        )
    return _ReadyEvidence(
        source=source,
        document=document,
        progress=progress,
        checkpoint=checkpoint,
        checkpoint_bytes=checkpoint_bytes,
        checkpoint_sha256=checkpoint_sha256,
        metadata=metadata,
        metadata_bytes=metadata_bytes,
        metadata_sha256=metadata_sha256,
        step=step,
        wandb_id=wandb_id,
    )


def _failed_detail(document: dict[str, object]) -> tuple[dict[str, object] | None, str]:
    required = {
        "schema_version",
        "status",
        "request_id",
        "queue_item_id",
        "step",
        "error",
    }
    allowed = required | {"progress"}
    if not required.issubset(document) or not set(document).issubset(allowed):
        raise LegacyV0ContinuationError(
            "failed legacy yield receipt has invalid fields; expected exact v0 "
            f"fields {sorted(required)} with optional progress"
        )
    progress = _progress(document)
    _validate_step(document, progress=progress)
    error = _text(
        document["error"], field_name="legacy yield receipt error", maximum=4096
    )
    return progress, error


def _event(
    connection: sqlite3.Connection,
    *,
    created_at: str,
    actor: str,
    event_type: str,
    project_id: int,
    queue_item_id: int,
    payload: Mapping[str, JSONValue],
) -> None:
    connection.execute(
        """
        INSERT INTO events(
            created_at, actor, event_type, queue_item_id, payload_json,
            scope, project_id
        ) VALUES (?, ?, ?, ?, ?, 'project', ?)
        """,
        (
            created_at,
            actor,
            event_type,
            queue_item_id,
            canonical_json_bytes(cast(JSONValue, dict(payload))).decode("utf-8"),
            project_id,
        ),
    )


class LegacyV0ContinuationCoordinator:
    """Coordinate manual yield for imported v4 rows and no other admissions."""

    def __init__(self, repository: V5ProjectRepository):
        if type(repository) is not V5ProjectRepository:
            raise TypeError(
                f"repository must be exactly V5ProjectRepository, got "
                f"{type(repository).__name__}"
            )
        self.repository = repository

    def _raw_item(self, item_id: int) -> sqlite3.Row:
        try:
            with self.repository.store.connect() as connection:
                row = connection.execute(
                    "SELECT * FROM queue_items WHERE id = ?", (item_id,)
                ).fetchone()
        except Exception as exc:
            raise LegacyV0ContinuationError(
                f"could not load legacy queue item {item_id}: {exc}"
            ) from exc
        if row is None:
            raise LegacyV0ContinuationError(
                f"legacy queue item {item_id} does not exist"
            )
        return row

    def _discard_recorded_failed_receipt(self, item_id: int, path: Path) -> None:
        """Clear only a stale receipt already authenticated by a durable event."""

        if not path.exists() and not path.is_symlink():
            return
        source = _read_regular(path, label="stale legacy yield receipt")
        digest = hashlib.sha256(source).hexdigest()
        with self.repository.store.connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json FROM events
                WHERE queue_item_id = ?
                  AND event_type = 'COOPERATIVE_YIELD_FAILED'
                ORDER BY id DESC LIMIT 1
                """,
                (item_id,),
            ).fetchone()
        try:
            payload = None if row is None else json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise LegacyV0ContinuationError(
                "could not authenticate stale legacy failed-receipt event"
            ) from exc
        if type(payload) is not dict or payload.get("receipt_sha256") != digest:
            raise LegacyV0ContinuationError(
                "legacy yield receipt path already contains unrecorded evidence; "
                "refusing a new preemption request"
            )
        _unlink_regular_control_file(path, label="recorded failed legacy yield receipt")

    @staticmethod
    def _validate_prepared(item: V5QueueItem, prepared: PreparedAttempt) -> None:
        if type(prepared) is not PreparedAttempt:
            raise TypeError(
                f"prepared must be exactly PreparedAttempt, got "
                f"{type(prepared).__name__}"
            )
        expected = (
            item.id,
            item.project_id,
            item.revision_id,
            item.experiment_id,
            item.attempt,
            item.segment,
            item.git_commit,
            _ADMISSION_KIND,
            None,
            "legacy-shell",
        )
        actual = (
            prepared.queue_item_id,
            prepared.project_id,
            prepared.project_revision_id,
            prepared.experiment_id,
            prepared.attempt,
            prepared.segment,
            prepared.git_commit,
            prepared.admission_kind,
            prepared.resolved_spec_sha256,
            prepared.command_kind,
        )
        if actual != expected:
            raise LegacyV0ContinuationError(
                "PreparedAttempt identity differs from the imported legacy queue item"
            )

    def _isolate(
        self,
        item_id: int,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        terminal: bool,
        cause: BaseException,
    ) -> None:
        try:
            self.repository.isolate_continuation_failure(
                item_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
                terminal=terminal,
            )
        except V5RepositoryError as isolation_error:
            raise LegacyV0ContinuationError(
                f"{reason}; item isolation did not override a concurrent state "
                f"transition: {isolation_error}"
            ) from cause
        raise LegacyV0ContinuationError(reason) from cause

    def _raise_signal_uncertainty(
        self,
        *,
        item: V5QueueItem,
        request_id: str,
        actor: str,
        changed_at: str,
        detail: str,
        cause: BaseException,
    ) -> None:
        """Quarantine dispatch without destroying ambiguous legacy evidence.

        A published v0 request may have reached the process even when the
        signaling helper raises or returns false.  Keep the item ``yielding``
        and retain its PID, process group, and GPU assignment so the bounded
        legacy recovery path can authenticate the eventual receipt or process
        outcome.
        """

        reason = (
            f"legacy manual-yield request {request_id!r} has uncertain SIGINT "
            f"delivery: {detail}; the active yielding state is preserved for "
            "recovery"
        )
        try:
            V5SchedulingController(self.repository.store).quarantine_project(
                item.project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
                queue_item_id=item.id,
            )
        except V5SchedulerError as quarantine_error:
            raise LegacyV0ContinuationError(
                f"{reason}; Project quarantine failed: {quarantine_error}"
            ) from cause
        raise LegacyV0ContinuationError(reason) from cause

    def request_manual_yield(
        self,
        prepared: PreparedAttempt,
        *,
        note: str,
        actor: str,
        requested_at: str,
        request_id: str | None = None,
    ) -> LegacyV0PendingContinuation:
        """Persist, atomically publish, and signal one grandfathered v0 request."""

        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(requested_at, field_name="requested_at")
        if type(note) is not str or "\x00" in note:
            raise LegacyV0ContinuationError("note must be NUL-free text")
        reason = " ".join(note.split()).strip() or "manual operator preemption"
        reason = _text(reason, field_name="note", maximum=200)
        item = self.repository.get_queue_item(prepared.queue_item_id)
        self._validate_prepared(item, prepared)
        if (
            item.state != "running"
            or item.admission_kind != _ADMISSION_KIND
            or item.snapshot is not None
            or not item.preemptible
        ):
            raise LegacyV0ContinuationError(
                f"queue item {item.id} must be a running, imported, explicitly "
                "preemptible LegacyMarkdownCard/v0 admission"
            )
        self._discard_recorded_failed_receipt(item.id, prepared.paths.yield_receipt)
        allowed_roots = _allowed_run_roots(prepared)
        runner = _runner_receipt(prepared, allowed_roots=allowed_roots)
        if runner is not None and runner.status != "running":
            raise LegacyV0ContinuationError(
                f"runner receipt is {runner.status!r}; manual yield requires a "
                "stably running segment"
            )
        request_key = (
            f"legacy-manual:{item.id}:{item.segment}:{secrets.token_hex(12)}"
            if request_id is None
            else _text(request_id, field_name="request_id", maximum=256)
        )

        try:
            with self.repository.store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM queue_items WHERE id = ?", (item.id,)
                ).fetchone()
                if row is None:
                    raise LegacyV0ContinuationError(
                        f"legacy queue item {item.id} disappeared"
                    )
                if (
                    row["state"] != "running"
                    or row["admission_kind"] != _ADMISSION_KIND
                    or not bool(row["preemptible"])
                    or int(row["segment"]) != item.segment
                ):
                    raise LegacyV0ContinuationError(
                        f"legacy queue item {item.id} changed before its yield "
                        "request could be persisted"
                    )
                gpu_uuid = _text(
                    row["assigned_gpu_uuid"],
                    field_name="assigned GPU UUID",
                    maximum=256,
                )
                if row["pid"] is None or row["pgid"] is None:
                    raise LegacyV0ContinuationError(
                        f"legacy queue item {item.id} lacks persisted process identity"
                    )
                pid = _positive_integer(row["pid"], field_name="pid")
                pgid = _positive_integer(row["pgid"], field_name="pgid")
                process_start_ticks = (
                    None
                    if row["proc_start_ticks"] is None
                    else str(row["proc_start_ticks"])
                )
                document = _request_document(
                    request_id=request_key,
                    queue_item_id=item.id,
                    segment=item.segment,
                    gpu_uuid=gpu_uuid,
                    requested_at=timestamp,
                    requested_by=event_actor,
                    note=reason,
                )
                source = _v0_wire_bytes(document)
                updated = connection.execute(
                    """
                    UPDATE queue_items
                    SET state = 'yielding', yield_requested_at = ?,
                        yield_requested_by = ?, yield_request_id = ?, yield_note = ?,
                        yield_duration_hours = NULL, state_detail = ?,
                        runner_run_dir = COALESCE(?, runner_run_dir),
                        runner_manifest_path = COALESCE(?, runner_manifest_path),
                        rsync_pull_command = COALESCE(?, rsync_pull_command)
                    WHERE id = ? AND state = 'running' AND segment = ?
                    """,
                    (
                        timestamp,
                        event_actor,
                        request_key,
                        reason,
                        f"checkpointing for manual preemption: {reason}",
                        None if runner is None else str(runner.run_directory),
                        None if runner is None else str(runner.manifest),
                        None if runner is None else runner.rsync_pull_command,
                        item.id,
                        item.segment,
                    ),
                )
                if updated.rowcount != 1:
                    raise LegacyV0ContinuationError(
                        f"legacy queue item {item.id} changed during yield persistence"
                    )
                _event(
                    connection,
                    created_at=timestamp,
                    actor=event_actor,
                    event_type="MANUAL_PREEMPTION_REQUESTED",
                    project_id=item.project_id,
                    queue_item_id=item.id,
                    payload={
                        "request_id": request_key,
                        "gpu_uuid": gpu_uuid,
                        "segment": item.segment,
                        "reason": reason,
                        "protocol": "CooperativeYieldRequest/v0",
                        "request_sha256": hashlib.sha256(source).hexdigest(),
                    },
                )
        except LegacyV0ContinuationError:
            raise
        except Exception as exc:
            raise LegacyV0ContinuationError(
                f"could not persist legacy manual-yield request: {exc}"
            ) from exc

        try:
            _atomic_publish(
                prepared.paths.yield_request,
                source,
                root=prepared.paths.segment_root,
            )
        except LegacyV0ContinuationError as exc:
            with self.repository.store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                restored = connection.execute(
                    """
                    UPDATE queue_items
                    SET state = 'running', state_detail = ?,
                        yield_requested_at = NULL, yield_requested_by = NULL,
                        yield_request_id = NULL, yield_note = NULL,
                        yield_duration_hours = NULL
                    WHERE id = ? AND state = 'yielding'
                      AND segment = ? AND yield_request_id = ?
                    """,
                    (
                        f"legacy manual preemption request could not be published: {exc}",
                        item.id,
                        item.segment,
                        request_key,
                    ),
                )
                _event(
                    connection,
                    created_at=timestamp,
                    actor=event_actor,
                    event_type="MANUAL_PREEMPTION_REQUEST_FAILED",
                    project_id=item.project_id,
                    queue_item_id=item.id,
                    payload={
                        "request_id": request_key,
                        "error": str(exc),
                        "item_state_restored": restored.rowcount == 1,
                    },
                )
            raise LegacyV0ContinuationError(
                f"persisted legacy manual-yield request could not be published: {exc}"
            ) from exc

        pending = _construct_pending(
            project_id=item.project_id,
            revision_id=item.revision_id,
            queue_item_id=item.id,
            segment=item.segment,
            gpu_uuid=gpu_uuid,
            request_id=request_key,
            requested_at=timestamp,
            requested_by=event_actor,
            note=reason,
            request_path=prepared.paths.yield_request,
            receipt_path=prepared.paths.yield_receipt,
            request_source=source,
            request_sha256=hashlib.sha256(source).hexdigest(),
            runner_run_id=None if runner is None else runner.run_id,
            runner_run_directory=None if runner is None else runner.run_directory,
            runner_manifest=None if runner is None else runner.manifest,
            rsync_pull_command=None if runner is None else runner.rsync_pull_command,
            allowed_run_roots=allowed_roots,
        )
        controller = V5SchedulingController(self.repository.store)
        claim = controller.claim_manual_yield_signal_attempt(
            item.id,
            request_id=request_key,
            attempt_token=secrets.token_hex(16),
            signal_epoch=time.time(),
            retry_after_seconds=_SIGNAL_ATTEMPT_LEASE_SECONDS,
            actor=event_actor,
            changed_at=timestamp,
        )
        if claim is None:
            return pending
        try:
            signaled = signal_recorded_process(
                pid=pid,
                pgid=pgid,
                process_start_ticks=process_start_ticks,
                signum=signal.SIGINT,
            )
        except Exception as exc:
            try:
                controller.record_manual_yield_signal_result(
                    claim,
                    delivered=False,
                    detail=f"signal operation raised {exc}",
                    result_epoch=time.time(),
                    actor=event_actor,
                    changed_at=timestamp,
                )
            except V5SchedulerError as audit_error:
                exc = LegacyV0ContinuationError(
                    f"signal operation raised {exc}; result audit failed: "
                    f"{audit_error}"
                )
            self._raise_signal_uncertainty(
                item=item,
                request_id=request_key,
                actor=event_actor,
                changed_at=timestamp,
                detail=f"signal operation raised {exc}",
                cause=exc,
            )
        if not signaled:
            cause = LegacyV0ContinuationError(
                "authenticated signal delivery returned false"
            )
            try:
                controller.record_manual_yield_signal_result(
                    claim,
                    delivered=False,
                    detail=str(cause),
                    result_epoch=time.time(),
                    actor=event_actor,
                    changed_at=timestamp,
                )
            except V5SchedulerError as audit_error:
                cause = LegacyV0ContinuationError(
                    f"{cause}; result audit failed: {audit_error}"
                )
            self._raise_signal_uncertainty(
                item=item,
                request_id=request_key,
                actor=event_actor,
                changed_at=timestamp,
                detail=str(cause),
                cause=cause,
            )
        try:
            controller.record_manual_yield_signal_result(
                claim,
                delivered=True,
                detail="authenticated SIGINT was delivered",
                result_epoch=time.time(),
                actor=event_actor,
                changed_at=timestamp,
            )
        except V5SchedulerError as exc:
            self._raise_signal_uncertainty(
                item=item,
                request_id=request_key,
                actor=event_actor,
                changed_at=timestamp,
                detail=f"signal was delivered but result audit failed: {exc}",
                cause=exc,
            )
        return pending

    def recover_pending(
        self,
        prepared: PreparedAttempt,
    ) -> LegacyV0PendingContinuation:
        """Rehydrate one persisted and published v0 request after restart."""

        item = self.repository.get_queue_item(prepared.queue_item_id)
        self._validate_prepared(item, prepared)
        row = self._raw_item(item.id)
        if (
            item.state != "yielding"
            or item.admission_kind != _ADMISSION_KIND
            or not item.preemptible
            or row["yield_request_id"] is None
        ):
            raise LegacyV0ContinuationError(
                f"queue item {item.id} is not a persisted legacy yielding operation"
            )
        request_id = _text(
            row["yield_request_id"], field_name="yield_request_id", maximum=256
        )
        gpu_uuid = _text(
            row["assigned_gpu_uuid"], field_name="assigned GPU UUID", maximum=256
        )
        requested_at = _timestamp(
            row["yield_requested_at"], field_name="yield_requested_at"
        )
        requested_by = _text(
            row["yield_requested_by"], field_name="yield_requested_by", maximum=256
        )
        note = _text(row["yield_note"], field_name="yield_note", maximum=200)
        source = _v0_wire_bytes(
            _request_document(
                request_id=request_id,
                queue_item_id=item.id,
                segment=item.segment,
                gpu_uuid=gpu_uuid,
                requested_at=requested_at,
                requested_by=requested_by,
                note=note,
            )
        )
        published = _read_regular(
            prepared.paths.yield_request, label="published legacy yield request"
        )
        if published != source:
            raise LegacyV0ContinuationError(
                "published legacy yield request differs from persisted queue fields"
            )
        allowed_roots = _allowed_run_roots(prepared)
        runner = _runner_receipt(prepared, allowed_roots=allowed_roots)
        stored_run = (
            None
            if row["runner_run_dir"] is None
            else _canonical_directory(
                Path(str(row["runner_run_dir"])), label="stored legacy run directory"
            )
        )
        if runner is not None and stored_run is not None and runner.run_directory != stored_run:
            raise LegacyV0ContinuationError(
                "runner receipt run directory differs from persisted legacy evidence"
            )
        run_directory = stored_run if stored_run is not None else (
            None if runner is None else runner.run_directory
        )
        return _construct_pending(
            project_id=item.project_id,
            revision_id=item.revision_id,
            queue_item_id=item.id,
            segment=item.segment,
            gpu_uuid=gpu_uuid,
            request_id=request_id,
            requested_at=requested_at,
            requested_by=requested_by,
            note=note,
            request_path=prepared.paths.yield_request,
            receipt_path=prepared.paths.yield_receipt,
            request_source=source,
            request_sha256=hashlib.sha256(source).hexdigest(),
            runner_run_id=None if runner is None else runner.run_id,
            runner_run_directory=run_directory,
            runner_manifest=(
                Path(str(row["runner_manifest_path"])).resolve()
                if runner is None and row["runner_manifest_path"] is not None
                else None if runner is None else runner.manifest
            ),
            rsync_pull_command=(
                None
                if row["rsync_pull_command"] is None
                else str(row["rsync_pull_command"])
            ),
            allowed_run_roots=allowed_roots,
        )

    def _read_receipt(
        self,
        pending: LegacyV0PendingContinuation,
    ) -> tuple[bytes, dict[str, object], str]:
        if type(pending) is not LegacyV0PendingContinuation:
            raise TypeError(
                f"pending must be exactly LegacyV0PendingContinuation, got "
                f"{type(pending).__name__}"
            )
        source = _read_regular(pending.receipt_path, label="legacy yield receipt")
        document = _decode_document(source, label="legacy yield receipt")
        status = _validate_receipt_identity(document, pending)
        return source, document, status

    def reconcile_live_failure(
        self,
        pending: LegacyV0PendingContinuation,
        *,
        actor: str,
        changed_at: str,
    ) -> LegacyV0ContinuationOutcome | None:
        """Restore a live process after an exact failed v0 checkpoint attempt."""

        if not pending.receipt_path.exists() and not pending.receipt_path.is_symlink():
            return None
        source, document, status = self._read_receipt(pending)
        if status != "failed":
            return None
        progress, error = _failed_detail(document)
        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        row = self._raw_item(pending.queue_item_id)
        alive = (
            row["pid"] is not None
            and row["pgid"] is not None
            and process_identity_matches(
                pid=int(row["pid"]),
                pgid=int(row["pgid"]),
                process_start_ticks=(
                    None
                    if row["proc_start_ticks"] is None
                    else str(row["proc_start_ticks"])
                ),
            )
        )
        if not alive:
            raise LegacyV0ContinuationError(
                "failed legacy yield receipt cannot restore an absent process"
            )
        detail = f"cooperative yield failed at {_progress_text(document, progress)}: {error}"
        with self.repository.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE queue_items
                SET state = 'running', state_detail = ?,
                    yield_requested_at = NULL, yield_requested_by = NULL,
                    yield_request_id = NULL, yield_note = NULL,
                    yield_duration_hours = NULL
                WHERE id = ? AND state = 'yielding' AND segment = ?
                  AND yield_request_id = ?
                """,
                (detail, pending.queue_item_id, pending.segment, pending.request_id),
            )
            if updated.rowcount != 1:
                raise LegacyV0ContinuationError(
                    "termination or another state transition won before failed "
                    "legacy yield recovery"
                )
            _event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="COOPERATIVE_YIELD_FAILED",
                project_id=pending.project_id,
                queue_item_id=pending.queue_item_id,
                payload={
                    "request_id": pending.request_id,
                    "protocol": "CooperativeYieldReceipt/v0",
                    "receipt_sha256": hashlib.sha256(source).hexdigest(),
                    "error": error,
                    "progress": cast(JSONValue, progress),
                },
            )
        _unlink_regular_control_file(
            pending.receipt_path,
            label="accepted failed legacy yield receipt",
        )
        return LegacyV0ContinuationOutcome(
            item=self.repository.get_queue_item(pending.queue_item_id),
            request_id=pending.request_id,
            receipt_sha256=hashlib.sha256(source).hexdigest(),
            requeued=False,
            resumed_running=True,
        )

    def finalize_manual_yield(
        self,
        pending: LegacyV0PendingContinuation,
        *,
        executor_return_code: int,
        actor: str,
        changed_at: str,
    ) -> LegacyV0ContinuationOutcome:
        """Validate one terminal v0 receipt and requeue or isolate its item."""

        if type(executor_return_code) is not int or executor_return_code < 0:
            raise LegacyV0ContinuationError(
                "executor_return_code must be a nonnegative integer"
            )
        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        try:
            source, document, status = self._read_receipt(pending)
            if status == "failed":
                _progress_value, error = _failed_detail(document)
                cause = LegacyV0ContinuationError(
                    f"project declined legacy manual continuation: {error}"
                )
                self._isolate(
                    pending.queue_item_id,
                    reason=str(cause),
                    actor=event_actor,
                    changed_at=timestamp,
                    terminal=True,
                    cause=cause,
                )
            if executor_return_code != _YIELD_EXIT_CODE:
                raise LegacyV0ContinuationError(
                    f"ready legacy yield requires executor return code "
                    f"{_YIELD_EXIT_CODE}, got {executor_return_code}"
                )
            evidence = _ready_evidence(
                source=source,
                document=document,
                pending=pending,
            )
            runner = _runner_from_pending_path(pending)
            if runner is not None:
                if (
                    runner.status != "yielded"
                    or runner.return_code != _YIELD_EXIT_CODE
                ):
                    raise LegacyV0ContinuationError(
                        "terminal runner receipt is not the yielded segment"
                    )
                if (
                    pending.runner_run_id is not None
                    and runner.run_id != pending.runner_run_id
                ):
                    raise LegacyV0ContinuationError(
                        "terminal runner receipt changed legacy run identity"
                    )
                if (
                    pending.runner_run_directory is not None
                    and runner.run_directory != pending.runner_run_directory
                ):
                    raise LegacyV0ContinuationError(
                        "terminal runner receipt changed legacy run directory"
                    )
        except LegacyV0ContinuationError as exc:
            current = self.repository.get_queue_item(pending.queue_item_id)
            if current.state != "yielding":
                raise
            self._isolate(
                pending.queue_item_id,
                reason=f"legacy manual continuation failed validation: {exc}",
                actor=event_actor,
                changed_at=timestamp,
                # The caller supplied the ended executor's return code, so the
                # item may become terminal while its GPU lease remains held.
                terminal=True,
                cause=exc,
            )
        assert isinstance(evidence, _ReadyEvidence)
        receipt_sha256 = hashlib.sha256(evidence.source).hexdigest()
        run_directory = (
            pending.runner_run_directory if runner is None else runner.run_directory
        )
        manifest = pending.runner_manifest if runner is None else runner.manifest
        pull_command = (
            pending.rsync_pull_command if runner is None else runner.rsync_pull_command
        )
        with self.repository.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE queue_items
                SET state = 'queued', state_detail = ?, segment = segment + 1,
                    resume_front = 1, runtime_gpu_lease_held = 0,
                    runtime_gpu_lease_released_at = ?,
                    pid = NULL, pgid = NULL,
                    proc_start_ticks = NULL, return_code = NULL,
                    continuation_checkpoint = ?,
                    continuation_checkpoint_sha256 = ?,
                    continuation_checkpoint_metadata = ?,
                    continuation_checkpoint_metadata_sha256 = ?,
                    continuation_step = ?, continuation_wandb_id = ?,
                    runner_run_dir = COALESCE(?, runner_run_dir),
                    runner_manifest_path = COALESCE(?, runner_manifest_path),
                    rsync_pull_command = COALESCE(?, rsync_pull_command)
                WHERE id = ? AND state = 'yielding'
                  AND runtime_gpu_lease_held = 1 AND segment = ?
                  AND yield_request_id = ?
                """,
                (
                    f"resume from verified {_progress_text(evidence.document, evidence.progress)}",
                    timestamp,
                    str(evidence.checkpoint),
                    evidence.checkpoint_sha256,
                    str(evidence.metadata),
                    evidence.metadata_sha256,
                    evidence.step,
                    evidence.wandb_id,
                    None if run_directory is None else str(run_directory),
                    None if manifest is None else str(manifest),
                    pull_command,
                    pending.queue_item_id,
                    pending.segment,
                    pending.request_id,
                ),
            )
            if updated.rowcount != 1:
                raise LegacyV0ContinuationError(
                    "termination or another state transition won before legacy "
                    "yield finalization; stale receipt was not requeued"
                )
            _event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="EXPERIMENT_YIELDED_AND_REQUEUED",
                project_id=pending.project_id,
                queue_item_id=pending.queue_item_id,
                payload={
                    "request_id": pending.request_id,
                    "protocol": "CooperativeYieldReceipt/v0",
                    "receipt_sha256": receipt_sha256,
                    "segment_finished": pending.segment,
                    "next_segment": pending.segment + 1,
                    "runtime_gpu_lease_released_at": timestamp,
                    "checkpoint": str(evidence.checkpoint),
                    "checkpoint_sha256": evidence.checkpoint_sha256,
                    "checkpoint_metadata": str(evidence.metadata),
                    "checkpoint_metadata_sha256": evidence.metadata_sha256,
                    "step": evidence.step,
                    "wandb_id": evidence.wandb_id,
                    "progress": cast(JSONValue, evidence.progress),
                },
            )
        return LegacyV0ContinuationOutcome(
            item=self.repository.get_queue_item(pending.queue_item_id),
            request_id=pending.request_id,
            receipt_sha256=receipt_sha256,
            requeued=True,
            resumed_running=False,
            checkpoint=evidence.checkpoint,
            checkpoint_sha256=evidence.checkpoint_sha256,
            checkpoint_metadata=evidence.metadata,
            checkpoint_metadata_sha256=evidence.metadata_sha256,
        )


def _progress_text(
    document: Mapping[str, object],
    progress: Mapping[str, object] | None,
) -> str:
    if progress is not None:
        completed = f"{int(cast(int, progress['completed'])):,}"
        total = progress.get("total")
        amount = completed if total is None else f"{completed}/{int(cast(int, total)):,}"
        return f"{amount} {progress['unit']}"
    return f"step {int(cast(int, document['step'])):,}"


def _runner_from_pending_path(
    pending: LegacyV0PendingContinuation,
) -> _RunnerEvidence | None:
    """Re-read RunnerReceipt/v1 using the pending segment's immutable path."""

    return _runner_receipt_from_path(
        pending.request_path.parent / "runner.json",
        queue_item_id=pending.queue_item_id,
        segment=pending.segment,
        allowed_roots=pending.allowed_run_roots,
    )


__all__ = [
    "LegacyV0ContinuationCoordinator",
    "LegacyV0ContinuationError",
    "LegacyV0ContinuationOutcome",
    "LegacyV0PendingContinuation",
]
