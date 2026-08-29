"""Run one durable queue child from a strict project-qualified control payload."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Mapping, Self, Sequence, cast

from experiment_queue.serialization import JSONValue, canonical_json_bytes


MAX_EXECUTOR_PAYLOAD_BYTES = 1024 * 1024
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OBJECT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


class ExecutorError(RuntimeError):
    """Raised when a scheduler-owned attempt payload is invalid or unsafe."""


class ExecutorEvidencePublicationError(ExecutorError):
    """Report a failed immutable control-evidence publication.

    ``staging_path`` identifies complete evidence retained for inspection when
    one was created.  Callers must not infer final-name durability merely from
    visibility; ``final_durable`` becomes true only after the parent directory
    fsync that follows the no-clobber hard link.
    """

    def __init__(
        self,
        message: str,
        *,
        final_path: Path,
        staging_path: Path | None,
        final_visible: bool,
        final_durable: bool,
    ) -> None:
        super().__init__(message)
        self.final_path = final_path
        self.staging_path = staging_path
        self.final_visible = final_visible
        self.final_durable = final_durable


class ExecutorEvidencePublicationUncertainError(
    ExecutorEvidencePublicationError
):
    """The final evidence name is visible but its durability is indeterminate."""


def utc_now_iso() -> str:
    """Return one second-precision UTC protocol timestamp."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_strict_json_source(path: Path) -> tuple[dict[str, object], bytes]:
    """Read strict JSON plus the exact bytes authenticated by its consumer."""

    try:
        if path.is_symlink():
            raise ExecutorError(f"executor payload must not be a symlink: {path}")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except ExecutorError:
        raise
    except OSError as exc:
        raise ExecutorError(f"could not open executor payload {path}: {exc}") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ExecutorError(f"executor payload must be a regular file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            source = stream.read(MAX_EXECUTOR_PAYLOAD_BYTES + 1)
    finally:
        os.close(descriptor)
    if not source or len(source) > MAX_EXECUTOR_PAYLOAD_BYTES:
        raise ExecutorError(
            f"executor payload {path} must contain 1 through "
            f"{MAX_EXECUTOR_PAYLOAD_BYTES} bytes"
        )

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise ExecutorError(f"executor payload repeats JSON key {key!r}")
            document[key] = value
        return document

    def reject_constant(value: str) -> None:
        raise ExecutorError(
            f"executor payload contains unsupported JSON constant {value!r}"
        )

    try:
        value = json.loads(
            source.decode("utf-8", errors="strict"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except ExecutorError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ExecutorError(f"executor payload {path} is not strict UTF-8 JSON: {exc}") from exc
    if type(value) is not dict:
        raise ExecutorError(f"executor payload {path} must contain a JSON object")
    return cast(dict[str, object], value), source


def _read_strict_json(path: Path) -> dict[str, object]:
    """Read one bounded regular JSON document without following its final symlink."""

    return _read_strict_json_source(path)[0]


def _exact_fields(
    document: Mapping[str, object],
    *,
    expected: set[str],
    label: str,
) -> None:
    fields = set(document)
    missing = sorted(expected - fields)
    unknown = sorted(fields - expected)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields {missing}")
        if unknown:
            details.append(f"unknown fields {unknown}")
        raise ExecutorError(f"{label} has invalid fields: " + "; ".join(details))


def _text(
    value: object,
    *,
    field_name: str,
    allow_empty: bool = False,
    maximum: int = 4096,
) -> str:
    if type(value) is not str or (not value and not allow_empty):
        raise ExecutorError(f"{field_name} must be a string")
    if "\x00" in value:
        raise ExecutorError(f"{field_name} must not contain a NUL byte")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ExecutorError(f"{field_name} must contain valid Unicode text") from exc
    if len(value) > maximum:
        raise ExecutorError(f"{field_name} must be {maximum} characters or fewer")
    return value


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ExecutorError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ExecutorError(f"{field_name} must be a nonnegative integer")
    return value


def _timestamp(value: object, *, field_name: str) -> str:
    timestamp = _text(value, field_name=field_name)
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise ExecutorError(
            f"{field_name} must be a valid timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutorError(
            f"{field_name} must be a valid timezone-aware ISO 8601 timestamp"
        )
    return timestamp


@dataclass(frozen=True, slots=True, init=False)
class ExecutorLaunchReceipt:
    """Durable executor identity published before any scientific child starts."""

    queue_item_id: int
    project_id: int
    project_key: str
    project_revision_id: int
    project_revision: str
    experiment_id: str
    attempt: int
    segment: int
    payload_sha256: str
    pid: int
    pgid: int
    process_start_ticks: str | None
    gpu_uuid: str | None
    published_at: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "ExecutorLaunchReceipt is validated-only; use "
            "ExecutorLaunchReceipt.read()"
        )

    @classmethod
    def inspect(cls, path: Path) -> Self:
        """Validate the sidecar shape without trusting its scheduled binding."""

        if cls is not ExecutorLaunchReceipt:
            raise TypeError(
                "ExecutorLaunchReceipt.inspect() constructs exactly "
                "ExecutorLaunchReceipt"
            )
        confirm_immutable_evidence_for_read(Path(path))
        document = _read_strict_json(Path(path))
        ticks = document.get("process_start_ticks")
        receipt = cls.read(
            path,
            queue_item_id=_positive_integer(
                document.get("queue_item_id"), field_name="queue_item_id"
            ),
            project_id=_positive_integer(
                document.get("project_id"), field_name="project_id"
            ),
            project_key=_text(document.get("project_key"), field_name="project_key"),
            project_revision_id=_positive_integer(
                document.get("project_revision_id"),
                field_name="project_revision_id",
            ),
            project_revision=_text(
                document.get("project_revision"), field_name="project_revision"
            ),
            experiment_id=_text(
                document.get("experiment_id"), field_name="experiment_id"
            ),
            attempt=_positive_integer(
                document.get("attempt"), field_name="attempt"
            ),
            segment=_positive_integer(
                document.get("segment"), field_name="segment"
            ),
            payload_sha256=_text(
                document.get("payload_sha256"), field_name="payload_sha256"
            ),
            gpu_uuid=(
                None
                if document.get("gpu_uuid") is None
                else _text(document.get("gpu_uuid"), field_name="gpu_uuid")
            ),
            pid=_positive_integer(document.get("pid"), field_name="pid"),
            pgid=_positive_integer(document.get("pgid"), field_name="pgid"),
            process_start_ticks=(
                None
                if ticks is None
                else _text(ticks, field_name="process_start_ticks", maximum=256)
            ),
        )
        if receipt.process_start_ticks != ticks:
            raise ExecutorError(
                "executor launch receipt process_start_ticks changed while reading"
            )
        return receipt

    @classmethod
    def read(
        cls,
        path: Path,
        *,
        queue_item_id: int,
        project_id: int,
        project_key: str,
        project_revision_id: int,
        project_revision: str,
        experiment_id: str,
        attempt: int,
        segment: int,
        payload_sha256: str,
        gpu_uuid: str | None,
        pid: int | None = None,
        pgid: int | None = None,
        process_start_ticks: str | None = None,
    ) -> Self:
        """Authenticate one launch sidecar against scheduler-owned identity."""

        if cls is not ExecutorLaunchReceipt:
            raise TypeError(
                "ExecutorLaunchReceipt.read() constructs exactly "
                "ExecutorLaunchReceipt"
            )
        confirm_immutable_evidence_for_read(Path(path))
        document = _read_strict_json(Path(path))
        _exact_fields(
            document,
            expected={
                "schema_version",
                "queue_item_id",
                "project_id",
                "project_key",
                "project_revision_id",
                "project_revision",
                "experiment_id",
                "attempt",
                "segment",
                "payload_sha256",
                "pid",
                "pgid",
                "process_start_ticks",
                "gpu_uuid",
                "published_at",
            },
            label="executor launch receipt",
        )
        if document["schema_version"] != 1:
            raise ExecutorError(
                "executor launch receipt schema_version must be integer 1, got "
                f"{document['schema_version']!r}"
            )
        actual_ticks = document["process_start_ticks"]
        if actual_ticks is not None:
            actual_ticks = _text(
                actual_ticks,
                field_name="process_start_ticks",
                maximum=256,
            )
        actual_gpu = document["gpu_uuid"]
        if actual_gpu is not None:
            actual_gpu = _text(actual_gpu, field_name="gpu_uuid")
        actual: dict[str, object] = {
            "queue_item_id": _positive_integer(
                document["queue_item_id"], field_name="queue_item_id"
            ),
            "project_id": _positive_integer(
                document["project_id"], field_name="project_id"
            ),
            "project_key": _text(document["project_key"], field_name="project_key"),
            "project_revision_id": _positive_integer(
                document["project_revision_id"], field_name="project_revision_id"
            ),
            "project_revision": _text(
                document["project_revision"], field_name="project_revision"
            ),
            "experiment_id": _text(
                document["experiment_id"], field_name="experiment_id"
            ),
            "attempt": _positive_integer(document["attempt"], field_name="attempt"),
            "segment": _positive_integer(document["segment"], field_name="segment"),
            "payload_sha256": _text(
                document["payload_sha256"], field_name="payload_sha256"
            ),
            "pid": _positive_integer(document["pid"], field_name="pid"),
            "pgid": _positive_integer(document["pgid"], field_name="pgid"),
            "process_start_ticks": actual_ticks,
            "gpu_uuid": actual_gpu,
        }
        if _SHA256_PATTERN.fullmatch(str(actual["payload_sha256"])) is None:
            raise ExecutorError(
                "executor launch receipt payload_sha256 must be a lowercase "
                "SHA-256 digest"
            )
        expected: dict[str, object] = {
            "queue_item_id": queue_item_id,
            "project_id": project_id,
            "project_key": project_key,
            "project_revision_id": project_revision_id,
            "project_revision": project_revision,
            "experiment_id": experiment_id,
            "attempt": attempt,
            "segment": segment,
            "payload_sha256": payload_sha256,
            "gpu_uuid": gpu_uuid,
        }
        if pid is not None:
            expected["pid"] = pid
        if pgid is not None:
            expected["pgid"] = pgid
        if process_start_ticks is not None:
            expected["process_start_ticks"] = process_start_ticks
        for field_name, expected_value in expected.items():
            if actual[field_name] != expected_value:
                raise ExecutorError(
                    f"executor launch receipt {field_name} "
                    f"{actual[field_name]!r} does not match scheduled value "
                    f"{expected_value!r}"
                )
        published_at = _timestamp(
            document["published_at"], field_name="published_at"
        )
        values = {**actual, "published_at": published_at}
        instance = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(instance, name, value)
        return cast(Self, instance)


@dataclass(frozen=True, slots=True, init=False)
class ExecutorReceipt:
    """Authenticated terminal evidence emitted by one durable executor."""

    queue_item_id: int
    project_id: int
    project_revision_id: int
    project_key: str
    project_revision: str
    experiment_id: str
    attempt: int
    resolved_spec_sha256: str | None
    admission_kind: str
    segment: int
    git_commit: str
    worktree: str
    command_kind: str
    command_sha256: str
    started_at: str
    finished_at: str
    return_code: int
    signals_received: tuple[str, ...]
    gpu_uuid: str | None

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "ExecutorReceipt is validated-only; use ExecutorReceipt.read()"
        )

    @classmethod
    def read(
        cls,
        path: Path,
        *,
        queue_item_id: int,
        project_id: int,
        project_revision_id: int,
        project_key: str,
        project_revision: str,
        experiment_id: str,
        attempt: int,
        resolved_spec_sha256: str | None,
        admission_kind: str,
        segment: int,
        git_commit: str,
        worktree: Path,
        command_kind: str,
        command_sha256: str,
        gpu_uuid: str | None,
    ) -> Self:
        """Read one strict receipt and bind it to scheduler-owned expectations."""

        if cls is not ExecutorReceipt:
            raise TypeError("ExecutorReceipt.read() constructs exactly ExecutorReceipt")
        confirm_immutable_evidence_for_read(Path(path))
        document = _read_strict_json(Path(path))
        _exact_fields(
            document,
            expected={
                "schema_version",
                "queue_item_id",
                "project_id",
                "project_revision_id",
                "project_key",
                "project_revision",
                "experiment_id",
                "attempt",
                "resolved_spec_sha256",
                "admission_kind",
                "segment",
                "git_commit",
                "worktree",
                "command_kind",
                "command_sha256",
                "started_at",
                "finished_at",
                "return_code",
                "signals_received",
                "gpu_uuid",
            },
            label="executor receipt",
        )
        if document["schema_version"] != 3:
            raise ExecutorError(
                "executor receipt schema_version must be integer 3, got "
                f"{document['schema_version']!r}"
            )
        actual_admission = _text(
            document["admission_kind"], field_name="admission_kind"
        )
        actual_resolved = document["resolved_spec_sha256"]
        if actual_admission == "ExperimentCard/v1":
            if (
                type(actual_resolved) is not str
                or _SHA256_PATTERN.fullmatch(actual_resolved) is None
            ):
                raise ExecutorError(
                    "structured executor receipt requires resolved_spec_sha256 evidence"
                )
        elif actual_admission == "LegacyMarkdownCard/v0":
            if actual_resolved is not None:
                raise ExecutorError(
                    "legacy executor receipt must use null resolved_spec_sha256"
                )
        else:
            raise ExecutorError(
                f"executor receipt has unsupported admission_kind {actual_admission!r}"
            )
        actual_commit = _text(document["git_commit"], field_name="git_commit")
        if _GIT_OBJECT_PATTERN.fullmatch(actual_commit) is None:
            raise ExecutorError(
                "executor receipt git_commit must be a full lowercase Git object ID"
            )
        actual_command_kind = _text(
            document["command_kind"], field_name="command_kind"
        )
        if actual_command_kind not in {"argv", "legacy-shell"}:
            raise ExecutorError(
                "executor receipt command_kind must be 'argv' or 'legacy-shell'"
            )
        actual_command_hash = _text(
            document["command_sha256"], field_name="command_sha256"
        )
        if _SHA256_PATTERN.fullmatch(actual_command_hash) is None:
            raise ExecutorError(
                "executor receipt command_sha256 must be a lowercase SHA-256 digest"
            )
        signals_value = document["signals_received"]
        if type(signals_value) is not list:
            raise ExecutorError("executor receipt signals_received must be an array")
        signals = tuple(
            _text(value, field_name=f"signals_received[{index}]", maximum=4096)
            for index, value in enumerate(cast(list[object], signals_value))
        )
        actual_gpu = document["gpu_uuid"]
        if actual_gpu is not None:
            actual_gpu = _text(actual_gpu, field_name="gpu_uuid")

        actual: dict[str, object] = {
            "queue_item_id": _positive_integer(
                document["queue_item_id"], field_name="queue_item_id"
            ),
            "project_id": _positive_integer(
                document["project_id"], field_name="project_id"
            ),
            "project_revision_id": _positive_integer(
                document["project_revision_id"], field_name="project_revision_id"
            ),
            "project_key": _text(document["project_key"], field_name="project_key"),
            "project_revision": _text(
                document["project_revision"], field_name="project_revision"
            ),
            "experiment_id": _text(
                document["experiment_id"], field_name="experiment_id"
            ),
            "attempt": _positive_integer(document["attempt"], field_name="attempt"),
            "resolved_spec_sha256": actual_resolved,
            "admission_kind": actual_admission,
            "segment": _positive_integer(document["segment"], field_name="segment"),
            "git_commit": actual_commit,
            "worktree": _text(document["worktree"], field_name="worktree"),
            "command_kind": actual_command_kind,
            "command_sha256": actual_command_hash,
            "gpu_uuid": actual_gpu,
        }
        expected: dict[str, object] = {
            "queue_item_id": queue_item_id,
            "project_id": project_id,
            "project_revision_id": project_revision_id,
            "project_key": project_key,
            "project_revision": project_revision,
            "experiment_id": experiment_id,
            "attempt": attempt,
            "resolved_spec_sha256": resolved_spec_sha256,
            "admission_kind": admission_kind,
            "segment": segment,
            "git_commit": git_commit,
            "worktree": str(worktree),
            "command_kind": command_kind,
            "command_sha256": command_sha256,
            "gpu_uuid": gpu_uuid,
        }
        for field_name, expected_value in expected.items():
            if actual[field_name] != expected_value:
                raise ExecutorError(
                    f"executor receipt {field_name} {actual[field_name]!r} does not "
                    f"match scheduled value {expected_value!r}"
                )

        started_at = _timestamp(document["started_at"], field_name="started_at")
        finished_at = _timestamp(document["finished_at"], field_name="finished_at")
        if datetime.fromisoformat(finished_at.replace("Z", "+00:00")) < datetime.fromisoformat(
            started_at.replace("Z", "+00:00")
        ):
            raise ExecutorError("executor receipt finished_at precedes started_at")
        values = {
            **actual,
            "started_at": started_at,
            "finished_at": finished_at,
            "return_code": _nonnegative_integer(
                document["return_code"], field_name="return_code"
            ),
            "signals_received": signals,
        }
        instance = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(instance, name, value)
        return cast(Self, instance)


def _absolute_directory(value: object, *, field_name: str) -> Path:
    path = Path(_text(value, field_name=field_name))
    if not path.is_absolute():
        raise ExecutorError(f"{field_name} must be absolute, got {path}")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExecutorError(f"{field_name} {path} cannot be resolved: {exc}") from exc
    if resolved != path or not resolved.is_dir():
        raise ExecutorError(
            f"{field_name} must remain the recorded canonical directory {path}, "
            f"resolved to {resolved}"
        )
    return resolved


def _absolute_control_file(
    value: object,
    *,
    field_name: str,
    control_root: Path,
) -> Path:
    path = Path(_text(value, field_name=field_name))
    if not path.is_absolute():
        raise ExecutorError(f"{field_name} must be absolute, got {path}")
    try:
        resolved_parent = path.parent.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExecutorError(f"{field_name} parent cannot be resolved: {exc}") from exc
    if resolved_parent != path.parent:
        raise ExecutorError(
            f"{field_name} parent changed canonical target from {path.parent} to "
            f"{resolved_parent}"
        )
    if resolved_parent != control_root and control_root not in resolved_parent.parents:
        raise ExecutorError(
            f"{field_name} {path} is outside scheduler control root {control_root}"
        )
    return path


def _command(document: Mapping[str, object]) -> tuple[tuple[str, ...], str, str]:
    command_kind = _text(document["command_kind"], field_name="command_kind")
    command_value = document["command"]
    if command_kind == "argv":
        if type(command_value) is not list or not command_value:
            raise ExecutorError("argv command must be a non-empty JSON array")
        argv = tuple(
            _text(
                value,
                field_name=f"command[{index}]",
                allow_empty=index > 0,
            )
            for index, value in enumerate(cast(list[object], command_value))
        )
        digest = hashlib.sha256(
            canonical_json_bytes(cast(JSONValue, list(argv)))
        ).hexdigest()
        return argv, digest, command_kind
    if command_kind == "legacy-shell":
        script = _text(command_value, field_name="command")
        return (
            ("/bin/bash", "-lc", script),
            hashlib.sha256(script.encode("utf-8")).hexdigest(),
            command_kind,
        )
    raise ExecutorError(
        f"command_kind must be 'argv' or 'legacy-shell', got {command_kind!r}"
    )


def _validated_payload(
    path: Path,
) -> tuple[dict[str, object], tuple[str, ...], Path, Path, Path, str]:
    payload, payload_source = _read_strict_json_source(path)
    _exact_fields(
        payload,
        expected={
            "schema_version",
            "queue_item_id",
            "project_id",
            "project_revision_id",
            "project_key",
            "project_revision",
            "experiment_id",
            "attempt",
            "resolved_spec_sha256",
            "admission_kind",
            "segment",
            "git_commit",
            "worktree",
            "cwd",
            "command_kind",
            "command",
            "control_root",
            "receipt_path",
        },
        label="executor payload",
    )
    if payload["schema_version"] != 1:
        raise ExecutorError(
            f"executor payload schema_version must be integer 1, got "
            f"{payload['schema_version']!r}"
        )
    _positive_integer(payload["queue_item_id"], field_name="queue_item_id")
    _positive_integer(payload["project_id"], field_name="project_id")
    _positive_integer(
        payload["project_revision_id"], field_name="project_revision_id"
    )
    _positive_integer(payload["segment"], field_name="segment")
    _positive_integer(payload["attempt"], field_name="attempt")
    _text(payload["project_key"], field_name="project_key")
    _text(payload["project_revision"], field_name="project_revision")
    _text(payload["experiment_id"], field_name="experiment_id")
    admission_kind = _text(payload["admission_kind"], field_name="admission_kind")
    if admission_kind not in {"ExperimentCard/v1", "LegacyMarkdownCard/v0"}:
        raise ExecutorError(f"unsupported admission_kind {admission_kind!r}")
    resolved_digest = payload["resolved_spec_sha256"]
    if admission_kind == "ExperimentCard/v1":
        if type(resolved_digest) is not str or _SHA256_PATTERN.fullmatch(resolved_digest) is None:
            raise ExecutorError(
                "structured admission requires resolved_spec_sha256 evidence"
            )
    elif resolved_digest is not None:
        raise ExecutorError(
            "legacy admission must use null resolved_spec_sha256 rather than "
            "fabricated structured evidence"
        )
    commit = _text(payload["git_commit"], field_name="git_commit")
    if _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
        raise ExecutorError("git_commit must be a full lowercase Git object ID")
    worktree = _absolute_directory(payload["worktree"], field_name="worktree")
    cwd = _absolute_directory(payload["cwd"], field_name="cwd")
    if cwd != worktree and worktree not in cwd.parents:
        raise ExecutorError(f"cwd {cwd} is outside admitted worktree {worktree}")
    control_root = _absolute_directory(payload["control_root"], field_name="control_root")
    try:
        canonical_payload = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExecutorError(f"executor payload path changed while reading: {exc}") from exc
    if (
        canonical_payload.parent != control_root
        and control_root not in canonical_payload.parent.parents
    ):
        raise ExecutorError(
            f"executor payload {canonical_payload} is outside scheduler control root "
            f"{control_root}"
        )
    receipt_path = _absolute_control_file(
        payload["receipt_path"],
        field_name="receipt_path",
        control_root=control_root,
    )
    if canonical_payload == receipt_path:
        raise ExecutorError("executor payload and exit receipt must be different files")
    launch_receipt_path = canonical_payload.with_name("launch.json")
    if launch_receipt_path == receipt_path:
        raise ExecutorError(
            "executor launch receipt and exit receipt must be different files"
        )
    argv, command_sha256, command_kind = _command(payload)
    payload["command_sha256"] = command_sha256
    payload["command_kind"] = command_kind
    return (
        payload,
        argv,
        cwd,
        receipt_path,
        launch_receipt_path,
        hashlib.sha256(payload_source).hexdigest(),
    )


def _fsync_directory(path: Path) -> None:
    """Synchronize one directory entry namespace on supported POSIX hosts."""

    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def confirm_immutable_evidence_for_read(path: Path) -> None:
    """Confirm or reject retained immutable-publication staging evidence.

    A same-inode final/staging pair is made unambiguous by fsyncing the parent
    before the final is parsed. Staging-only or changed companion identity is
    evidence for operator inspection and is never promoted or deleted.
    """

    final = Path(path)
    parent = final.parent
    prefix = f".{final.name}."
    suffix = ".tmp"
    try:
        companions = tuple(
            entry
            for entry in parent.iterdir()
            if entry.name.startswith(prefix)
            and entry.name.endswith(suffix)
            and len(entry.name) > len(prefix) + len(suffix)
        )
    except OSError as exc:
        raise ExecutorError(
            f"could not inspect immutable evidence companions for {final}: {exc}"
        ) from exc
    if not companions:
        return
    try:
        final_descriptor = os.open(final, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ExecutorError(
            f"immutable evidence final {final} is absent or unsafe while staging "
            "evidence remains; preserve every path for inspection"
        ) from exc
    companion_descriptors: list[tuple[Path, int]] = []
    try:
        final_stat = os.fstat(final_descriptor)
        if not stat.S_ISREG(final_stat.st_mode):
            raise ExecutorError(
                f"immutable evidence final must be regular: {final}"
            )
        for companion in companions:
            try:
                descriptor = os.open(
                    companion,
                    os.O_RDONLY | os.O_NOFOLLOW,
                )
            except OSError as exc:
                raise ExecutorError(
                    f"immutable evidence companion {companion} is unreadable or "
                    "unsafe; preserve all evidence for inspection"
                ) from exc
            companion_descriptors.append((companion, descriptor))
            companion_stat = os.fstat(descriptor)
            if not stat.S_ISREG(companion_stat.st_mode) or (
                companion_stat.st_dev,
                companion_stat.st_ino,
            ) != (final_stat.st_dev, final_stat.st_ino):
                raise ExecutorError(
                    f"immutable evidence companion {companion} is not a regular "
                    f"hard link to {final}; preserve all evidence for inspection"
                )
        try:
            _fsync_directory(parent)
        except OSError as exc:
            raise ExecutorError(
                f"could not confirm immutable evidence final {final} durable: {exc}; "
                "preserve all evidence for inspection"
            ) from exc
    finally:
        os.close(final_descriptor)
        for _companion, descriptor in companion_descriptors:
            os.close(descriptor)

    # The final name is now directory-durable. Companion cleanup cannot revoke
    # it, so changed/unlink-failing entries are simply retained for inspection.
    removed = False
    for companion in companions:
        try:
            current = companion.lstat()
            final_current = final.stat(follow_symlinks=False)
            if (
                stat.S_ISREG(current.st_mode)
                and stat.S_ISREG(final_current.st_mode)
                and (current.st_dev, current.st_ino)
                == (final_current.st_dev, final_current.st_ino)
            ):
                companion.unlink()
                removed = True
        except OSError:
            continue
    if removed:
        try:
            _fsync_directory(parent)
        except OSError:
            pass


def _immutable_evidence_exists_error(
    *, path: Path, label: str
) -> ExecutorEvidencePublicationError:
    """Build the stable refusal used for an existing final evidence name."""

    return ExecutorEvidencePublicationError(
        f"{label} already exists at {path}; refuse duplicate, stale, or "
        "replacement evidence and inspect the existing file before retrying",
        final_path=path,
        staging_path=None,
        final_visible=True,
        final_durable=False,
    )


def _publish_immutable_json(
    path: Path,
    document: Mapping[str, object],
    *,
    label: str,
) -> None:
    """Publish JSON with a durable staging name and an atomic no-clobber link.

    The staging entry is directory-fsynced before the final hard link is
    attempted.  Therefore a failed post-link directory fsync retains a durable
    same-inode fallback.  Once the final link is known durable, staging cleanup
    is best-effort: cleanup failure cannot invalidate or hide committed
    evidence.
    """

    encoded = json.dumps(
        document,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    if os.path.lexists(path):
        raise _immutable_evidence_exists_error(path=path, label=label)

    temporary: Path | None = None
    descriptor: int | None = None
    complete_staging = False
    staging_durable = False
    linked = False
    final_durable = False
    preserve_temporary = False
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        complete_staging = True
        # This first directory fsync makes the complete staging name the
        # durable fallback before the final name can become visible.
        _fsync_directory(path.parent)
        staging_durable = True
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as exc:
            raise _immutable_evidence_exists_error(path=path, label=label) from exc
        linked = True
        _fsync_directory(path.parent)
        final_durable = True

        # The final hard link is now authoritative.  Failure to clean the
        # staging link is harmless and deliberately cannot turn a completed
        # attempt into missing terminal evidence.
        try:
            temporary.unlink()
        except OSError:
            preserve_temporary = True
            return
        temporary = None
        try:
            _fsync_directory(path.parent)
        except OSError:
            # Final-name durability was already established.  At worst the
            # now-unlinked staging name can reappear after a system crash.
            return
    except BaseException as exc:
        final_visible = False
        same_published_inode = False
        if temporary is not None and os.path.lexists(path):
            final_visible = True
            try:
                same_published_inode = os.path.samefile(temporary, path)
            except OSError:
                pass
        if (linked or same_published_inode) and not final_durable:
            preserve_temporary = complete_staging
            if isinstance(exc, ExecutorEvidencePublicationUncertainError):
                raise
            detail = str(exc) or type(exc).__name__
            raise ExecutorEvidencePublicationUncertainError(
                f"{label} {path} is visible but final-link directory durability "
                f"could not be confirmed ({detail}); preserve both {temporary} "
                "and the final path, which name the same complete evidence, and "
                "inspect them before retrying",
                final_path=path,
                staging_path=temporary,
                final_visible=True,
                final_durable=False,
            ) from exc
        if isinstance(exc, ExecutorError):
            raise
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        preserve_temporary = complete_staging
        detail = str(exc) or type(exc).__name__
        staging_detail = (
            f"; complete {'durable ' if staging_durable else ''}staging evidence "
            f"is preserved at {temporary} for inspection"
            if preserve_temporary
            else ""
        )
        raise ExecutorEvidencePublicationError(
            f"could not publish {label} {path}: {detail}{staging_detail}",
            final_path=path,
            staging_path=temporary if preserve_temporary else None,
            final_visible=final_visible,
            final_durable=False,
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if (
            temporary is not None
            and not preserve_temporary
            and os.path.lexists(temporary)
        ):
            try:
                temporary.unlink()
            except OSError:
                pass


def _atomic_write_json(path: Path, document: Mapping[str, object]) -> None:
    """Publish immutable terminal evidence without replacing any prior receipt."""

    _publish_immutable_json(path, document, label="executor exit receipt")


def _atomic_create_json(path: Path, document: Mapping[str, object]) -> None:
    """Publish immutable launch evidence and refuse every prior path identity."""

    _publish_immutable_json(path, document, label="executor launch receipt")


def _process_start_ticks(pid: int) -> str | None:
    """Read Linux process start time without misparsing spaces in ``comm``."""

    try:
        source = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    closing = source.rfind(")")
    if closing < 0:
        return None
    fields_after_comm = source[closing + 1 :].split()
    return fields_after_comm[19] if len(fields_after_comm) > 19 else None


def _scientific_process_group_has_members(
    *,
    pgid: int,
    executor_pid: int,
    proc_root: Path | None = None,
) -> bool:
    """Report whether Linux still has a non-executor member of this attempt group."""

    proc = Path("/proc") if proc_root is None else proc_root
    if not proc.is_dir():
        # macOS is a development/test platform, not a supported GPU dispatcher.
        return False
    try:
        entries = tuple(proc.iterdir())
    except OSError as exc:
        raise ExecutorError(
            f"could not inspect Linux process group {pgid} for terminal drain: {exc}"
        ) from exc
    for entry in entries:
        if not entry.name.isdecimal() or int(entry.name) == executor_pid:
            continue
        try:
            source = (entry / "stat").read_text(encoding="ascii")
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError):
            # /proc is a host-wide namespace. Processes outside the service
            # account may disappear or be unreadable while we scan; queue-owned
            # descendants run as this executor's UID and remain readable.
            continue
        closing = source.rfind(")")
        if closing < 0:
            continue
        fields_after_comm = source[closing + 1 :].split()
        if len(fields_after_comm) <= 2:
            continue
        try:
            member_pgid = int(fields_after_comm[2])
        except ValueError:
            continue
        if member_pgid == pgid:
            return True
    return False


def _publish_launch_receipt(
    *,
    path: Path,
    payload: Mapping[str, object],
    payload_sha256: str,
) -> None:
    """Fsync exact executor identity before the scientific process can exist."""

    pid = os.getpid()
    pgid = os.getpgrp()
    if pgid != pid:
        raise ExecutorError(
            f"durable executor process group {pgid} differs from its PID {pid}"
        )
    process_start_ticks = _process_start_ticks(pid)
    if Path("/proc").is_dir() and process_start_ticks is None:
        raise ExecutorError(
            f"could not authenticate Linux executor process start time for PID {pid}"
        )
    _atomic_create_json(
        path,
        {
            "schema_version": 1,
            "queue_item_id": payload["queue_item_id"],
            "project_id": payload["project_id"],
            "project_key": payload["project_key"],
            "project_revision_id": payload["project_revision_id"],
            "project_revision": payload["project_revision"],
            "experiment_id": payload["experiment_id"],
            "attempt": payload["attempt"],
            "segment": payload["segment"],
            "payload_sha256": payload_sha256,
            "pid": pid,
            "pgid": pgid,
            "process_start_ticks": process_start_ticks,
            "gpu_uuid": os.environ.get("EXPERIMENT_QUEUE_GPU_UUID"),
            "published_at": utc_now_iso(),
        },
    )


def execute_payload(path: Path) -> int:
    """Execute one attempt, coalescing each graceful signal per executor."""

    (
        payload,
        argv,
        cwd,
        receipt_path,
        launch_receipt_path,
        payload_sha256,
    ) = _validated_payload(Path(path))
    signals_received: list[str] = []
    pending_signals: list[int] = []
    accepted_signals: set[int] = set()
    child: subprocess.Popen[bytes] | None = None

    def signal_scientific_group_once(signum: int) -> None:
        """Broadcast once while suppressing the executor's self-delivery."""

        if child is None:
            return
        # The executor is the session/process-group leader.  Ignoring only
        # during this synchronous broadcast prevents recursive self-forwarding
        # while every existing scientific descendant receives the signal once.
        signal.signal(signum, signal.SIG_IGN)
        try:
            os.killpg(os.getpgrp(), signum)
        except ProcessLookupError:
            pass
        finally:
            signal.signal(signum, forward)

    def forward(signum: int, _frame: object) -> None:
        # Durable senders intentionally retry after ambiguous crashes. Manual
        # yield and termination both use SIGINT, so the executor coalesces each
        # graceful signum to at most one scientific-group broadcast per segment.
        if signum in accepted_signals:
            return
        accepted_signals.add(signum)
        signals_received.append(signal.Signals(signum).name)
        if child is None:
            pending_signals.append(signum)
        else:
            signal_scientific_group_once(signum)

    previous: dict[int, object] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, forward)
    started_at = utc_now_iso()
    try:
        # Publication is the launch barrier.  Handlers are already installed,
        # so a scheduler signal in the durable sidecar-to-Popen crash window is
        # coalesced and replayed once after the child handle is assigned.
        _publish_launch_receipt(
            path=launch_receipt_path,
            payload=payload,
            payload_sha256=payload_sha256,
        )
        child = subprocess.Popen(argv, cwd=cwd, shell=False)
        for signum in pending_signals:
            if child.poll() is not None:
                break
            signal_scientific_group_once(signum)
        pending_signals.clear()
        raw_return_code = child.wait()
        return_code = (
            128 + abs(raw_return_code) if raw_return_code < 0 else raw_return_code
        )
        while _scientific_process_group_has_members(
            pgid=os.getpgrp(),
            executor_pid=os.getpid(),
        ):
            time.sleep(0.05)
    except OSError as exc:
        return_code = 127
        signals_received.append(f"launch_error:{exc}")
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)  # type: ignore[arg-type]

    receipt = {
        "schema_version": 3,
        "queue_item_id": payload["queue_item_id"],
        "project_id": payload["project_id"],
        "project_revision_id": payload["project_revision_id"],
        "project_key": payload["project_key"],
        "project_revision": payload["project_revision"],
        "experiment_id": payload["experiment_id"],
        "attempt": payload["attempt"],
        "resolved_spec_sha256": payload["resolved_spec_sha256"],
        "admission_kind": payload["admission_kind"],
        "segment": payload["segment"],
        "git_commit": payload["git_commit"],
        "worktree": payload["worktree"],
        "command_kind": payload["command_kind"],
        "command_sha256": payload["command_sha256"],
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "return_code": return_code,
        "signals_received": signals_received,
        "gpu_uuid": os.environ.get("EXPERIMENT_QUEUE_GPU_UUID"),
    }
    _atomic_write_json(receipt_path, receipt)
    return return_code


def main(argv: Sequence[str] | None = None) -> int:
    """Run the internal executor CLI without exposing a general shell surface."""

    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1:
        print(
            "experiment queue executor error: expected one absolute scheduler-owned "
            "payload path",
            file=sys.stderr,
        )
        return 2
    path = Path(arguments[0])
    if not path.is_absolute():
        print(
            f"experiment queue executor error: payload path must be absolute: {path}",
            file=sys.stderr,
        )
        return 2
    try:
        return execute_payload(path)
    except ExecutorError as exc:
        print(f"experiment queue executor error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ExecutorError",
    "ExecutorEvidencePublicationError",
    "ExecutorEvidencePublicationUncertainError",
    "ExecutorLaunchReceipt",
    "ExecutorReceipt",
    "MAX_EXECUTOR_PAYLOAD_BYTES",
    "confirm_immutable_evidence_for_read",
    "execute_payload",
    "main",
]
