"""Generic CooperativeYield/v1 documents and project-side helper APIs.

The queue owns the request and validates the resulting receipt.  A scientific
project owns how checkpoints are produced and how the opaque resume context is
interpreted; this module deliberately does not import project code or name a
tracker, framework, or checkpoint format.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Final, Self, TypeAlias, cast

from experiment_queue.protocols import (
    COOPERATIVE_YIELD_RECEIPT_V1,
    COOPERATIVE_YIELD_REQUEST_V1,
    ProtocolIdentityError,
    ProtocolVersion,
)


YIELD_REQUEST_ENV: Final = "EXPERIMENT_QUEUE_YIELD_REQUEST_PATH"
YIELD_RECEIPT_ENV: Final = "EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"
CONTINUATION_RECEIPT_ENV: Final = "EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH"

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_GIT_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,63}\Z")
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,255}\Z")
_MEDIA_TYPE_PATTERN = re.compile(
    r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+\Z"
)
_MAX_NOTE_CHARACTERS: Final = 1_000
_MAX_ERROR_CHARACTERS: Final = 4_096
_HASH_CHUNK_BYTES: Final = 1024 * 1024
_MAX_SAFE_JSON_INTEGER: Final = (2**53) - 1
_TIMESTAMP_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.[0-9]+)?"
    r"(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class CooperativeYieldError(ValueError):
    """Base class for an invalid cooperative-yield operation."""


class YieldDocumentError(CooperativeYieldError):
    """Raised when a request or receipt is malformed or unsupported."""


class YieldIntegrityError(CooperativeYieldError):
    """Raised when continuation evidence or a checkpoint has changed."""


class YieldRequestKind(StrEnum):
    """Queue-owned reasons that may request a cooperative checkpoint."""

    MANUAL_PREEMPTION = "manual_preemption"
    GPU_RESERVATION = "gpu_reservation"


class YieldReceiptStatus(StrEnum):
    """Terminal outcomes of one cooperative-yield request."""

    READY = "ready"
    FAILED = "failed"


def utc_now_iso() -> str:
    """Return a UTC protocol timestamp with second precision."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _require_integer(value: object, *, field: str, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > _MAX_SAFE_JSON_INTEGER
    ):
        raise YieldDocumentError(
            f"{field} must be an integer from {minimum} through "
            f"{_MAX_SAFE_JSON_INTEGER} for interoperable JSON"
        )
    return value


def _require_unicode_scalar_text(value: str, *, field: str) -> None:
    """Reject Python-only surrogate code points that are not Unicode text."""

    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise YieldDocumentError(
            f"{field} must contain only Unicode scalar values, not lone surrogates"
        )


def _require_log_safe_path_text(value: str, *, field: str) -> None:
    """Reject path spellings that POSIX cannot use or receipts cannot log safely."""

    _require_unicode_scalar_text(value, field=field)
    if any(
        ord(character) < 32
        or 0x7F <= ord(character) <= 0x9F
        or ord(character) in {0x2028, 0x2029}
        for character in value
    ):
        raise YieldDocumentError(
            f"{field} must not contain NUL, control, or line-separator characters"
        )


def _require_text(
    value: object,
    *,
    field: str,
    maximum: int,
    pattern: re.Pattern[str] | None = None,
) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise YieldDocumentError(
            f"{field} must be a non-empty string without surrounding whitespace"
        )
    _require_unicode_scalar_text(value, field=field)
    if len(value) > maximum:
        raise YieldDocumentError(f"{field} must be {maximum} characters or fewer")
    if pattern is not None and pattern.fullmatch(value) is None:
        raise YieldDocumentError(f"{field} has invalid syntax: {value!r}")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise YieldDocumentError(f"{field} must not contain control characters")
    return value


def _require_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise YieldDocumentError(
            f"{field} must be a lowercase 64-character SHA-256 digest"
        )
    return value


def _require_timestamp(value: object, *, field: str) -> str:
    timestamp = _require_text(value, field=field, maximum=64)
    matched = _TIMESTAMP_PATTERN.fullmatch(timestamp)
    if matched is None:
        raise YieldDocumentError(
            f"{field} must use RFC 3339 spelling "
            "YYYY-MM-DDTHH:MM:SS[.fraction](Z|+HH:MM|-HH:MM)"
        )
    if (
        int(matched.group("hour")) > 23
        or int(matched.group("minute")) > 59
        or int(matched.group("second")) > 59
    ):
        raise YieldDocumentError(
            f"{field} must be a real date and time with a valid UTC offset"
        )
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise YieldDocumentError(
            f"{field} must be a real date and time with a valid UTC offset"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise YieldDocumentError(
            f"{field} must be a real date and time with a valid UTC offset"
        )
    return timestamp


def _require_exact_fields(
    document: Mapping[str, object],
    *,
    expected: set[str],
    label: str,
) -> None:
    actual_fields = set(document)
    non_text_fields = [field for field in actual_fields if not isinstance(field, str)]
    if non_text_fields:
        raise YieldDocumentError(
            f"{label} object keys must be strings, got {non_text_fields!r}"
        )
    missing = sorted(expected - actual_fields)
    unknown = sorted(actual_fields - expected)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing fields {missing}")
        if unknown:
            details.append(f"unknown fields {unknown}")
        raise YieldDocumentError(f"{label} has invalid fields: " + "; ".join(details))


def _require_protocol_identity(
    document: Mapping[str, object],
    *,
    expected: ProtocolVersion,
    label: str,
) -> None:
    try:
        actual = ProtocolVersion.from_document(document)
    except ProtocolIdentityError as exc:
        raise YieldDocumentError(f"{label} has invalid protocol identity: {exc}") from exc
    if actual != expected:
        raise YieldDocumentError(
            f"{label} uses unsupported {actual.kind.value}/v{actual.major}; "
            f"expected {expected.kind.value}/v{expected.major}"
        )


def _sha256_transcript(fields: Iterable[tuple[str, str]]) -> str:
    """Hash an unambiguous, length-framed sequence of UTF-8 evidence fields."""

    digest = hashlib.sha256()
    digest.update(b"experiment-queue/ContinuationIdentity/v1\x00")
    for name, value in fields:
        name_bytes = name.encode("ascii")
        if not isinstance(value, str):
            raise YieldDocumentError(
                f"continuation.{name} must be text, got {type(value).__name__}"
            )
        try:
            value_bytes = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise YieldDocumentError(
                f"continuation.{name} must contain only Unicode scalar values"
            ) from exc
        digest.update(len(name_bytes).to_bytes(4, "big"))
        digest.update(name_bytes)
        digest.update(len(value_bytes).to_bytes(8, "big"))
        digest.update(value_bytes)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    """Return the lowercase SHA-256 digest of exact protocol evidence bytes."""

    if not isinstance(payload, bytes):
        raise TypeError("SHA-256 payload must be bytes")
    return hashlib.sha256(payload).hexdigest()


def _hash_regular_file(source: Path) -> tuple[Path, int, str]:
    """Hash one path-bound stable regular file without following a final symlink.

    The directory entry, opened descriptor, and resolved path must identify the
    same file before and after hashing.  This prevents a path swap between an
    initial policy check and ``open(2)`` from authenticating a different file.
    """

    original = Path(source)
    try:
        entry_before = os.stat(original, follow_symlinks=False)
        resolved_before = original.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise YieldIntegrityError(
            f"checkpoint evidence cannot be inspected as a regular file: {original}: "
            f"{exc}"
        ) from exc
    if stat.S_ISLNK(entry_before.st_mode):
        raise YieldIntegrityError(f"checkpoint evidence must not be a symlink: {original}")
    if not stat.S_ISREG(entry_before.st_mode):
        raise YieldIntegrityError(
            f"checkpoint evidence is not a regular file: {resolved_before}"
        )

    flags = os.O_RDONLY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(original, flags)
    except OSError as exc:
        raise YieldIntegrityError(
            f"checkpoint evidence cannot be opened as a regular file: {original}: {exc}"
        ) from exc
    try:
        descriptor_before = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise YieldIntegrityError(
                f"checkpoint evidence is not a regular file: {resolved_before}"
            )
        if (entry_before.st_dev, entry_before.st_ino) != (
            descriptor_before.st_dev,
            descriptor_before.st_ino,
        ):
            raise YieldIntegrityError(
                f"checkpoint evidence changed before it was opened: {original}"
            )
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(_HASH_CHUNK_BYTES):
                digest.update(chunk)
        descriptor_after = os.fstat(descriptor)
        try:
            entry_after = os.stat(original, follow_symlinks=False)
            resolved_after = original.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise YieldIntegrityError(
                f"checkpoint evidence path changed while it was being hashed: "
                f"{original}: {exc}"
            ) from exc
        if stat.S_ISLNK(entry_after.st_mode):
            raise YieldIntegrityError(
                f"checkpoint evidence became a symlink while it was being hashed: "
                f"{original}"
            )
        identity_before = (
            descriptor_before.st_dev,
            descriptor_before.st_ino,
            descriptor_before.st_mode,
            descriptor_before.st_size,
            descriptor_before.st_mtime_ns,
            descriptor_before.st_ctime_ns,
        )
        identity_after = (
            descriptor_after.st_dev,
            descriptor_after.st_ino,
            descriptor_after.st_mode,
            descriptor_after.st_size,
            descriptor_after.st_mtime_ns,
            descriptor_after.st_ctime_ns,
        )
        entry_identity_after = (
            entry_after.st_dev,
            entry_after.st_ino,
            entry_after.st_mode,
            entry_after.st_size,
            entry_after.st_mtime_ns,
            entry_after.st_ctime_ns,
        )
        if (
            identity_before != identity_after
            or identity_after != entry_identity_after
            or resolved_before != resolved_after
        ):
            raise YieldIntegrityError(
                f"checkpoint evidence changed while it was being hashed: {original}"
            )
        return resolved_after, descriptor_after.st_size, digest.hexdigest()
    finally:
        os.close(descriptor)


def sha256_file(source: Path) -> str:
    """Hash an exact, stable regular file for receipt or checkpoint evidence."""

    return _hash_regular_file(Path(source))[2]


@dataclass(frozen=True, slots=True)
class YieldProgress:
    """Project-neutral monotonic progress attached to a yield result."""

    unit: str
    completed: int
    total: int | None = None

    def __post_init__(self) -> None:
        _require_text(self.unit, field="progress.unit", maximum=64, pattern=_TOKEN_PATTERN)
        _require_integer(self.completed, field="progress.completed")
        if self.total is not None:
            total = _require_integer(self.total, field="progress.total")
            if total < self.completed:
                raise YieldDocumentError(
                    "progress.total must be greater than or equal to progress.completed"
                )

    def to_document(self) -> dict[str, object]:
        """Return a fresh JSON-native progress object."""

        document: dict[str, object] = {
            "unit": self.unit,
            "completed": self.completed,
        }
        if self.total is not None:
            document["total"] = self.total
        return document

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Strictly parse one progress object."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("progress must be an object")
        allowed = {"unit", "completed"}
        if "total" in document:
            allowed.add("total")
        _require_exact_fields(document, expected=allowed, label="progress")
        return cls(
            unit=document["unit"],  # type: ignore[arg-type]
            completed=document["completed"],  # type: ignore[arg-type]
            total=document.get("total"),  # type: ignore[arg-type]
        )

    def assert_not_regressed_from(self, previous: Self) -> None:
        """Reject ambiguous unit changes, lower progress, or a changed known total."""

        if self.unit != previous.unit:
            raise YieldIntegrityError(
                f"progress unit changed from {previous.unit!r} to {self.unit!r}"
            )
        if self.completed < previous.completed:
            raise YieldIntegrityError(
                f"progress regressed from {previous.completed} to {self.completed} "
                f"{self.unit}"
            )
        if previous.total is not None and self.total != previous.total:
            raise YieldIntegrityError(
                f"progress total changed from {previous.total!r} to {self.total!r}"
            )


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    """One named, content-addressed project checkpoint artifact."""

    name: str
    path: str
    bytes: int
    sha256: str
    media_type: str

    def __post_init__(self) -> None:
        _require_text(
            self.name,
            field="checkpoint_artifact.name",
            maximum=64,
            pattern=_TOKEN_PATTERN,
        )
        if not isinstance(self.path, str) or not Path(self.path).is_absolute():
            raise YieldDocumentError(
                f"checkpoint_artifact.path must be absolute, got {self.path!r}"
            )
        _require_log_safe_path_text(self.path, field="checkpoint_artifact.path")
        _require_integer(self.bytes, field="checkpoint_artifact.bytes")
        _require_sha256(self.sha256, field="checkpoint_artifact.sha256")
        _require_text(
            self.media_type,
            field="checkpoint_artifact.media_type",
            maximum=127,
            pattern=_MEDIA_TYPE_PATTERN,
        )

    @classmethod
    def from_file(
        cls,
        name: str,
        path: Path,
        *,
        media_type: str = "application/octet-stream",
    ) -> Self:
        """Resolve and hash a stable regular file for a ready receipt."""

        resolved, size, digest = _hash_regular_file(Path(path))
        return cls(
            name=name,
            path=str(resolved),
            bytes=size,
            sha256=digest,
            media_type=media_type,
        )

    def to_document(self) -> dict[str, object]:
        """Return a fresh JSON-native checkpoint descriptor."""

        return {
            "name": self.name,
            "path": self.path,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "media_type": self.media_type,
        }

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Strictly parse one checkpoint descriptor without touching its path."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("checkpoint artifact must be an object")
        _require_exact_fields(
            document,
            expected={"name", "path", "bytes", "sha256", "media_type"},
            label="checkpoint artifact",
        )
        return cls(
            name=document["name"],  # type: ignore[arg-type]
            path=document["path"],  # type: ignore[arg-type]
            bytes=document["bytes"],  # type: ignore[arg-type]
            sha256=document["sha256"],  # type: ignore[arg-type]
            media_type=document["media_type"],  # type: ignore[arg-type]
        )


def _require_json_native_value(value: object, *, field: str) -> None:
    """Validate an exact, finite, interoperable JSON data-model tree."""

    def visit(current: object, *, location: str, ancestors: set[int]) -> None:
        current_type = type(current)
        if current is None or current_type is bool:
            return
        if current_type is str:
            _require_unicode_scalar_text(current, field=location)
            return
        if current_type is int:
            if not -_MAX_SAFE_JSON_INTEGER <= current <= _MAX_SAFE_JSON_INTEGER:
                raise YieldDocumentError(
                    f"{location} integer is outside the interoperable JSON range "
                    f"[-{_MAX_SAFE_JSON_INTEGER}, {_MAX_SAFE_JSON_INTEGER}]"
                )
            return
        if current_type is float:
            if not math.isfinite(current):
                raise YieldDocumentError(
                    f"{location} must be a finite JSON-native number, "
                    "not NaN or infinite"
                )
            return
        if current_type not in {list, dict}:
            raise YieldDocumentError(
                f"{location} has non-JSON-native type {current_type.__name__}; "
                "use only objects, arrays, strings, safe integers, finite numbers, "
                "booleans, or null"
            )

        identity = id(current)
        if identity in ancestors:
            raise YieldDocumentError(f"{location} contains a recursive JSON value")
        ancestors.add(identity)
        try:
            if current_type is list:
                for index, item in enumerate(current):
                    visit(
                        item,
                        location=f"{location}[{index}]",
                        ancestors=ancestors,
                    )
                return
            for key, item in current.items():
                if type(key) is not str:
                    raise YieldDocumentError(
                        f"{location} object key {key!r} is not a string"
                    )
                _require_unicode_scalar_text(key, field=f"{location} object key")
                visit(
                    item,
                    location=f"{location}[{key!r}]",
                    ancestors=ancestors,
                )
        finally:
            ancestors.remove(identity)

    try:
        visit(value, location=field, ancestors=set())
    except RecursionError as exc:
        raise YieldDocumentError(
            f"{field} exceeds the supported JSON nesting depth"
        ) from exc


def _parse_safe_json_integer(value: str, *, field: str) -> int:
    try:
        integer = int(value)
    except ValueError as exc:
        raise YieldDocumentError(f"{field} contains an invalid JSON integer") from exc
    if not -_MAX_SAFE_JSON_INTEGER <= integer <= _MAX_SAFE_JSON_INTEGER:
        raise YieldDocumentError(
            f"{field} integer is outside the interoperable JSON range "
            f"[-{_MAX_SAFE_JSON_INTEGER}, {_MAX_SAFE_JSON_INTEGER}]"
        )
    return integer


def _decode_strict_json_value(payload: bytes, *, field: str) -> JsonValue:
    """Decode JSON without Python's duplicate, non-finite, or wide-int extensions."""

    try:
        text = payload.decode("utf-8")
        value = json.loads(
            text,
            object_pairs_hook=_json_object_without_duplicates,
            parse_constant=lambda spelling: (_ for _ in ()).throw(
                YieldDocumentError(
                    f"{field} contains unsupported JSON constant {spelling!r}"
                )
            ),
            parse_int=lambda spelling: _parse_safe_json_integer(
                spelling, field=field
            ),
        )
    except YieldDocumentError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise YieldDocumentError(f"{field} is not valid interoperable JSON: {exc}") from exc
    _require_json_native_value(value, field=field)
    return cast(JsonValue, value)


@dataclass(frozen=True, slots=True)
class OpaqueResumeContext:
    """Integrity-protected bytes whose meaning belongs solely to the project."""

    payload: bytes
    media_type: str
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("resume_context.payload must be bytes")
        _require_text(
            self.media_type,
            field="resume_context.media_type",
            maximum=127,
            pattern=_MEDIA_TYPE_PATTERN,
        )
        expected = _require_sha256(self.sha256, field="resume_context.sha256")
        actual = sha256_bytes(self.payload)
        if not hmac.compare_digest(actual, expected):
            raise YieldIntegrityError(
                "resume context SHA-256 does not match its decoded payload"
            )

    @classmethod
    def from_bytes(
        cls,
        payload: bytes,
        *,
        media_type: str = "application/octet-stream",
    ) -> Self:
        """Protect arbitrary project-owned bytes without interpreting them."""

        return cls(
            payload=payload,
            media_type=media_type,
            sha256=sha256_bytes(payload),
        )

    @classmethod
    def from_json(cls, payload: JsonValue) -> Self:
        """Encode a JSON-native project value as opaque deterministic bytes."""

        _require_json_native_value(payload, field="resume context")
        try:
            encoded = json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise YieldDocumentError(
                f"resume context is not a finite JSON-native value: {exc}"
            ) from exc
        return cls.from_bytes(encoded, media_type="application/json")

    def json_value(self) -> JsonValue:
        """Decode a JSON context for project code after generic validation."""

        if self.media_type != "application/json":
            raise YieldDocumentError(
                f"resume context media type is {self.media_type!r}, not application/json"
            )
        return _decode_strict_json_value(self.payload, field="resume context")

    def to_document(self) -> dict[str, object]:
        """Return the base64 wire envelope for the opaque bytes."""

        return {
            "encoding": "base64",
            "media_type": self.media_type,
            "data": base64.b64encode(self.payload).decode("ascii"),
            "bytes": len(self.payload),
            "sha256": self.sha256,
        }

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Decode and authenticate an opaque resume-context envelope."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("resume_context must be an object")
        _require_exact_fields(
            document,
            expected={"encoding", "media_type", "data", "bytes", "sha256"},
            label="resume_context",
        )
        if document["encoding"] != "base64":
            raise YieldDocumentError("resume_context.encoding must be 'base64'")
        data = document["data"]
        if not isinstance(data, str):
            raise YieldDocumentError("resume_context.data must be a base64 string")
        try:
            payload = base64.b64decode(data, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise YieldDocumentError(
                "resume_context.data must use valid canonical base64"
            ) from exc
        if base64.b64encode(payload).decode("ascii") != data:
            raise YieldDocumentError(
                "resume_context.data must use valid canonical base64"
            )
        expected_bytes = _require_integer(
            document["bytes"], field="resume_context.bytes"
        )
        if len(payload) != expected_bytes:
            raise YieldIntegrityError(
                "resume context decoded size does not match resume_context.bytes"
            )
        return cls(
            payload=payload,
            media_type=document["media_type"],  # type: ignore[arg-type]
            sha256=document["sha256"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class ContinuationIdentity:
    """Immutable evidence binding one resume to the exact admitted execution.

    ``prior_receipt_sha256`` is the SHA-256 of the exact receipt bytes selected
    by the queue as the preceding segment evidence.  The queue decides which
    receipt is authoritative and records that choice; project code only echoes
    the already-bound identity from the yield request.
    """

    resolved_spec_sha256: str
    project_revision: str
    git_commit: str
    run_id: str
    prior_receipt_sha256: str
    identity_sha256: str

    def __post_init__(self) -> None:
        _require_sha256(
            self.resolved_spec_sha256, field="continuation.resolved_spec_sha256"
        )
        _require_text(
            self.project_revision,
            field="continuation.project_revision",
            maximum=256,
            pattern=_IDENTIFIER_PATTERN,
        )
        if (
            not isinstance(self.git_commit, str)
            or _GIT_COMMIT_PATTERN.fullmatch(self.git_commit) is None
        ):
            raise YieldDocumentError(
                "continuation.git_commit must be a lowercase 40- or 64-character "
                "Git object ID"
            )
        _require_text(
            self.run_id,
            field="continuation.run_id",
            maximum=256,
            pattern=_IDENTIFIER_PATTERN,
        )
        _require_sha256(
            self.prior_receipt_sha256,
            field="continuation.prior_receipt_sha256",
        )
        supplied = _require_sha256(
            self.identity_sha256,
            field="continuation.identity_sha256",
        )
        computed = self._computed_digest()
        if not hmac.compare_digest(supplied, computed):
            raise YieldIntegrityError(
                "continuation identity SHA-256 does not match its bound evidence"
            )

    def _evidence_fields(self) -> tuple[tuple[str, str], ...]:
        return (
            ("resolved_spec_sha256", self.resolved_spec_sha256),
            ("project_revision", self.project_revision),
            ("git_commit", self.git_commit),
            ("run_id", self.run_id),
            ("prior_receipt_sha256", self.prior_receipt_sha256),
        )

    def _computed_digest(self) -> str:
        return _sha256_transcript(self._evidence_fields())

    @classmethod
    def create(
        cls,
        *,
        resolved_spec_sha256: str,
        project_revision: str,
        git_commit: str,
        run_id: str,
        prior_receipt_sha256: str,
    ) -> Self:
        """Build a continuation binding without duplicating digest logic."""

        evidence = (
            ("resolved_spec_sha256", resolved_spec_sha256),
            ("project_revision", project_revision),
            ("git_commit", git_commit),
            ("run_id", run_id),
            ("prior_receipt_sha256", prior_receipt_sha256),
        )
        return cls(
            resolved_spec_sha256=resolved_spec_sha256,
            project_revision=project_revision,
            git_commit=git_commit,
            run_id=run_id,
            prior_receipt_sha256=prior_receipt_sha256,
            identity_sha256=_sha256_transcript(evidence),
        )

    def to_document(self) -> dict[str, object]:
        """Return a fresh JSON-native continuation identity."""

        return {
            "resolved_spec_sha256": self.resolved_spec_sha256,
            "project_revision": self.project_revision,
            "git_commit": self.git_commit,
            "run_id": self.run_id,
            "prior_receipt_sha256": self.prior_receipt_sha256,
            "identity_sha256": self.identity_sha256,
        }

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Strictly parse and authenticate a continuation identity."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("continuation must be an object")
        _require_exact_fields(
            document,
            expected={
                "resolved_spec_sha256",
                "project_revision",
                "git_commit",
                "run_id",
                "prior_receipt_sha256",
                "identity_sha256",
            },
            label="continuation",
        )
        return cls(
            resolved_spec_sha256=document["resolved_spec_sha256"],  # type: ignore[arg-type]
            project_revision=document["project_revision"],  # type: ignore[arg-type]
            git_commit=document["git_commit"],  # type: ignore[arg-type]
            run_id=document["run_id"],  # type: ignore[arg-type]
            prior_receipt_sha256=document["prior_receipt_sha256"],  # type: ignore[arg-type]
            identity_sha256=document["identity_sha256"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class CooperativeYieldRequest:
    """Strict queue-to-project CooperativeYieldRequest/v1 document."""

    request_id: str
    queue_item_id: int
    segment: int
    request_kind: YieldRequestKind
    requested_at: str
    requested_by: str
    note: str
    continuation: ContinuationIdentity

    def __post_init__(self) -> None:
        _require_text(
            self.request_id,
            field="yield_request.request_id",
            maximum=256,
            pattern=_IDENTIFIER_PATTERN,
        )
        _require_integer(self.queue_item_id, field="yield_request.queue_item_id", minimum=1)
        _require_integer(self.segment, field="yield_request.segment", minimum=1)
        if not isinstance(self.request_kind, YieldRequestKind):
            raise YieldDocumentError(
                "yield_request.request_kind must be a YieldRequestKind"
            )
        _require_timestamp(self.requested_at, field="yield_request.requested_at")
        _require_text(
            self.requested_by,
            field="yield_request.requested_by",
            maximum=256,
        )
        _require_text(
            self.note,
            field="yield_request.note",
            maximum=_MAX_NOTE_CHARACTERS,
        )
        if not isinstance(self.continuation, ContinuationIdentity):
            raise YieldDocumentError(
                "yield_request.continuation must be a ContinuationIdentity"
            )

    def to_document(self) -> dict[str, object]:
        """Return the strict CooperativeYieldRequest/v1 wire document."""

        return {
            **COOPERATIVE_YIELD_REQUEST_V1.document_identity(),
            "request_id": self.request_id,
            "queue_item_id": self.queue_item_id,
            "segment": self.segment,
            "request_kind": self.request_kind.value,
            "requested_at": self.requested_at,
            "requested_by": self.requested_by,
            "note": self.note,
            "continuation": self.continuation.to_document(),
        }

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Strictly parse one CooperativeYieldRequest/v1 object."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("yield request must be a JSON object")
        _require_protocol_identity(
            document,
            expected=COOPERATIVE_YIELD_REQUEST_V1,
            label="yield request",
        )
        _require_exact_fields(
            document,
            expected={
                "apiVersion",
                "kind",
                "request_id",
                "queue_item_id",
                "segment",
                "request_kind",
                "requested_at",
                "requested_by",
                "note",
                "continuation",
            },
            label="yield request",
        )
        try:
            request_kind = YieldRequestKind(document["request_kind"])
        except (TypeError, ValueError) as exc:
            raise YieldDocumentError(
                f"yield_request.request_kind is unsupported: "
                f"{document['request_kind']!r}"
            ) from exc
        return cls(
            request_id=document["request_id"],  # type: ignore[arg-type]
            queue_item_id=document["queue_item_id"],  # type: ignore[arg-type]
            segment=document["segment"],  # type: ignore[arg-type]
            request_kind=request_kind,
            requested_at=document["requested_at"],  # type: ignore[arg-type]
            requested_by=document["requested_by"],  # type: ignore[arg-type]
            note=document["note"],  # type: ignore[arg-type]
            continuation=ContinuationIdentity.from_document(document["continuation"]),
        )


@dataclass(frozen=True, slots=True)
class CooperativeYieldReceipt:
    """Strict project-to-queue CooperativeYieldReceipt/v1 document."""

    request_id: str
    queue_item_id: int
    segment: int
    status: YieldReceiptStatus
    written_at: str
    progress: YieldProgress | None
    continuation: ContinuationIdentity | None = None
    checkpoint_artifacts: tuple[CheckpointArtifact, ...] = ()
    resume_context: OpaqueResumeContext | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_text(
            self.request_id,
            field="yield_receipt.request_id",
            maximum=256,
            pattern=_IDENTIFIER_PATTERN,
        )
        _require_integer(self.queue_item_id, field="yield_receipt.queue_item_id", minimum=1)
        _require_integer(self.segment, field="yield_receipt.segment", minimum=1)
        if not isinstance(self.status, YieldReceiptStatus):
            raise YieldDocumentError("yield_receipt.status must be a YieldReceiptStatus")
        _require_timestamp(self.written_at, field="yield_receipt.written_at")
        if self.progress is not None and not isinstance(self.progress, YieldProgress):
            raise YieldDocumentError("yield_receipt.progress must be YieldProgress or null")
        if not isinstance(self.checkpoint_artifacts, tuple):
            raise YieldDocumentError("yield_receipt.checkpoint_artifacts must be a tuple")
        names: set[str] = set()
        paths: set[str] = set()
        for artifact in self.checkpoint_artifacts:
            if not isinstance(artifact, CheckpointArtifact):
                raise YieldDocumentError(
                    "yield_receipt.checkpoint_artifacts must contain CheckpointArtifact values"
                )
            if artifact.name in names:
                raise YieldDocumentError(
                    f"yield receipt repeats checkpoint artifact name {artifact.name!r}"
                )
            if artifact.path in paths:
                raise YieldDocumentError(
                    f"yield receipt repeats checkpoint artifact path {artifact.path!r}"
                )
            names.add(artifact.name)
            paths.add(artifact.path)
        if self.status is YieldReceiptStatus.READY:
            if self.progress is None:
                raise YieldDocumentError("ready yield receipt requires typed progress")
            if not isinstance(self.continuation, ContinuationIdentity):
                raise YieldDocumentError("ready yield receipt requires continuation identity")
            if not self.checkpoint_artifacts:
                raise YieldDocumentError(
                    "ready yield receipt requires at least one checkpoint artifact"
                )
            if not isinstance(self.resume_context, OpaqueResumeContext):
                raise YieldDocumentError("ready yield receipt requires resume_context")
            if self.error is not None:
                raise YieldDocumentError("ready yield receipt must not contain error")
        else:
            if self.continuation is not None or self.checkpoint_artifacts:
                raise YieldDocumentError(
                    "failed yield receipt must not claim continuation artifacts"
                )
            if self.resume_context is not None:
                raise YieldDocumentError(
                    "failed yield receipt must not contain resume_context"
                )
            _require_text(
                self.error,
                field="yield_receipt.error",
                maximum=_MAX_ERROR_CHARACTERS,
            )

    @classmethod
    def ready(
        cls,
        request: CooperativeYieldRequest,
        *,
        progress: YieldProgress,
        checkpoint_artifacts: Iterable[CheckpointArtifact],
        resume_context: OpaqueResumeContext,
        written_at: str | None = None,
    ) -> Self:
        """Build a ready receipt that echoes the queue-owned request identity."""

        if not isinstance(request, CooperativeYieldRequest):
            raise TypeError("ready yield receipt requires CooperativeYieldRequest")
        return cls(
            request_id=request.request_id,
            queue_item_id=request.queue_item_id,
            segment=request.segment,
            status=YieldReceiptStatus.READY,
            written_at=written_at or utc_now_iso(),
            progress=progress,
            continuation=request.continuation,
            checkpoint_artifacts=tuple(checkpoint_artifacts),
            resume_context=resume_context,
        )

    @classmethod
    def failed(
        cls,
        request: CooperativeYieldRequest,
        *,
        error: str,
        progress: YieldProgress | None = None,
        written_at: str | None = None,
    ) -> Self:
        """Build a failed receipt that leaves the running job non-resumable."""

        if not isinstance(request, CooperativeYieldRequest):
            raise TypeError("failed yield receipt requires CooperativeYieldRequest")
        return cls(
            request_id=request.request_id,
            queue_item_id=request.queue_item_id,
            segment=request.segment,
            status=YieldReceiptStatus.FAILED,
            written_at=written_at or utc_now_iso(),
            progress=progress,
            error=error,
        )

    def to_document(self) -> dict[str, object]:
        """Return the strict status-specific CooperativeYieldReceipt/v1 document."""

        document: dict[str, object] = {
            **COOPERATIVE_YIELD_RECEIPT_V1.document_identity(),
            "request_id": self.request_id,
            "queue_item_id": self.queue_item_id,
            "segment": self.segment,
            "status": self.status.value,
            "written_at": self.written_at,
        }
        if self.progress is not None:
            document["progress"] = self.progress.to_document()
        if self.status is YieldReceiptStatus.READY:
            assert self.continuation is not None
            assert self.resume_context is not None
            document.update(
                {
                    "continuation": self.continuation.to_document(),
                    "checkpoint_artifacts": [
                        artifact.to_document()
                        for artifact in self.checkpoint_artifacts
                    ],
                    "resume_context": self.resume_context.to_document(),
                }
            )
        else:
            document["error"] = self.error
        return document

    @classmethod
    def from_document(cls, document: object) -> Self:
        """Strictly parse one status-specific CooperativeYieldReceipt/v1 object."""

        if not isinstance(document, Mapping):
            raise YieldDocumentError("yield receipt must be a JSON object")
        _require_protocol_identity(
            document,
            expected=COOPERATIVE_YIELD_RECEIPT_V1,
            label="yield receipt",
        )
        try:
            status = YieldReceiptStatus(document.get("status"))
        except (TypeError, ValueError) as exc:
            raise YieldDocumentError(
                f"yield_receipt.status is unsupported: {document.get('status')!r}"
            ) from exc
        common = {
            "apiVersion",
            "kind",
            "request_id",
            "queue_item_id",
            "segment",
            "status",
            "written_at",
        }
        if status is YieldReceiptStatus.READY:
            expected = common | {
                "progress",
                "continuation",
                "checkpoint_artifacts",
                "resume_context",
            }
        else:
            expected = common | {"error"}
            if "progress" in document:
                expected.add("progress")
        _require_exact_fields(document, expected=expected, label="yield receipt")
        progress = (
            YieldProgress.from_document(document["progress"])
            if "progress" in document
            else None
        )
        if status is YieldReceiptStatus.READY:
            artifacts_value = document["checkpoint_artifacts"]
            if not isinstance(artifacts_value, list):
                raise YieldDocumentError("checkpoint_artifacts must be an array")
            return cls(
                request_id=document["request_id"],  # type: ignore[arg-type]
                queue_item_id=document["queue_item_id"],  # type: ignore[arg-type]
                segment=document["segment"],  # type: ignore[arg-type]
                status=status,
                written_at=document["written_at"],  # type: ignore[arg-type]
                progress=progress,
                continuation=ContinuationIdentity.from_document(
                    document["continuation"]
                ),
                checkpoint_artifacts=tuple(
                    CheckpointArtifact.from_document(artifact)
                    for artifact in artifacts_value
                ),
                resume_context=OpaqueResumeContext.from_document(
                    document["resume_context"]
                ),
            )
        return cls(
            request_id=document["request_id"],  # type: ignore[arg-type]
            queue_item_id=document["queue_item_id"],  # type: ignore[arg-type]
            segment=document["segment"],  # type: ignore[arg-type]
            status=status,
            written_at=document["written_at"],  # type: ignore[arg-type]
            progress=progress,
            error=document["error"],  # type: ignore[arg-type]
        )


def _json_object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise YieldDocumentError(f"protocol JSON repeats object key {key!r}")
        document[key] = value
    return document


def _read_json_object(path: Path, *, label: str) -> dict[str, object]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise YieldDocumentError(f"{label} is not a regular file: {source}")
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise YieldDocumentError(f"{label} cannot be read: {source}: {exc}") from exc
    document = _decode_strict_json_value(raw, field=label)
    if type(document) is not dict:
        raise YieldDocumentError(f"{label} must contain a JSON object: {source}")
    return cast(dict[str, object], document)


def _encode_document(document: Mapping[str, object]) -> bytes:
    try:
        return (
            json.dumps(
                document,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise YieldDocumentError(f"protocol document is not finite JSON: {exc}") from exc


def _atomic_write_document(path: Path, document: Mapping[str, object]) -> None:
    """Durably replace one document without exposing a partial receipt."""

    destination = Path(path)
    if not destination.is_absolute():
        raise YieldDocumentError(
            f"protocol document path must be absolute: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    encoded = _encode_document(document)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        directory_descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_yield_request(path: Path) -> CooperativeYieldRequest:
    """Read and strictly validate CooperativeYieldRequest/v1 from disk."""

    return CooperativeYieldRequest.from_document(
        _read_json_object(Path(path), label="yield request")
    )


def write_yield_request(path: Path, request: CooperativeYieldRequest) -> None:
    """Atomically write a queue-owned CooperativeYieldRequest/v1."""

    if not isinstance(request, CooperativeYieldRequest):
        raise TypeError("write_yield_request requires CooperativeYieldRequest")
    _atomic_write_document(Path(path), request.to_document())


def read_yield_receipt(path: Path) -> CooperativeYieldReceipt:
    """Read and strictly validate CooperativeYieldReceipt/v1 from disk."""

    return CooperativeYieldReceipt.from_document(
        _read_json_object(Path(path), label="yield receipt")
    )


def write_yield_receipt(path: Path, receipt: CooperativeYieldReceipt) -> None:
    """Atomically write a project-owned CooperativeYieldReceipt/v1."""

    if not isinstance(receipt, CooperativeYieldReceipt):
        raise TypeError("write_yield_receipt requires CooperativeYieldReceipt")
    _atomic_write_document(Path(path), receipt.to_document())


def validate_receipt_for_request(
    receipt: CooperativeYieldReceipt,
    request: CooperativeYieldRequest,
    *,
    previous_progress: YieldProgress | None = None,
) -> None:
    """Reject stale, cross-item, or regressed request/receipt pairs."""

    comparisons = (
        ("request_id", receipt.request_id, request.request_id),
        ("queue_item_id", receipt.queue_item_id, request.queue_item_id),
        ("segment", receipt.segment, request.segment),
    )
    for field, actual, expected in comparisons:
        if actual != expected:
            raise YieldIntegrityError(
                f"yield receipt {field} {actual!r} does not match request {expected!r}"
            )
    if receipt.status is YieldReceiptStatus.READY:
        if receipt.continuation != request.continuation:
            raise YieldIntegrityError(
                "yield receipt continuation identity does not match the request"
            )
    if previous_progress is not None:
        if receipt.progress is None:
            raise YieldIntegrityError(
                "yield receipt omits progress required to prove non-regression"
            )
        receipt.progress.assert_not_regressed_from(previous_progress)


def validate_continuation_identity(
    identity: ContinuationIdentity,
    *,
    resolved_spec_sha256: str,
    project_revision: str,
    git_commit: str,
    run_id: str,
    prior_receipt_sha256: str,
) -> None:
    """Rebuild and compare the complete continuation evidence binding."""

    expected = ContinuationIdentity.create(
        resolved_spec_sha256=resolved_spec_sha256,
        project_revision=project_revision,
        git_commit=git_commit,
        run_id=run_id,
        prior_receipt_sha256=prior_receipt_sha256,
    )
    for field in (
        "resolved_spec_sha256",
        "project_revision",
        "git_commit",
        "run_id",
        "prior_receipt_sha256",
    ):
        actual_value = getattr(identity, field)
        expected_value = getattr(expected, field)
        matches = (
            hmac.compare_digest(actual_value, expected_value)
            if field.endswith("sha256")
            else actual_value == expected_value
        )
        if not matches:
            raise YieldIntegrityError(
                f"continuation {field} does not match admitted execution evidence"
            )
    if not hmac.compare_digest(identity.identity_sha256, expected.identity_sha256):
        raise YieldIntegrityError(
            "continuation identity digest does not match admitted execution evidence"
        )


def _path_is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
        except ValueError:
            continue
        return True
    return False


def _resolve_allowed_artifact_roots(allowed_roots: Iterable[Path]) -> tuple[Path, ...]:
    """Resolve queue-authorized roots or fail with an operator-actionable error."""

    roots: list[Path] = []
    try:
        root_values = iter(allowed_roots)
    except TypeError as exc:
        raise YieldIntegrityError(
            "allowed artifact roots must be an iterable of absolute paths to "
            "existing directories"
        ) from exc
    for index, root in enumerate(root_values, start=1):
        try:
            candidate = Path(root)
        except (TypeError, ValueError) as exc:
            raise YieldIntegrityError(
                f"allowed artifact root #{index} is not a filesystem path: {root!r}; "
                "provide an absolute path to an existing directory"
            ) from exc
        if not candidate.is_absolute():
            raise YieldIntegrityError(
                f"allowed artifact root #{index} must be absolute, got {candidate!s}; "
                "provide an absolute path to an existing directory"
            )
        try:
            resolved = candidate.resolve(strict=True)
            root_metadata = resolved.stat()
        except (OSError, RuntimeError, ValueError) as exc:
            raise YieldIntegrityError(
                f"allowed artifact root #{index} cannot be resolved: {candidate}: {exc}; "
                "provide an absolute path to an existing directory"
            ) from exc
        if not stat.S_ISDIR(root_metadata.st_mode):
            raise YieldIntegrityError(
                f"allowed artifact root #{index} must resolve to a directory, "
                f"got {resolved}"
            )
        roots.append(resolved)
    if not roots:
        raise YieldIntegrityError(
            "checkpoint verification requires at least one allowed artifact root"
        )
    return tuple(roots)


def verify_checkpoint_artifacts(
    artifacts: Iterable[CheckpointArtifact],
    *,
    allowed_roots: Iterable[Path],
) -> tuple[CheckpointArtifact, ...]:
    """Rehash ready artifacts and enforce queue-supplied resolved path roots."""

    artifact_values = tuple(artifacts)
    if not artifact_values:
        raise YieldIntegrityError("continuation has no checkpoint artifacts")
    roots = _resolve_allowed_artifact_roots(allowed_roots)
    for artifact in artifact_values:
        source = Path(artifact.path)
        if not source.is_absolute():
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} path must be absolute: {source}"
            )
        try:
            resolved = source.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} cannot be resolved: {source}: {exc}"
            ) from exc
        if not _path_is_within(resolved, roots):
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} is outside allowed roots: "
                f"{resolved}"
            )
        actual_path, actual_bytes, actual_sha256 = _hash_regular_file(source)
        if actual_path != resolved:
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} changed path during verification"
            )
        if actual_bytes != artifact.bytes:
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} size differs from its receipt"
            )
        if not hmac.compare_digest(actual_sha256, artifact.sha256):
            raise YieldIntegrityError(
                f"checkpoint artifact {artifact.name!r} SHA-256 differs from its receipt"
            )
    return artifact_values


def validate_ready_continuation(
    receipt: CooperativeYieldReceipt,
    request: CooperativeYieldRequest,
    *,
    resolved_spec_sha256: str,
    project_revision: str,
    git_commit: str,
    run_id: str,
    prior_receipt_sha256: str,
    allowed_artifact_roots: Iterable[Path],
    expected_checkpoint_names: Iterable[str],
    previous_progress: YieldProgress | None = None,
) -> None:
    """Perform the queue's complete admission check for a resumable receipt."""

    validate_receipt_for_request(
        receipt,
        request,
        previous_progress=previous_progress,
    )
    if receipt.status is not YieldReceiptStatus.READY:
        raise YieldIntegrityError(
            f"yield receipt status is {receipt.status.value!r}, not 'ready'"
        )
    try:
        expected_names = tuple(expected_checkpoint_names)
    except TypeError as exc:
        raise YieldIntegrityError(
            "declared checkpoint artifact names must be a nonempty iterable of text"
        ) from exc
    if not expected_names:
        raise YieldIntegrityError(
            "resumable admission requires at least one declared checkpoint artifact"
        )
    if any(
        not isinstance(name, str) or _TOKEN_PATTERN.fullmatch(name) is None
        for name in expected_names
    ):
        raise YieldIntegrityError(
            "declared checkpoint artifact names must use the protocol token grammar"
        )
    if len(set(expected_names)) != len(expected_names):
        raise YieldIntegrityError("declared checkpoint artifact names must be unique")
    actual_names = {artifact.name for artifact in receipt.checkpoint_artifacts}
    missing_names = sorted(set(expected_names) - actual_names)
    unexpected_names = sorted(actual_names - set(expected_names))
    if missing_names or unexpected_names:
        raise YieldIntegrityError(
            "yield receipt checkpoint artifacts differ from the admitted card: "
            f"missing {missing_names}; unexpected {unexpected_names}"
        )
    assert receipt.continuation is not None
    validate_continuation_identity(
        receipt.continuation,
        resolved_spec_sha256=resolved_spec_sha256,
        project_revision=project_revision,
        git_commit=git_commit,
        run_id=run_id,
        prior_receipt_sha256=prior_receipt_sha256,
    )
    verify_checkpoint_artifacts(
        receipt.checkpoint_artifacts,
        allowed_roots=allowed_artifact_roots,
    )


def read_continuation_receipt_from_environment(
    environment: Mapping[str, str] | None = None,
) -> CooperativeYieldReceipt | None:
    """Read the authenticated prior ready receipt exposed to a resumed segment."""

    values = os.environ if environment is None else environment
    raw = values.get(CONTINUATION_RECEIPT_ENV)
    if raw is None:
        return None
    if not raw:
        raise YieldDocumentError(f"{CONTINUATION_RECEIPT_ENV} must not be empty")
    path = Path(raw)
    if not path.is_absolute():
        raise YieldDocumentError(
            f"{CONTINUATION_RECEIPT_ENV} must name an absolute scheduler-owned file"
        )
    receipt = read_yield_receipt(path)
    if receipt.status is not YieldReceiptStatus.READY:
        raise YieldDocumentError(
            f"{CONTINUATION_RECEIPT_ENV} must contain a ready receipt"
        )
    return receipt


@dataclass(frozen=True, slots=True)
class CooperativeYieldHelper:
    """Optional project-side helper for polling and responding without dependencies."""

    request_path: Path
    receipt_path: Path

    def __post_init__(self) -> None:
        normalized_paths: list[Path] = []
        for field, value in (
            ("request_path", self.request_path),
            ("receipt_path", self.receipt_path),
        ):
            if not isinstance(value, Path) or not value.is_absolute():
                raise YieldDocumentError(f"yield helper {field} must be an absolute Path")
            _require_log_safe_path_text(str(value), field=f"yield helper {field}")
            try:
                normalized_paths.append(value.resolve(strict=False))
            except (OSError, RuntimeError, ValueError) as exc:
                raise YieldDocumentError(
                    f"yield helper {field} cannot be normalized: {value}: {exc}"
                ) from exc
        same_existing_file = False
        try:
            same_existing_file = os.path.samefile(
                self.request_path,
                self.receipt_path,
            )
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise YieldDocumentError(
                "yield request/receipt identity cannot be inspected: "
                f"{self.request_path}, {self.receipt_path}: {exc}"
            ) from exc
        if normalized_paths[0] == normalized_paths[1] or same_existing_file:
            raise YieldDocumentError(
                "yield request and receipt paths must identify different files"
            )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> Self | None:
        """Discover queue control paths, or return ``None`` for incapable jobs."""

        values = os.environ if environment is None else environment
        request_value = values.get(YIELD_REQUEST_ENV)
        receipt_value = values.get(YIELD_RECEIPT_ENV)
        if request_value is None and receipt_value is None:
            return None
        if not request_value or not receipt_value:
            raise YieldDocumentError(
                f"{YIELD_REQUEST_ENV} and {YIELD_RECEIPT_ENV} must be set together"
            )
        return cls(request_path=Path(request_value), receipt_path=Path(receipt_value))

    def request_if_present(self) -> CooperativeYieldRequest | None:
        """Poll the atomic request path without treating absence as an error."""

        try:
            self.request_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise YieldDocumentError(
                f"yield request path cannot be inspected: {self.request_path}: {exc}"
            ) from exc
        return read_yield_request(self.request_path)

    def write_ready(
        self,
        request: CooperativeYieldRequest,
        *,
        checkpoint_files: Mapping[str, Path],
        progress: YieldProgress,
        resume_context: OpaqueResumeContext,
        media_types: Mapping[str, str] | None = None,
        written_at: str | None = None,
    ) -> CooperativeYieldReceipt:
        """Hash project checkpoints and atomically publish a ready receipt."""

        declared_media_types = media_types or {}
        unknown_media_types = sorted(set(declared_media_types) - set(checkpoint_files))
        if unknown_media_types:
            raise YieldDocumentError(
                "media_types names no checkpoint file for "
                f"{unknown_media_types}"
            )
        artifacts = tuple(
            CheckpointArtifact.from_file(
                name,
                checkpoint_path,
                media_type=declared_media_types.get(
                    name, "application/octet-stream"
                ),
            )
            for name, checkpoint_path in checkpoint_files.items()
        )
        receipt = CooperativeYieldReceipt.ready(
            request,
            progress=progress,
            checkpoint_artifacts=artifacts,
            resume_context=resume_context,
            written_at=written_at,
        )
        write_yield_receipt(self.receipt_path, receipt)
        return receipt

    def write_failed(
        self,
        request: CooperativeYieldRequest,
        *,
        error: str,
        progress: YieldProgress | None = None,
        written_at: str | None = None,
    ) -> CooperativeYieldReceipt:
        """Atomically report that the job could not produce a safe continuation."""

        receipt = CooperativeYieldReceipt.failed(
            request,
            error=error,
            progress=progress,
            written_at=written_at,
        )
        write_yield_receipt(self.receipt_path, receipt)
        return receipt


__all__ = [
    "CONTINUATION_RECEIPT_ENV",
    "CheckpointArtifact",
    "ContinuationIdentity",
    "CooperativeYieldError",
    "CooperativeYieldHelper",
    "CooperativeYieldReceipt",
    "CooperativeYieldRequest",
    "JsonValue",
    "OpaqueResumeContext",
    "YIELD_RECEIPT_ENV",
    "YIELD_REQUEST_ENV",
    "YieldDocumentError",
    "YieldIntegrityError",
    "YieldProgress",
    "YieldReceiptStatus",
    "YieldRequestKind",
    "read_yield_receipt",
    "read_continuation_receipt_from_environment",
    "read_yield_request",
    "sha256_bytes",
    "sha256_file",
    "utc_now_iso",
    "validate_continuation_identity",
    "validate_ready_continuation",
    "validate_receipt_for_request",
    "verify_checkpoint_artifacts",
    "write_yield_receipt",
    "write_yield_request",
]
