"""Persist and rehydrate typed multi-project state through schema-v5.

This service layer is the only ordinary typed mutation boundary above the raw
fresh-v5 schema.  It validates lifecycle models and exact evidence before every
insert, keeps registration/revision/admission/event changes transactional, and
turns SQLite failures into operator-actionable domain errors.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Final, Iterator, Mapping, cast

from experiment_queue.admission import (
    AdmissionError,
    AdmissionSnapshot,
    admission_snapshot_to_stored_evidence,
    rehydrate_admission_snapshot,
)
from experiment_queue.authoring import Project
from experiment_queue.cooperative_yield import (
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    CooperativeYieldError,
    YieldReceiptStatus,
    validate_ready_continuation,
    validate_receipt_for_request,
)
from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.git_resolver import (
    GitBlobEvidence,
    GitResolvedAdmission,
    GitResolvedProjectRevision,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    LifecycleValidationError,
    MountBinding,
    ProjectHealth,
    ProjectLifecycle,
    ProjectRevision,
    ProjectRuntimeState,
    RegisteredProject,
)
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


_TYPED_ADMISSION_KIND: Final = "ExperimentCard/v1"
_TYPED_REVISION_KIND: Final = "project-v1"
_RUNNER_NAME: Final = "run-experiment"
_TERMINAL_NON_SUCCESS_STATES: Final = frozenset(
    {"failed", "interrupted", "force_killed", "removed"}
)
_ALL_QUEUE_STATES: Final = frozenset(
    {
        "queued",
        "held",
        "blocked",
        "starting",
        "running",
        "yielding",
        "terminating",
        "force_killing",
        "succeeded",
        "failed",
        "interrupted",
        "force_killed",
        "removed",
    }
)
_FULL_GIT_OBJECT_PATTERN: Final = re.compile(
    r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z"
)
_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_MAX_PROTOCOL_BYTES: Final = 8 * 1024 * 1024
_ADMISSION_EVIDENCE_COLUMNS: Final[tuple[str, ...]] = (
    "project_revision_label",
    "git_commit",
    "project_source_name",
    "project_source",
    "project_source_sha256",
    "project_normalized_json",
    "project_normalized_sha256",
    "project_schema_api_version",
    "project_schema_kind",
    "project_schema_id",
    "project_schema_sha256",
    "card_source_name",
    "card_source",
    "card_source_sha256",
    "card_normalized_json",
    "card_normalized_sha256",
    "card_schema_api_version",
    "card_schema_kind",
    "card_schema_id",
    "card_schema_sha256",
    "extension_schema_source_name",
    "extension_schema_reference_path",
    "extension_schema_source",
    "extension_schema_source_sha256",
    "extension_schema_canonical_json",
    "extension_schema_canonical_sha256",
    "extension_schema_id",
    "resolved_json",
    "resolved_sha256",
    "command_kind",
    "command_json",
    "command_sha256",
    "package_version",
    "policy_project_key",
    "policy_card_path",
    "policy_job_id",
    "policy_priority",
    "policy_hold_reason",
    "policy_operator",
    "policy_preemption_authorized",
    "policy_bindings_json",
    "policy_bindings_sha256",
    "policy_dependencies_json",
    "policy_dependencies_sha256",
    "policy_json",
    "policy_sha256",
)


class V5RepositoryError(RuntimeError):
    """Raised when a typed schema-v5 service operation cannot complete safely."""


class V5NotFoundError(V5RepositoryError):
    """Raised when a requested Project, revision, item, snapshot, or event is absent."""


class V5EvidenceError(V5RepositoryError):
    """Raised when stored or caller-supplied immutable evidence fails validation."""


@dataclass(frozen=True, slots=True)
class V5ProjectView:
    """Typed Project show/list result with current revision and queue counts."""

    project: RegisteredProject
    runtime_state: ProjectRuntimeState
    current_revision: ProjectRevision = field(repr=False)
    queue_counts: tuple[tuple[str, int], ...] = ()

    @property
    def dispatch_allowed(self) -> bool:
        """Return the combined operator-lifecycle and health-circuit decision."""

        return self.project.dispatch_allowed(self.runtime_state)


@dataclass(frozen=True, slots=True)
class V5QueueItem:
    """Immutable read view of scheduler-critical queue identity and state."""

    id: int
    project_id: int
    revision_id: int
    admission_kind: str
    snapshot_id: int | None
    job_id: str | None
    experiment_id: str
    attempt: int
    state: str
    priority: int
    card_path: str
    card_sha256: str
    command_text: str
    runner_name: str
    git_commit: str
    added_at: str
    added_by: str
    state_detail: str | None
    preemptible: bool
    segment: int
    resume_front: bool
    snapshot: AdmissionSnapshot | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class V5Event:
    """Append-only host/project event with canonical JSON payload bytes."""

    id: int
    created_at: str
    actor: str
    event_type: str
    queue_item_id: int | None
    scope: str
    project_id: int | None
    _payload_json: str = field(repr=False)

    @property
    def payload(self) -> dict[str, JSONValue]:
        """Return a fresh decoded payload object."""

        value = json.loads(self._payload_json)
        assert type(value) is dict
        return cast(dict[str, JSONValue], value)

    @property
    def payload_json(self) -> str:
        """Return the exact persisted canonical JSON text."""

        return self._payload_json


@dataclass(frozen=True, slots=True)
class V5YieldRequestRecord:
    """Authenticated typed request plus its exact persisted wire bytes."""

    request: CooperativeYieldRequest
    source: bytes = field(repr=False)
    sha256: str
    project_id: int
    revision_id: int


@dataclass(frozen=True, slots=True)
class V5YieldReceiptRecord:
    """Authenticated typed receipt plus its exact persisted wire bytes."""

    receipt: CooperativeYieldReceipt
    source: bytes = field(repr=False)
    sha256: str
    project_id: int
    revision_id: int


@dataclass(frozen=True, slots=True)
class V5StoredGitBlob:
    """Rehashed Git regular-blob evidence loaded from immutable v5 storage."""

    path: str
    object_id: str
    mode: str
    size: int
    source_sha256: str


@dataclass(frozen=True, slots=True)
class V5RevisionGitEvidence:
    """Stored revision provenance separate from the lifecycle-neutral model."""

    revision_id: int
    repository_root: str
    git_commit: str
    project_blob: V5StoredGitBlob
    extension_schema_blob: V5StoredGitBlob | None


def _require_text(value: object, *, field_name: str, maximum: int = 4_000) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5RepositoryError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum:
        raise V5RepositoryError(
            f"{field_name} must be {maximum} characters or fewer, got {len(value)}"
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise V5RepositoryError(f"{field_name} must not contain control characters")
    return value


def _require_timestamp(value: object, *, field_name: str) -> str:
    timestamp = _require_text(value, field_name=field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise V5RepositoryError(
            f"{field_name} must be an RFC 3339 date-time with explicit UTC offset"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V5RepositoryError(
            f"{field_name} must include Z or an explicit UTC offset"
        )
    return timestamp


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise V5RepositoryError(
            f"{field_name} must be a positive integer, got {value!r}"
        )
    return value


def _canonical_payload(payload: Mapping[str, object]) -> str:
    if not isinstance(payload, Mapping):
        raise TypeError(
            f"event payload must be a mapping, got {type(payload).__name__}"
        )
    try:
        encoded = canonical_json_bytes(dict(payload))
    except (TypeError, ValueError) as exc:
        raise V5RepositoryError(
            f"event payload must be bounded canonical JSON: {exc}"
        ) from exc
    return encoded.decode("utf-8")


def _decode_canonical_blob(value: object, *, field_name: str) -> JSONValue:
    if type(value) is not bytes:
        raise V5EvidenceError(
            f"stored {field_name} must be a SQLite BLOB, got {type(value).__name__}"
        )
    try:
        decoded = cast(JSONValue, json.loads(value.decode("utf-8", errors="strict")))
        encoded = canonical_json_bytes(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise V5EvidenceError(
            f"stored {field_name} is not bounded canonical JSON: {exc}"
        ) from exc
    if encoded != value:
        raise V5EvidenceError(
            f"stored {field_name} is JSON but not its exact canonical encoding"
        )
    return decoded


def _row_bytes(row: sqlite3.Row, field_name: str, *, optional: bool = False) -> bytes | None:
    value = row[field_name]
    if optional and value is None:
        return None
    if type(value) is not bytes:
        raise V5EvidenceError(
            f"stored {field_name} must be BLOB bytes, got {type(value).__name__}"
        )
    return value


def _row_text(row: sqlite3.Row, field_name: str, *, optional: bool = False) -> str | None:
    value = row[field_name]
    if optional and value is None:
        return None
    if type(value) is not str:
        raise V5EvidenceError(
            f"stored {field_name} must be text, got {type(value).__name__}"
        )
    return value


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _git_blob_object_id(source: bytes, object_id: str) -> str:
    """Recompute a loose Git blob identity using the recorded hash family."""

    header = f"blob {len(source)}\0".encode("ascii")
    if len(object_id) == 40:
        return hashlib.sha1(header + source, usedforsecurity=False).hexdigest()
    if len(object_id) == 64:
        return hashlib.sha256(header + source).hexdigest()
    return ""


def _validate_git_blob(
    blob: GitBlobEvidence,
    *,
    source: bytes,
    expected_path: str,
    field_name: str,
) -> None:
    if type(blob) is not GitBlobEvidence:
        raise V5EvidenceError(
            f"{field_name} must be exact factory-only GitBlobEvidence"
        )
    if blob.path != expected_path:
        raise V5EvidenceError(
            f"{field_name}.path {blob.path!r} does not match source path "
            f"{expected_path!r}"
        )
    if (
        type(blob.object_id) is not str
        or _FULL_GIT_OBJECT_PATTERN.fullmatch(blob.object_id) is None
        or blob.object_id != blob.object_id.lower()
    ):
        raise V5EvidenceError(
            f"{field_name}.object_id must be a lowercase full Git object ID"
        )
    if blob.mode not in {"100644", "100755"}:
        raise V5EvidenceError(
            f"{field_name}.mode must identify a regular Git blob, got "
            f"{blob.mode!r}"
        )
    if type(blob.size) is not int or blob.size != len(source):
        raise V5EvidenceError(
            f"{field_name}.size {blob.size!r} does not match source byte length "
            f"{len(source)}"
        )
    digest = sha256_bytes(source)
    if blob.source_sha256 != digest:
        raise V5EvidenceError(
            f"{field_name}.source_sha256 does not match exact source bytes"
        )
    computed_object_id = _git_blob_object_id(source, blob.object_id)
    if computed_object_id != blob.object_id:
        raise V5EvidenceError(
            f"{field_name}.object_id {blob.object_id!r} is not the Git blob "
            "identity recomputed from the exact source bytes"
        )


def _wire_protocol_bytes(document: Mapping[str, object]) -> bytes:
    """Match the CooperativeYield/v1 deterministic on-disk JSON encoder."""

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
    except (TypeError, ValueError, UnicodeError) as exc:
        raise V5EvidenceError(
            f"cooperative-yield document is not finite UTF-8 JSON: {exc}"
        ) from exc


def _decode_wire_protocol(source: object, *, label: str) -> dict[str, object]:
    if type(source) is not bytes:
        raise TypeError(f"{label} source must be immutable bytes")
    if not source or len(source) > _MAX_PROTOCOL_BYTES:
        raise V5EvidenceError(
            f"{label} source must contain 1 through {_MAX_PROTOCOL_BYTES} bytes"
        )

    def object_without_duplicates(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise V5EvidenceError(f"{label} repeats JSON object key {key!r}")
            result[key] = value
        return result

    try:
        document = json.loads(
            source.decode("utf-8", errors="strict"),
            object_pairs_hook=object_without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON token {token}")
            ),
        )
    except V5EvidenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise V5EvidenceError(f"{label} is not strict UTF-8 JSON: {exc}") from exc
    if type(document) is not dict:
        raise V5EvidenceError(f"{label} must contain one JSON object")
    typed = cast(dict[str, object], document)
    if _wire_protocol_bytes(typed) != source:
        raise V5EvidenceError(
            f"{label} does not use the exact deterministic CooperativeYield/v1 "
            "wire encoding"
        )
    return typed


class V5ProjectRepository:
    """Typed project/admission service over an explicitly initialized v5 store."""

    def __init__(self, store: V5QueueStore):
        if type(store) is not V5QueueStore:
            raise TypeError(
                f"store must be exactly V5QueueStore, got {type(store).__name__}"
            )
        self.store = store

    @contextmanager
    def _connection(
        self,
        *,
        operation: str,
        write: bool,
    ) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        try:
            connection = self.store.connect()
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except sqlite3.Error as exc:
            if connection is not None:
                connection.rollback()
            raise V5RepositoryError(
                f"schema-v5 could not {operation}: {exc}; no partial typed "
                "operation was committed"
            ) from exc
        except V5DatabaseError as exc:
            if connection is not None:
                connection.rollback()
            raise V5RepositoryError(
                f"schema-v5 could not {operation}: {exc}"
            ) from exc
        except BaseException:
            if connection is not None and write:
                connection.rollback()
            raise
        finally:
            if connection is not None:
                connection.close()

    def _assert_root_isolation(
        self,
        connection: sqlite3.Connection,
        revision: ProjectRevision,
    ) -> None:
        """Recheck actual state and every other Project's persisted root."""

        revision.enrollment.validate_current_paths()
        local_roots: list[tuple[str, Path]] = [
            ("checkout", revision.enrollment.checkout_directory)
        ]
        local_roots.extend(
            (f"mount {mount.name!r}", mount.path)
            for mount in revision.enrollment.mounts
        )
        for environment in revision.enrollment.environments:
            local_roots.extend(
                (
                    f"environment {environment.name!r} search directory {index}",
                    path,
                )
                for index, path in enumerate(
                    environment.executable_search_directories
                )
            )
        state_directory = self.store.state_dir.resolve(strict=True)
        for label, path in local_roots:
            if _paths_overlap(state_directory, path):
                raise V5RepositoryError(
                    f"Project {revision.project_key!r} {label} {str(path)!r} "
                    f"overlaps actual queue state directory "
                    f"{str(state_directory)!r}; bind distinct roots"
                )

        existing: list[tuple[str, str, Path]] = []
        for row in connection.execute(
            """
            SELECT p.project_key, r.id, r.checkout_path
            FROM project_revisions AS r
            JOIN projects AS p ON p.id = r.project_id
            WHERE r.project_id <> ?
            """,
            (revision.project_id,),
        ):
            existing.append(
                (
                    str(row["project_key"]),
                    f"revision {int(row['id'])} checkout",
                    Path(str(row["checkout_path"])),
                )
            )
        for row in connection.execute(
            """
            SELECT p.project_key, m.revision_id, m.mount_name, m.mount_path
            FROM project_mounts AS m
            JOIN projects AS p ON p.id = m.project_id
            WHERE m.project_id <> ?
            """,
            (revision.project_id,),
        ):
            existing.append(
                (
                    str(row["project_key"]),
                    f"revision {int(row['revision_id'])} mount "
                    f"{str(row['mount_name'])!r}",
                    Path(str(row["mount_path"])),
                )
            )
        for row in connection.execute(
            """
            SELECT p.project_key, e.revision_id, e.environment_name,
                   e.search_directories_json
            FROM project_environments AS e
            JOIN projects AS p ON p.id = e.project_id
            WHERE e.project_id <> ?
            """,
            (revision.project_id,),
        ):
            directories = _decode_canonical_blob(
                row["search_directories_json"],
                field_name="project_environments.search_directories_json",
            )
            if type(directories) is not list or not all(
                type(path) is str for path in directories
            ):
                raise V5EvidenceError(
                    "stored environment search directories must be a JSON string "
                    "array"
                )
            existing.extend(
                (
                    str(row["project_key"]),
                    f"revision {int(row['revision_id'])} environment "
                    f"{str(row['environment_name'])!r} search directory {index}",
                    Path(cast(str, path)),
                )
                for index, path in enumerate(directories)
            )
        for local_label, local_path in local_roots:
            for project_key, existing_label, existing_path in existing:
                if _paths_overlap(local_path, existing_path):
                    raise V5RepositoryError(
                        f"Project {revision.project_key!r} {local_label} "
                        f"{str(local_path)!r} overlaps Project {project_key!r} "
                        f"{existing_label} {str(existing_path)!r}; version 1 "
                        "requires distinct roots"
                    )

    @staticmethod
    def _validate_resolved_revision(
        resolved: GitResolvedProjectRevision,
    ) -> ProjectRevision:
        """Revalidate all factory-only Git proof fields before persistence."""

        if type(resolved) is not GitResolvedProjectRevision:
            raise TypeError(
                "resolved_revision must be exactly GitResolvedProjectRevision "
                f"from verify_project_revision(), got {type(resolved).__name__}"
            )
        revision = resolved.revision
        if type(revision) is not ProjectRevision:
            raise V5EvidenceError(
                "resolved revision does not contain an exact ProjectRevision"
            )
        expected = (
            revision.project_id,
            revision.id,
            revision.project_key,
            revision.label,
            str(revision.enrollment.checkout_directory),
            revision.git_commit,
        )
        actual = (
            resolved.project_id,
            resolved.project_revision_id,
            resolved.project_key,
            resolved.project_revision_label,
            resolved.repository_root,
            resolved.git_commit,
        )
        if actual != expected:
            raise V5EvidenceError(
                "GitResolvedProjectRevision identity/root/commit fields do not "
                "match its immutable ProjectRevision"
            )
        _validate_git_blob(
            resolved.project_blob,
            source=revision.project_source,
            expected_path=revision.project_source_path,
            field_name="resolved_revision.project_blob",
        )
        extension_blob = resolved.extension_schema_blob
        if revision.extension_schema_source is None:
            if extension_blob is not None:
                raise V5EvidenceError(
                    "resolved revision has extension blob evidence although the "
                    "ProjectRevision declares no extension schema"
                )
        else:
            if extension_blob is None or revision.extension_schema_source_path is None:
                raise V5EvidenceError(
                    "resolved revision is missing required extension-schema Git "
                    "blob evidence"
                )
            _validate_git_blob(
                extension_blob,
                source=revision.extension_schema_source,
                expected_path=revision.extension_schema_source_path,
                field_name="resolved_revision.extension_schema_blob",
            )
        return revision

    def _insert_revision(
        self,
        connection: sqlite3.Connection,
        resolved_revision: GitResolvedProjectRevision,
    ) -> None:
        """Insert one complete immutable typed revision and all child bindings."""

        revision = self._validate_resolved_revision(resolved_revision)
        project_blob = resolved_revision.project_blob
        extension_blob = resolved_revision.extension_schema_blob
        self._assert_root_isolation(connection, revision)
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, project_source_path, project_source,
                project_source_sha256, project_blob_object_id,
                project_blob_mode, project_blob_size, project_normalized_json,
                project_normalized_sha256, project_schema_api_version,
                project_schema_kind, project_schema_id, project_schema_sha256,
                extension_schema_source_path, extension_schema_source,
                extension_schema_source_sha256,
                extension_schema_blob_object_id, extension_schema_blob_mode,
                extension_schema_blob_size,
                extension_schema_canonical_json,
                extension_schema_canonical_sha256, extension_schema_id,
                checkout_path, project_manifest_path, enrollment_json,
                enrollment_sha256, validated_package_version, created_at,
                created_actor
            ) VALUES (
                ?, ?, ?, ?, 'project-v1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                revision.id,
                revision.project_id,
                revision.sequence,
                revision.label,
                revision.display_name,
                revision.git_commit,
                revision.project_source_path,
                revision.project_source,
                revision.project_source_sha256,
                project_blob.object_id,
                project_blob.mode,
                project_blob.size,
                revision.project_normalized_json,
                revision.project_normalized_sha256,
                revision.project_schema_api_version,
                revision.project_schema_kind,
                revision.project_schema_id,
                revision.project_schema_sha256,
                revision.extension_schema_source_path,
                revision.extension_schema_source,
                revision.extension_schema_source_sha256,
                None if extension_blob is None else extension_blob.object_id,
                None if extension_blob is None else extension_blob.mode,
                None if extension_blob is None else extension_blob.size,
                revision.extension_schema_canonical_json,
                revision.extension_schema_canonical_sha256,
                revision.extension_schema_id,
                str(revision.enrollment.checkout_directory),
                revision.enrollment.project_manifest_path,
                revision.enrollment.canonical_json,
                revision.enrollment.sha256,
                revision.validated_package_version,
                revision.created_at,
                revision.created_actor,
            ),
        )
        declarations = {volume.name: volume for volume in revision.project.volumes}
        for mount in revision.enrollment.mounts:
            declaration = declarations[mount.name]
            checkout_descendant = (
                revision.enrollment.checkout_directory in mount.path.parents
            )
            git_ignored = checkout_descendant and any(
                proof == mount.path or proof in mount.path.parents
                for proof in revision.enrollment.git_ignored_checkout_descendants
            )
            connection.execute(
                """
                INSERT INTO project_mounts(
                    project_id, revision_id, mount_name, mount_path,
                    declared_access, access, required, checkout_descendant,
                    git_ignored
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    revision.project_id,
                    revision.id,
                    mount.name,
                    str(mount.path),
                    declaration.access.value,
                    mount.access.value,
                    int(declaration.required is True),
                    int(checkout_descendant),
                    int(git_ignored),
                ),
            )
        for artifact_root in revision.enrollment.artifact_roots:
            connection.execute(
                """
                INSERT INTO project_artifact_roots(
                    project_id, revision_id, mount_name
                ) VALUES (?, ?, ?)
                """,
                (revision.project_id, revision.id, artifact_root.name),
            )
        for environment in revision.enrollment.environments:
            search_json = canonical_json_bytes(
                [str(path) for path in environment.executable_search_directories]
            )
            prefix_json = (
                None
                if environment.command_prefix_argv is None
                else canonical_json_bytes(list(environment.command_prefix_argv))
            )
            inherit_json = canonical_json_bytes(list(environment.inherit_variables))
            connection.execute(
                """
                INSERT INTO project_environments(
                    project_id, revision_id, environment_name,
                    search_directories_json, search_directories_sha256,
                    command_prefix_json, command_prefix_sha256,
                    inherit_variables_json, inherit_variables_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    revision.project_id,
                    revision.id,
                    environment.name,
                    search_json,
                    sha256_bytes(search_json),
                    prefix_json,
                    None if prefix_json is None else sha256_bytes(prefix_json),
                    inherit_json,
                    sha256_bytes(inherit_json),
                ),
            )

    def _load_enrollment(
        self,
        row: sqlite3.Row,
        project: Project,
    ) -> Enrollment:
        enrollment_json = cast(bytes, _row_bytes(row, "enrollment_json"))
        document = _decode_canonical_blob(
            enrollment_json,
            field_name="project_revisions.enrollment_json",
        )
        if type(document) is not dict:
            raise V5EvidenceError("stored Enrollment root must be a JSON object")
        required = {
            "apiVersion",
            "kind",
            "projectKey",
            "projectNormalizedSha256",
            "checkoutDirectory",
            "projectManifestPath",
            "mounts",
            "artifactRoots",
            "environments",
            "gitIgnoredCheckoutDescendants",
        }
        if set(document) != required:
            raise V5EvidenceError(
                "stored Enrollment has invalid fields; expected exact "
                f"{sorted(required)}, got {sorted(document)}"
            )
        if (
            document["apiVersion"] != "experiment-queue/v1"
            or document["kind"] != "Enrollment"
        ):
            raise V5EvidenceError(
                "stored Enrollment must identify experiment-queue/v1 Enrollment"
            )
        mounts_value = document["mounts"]
        environments_value = document["environments"]
        ignored_value = document["gitIgnoredCheckoutDescendants"]
        if type(mounts_value) is not list or type(environments_value) is not list:
            raise V5EvidenceError(
                "stored Enrollment mounts and environments must be JSON arrays"
            )
        if type(ignored_value) is not list or not all(
            type(path) is str for path in ignored_value
        ):
            raise V5EvidenceError(
                "stored Enrollment Git-ignore proofs must be a JSON string array"
            )
        mounts: list[MountBinding] = []
        for index, value in enumerate(mounts_value):
            if type(value) is not dict or set(value) != {"name", "path", "access"}:
                raise V5EvidenceError(
                    f"stored Enrollment mounts[{index}] must contain exactly "
                    "name, path, and access"
                )
            mounts.append(
                MountBinding.create(
                    name=cast(str, value["name"]),
                    path=cast(str, value["path"]),
                    access=cast(str, value["access"]),
                )
            )
        environments: list[EnvironmentBinding] = []
        for index, value in enumerate(environments_value):
            if type(value) is not dict:
                raise V5EvidenceError(
                    f"stored Enrollment environments[{index}] must be an object"
                )
            environments.append(
                EnvironmentBinding.from_document(cast(dict[str, object], value))
            )
        try:
            enrollment = Enrollment.create(
                project=project,
                checkout_directory=cast(str, document["checkoutDirectory"]),
                project_manifest_path=cast(str, document["projectManifestPath"]),
                mounts=mounts,
                environments=environments,
                state_directory=self.store.state_dir,
                git_ignored_checkout_descendants=cast(list[str], ignored_value),
            )
        except (LifecycleValidationError, TypeError) as exc:
            raise V5EvidenceError(
                f"stored Enrollment could not be revalidated against current "
                f"paths and Project/v1: {exc}"
            ) from exc
        if enrollment.canonical_json != enrollment_json:
            raise V5EvidenceError(
                "stored Enrollment bytes differ from exact evidence reconstructed "
                "through the lifecycle model"
            )
        stored_digest = cast(str, _row_text(row, "enrollment_sha256"))
        if enrollment.sha256 != stored_digest:
            raise V5EvidenceError(
                f"stored Enrollment SHA-256 {stored_digest!r} does not match "
                f"recomputed {enrollment.sha256}"
            )
        return enrollment

    def _verify_revision_children(
        self,
        connection: sqlite3.Connection,
        revision: ProjectRevision,
    ) -> None:
        declarations = {volume.name: volume for volume in revision.project.volumes}
        expected_mounts: list[tuple[object, ...]] = []
        for mount in revision.enrollment.mounts:
            declaration = declarations[mount.name]
            checkout_descendant = (
                revision.enrollment.checkout_directory in mount.path.parents
            )
            git_ignored = checkout_descendant and any(
                proof == mount.path or proof in mount.path.parents
                for proof in revision.enrollment.git_ignored_checkout_descendants
            )
            expected_mounts.append(
                (
                    mount.name,
                    str(mount.path),
                    declaration.access.value,
                    mount.access.value,
                    int(declaration.required is True),
                    int(checkout_descendant),
                    int(git_ignored),
                )
            )
        actual_mounts = [
            tuple(row)
            for row in connection.execute(
                """
                SELECT mount_name, mount_path, declared_access, access, required,
                       checkout_descendant, git_ignored
                FROM project_mounts
                WHERE project_id = ? AND revision_id = ?
                ORDER BY mount_name
                """,
                (revision.project_id, revision.id),
            )
        ]
        if sorted(expected_mounts) != actual_mounts:
            raise V5EvidenceError(
                f"stored mount rows for revision {revision.label!r} differ from "
                "its exact Enrollment and Project declarations"
            )
        expected_artifacts = sorted(root.name for root in revision.enrollment.artifact_roots)
        actual_artifacts = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT mount_name FROM project_artifact_roots
                WHERE project_id = ? AND revision_id = ? ORDER BY mount_name
                """,
                (revision.project_id, revision.id),
            )
        ]
        if expected_artifacts != actual_artifacts:
            raise V5EvidenceError(
                f"stored artifact-root rows for revision {revision.label!r} "
                "differ from its derived read-write mount subset"
            )
        environment_rows = {
            str(row["environment_name"]): row
            for row in connection.execute(
                """
                SELECT * FROM project_environments
                WHERE project_id = ? AND revision_id = ?
                """,
                (revision.project_id, revision.id),
            )
        }
        if set(environment_rows) != {
            environment.name for environment in revision.enrollment.environments
        }:
            raise V5EvidenceError(
                f"stored environment rows for revision {revision.label!r} do not "
                "match its complete Enrollment"
            )
        for environment in revision.enrollment.environments:
            child = environment_rows[environment.name]
            search_json = canonical_json_bytes(
                [str(path) for path in environment.executable_search_directories]
            )
            prefix_json = (
                None
                if environment.command_prefix_argv is None
                else canonical_json_bytes(list(environment.command_prefix_argv))
            )
            inherit_json = canonical_json_bytes(list(environment.inherit_variables))
            expected = (
                search_json,
                sha256_bytes(search_json),
                prefix_json,
                None if prefix_json is None else sha256_bytes(prefix_json),
                inherit_json,
                sha256_bytes(inherit_json),
            )
            actual = tuple(
                child[name]
                for name in (
                    "search_directories_json",
                    "search_directories_sha256",
                    "command_prefix_json",
                    "command_prefix_sha256",
                    "inherit_variables_json",
                    "inherit_variables_sha256",
                )
            )
            if expected != actual:
                raise V5EvidenceError(
                    f"stored environment {environment.name!r} evidence for "
                    f"revision {revision.label!r} differs from recomputed canonical "
                    "binding bytes or hashes"
                )

    @staticmethod
    def _stored_git_blob(
        row: sqlite3.Row,
        *,
        prefix: str,
        path: str,
        source: bytes,
        optional: bool,
    ) -> V5StoredGitBlob | None:
        object_id = _row_text(
            row,
            f"{prefix}_blob_object_id",
            optional=optional,
        )
        mode = _row_text(row, f"{prefix}_blob_mode", optional=optional)
        size_value = row[f"{prefix}_blob_size"]
        presence = (object_id is not None, mode is not None, size_value is not None)
        if optional and not any(presence):
            return None
        if not all(presence):
            raise V5EvidenceError(
                f"stored {prefix} Git blob evidence is partial; object ID, mode, "
                "and size must be all present or all null"
            )
        assert object_id is not None and mode is not None
        if (
            _FULL_GIT_OBJECT_PATTERN.fullmatch(object_id) is None
            or object_id != object_id.lower()
        ):
            raise V5EvidenceError(
                f"stored {prefix} Git blob object ID is not a lowercase full ID"
            )
        if mode not in {"100644", "100755"}:
            raise V5EvidenceError(
                f"stored {prefix} Git blob mode {mode!r} is not a regular file"
            )
        if type(size_value) is not int or size_value != len(source):
            raise V5EvidenceError(
                f"stored {prefix} Git blob size {size_value!r} does not match "
                f"source byte length {len(source)}"
            )
        source_sha256 = sha256_bytes(source)
        if _git_blob_object_id(source, object_id) != object_id:
            raise V5EvidenceError(
                f"stored {prefix} Git blob object ID does not authenticate its "
                "exact source bytes"
            )
        return V5StoredGitBlob(
            path=path,
            object_id=object_id,
            mode=mode,
            size=size_value,
            source_sha256=source_sha256,
        )

    def _revision_git_evidence_from_row(
        self,
        row: sqlite3.Row,
        revision: ProjectRevision,
    ) -> V5RevisionGitEvidence:
        project_blob = self._stored_git_blob(
            row,
            prefix="project",
            path=revision.project_source_path,
            source=revision.project_source,
            optional=False,
        )
        assert project_blob is not None
        extension_source = revision.extension_schema_source
        if extension_source is None:
            extension_blob = self._stored_git_blob(
                row,
                prefix="extension_schema",
                path="",
                source=b"",
                optional=True,
            )
            if extension_blob is not None:
                raise V5EvidenceError(
                    "stored revision has extension Git blob evidence but no "
                    "extension-schema source"
                )
        else:
            assert revision.extension_schema_source_path is not None
            extension_blob = self._stored_git_blob(
                row,
                prefix="extension_schema",
                path=revision.extension_schema_source_path,
                source=extension_source,
                optional=False,
            )
        return V5RevisionGitEvidence(
            revision_id=revision.id,
            repository_root=str(revision.enrollment.checkout_directory),
            git_commit=revision.git_commit,
            project_blob=project_blob,
            extension_schema_blob=extension_blob,
        )

    def _load_revision(
        self,
        connection: sqlite3.Connection,
        revision_id: int,
    ) -> ProjectRevision:
        row = connection.execute(
            "SELECT * FROM project_revisions WHERE id = ?",
            (_positive_integer(revision_id, field_name="revision_id"),),
        ).fetchone()
        if row is None:
            raise V5NotFoundError(
                f"schema-v5 has no ProjectRevision with id {revision_id}"
            )
        if row["revision_kind"] != _TYPED_REVISION_KIND:
            raise V5EvidenceError(
                f"revision id {revision_id} is {row['revision_kind']!r}; legacy-v4 "
                "rows cannot be rehydrated as typed ProjectRevision models"
            )
        project_source = cast(bytes, _row_bytes(row, "project_source"))
        project_source_path = cast(str, _row_text(row, "project_source_path"))
        try:
            project = Project.from_yaml(
                project_source,
                source_name=project_source_path,
            )
            enrollment = self._load_enrollment(row, project)
            revision = ProjectRevision.from_recorded_evidence(
                revision_id=int(row["id"]),
                project_id=int(row["project_id"]),
                sequence=int(row["sequence"]),
                recorded_revision_label=str(row["revision_label"]),
                recorded_display_name=str(row["display_name"]),
                project=project,
                project_source_path=project_source_path,
                project_source=project_source,
                project_source_sha256=cast(
                    str,
                    _row_text(row, "project_source_sha256"),
                ),
                project_normalized_json=cast(
                    bytes,
                    _row_bytes(row, "project_normalized_json"),
                ),
                project_normalized_sha256=cast(
                    str,
                    _row_text(row, "project_normalized_sha256"),
                ),
                project_schema_api_version=cast(
                    str,
                    _row_text(row, "project_schema_api_version"),
                ),
                project_schema_kind=cast(
                    str,
                    _row_text(row, "project_schema_kind"),
                ),
                project_schema_id=cast(
                    str,
                    _row_text(row, "project_schema_id"),
                ),
                project_schema_sha256=cast(
                    str,
                    _row_text(row, "project_schema_sha256"),
                ),
                git_commit=cast(str, _row_text(row, "git_commit")),
                enrollment=enrollment,
                enrollment_json=cast(bytes, _row_bytes(row, "enrollment_json")),
                enrollment_sha256=cast(
                    str,
                    _row_text(row, "enrollment_sha256"),
                ),
                extension_schema_source=cast(
                    bytes | None,
                    _row_bytes(row, "extension_schema_source", optional=True),
                ),
                extension_schema_source_path=cast(
                    str | None,
                    _row_text(row, "extension_schema_source_path", optional=True),
                ),
                extension_schema_source_sha256=cast(
                    str | None,
                    _row_text(
                        row,
                        "extension_schema_source_sha256",
                        optional=True,
                    ),
                ),
                extension_schema_canonical_json=cast(
                    bytes | None,
                    _row_bytes(
                        row,
                        "extension_schema_canonical_json",
                        optional=True,
                    ),
                ),
                extension_schema_canonical_sha256=cast(
                    str | None,
                    _row_text(
                        row,
                        "extension_schema_canonical_sha256",
                        optional=True,
                    ),
                ),
                extension_schema_id=cast(
                    str | None,
                    _row_text(row, "extension_schema_id", optional=True),
                ),
                validated_package_version=cast(
                    str,
                    _row_text(row, "validated_package_version"),
                ),
                created_actor=str(row["created_actor"]),
                created_at=str(row["created_at"]),
            )
        except (LifecycleValidationError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored revision id {revision_id} failed typed evidence "
                f"rehydration: {exc}"
            ) from exc
        self._revision_git_evidence_from_row(row, revision)
        self._verify_revision_children(connection, revision)
        return revision

    def _load_runtime_state(
        self,
        connection: sqlite3.Connection,
        *,
        project_id: int,
        project_key: str,
    ) -> ProjectRuntimeState:
        row = connection.execute(
            "SELECT * FROM project_runtime_state WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        if row is None:
            raise V5EvidenceError(
                f"Project {project_key!r} id {project_id} has no runtime-state row"
            )
        try:
            return ProjectRuntimeState.create(
                project_id=project_id,
                project_key=project_key,
                health=str(row["health"]),
                circuit_failure_count=int(row["circuit_failure_count"]),
                reason=str(row["health_reason"]),
                actor=str(row["health_actor"]),
                changed_at=str(row["health_changed_at"]),
            )
        except (LifecycleValidationError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"Project {project_key!r} runtime-state evidence is invalid: {exc}"
            ) from exc

    def _project_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> RegisteredProject:
        project_id = int(row["id"])
        first_row = connection.execute(
            """
            SELECT id, revision_kind FROM project_revisions
            WHERE project_id = ? AND sequence = 1
            """,
            (project_id,),
        ).fetchone()
        if first_row is None:
            raise V5EvidenceError(
                f"Project id {project_id} has no first revision sequence 1"
            )
        imported_history = str(first_row["revision_kind"]) == "legacy-v4"
        if imported_history:
            typed_row = connection.execute(
                """
                SELECT id FROM project_revisions
                WHERE project_id = ? AND revision_kind = ?
                ORDER BY sequence LIMIT 1
                """,
                (project_id, _TYPED_REVISION_KIND),
            ).fetchone()
            if typed_row is None:
                raise V5EvidenceError(
                    f"Project id {project_id} still has only imported legacy-v4 "
                    "revision evidence; append and activate a resolver-verified "
                    "Project/v1 revision before requesting a typed Project view"
                )
            first = self._load_revision(connection, int(typed_row["id"]))
        else:
            first = self._load_revision(connection, int(first_row["id"]))
        current_row = connection.execute(
            "SELECT revision_kind FROM project_revisions WHERE id = ? AND project_id = ?",
            (int(row["current_revision_id"]), project_id),
        ).fetchone()
        if current_row is None:
            raise V5EvidenceError(
                f"Project id {project_id} current revision row is missing"
            )
        if str(current_row["revision_kind"]) != _TYPED_REVISION_KIND:
            raise V5EvidenceError(
                f"Project id {project_id} current revision is imported legacy-v4; "
                "append and activate a resolver-verified Project/v1 revision "
                "before requesting a typed Project view"
            )
        current = (
            first
            if first.id == int(row["current_revision_id"])
            else self._load_revision(connection, int(row["current_revision_id"]))
        )
        stored_lifecycle = str(row["lifecycle"])
        initial_lifecycle = (
            ProjectLifecycle.PAUSED
            if stored_lifecycle == ProjectLifecycle.ARCHIVED.value
            else stored_lifecycle
        )
        try:
            if imported_history:
                project = RegisteredProject.adopt_imported_history(
                    revision=first,
                    lifecycle=initial_lifecycle,
                    reason=str(row["lifecycle_reason"]),
                    actor=str(row["lifecycle_actor"]),
                    changed_at=str(row["lifecycle_changed_at"]),
                )
            else:
                project = RegisteredProject.register(
                    revision=first,
                    initial_lifecycle=initial_lifecycle,
                    reason=str(row["lifecycle_reason"]),
                    actor=str(row["lifecycle_actor"]),
                    changed_at=str(row["lifecycle_changed_at"]),
                )
            if current.id != first.id:
                project = project.with_current_revision(current)
            if stored_lifecycle == ProjectLifecycle.ARCHIVED.value:
                states = [
                    str(item[0])
                    for item in connection.execute(
                        "SELECT state FROM queue_items WHERE project_id = ?",
                        (project_id,),
                    )
                ]
                incomplete = connection.execute(
                    """
                    SELECT 1 FROM queue_items
                    WHERE project_id = ? AND (
                        ((worktree_cleanup_error IS NOT NULL
                          OR (worktree_path IS NOT NULL AND worktree_removed_at IS NULL))
                         AND runtime_worktree_removed_at IS NULL)
                        OR runtime_worktree_cleanup_error IS NOT NULL
                        OR (runtime_worktree_path IS NOT NULL
                            AND runtime_worktree_removed_at IS NULL)
                    ) LIMIT 1
                    """,
                    (project_id,),
                ).fetchone() is not None
                project = project.transition(
                    ProjectLifecycle.ARCHIVED,
                    reason=str(row["lifecycle_reason"]),
                    actor=str(row["lifecycle_actor"]),
                    changed_at=str(row["lifecycle_changed_at"]),
                    queue_item_states=states,
                    incomplete_cleanup=incomplete,
                )
        except (LifecycleValidationError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored Project id {project_id} lifecycle/current-revision "
                f"evidence is invalid: {exc}"
            ) from exc
        expected = (
            project.key,
            project.display_name,
            project.lifecycle.value,
            project.current_revision_id,
            project.current_revision_sequence,
        )
        actual = (
            str(row["project_key"]),
            str(row["display_name"]),
            stored_lifecycle,
            int(row["current_revision_id"]),
            int(row["current_revision_sequence"]),
        )
        if expected != actual:
            raise V5EvidenceError(
                f"stored Project id {project_id} fields do not match the typed "
                "lifecycle/revision model"
            )
        return project

    def _project_view_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> V5ProjectView:
        project = self._project_from_row(connection, row)
        runtime = self._load_runtime_state(
            connection,
            project_id=project.id,
            project_key=project.key,
        )
        revision = self._load_revision(connection, project.current_revision_id)
        counts = tuple(
            (str(count_row["state"]), int(count_row["count"]))
            for count_row in connection.execute(
                """
                SELECT state, COUNT(*) AS count FROM queue_items
                WHERE project_id = ? GROUP BY state ORDER BY state
                """,
                (project.id,),
            )
        )
        return V5ProjectView(
            project=project,
            runtime_state=runtime,
            current_revision=revision,
            queue_counts=counts,
        )

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        *,
        created_at: str,
        actor: str,
        event_type: str,
        scope: str,
        project_id: int | None,
        queue_item_id: int | None,
        payload: Mapping[str, object],
    ) -> int:
        """Insert one canonical scoped event inside its caller's transaction."""

        timestamp = _require_timestamp(created_at, field_name="event.created_at")
        event_actor = _require_text(actor, field_name="event.actor", maximum=256)
        event_name = _require_text(
            event_type,
            field_name="event.event_type",
            maximum=256,
        )
        if scope not in {"host", "project"}:
            raise V5RepositoryError(
                f"event.scope must be 'host' or 'project', got {scope!r}"
            )
        if scope == "host":
            if project_id is not None or queue_item_id is not None:
                raise V5RepositoryError(
                    "host-scoped events cannot name a Project or queue item"
                )
        else:
            _positive_integer(project_id, field_name="event.project_id")
            if queue_item_id is not None:
                _positive_integer(queue_item_id, field_name="event.queue_item_id")
        cursor = connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                event_actor,
                event_name,
                queue_item_id,
                _canonical_payload(payload),
                scope,
                project_id,
            ),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> V5Event:
        payload_text = _row_text(row, "payload_json")
        assert payload_text is not None
        try:
            payload_value = cast(JSONValue, json.loads(payload_text))
            canonical = canonical_json_bytes(payload_value).decode("utf-8")
        except (json.JSONDecodeError, UnicodeError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored event id {row['id']} payload is not bounded canonical "
                f"JSON: {exc}"
            ) from exc
        if type(payload_value) is not dict or canonical != payload_text:
            raise V5EvidenceError(
                f"stored event id {row['id']} payload is not an exact canonical "
                "JSON object"
            )
        return V5Event(
            id=int(row["id"]),
            created_at=str(row["created_at"]),
            actor=str(row["actor"]),
            event_type=str(row["event_type"]),
            queue_item_id=(
                None if row["queue_item_id"] is None else int(row["queue_item_id"])
            ),
            scope=str(row["scope"]),
            project_id=(
                None if row["project_id"] is None else int(row["project_id"])
            ),
            _payload_json=payload_text,
        )

    @staticmethod
    def _select_project_row(
        connection: sqlite3.Connection,
        *,
        project_id: int | None = None,
        project_key: str | None = None,
    ) -> sqlite3.Row:
        if (project_id is None) == (project_key is None):
            raise V5RepositoryError(
                "select a Project by exactly one of project_id or project_key"
            )
        if project_id is not None:
            value = _positive_integer(project_id, field_name="project_id")
            row = connection.execute(
                "SELECT * FROM projects WHERE id = ?",
                (value,),
            ).fetchone()
            label = f"id {value}"
        else:
            value = _require_text(
                project_key,
                field_name="project_key",
                maximum=63,
            )
            row = connection.execute(
                "SELECT * FROM projects WHERE project_key = ?",
                (value,),
            ).fetchone()
            label = f"key {value!r}"
        if row is None:
            raise V5NotFoundError(f"schema-v5 has no registered Project with {label}")
        return row

    def register_project(
        self,
        project: RegisteredProject,
        resolved_revision: GitResolvedProjectRevision,
        runtime_state: ProjectRuntimeState,
    ) -> V5ProjectView:
        """Atomically register one Project and its complete first revision.

        Explicit positive IDs close the deferred Project/current-revision FK
        cycle without an invalid placeholder row.  No partial registration is
        visible if any child binding, root isolation, or event write fails.
        """

        if type(project) is not RegisteredProject:
            raise TypeError(
                f"project must be exactly RegisteredProject, got "
                f"{type(project).__name__}"
            )
        revision = self._validate_resolved_revision(resolved_revision)
        if type(runtime_state) is not ProjectRuntimeState:
            raise TypeError(
                f"runtime_state must be exactly ProjectRuntimeState, got "
                f"{type(runtime_state).__name__}"
            )
        try:
            expected_project = RegisteredProject.register(
                revision=revision,
                initial_lifecycle=project.lifecycle,
                reason=project.lifecycle_reason,
                actor=project.lifecycle_actor,
                changed_at=project.lifecycle_changed_at,
            )
        except (LifecycleValidationError, TypeError, ValueError) as exc:
            raise V5RepositoryError(
                f"cannot register Project {revision.project_key!r}: {exc}"
            ) from exc
        if expected_project != project:
            raise V5RepositoryError(
                "RegisteredProject does not exactly describe the supplied first "
                "ProjectRevision; rebuild it with RegisteredProject.register()"
            )
        if (
            runtime_state.project_id != project.id
            or runtime_state.project_key != project.key
        ):
            raise V5RepositoryError(
                f"runtime state belongs to Project id/key "
                f"({runtime_state.project_id}, {runtime_state.project_key!r}), not "
                f"({project.id}, {project.key!r})"
            )
        with self._connection(operation=f"register Project {project.key!r}", write=True) as connection:
            connection.execute(
                """
                INSERT INTO projects(
                    id, project_key, display_name, lifecycle,
                    current_revision_id, current_revision_sequence,
                    created_at, created_by, lifecycle_changed_at,
                    lifecycle_actor, lifecycle_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.id,
                    project.key,
                    project.display_name,
                    project.lifecycle.value,
                    project.current_revision_id,
                    project.current_revision_sequence,
                    revision.created_at,
                    revision.created_actor,
                    project.lifecycle_changed_at,
                    project.lifecycle_actor,
                    project.lifecycle_reason,
                ),
            )
            self._insert_revision(connection, resolved_revision)
            connection.execute(
                """
                INSERT INTO project_runtime_state(
                    project_id, health, circuit_failure_count, health_reason,
                    health_actor, health_changed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    project.id,
                    runtime_state.health.value,
                    runtime_state.circuit_failure_count,
                    runtime_state.health_reason,
                    runtime_state.health_actor,
                    runtime_state.health_changed_at,
                ),
            )
            self._insert_event(
                connection,
                created_at=revision.created_at,
                actor=revision.created_actor,
                event_type="project_registered",
                scope="project",
                project_id=project.id,
                queue_item_id=None,
                payload={
                    "projectKey": project.key,
                    "revisionId": revision.id,
                    "revisionLabel": revision.label,
                    "lifecycle": project.lifecycle.value,
                },
            )
            row = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, row)

    def list_projects(self) -> tuple[V5ProjectView, ...]:
        """Return all typed Projects ordered by stable key."""

        with self._connection(operation="list Projects", write=False) as connection:
            rows = connection.execute(
                "SELECT * FROM projects ORDER BY project_key"
            ).fetchall()
            return tuple(self._project_view_from_row(connection, row) for row in rows)

    def get_project(
        self,
        *,
        project_id: int | None = None,
        project_key: str | None = None,
    ) -> V5ProjectView:
        """Load one Project by exactly one stable selector."""

        with self._connection(operation="show Project", write=False) as connection:
            row = self._select_project_row(
                connection,
                project_id=project_id,
                project_key=project_key,
            )
            return self._project_view_from_row(connection, row)

    def get_revision(self, revision_id: int) -> ProjectRevision:
        """Load and fully reverify one typed ProjectRevision."""

        with self._connection(operation="show Project revision", write=False) as connection:
            return self._load_revision(connection, revision_id)

    def get_revision_git_evidence(
        self,
        revision_id: int,
    ) -> V5RevisionGitEvidence:
        """Load rehashed stored Git provenance for one typed revision."""

        revision_key = _positive_integer(revision_id, field_name="revision_id")
        with self._connection(
            operation="show Project revision Git evidence",
            write=False,
        ) as connection:
            revision = self._load_revision(connection, revision_key)
            row = connection.execute(
                "SELECT * FROM project_revisions WHERE id = ?",
                (revision_key,),
            ).fetchone()
            assert row is not None
            return self._revision_git_evidence_from_row(row, revision)

    def append_revision(
        self,
        resolved_revision: GitResolvedProjectRevision,
        *,
        activate: bool = True,
    ) -> V5ProjectView:
        """Append one immutable revision and optionally activate it atomically."""

        revision = self._validate_resolved_revision(resolved_revision)
        if type(activate) is not bool:
            raise TypeError(f"activate must be a boolean, got {type(activate).__name__}")
        with self._connection(
            operation=f"append revision {revision.label!r}",
            write=True,
        ) as connection:
            project_row = self._select_project_row(
                connection,
                project_id=revision.project_id,
            )
            current_row = connection.execute(
                """
                SELECT id, sequence, revision_kind FROM project_revisions
                WHERE id = ? AND project_id = ?
                """,
                (int(project_row["current_revision_id"]), revision.project_id),
            ).fetchone()
            if current_row is None:
                raise V5EvidenceError(
                    f"Project {project_row['project_key']!r} current revision row "
                    "is missing"
                )
            adopting_import = str(current_row["revision_kind"]) == "legacy-v4"
            if adopting_import:
                if not activate:
                    raise V5RepositoryError(
                        f"Project {project_row['project_key']!r} still has an "
                        "imported legacy-v4 current revision; its first typed "
                        "Project/v1 revision must be appended and activated "
                        "atomically"
                    )
                if str(project_row["project_key"]) != revision.project_key:
                    raise V5RepositoryError(
                        f"revision {revision.label!r} key differs from imported "
                        f"Project key {project_row['project_key']!r}"
                    )
                if str(project_row["lifecycle"]) == ProjectLifecycle.ARCHIVED.value:
                    raise V5RepositoryError(
                        f"Project {revision.project_key!r} is archived; append a "
                        "revision only after registering a new Project identity"
                    )
                current_sequence = int(current_row["sequence"])
                if revision.sequence <= current_sequence:
                    raise V5RepositoryError(
                        f"typed adoption revision {revision.label!r} sequence must "
                        f"be greater than imported current sequence "
                        f"{current_sequence}"
                    )
                self._insert_revision(connection, resolved_revision)
                connection.execute(
                    """
                    UPDATE projects
                    SET current_revision_id = ?, current_revision_sequence = ?,
                        display_name = ?
                    WHERE id = ?
                    """,
                    (
                        revision.id,
                        revision.sequence,
                        revision.display_name,
                        revision.project_id,
                    ),
                )
                self._insert_event(
                    connection,
                    created_at=revision.created_at,
                    actor=revision.created_actor,
                    event_type="project_revision_activated",
                    scope="project",
                    project_id=revision.project_id,
                    queue_item_id=None,
                    payload={
                        "revisionId": revision.id,
                        "revisionLabel": revision.label,
                        "sequence": revision.sequence,
                        "activated": True,
                        "adoptedImportedHistory": True,
                    },
                )
                updated = self._select_project_row(
                    connection, project_id=revision.project_id
                )
                return self._project_view_from_row(connection, updated)

            project = self._project_from_row(connection, project_row)
            if not project.revision_creation_allowed:
                raise V5RepositoryError(
                    f"Project {project.key!r} is archived; append a revision only "
                    "after registering a new Project identity"
                )
            try:
                activated = project.with_current_revision(revision)
            except (LifecycleValidationError, TypeError, ValueError) as exc:
                raise V5RepositoryError(
                    f"revision {revision.label!r} cannot follow current revision: {exc}"
                ) from exc
            self._insert_revision(connection, resolved_revision)
            if activate:
                connection.execute(
                    """
                    UPDATE projects
                    SET current_revision_id = ?, current_revision_sequence = ?,
                        display_name = ?
                    WHERE id = ?
                    """,
                    (
                        activated.current_revision_id,
                        activated.current_revision_sequence,
                        activated.display_name,
                        activated.id,
                    ),
                )
            self._insert_event(
                connection,
                created_at=revision.created_at,
                actor=revision.created_actor,
                event_type=("project_revision_activated" if activate else "project_revision_appended"),
                scope="project",
                project_id=project.id,
                queue_item_id=None,
                payload={
                    "revisionId": revision.id,
                    "revisionLabel": revision.label,
                    "sequence": revision.sequence,
                    "activated": activate,
                },
            )
            updated = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, updated)

    def activate_revision(
        self,
        *,
        project_id: int,
        revision_id: int,
        actor: str,
        changed_at: str,
    ) -> V5ProjectView:
        """Activate an already appended newer revision without mutating it."""

        timestamp = _require_timestamp(changed_at, field_name="changed_at")
        event_actor = _require_text(actor, field_name="actor", maximum=256)
        with self._connection(operation="activate Project revision", write=True) as connection:
            row = self._select_project_row(connection, project_id=project_id)
            project = self._project_from_row(connection, row)
            target = self._load_revision(connection, revision_id)
            try:
                activated = project.with_current_revision(target)
            except (LifecycleValidationError, TypeError, ValueError) as exc:
                raise V5RepositoryError(
                    f"cannot activate revision id {revision_id} for Project "
                    f"{project.key!r}: {exc}"
                ) from exc
            connection.execute(
                """
                UPDATE projects SET current_revision_id = ?,
                    current_revision_sequence = ?, display_name = ? WHERE id = ?
                """,
                (
                    activated.current_revision_id,
                    activated.current_revision_sequence,
                    activated.display_name,
                    activated.id,
                ),
            )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="project_revision_activated",
                scope="project",
                project_id=project.id,
                queue_item_id=None,
                payload={
                    "revisionId": target.id,
                    "revisionLabel": target.label,
                    "sequence": target.sequence,
                    "activated": True,
                },
            )
            updated = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, updated)

    def transition_project(
        self,
        *,
        project_id: int,
        target: ProjectLifecycle | str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5ProjectView:
        """Apply one audited lifecycle transition without cascading queue state."""

        with self._connection(operation="transition Project lifecycle", write=True) as connection:
            row = self._select_project_row(connection, project_id=project_id)
            project = self._project_from_row(connection, row)
            states = [
                str(item["state"])
                for item in connection.execute(
                    "SELECT state FROM queue_items WHERE project_id = ? ORDER BY id",
                    (project.id,),
                )
            ]
            incomplete_cleanup = connection.execute(
                """
                SELECT 1 FROM queue_items
                WHERE project_id = ? AND (
                    ((worktree_cleanup_error IS NOT NULL
                      OR (worktree_path IS NOT NULL AND worktree_removed_at IS NULL))
                     AND runtime_worktree_removed_at IS NULL)
                    OR runtime_worktree_cleanup_error IS NOT NULL
                    OR (runtime_worktree_path IS NOT NULL
                        AND runtime_worktree_removed_at IS NULL)
                ) LIMIT 1
                """,
                (project.id,),
            ).fetchone() is not None
            try:
                transitioned = project.transition(
                    target,
                    reason=reason,
                    actor=actor,
                    changed_at=changed_at,
                    queue_item_states=states,
                    incomplete_cleanup=incomplete_cleanup,
                )
            except (LifecycleValidationError, TypeError, ValueError) as exc:
                raise V5RepositoryError(
                    f"cannot transition Project {project.key!r} from "
                    f"{project.lifecycle.value!r} to {target!r}: {exc}"
                ) from exc
            connection.execute(
                """
                UPDATE projects SET lifecycle = ?, lifecycle_reason = ?,
                    lifecycle_actor = ?, lifecycle_changed_at = ? WHERE id = ?
                """,
                (
                    transitioned.lifecycle.value,
                    transitioned.lifecycle_reason,
                    transitioned.lifecycle_actor,
                    transitioned.lifecycle_changed_at,
                    transitioned.id,
                ),
            )
            self._insert_event(
                connection,
                created_at=transitioned.lifecycle_changed_at,
                actor=transitioned.lifecycle_actor,
                event_type="project_lifecycle_changed",
                scope="project",
                project_id=transitioned.id,
                queue_item_id=None,
                payload={
                    "from": project.lifecycle.value,
                    "to": transitioned.lifecycle.value,
                    "reason": transitioned.lifecycle_reason,
                },
            )
            updated = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, updated)

    @staticmethod
    def _update_runtime_state(
        connection: sqlite3.Connection,
        runtime_state: ProjectRuntimeState,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE project_runtime_state SET health = ?,
                circuit_failure_count = ?, health_reason = ?, health_actor = ?,
                health_changed_at = ? WHERE project_id = ?
            """,
            (
                runtime_state.health.value,
                runtime_state.circuit_failure_count,
                runtime_state.health_reason,
                runtime_state.health_actor,
                runtime_state.health_changed_at,
                runtime_state.project_id,
            ),
        )
        if cursor.rowcount != 1:
            raise V5EvidenceError(
                f"Project id {runtime_state.project_id} lost its runtime-state row"
            )

    def record_project_failure(
        self,
        *,
        project_id: int,
        reason: str,
        actor: str,
        changed_at: str,
        open_circuit: bool,
    ) -> V5ProjectView:
        """Record one Project-scoped failure and optionally open its circuit."""

        with self._connection(operation="record Project failure", write=True) as connection:
            row = self._select_project_row(connection, project_id=project_id)
            project = self._project_from_row(connection, row)
            current = self._load_runtime_state(
                connection,
                project_id=project.id,
                project_key=project.key,
            )
            try:
                updated_runtime = current.record_failure(
                    reason=reason,
                    actor=actor,
                    changed_at=changed_at,
                    open_circuit=open_circuit,
                )
            except (LifecycleValidationError, TypeError, ValueError) as exc:
                raise V5RepositoryError(
                    f"cannot record failure for Project {project.key!r}: {exc}"
                ) from exc
            self._update_runtime_state(connection, updated_runtime)
            self._insert_event(
                connection,
                created_at=updated_runtime.health_changed_at,
                actor=updated_runtime.health_actor,
                event_type="project_failure_recorded",
                scope="project",
                project_id=project.id,
                queue_item_id=None,
                payload={
                    "health": updated_runtime.health.value,
                    "failureCount": updated_runtime.circuit_failure_count,
                    "reason": updated_runtime.health_reason,
                },
            )
            refreshed = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, refreshed)

    def close_project_circuit(
        self,
        *,
        project_id: int,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5ProjectView:
        """Close one Project health circuit after an operator decision."""

        with self._connection(operation="close Project circuit", write=True) as connection:
            row = self._select_project_row(connection, project_id=project_id)
            project = self._project_from_row(connection, row)
            current = self._load_runtime_state(
                connection,
                project_id=project.id,
                project_key=project.key,
            )
            try:
                updated_runtime = current.close_circuit(
                    reason=reason,
                    actor=actor,
                    changed_at=changed_at,
                )
            except (LifecycleValidationError, TypeError, ValueError) as exc:
                raise V5RepositoryError(
                    f"cannot close circuit for Project {project.key!r}: {exc}"
                ) from exc
            self._update_runtime_state(connection, updated_runtime)
            self._insert_event(
                connection,
                created_at=updated_runtime.health_changed_at,
                actor=updated_runtime.health_actor,
                event_type="project_circuit_closed",
                scope="project",
                project_id=project.id,
                queue_item_id=None,
                payload={
                    "health": updated_runtime.health.value,
                    "failureCount": updated_runtime.circuit_failure_count,
                    "reason": updated_runtime.health_reason,
                },
            )
            refreshed = self._select_project_row(connection, project_id=project.id)
            return self._project_view_from_row(connection, refreshed)

    def record_event(
        self,
        *,
        created_at: str,
        actor: str,
        event_type: str,
        scope: str,
        payload: Mapping[str, object],
        project_id: int | None = None,
        queue_item_id: int | None = None,
    ) -> V5Event:
        """Append one host- or Project-scoped canonical event."""

        with self._connection(operation="record event", write=True) as connection:
            event_id = self._insert_event(
                connection,
                created_at=created_at,
                actor=actor,
                event_type=event_type,
                scope=scope,
                project_id=project_id,
                queue_item_id=queue_item_id,
                payload=payload,
            )
            row = connection.execute(
                "SELECT * FROM events WHERE id = ?",
                (event_id,),
            ).fetchone()
            assert row is not None
            return self._event_from_row(row)

    def list_events(
        self,
        *,
        project_id: int | None = None,
        queue_item_id: int | None = None,
        after_id: int = 0,
        limit: int = 500,
    ) -> tuple[V5Event, ...]:
        """Read append-only events with explicit bounded selectors."""

        if type(after_id) is not int or after_id < 0:
            raise V5RepositoryError(
                f"after_id must be a nonnegative integer, got {after_id!r}"
            )
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise V5RepositoryError(
                f"event limit must be an integer from 1 through 10000, got {limit!r}"
            )
        conditions = ["id > ?"]
        arguments: list[object] = [after_id]
        if project_id is not None:
            conditions.append("project_id = ?")
            arguments.append(_positive_integer(project_id, field_name="project_id"))
        if queue_item_id is not None:
            conditions.append("queue_item_id = ?")
            arguments.append(
                _positive_integer(queue_item_id, field_name="queue_item_id")
            )
        arguments.append(limit)
        query = (
            "SELECT * FROM events WHERE "
            + " AND ".join(conditions)
            + " ORDER BY id LIMIT ?"
        )
        with self._connection(operation="list events", write=False) as connection:
            return tuple(
                self._event_from_row(row)
                for row in connection.execute(query, tuple(arguments))
            )

    def _validate_resolved_admission(
        self,
        connection: sqlite3.Connection,
        resolved: GitResolvedAdmission,
        revision: ProjectRevision,
    ) -> AdmissionSnapshot:
        """Authenticate the resolver wrapper against revision and stored Git proof."""

        if type(resolved) is not GitResolvedAdmission:
            raise TypeError(
                "resolved admission must be exactly GitResolvedAdmission from "
                f"compile_admission_from_revision(), got {type(resolved).__name__}"
            )
        snapshot = resolved.snapshot
        if type(snapshot) is not AdmissionSnapshot:
            raise V5EvidenceError(
                "GitResolvedAdmission does not contain an exact AdmissionSnapshot"
            )
        expected_identity = (
            revision.project_id,
            revision.id,
            revision.project_key,
            revision.label,
            str(revision.enrollment.checkout_directory),
            revision.git_commit,
        )
        actual_identity = (
            resolved.project_id,
            resolved.project_revision_id,
            resolved.project_key,
            resolved.project_revision_label,
            resolved.repository_root,
            resolved.git_commit,
        )
        if actual_identity != expected_identity:
            raise V5EvidenceError(
                "GitResolvedAdmission identity/root/commit fields do not match "
                "the owned persisted ProjectRevision"
            )
        _validate_git_blob(
            resolved.project_blob,
            source=snapshot.project_source,
            expected_path=snapshot.project_source_name,
            field_name="resolved_admission.project_blob",
        )
        _validate_git_blob(
            resolved.card_blob,
            source=snapshot.card_source,
            expected_path=snapshot.card_source_name,
            field_name="resolved_admission.card_blob",
        )
        extension = snapshot.extension_schema
        if extension is None:
            if resolved.extension_schema_blob is not None:
                raise V5EvidenceError(
                    "resolved admission contains extension Git blob evidence but "
                    "its AdmissionSnapshot has no extension schema"
                )
        else:
            if resolved.extension_schema_blob is None:
                raise V5EvidenceError(
                    "resolved admission is missing required extension Git blob "
                    "evidence"
                )
            _validate_git_blob(
                resolved.extension_schema_blob,
                source=extension.source,
                expected_path=extension.reference_path,
                field_name="resolved_admission.extension_schema_blob",
            )

        if (
            snapshot.project_revision != revision.label
            or snapshot.git_commit != revision.git_commit
            or snapshot.project_source_name != revision.project_source_path
            or snapshot.project_source != revision.project_source
            or snapshot.project_source_sha256 != revision.project_source_sha256
            or snapshot.project_normalized_json != revision.project_normalized_json
            or snapshot.project_normalized_sha256
            != revision.project_normalized_sha256
            or snapshot.project_schema.api_version
            != revision.project_schema_api_version
            or snapshot.project_schema.kind != revision.project_schema_kind
            or snapshot.project_schema.schema_id != revision.project_schema_id
            or snapshot.project_schema.sha256 != revision.project_schema_sha256
            or snapshot.submission_policy.project_key != revision.project_key
        ):
            raise V5EvidenceError(
                "AdmissionSnapshot Project/revision/schema/policy evidence does not "
                "exactly match the owned persisted ProjectRevision"
            )
        if extension is None:
            if revision.extension_schema_source is not None:
                raise V5EvidenceError(
                    "AdmissionSnapshot omits the extension schema required by its "
                    "ProjectRevision"
                )
        elif (
            extension.reference_path != revision.extension_schema_source_path
            or extension.source != revision.extension_schema_source
            or extension.source_sha256
            != revision.extension_schema_source_sha256
            or extension.canonical_json
            != revision.extension_schema_canonical_json
            or extension.canonical_sha256
            != revision.extension_schema_canonical_sha256
            or extension.schema_id != revision.extension_schema_id
        ):
            raise V5EvidenceError(
                "AdmissionSnapshot extension-schema evidence does not exactly "
                "match its ProjectRevision"
            )

        revision_row = connection.execute(
            "SELECT * FROM project_revisions WHERE id = ?",
            (revision.id,),
        ).fetchone()
        assert revision_row is not None
        stored_git = self._revision_git_evidence_from_row(revision_row, revision)
        if (
            stored_git.project_blob.object_id != resolved.project_blob.object_id
            or stored_git.project_blob.mode != resolved.project_blob.mode
            or stored_git.project_blob.size != resolved.project_blob.size
        ):
            raise V5EvidenceError(
                "admission Project Git blob proof differs from immutable revision "
                "Git provenance"
            )
        if (stored_git.extension_schema_blob is None) != (
            resolved.extension_schema_blob is None
        ):
            raise V5EvidenceError(
                "admission and revision extension Git blob presence differs"
            )
        if (
            stored_git.extension_schema_blob is not None
            and resolved.extension_schema_blob is not None
            and (
                stored_git.extension_schema_blob.object_id
                != resolved.extension_schema_blob.object_id
                or stored_git.extension_schema_blob.mode
                != resolved.extension_schema_blob.mode
                or stored_git.extension_schema_blob.size
                != resolved.extension_schema_blob.size
            )
        ):
            raise V5EvidenceError(
                "admission extension Git blob proof differs from immutable "
                "revision Git provenance"
            )

        try:
            evidence = admission_snapshot_to_stored_evidence(snapshot)
            rebuilt = rehydrate_admission_snapshot(evidence)
        except (AdmissionError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"resolved admission snapshot failed exact storage-evidence "
                f"authentication: {exc}"
            ) from exc
        if rebuilt != snapshot:
            raise V5EvidenceError(
                "resolved admission snapshot differs from the exact snapshot "
                "rehydrated from its decomposed evidence"
            )
        return snapshot

    def _snapshot_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> AdmissionSnapshot:
        if row["revision_kind"] != _TYPED_REVISION_KIND:
            raise V5EvidenceError(
                f"admission snapshot id {row['id']} has unsupported revision kind "
                f"{row['revision_kind']!r}"
            )
        evidence = {name: row[name] for name in _ADMISSION_EVIDENCE_COLUMNS}
        try:
            snapshot = rehydrate_admission_snapshot(evidence)
        except (AdmissionError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored admission snapshot id {row['id']} failed exact "
                f"rehydration: {exc}"
            ) from exc
        revision = self._load_revision(connection, int(row["revision_id"]))
        if int(row["project_id"]) != revision.project_id:
            raise V5EvidenceError(
                f"snapshot id {row['id']} Project ownership differs from its "
                "ProjectRevision"
            )
        if (
            snapshot.project_revision != revision.label
            or snapshot.git_commit != revision.git_commit
            or snapshot.project_source_name != revision.project_source_path
            or snapshot.project_source != revision.project_source
            or snapshot.project_normalized_json != revision.project_normalized_json
            or snapshot.project_schema.api_version
            != revision.project_schema_api_version
            or snapshot.project_schema.kind != revision.project_schema_kind
            or snapshot.project_schema.schema_id != revision.project_schema_id
            or snapshot.project_schema.sha256 != revision.project_schema_sha256
            or snapshot.submission_policy.project_key != revision.project_key
        ):
            raise V5EvidenceError(
                f"snapshot id {row['id']} Project/revision evidence differs from "
                "its immutable owned revision"
            )
        project_blob = self._stored_git_blob(
            row,
            prefix="project",
            path=snapshot.project_source_name,
            source=snapshot.project_source,
            optional=False,
        )
        card_blob = self._stored_git_blob(
            row,
            prefix="card",
            path=snapshot.card_source_name,
            source=snapshot.card_source,
            optional=False,
        )
        assert project_blob is not None and card_blob is not None
        revision_row = connection.execute(
            "SELECT * FROM project_revisions WHERE id = ?",
            (revision.id,),
        ).fetchone()
        assert revision_row is not None
        revision_git = self._revision_git_evidence_from_row(revision_row, revision)
        if project_blob != revision_git.project_blob:
            raise V5EvidenceError(
                f"snapshot id {row['id']} Project Git blob evidence differs from "
                "its immutable revision"
            )
        extension = snapshot.extension_schema
        if extension is None:
            extension_blob = self._stored_git_blob(
                row,
                prefix="extension_schema",
                path="",
                source=b"",
                optional=True,
            )
            if extension_blob is not None or revision_git.extension_schema_blob is not None:
                raise V5EvidenceError(
                    f"snapshot id {row['id']} has inconsistent extension Git "
                    "blob presence"
                )
        else:
            extension_blob = self._stored_git_blob(
                row,
                prefix="extension_schema",
                path=extension.reference_path,
                source=extension.source,
                optional=False,
            )
            if (
                extension_blob != revision_git.extension_schema_blob
                or extension.reference_path
                != revision.extension_schema_source_path
                or extension.source != revision.extension_schema_source
                or extension.canonical_json
                != revision.extension_schema_canonical_json
                or extension.schema_id != revision.extension_schema_id
            ):
                raise V5EvidenceError(
                    f"snapshot id {row['id']} extension-schema evidence differs "
                    "from its immutable revision"
                )
        return snapshot

    def get_admission_snapshot(self, snapshot_id: int) -> AdmissionSnapshot:
        """Load and authenticate one exact typed admission snapshot."""

        key = _positive_integer(snapshot_id, field_name="snapshot_id")
        with self._connection(operation="show admission snapshot", write=False) as connection:
            row = connection.execute(
                "SELECT * FROM admission_snapshots WHERE id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5NotFoundError(
                    f"schema-v5 has no admission snapshot with id {key}"
                )
            return self._snapshot_from_row(connection, row)

    @staticmethod
    def _snapshot_experiment_id(snapshot: AdmissionSnapshot) -> str:
        document = snapshot.card_document
        metadata = document.get("metadata")
        if type(metadata) is not dict:
            raise V5EvidenceError(
                "authenticated ExperimentCard normalized document has no metadata "
                "object"
            )
        return _require_text(
            metadata.get("experimentId"),
            field_name="ExperimentCard metadata.experimentId",
            maximum=256,
        )

    def admit(
        self,
        resolved: GitResolvedAdmission,
        *,
        added_at: str,
    ) -> V5QueueItem:
        """Atomically persist authenticated evidence, item, dependencies, and event.

        A bare :class:`AdmissionSnapshot` is intentionally rejected even though
        it can be decomposed: only the factory-only Git resolver wrapper proves
        that source bytes came from the registered exact commit tree.
        """

        if type(resolved) is not GitResolvedAdmission:
            raise TypeError(
                "admit() requires exactly GitResolvedAdmission from "
                f"compile_admission_from_revision(), got {type(resolved).__name__}"
            )
        timestamp = _require_timestamp(added_at, field_name="added_at")
        with self._connection(operation="admit typed queue item", write=True) as connection:
            project_row = self._select_project_row(
                connection,
                project_id=resolved.project_id,
            )
            project = self._project_from_row(connection, project_row)
            if not project.admission_allowed:
                raise V5RepositoryError(
                    f"Project {project.key!r} is archived; admission is permanently "
                    "disabled while history remains immutable"
                )
            if resolved.project_revision_id != project.current_revision_id:
                raise V5RepositoryError(
                    f"resolved revision id {resolved.project_revision_id} is not "
                    f"current revision id {project.current_revision_id} for Project "
                    f"{project.key!r}; recompile against the current revision"
                )
            revision = self._load_revision(
                connection,
                resolved.project_revision_id,
            )
            snapshot = self._validate_resolved_admission(
                connection,
                resolved,
                revision,
            )
            policy = snapshot.submission_policy
            for dependency_id in policy.dependencies:
                dependency = connection.execute(
                    "SELECT id, state FROM queue_items WHERE id = ?",
                    (dependency_id,),
                ).fetchone()
                if dependency is None:
                    raise V5RepositoryError(
                        f"submission dependency queue item {dependency_id} does "
                        "not exist; admit dependencies before dependent work"
                    )
                dependency_state = str(dependency["state"])
                if dependency_state in _TERMINAL_NON_SUCCESS_STATES:
                    raise V5RepositoryError(
                        f"submission dependency queue item {dependency_id} is "
                        f"already terminal in non-success state "
                        f"{dependency_state!r}; submit without that dependency or "
                        "create a replacement successful item"
                    )

            evidence = admission_snapshot_to_stored_evidence(snapshot)
            # Rehash every source/canonical/command/policy field immediately
            # before insertion, independent of the compiler-owned dataclasses.
            try:
                rehydrate_admission_snapshot(evidence)
            except (AdmissionError, TypeError, ValueError) as exc:
                raise V5EvidenceError(
                    f"admission evidence changed before persistence: {exc}"
                ) from exc
            blob_fields: dict[str, object] = {
                "project_blob_object_id": resolved.project_blob.object_id,
                "project_blob_mode": resolved.project_blob.mode,
                "project_blob_size": resolved.project_blob.size,
                "card_blob_object_id": resolved.card_blob.object_id,
                "card_blob_mode": resolved.card_blob.mode,
                "card_blob_size": resolved.card_blob.size,
                "extension_schema_blob_object_id": (
                    None
                    if resolved.extension_schema_blob is None
                    else resolved.extension_schema_blob.object_id
                ),
                "extension_schema_blob_mode": (
                    None
                    if resolved.extension_schema_blob is None
                    else resolved.extension_schema_blob.mode
                ),
                "extension_schema_blob_size": (
                    None
                    if resolved.extension_schema_blob is None
                    else resolved.extension_schema_blob.size
                ),
            }
            snapshot_record: dict[str, object] = {
                "project_id": project.id,
                "revision_id": revision.id,
                "revision_kind": _TYPED_REVISION_KIND,
                **evidence,
                **blob_fields,
                "created_at": timestamp,
            }
            snapshot_columns = tuple(snapshot_record)
            placeholders = ", ".join("?" for _ in snapshot_columns)
            snapshot_cursor = connection.execute(
                f"INSERT INTO admission_snapshots({', '.join(snapshot_columns)}) "
                f"VALUES ({placeholders})",
                tuple(snapshot_record[name] for name in snapshot_columns),
            )
            snapshot_id = int(snapshot_cursor.lastrowid)

            experiment_id = self._snapshot_experiment_id(snapshot)
            attempt = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0) + 1
                    FROM queue_items
                    WHERE project_id = ? AND experiment_id = ?
                    """,
                    (project.id, experiment_id),
                ).fetchone()[0]
            )
            state = "held" if policy.hold_reason is not None else "queued"
            command_json = cast(bytes, evidence["command_json"])
            try:
                command_text = command_json.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:  # pragma: no cover - canonical invariant
                raise V5EvidenceError(
                    "admitted command JSON is not UTF-8"
                ) from exc
            item_cursor = connection.execute(
                """
                INSERT INTO queue_items(
                    project_id, revision_id, admission_kind, snapshot_id,
                    job_id, experiment_id, attempt, state, priority, card_path,
                    card_sha256, command_text, runner_name, git_commit,
                    added_at, added_by, state_detail, preemptible
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.id,
                    revision.id,
                    _TYPED_ADMISSION_KIND,
                    snapshot_id,
                    policy.job_id,
                    experiment_id,
                    attempt,
                    state,
                    policy.priority,
                    policy.card_path,
                    snapshot.card_source_sha256,
                    command_text,
                    _RUNNER_NAME,
                    snapshot.git_commit,
                    timestamp,
                    policy.operator,
                    policy.hold_reason,
                    int(policy.preemption_authorized),
                ),
            )
            item_id = int(item_cursor.lastrowid)
            connection.executemany(
                """
                INSERT INTO dependencies(queue_item_id, dependency_item_id)
                VALUES (?, ?)
                """,
                ((item_id, dependency_id) for dependency_id in policy.dependencies),
            )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=policy.operator,
                event_type="queue_item_admitted",
                scope="project",
                project_id=project.id,
                queue_item_id=item_id,
                payload={
                    "snapshotId": snapshot_id,
                    "revisionId": revision.id,
                    "experimentId": experiment_id,
                    "attempt": attempt,
                    "state": state,
                    "dependencies": list(policy.dependencies),
                },
            )
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (item_id,),
            ).fetchone()
            assert row is not None
            return self._queue_item_from_row(connection, row)

    def _queue_item_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> V5QueueItem:
        admission_kind = str(row["admission_kind"])
        if admission_kind not in {
            _TYPED_ADMISSION_KIND,
            "LegacyMarkdownCard/v0",
        }:
            raise V5EvidenceError(
                f"queue item id {row['id']} has unsupported admission kind "
                f"{admission_kind!r}"
            )
        state = str(row["state"])
        if state not in _ALL_QUEUE_STATES:
            raise V5EvidenceError(
                f"queue item id {row['id']} has unknown state {state!r}"
            )
        snapshot: AdmissionSnapshot | None = None
        snapshot_id = None if row["snapshot_id"] is None else int(row["snapshot_id"])
        job_id = None if row["job_id"] is None else str(row["job_id"])
        if admission_kind == _TYPED_ADMISSION_KIND:
            if snapshot_id is None or job_id is None:
                raise V5EvidenceError(
                    f"typed queue item id {row['id']} lacks snapshot/job identity"
                )
            snapshot_row = connection.execute(
                "SELECT * FROM admission_snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()
            if snapshot_row is None:
                raise V5EvidenceError(
                    f"typed queue item id {row['id']} references missing snapshot "
                    f"id {snapshot_id}"
                )
            snapshot = self._snapshot_from_row(connection, snapshot_row)
            evidence = admission_snapshot_to_stored_evidence(snapshot)
            expected = (
                int(snapshot_row["project_id"]),
                int(snapshot_row["revision_id"]),
                snapshot.submission_policy.job_id,
                self._snapshot_experiment_id(snapshot),
                snapshot.submission_policy.card_path,
                snapshot.card_source_sha256,
                cast(bytes, evidence["command_json"]).decode("utf-8"),
                _RUNNER_NAME,
                snapshot.git_commit,
                int(snapshot.submission_policy.preemption_authorized),
            )
            actual = (
                int(row["project_id"]),
                int(row["revision_id"]),
                job_id,
                str(row["experiment_id"]),
                str(row["card_path"]),
                str(row["card_sha256"]),
                str(row["command_text"]),
                str(row["runner_name"]),
                str(row["git_commit"]),
                int(row["preemptible"]),
            )
            if actual != expected:
                raise V5EvidenceError(
                    f"typed queue item id {row['id']} identity/scheduling fields "
                    "differ from its immutable admission snapshot"
                )
        elif snapshot_id is not None or job_id is not None:
            raise V5EvidenceError(
                f"legacy queue item id {row['id']} improperly references typed "
                "admission evidence"
            )
        return V5QueueItem(
            id=int(row["id"]),
            project_id=int(row["project_id"]),
            revision_id=int(row["revision_id"]),
            admission_kind=admission_kind,
            snapshot_id=snapshot_id,
            job_id=job_id,
            experiment_id=str(row["experiment_id"]),
            attempt=int(row["attempt"]),
            state=state,
            priority=int(row["priority"]),
            card_path=str(row["card_path"]),
            card_sha256=str(row["card_sha256"]),
            command_text=str(row["command_text"]),
            runner_name=str(row["runner_name"]),
            git_commit=str(row["git_commit"]),
            added_at=str(row["added_at"]),
            added_by=str(row["added_by"]),
            state_detail=(
                None if row["state_detail"] is None else str(row["state_detail"])
            ),
            preemptible=bool(row["preemptible"]),
            segment=int(row["segment"]),
            resume_front=bool(row["resume_front"]),
            snapshot=snapshot,
        )

    def get_queue_item(self, item_id: int) -> V5QueueItem:
        """Load one queue item and authenticate typed admission evidence."""

        key = _positive_integer(item_id, field_name="item_id")
        with self._connection(operation="show queue item", write=False) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5NotFoundError(f"schema-v5 has no queue item with id {key}")
            return self._queue_item_from_row(connection, row)

    def list_dispatch_candidates(self, *, limit: int = 100) -> tuple[V5QueueItem, ...]:
        """Return currently eligible items in global scheduling order.

        This read deliberately does not mutate dependency state or perform
        process reconciliation.  It excludes every item with an unsatisfied
        dependency, every non-active Project, and every open Project circuit;
        later dependency failure/hold transitions and stale-process recovery
        remain explicit scheduler transactions.
        """

        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise V5RepositoryError(
                f"dispatch candidate limit must be 1 through 10000, got {limit!r}"
            )
        with self._connection(operation="list dispatch candidates", write=False) as connection:
            rows = connection.execute(
                """
                SELECT q.*
                FROM queue_items AS q
                JOIN projects AS p ON p.id = q.project_id
                JOIN project_runtime_state AS r ON r.project_id = q.project_id
                WHERE q.state = 'queued'
                  AND p.lifecycle = 'active'
                  AND r.health = 'closed'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM dependencies AS d
                      JOIN queue_items AS dependency
                        ON dependency.id = d.dependency_item_id
                      WHERE d.queue_item_id = q.id
                        AND dependency.state <> 'succeeded'
                  )
                ORDER BY q.priority DESC, q.resume_front DESC, q.id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return tuple(self._queue_item_from_row(connection, row) for row in rows)

    def _yield_request_from_row(
        self,
        row: sqlite3.Row,
    ) -> V5YieldRequestRecord:
        source = cast(bytes, _row_bytes(row, "request_json"))
        digest = sha256_bytes(source)
        if digest != row["request_sha256"]:
            raise V5EvidenceError(
                f"stored yield request {row['request_id']!r} SHA-256 does not "
                "match its exact wire bytes"
            )
        try:
            request = CooperativeYieldRequest.from_document(
                _decode_wire_protocol(source, label="stored yield request")
            )
        except (CooperativeYieldError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored yield request {row['request_id']!r} is invalid: {exc}"
            ) from exc
        continuation = request.continuation
        expected = (
            request.request_id,
            request.queue_item_id,
            request.segment,
            "experiment-queue/v1",
            "CooperativeYieldRequest",
            request.request_kind.value,
            request.requested_at,
            request.requested_by,
            request.note,
            continuation.resolved_spec_sha256,
            continuation.project_revision,
            continuation.git_commit,
            continuation.run_id,
            continuation.prior_receipt_sha256,
            continuation.identity_sha256,
        )
        actual = tuple(
            row[name]
            for name in (
                "request_id",
                "queue_item_id",
                "segment",
                "protocol_api_version",
                "protocol_kind",
                "request_kind",
                "requested_at",
                "requested_by",
                "note",
                "resolved_spec_sha256",
                "project_revision_label",
                "git_commit",
                "run_id",
                "prior_receipt_sha256",
                "continuation_identity_sha256",
            )
        )
        if actual != expected:
            raise V5EvidenceError(
                f"stored yield request {request.request_id!r} decomposed fields "
                "differ from its authenticated exact wire document"
            )
        return V5YieldRequestRecord(
            request=request,
            source=source,
            sha256=digest,
            project_id=int(row["project_id"]),
            revision_id=int(row["revision_id"]),
        )

    def record_yield_request(
        self,
        request: CooperativeYieldRequest,
        *,
        source: bytes,
    ) -> V5YieldRequestRecord:
        """Append one typed request with its exact deterministic wire bytes."""

        if type(request) is not CooperativeYieldRequest:
            raise TypeError(
                f"request must be exactly CooperativeYieldRequest, got "
                f"{type(request).__name__}"
            )
        try:
            parsed = CooperativeYieldRequest.from_document(
                _decode_wire_protocol(source, label="yield request")
            )
        except (CooperativeYieldError, TypeError, ValueError) as exc:
            raise V5EvidenceError(f"yield request wire evidence is invalid: {exc}") from exc
        if parsed != request:
            raise V5EvidenceError(
                "yield request object differs from its exact supplied wire bytes"
            )
        continuation = request.continuation
        with self._connection(operation="record cooperative-yield request", write=True) as connection:
            existing_request = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
                (request.request_id,),
            ).fetchone()
            if existing_request is not None:
                existing_record = self._yield_request_from_row(existing_request)
                if existing_record.request == request and existing_record.source == source:
                    return existing_record
                raise V5RepositoryError(
                    f"yield request ID {request.request_id!r} already identifies "
                    "different immutable evidence"
                )
            item_row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (request.queue_item_id,),
            ).fetchone()
            if item_row is None:
                raise V5NotFoundError(
                    f"yield request names missing queue item {request.queue_item_id}"
                )
            item = self._queue_item_from_row(connection, item_row)
            if item.admission_kind != _TYPED_ADMISSION_KIND or item.snapshot is None:
                raise V5RepositoryError(
                    f"queue item {item.id} is not an ExperimentCard/v1 admission; "
                    "typed CooperativeYield/v1 evidence is unavailable"
                )
            if request.segment != item.segment:
                raise V5RepositoryError(
                    f"yield request segment {request.segment} does not match queue "
                    f"item {item.id} current segment {item.segment}"
                )
            if item.state != "running":
                prior = connection.execute(
                    """
                    SELECT request_id FROM cooperative_yield_requests
                    WHERE queue_item_id = ? AND segment = ?
                    """,
                    (item.id, item.segment),
                ).fetchone()
                if prior is not None:
                    raise V5RepositoryError(
                        f"queue item {item.id} segment {item.segment} already has "
                        f"yield request {prior['request_id']!r}"
                    )
                raise V5RepositoryError(
                    f"queue item {item.id} is {item.state!r}; a cooperative-yield "
                    "request may be persisted only for a running attempt"
                )
            if not item.preemptible:
                raise V5RepositoryError(
                    f"queue item {item.id} was not admitted with cooperative "
                    "preemption authorization"
                )
            if (
                continuation.resolved_spec_sha256 != item.snapshot.resolved_sha256
                or continuation.project_revision
                != item.snapshot.project_revision
                or continuation.git_commit != item.git_commit
            ):
                raise V5EvidenceError(
                    "yield request continuation identity does not match the "
                    "queue item's immutable admission/revision evidence"
                )
            source_digest = sha256_bytes(source)
            connection.execute(
                """
                INSERT INTO cooperative_yield_requests(
                    request_id, queue_item_id, project_id, revision_id,
                    admission_kind, segment, protocol_api_version, protocol_kind,
                    request_kind, requested_at, requested_by, note, request_json,
                    request_sha256, resolved_spec_sha256,
                    project_revision_label, git_commit, run_id,
                    prior_receipt_sha256, continuation_identity_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.request_id,
                    item.id,
                    item.project_id,
                    item.revision_id,
                    _TYPED_ADMISSION_KIND,
                    request.segment,
                    "experiment-queue/v1",
                    "CooperativeYieldRequest",
                    request.request_kind.value,
                    request.requested_at,
                    request.requested_by,
                    request.note,
                    source,
                    source_digest,
                    continuation.resolved_spec_sha256,
                    continuation.project_revision,
                    continuation.git_commit,
                    continuation.run_id,
                    continuation.prior_receipt_sha256,
                    continuation.identity_sha256,
                ),
            )
            updated = connection.execute(
                """
                UPDATE queue_items
                SET state = 'yielding', yield_requested_at = ?,
                    yield_requested_by = ?, yield_request_id = ?, yield_note = ?
                WHERE id = ? AND state = 'running' AND segment = ?
                """,
                (
                    request.requested_at,
                    request.requested_by,
                    request.request_id,
                    request.note,
                    item.id,
                    request.segment,
                ),
            )
            if updated.rowcount != 1:
                raise V5RepositoryError(
                    f"queue item {item.id} changed state or segment while its "
                    "yield request was being persisted; retry from current state"
                )
            self._insert_event(
                connection,
                created_at=request.requested_at,
                actor=request.requested_by,
                event_type="cooperative_yield_requested",
                scope="project",
                project_id=item.project_id,
                queue_item_id=item.id,
                payload={
                    "requestId": request.request_id,
                    "segment": request.segment,
                    "requestKind": request.request_kind.value,
                    "requestSha256": source_digest,
                },
            )
            row = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
                (request.request_id,),
            ).fetchone()
            assert row is not None
            return self._yield_request_from_row(row)

    def get_yield_request(self, request_id: str) -> V5YieldRequestRecord:
        """Load and authenticate one exact CooperativeYieldRequest/v1 row."""

        key = _require_text(request_id, field_name="request_id", maximum=256)
        with self._connection(operation="show cooperative-yield request", write=False) as connection:
            row = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5NotFoundError(f"schema-v5 has no yield request {key!r}")
            return self._yield_request_from_row(row)

    def _yield_receipt_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> V5YieldReceiptRecord:
        source = cast(bytes, _row_bytes(row, "receipt_json"))
        digest = sha256_bytes(source)
        if digest != row["receipt_sha256"]:
            raise V5EvidenceError(
                f"stored yield receipt {row['request_id']!r} SHA-256 does not "
                "match its exact wire bytes"
            )
        try:
            receipt = CooperativeYieldReceipt.from_document(
                _decode_wire_protocol(source, label="stored yield receipt")
            )
        except (CooperativeYieldError, TypeError, ValueError) as exc:
            raise V5EvidenceError(
                f"stored yield receipt {row['request_id']!r} is invalid: {exc}"
            ) from exc
        request_row = connection.execute(
            "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
            (receipt.request_id,),
        ).fetchone()
        if request_row is None:
            raise V5EvidenceError(
                f"stored yield receipt {receipt.request_id!r} has no request"
            )
        request_record = self._yield_request_from_row(request_row)
        try:
            validate_receipt_for_request(receipt, request_record.request)
        except CooperativeYieldError as exc:
            raise V5EvidenceError(
                f"stored yield receipt {receipt.request_id!r} does not match its "
                f"request: {exc}"
            ) from exc
        progress_json = (
            None
            if receipt.progress is None
            else canonical_json_bytes(receipt.progress.to_document())
        )
        artifacts_json = (
            None
            if receipt.status is YieldReceiptStatus.FAILED
            else canonical_json_bytes(
                [artifact.to_document() for artifact in receipt.checkpoint_artifacts]
            )
        )
        resume = receipt.resume_context
        expected = (
            receipt.request_id,
            receipt.queue_item_id,
            receipt.segment,
            request_record.request.continuation.identity_sha256,
            "experiment-queue/v1",
            "CooperativeYieldReceipt",
            receipt.status.value,
            receipt.written_at,
            progress_json,
            None if progress_json is None else sha256_bytes(progress_json),
            artifacts_json,
            None if artifacts_json is None else sha256_bytes(artifacts_json),
            None if resume is None else resume.payload,
            None if resume is None else len(resume.payload),
            None if resume is None else resume.media_type,
            None if resume is None else resume.sha256,
            receipt.error,
        )
        actual = tuple(
            row[name]
            for name in (
                "request_id",
                "queue_item_id",
                "segment",
                "bound_continuation_identity_sha256",
                "protocol_api_version",
                "protocol_kind",
                "status",
                "written_at",
                "progress_json",
                "progress_sha256",
                "checkpoint_artifacts_json",
                "checkpoint_artifacts_sha256",
                "resume_context",
                "resume_context_bytes",
                "resume_context_media_type",
                "resume_context_sha256",
                "error",
            )
        )
        if actual != expected:
            raise V5EvidenceError(
                f"stored yield receipt {receipt.request_id!r} decomposed evidence "
                "differs from its authenticated exact wire document/request"
            )
        return V5YieldReceiptRecord(
            receipt=receipt,
            source=source,
            sha256=digest,
            project_id=int(row["project_id"]),
            revision_id=int(row["revision_id"]),
        )

    def record_yield_receipt(
        self,
        receipt: CooperativeYieldReceipt,
        *,
        source: bytes,
        actor: str,
    ) -> V5YieldReceiptRecord:
        """Append one project-owned typed receipt with all evidence rehashed."""

        if type(receipt) is not CooperativeYieldReceipt:
            raise TypeError(
                f"receipt must be exactly CooperativeYieldReceipt, got "
                f"{type(receipt).__name__}"
            )
        event_actor = _require_text(actor, field_name="actor", maximum=256)
        try:
            parsed = CooperativeYieldReceipt.from_document(
                _decode_wire_protocol(source, label="yield receipt")
            )
        except (CooperativeYieldError, TypeError, ValueError) as exc:
            raise V5EvidenceError(f"yield receipt wire evidence is invalid: {exc}") from exc
        if parsed != receipt:
            raise V5EvidenceError(
                "yield receipt object differs from its exact supplied wire bytes"
            )
        with self._connection(operation="record cooperative-yield receipt", write=True) as connection:
            existing_receipt = connection.execute(
                "SELECT * FROM cooperative_yield_receipts WHERE request_id = ?",
                (receipt.request_id,),
            ).fetchone()
            if existing_receipt is not None:
                existing_record = self._yield_receipt_from_row(
                    connection,
                    existing_receipt,
                )
                if existing_record.receipt == receipt and existing_record.source == source:
                    return existing_record
                raise V5RepositoryError(
                    f"yield receipt for request {receipt.request_id!r} already "
                    "identifies different immutable evidence"
                )
            request_row = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
                (receipt.request_id,),
            ).fetchone()
            if request_row is None:
                raise V5NotFoundError(
                    f"yield receipt names missing request {receipt.request_id!r}"
                )
            request_record = self._yield_request_from_row(request_row)
            try:
                validate_receipt_for_request(receipt, request_record.request)
            except CooperativeYieldError as exc:
                raise V5EvidenceError(
                    f"yield receipt does not match request {receipt.request_id!r}: "
                    f"{exc}"
                ) from exc
            progress_json = (
                None
                if receipt.progress is None
                else canonical_json_bytes(receipt.progress.to_document())
            )
            artifacts_json = (
                None
                if receipt.status is YieldReceiptStatus.FAILED
                else canonical_json_bytes(
                    [
                        artifact.to_document()
                        for artifact in receipt.checkpoint_artifacts
                    ]
                )
            )
            resume = receipt.resume_context
            connection.execute(
                """
                INSERT INTO cooperative_yield_receipts(
                    request_id, queue_item_id, project_id, revision_id, segment,
                    bound_continuation_identity_sha256, protocol_api_version,
                    protocol_kind, status, written_at, receipt_json,
                    receipt_sha256, progress_json, progress_sha256,
                    checkpoint_artifacts_json, checkpoint_artifacts_sha256,
                    resume_context, resume_context_bytes,
                    resume_context_media_type, resume_context_sha256, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.request_id,
                    receipt.queue_item_id,
                    request_record.project_id,
                    request_record.revision_id,
                    receipt.segment,
                    request_record.request.continuation.identity_sha256,
                    "experiment-queue/v1",
                    "CooperativeYieldReceipt",
                    receipt.status.value,
                    receipt.written_at,
                    source,
                    sha256_bytes(source),
                    progress_json,
                    None if progress_json is None else sha256_bytes(progress_json),
                    artifacts_json,
                    None if artifacts_json is None else sha256_bytes(artifacts_json),
                    None if resume is None else resume.payload,
                    None if resume is None else len(resume.payload),
                    None if resume is None else resume.media_type,
                    None if resume is None else resume.sha256,
                    receipt.error,
                ),
            )
            self._insert_event(
                connection,
                created_at=receipt.written_at,
                actor=event_actor,
                event_type="cooperative_yield_receipt_recorded",
                scope="project",
                project_id=request_record.project_id,
                queue_item_id=receipt.queue_item_id,
                payload={
                    "requestId": receipt.request_id,
                    "segment": receipt.segment,
                    "status": receipt.status.value,
                    "receiptSha256": sha256_bytes(source),
                },
            )
            row = connection.execute(
                "SELECT * FROM cooperative_yield_receipts WHERE request_id = ?",
                (receipt.request_id,),
            ).fetchone()
            assert row is not None
            return self._yield_receipt_from_row(connection, row)

    def get_yield_receipt(self, request_id: str) -> V5YieldReceiptRecord:
        """Load and authenticate one exact CooperativeYieldReceipt/v1 row."""

        key = _require_text(request_id, field_name="request_id", maximum=256)
        with self._connection(operation="show cooperative-yield receipt", write=False) as connection:
            row = connection.execute(
                "SELECT * FROM cooperative_yield_receipts WHERE request_id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5NotFoundError(f"schema-v5 has no yield receipt {key!r}")
            return self._yield_receipt_from_row(connection, row)

    def get_ready_yield_receipt_for_segment(
        self,
        queue_item_id: int,
        *,
        completed_segment: int,
    ) -> V5YieldReceiptRecord:
        """Load the one authenticated ready receipt that resumes a later segment."""

        item_key = _positive_integer(queue_item_id, field_name="queue_item_id")
        segment = _positive_integer(
            completed_segment, field_name="completed_segment"
        )
        with self._connection(
            operation=(
                f"load queue item {item_key} segment {segment} continuation receipt"
            ),
            write=False,
        ) as connection:
            rows = connection.execute(
                """
                SELECT receipt.*
                FROM cooperative_yield_receipts AS receipt
                JOIN cooperative_yield_requests AS request
                  ON request.request_id = receipt.request_id
                WHERE request.queue_item_id = ? AND request.segment = ?
                  AND receipt.status = 'ready'
                ORDER BY receipt.request_id
                """,
                (item_key, segment),
            ).fetchall()
            if len(rows) != 1:
                raise V5NotFoundError(
                    f"queue item {item_key} segment {segment} requires exactly one "
                    f"ready continuation receipt, found {len(rows)}"
                )
            return self._yield_receipt_from_row(connection, rows[0])

    @staticmethod
    def _checkpoint_names(snapshot: AdmissionSnapshot) -> tuple[str, ...]:
        resolved = snapshot.resolved_document
        job = resolved.get("job")
        if type(job) is not dict:
            raise V5EvidenceError(
                "admitted resolved execution has no typed job object"
            )
        resources = job.get("resources")
        if type(resources) is dict:
            gpu_count = resources.get("gpus", 0)
            if type(gpu_count) is not int or gpu_count not in {0, 1}:
                raise V5EvidenceError(
                    "version-1 continuation supports one independently schedulable "
                    "GPU job only; DDP/gang continuation is unsupported"
                )
        capabilities = job.get("capabilities")
        cooperative = (
            capabilities.get("cooperativeYield")
            if type(capabilities) is dict
            else None
        )
        names = (
            cooperative.get("checkpointArtifacts")
            if type(cooperative) is dict
            else None
        )
        if type(names) is not list or not names or not all(
            type(name) is str for name in names
        ):
            raise V5EvidenceError(
                "admitted job lacks nonempty cooperative-yield checkpoint names"
            )
        return tuple(cast(list[str], names))

    def requeue_ready_continuation(
        self,
        request_id: str,
        *,
        actor: str,
        changed_at: str,
    ) -> V5QueueItem:
        """Revalidate a persisted ready receipt and atomically append its segment.

        Termination wins the compare-and-set: only the exact ``yielding`` item
        and segment named by the immutable request can return to ``queued``.
        Project filesystem evidence is rehashed again inside this transaction,
        immediately before the state change.
        """

        key = _require_text(request_id, field_name="request_id", maximum=256)
        event_actor = _require_text(actor, field_name="actor", maximum=256)
        timestamp = _require_timestamp(changed_at, field_name="changed_at")
        with self._connection(operation="requeue ready continuation", write=True) as connection:
            request_row = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE request_id = ?",
                (key,),
            ).fetchone()
            if request_row is None:
                raise V5NotFoundError(f"schema-v5 has no yield request {key!r}")
            request_record = self._yield_request_from_row(request_row)
            receipt_row = connection.execute(
                "SELECT * FROM cooperative_yield_receipts WHERE request_id = ?",
                (key,),
            ).fetchone()
            if receipt_row is None:
                raise V5RepositoryError(
                    f"yield request {key!r} has no persisted receipt; read, "
                    "validate, and persist it before requeue"
                )
            receipt_record = self._yield_receipt_from_row(connection, receipt_row)
            item_row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (request_record.request.queue_item_id,),
            ).fetchone()
            if item_row is None:  # pragma: no cover - composite FK invariant
                raise V5EvidenceError(
                    f"yield request {key!r} lost queue item ownership"
                )
            current_state = str(item_row["state"])
            current_segment = int(item_row["segment"])
            next_segment = request_record.request.segment + 1
            if current_state == "queued" and current_segment == next_segment:
                return self._queue_item_from_row(connection, item_row)
            if current_state != "yielding":
                raise V5RepositoryError(
                    f"queue item {request_record.request.queue_item_id} is "
                    f"{current_state!r}; only the matching yielding state may "
                    "requeue, so termination or failure wins"
                )
            if (
                current_segment != request_record.request.segment
                or item_row["yield_request_id"] != key
            ):
                raise V5EvidenceError(
                    f"queue item {request_record.request.queue_item_id} current "
                    "segment/request identity differs from persisted yield evidence"
                )
            item = self._queue_item_from_row(connection, item_row)
            if item.snapshot is None:
                raise V5EvidenceError(
                    "typed continuation cannot requeue an item without an "
                    "authenticated AdmissionSnapshot"
                )
            revision = self._load_revision(connection, item.revision_id)
            previous_row = connection.execute(
                """
                SELECT receipt.*
                FROM cooperative_yield_receipts AS receipt
                JOIN cooperative_yield_requests AS request
                  ON request.request_id = receipt.request_id
                WHERE request.queue_item_id = ? AND request.segment < ?
                  AND receipt.status = 'ready'
                ORDER BY request.segment DESC LIMIT 1
                """,
                (item.id, item.segment),
            ).fetchone()
            previous_progress = (
                None
                if previous_row is None
                else self._yield_receipt_from_row(connection, previous_row).receipt.progress
            )
            continuation = request_record.request.continuation
            try:
                validate_ready_continuation(
                    receipt_record.receipt,
                    request_record.request,
                    resolved_spec_sha256=item.snapshot.resolved_sha256,
                    project_revision=revision.label,
                    git_commit=revision.git_commit,
                    run_id=continuation.run_id,
                    prior_receipt_sha256=continuation.prior_receipt_sha256,
                    allowed_artifact_roots=(
                        root.path for root in revision.enrollment.artifact_roots
                    ),
                    expected_checkpoint_names=self._checkpoint_names(item.snapshot),
                    previous_progress=previous_progress,
                )
            except CooperativeYieldError as exc:
                raise V5EvidenceError(
                    f"yield request {key!r} cannot requeue because continuation "
                    f"evidence failed final validation: {exc}"
                ) from exc
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = 'queued', state_detail = NULL, segment = ?,
                    resume_front = 1, runtime_gpu_lease_held = 0,
                    runtime_gpu_lease_released_at = ?,
                    pid = NULL, pgid = NULL,
                    proc_start_ticks = NULL, started_at = NULL,
                    finished_at = NULL, return_code = NULL,
                    terminate_requested_at = NULL, terminate_reason = NULL,
                    termination_stage = NULL, termination_signal_epoch = NULL,
                    yield_requested_at = NULL, yield_requested_by = NULL,
                    yield_request_id = NULL, yield_note = NULL
                WHERE id = ? AND state = 'yielding'
                  AND runtime_gpu_lease_held = 1 AND segment = ?
                  AND yield_request_id = ?
                """,
                (next_segment, timestamp, item.id, item.segment, key),
            )
            if cursor.rowcount != 1:
                raise V5RepositoryError(
                    f"queue item {item.id} changed during continuation finalization; "
                    "termination/failure wins and no stale receipt was requeued"
                )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="cooperative_yield_requeued",
                scope="project",
                project_id=item.project_id,
                queue_item_id=item.id,
                payload={
                    "requestId": key,
                    "receiptSha256": receipt_record.sha256,
                    "completedSegment": item.segment,
                    "nextSegment": next_segment,
                    "resumeFront": True,
                    "gpuUuid": item_row["assigned_gpu_uuid"],
                    "gpuIndex": item_row["assigned_gpu_index"],
                    "runtimeGpuLeaseReleasedAt": timestamp,
                },
            )
            updated = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (item.id,),
            ).fetchone()
            assert updated is not None
            return self._queue_item_from_row(connection, updated)

    def isolate_continuation_failure(
        self,
        item_id: int,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        terminal: bool,
    ) -> V5QueueItem:
        """Quarantine a yielding item without releasing ambiguous runtime work.

        Nonterminal isolation keeps the active ``yielding`` state and exact
        process/GPU lease identity for later recovery.  Terminal isolation is
        used only after authenticated executor exit; it records ``failed`` but
        still preserves historical process and GPU identity until the separate
        telemetry-gated lease release.
        """

        key = _positive_integer(item_id, field_name="item_id")
        detail = _require_text(reason, field_name="reason", maximum=4000)
        event_actor = _require_text(actor, field_name="actor", maximum=256)
        timestamp = _require_timestamp(changed_at, field_name="changed_at")
        if type(terminal) is not bool:
            raise TypeError(f"terminal must be boolean, got {type(terminal).__name__}")
        target = "failed" if terminal else "yielding"
        with self._connection(operation="isolate continuation failure", write=True) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5NotFoundError(f"schema-v5 has no queue item with id {key}")
            current = str(row["state"])
            if current == target and row["state_detail"] == detail:
                return self._queue_item_from_row(connection, row)
            if current != "yielding":
                raise V5RepositoryError(
                    f"queue item {key} is {current!r}; continuation isolation may "
                    "change only its yielding state and never override termination"
                )
            if terminal:
                connection.execute(
                    """
                    UPDATE queue_items
                    SET state = 'failed', state_detail = ?, finished_at = ?
                    WHERE id = ? AND state = 'yielding'
                    """,
                    (detail, timestamp, key),
                )
            else:
                connection.execute(
                    """
                    UPDATE queue_items
                    SET state_detail = ?
                    WHERE id = ? AND state = 'yielding'
                    """,
                    (detail, key),
                )
            project_id = int(row["project_id"])
            runtime = connection.execute(
                "SELECT circuit_failure_count FROM project_runtime_state "
                "WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            if runtime is None:
                raise V5EvidenceError(
                    f"Project id {project_id} has no runtime circuit row"
                )
            connection.execute(
                """
                UPDATE project_runtime_state
                SET health = 'open', circuit_failure_count = ?,
                    health_reason = ?, health_actor = ?, health_changed_at = ?
                WHERE project_id = ?
                """,
                (
                    int(runtime["circuit_failure_count"]) + 1,
                    detail,
                    event_actor,
                    timestamp,
                    project_id,
                ),
            )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="cooperative_yield_isolated",
                scope="project",
                project_id=project_id,
                queue_item_id=key,
                payload={
                    "previousState": current,
                    "state": target,
                    "reason": detail,
                    "segment": int(row["segment"]),
                },
            )
            updated = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (key,),
            ).fetchone()
            assert updated is not None
            return self._queue_item_from_row(connection, updated)


__all__ = [
    "V5EvidenceError",
    "V5Event",
    "V5NotFoundError",
    "V5ProjectRepository",
    "V5ProjectView",
    "V5QueueItem",
    "V5RepositoryError",
    "V5RevisionGitEvidence",
    "V5StoredGitBlob",
    "V5YieldReceiptRecord",
    "V5YieldRequestRecord",
]
