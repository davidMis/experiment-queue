"""Typed operator transactions and project-scoped reads for schema-v5.

The command layer never issues SQL.  This service owns the small set of
operator-authorized mutable fields, repeats project ownership checks inside
``BEGIN IMMEDIATE`` transactions, and delegates immutable Project/admission and
cooperative-yield evidence authentication to :mod:`v5_repository`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Final, Iterator, Mapping, cast

from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.execution import ExecutionValidationError, resolve_artifact_path
from experiment_queue.identity import validate_project_key
from experiment_queue.project_lifecycle import (
    HostRootClaim,
    ProjectHealth,
    ProjectLifecycle,
    ProjectRevision,
)
from experiment_queue.serialization import JSONValue, canonical_json_bytes, sha256_bytes
from experiment_queue.queue_export import (
    MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES,
    MAX_QUEUE_EXPORT_TOTAL_RECORDS,
)
from experiment_queue.v5_repository import (
    V5Event,
    V5ProjectRepository,
    V5ProjectView,
    V5QueueItem,
    V5RevisionGitEvidence,
    V5YieldRequestRecord,
    V5YieldReceiptRecord,
)


_QUEUE_STATES: Final = frozenset(
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
_PENDING_STATES: Final = frozenset({"queued", "held", "blocked"})
_PRIORITY_STATES: Final = frozenset(
    {"queued", "held", "blocked", "starting", "running", "yielding"}
)
_ACTIVE_STATES: Final = frozenset(
    {"starting", "running", "yielding", "terminating", "force_killing"}
)
_SIGNED_64_MIN: Final = -(2**63)
_SIGNED_64_MAX: Final = 2**63 - 1


class V5OperatorError(RuntimeError):
    """Raised when an operator read or mutation cannot complete safely."""


class V5OperatorNotFoundError(V5OperatorError):
    """Raised when a project-qualified selector resolves to no authorized row."""


class V5OperatorEvidenceError(V5OperatorError):
    """Raised when immutable operator-visible evidence cannot be authenticated."""


@dataclass(frozen=True, slots=True)
class V5OperatorItemView:
    """Authenticated queue identity plus its mutable runtime state snapshot."""

    item: V5QueueItem
    project_key: str
    revision_label: str
    dependencies: tuple[int, ...]
    assigned_gpu_uuid: str | None
    assigned_gpu_index: str | None
    runtime_gpu_lease_held: bool
    runtime_gpu_lease_released_at: str | None
    pid: int | None
    pgid: int | None
    process_start_ticks: str | None
    started_at: str | None
    finished_at: str | None
    return_code: int | None
    terminate_requested_at: str | None
    terminate_reason: str | None
    termination_stage: str | None
    runner_run_dir: str | None
    runner_manifest_path: str | None
    rsync_pull_command: str | None
    yield_request_id: str | None
    yield_requested_at: str | None
    continuation_checkpoint: str | None
    continuation_checkpoint_sha256: str | None
    historical_git_ref: str | None
    historical_worktree_path: str | None
    runtime_git_ref: str | None
    runtime_worktree_path: str | None
    runtime_worktree_cleanup_error: str | None
    dependency_targets: tuple[V5DependencyTarget, ...] = ()
    persisted_runtime: Mapping[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class V5DependencyTarget:
    """Authenticated identity and current state of one dependency target."""

    item_id: int
    project_id: int
    project_key: str
    revision_id: int
    revision_label: str
    state: str


@dataclass(frozen=True, slots=True)
class V5ProjectStatus:
    """One authenticated Project view combined with the host dispatch gate."""

    project: V5ProjectView
    host_dispatch_paused: bool
    host_pause_reason: str

    @property
    def dispatch_allowed(self) -> bool:
        """Return the complete host-and-Project dispatch decision."""

        return not self.host_dispatch_paused and self.project.dispatch_allowed


@dataclass(frozen=True, slots=True)
class V5ProjectSummary:
    """Legacy-safe Project identity, lifecycle, health, and current revision."""

    id: int
    key: str
    display_name: str
    lifecycle: ProjectLifecycle
    current_revision_id: int
    current_revision_sequence: int
    current_revision_label: str
    current_revision_kind: str
    current_git_commit: str | None
    health: ProjectHealth
    circuit_failure_count: int
    lifecycle_reason: str
    lifecycle_actor: str
    lifecycle_changed_at: str
    health_reason: str
    health_actor: str
    health_changed_at: str
    host_dispatch_paused: bool
    host_pause_reason: str
    queue_counts: tuple[tuple[str, int], ...]
    typed_view: V5ProjectView | None = field(default=None, repr=False)

    @property
    def dispatch_allowed(self) -> bool:
        """Return the combined host, lifecycle, and health gate."""

        return (
            not self.host_dispatch_paused
            and self.lifecycle is ProjectLifecycle.ACTIVE
            and self.health is ProjectHealth.CLOSED
        )


@dataclass(frozen=True, slots=True)
class V5GpuAllowlistEntry:
    """One exact GPU allowlist identity and current administrative flags."""

    uuid: str
    requested_identifier: str
    last_index: str
    name: str
    enabled: bool
    draining: bool
    updated_at: str
    assigned_queue_item_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class V5ArtifactRecord:
    """Authenticated immutable artifact observation owned by one global item."""

    id: int
    queue_item_id: int
    project_id: int
    revision_id: int
    segment: int
    evidence_kind: str
    artifact_name: str
    artifact_type: str
    root_name: str | None
    relative_path: str | None
    absolute_path: Path
    size_bytes: int | None
    sha256: str | None
    recorded_at: str
    _metadata_json: bytes | None = field(default=None, repr=False)

    @property
    def metadata(self) -> JSONValue:
        """Return detached metadata decoded from exact canonical stored bytes."""

        if self._metadata_json is None:
            return None
        return cast(JSONValue, json.loads(self._metadata_json))


@dataclass(frozen=True, slots=True)
class V5RevisionSummary:
    """Authenticated typed or exact imported revision evidence for presentation."""

    id: int
    project_id: int
    sequence: int
    label: str
    kind: str
    display_name: str
    git_commit: str | None
    checkout_path: Path
    created_at: str
    created_actor: str
    enrollment_sha256: str
    enrollment_source: bytes = field(repr=False)
    typed_revision: ProjectRevision | None = field(default=None, repr=False)
    git_evidence: V5RevisionGitEvidence | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class V5ProjectExport:
    """Authenticated evidence used to construct one QueueExport/v1 document."""

    project: V5ProjectSummary
    revisions: tuple[V5RevisionSummary, ...]
    items: tuple[V5OperatorItemView, ...]
    events: tuple[V5Event, ...]
    artifacts: tuple[V5ArtifactRecord, ...]
    yield_requests: tuple[V5YieldRequestRecord, ...]
    yield_receipts: tuple[V5YieldReceiptRecord, ...]
    host_state: V5HostState


@dataclass(frozen=True, slots=True)
class V5HostState:
    """Host dispatch gate plus the latest persisted provenance event, if any."""

    dispatch_paused: bool
    reason: str
    provenance_event: V5Event | None


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise V5OperatorError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise V5OperatorError(f"{field_name} must be a nonnegative integer")
    return value


def _priority(value: object) -> int:
    if type(value) is not int or not _SIGNED_64_MIN <= value <= _SIGNED_64_MAX:
        raise V5OperatorError("priority must be a signed 64-bit integer")
    return value


def _text(value: object, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5OperatorError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise V5OperatorError(
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
        raise V5OperatorError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V5OperatorError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        )
    return timestamp


def _optional_text(row: sqlite3.Row, name: str) -> str | None:
    return None if row[name] is None else str(row[name])


def _payload_json(payload: Mapping[str, object]) -> str:
    try:
        return canonical_json_bytes(dict(payload)).decode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise V5OperatorError(f"event payload is not canonical JSON: {exc}") from exc


def _canonical_metadata(value: object, *, artifact_id: int) -> bytes | None:
    if value is None:
        return None
    if type(value) is not bytes:
        raise V5OperatorEvidenceError(
            f"artifact id {artifact_id} metadata_json is not stored as bytes"
        )
    source = cast(bytes, value)
    try:
        parsed = cast(JSONValue, json.loads(source))
        canonical = canonical_json_bytes(parsed)
    except (json.JSONDecodeError, UnicodeError, TypeError, ValueError) as exc:
        raise V5OperatorEvidenceError(
            f"artifact id {artifact_id} metadata_json is invalid: {exc}"
        ) from exc
    if source != canonical:
        raise V5OperatorEvidenceError(
            f"artifact id {artifact_id} metadata_json is not exact canonical JSON"
        )
    return source


class V5OperatorRepository:
    """Public project-authorized boundary for schema-v5 operator commands."""

    def __init__(self, store: V5QueueStore):
        if type(store) is not V5QueueStore:
            raise TypeError(
                f"store must be exactly V5QueueStore, got {type(store).__name__}"
            )
        self.store = store
        self.projects = V5ProjectRepository(store)

    @contextmanager
    def _connection(
        self, *, operation: str, write: bool
    ) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        try:
            connection = self.store.connect()
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            if write:
                connection.commit()
        except (sqlite3.Error, V5DatabaseError) as exc:
            if connection is not None:
                connection.rollback()
            raise V5OperatorError(
                f"schema-v5 could not {operation}: {exc}; no partial operator "
                "state was committed"
            ) from exc
        except BaseException:
            if connection is not None:
                connection.rollback()
            raise
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        *,
        created_at: str,
        actor: str,
        event_type: str,
        payload: Mapping[str, object],
        project_id: int | None = None,
        queue_item_id: int | None = None,
    ) -> None:
        if project_id is None:
            if queue_item_id is not None:
                raise V5OperatorError(
                    "host-scoped events cannot identify a queue item"
                )
            scope = "host"
        else:
            _positive_integer(project_id, field_name="event project_id")
            if queue_item_id is not None:
                _positive_integer(queue_item_id, field_name="event queue_item_id")
            scope = "project"
        connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _timestamp(created_at, field_name="event created_at"),
                _text(actor, field_name="event actor", maximum=256),
                _text(event_type, field_name="event type", maximum=256),
                queue_item_id,
                _payload_json(payload),
                scope,
                project_id,
            ),
        )

    def next_project_identity(self) -> tuple[int, int]:
        """Return candidate positive Project/revision IDs for trusted factories.

        The eventual typed registration transaction is authoritative.  A rare
        concurrent registration may consume either candidate and will fail
        cleanly instead of overwriting or reusing identity.
        """

        with self._connection(operation="allocate Project identity", write=False) as connection:
            project_id = int(
                connection.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM projects").fetchone()[0]
            )
            revision_id = int(
                connection.execute(
                    "SELECT COALESCE(MAX(id), 0) + 1 FROM project_revisions"
                ).fetchone()[0]
            )
        return project_id, revision_id

    def next_revision_identity(self, project_id: int) -> tuple[int, int]:
        """Return candidate global revision ID and per-Project sequence."""

        project_key = _positive_integer(project_id, field_name="project_id")
        summary = self.get_project_summary(project_id=project_key)
        if summary.lifecycle is ProjectLifecycle.ARCHIVED:
            raise V5OperatorError(
                f"Project {summary.key!r} is archived; no revision identity can "
                "be allocated"
            )
        with self._connection(operation="allocate Project revision identity", write=False) as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT COALESCE(MAX(id), 0) + 1 FROM project_revisions) AS revision_id,
                    COALESCE(MAX(sequence), 0) + 1 AS sequence
                FROM project_revisions WHERE project_id = ?
                """,
                (project_key,),
            ).fetchone()
            assert row is not None
            return int(row["revision_id"]), int(row["sequence"])

    def occupied_roots(
        self, *, exclude_project_id: int | None = None
    ) -> tuple[HostRootClaim, ...]:
        """Authenticate and return every other Project revision root claim."""

        excluded = (
            None
            if exclude_project_id is None
            else _positive_integer(exclude_project_id, field_name="exclude_project_id")
        )
        with self._connection(operation="inventory occupied Project roots", write=False) as connection:
            if excluded is None:
                revision_rows = connection.execute(
                    """
                    SELECT r.id, r.project_id, r.revision_kind, r.checkout_path,
                           p.project_key
                    FROM project_revisions AS r
                    JOIN projects AS p ON p.id = r.project_id
                    ORDER BY r.id
                    """
                ).fetchall()
            else:
                revision_rows = connection.execute(
                    """
                    SELECT r.id, r.project_id, r.revision_kind, r.checkout_path,
                           p.project_key
                    FROM project_revisions AS r
                    JOIN projects AS p ON p.id = r.project_id
                    WHERE r.project_id <> ?
                    ORDER BY r.id
                    """,
                    (excluded,),
                ).fetchall()
            legacy_mount_rows = connection.execute(
                """
                SELECT m.project_id, m.revision_id, m.mount_name, m.mount_path,
                       p.project_key
                FROM project_mounts AS m
                JOIN project_revisions AS r ON r.id = m.revision_id
                JOIN projects AS p ON p.id = m.project_id
                WHERE r.revision_kind = 'legacy-v4'
                  AND (? IS NULL OR m.project_id <> ?)
                ORDER BY m.revision_id, m.mount_name
                """,
                (excluded, excluded),
            ).fetchall()
        claims: list[HostRootClaim] = []
        for row in revision_rows:
            key = str(row["project_key"])
            if str(row["revision_kind"]) == "legacy-v4":
                claims.append(
                    HostRootClaim.create(
                        project_key=key,
                        role=f"revision {int(row['id'])} imported checkout",
                        path=Path(str(row["checkout_path"])),
                    )
                )
                continue
            revision = self.projects.get_revision(int(row["id"]))
            claims.append(
                HostRootClaim.create(
                    project_key=key,
                    role=f"revision {revision.id} checkout",
                    path=revision.enrollment.checkout_directory,
                )
            )
            claims.extend(
                HostRootClaim.create(
                    project_key=key,
                    role=f"revision {revision.id} mount {mount.name!r}",
                    path=mount.path,
                )
                for mount in revision.enrollment.mounts
            )
            for environment in revision.enrollment.environments:
                claims.extend(
                    HostRootClaim.create(
                        project_key=key,
                        role=(
                            f"revision {revision.id} environment "
                            f"{environment.name!r} search directory {index}"
                        ),
                        path=path,
                    )
                    for index, path in enumerate(
                        environment.executable_search_directories
                    )
                )
        claims.extend(
            HostRootClaim.create(
                project_key=str(row["project_key"]),
                role=(
                    f"revision {int(row['revision_id'])} imported mount "
                    f"{str(row['mount_name'])!r}"
                ),
                path=Path(str(row["mount_path"])),
            )
            for row in legacy_mount_rows
        )
        return tuple(claims)

    def infer_project_from_cwd(self, cwd: Path) -> V5ProjectSummary:
        """Infer only when one current registered checkout contains canonical cwd."""

        if not isinstance(cwd, Path) or not cwd.is_absolute():
            raise V5OperatorError("cwd inference requires an absolute pathlib.Path")
        try:
            canonical = cwd.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise V5OperatorError(f"cannot resolve current directory {cwd}: {exc}") from exc
        if canonical != cwd or not canonical.is_dir():
            raise V5OperatorError(
                f"current directory must be its canonical directory path, got {cwd}"
            )
        with self._connection(operation="infer Project from cwd", write=False) as connection:
            rows = connection.execute(
                """
                SELECT p.id, r.checkout_path
                FROM projects AS p
                JOIN project_revisions AS r ON r.id = p.current_revision_id
                ORDER BY p.id
                """
            ).fetchall()
        matches: list[V5ProjectSummary] = []
        for row in rows:
            checkout = Path(str(row["checkout_path"]))
            try:
                resolved = checkout.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise V5OperatorEvidenceError(
                    f"registered current checkout {checkout} cannot be resolved: "
                    f"{exc}; repair or append a valid Project revision"
                ) from exc
            if resolved != checkout or not resolved.is_dir():
                raise V5OperatorEvidenceError(
                    f"registered current checkout {checkout} is no longer its "
                    "canonical directory"
                )
            if canonical == checkout or checkout in canonical.parents:
                matches.append(self.get_project_summary(project_id=int(row["id"])))
        if not matches:
            raise V5OperatorNotFoundError(
                f"current directory {canonical} is not inside any current registered "
                "Project checkout; pass --project explicitly"
            )
        if len(matches) != 1:
            keys = ", ".join(sorted(view.key for view in matches))
            raise V5OperatorError(
                f"current directory {canonical} matches multiple Projects ({keys}); "
                "pass --project explicitly"
            )
        return matches[0]

    @staticmethod
    def _host_gate(connection: sqlite3.Connection) -> tuple[bool, str]:
        metadata = dict(
            connection.execute(
                "SELECT key, value FROM metadata WHERE key IN "
                "('dispatch_paused', 'pause_reason')"
            )
        )
        if set(metadata) != {"dispatch_paused", "pause_reason"}:
            raise V5OperatorEvidenceError(
                "schema-v5 host dispatch metadata is incomplete"
            )
        paused = metadata["dispatch_paused"]
        if paused not in {"0", "1"}:
            raise V5OperatorEvidenceError(
                f"schema-v5 dispatch_paused is invalid: {paused!r}"
            )
        return paused == "1", metadata["pause_reason"]

    def _summary_from_row(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> V5ProjectSummary:
        project_id = _positive_integer(int(row["id"]), field_name="stored Project id")
        try:
            key = validate_project_key(str(row["project_key"]))
            lifecycle = ProjectLifecycle(str(row["lifecycle"]))
            health = ProjectHealth(str(row["health"]))
        except (TypeError, ValueError) as exc:
            raise V5OperatorEvidenceError(
                f"stored Project id {project_id} identity/lifecycle is invalid: {exc}"
            ) from exc
        revision_id = int(row["current_revision_id"])
        sequence = int(row["current_revision_sequence"])
        if revision_id <= 0 or sequence <= 0:
            raise V5OperatorEvidenceError(
                f"stored Project {key!r} has invalid current revision identity"
            )
        kind = str(row["revision_kind"])
        if kind not in {"project-v1", "legacy-v4"}:
            raise V5OperatorEvidenceError(
                f"stored Project {key!r} has unknown revision kind {kind!r}"
            )
        label = str(row["revision_label"])
        expected_label = (
            f"{key}:r{sequence}"
            if kind == "project-v1"
            else f"{key}:legacy-r{sequence}"
        )
        if label != expected_label:
            raise V5OperatorEvidenceError(
                f"stored Project {key!r} current revision label {label!r} does "
                f"not match {kind} sequence {sequence}"
            )
        failures = int(row["circuit_failure_count"])
        if failures < 0 or (health is ProjectHealth.OPEN and failures == 0):
            raise V5OperatorEvidenceError(
                f"stored Project {key!r} health/failure count is inconsistent"
            )
        counts = tuple(
            (str(count["state"]), int(count["count"]))
            for count in connection.execute(
                "SELECT state, COUNT(*) AS count FROM queue_items "
                "WHERE project_id = ? GROUP BY state ORDER BY state",
                (project_id,),
            )
        )
        if any(state not in _QUEUE_STATES or count < 1 for state, count in counts):
            raise V5OperatorEvidenceError(
                f"stored Project {key!r} has invalid queue state counts"
            )
        paused, pause_reason = self._host_gate(connection)
        commit = _optional_text(row, "git_commit")
        summary = V5ProjectSummary(
            id=project_id,
            key=key,
            display_name=_text(
                row["display_name"],
                field_name=f"Project {key} display_name",
                maximum=500,
            ),
            lifecycle=lifecycle,
            current_revision_id=revision_id,
            current_revision_sequence=sequence,
            current_revision_label=label,
            current_revision_kind=kind,
            current_git_commit=commit,
            health=health,
            circuit_failure_count=failures,
            lifecycle_reason=_text(
                row["lifecycle_reason"],
                field_name=f"Project {key} lifecycle reason",
            ),
            lifecycle_actor=_text(
                row["lifecycle_actor"],
                field_name=f"Project {key} lifecycle actor",
                maximum=256,
            ),
            lifecycle_changed_at=_timestamp(
                row["lifecycle_changed_at"],
                field_name=f"Project {key} lifecycle changed_at",
            ),
            health_reason=_text(
                row["health_reason"], field_name=f"Project {key} health reason"
            ),
            health_actor=_text(
                row["health_actor"],
                field_name=f"Project {key} health actor",
                maximum=256,
            ),
            health_changed_at=_timestamp(
                row["health_changed_at"],
                field_name=f"Project {key} health changed_at",
            ),
            host_dispatch_paused=paused,
            host_pause_reason=pause_reason,
            queue_counts=counts,
        )
        return summary

    def get_project_summary(
        self,
        *,
        project_id: int | None = None,
        project_key: str | None = None,
    ) -> V5ProjectSummary:
        """Load one typed or pre-adoption imported Project by one exact selector."""

        if (project_id is None) == (project_key is None):
            raise V5OperatorError(
                "select a Project by exactly one of project_id or project_key"
            )
        parameters: tuple[object, ...]
        predicate: str
        if project_id is not None:
            value: object = _positive_integer(project_id, field_name="project_id")
            predicate = "p.id = ?"
            parameters = (value,)
        else:
            try:
                value = validate_project_key(cast(str, project_key))
            except (TypeError, ValueError) as exc:
                raise V5OperatorError(f"project_key is invalid: {exc}") from exc
            predicate = "p.project_key = ?"
            parameters = (value,)
        with self._connection(operation="show Project summary", write=False) as connection:
            row = connection.execute(
                """
                SELECT p.*, r.revision_label, r.revision_kind, r.git_commit,
                       runtime.health, runtime.circuit_failure_count,
                       runtime.health_reason, runtime.health_actor,
                       runtime.health_changed_at
                FROM projects AS p
                JOIN project_revisions AS r
                  ON r.id = p.current_revision_id AND r.project_id = p.id
                JOIN project_runtime_state AS runtime ON runtime.project_id = p.id
                WHERE """ + predicate,
                parameters,
            ).fetchone()
            if row is None:
                rendered = (
                    f"id {project_id}"
                    if project_id is not None
                    else f"key {project_key!r}"
                )
                raise V5OperatorNotFoundError(
                    f"schema-v5 has no registered Project with {rendered}"
                )
            summary = self._summary_from_row(connection, row)
        if summary.current_revision_kind == "project-v1":
            typed = self.projects.get_project(project_id=summary.id)
            if (
                typed.project.id != summary.id
                or typed.project.key != summary.key
                or typed.project.display_name != summary.display_name
                or typed.project.lifecycle is not summary.lifecycle
                or typed.current_revision.id != summary.current_revision_id
                or typed.current_revision.sequence != summary.current_revision_sequence
                or typed.current_revision.label != summary.current_revision_label
                or typed.current_revision.git_commit != summary.current_git_commit
                or typed.runtime_state.health is not summary.health
                or typed.runtime_state.circuit_failure_count
                != summary.circuit_failure_count
                or typed.queue_counts != summary.queue_counts
            ):
                raise V5OperatorEvidenceError(
                    f"Project {summary.key!r} summary differs from authenticated "
                    "typed lifecycle evidence"
                )
            summary = V5ProjectSummary(
                **{
                    field.name: getattr(summary, field.name)
                    for field in V5ProjectSummary.__dataclass_fields__.values()
                    if field.name != "typed_view"
                },
                typed_view=typed,
            )
        return summary

    def list_project_summaries(
        self,
        *,
        project_keys: tuple[str, ...] = (),
        after_id: int = 0,
        limit: int = 500,
    ) -> tuple[V5ProjectSummary, ...]:
        """List stable-ID Project summaries, including pre-adoption imports."""

        after = _nonnegative_integer(after_id, field_name="after_id")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise V5OperatorError("limit must be an integer from 1 through 10000")
        if type(project_keys) is not tuple:
            raise V5OperatorError("project_keys must be a tuple")
        try:
            keys = tuple(validate_project_key(key) for key in project_keys)
        except (TypeError, ValueError) as exc:
            raise V5OperatorError(f"project_keys contains an invalid key: {exc}") from exc
        parameters: list[object] = [after]
        predicate = ""
        if keys:
            predicate = " AND project_key IN (" + ",".join("?" for _ in keys) + ")"
            parameters.extend(keys)
        parameters.append(limit)
        with self._connection(operation="list Project summaries", write=False) as connection:
            ids = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM projects WHERE id > ?" + predicate + " ORDER BY id LIMIT ?",
                    parameters,
                )
            )
        return tuple(self.get_project_summary(project_id=value) for value in ids)

    def project_status(self, project_id: int) -> V5ProjectStatus:
        """Return authenticated Project health/lifecycle and host dispatch state."""

        key = _positive_integer(project_id, field_name="project_id")
        project = self.projects.get_project(project_id=key)
        with self._connection(operation="read host dispatch state", write=False) as connection:
            paused, pause_reason = self._host_gate(connection)
        return V5ProjectStatus(
            project=project,
            host_dispatch_paused=paused,
            host_pause_reason=pause_reason,
        )

    def transition_project(
        self,
        *,
        project_id: int,
        target: ProjectLifecycle | str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5ProjectSummary:
        """Delegate one fully validated lifecycle mutation and return its summary."""

        key = _positive_integer(project_id, field_name="project_id")
        self.projects.transition_project(
            project_id=key,
            target=target,
            reason=reason,
            actor=actor,
            changed_at=changed_at,
        )
        return self.get_project_summary(project_id=key)

    def close_project_circuit(
        self,
        *,
        project_id: int,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5ProjectSummary:
        """Close one Project-local circuit without altering host or lifecycle state."""

        key = _positive_integer(project_id, field_name="project_id")
        self.projects.close_project_circuit(
            project_id=key,
            reason=reason,
            actor=actor,
            changed_at=changed_at,
        )
        return self.get_project_summary(project_id=key)

    @staticmethod
    def _item_core_tuple_from_row(row: sqlite3.Row) -> tuple[object, ...]:
        return (
            int(row["id"]),
            int(row["project_id"]),
            int(row["revision_id"]),
            str(row["admission_kind"]),
            None if row["snapshot_id"] is None else int(row["snapshot_id"]),
            _optional_text(row, "job_id"),
            str(row["experiment_id"]),
            int(row["attempt"]),
            str(row["state"]),
            int(row["priority"]),
            str(row["card_path"]),
            str(row["card_sha256"]),
            str(row["command_text"]),
            str(row["runner_name"]),
            str(row["git_commit"]),
            str(row["added_at"]),
            str(row["added_by"]),
            _optional_text(row, "state_detail"),
            bool(row["preemptible"]),
            int(row["segment"]),
            bool(row["resume_front"]),
        )

    @staticmethod
    def _item_core_tuple(item: V5QueueItem) -> tuple[object, ...]:
        return (
            item.id,
            item.project_id,
            item.revision_id,
            item.admission_kind,
            item.snapshot_id,
            item.job_id,
            item.experiment_id,
            item.attempt,
            item.state,
            item.priority,
            item.card_path,
            item.card_sha256,
            item.command_text,
            item.runner_name,
            item.git_commit,
            item.added_at,
            item.added_by,
            item.state_detail,
            item.preemptible,
            item.segment,
            item.resume_front,
        )

    def _item_view(self, item_id: int, *, project_id: int) -> V5OperatorItemView:
        item_key = _positive_integer(item_id, field_name="item_id")
        project_key = _positive_integer(project_id, field_name="project_id")
        for _attempt in range(3):
            with self._connection(operation="read project queue item", write=False) as connection:
                row = connection.execute(
                    """
                    SELECT q.*, p.project_key, r.revision_label
                    FROM queue_items AS q
                    JOIN projects AS p ON p.id = q.project_id
                    JOIN project_revisions AS r ON r.id = q.revision_id
                    WHERE q.id = ? AND q.project_id = ?
                    """,
                    (item_key, project_key),
                ).fetchone()
                if row is None:
                    raise V5OperatorNotFoundError(
                        f"Project id {project_key} has no queue item with global id "
                        f"{item_key}"
                    )
                dependencies = tuple(
                    int(value[0])
                    for value in connection.execute(
                        "SELECT dependency_item_id FROM dependencies "
                        "WHERE queue_item_id = ? ORDER BY dependency_item_id",
                        (item_key,),
                    )
                )
            trusted = self.projects.get_queue_item(item_key)
            if self._item_core_tuple(trusted) != self._item_core_tuple_from_row(row):
                continue
            return V5OperatorItemView(
                item=trusted,
                project_key=str(row["project_key"]),
                revision_label=str(row["revision_label"]),
                dependencies=dependencies,
                assigned_gpu_uuid=_optional_text(row, "assigned_gpu_uuid"),
                assigned_gpu_index=_optional_text(row, "assigned_gpu_index"),
                runtime_gpu_lease_held=bool(row["runtime_gpu_lease_held"]),
                runtime_gpu_lease_released_at=_optional_text(
                    row, "runtime_gpu_lease_released_at"
                ),
                pid=None if row["pid"] is None else int(row["pid"]),
                pgid=None if row["pgid"] is None else int(row["pgid"]),
                process_start_ticks=_optional_text(row, "proc_start_ticks"),
                started_at=_optional_text(row, "started_at"),
                finished_at=_optional_text(row, "finished_at"),
                return_code=(
                    None if row["return_code"] is None else int(row["return_code"])
                ),
                terminate_requested_at=_optional_text(row, "terminate_requested_at"),
                terminate_reason=_optional_text(row, "terminate_reason"),
                termination_stage=_optional_text(row, "termination_stage"),
                runner_run_dir=_optional_text(row, "runner_run_dir"),
                runner_manifest_path=_optional_text(row, "runner_manifest_path"),
                rsync_pull_command=_optional_text(row, "rsync_pull_command"),
                yield_request_id=_optional_text(row, "yield_request_id"),
                yield_requested_at=_optional_text(row, "yield_requested_at"),
                continuation_checkpoint=_optional_text(row, "continuation_checkpoint"),
                continuation_checkpoint_sha256=_optional_text(
                    row, "continuation_checkpoint_sha256"
                ),
                historical_git_ref=_optional_text(row, "git_ref"),
                historical_worktree_path=_optional_text(row, "worktree_path"),
                runtime_git_ref=_optional_text(row, "runtime_git_ref"),
                runtime_worktree_path=_optional_text(row, "runtime_worktree_path"),
                runtime_worktree_cleanup_error=_optional_text(
                    row, "runtime_worktree_cleanup_error"
                ),
            )
        raise V5OperatorError(
            f"queue item {item_key} changed repeatedly while being read; retry the "
            "operator command"
        )

    def get_item(self, item_id: int, *, project_id: int) -> V5OperatorItemView:
        """Show one global item only through its explicit owning Project."""

        return self._item_view(item_id, project_id=project_id)

    def list_items(
        self,
        *,
        project_id: int,
        states: tuple[str, ...] = (),
        after_id: int = 0,
        limit: int = 500,
    ) -> tuple[V5OperatorItemView, ...]:
        """List authenticated items within one Project and stable global-ID page."""

        project_key = _positive_integer(project_id, field_name="project_id")
        self.get_project_summary(project_id=project_key)
        after = _nonnegative_integer(after_id, field_name="after_id")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise V5OperatorError("limit must be an integer from 1 through 10000")
        if type(states) is not tuple or any(type(value) is not str for value in states):
            raise V5OperatorError("states must be a tuple of queue-state strings")
        unknown = sorted(set(states) - _QUEUE_STATES)
        if unknown:
            raise V5OperatorError(
                f"unknown queue states {unknown}; choose from {sorted(_QUEUE_STATES)}"
            )
        parameters: list[object] = [project_key, after]
        predicate = ""
        if states:
            placeholders = ", ".join("?" for _ in states)
            predicate = f" AND state IN ({placeholders})"
            parameters.extend(states)
        parameters.append(limit)
        with self._connection(operation="list project queue items", write=False) as connection:
            ids = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM queue_items WHERE project_id = ? AND id > ?"
                    f"{predicate} ORDER BY id LIMIT ?",
                    parameters,
                )
            )
        return tuple(
            self._item_view(item_id, project_id=project_key) for item_id in ids
        )

    def _mutate_item(
        self,
        item_id: int,
        *,
        project_id: int,
        target_state: str | None,
        allowed_states: frozenset[str],
        reason: str | None,
        priority: int | None,
        actor: str,
        changed_at: str,
        event_type: str,
    ) -> V5OperatorItemView:
        item_key = _positive_integer(item_id, field_name="item_id")
        project_key = _positive_integer(project_id, field_name="project_id")
        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        detail = None if reason is None else _text(reason, field_name="reason")
        trusted = self._item_view(item_key, project_id=project_key).item
        with self._connection(operation=f"mutate queue item {item_key}", write=True) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ? AND project_id = ?",
                (item_key, project_key),
            ).fetchone()
            if row is None:
                raise V5OperatorNotFoundError(
                    f"Project id {project_key} has no queue item with global id "
                    f"{item_key}"
                )
            if self._item_core_tuple_from_row(row) != self._item_core_tuple(trusted):
                raise V5OperatorError(
                    f"queue item {item_key} changed after authorization; retry the "
                    "operator command"
                )
            state = str(row["state"])
            if state not in allowed_states:
                raise V5OperatorError(
                    f"queue item {item_key} is {state!r}; {event_type} requires one "
                    f"of {sorted(allowed_states)}"
                )
            if target_state is not None:
                connection.execute(
                    "UPDATE queue_items SET state = ?, state_detail = ?, "
                    "finished_at = CASE WHEN ? = 'removed' THEN ? ELSE finished_at END "
                    "WHERE id = ? AND project_id = ?",
                    (
                        target_state,
                        detail,
                        target_state,
                        timestamp,
                        item_key,
                        project_key,
                    ),
                )
                payload: dict[str, object] = {
                    "oldState": state,
                    "newState": target_state,
                    "reason": detail,
                }
            else:
                assert priority is not None
                if int(row["priority"]) == priority:
                    raise V5OperatorError(
                        f"queue item {item_key} priority is already {priority}; "
                        "no audit event was written"
                    )
                connection.execute(
                    "UPDATE queue_items SET priority = ? WHERE id = ? AND project_id = ?",
                    (priority, item_key, project_key),
                )
                payload = {"oldPriority": int(row["priority"]), "newPriority": priority}
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type=event_type,
                project_id=project_key,
                queue_item_id=item_key,
                payload=payload,
            )
            if target_state == "removed":
                dependents = connection.execute(
                    """
                    SELECT child.id, child.project_id, child.state
                    FROM dependencies AS dependency
                    JOIN queue_items AS child ON child.id = dependency.queue_item_id
                    WHERE dependency.dependency_item_id = ?
                      AND child.state IN ('queued', 'blocked')
                    ORDER BY child.id
                    """,
                    (item_key,),
                ).fetchall()
                for dependent in dependents:
                    dependent_id = int(dependent["id"])
                    dependent_project = int(dependent["project_id"])
                    dependent_reason = (
                        f"dependency global queue item {item_key} was removed"
                    )
                    connection.execute(
                        "UPDATE queue_items SET state = 'held', state_detail = ? "
                        "WHERE id = ? AND project_id = ?",
                        (dependent_reason, dependent_id, dependent_project),
                    )
                    self._insert_event(
                        connection,
                        created_at=timestamp,
                        actor=event_actor,
                        event_type="queue_dependency_held",
                        project_id=dependent_project,
                        queue_item_id=dependent_id,
                        payload={
                            "dependencyQueueItemId": item_key,
                            "reason": dependent_reason,
                        },
                    )
        return self._item_view(item_key, project_id=project_key)

    def hold_item(
        self,
        item_id: int,
        *,
        project_id: int,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5OperatorItemView:
        """Move queued or dependency-blocked work to an explicit operator hold."""

        return self._mutate_item(
            item_id,
            project_id=project_id,
            target_state="held",
            allowed_states=frozenset({"queued", "blocked"}),
            reason=reason,
            priority=None,
            actor=actor,
            changed_at=changed_at,
            event_type="queue_item_held",
        )

    def release_item(
        self,
        item_id: int,
        *,
        project_id: int,
        actor: str,
        changed_at: str,
    ) -> V5OperatorItemView:
        """Return held or dependency-blocked work to explicit queue membership."""

        return self._mutate_item(
            item_id,
            project_id=project_id,
            target_state="queued",
            allowed_states=frozenset({"held", "blocked"}),
            reason=None,
            priority=None,
            actor=actor,
            changed_at=changed_at,
            event_type="queue_item_released",
        )

    def set_item_priority(
        self,
        item_id: int,
        *,
        project_id: int,
        priority: int,
        actor: str,
        changed_at: str,
    ) -> V5OperatorItemView:
        """Change global scheduling priority without authorizing preemption."""

        wanted = _priority(priority)
        return self._mutate_item(
            item_id,
            project_id=project_id,
            target_state=None,
            allowed_states=_PRIORITY_STATES,
            reason=None,
            priority=wanted,
            actor=actor,
            changed_at=changed_at,
            event_type="queue_item_priority_changed",
        )

    def remove_item(
        self,
        item_id: int,
        *,
        project_id: int,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5OperatorItemView:
        """Mark pending work removed while preserving all history and artifacts."""

        return self._mutate_item(
            item_id,
            project_id=project_id,
            target_state="removed",
            allowed_states=_PENDING_STATES,
            reason=reason,
            priority=None,
            actor=actor,
            changed_at=changed_at,
            event_type="queue_item_removed",
        )

    def list_gpus(self) -> tuple[V5GpuAllowlistEntry, ...]:
        """Return the exact allowlist and active global queue-item assignments."""

        with self._connection(operation="list GPU allowlist", write=False) as connection:
            rows = connection.execute(
                "SELECT * FROM gpu_allowlist ORDER BY last_index, uuid"
            ).fetchall()
            assigned: dict[str, list[int]] = {}
            for row in connection.execute(
                "SELECT id, assigned_gpu_uuid FROM queue_items "
                "WHERE runtime_gpu_lease_held = 1 "
                "AND assigned_gpu_uuid IS NOT NULL ORDER BY id"
            ):
                assigned.setdefault(str(row["assigned_gpu_uuid"]), []).append(
                    int(row["id"])
                )
        result: list[V5GpuAllowlistEntry] = []
        for row in rows:
            enabled = int(row["enabled"])
            draining = int(row["draining"])
            if enabled not in {0, 1} or draining not in {0, 1}:
                raise V5OperatorEvidenceError(
                    f"GPU {row['uuid']!r} has invalid boolean flags"
                )
            uuid = _text(row["uuid"], field_name="stored GPU uuid", maximum=256)
            result.append(
                V5GpuAllowlistEntry(
                    uuid=uuid,
                    requested_identifier=_text(
                        row["requested_identifier"],
                        field_name=f"GPU {uuid} requested identifier",
                        maximum=256,
                    ),
                    last_index=_text(
                        row["last_index"],
                        field_name=f"GPU {uuid} last index",
                        maximum=64,
                    ),
                    name=_text(
                        row["name"], field_name=f"GPU {uuid} name", maximum=256
                    ),
                    enabled=bool(enabled),
                    draining=bool(draining),
                    updated_at=_timestamp(
                        row["updated_at"], field_name=f"GPU {uuid} updated_at"
                    ),
                    assigned_queue_item_ids=tuple(assigned.get(uuid, ())),
                )
            )
        return tuple(result)

    def add_gpu(
        self,
        *,
        uuid: str,
        requested_identifier: str,
        last_index: str,
        name: str,
        actor: str,
        changed_at: str,
    ) -> V5GpuAllowlistEntry:
        """Add one fully identified observed GPU as enabled and undrained."""

        gpu_uuid = _text(uuid, field_name="GPU uuid", maximum=256)
        requested = _text(
            requested_identifier, field_name="requested GPU identifier", maximum=256
        )
        index = _text(last_index, field_name="GPU index", maximum=64)
        gpu_name = _text(name, field_name="GPU name", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        event_actor = _text(actor, field_name="actor", maximum=256)
        with self._connection(operation=f"add GPU {gpu_uuid}", write=True) as connection:
            if connection.execute(
                "SELECT 1 FROM gpu_allowlist WHERE uuid = ?", (gpu_uuid,)
            ).fetchone() is not None:
                raise V5OperatorError(
                    f"GPU {gpu_uuid!r} is already in the allowlist; use enable or "
                    "undrain instead of replacing its identity"
                )
            connection.execute(
                """
                INSERT INTO gpu_allowlist(
                    uuid, requested_identifier, last_index, name,
                    enabled, draining, updated_at
                ) VALUES (?, ?, ?, ?, 1, 0, ?)
                """,
                (gpu_uuid, requested, index, gpu_name, timestamp),
            )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="gpu_allowlist_added",
                payload={
                    "gpuUuid": gpu_uuid,
                    "requestedIdentifier": requested,
                    "lastIndex": index,
                    "name": gpu_name,
                },
            )
        return next(entry for entry in self.list_gpus() if entry.uuid == gpu_uuid)

    def _transition_gpu(
        self,
        uuid: str,
        *,
        field_name: str,
        current: bool,
        target: bool,
        actor: str,
        changed_at: str,
        event_type: str,
    ) -> V5GpuAllowlistEntry:
        gpu_uuid = _text(uuid, field_name="GPU uuid", maximum=256)
        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        if field_name not in {"enabled", "draining"}:
            raise AssertionError("internal GPU transition field is invalid")
        with self._connection(operation=f"{event_type} GPU {gpu_uuid}", write=True) as connection:
            row = connection.execute(
                "SELECT enabled, draining FROM gpu_allowlist WHERE uuid = ?",
                (gpu_uuid,),
            ).fetchone()
            if row is None:
                raise V5OperatorNotFoundError(
                    f"GPU {gpu_uuid!r} is not in the allowlist; add the full "
                    "observed UUID first"
                )
            actual = bool(row[field_name])
            if actual is not current:
                state = "enabled" if actual and field_name == "enabled" else (
                    "disabled" if field_name == "enabled" else (
                        "draining" if actual else "undrained"
                    )
                )
                raise V5OperatorError(
                    f"GPU {gpu_uuid!r} is already {state}; no state changed"
                )
            connection.execute(
                f"UPDATE gpu_allowlist SET {field_name} = ?, updated_at = ? "
                "WHERE uuid = ?",
                (int(target), timestamp, gpu_uuid),
            )
            self._insert_event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type=event_type,
                payload={"gpuUuid": gpu_uuid, field_name: target},
            )
        return next(entry for entry in self.list_gpus() if entry.uuid == gpu_uuid)

    def enable_gpu(self, uuid: str, *, actor: str, changed_at: str) -> V5GpuAllowlistEntry:
        """Enable future dispatch; a separately draining GPU remains draining."""

        return self._transition_gpu(
            uuid,
            field_name="enabled",
            current=False,
            target=True,
            actor=actor,
            changed_at=changed_at,
            event_type="gpu_allowlist_enabled",
        )

    def disable_gpu(self, uuid: str, *, actor: str, changed_at: str) -> V5GpuAllowlistEntry:
        """Disable future dispatch without interrupting an assigned item."""

        return self._transition_gpu(
            uuid,
            field_name="enabled",
            current=True,
            target=False,
            actor=actor,
            changed_at=changed_at,
            event_type="gpu_allowlist_disabled",
        )

    def drain_gpu(self, uuid: str, *, actor: str, changed_at: str) -> V5GpuAllowlistEntry:
        """Block new dispatch while allowing an assigned item to finish."""

        return self._transition_gpu(
            uuid,
            field_name="draining",
            current=False,
            target=True,
            actor=actor,
            changed_at=changed_at,
            event_type="gpu_allowlist_draining",
        )

    def undrain_gpu(self, uuid: str, *, actor: str, changed_at: str) -> V5GpuAllowlistEntry:
        """Clear drain state; disabled GPUs remain disabled."""

        return self._transition_gpu(
            uuid,
            field_name="draining",
            current=True,
            target=False,
            actor=actor,
            changed_at=changed_at,
            event_type="gpu_allowlist_undrained",
        )

    def list_artifacts(
        self,
        *,
        project_id: int,
        queue_item_id: int | None = None,
        after_id: int = 0,
        limit: int = 500,
    ) -> tuple[V5ArtifactRecord, ...]:
        """List immutable artifact rows after rechecking Project path ownership."""

        project_key = _positive_integer(project_id, field_name="project_id")
        after = _nonnegative_integer(after_id, field_name="after_id")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise V5OperatorError("limit must be an integer from 1 through 10000")
        item_key = (
            None
            if queue_item_id is None
            else _positive_integer(queue_item_id, field_name="queue_item_id")
        )
        if item_key is not None:
            self._item_view(item_key, project_id=project_key)
        else:
            self.get_project_summary(project_id=project_key)
        parameters: list[object] = [project_key, after]
        predicate = ""
        if item_key is not None:
            predicate = " AND queue_item_id = ?"
            parameters.append(item_key)
        parameters.append(limit)
        with self._connection(operation="list project artifacts", write=False) as connection:
            rows = connection.execute(
                "SELECT * FROM job_artifacts WHERE project_id = ? AND id > ?"
                f"{predicate} ORDER BY id LIMIT ?",
                parameters,
            ).fetchall()
        revisions: dict[int, ProjectRevision] = {}
        records: list[V5ArtifactRecord] = []
        for row in rows:
            artifact_id = int(row["id"])
            row_item_id = int(row["queue_item_id"])
            item = self._item_view(row_item_id, project_id=project_key).item
            revision_id = int(row["revision_id"])
            if item.revision_id != revision_id:
                raise V5OperatorEvidenceError(
                    f"artifact id {artifact_id} revision differs from queue item "
                    f"{row_item_id}"
                )
            evidence_kind = str(row["evidence_kind"])
            absolute = Path(str(row["absolute_path"]))
            root_name = _optional_text(row, "root_name")
            relative = _optional_text(row, "relative_path")
            if evidence_kind == "declared-v1":
                revision = revisions.get(revision_id)
                if revision is None:
                    revision = self.projects.get_revision(revision_id)
                    revisions[revision_id] = revision
                if root_name is None or relative is None or row["root_access"] != "readWrite":
                    raise V5OperatorEvidenceError(
                        f"declared artifact id {artifact_id} lacks root authorization"
                    )
                try:
                    expected = resolve_artifact_path(
                        revision.enrollment.artifact_root(root_name).path,
                        relative,
                        field_name=f"artifact id {artifact_id}",
                    )
                except (ExecutionValidationError, KeyError) as exc:
                    raise V5OperatorEvidenceError(
                        f"artifact id {artifact_id} path authorization failed: {exc}"
                    ) from exc
                if absolute != expected:
                    raise V5OperatorEvidenceError(
                        f"artifact id {artifact_id} absolute path {absolute} differs "
                        f"from authorized path {expected}"
                    )
            elif evidence_kind == "legacy-v4":
                if not absolute.is_absolute() or ".." in absolute.parts:
                    raise V5OperatorEvidenceError(
                        f"legacy artifact id {artifact_id} has non-absolute or "
                        "traversing historical path"
                    )
            else:  # protected by schema, retained for corrupt evidence messages
                raise V5OperatorEvidenceError(
                    f"artifact id {artifact_id} has unknown evidence kind "
                    f"{evidence_kind!r}"
                )
            records.append(
                V5ArtifactRecord(
                    id=artifact_id,
                    queue_item_id=row_item_id,
                    project_id=project_key,
                    revision_id=revision_id,
                    segment=int(row["segment"]),
                    evidence_kind=evidence_kind,
                    artifact_name=str(row["artifact_name"]),
                    artifact_type=str(row["artifact_type"]),
                    root_name=root_name,
                    relative_path=relative,
                    absolute_path=absolute,
                    size_bytes=(
                        None if row["size_bytes"] is None else int(row["size_bytes"])
                    ),
                    sha256=_optional_text(row, "sha256"),
                    recorded_at=_timestamp(
                        row["recorded_at"],
                        field_name=f"artifact id {artifact_id} recorded_at",
                    ),
                    _metadata_json=_canonical_metadata(
                        row["metadata_json"], artifact_id=artifact_id
                    ),
                )
            )
        return tuple(records)

    def list_yield_receipts(
        self,
        *,
        project_id: int,
        queue_item_id: int | None = None,
    ) -> tuple[V5YieldReceiptRecord, ...]:
        """List exact authenticated typed yield receipts within one Project."""

        project_key = _positive_integer(project_id, field_name="project_id")
        item_key = (
            None
            if queue_item_id is None
            else _positive_integer(queue_item_id, field_name="queue_item_id")
        )
        if item_key is not None:
            self._item_view(item_key, project_id=project_key)
        else:
            self.get_project_summary(project_id=project_key)
        parameters: list[object] = [project_key]
        predicate = ""
        if item_key is not None:
            predicate = " AND queue_item_id = ?"
            parameters.append(item_key)
        with self._connection(operation="list cooperative-yield receipts", write=False) as connection:
            request_ids = tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT request_id FROM cooperative_yield_receipts "
                    "WHERE project_id = ?" + predicate + " ORDER BY queue_item_id, segment",
                    parameters,
                )
            )
        records = tuple(self.projects.get_yield_receipt(value) for value in request_ids)
        if any(record.project_id != project_key for record in records):
            raise V5OperatorEvidenceError(
                "cooperative-yield receipt escaped requested Project ownership"
            )
        return records

    def list_yield_requests(
        self,
        *,
        project_id: int,
        queue_item_id: int | None = None,
    ) -> tuple[V5YieldRequestRecord, ...]:
        """List exact authenticated typed yield requests within one Project."""

        project_key = _positive_integer(project_id, field_name="project_id")
        item_key = (
            None
            if queue_item_id is None
            else _positive_integer(queue_item_id, field_name="queue_item_id")
        )
        if item_key is not None:
            self._item_view(item_key, project_id=project_key)
        else:
            self.get_project_summary(project_id=project_key)
        parameters: list[object] = [project_key]
        predicate = ""
        if item_key is not None:
            predicate = " AND queue_item_id = ?"
            parameters.append(item_key)
        with self._connection(
            operation="list cooperative-yield requests",
            write=False,
        ) as connection:
            request_ids = tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT request_id FROM cooperative_yield_requests "
                    "WHERE project_id = ?" + predicate
                    + " ORDER BY queue_item_id, segment",
                    parameters,
                )
            )
        records = tuple(self.projects.get_yield_request(value) for value in request_ids)
        if any(record.project_id != project_key for record in records):
            raise V5OperatorEvidenceError(
                "cooperative-yield request escaped requested Project ownership"
            )
        return records

    def list_events(
        self,
        *,
        project_id: int,
        after_id: int = 0,
        limit: int = 500,
    ) -> tuple[V5Event, ...]:
        """List canonical append-only events within one authorized Project."""

        project_key = _positive_integer(project_id, field_name="project_id")
        self.get_project_summary(project_id=project_key)
        return self.projects.list_events(
            project_id=project_key,
            after_id=_nonnegative_integer(after_id, field_name="after_id"),
            limit=limit,
        )

    def list_revisions(self, *, project_id: int) -> tuple[V5RevisionSummary, ...]:
        """List typed revisions and rehashed imported enrollment evidence."""

        project_key = _positive_integer(project_id, field_name="project_id")
        summary = self.get_project_summary(project_id=project_key)
        with self._connection(operation="list Project revisions", write=False) as connection:
            rows = connection.execute(
                "SELECT * FROM project_revisions WHERE project_id = ? ORDER BY sequence",
                (project_key,),
            ).fetchall()
        revisions: list[V5RevisionSummary] = []
        for row in rows:
            revision_id = int(row["id"])
            sequence = int(row["sequence"])
            kind = str(row["revision_kind"])
            label = str(row["revision_label"])
            expected_label = (
                f"{summary.key}:r{sequence}"
                if kind == "project-v1"
                else f"{summary.key}:legacy-r{sequence}"
            )
            if label != expected_label:
                raise V5OperatorEvidenceError(
                    f"revision id {revision_id} label {label!r} differs from "
                    f"expected {expected_label!r}"
                )
            enrollment = row["enrollment_json"]
            if type(enrollment) is not bytes:
                raise V5OperatorEvidenceError(
                    f"revision id {revision_id} Enrollment is not stored as bytes"
                )
            enrollment_source = cast(bytes, enrollment)
            digest = sha256_bytes(enrollment_source)
            if digest != row["enrollment_sha256"]:
                raise V5OperatorEvidenceError(
                    f"revision id {revision_id} Enrollment SHA-256 does not match "
                    "its exact stored bytes"
                )
            typed_revision: ProjectRevision | None = None
            git_evidence: V5RevisionGitEvidence | None = None
            if kind == "project-v1":
                typed_revision = self.projects.get_revision(revision_id)
                git_evidence = self.projects.get_revision_git_evidence(revision_id)
            elif kind == "legacy-v4":
                try:
                    document = cast(JSONValue, json.loads(enrollment_source))
                    canonical = canonical_json_bytes(document)
                except (json.JSONDecodeError, UnicodeError, TypeError, ValueError) as exc:
                    raise V5OperatorEvidenceError(
                        f"legacy revision id {revision_id} Enrollment is invalid: {exc}"
                    ) from exc
                if canonical != enrollment_source or type(document) is not dict:
                    raise V5OperatorEvidenceError(
                        f"legacy revision id {revision_id} Enrollment is not an "
                        "exact canonical JSON object"
                    )
                legacy = cast(dict[str, JSONValue], document)
                if (
                    legacy.get("kind") != "LegacyEnrollment"
                    or legacy.get("projectKey") != summary.key
                    or legacy.get("checkoutDirectory") != str(row["checkout_path"])
                    or legacy.get("gitCommit") != row["git_commit"]
                ):
                    raise V5OperatorEvidenceError(
                        f"legacy revision id {revision_id} Enrollment identity "
                        "differs from its immutable revision row"
                    )
            else:
                raise V5OperatorEvidenceError(
                    f"revision id {revision_id} has unknown kind {kind!r}"
                )
            revisions.append(
                V5RevisionSummary(
                    id=revision_id,
                    project_id=project_key,
                    sequence=sequence,
                    label=label,
                    kind=kind,
                    display_name=str(row["display_name"]),
                    git_commit=_optional_text(row, "git_commit"),
                    checkout_path=Path(str(row["checkout_path"])),
                    created_at=_timestamp(
                        row["created_at"],
                        field_name=f"revision id {revision_id} created_at",
                    ),
                    created_actor=_text(
                        row["created_actor"],
                        field_name=f"revision id {revision_id} created_actor",
                        maximum=256,
                    ),
                    enrollment_sha256=digest,
                    enrollment_source=enrollment_source,
                    typed_revision=typed_revision,
                    git_evidence=git_evidence,
                )
            )
        return tuple(revisions)

    def project_export(self, project_id: int) -> V5ProjectExport:
        """Build a complete authenticated Project diagnostic/export snapshot.

        Exact ExecutorReceipt wire bytes are intentionally absent because v5
        currently persists their terminal outcome, not their source document.
        This view must not be labeled or consumed as an ExecutorReceipt protocol.
        """

        project_key = _positive_integer(project_id, field_name="project_id")
        with self._connection(
            operation="export bounded Project evidence snapshot", write=False
        ) as connection:
            # The first SELECT fixes SQLite's read snapshot. Counts and exact-source
            # byte totals are checked before any unbounded record set is fetched.
            project_row = connection.execute(
                """
                SELECT p.*, r.revision_label, r.revision_kind, r.git_commit,
                       runtime.health, runtime.circuit_failure_count,
                       runtime.health_reason, runtime.health_actor,
                       runtime.health_changed_at
                FROM projects AS p
                JOIN project_revisions AS r
                  ON r.id = p.current_revision_id AND r.project_id = p.id
                JOIN project_runtime_state AS runtime ON runtime.project_id = p.id
                WHERE p.id = ?
                """,
                (project_key,),
            ).fetchone()
            if project_row is None:
                raise V5OperatorNotFoundError(
                    f"schema-v5 has no registered Project with id {project_key}"
                )
            count_row = connection.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM project_revisions WHERE project_id = ?) revisions,
                  (SELECT COUNT(*) FROM queue_items WHERE project_id = ?) items,
                  (SELECT COUNT(*) FROM dependencies d JOIN queue_items q ON q.id=d.queue_item_id WHERE q.project_id = ?) dependencies,
                  (SELECT COUNT(*) FROM events WHERE project_id = ?) events,
                  (SELECT COUNT(*) FROM job_artifacts WHERE project_id = ?) artifacts,
                  (SELECT COUNT(*) FROM cooperative_yield_requests WHERE project_id = ?) requests,
                  (SELECT COUNT(*) FROM cooperative_yield_receipts WHERE project_id = ?) receipts,
                  (SELECT COUNT(*) FROM admission_snapshots WHERE project_id = ?) snapshots
                """,
                (project_key,) * 8,
            ).fetchone()
            assert count_row is not None
            total_records = 2 + sum(int(count_row[name]) for name in count_row.keys())
            if total_records > MAX_QUEUE_EXPORT_TOTAL_RECORDS:
                raise V5OperatorEvidenceError(
                    f"Project id {project_key} export has {total_records} records; "
                    f"limit is {MAX_QUEUE_EXPORT_TOTAL_RECORDS}"
                )
            byte_row = connection.execute(
                """
                SELECT
                  COALESCE((SELECT SUM(length(enrollment_json)+COALESCE(length(project_source),0)+COALESCE(length(project_normalized_json),0)+COALESCE(length(extension_schema_source),0)+COALESCE(length(extension_schema_canonical_json),0)) FROM project_revisions WHERE project_id = ?),0)
                + COALESCE((SELECT SUM(length(project_source)+length(project_normalized_json)+length(card_source)+length(card_normalized_json)+COALESCE(length(extension_schema_source),0)+COALESCE(length(extension_schema_canonical_json),0)+length(resolved_json)+length(command_json)+length(policy_bindings_json)+length(policy_dependencies_json)+length(policy_json)) FROM admission_snapshots WHERE project_id = ?),0)
                + COALESCE((SELECT SUM(length(payload_json)) FROM events WHERE project_id = ?),0)
                + COALESCE((SELECT SUM(COALESCE(length(metadata_json),0)) FROM job_artifacts WHERE project_id = ?),0)
                + COALESCE((SELECT SUM(length(request_json)) FROM cooperative_yield_requests WHERE project_id = ?),0)
                + COALESCE((SELECT SUM(length(receipt_json)) FROM cooperative_yield_receipts WHERE project_id = ?),0)
                AS exact_bytes
                """,
                (project_key,) * 6,
            ).fetchone()
            assert byte_row is not None
            exact_bytes = int(byte_row["exact_bytes"])
            if exact_bytes > MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES:
                raise V5OperatorEvidenceError(
                    f"Project id {project_key} export has {exact_bytes} exact source "
                    f"bytes; limit is {MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES}"
                )

            project = self._summary_from_row(connection, project_row)
            if project.current_revision_kind == "project-v1":
                typed = self.projects._project_view_from_row(connection, project_row)
                project = replace(project, typed_view=typed)

            revision_rows = connection.execute(
                "SELECT * FROM project_revisions WHERE project_id = ? ORDER BY sequence",
                (project_key,),
            ).fetchall()
            revisions: list[V5RevisionSummary] = []
            typed_revisions: dict[int, ProjectRevision] = {}
            for row in revision_rows:
                revision_id = int(row["id"])
                sequence = int(row["sequence"])
                kind = str(row["revision_kind"])
                label = str(row["revision_label"])
                expected = (
                    f"{project.key}:r{sequence}"
                    if kind == "project-v1"
                    else f"{project.key}:legacy-r{sequence}"
                )
                if label != expected:
                    raise V5OperatorEvidenceError(
                        f"revision id {revision_id} label {label!r} differs from {expected!r}"
                    )
                enrollment = row["enrollment_json"]
                if type(enrollment) is not bytes:
                    raise V5OperatorEvidenceError(
                        f"revision id {revision_id} Enrollment is not stored as bytes"
                    )
                enrollment_source = cast(bytes, enrollment)
                digest = sha256_bytes(enrollment_source)
                if digest != row["enrollment_sha256"]:
                    raise V5OperatorEvidenceError(
                        f"revision id {revision_id} Enrollment digest differs from source"
                    )
                try:
                    enrollment_document = json.loads(enrollment_source)
                    if canonical_json_bytes(cast(JSONValue, enrollment_document)) != enrollment_source:
                        raise ValueError("not canonical")
                except (json.JSONDecodeError, UnicodeError, TypeError, ValueError) as exc:
                    raise V5OperatorEvidenceError(
                        f"revision id {revision_id} Enrollment is invalid canonical JSON: {exc}"
                    ) from exc
                typed_revision = None
                git_evidence = None
                if kind == "project-v1":
                    typed_revision = self.projects._load_revision(connection, revision_id)
                    typed_revisions[revision_id] = typed_revision
                    git_evidence = self.projects._revision_git_evidence_from_row(
                        row, typed_revision
                    )
                elif kind != "legacy-v4":
                    raise V5OperatorEvidenceError(
                        f"revision id {revision_id} has unknown kind {kind!r}"
                    )
                elif not isinstance(enrollment_document, dict) or (
                    enrollment_document.get("kind") != "LegacyEnrollment"
                    or enrollment_document.get("projectKey") != project.key
                    or enrollment_document.get("checkoutDirectory") != str(row["checkout_path"])
                    or enrollment_document.get("gitCommit") != row["git_commit"]
                ):
                    raise V5OperatorEvidenceError(
                        f"legacy revision id {revision_id} Enrollment identity differs"
                    )
                revisions.append(V5RevisionSummary(
                    id=revision_id, project_id=project_key, sequence=sequence,
                    label=label, kind=kind, display_name=str(row["display_name"]),
                    git_commit=_optional_text(row, "git_commit"),
                    checkout_path=Path(str(row["checkout_path"])),
                    created_at=_timestamp(row["created_at"], field_name=f"revision id {revision_id} created_at"),
                    created_actor=_text(row["created_actor"], field_name=f"revision id {revision_id} created_actor", maximum=256),
                    enrollment_sha256=digest, enrollment_source=enrollment_source,
                    typed_revision=typed_revision, git_evidence=git_evidence,
                ))

            dependency_rows = connection.execute(
                """
                SELECT d.queue_item_id, target.id item_id, target.project_id,
                       p.project_key, target.revision_id, r.revision_label, target.state
                FROM dependencies d
                JOIN queue_items owner ON owner.id=d.queue_item_id
                JOIN queue_items target ON target.id=d.dependency_item_id
                JOIN projects p ON p.id=target.project_id
                JOIN project_revisions r ON r.id=target.revision_id
                WHERE owner.project_id=? ORDER BY d.queue_item_id,target.id
                """, (project_key,)
            ).fetchall()
            targets: dict[int, list[V5DependencyTarget]] = {}
            for row in dependency_rows:
                target = V5DependencyTarget(
                    item_id=int(row["item_id"]), project_id=int(row["project_id"]),
                    project_key=str(row["project_key"]), revision_id=int(row["revision_id"]),
                    revision_label=str(row["revision_label"]), state=str(row["state"]),
                )
                targets.setdefault(int(row["queue_item_id"]), []).append(target)

            item_rows = connection.execute(
                """SELECT q.*, p.project_key, r.revision_label FROM queue_items q
                   JOIN projects p ON p.id=q.project_id
                   JOIN project_revisions r ON r.id=q.revision_id
                   WHERE q.project_id=? ORDER BY q.id""", (project_key,)
            ).fetchall()
            items: list[V5OperatorItemView] = []
            runtime_names = (
                "assigned_gpu_uuid", "assigned_gpu_index", "runtime_gpu_lease_held",
                "runtime_gpu_lease_released_at", "pid", "pgid", "proc_start_ticks",
                "started_at", "finished_at", "return_code", "terminate_requested_at",
                "terminate_reason", "termination_stage", "termination_signal_epoch",
                "contention_detected", "repo_drift_detected", "runner_run_dir",
                "runner_manifest_path", "rsync_pull_command", "yield_requested_at",
                "yield_requested_by", "yield_request_id", "yield_note", "yield_duration_hours",
                "continuation_checkpoint", "continuation_checkpoint_sha256",
                "continuation_checkpoint_metadata", "continuation_checkpoint_metadata_sha256",
                "continuation_step", "continuation_wandb_id", "git_ref", "worktree_path",
                "worktree_created_at", "worktree_removed_at", "worktree_cleanup_error",
                "runtime_git_ref", "runtime_worktree_path", "runtime_worktree_created_at",
                "runtime_worktree_removed_at", "runtime_worktree_cleanup_error",
            )
            for row in item_rows:
                trusted = self.projects._queue_item_from_row(connection, row)
                item_targets = tuple(targets.get(trusted.id, ()))
                runtime: dict[str, JSONValue] = {}
                for name in runtime_names:
                    value = row[name]
                    runtime[name] = (
                        bool(value) if name in {
                            "contention_detected",
                            "repo_drift_detected",
                            "runtime_gpu_lease_held",
                        }
                        else cast(JSONValue, value)
                    )
                items.append(V5OperatorItemView(
                    item=trusted, project_key=str(row["project_key"]),
                    revision_label=str(row["revision_label"]),
                    dependencies=tuple(value.item_id for value in item_targets),
                    assigned_gpu_uuid=_optional_text(row, "assigned_gpu_uuid"),
                    assigned_gpu_index=_optional_text(row, "assigned_gpu_index"),
                    runtime_gpu_lease_held=bool(row["runtime_gpu_lease_held"]),
                    runtime_gpu_lease_released_at=_optional_text(
                        row, "runtime_gpu_lease_released_at"
                    ),
                    pid=None if row["pid"] is None else int(row["pid"]),
                    pgid=None if row["pgid"] is None else int(row["pgid"]),
                    process_start_ticks=_optional_text(row, "proc_start_ticks"),
                    started_at=_optional_text(row, "started_at"), finished_at=_optional_text(row, "finished_at"),
                    return_code=None if row["return_code"] is None else int(row["return_code"]),
                    terminate_requested_at=_optional_text(row, "terminate_requested_at"),
                    terminate_reason=_optional_text(row, "terminate_reason"),
                    termination_stage=_optional_text(row, "termination_stage"),
                    runner_run_dir=_optional_text(row, "runner_run_dir"),
                    runner_manifest_path=_optional_text(row, "runner_manifest_path"),
                    rsync_pull_command=_optional_text(row, "rsync_pull_command"),
                    yield_request_id=_optional_text(row, "yield_request_id"),
                    yield_requested_at=_optional_text(row, "yield_requested_at"),
                    continuation_checkpoint=_optional_text(row, "continuation_checkpoint"),
                    continuation_checkpoint_sha256=_optional_text(row, "continuation_checkpoint_sha256"),
                    historical_git_ref=_optional_text(row, "git_ref"),
                    historical_worktree_path=_optional_text(row, "worktree_path"),
                    runtime_git_ref=_optional_text(row, "runtime_git_ref"),
                    runtime_worktree_path=_optional_text(row, "runtime_worktree_path"),
                    runtime_worktree_cleanup_error=_optional_text(row, "runtime_worktree_cleanup_error"),
                    dependency_targets=item_targets, persisted_runtime=runtime,
                ))

            event_rows = connection.execute(
                "SELECT * FROM events WHERE project_id=? ORDER BY id", (project_key,)
            ).fetchall()
            events = tuple(self.projects._event_from_row(row) for row in event_rows)
            host_row = connection.execute(
                """SELECT * FROM events WHERE scope='host'
                   AND event_type IN ('HOST_DISPATCH_PAUSED','HOST_DISPATCH_RESUMED')
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone()
            host_event = None if host_row is None else self.projects._event_from_row(host_row)
            if host_event is not None:
                expected_type = "HOST_DISPATCH_PAUSED" if project.host_dispatch_paused else "HOST_DISPATCH_RESUMED"
                if host_event.event_type != expected_type or (
                    project.host_dispatch_paused and host_event.payload.get("reason") != project.host_pause_reason
                ):
                    raise V5OperatorEvidenceError("host gate metadata differs from latest host event")

            artifact_rows = connection.execute(
                "SELECT * FROM job_artifacts WHERE project_id=? ORDER BY id", (project_key,)
            ).fetchall()
            item_by_id = {view.item.id: view.item for view in items}
            artifacts: list[V5ArtifactRecord] = []
            for row in artifact_rows:
                artifact_id = int(row["id"]); item_id = int(row["queue_item_id"])
                revision_id = int(row["revision_id"])
                if item_by_id[item_id].revision_id != revision_id:
                    raise V5OperatorEvidenceError(f"artifact id {artifact_id} revision differs from item")
                kind = str(row["evidence_kind"]); absolute = Path(str(row["absolute_path"]))
                root = _optional_text(row, "root_name"); relative = _optional_text(row, "relative_path")
                if kind == "declared-v1":
                    revision = typed_revisions[revision_id]
                    if root is None or relative is None or row["root_access"] != "readWrite":
                        raise V5OperatorEvidenceError(f"declared artifact id {artifact_id} lacks root authorization")
                    try:
                        expected_path = resolve_artifact_path(revision.enrollment.artifact_root(root).path, relative, field_name=f"artifact id {artifact_id}")
                    except (ExecutionValidationError, KeyError) as exc:
                        raise V5OperatorEvidenceError(f"artifact id {artifact_id} path authorization failed: {exc}") from exc
                    if absolute != expected_path:
                        raise V5OperatorEvidenceError(f"artifact id {artifact_id} absolute path differs from authorization")
                elif kind != "legacy-v4" or not absolute.is_absolute() or ".." in absolute.parts:
                    raise V5OperatorEvidenceError(f"artifact id {artifact_id} evidence/path is invalid")
                artifacts.append(V5ArtifactRecord(
                    id=artifact_id, queue_item_id=item_id, project_id=project_key,
                    revision_id=revision_id, segment=int(row["segment"]), evidence_kind=kind,
                    artifact_name=str(row["artifact_name"]), artifact_type=str(row["artifact_type"]),
                    root_name=root, relative_path=relative, absolute_path=absolute,
                    size_bytes=None if row["size_bytes"] is None else int(row["size_bytes"]),
                    sha256=_optional_text(row, "sha256"),
                    recorded_at=_timestamp(row["recorded_at"], field_name=f"artifact id {artifact_id} recorded_at"),
                    _metadata_json=_canonical_metadata(row["metadata_json"], artifact_id=artifact_id),
                ))
            request_rows = connection.execute(
                "SELECT * FROM cooperative_yield_requests WHERE project_id=? ORDER BY queue_item_id,segment", (project_key,)
            ).fetchall()
            requests = tuple(self.projects._yield_request_from_row(row) for row in request_rows)
            receipt_rows = connection.execute(
                "SELECT * FROM cooperative_yield_receipts WHERE project_id=? ORDER BY queue_item_id,segment", (project_key,)
            ).fetchall()
            receipts = tuple(self.projects._yield_receipt_from_row(connection, row) for row in receipt_rows)
            return V5ProjectExport(
                project=project, revisions=tuple(revisions), items=tuple(items),
                events=events, artifacts=tuple(artifacts), yield_requests=requests,
                yield_receipts=receipts,
                host_state=V5HostState(project.host_dispatch_paused, project.host_pause_reason, host_event),
            )


__all__ = [
    "V5ArtifactRecord",
    "V5DependencyTarget",
    "V5GpuAllowlistEntry",
    "V5OperatorError",
    "V5OperatorEvidenceError",
    "V5OperatorItemView",
    "V5OperatorNotFoundError",
    "V5OperatorRepository",
    "V5ProjectExport",
    "V5ProjectSummary",
    "V5ProjectStatus",
    "V5RevisionSummary",
    "V5HostState",
]
