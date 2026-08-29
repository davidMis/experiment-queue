"""Project-aware schema-v5 dispatch state and failure-isolation primitives."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import json
from pathlib import Path
import shutil
import sqlite3
from typing import Iterator, Mapping

from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.execution import ObservedArtifact
from experiment_queue.executor import ExecutorReceipt
from experiment_queue.project_worktrees import ProjectWorktreeEvidence
from experiment_queue.serialization import canonical_json_bytes


_TERMINAL_NON_SUCCESS = frozenset(
    {"failed", "interrupted", "force_killed", "removed"}
)
_ACTIVE_STATES = frozenset(
    {"starting", "running", "yielding", "terminating", "force_killing"}
)


class V5SchedulerError(RuntimeError):
    """Raised when project-aware scheduling state cannot change safely."""


class FailureScope(StrEnum):
    """The isolation boundary affected by an operational failure."""

    HOST = "host"
    PROJECT = "project"


@dataclass(frozen=True, slots=True)
class V5DispatchCandidate:
    """One globally ordered item whose Project and dependencies allow dispatch."""

    id: int
    project_id: int
    project_key: str
    revision_id: int
    revision_label: str
    revision_kind: str
    admission_kind: str
    snapshot_id: int | None
    experiment_id: str
    attempt: int
    priority: int
    resume_front: bool
    segment: int
    git_commit: str


@dataclass(frozen=True, slots=True)
class V5ActiveAttempt:
    """Persistent active-process identity used for restart reconciliation."""

    id: int
    project_id: int
    project_key: str
    revision_id: int
    revision_label: str
    admission_kind: str
    snapshot_id: int | None
    experiment_id: str
    attempt: int
    state: str
    segment: int
    git_commit: str
    assigned_gpu_uuid: str | None
    assigned_gpu_index: str | None
    pid: int | None
    pgid: int | None
    process_start_ticks: str | None
    started_at: str | None
    terminate_requested_at: str | None
    terminate_reason: str | None
    termination_stage: str | None
    termination_signal_epoch: float | None


@dataclass(frozen=True, slots=True)
class V5TerminationAction:
    """One committed termination stage plus exact process identity to signal.

    The database transition always commits before the caller may signal.  PID,
    process-group, and Linux start-tick evidence are captured from that same
    transaction so a separate operator process can authenticate the target and
    fail closed if the PID was reused.
    """

    item_id: int
    project_id: int
    segment: int
    state: str
    stage: str
    requested_at: str
    reason: str
    signal_epoch: float
    pid: int | None
    pgid: int | None
    process_start_ticks: str | None


@dataclass(frozen=True, slots=True)
class V5AbandonedLaunchResolution:
    """Auditable terminal transition for one operator-proven abandoned attempt."""

    item_id: int
    project_id: int
    gpu_uuid: str
    previous_state: str
    event_type: str
    state: str
    reason: str
    resolved_at: str


@dataclass(frozen=True, slots=True)
class V5ManualYieldSignalClaim:
    """One durable exclusive lease to attempt a manual-yield SIGINT."""

    item_id: int
    project_id: int
    request_id: str
    attempt_token: str
    attempt: int
    replay: bool
    signal_epoch: float


@dataclass(frozen=True, slots=True)
class DiskCapacity:
    """Free-space evidence for one scheduler or Project filesystem root."""

    scope: FailureScope
    root: Path
    free_gib: float
    minimum_gib: float
    project_id: int | None = None
    project_key: str | None = None

    @property
    def sufficient(self) -> bool:
        """Return whether this root satisfies the configured dispatch floor."""

        return self.free_gib >= self.minimum_gib


def _timestamp(value: str, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5SchedulerError(f"{field_name} must be non-empty timestamp text")
    try:
        parsed = datetime.fromisoformat(
            f"{value[:-1]}+00:00" if value.endswith("Z") else value
        )
    except ValueError as exc:
        raise V5SchedulerError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V5SchedulerError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        )
    return value


def _text(value: str, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5SchedulerError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise V5SchedulerError(
            f"{field_name} must be log-safe text of at most {maximum} characters"
        )
    return value


def _positive_integer(value: int, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise V5SchedulerError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_float(value: float, *, field_name: str) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise V5SchedulerError(f"{field_name} must be a nonnegative number")
    converted = float(value)
    if converted < 0.0 or converted != converted or converted == float("inf"):
        raise V5SchedulerError(f"{field_name} must be a finite nonnegative number")
    return converted


def _optional_positive_integer(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise V5SchedulerError(f"{field_name} must be a positive integer or null")
    return value


def _payload_json(payload: Mapping[str, object]) -> str:
    try:
        encoded = canonical_json_bytes(dict(payload))
    except (TypeError, ValueError) as exc:
        raise V5SchedulerError(f"event payload is not canonical JSON: {exc}") from exc
    return encoded.decode("utf-8")


class V5SchedulingController:
    """Own atomic dispatch claims and host-versus-Project failure isolation.

    Candidate/resource selection and the claim transaction repeat the durable
    predicates. Therefore a lifecycle, health, dependency, global-pause,
    allowlist, reservation, or active-GPU-assignment change between inspection
    and claim can never launch stale work.
    """

    def __init__(self, store: V5QueueStore):
        if type(store) is not V5QueueStore:
            raise TypeError(
                f"store must be exactly V5QueueStore, got {type(store).__name__}"
            )
        self.store = store

    @staticmethod
    def _termination_action_from_row(row: sqlite3.Row) -> V5TerminationAction:
        """Validate persisted termination/process evidence as one signal action."""

        state = str(row["state"])
        stage = row["termination_stage"]
        requested_at = row["terminate_requested_at"]
        reason = row["terminate_reason"]
        signal_epoch = row["termination_signal_epoch"]
        if state not in {"terminating", "force_killing"}:
            raise V5SchedulerError(
                f"queue item {row['id']} is {state!r}, not awaiting termination"
            )
        if stage not in {"interrupt", "terminate", "kill"}:
            raise V5SchedulerError(
                f"queue item {row['id']} has invalid termination stage {stage!r}; "
                "repair or restore its persistent evidence before signaling"
            )
        if state == "force_killing" and stage != "kill":
            raise V5SchedulerError(
                f"queue item {row['id']} force-killing state requires kill stage"
            )
        if state == "terminating" and stage == "kill":
            raise V5SchedulerError(
                f"queue item {row['id']} terminating state cannot use kill stage"
            )
        return V5TerminationAction(
            item_id=_positive_integer(int(row["id"]), field_name="item_id"),
            project_id=_positive_integer(
                int(row["project_id"]), field_name="project_id"
            ),
            segment=_positive_integer(int(row["segment"]), field_name="segment"),
            state=state,
            stage=str(stage),
            requested_at=_timestamp(
                str(requested_at), field_name="terminate_requested_at"
            ),
            reason=_text(str(reason), field_name="terminate_reason"),
            signal_epoch=_nonnegative_float(
                signal_epoch, field_name="termination_signal_epoch"
            ),
            pid=_optional_positive_integer(row["pid"], field_name="pid"),
            pgid=_optional_positive_integer(row["pgid"], field_name="pgid"),
            process_start_ticks=(
                None
                if row["proc_start_ticks"] is None
                else _text(
                    str(row["proc_start_ticks"]),
                    field_name="process_start_ticks",
                    maximum=256,
                )
            ),
        )

    @contextmanager
    def _connection(self, *, operation: str) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        try:
            connection = self.store.connect()
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except (sqlite3.Error, V5DatabaseError) as exc:
            if connection is not None:
                connection.rollback()
            raise V5SchedulerError(
                f"schema-v5 could not {operation}: {exc}; no partial scheduler "
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
    def _event(
        connection: sqlite3.Connection,
        *,
        created_at: str,
        actor: str,
        event_type: str,
        scope: FailureScope,
        payload: Mapping[str, object],
        project_id: int | None = None,
        queue_item_id: int | None = None,
    ) -> None:
        if scope is FailureScope.HOST:
            if project_id is not None or queue_item_id is not None:
                raise V5SchedulerError(
                    "host-scoped events cannot identify a Project or queue item"
                )
        elif project_id is None:
            raise V5SchedulerError("project-scoped events require project_id")
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
                scope.value,
                project_id,
            ),
        )

    def host_dispatch_state(self) -> tuple[bool, str]:
        """Read the global dispatch gate without changing queue state."""

        try:
            with self.store.connect() as connection:
                metadata = dict(
                    connection.execute(
                        "SELECT key, value FROM metadata WHERE key IN "
                        "('dispatch_paused', 'pause_reason')"
                    )
                )
        except (sqlite3.Error, V5DatabaseError) as exc:
            raise V5SchedulerError(f"could not read global dispatch state: {exc}") from exc
        if set(metadata) != {"dispatch_paused", "pause_reason"}:
            raise V5SchedulerError(
                "schema-v5 global dispatch metadata is incomplete; restore an "
                "intact database before starting the scheduler"
            )
        return metadata["dispatch_paused"] == "1", metadata["pause_reason"]

    def pause_host(self, *, reason: str, actor: str, changed_at: str) -> bool:
        """Pause all new dispatch for a host-global failure or operator action."""

        reason = _text(reason, field_name="host pause reason")
        with self._connection(operation="pause global dispatch") as connection:
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()
            previous_reason = connection.execute(
                "SELECT value FROM metadata WHERE key = 'pause_reason'"
            ).fetchone()
            if paused is None or previous_reason is None:
                raise V5SchedulerError("global dispatch metadata is incomplete")
            changed = str(paused[0]) != "1" or str(previous_reason[0]) != reason
            if not changed:
                return False
            connection.execute(
                "UPDATE metadata SET value = '1' WHERE key = 'dispatch_paused'"
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE key = 'pause_reason'",
                (reason,),
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="HOST_DISPATCH_PAUSED",
                scope=FailureScope.HOST,
                payload={"reason": reason},
            )
            return True

    def resume_host(self, *, actor: str, changed_at: str) -> bool:
        """Resume global dispatch without altering any Project lifecycle/circuit."""

        with self._connection(operation="resume global dispatch") as connection:
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()
            if paused is None:
                raise V5SchedulerError("global dispatch metadata is incomplete")
            if str(paused[0]) == "0":
                return False
            connection.execute(
                "UPDATE metadata SET value = '0' WHERE key = 'dispatch_paused'"
            )
            connection.execute(
                "UPDATE metadata SET value = '' WHERE key = 'pause_reason'"
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="HOST_DISPATCH_RESUMED",
                scope=FailureScope.HOST,
                payload={},
            )
            return True

    def quarantine_project(
        self,
        project_id: int,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        queue_item_id: int | None = None,
    ) -> bool:
        """Open one Project circuit immediately while healthy Projects continue."""

        project_id = _positive_integer(project_id, field_name="project_id")
        reason = _text(reason, field_name="Project quarantine reason")
        with self._connection(operation=f"quarantine Project {project_id}") as connection:
            row = connection.execute(
                "SELECT health FROM project_runtime_state WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"Project {project_id} does not exist")
            if str(row["health"]) == "open":
                return False
            connection.execute(
                """
                UPDATE project_runtime_state
                SET health = 'open',
                    circuit_failure_count = circuit_failure_count + 1,
                    health_reason = ?, health_actor = ?, health_changed_at = ?
                WHERE project_id = ?
                """,
                (
                    reason,
                    _text(actor, field_name="Project quarantine actor", maximum=256),
                    _timestamp(changed_at, field_name="Project quarantine changed_at"),
                    project_id,
                ),
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="PROJECT_CIRCUIT_OPENED",
                scope=FailureScope.PROJECT,
                project_id=project_id,
                queue_item_id=queue_item_id,
                payload={"reason": reason},
            )
            return True

    def close_project_circuit(
        self,
        project_id: int,
        *,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> bool:
        """Explicitly close one circuit after its Project-local problem is repaired."""

        project_id = _positive_integer(project_id, field_name="project_id")
        reason = _text(reason, field_name="Project circuit close reason")
        with self._connection(
            operation=f"close Project {project_id} circuit"
        ) as connection:
            row = connection.execute(
                "SELECT health FROM project_runtime_state WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"Project {project_id} does not exist")
            if str(row["health"]) == "closed":
                return False
            connection.execute(
                """
                UPDATE project_runtime_state
                SET health = 'closed', circuit_failure_count = 0,
                    health_reason = ?, health_actor = ?, health_changed_at = ?
                WHERE project_id = ?
                """,
                (
                    reason,
                    _text(actor, field_name="Project circuit actor", maximum=256),
                    _timestamp(changed_at, field_name="Project circuit changed_at"),
                    project_id,
                ),
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="PROJECT_CIRCUIT_CLOSED",
                scope=FailureScope.PROJECT,
                project_id=project_id,
                payload={"reason": reason},
            )
            return True

    def reconcile_failed_dependencies(self, *, actor: str, changed_at: str) -> int:
        """Block queued dependants whose prerequisites ended unsuccessfully."""

        with self._connection(operation="reconcile failed dependencies") as connection:
            rows = list(
                connection.execute(
                    """
                    SELECT item.id, item.project_id, dependency.id AS dependency_id,
                           dependency.state AS dependency_state
                    FROM queue_items AS item
                    JOIN dependencies AS link ON link.queue_item_id = item.id
                    JOIN queue_items AS dependency
                      ON dependency.id = link.dependency_item_id
                    WHERE item.state = 'queued'
                      AND dependency.state IN ('failed', 'interrupted',
                                               'force_killed', 'removed')
                    ORDER BY item.id, dependency.id
                    """
                )
            )
            grouped: dict[tuple[int, int], list[tuple[int, str]]] = {}
            for row in rows:
                grouped.setdefault(
                    (int(row["id"]), int(row["project_id"])), []
                ).append((int(row["dependency_id"]), str(row["dependency_state"])))
            changed = 0
            for (item_id, project_id), failures in grouped.items():
                detail = "dependency ended without success: " + ", ".join(
                    f"{dependency_id}={state}"
                    for dependency_id, state in failures
                )
                cursor = connection.execute(
                    """
                    UPDATE queue_items SET state = 'blocked', state_detail = ?
                    WHERE id = ? AND state = 'queued'
                    """,
                    (detail, item_id),
                )
                if cursor.rowcount != 1:
                    continue
                changed += 1
                self._event(
                    connection,
                    created_at=changed_at,
                    actor=actor,
                    event_type="QUEUE_DEPENDENCY_BLOCKED",
                    scope=FailureScope.PROJECT,
                    project_id=project_id,
                    queue_item_id=item_id,
                    payload={
                        "reason": detail,
                        "dependencies": [
                            {"id": dependency_id, "state": state}
                            for dependency_id, state in failures
                        ],
                    },
                )
            return changed

    @staticmethod
    def _candidate_from_row(row: sqlite3.Row) -> V5DispatchCandidate:
        return V5DispatchCandidate(
            id=int(row["id"]),
            project_id=int(row["project_id"]),
            project_key=str(row["project_key"]),
            revision_id=int(row["revision_id"]),
            revision_label=str(row["revision_label"]),
            revision_kind=str(row["revision_kind"]),
            admission_kind=str(row["admission_kind"]),
            snapshot_id=(
                None if row["snapshot_id"] is None else int(row["snapshot_id"])
            ),
            experiment_id=str(row["experiment_id"]),
            attempt=int(row["attempt"]),
            priority=int(row["priority"]),
            resume_front=bool(row["resume_front"]),
            segment=int(row["segment"]),
            git_commit=str(row["git_commit"]),
        )

    @staticmethod
    def _candidate_query(*, one_item: bool = False) -> str:
        item_filter = "AND item.id = ?" if one_item else ""
        return f"""
            SELECT item.*, project.project_key, revision.revision_label,
                   revision.revision_kind
            FROM queue_items AS item
            JOIN projects AS project ON project.id = item.project_id
            JOIN project_revisions AS revision
              ON revision.id = item.revision_id
             AND revision.project_id = item.project_id
            JOIN project_runtime_state AS runtime
              ON runtime.project_id = item.project_id
            WHERE item.state = 'queued'
              AND project.lifecycle = 'active'
              AND runtime.health = 'closed'
              {item_filter}
              AND NOT EXISTS (
                  SELECT 1
                  FROM dependencies AS link
                  JOIN queue_items AS dependency
                    ON dependency.id = link.dependency_item_id
                  WHERE link.queue_item_id = item.id
                    AND dependency.state <> 'succeeded'
              )
            ORDER BY item.priority DESC, item.resume_front DESC, item.id ASC
        """

    def list_dispatch_candidates(
        self, *, limit: int | None = None
    ) -> tuple[V5DispatchCandidate, ...]:
        """Return healthy candidates in the single global priority order."""

        paused, _reason = self.host_dispatch_state()
        if paused:
            return ()
        if limit is not None:
            _positive_integer(limit, field_name="limit")
        try:
            with self.store.connect() as connection:
                rows = list(connection.execute(self._candidate_query()))
        except (sqlite3.Error, V5DatabaseError) as exc:
            raise V5SchedulerError(f"could not list dispatch candidates: {exc}") from exc
        candidates = tuple(self._candidate_from_row(row) for row in rows)
        return candidates if limit is None else candidates[:limit]

    def claim(
        self,
        item_id: int,
        *,
        gpu_uuid: str,
        gpu_index: str,
        actor: str,
        changed_at: str,
    ) -> V5DispatchCandidate | None:
        """Atomically claim an item only if every dispatch predicate still holds."""

        item_id = _positive_integer(item_id, field_name="item_id")
        gpu_uuid = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
        gpu_index = _text(gpu_index, field_name="gpu_index", maximum=64)
        with self._connection(operation=f"claim queue item {item_id}") as connection:
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()
            if paused is None:
                raise V5SchedulerError("global dispatch metadata is incomplete")
            if str(paused[0]) == "1":
                return None
            allowed_gpu = connection.execute(
                """
                SELECT 1 FROM gpu_allowlist
                WHERE uuid = ? AND enabled = 1 AND draining = 0
                """,
                (gpu_uuid,),
            ).fetchone()
            if allowed_gpu is None:
                return None
            reservation = connection.execute(
                """
                SELECT id FROM gpu_reservations
                WHERE gpu_uuid = ? AND status IN ('pending', 'active')
                LIMIT 1
                """,
                (gpu_uuid,),
            ).fetchone()
            if reservation is not None:
                return None
            held_assignment = connection.execute(
                "SELECT id FROM queue_items WHERE assigned_gpu_uuid = ? "
                "AND runtime_gpu_lease_held = 1 LIMIT 1",
                (gpu_uuid,),
            ).fetchone()
            if held_assignment is not None:
                return None
            row = connection.execute(
                self._candidate_query(one_item=True), (item_id,)
            ).fetchone()
            if row is None:
                return None
            candidate = self._candidate_from_row(row)
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = 'starting', assigned_gpu_uuid = ?,
                    assigned_gpu_index = ?, runtime_gpu_lease_held = 1,
                    runtime_gpu_lease_released_at = NULL,
                    state_detail = NULL, resume_front = 0,
                    pid = NULL, pgid = NULL, proc_start_ticks = NULL,
                    started_at = NULL, finished_at = NULL, return_code = NULL
                WHERE id = ? AND state = 'queued'
                """,
                (gpu_uuid, gpu_index, item_id),
            )
            if cursor.rowcount != 1:
                return None
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="EXPERIMENT_STARTING",
                scope=FailureScope.PROJECT,
                project_id=candidate.project_id,
                queue_item_id=item_id,
                payload={
                    "gpu_uuid": gpu_uuid,
                    "gpu_index": gpu_index,
                    "project_key": candidate.project_key,
                    "project_revision": candidate.revision_label,
                    "segment": candidate.segment,
                },
            )
            return candidate

    def active_items(self) -> tuple[tuple[int, int, str], ...]:
        """Return global item ID, Project ID, and state for process recovery."""

        placeholders = ",".join("?" for _ in _ACTIVE_STATES)
        try:
            with self.store.connect() as connection:
                rows = connection.execute(
                    f"SELECT id, project_id, state FROM queue_items "
                    f"WHERE state IN ({placeholders}) ORDER BY id",
                    tuple(sorted(_ACTIVE_STATES)),
                )
                return tuple(
                    (int(row["id"]), int(row["project_id"]), str(row["state"]))
                    for row in rows
                )
        except (sqlite3.Error, V5DatabaseError) as exc:
            raise V5SchedulerError(f"could not list active queue items: {exc}") from exc

    def active_attempts(self) -> tuple[V5ActiveAttempt, ...]:
        """Return complete process evidence for restart recovery and signaling."""

        placeholders = ",".join("?" for _ in _ACTIVE_STATES)
        try:
            with self.store.connect() as connection:
                rows = list(
                    connection.execute(
                        f"""
                        SELECT item.*, project.project_key,
                               revision.revision_label
                        FROM queue_items AS item
                        JOIN projects AS project ON project.id = item.project_id
                        JOIN project_revisions AS revision
                          ON revision.id = item.revision_id
                         AND revision.project_id = item.project_id
                        WHERE item.state IN ({placeholders})
                        ORDER BY item.id
                        """,
                        tuple(sorted(_ACTIVE_STATES)),
                    )
                )
        except (sqlite3.Error, V5DatabaseError) as exc:
            raise V5SchedulerError(f"could not list active attempts: {exc}") from exc
        return tuple(
            V5ActiveAttempt(
                id=int(row["id"]),
                project_id=int(row["project_id"]),
                project_key=str(row["project_key"]),
                revision_id=int(row["revision_id"]),
                revision_label=str(row["revision_label"]),
                admission_kind=str(row["admission_kind"]),
                snapshot_id=(
                    None if row["snapshot_id"] is None else int(row["snapshot_id"])
                ),
                experiment_id=str(row["experiment_id"]),
                attempt=int(row["attempt"]),
                state=str(row["state"]),
                segment=int(row["segment"]),
                git_commit=str(row["git_commit"]),
                assigned_gpu_uuid=(
                    None
                    if row["assigned_gpu_uuid"] is None
                    else str(row["assigned_gpu_uuid"])
                ),
                assigned_gpu_index=(
                    None
                    if row["assigned_gpu_index"] is None
                    else str(row["assigned_gpu_index"])
                ),
                pid=None if row["pid"] is None else int(row["pid"]),
                pgid=None if row["pgid"] is None else int(row["pgid"]),
                process_start_ticks=(
                    None
                    if row["proc_start_ticks"] is None
                    else str(row["proc_start_ticks"])
                ),
                started_at=(
                    None if row["started_at"] is None else str(row["started_at"])
                ),
                terminate_requested_at=(
                    None
                    if row["terminate_requested_at"] is None
                    else str(row["terminate_requested_at"])
                ),
                terminate_reason=(
                    None
                    if row["terminate_reason"] is None
                    else str(row["terminate_reason"])
                ),
                termination_stage=(
                    None
                    if row["termination_stage"] is None
                    else str(row["termination_stage"])
                ),
                termination_signal_epoch=(
                    None
                    if row["termination_signal_epoch"] is None
                    else float(row["termination_signal_epoch"])
                ),
            )
            for row in rows
        )

    def request_termination(
        self,
        item_id: int,
        *,
        reason: str,
        force: bool,
        actor: str,
        requested_at: str,
        signal_epoch: float,
    ) -> V5TerminationAction:
        """CAS one active item into a durable graceful or force-kill request.

        This method deliberately does not signal.  Committing operator intent
        first makes a crash between the database transaction and ``killpg``
        recoverable, while the returned process evidence lets the service
        authenticate the exact process group from a separate process.
        """

        key = _positive_integer(item_id, field_name="item_id")
        detail = _text(reason, field_name="termination reason")
        event_actor = _text(actor, field_name="termination actor", maximum=256)
        timestamp = _timestamp(requested_at, field_name="terminate_requested_at")
        epoch = _nonnegative_float(
            signal_epoch, field_name="termination_signal_epoch"
        )
        if type(force) is not bool:
            raise TypeError(f"force must be boolean, got {type(force).__name__}")
        with self._connection(
            operation=f"request termination for queue item {key}"
        ) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (key,)
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            current = str(row["state"])
            if current == "starting":
                raise V5SchedulerError(
                    f"queue item {key} is 'starting' without a committed launch "
                    "identity; recover/adopt the launch first, or use guarded "
                    "abandoned-launch resolution when no process exists"
                )
            if current not in _ACTIVE_STATES:
                raise V5SchedulerError(
                    f"queue item {key} is {current!r}; termination requires an "
                    "active running, yielding, or terminating item"
                )
            if not force and current == "force_killing":
                raise V5SchedulerError(
                    f"queue item {key} already has a force-kill request"
                )
            if (
                (not force and current == "terminating")
                or (force and current == "force_killing")
            ):
                # Repeated operator delivery is idempotent and never extends an
                # escalation deadline by rewriting its original evidence.
                return self._termination_action_from_row(row)

            target_state = "force_killing" if force else "terminating"
            stage = "kill" if force else "interrupt"
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = ?, terminate_requested_at = ?, terminate_reason = ?,
                    termination_stage = ?, termination_signal_epoch = ?,
                    state_detail = ?
                WHERE id = ? AND state = ?
                """,
                (
                    target_state,
                    timestamp,
                    detail,
                    stage,
                    epoch,
                    detail,
                    key,
                    current,
                ),
            )
            if cursor.rowcount != 1:
                raise V5SchedulerError(
                    f"queue item {key} changed from {current!r} while its "
                    "termination request was being persisted; retry from current state"
                )
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type=(
                    "FORCE_KILL_REQUESTED" if force else "TERMINATION_REQUESTED"
                ),
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={
                    "previous_state": current,
                    "state": target_state,
                    "stage": stage,
                    "signal": "SIGKILL" if force else "SIGINT",
                    "reason": detail,
                    "segment": int(row["segment"]),
                },
            )
            updated = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (key,)
            ).fetchone()
            assert updated is not None
            return self._termination_action_from_row(updated)

    def escalate_termination(
        self,
        item_id: int,
        *,
        expected_stage: str,
        expected_signal_epoch: float,
        actor: str,
        changed_at: str,
        signal_epoch: float,
    ) -> V5TerminationAction | None:
        """Advance SIGINT→SIGTERM→SIGKILL only from exact persisted evidence.

        Returning ``None`` means a receipt, continuation, force request, or
        another scheduler transition committed first.  The caller must not
        send the stale escalation signal in that case.
        """

        key = _positive_integer(item_id, field_name="item_id")
        if expected_stage not in {"interrupt", "terminate"}:
            raise V5SchedulerError(
                "expected_stage must be 'interrupt' or 'terminate'"
            )
        expected_epoch = _nonnegative_float(
            expected_signal_epoch,
            field_name="expected termination_signal_epoch",
        )
        next_epoch = _nonnegative_float(
            signal_epoch, field_name="termination_signal_epoch"
        )
        event_actor = _text(actor, field_name="termination actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="termination changed_at")
        next_stage = "terminate" if expected_stage == "interrupt" else "kill"
        next_state = "terminating" if next_stage == "terminate" else "force_killing"
        next_signal = "SIGTERM" if next_stage == "terminate" else "SIGKILL"
        with self._connection(
            operation=f"escalate termination for queue item {key}"
        ) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (key,)
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            if (
                str(row["state"]) != "terminating"
                or row["termination_stage"] != expected_stage
                or row["termination_signal_epoch"] is None
                or float(row["termination_signal_epoch"]) != expected_epoch
            ):
                return None
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = ?, termination_stage = ?,
                    termination_signal_epoch = ?
                WHERE id = ? AND state = 'terminating'
                  AND termination_stage = ?
                  AND termination_signal_epoch = ?
                """,
                (
                    next_state,
                    next_stage,
                    next_epoch,
                    key,
                    expected_stage,
                    expected_epoch,
                ),
            )
            if cursor.rowcount != 1:
                return None
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="TERMINATION_ESCALATED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={
                    "previous_stage": expected_stage,
                    "stage": next_stage,
                    "state": next_state,
                    "signal": next_signal,
                    "reason": str(row["terminate_reason"]),
                    "segment": int(row["segment"]),
                },
            )
            updated = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (key,)
            ).fetchone()
            assert updated is not None
            return self._termination_action_from_row(updated)

    def record_termination_signal_attempt(
        self,
        action: V5TerminationAction,
        *,
        signal_name: str,
        delivered: bool,
        actor: str,
        attempted_at: str,
    ) -> None:
        """Append an audit event after an authenticated signal attempt."""

        if type(action) is not V5TerminationAction:
            raise TypeError(
                "action must be exactly V5TerminationAction, got "
                f"{type(action).__name__}"
            )
        expected_signal = {
            "interrupt": "SIGINT",
            "terminate": "SIGTERM",
            "kill": "SIGKILL",
        }[action.stage]
        if signal_name != expected_signal:
            raise V5SchedulerError(
                f"termination stage {action.stage!r} requires {expected_signal}, "
                f"got {signal_name!r}"
            )
        if type(delivered) is not bool:
            raise TypeError(
                f"delivered must be boolean, got {type(delivered).__name__}"
            )
        with self._connection(
            operation=f"record queue item {action.item_id} termination signal"
        ) as connection:
            row = connection.execute(
                "SELECT project_id, state, termination_stage, segment "
                "FROM queue_items WHERE id = ?",
                (action.item_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(
                    f"queue item {action.item_id} does not exist"
                )
            if int(row["project_id"]) != action.project_id:
                raise V5SchedulerError(
                    f"queue item {action.item_id} changed Project ownership"
                )
            self._event(
                connection,
                created_at=attempted_at,
                actor=actor,
                event_type=(
                    "TERMINATION_SIGNAL_SENT"
                    if delivered
                    else "TERMINATION_SIGNAL_PENDING"
                ),
                scope=FailureScope.PROJECT,
                project_id=action.project_id,
                queue_item_id=action.item_id,
                payload={
                    "signal": signal_name,
                    "signal_sent": delivered,
                    "requested_stage": action.stage,
                    "current_stage": row["termination_stage"],
                    "current_state": str(row["state"]),
                    "segment": action.segment,
                    "pid": action.pid,
                    "pgid": action.pgid,
                    "process_start_ticks": action.process_start_ticks,
                },
            )

    def record_termination_completion(
        self,
        item_id: int,
        *,
        actor: str,
        finished_at: str,
        return_code: int | None,
    ) -> str:
        """Finalize an ended requested process when no exit receipt was written.

        SIGKILL cannot permit the executor to publish a receipt.  A persisted
        request plus an observed ended process is sufficient to record the
        operator-selected terminal state, without treating that expected lack
        of a receipt as a Project failure.
        """

        key = _positive_integer(item_id, field_name="item_id")
        event_actor = _text(actor, field_name="termination actor", maximum=256)
        timestamp = _timestamp(finished_at, field_name="termination finished_at")
        if return_code is not None and type(return_code) is not int:
            raise V5SchedulerError("return_code must be an integer or null")
        with self._connection(
            operation=f"finalize terminated queue item {key}"
        ) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (key,)
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            current = str(row["state"])
            if current in {"interrupted", "force_killed"}:
                return current
            if current not in {"terminating", "force_killing"}:
                raise V5SchedulerError(
                    f"queue item {key} is {current!r}; only a persisted termination "
                    "request may finalize without an executor receipt"
                )
            terminal = (
                "interrupted" if current == "terminating" else "force_killed"
            )
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = ?, finished_at = ?, return_code = ?, state_detail = ?
                WHERE id = ? AND state = ?
                """,
                (
                    terminal,
                    timestamp,
                    return_code,
                    (
                        "requested process ended without an executor receipt; "
                        f"termination stage {row['termination_stage']}"
                    ),
                    key,
                    current,
                ),
            )
            if cursor.rowcount != 1:
                raise V5SchedulerError(
                    f"queue item {key} changed while termination completion was "
                    "being committed"
                )
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="EXPERIMENT_TERMINATION_COMPLETED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={
                    "previous_state": current,
                    "state": terminal,
                    "stage": row["termination_stage"],
                    "reason": row["terminate_reason"],
                    "segment": int(row["segment"]),
                    "return_code": return_code,
                    "executor_receipt": False,
                },
            )
            return terminal

    def release_gpu_lease(
        self,
        item_id: int,
        *,
        gpu_uuid: str,
        observed_gpu_index: str,
        memory_total_mib: float,
        memory_used_mib: float,
        utilization_percent: float,
        compute_pids: tuple[int, ...],
        minimum_free_memory_fraction: float,
        maximum_utilization_percent: float,
        actor: str,
        observed_at: str,
    ) -> bool:
        """Release one ended attempt's runtime lease from exact idle telemetry.

        Historical GPU assignment fields are immutable provenance.  Only this
        separate lease bit controls dispatch and reservation exclusion, so a
        crash after terminalization still leaves a durable resource barrier.
        The caller must hold the host-wide GPU lock while obtaining telemetry
        and committing this guarded transition.
        """

        key = _positive_integer(item_id, field_name="item_id")
        gpu = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
        observed_index = _text(
            observed_gpu_index,
            field_name="observed_gpu_index",
            maximum=64,
        )
        total = _nonnegative_float(
            memory_total_mib,
            field_name="memory_total_mib",
        )
        used = _nonnegative_float(memory_used_mib, field_name="memory_used_mib")
        utilization = _nonnegative_float(
            utilization_percent,
            field_name="utilization_percent",
        )
        minimum_free = _nonnegative_float(
            minimum_free_memory_fraction,
            field_name="minimum_free_memory_fraction",
        )
        maximum_utilization = _nonnegative_float(
            maximum_utilization_percent,
            field_name="maximum_utilization_percent",
        )
        if total <= 0 or used > total:
            raise V5SchedulerError(
                "GPU release telemetry requires positive total memory and used "
                "memory no greater than total memory"
            )
        if utilization > 100 or minimum_free > 1 or maximum_utilization > 100:
            raise V5SchedulerError(
                "GPU release telemetry percentages or configured thresholds are "
                "outside their valid ranges"
            )
        if type(compute_pids) is not tuple or any(
            type(pid) is not int or pid <= 0 for pid in compute_pids
        ):
            raise V5SchedulerError(
                "GPU release compute_pids must be a tuple of positive integers"
            )
        if len(set(compute_pids)) != len(compute_pids):
            raise V5SchedulerError("GPU release compute_pids must be unique")
        free_fraction = max(0.0, 1.0 - used / total)
        if (
            compute_pids
            or free_fraction < minimum_free
            or utilization > maximum_utilization
        ):
            raise V5SchedulerError(
                f"GPU {gpu!r} is not idle under the configured release thresholds"
            )
        event_actor = _text(actor, field_name="GPU release actor", maximum=256)
        timestamp = _timestamp(observed_at, field_name="GPU observed_at")
        with self._connection(
            operation=f"release GPU runtime lease for queue item {key}"
        ) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            if row["assigned_gpu_uuid"] != gpu:
                raise V5SchedulerError(
                    f"queue item {key} historical GPU assignment "
                    f"{row['assigned_gpu_uuid']!r} does not match observed GPU "
                    f"{gpu!r}"
                )
            state = str(row["state"])
            if state in _ACTIVE_STATES:
                raise V5SchedulerError(
                    f"queue item {key} remains {state!r}; its runtime GPU lease "
                    "cannot be released before process finalization"
                )
            if int(row["runtime_gpu_lease_held"]) == 0:
                if row["runtime_gpu_lease_released_at"] is None:
                    raise V5SchedulerError(
                        f"queue item {key} has no held or previously released "
                        "runtime GPU lease"
                    )
                return False
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET runtime_gpu_lease_held = 0,
                    runtime_gpu_lease_released_at = ?
                WHERE id = ? AND runtime_gpu_lease_held = 1
                  AND assigned_gpu_uuid = ?
                """,
                (timestamp, key, gpu),
            )
            if cursor.rowcount != 1:
                raise V5SchedulerError(
                    f"queue item {key} GPU lease changed during guarded release"
                )
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="GPU_RUNTIME_LEASE_RELEASED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={
                    "gpu_uuid": gpu,
                    "assigned_gpu_index": str(row["assigned_gpu_index"]),
                    "observed_gpu_index": observed_index,
                    "state": state,
                    "segment": int(row["segment"]),
                    "memory_total_mib": total,
                    "memory_used_mib": used,
                    "free_memory_fraction": free_fraction,
                    "utilization_percent": utilization,
                    "compute_pids": list(compute_pids),
                    "minimum_free_memory_fraction": minimum_free,
                    "maximum_utilization_percent": maximum_utilization,
                },
            )
            return True

    def record_worktree_prepared(
        self,
        evidence: ProjectWorktreeEvidence,
        *,
        actor: str,
        changed_at: str,
    ) -> bool:
        """Persist exact structured-worktree identity before a launch claim."""

        if type(evidence) is not ProjectWorktreeEvidence:
            raise TypeError(
                "evidence must be exactly ProjectWorktreeEvidence, got "
                f"{type(evidence).__name__}"
            )
        with self._connection(
            operation=f"record queue item {evidence.queue_item_id} worktree"
        ) as connection:
            row = connection.execute(
                """
                SELECT project_id, revision_id, git_commit, runtime_git_ref,
                       runtime_worktree_path, runtime_worktree_created_at,
                       runtime_worktree_removed_at
                FROM queue_items WHERE id = ?
                """,
                (evidence.queue_item_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(
                    f"queue item {evidence.queue_item_id} does not exist"
                )
            expected = (
                evidence.project_id,
                evidence.project_revision_id,
                evidence.git_commit,
            )
            actual = (
                int(row["project_id"]),
                int(row["revision_id"]),
                str(row["git_commit"]),
            )
            if actual != expected:
                raise V5SchedulerError(
                    f"worktree evidence ownership {expected!r} does not match "
                    f"queue item {evidence.queue_item_id} {actual!r}"
                )
            existing = (
                row["runtime_git_ref"],
                row["runtime_worktree_path"],
            )
            wanted = (evidence.git_ref, str(evidence.worktree))
            if existing == wanted and row["runtime_worktree_removed_at"] is None:
                return False
            if existing not in {(None, None), wanted}:
                raise V5SchedulerError(
                    f"queue item {evidence.queue_item_id} already records different "
                    f"runtime worktree identity {existing!r}; expected {wanted!r}"
                )
            connection.execute(
                """
                UPDATE queue_items
                SET runtime_git_ref = ?, runtime_worktree_path = ?,
                    runtime_worktree_created_at = ?,
                    runtime_worktree_removed_at = NULL,
                    runtime_worktree_cleanup_error = NULL
                WHERE id = ?
                """,
                (
                    evidence.git_ref,
                    str(evidence.worktree),
                    _timestamp(changed_at, field_name="worktree prepared_at"),
                    evidence.queue_item_id,
                ),
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type="PROJECT_WORKTREE_PREPARED",
                scope=FailureScope.PROJECT,
                project_id=evidence.project_id,
                queue_item_id=evidence.queue_item_id,
                payload=evidence.to_document(),
            )
            return True

    def record_worktree_cleanup(
        self,
        evidence: ProjectWorktreeEvidence,
        *,
        actor: str,
        changed_at: str,
        error: str | None = None,
    ) -> bool:
        """Record exact cleanup success or a Project-scoped actionable error."""

        if type(evidence) is not ProjectWorktreeEvidence:
            raise TypeError(
                "evidence must be exactly ProjectWorktreeEvidence, got "
                f"{type(evidence).__name__}"
            )
        if error is not None:
            error = _text(error, field_name="worktree cleanup error")
        with self._connection(
            operation=f"record queue item {evidence.queue_item_id} worktree cleanup"
        ) as connection:
            row = connection.execute(
                """
                SELECT project_id, runtime_git_ref, runtime_worktree_path,
                       runtime_worktree_removed_at, runtime_worktree_cleanup_error
                FROM queue_items WHERE id = ?
                """,
                (evidence.queue_item_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(
                    f"queue item {evidence.queue_item_id} does not exist"
                )
            if (
                row["runtime_git_ref"],
                row["runtime_worktree_path"],
            ) != (evidence.git_ref, str(evidence.worktree)):
                raise V5SchedulerError(
                    f"refused cleanup record for queue item {evidence.queue_item_id}: "
                    "runtime worktree identity differs"
                )
            if error is None and row["runtime_worktree_removed_at"] is not None:
                return False
            if error is not None and row["runtime_worktree_cleanup_error"] == error:
                return False
            connection.execute(
                """
                UPDATE queue_items
                SET runtime_worktree_removed_at = ?,
                    runtime_worktree_cleanup_error = ?
                WHERE id = ?
                """,
                (
                    None
                    if error is not None
                    else _timestamp(changed_at, field_name="worktree removed_at"),
                    error,
                    evidence.queue_item_id,
                ),
            )
            self._event(
                connection,
                created_at=changed_at,
                actor=actor,
                event_type=(
                    "PROJECT_WORKTREE_CLEANUP_FAILED"
                    if error is not None
                    else "PROJECT_WORKTREE_REMOVED"
                ),
                scope=FailureScope.PROJECT,
                project_id=evidence.project_id,
                queue_item_id=evidence.queue_item_id,
                payload={
                    "git_ref": evidence.git_ref,
                    "worktree": str(evidence.worktree),
                    "error": error,
                },
            )
            return True

    def record_legacy_worktree_adopted(
        self,
        item_id: int,
        *,
        git_ref: str,
        worktree_path: Path,
        actor: str,
        changed_at: str,
    ) -> bool:
        """Persist a destination-owned runtime identity without changing v4 evidence."""

        key = _positive_integer(item_id, field_name="item_id")
        reference = _text(git_ref, field_name="legacy git_ref", maximum=512)
        path_text = str(worktree_path)
        if not worktree_path.is_absolute():
            raise V5SchedulerError("legacy runtime worktree_path must be absolute")
        timestamp = _timestamp(changed_at, field_name="legacy adoption changed_at")
        with self._connection(
            operation=f"adopt legacy queue item {key} runtime worktree"
        ) as connection:
            row = connection.execute(
                """
                SELECT item.project_id, item.revision_id, item.git_commit,
                       item.admission_kind, project.project_key,
                       runtime_git_ref, runtime_worktree_path,
                       runtime_worktree_created_at,
                       runtime_worktree_removed_at,
                       runtime_worktree_cleanup_error
                FROM queue_items AS item
                JOIN projects AS project ON project.id = item.project_id
                WHERE item.id = ?
                """,
                (key,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            if row["admission_kind"] != "LegacyMarkdownCard/v0":
                raise V5SchedulerError(
                    f"queue item {key} is not grandfathered legacy admission"
                )
            expected_ref = (
                f"refs/experiment-queue/projects/{row['project_key']}/revisions/"
                f"{row['revision_id']}/items/{key}"
            )
            expected_path = self.store.state_dir / "worktrees" / (
                f"{row['project_key']}-r{row['revision_id']}-item-{key}-"
                f"{str(row['git_commit'])[:12]}"
            )
            if (reference, worktree_path) != (expected_ref, expected_path):
                raise V5SchedulerError(
                    f"queue item {key} runtime ref/worktree must be exact "
                    f"destination-owned identity {(expected_ref, str(expected_path))!r}"
                )
            existing = (row["runtime_git_ref"], row["runtime_worktree_path"])
            wanted = (reference, path_text)
            if existing == wanted and row["runtime_worktree_removed_at"] is None:
                return False
            if existing not in {(None, None), wanted}:
                raise V5SchedulerError(
                    f"queue item {key} already records a different v5 "
                    "runtime worktree identity"
                )
            if (
                existing == wanted
                and row["runtime_worktree_cleanup_error"] is not None
            ):
                raise V5SchedulerError(
                    f"queue item {key} cannot re-adopt a removed legacy runtime "
                    "while cleanup error evidence remains"
                )
            connection.execute(
                """
                UPDATE queue_items
                SET runtime_git_ref = ?, runtime_worktree_path = ?,
                    runtime_worktree_created_at = ?,
                    runtime_worktree_removed_at = NULL,
                    runtime_worktree_cleanup_error = NULL
                WHERE id = ?
                """,
                (reference, path_text, timestamp, key),
            )
            self._event(
                connection,
                created_at=timestamp,
                actor=actor,
                event_type="LEGACY_WORKTREE_ADOPTED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={"git_ref": reference, "worktree": path_text},
            )
            return True

    def record_legacy_worktree_cleanup(
        self,
        item_id: int,
        *,
        git_ref: str,
        worktree_path: Path,
        actor: str,
        changed_at: str,
        error: str | None = None,
    ) -> bool:
        """Record cleanup of only the exact destination-owned legacy runtime."""

        key = _positive_integer(item_id, field_name="item_id")
        reference = _text(git_ref, field_name="legacy git_ref", maximum=512)
        path_text = str(worktree_path)
        if not worktree_path.is_absolute():
            raise V5SchedulerError("legacy cleanup worktree_path must be absolute")
        if error is not None:
            error = _text(error, field_name="legacy worktree cleanup error")
        timestamp = _timestamp(changed_at, field_name="legacy cleanup changed_at")
        with self._connection(
            operation=f"record legacy queue item {key} worktree cleanup"
        ) as connection:
            row = connection.execute(
                """
                SELECT item.project_id, item.revision_id, item.git_commit,
                       item.admission_kind, item.state, project.project_key,
                       runtime_git_ref, runtime_worktree_path,
                       runtime_worktree_removed_at,
                       runtime_worktree_cleanup_error
                FROM queue_items AS item
                JOIN projects AS project ON project.id = item.project_id
                WHERE item.id = ?
                """,
                (key,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            if row["admission_kind"] != "LegacyMarkdownCard/v0":
                raise V5SchedulerError(
                    f"queue item {key} is not grandfathered legacy admission"
                )
            expected_ref = (
                f"refs/experiment-queue/projects/{row['project_key']}/revisions/"
                f"{row['revision_id']}/items/{key}"
            )
            expected_path = self.store.state_dir / "worktrees" / (
                f"{row['project_key']}-r{row['revision_id']}-item-{key}-"
                f"{str(row['git_commit'])[:12]}"
            )
            if (reference, worktree_path) != (expected_ref, expected_path):
                raise V5SchedulerError(
                    f"queue item {key} cleanup identity is not exact "
                    "destination-owned legacy runtime identity"
                )
            if (row["runtime_git_ref"], row["runtime_worktree_path"]) != (
                reference,
                path_text,
            ):
                raise V5SchedulerError(
                    f"queue item {key} legacy ref/worktree identity changed before "
                    "cleanup recording"
                )
            if str(row["state"]) in _ACTIVE_STATES:
                raise V5SchedulerError(
                    f"queue item {key} is {row['state']!r}; legacy cleanup is "
                    "forbidden while an attempt remains active"
                )
            if error is None and row["runtime_worktree_removed_at"] is not None:
                return False
            if error is not None and row["runtime_worktree_cleanup_error"] == error:
                return False
            connection.execute(
                """
                UPDATE queue_items
                SET runtime_worktree_removed_at = ?,
                    runtime_worktree_cleanup_error = ?
                WHERE id = ?
                """,
                (None if error is not None else timestamp, error, key),
            )
            self._event(
                connection,
                created_at=timestamp,
                actor=actor,
                event_type=(
                    "LEGACY_WORKTREE_CLEANUP_FAILED"
                    if error is not None
                    else "LEGACY_WORKTREE_REMOVED"
                ),
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=key,
                payload={
                    "git_ref": reference,
                    "worktree": path_text,
                    "error": error,
                },
            )
            return True

    def record_launched(
        self,
        item_id: int,
        *,
        segment: int,
        gpu_uuid: str,
        pid: int,
        pgid: int,
        process_start_ticks: str | None,
        actor: str,
        started_at: str,
    ) -> str:
        """Persist process identity without clobbering a raced termination request."""

        item_id = _positive_integer(item_id, field_name="item_id")
        segment = _positive_integer(segment, field_name="segment")
        pid = _positive_integer(pid, field_name="pid")
        pgid = _positive_integer(pgid, field_name="pgid")
        gpu_uuid = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
        if process_start_ticks is not None:
            process_start_ticks = _text(
                process_start_ticks, field_name="process_start_ticks", maximum=256
            )
        with self._connection(operation=f"record queue item {item_id} launch") as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (item_id,)
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {item_id} does not exist")
            state = str(row["state"])
            if state not in {"starting", "terminating", "force_killing"}:
                raise V5SchedulerError(
                    f"queue item {item_id} is {state!r}, not launchable/recoverable"
                )
            if int(row["segment"]) != segment:
                raise V5SchedulerError(
                    f"queue item {item_id} segment changed from {segment} to "
                    f"{row['segment']} during launch"
                )
            if row["assigned_gpu_uuid"] != gpu_uuid:
                raise V5SchedulerError(
                    f"queue item {item_id} assigned GPU {row['assigned_gpu_uuid']!r} "
                    f"does not match launched GPU {gpu_uuid!r}"
                )
            if any(
                row[column] is not None
                for column in ("pid", "pgid", "proc_start_ticks", "started_at")
            ):
                raise V5SchedulerError(
                    f"queue item {item_id} already has partial or complete process "
                    "identity; refuse launch identity substitution"
                )
            next_state = "running" if state == "starting" else state
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = ?, pid = ?, pgid = ?, proc_start_ticks = ?, started_at = ?
                WHERE id = ? AND state = ? AND segment = ?
                  AND assigned_gpu_uuid = ?
                  AND pid IS NULL AND pgid IS NULL
                  AND proc_start_ticks IS NULL AND started_at IS NULL
                """,
                (
                    next_state,
                    pid,
                    pgid,
                    process_start_ticks,
                    _timestamp(started_at, field_name="attempt started_at"),
                    item_id,
                    state,
                    segment,
                    gpu_uuid,
                ),
            )
            if cursor.rowcount != 1:
                raise V5SchedulerError(
                    f"queue item {item_id} launch identity lost its starting-row "
                    "compare-and-swap"
                )
            self._event(
                connection,
                created_at=started_at,
                actor=actor,
                event_type="EXPERIMENT_LAUNCHED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=item_id,
                payload={
                    "pid": pid,
                    "pgid": pgid,
                    "gpu_uuid": gpu_uuid,
                    "segment": segment,
                    "state": next_state,
                    "process_start_ticks": process_start_ticks,
                },
            )
            return next_state

    def record_job_artifacts(
        self,
        item_id: int,
        *,
        segment: int,
        observations: tuple[ObservedArtifact, ...],
        actor: str,
        recorded_at: str,
    ) -> bool:
        """Append one idempotent observation set before terminal finalization."""

        key = _positive_integer(item_id, field_name="item_id")
        segment = _positive_integer(segment, field_name="segment")
        if type(observations) is not tuple or any(
            type(observation) is not ObservedArtifact for observation in observations
        ):
            raise TypeError(
                "observations must be a tuple containing exactly ObservedArtifact"
            )
        names = [observation.name for observation in observations]
        if len(names) != len(set(names)):
            raise V5SchedulerError("artifact observations repeat an artifact name")
        timestamp = _timestamp(recorded_at, field_name="artifact recorded_at")
        event_actor = _text(actor, field_name="artifact actor", maximum=256)

        expected: list[tuple[object, ...]] = []
        metadata_by_name: dict[str, bytes] = {}
        for observation in observations:
            metadata = canonical_json_bytes(
                {
                    "present": observation.present,
                    "required": observation.required,
                    "digestPolicy": "not-hashed-general-artifact",
                }
            )
            metadata_by_name[observation.name] = metadata
            expected.append(
                (
                    observation.name,
                    observation.artifact_type,
                    observation.root_name,
                    "readWrite",
                    observation.relative_path,
                    str(observation.path),
                    observation.size_bytes,
                    None,
                    metadata,
                )
            )
        expected.sort(key=lambda values: str(values[0]))

        with self._connection(
            operation=f"record queue item {key} segment {segment} artifacts"
        ) as connection:
            row = connection.execute(
                "SELECT project_id, revision_id, state, segment "
                "FROM queue_items WHERE id = ?",
                (key,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {key} does not exist")
            if int(row["segment"]) != segment:
                raise V5SchedulerError(
                    f"queue item {key} current segment {row['segment']} does not "
                    f"match artifact segment {segment}"
                )
            if str(row["state"]) not in _ACTIVE_STATES:
                raise V5SchedulerError(
                    f"queue item {key} is {row['state']!r}; artifacts must be "
                    "recorded before terminal finalization"
                )
            existing_rows = connection.execute(
                """
                SELECT artifact_name, artifact_type, root_name, root_access,
                       relative_path, absolute_path, size_bytes, sha256,
                       metadata_json
                FROM job_artifacts
                WHERE queue_item_id = ? AND segment = ?
                ORDER BY artifact_name
                """,
                (key, segment),
            ).fetchall()
            if existing_rows:
                existing = [
                    (
                        str(existing_row["artifact_name"]),
                        str(existing_row["artifact_type"]),
                        str(existing_row["root_name"]),
                        str(existing_row["root_access"]),
                        str(existing_row["relative_path"]),
                        str(existing_row["absolute_path"]),
                        existing_row["size_bytes"],
                        existing_row["sha256"],
                        bytes(existing_row["metadata_json"]),
                    )
                    for existing_row in existing_rows
                ]
                if existing == expected:
                    return False
                raise V5SchedulerError(
                    f"queue item {key} segment {segment} already has different "
                    "append-only artifact evidence"
                )
            project_id = int(row["project_id"])
            revision_id = int(row["revision_id"])
            for observation in observations:
                connection.execute(
                    """
                    INSERT INTO job_artifacts(
                        queue_item_id, project_id, revision_id, segment,
                        evidence_kind, artifact_name, artifact_type, root_name,
                        root_access, relative_path, absolute_path, size_bytes,
                        sha256, recorded_at, metadata_json
                    ) VALUES (?, ?, ?, ?, 'declared-v1', ?, ?, ?, 'readWrite',
                              ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        key,
                        project_id,
                        revision_id,
                        segment,
                        observation.name,
                        observation.artifact_type,
                        observation.root_name,
                        observation.relative_path,
                        str(observation.path),
                        observation.size_bytes,
                        timestamp,
                        metadata_by_name[observation.name],
                    ),
                )
            if observations:
                self._event(
                    connection,
                    created_at=timestamp,
                    actor=event_actor,
                    event_type="JOB_ARTIFACTS_RECORDED",
                    scope=FailureScope.PROJECT,
                    project_id=project_id,
                    queue_item_id=key,
                    payload={
                        "segment": segment,
                        "artifacts": [
                            {
                                "name": observation.name,
                                "present": observation.present,
                                "required": observation.required,
                            }
                            for observation in observations
                        ],
                    },
                )
            return bool(observations)

    def record_executor_completion(
        self,
        receipt: ExecutorReceipt,
        *,
        actor: str,
    ) -> str:
        """Finalize an authenticated non-yielding executor receipt exactly once."""

        if type(receipt) is not ExecutorReceipt:
            raise TypeError(
                f"receipt must be exactly ExecutorReceipt, got {type(receipt).__name__}"
            )
        with self._connection(
            operation=f"finalize queue item {receipt.queue_item_id}"
        ) as connection:
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ?", (receipt.queue_item_id,)
            ).fetchone()
            if row is None:
                raise V5SchedulerError(
                    f"queue item {receipt.queue_item_id} does not exist"
                )
            identity = (
                int(row["project_id"]),
                int(row["revision_id"]),
                str(row["experiment_id"]),
                int(row["attempt"]),
                int(row["segment"]),
                str(row["git_commit"]),
            )
            expected = (
                receipt.project_id,
                receipt.project_revision_id,
                receipt.experiment_id,
                receipt.attempt,
                receipt.segment,
                receipt.git_commit,
            )
            if identity != expected:
                raise V5SchedulerError(
                    f"executor receipt identity {expected!r} does not match queue "
                    f"item {receipt.queue_item_id} {identity!r}"
                )
            state = str(row["state"])
            if state == "yielding":
                raise V5SchedulerError(
                    f"queue item {receipt.queue_item_id} is yielding; validate its "
                    "typed continuation before choosing a terminal/requeue state"
                )
            if state in {"succeeded", "failed", "interrupted", "force_killed"}:
                if row["return_code"] == receipt.return_code:
                    return state
                raise V5SchedulerError(
                    f"queue item {receipt.queue_item_id} already finalized as {state} "
                    "with different return-code evidence"
                )
            terminal_by_state = {
                "starting": "succeeded" if receipt.return_code == 0 else "failed",
                "running": "succeeded" if receipt.return_code == 0 else "failed",
                "terminating": "interrupted",
                "force_killing": "force_killed",
            }
            terminal = terminal_by_state.get(state)
            if terminal is None:
                raise V5SchedulerError(
                    f"queue item {receipt.queue_item_id} state {state!r} cannot "
                    "consume an executor receipt"
                )
            detail = (
                None
                if terminal == "succeeded"
                else f"executor returned {receipt.return_code}"
            )
            connection.execute(
                """
                UPDATE queue_items
                SET state = ?, finished_at = ?, return_code = ?, state_detail = ?
                WHERE id = ?
                """,
                (
                    terminal,
                    receipt.finished_at,
                    receipt.return_code,
                    detail,
                    receipt.queue_item_id,
                ),
            )
            self._event(
                connection,
                created_at=receipt.finished_at,
                actor=actor,
                event_type="EXPERIMENT_FINISHED",
                scope=FailureScope.PROJECT,
                project_id=receipt.project_id,
                queue_item_id=receipt.queue_item_id,
                payload={
                    "state": terminal,
                    "return_code": receipt.return_code,
                    "segment": receipt.segment,
                    "signals_received": list(receipt.signals_received),
                },
            )
            return terminal

    def fail_active_item(
        self,
        item_id: int,
        *,
        reason: str,
        actor: str,
        finished_at: str,
        return_code: int | None = None,
    ) -> str:
        """Fail closed when a launched process has no trustworthy terminal path."""

        item_id = _positive_integer(item_id, field_name="item_id")
        reason = _text(reason, field_name="active item failure reason")
        if return_code is not None and (type(return_code) is not int or return_code < 0):
            raise V5SchedulerError("return_code must be a nonnegative integer or null")
        with self._connection(operation=f"fail active queue item {item_id}") as connection:
            row = connection.execute(
                "SELECT state, project_id, segment FROM queue_items WHERE id = ?",
                (item_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {item_id} does not exist")
            current = str(row["state"])
            terminal = (
                "interrupted"
                if current == "terminating"
                else "force_killed"
                if current == "force_killing"
                else "failed"
            )
            if current in {"succeeded", "failed", "interrupted", "force_killed"}:
                return current
            if current not in _ACTIVE_STATES:
                raise V5SchedulerError(
                    f"queue item {item_id} is {current!r}, not an active attempt"
                )
            connection.execute(
                """
                UPDATE queue_items
                SET state = ?, state_detail = ?, finished_at = ?, return_code = ?
                WHERE id = ?
                """,
                (
                    terminal,
                    reason,
                    _timestamp(finished_at, field_name="active failure finished_at"),
                    return_code,
                    item_id,
                ),
            )
            self._event(
                connection,
                created_at=finished_at,
                actor=actor,
                event_type="EXPERIMENT_PROCESS_EVIDENCE_FAILED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=item_id,
                payload={
                    "previous_state": current,
                    "state": terminal,
                    "reason": reason,
                    "segment": int(row["segment"]),
                    "return_code": return_code,
                },
            )
            return terminal

    def resolve_abandoned_launch(
        self,
        item_id: int,
        *,
        project_id: int,
        gpu_uuid: str,
        pid: int | None,
        pgid: int | None,
        process_start_ticks: str | None,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5AbandonedLaunchResolution:
        """Fail one operator-proven abandoned active attempt under exact guards."""

        item_key = _positive_integer(item_id, field_name="item_id")
        project_key = _positive_integer(project_id, field_name="project_id")
        gpu = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
        detail = _text(reason, field_name="abandoned launch reason")
        event_actor = _text(actor, field_name="abandoned launch actor", maximum=256)
        timestamp = _timestamp(
            changed_at,
            field_name="abandoned launch changed_at",
        )
        if (pid is None) != (pgid is None):
            raise V5SchedulerError(
                "abandoned-attempt expected PID and process group must both be "
                "null or both be present"
            )
        expected_pid = (
            None if pid is None else _positive_integer(pid, field_name="pid")
        )
        expected_pgid = (
            None if pgid is None else _positive_integer(pgid, field_name="pgid")
        )
        expected_ticks = (
            None
            if process_start_ticks is None
            else _text(
                process_start_ticks,
                field_name="process_start_ticks",
                maximum=256,
            )
        )
        with self._connection(
            operation=f"resolve abandoned launch for queue item {item_key}"
        ) as connection:
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()
            if paused is None or str(paused[0]) != "1":
                raise V5SchedulerError(
                    "host dispatch must already be paused before resolving an "
                    "abandoned launch"
                )
            row = connection.execute(
                "SELECT * FROM queue_items WHERE id = ? AND project_id = ?",
                (item_key, project_key),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(
                    f"Project id {project_key} has no queue item with global id "
                    f"{item_key}"
                )
            previous_state = str(row["state"])
            allowed_states = {
                "starting",
                "running",
                "yielding",
                "terminating",
                "force_killing",
            }
            if previous_state not in allowed_states:
                raise V5SchedulerError(
                    f"queue item {item_key} is {previous_state!r}; abandoned-attempt "
                    f"resolution requires an active state in {sorted(allowed_states)!r}"
                )
            if row["assigned_gpu_uuid"] is None or row["assigned_gpu_index"] is None:
                raise V5SchedulerError(
                    f"queue item {item_key} lacks a complete assigned GPU identity"
                )
            if str(row["assigned_gpu_uuid"]) != gpu:
                raise V5SchedulerError(
                    f"queue item {item_key} is assigned GPU "
                    f"{row['assigned_gpu_uuid']!r}, not confirmed GPU {gpu!r}"
                )
            recorded_identity = (
                row["pid"],
                row["pgid"],
                row["proc_start_ticks"],
            )
            expected_identity = (expected_pid, expected_pgid, expected_ticks)
            if recorded_identity != expected_identity:
                raise V5SchedulerError(
                    f"queue item {item_key} process identity changed from exact "
                    f"operator-checked evidence {expected_identity!r} to "
                    f"{recorded_identity!r}"
                )
            if expected_pid is None and previous_state != "starting":
                raise V5SchedulerError(
                    f"queue item {item_key} has no process identity in state "
                    f"{previous_state!r}; only a pre-launch 'starting' claim may "
                    "be resolved without recorded identity"
                )
            if expected_pid is not None and previous_state == "starting":
                raise V5SchedulerError(
                    f"queue item {item_key} records process identity while still "
                    "'starting'; this inconsistent evidence requires repair"
                )
            identity_kind = (
                "pre-launch claim" if expected_pid is None else "dead recorded process"
            )
            event_type = (
                "ABANDONED_LAUNCH_RESOLVED"
                if expected_pid is None
                else "DEAD_PROCESS_RESOLVED"
            )
            failure_detail = (
                f"operator resolved abandoned attempt ({identity_kind}): {detail}"
            )
            cursor = connection.execute(
                """
                UPDATE queue_items
                SET state = 'failed', state_detail = ?, finished_at = ?,
                    return_code = 127
                WHERE id = ? AND project_id = ? AND state = ?
                  AND assigned_gpu_uuid = ? AND assigned_gpu_index IS NOT NULL
                  AND runtime_gpu_lease_held = 1
                  AND pid IS ? AND pgid IS ? AND proc_start_ticks IS ?
                """,
                (
                    failure_detail,
                    timestamp,
                    item_key,
                    project_key,
                    previous_state,
                    gpu,
                    expected_pid,
                    expected_pgid,
                    expected_ticks,
                ),
            )
            if cursor.rowcount != 1:
                raise V5SchedulerError(
                    f"queue item {item_key} abandoned-launch guards changed before "
                    "the terminal compare-and-swap"
                )
            runtime = connection.execute(
                "SELECT circuit_failure_count FROM project_runtime_state "
                "WHERE project_id = ?",
                (project_key,),
            ).fetchone()
            if runtime is None:
                raise V5SchedulerError(
                    f"Project id {project_key} lacks runtime health state"
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
                    failure_detail,
                    event_actor,
                    timestamp,
                    project_key,
                ),
            )
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type=event_type,
                scope=FailureScope.PROJECT,
                project_id=project_key,
                queue_item_id=item_key,
                payload={
                    "previous_state": previous_state,
                    "state": "failed",
                    "gpu_uuid": gpu,
                    "identity_kind": identity_kind,
                    "pid": expected_pid,
                    "pgid": expected_pgid,
                    "process_start_ticks": expected_ticks,
                    "reason": detail,
                    "confirmation": "RESOLVE-ABANDONED-LAUNCH",
                },
            )
            return V5AbandonedLaunchResolution(
                item_id=item_key,
                project_id=project_key,
                gpu_uuid=gpu,
                previous_state=previous_state,
                event_type=event_type,
                state="failed",
                reason=detail,
                resolved_at=timestamp,
            )

    def claim_manual_yield_signal_attempt(
        self,
        item_id: int,
        *,
        request_id: str,
        attempt_token: str,
        signal_epoch: float,
        retry_after_seconds: float,
        actor: str,
        changed_at: str,
    ) -> V5ManualYieldSignalClaim | None:
        """Claim one durable signal attempt, retrying only after its lease."""

        item_key = _positive_integer(item_id, field_name="item_id")
        request_key = _text(request_id, field_name="request_id", maximum=256)
        token = _text(attempt_token, field_name="attempt_token", maximum=256)
        epoch = _nonnegative_float(signal_epoch, field_name="signal_epoch")
        retry = _nonnegative_float(
            retry_after_seconds, field_name="retry_after_seconds"
        )
        event_actor = _text(actor, field_name="signal claim actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="signal claim changed_at")
        with self._connection(
            operation=f"claim manual-yield signal for queue item {item_key}"
        ) as connection:
            row = connection.execute(
                """
                SELECT project_id, state, segment, yield_request_id
                FROM queue_items WHERE id = ?
                """,
                (item_key,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {item_key} does not exist")
            if str(row["state"]) != "yielding" or row["yield_request_id"] != request_key:
                raise V5SchedulerError(
                    f"queue item {item_key} no longer has yielding request "
                    f"{request_key!r}; refusing stale signal claim"
                )
            claims: list[dict[str, object]] = []
            results: dict[str, dict[str, object]] = {}
            for event in connection.execute(
                """
                SELECT event_type, payload_json FROM events
                WHERE queue_item_id = ?
                  AND event_type IN (
                    'MANUAL_PREEMPTION_SIGNAL_CLAIMED',
                    'MANUAL_PREEMPTION_SIGNAL_RESULT'
                  )
                ORDER BY id
                """,
                (item_key,),
            ):
                try:
                    payload = json.loads(str(event["payload_json"]))
                except (json.JSONDecodeError, TypeError) as exc:
                    raise V5SchedulerError(
                        f"queue item {item_key} has corrupt signal-attempt event"
                    ) from exc
                if type(payload) is not dict or payload.get("request_id") != request_key:
                    continue
                if event["event_type"] == "MANUAL_PREEMPTION_SIGNAL_CLAIMED":
                    claims.append(payload)
                else:
                    result_token = payload.get("attempt_token")
                    if type(result_token) is str:
                        results[result_token] = payload
            if claims:
                latest = claims[-1]
                latest_token = latest.get("attempt_token")
                latest_epoch = latest.get("signal_epoch")
                latest_attempt = latest.get("attempt")
                if (
                    type(latest_token) is not str
                    or type(latest_epoch) not in {int, float}
                    or type(latest_attempt) is not int
                ):
                    raise V5SchedulerError(
                        f"queue item {item_key} has invalid signal-claim evidence"
                    )
                result = results.get(latest_token)
                if result is not None and result.get("delivered") is True:
                    return None
                reference = (
                    latest_epoch
                    if result is None
                    else result.get("result_epoch")
                )
                if type(reference) not in {int, float}:
                    raise V5SchedulerError(
                        f"queue item {item_key} has invalid signal-result epoch"
                    )
                if epoch < float(reference) + retry:
                    return None
                attempt = latest_attempt + 1
            else:
                attempt = 1
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="MANUAL_PREEMPTION_SIGNAL_CLAIMED",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=item_key,
                payload={
                    "request_id": request_key,
                    "segment": int(row["segment"]),
                    "attempt_token": token,
                    "attempt": attempt,
                    "replay": attempt > 1,
                    "signal_epoch": epoch,
                    "retry_after_seconds": retry,
                    "delivery_semantics": "at-least-once",
                },
            )
            return V5ManualYieldSignalClaim(
                item_id=item_key,
                project_id=int(row["project_id"]),
                request_id=request_key,
                attempt_token=token,
                attempt=attempt,
                replay=attempt > 1,
                signal_epoch=epoch,
            )

    def record_manual_yield_signal_result(
        self,
        claim: V5ManualYieldSignalClaim,
        *,
        delivered: bool,
        detail: str,
        result_epoch: float,
        actor: str,
        changed_at: str,
    ) -> bool:
        """Append the exact outcome for one previously committed signal claim."""

        if type(claim) is not V5ManualYieldSignalClaim:
            raise TypeError(
                "claim must be exactly V5ManualYieldSignalClaim, got "
                f"{type(claim).__name__}"
            )
        if type(delivered) is not bool:
            raise TypeError("delivered must be a boolean")
        result_detail = _text(detail, field_name="signal result detail", maximum=4096)
        epoch = _nonnegative_float(result_epoch, field_name="result_epoch")
        event_actor = _text(actor, field_name="signal result actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="signal result changed_at")
        with self._connection(
            operation=f"record manual-yield signal result for queue item {claim.item_id}"
        ) as connection:
            row = connection.execute(
                """
                SELECT project_id, state, segment, yield_request_id
                FROM queue_items WHERE id = ?
                """,
                (claim.item_id,),
            ).fetchone()
            if row is None:
                raise V5SchedulerError(f"queue item {claim.item_id} does not exist")
            if (
                str(row["state"]) != "yielding"
                or row["yield_request_id"] != claim.request_id
                or int(row["project_id"]) != claim.project_id
            ):
                raise V5SchedulerError(
                    f"queue item {claim.item_id} no longer has exact yielding "
                    f"request {claim.request_id!r}; refusing stale signal result"
                )
            existing: dict[str, object] | None = None
            claim_found = False
            for event in connection.execute(
                """
                SELECT event_type, payload_json FROM events
                WHERE queue_item_id = ?
                  AND event_type IN (
                    'MANUAL_PREEMPTION_SIGNAL_CLAIMED',
                    'MANUAL_PREEMPTION_SIGNAL_RESULT'
                  )
                ORDER BY id
                """,
                (claim.item_id,),
            ):
                payload = json.loads(str(event["payload_json"]))
                if (
                    type(payload) is not dict
                    or payload.get("request_id") != claim.request_id
                    or payload.get("attempt_token") != claim.attempt_token
                ):
                    continue
                if event["event_type"] == "MANUAL_PREEMPTION_SIGNAL_CLAIMED":
                    claim_found = True
                else:
                    existing = payload
            if not claim_found:
                raise V5SchedulerError(
                    f"manual-yield signal claim {claim.attempt_token!r} is missing"
                )
            expected_result = {
                "request_id": claim.request_id,
                "segment": int(row["segment"]),
                "attempt_token": claim.attempt_token,
                "attempt": claim.attempt,
                "replay": claim.replay,
                "delivered": delivered,
                "detail": result_detail,
                "result_epoch": epoch,
                "delivery_semantics": "at-least-once",
            }
            if existing is not None:
                if existing == expected_result:
                    return False
                raise V5SchedulerError(
                    f"manual-yield signal claim {claim.attempt_token!r} already "
                    "has a different result"
                )
            self._event(
                connection,
                created_at=timestamp,
                actor=event_actor,
                event_type="MANUAL_PREEMPTION_SIGNAL_RESULT",
                scope=FailureScope.PROJECT,
                project_id=int(row["project_id"]),
                queue_item_id=claim.item_id,
                payload=expected_result,
            )
            return True

    def check_disk_capacity(
        self,
        project_id: int,
        *,
        minimum_gib: float,
        revision_id: int | None = None,
    ) -> tuple[DiskCapacity, ...]:
        """Measure central and Project artifact filesystems without mutating state."""

        project_id = _positive_integer(project_id, field_name="project_id")
        if revision_id is not None:
            revision_id = _positive_integer(revision_id, field_name="revision_id")
        minimum = _nonnegative_float(minimum_gib, field_name="minimum_gib")
        try:
            with self.store.connect() as connection:
                project = connection.execute(
                    "SELECT project_key FROM projects WHERE id = ?", (project_id,)
                ).fetchone()
                if project is None:
                    raise V5SchedulerError(f"Project {project_id} does not exist")
                roots = [
                    Path(str(row["mount_path"]))
                    for row in connection.execute(
                        """
                        SELECT DISTINCT mount.mount_path
                        FROM projects AS project
                        JOIN project_artifact_roots AS artifact
                          ON artifact.project_id = project.id
                         AND artifact.revision_id = COALESCE(?, project.current_revision_id)
                        JOIN project_mounts AS mount
                          ON mount.project_id = artifact.project_id
                         AND mount.revision_id = artifact.revision_id
                         AND mount.mount_name = artifact.mount_name
                        WHERE project.id = ?
                        ORDER BY mount.mount_path
                        """,
                        (revision_id, project_id),
                    )
                ]
        except (sqlite3.Error, V5DatabaseError) as exc:
            raise V5SchedulerError(f"could not load disk roots: {exc}") from exc
        project_key = str(project["project_key"])
        checks: list[DiskCapacity] = []
        for scope, root in (
            (FailureScope.HOST, self.store.state_dir),
            *((FailureScope.PROJECT, root) for root in roots),
        ):
            try:
                free = shutil.disk_usage(root).free / (1024**3)
            except OSError as exc:
                label = "central state" if scope is FailureScope.HOST else project_key
                raise V5SchedulerError(
                    f"could not inspect {label} filesystem at {root}: {exc}"
                ) from exc
            checks.append(
                DiskCapacity(
                    scope=scope,
                    root=root,
                    free_gib=free,
                    minimum_gib=minimum,
                    project_id=(project_id if scope is FailureScope.PROJECT else None),
                    project_key=(project_key if scope is FailureScope.PROJECT else None),
                )
            )
        return tuple(checks)

    def enforce_disk_capacity(
        self,
        project_id: int,
        *,
        minimum_gib: float,
        revision_id: int | None = None,
        actor: str,
        changed_at: str,
    ) -> tuple[DiskCapacity, ...]:
        """Apply the correct failure scope to measured filesystem pressure."""

        checks = self.check_disk_capacity(
            project_id,
            minimum_gib=minimum_gib,
            revision_id=revision_id,
        )
        host_failure = next(
            (
                check
                for check in checks
                if check.scope is FailureScope.HOST and not check.sufficient
            ),
            None,
        )
        if host_failure is not None:
            self.pause_host(
                reason=(
                    f"central state filesystem {host_failure.root} has only "
                    f"{host_failure.free_gib:.2f} GiB free; minimum is "
                    f"{host_failure.minimum_gib:.2f} GiB"
                ),
                actor=actor,
                changed_at=changed_at,
            )
            return checks
        project_failure = next(
            (
                check
                for check in checks
                if check.scope is FailureScope.PROJECT and not check.sufficient
            ),
            None,
        )
        if project_failure is not None:
            self.quarantine_project(
                project_id,
                reason=(
                    f"Project artifact filesystem {project_failure.root} has only "
                    f"{project_failure.free_gib:.2f} GiB free; minimum is "
                    f"{project_failure.minimum_gib:.2f} GiB"
                ),
                actor=actor,
                changed_at=changed_at,
            )
        return checks


__all__ = [
    "DiskCapacity",
    "FailureScope",
    "V5AbandonedLaunchResolution",
    "V5ActiveAttempt",
    "V5DispatchCandidate",
    "V5ManualYieldSignalClaim",
    "V5SchedulerError",
    "V5SchedulingController",
    "V5TerminationAction",
]
