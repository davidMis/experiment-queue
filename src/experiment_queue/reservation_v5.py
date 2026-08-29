"""Transactional, process-neutral GPU reservations for schema-v5.

This module owns reservation SQL and host-scoped audit events.  A reservation
never changes a queue item and never signals a process: an occupied GPU receives
a passive ``pending`` reservation which blocks new dispatch and becomes active
only after a later reconciliation observes that every queue assignment ended.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import sqlite3
from typing import Final, Iterator, Mapping

from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.serialization import canonical_json_bytes


MIN_RESERVATION_HOURS: Final = 1
MAX_RESERVATION_HOURS: Final = 24


class V5ReservationError(RuntimeError):
    """Raised when a schema-v5 reservation operation cannot complete safely."""


class V5ReservationNotFoundError(V5ReservationError):
    """Raised when a requested reservation identity does not exist."""


class V5ReservationEvidenceError(V5ReservationError):
    """Raised when stored reservation or GPU ownership evidence is inconsistent."""


class V5ReservationStatus(StrEnum):
    """Database-v5 reservation states with exactly one open state lineage."""

    PENDING = "pending"
    ACTIVE = "active"
    EXPIRED = "expired"
    RELEASED = "released"
    FAILED = "failed"

    @property
    def is_open(self) -> bool:
        """Return whether this state prevents new dispatch on its GPU."""

        return self in {self.PENDING, self.ACTIVE}


@dataclass(frozen=True, slots=True)
class V5GpuReservation:
    """Role-neutral immutable view of one reservation history row."""

    id: int
    gpu_uuid: str
    queue_item_id: int | None
    status: V5ReservationStatus
    requested_at: str
    requested_by: str
    note: str
    duration_hours: int
    starts_at: str | None
    expires_at: str | None
    released_at: str | None
    released_by: str | None
    state_detail: str | None

    @property
    def is_open(self) -> bool:
        """Return whether this row currently blocks dispatch on its GPU."""

        return self.status.is_open


@dataclass(frozen=True, slots=True)
class V5ReservationReconciliation:
    """Exact state changes made by one idempotent reconciliation pass."""

    reconciled_at: str
    activated: tuple[V5GpuReservation, ...]
    expired: tuple[V5GpuReservation, ...]
    failed: tuple[V5GpuReservation, ...]

    @property
    def changed(self) -> tuple[V5GpuReservation, ...]:
        """Return every changed row in transition order."""

        return self.expired + self.activated + self.failed


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise V5ReservationError(f"{field_name} must be a positive integer")
    return value


def _duration(value: object) -> int:
    if (
        type(value) is not int
        or not MIN_RESERVATION_HOURS <= value <= MAX_RESERVATION_HOURS
    ):
        raise V5ReservationError(
            "reservation duration_hours must be a whole number from "
            f"{MIN_RESERVATION_HOURS} through {MAX_RESERVATION_HOURS}"
        )
    return value


def _text(value: object, *, field_name: str, maximum: int) -> str:
    if type(value) is not str or not value:
        raise V5ReservationError(f"{field_name} must be nonempty text")
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise V5ReservationError(
            f"{field_name} must be log-safe text of at most {maximum} characters"
        )
    return value


def _gpu_uuid(value: object) -> str:
    uuid = _text(value, field_name="gpu_uuid", maximum=256)
    if uuid != uuid.strip():
        raise V5ReservationError(
            "gpu_uuid must be the exact allowlisted identity without surrounding whitespace"
        )
    return uuid


def _note(value: object) -> str:
    if type(value) is not str:
        raise V5ReservationError(
            "reservation note must be text identifying who or what needs the GPU"
        )
    note = " ".join(value.split())
    if not note:
        raise V5ReservationError(
            "reservation note is required; identify who or what needs the GPU"
        )
    if len(note) > 200:
        raise V5ReservationError("reservation note must be 200 characters or fewer")
    return note


def _parse_timestamp(value: object, *, field_name: str) -> datetime:
    timestamp = _text(value, field_name=field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise V5ReservationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V5ReservationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        )
    return parsed.astimezone(timezone.utc)


def _timestamp(value: object, *, field_name: str) -> tuple[str, datetime]:
    parsed = _parse_timestamp(value, field_name=field_name)
    timespec = "microseconds" if parsed.microsecond else "seconds"
    return parsed.isoformat(timespec=timespec), parsed


def _stored_optional_text(
    row: sqlite3.Row,
    name: str,
    *,
    reservation_id: int,
    maximum: int,
) -> str | None:
    value = row[name]
    if value is None:
        return None
    try:
        return _text(
            value,
            field_name=f"reservation {reservation_id} {name}",
            maximum=maximum,
        )
    except V5ReservationError as exc:
        raise V5ReservationEvidenceError(str(exc)) from exc


def _payload_json(payload: Mapping[str, object]) -> str:
    try:
        return canonical_json_bytes(dict(payload)).decode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise V5ReservationError(f"reservation event payload is invalid: {exc}") from exc


class V5ReservationService:
    """Own schema-v5 reservation transactions without process-control authority.

    Request idempotency uses the complete immutable request tuple
    ``(gpu_uuid, requested_at, requested_by, normalized_note, duration_hours)``.
    Callers must reuse the same timestamp when retrying one logical request.
    """

    def __init__(self, store: V5QueueStore):
        if type(store) is not V5QueueStore:
            raise TypeError(
                f"store must be exactly V5QueueStore, got {type(store).__name__}"
            )
        self.store = store

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
            raise V5ReservationError(
                f"schema-v5 could not {operation}: {exc}; no partial reservation "
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
        payload: Mapping[str, object],
    ) -> None:
        """Append a host-scoped event; global reservations own no Project."""

        connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, ?, ?, NULL, ?, 'host', NULL)
            """,
            (created_at, actor, event_type, _payload_json(payload)),
        )

    @staticmethod
    def _active_assignments(
        connection: sqlite3.Connection, gpu_uuid: str
    ) -> tuple[int, ...]:
        rows = connection.execute(
            "SELECT id FROM queue_items WHERE assigned_gpu_uuid = ? "
            "AND runtime_gpu_lease_held = 1 ORDER BY id",
            (gpu_uuid,),
        ).fetchall()
        return tuple(int(row["id"]) for row in rows)

    @staticmethod
    def _reservation_from_row(row: sqlite3.Row) -> V5GpuReservation:
        try:
            reservation_id = _positive_integer(row["id"], field_name="reservation id")
            uuid = _gpu_uuid(row["gpu_uuid"])
            status = V5ReservationStatus(str(row["status"]))
            requested_at = _text(
                row["requested_at"],
                field_name=f"reservation {reservation_id} requested_at",
                maximum=64,
            )
            requested_time = _parse_timestamp(
                requested_at,
                field_name=f"reservation {reservation_id} requested_at",
            )
            requested_by = _text(
                row["requested_by"],
                field_name=f"reservation {reservation_id} requested_by",
                maximum=256,
            )
            note = _text(
                row["note"],
                field_name=f"reservation {reservation_id} note",
                maximum=200,
            )
            duration = _duration(row["duration_hours"])
        except (ValueError, V5ReservationError) as exc:
            raise V5ReservationEvidenceError(
                f"stored GPU reservation evidence is invalid: {exc}"
            ) from exc
        queue_item_value = row["queue_item_id"]
        if queue_item_value is not None:
            try:
                queue_item_id = _positive_integer(
                    queue_item_value,
                    field_name=f"reservation {reservation_id} queue_item_id",
                )
            except V5ReservationError as exc:
                raise V5ReservationEvidenceError(str(exc)) from exc
        else:
            queue_item_id = None
        starts_at = _stored_optional_text(
            row, "starts_at", reservation_id=reservation_id, maximum=64
        )
        expires_at = _stored_optional_text(
            row, "expires_at", reservation_id=reservation_id, maximum=64
        )
        released_at = _stored_optional_text(
            row, "released_at", reservation_id=reservation_id, maximum=64
        )
        released_by = _stored_optional_text(
            row, "released_by", reservation_id=reservation_id, maximum=256
        )
        state_detail = _stored_optional_text(
            row, "state_detail", reservation_id=reservation_id, maximum=4000
        )
        if (starts_at is None) != (expires_at is None):
            raise V5ReservationEvidenceError(
                f"reservation {reservation_id} must store starts_at and expires_at together"
            )
        start_time: datetime | None = None
        expiry_time: datetime | None = None
        if starts_at is not None and expires_at is not None:
            try:
                start_time = _parse_timestamp(
                    starts_at,
                    field_name=f"reservation {reservation_id} starts_at",
                )
                expiry_time = _parse_timestamp(
                    expires_at,
                    field_name=f"reservation {reservation_id} expires_at",
                )
            except V5ReservationError as exc:
                raise V5ReservationEvidenceError(str(exc)) from exc
            if start_time < requested_time:
                raise V5ReservationEvidenceError(
                    f"reservation {reservation_id} starts before it was requested"
                )
            if expiry_time != start_time + timedelta(hours=duration):
                raise V5ReservationEvidenceError(
                    f"reservation {reservation_id} expiry does not equal starts_at "
                    f"plus {duration} hours"
                )
        if status is V5ReservationStatus.PENDING:
            if queue_item_id is None or starts_at is not None or released_at is not None:
                raise V5ReservationEvidenceError(
                    f"pending reservation {reservation_id} has invalid ownership/time fields"
                )
        elif status in {V5ReservationStatus.ACTIVE, V5ReservationStatus.EXPIRED}:
            if starts_at is None or released_at is not None or released_by is not None:
                raise V5ReservationEvidenceError(
                    f"{status.value} reservation {reservation_id} has invalid time fields"
                )
        elif status is V5ReservationStatus.RELEASED:
            if released_at is None or released_by is None:
                raise V5ReservationEvidenceError(
                    f"released reservation {reservation_id} lacks release evidence"
                )
        if (released_at is None) != (released_by is None):
            raise V5ReservationEvidenceError(
                f"reservation {reservation_id} has incomplete release evidence"
            )
        if released_at is not None:
            try:
                release_time = _parse_timestamp(
                    released_at,
                    field_name=f"reservation {reservation_id} released_at",
                )
            except V5ReservationError as exc:
                raise V5ReservationEvidenceError(str(exc)) from exc
            earliest = start_time if start_time is not None else requested_time
            if release_time < earliest:
                raise V5ReservationEvidenceError(
                    f"reservation {reservation_id} was released before its open period"
                )
        return V5GpuReservation(
            id=reservation_id,
            gpu_uuid=uuid,
            queue_item_id=queue_item_id,
            status=status,
            requested_at=requested_at,
            requested_by=requested_by,
            note=note,
            duration_hours=duration,
            starts_at=starts_at,
            expires_at=expires_at,
            released_at=released_at,
            released_by=released_by,
            state_detail=state_detail,
        )

    def get_reservation(self, reservation_id: int) -> V5GpuReservation:
        """Load and authenticate one reservation row by global ID."""

        key = _positive_integer(reservation_id, field_name="reservation_id")
        with self._connection(operation=f"load reservation {key}", write=False) as connection:
            row = connection.execute(
                "SELECT * FROM gpu_reservations WHERE id = ?", (key,)
            ).fetchone()
            if row is None:
                raise V5ReservationNotFoundError(
                    f"schema-v5 has no GPU reservation with id {key}"
                )
            return self._reservation_from_row(row)

    def list_reservations(
        self,
        *,
        gpu_uuid: str | None = None,
        requested_by: str | None = None,
        open_only: bool = False,
    ) -> tuple[V5GpuReservation, ...]:
        """Return newest-first records with role-neutral exact-value filters.

        Authorization belongs to the caller.  In particular, a reserver-facing
        adapter can pass its authenticated subject as ``requested_by`` without
        teaching this storage boundary about HTTP roles.
        """

        if type(open_only) is not bool:
            raise TypeError(f"open_only must be boolean, got {type(open_only).__name__}")
        clauses: list[str] = []
        parameters: list[object] = []
        if gpu_uuid is not None:
            clauses.append("gpu_uuid = ?")
            parameters.append(_gpu_uuid(gpu_uuid))
        if requested_by is not None:
            clauses.append("requested_by = ?")
            parameters.append(
                _text(requested_by, field_name="requested_by", maximum=256)
            )
        if open_only:
            clauses.append("status IN ('pending', 'active')")
        where = "" if not clauses else " WHERE " + " AND ".join(clauses)
        with self._connection(operation="list GPU reservations", write=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM gpu_reservations{where} ORDER BY id DESC",
                tuple(parameters),
            ).fetchall()
            return tuple(self._reservation_from_row(row) for row in rows)

    def open_gpu_uuids(self) -> frozenset[str]:
        """Return exact GPU UUIDs blocked by pending or active reservations."""

        return frozenset(
            reservation.gpu_uuid
            for reservation in self.list_reservations(open_only=True)
        )

    def request_reservation(
        self,
        gpu_uuid: str,
        *,
        duration_hours: int,
        note: str,
        requested_by: str,
        requested_at: str,
    ) -> V5GpuReservation:
        """Create or idempotently return one passive reservation request.

        Only an exact enabled, undrained allowlist UUID is accepted.  If one
        queue item currently owns the GPU, the returned row is ``pending`` and
        points to that item for explanation only; the queue item is untouched.
        """

        uuid = _gpu_uuid(gpu_uuid)
        duration = _duration(duration_hours)
        cleaned_note = _note(note)
        actor = _text(requested_by, field_name="requested_by", maximum=256)
        timestamp, requested_time = _timestamp(
            requested_at, field_name="requested_at"
        )
        with self._connection(
            operation=f"request reservation for GPU {uuid}", write=True
        ) as connection:
            duplicates = connection.execute(
                """
                SELECT * FROM gpu_reservations
                WHERE gpu_uuid = ? AND requested_at = ? AND requested_by = ?
                  AND note = ? AND duration_hours = ?
                ORDER BY id
                """,
                (uuid, timestamp, actor, cleaned_note, duration),
            ).fetchall()
            if len(duplicates) > 1:
                raise V5ReservationEvidenceError(
                    "the immutable reservation request tuple identifies multiple "
                    f"rows for GPU {uuid}; repair copied state before retrying"
                )
            if duplicates:
                return self._reservation_from_row(duplicates[0])
            allowlist = connection.execute(
                "SELECT uuid, enabled, draining FROM gpu_allowlist WHERE uuid = ?",
                (uuid,),
            ).fetchone()
            if allowlist is None:
                raise V5ReservationError(
                    f"GPU {uuid!r} is not an exact allowlisted UUID; refresh the "
                    "allowlist and retry with the full UUID, not an index or name"
                )
            if str(allowlist["uuid"]) != uuid:
                raise V5ReservationEvidenceError(
                    f"allowlist lookup for GPU {uuid!r} returned a different identity"
                )
            enabled = int(allowlist["enabled"])
            draining = int(allowlist["draining"])
            if enabled not in {0, 1} or draining not in {0, 1}:
                raise V5ReservationEvidenceError(
                    f"GPU {uuid!r} has invalid allowlist flags"
                )
            if not enabled or draining:
                state = "disabled" if not enabled else "draining"
                raise V5ReservationError(
                    f"GPU {uuid!r} is {state}; reservations require an enabled, "
                    "undrained allowlist UUID"
                )
            existing = connection.execute(
                """
                SELECT id, status FROM gpu_reservations
                WHERE gpu_uuid = ? AND status IN ('pending', 'active')
                """,
                (uuid,),
            ).fetchone()
            if existing is not None:
                raise V5ReservationError(
                    f"GPU {uuid!r} already has open reservation {int(existing['id'])} "
                    f"in {str(existing['status'])!r} state; release it or wait for expiry"
                )
            assignments = self._active_assignments(connection, uuid)
            if len(assignments) > 1:
                raise V5ReservationEvidenceError(
                    f"GPU {uuid!r} is assigned to multiple active queue items "
                    f"{list(assignments)}; no reservation was created"
                )
            if assignments:
                queue_item_id = assignments[0]
                status = V5ReservationStatus.PENDING
                starts_at = None
                expires_at = None
                state_detail = (
                    f"waiting for queue item {queue_item_id} to finish on GPU {uuid}"
                )
            else:
                queue_item_id = None
                status = V5ReservationStatus.ACTIVE
                starts_at = timestamp
                expires_at = (requested_time + timedelta(hours=duration)).isoformat(
                    timespec=(
                        "microseconds" if requested_time.microsecond else "seconds"
                    )
                )
                state_detail = None
            cursor = connection.execute(
                """
                INSERT INTO gpu_reservations(
                    gpu_uuid, queue_item_id, status, requested_at, requested_by,
                    note, duration_hours, starts_at, expires_at, state_detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid,
                    queue_item_id,
                    status.value,
                    timestamp,
                    actor,
                    cleaned_note,
                    duration,
                    starts_at,
                    expires_at,
                    state_detail,
                ),
            )
            reservation_id = int(cursor.lastrowid)
            self._event(
                connection,
                created_at=timestamp,
                actor=actor,
                event_type="gpu_reservation_requested",
                payload={
                    "reservationId": reservation_id,
                    "gpuUuid": uuid,
                    "status": status.value,
                    "durationHours": duration,
                    "waitingForQueueItemId": queue_item_id,
                },
            )
            row = connection.execute(
                "SELECT * FROM gpu_reservations WHERE id = ?", (reservation_id,)
            ).fetchone()
            assert row is not None
            return self._reservation_from_row(row)

    def release_reservation(
        self,
        reservation_id: int,
        *,
        released_by: str,
        released_at: str,
    ) -> V5GpuReservation:
        """Release a pending or active reservation without touching queue work.

        Repeating the exact release actor/timestamp is idempotent.  Releasing a
        passive pending request is safe because this service never initiated a
        checkpoint or any other process transition.
        """

        key = _positive_integer(reservation_id, field_name="reservation_id")
        actor = _text(released_by, field_name="released_by", maximum=256)
        timestamp, release_time = _timestamp(released_at, field_name="released_at")
        with self._connection(operation=f"release reservation {key}", write=True) as connection:
            row = connection.execute(
                "SELECT * FROM gpu_reservations WHERE id = ?", (key,)
            ).fetchone()
            if row is None:
                raise V5ReservationNotFoundError(
                    f"schema-v5 has no GPU reservation with id {key}"
                )
            reservation = self._reservation_from_row(row)
            if reservation.status is V5ReservationStatus.RELEASED:
                if (
                    reservation.released_by == actor
                    and reservation.released_at == timestamp
                ):
                    return reservation
                raise V5ReservationError(
                    f"GPU reservation {key} was already released at "
                    f"{reservation.released_at}; no state changed"
                )
            if not reservation.is_open:
                raise V5ReservationError(
                    f"GPU reservation {key} is already {reservation.status.value!r}; "
                    "only pending or active reservations can be released"
                )
            earliest = _parse_timestamp(
                reservation.starts_at or reservation.requested_at,
                field_name=f"reservation {key} open time",
            )
            if release_time < earliest:
                raise V5ReservationError(
                    f"released_at precedes reservation {key} open time {earliest.isoformat()}"
                )
            detail = (
                "released while waiting for the assigned queue item"
                if reservation.status is V5ReservationStatus.PENDING
                else "released early"
            )
            cursor = connection.execute(
                """
                UPDATE gpu_reservations
                SET status = 'released', released_at = ?, released_by = ?,
                    state_detail = ?
                WHERE id = ? AND status IN ('pending', 'active')
                """,
                (timestamp, actor, detail, key),
            )
            if cursor.rowcount != 1:
                raise V5ReservationError(
                    f"GPU reservation {key} changed concurrently; reload it before retrying"
                )
            self._event(
                connection,
                created_at=timestamp,
                actor=actor,
                event_type="gpu_reservation_released",
                payload={
                    "reservationId": key,
                    "gpuUuid": reservation.gpu_uuid,
                    "previousStatus": reservation.status.value,
                },
            )
            updated = connection.execute(
                "SELECT * FROM gpu_reservations WHERE id = ?", (key,)
            ).fetchone()
            assert updated is not None
            return self._reservation_from_row(updated)

    def reconcile(
        self,
        *,
        reconciled_at: str,
        actor: str = "scheduler",
    ) -> V5ReservationReconciliation:
        """Atomically activate free pending rows and expire elapsed active rows.

        Expiration is inclusive at ``expires_at``.  Pending duration begins at
        this pass's timestamp, not at the original request.  Repeating a pass at
        the same time is idempotent and returns no additional transitions.
        """

        timestamp, current_time = _timestamp(
            reconciled_at, field_name="reconciled_at"
        )
        event_actor = _text(actor, field_name="actor", maximum=256)
        activated: list[V5GpuReservation] = []
        expired: list[V5GpuReservation] = []
        failed: list[V5GpuReservation] = []
        with self._connection(operation="reconcile GPU reservations", write=True) as connection:
            rows = connection.execute(
                """
                SELECT * FROM gpu_reservations
                WHERE status IN ('pending', 'active') ORDER BY id
                """
            ).fetchall()
            for row in rows:
                reservation = self._reservation_from_row(row)
                if reservation.status is V5ReservationStatus.ACTIVE:
                    assert reservation.expires_at is not None
                    expiry_time = _parse_timestamp(
                        reservation.expires_at,
                        field_name=f"reservation {reservation.id} expires_at",
                    )
                    if current_time < expiry_time:
                        continue
                    connection.execute(
                        """
                        UPDATE gpu_reservations
                        SET status = 'expired', state_detail = ?
                        WHERE id = ? AND status = 'active'
                        """,
                        ("reservation duration elapsed", reservation.id),
                    )
                    self._event(
                        connection,
                        created_at=timestamp,
                        actor=event_actor,
                        event_type="gpu_reservation_expired",
                        payload={
                            "reservationId": reservation.id,
                            "gpuUuid": reservation.gpu_uuid,
                            "expiresAt": reservation.expires_at,
                        },
                    )
                    changed = connection.execute(
                        "SELECT * FROM gpu_reservations WHERE id = ?",
                        (reservation.id,),
                    ).fetchone()
                    assert changed is not None
                    expired.append(self._reservation_from_row(changed))
                    continue
                requested_time = _parse_timestamp(
                    reservation.requested_at,
                    field_name=f"reservation {reservation.id} requested_at",
                )
                if current_time < requested_time:
                    continue
                allowlist = connection.execute(
                    "SELECT uuid FROM gpu_allowlist WHERE uuid = ?",
                    (reservation.gpu_uuid,),
                ).fetchone()
                if allowlist is None:
                    detail = (
                        f"GPU {reservation.gpu_uuid} is no longer present in the allowlist"
                    )
                    connection.execute(
                        """
                        UPDATE gpu_reservations
                        SET status = 'failed', state_detail = ?
                        WHERE id = ? AND status = 'pending'
                        """,
                        (detail, reservation.id),
                    )
                    self._event(
                        connection,
                        created_at=timestamp,
                        actor=event_actor,
                        event_type="gpu_reservation_failed",
                        payload={
                            "reservationId": reservation.id,
                            "gpuUuid": reservation.gpu_uuid,
                            "reason": detail,
                        },
                    )
                    changed = connection.execute(
                        "SELECT * FROM gpu_reservations WHERE id = ?",
                        (reservation.id,),
                    ).fetchone()
                    assert changed is not None
                    failed.append(self._reservation_from_row(changed))
                    continue
                assignments = self._active_assignments(connection, reservation.gpu_uuid)
                if len(assignments) > 1:
                    raise V5ReservationEvidenceError(
                        f"GPU {reservation.gpu_uuid!r} is assigned to multiple active "
                        f"queue items {list(assignments)}; reservation "
                        f"{reservation.id} remains pending"
                    )
                if assignments:
                    continue
                starts_at = timestamp
                expires_at = (current_time + timedelta(hours=reservation.duration_hours)).isoformat(
                    timespec="microseconds" if current_time.microsecond else "seconds"
                )
                cursor = connection.execute(
                    """
                    UPDATE gpu_reservations
                    SET status = 'active', starts_at = ?, expires_at = ?,
                        state_detail = ?
                    WHERE id = ? AND status = 'pending'
                    """,
                    (
                        starts_at,
                        expires_at,
                        "assigned queue item finished; reservation duration started",
                        reservation.id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise V5ReservationError(
                        f"GPU reservation {reservation.id} changed during reconciliation"
                    )
                self._event(
                    connection,
                    created_at=timestamp,
                    actor=event_actor,
                    event_type="gpu_reservation_activated",
                    payload={
                        "reservationId": reservation.id,
                        "gpuUuid": reservation.gpu_uuid,
                        "startsAt": starts_at,
                        "expiresAt": expires_at,
                    },
                )
                changed = connection.execute(
                    "SELECT * FROM gpu_reservations WHERE id = ?",
                    (reservation.id,),
                ).fetchone()
                assert changed is not None
                activated.append(self._reservation_from_row(changed))
        return V5ReservationReconciliation(
            reconciled_at=timestamp,
            activated=tuple(activated),
            expired=tuple(expired),
            failed=tuple(failed),
        )


__all__ = [
    "MAX_RESERVATION_HOURS",
    "MIN_RESERVATION_HOURS",
    "V5GpuReservation",
    "V5ReservationError",
    "V5ReservationEvidenceError",
    "V5ReservationNotFoundError",
    "V5ReservationReconciliation",
    "V5ReservationService",
    "V5ReservationStatus",
]
