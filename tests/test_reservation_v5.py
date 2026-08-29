"""Verify passive, transactional schema-v5 GPU reservation behavior."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import threading

import pytest

from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.reservation_v5 import (
    V5ReservationError,
    V5ReservationService,
    V5ReservationStatus,
)


NOW = "2026-08-28T12:00:00+00:00"
SHA = "0" * 64
COMMIT = "a" * 40


@pytest.fixture
def store(tmp_path: Path) -> V5QueueStore:
    value = V5QueueStore((tmp_path / "state").resolve())
    value.initialize()
    return value


def _allow_gpu(
    store: V5QueueStore,
    uuid: str,
    *,
    index: str = "0",
    enabled: int = 1,
    draining: int = 0,
) -> None:
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name,
                enabled, draining, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (uuid, uuid, index, f"Fixture {index}", enabled, draining, NOW),
        )


def _running_item(
    store: V5QueueStore,
    *,
    gpu_uuid: str,
    item_id: int = 101,
    project_id: int = 1,
) -> None:
    revision_id = project_id * 10 + 1
    project_key = f"project-{project_id}"
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO projects(
                id, project_key, display_name, lifecycle, current_revision_id,
                current_revision_sequence, created_at, created_by,
                lifecycle_changed_at, lifecycle_actor, lifecycle_reason
            ) VALUES (?, ?, ?, 'active', ?, 1, ?, 'tester', ?, 'tester', 'fixture')
            """,
            (project_id, project_key, project_key, revision_id, NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, checkout_path, enrollment_json,
                enrollment_sha256, created_at, created_actor
            ) VALUES (?, ?, 1, ?, 'legacy-v4', ?, ?, ?, X'7b7d', ?, ?, 'tester')
            """,
            (
                revision_id,
                project_id,
                f"{project_key}:r1",
                project_key,
                COMMIT,
                f"/tmp/{project_key}",
                SHA,
                NOW,
            ),
        )
        connection.execute(
            """
            INSERT INTO project_runtime_state(
                project_id, health_reason, health_actor, health_changed_at
            ) VALUES (?, 'healthy', 'tester', ?)
            """,
            (project_id, NOW),
        )
        connection.execute(
            """
            INSERT INTO queue_items(
                id, project_id, revision_id, admission_kind, snapshot_id,
                job_id, experiment_id, attempt, state, card_path, card_sha256,
                command_text, runner_name, git_commit, added_at, added_by,
                assigned_gpu_uuid, assigned_gpu_index, runtime_gpu_lease_held
            ) VALUES (?, ?, ?, 'LegacyMarkdownCard/v0', NULL, NULL, ?, 1,
                      'running', 'cards/fixture.md', ?, 'true', 'fixture', ?,
                      ?, 'tester', ?, '0', 1)
            """,
            (
                item_id,
                project_id,
                revision_id,
                f"EXP-{item_id}",
                SHA,
                COMMIT,
                NOW,
                gpu_uuid,
            ),
        )


def _request(
    service: V5ReservationService,
    gpu_uuid: str,
    *,
    note: str = "Alex — local benchmark",
    requested_by: str = "reserver:alex",
    requested_at: str = NOW,
    duration_hours: int = 2,
):
    return service.request_reservation(
        gpu_uuid,
        duration_hours=duration_hours,
        note=note,
        requested_by=requested_by,
        requested_at=requested_at,
    )


def test_idle_request_uses_exact_schedulable_uuid_and_role_neutral_list(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-exact-0000")
    _allow_gpu(store, "GPU-disabled", index="1", enabled=0)
    _allow_gpu(store, "GPU-draining", index="2", draining=1)
    service = V5ReservationService(store)

    with pytest.raises(V5ReservationError, match="exact allowlisted UUID"):
        _request(service, "0")
    with pytest.raises(V5ReservationError, match="disabled"):
        _request(service, "GPU-disabled")
    with pytest.raises(V5ReservationError, match="draining"):
        _request(service, "GPU-draining")

    reservation = _request(service, "GPU-exact-0000")
    assert reservation.status is V5ReservationStatus.ACTIVE
    assert reservation.starts_at == NOW
    assert reservation.expires_at == "2026-08-28T14:00:00+00:00"
    assert reservation.queue_item_id is None
    assert service.open_gpu_uuids() == frozenset({"GPU-exact-0000"})
    assert service.list_reservations(requested_by="reserver:alex") == (reservation,)
    assert service.list_reservations(requested_by="reserver:other") == ()
    with store.connect() as connection:
        event = connection.execute(
            "SELECT scope, project_id, queue_item_id, actor, event_type "
            "FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert tuple(event) == (
        "host",
        None,
        None,
        "reserver:alex",
        "gpu_reservation_requested",
    )


@pytest.mark.parametrize("duration", [True, 0, 25, 1.5])
def test_duration_is_one_through_twenty_four_whole_hours(
    store: V5QueueStore, duration: object
) -> None:
    _allow_gpu(store, "GPU-duration")
    with pytest.raises(V5ReservationError, match="whole number"):
        V5ReservationService(store).request_reservation(
            "GPU-duration",
            duration_hours=duration,  # type: ignore[arg-type]
            note="duration validation",
            requested_by="tester",
            requested_at=NOW,
        )


def test_duration_boundaries_are_accepted(store: V5QueueStore) -> None:
    _allow_gpu(store, "GPU-one-hour")
    _allow_gpu(store, "GPU-twenty-four-hours", index="1")
    service = V5ReservationService(store)

    one = _request(service, "GPU-one-hour", duration_hours=1)
    twenty_four = _request(
        service,
        "GPU-twenty-four-hours",
        note="day-long reservation",
        requested_by="reserver:day",
        duration_hours=24,
    )
    assert one.expires_at == "2026-08-28T13:00:00+00:00"
    assert twenty_four.expires_at == "2026-08-29T12:00:00+00:00"


def test_pending_waits_without_preemption_then_activates_when_job_finishes(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-busy")
    _allow_gpu(store, "GPU-independent", index="1")
    _running_item(store, gpu_uuid="GPU-busy")
    service = V5ReservationService(store)

    pending = _request(service, "GPU-busy", duration_hours=3)
    independent = _request(
        service,
        "GPU-independent",
        note="Morgan — independent GPU",
        requested_by="reserver:morgan",
        duration_hours=4,
    )
    assert pending.status is V5ReservationStatus.PENDING
    assert pending.queue_item_id == 101
    assert pending.starts_at is None
    assert independent.status is V5ReservationStatus.ACTIVE
    with store.connect() as connection:
        item = connection.execute(
            "SELECT state, yield_request_id, yield_requested_at, yield_requested_by "
            "FROM queue_items WHERE id = 101"
        ).fetchone()
    assert tuple(item) == ("running", None, None, None)

    unchanged = service.reconcile(
        reconciled_at="2026-08-28T12:30:00+00:00"
    )
    assert unchanged.changed == ()
    with store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'succeeded', finished_at = ? WHERE id = 101",
            ("2026-08-28T12:31:00+00:00",),
        )
    still_pending = service.reconcile(
        reconciled_at="2026-08-28T12:31:00+00:00"
    )
    assert still_pending.changed == ()
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET runtime_gpu_lease_held = 0,
                runtime_gpu_lease_released_at = ?
            WHERE id = 101
            """,
            ("2026-08-28T12:31:01+00:00",),
        )
    changed = service.reconcile(reconciled_at="2026-08-28T12:31:01+00:00")
    assert [row.id for row in changed.activated] == [pending.id]
    active = changed.activated[0]
    assert active.status is V5ReservationStatus.ACTIVE
    assert active.starts_at == "2026-08-28T12:31:01+00:00"
    assert active.expires_at == "2026-08-28T15:31:01+00:00"
    assert service.open_gpu_uuids() == frozenset(
        {"GPU-busy", "GPU-independent"}
    )
    expiry = service.reconcile(reconciled_at="2026-08-28T15:31:01+00:00")
    assert [row.id for row in expiry.expired] == [pending.id]
    assert service.open_gpu_uuids() == frozenset({"GPU-independent"})


def test_expiration_uses_inclusive_exact_boundary_and_is_idempotent(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-boundary")
    service = V5ReservationService(store)
    reservation = _request(
        service,
        "GPU-boundary",
        duration_hours=1,
    )

    before = service.reconcile(
        reconciled_at="2026-08-28T12:59:59.999999+00:00"
    )
    assert before.changed == ()
    assert service.get_reservation(reservation.id).status is V5ReservationStatus.ACTIVE

    boundary = service.reconcile(
        reconciled_at="2026-08-28T13:00:00+00:00"
    )
    assert [row.id for row in boundary.expired] == [reservation.id]
    assert boundary.expired[0].status is V5ReservationStatus.EXPIRED
    assert service.open_gpu_uuids() == frozenset()
    assert service.reconcile(
        reconciled_at="2026-08-28T13:00:00+00:00"
    ).changed == ()
    with store.connect() as connection:
        enabled = connection.execute(
            "SELECT enabled FROM gpu_allowlist WHERE uuid = 'GPU-boundary'"
        ).fetchone()[0]
    assert enabled == 1


def test_exact_request_and_release_retries_are_idempotent(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-idempotent")
    service = V5ReservationService(store)
    first = _request(service, "GPU-idempotent")
    retried = _request(
        service,
        "GPU-idempotent",
        note="  Alex   — local benchmark  ",
    )
    assert retried == first
    assert len(service.list_reservations()) == 1

    released = service.release_reservation(
        first.id,
        released_by="reserver:alex",
        released_at="2026-08-28T12:15:00+00:00",
    )
    assert released.status is V5ReservationStatus.RELEASED
    assert service.release_reservation(
        first.id,
        released_by="reserver:alex",
        released_at="2026-08-28T12:15:00+00:00",
    ) == released
    with pytest.raises(V5ReservationError, match="already released"):
        service.release_reservation(
            first.id,
            released_by="reserver:other",
            released_at="2026-08-28T12:16:00+00:00",
        )


def test_pending_request_can_be_cancelled_without_touching_running_item(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-cancel")
    _running_item(store, gpu_uuid="GPU-cancel")
    service = V5ReservationService(store)
    pending = _request(service, "GPU-cancel")

    released = service.release_reservation(
        pending.id,
        released_by="reserver:alex",
        released_at="2026-08-28T12:01:00+00:00",
    )
    assert released.status is V5ReservationStatus.RELEASED
    assert released.starts_at is None
    assert service.open_gpu_uuids() == frozenset()
    with store.connect() as connection:
        item = connection.execute(
            "SELECT state, assigned_gpu_uuid, yield_request_id FROM queue_items WHERE id = 101"
        ).fetchone()
    assert tuple(item) == ("running", "GPU-cancel", None)


def test_concurrent_exact_retry_converges_and_distinct_overlap_is_rejected(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-concurrent")
    barrier = threading.Barrier(2)

    def exact_worker():
        barrier.wait(timeout=5)
        return _request(V5ReservationService(store), "GPU-concurrent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        exact_results = tuple(executor.map(lambda _value: exact_worker(), range(2)))
    assert exact_results[0].id == exact_results[1].id
    assert len(V5ReservationService(store).list_reservations()) == 1

    V5ReservationService(store).release_reservation(
        exact_results[0].id,
        released_by="reserver:alex",
        released_at="2026-08-28T12:01:00+00:00",
    )
    conflict_barrier = threading.Barrier(2)

    def distinct_worker(index: int):
        conflict_barrier.wait(timeout=5)
        try:
            return _request(
                V5ReservationService(store),
                "GPU-concurrent",
                note=f"distinct request {index}",
                requested_by=f"reserver:{index}",
                requested_at=f"2026-08-28T12:0{index + 2}:00+00:00",
            )
        except V5ReservationError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(distinct_worker, range(2)))
    assert sum(not isinstance(result, Exception) for result in results) == 1
    errors = [result for result in results if isinstance(result, Exception)]
    assert len(errors) == 1
    assert "already has open reservation" in str(errors[0])
    assert len(V5ReservationService(store).list_reservations(open_only=True)) == 1


def test_missing_allowlist_identity_fails_only_its_pending_reservation(
    store: V5QueueStore,
) -> None:
    _allow_gpu(store, "GPU-removed")
    _allow_gpu(store, "GPU-healthy", index="1")
    _running_item(store, gpu_uuid="GPU-removed")
    service = V5ReservationService(store)
    pending = _request(service, "GPU-removed")
    healthy = _request(
        service,
        "GPU-healthy",
        requested_by="reserver:healthy",
        note="healthy reservation",
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'succeeded' WHERE id = 101"
        )
        connection.execute(
            "DELETE FROM gpu_allowlist WHERE uuid = 'GPU-removed'"
        )

    result = service.reconcile(reconciled_at="2026-08-28T12:10:00+00:00")
    assert [row.id for row in result.failed] == [pending.id]
    assert result.failed[0].status is V5ReservationStatus.FAILED
    assert service.get_reservation(healthy.id).status is V5ReservationStatus.ACTIVE
    assert service.open_gpu_uuids() == frozenset({"GPU-healthy"})
