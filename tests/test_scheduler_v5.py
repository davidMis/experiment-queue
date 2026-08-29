"""Verify global ordering, atomic claims, and schema-v5 failure isolation."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.executor import ExecutorReceipt
from experiment_queue.project_worktrees import ProjectWorktreeEvidence
from experiment_queue.scheduler_v5 import (
    DiskCapacity,
    FailureScope,
    V5SchedulerError,
    V5SchedulingController,
)


NOW = "2026-08-28T16:00:00+00:00"
SHA = "0" * 64


@pytest.fixture
def store(tmp_path: Path) -> V5QueueStore:
    value = V5QueueStore((tmp_path / "state").resolve())
    value.initialize()
    return value


def _project(
    connection: sqlite3.Connection,
    *,
    project_id: int,
    key: str,
    lifecycle: str = "active",
    health: str = "closed",
    artifact_root: Path | None = None,
) -> None:
    revision_id = project_id * 10
    commit = f"{project_id:x}" * 40
    connection.execute(
        """
        INSERT INTO projects(
            id, project_key, display_name, lifecycle, current_revision_id,
            current_revision_sequence, created_at, created_by,
            lifecycle_changed_at, lifecycle_actor, lifecycle_reason
        ) VALUES (?, ?, ?, ?, ?, 1, ?, 'tester', ?, 'tester', 'fixture')
        """,
        (project_id, key, key, lifecycle, revision_id, NOW, NOW),
    )
    connection.execute(
        """
        INSERT INTO project_revisions(
            id, project_id, sequence, revision_label, revision_kind,
            display_name, git_commit, checkout_path, enrollment_json,
            enrollment_sha256, created_at, created_actor
        ) VALUES (?, ?, 1, ?, 'legacy-v4', ?, ?, ?, ?, ?, ?, 'tester')
        """,
        (
            revision_id,
            project_id,
            f"{key}:r1",
            key,
            commit,
            f"/tmp/{key}",
            b"{}",
            SHA,
            NOW,
        ),
    )
    connection.execute(
        """
        INSERT INTO project_runtime_state(
            project_id, health, circuit_failure_count, health_reason,
            health_actor, health_changed_at
        ) VALUES (?, ?, ?, 'fixture health', 'tester', ?)
        """,
        (project_id, health, 1 if health == "open" else 0, NOW),
    )
    if artifact_root is not None:
        connection.execute(
            """
            INSERT INTO project_mounts(
                project_id, revision_id, mount_name, mount_path,
                declared_access, access, required
            ) VALUES (?, ?, 'outputs', ?, 'readWrite', 'readWrite', 1)
            """,
            (project_id, revision_id, str(artifact_root)),
        )
        connection.execute(
            """
            INSERT INTO project_artifact_roots(project_id, revision_id, mount_name)
            VALUES (?, ?, 'outputs')
            """,
            (project_id, revision_id),
        )


def _item(
    connection: sqlite3.Connection,
    *,
    item_id: int,
    project_id: int,
    priority: int = 0,
    resume_front: bool = False,
    state: str = "queued",
) -> None:
    key = str(
        connection.execute(
            "SELECT project_key FROM projects WHERE id = ?", (project_id,)
        ).fetchone()[0]
    )
    commit = str(
        connection.execute(
            "SELECT git_commit FROM project_revisions WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0]
    )
    active = state in {
        "starting",
        "running",
        "yielding",
        "terminating",
        "force_killing",
    }
    connection.execute(
        """
        INSERT INTO queue_items(
            id, project_id, revision_id, admission_kind, snapshot_id, job_id,
            experiment_id, attempt, state, priority, card_path, card_sha256,
            command_text, runner_name, git_commit, added_at, added_by,
            resume_front, assigned_gpu_uuid, assigned_gpu_index,
            runtime_gpu_lease_held
        ) VALUES (?, ?, ?, 'LegacyMarkdownCard/v0', NULL, NULL, ?, 1, ?, ?,
                  'cards/example.md', ?, 'true', 'legacy', ?, ?, 'tester', ?,
                  ?, ?, ?)
        """,
        (
            item_id,
            project_id,
            project_id * 10,
            f"{key}-item-{item_id}",
            state,
            priority,
            SHA,
            commit,
            NOW,
            int(resume_front),
            "GPU-1" if active else None,
            "0" if active else None,
            int(active),
        ),
    )


def _allow_gpu(connection: sqlite3.Connection, *, uuid: str = "GPU-1") -> None:
    """Enroll one enabled, non-draining GPU for claim-transaction tests."""

    connection.execute(
        """
        INSERT INTO gpu_allowlist(
            uuid, requested_identifier, last_index, name, enabled, draining,
            updated_at
        ) VALUES (?, '0', '0', 'Fixture GPU', 1, 0, ?)
        """,
        (uuid, NOW),
    )


def test_candidates_preserve_global_order_and_skip_unhealthy_projects_and_dependencies(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _project(connection, project_id=2, key="project-two")
        _project(connection, project_id=3, key="project-paused", lifecycle="paused")
        _item(connection, item_id=101, project_id=1, priority=10)
        _item(connection, item_id=102, project_id=1, priority=10, resume_front=True)
        _item(connection, item_id=103, project_id=1, priority=100)
        _item(connection, item_id=104, project_id=1)
        _item(connection, item_id=201, project_id=2, priority=20)
        _item(connection, item_id=301, project_id=3, priority=999)
        connection.execute(
            "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (103, 104)"
        )

    controller = V5SchedulingController(store)
    assert [item.id for item in controller.list_dispatch_candidates()] == [
        201,
        102,
        101,
        104,
    ]

    with store.connect() as connection:
        connection.execute(
            "UPDATE project_runtime_state SET health = 'open', "
            "circuit_failure_count = 1 WHERE project_id = 2"
        )
        connection.execute("UPDATE queue_items SET state = 'succeeded' WHERE id = 104")
    assert [item.id for item in controller.list_dispatch_candidates()] == [103, 102, 101]


def test_claim_rechecks_global_project_and_dependency_predicates_atomically(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, priority=10)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.list_dispatch_candidates()[0].id == 101

    with store.connect() as connection:
        connection.execute(
            "UPDATE projects SET lifecycle = 'paused', lifecycle_reason = 'test' "
            "WHERE id = 1"
        )
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is None

    with store.connect() as connection:
        connection.execute("UPDATE projects SET lifecycle = 'active' WHERE id = 1")
    controller.pause_host(reason="operator check", actor="operator", changed_at=NOW)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is None
    controller.resume_host(actor="operator", changed_at=NOW)

    claimed = controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    )
    assert claimed is not None and claimed.project_key == "project-one"
    with store.connect() as connection:
        item = connection.execute(
            "SELECT state, assigned_gpu_uuid, resume_front FROM queue_items WHERE id = 101"
        ).fetchone()
        event = connection.execute(
            "SELECT scope, project_id, queue_item_id FROM events "
            "WHERE event_type = 'EXPERIMENT_STARTING'"
        ).fetchone()
    assert tuple(item) == ("starting", "GPU-1", 0)
    assert tuple(event) == ("project", 1, 101)


@pytest.mark.parametrize(
    "gpu_change",
    ("UPDATE gpu_allowlist SET draining = 1", "UPDATE gpu_allowlist SET enabled = 0"),
)
def test_claim_rechecks_allowlist_after_candidate_selection(
    store: V5QueueStore,
    gpu_change: str,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.list_dispatch_candidates()[0].id == 101

    with store.connect() as connection:
        connection.execute(gpu_change)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is None
    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, assigned_gpu_uuid FROM queue_items WHERE id = 101"
        ).fetchone()
    assert tuple(row) == ("queued", None)


def test_claim_rejects_gpu_with_another_active_assignment(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _project(connection, project_id=2, key="project-two")
        _item(connection, item_id=101, project_id=1)
        _item(connection, item_id=201, project_id=2)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    assert controller.claim(
        201,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is None
    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, assigned_gpu_uuid FROM queue_items WHERE id = 201"
        ).fetchone()
    assert tuple(row) == ("queued", None)


def test_project_quarantine_does_not_pause_host_or_block_healthy_project(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _project(connection, project_id=2, key="project-two")
        _item(connection, item_id=101, project_id=1, priority=100)
        _item(connection, item_id=201, project_id=2, priority=10)
    controller = V5SchedulingController(store)

    assert controller.quarantine_project(
        1,
        reason="repository identity changed",
        actor="scheduler",
        changed_at=NOW,
        queue_item_id=101,
    )
    assert controller.host_dispatch_state() == (False, "")
    assert [item.id for item in controller.list_dispatch_candidates()] == [201]
    assert not controller.quarantine_project(
        1,
        reason="repository identity changed",
        actor="scheduler",
        changed_at=NOW,
    )

    with store.connect() as connection:
        event = connection.execute(
            "SELECT scope, project_id, queue_item_id FROM events "
            "WHERE event_type = 'PROJECT_CIRCUIT_OPENED'"
        ).fetchone()
    assert tuple(event) == ("project", 1, 101)

    assert controller.pause_host(
        reason="GPU telemetry unavailable", actor="scheduler", changed_at=NOW
    )
    assert controller.list_dispatch_candidates() == ()
    assert controller.close_project_circuit(
        1, reason="repository repaired", actor="operator", changed_at=NOW
    )
    assert controller.resume_host(actor="operator", changed_at=NOW)
    assert [item.id for item in controller.list_dispatch_candidates()] == [101, 201]


def test_failed_cross_project_dependency_blocks_only_its_dependant(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _project(connection, project_id=2, key="project-two")
        _item(connection, item_id=101, project_id=1, priority=100)
        _item(connection, item_id=102, project_id=1, priority=10)
        _item(connection, item_id=201, project_id=2, state="failed")
        connection.execute(
            "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (101, 201)"
        )
    controller = V5SchedulingController(store)

    assert controller.reconcile_failed_dependencies(actor="scheduler", changed_at=NOW) == 1

    with store.connect() as connection:
        dependant = connection.execute(
            "SELECT state, state_detail FROM queue_items WHERE id = 101"
        ).fetchone()
        event = connection.execute(
            "SELECT scope, project_id, queue_item_id FROM events "
            "WHERE event_type = 'QUEUE_DEPENDENCY_BLOCKED'"
        ).fetchone()
    assert dependant["state"] == "blocked"
    assert "201=failed" in dependant["state_detail"]
    assert tuple(event) == ("project", 1, 101)
    assert [item.id for item in controller.list_dispatch_candidates()] == [102]


def test_disk_pressure_applies_host_or_project_scope(
    store: V5QueueStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    with store.connect() as connection:
        _project(
            connection,
            project_id=1,
            key="project-one",
            artifact_root=artifact_root,
        )
        _project(connection, project_id=2, key="project-two")
        _item(connection, item_id=101, project_id=1, priority=100)
        _item(connection, item_id=201, project_id=2, priority=10)
    controller = V5SchedulingController(store)

    actual = controller.check_disk_capacity(1, minimum_gib=0)
    assert [check.scope for check in actual] == [FailureScope.HOST, FailureScope.PROJECT]
    assert actual[1].root == artifact_root.resolve()

    monkeypatch.setattr(
        controller,
        "check_disk_capacity",
        lambda _project_id, *, minimum_gib, revision_id=None: (
            DiskCapacity(FailureScope.HOST, store.state_dir, 100.0, minimum_gib),
            DiskCapacity(
                FailureScope.PROJECT,
                artifact_root,
                1.0,
                minimum_gib,
                project_id=1,
                project_key="project-one",
            ),
        ),
    )
    controller.enforce_disk_capacity(
        1, minimum_gib=50, actor="scheduler", changed_at=NOW
    )
    assert controller.host_dispatch_state() == (False, "")
    assert [item.id for item in controller.list_dispatch_candidates()] == [201]

    controller.close_project_circuit(
        1, reason="space repaired", actor="operator", changed_at=NOW
    )
    monkeypatch.setattr(
        controller,
        "check_disk_capacity",
        lambda _project_id, *, minimum_gib, revision_id=None: (
            DiskCapacity(FailureScope.HOST, store.state_dir, 1.0, minimum_gib),
            DiskCapacity(
                FailureScope.PROJECT,
                artifact_root,
                100.0,
                minimum_gib,
                project_id=1,
                project_key="project-one",
            ),
        ),
    )
    controller.enforce_disk_capacity(
        1, minimum_gib=50, actor="scheduler", changed_at=NOW
    )
    assert controller.host_dispatch_state()[0]
    with store.connect() as connection:
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    assert health == "closed"


def test_worktree_process_and_terminal_evidence_round_trip(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, priority=10)
        _allow_gpu(connection)
    repository = tmp_path / "repository"
    worktree = tmp_path / f"project-one-r10-item-101-{'1' * 12}"
    repository.mkdir()
    worktree.mkdir()
    evidence = ProjectWorktreeEvidence.from_document(
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "ProjectWorktreeEvidence",
            "projectId": 1,
            "projectKey": "project-one",
            "projectRevisionId": 10,
            "projectRevision": "project-one:r1",
            "projectRevisionSequence": 1,
            "queueItemId": 101,
            "repository": str(repository),
            "gitRef": "refs/experiment-queue/projects/project-one/revisions/10/items/101",
            "worktree": str(worktree),
            "gitCommit": "1" * 40,
        }
    )
    controller = V5SchedulingController(store)
    assert controller.record_worktree_prepared(
        evidence, actor="scheduler", changed_at=NOW
    )
    assert not controller.record_worktree_prepared(
        evidence, actor="scheduler", changed_at=NOW
    )
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    assert (
        controller.record_launched(
            101,
            segment=1,
            gpu_uuid="GPU-1",
            pid=12345,
            pgid=12345,
            process_start_ticks="987654",
            actor="scheduler",
            started_at=NOW,
        )
        == "running"
    )
    active = controller.active_attempts()
    assert len(active) == 1
    assert active[0].project_key == "project-one"
    assert active[0].process_start_ticks == "987654"

    receipt_path = tmp_path / "exit.json"
    receipt_path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "queue_item_id": 101,
                "project_id": 1,
                "project_revision_id": 10,
                "project_key": "project-one",
                "project_revision": "project-one:r1",
                "experiment_id": "project-one-item-101",
                "attempt": 1,
                "resolved_spec_sha256": None,
                "admission_kind": "LegacyMarkdownCard/v0",
                "segment": 1,
                "git_commit": "1" * 40,
                "worktree": str(worktree),
                "command_kind": "legacy-shell",
                "command_sha256": "a" * 64,
                "started_at": NOW,
                "finished_at": "2026-08-28T16:01:00+00:00",
                "return_code": 0,
                "signals_received": [],
                "gpu_uuid": "GPU-1",
            }
        ),
        encoding="utf-8",
    )
    receipt = ExecutorReceipt.read(
        receipt_path,
        queue_item_id=101,
        project_id=1,
        project_revision_id=10,
        project_key="project-one",
        project_revision="project-one:r1",
        experiment_id="project-one-item-101",
        attempt=1,
        resolved_spec_sha256=None,
        admission_kind="LegacyMarkdownCard/v0",
        segment=1,
        git_commit="1" * 40,
        worktree=worktree,
        command_kind="legacy-shell",
        command_sha256="a" * 64,
        gpu_uuid="GPU-1",
    )
    assert controller.record_executor_completion(receipt, actor="scheduler") == (
        "succeeded"
    )
    assert controller.active_attempts() == ()
    assert controller.record_worktree_cleanup(
        evidence,
        actor="scheduler",
        changed_at="2026-08-28T16:02:00+00:00",
        error="temporary Git cleanup failure",
    )
    assert controller.record_worktree_cleanup(
        evidence,
        actor="scheduler",
        changed_at="2026-08-28T16:03:00+00:00",
    )
    assert controller.record_worktree_prepared(
        evidence,
        actor="scheduler",
        changed_at="2026-08-28T16:04:00+00:00",
    )
    assert controller.record_worktree_cleanup(
        evidence,
        actor="scheduler",
        changed_at="2026-08-28T16:05:00+00:00",
    )
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, return_code, runtime_git_ref,
                   runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = 101
            """
        ).fetchone()
    assert tuple(row) == (
        "succeeded",
        0,
        evidence.git_ref,
        "2026-08-28T16:05:00+00:00",
        None,
    )


def test_abandoned_launch_resolution_is_auditable_and_keeps_safety_barriers(
    store: V5QueueStore,
) -> None:
    """The controller atomically fails only the exact paused starting claim."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    controller.pause_host(
        reason="inspect abandoned claim",
        actor="test:operator",
        changed_at=NOW,
    )

    resolution = controller.resolve_abandoned_launch(
        101,
        project_id=1,
        gpu_uuid="GPU-1",
        pid=None,
        pgid=None,
        process_start_ticks=None,
        reason="operator verified no process or GPU work",
        actor="test:operator",
        changed_at=NOW,
    )

    assert resolution.state == "failed"
    with store.connect() as connection:
        item = connection.execute(
            """
            SELECT state, state_detail, assigned_gpu_uuid,
                   assigned_gpu_index, runtime_gpu_lease_held, return_code
            FROM queue_items WHERE id = 101
            """
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
        event = connection.execute(
            """
            SELECT event_type, payload_json FROM events
            WHERE queue_item_id = 101 ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    assert tuple(item) == (
        "failed",
        "operator resolved abandoned attempt (pre-launch claim): operator verified no process or GPU work",
        "GPU-1",
        "0",
        1,
        127,
    )
    assert health == "open"
    assert paused == "1"
    assert event["event_type"] == "ABANDONED_LAUNCH_RESOLVED"
    assert json.loads(event["payload_json"])["confirmation"] == (
        "RESOLVE-ABANDONED-LAUNCH"
    )


@pytest.mark.parametrize(
    ("guard", "message"),
    [
        ("host-active", "host dispatch must already be paused"),
        ("project", "has no queue item"),
        ("state", "only a pre-launch 'starting' claim"),
        ("missing-gpu", "complete assigned GPU identity"),
        ("missing-index", "complete assigned GPU identity"),
        ("gpu-mismatch", "not confirmed GPU"),
        ("pid", "process identity changed"),
        ("pgid", "process identity changed"),
        ("start-token", "process identity changed"),
    ],
)
def test_abandoned_launch_resolution_rejects_each_durable_guard(
    store: V5QueueStore,
    guard: str,
    message: str,
) -> None:
    """No partial operator assertion can bypass a durable resolution guard."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    if guard != "host-active":
        controller.pause_host(
            reason="test guard",
            actor="test:operator",
            changed_at=NOW,
        )
    project_id = 2 if guard == "project" else 1
    confirmed_gpu = "GPU-other" if guard == "gpu-mismatch" else "GPU-1"
    updates = {
        "state": "state = 'running'",
        "missing-gpu": "assigned_gpu_uuid = NULL",
        "missing-index": "assigned_gpu_index = NULL",
        "pid": "pid = 123",
        "pgid": "pgid = 123",
        "start-token": "proc_start_ticks = '456'",
    }
    if guard in updates:
        if guard in {"missing-gpu", "missing-index"}:
            with store.connect() as connection, pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    f"UPDATE queue_items SET {updates[guard]} WHERE id = 101"
                )
            return
        with store.connect() as connection:
            connection.execute(
                f"UPDATE queue_items SET {updates[guard]} WHERE id = 101"
            )

    with pytest.raises(V5SchedulerError, match=message):
        controller.resolve_abandoned_launch(
            101,
            project_id=project_id,
            gpu_uuid=confirmed_gpu,
            pid=None,
            pgid=None,
            process_start_ticks=None,
            reason="guard test",
            actor="test:operator",
            changed_at=NOW,
        )

    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'ABANDONED_LAUNCH_RESOLVED'"
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "state",
    ["running", "yielding", "terminating", "force_killing"],
)
def test_dead_recorded_process_resolution_covers_each_active_state(
    store: V5QueueStore,
    state: str,
) -> None:
    """A dead persisted identity is terminalized by one exact auditable CAS."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, state=state)
        connection.execute(
            """
            UPDATE queue_items
            SET assigned_gpu_uuid = 'GPU-1', assigned_gpu_index = '0',
                pid = 43210, pgid = 43210, proc_start_ticks = '98765',
                started_at = ?
            WHERE id = 101
            """,
            (NOW,),
        )
    controller = V5SchedulingController(store)
    controller.pause_host(
        reason="operator proved recorded process is dead",
        actor="test:operator",
        changed_at=NOW,
    )

    resolution = controller.resolve_abandoned_launch(
        101,
        project_id=1,
        gpu_uuid="GPU-1",
        pid=43210,
        pgid=43210,
        process_start_ticks="98765",
        reason="no recorded process group remains",
        actor="test:operator",
        changed_at=NOW,
    )

    assert resolution.previous_state == state
    assert resolution.event_type == "DEAD_PROCESS_RESOLVED"
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, pid, pgid, proc_start_ticks, assigned_gpu_uuid,
                   runtime_gpu_lease_held
            FROM queue_items WHERE id = 101
            """
        ).fetchone()
        event = connection.execute(
            """
            SELECT event_type, payload_json FROM events
            WHERE queue_item_id = 101 ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    assert tuple(row) == ("failed", 43210, 43210, "98765", "GPU-1", 1)
    assert event["event_type"] == "DEAD_PROCESS_RESOLVED"
    payload = json.loads(event["payload_json"])
    assert payload["previous_state"] == state
    assert payload["identity_kind"] == "dead recorded process"


@pytest.mark.parametrize("force", [False, True])
def test_termination_refuses_starting_claim_without_launch_identity(
    store: V5QueueStore,
    force: bool,
) -> None:
    """Neither graceful nor force control can create a null-identity wedge."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1)
        _allow_gpu(connection)
    controller = V5SchedulingController(store)
    assert controller.claim(
        101,
        gpu_uuid="GPU-1",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None

    with pytest.raises(V5SchedulerError, match="recover/adopt the launch first"):
        controller.request_termination(
            101,
            reason="must wait for launch identity",
            force=force,
            actor="test:operator",
            requested_at=NOW,
            signal_epoch=1.0,
        )

    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, pid, pgid, terminate_requested_at FROM queue_items WHERE id = 101"
        ).fetchone()
    assert tuple(row) == ("starting", None, None, None)


def test_manual_yield_startup_signal_replay_is_audited_exactly(
    store: V5QueueStore,
) -> None:
    """Replay audit binds the current yielding request and delivery result."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, state="yielding")
        connection.execute(
            "UPDATE queue_items SET yield_request_id = 'manual:101:1:test' "
            "WHERE id = 101"
        )
    controller = V5SchedulingController(store)
    claim = controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="attempt-one",
        signal_epoch=10.0,
        retry_after_seconds=5.0,
        actor="scheduler:recovery",
        changed_at=NOW,
    )
    assert claim is not None
    controller.record_manual_yield_signal_result(
        claim,
        delivered=True,
        detail="authenticated SIGINT was delivered",
        result_epoch=10.1,
        actor="scheduler:recovery",
        changed_at=NOW,
    )

    with store.connect() as connection:
        event = connection.execute(
            """
            SELECT event_type, payload_json FROM events
            WHERE queue_item_id = 101 ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    assert event["event_type"] == "MANUAL_PREEMPTION_SIGNAL_RESULT"
    payload = json.loads(event["payload_json"])
    assert payload["request_id"] == "manual:101:1:test"
    assert payload["delivered"] is True
    assert payload["delivery_semantics"] == "at-least-once"


def test_manual_yield_signal_claim_retries_only_after_bounded_lease(
    store: V5QueueStore,
) -> None:
    """Missing or failed outcomes are retried durably after their exact lease."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, state="yielding")
        connection.execute(
            "UPDATE queue_items SET yield_request_id = 'manual:101:1:test' "
            "WHERE id = 101"
        )
    controller = V5SchedulingController(store)
    first = controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="attempt-one",
        signal_epoch=10.0,
        retry_after_seconds=5.0,
        actor="test:sender",
        changed_at=NOW,
    )
    assert first is not None
    assert controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="too-early",
        signal_epoch=14.9,
        retry_after_seconds=5.0,
        actor="test:recovery",
        changed_at=NOW,
    ) is None
    controller.record_manual_yield_signal_result(
        first,
        delivered=False,
        detail="fixture ambiguous delivery",
        result_epoch=15.0,
        actor="test:sender",
        changed_at=NOW,
    )
    assert controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="still-too-early",
        signal_epoch=19.9,
        retry_after_seconds=5.0,
        actor="test:recovery",
        changed_at=NOW,
    ) is None
    second = controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="attempt-two",
        signal_epoch=20.0,
        retry_after_seconds=5.0,
        actor="test:recovery",
        changed_at=NOW,
    )
    assert second is not None
    assert second.attempt == 2
    assert second.replay is True
    controller.record_manual_yield_signal_result(
        second,
        delivered=True,
        detail="authenticated SIGINT was delivered",
        result_epoch=20.1,
        actor="test:recovery",
        changed_at=NOW,
    )
    assert controller.claim_manual_yield_signal_attempt(
        101,
        request_id="manual:101:1:test",
        attempt_token="must-not-retry-success",
        signal_epoch=100.0,
        retry_after_seconds=5.0,
        actor="test:recovery",
        changed_at=NOW,
    ) is None


def test_termination_cas_escalates_and_finalizes_without_receipt(
    store: V5QueueStore,
) -> None:
    """Persisted intent advances once per exact stage and never needs PID trust."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, state="running")
        connection.execute(
            """
            UPDATE queue_items
            SET assigned_gpu_uuid = 'GPU-1', assigned_gpu_index = '0',
                pid = 12345, pgid = 12345, proc_start_ticks = '987654',
                started_at = ?
            WHERE id = 101
            """,
            (NOW,),
        )
    controller = V5SchedulingController(store)

    requested = controller.request_termination(
        101,
        reason="operator requested graceful stop",
        force=False,
        actor="test:operator",
        requested_at=NOW,
        signal_epoch=100.0,
    )
    assert requested.state == "terminating"
    assert requested.stage == "interrupt"
    assert requested.pid == requested.pgid == 12345
    assert requested.process_start_ticks == "987654"

    # A repeated delivery does not move the persisted deadline or replace the
    # original reason, so retries cannot prevent bounded escalation.
    repeated = controller.request_termination(
        101,
        reason="retry with different text",
        force=False,
        actor="test:operator",
        requested_at="2026-08-28T16:00:01+00:00",
        signal_epoch=999.0,
    )
    assert repeated == requested

    terminated = controller.escalate_termination(
        101,
        expected_stage="interrupt",
        expected_signal_epoch=100.0,
        actor="scheduler",
        changed_at="2026-08-28T16:00:30+00:00",
        signal_epoch=130.0,
    )
    assert terminated is not None
    assert terminated.state == "terminating"
    assert terminated.stage == "terminate"
    assert controller.escalate_termination(
        101,
        expected_stage="interrupt",
        expected_signal_epoch=100.0,
        actor="stale-scheduler",
        changed_at="2026-08-28T16:00:31+00:00",
        signal_epoch=131.0,
    ) is None

    killed = controller.escalate_termination(
        101,
        expected_stage="terminate",
        expected_signal_epoch=130.0,
        actor="scheduler",
        changed_at="2026-08-28T16:01:00+00:00",
        signal_epoch=160.0,
    )
    assert killed is not None
    assert killed.state == "force_killing"
    assert killed.stage == "kill"
    controller.record_termination_signal_attempt(
        killed,
        signal_name="SIGKILL",
        delivered=True,
        actor="scheduler",
        attempted_at="2026-08-28T16:01:00+00:00",
    )
    assert controller.record_termination_completion(
        101,
        actor="scheduler",
        finished_at="2026-08-28T16:01:01+00:00",
        return_code=137,
    ) == "force_killed"

    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, terminate_requested_at, terminate_reason,
                   termination_stage, termination_signal_epoch, return_code
            FROM queue_items WHERE id = 101
            """
        ).fetchone()
        events = [
            str(event[0])
            for event in connection.execute(
                "SELECT event_type FROM events WHERE queue_item_id = 101 ORDER BY id"
            )
        ]
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    assert tuple(row) == (
        "force_killed",
        NOW,
        "operator requested graceful stop",
        "kill",
        160.0,
        137,
    )
    assert events == [
        "TERMINATION_REQUESTED",
        "TERMINATION_ESCALATED",
        "TERMINATION_ESCALATED",
        "TERMINATION_SIGNAL_SENT",
        "EXPERIMENT_TERMINATION_COMPLETED",
    ]
    assert health == "closed"


def test_force_kill_wins_yield_race_and_terminal_state_rejects_new_request(
    store: V5QueueStore,
) -> None:
    """The force CAS supersedes yielding and cannot overwrite a terminal winner."""

    with store.connect() as connection:
        _project(connection, project_id=1, key="project-one")
        _item(connection, item_id=101, project_id=1, state="yielding")
    controller = V5SchedulingController(store)
    action = controller.request_termination(
        101,
        reason="operator force stop",
        force=True,
        actor="test:operator",
        requested_at=NOW,
        signal_epoch=100.0,
    )
    assert action.state == "force_killing"
    assert action.stage == "kill"
    with store.connect() as connection:
        stale_continuation = connection.execute(
            "UPDATE queue_items SET state = 'queued', segment = 2 "
            "WHERE id = 101 AND state = 'yielding' AND segment = 1"
        )
        assert stale_continuation.rowcount == 0
        connection.execute(
            "UPDATE queue_items SET state = 'force_killed' WHERE id = 101"
        )
    with pytest.raises(V5SchedulerError, match="termination requires an active"):
        controller.request_termination(
            101,
            reason="stale retry",
            force=True,
            actor="test:operator",
            requested_at=NOW,
            signal_epoch=101.0,
        )
