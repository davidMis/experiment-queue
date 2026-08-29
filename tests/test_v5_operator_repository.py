"""Exercise the typed project-authorized schema-v5 operator boundary."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes
from experiment_queue.v5_operator_repository import (
    V5OperatorError,
    V5OperatorEvidenceError,
    V5OperatorNotFoundError,
    V5OperatorRepository,
)


NOW = "2026-08-28T18:00:00+00:00"
SHA = "0" * 64


@pytest.fixture
def store(tmp_path: Path) -> V5QueueStore:
    value = V5QueueStore((tmp_path / "state").resolve())
    value.initialize()
    return value


def _legacy_project(
    connection: sqlite3.Connection,
    *,
    root: Path,
    project_id: int,
    key: str,
    lifecycle: str = "paused",
) -> None:
    checkout = (root / f"{key}-checkout").resolve()
    checkout.mkdir()
    revision_id = project_id * 10
    commit = f"{project_id:x}" * 40
    enrollment = canonical_json_bytes(
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "LegacyEnrollment",
            "projectKey": key,
            "checkoutDirectory": str(checkout),
            "projectManifestPath": None,
            "sourceSchemaVersion": 4,
            "sourceStateIdentitySha256": f"{project_id:x}" * 64,
            "gitCommit": commit,
            "mounts": [],
            "artifactRoots": [],
            "environments": [],
        }
    )
    connection.execute(
        """
        INSERT INTO projects(
            id, project_key, display_name, lifecycle, current_revision_id,
            current_revision_sequence, created_at, created_by,
            lifecycle_changed_at, lifecycle_actor, lifecycle_reason
        ) VALUES (?, ?, ?, ?, ?, 1, ?, 'importer', ?, 'importer',
                  'offline legacy import')
        """,
        (project_id, key, key, lifecycle, revision_id, NOW, NOW),
    )
    connection.execute(
        """
        INSERT INTO project_revisions(
            id, project_id, sequence, revision_label, revision_kind,
            display_name, git_commit, checkout_path, enrollment_json,
            enrollment_sha256, created_at, created_actor
        ) VALUES (?, ?, 1, ?, 'legacy-v4', ?, ?, ?, ?, ?, ?, 'importer')
        """,
        (
            revision_id,
            project_id,
            f"{key}:legacy-r1",
            key,
            commit,
            str(checkout),
            enrollment,
            sha256_bytes(enrollment),
            NOW,
        ),
    )
    connection.execute(
        """
        INSERT INTO project_runtime_state(
            project_id, health, circuit_failure_count, health_reason,
            health_actor, health_changed_at
        ) VALUES (?, 'closed', 0, 'import pending adoption', 'importer', ?)
        """,
        (project_id, NOW),
    )


def _item(
    connection: sqlite3.Connection,
    *,
    item_id: int,
    project_id: int,
    state: str = "queued",
    priority: int = 0,
) -> None:
    key = str(
        connection.execute(
            "SELECT project_key FROM projects WHERE id = ?", (project_id,)
        ).fetchone()[0]
    )
    revision_id, commit = connection.execute(
        "SELECT id, git_commit FROM project_revisions WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO queue_items(
            id, project_id, revision_id, admission_kind, snapshot_id, job_id,
            experiment_id, attempt, state, priority, card_path, card_sha256,
            command_text, runner_name, git_commit, added_at, added_by
        ) VALUES (?, ?, ?, 'LegacyMarkdownCard/v0', NULL, NULL, ?, 1, ?, ?,
                  'cards/imported.md', ?, 'python legacy.py', 'legacy', ?, ?,
                  'importer')
        """,
        (
            item_id,
            project_id,
            int(revision_id),
            f"{key}-experiment-{item_id}",
            state,
            priority,
            SHA,
            str(commit),
            NOW,
        ),
    )


def _two_projects(store: V5QueueStore, tmp_path: Path) -> V5OperatorRepository:
    with store.connect() as connection:
        _legacy_project(connection, root=tmp_path, project_id=1, key="project-one")
        _legacy_project(connection, root=tmp_path, project_id=2, key="project-two")
        _item(connection, item_id=101, project_id=1)
        _item(connection, item_id=201, project_id=2)
        connection.execute(
            "INSERT INTO dependencies(queue_item_id, dependency_item_id) "
            "VALUES (201, 101)"
        )
        connection.commit()
    return V5OperatorRepository(store)


def test_legacy_safe_summaries_cwd_inference_and_export(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    repository = _two_projects(store, tmp_path)

    summary = repository.get_project_summary(project_key="project-one")
    assert summary.id == 1
    assert summary.current_revision_kind == "legacy-v4"
    assert summary.current_revision_label == "project-one:legacy-r1"
    assert summary.queue_counts == (("queued", 1),)
    assert not summary.dispatch_allowed
    assert repository.list_project_summaries(after_id=1)[0].key == "project-two"

    checkout = tmp_path / "project-one-checkout"
    child = checkout / "nested"
    child.mkdir()
    inferred = repository.infer_project_from_cwd(child.resolve())
    assert inferred.key == "project-one"

    exported = repository.project_export(1)
    assert exported.project.key == "project-one"
    assert [revision.kind for revision in exported.revisions] == ["legacy-v4"]
    assert exported.revisions[0].enrollment_source.startswith(b"{")
    assert [item.item.id for item in exported.items] == [101]
    assert exported.artifacts == ()
    assert exported.yield_requests == ()
    assert exported.yield_receipts == ()
    assert exported.host_state.dispatch_paused is False

    cross_project = repository.project_export(2)
    target = cross_project.items[0].dependency_targets[0]
    assert (target.item_id, target.project_id, target.project_key) == (
        101, 1, "project-one"
    )


def test_item_mutations_require_project_and_hold_cross_project_dependants(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    repository = _two_projects(store, tmp_path)

    with pytest.raises(V5OperatorNotFoundError, match="Project id 2"):
        repository.get_item(101, project_id=2)
    held = repository.hold_item(
        101,
        project_id=1,
        reason="operator inspection",
        actor="tester",
        changed_at=NOW,
    )
    assert held.item.state == "held"
    assert held.item.state_detail == "operator inspection"
    reprioritized = repository.set_item_priority(
        101,
        project_id=1,
        priority=9,
        actor="tester",
        changed_at=NOW,
    )
    assert reprioritized.item.priority == 9
    released = repository.release_item(
        101, project_id=1, actor="tester", changed_at=NOW
    )
    assert released.item.state == "queued"
    removed = repository.remove_item(
        101,
        project_id=1,
        reason="superseded",
        actor="tester",
        changed_at=NOW,
    )
    assert removed.item.state == "removed"
    assert removed.finished_at == NOW
    dependent = repository.get_item(201, project_id=2)
    assert dependent.item.state == "held"
    assert "global queue item 101" in str(dependent.item.state_detail)

    project_one_events = repository.list_events(project_id=1)
    project_two_events = repository.list_events(project_id=2)
    assert [event.event_type for event in project_one_events] == [
        "queue_item_held",
        "queue_item_priority_changed",
        "queue_item_released",
        "queue_item_removed",
    ]
    assert [event.event_type for event in project_two_events] == [
        "queue_dependency_held"
    ]
    with pytest.raises(V5OperatorError, match="requires one of"):
        repository.remove_item(
            101,
            project_id=1,
            reason="again",
            actor="tester",
            changed_at=NOW,
        )


def test_gpu_allowlist_transitions_are_strict_and_preserve_active_assignment(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    repository = _two_projects(store, tmp_path)
    gpu = repository.add_gpu(
        uuid="GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        requested_identifier="0",
        last_index="0",
        name="Fixture GPU",
        actor="tester",
        changed_at=NOW,
    )
    assert gpu.enabled and not gpu.draining
    with pytest.raises(V5OperatorError, match="already in the allowlist"):
        repository.add_gpu(
            uuid=gpu.uuid,
            requested_identifier="0",
            last_index="0",
            name="Fixture GPU",
            actor="tester",
            changed_at=NOW,
        )

    disabled = repository.disable_gpu(gpu.uuid, actor="tester", changed_at=NOW)
    assert not disabled.enabled
    enabled = repository.enable_gpu(gpu.uuid, actor="tester", changed_at=NOW)
    assert enabled.enabled
    draining = repository.drain_gpu(gpu.uuid, actor="tester", changed_at=NOW)
    assert draining.draining
    undrained = repository.undrain_gpu(gpu.uuid, actor="tester", changed_at=NOW)
    assert not undrained.draining

    with store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ?, "
            "assigned_gpu_index = '0', runtime_gpu_lease_held = 1 WHERE id = 101",
            (gpu.uuid,),
        )
        connection.commit()
    assert repository.list_gpus()[0].assigned_queue_item_ids == (101,)
    assert [event.event_type for event in repository.projects.list_events()] == [
        "gpu_allowlist_added",
        "gpu_allowlist_disabled",
        "gpu_allowlist_enabled",
        "gpu_allowlist_draining",
        "gpu_allowlist_undrained",
    ]


def test_artifact_reads_are_project_scoped_and_rehash_canonical_metadata(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    repository = _two_projects(store, tmp_path)
    artifact = (tmp_path / "result.bin").resolve()
    artifact.write_bytes(b"result")
    metadata = canonical_json_bytes({"source": "legacy-import"})
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO job_artifacts(
                id, queue_item_id, project_id, revision_id, segment,
                evidence_kind, artifact_name, artifact_type, absolute_path,
                size_bytes, sha256, recorded_at, metadata_json
            ) VALUES (1, 101, 1, 10, 1, 'legacy-v4', 'result', 'file',
                      ?, 6, ?, ?, ?)
            """,
            (str(artifact), sha256_bytes(b"result"), NOW, metadata),
        )
        connection.commit()

    records = repository.list_artifacts(project_id=1, queue_item_id=101)
    assert records[0].absolute_path == artifact
    assert records[0].metadata == {"source": "legacy-import"}
    assert repository.list_artifacts(project_id=2) == ()
    with pytest.raises(V5OperatorNotFoundError):
        repository.list_artifacts(project_id=2, queue_item_id=101)

    with store.connect() as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'job_artifacts_immutable_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER job_artifacts_immutable_update")
        connection.execute(
            "UPDATE job_artifacts SET metadata_json = ? WHERE id = 1", (b"{ }",)
        )
        connection.execute(trigger)
        connection.commit()
    with pytest.raises(V5OperatorEvidenceError, match="not exact canonical JSON"):
        repository.list_artifacts(project_id=1)


def test_occupied_root_inventory_includes_imported_checkout(
    store: V5QueueStore,
    tmp_path: Path,
) -> None:
    repository = _two_projects(store, tmp_path)
    claims = repository.occupied_roots(exclude_project_id=1)
    assert [(claim.project_key, claim.role) for claim in claims] == [
        ("project-two", "revision 20 imported checkout")
    ]
    assert claims[0].path == (tmp_path / "project-two-checkout").resolve()
