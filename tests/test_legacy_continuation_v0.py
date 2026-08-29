"""Exercise grandfathered schema-v4 manual yield on imported v5 rows."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path

import pytest

import experiment_queue.legacy_continuation_v0 as legacy_module
from experiment_queue.attempt_runtime import PreparedAttempt, prepare_legacy_attempt
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.legacy_continuation_v0 import (
    LegacyV0ContinuationCoordinator,
    LegacyV0ContinuationError,
)
from experiment_queue.protocols import RUNNER_RECEIPT_V1
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes
from experiment_queue.v5_repository import V5ProjectRepository


NOW = "2026-08-28T12:00:00+00:00"
COMMIT = "a" * 40


@dataclass(frozen=True, slots=True)
class LegacyHarness:
    """One imported v4 item prepared as an active legacy-shell segment."""

    store: V5QueueStore
    repository: V5ProjectRepository
    coordinator: LegacyV0ContinuationCoordinator
    prepared: PreparedAttempt
    run_directory: Path
    item_id: int


def _process_start_ticks() -> str | None:
    try:
        fields = Path(f"/proc/{os.getpid()}/stat").read_text(encoding="ascii").split()
    except OSError:
        return None
    return fields[21]


def _write_runner(
    harness: LegacyHarness,
    *,
    status: str,
    return_code: int | None,
) -> None:
    document = {
        **RUNNER_RECEIPT_V1.document_identity(),
        "run_id": "legacy-run-1",
        "queue_item_id": harness.item_id,
        "segment": 1,
        "status": status,
        "return_code": return_code,
        "run_directory": str(harness.run_directory),
        "manifest": str(harness.run_directory / "manifest.json"),
        "logs": {
            "stdout": str(harness.run_directory / "stdout.log"),
            "stderr": str(harness.run_directory / "stderr.log"),
        },
        "sync": None,
        "written_at": NOW,
    }
    (harness.prepared.paths.segment_root / "runner.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _make_harness(tmp_path: Path) -> LegacyHarness:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    checkout = (tmp_path / "legacy-checkout").resolve()
    checkout.mkdir()
    run_directory = checkout / "outputs" / "experiments" / "legacy-run-1"
    run_directory.mkdir(parents=True)
    for name in ("manifest.json", "stdout.log", "stderr.log"):
        (run_directory / name).write_text("{}\n", encoding="utf-8")
    enrollment = canonical_json_bytes(
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "LegacyEnrollment",
            "projectKey": "legacy-project",
            "checkoutDirectory": str(checkout),
            "projectManifestPath": None,
            "sourceSchemaVersion": 4,
            "sourceStateIdentitySha256": "b" * 64,
            "gitCommit": COMMIT,
            "mounts": [],
            "artifactRoots": [],
            "environments": [],
        }
    )
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO projects(
                id, project_key, display_name, lifecycle, current_revision_id,
                current_revision_sequence, created_at, created_by,
                lifecycle_changed_at, lifecycle_actor, lifecycle_reason
            ) VALUES (1, 'legacy-project', 'Legacy Project', 'active', 1, 1,
                      ?, 'test:importer', ?, 'test:operator', 'test activation')
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, checkout_path, project_manifest_path,
                enrollment_json, enrollment_sha256, created_at, created_actor
            ) VALUES (1, 1, 1, 'legacy-project:legacy-r1', 'legacy-v4',
                      'Legacy Project', ?, ?, NULL, ?, ?, ?, 'test:importer')
            """,
            (COMMIT, str(checkout), enrollment, sha256_bytes(enrollment), NOW),
        )
        connection.execute(
            """
            INSERT INTO project_runtime_state(
                project_id, health, circuit_failure_count, health_reason,
                health_actor, health_changed_at
            ) VALUES (1, 'closed', 0, 'healthy', 'test:operator', ?)
            """,
            (NOW,),
        )
        cursor = connection.execute(
            """
            INSERT INTO queue_items(
                project_id, revision_id, admission_kind, snapshot_id, job_id,
                experiment_id, attempt, state, priority, card_path, card_sha256,
                command_text, runner_name, git_commit, added_at, added_by,
                preemptible
            ) VALUES (1, 1, 'LegacyMarkdownCard/v0', NULL, NULL, 'LEG-001',
                      1, 'queued', 5, 'docs/LEG-001.md', ?, 'sleep 300',
                      'run-experiment', ?, ?, 'test:importer', 1)
            """,
            ("c" * 64, COMMIT, NOW),
        )
        item_id = int(cursor.lastrowid)
    prepared = prepare_legacy_attempt(
        state_directory=state,
        queue_item_id=item_id,
        project_id=1,
        project_key="legacy-project",
        project_revision_id=1,
        project_revision="legacy-project:legacy-r1",
        experiment_id="LEG-001",
        attempt=1,
        segment=1,
        git_commit=COMMIT,
        execution_root=checkout,
        primary_checkout=checkout,
        command_text="sleep 300",
        ambient_environment={},
        gpu_uuid="GPU-legacy",
        gpu_index="0",
        preemptible=True,
    )
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = 'GPU-legacy',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1,
                pid = ?, pgid = ?,
                proc_start_ticks = ?, started_at = ?
            WHERE id = ?
            """,
            (
                os.getpid(),
                os.getpgid(os.getpid()),
                _process_start_ticks(),
                NOW,
                item_id,
            ),
        )
    repository = V5ProjectRepository(store)
    harness = LegacyHarness(
        store=store,
        repository=repository,
        coordinator=LegacyV0ContinuationCoordinator(repository),
        prepared=prepared,
        run_directory=run_directory,
        item_id=item_id,
    )
    _write_runner(harness, status="running", return_code=None)
    return harness


def _request(
    harness: LegacyHarness,
    monkeypatch: pytest.MonkeyPatch,
    *,
    request_id: str,
) -> object:
    monkeypatch.setattr(
        legacy_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    return harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint the imported job",
        actor="test:operator",
        requested_at=NOW,
        request_id=request_id,
    )


def _ready_receipt(harness: LegacyHarness, request_id: str) -> tuple[Path, Path]:
    checkpoint = harness.run_directory / "checkpoint.bin"
    metadata = harness.run_directory / "checkpoint.json"
    checkpoint.write_bytes(b"legacy-checkpoint")
    metadata.write_text('{"step":7}\n', encoding="utf-8")
    document = {
        "schema_version": 1,
        "status": "ready",
        "request_id": request_id,
        "queue_item_id": harness.item_id,
        "step": 7,
        "progress": {"unit": "steps", "completed": 7, "total": 20},
        "checkpoint": str(checkpoint),
        "checkpoint_metadata": str(metadata),
        "checkpoint_bytes": checkpoint.stat().st_size,
        "checkpoint_sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
        "wandb": {"id": "legacy-wandb"},
    }
    harness.prepared.paths.yield_receipt.write_text(
        json.dumps(document), encoding="utf-8"
    )
    return checkpoint, metadata


def test_request_persists_and_publishes_exact_v0_before_authenticated_sigint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)
    observed: list[str] = []

    def signal_after_evidence(**arguments: object) -> bool:
        with harness.store.connect() as connection:
            row = connection.execute(
                "SELECT state, yield_request_id FROM queue_items WHERE id = ?",
                (harness.item_id,),
            ).fetchone()
        assert row["state"] == "yielding"
        assert row["yield_request_id"] == "legacy-request-order"
        document = json.loads(harness.prepared.paths.yield_request.read_text())
        assert document == {
            "schema_version": 1,
            "request_kind": "manual_preemption",
            "request_id": "legacy-request-order",
            "queue_item_id": harness.item_id,
            "segment": 1,
            "gpu_uuid": "GPU-legacy",
            "requested_at": NOW,
            "requested_by": "test:operator",
            "note": "checkpoint the imported job",
        }
        assert arguments["signum"] == 2
        observed.append("signal")
        return True

    monkeypatch.setattr(
        legacy_module,
        "signal_recorded_process",
        signal_after_evidence,
    )
    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint the imported job",
        actor="test:operator",
        requested_at=NOW,
        request_id="legacy-request-order",
    )

    assert observed == ["signal"]
    assert pending.request_sha256 == hashlib.sha256(
        harness.prepared.paths.yield_request.read_bytes()
    ).hexdigest()
    assert pending.runner_run_id == "legacy-run-1"


def test_signal_exception_preserves_legacy_active_evidence_for_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)

    def uncertain_signal(**_arguments: object) -> bool:
        raise OSError("signal syscall result unavailable")

    monkeypatch.setattr(
        legacy_module,
        "signal_recorded_process",
        uncertain_signal,
    )

    with pytest.raises(
        LegacyV0ContinuationError,
        match="uncertain SIGINT delivery",
    ):
        harness.coordinator.request_manual_yield(
            harness.prepared,
            note="checkpoint before uncertain signal",
            actor="test:operator",
            requested_at=NOW,
            request_id="legacy-signal-exception",
        )

    with harness.store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index, pid, pgid,
                   proc_start_ticks, yield_request_id, finished_at
            FROM queue_items WHERE id = ?
            """,
            (harness.item_id,),
        ).fetchone()
        project_health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        dispatch_paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(row) == (
        "yielding",
        "GPU-legacy",
        "0",
        os.getpid(),
        os.getpgid(os.getpid()),
        _process_start_ticks(),
        "legacy-signal-exception",
        None,
    )
    assert project_health == "open"
    assert dispatch_paused == "0"

    recovered = LegacyV0ContinuationCoordinator(
        harness.repository
    ).recover_pending(harness.prepared)
    assert recovered.request_id == "legacy-signal-exception"


def test_false_signal_after_ready_v0_receipt_exit_race_is_finalizable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)

    def receipt_wins_signal_race(**_arguments: object) -> bool:
        _ready_receipt(harness, "legacy-receipt-race")
        _write_runner(harness, status="yielded", return_code=75)
        return False

    monkeypatch.setattr(
        legacy_module,
        "signal_recorded_process",
        receipt_wins_signal_race,
    )

    with pytest.raises(
        LegacyV0ContinuationError,
        match="uncertain SIGINT delivery",
    ):
        harness.coordinator.request_manual_yield(
            harness.prepared,
            note="checkpoint at exit race",
            actor="test:operator",
            requested_at=NOW,
            request_id="legacy-receipt-race",
        )

    item = harness.repository.get_queue_item(harness.item_id)
    assert item.state == "yielding"
    with harness.store.connect() as connection:
        assignment = connection.execute(
            "SELECT assigned_gpu_uuid, pid, pgid FROM queue_items WHERE id = ?",
            (harness.item_id,),
        ).fetchone()
    assert tuple(assignment) == (
        "GPU-legacy",
        os.getpid(),
        os.getpgid(os.getpid()),
    )
    recovered = harness.coordinator.recover_pending(harness.prepared)
    outcome = harness.coordinator.finalize_manual_yield(
        recovered,
        executor_return_code=75,
        actor="scheduler:recovery",
        changed_at=NOW,
    )
    assert outcome.requeued
    assert outcome.item.state == "queued"
    assert outcome.item.segment == 2


def test_ready_v0_receipt_requeues_same_item_with_verified_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)
    pending = _request(harness, monkeypatch, request_id="legacy-request-ready")
    checkpoint, metadata = _ready_receipt(harness, "legacy-request-ready")
    _write_runner(harness, status="yielded", return_code=75)

    outcome = harness.coordinator.finalize_manual_yield(
        pending,  # type: ignore[arg-type]
        executor_return_code=75,
        actor="scheduler",
        changed_at=NOW,
    )

    assert outcome.requeued is True
    assert outcome.item.state == "queued"
    assert outcome.item.segment == 2
    assert outcome.item.resume_front is True
    with harness.store.connect() as connection:
        row = connection.execute(
            "SELECT * FROM queue_items WHERE id = ?", (harness.item_id,)
        ).fetchone()
        event = connection.execute(
            "SELECT event_type FROM events WHERE queue_item_id = ? ORDER BY id DESC",
            (harness.item_id,),
        ).fetchone()
    assert row["continuation_checkpoint"] == str(checkpoint)
    assert row["continuation_checkpoint_metadata"] == str(metadata)
    assert row["continuation_step"] == 7
    assert row["continuation_wandb_id"] == "legacy-wandb"
    assert row["assigned_gpu_uuid"] == "GPU-legacy"
    assert row["runtime_gpu_lease_held"] == 0
    assert row["runtime_gpu_lease_released_at"] == NOW
    assert event["event_type"] == "EXPERIMENT_YIELDED_AND_REQUEUED"


def test_exact_failed_v0_receipt_restores_a_still_authenticated_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)
    stable_token = "test-process-start-token"
    with harness.store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET proc_start_ticks = ? WHERE id = ?",
            (stable_token, harness.item_id),
        )

    def stable_process_identity(**arguments: object) -> bool:
        return arguments == {
            "pid": os.getpid(),
            "pgid": os.getpgid(os.getpid()),
            "process_start_ticks": stable_token,
        }

    monkeypatch.setattr(
        legacy_module,
        "process_identity_matches",
        stable_process_identity,
    )
    pending = _request(harness, monkeypatch, request_id="legacy-request-failed")
    harness.prepared.paths.yield_receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "failed",
                "request_id": "legacy-request-failed",
                "queue_item_id": harness.item_id,
                "step": 4,
                "error": "checkpoint backend unavailable",
            }
        ),
        encoding="utf-8",
    )
    failed_source = harness.prepared.paths.yield_receipt.read_bytes()

    outcome = harness.coordinator.reconcile_live_failure(
        pending,  # type: ignore[arg-type]
        actor="scheduler",
        changed_at=NOW,
    )

    assert outcome is not None
    assert outcome.resumed_running is True
    assert outcome.item.state == "running"
    with harness.store.connect() as connection:
        row = connection.execute(
            "SELECT yield_request_id, state_detail FROM queue_items WHERE id = ?",
            (harness.item_id,),
        ).fetchone()
    assert row["yield_request_id"] is None
    assert "checkpoint backend unavailable" in row["state_detail"]
    assert not harness.prepared.paths.yield_receipt.exists()

    # A crash after the database commit but before unlink may leave the exact
    # old file behind. Its durable event hash authorizes removal; arbitrary
    # unrecorded evidence would be refused instead.
    harness.prepared.paths.yield_receipt.write_bytes(failed_source)
    retried = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="retry checkpoint",
        actor="test:operator",
        requested_at=NOW,
        request_id="legacy-request-retry",
    )
    assert retried.request_id == "legacy-request-retry"
    assert not harness.prepared.paths.yield_receipt.exists()


def test_ready_receipt_outside_bound_run_directory_fails_with_lease_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)
    pending = _request(harness, monkeypatch, request_id="legacy-request-escape")
    checkpoint = tmp_path / "outside-checkpoint.bin"
    metadata = tmp_path / "outside-checkpoint.json"
    checkpoint.write_bytes(b"outside")
    metadata.write_text("{}\n", encoding="utf-8")
    harness.prepared.paths.yield_receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "ready",
                "request_id": "legacy-request-escape",
                "queue_item_id": harness.item_id,
                "step": 1,
                "checkpoint": str(checkpoint),
                "checkpoint_metadata": str(metadata),
                "checkpoint_bytes": checkpoint.stat().st_size,
                "checkpoint_sha256": hashlib.sha256(
                    checkpoint.read_bytes()
                ).hexdigest(),
                "wandb": None,
            }
        ),
        encoding="utf-8",
    )
    _write_runner(harness, status="yielded", return_code=75)

    with pytest.raises(LegacyV0ContinuationError, match="outside authorized roots"):
        harness.coordinator.finalize_manual_yield(
            pending,  # type: ignore[arg-type]
            executor_return_code=75,
            actor="scheduler",
            changed_at=NOW,
        )

    assert harness.repository.get_queue_item(harness.item_id).state == "failed"
    with harness.store.connect() as connection:
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        runtime = connection.execute(
            """
            SELECT assigned_gpu_uuid, runtime_gpu_lease_held, pid
            FROM queue_items WHERE id = ?
            """,
            (harness.item_id,),
        ).fetchone()
    assert health == "open"
    assert tuple(runtime) == ("GPU-legacy", 1, os.getpid())


def test_ready_requeue_compare_and_set_never_overrides_termination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path)
    pending = _request(harness, monkeypatch, request_id="legacy-request-race")
    _ready_receipt(harness, "legacy-request-race")
    _write_runner(harness, status="yielded", return_code=75)
    with harness.store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'terminating', state_detail = ? WHERE id = ?",
            ("operator termination", harness.item_id),
        )

    with pytest.raises(LegacyV0ContinuationError, match="stale receipt was not requeued"):
        harness.coordinator.finalize_manual_yield(
            pending,  # type: ignore[arg-type]
            executor_return_code=75,
            actor="scheduler",
            changed_at=NOW,
        )

    item = harness.repository.get_queue_item(harness.item_id)
    assert item.state == "terminating"
    assert item.state_detail == "operator termination"
