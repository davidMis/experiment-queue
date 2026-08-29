"""Verify schema-v5 manual continuation ordering and Project isolation."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path

import pytest

import experiment_queue.continuation_v5 as continuation_module
from experiment_queue.attempt_runtime import PreparedAttempt, prepare_structured_attempt
from experiment_queue.continuation_v5 import (
    V5ContinuationCoordinator,
    V5ContinuationError,
)
from experiment_queue.cooperative_yield import (
    CheckpointArtifact,
    CooperativeYieldRequest,
    CooperativeYieldReceipt,
    OpaqueResumeContext,
    YieldProgress,
    write_yield_receipt,
)
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.execution import build_execution_plan
from experiment_queue.git_resolver import verify_project_revision
from experiment_queue.project_worktrees import ProjectWorktreeManager
from experiment_queue.protocols import RUNNER_RECEIPT_V1
from experiment_queue.serialization import sha256_bytes
from experiment_queue.v5_repository import V5ProjectRepository
from test_v5_repository import (
    NOW,
    ProjectBundle,
    _make_bundle,
    _resolved,
)


@dataclass(frozen=True, slots=True)
class ContinuationHarness:
    """One admitted running segment with real temporary control roots."""

    service: V5ProjectRepository
    bundle: ProjectBundle
    prepared: PreparedAttempt
    coordinator: V5ContinuationCoordinator
    item_id: int


def _runner_receipt(prepared: PreparedAttempt, *, run_id: str = "run-1") -> bytes:
    document = {
        **RUNNER_RECEIPT_V1.document_identity(),
        "run_id": run_id,
        "queue_item_id": prepared.queue_item_id,
        "segment": prepared.segment,
        "status": "running",
        "return_code": None,
        "run_directory": str(prepared.paths.segment_root / "run"),
        "manifest": str(prepared.paths.segment_root / "run" / "manifest.json"),
        "logs": {
            "stdout": str(prepared.paths.segment_root / "run" / "stdout.log"),
            "stderr": str(prepared.paths.segment_root / "run" / "stderr.log"),
        },
        "sync": None,
        "written_at": NOW,
    }
    source = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    (prepared.paths.segment_root / "runner.json").write_bytes(source)
    return source


def _make_harness(
    tmp_path: Path,
    *,
    preemptible: bool = True,
) -> ContinuationHarness:
    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir)
    store.initialize()
    bundle = _make_bundle(
        tmp_path / "project",
        state_dir=state_dir,
        project_id=1,
        revision_id=1,
        key="continuation-project",
    )
    service = V5ProjectRepository(store)
    service.register_project(
        bundle.registered,
        verify_project_revision(bundle.revision),
        bundle.runtime,
    )
    item = service.admit(
        _resolved(bundle, preemption_authorized=preemptible, priority=5),
        added_at=NOW,
    )
    assert item.snapshot is not None
    worktree_root = state_dir / "worktrees"
    worktree_root.mkdir()
    manager = ProjectWorktreeManager.create(worktree_root)
    worktree = manager.prepare(
        revision=bundle.revision,
        queue_item_id=item.id,
    )
    plan = build_execution_plan(
        snapshot=item.snapshot,
        revision=bundle.revision,
        worktree=worktree.worktree,
        ambient_environment={},
        assigned_gpu="GPU-fixture",
    )
    prepared = prepare_structured_attempt(
        state_directory=state_dir,
        queue_item_id=item.id,
        experiment_id=item.experiment_id,
        attempt=item.attempt,
        segment=item.segment,
        revision=bundle.revision,
        snapshot=item.snapshot,
        execution_plan=plan,
        worktree_evidence=worktree,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1,
                pid = ?, pgid = ?, started_at = ?
            WHERE id = ?
            """,
            (os.getpid(), os.getpgid(os.getpid()), NOW, item.id),
        )
        connection.commit()
    _runner_receipt(prepared)
    return ContinuationHarness(
        service=service,
        bundle=bundle,
        prepared=prepared,
        coordinator=V5ContinuationCoordinator(service),
        item_id=item.id,
    )


def _ready_receipt(harness: ContinuationHarness, request: object) -> None:
    checkpoint = harness.bundle.enrollment.artifact_root("scratch").path / "checkpoint.bin"
    checkpoint.write_bytes(b"checkpoint-v1")
    receipt = CooperativeYieldReceipt.ready(
        request,  # type: ignore[arg-type]
        progress=YieldProgress(unit="steps", completed=7, total=20),
        checkpoint_artifacts=(
            CheckpointArtifact.from_file("checkpoint", checkpoint),
        ),
        resume_context=OpaqueResumeContext.from_json(
            {"nextStep": 8, "optimizer": "opaque-to-queue"}
        ),
        written_at=NOW,
    )
    write_yield_receipt(harness.prepared.paths.yield_receipt, receipt)


@pytest.fixture
def harness(tmp_path: Path) -> ContinuationHarness:
    return _make_harness(tmp_path)


def test_request_is_persisted_then_published_before_manual_sigint(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def signal_attempt(**arguments: object) -> bool:
        assert arguments["signum"] != 0
        record = harness.service.get_yield_request("request-order")
        assert record.request.request_id == "request-order"
        assert harness.prepared.paths.yield_request.read_bytes() == record.source
        assert harness.service.get_queue_item(harness.item_id).state == "yielding"
        observed.append("signal")
        return True

    monkeypatch.setattr(continuation_module, "signal_recorded_process", signal_attempt)
    runner_source = (harness.prepared.paths.segment_root / "runner.json").read_bytes()

    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="operator requested a checkpoint",
        actor="test:operator",
        requested_at=NOW,
        request_id="request-order",
    )

    assert observed == ["signal"]
    assert pending.prior_runner_receipt_sha256 == sha256_bytes(runner_source)
    assert pending.request.request_kind.value == "manual_preemption"
    assert pending.request.continuation.run_id == "run-1"


def test_signal_exception_preserves_active_evidence_for_restart_recovery(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def uncertain_signal(**_arguments: object) -> bool:
        raise OSError("signal syscall result unavailable")

    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        uncertain_signal,
    )

    with pytest.raises(V5ContinuationError, match="uncertain SIGINT delivery"):
        harness.coordinator.request_manual_yield(
            harness.prepared,
            note="checkpoint before uncertain signal",
            actor="test:operator",
            requested_at=NOW,
            request_id="request-signal-exception",
        )

    with harness.service.store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index, pid, pgid,
                   yield_request_id, finished_at
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
        "GPU-fixture",
        "0",
        os.getpid(),
        os.getpgid(os.getpid()),
        "request-signal-exception",
        None,
    )
    assert project_health == "open"
    assert dispatch_paused == "0"

    recovered = V5ContinuationCoordinator(harness.service).recover_pending(
        harness.prepared
    )
    assert recovered.request.request_id == "request-signal-exception"


def test_false_signal_after_ready_receipt_exit_race_remains_finalizable(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def receipt_wins_signal_race(**_arguments: object) -> bool:
        request = CooperativeYieldRequest.from_document(
            json.loads(harness.prepared.paths.yield_request.read_bytes())
        )
        _ready_receipt(harness, request)
        runner_path = harness.prepared.paths.segment_root / "runner.json"
        runner = json.loads(runner_path.read_bytes())
        runner["status"] = "yielded"
        runner["return_code"] = 75
        runner_path.write_text(
            json.dumps(runner, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return False

    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        receipt_wins_signal_race,
    )

    with pytest.raises(V5ContinuationError, match="uncertain SIGINT delivery"):
        harness.coordinator.request_manual_yield(
            harness.prepared,
            note="checkpoint at exit race",
            actor="test:operator",
            requested_at=NOW,
            request_id="request-receipt-race",
        )

    item = harness.service.get_queue_item(harness.item_id)
    assert item.state == "yielding"
    with harness.service.store.connect() as connection:
        assignment = connection.execute(
            "SELECT assigned_gpu_uuid, pid, pgid FROM queue_items WHERE id = ?",
            (harness.item_id,),
        ).fetchone()
    assert tuple(assignment) == (
        "GPU-fixture",
        os.getpid(),
        os.getpgid(os.getpid()),
    )
    recovered = harness.coordinator.recover_pending(harness.prepared)
    outcome = harness.coordinator.finalize_manual_yield(
        recovered,
        actor="scheduler:recovery",
        changed_at=NOW,
    )
    assert outcome.requeued
    assert outcome.item.state == "queued"
    assert outcome.item.segment == 2


def test_ready_receipt_requeues_same_item_at_next_resume_front_segment(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint now",
        actor="test:operator",
        requested_at=NOW,
        request_id="request-ready",
    )
    _ready_receipt(harness, pending.request)

    outcome = harness.coordinator.finalize_manual_yield(
        pending,
        actor="scheduler",
        changed_at=NOW,
    )

    assert outcome.requeued is True
    assert outcome.item.id == harness.item_id
    assert outcome.item.state == "queued"
    assert outcome.item.segment == 2
    assert outcome.item.resume_front is True
    assert outcome.receipt_record.receipt.status.value == "ready"
    assert harness.service.get_yield_receipt("request-ready") == outcome.receipt_record
    assert harness.service.get_ready_yield_receipt_for_segment(
        harness.item_id, completed_segment=1
    ) == outcome.receipt_record
    assert [item.id for item in harness.service.list_dispatch_candidates()] == [
        harness.item_id
    ]


def test_persisted_pending_operation_rehydrates_without_running_receipt(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    requested = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint before scheduler restart",
        actor="test:operator",
        requested_at=NOW,
        request_id="request-recovered",
    )
    # The project may replace its running RunnerReceipt while shutting down;
    # recovery is anchored by the already persisted request identity.
    (harness.prepared.paths.segment_root / "runner.json").write_text(
        "terminal runner evidence may differ\n",
        encoding="utf-8",
    )

    recovered = V5ContinuationCoordinator(harness.service).recover_pending(
        harness.prepared
    )

    assert recovered.request == requested.request
    assert recovered.request_source == requested.request_source
    assert recovered.prior_runner_receipt_sha256 == (
        requested.prior_runner_receipt_sha256
    )
    _ready_receipt(harness, recovered.request)
    outcome = harness.coordinator.finalize_manual_yield(
        recovered,
        actor="scheduler:recovery",
        changed_at=NOW,
    )
    assert outcome.requeued
    assert outcome.item.segment == 2


@pytest.mark.parametrize(
    ("receipt_source", "message"),
    [
        (None, "missing"),
        (b'{"not":"a receipt"}\n', "invalid"),
    ],
)
def test_missing_or_corrupt_receipt_retains_yielding_lease_and_opens_only_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_source: bytes | None,
    message: str,
) -> None:
    harness = _make_harness(tmp_path)
    second = _make_bundle(
        tmp_path / "healthy",
        state_dir=harness.service.store.state_dir,
        project_id=2,
        revision_id=2,
        key="healthy-project",
    )
    harness.service.register_project(
        second.registered,
        verify_project_revision(second.revision),
        second.runtime,
    )
    healthy_item = harness.service.admit(_resolved(second, priority=1), added_at=NOW)
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint now",
        actor="test:operator",
        requested_at=NOW,
        request_id=f"request-{message}",
    )
    if receipt_source is not None:
        harness.prepared.paths.yield_receipt.write_bytes(receipt_source)

    with pytest.raises(V5ContinuationError, match="missing or invalid"):
        harness.coordinator.finalize_manual_yield(
            pending,
            actor="scheduler",
            changed_at=NOW,
        )

    isolated = harness.service.get_queue_item(harness.item_id)
    assert isolated.state == "yielding"
    with harness.service.store.connect() as connection:
        runtime = connection.execute(
            """
            SELECT assigned_gpu_uuid, runtime_gpu_lease_held, pid
            FROM queue_items WHERE id = ?
            """,
            (harness.item_id,),
        ).fetchone()
    assert tuple(runtime) == ("GPU-fixture", 1, os.getpid())
    assert harness.service.get_project(project_id=1).runtime_state.health.value == "open"
    assert harness.service.get_project(project_id=2).runtime_state.health.value == "closed"
    assert [item.id for item in harness.service.list_dispatch_candidates()] == [
        healthy_item.id
    ]


def test_project_failed_receipt_fails_only_item_and_opens_circuit(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint now",
        actor="test:operator",
        requested_at=NOW,
        request_id="request-failed",
    )
    receipt = CooperativeYieldReceipt.failed(
        pending.request,
        error="project cannot checkpoint safely",
        written_at=NOW,
    )
    write_yield_receipt(harness.prepared.paths.yield_receipt, receipt)

    outcome = harness.coordinator.finalize_manual_yield(
        pending,
        actor="scheduler",
        changed_at=NOW,
    )

    assert outcome.requeued is False
    assert outcome.item.state == "failed"
    assert harness.service.get_project(project_id=1).runtime_state.health.value == "open"


def test_changed_checkpoint_retains_yielding_lease_before_recovery(
    harness: ContinuationHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: True,
    )
    pending = harness.coordinator.request_manual_yield(
        harness.prepared,
        note="checkpoint now",
        actor="test:operator",
        requested_at=NOW,
        request_id="request-changed-checkpoint",
    )
    _ready_receipt(harness, pending.request)
    checkpoint = harness.bundle.enrollment.artifact_root("scratch").path / "checkpoint.bin"
    checkpoint.write_bytes(b"changed-after-receipt")

    with pytest.raises(V5ContinuationError, match="failed validation"):
        harness.coordinator.finalize_manual_yield(
            pending,
            actor="scheduler",
            changed_at=NOW,
        )

    assert harness.service.get_queue_item(harness.item_id).state == "yielding"
    with harness.service.store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM cooperative_yield_receipts"
        ).fetchone()[0] == 0


def test_nonpreemptible_item_cannot_enter_manual_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _make_harness(tmp_path, preemptible=False)
    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        lambda **_arguments: pytest.fail("nonpreemptible item must not be signaled"),
    )

    with pytest.raises(V5ContinuationError, match="explicitly preemptible"):
        harness.coordinator.request_manual_yield(
            harness.prepared,
            note="should be rejected",
            actor="test:operator",
            requested_at=NOW,
        )
