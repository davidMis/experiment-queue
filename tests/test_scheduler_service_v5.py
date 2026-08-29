"""Run schema-v5 service iterations without touching GPUs or operator state."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import sqlite3
import subprocess
import sys
import time

import pytest

import experiment_queue.continuation_v5 as continuation_module
import experiment_queue.host_locks as host_locks_module
import experiment_queue.legacy_continuation_v0 as legacy_continuation_module
import experiment_queue.scheduler_service_v5 as scheduler_service_module
from experiment_queue.admission import Submission
from experiment_queue.attempt_runtime import (
    AttemptLaunchUncertainError,
    AttemptPaths,
    launch_prepared_attempt,
    signal_recorded_process,
)
from experiment_queue.authoring import Project
from experiment_queue.cli_v5 import main as cli_main
from experiment_queue.continuation_v5 import V5ContinuationError
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.git_resolver import (
    compile_admission_from_revision,
    verify_project_revision,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
    ProjectRevision,
    ProjectRuntimeState,
    RegisteredProject,
)
from experiment_queue.project_worktrees import (
    ProjectWorktreeError,
    ProjectWorktreeManager,
)
from experiment_queue.queue import GpuSnapshot
from experiment_queue.reservation_v5 import V5ReservationService
from experiment_queue.scheduler_service_v5 import (
    V5SchedulerService,
    V5SchedulerServiceError,
)
from experiment_queue.scheduler_v5 import V5SchedulerError, V5SchedulingController
from experiment_queue.v5_operator_repository import V5OperatorRepository
from experiment_queue.v5_repository import V5ProjectRepository
from experiment_queue.web_v5 import (
    ROLE_HOST_ADMIN,
    ROLE_VIEWER,
    V5AuthManager,
    V5WebApplication,
    V5WebRepositoryAdapter,
    initialize_v5_web_auth,
)


NOW = "2026-08-28T17:00:00+00:00"
SHA = "0" * 64


@pytest.fixture(autouse=True)
def _isolated_host_gpu_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep disposable service tests out of the real host-wide GPU namespace."""

    parent = tmp_path / "host-lock-parent"
    parent.mkdir()
    monkeypatch.setattr(host_locks_module, "HOST_LOCK_PARENT", parent)
    if sys.platform == "darwin":
        # Production Linux sidecars carry /proc start ticks.  macOS has no
        # supported durable token, so real-child service tests supply an exact
        # fixture-owned live identity while attempt_runtime tests verify that
        # the production primitive itself rejects a missing token.
        def fixture_identity(
            *,
            pid: int,
            pgid: int,
            process_start_ticks: str | None,
        ) -> bool:
            del process_start_ticks
            try:
                os.kill(pid, 0)
                return os.getpgid(pid) == pgid
            except (ProcessLookupError, PermissionError):
                return False

        def fixture_signal(
            *,
            pid: int,
            pgid: int,
            process_start_ticks: str | None,
            signum: int,
        ) -> bool:
            if not fixture_identity(
                pid=pid,
                pgid=pgid,
                process_start_ticks=process_start_ticks,
            ):
                return False
            try:
                if signum == signal.SIGKILL:
                    os.killpg(pgid, signum)
                else:
                    os.kill(pid, signum)
            except ProcessLookupError:
                return False
            return True

        monkeypatch.setattr(
            scheduler_service_module,
            "process_identity_matches",
            fixture_identity,
        )
        monkeypatch.setattr(
            scheduler_service_module,
            "signal_recorded_process",
            fixture_signal,
        )
        monkeypatch.setattr(
            continuation_module,
            "signal_recorded_process",
            fixture_signal,
        )
        monkeypatch.setattr(
            legacy_continuation_module,
            "signal_recorded_process",
            fixture_signal,
        )


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def _legacy_project(
    connection: sqlite3.Connection,
    *,
    project_id: int,
    key: str,
    checkout: Path,
    commit: str,
    card_path: str,
    card_sha256: str,
    command: str,
    priority: int,
    preemptible: bool = False,
    git_ref: str | None = None,
    worktree: Path | None = None,
    source_schema_version: int = 4,
) -> int:
    revision_id = project_id * 10
    item_id = project_id * 100
    connection.execute(
        """
        INSERT INTO projects(
            id, project_key, display_name, lifecycle, current_revision_id,
            current_revision_sequence, created_at, created_by,
            lifecycle_changed_at, lifecycle_actor, lifecycle_reason
        ) VALUES (?, ?, ?, 'active', ?, 1, ?, 'tester', ?, 'tester', 'fixture')
        """,
        (project_id, key, key, revision_id, NOW, NOW),
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
            str(checkout),
            b"{}",
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
        INSERT INTO migration_sources(
            source_schema_version, source_state_path, source_database_path,
            source_database_sha256, source_database_size_bytes,
            source_database_mtime_ns, source_state_identity_json,
            source_state_identity_sha256, project_id, revision_id,
            importer_package_version, imported_at, imported_by
        ) VALUES (?, ?, ?, ?, 1, 1, ?, ?, ?, ?, 'test-fixture', ?, 'tester')
        """,
        (
            source_schema_version,
            str(checkout.parent / f"legacy-state-{project_id}"),
            str(checkout.parent / f"legacy-state-{project_id}" / "queue.sqlite3"),
            hashlib.sha256(f"database:{project_id}".encode()).hexdigest(),
            b"{}",
            hashlib.sha256(f"identity:{project_id}".encode()).hexdigest(),
            project_id,
            revision_id,
            NOW,
        ),
    )
    connection.execute(
        """
        INSERT INTO queue_items(
            id, project_id, revision_id, admission_kind, snapshot_id, job_id,
            experiment_id, attempt, state, priority, card_path, card_sha256,
            command_text, runner_name, git_commit, added_at, added_by,
            git_ref, worktree_path, worktree_created_at, preemptible
        ) VALUES (?, ?, ?, 'LegacyMarkdownCard/v0', NULL, NULL, ?, 1,
                  'queued', ?, ?, ?, ?, 'legacy-fixture', ?, ?, 'tester',
                  ?, ?, ?, ?)
        """,
        (
            item_id,
            project_id,
            revision_id,
            f"{key.upper()}-001",
            priority,
            card_path,
            card_sha256,
            command,
            commit,
            NOW,
            git_ref,
            None if worktree is None else str(worktree),
            None if worktree is None else NOW,
            int(preemptible),
        ),
    )
    return item_id


def _checkout(tmp_path: Path, name: str) -> tuple[Path, str, str, str]:
    checkout = tmp_path / name
    checkout.mkdir()
    (checkout / ".gitignore").write_text("/outputs/\n", encoding="utf-8")
    card_relative = "cards/legacy.md"
    card = checkout / card_relative
    card.parent.mkdir()
    card.write_text(f"# {name} legacy evidence\n", encoding="utf-8")
    _git(checkout, "init", "-q")
    _git(checkout, "config", "user.name", "Queue Test")
    _git(checkout, "config", "user.email", "queue@example.invalid")
    _git(checkout, "add", ".")
    _git(checkout, "commit", "-qm", "legacy fixture")
    return (
        checkout,
        _git(checkout, "rev-parse", "HEAD"),
        card_relative,
        hashlib.sha256(card.read_bytes()).hexdigest(),
    )


def _gpu() -> GpuSnapshot:
    return GpuSnapshot(
        index="0",
        uuid="GPU-fixture",
        name="Fixture GPU",
        memory_total_mib=10_000,
        memory_used_mib=0,
        utilization_percent=0,
    )


def _allow_fixture_gpu(store: V5QueueStore) -> None:
    """Enroll the disposable GPU identity used by service tests."""

    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )


def test_service_rejects_redirected_or_writable_worktree_root(
    tmp_path: Path,
) -> None:
    """Startup never follows or trusts an insecure managed-root leaf."""

    for kind in ("symlink", "writable"):
        state = (tmp_path / kind / "state").resolve()
        store = V5QueueStore(state)
        store.initialize()
        worktrees = state / "worktrees"
        if kind == "symlink":
            external = tmp_path / kind / "external"
            external.mkdir()
            worktrees.symlink_to(external, target_is_directory=True)
        else:
            worktrees.mkdir(mode=0o700)
            worktrees.chmod(0o770)
        with pytest.raises(
            V5SchedulerServiceError,
            match="scheduler worktree root is unsafe",
        ):
            V5SchedulerService(
                store,
                min_free_disk_gib=0,
                gpu_provider=lambda: [_gpu()],
                ambient_environment={},
                clock=lambda: NOW,
            )


def _structured_project(
    tmp_path: Path,
    store: V5QueueStore,
    *,
    produce_artifact: bool = True,
    cooperative: bool = False,
    project_id: int = 1,
    revision_id: int = 1,
    key: str = "structured-project",
    experiment_id: str = "SHARED-001",
    priority: int = 0,
    signal_counter: Path | None = None,
) -> tuple[int, Path]:
    """Register and admit one exact-Git structured job that writes an artifact."""

    prefix = "structured" if key == "structured-project" else key
    checkout = tmp_path / f"{prefix}-checkout"
    scratch = tmp_path / f"{prefix}-scratch"
    environment_bin = tmp_path / f"{prefix}-environment-bin"
    checkout.mkdir()
    scratch.mkdir()
    environment_bin.mkdir()
    marker = scratch / "runs" / "result.txt"
    project_document = {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": key,
            "displayName": f"Structured project {key}",
        },
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True}
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {"inherit": "none", "allowVariables": []},
            "supportedProtocols": (
                [
                    {
                        "apiVersion": "experiment-queue/v1",
                        "kind": "CooperativeYieldRequest",
                    },
                    {
                        "apiVersion": "experiment-queue/v1",
                        "kind": "CooperativeYieldReceipt",
                    },
                ]
                if cooperative
                else []
            ),
        },
    }
    command = (
        (
            "from pathlib import Path; "
            f"p = Path({str(marker)!r}); "
            "p.parent.mkdir(parents=True, exist_ok=True); "
            "p.write_text('structured', encoding='utf-8')"
        )
        if produce_artifact
        else "raise SystemExit(0)"
    )
    command_document: dict[str, object] = {
        "type": "argv",
        "argv": [sys.executable, "-c", command],
    }
    if cooperative:
        worker = checkout / "cooperative_worker.py"
        worker_source = """from __future__ import annotations
import json
import os
from pathlib import Path
import signal
import sys
import time
from experiment_queue.cooperative_yield import CooperativeYieldHelper, OpaqueResumeContext, YieldProgress

stopping = False
def handle(_signum, _frame):
    global stopping
    stopping = True

signal.signal(signal.SIGINT, handle)
runner = Path(os.environ["EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"])
document = {
    "apiVersion": "experiment-queue/v1",
    "kind": "RunnerReceipt",
    "run_id": "service-e2e-run",
    "queue_item_id": int(os.environ["EXPERIMENT_QUEUE_ITEM_ID"]),
    "segment": int(os.environ["EXPERIMENT_QUEUE_SEGMENT"]),
    "status": "running",
    "return_code": None,
    "run_directory": str(runner.parent / "project-run"),
    "manifest": str(runner.parent / "project-run" / "manifest.json"),
    "logs": {"stdout": str(runner.parent / "stdout.log"), "stderr": str(runner.parent / "stderr.log")},
    "sync": None,
    "written_at": "2026-08-28T17:00:00+00:00",
}
runner.write_text(json.dumps(document, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
while not stopping:
    time.sleep(0.01)
helper = CooperativeYieldHelper.from_environment()
if helper is None:
    raise RuntimeError("missing cooperative-yield environment")
request = helper.request_if_present()
if request is None:
    raise RuntimeError("missing published cooperative-yield request")
checkpoint = Path(sys.argv[1]) / "checkpoint.bin"
checkpoint.write_bytes(b"service-e2e-checkpoint")
helper.write_ready(
    request,
    checkpoint_files={"checkpoint": checkpoint},
    progress=YieldProgress(unit="steps", completed=3, total=10),
    resume_context=OpaqueResumeContext.from_json({"nextStep": 4}),
)
"""
        if signal_counter is not None:
            worker_source = worker_source.replace(
                "stopping = False\ndef handle(_signum, _frame):\n"
                "    global stopping\n    stopping = True",
                "stopping = False\n"
                f"signal_counter = Path({str(signal_counter)!r})\n"
                "def handle(_signum, _frame):\n"
                "    count = int(signal_counter.read_text()) + 1 "
                "if signal_counter.exists() else 1\n"
                "    signal_counter.write_text(str(count))",
            )
        worker.write_text(worker_source, encoding="utf-8")
        command_document = {
            "type": "argv",
            "argv": [sys.executable, "cooperative_worker.py", str(scratch)],
        }
    job: dict[str, object] = {
        "id": "run",
        "environment": "python",
        "command": command_document,
        "resources": {"gpus": 1},
        "artifacts": [
            {
                "name": "result",
                "root": "scratch",
                "path": "runs/result.txt",
                "type": "file",
                "required": True,
            }
        ],
    }
    if cooperative:
        artifacts = job["artifacts"]
        assert isinstance(artifacts, list)
        artifacts.append(
            {
                "name": "checkpoint",
                "root": "scratch",
                "path": "checkpoint.bin",
                "type": "file",
                "required": False,
            }
        )
        job["capabilities"] = {
            "cooperativeYield": {
                "requestProtocol": {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldRequest",
                },
                "receiptProtocol": {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldReceipt",
                },
                "checkpointArtifacts": ["checkpoint"],
            }
        }
    card_document = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": key,
            "experimentId": experiment_id,
            "title": "Structured scheduler service fixture",
        },
        "spec": {
            "parameters": {},
            "jobs": [job],
        },
    }
    project_source = (json.dumps(project_document, indent=2) + "\n").encode()
    card_source = (json.dumps(card_document, indent=2) + "\n").encode()
    (checkout / "project.yaml").write_bytes(project_source)
    cards = checkout / "cards"
    cards.mkdir()
    (cards / "run.yaml").write_bytes(card_source)
    _git(checkout, "init", "-q")
    _git(checkout, "config", "user.name", "Queue Test")
    _git(checkout, "config", "user.email", "queue@example.invalid")
    _git(checkout, "add", ".")
    _git(checkout, "commit", "-qm", "structured fixture")
    commit = _git(checkout, "rev-parse", "HEAD")

    project = Project.from_yaml(project_source, source_name="project.yaml")
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=checkout,
        project_manifest_path="project.yaml",
        mounts=(MountBinding.create(name="scratch", path=scratch, access="readWrite"),),
        environments=(
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=(environment_bin,),
            ),
        ),
        state_directory=store.state_dir,
    )
    revision = ProjectRevision.create(
        revision_id=revision_id,
        project_id=project_id,
        sequence=1,
        project=project,
        project_source_path="project.yaml",
        project_source=project_source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    registered = RegisteredProject.register(
        revision=revision,
        reason="structured scheduler fixture",
        actor="test:operator",
        changed_at=NOW,
    )
    runtime = ProjectRuntimeState.create(
        project_id=project_id,
        project_key=project.key,
        reason="healthy",
        actor="test:operator",
        changed_at=NOW,
    )
    repository = V5ProjectRepository(store)
    repository.register_project(
        registered,
        verify_project_revision(revision),
        runtime,
    )
    resolved = compile_admission_from_revision(
        revision=revision,
        submission=Submission(
            project_key=project.key,
            card_path="cards/run.yaml",
            job_id="run",
            operator="test:operator",
            preemption_authorized=cooperative,
            priority=priority,
        ),
    )
    item = repository.admit(resolved, added_at=NOW)
    return item.id, marker


def _paused_structured_starting_claim(
    tmp_path: Path,
) -> tuple[V5QueueStore, V5SchedulerService, int, object]:
    """Build one disposable paused pre-launch claim for recovery guard tests."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        produce_artifact=False,
    )
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    assert service.controller.claim(
        item_id,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    service.controller.pause_host(
        reason="inspect abandoned attempt",
        actor="test:operator",
        changed_at=NOW,
    )
    return store, service, item_id, context


def _running_typed_cooperative_attempt(
    tmp_path: Path,
    *,
    manual_yield_signal_retry_seconds: float = 5.0,
) -> tuple[V5QueueStore, V5SchedulerService, int, AttemptPaths, object]:
    """Launch one disposable typed cooperative worker for crash-window tests."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(tmp_path, store, cooperative=True)
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
        manual_yield_signal_retry_seconds=manual_yield_signal_retry_seconds,
    )
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "running" and (paths.segment_root / "runner.json").is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"
    assert (paths.segment_root / "runner.json").is_file()
    return store, service, item_id, paths, service.processes[item_id]


def test_legacy_project_failure_isolated_while_healthy_project_dispatches(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    store = V5QueueStore(state.resolve())
    store.initialize()
    broken_checkout, broken_commit, broken_card, _broken_hash = _checkout(
        tmp_path, "broken"
    )
    healthy_checkout, healthy_commit, healthy_card, healthy_hash = _checkout(
        tmp_path, "healthy"
    )
    marker = healthy_checkout / "completed.txt"
    with store.connect() as connection:
        broken_item = _legacy_project(
            connection,
            project_id=1,
            key="broken-project",
            checkout=broken_checkout,
            commit=broken_commit,
            card_path=broken_card,
            card_sha256="f" * 64,
            command="true",
            priority=100,
        )
        healthy_item = _legacy_project(
            connection,
            project_id=2,
            key="healthy-project",
            checkout=healthy_checkout,
            commit=healthy_commit,
            card_path=healthy_card,
            card_sha256=healthy_hash,
            command=f"printf healthy > '{marker}'",
            priority=10,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    service.run_iteration(force_gpu_poll=True)
    with store.connect() as connection:
        broken_health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        host_paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert broken_health == "open"
    assert host_paused == "0"

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        with store.connect() as connection:
            state_value = connection.execute(
                "SELECT state FROM queue_items WHERE id = ?", (healthy_item,)
            ).fetchone()[0]
        if state_value == "succeeded":
            break
        time.sleep(0.01)
    assert state_value == "succeeded"
    assert marker.read_text() == "healthy"
    with store.connect() as connection:
        broken_state = connection.execute(
            "SELECT state FROM queue_items WHERE id = ?", (broken_item,)
        ).fetchone()[0]
        healthy_receipt = connection.execute(
            "SELECT return_code FROM queue_items WHERE id = ?", (healthy_item,)
        ).fetchone()[0]
    assert broken_state == "queued"
    assert healthy_receipt == 0


def test_terminal_legacy_item_cleans_only_destination_runtime_and_preserves_history(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-cleanup")
    worktree = tmp_path / "legacy-worktree"
    item_id = 100
    git_ref = f"refs/experiment-queue/items/{item_id}"
    _git(checkout, "update-ref", git_ref, commit)
    _git(checkout, "worktree", "add", "--detach", str(worktree), git_ref)
    with store.connect() as connection:
        inserted = _legacy_project(
            connection,
            project_id=1,
            key="legacy-cleanup",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command="true",
            priority=1,
            git_ref=git_ref,
            worktree=worktree,
        )
        assert inserted == item_id
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "succeeded":
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    assert worktree.is_dir()
    assert _git(checkout, "rev-parse", "--verify", git_ref) == commit
    with store.connect() as connection:
        cleanup = connection.execute(
            """
            SELECT worktree_removed_at, worktree_cleanup_error,
                   runtime_git_ref, runtime_worktree_path,
                   runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert cleanup["worktree_removed_at"] is None
    assert cleanup["worktree_cleanup_error"] is None
    assert cleanup["runtime_git_ref"] == (
        f"refs/experiment-queue/projects/legacy-cleanup/revisions/10/items/{item_id}"
    )
    assert cleanup["runtime_worktree_path"] != str(worktree)
    assert not Path(cleanup["runtime_worktree_path"]).exists()
    assert cleanup["runtime_worktree_removed_at"] == NOW
    assert cleanup["runtime_worktree_cleanup_error"] is None


def test_pending_legacy_item_runs_pinned_old_commit_in_destination_worktree(
    tmp_path: Path,
) -> None:
    """A v4-style queued ref remains runnable after the primary checkout advances."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-old-commit")
    (checkout / "version.txt").write_text("old\n", encoding="utf-8")
    _git(checkout, "add", "version.txt")
    _git(checkout, "commit", "-qm", "old queued revision")
    commit = _git(checkout, "rev-parse", "HEAD")
    item_id = 100
    historical_ref = f"refs/experiment-queue/items/{item_id}"
    _git(checkout, "update-ref", historical_ref, commit)
    (checkout / "version.txt").write_text("new\n", encoding="utf-8")
    _git(checkout, "commit", "-qam", "advance primary checkout")
    marker = checkout / "outputs" / "observed-version.txt"
    with store.connect() as connection:
        _legacy_project(
            connection,
            project_id=1,
            key="legacy-old-commit",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                "cd ~/3D_Helmholtz\n"
                'test "$(cat version.txt)" = old && '
                'printf old > "$EXPERIMENT_QUEUE_PRIMARY_REPO/'
                'outputs/observed-version.txt"'
            ),
            priority=1,
            git_ref=historical_ref,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "succeeded":
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    assert marker.read_text(encoding="utf-8") == "old"
    assert (checkout / "version.txt").read_text(encoding="utf-8") == "new\n"
    assert _git(checkout, "rev-parse", "--verify", historical_ref) == commit
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT git_ref, worktree_path, runtime_git_ref,
                   runtime_worktree_path, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert row["git_ref"] == historical_ref
    assert row["worktree_path"] is None
    assert row["runtime_git_ref"] != historical_ref
    assert not Path(str(row["runtime_worktree_path"])).exists()
    assert row["runtime_worktree_removed_at"] == NOW


def test_legacy_child_git_status_inherits_frozen_shared_path_excludes(
    tmp_path: Path,
) -> None:
    """Imported require-clean commands do not reject compatibility symlinks."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-clean-command")
    marker = checkout / "outputs" / "clean-command-ran.txt"
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="legacy-clean-command",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                'test -z "$(git status --porcelain --untracked-files=all)" && '
                'printf clean > "$EXPERIMENT_QUEUE_PRIMARY_REPO/'
                'outputs/clean-command-ran.txt"'
            ),
            priority=1,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state in {"succeeded", "failed"}:
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    assert marker.read_text() == "clean"


@pytest.mark.parametrize("source_schema_version", [2, 3])
def test_authentic_pre_v4_legacy_continuation_runs_without_metadata_columns(
    tmp_path: Path,
    source_schema_version: int,
) -> None:
    """Schema-v2/v3 resumes authenticate their historical checkpoint shape."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(
        tmp_path,
        f"legacy-v{source_schema_version}-continuation",
    )
    run_directory = tmp_path / f"legacy-v{source_schema_version}-run"
    run_directory.mkdir()
    checkpoint = run_directory / "checkpoint.bin"
    checkpoint.write_bytes(f"v{source_schema_version}-checkpoint".encode())
    marker = checkout / "outputs" / f"v{source_schema_version}-resumed.txt"
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key=f"legacy-v{source_schema_version}-continuation",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                'test "$EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT" = '
                f"{shlex.quote(str(checkpoint))} && "
                'printf resumed > "$EXPERIMENT_QUEUE_PRIMARY_REPO/outputs/'
                f'v{source_schema_version}-resumed.txt"'
            ),
            priority=1,
            source_schema_version=source_schema_version,
        )
        connection.execute(
            """
            UPDATE queue_items
            SET segment = 2, preemptible = 1, runner_run_dir = ?,
                continuation_checkpoint = ?,
                continuation_checkpoint_sha256 = ?,
                continuation_checkpoint_metadata = NULL,
                continuation_checkpoint_metadata_sha256 = NULL
            WHERE id = ?
            """,
            (
                str(run_directory),
                str(checkpoint),
                hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                item_id,
            ),
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state in {"succeeded", "failed"}:
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    assert marker.read_text() == "resumed"


def test_dirty_legacy_worktree_is_preserved_and_project_quarantined(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-dirty")
    worktree = tmp_path / "legacy-dirty-worktree"
    item_id = 100
    git_ref = f"refs/experiment-queue/items/{item_id}"
    _git(checkout, "update-ref", git_ref, commit)
    _git(checkout, "worktree", "add", "--detach", str(worktree), git_ref)
    with store.connect() as connection:
        _legacy_project(
            connection,
            project_id=1,
            key="legacy-dirty",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                'printf preserve > "$EXPERIMENT_QUEUE_WORKTREE/'
                'untracked-scientific-output.bin"'
            ),
            priority=1,
            git_ref=git_ref,
            worktree=worktree,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "succeeded":
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    with store.connect() as connection:
        runtime_path = Path(
            str(
                connection.execute(
                    "SELECT runtime_worktree_path FROM queue_items WHERE id = ?",
                    (item_id,),
                ).fetchone()[0]
            )
        )
    scientific_output = runtime_path / "untracked-scientific-output.bin"
    assert scientific_output.read_text() == "preserve"
    assert worktree.is_dir()
    assert _git(checkout, "rev-parse", "--verify", git_ref) == commit
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    assert row["runtime_worktree_removed_at"] is None
    assert "is dirty" in row["runtime_worktree_cleanup_error"]
    assert health == "open"


def test_ignored_shared_legacy_output_is_preserved_and_project_quarantined(
    tmp_path: Path,
) -> None:
    """Cleanup treats ignored content replacing a compatibility link as data."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-ignored-output")
    historical_worktree = tmp_path / "legacy-ignored-historical-worktree"
    item_id = 100
    historical_ref = f"refs/experiment-queue/items/{item_id}"
    _git(checkout, "update-ref", historical_ref, commit)
    _git(
        checkout,
        "worktree",
        "add",
        "--detach",
        str(historical_worktree),
        historical_ref,
    )
    with store.connect() as connection:
        _legacy_project(
            connection,
            project_id=1,
            key="legacy-ignored-output",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                'rm -- "$EXPERIMENT_QUEUE_WORKTREE/outputs" && '
                'mkdir -p "$EXPERIMENT_QUEUE_WORKTREE/outputs/data" && '
                'printf preserve > "$EXPERIMENT_QUEUE_WORKTREE/'
                'outputs/data/scientific.bin"'
            ),
            priority=1,
            git_ref=historical_ref,
            worktree=historical_worktree,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={"PATH": "/usr/bin:/bin"},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "succeeded":
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_path, runtime_worktree_removed_at,
                   runtime_worktree_cleanup_error, git_ref, worktree_path
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    runtime = Path(str(row["runtime_worktree_path"]))
    assert (runtime / "outputs/data/scientific.bin").read_text() == "preserve"
    assert row["runtime_worktree_removed_at"] is None
    assert "ignored non-compatibility content" in row["runtime_worktree_cleanup_error"]
    assert row["git_ref"] == historical_ref
    assert row["worktree_path"] == str(historical_worktree)
    assert historical_worktree.is_dir()
    assert _git(checkout, "rev-parse", "--verify", historical_ref) == commit
    assert health == "open"


def test_legacy_orphan_cleanup_rejects_persisted_runtime_path_substitution(
    tmp_path: Path,
) -> None:
    """Cleanup never acts on either side of a substituted legacy DB path."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-substitution")
    historical_ref = "refs/experiment-queue/items/100"
    _git(checkout, "update-ref", historical_ref, commit)
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="legacy-substitution",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command="true",
            priority=1,
            git_ref=historical_ref,
        )
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
    )
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    assert context.legacy_context is not None
    legitimate = context.legacy_context
    substituted = tmp_path / "operator-data-never-delete"
    substituted.mkdir()
    (substituted / "evidence.bin").write_bytes(b"preserve")
    with store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET runtime_worktree_path = ? WHERE id = ?",
            (str(substituted), item_id),
        )

    service.run_iteration(force_gpu_poll=True)

    assert legitimate.worktree_path.is_dir()
    assert _git(checkout, "rev-parse", "--verify", legitimate.git_ref) == commit
    assert (substituted / "evidence.bin").read_bytes() == b"preserve"
    with store.connect() as connection:
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        removed = connection.execute(
            "SELECT runtime_worktree_removed_at FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()[0]
    assert health == "open"
    assert removed is None


def test_legacy_orphan_cleanup_finishes_after_filesystem_only_crash_window(
    tmp_path: Path,
) -> None:
    """Absent exact runtime resources are recorded removed after a cleanup crash."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "legacy-cleanup-crash")
    historical_ref = "refs/experiment-queue/items/100"
    _git(checkout, "update-ref", historical_ref, commit)
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="legacy-cleanup-crash",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command="true",
            priority=1,
            git_ref=historical_ref,
        )
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
    )
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    assert context.legacy_context is not None
    runtime = context.legacy_context
    _git(
        checkout,
        "-c",
        f"core.excludesFile={service._legacy_excludes_file()}",  # noqa: SLF001
        "worktree",
        "remove",
        str(runtime.worktree_path),
    )
    _git(checkout, "update-ref", "-d", runtime.git_ref, commit)

    service.run_iteration(force_gpu_poll=True)

    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    assert tuple(row) == (NOW, None)
    assert health == "closed"
    assert _git(checkout, "rev-parse", "--verify", historical_ref) == commit


def test_recovery_preserves_dirty_typed_worktree_and_quarantines_only_project(
    tmp_path: Path,
) -> None:
    """A crash-window cleanup never force-deletes untracked scientific output."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        produce_artifact=False,
    )
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
    )
    item = service.repository.get_queue_item(item_id)
    revision = service.repository.get_revision(item.revision_id)
    evidence = service.worktrees.prepare(
        revision=revision,
        queue_item_id=item_id,
    )
    service.controller.record_worktree_prepared(
        evidence,
        actor="scheduler",
        changed_at=NOW,
    )
    scientific_output = evidence.worktree / "untracked-scientific-output.bin"
    scientific_output.write_bytes(b"preserve typed output")
    service.controller.pause_host(
        reason="exercise orphan cleanup",
        actor="test:operator",
        changed_at=NOW,
    )

    service.run_iteration(force_gpu_poll=True)

    assert scientific_output.read_bytes() == b"preserve typed output"
    assert evidence.worktree.is_dir()
    assert _git(revision.enrollment.checkout_directory, "rev-parse", "--verify", evidence.git_ref) == revision.git_commit
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = ?",
            (item.project_id,),
        ).fetchone()[0]
    assert row["runtime_worktree_removed_at"] is None
    assert "is dirty" in row["runtime_worktree_cleanup_error"]
    assert health == "open"


def _substitute_typed_runtime_identity(
    *,
    store: V5QueueStore,
    service: V5SchedulerService,
    item_id: int,
    tmp_path: Path,
    field: str,
) -> tuple[Path, str, Path | None, str | None]:
    """Replace one persisted field with a plausible but unauthorized identity."""

    item = service.repository.get_queue_item(item_id)
    revision = service.repository.get_revision(item.revision_id)
    expected = service.worktrees.expected_evidence(
        revision=revision,
        queue_item_id=item_id,
    )
    alternate_path: Path | None = None
    alternate_ref: str | None = None
    with store.connect() as connection:
        if field == "worktree":
            alternate_path = tmp_path / "substituted-root" / expected.worktree.name
            alternate_path.mkdir(parents=True)
            (alternate_path / "do-not-remove.txt").write_text(
                "unrelated path", encoding="utf-8"
            )
            connection.execute(
                "UPDATE queue_items SET runtime_worktree_path = ? WHERE id = ?",
                (str(alternate_path), item_id),
            )
        elif field == "git-ref":
            alternate_ref = expected.git_ref.rsplit("/", 1)[0] + f"/{item_id + 999}"
            _git(
                revision.enrollment.checkout_directory,
                "update-ref",
                alternate_ref,
                revision.git_commit,
            )
            connection.execute(
                "UPDATE queue_items SET runtime_git_ref = ? WHERE id = ?",
                (alternate_ref, item_id),
            )
        else:  # pragma: no cover - closed test parametrization
            raise AssertionError(f"unsupported substitution field {field!r}")
    return expected.worktree, expected.git_ref, alternate_path, alternate_ref


def _assert_substituted_identity_was_preserved(
    *,
    checkout: Path,
    expected_worktree: Path,
    expected_ref: str,
    alternate_path: Path | None,
    alternate_ref: str | None,
    commit: str,
) -> None:
    """Prove recovery did not clean either the real or substituted identity."""

    assert expected_worktree.is_dir()
    assert _git(checkout, "rev-parse", "--verify", expected_ref) == commit
    if alternate_path is not None:
        assert (alternate_path / "do-not-remove.txt").read_text(encoding="utf-8") == (
            "unrelated path"
        )
    if alternate_ref is not None:
        assert _git(checkout, "rev-parse", "--verify", alternate_ref) == commit


@pytest.mark.parametrize("field", ["worktree", "git-ref"])
def test_active_restart_rejects_persisted_typed_runtime_identity_substitution(
    tmp_path: Path,
    field: str,
) -> None:
    """A terminal receipt cannot authorize restart through a substituted identity."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, marker = _structured_project(tmp_path, store)
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    service.run_iteration(force_gpu_poll=True)
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )
    launched = service.processes[item_id]
    assert launched.process.wait(timeout=10) == 0
    assert paths.exit_receipt.is_file()
    assert marker.read_text(encoding="utf-8") == "structured"

    item = service.repository.get_queue_item(item_id)
    revision = service.repository.get_revision(item.revision_id)
    (
        expected_worktree,
        expected_ref,
        alternate_path,
        alternate_ref,
    ) = _substitute_typed_runtime_identity(
        store=store,
        service=service,
        item_id=item_id,
        tmp_path=tmp_path,
        field=field,
    )
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()

    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted._reconcile_restarted_processes()  # noqa: SLF001

    failed = restarted.repository.get_queue_item(item_id)
    assert failed.state == "failed"
    assert "recovered executor evidence rejected" in (failed.state_detail or "")
    if field == "worktree":
        assert "recorded worktree evidence differs" in (failed.state_detail or "")
        assert "['worktree']" in (failed.state_detail or "")
    else:
        assert "persisted runtime worktree evidence is invalid" in (
            failed.state_detail or ""
        )
        assert "gitRef" in (failed.state_detail or "")
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = ?",
            (item.project_id,),
        ).fetchone()[0]
    assert row["runtime_worktree_removed_at"] is None
    assert row["runtime_worktree_cleanup_error"] is None
    assert health == "open"
    _assert_substituted_identity_was_preserved(
        checkout=revision.enrollment.checkout_directory,
        expected_worktree=expected_worktree,
        expected_ref=expected_ref,
        alternate_path=alternate_path,
        alternate_ref=alternate_ref,
        commit=revision.git_commit,
    )


@pytest.mark.parametrize("field", ["worktree", "git-ref"])
def test_orphan_cleanup_rejects_persisted_typed_runtime_identity_substitution(
    tmp_path: Path,
    field: str,
) -> None:
    """Crash-window cleanup preserves all paths/refs when database identity changed."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        produce_artifact=False,
    )
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
    )
    item = service.repository.get_queue_item(item_id)
    revision = service.repository.get_revision(item.revision_id)
    evidence = service.worktrees.prepare(
        revision=revision,
        queue_item_id=item_id,
    )
    service.controller.record_worktree_prepared(
        evidence,
        actor="scheduler",
        changed_at=NOW,
    )
    (
        expected_worktree,
        expected_ref,
        alternate_path,
        alternate_ref,
    ) = _substitute_typed_runtime_identity(
        store=store,
        service=service,
        item_id=item_id,
        tmp_path=tmp_path,
        field=field,
    )

    service._reconcile_orphaned_worktree_cleanup()  # noqa: SLF001

    assert service.repository.get_queue_item(item_id).state == "queued"
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        runtime = connection.execute(
            """
            SELECT health, health_reason FROM project_runtime_state
            WHERE project_id = ?
            """,
            (item.project_id,),
        ).fetchone()
    assert row["runtime_worktree_removed_at"] is None
    if field == "worktree":
        assert "recorded worktree evidence differs" in row[
            "runtime_worktree_cleanup_error"
        ]
        assert "['worktree']" in row["runtime_worktree_cleanup_error"]
        assert "worktree cleanup failed" in runtime["health_reason"]
    else:
        assert row["runtime_worktree_cleanup_error"] is None
        assert "orphaned worktree cleanup could not be recovered" in runtime[
            "health_reason"
        ]
        assert "persisted runtime worktree evidence is invalid" in runtime[
            "health_reason"
        ]
        assert "gitRef" in runtime["health_reason"]
    assert runtime["health"] == "open"
    _assert_substituted_identity_was_preserved(
        checkout=revision.enrollment.checkout_directory,
        expected_worktree=expected_worktree,
        expected_ref=expected_ref,
        alternate_path=alternate_path,
        alternate_ref=alternate_ref,
        commit=revision.git_commit,
    )


def test_gpu_telemetry_failure_pauses_host_without_operator_state(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()

    def unavailable() -> list[GpuSnapshot]:
        raise RuntimeError("nvidia-smi unavailable")

    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=unavailable,
        ambient_environment={},
        clock=lambda: NOW,
    )
    service.run_iteration(force_gpu_poll=True)

    paused, reason = service.controller.host_dispatch_state()
    assert paused
    assert "nvidia-smi unavailable" in reason
    with store.connect() as connection:
        event = connection.execute(
            "SELECT scope, project_id FROM events WHERE event_type = 'HOST_DISPATCH_PAUSED'"
        ).fetchone()
    assert tuple(event) == ("host", None)


@pytest.mark.parametrize(
    "snapshots",
    [
        [
            _gpu(),
            GpuSnapshot(
                index="1", uuid="GPU-fixture", name="duplicate uuid",
                memory_total_mib=10_000, memory_used_mib=0,
                utilization_percent=0,
            ),
        ],
        [
            _gpu(),
            GpuSnapshot(
                index="0", uuid="GPU-other", name="duplicate index",
                memory_total_mib=10_000, memory_used_mib=0,
                utilization_percent=0,
            ),
        ],
        [
            GpuSnapshot(
                index="0", uuid="GPU-fixture", name="invalid metrics",
                memory_total_mib=10_000, memory_used_mib=float("nan"),
                utilization_percent=0,
            ),
        ],
        [
            GpuSnapshot(
                index="0", uuid="GPU-fixture", name="duplicate pids",
                memory_total_mib=10_000, memory_used_mib=0,
                utilization_percent=0, compute_pids=(42, 42),
            ),
        ],
    ],
)
def test_malformed_or_ambiguous_gpu_snapshot_pauses_before_dispatch(
    tmp_path: Path,
    snapshots: list[GpuSnapshot],
) -> None:
    """Dispatch and lease release share one strict telemetry trust boundary."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: snapshots,
        ambient_environment={},
        clock=lambda: NOW,
    )
    service.run_iteration(force_gpu_poll=True)
    paused, reason = service.controller.host_dispatch_state()
    assert paused
    assert "telemetry" in reason.lower()


def test_structured_item_runs_from_pinned_worktree_and_cleans_it(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, marker = _structured_project(tmp_path, store)
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "succeeded":
            break
        time.sleep(0.01)

    assert item.state == "succeeded"
    assert marker.read_text(encoding="utf-8") == "structured"
    with store.connect() as connection:
        worktree = connection.execute(
            """
            SELECT runtime_worktree_path, runtime_git_ref,
                   runtime_worktree_removed_at, runtime_worktree_cleanup_error
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        artifact = connection.execute(
            "SELECT * FROM job_artifacts WHERE queue_item_id = ?",
            (item_id,),
        ).fetchone()
    assert worktree["runtime_worktree_removed_at"] == NOW
    assert worktree["runtime_worktree_cleanup_error"] is None
    assert not Path(worktree["runtime_worktree_path"]).exists()
    checkout = tmp_path / "structured-checkout"
    missing_ref = subprocess.run(
        [
            "git",
            "-C",
            str(checkout),
            "rev-parse",
            "--verify",
            worktree["runtime_git_ref"],
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert missing_ref.returncode != 0
    assert artifact["artifact_name"] == "result"
    assert artifact["absolute_path"] == str(marker)
    assert artifact["size_bytes"] == len("structured")
    assert json.loads(bytes(artifact["metadata_json"])) == {
        "digestPolicy": "not-hashed-general-artifact",
        "present": True,
        "required": True,
    }


def test_two_structured_projects_isolate_failure_with_colliding_experiment_ids(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    failed_id, failed_marker = _structured_project(
        tmp_path,
        store,
        project_id=1,
        revision_id=11,
        key="project-one",
        experiment_id="COLLIDING-001",
        priority=100,
        produce_artifact=False,
    )
    healthy_id, healthy_marker = _structured_project(
        tmp_path,
        store,
        project_id=2,
        revision_id=22,
        key="project-two",
        experiment_id="COLLIDING-001",
        priority=10,
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    for _ in range(250):
        service.run_iteration(force_gpu_poll=True)
        failed = service.repository.get_queue_item(failed_id)
        healthy = service.repository.get_queue_item(healthy_id)
        if failed.state == "failed" and healthy.state == "succeeded":
            break
        time.sleep(0.01)

    assert failed.experiment_id == healthy.experiment_id == "COLLIDING-001"
    assert failed.attempt == healthy.attempt == 1
    assert failed.state == "failed"
    assert healthy.state == "succeeded"
    assert not failed_marker.exists()
    assert healthy_marker.read_text() == "structured"
    with store.connect() as connection:
        health = {
            int(row["project_id"]): str(row["health"])
            for row in connection.execute(
                "SELECT project_id, health FROM project_runtime_state"
            )
        }
        artifacts = list(
            connection.execute(
                "SELECT project_id, absolute_path FROM job_artifacts ORDER BY id"
            )
        )
        host_paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert health == {1: "open", 2: "closed"}
    assert [(row["project_id"], row["absolute_path"]) for row in artifacts] == [
        (2, str(healthy_marker))
    ]
    assert host_paused == "0"


def test_success_without_required_artifact_fails_only_its_project(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, marker = _structured_project(
        tmp_path,
        store,
        produce_artifact=False,
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    for _ in range(100):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "failed":
            break
        time.sleep(0.01)

    assert item.state == "failed"
    assert not marker.exists()
    assert "required artifact" in (item.state_detail or "")
    with store.connect() as connection:
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert health == "open"
    assert paused == "0"


def test_serve_once_never_dispatches_eligible_queued_work(tmp_path: Path) -> None:
    """The one-shot production surface is recovery-only and cannot orphan work."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(tmp_path, store)
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    service.run(once=True)

    assert service.repository.get_queue_item(item_id).state == "queued"
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )
    assert not paths.launch_receipt.exists()
    assert not paths.exit_receipt.exists()


def test_serve_once_reconciles_existing_terminal_receipt(tmp_path: Path) -> None:
    """Recovery-only one-shot mode still finalizes durable executor evidence."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, marker = _structured_project(tmp_path, store)
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    assert service.run_iteration(force_gpu_poll=True) is None
    launched = service.processes[item_id]
    assert launched.process.wait(timeout=5) == 0
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()

    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted.run(once=True)

    assert restarted.repository.get_queue_item(item_id).state == "succeeded"
    assert marker.read_text() == "structured"


def test_record_launched_failure_retains_live_attempt_and_gpu_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unproven group teardown cannot terminalize or release a starting claim."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
    )
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    launched_attempts: list[object] = []
    real_launch = scheduler_service_module.launch_prepared_attempt

    def capture_launch(prepared: object) -> object:
        launched = real_launch(prepared)  # type: ignore[arg-type]
        launched_attempts.append(launched)
        return launched

    def reject_record(*_args: object, **_kwargs: object) -> str:
        raise V5SchedulerError("fixture record_launched CAS failure")

    def refuse_group_stop(launched: object) -> None:
        raise AttemptLaunchUncertainError(
            "fixture live descendant remains",
            pid=launched.pid,  # type: ignore[attr-defined]
            pgid=launched.pgid,  # type: ignore[attr-defined]
            process_start_ticks=launched.process_start_ticks,  # type: ignore[attr-defined]
        )

    monkeypatch.setattr(
        scheduler_service_module,
        "launch_prepared_attempt",
        capture_launch,
    )
    monkeypatch.setattr(V5SchedulingController, "record_launched", reject_record)
    monkeypatch.setattr(
        scheduler_service_module,
        "stop_launched_attempt",
        refuse_group_stop,
    )

    try:
        service.run_iteration(force_gpu_poll=True)
        assert launched_attempts
        launched = launched_attempts[0]
        assert launched.process.poll() is None  # type: ignore[attr-defined]
        with store.connect() as connection:
            row = connection.execute(
                """
                SELECT state, assigned_gpu_uuid, runtime_gpu_lease_held,
                       runtime_worktree_path
                FROM queue_items WHERE id = ?
                """,
                (item_id,),
            ).fetchone()
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()[0]
        assert tuple(row[:3]) == ("starting", "GPU-fixture", 1)
        assert Path(str(row["runtime_worktree_path"])).is_dir()
        assert paused == "1"
        assert "GPU-fixture" in service.gpu_locks
    finally:
        if launched_attempts:
            launched = launched_attempts[0]
            if launched.process.poll() is None:  # type: ignore[attr-defined]
                os.killpg(launched.pgid, signal.SIGKILL)  # type: ignore[attr-defined]
                launched.process.wait(timeout=5)  # type: ignore[attr-defined]
        for lock in service.gpu_locks.values():
            lock.close()
        service.gpu_locks.clear()


def test_manual_typed_preemption_requeues_same_item_at_next_segment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )

    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "running" and (paths.segment_root / "runner.json").is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"
    def crash_after_publication(**_kwargs: object) -> bool:
        raise KeyboardInterrupt("fixture crash after durable request publication")

    monkeypatch.setattr(
        "experiment_queue.continuation_v5.signal_recorded_process",
        crash_after_publication,
    )
    with pytest.raises(KeyboardInterrupt, match="fixture crash"):
        service.request_manual_preemption(
            item_id,
            note="exercise typed continuation",
            actor="test:operator",
            requested_at=NOW,
        )
    assert service.repository.get_queue_item(item_id).state == "yielding"
    with store.connect() as connection:
        request_id = str(
            connection.execute(
                "SELECT yield_request_id FROM queue_items WHERE id = ?",
                (item_id,),
            ).fetchone()[0]
        )
    service.controller.pause_host(
        reason="keep resumed segment queued for assertion",
        actor="test:operator",
        changed_at=NOW,
    )
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "queued" and item.segment == 2:
            break
        time.sleep(0.01)

    assert item.state == "queued"
    assert item.segment == 2
    assert item.resume_front is True
    receipt = service.repository.get_yield_receipt(request_id)
    assert receipt.receipt.status.value == "ready"
    assert receipt.receipt.checkpoint_artifacts[0].name == "checkpoint"
    with store.connect() as connection:
        runtime = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        row = connection.execute(
            """
            SELECT assigned_gpu_uuid, pid, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        replay_events = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE queue_item_id = ?
              AND event_type = 'MANUAL_PREEMPTION_SIGNAL_RESULT'
            """,
            (item_id,),
        ).fetchone()[0]
    assert runtime == "closed"
    assert row["assigned_gpu_uuid"] == "GPU-fixture"
    assert row["pid"] is None
    assert row["runtime_worktree_removed_at"] == NOW
    assert replay_events == 1

    service.controller.resume_host(actor="test:operator", changed_at=NOW)
    resumed_paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=2,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "running":
            break
        time.sleep(0.01)
    assert item.state == "running"
    assert resumed_paths.continuation_receipt.read_bytes() == receipt.source
    resumed = service.dispatch_contexts[item_id].prepared
    assert resumed.environment["EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH"] == str(
        resumed_paths.continuation_receipt
    )
    service.request_termination(
        item_id,
        reason="end resumed-segment fixture",
        actor="test:operator",
        force=True,
        requested_at=NOW,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "force_killed":
            break
        time.sleep(0.01)
    assert item.state == "force_killed"


def test_startup_yield_signal_replay_failure_is_audited_and_quarantined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed startup replay keeps the live yielding attempt isolated."""

    store, service, item_id, _paths, launched = _running_typed_cooperative_attempt(
        tmp_path
    )

    def crash_after_publication(**_kwargs: object) -> bool:
        raise KeyboardInterrupt("fixture crash before original SIGINT")

    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        crash_after_publication,
    )
    with pytest.raises(KeyboardInterrupt, match="before original SIGINT"):
        service.request_manual_preemption(
            item_id,
            note="replay must fail closed",
            actor="test:operator",
            requested_at=NOW,
        )
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()

    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
        manual_yield_signal_retry_seconds=0,
    )
    monkeypatch.setattr(
        scheduler_service_module,
        "signal_recorded_process",
        lambda **_kwargs: False,
    )
    try:
        restarted.run_iteration(force_gpu_poll=True)
        item = restarted.repository.get_queue_item(item_id)
        with store.connect() as connection:
            assigned_gpu_uuid = connection.execute(
                "SELECT assigned_gpu_uuid FROM queue_items WHERE id = ?",
                (item_id,),
            ).fetchone()[0]
            health = connection.execute(
                "SELECT health FROM project_runtime_state WHERE project_id = 1"
            ).fetchone()[0]
            events = list(
                connection.execute(
                    """
                    SELECT payload_json FROM events
                    WHERE queue_item_id = ?
                      AND event_type = 'MANUAL_PREEMPTION_SIGNAL_RESULT'
                    """,
                    (item_id,),
                )
            )
        assert item.state == "yielding"
        assert assigned_gpu_uuid == "GPU-fixture"
        assert health == "open"
        assert len(events) == 1
        assert json.loads(events[0][0])["delivered"] is False
    finally:
        os.killpg(launched.pgid, signal.SIGKILL)
        launched.process.wait(timeout=5)
        restarted._release_gpu_lock("GPU-fixture")  # noqa: SLF001


def test_running_scheduler_retries_external_unacknowledged_yield_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scheduler created before a short-lived client crash sees durable intent."""

    store, scheduler, item_id, _paths, launched = (
        _running_typed_cooperative_attempt(
            tmp_path,
            manual_yield_signal_retry_seconds=0,
        )
    )
    external = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )

    def crash_after_publication(**_kwargs: object) -> bool:
        raise KeyboardInterrupt("fixture external client died before SIGINT")

    monkeypatch.setattr(
        continuation_module,
        "signal_recorded_process",
        crash_after_publication,
    )
    with pytest.raises(KeyboardInterrupt, match="external client died"):
        external.request_manual_preemption(
            item_id,
            note="long-lived scheduler must observe this request",
            actor="test:external-client",
            requested_at=NOW,
        )
    scheduler.controller.pause_host(
        reason="keep continuation queued for assertion",
        actor="test:operator",
        changed_at=NOW,
    )

    for _ in range(200):
        scheduler.run_iteration(force_gpu_poll=True)
        item = scheduler.repository.get_queue_item(item_id)
        if item.state == "queued" and item.segment == 2:
            break
        time.sleep(0.01)

    assert item.state == "queued"
    assert item.segment == 2
    assert launched.process.wait(timeout=5) == 0
    with store.connect() as connection:
        events = list(
            connection.execute(
                """
                SELECT event_type, payload_json FROM events
                WHERE queue_item_id = ?
                  AND event_type IN (
                    'MANUAL_PREEMPTION_SIGNAL_CLAIMED',
                    'MANUAL_PREEMPTION_SIGNAL_RESULT'
                  )
                ORDER BY id
                """,
                (item_id,),
            )
        )
    assert [event["event_type"] for event in events] == [
        "MANUAL_PREEMPTION_SIGNAL_CLAIMED",
        "MANUAL_PREEMPTION_SIGNAL_CLAIMED",
        "MANUAL_PREEMPTION_SIGNAL_RESULT",
    ]
    assert json.loads(events[-1]["payload_json"])["delivered"] is True


def test_restart_refuses_live_attempt_when_gpu_lock_cannot_be_reacquired(
    tmp_path: Path,
) -> None:
    """A crash-gap lock conflict pauses host before any live-work reconciliation."""

    store, original, item_id, _paths, launched = (
        _running_typed_cooperative_attempt(tmp_path)
    )
    original.processes.clear()
    original.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    try:
        restarted._reconcile_restarted_processes()  # noqa: SLF001
        assert restarted.repository.get_queue_item(item_id).state == "running"
        with store.connect() as connection:
            paused = connection.execute(
                "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
            ).fetchone()[0]
            health = connection.execute(
                "SELECT health FROM project_runtime_state WHERE project_id = 1"
            ).fetchone()[0]
        assert paused == "1"
        assert health == "open"
        assert "GPU-fixture" not in restarted.gpu_locks
    finally:
        if launched.process.poll() is None:
            os.killpg(launched.pgid, signal.SIGKILL)
            launched.process.wait(timeout=5)
        for lock in original.gpu_locks.values():
            lock.close()
        original.gpu_locks.clear()


def test_typed_yield_publication_failure_retains_live_identity_for_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed fsync cannot release a live child or its assigned GPU."""

    store, service, item_id, paths, launched = _running_typed_cooperative_attempt(
        tmp_path
    )
    original_publish = continuation_module._atomic_create_or_verify  # noqa: SLF001

    def fail_publication(*_args: object, **_kwargs: object) -> None:
        raise V5ContinuationError("fixture request fsync failure")

    monkeypatch.setattr(
        continuation_module,
        "_atomic_create_or_verify",
        fail_publication,
    )
    with pytest.raises(V5ContinuationError, match="live yielding process identity"):
        service.request_manual_preemption(
            item_id,
            note="exercise publication recovery",
            actor="test:operator",
            requested_at=NOW,
        )
    item = service.repository.get_queue_item(item_id)
    assert item.state == "yielding"
    with store.connect() as connection:
        active_identity = connection.execute(
            "SELECT pid, pgid, assigned_gpu_uuid FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    assert (active_identity["pid"], active_identity["pgid"]) == (
        launched.pid,
        launched.pgid,
    )
    assert active_identity["assigned_gpu_uuid"] == "GPU-fixture"
    assert launched.process.poll() is None
    assert not paths.yield_request.exists()

    monkeypatch.setattr(
        continuation_module,
        "_atomic_create_or_verify",
        original_publish,
    )
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        item = restarted.repository.get_queue_item(item_id)
        if item.state == "queued" and item.segment == 2:
            break
        time.sleep(0.01)

    assert item.state == "queued"
    assert item.segment == 2
    assert paths.yield_request.is_file()
    with store.connect() as connection:
        event = connection.execute(
            """
            SELECT payload_json FROM events
            WHERE queue_item_id = ?
              AND event_type = 'MANUAL_PREEMPTION_SIGNAL_RESULT'
            ORDER BY id DESC LIMIT 1
            """,
            (item_id,),
        ).fetchone()
    assert json.loads(event[0])["delivered"] is True


def test_two_project_cli_web_preemption_survives_scheduler_restart(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Smoke the disposable cutover surface across a real restart boundary."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    preempted_id, _preempted_marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
        project_id=1,
        revision_id=1,
        key="cutover-alpha",
        experiment_id="ALPHA-001",
        priority=10,
    )
    completed_id, completed_marker = _structured_project(
        tmp_path,
        store,
        project_id=2,
        revision_id=2,
        key="cutover-beta",
        experiment_id="BETA-001",
        priority=0,
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    first_paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="cutover-alpha",
        queue_item_id=preempted_id,
        segment=1,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        first = service.repository.get_queue_item(preempted_id)
        if first.state == "running" and (
            first_paths.segment_root / "runner.json"
        ).is_file():
            break
        time.sleep(0.01)
    assert first.state == "running"
    assert service.repository.get_queue_item(completed_id).state == "queued"
    launched = service.processes[preempted_id]

    state_arguments = ["--state-dir", str(store.state_dir)]

    def cli_json(arguments: list[str]) -> dict[str, object]:
        assert cli_main([*state_arguments, *arguments, "--json"]) == 0
        captured = capsys.readouterr()
        assert captured.err == ""
        parsed = json.loads(captured.out)
        assert isinstance(parsed, dict)
        return parsed

    paused = cli_json(
        [
            "host",
            "pause",
            "--reason",
            "hold dispatch across restart",
            "--actor",
            "smoke:operator",
        ]
    )
    assert paused["dispatchPaused"] is True
    requested = cli_json(
        [
            "item",
            "preempt",
            str(preempted_id),
            "--project",
            "cutover-alpha",
            "--note",
            "checkpoint before scheduler restart",
            "--actor",
            "smoke:operator",
        ]
    )
    assert requested["queueItemId"] == preempted_id
    assert launched.process.wait(timeout=5) == 0
    assert first_paths.exit_receipt.is_file()

    # Model a service-process loss after the executor committed its receipt but
    # before the original scheduler reconciled any terminal evidence.
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        first = restarted.repository.get_queue_item(preempted_id)
        if first.state == "queued" and first.segment == 2:
            break
        time.sleep(0.01)
    assert first.state == "queued"
    assert first.segment == 2
    assert first.resume_front is True
    assert restarted.repository.get_queue_item(completed_id).state == "queued"

    held = cli_json(
        [
            "item",
            "hold",
            str(preempted_id),
            "--project",
            "cutover-alpha",
            "--reason",
            "inspect recovered continuation",
            "--actor",
            "smoke:operator",
        ]
    )
    assert held["item"]["state"] == "held"  # type: ignore[index]
    resumed = cli_json(["host", "resume", "--actor", "smoke:operator"])
    assert resumed["dispatchPaused"] is False
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        second = restarted.repository.get_queue_item(completed_id)
        if second.state == "succeeded":
            break
        time.sleep(0.01)
    assert second.state == "succeeded"
    assert completed_marker.read_text(encoding="utf-8") == "structured"

    auth = V5AuthManager(
        initialize_v5_web_auth(
            store.state_dir,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_VIEWER: "migration-smoke-viewer-secret",
            },
            project_scopes={
                ROLE_VIEWER: ["cutover-alpha", "cutover-beta"],
            },
        )
    )
    app = V5WebApplication(
        V5WebRepositoryAdapter(
            V5OperatorRepository(store),
            V5ReservationService(store),
            restarted,
        ),
        auth,
    )
    _token, viewer = auth.issue_session(ROLE_VIEWER)
    projects = app.render_projects(viewer, {}).decode("utf-8")
    assert "cutover-alpha" in projects
    assert "cutover-beta" in projects
    first_page = app.render_item(
        viewer, "cutover-alpha", preempted_id
    ).decode("utf-8")
    assert "ALPHA-001" in first_page
    assert "BETA-001" not in first_page
    assert "1 / 2" in first_page
    assert "ready" in first_page
    second_page = app.render_item(
        viewer, "cutover-beta", completed_id
    ).decode("utf-8")
    assert "BETA-001" in second_page
    assert "ALPHA-001" not in second_page
    assert "succeeded" in second_page


def test_manual_imported_legacy_preemption_requeues_with_v0_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v5 service preserves exact cooperative-yield behavior for v4 imports."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, _commit, card, card_hash = _checkout(tmp_path, "legacy-preemption")
    worker = checkout / "legacy_preemption_worker.py"
    worker.write_text(
        """from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import signal
import time

def checkpoint(_signum, _frame):
    request = json.loads(Path(os.environ["EXPERIMENT_QUEUE_YIELD_REQUEST_PATH"]).read_text(encoding="utf-8"))
    root = Path(os.environ["EXPERIMENT_QUEUE_PRIMARY_REPO"])
    output = root / "outputs" / "legacy-preemption" / "run"
    output.mkdir(parents=True, exist_ok=True)
    payload = output / "checkpoint.bin"
    metadata = output / "checkpoint.json"
    payload.write_bytes(b"legacy-service-checkpoint")
    metadata.write_text('{"step": 4}\\n', encoding="utf-8")
    receipt = {
        "schema_version": 1,
        "status": "ready",
        "request_id": request["request_id"],
        "queue_item_id": request["queue_item_id"],
        "step": 4,
        "progress": {"unit": "steps", "completed": 4, "total": 10},
        "checkpoint": str(payload),
        "checkpoint_metadata": str(metadata),
        "checkpoint_bytes": payload.stat().st_size,
        "checkpoint_sha256": hashlib.sha256(payload.read_bytes()).hexdigest(),
        "wandb": None,
    }
    Path(os.environ["EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"]).write_text(json.dumps(receipt), encoding="utf-8")
    runner_path = Path(os.environ["EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"])
    runner_document = json.loads(runner_path.read_text(encoding="utf-8"))
    runner_document["status"] = "yielded"
    runner_document["return_code"] = 75
    runner_path.write_text(json.dumps(runner_document, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
    raise SystemExit(75)

signal.signal(signal.SIGINT, checkpoint)
runner = Path(os.environ["EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"])
run_directory = Path(os.environ["EXPERIMENT_QUEUE_PRIMARY_REPO"]) / "outputs" / "legacy-preemption" / "run"
run_directory.mkdir(parents=True, exist_ok=True)
for name in ("manifest.json", "stdout.log", "stderr.log"):
    (run_directory / name).write_text("{}\\n", encoding="utf-8")
runner.write_text(json.dumps({
    "apiVersion": "experiment-queue/v1",
    "kind": "RunnerReceipt",
    "run_id": "legacy-service-run",
    "queue_item_id": int(os.environ["EXPERIMENT_QUEUE_ITEM_ID"]),
    "segment": int(os.environ["EXPERIMENT_QUEUE_SEGMENT"]),
    "status": "running",
    "return_code": None,
    "run_directory": str(run_directory),
    "manifest": str(run_directory / "manifest.json"),
    "logs": {"stdout": str(run_directory / "stdout.log"), "stderr": str(run_directory / "stderr.log")},
    "sync": None,
    "written_at": "2026-08-28T17:00:00+00:00",
}, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
runner.with_name("worker-ready").write_text("ready", encoding="utf-8")
while True:
    time.sleep(0.01)
""",
        encoding="utf-8",
    )
    _git(checkout, "add", "legacy_preemption_worker.py")
    _git(checkout, "commit", "-qm", "add legacy preemption worker")
    commit = _git(checkout, "rev-parse", "HEAD")
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="legacy-preemption",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command=(
                f"{shlex.quote(sys.executable)} "
                f"{shlex.quote(str(worker))}"
            ),
            priority=1,
            preemptible=True,
        )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    attempt_paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="legacy-preemption",
        queue_item_id=item_id,
        segment=1,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "running" and (
            attempt_paths.segment_root / "worker-ready"
        ).is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"
    assert (attempt_paths.segment_root / "worker-ready").is_file()
    def crash_after_publication(**_kwargs: object) -> bool:
        raise KeyboardInterrupt("fixture legacy crash after request publication")

    monkeypatch.setattr(
        "experiment_queue.legacy_continuation_v0.signal_recorded_process",
        crash_after_publication,
    )
    with pytest.raises(KeyboardInterrupt, match="fixture legacy crash"):
        service.request_manual_preemption(
            item_id,
            note="exercise imported v0 continuation",
            actor="test:operator",
            requested_at=NOW,
        )
    with store.connect() as connection:
        request_id = str(
            connection.execute(
                "SELECT yield_request_id FROM queue_items WHERE id = ?",
                (item_id,),
            ).fetchone()[0]
        )
    assert request_id.startswith("legacy-manual:")
    service.controller.pause_host(
        reason="keep imported continuation queued",
        actor="test:operator",
        changed_at=NOW,
    )
    # Drop every in-memory process/context/lease reference to exercise the
    # separate scheduler recovery path, including legacy PreparedAttempt
    # reconstruction in the presence of an existing exit receipt.
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
        manual_yield_signal_retry_seconds=0,
    )
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        item = restarted.repository.get_queue_item(item_id)
        if item.state == "queued" and item.segment == 2:
            break
        time.sleep(0.01)
    assert item.state == "queued"
    assert item.segment == 2
    assert item.resume_front is True
    with store.connect() as connection:
        continuation = connection.execute(
            """
            SELECT continuation_checkpoint, continuation_checkpoint_sha256,
                   runtime_git_ref, runtime_worktree_path,
                   runtime_worktree_removed_at, git_ref, worktree_path
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        replay_events = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE queue_item_id = ?
              AND event_type = 'MANUAL_PREEMPTION_SIGNAL_RESULT'
            """,
            (item_id,),
        ).fetchone()[0]
    assert continuation["continuation_checkpoint"] is not None
    assert continuation["continuation_checkpoint_sha256"] is not None
    assert replay_events == 1
    runtime_ref = str(continuation["runtime_git_ref"])
    runtime_path = Path(str(continuation["runtime_worktree_path"]))
    assert continuation["runtime_worktree_removed_at"] == NOW
    assert not runtime_path.exists()
    historical_identity = (
        continuation["git_ref"],
        continuation["worktree_path"],
    )

    restarted.controller.resume_host(actor="test:operator", changed_at=NOW)
    resumed_paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="legacy-preemption",
        queue_item_id=item_id,
        segment=2,
    )
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        item = restarted.repository.get_queue_item(item_id)
        if item.state == "running" and (
            resumed_paths.segment_root / "worker-ready"
        ).is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"
    assert runtime_path.is_dir()
    assert _git(checkout, "rev-parse", "--verify", runtime_ref) == commit
    with store.connect() as connection:
        adopted = connection.execute(
            """
            SELECT runtime_git_ref, runtime_worktree_path,
                   runtime_worktree_removed_at, runtime_worktree_cleanup_error,
                   git_ref, worktree_path
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        adoption_events = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE queue_item_id = ? AND event_type = 'LEGACY_WORKTREE_ADOPTED'
            """,
            (item_id,),
        ).fetchone()[0]
    assert adopted["runtime_git_ref"] == runtime_ref
    assert adopted["runtime_worktree_path"] == str(runtime_path)
    assert adopted["runtime_worktree_removed_at"] is None
    assert adopted["runtime_worktree_cleanup_error"] is None
    assert (adopted["git_ref"], adopted["worktree_path"]) == historical_identity
    assert adoption_events == 2

    restarted.request_termination(
        item_id,
        reason="end re-adopted legacy segment fixture",
        actor="test:operator",
        force=True,
        requested_at=NOW,
    )
    for _ in range(200):
        restarted.run_iteration(force_gpu_poll=True)
        item = restarted.repository.get_queue_item(item_id)
        if item.state == "force_killed":
            break
        time.sleep(0.01)
    assert item.state == "force_killed"


def test_restart_adopts_launch_sidecar_from_popen_database_crash_window(
    tmp_path: Path,
) -> None:
    """A fsynced executor identity closes the Popen-to-database crash window."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
    )
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    assert service._global_gpu_lock(_gpu().uuid)  # noqa: SLF001
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    claim = service.controller.claim(
        item_id,
        gpu_uuid=_gpu().uuid,
        gpu_index=_gpu().index,
        actor="scheduler",
        changed_at=NOW,
    )
    assert claim is not None
    launched = launch_prepared_attempt(context.prepared)
    launch_receipt = context.prepared.read_launch_receipt(
        pid=launched.pid,
        pgid=launched.pgid,
        process_start_ticks=launched.process_start_ticks,
    )
    with store.connect() as connection:
        starting = connection.execute(
            "SELECT state, pid, pgid FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    assert tuple(starting) == ("starting", None, None)

    # Simulate scheduler death: the durable executor remains, but its database
    # identity write and every in-memory handle were lost.
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.controller.pause_host(
        reason="inspect launch crash window",
        actor="test:operator",
        changed_at=NOW,
    )
    resolver = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    with pytest.raises(
        V5SchedulerServiceError,
        match="live authenticated executor|extant process group",
    ):
        resolver.resolve_abandoned_launch(
            item_id,
            project_id=1,
            gpu_uuid="GPU-fixture",
            reason="must refuse the live launch",
            actor="test:operator",
            confirm="RESOLVE-ABANDONED-LAUNCH",
            changed_at=NOW,
        )
    # The guarded short-lived operator service retained the active GPU lock on
    # refusal. Its process exit would close that descriptor; model that before
    # constructing the restarted scheduler in this in-process test.
    resolver._release_gpu_lock("GPU-fixture")  # noqa: SLF001
    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted._reconcile_restarted_processes()  # noqa: SLF001

    with store.connect() as connection:
        recovered = connection.execute(
            """
            SELECT state, pid, pgid, proc_start_ticks, assigned_gpu_uuid
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(recovered) == (
        "running",
        launch_receipt.pid,
        launch_receipt.pgid,
        launch_receipt.process_start_ticks,
        "GPU-fixture",
    )
    assert "GPU-fixture" in restarted.gpu_locks

    with pytest.raises(
        V5SchedulerServiceError,
        match="live authenticated executor|extant process group",
    ):
        restarted.resolve_abandoned_launch(
            item_id,
            project_id=1,
            gpu_uuid="GPU-fixture",
            reason="must refuse a recorded live executor",
            actor="test:operator",
            confirm="RESOLVE-ABANDONED-LAUNCH",
            changed_at=NOW,
        )
    restarted.request_termination(
        item_id,
        reason="end launch-sidecar recovery fixture",
        actor="test:operator",
        force=True,
        requested_at=NOW,
    )
    # SIGKILL cannot produce an executor receipt.  The test owns this exact
    # process group and simulates the persisted-dead recovery window.
    assert launched.process.wait(timeout=5) != 0
    outcome = restarted.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator verified the recorded executor group is gone",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )
    assert outcome.resolution.previous_state == "force_killing"
    assert outcome.resolution.event_type == "DEAD_PROCESS_RESOLVED"
    assert outcome.resolution.state == "failed"
    assert outcome.launch_receipt_status == "valid-inactive"
    assert "GPU-fixture" not in restarted.gpu_locks


def test_restart_without_launch_sidecar_keeps_host_paused_and_gpu_assigned(
    tmp_path: Path,
) -> None:
    """A pre-Popen crash is ambiguous and cannot release or redispatch its GPU."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        produce_artifact=False,
    )
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    assert service.controller.claim(
        item_id,
        gpu_uuid=_gpu().uuid,
        gpu_index=_gpu().index,
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    assert not context.prepared.paths.launch_receipt.exists()

    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted._reconcile_restarted_processes()  # noqa: SLF001

    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, pid, pgid
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(row) == ("starting", "GPU-fixture", None, None)
    assert paused == "1"
    assert "GPU-fixture" in restarted.gpu_locks
    assert context.worktree_evidence is not None
    assert context.worktree_evidence.worktree.is_dir()

    outcome = restarted.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator verified the pre-Popen claim is abandoned",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )
    assert outcome.resolution.state == "failed"
    assert outcome.launch_receipt_status == "absent"
    assert outcome.worktree_cleanup_error is None
    assert not context.worktree_evidence.worktree.exists()
    with store.connect() as connection:
        resolved = connection.execute(
            "SELECT state, assigned_gpu_uuid, runtime_gpu_lease_held "
            "FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        still_paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(resolved) == ("failed", "GPU-fixture", 0)
    assert health == "open"
    assert still_paused == "1"


@pytest.mark.parametrize("force", [False, True])
def test_service_refuses_termination_before_launch_identity_is_recorded(
    tmp_path: Path,
    force: bool,
) -> None:
    """A pre-launch control cannot mutate the claim into a null-identity wedge."""

    store, service, item_id, context = _paused_structured_starting_claim(tmp_path)
    with pytest.raises(V5SchedulerError, match="recover/adopt the launch first"):
        service.request_termination(
            item_id,
            reason="pre-Popen operator race",
            actor="test:operator",
            force=force,
            requested_at=NOW,
        )

    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, pid, pgid, assigned_gpu_uuid FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    assert tuple(row) == ("starting", None, None, "GPU-fixture")
    assert not context.prepared.paths.launch_receipt.exists()
    assert not context.prepared.paths.exit_receipt.exists()


@pytest.mark.parametrize(
    ("guard", "message"),
    [
        ("confirmation", "exact confirmation"),
        ("project", "not authorized Project"),
        ("host-active", "host dispatch must already be paused"),
        ("state", "only a pre-launch 'starting' claim"),
        ("missing-gpu", "does not match confirmed GPU"),
        ("gpu-mismatch", "does not match confirmed GPU"),
        ("partial-identity", "incomplete persisted PID"),
        ("exit-receipt", "terminal executor evidence"),
        ("gpu-lock", "locked by another scheduler/process"),
    ],
)
def test_abandoned_attempt_service_rejects_each_operator_guard(
    tmp_path: Path,
    guard: str,
    message: str,
) -> None:
    """The filesystem/process boundary rechecks every operator assertion."""

    store, service, item_id, context = _paused_structured_starting_claim(tmp_path)
    project_id = 2 if guard == "project" else 1
    gpu_uuid = "GPU-other" if guard == "gpu-mismatch" else "GPU-fixture"
    confirm = "wrong" if guard == "confirmation" else "RESOLVE-ABANDONED-LAUNCH"
    lock_owner: V5SchedulerService | None = None
    if guard == "host-active":
        with store.connect() as connection:
            connection.execute(
                "UPDATE metadata SET value = '0' WHERE key = 'dispatch_paused'"
            )
    elif guard == "state":
        with store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running' WHERE id = ?",
                (item_id,),
            )
    elif guard == "missing-gpu":
        with store.connect() as connection, pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE queue_items SET assigned_gpu_uuid = NULL WHERE id = ?",
                (item_id,),
            )
        return
    elif guard == "partial-identity":
        with store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET pid = 987654321 WHERE id = ?",
                (item_id,),
            )
    elif guard == "exit-receipt":
        context.prepared.paths.exit_receipt.write_text("{}\n", encoding="utf-8")
    elif guard == "gpu-lock":
        lock_owner = V5SchedulerService(
            store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [_gpu()],
            ambient_environment={},
            clock=lambda: NOW,
        )
        assert lock_owner._global_gpu_lock("GPU-fixture")  # noqa: SLF001

    try:
        with pytest.raises(V5SchedulerServiceError, match=message):
            service.resolve_abandoned_launch(
                item_id,
                project_id=project_id,
                gpu_uuid=gpu_uuid,
                reason="operator guard test",
                actor="test:operator",
                confirm=confirm,
                changed_at=NOW,
            )
    finally:
        if lock_owner is not None:
            lock_owner._release_gpu_lock("GPU-fixture")  # noqa: SLF001

    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type IN "
            "('ABANDONED_LAUNCH_RESOLVED', 'DEAD_PROCESS_RESOLVED')"
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "state",
    ["running", "yielding", "terminating", "force_killing"],
)
def test_service_resolves_recorded_dead_identity_without_launch_sidecar(
    tmp_path: Path,
    state: str,
) -> None:
    """A confirmed absent recorded group is recoverable in every active state."""

    store, service, item_id, _context = _paused_structured_starting_claim(tmp_path)
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = ?, pid = 987654321, pgid = 987654321,
                proc_start_ticks = '123456', started_at = ?
            WHERE id = ?
            """,
            (state, NOW, item_id),
        )

    outcome = service.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator proved the recorded group and GPU workload are absent",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )

    assert outcome.resolution.previous_state == state
    assert outcome.resolution.event_type == "DEAD_PROCESS_RESOLVED"
    assert outcome.launch_receipt_status == "absent"
    assert outcome.resolution.state == "failed"
    assert "GPU-fixture" not in service.gpu_locks


def test_abandoned_launch_retains_lease_on_duplicate_gpu_telemetry_then_releases_idle(
    tmp_path: Path,
) -> None:
    """Ambiguous telemetry cannot terminalize, clean, or unlock a typed claim."""

    store, service, item_id, context = _paused_structured_starting_claim(tmp_path)
    duplicate = GpuSnapshot(
        index="1",
        uuid="GPU-fixture",
        name="Duplicate Fixture GPU",
        memory_total_mib=10_000,
        memory_used_mib=0,
        utilization_percent=0,
    )
    service.gpu_provider = lambda: []
    with pytest.raises(V5SchedulerServiceError, match="0 exact records"):
        service.resolve_abandoned_launch(
            item_id,
            project_id=1,
            gpu_uuid="GPU-fixture",
            reason="operator proved the pre-launch claim is abandoned",
            actor="test:operator",
            confirm="RESOLVE-ABANDONED-LAUNCH",
            changed_at=NOW,
        )
    service.gpu_provider = lambda: [_gpu(), duplicate]

    with pytest.raises(V5SchedulerServiceError, match="duplicate UUID"):
        service.resolve_abandoned_launch(
            item_id,
            project_id=1,
            gpu_uuid="GPU-fixture",
            reason="operator proved the pre-launch claim is abandoned",
            actor="test:operator",
            confirm="RESOLVE-ABANDONED-LAUNCH",
            changed_at=NOW,
        )
    with store.connect() as connection:
        retained = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index,
                   runtime_gpu_lease_held, runtime_gpu_lease_released_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(retained) == ("starting", "GPU-fixture", "0", 1, None)
    assert "GPU-fixture" in service.gpu_locks
    assert context.worktree_evidence is not None
    assert context.worktree_evidence.worktree.is_dir()

    service.gpu_provider = lambda: [_gpu()]
    outcome = service.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator proved the pre-launch claim is abandoned",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )
    assert outcome.resolution.state == "failed"
    with store.connect() as connection:
        released = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index,
                   runtime_gpu_lease_held, runtime_gpu_lease_released_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(released) == ("failed", "GPU-fixture", "0", 0, NOW)
    assert "GPU-fixture" not in service.gpu_locks
    assert not context.worktree_evidence.worktree.exists()


def test_force_kill_terminal_lease_survives_busy_gpu_and_restart_until_idle(
    tmp_path: Path,
) -> None:
    """Detached separate-session GPU work blocks reuse across a crash window."""

    store, service, item_id, _paths, launched = _running_typed_cooperative_attempt(
        tmp_path
    )
    runtime_worktree = service.dispatch_contexts[
        item_id
    ].worktree_evidence.worktree
    reservation = V5ReservationService(store).request_reservation(
        "GPU-fixture",
        duration_hours=2,
        note="hold GPU after this attempt",
        requested_by="reserver:test",
        requested_at=NOW,
    )
    assert reservation.status.value == "pending"

    service.request_termination(
        item_id,
        reason="exercise no-receipt forced completion",
        actor="test:operator",
        force=True,
        requested_at=NOW,
    )
    launched.process.wait(timeout=5)
    busy = GpuSnapshot(
        index="0",
        uuid="GPU-fixture",
        name="Fixture GPU",
        memory_total_mib=10_000,
        memory_used_mib=2_000,
        utilization_percent=25,
        # This PID is deliberately unrelated to the executor process group and
        # models imported scientific work continuing in a separate session.
        compute_pids=(987654321,),
    )
    service.gpu_provider = lambda: [busy]
    service.run_iteration(force_gpu_poll=True, allow_dispatch=False)

    with store.connect() as connection:
        terminal_held = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index,
                   runtime_gpu_lease_held, runtime_gpu_lease_released_at,
                   runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        host_paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(terminal_held[:5]) == (
        "force_killed", "GPU-fixture", "0", 1, None
    )
    assert terminal_held["runtime_worktree_removed_at"] is None
    assert host_paused == "0"
    assert runtime_worktree.is_dir()
    assert "GPU-fixture" in service.gpu_locks
    assert V5ReservationService(store).list_reservations()[0].status.value == "pending"

    # Simulate scheduler death after terminal commit but before lease release.
    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [busy],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted.run_iteration(force_gpu_poll=True, allow_dispatch=False)
    with store.connect() as connection:
        still_held = connection.execute(
            """
            SELECT runtime_gpu_lease_held, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(still_held) == (1, None)
    assert runtime_worktree.is_dir()
    assert "GPU-fixture" in restarted.gpu_locks
    assert V5ReservationService(store).list_reservations()[0].status.value == "pending"

    restarted.gpu_provider = lambda: [_gpu()]
    restarted.run_iteration(force_gpu_poll=True, allow_dispatch=False)
    with store.connect() as connection:
        released = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, assigned_gpu_index,
                   runtime_gpu_lease_held, runtime_gpu_lease_released_at,
                   runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        release_events = connection.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE queue_item_id = ?
              AND event_type = 'GPU_RUNTIME_LEASE_RELEASED'
            """,
            (item_id,),
        ).fetchone()[0]
    assert tuple(released[:5]) == (
        "force_killed", "GPU-fixture", "0", 0, NOW
    )
    assert released["runtime_worktree_removed_at"] == NOW
    assert release_events == 1
    assert not runtime_worktree.exists()
    assert "GPU-fixture" not in restarted.gpu_locks
    assert V5ReservationService(store).list_reservations()[0].status.value == "active"


def test_crash_after_terminal_commit_before_gpu_lease_release_is_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception in the exact terminal/release gap leaves a durable barrier."""

    store, service, item_id, _paths, launched = _running_typed_cooperative_attempt(
        tmp_path
    )
    runtime_worktree = service.dispatch_contexts[
        item_id
    ].worktree_evidence.worktree
    service.request_termination(
        item_id,
        reason="exercise terminal commit crash gap",
        actor="test:operator",
        force=True,
        requested_at=NOW,
    )
    launched.process.wait(timeout=5)

    def crash_before_release(*_args: object, **_kwargs: object) -> bool:
        raise KeyboardInterrupt("fixture crash before GPU lease release")

    monkeypatch.setattr(
        service,
        "_release_finalized_gpu_lease",
        crash_before_release,
    )
    with pytest.raises(KeyboardInterrupt, match="before GPU lease release"):
        service.run_iteration(force_gpu_poll=True, allow_dispatch=False)
    with store.connect() as connection:
        committed = connection.execute(
            """
            SELECT state, assigned_gpu_uuid, runtime_gpu_lease_held,
                   runtime_gpu_lease_released_at, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(committed) == (
        "force_killed", "GPU-fixture", 1, None, None
    )
    assert runtime_worktree.is_dir()
    assert "GPU-fixture" in service.gpu_locks

    for lock in service.gpu_locks.values():
        lock.close()
    service.gpu_locks.clear()
    service.processes.clear()
    service.dispatch_contexts.clear()
    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted.run_iteration(force_gpu_poll=True, allow_dispatch=False)
    with store.connect() as connection:
        recovered = connection.execute(
            """
            SELECT assigned_gpu_uuid, runtime_gpu_lease_held,
                   runtime_gpu_lease_released_at, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(recovered) == ("GPU-fixture", 0, NOW, NOW)
    assert not runtime_worktree.exists()


def test_abandoned_attempt_cleanup_failure_is_preserved_and_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminalization never deletes a dirty/uncertain runtime after cleanup failure."""

    store, service, item_id, context = _paused_structured_starting_claim(tmp_path)

    def reject_cleanup(_self: object, **_kwargs: object) -> None:
        raise ProjectWorktreeError("fixture scientific output blocks cleanup")

    monkeypatch.setattr(ProjectWorktreeManager, "cleanup", reject_cleanup)
    outcome = service.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator proved no process or GPU workload",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )

    assert outcome.resolution.state == "failed"
    assert outcome.worktree_cleanup_error is not None
    assert "fixture scientific output blocks cleanup" in outcome.worktree_cleanup_error
    assert context.worktree_evidence.worktree.is_dir()
    with store.connect() as connection:
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert health == "open"
    assert paused == "1"


def test_restart_rejects_tampered_launch_sidecar_without_releasing_gpu(
    tmp_path: Path,
) -> None:
    """Changed launch identity is preserved for inspection, never adopted."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
    )
    _allow_fixture_gpu(store)
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    candidate = service.controller.list_dispatch_candidates(limit=1)[0]
    context = service._prepare_dispatch(candidate, _gpu())  # noqa: SLF001
    assert service.controller.claim(
        item_id,
        gpu_uuid=_gpu().uuid,
        gpu_index=_gpu().index,
        actor="scheduler",
        changed_at=NOW,
    ) is not None
    launched = launch_prepared_attempt(context.prepared)
    document = json.loads(context.prepared.paths.launch_receipt.read_text())
    document["payload_sha256"] = "f" * 64
    context.prepared.paths.launch_receipt.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    restarted = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    restarted._reconcile_restarted_processes()  # noqa: SLF001
    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, assigned_gpu_uuid, pid FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(row) == ("starting", "GPU-fixture", None)
    assert paused == "1"
    assert "GPU-fixture" in restarted.gpu_locks
    assert context.worktree_evidence is not None
    assert context.worktree_evidence.worktree.is_dir()

    with pytest.raises(
        V5SchedulerServiceError,
        match="live authenticated executor|extant process group",
    ):
        restarted.resolve_abandoned_launch(
            item_id,
            project_id=1,
            gpu_uuid="GPU-fixture",
            reason="must reject the still-live named group",
            actor="test:operator",
            confirm="RESOLVE-ABANDONED-LAUNCH",
            changed_at=NOW,
        )
    os.killpg(launched.pgid, signal.SIGKILL)
    assert launched.process.wait(timeout=5) != 0
    outcome = restarted.resolve_abandoned_launch(
        item_id,
        project_id=1,
        gpu_uuid="GPU-fixture",
        reason="operator verified the rejected sidecar group is gone",
        actor="test:operator",
        confirm="RESOLVE-ABANDONED-LAUNCH",
        changed_at=NOW,
    )
    assert outcome.resolution.state == "failed"
    assert outcome.launch_receipt_status == "rejected"
    with store.connect() as connection:
        event = connection.execute(
            """
            SELECT event_type FROM events
            WHERE queue_item_id = ? ORDER BY id DESC LIMIT 1
            """,
            (item_id,),
        ).fetchone()[0]
    assert event in {"ABANDONED_LAUNCH_RESOLVED", "PROJECT_WORKTREE_REMOVED"}


def test_termination_from_separate_service_replays_and_escalates_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short-lived operator request remains enforceable by a new scheduler."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "termination-restart")
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="termination-restart",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command="true",
            priority=1,
        )
        connection.execute(
            """
                UPDATE queue_items
                SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                    assigned_gpu_index = '0', runtime_gpu_lease_held = 1,
                    pid = 43210, pgid = 43210,
                proc_start_ticks = '24680', started_at = ?
            WHERE id = ?
            """,
            (NOW, item_id),
        )

    epochs = [100.0]
    delivered: list[dict[str, object]] = []

    def signal_attempt(**kwargs: object) -> bool:
        delivered.append(dict(kwargs))
        return True

    monkeypatch.setattr(
        scheduler_service_module,
        "signal_recorded_process",
        signal_attempt,
    )
    monkeypatch.setattr(
        scheduler_service_module,
        "process_identity_matches",
        lambda **_kwargs: True,
    )
    operator_service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
        epoch_clock=lambda: epochs[0],
        termination_grace_seconds=10,
    )
    outcome = operator_service.request_termination(
        item_id,
        reason="stop for maintenance",
        actor="test:operator",
        requested_at=NOW,
    )
    assert outcome.signal_delivered
    assert outcome.action.stage == "interrupt"

    # This instance has no in-memory child handle or knowledge of the first
    # delivery. It safely authenticates and replays from persistent evidence.
    recovery_service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
        epoch_clock=lambda: epochs[0],
        termination_grace_seconds=10,
    )
    epochs[0] = 111.0
    recovery_service._reconcile_restarted_processes()  # noqa: SLF001
    with store.connect() as connection:
        replayed = connection.execute(
            "SELECT state, termination_stage FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    assert tuple(replayed) == ("terminating", "interrupt")
    # Only after replaying the possibly-undelivered persisted SIGINT may this
    # new scheduler advance the already-expired deadline.
    recovery_service._reconcile_restarted_processes()  # noqa: SLF001
    epochs[0] = 122.0
    recovery_service._reconcile_restarted_processes()  # noqa: SLF001

    assert [call["signum"] for call in delivered] == [
        signal.SIGINT,
        signal.SIGINT,
        signal.SIGTERM,
        signal.SIGKILL,
    ]
    assert all(call["pid"] == 43210 for call in delivered)
    assert all(call["pgid"] == 43210 for call in delivered)
    assert all(call["process_start_ticks"] == "24680" for call in delivered)
    with store.connect() as connection:
        killing = connection.execute(
            """
            SELECT state, terminate_requested_at, terminate_reason,
                   termination_stage, termination_signal_epoch
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    assert tuple(killing) == (
        "force_killing",
        NOW,
        "stop for maintenance",
        "kill",
        122.0,
    )

    # A vanished executor leader does not prove that every process in its
    # group is gone. Without a terminal receipt recovery retains the force-kill
    # intent and GPU assignment for explicit operator reconciliation.
    monkeypatch.setattr(
        scheduler_service_module,
        "process_identity_matches",
        lambda **_kwargs: False,
    )
    recovery_service._reconcile_restarted_processes()  # noqa: SLF001
    with store.connect() as connection:
        finished = connection.execute(
            "SELECT state, return_code, assigned_gpu_uuid FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
        events = [
            str(row[0])
            for row in connection.execute(
                "SELECT event_type FROM events WHERE queue_item_id = ? ORDER BY id",
                (item_id,),
            )
        ]
        paused = connection.execute(
            "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
        ).fetchone()[0]
    assert tuple(finished) == ("force_killing", None, "GPU-fixture")
    assert health == "closed"
    assert paused == "1"
    assert "GPU-fixture" in recovery_service.gpu_locks
    assert events.count("TERMINATION_REQUESTED") == 1
    assert events.count("TERMINATION_ESCALATED") == 2
    assert events.count("TERMINATION_SIGNAL_SENT") == 4
    assert "EXPERIMENT_TERMINATION_COMPLETED" not in events


def test_termination_signal_fails_closed_for_reused_process_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed PID/start-tick authentication leaves durable intent pending."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    checkout, commit, card, card_hash = _checkout(tmp_path, "termination-reused")
    with store.connect() as connection:
        item_id = _legacy_project(
            connection,
            project_id=1,
            key="termination-reused",
            checkout=checkout,
            commit=commit,
            card_path=card,
            card_sha256=card_hash,
            command="true",
            priority=1,
        )
        connection.execute(
            """
                UPDATE queue_items
                SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                    assigned_gpu_index = '0', runtime_gpu_lease_held = 1,
                    pid = 54321, pgid = 54321,
                proc_start_ticks = 'original-start', started_at = ?
            WHERE id = ?
            """,
            (NOW, item_id),
        )
    attempts: list[dict[str, object]] = []

    def reject_reused(**kwargs: object) -> bool:
        attempts.append(dict(kwargs))
        return False

    monkeypatch.setattr(
        scheduler_service_module,
        "signal_recorded_process",
        reject_reused,
    )
    service = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: NOW,
        epoch_clock=lambda: 100.0,
    )
    outcome = service.request_termination(
        item_id,
        reason="authenticate before stop",
        actor="test:operator",
    )
    assert not outcome.signal_delivered
    assert attempts == [
        {
            "pid": 54321,
            "pgid": 54321,
            "process_start_ticks": "original-start",
            "signum": signal.SIGINT,
        }
    ]
    with store.connect() as connection:
        row = connection.execute(
            "SELECT state, termination_stage FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        last_event = connection.execute(
            "SELECT event_type FROM events WHERE queue_item_id = ? ORDER BY id DESC",
            (item_id,),
        ).fetchone()[0]
    assert tuple(row) == ("terminating", "interrupt")
    assert last_event == "TERMINATION_SIGNAL_PENDING"


def test_repeated_external_termination_coalesces_scientific_sigint(
    tmp_path: Path,
) -> None:
    """Separate senders may retry, but one executor broadcasts SIGINT only once."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    counter = tmp_path / "scientific-sigint-count"
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
        signal_counter=counter,
    )
    _allow_fixture_gpu(store)
    scheduler = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        termination_grace_seconds=60,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
        epoch_clock=lambda: 100.0,
    )
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )
    for _ in range(200):
        scheduler.run_iteration(force_gpu_poll=True)
        item = scheduler.repository.get_queue_item(item_id)
        if item.state == "running" and (paths.segment_root / "runner.json").is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"

    operator = V5SchedulerService(
        store,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
        epoch_clock=lambda: 100.0,
        termination_grace_seconds=60,
    )
    try:
        first = operator.request_termination(
            item_id,
            reason="coalesce repeated graceful stop",
            actor="test:external",
            requested_at=NOW,
        )
        second = operator.request_termination(
            item_id,
            reason="coalesce repeated graceful stop",
            actor="test:external",
            requested_at=NOW,
        )
        assert first.signal_delivered and second.signal_delivered
        scheduler.run_iteration(force_gpu_poll=True)
        for _ in range(200):
            if counter.exists():
                time.sleep(0.1)
                break
            time.sleep(0.01)
        assert counter.read_text() == "1"

        operator.request_termination(
            item_id,
            reason="end coalescing fixture",
            actor="test:external",
            force=True,
            requested_at=NOW,
        )
        for _ in range(200):
            scheduler.run_iteration(force_gpu_poll=True)
            item = scheduler.repository.get_queue_item(item_id)
            if item.state == "force_killed":
                break
            time.sleep(0.01)
        assert item.state == "force_killed"
    finally:
        launched = scheduler.processes.get(item_id)
        if launched is not None and launched.process.poll() is None:
            os.killpg(launched.pgid, signal.SIGKILL)
            launched.process.wait(timeout=5)


@pytest.mark.parametrize(
    ("force", "terminal_state"),
    [(False, "interrupted"), (True, "force_killed")],
)
def test_real_process_group_termination_completes_and_cleans_worktree(
    tmp_path: Path,
    force: bool,
    terminal_state: str,
) -> None:
    """Exercise authenticated SIGINT and SIGKILL against a durable executor."""

    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    item_id, _marker = _structured_project(
        tmp_path,
        store,
        cooperative=True,
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 0, ?)
            """,
            (NOW,),
        )
    service = V5SchedulerService(
        store,
        poll_seconds=0.01,
        control_seconds=0.01,
        termination_grace_seconds=10,
        min_free_disk_gib=0,
        gpu_provider=lambda: [_gpu()],
        ambient_environment={},
        clock=lambda: NOW,
    )
    paths = AttemptPaths.create(
        state_directory=store.state_dir,
        project_key="structured-project",
        queue_item_id=item_id,
        segment=1,
    )
    for _ in range(200):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == "running" and (paths.segment_root / "runner.json").is_file():
            break
        time.sleep(0.01)
    assert item.state == "running"

    outcome = service.request_termination(
        item_id,
        reason="real process-group termination test",
        actor="test:operator",
        force=force,
        requested_at=NOW,
    )
    assert outcome.signal_delivered
    assert outcome.action.stage == ("kill" if force else "interrupt")
    for _ in range(300):
        service.run_iteration(force_gpu_poll=True)
        item = service.repository.get_queue_item(item_id)
        if item.state == terminal_state:
            break
        time.sleep(0.01)
    assert item.state == terminal_state
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT terminate_reason, termination_stage,
                   runtime_worktree_path, runtime_worktree_removed_at
            FROM queue_items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT health FROM project_runtime_state WHERE project_id = 1"
        ).fetchone()[0]
    assert row["terminate_reason"] == "real process-group termination test"
    assert row["termination_stage"] == ("kill" if force else "interrupt")
    assert row["runtime_worktree_removed_at"] == NOW
    assert not Path(str(row["runtime_worktree_path"])).exists()
    assert health == "closed"
    if force:
        assert not paths.exit_receipt.exists()
    else:
        assert paths.exit_receipt.is_file()
