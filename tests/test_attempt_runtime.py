"""Exercise project-qualified payload publication and durable executor launch."""

from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys

import pytest

import experiment_queue.attempt_runtime as attempt_runtime_module
from experiment_queue.admission import Submission, compile_admission
from experiment_queue.attempt_runtime import (
    AttemptLaunchUncertainError,
    AttemptPaths,
    AttemptRuntimeError,
    launch_prepared_attempt,
    prepare_legacy_attempt,
    prepare_structured_attempt,
    process_identity_matches,
    signal_recorded_process,
)
from experiment_queue.authoring import Project, VolumeAccess
from experiment_queue.executor import ExecutorError
from experiment_queue.execution import ExecutionPlan, build_execution_plan
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
    ProjectRevision,
)
from experiment_queue.project_worktrees import ProjectWorktreeManager


def _run_git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def _source(document: dict[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _fixture(tmp_path: Path):
    checkout = tmp_path / "checkout"
    state = tmp_path / "state"
    scratch = tmp_path / "scratch"
    work = checkout / "work"
    for directory in (checkout, state, scratch, work):
        directory.mkdir(parents=True, exist_ok=True)
    project_document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "runtime-fixture",
            "displayName": "Runtime fixture",
        },
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True}
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": ["LANG"],
            },
            "supportedProtocols": [],
        },
    }
    card_document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "runtime-fixture",
            "experimentId": "RUNTIME-001",
            "title": "Runtime fixture",
        },
        "spec": {
            "parameters": {},
            "jobs": [
                {
                    "id": "run",
                    "environment": "python",
                    "workingDirectory": "work",
                    "command": {
                        "type": "argv",
                        "argv": [sys.executable, "-c", "raise SystemExit(0)"],
                    },
                    "resources": {"gpus": 1},
                    "artifacts": [
                        {
                            "name": "result",
                            "root": "scratch",
                            "path": "runs/result.json",
                            "type": "file",
                            "required": False,
                        }
                    ],
                }
            ],
        },
    }
    project_source = _source(project_document)
    card_source = _source(card_document)
    (checkout / "project.yaml").write_bytes(project_source)
    cards = checkout / "cards"
    cards.mkdir()
    (cards / "runtime.yaml").write_bytes(card_source)
    (work / ".keep").write_text("committed work directory\n", encoding="utf-8")
    _run_git(checkout, "init", "-q")
    _run_git(checkout, "config", "user.name", "Queue Test")
    _run_git(checkout, "config", "user.email", "queue@example.invalid")
    _run_git(checkout, "add", ".")
    _run_git(checkout, "commit", "-qm", "fixture")
    commit = _run_git(checkout, "rev-parse", "HEAD")

    project = Project.from_yaml(project_source, source_name="project.yaml")
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=checkout,
        project_manifest_path="project.yaml",
        mounts=[
            MountBinding.create(
                name="scratch", path=scratch, access=VolumeAccess.READ_WRITE
            )
        ],
        environments=[
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=[Path(sys.executable).resolve().parent],
                inherit_variables=["LANG"],
            )
        ],
        state_directory=state,
    )
    revision = ProjectRevision.create(
        revision_id=11,
        project_id=7,
        sequence=1,
        project=project,
        project_source_path="project.yaml",
        project_source=project_source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at="2026-08-28T16:00:00Z",
    )
    snapshot = compile_admission(
        project_source=project_source,
        card_source=card_source,
        submission=Submission(
            project_key="runtime-fixture",
            card_path="cards/runtime.yaml",
            job_id="run",
            operator="test:operator",
        ),
        project_revision=revision.label,
        git_commit=commit,
        project_source_name="project.yaml",
    )
    worktree_root = state / "worktrees"
    worktree_root.mkdir()
    manager = ProjectWorktreeManager.create(worktree_root)
    evidence = manager.prepare(revision=revision, queue_item_id=42)
    plan = build_execution_plan(
        snapshot=snapshot,
        revision=revision,
        worktree=evidence.worktree,
        ambient_environment={
            "LANG": "C.UTF-8",
            "HOME": "/must/not/leak",
            "CUDA_VISIBLE_DEVICES": "wrong",
        },
        assigned_gpu="GPU-fixture",
    )
    return state, revision, snapshot, manager, evidence, plan


def test_attempt_paths_are_private_under_permissive_umask(tmp_path: Path) -> None:
    """Every queue-owned control directory is 0700 even with umask 0002."""

    state = (tmp_path / "state").resolve()
    state.mkdir(mode=0o700)
    prior_umask = os.umask(0o002)
    try:
        paths = AttemptPaths.create(
            state_directory=state,
            project_key="runtime-fixture",
            queue_item_id=42,
            segment=1,
        )
    finally:
        os.umask(prior_umask)

    current = state
    for component in paths.segment_root.relative_to(state).parts:
        current /= component
        assert stat.S_IMODE(current.stat(follow_symlinks=False).st_mode) == 0o700


@pytest.mark.parametrize("replacement", ["symlink", "writable"])
def test_attempt_paths_reject_unsafe_existing_component(
    tmp_path: Path,
    replacement: str,
) -> None:
    """A linked or group-writable control ancestor cannot redirect evidence."""

    state = (tmp_path / "state").resolve()
    state.mkdir(mode=0o700)
    attempts = state / "attempts"
    if replacement == "symlink":
        external = tmp_path / "external"
        external.mkdir()
        attempts.symlink_to(external, target_is_directory=True)
    else:
        attempts.mkdir(mode=0o700)
        attempts.chmod(0o770)

    with pytest.raises(
        AttemptRuntimeError,
        match="real directory|not group/world writable",
    ):
        AttemptPaths.create(
            state_directory=state,
            project_key="runtime-fixture",
            queue_item_id=42,
            segment=1,
        )


def test_prepare_launch_and_authenticate_structured_attempt(tmp_path: Path) -> None:
    state, revision, snapshot, manager, evidence, plan = _fixture(tmp_path)
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=1,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )

    environment = prepared.environment
    assert environment["CUDA_VISIBLE_DEVICES"] == "GPU-fixture"
    assert environment["EXPERIMENT_QUEUE_ITEM_ID"] == "42"
    assert environment["EXPERIMENT_QUEUE_PROJECT_REVISION_ID"] == "11"
    assert "EXPERIMENT_QUEUE_YIELD_REQUEST_PATH" not in environment
    assert environment["EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"] == str(
        prepared.paths.segment_root / "runner.json"
    )
    assert "HOME" not in environment
    payload = json.loads(prepared.paths.payload.read_text())
    assert "environment" not in payload
    assert payload["command_kind"] == "argv"
    assert payload["worktree"] == str(evidence.worktree)

    launched = launch_prepared_attempt(prepared)
    launch_receipt = prepared.read_launch_receipt(
        pid=launched.pid,
        pgid=launched.pgid,
        process_start_ticks=launched.process_start_ticks,
    )
    assert launch_receipt.queue_item_id == 42
    assert launch_receipt.project_id == revision.project_id
    assert launch_receipt.project_revision_id == revision.id
    assert launch_receipt.segment == 1
    assert launch_receipt.payload_sha256 == prepared.payload_sha256
    assert launched.pgid == launched.pid
    assert process_identity_matches(
        pid=launched.pid,
        pgid=launched.pgid,
        process_start_ticks=launched.process_start_ticks,
    ) is (launched.process_start_ticks is not None)
    assert not process_identity_matches(
        pid=launched.pid,
        pgid=launched.pgid + 1,
        process_start_ticks=launched.process_start_ticks,
    )
    assert launched.process.wait(timeout=10) == 0
    receipt = prepared.read_exit_receipt()
    assert receipt.return_code == 0
    assert receipt.project_id == revision.project_id
    assert receipt.resolved_spec_sha256 == snapshot.resolved_sha256
    assert prepared.paths.launcher_log.is_file()
    with pytest.raises(AttemptRuntimeError, match="receipt already exists"):
        launch_prepared_attempt(prepared)

    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_structured_attempt_rejects_replaced_or_forged_execution_plan(
    tmp_path: Path,
) -> None:
    """Only the builder's unchanged launch authorization crosses the boundary."""

    state, revision, snapshot, manager, evidence, plan = _fixture(tmp_path)
    with pytest.raises(TypeError, match="validated-only"):
        replace(plan, argv=("/bin/sh", "-c", "exit 0"))

    forged = object.__new__(ExecutionPlan)
    for name in (
        "project_id",
        "project_key",
        "project_revision_id",
        "project_revision",
        "git_commit",
        "resolved_spec_sha256",
        "worktree_root",
        "cwd",
        "_environment_items",
        "artifacts",
        "_integrity_sha256",
    ):
        object.__setattr__(forged, name, getattr(plan, name))
    object.__setattr__(forged, "argv", ("/bin/sh", "-c", "exit 0"))

    with pytest.raises(AttemptRuntimeError, match="factory-integrity"):
        prepare_structured_attempt(
            state_directory=state,
            queue_item_id=42,
            experiment_id="RUNTIME-001",
            attempt=1,
            segment=1,
            revision=revision,
            snapshot=snapshot,
            execution_plan=forged,
            worktree_evidence=evidence,
            gpu_uuid="GPU-fixture",
            gpu_index="0",
        )
    assert not (state / "attempts").exists()
    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_structured_attempt_rejects_plan_from_another_admission_snapshot(
    tmp_path: Path,
) -> None:
    """A same-revision plan cannot mislabel another admission's execution."""

    state, revision, snapshot, manager, evidence, plan = _fixture(tmp_path)
    alternate_card = json.loads(snapshot.card_source)
    alternate_card["metadata"]["experimentId"] = "RUNTIME-ALT"
    alternate_card["spec"]["jobs"][0]["command"]["argv"] = [
        sys.executable,
        "-c",
        "raise SystemExit(3)",
    ]
    alternate = compile_admission(
        project_source=snapshot.project_source,
        card_source=_source(alternate_card),
        submission=Submission(
            project_key="runtime-fixture",
            card_path="cards/runtime.yaml",
            job_id="run",
            operator="test:operator",
        ),
        project_revision=revision.label,
        git_commit=revision.git_commit,
        project_source_name="project.yaml",
    )

    with pytest.raises(AttemptRuntimeError, match="plan.resolved_spec_sha256"):
        prepare_structured_attempt(
            state_directory=state,
            queue_item_id=42,
            experiment_id="RUNTIME-ALT",
            attempt=1,
            segment=1,
            revision=revision,
            snapshot=alternate,
            execution_plan=plan,
            worktree_evidence=evidence,
            gpu_uuid="GPU-fixture",
            gpu_index="0",
        )
    assert not (state / "attempts").exists()
    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_structured_attempt_requires_plan_built_from_exact_recorded_worktree(
    tmp_path: Path,
) -> None:
    """A canonical descendant cannot substitute for the recorded worktree root."""

    state, revision, snapshot, manager, evidence, _plan = _fixture(tmp_path)
    descendant_root = evidence.worktree / "work"
    nested_working_directory = descendant_root / "work"
    nested_working_directory.mkdir()
    descendant_plan = build_execution_plan(
        snapshot=snapshot,
        revision=revision,
        worktree=descendant_root,
        ambient_environment={"LANG": "C.UTF-8"},
        assigned_gpu="GPU-fixture",
    )
    nested_working_directory.rmdir()

    with pytest.raises(AttemptRuntimeError, match="plan.worktree_root"):
        prepare_structured_attempt(
            state_directory=state,
            queue_item_id=42,
            experiment_id="RUNTIME-001",
            attempt=1,
            segment=1,
            revision=revision,
            snapshot=snapshot,
            execution_plan=descendant_plan,
            worktree_evidence=evidence,
            gpu_uuid="GPU-fixture",
            gpu_index="0",
        )
    assert not (state / "attempts").exists()
    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_process_identity_without_start_token_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live PID/PGID pair alone never authenticates on any platform."""

    monkeypatch.setattr("experiment_queue.attempt_runtime.os.kill", lambda *_args: None)
    monkeypatch.setattr("experiment_queue.attempt_runtime.os.getpgid", lambda _pid: 7)
    assert not process_identity_matches(
        pid=7,
        pgid=7,
        process_start_ticks=None,
    )


def test_graceful_signals_target_executor_but_kill_targets_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The executor forwards graceful signals; only SIGKILL fans out directly."""

    sent: list[tuple[str, int, int]] = []
    monkeypatch.setattr(
        "experiment_queue.attempt_runtime.process_identity_matches",
        lambda **_kwargs: True,
    )
    monkeypatch.setattr(
        "experiment_queue.attempt_runtime.os.kill",
        lambda pid, signum: sent.append(("pid", pid, signum)),
    )
    monkeypatch.setattr(
        "experiment_queue.attempt_runtime.os.killpg",
        lambda pgid, signum: sent.append(("group", pgid, signum)),
    )

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        assert signal_recorded_process(
            pid=17,
            pgid=19,
            process_start_ticks="23",
            signum=signum,
        )

    assert sent == [
        ("pid", 17, signal.SIGINT),
        ("pid", 17, signal.SIGTERM),
        ("group", 19, signal.SIGKILL),
    ]


def test_changed_payload_and_cross_item_worktree_identity_fail_closed(
    tmp_path: Path,
) -> None:
    state, revision, snapshot, manager, evidence, plan = _fixture(tmp_path)
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=1,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )
    prepared.paths.payload.write_text("{}\n", encoding="utf-8")
    with pytest.raises(AttemptRuntimeError, match="changed after preparation"):
        launch_prepared_attempt(prepared)

    with pytest.raises(AttemptRuntimeError, match="worktree.item_id"):
        prepare_structured_attempt(
            state_directory=state,
            queue_item_id=43,
            experiment_id="RUNTIME-001",
            attempt=1,
            segment=1,
            revision=revision,
            snapshot=snapshot,
            execution_plan=plan,
            worktree_evidence=evidence,
            gpu_uuid="GPU-fixture",
            gpu_index="0",
        )
    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_continued_structured_segment_receives_exact_prior_receipt(
    tmp_path: Path,
) -> None:
    """A resumed project can consume the immutable receipt that authorized it."""

    state, revision, snapshot, manager, evidence, plan = _fixture(tmp_path)
    source = b'{"apiVersion":"experiment-queue/v1","kind":"CooperativeYieldReceipt"}\n'
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=2,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
        prior_yield_receipt_source=source,
    )
    assert prepared.paths.continuation_receipt.read_bytes() == source
    assert prepared.environment["EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH"] == str(
        prepared.paths.continuation_receipt
    )
    with pytest.raises(AttemptRuntimeError, match="first structured segment"):
        prepare_structured_attempt(
            state_directory=state,
            queue_item_id=42,
            experiment_id="RUNTIME-001",
            attempt=1,
            segment=1,
            revision=revision,
            snapshot=snapshot,
            execution_plan=plan,
            worktree_evidence=evidence,
            gpu_uuid="GPU-fixture",
            gpu_index="0",
            prior_yield_receipt_source=source,
        )
    manager.cleanup(revision=revision, recorded_evidence=evidence)


def test_explicit_legacy_launch_preserves_shell_text_and_compatibility_environment(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    checkout = tmp_path / "legacy-checkout"
    state.mkdir()
    checkout.mkdir()
    marker = checkout / "legacy result.txt"
    command = f"printf '%s' \"$EXPERIMENT_QUEUE_PROJECT_KEY\" > '{marker}'"
    prepared = prepare_legacy_attempt(
        state_directory=state,
        queue_item_id=9,
        project_id=3,
        project_key="legacy-fixture",
        project_revision_id=30,
        project_revision="legacy-fixture:r1",
        experiment_id="LEGACY-001",
        attempt=2,
        segment=1,
        git_commit="b" * 40,
        execution_root=checkout,
        primary_checkout=checkout,
        command_text=command,
        ambient_environment={
            "PATH": "/usr/bin:/bin",
            "LEGACY_REQUIRED_SECRET": "retained-only-for-grandfathered-path",
            "CUDA_VISIBLE_DEVICES": "wrong",
        },
        gpu_uuid="GPU-legacy",
        gpu_index="1",
        preemptible=True,
    )
    payload = json.loads(prepared.paths.payload.read_text())
    assert payload["admission_kind"] == "LegacyMarkdownCard/v0"
    assert payload["command_kind"] == "legacy-shell"
    assert payload["command"] == command
    assert prepared.environment["LEGACY_REQUIRED_SECRET"].startswith("retained")
    assert prepared.environment["CUDA_VISIBLE_DEVICES"] == "GPU-legacy"
    assert prepared.environment["EXPERIMENT_QUEUE_YIELD_REQUEST_PATH"] == str(
        prepared.paths.yield_request
    )

    launched = launch_prepared_attempt(prepared)
    assert launched.process.wait(timeout=10) == 0
    assert marker.read_text() == "legacy-fixture"
    receipt = prepared.read_exit_receipt()
    assert receipt.admission_kind == "LegacyMarkdownCard/v0"
    assert receipt.resolved_spec_sha256 is None


def test_executor_import_is_isolated_while_scientific_environment_is_preserved(
    tmp_path: Path,
) -> None:
    """Project import controls cannot replace the trusted durable executor."""

    state = tmp_path / "state"
    checkout = tmp_path / "legacy-checkout"
    state.mkdir()
    checkout.mkdir()
    malicious_marker = checkout / "malicious-executor-ran"
    environment_marker = checkout / "scientific-environment.txt"
    package = checkout / "experiment_queue"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "executor.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(malicious_marker)!r}).write_text('replaced')\n",
        encoding="utf-8",
    )
    command = (
        "printf '%s\\n' \"$PYTHONPATH\" \"$PYTHONHOME\" "
        f"\"$SCIENTIFIC_VALUE\" > {str(environment_marker)!r}"
    )
    prepared = prepare_legacy_attempt(
        state_directory=state,
        queue_item_id=9,
        project_id=3,
        project_key="legacy-fixture",
        project_revision_id=30,
        project_revision="legacy-fixture:r1",
        experiment_id="LEGACY-ISOLATION-001",
        attempt=1,
        segment=1,
        git_commit="b" * 40,
        execution_root=checkout,
        primary_checkout=checkout,
        command_text=command,
        ambient_environment={
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(checkout),
            "PYTHONHOME": sys.base_prefix,
            "SCIENTIFIC_VALUE": "preserved-for-child",
        },
        gpu_uuid="GPU-legacy",
        gpu_index="1",
    )

    launched = launch_prepared_attempt(prepared)
    assert launched.process.wait(timeout=10) == 0
    receipt = prepared.read_exit_receipt()

    assert receipt.return_code == 0
    assert not malicious_marker.exists()
    assert environment_marker.read_text().splitlines() == [
        str(checkout),
        sys.base_prefix,
        "preserved-for-child",
    ]


def test_launch_receipt_failure_retains_uncertainty_when_group_kill_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected launch receipt cannot fall back to killing only its leader."""

    state, revision, snapshot, _manager, evidence, plan = _fixture(tmp_path)
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=1,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )

    class LiveFailedLaunch:
        pid = 4242
        returncode: int | None = None

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            prepared.paths.launch_receipt.write_text("{}\n", encoding="utf-8")

        def poll(self) -> int | None:
            return self.returncode

        def wait(self, *, timeout: float | None = None) -> int:
            del timeout
            self.returncode = -signal.SIGKILL
            return self.returncode

    def reject_receipt(*_args: object, **_kwargs: object) -> object:
        raise ExecutorError("fixture rejected launch receipt")

    monkeypatch.setattr(attempt_runtime_module.subprocess, "Popen", LiveFailedLaunch)
    monkeypatch.setattr(
        attempt_runtime_module,
        "_linux_process_start_ticks",
        lambda _pid: "fixture-token",
    )
    monkeypatch.setattr(type(prepared), "read_launch_receipt", reject_receipt)
    monkeypatch.setattr(
        attempt_runtime_module,
        "signal_recorded_process",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        attempt_runtime_module,
        "_named_process_group_exists",
        lambda _pgid: True,
    )

    with pytest.raises(
        AttemptLaunchUncertainError,
        match="authenticated SIGKILL was not delivered",
    ) as raised:
        launch_prepared_attempt(prepared)

    assert (raised.value.pid, raised.value.pgid) == (4242, 4242)
    assert raised.value.process_start_ticks == "fixture-token"


def test_missing_linux_start_token_tears_down_complete_new_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing launch identity never falls back to killing only the leader."""

    state, revision, snapshot, _manager, evidence, plan = _fixture(tmp_path)
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=1,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )

    class LiveUnidentifiedLaunch:
        pid = 4343
        returncode: int | None = None

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def poll(self) -> int | None:
            return self.returncode

        def wait(self, *, timeout: float | None = None) -> int:
            del timeout
            self.returncode = -signal.SIGKILL
            return self.returncode

        def kill(self) -> None:
            pytest.fail("launch cleanup killed only the executor leader")

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(
        attempt_runtime_module.subprocess,
        "Popen",
        LiveUnidentifiedLaunch,
    )
    monkeypatch.setattr(
        attempt_runtime_module,
        "_linux_process_identity_required",
        lambda: True,
    )
    monkeypatch.setattr(
        attempt_runtime_module,
        "_linux_process_start_ticks",
        lambda _pid: None,
    )
    monkeypatch.setattr(attempt_runtime_module.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(
        attempt_runtime_module.os,
        "killpg",
        lambda pgid, signum: sent.append((pgid, signum)),
    )
    monkeypatch.setattr(
        attempt_runtime_module,
        "_named_process_group_exists",
        lambda _pgid: True,
    )

    with pytest.raises(
        AttemptLaunchUncertainError,
        match="still exists after SIGKILL",
    ) as raised:
        launch_prepared_attempt(prepared)

    assert sent == [(4343, signal.SIGKILL)]
    assert (raised.value.pid, raised.value.pgid) == (4343, 4343)
    assert raised.value.process_start_ticks is None


def test_staging_only_launch_evidence_blocks_executor_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery preserves an unpromoted launch staging file and launches nothing."""

    state, revision, snapshot, _manager, evidence, plan = _fixture(tmp_path)
    prepared = prepare_structured_attempt(
        state_directory=state,
        queue_item_id=42,
        experiment_id="RUNTIME-001",
        attempt=1,
        segment=1,
        revision=revision,
        snapshot=snapshot,
        execution_plan=plan,
        worktree_evidence=evidence,
        gpu_uuid="GPU-fixture",
        gpu_index="0",
    )
    staging = prepared.paths.launch_receipt.with_name(
        f".{prepared.paths.launch_receipt.name}.fixture.tmp"
    )
    staging.write_text('{"complete":"unpublished"}\n', encoding="utf-8")

    def must_not_spawn(*_args: object, **_kwargs: object) -> object:
        pytest.fail("executor spawned despite staging-only launch evidence")

    monkeypatch.setattr(attempt_runtime_module.subprocess, "Popen", must_not_spawn)
    with pytest.raises(
        AttemptLaunchUncertainError,
        match="requires operator inspection",
    ) as raised:
        launch_prepared_attempt(prepared)

    assert raised.value.pid is None
    assert staging.is_file()
    assert not prepared.paths.launch_receipt.exists()
