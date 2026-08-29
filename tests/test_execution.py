"""Verify project-qualified environment, command, and path authorization."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from experiment_queue.admission import AdmissionSnapshot, Submission, compile_admission
from experiment_queue.authoring import Project, VolumeAccess
from experiment_queue.execution import (
    ExecutionValidationError,
    build_execution_plan,
    construct_child_environment,
    resolve_artifact_path,
    resolve_existing_project_path,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
    ProjectRevision,
)


def _project_document() -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "execution-fixture",
            "displayName": "Execution fixture",
        },
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True}
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": ["LANG", "SECRET_TOKEN", "PATH"],
            },
            "supportedProtocols": [],
        },
    }


def _card_document(
    *,
    command: dict[str, object] | None = None,
    working_directory: str | None = "work",
    artifact_path: str = "runs/result.json",
) -> dict[str, object]:
    job: dict[str, object] = {
        "id": "run",
        "environment": "python",
        "command": command or {"type": "argv", "argv": ["python", "run.py"]},
        "resources": {"gpus": 1},
        "artifacts": [
            {
                "name": "result",
                "root": "scratch",
                "path": artifact_path,
                "type": "file",
                "required": True,
            }
        ],
    }
    if working_directory is not None:
        job["workingDirectory"] = working_directory
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "execution-fixture",
            "experimentId": "EXEC-001",
            "title": "Execution fixture",
        },
        "spec": {"parameters": {}, "jobs": [job]},
    }


def _source(document: dict[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _fixture(
    tmp_path: Path,
    *,
    command: dict[str, object] | None = None,
    working_directory: str | None = "work",
    artifact_path: str = "runs/result.json",
    project_revision: str = "execution-fixture:r1",
) -> tuple[ProjectRevision, AdmissionSnapshot, dict[str, Path]]:
    checkout = tmp_path / "checkout"
    state = tmp_path / "state"
    scratch = tmp_path / "scratch"
    executables = tmp_path / "executables"
    for directory in (checkout, state, scratch, executables, checkout / "work"):
        directory.mkdir(parents=True, exist_ok=True)

    project_source = _source(_project_document())
    project = Project.from_yaml(project_source, source_name="project.yaml")
    mount = MountBinding.create(
        name="scratch",
        path=scratch,
        access=VolumeAccess.READ_WRITE,
    )
    environment = EnvironmentBinding.create(
        name="python",
        executable_search_directories=[executables],
        inherit_variables=["LANG"],
    )
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=checkout,
        project_manifest_path="project.yaml",
        mounts=[mount],
        environments=[environment],
        state_directory=state,
    )
    revision = ProjectRevision.create(
        revision_id=11,
        project_id=7,
        sequence=1,
        project=project,
        project_source_path="project.yaml",
        project_source=project_source,
        git_commit="a" * 40,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at="2026-08-28T12:00:00Z",
    )
    card_source = _source(
        _card_document(
            command=command,
            working_directory=working_directory,
            artifact_path=artifact_path,
        )
    )
    snapshot = compile_admission(
        project_source=project_source,
        card_source=card_source,
        submission=Submission(
            project_key="execution-fixture",
            card_path="cards/EXEC-001.yaml",
            job_id="run",
            operator="test:operator",
        ),
        project_revision=project_revision,
        git_commit="a" * 40,
        project_source_name="project.yaml",
    )
    return revision, snapshot, {
        "checkout": checkout,
        "state": state,
        "scratch": scratch,
        "executables": executables,
    }


def test_build_execution_plan_uses_empty_environment_and_revision_paths(
    tmp_path: Path,
) -> None:
    revision, snapshot, paths = _fixture(tmp_path)

    plan = build_execution_plan(
        snapshot=snapshot,
        revision=revision,
        worktree=paths["checkout"],
        ambient_environment={
            "PATH": "/host/bin",
            "LANG": "C.UTF-8",
            "SECRET_TOKEN": "must-not-leak",
            "CUDA_VISIBLE_DEVICES": "9",
            "EXPERIMENT_QUEUE_ITEM_ID": "evil",
            "HOME": "/private/home",
        },
        assigned_gpu="GPU-deadbeef",
        queue_variables={
            "EXPERIMENT_QUEUE_ITEM_ID": "42",
            "EXPERIMENT_QUEUE_PROJECT_KEY": "cannot-override",
            "EXPERIMENT_QUEUE_MOUNT_SCRATCH": "/cannot/override",
            "EXPERIMENT_QUEUE_ARTIFACT_RESULT": "/cannot/override",
        },
    )

    assert plan.project_id == 7
    assert plan.project_revision_id == 11
    assert plan.project_revision == "execution-fixture:r1"
    assert plan.argv == ("python", "run.py")
    assert plan.cwd == paths["checkout"] / "work"
    assert plan.artifacts[0].path == paths["scratch"] / "runs" / "result.json"
    assert plan.artifacts[0].required is True
    assert plan.environment == {
        "PATH": str(paths["executables"]),
        "LANG": "C.UTF-8",
        "CUDA_VISIBLE_DEVICES": "GPU-deadbeef",
        "EXPERIMENT_QUEUE_GIT_COMMIT": "a" * 40,
        "EXPERIMENT_QUEUE_ITEM_ID": "42",
        "EXPERIMENT_QUEUE_MOUNT_SCRATCH": str(paths["scratch"]),
        "EXPERIMENT_QUEUE_ARTIFACT_RESULT": str(
            paths["scratch"] / "runs" / "result.json"
        ),
        "EXPERIMENT_QUEUE_PROJECT_KEY": "execution-fixture",
        "EXPERIMENT_QUEUE_PROJECT_REVISION": "execution-fixture:r1",
    }
    detached = plan.environment
    detached["LANG"] = "mutated"
    assert plan.environment["LANG"] == "C.UTF-8"


def test_environment_binding_can_only_narrow_portable_allowlist(tmp_path: Path) -> None:
    revision, _snapshot, _paths = _fixture(tmp_path)
    binding = revision.enrollment.environment("python")

    environment = construct_child_environment(
        revision=revision,
        binding=binding,
        ambient_environment={"LANG": "", "SECRET_TOKEN": "not-bound"},
        assigned_gpu=None,
    )

    assert environment["LANG"] == ""
    assert "SECRET_TOKEN" not in environment
    assert environment["CUDA_VISIBLE_DEVICES"] == ""
    assert environment["EXPERIMENT_QUEUE_MOUNT_SCRATCH"] == str(
        revision.enrollment.mount("scratch").path
    )


@pytest.mark.parametrize(
    "queue_variables",
    [
        {"PATH": "bad"},
        {"CUDA_VISIBLE_DEVICES": "bad"},
        {"EXPERIMENT_QUEUE_bad": "bad"},
        {"EXPERIMENT_QUEUE_ITEM_ID": "bad\x00value"},
    ],
)
def test_queue_environment_rejects_nonowned_or_invalid_values(
    tmp_path: Path,
    queue_variables: dict[str, str],
) -> None:
    revision, _snapshot, _paths = _fixture(tmp_path)
    with pytest.raises(ExecutionValidationError, match="queue variable|NUL"):
        construct_child_environment(
            revision=revision,
            binding=revision.enrollment.environment("python"),
            ambient_environment={},
            assigned_gpu="0",
            queue_variables=queue_variables,
        )


def test_artifact_symlink_escape_is_rejected(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(
        tmp_path,
        artifact_path="escape/result.json",
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (paths["scratch"] / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ExecutionValidationError, match="escapes authorized artifact"):
        build_execution_plan(
            snapshot=snapshot,
            revision=revision,
            worktree=paths["checkout"],
            ambient_environment={},
            assigned_gpu="0",
        )


def test_retargeted_frozen_root_is_rejected_at_use_time(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(tmp_path)
    old_scratch = tmp_path / "old-scratch"
    paths["scratch"].rename(old_scratch)
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    paths["scratch"].symlink_to(replacement, target_is_directory=True)

    with pytest.raises(ExecutionValidationError, match="changed canonical target"):
        build_execution_plan(
            snapshot=snapshot,
            revision=revision,
            worktree=paths["checkout"],
            ambient_environment={},
            assigned_gpu="0",
        )


def test_working_directory_symlink_escape_is_rejected(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(tmp_path)
    outside = tmp_path / "outside-work"
    outside.mkdir()
    (paths["checkout"] / "work").rmdir()
    (paths["checkout"] / "work").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ExecutionValidationError, match="resolves outside"):
        build_execution_plan(
            snapshot=snapshot,
            revision=revision,
            worktree=paths["checkout"],
            ambient_environment={},
            assigned_gpu="0",
        )


def test_wrapper_runs_directly_and_cannot_escape_worktree(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(
        tmp_path,
        command={
            "type": "wrapper",
            "path": "scripts/run.sh",
            "args": ["--exact"],
        },
        working_directory=None,
    )
    scripts = paths["checkout"] / "scripts"
    scripts.mkdir()
    wrapper = scripts / "run.sh"
    wrapper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    wrapper.chmod(0o755)

    plan = build_execution_plan(
        snapshot=snapshot,
        revision=revision,
        worktree=paths["checkout"],
        ambient_environment={},
        assigned_gpu=None,
    )
    assert plan.argv == (str(wrapper), "--exact")
    assert plan.cwd == paths["checkout"]

    outside = tmp_path / "outside-wrapper"
    outside.write_text("#!/bin/sh\n", encoding="utf-8")
    outside.chmod(0o755)
    wrapper.unlink()
    wrapper.symlink_to(outside)
    with pytest.raises(ExecutionValidationError, match="resolves outside"):
        build_execution_plan(
            snapshot=snapshot,
            revision=revision,
            worktree=paths["checkout"],
            ambient_environment={},
            assigned_gpu=None,
        )


def test_shell_compatibility_and_command_prefix_are_structured(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(
        tmp_path,
        command={
            "type": "shell",
            "script": "exec python legacy.py",
            "compatibilityReason": "Temporary migration bridge.",
        },
    )
    binding = revision.enrollment.environment("python")
    # The normal fixture has no prefix; the shell still becomes explicit argv.
    assert binding.command_prefix_argv is None
    plan = build_execution_plan(
        snapshot=snapshot,
        revision=revision,
        worktree=paths["checkout"],
        ambient_environment={},
        assigned_gpu="0",
    )
    assert plan.argv == ("sh", "-c", "exec python legacy.py")


def test_snapshot_revision_mismatch_fails_before_path_use(tmp_path: Path) -> None:
    revision, snapshot, paths = _fixture(
        tmp_path,
        project_revision="execution-fixture:r2",
    )
    with pytest.raises(ExecutionValidationError, match="does not belong.*r2.*r1"):
        build_execution_plan(
            snapshot=snapshot,
            revision=revision,
            worktree=paths["checkout"],
            ambient_environment={},
            assigned_gpu="0",
        )


def test_low_level_path_resolvers_require_portable_descendants(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "existing").mkdir()
    assert resolve_existing_project_path(
        root,
        "existing",
        field_name="fixture path",
        require_directory=True,
    ) == root / "existing"
    assert resolve_artifact_path(
        root,
        "future/result.json",
        field_name="fixture artifact",
    ) == root / "future" / "result.json"
    for value in ("../escape", "/absolute", "a//b", "a\\b"):
        with pytest.raises(ExecutionValidationError, match="portable|components"):
            resolve_artifact_path(root, value, field_name="fixture artifact")
