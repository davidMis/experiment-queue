"""Keep all onboarding examples valid and executable without a GPU."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from experiment_queue.authoring import ExperimentCard, Project, validate_card_for_project
from experiment_queue.cooperative_yield import (
    ContinuationIdentity,
    CooperativeYieldRequest,
    YieldRequestKind,
    read_yield_receipt,
    validate_receipt_for_request,
    write_yield_request,
)
from experiment_queue.extensions import validate_namespaced_extensions
from experiment_queue.operator_services import doctor_project_revision, load_enrollment_document
from experiment_queue.project_lifecycle import ProjectRevision


EXAMPLES = Path(__file__).parents[1] / "examples"


@pytest.mark.parametrize(
    ("directory", "card_name"),
    [
        ("ordinary", "ORD-001.yaml"),
        ("python-training", "TRAIN-001.yaml"),
        ("data-pipeline", "PIPE-001.yaml"),
        ("cooperative-preemption", "PREEMPT-001.yaml"),
    ],
)
def test_example_project_and_card_validate(directory: str, card_name: str) -> None:
    """Checked-in examples need no manual schema repair."""

    root = EXAMPLES / directory
    project = Project.from_yaml(
        (root / "Project.yaml").read_bytes(), source_name=f"{directory}/Project.yaml"
    )
    card = ExperimentCard.from_yaml(
        (root / "cards" / card_name).read_bytes(),
        source_name=f"{directory}/cards/{card_name}",
    )
    validate_card_for_project(project, card)


def test_ordinary_training_and_pipeline_scripts_use_only_injected_paths(
    tmp_path: Path,
) -> None:
    """Non-preemptible examples run with isolated logical mount/output paths."""

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "sample.txt").write_text("sample\n", encoding="utf-8")
    cases = (
        (
            EXAMPLES / "ordinary" / "run.py",
            {"EXPERIMENT_QUEUE_ARTIFACT_RESULT": str(tmp_path / "ordinary.json")},
        ),
        (
            EXAMPLES / "python-training" / "train.py",
            {
                "EXPERIMENT_QUEUE_MOUNT_DATASET": str(inputs),
                "EXPERIMENT_QUEUE_ARTIFACT_MODEL": str(tmp_path / "model.json"),
            },
        ),
        (
            EXAMPLES / "data-pipeline" / "pipeline.py",
            {
                "EXPERIMENT_QUEUE_MOUNT_INPUTS": str(inputs),
                "EXPERIMENT_QUEUE_ARTIFACT_TRANSFORMED": str(tmp_path / "transformed"),
            },
        ),
    )
    for script, values in cases:
        environment = {"PATH": os.environ.get("PATH", ""), "EXPERIMENT_QUEUE_PROJECT_KEY": "example"}
        environment.update(values)
        completed = subprocess.run(
            [sys.executable, str(script)],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr


def test_cooperative_example_emits_a_typed_ready_receipt(tmp_path: Path) -> None:
    """The project-owned example interoperates with the public helper contract."""

    request_path = tmp_path / "request.json"
    receipt_path = tmp_path / "receipt.json"
    checkpoint = tmp_path / "checkpoint.json"
    result = tmp_path / "result.json"
    request = CooperativeYieldRequest(
        request_id="example-request",
        queue_item_id=1,
        segment=1,
        request_kind=YieldRequestKind.MANUAL_PREEMPTION,
        requested_at="2026-08-28T12:00:00+00:00",
        requested_by="test:operator",
        note="example checkpoint",
        continuation=ContinuationIdentity.create(
            resolved_spec_sha256="1" * 64,
            project_revision="cooperative-example:r1",
            git_commit="2" * 40,
            run_id="example-run",
            prior_receipt_sha256="3" * 64,
        ),
    )
    write_yield_request(request_path, request)
    environment = dict(os.environ)
    environment.update(
        {
            "EXPERIMENT_QUEUE_ARTIFACT_CHECKPOINT": str(checkpoint),
            "EXPERIMENT_QUEUE_ARTIFACT_RESULT": str(result),
            "EXPERIMENT_QUEUE_YIELD_REQUEST_PATH": str(request_path),
            "EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH": str(receipt_path),
        }
    )
    completed = subprocess.run(
        [sys.executable, str(EXAMPLES / "cooperative-preemption" / "worker.py")],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    receipt = read_yield_receipt(receipt_path)
    validate_receipt_for_request(receipt, request)
    assert checkpoint.is_file()


@pytest.mark.parametrize(
    "card_name",
    (
        "FLOWERS-SIMPLE.yaml",
        "FLOWERS-WANDB-PREEMPTIBLE.yaml",
        "FLOWERS-SPECFEM-WORKER.yaml",
    ),
)
def test_flowers_compatibility_cards_match_local_extension_schema(
    card_name: str,
) -> None:
    """Representative Flowers shapes validate without consulting live state."""

    root = EXAMPLES / "flowers-compatibility"
    project = Project.from_yaml((root / "Project.yaml").read_bytes())
    card = ExperimentCard.from_yaml((root / "cards" / card_name).read_bytes())
    validate_card_for_project(project, card)
    validate_namespaced_extensions(
        project,
        card,
        schema_source=(root / "schemas" / "flowers-extension.schema.json").read_bytes(),
    )


def test_flowers_enrollment_generator_and_doctor_are_local_only(tmp_path: Path) -> None:
    """The compatibility fixture produces exact Git/path evidence in temp roots."""

    source_root = EXAMPLES / "flowers-compatibility"
    checkout = tmp_path / "flowers-checkout"
    shutil.copytree(source_root, checkout)
    subprocess.run(["git", "-C", str(checkout), "init", "--quiet"], check=True)
    subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(checkout),
            "-c",
            "user.name=Example Tests",
            "-c",
            "user.email=examples@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "flowers fixture",
        ],
        check=True,
    )
    commit = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    roots = {
        name: tmp_path / name
        for name in ("state", "datasets", "outputs", "scratch", "environment-bin")
    }
    for root in roots.values():
        root.mkdir()
    enrollment_path = tmp_path / "Enrollment.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(checkout / "make_enrollment.py"),
            "--checkout",
            str(checkout),
            "--state-dir",
            str(roots["state"]),
            "--datasets",
            str(roots["datasets"]),
            "--outputs",
            str(roots["outputs"]),
            "--scratch",
            str(roots["scratch"]),
            "--environment-bin",
            str(roots["environment-bin"]),
            "--output",
            str(enrollment_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    project_source = (checkout / "Project.yaml").read_bytes()
    project = Project.from_yaml(project_source, source_name="Project.yaml")
    enrollment = load_enrollment_document(
        source=enrollment_path.read_bytes(),
        source_name=str(enrollment_path),
        project=project,
        state_directory=roots["state"],
    )
    revision = ProjectRevision.create(
        revision_id=1,
        project_id=1,
        sequence=1,
        project=project,
        project_source_path="Project.yaml",
        project_source=project_source,
        extension_schema_source=(
            checkout / "schemas" / "flowers-extension.schema.json"
        ).read_bytes(),
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at="2026-08-28T12:00:00Z",
    )
    report = doctor_project_revision(revision=revision)
    assert report["valid"] is True
    assert report["revision"]["projectKey"] == "flowers-3d-helmholtz"  # type: ignore[index]
