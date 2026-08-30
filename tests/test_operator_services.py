"""Verify strict, non-mutating Project/Enrollment/card operator services."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess

import pytest

from experiment_queue.admission import Submission
from experiment_queue.authoring import (
    AuthoringValidationError,
    ExperimentCard,
    Project,
    validate_card_for_project,
)
from experiment_queue.execution import ExecutionValidationError
from experiment_queue.operator_services import (
    OperatorServiceError,
    doctor_project_revision,
    experiment_card_scaffold,
    export_editor_schema,
    load_enrollment_document,
    project_manifest_scaffold,
    submission_dry_run,
    validate_card_source,
    validate_project_source,
)
from experiment_queue.schema_registry import load_bundled_schema
from experiment_queue.protocols import EXPERIMENT_CARD_V1, PROJECT_V1
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    LifecycleValidationError,
    MountBinding,
    ProjectRevision,
)
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes


NOW = "2026-08-28T20:00:00Z"


def git(repository: Path, *arguments: str) -> str:
    """Run one fixture Git command and return stripped stdout."""

    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Git fixture command {arguments!r} failed: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def project_document() -> dict[str, object]:
    """Return a Project with one writable root and cooperative protocols."""

    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "operator-project",
            "displayName": "Operator project",
        },
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {
                    "name": "scratch",
                    "access": "readWrite",
                    "required": True,
                }
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": ["LANG"],
            },
            "supportedProtocols": [
                {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldRequest",
                },
                {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldReceipt",
                },
            ],
        },
    }


def card_document(*, artifact_path: str = "runs/result.json") -> dict[str, object]:
    """Return one explicit preemptible job for operator dry-run fixtures."""

    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "operator-project",
            "experimentId": "OP-001",
            "title": "Operator dry run",
        },
        "spec": {
            "parameters": {"epochs": 5},
            "jobs": [
                {
                    "id": "run",
                    "environment": "python",
                    "workingDirectory": "work",
                    "command": {
                        "type": "argv",
                        "argv": ["python", "run.py"],
                    },
                    "resources": {
                        "gpus": 1,
                        "cpus": 4,
                        "memoryBytes": 1024,
                        "wallTimeSeconds": 60,
                    },
                    "artifacts": [
                        {
                            "name": "result",
                            "root": "scratch",
                            "path": artifact_path,
                            "type": "file",
                            "required": True,
                        }
                    ],
                    "capabilities": {
                        "cooperativeYield": {
                            "requestProtocol": {
                                "apiVersion": "experiment-queue/v1",
                                "kind": "CooperativeYieldRequest",
                            },
                            "receiptProtocol": {
                                "apiVersion": "experiment-queue/v1",
                                "kind": "CooperativeYieldReceipt",
                            },
                            "checkpointArtifacts": ["result"],
                        }
                    },
                }
            ],
        },
    }


def source(document: dict[str, object]) -> bytes:
    """Encode fixture authoring bytes deterministically."""

    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def test_card_scaffold_and_editor_schema_exports_validate_without_repair() -> None:
    """Generated onboarding artifacts are immediately strict-parser valid."""

    project = Project.from_document(project_document())
    card_source = experiment_card_scaffold(
        project=project,
        experiment_id="ONBOARD-001",
        title="First queued run",
    )
    card = ExperimentCard.from_yaml(card_source, source_name="generated-card.json")
    validate_card_for_project(project, card)
    assert card.project_key == project.key
    assert card.job("run").environment == "python"
    assert card.job("run").artifacts[0].root == "scratch"

    assert json.loads(export_editor_schema("project")) == load_bundled_schema(
        PROJECT_V1
    )
    assert json.loads(export_editor_schema("card")) == load_bundled_schema(
        EXPERIMENT_CARD_V1
    )


def test_card_scaffold_rejects_undeclared_environment_or_artifact_root() -> None:
    """Scaffolding never invents Project-owned names that validation would reject."""

    project = Project.from_document(project_document())
    with pytest.raises(OperatorServiceError, match="not declared"):
        experiment_card_scaffold(
            project=project,
            experiment_id="ONBOARD-002",
            title="Bad environment",
            environment="missing",
        )
    with pytest.raises(OperatorServiceError, match="not a writable"):
        experiment_card_scaffold(
            project=project,
            experiment_id="ONBOARD-003",
            title="Bad root",
            artifact_root="missing",
        )


@dataclass(slots=True)
class OperatorFixture:
    """Complete temporary repository and immutable revision."""

    repository: Path
    state: Path
    scratch: Path
    executable_root: Path
    project_source: bytes
    card_source: bytes
    enrollment_source: bytes
    revision: ProjectRevision


def make_fixture(
    tmp_path: Path,
    *,
    artifact_path: str = "runs/result.json",
) -> OperatorFixture:
    """Create one committed Project/card and matching host Enrollment."""

    repository = tmp_path / "repository"
    state = tmp_path / "state"
    scratch = tmp_path / "scratch"
    executable_root = tmp_path / "bin"
    for directory in (
        repository,
        state,
        scratch,
        executable_root,
        repository / "cards",
        repository / "work",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    git(repository, "init", "--quiet")
    git(repository, "config", "user.name", "Operator Tests")
    git(repository, "config", "user.email", "operator@example.invalid")

    project_source = source(project_document())
    card_source = source(card_document(artifact_path=artifact_path))
    (repository / "Project.yaml").write_bytes(project_source)
    (repository / "cards" / "OP-001.yaml").write_bytes(card_source)
    (repository / "run.py").write_text("print('run')\n", encoding="utf-8")
    git(repository, "add", "Project.yaml", "cards/OP-001.yaml", "run.py")
    git(repository, "commit", "--quiet", "-m", "operator fixture")
    commit = git(repository, "rev-parse", "HEAD")

    project = Project.from_yaml(project_source, source_name="Project.yaml")
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=repository,
        project_manifest_path="Project.yaml",
        mounts=[
            MountBinding.create(
                name="scratch",
                path=scratch,
                access="readWrite",
            )
        ],
        environments=[
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=[executable_root],
                inherit_variables=["LANG"],
            )
        ],
        state_directory=state,
    )
    revision = ProjectRevision.create(
        revision_id=13,
        project_id=7,
        sequence=3,
        project=project,
        project_source_path="Project.yaml",
        project_source=project_source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    return OperatorFixture(
        repository=repository.resolve(),
        state=state.resolve(),
        scratch=scratch.resolve(),
        executable_root=executable_root.resolve(),
        project_source=project_source,
        card_source=card_source,
        enrollment_source=(
            json.dumps(enrollment.to_document(), indent=2) + "\n"
        ).encode(),
        revision=revision,
    )


def test_project_scaffold_and_validation_explain_are_deterministic() -> None:
    """The scaffold validates itself and exposes exact schema/digest evidence."""

    first = project_manifest_scaffold(
        key="example-project",
        display_name='Example: "quoted" project',
    )
    second = project_manifest_scaffold(
        key="example-project",
        display_name='Example: "quoted" project',
    )
    assert first == second
    project = Project.from_yaml(first, source_name="Project.yaml")
    assert project.key == "example-project"
    assert project.volumes == ()
    report = validate_project_source(
        source=first,
        source_name="Project.yaml",
        explain=True,
    )
    assert report["valid"] is True
    assert report["projectKey"] == "example-project"
    assert report["source"]["sha256"] == sha256_bytes(first)  # type: ignore[index]
    assert report["explanation"]["cardRoots"] == ["experiments"]  # type: ignore[index]
    assert report["schema"]["kind"] == "Project"  # type: ignore[index]

    card = ExperimentCard.from_yaml(
        experiment_card_scaffold(
            project=project,
            experiment_id="SIMPLE-001",
            title="Simple trusted job",
        ),
        source_name="experiments/SIMPLE-001.yaml",
    )
    assert card.jobs[0].artifacts == ()


def test_card_validate_and_explain_cover_cross_project_job_contract(
    tmp_path: Path,
) -> None:
    """Card validation reports every environment/resource/artifact/capability."""

    fixture = make_fixture(tmp_path)
    report = validate_card_source(
        project_source=fixture.project_source,
        project_source_name="Project.yaml",
        card_source=fixture.card_source,
        card_source_name="cards/OP-001.yaml",
        explain=True,
    )
    assert report["valid"] is True
    assert report["experimentId"] == "OP-001"
    assert report["jobs"] == ["run"]
    explanation = report["explanation"]
    assert type(explanation) is dict
    job = explanation["jobs"][0]
    assert job["environment"] == "python"
    assert job["resources"]["gpus"] == 1
    assert job["artifacts"][0]["root"] == "scratch"
    assert "cooperativeYield" in job["capabilities"]

    wrong_card = card_document()
    wrong_card["metadata"]["projectKey"] = "other-project"  # type: ignore[index]
    with pytest.raises(AuthoringValidationError, match="projectKey|project key"):
        validate_card_source(
            project_source=fixture.project_source,
            project_source_name="Project.yaml",
            card_source=source(wrong_card),
            card_source_name="cards/wrong.yaml",
        )


def test_enrollment_loader_rederives_exact_document_and_rejects_edits(
    tmp_path: Path,
) -> None:
    """Host documents cannot edit derived digests, roots, fields, or secret data."""

    fixture = make_fixture(tmp_path)
    loaded = load_enrollment_document(
        source=fixture.enrollment_source,
        source_name="Enrollment.json",
        project=fixture.revision.project,
        state_directory=fixture.state,
    )
    assert loaded == fixture.revision.enrollment
    assert loaded.canonical_json == fixture.revision.enrollment.canonical_json

    document = loaded.to_document()
    document["projectNormalizedSha256"] = "0" * 64
    with pytest.raises(OperatorServiceError, match="regenerate host bindings"):
        load_enrollment_document(
            source=json.dumps(document).encode(),
            source_name="Enrollment.json",
            project=fixture.revision.project,
            state_directory=fixture.state,
        )

    document = loaded.to_document()
    document["artifactRoots"][0]["path"] = str(tmp_path / "other")  # type: ignore[index]
    with pytest.raises(OperatorServiceError, match="artifactRoots"):
        load_enrollment_document(
            source=json.dumps(document).encode(),
            source_name="Enrollment.json",
            project=fixture.revision.project,
            state_directory=fixture.state,
        )

    document = loaded.to_document()
    document["environments"][0]["secrets"] = {"TOKEN": "literal"}  # type: ignore[index]
    with pytest.raises(LifecycleValidationError, match="secrets are forbidden"):
        load_enrollment_document(
            source=json.dumps(document).encode(),
            source_name="Enrollment.json",
            project=fixture.revision.project,
            state_directory=fixture.state,
        )


def test_enrollment_loader_rejects_duplicate_keys_before_path_validation(
    tmp_path: Path,
) -> None:
    """Strict source parsing never silently chooses one duplicate host path."""

    fixture = make_fixture(tmp_path)
    duplicated = fixture.enrollment_source.replace(
        b'"kind": "Enrollment",',
        b'"kind": "Enrollment",\n  "kind": "Enrollment",',
        1,
    )
    with pytest.raises(OperatorServiceError, match="duplicate|Duplicate"):
        load_enrollment_document(
            source=duplicated,
            source_name="Enrollment.json",
            project=fixture.revision.project,
            state_directory=fixture.state,
        )


def test_enrollment_loader_never_trusts_claimed_git_ignore_paths(
    tmp_path: Path,
) -> None:
    """Document strings cannot manufacture trusted pinned Git-ignore evidence."""

    fixture = make_fixture(tmp_path)
    ignored = fixture.repository / "ignored-data"
    ignored.mkdir()
    document = fixture.revision.enrollment.to_document()
    document["gitIgnoredCheckoutDescendants"] = [str(ignored)]
    with pytest.raises(OperatorServiceError, match="cannot authenticate them"):
        load_enrollment_document(
            source=json.dumps(document).encode(),
            source_name="Enrollment.json",
            project=fixture.revision.project,
            state_directory=fixture.state,
        )


def test_doctor_authenticates_pinned_blobs_and_mutates_no_repo_or_state(
    tmp_path: Path,
) -> None:
    """Doctor is read-only even when the checkout working tree is dirty."""

    fixture = make_fixture(tmp_path)
    (fixture.repository / "Project.yaml").write_text(
        "dirty working tree must not be trusted\n",
        encoding="utf-8",
    )
    status_before = git(fixture.repository, "status", "--porcelain=v1")
    refs_before = git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    )
    worktrees_before = git(fixture.repository, "worktree", "list", "--porcelain")
    state_before = tuple(fixture.state.iterdir())

    report = doctor_project_revision(revision=fixture.revision)
    assert report["valid"] is True
    assert report["wouldMutateState"] is False
    assert report["revision"]["gitCommit"] == fixture.revision.git_commit  # type: ignore[index]
    project_blob = report["git"]["projectBlob"]  # type: ignore[index]
    assert project_blob["path"] == "Project.yaml"
    assert project_blob["sourceSha256"] == sha256_bytes(fixture.project_source)
    assert report["enrollment"]["currentPathsVerified"] is True  # type: ignore[index]

    assert git(fixture.repository, "status", "--porcelain=v1") == status_before
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    ) == refs_before
    assert git(fixture.repository, "worktree", "list", "--porcelain") == worktrees_before
    assert tuple(fixture.state.iterdir()) == state_before


def test_submission_dry_run_resolves_complete_evidence_without_secrets_or_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry-run reports Git/path/env/resource/preemption evidence and no values."""

    fixture = make_fixture(tmp_path)
    monkeypatch.setenv("LANG", "secret-locale-value")
    monkeypatch.setenv("SECRET_TOKEN", "must-not-appear")
    refs_before = git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    )
    state_before = tuple(fixture.state.iterdir())
    report = submission_dry_run(
        revision=fixture.revision,
        submission=Submission(
            project_key="operator-project",
            card_path="cards/OP-001.yaml",
            job_id="run",
            operator="test:operator",
            bindings={"epochs": 12},
            priority=9,
            dependencies=[2, 5],
            preemption_authorized=True,
        ),
    )
    encoded = json.dumps(report, sort_keys=True)
    assert report["valid"] is True
    assert report["wouldMutateState"] is False
    assert report["identity"]["projectRevision"] == "operator-project:r3"  # type: ignore[index]
    assert report["git"]["cardBlob"]["path"] == "cards/OP-001.yaml"  # type: ignore[index]
    assert report["paths"]["artifacts"][0]["resolvedPath"] == str(  # type: ignore[index]
        fixture.scratch / "runs" / "result.json"
    )
    assert report["environment"]["inheritVariableNames"] == ["LANG"]  # type: ignore[index]
    assert report["environment"]["literalValuesIncluded"] is False  # type: ignore[index]
    assert report["resources"] == {
        "gpus": 1,
        "cpus": 4,
        "memoryBytes": 1024,
        "wallTimeSeconds": 60,
    }
    assert report["preemption"]["automatic"] is False  # type: ignore[index]
    assert report["preemption"]["eligibleForManualPreemption"] is True  # type: ignore[index]
    assert report["resolvedExecution"]["parameters"] == {"epochs": 12}  # type: ignore[index]
    assert "secret-locale-value" not in encoded
    assert "must-not-appear" not in encoded
    assert tuple(fixture.state.iterdir()) == state_before
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    ) == refs_before


def test_submission_dry_run_rejects_artifact_symlink_escape(tmp_path: Path) -> None:
    """Resolved host output paths cannot escape a frozen writable root."""

    fixture = make_fixture(tmp_path, artifact_path="escape/result.json")
    outside = tmp_path / "outside"
    outside.mkdir()
    (fixture.scratch / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(
        ExecutionValidationError,
        match="escapes authorized artifact root",
    ):
        submission_dry_run(
            revision=fixture.revision,
            submission=Submission(
                project_key="operator-project",
                card_path="cards/OP-001.yaml",
                job_id="run",
                operator="test:operator",
            ),
        )


def test_reports_are_json_native_and_canonicalizable(tmp_path: Path) -> None:
    """Every public report can be emitted directly as stable machine JSON."""

    fixture = make_fixture(tmp_path)
    reports = [
        doctor_project_revision(revision=fixture.revision),
        submission_dry_run(
            revision=fixture.revision,
            submission=Submission(
                project_key="operator-project",
                card_path="cards/OP-001.yaml",
                job_id="run",
                operator="test:operator",
            ),
        ),
    ]
    for report in reports:
        encoded = canonical_json_bytes(report)
        assert json.loads(encoded) == report
