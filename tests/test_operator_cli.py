"""Exercise the mountable standalone Project/card/dry-run CLI surface."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess

from experiment_queue.authoring import Project
from experiment_queue.operator_cli import (
    add_operator_subcommands,
    build_arg_parser,
    main,
)
from experiment_queue.operator_services import project_manifest_scaffold
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
)


def git(repository: Path, *arguments: str) -> str:
    """Run one fixture Git command and return stripped output."""

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


@dataclass(slots=True)
class CLIFixture:
    """File arguments needed by project doctor and submission dry-run."""

    repository: Path
    manifest: Path
    enrollment: Path
    state: Path
    commit: str


def make_cli_fixture(tmp_path: Path) -> CLIFixture:
    """Create one committed scaffold project, card, and host enrollment file."""

    repository = tmp_path / "repository"
    state = tmp_path / "state"
    executable_root = tmp_path / "bin"
    for directory in (
        repository,
        repository / "experiments",
        state,
        executable_root,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    git(repository, "init", "--quiet")
    git(repository, "config", "user.name", "CLI Tests")
    git(repository, "config", "user.email", "cli@example.invalid")

    manifest = repository / "Project.yaml"
    manifest.write_bytes(
        project_manifest_scaffold(
            key="cli-project",
            display_name="CLI project",
        )
    )
    card = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "cli-project",
            "experimentId": "CLI-001",
            "title": "CLI dry run",
        },
        "spec": {
            "parameters": {"count": 1},
            "jobs": [
                {
                    "id": "run",
                    "environment": "python",
                    "command": {"type": "argv", "argv": ["python", "run.py"]},
                    "resources": {"gpus": 1},
                }
            ],
        },
    }
    (repository / "experiments" / "CLI-001.yaml").write_text(
        json.dumps(card, indent=2) + "\n",
        encoding="utf-8",
    )
    (repository / "run.py").write_text("print('cli')\n", encoding="utf-8")
    git(repository, "add", "Project.yaml", "experiments/CLI-001.yaml", "run.py")
    git(repository, "commit", "--quiet", "-m", "CLI fixture")
    commit = git(repository, "rev-parse", "HEAD")

    project = Project.from_yaml(manifest.read_bytes(), source_name="Project.yaml")
    enrollment_model = Enrollment.create(
        project=project,
        checkout_directory=repository,
        project_manifest_path="Project.yaml",
        mounts=[],
        environments=[
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=[executable_root],
            )
        ],
        state_directory=state,
    )
    enrollment = tmp_path / "Enrollment.json"
    enrollment.write_text(
        json.dumps(enrollment_model.to_document(), indent=2) + "\n",
        encoding="utf-8",
    )
    return CLIFixture(
        repository=repository.resolve(),
        manifest=manifest.resolve(),
        enrollment=enrollment.resolve(),
        state=state.resolve(),
        commit=commit,
    )


def test_all_operator_options_have_actionable_help() -> None:
    """Every mounted option documents requirements, defaults, and side effects."""

    parser = build_arg_parser()
    visited: set[int] = set()

    def inspect_parser(current: argparse.ArgumentParser) -> None:
        assert id(current) not in visited
        visited.add(id(current))
        assert current.description
        for action in current._actions:
            if action.option_strings and action.dest != "help":
                assert type(action.help) is str and len(action.help.strip()) >= 12
            if isinstance(action, argparse._SubParsersAction):
                for child in action.choices.values():
                    inspect_parser(child)

    inspect_parser(parser)
    assert len(visited) == 11


def test_operator_commands_mount_under_an_existing_parser() -> None:
    """A future v5 CLI can reuse command construction without duplicating flags."""

    host = argparse.ArgumentParser(prog="host")
    subparsers = host.add_subparsers(dest="command", required=True)
    add_operator_subcommands(subparsers)
    parsed = host.parse_args(
        [
            "project",
            "validate",
            "--manifest",
            "Project.yaml",
        ]
    )
    assert parsed.command == "project"
    assert parsed.project_action == "validate"
    assert parsed.manifest == Path("Project.yaml")


def test_project_init_refuses_overwrite_and_force_replaces_exact_file(
    tmp_path: Path,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    """Scaffolding is validated, JSON-reported, and overwrite-safe by default."""

    output = tmp_path / "Project.yaml"
    assert main(
        [
            "project",
            "init",
            "--key",
            "cli-project",
            "--display-name",
            "CLI project",
            "--output",
            str(output),
        ]
    ) == 0
    created = json.loads(capsys.readouterr().out)
    original = output.read_bytes()
    assert created["operation"] == "project.init"
    assert Project.from_yaml(original).key == "cli-project"

    assert main(
        [
            "project",
            "init",
            "--key",
            "other-project",
            "--display-name",
            "Other",
            "--output",
            str(output),
        ]
    ) == 2
    refused = json.loads(capsys.readouterr().err)
    assert "already exists" in refused["message"]
    assert output.read_bytes() == original

    assert main(
        [
            "project",
            "init",
            "--key",
            "other-project",
            "--display-name",
            "Other",
            "--output",
            str(output),
            "--force",
        ]
    ) == 0
    capsys.readouterr()
    assert Project.from_yaml(output.read_bytes()).key == "other-project"


def test_validate_and_explain_commands_emit_json_and_errors_to_stderr(
    tmp_path: Path,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    """Project/card surfaces produce one machine-readable success or error."""

    fixture = make_cli_fixture(tmp_path)
    assert main(
        ["project", "explain", "--manifest", str(fixture.manifest)]
    ) == 0
    project_report = json.loads(capsys.readouterr().out)
    assert project_report["valid"] is True
    assert project_report["explanation"]["cardRoots"] == ["experiments"]

    card_path = fixture.repository / "experiments" / "CLI-001.yaml"
    assert main(
        [
            "card",
            "explain",
            "--project-manifest",
            str(fixture.manifest),
            "--card",
            str(card_path),
        ]
    ) == 0
    card_report = json.loads(capsys.readouterr().out)
    assert card_report["jobs"] == ["run"]
    assert card_report["explanation"]["jobs"][0]["resources"] == {"gpus": 1}

    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("kind: Project\n", encoding="utf-8")
    assert main(["project", "validate", "--manifest", str(invalid)]) == 2
    streams = capsys.readouterr()
    assert streams.out == ""
    error = json.loads(streams.err)
    assert error["operation"] == "operator.error"
    assert error["ok"] is False
    assert "invalid" in error["message"]


def _revision_arguments(fixture: CLIFixture) -> list[str]:
    return [
        "--manifest",
        str(fixture.manifest),
        "--enrollment",
        str(fixture.enrollment),
        "--state-dir",
        str(fixture.state),
        "--project-id",
        "4",
        "--revision-id",
        "9",
        "--revision-sequence",
        "2",
        "--git-commit",
        fixture.commit,
    ]


def test_project_doctor_and_submission_dry_run_cli_are_read_only(
    tmp_path: Path,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    """Standalone CLI wires resolver evidence without refs, worktrees, or state."""

    fixture = make_cli_fixture(tmp_path)
    refs_before = git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    )
    state_before = tuple(fixture.state.iterdir())

    assert main(["project", "doctor", *_revision_arguments(fixture)]) == 0
    doctor = json.loads(capsys.readouterr().out)
    assert doctor["wouldMutateState"] is False
    assert doctor["git"]["projectBlob"]["path"] == "Project.yaml"

    assert main(
        [
            "submission",
            "dry-run",
            *_revision_arguments(fixture),
            "--card-path",
            "experiments/CLI-001.yaml",
            "--job-id",
            "run",
            "--operator",
            "test:operator",
            "--bindings-json",
            '{"count": 3}',
            "--priority",
            "7",
        ]
    ) == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["wouldMutateState"] is False
    assert dry_run["identity"]["projectRevision"] == "cli-project:r2"
    assert dry_run["git"]["cardBlob"]["path"] == "experiments/CLI-001.yaml"
    assert dry_run["resolvedExecution"]["parameters"] == {"count": 3}
    assert dry_run["resources"] == {"gpus": 1}
    assert tuple(fixture.state.iterdir()) == state_before
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
    ) == refs_before


def test_dry_run_rejects_duplicate_binding_json_without_git_mutation(
    tmp_path: Path,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    """Inline policy JSON is strict before any queue-state operation exists."""

    fixture = make_cli_fixture(tmp_path)
    assert main(
        [
            "submission",
            "dry-run",
            *_revision_arguments(fixture),
            "--card-path",
            "experiments/CLI-001.yaml",
            "--job-id",
            "run",
            "--operator",
            "test:operator",
            "--bindings-json",
            '{"count": 2, "count": 3}',
        ]
    ) == 2
    error = json.loads(capsys.readouterr().err)
    assert "repeats object key" in error["message"]
