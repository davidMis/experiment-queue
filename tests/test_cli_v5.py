"""Exercise the production schema-v5 operator CLI and its typed delegation."""

from __future__ import annotations

from base64 import b64decode
import argparse
import json
from pathlib import Path
from types import SimpleNamespace
import subprocess

import pytest

from experiment_queue.authoring import Project
from experiment_queue.cli_v5 import (
    _automatic_environment_directory,
    build_arg_parser,
    main,
)
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.operator_services import (
    OperatorServiceError,
    experiment_card_scaffold,
    project_manifest_scaffold,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
)
from experiment_queue.queue_export import QueueExport
from experiment_queue.serialization import canonical_json_bytes


PROJECT_PATH = "config/project.yaml"
CARD_PATH = "cards/example.yaml"


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _source(document: dict[str, object]) -> bytes:
    return (json.dumps(document, indent=2) + "\n").encode()


def _project_document(key: str, display_name: str) -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {"key": key, "displayName": display_name},
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True}
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {"inherit": "none", "allowVariables": []},
            "supportedProtocols": [],
        },
    }


def _card_document(key: str) -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": key,
            "experimentId": "CLI-001",
            "title": "CLI fixture",
        },
        "spec": {
            "parameters": {"epochs": 1},
            "jobs": [
                {
                    "id": "train",
                    "environment": "python",
                    "command": {"type": "argv", "argv": ["python", "train.py"]},
                    "artifacts": [
                        {
                            "name": "result",
                            "root": "scratch",
                            "path": "results/final.bin",
                            "type": "file",
                        }
                    ],
                }
            ],
        },
    }


class ProjectFixture:
    """Committed portable sources plus one strict host Enrollment document."""

    def __init__(self, tmp_path: Path) -> None:
        self.state = (tmp_path / "state").resolve()
        self.state.mkdir()
        self.checkout = (tmp_path / "checkout").resolve()
        self.checkout.mkdir()
        self.scratch = (tmp_path / "scratch").resolve()
        self.scratch.mkdir()
        self.environment = (tmp_path / "environment-bin").resolve()
        self.environment.mkdir()
        self.enrollment_path = (tmp_path / "Enrollment.json").resolve()
        _git(self.checkout, "init", "--quiet")
        self.write_revision("CLI Project")

    def write_revision(self, display_name: str) -> str:
        project_source = _source(_project_document("cli-project", display_name))
        card_source = _source(_card_document("cli-project"))
        project_target = self.checkout / PROJECT_PATH
        project_target.parent.mkdir(parents=True, exist_ok=True)
        project_target.write_bytes(project_source)
        card_target = self.checkout / CARD_PATH
        card_target.parent.mkdir(parents=True, exist_ok=True)
        card_target.write_bytes(card_source)
        _git(self.checkout, "add", "--", PROJECT_PATH, CARD_PATH)
        _git(
            self.checkout,
            "-c",
            "user.name=CLI Tests",
            "-c",
            "user.email=cli@example.invalid",
            "commit",
            "--quiet",
            "-m",
            display_name,
        )
        project = Project.from_yaml(project_source, source_name=PROJECT_PATH)
        enrollment = Enrollment.create(
            project=project,
            checkout_directory=self.checkout,
            project_manifest_path=PROJECT_PATH,
            mounts=(
                MountBinding.create(
                    name="scratch", path=self.scratch, access="readWrite"
                ),
            ),
            environments=(
                EnvironmentBinding.create(
                    name="python",
                    executable_search_directories=(self.environment,),
                ),
            ),
            state_directory=self.state,
        )
        self.enrollment_path.write_bytes(enrollment.canonical_json)
        return _git(self.checkout, "rev-parse", "HEAD")

    def register_arguments(self, *, commit: str) -> list[str]:
        return [
            "--state-dir",
            str(self.state),
            "project",
            "register",
            str(self.checkout),
            "--manifest",
            PROJECT_PATH,
            "--enrollment",
            str(self.enrollment_path),
            "--git-commit",
            commit,
            "--actor",
            "cli:test",
            "--reason",
            "CLI registration fixture",
            "--json",
        ]


@pytest.fixture
def registered(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> ProjectFixture:
    fixture = ProjectFixture(tmp_path)
    commit = _git(fixture.checkout, "rev-parse", "HEAD")
    assert main(fixture.register_arguments(commit=commit)) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["project"]["key"] == "cli-project"
    return fixture


def test_register_authenticates_checkout_local_root_at_pinned_commit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The public registration path earns, rather than trusts, ignore proof."""

    fixture = ProjectFixture(tmp_path)
    ignored = fixture.checkout / "mutable" / "artifacts"
    ignored.mkdir(parents=True)
    (fixture.checkout / ".gitignore").write_text("/mutable/\n", encoding="utf-8")
    _git(fixture.checkout, "add", "--", ".gitignore")
    _git(
        fixture.checkout,
        "-c",
        "user.name=CLI Tests",
        "-c",
        "user.email=cli@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "ignore checkout-local artifacts",
    )
    commit = _git(fixture.checkout, "rev-parse", "HEAD")
    project = Project.from_yaml(
        (fixture.checkout / PROJECT_PATH).read_bytes(),
        source_name=PROJECT_PATH,
    )
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=fixture.checkout,
        project_manifest_path=PROJECT_PATH,
        mounts=(
            MountBinding.create(
                name="scratch",
                path=ignored,
                access="readWrite",
            ),
        ),
        environments=(
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=(fixture.environment,),
            ),
        ),
        state_directory=fixture.state,
        git_ignored_checkout_descendants=(ignored,),
    )
    fixture.enrollment_path.write_bytes(enrollment.canonical_json)

    assert main(fixture.register_arguments(commit=commit)) == 0
    output = _json_output(capsys)
    assert output["project"]["currentRevision"]["gitCommit"] == commit  # type: ignore[index]


def test_register_automatically_uses_ignored_checkout_venv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The trusted-project path needs no mount inventory or Enrollment file."""

    state = (tmp_path / "state").resolve()
    state.mkdir()
    checkout = (tmp_path / "checkout").resolve()
    checkout.mkdir()
    _git(checkout, "init", "--quiet")
    (checkout / "Project.yaml").write_bytes(
        project_manifest_scaffold(
            key="simple-project",
            display_name="Simple Project",
        )
    )
    (checkout / ".gitignore").write_text("/.venv/\n", encoding="utf-8")
    environment = checkout / ".venv"
    environment_bin = environment / "bin"
    environment_bin.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text("home = /python\n", encoding="utf-8")
    python = environment_bin / "python3.14"
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    python.chmod(0o755)
    experiments = checkout / "experiments"
    experiments.mkdir()
    project = Project.from_yaml(
        (checkout / "Project.yaml").read_bytes(),
        source_name="Project.yaml",
    )
    (experiments / "SIMPLE-001.yaml").write_bytes(
        experiment_card_scaffold(
            project=project,
            experiment_id="SIMPLE-001",
            title="Simple trusted job",
        )
    )
    _git(
        checkout,
        "add",
        "--",
        "Project.yaml",
        ".gitignore",
        "experiments/SIMPLE-001.yaml",
    )
    _git(
        checkout,
        "-c",
        "user.name=CLI Tests",
        "-c",
        "user.email=cli@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "simple trusted project",
    )
    commit = _git(checkout, "rev-parse", "HEAD")

    assert main(
        [
            "--state-dir",
            str(state),
            "project",
            "register",
            str(checkout),
            "--git-commit",
            commit,
            "--actor",
            "cli:test",
            "--reason",
            "simple registration",
            "--json",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["project"]["key"] == "simple-project"

    with V5QueueStore(state).connect() as connection:
        enrollment = json.loads(
            bytes(
                connection.execute(
                    "SELECT enrollment_json FROM project_revisions WHERE id = 1"
                ).fetchone()[0]
            )
        )
    assert enrollment["mounts"] == []
    assert enrollment["artifactRoots"] == []
    assert enrollment["environments"] == [
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "EnvironmentBinding",
            "name": "python",
            "executableSearchDirectories": [str(environment_bin)],
            "inheritVariables": [],
        }
    ]
    assert enrollment["gitIgnoredCheckoutDescendants"] == [str(environment)]

    assert main(
        [
            "--state-dir",
            str(state),
            "submit",
            "--project",
            "simple-project",
            "--card-path",
            "experiments/SIMPLE-001.yaml",
            "--job-id",
            "run",
            "--operator",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["item"]["id"] == 1

    (checkout / "Project.yaml").write_bytes(
        project_manifest_scaffold(
            key="simple-project",
            display_name="Simple Project Revised",
        )
    )
    _git(checkout, "add", "--", "Project.yaml")
    _git(
        checkout,
        "-c",
        "user.name=CLI Tests",
        "-c",
        "user.email=cli@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "revise simple project",
    )
    second_commit = _git(checkout, "rev-parse", "HEAD")
    assert main(
        [
            "--state-dir",
            str(state),
            "project",
            "append-revision",
            str(checkout),
            "--project",
            "simple-project",
            "--git-commit",
            second_commit,
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    appended = json.loads(capsys.readouterr().out)
    assert appended["project"]["currentRevision"]["sequence"] == 2


def test_automatic_environment_accepts_venv_root_bin_or_python(
    tmp_path: Path,
) -> None:
    """Common venv spellings normalize before a Python symlink is resolved."""

    checkout = (tmp_path / "checkout").resolve()
    environment = checkout / ".venv"
    environment_bin = environment / "bin"
    environment_bin.mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text("home = /python\n", encoding="utf-8")
    external_python = tmp_path / "uv-python3.14"
    external_python.write_text("#!/bin/sh\n", encoding="utf-8")
    external_python.chmod(0o755)
    python = environment_bin / "python3.14"
    python.symlink_to(external_python)

    for requested in (
        environment,
        environment_bin,
        python,
        Path(".venv"),
        Path(".venv/bin"),
        Path(".venv/bin/python3.14"),
    ):
        assert _automatic_environment_directory(
            checkout=checkout,
            requested=requested,
        ) == environment_bin


def test_automatic_environment_rejects_a_non_executable_file(
    tmp_path: Path,
) -> None:
    """A random file is not silently interpreted as its parent PATH entry."""

    checkout = (tmp_path / "checkout").resolve()
    checkout.mkdir()
    ordinary_file = checkout / "requirements.txt"
    ordinary_file.write_text("pytest\n", encoding="utf-8")

    with pytest.raises(OperatorServiceError, match="is not executable"):
        _automatic_environment_directory(
            checkout=checkout,
            requested=ordinary_file,
        )


def test_enrollment_and_environment_bin_are_parse_time_exclusive(
    tmp_path: Path,
) -> None:
    """Conflicting registration modes fail before a state path can be opened."""

    state = tmp_path / "absent-state"
    with pytest.raises(SystemExit) as captured:
        main(
            [
                "--state-dir",
                str(state),
                "project",
                "register",
                str(tmp_path),
                "--enrollment",
                str(tmp_path / "Enrollment.json"),
                "--environment-bin",
                str(tmp_path / ".venv"),
                "--git-commit",
                "0" * 40,
                "--actor",
                "cli:test",
                "--reason",
                "invalid modes",
            ]
        )
    assert captured.value.code == 2
    assert not state.exists()


def test_automatic_enrollment_rejects_even_optional_volume_declarations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The simple path cannot admit a card whose optional artifact is unbound."""

    state = (tmp_path / "state").resolve()
    state.mkdir()
    checkout = (tmp_path / "checkout").resolve()
    checkout.mkdir()
    _git(checkout, "init", "--quiet")
    project_document = _project_document("cli-project", "CLI Project")
    project_document["spec"]["volumes"][0]["required"] = False  # type: ignore[index]
    (checkout / "Project.yaml").write_bytes(_source(project_document))
    (checkout / ".gitignore").write_text("/.venv/\n", encoding="utf-8")
    environment = checkout / ".venv"
    (environment / "bin").mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text("home = /python\n", encoding="utf-8")
    _git(checkout, "add", "--", "Project.yaml", ".gitignore")
    _git(
        checkout,
        "-c",
        "user.name=CLI Tests",
        "-c",
        "user.email=cli@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "optional volume",
    )
    commit = _git(checkout, "rev-parse", "HEAD")

    assert main(
        [
            "--state-dir",
            str(state),
            "project",
            "register",
            str(checkout),
            "--git-commit",
            commit,
            "--actor",
            "cli:test",
            "--reason",
            "invalid automatic enrollment",
            "--json",
        ]
    ) == 2
    error = json.loads(capsys.readouterr().err)
    assert "requires Project volumes: []" in error["message"]


def test_automatic_enrollment_proves_the_whole_checkout_venv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Tracked venv metadata outside bin invalidates automatic enrollment."""

    state = (tmp_path / "state").resolve()
    state.mkdir()
    checkout = (tmp_path / "checkout").resolve()
    checkout.mkdir()
    _git(checkout, "init", "--quiet")
    (checkout / "Project.yaml").write_bytes(
        project_manifest_scaffold(
            key="tracked-venv",
            display_name="Tracked Venv",
        )
    )
    (checkout / ".gitignore").write_text("/.venv/\n", encoding="utf-8")
    environment = checkout / ".venv"
    (environment / "bin").mkdir(parents=True)
    (environment / "pyvenv.cfg").write_text("home = /python\n", encoding="utf-8")
    _git(checkout, "add", "--", "Project.yaml", ".gitignore")
    _git(checkout, "add", "--force", "--", ".venv/pyvenv.cfg")
    _git(
        checkout,
        "-c",
        "user.name=CLI Tests",
        "-c",
        "user.email=cli@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "track venv metadata",
    )
    commit = _git(checkout, "rev-parse", "HEAD")

    assert main(
        [
            "--state-dir",
            str(state),
            "project",
            "register",
            str(checkout),
            "--git-commit",
            commit,
            "--actor",
            "cli:test",
            "--reason",
            "invalid tracked venv",
            "--json",
        ]
    ) == 2
    error = json.loads(capsys.readouterr().err)
    assert "contains tracked content" in error["message"]
    assert str(environment) in error["message"]


def _json_output(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    captured = capsys.readouterr()
    assert captured.err == ""
    return json.loads(captured.out)


def test_every_cli_option_has_help_and_every_leaf_has_json() -> None:
    """The parser is mountable and no production option is undocumented."""

    parser = build_arg_parser()

    def walk(current: argparse.ArgumentParser) -> None:
        children: list[argparse.ArgumentParser] = []
        for action in current._actions:  # noqa: SLF001 - parser contract audit
            if action.option_strings:
                assert action.help not in {None, argparse.SUPPRESS}
                assert str(action.help).strip()
            if isinstance(action, argparse._SubParsersAction):  # type: ignore[attr-defined]
                children.extend(action.choices.values())
        if children:
            for child in children:
                walk(child)
        else:
            assert any(action.dest == "json" for action in current._actions)  # noqa: SLF001

    walk(parser)


def test_read_command_does_not_create_absent_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A typoed/read-only state path never creates a directory or database."""

    absent = (tmp_path / "absent-state").resolve()
    assert main(
        ["--state-dir", str(absent), "project", "list", "--json"]
    ) == 2
    error = json.loads(capsys.readouterr().err)
    assert "does not exist" in error["message"]
    assert not absent.exists()


def test_card_new_and_schema_export_need_no_queue_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Onboarding scaffolds validate and export without creating queue state."""

    manifest = tmp_path / "Project.yaml"
    manifest.write_bytes(_source(_project_document("cli-project", "CLI Project")))
    cards = tmp_path / "cards"
    schemas = tmp_path / "schemas"
    cards.mkdir()
    schemas.mkdir()
    card = cards / "CLI-NEW.yaml"
    schema = schemas / "card.schema.json"

    assert main(
        [
            "card",
            "new",
            "--project-manifest",
            str(manifest),
            "--experiment-id",
            "CLI-NEW",
            "--title",
            "Generated CLI card",
            "--output",
            str(card),
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["operation"] == "card.new"
    assert main(
        [
            "card",
            "validate",
            "--project-manifest",
            str(manifest),
            "--card",
            str(card),
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["valid"] is True
    assert main(
        ["schema", "export", "card", "--output", str(schema), "--json"]
    ) == 0
    assert _json_output(capsys)["schemaKind"] == "card"
    assert json.loads(schema.read_text(encoding="utf-8"))["$id"].endswith(
        "experiment-card:v1"
    )


def test_register_show_append_activate_submit_and_receipt_are_authenticated(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The main cutover path retains exact Git evidence and global item IDs."""

    state_args = ["--state-dir", str(registered.state)]
    monkeypatch.chdir(registered.checkout)
    assert main([*state_args, "project", "show", "--json"]) == 0
    shown = _json_output(capsys)
    assert shown["project"]["currentRevision"]["kind"] == "project-v1"  # type: ignore[index]

    second_commit = registered.write_revision("CLI Project Renamed")
    assert main(
        [
            *state_args,
            "project",
            "append-revision",
            str(registered.checkout),
            "--project",
            "cli-project",
            "--manifest",
            PROJECT_PATH,
            "--enrollment",
            str(registered.enrollment_path),
            "--git-commit",
            second_commit,
            "--actor",
            "cli:test",
            "--no-activate",
            "--json",
        ]
    ) == 0
    appended = _json_output(capsys)
    assert appended["project"]["currentRevision"]["sequence"] == 1  # type: ignore[index]

    store = V5QueueStore(registered.state)
    with store.connect() as connection:
        revision_id = int(
            connection.execute(
                "SELECT id FROM project_revisions WHERE sequence = 2"
            ).fetchone()[0]
        )
    assert main(
        [
            *state_args,
            "project",
            "activate-revision",
            str(revision_id),
            "--project",
            "cli-project",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    activated = _json_output(capsys)
    assert activated["project"]["displayName"] == "CLI Project Renamed"  # type: ignore[index]

    submit_arguments = [
        *state_args,
        "submit",
        "--project",
        "cli-project",
        "--card-path",
        CARD_PATH,
        "--job-id",
        "train",
        "--operator",
        "cli:test",
        "--json",
    ]
    assert main([*submit_arguments, "--dry-run"]) == 0
    dry_run = _json_output(capsys)
    assert dry_run["wouldMutateState"] is False
    assert main(submit_arguments) == 0
    submitted = _json_output(capsys)
    assert submitted["item"]["id"] == 1  # type: ignore[index]
    assert submitted["item"]["projectKey"] == "cli-project"  # type: ignore[index]

    assert main(
        [
            *state_args,
            "receipt",
            "--project",
            "cli-project",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    captured_receipt = capsys.readouterr()
    assert captured_receipt.err == ""
    receipt_source = captured_receipt.out.encode("utf-8")
    receipt = json.loads(receipt_source)
    assert receipt_source == canonical_json_bytes(receipt)
    assert QueueExport.from_bytes(receipt_source).to_document() == receipt
    assert receipt["apiVersion"] == "experiment-queue/v1"
    assert receipt["kind"] == "QueueExport"
    assert receipt["actor"] == "cli:test"
    assert receipt["database"]["kind"] == "Database"  # type: ignore[index]
    assert receipt["database"]["instanceIdentity"] == store.instance_identity()  # type: ignore[index]
    assert receipt["hostState"]["dispatchPaused"] is False  # type: ignore[index]
    assert receipt["executorReceipts"]["exactSourceAvailable"] is False  # type: ignore[index]
    assert [item["id"] for item in receipt["items"]] == [1]  # type: ignore[index]
    item = receipt["items"][0]  # type: ignore[index]
    assert item["admissionSnapshot"]["submissionPolicy"]["document"]["dependencies"] == []  # type: ignore[index]
    assert item["commandText"] == b64decode(item["admissionSnapshot"]["command"]["sourceBase64"]).decode()  # type: ignore[index]


def test_unsorted_cli_dependencies_persist_and_export_in_canonical_order(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One ascending global-ID order binds policy, edges, event, and export."""

    state_args = ["--state-dir", str(registered.state)]
    submit = [
        *state_args,
        "submit",
        "--project",
        "cli-project",
        "--card-path",
        CARD_PATH,
        "--job-id",
        "train",
        "--operator",
        "cli:test",
        "--json",
    ]
    for expected_id in range(1, 6):
        assert main(submit) == 0
        assert _json_output(capsys)["item"]["id"] == expected_id  # type: ignore[index]
    assert main([*submit, "--dependency", "5", "--dependency", "2"]) == 0
    dependent = _json_output(capsys)
    assert dependent["item"]["id"] == 6  # type: ignore[index]

    store = V5QueueStore(registered.state)
    with store.connect() as connection:
        snapshot_dependencies = json.loads(
            connection.execute(
                """
                SELECT snapshot.policy_dependencies_json
                FROM queue_items AS item
                JOIN admission_snapshots AS snapshot
                  ON snapshot.id = item.snapshot_id
                WHERE item.id = 6
                """
            ).fetchone()[0]
        )
        edge_dependencies = [
            int(row[0])
            for row in connection.execute(
                """
                SELECT dependency_item_id FROM dependencies
                WHERE queue_item_id = 6 ORDER BY dependency_item_id
                """
            )
        ]
        event_dependencies = json.loads(
            connection.execute(
                """
                SELECT payload_json FROM events
                WHERE queue_item_id = 6 AND event_type = 'queue_item_admitted'
                """
            ).fetchone()[0]
        )["dependencies"]
    assert snapshot_dependencies == [2, 5]
    assert edge_dependencies == [2, 5]
    assert event_dependencies == [2, 5]

    assert main(
        [
            *state_args,
            "receipt",
            "--project",
            "cli-project",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    receipt_source = capsys.readouterr().out.encode("utf-8")
    receipt = QueueExport.from_bytes(receipt_source).to_document()
    exported = receipt["items"][5]  # type: ignore[index]
    assert exported["admissionSnapshot"]["submissionPolicy"]["document"]["dependencies"] == [2, 5]  # type: ignore[index]
    assert [dependency["itemId"] for dependency in exported["dependencies"]] == [2, 5]  # type: ignore[index]


def test_item_host_gpu_and_readable_status_commands(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutations stay project-qualified and every command also has readable output."""

    state_args = ["--state-dir", str(registered.state)]
    submit = [
        *state_args,
        "submit",
        "--project",
        "cli-project",
        "--card-path",
        CARD_PATH,
        "--job-id",
        "train",
        "--operator",
        "cli:test",
        "--json",
    ]
    assert main(submit) == 0
    _json_output(capsys)
    assert main(
        [
            *state_args,
            "item",
            "hold",
            "1",
            "--project",
            "cli-project",
            "--reason",
            "inspect",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["item"]["state"] == "held"  # type: ignore[index]
    assert main(
        [
            *state_args,
            "item",
            "priority",
            "1",
            "7",
            "--project",
            "cli-project",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["item"]["priority"] == 7  # type: ignore[index]

    assert main(
        [
            *state_args,
            "host",
            "pause",
            "--reason",
            "maintenance",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["dispatchPaused"] is True
    assert main(
        [*state_args, "host", "resume", "--actor", "cli:test", "--json"]
    ) == 0
    assert _json_output(capsys)["dispatchPaused"] is False

    with V5QueueStore(registered.state).connect() as connection:
        connection.execute(
            """
            UPDATE project_runtime_state
            SET health = 'open', circuit_failure_count = 1,
                health_reason = 'fixture failure'
            WHERE project_id = 1
            """
        )
        connection.commit()
    assert main(
        [
            *state_args,
            "project",
            "repair",
            "--project",
            "cli-project",
            "--reason",
            "fixture repaired",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    repaired = _json_output(capsys)["project"]  # type: ignore[index]
    assert repaired["health"] == "closed"
    assert repaired["circuitFailureCount"] == 0

    monkeypatch.setattr(
        "experiment_queue.cli_v5.query_gpus",
        lambda _executable: [
            SimpleNamespace(
                uuid="GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                index="0",
                name="Fixture GPU",
            )
        ],
    )
    assert main(
        [
            *state_args,
            "gpu",
            "add",
            "0",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    gpu = _json_output(capsys)["gpus"][0]  # type: ignore[index]
    assert gpu["enabled"] is True
    assert main(
        [
            *state_args,
            "gpu",
            "drain",
            str(gpu["uuid"]),
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["gpus"][0]["draining"] is True  # type: ignore[index]

    assert main([*state_args, "status", "--project", "cli-project"]) == 0
    readable = capsys.readouterr().out
    assert "GLOBAL-ID" in readable
    assert "CLI-001/a1" in readable


def test_gpu_reservation_cli_uses_typed_service(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Request, list, and release expose passive reservation state end to end."""

    state_args = ["--state-dir", str(registered.state)]
    uuid = "GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    monkeypatch.setattr(
        "experiment_queue.cli_v5.query_gpus",
        lambda _executable: [
            SimpleNamespace(uuid=uuid, index="0", name="Fixture GPU")
        ],
    )
    assert main(
        [*state_args, "gpu", "add", "0", "--actor", "cli:test", "--json"]
    ) == 0
    _json_output(capsys)
    assert main(
        [
            *state_args,
            "reservation",
            "request",
            uuid,
            "--duration-hours",
            "2",
            "--note",
            "CLI reservation fixture",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    requested = _json_output(capsys)["reservations"][0]  # type: ignore[index]
    assert requested["status"] == "active"
    reservation_id = str(requested["id"])
    assert main(
        [*state_args, "reservation", "list", "--open-only", "--json"]
    ) == 0
    assert len(_json_output(capsys)["reservations"]) == 1  # type: ignore[arg-type]
    assert main(
        [
            *state_args,
            "reservation",
            "release",
            reservation_id,
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    assert _json_output(capsys)["reservations"][0]["status"] == "released"  # type: ignore[index]


def test_manual_preemption_delegates_to_scheduler_service(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI publishes no signal or continuation SQL of its own."""

    state_args = ["--state-dir", str(registered.state)]
    assert main(
        [
            *state_args,
            "submit",
            "--project",
            "cli-project",
            "--card-path",
            CARD_PATH,
            "--job-id",
            "train",
            "--operator",
            "cli:test",
            "--json",
        ]
    ) == 0
    _json_output(capsys)
    with V5QueueStore(registered.state).connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1
            WHERE id = 1
            """
        )
        connection.commit()

    calls: list[tuple[int, str, str, str | None]] = []

    class FakeService:
        def __init__(self, _store: V5QueueStore) -> None:
            pass

        def request_manual_preemption(
            self,
            item_id: int,
            *,
            note: str,
            actor: str,
            requested_at: str | None = None,
        ) -> SimpleNamespace:
            calls.append((item_id, note, actor, requested_at))
            return SimpleNamespace(
                project_id=1,
                revision_id=1,
                queue_item_id=item_id,
                segment=1,
                request=SimpleNamespace(request_id="request-1"),
                request_sha256="a" * 64,
                request_path=registered.state / "request.json",
                receipt_path=registered.state / "receipt.json",
            )

    monkeypatch.setattr("experiment_queue.cli_v5.V5SchedulerService", FakeService)
    assert main(
        [
            *state_args,
            "item",
            "preempt",
            "1",
            "--project",
            "cli-project",
            "--note",
            "checkpoint for maintenance",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    output = _json_output(capsys)
    assert output["requestId"] == "request-1"
    assert calls[0][:3] == (1, "checkpoint for maintenance", "cli:test")


def test_termination_commands_delegate_after_project_authorization(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Graceful and confirmed force controls share the durable service boundary."""

    state_args = ["--state-dir", str(registered.state)]
    assert main(
        [
            *state_args,
            "submit",
            "--project",
            "cli-project",
            "--card-path",
            CARD_PATH,
            "--job-id",
            "train",
            "--operator",
            "cli:test",
            "--json",
        ]
    ) == 0
    _json_output(capsys)
    with V5QueueStore(registered.state).connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1
            WHERE id = 1
            """
        )
        connection.commit()

    calls: list[dict[str, object]] = []

    class FakeService:
        def __init__(self, _store: V5QueueStore) -> None:
            pass

        def request_termination(self, item_id: int, **values: object) -> SimpleNamespace:
            calls.append({"item_id": item_id, **values})
            force = bool(values["force"])
            action = SimpleNamespace(
                item_id=item_id,
                project_id=1,
                segment=1,
                state="force_killing" if force else "terminating",
                stage="kill" if force else "interrupt",
                requested_at=values["requested_at"],
                reason=values["reason"],
            )
            return SimpleNamespace(action=action, signal_delivered=True)

    monkeypatch.setattr("experiment_queue.cli_v5.V5SchedulerService", FakeService)
    assert main(
        [
            *state_args,
            "item",
            "terminate",
            "1",
            "--project",
            "cli-project",
            "--reason",
            "maintenance",
            "--actor",
            "cli:test",
            "--json",
        ]
    ) == 0
    graceful = _json_output(capsys)
    assert graceful["stage"] == "interrupt"
    assert graceful["signalDelivered"] is True

    assert main(
        [
            *state_args,
            "item",
            "force-kill",
            "1",
            "--project",
            "cli-project",
            "--reason",
            "unsafe child",
            "--actor",
            "cli:test",
            "--confirm",
            "FORCE-KILL",
            "--json",
        ]
    ) == 0
    forced = _json_output(capsys)
    assert forced["state"] == "force_killing"
    assert [call["force"] for call in calls] == [False, True]


@pytest.mark.parametrize("action", ["terminate", "force-kill"])
def test_cli_refuses_termination_during_starting_claim(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    action: str,
) -> None:
    """CLI control cannot turn the claim-to-launch window into a null-ID wedge."""

    state_args = ["--state-dir", str(registered.state)]
    assert main(
        [
            *state_args,
            "submit",
            "--project",
            "cli-project",
            "--card-path",
            CARD_PATH,
            "--job-id",
            "train",
            "--operator",
            "cli:test",
            "--json",
        ]
    ) == 0
    _json_output(capsys)
    with V5QueueStore(registered.state).connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'starting', "
            "assigned_gpu_uuid = 'GPU-fixture', assigned_gpu_index = '0', "
            "runtime_gpu_lease_held = 1 "
            "WHERE id = 1"
        )
    arguments = [
        *state_args,
        "item",
        action,
        "1",
        "--project",
        "cli-project",
        "--reason",
        "pre-launch race",
        "--actor",
        "cli:test",
        "--json",
    ]
    if action == "force-kill":
        arguments.extend(["--confirm", "FORCE-KILL"])

    assert main(arguments) == 2
    error = json.loads(capsys.readouterr().err)
    assert "recover/adopt the launch first" in error["message"]
    with V5QueueStore(registered.state).connect() as connection:
        row = connection.execute(
            "SELECT state, pid, pgid FROM queue_items WHERE id = 1"
        ).fetchone()
    assert tuple(row) == ("starting", None, None)


def test_resolve_abandoned_launch_delegates_with_exact_operator_evidence(
    registered: ProjectFixture,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guarded recovery command preserves project/GPU/reason/actor evidence."""

    state_args = ["--state-dir", str(registered.state)]
    assert main(
        [
            *state_args,
            "submit",
            "--project",
            "cli-project",
            "--card-path",
            CARD_PATH,
            "--job-id",
            "train",
            "--operator",
            "cli:test",
            "--json",
        ]
    ) == 0
    _json_output(capsys)
    with V5QueueStore(registered.state).connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'starting', assigned_gpu_uuid = 'GPU-fixture',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1
            WHERE id = 1
            """
        )

    calls: list[dict[str, object]] = []

    class FakeService:
        def __init__(self, _store: V5QueueStore) -> None:
            pass

        def resolve_abandoned_launch(
            self,
            item_id: int,
            **values: object,
        ) -> SimpleNamespace:
            calls.append({"item_id": item_id, **values})
            return SimpleNamespace(
                resolution=SimpleNamespace(
                    item_id=item_id,
                    project_id=1,
                    gpu_uuid="GPU-fixture",
                    previous_state="starting",
                    event_type="ABANDONED_LAUNCH_RESOLVED",
                    state="failed",
                    reason=values["reason"],
                    resolved_at=values["changed_at"],
                ),
                launch_receipt_status="absent",
                worktree_cleanup_error=None,
            )

    monkeypatch.setattr("experiment_queue.cli_v5.V5SchedulerService", FakeService)
    assert main(
        [
            *state_args,
            "item",
            "resolve-abandoned-launch",
            "1",
            "--project",
            "cli-project",
            "--gpu-uuid",
            "GPU-fixture",
            "--reason",
            "operator proved no process or GPU workload",
            "--actor",
            "cli:test",
            "--confirm",
            "RESOLVE-ABANDONED-LAUNCH",
            "--json",
        ]
    ) == 0
    output = _json_output(capsys)
    assert output["eventType"] == "ABANDONED_LAUNCH_RESOLVED"
    assert output["previousState"] == "starting"
    assert output["hostDispatchPaused"] is True
    assert output["projectRepairRequired"] is True
    assert calls == [
        {
            "item_id": 1,
            "project_id": 1,
            "gpu_uuid": "GPU-fixture",
            "reason": "operator proved no process or GPU workload",
            "actor": "cli:test",
            "confirm": "RESOLVE-ABANDONED-LAUNCH",
            "changed_at": calls[0]["changed_at"],
        }
    ]


def test_resolve_abandoned_launch_help_names_fail_closed_guards(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Operators can discover the exact confirmation and safety prerequisites."""

    with pytest.raises(SystemExit) as stopped:
        build_arg_parser().parse_args(
            ["item", "resolve-abandoned-launch", "--help"]
        )
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    normalized = " ".join(help_text.split())
    assert "RESOLVE-ABANDONED-LAUNCH" in normalized
    assert "Host dispatch must already be paused" in normalized
    assert "process group is absent" in normalized
    assert "--gpu-uuid" in normalized


def test_serve_and_migration_dispatch_without_cli_sql(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long-running and offline workflows call their typed service boundaries."""

    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    ran: list[bool] = []

    class FakeScheduler:
        def __init__(self, _store: V5QueueStore, **_kwargs: object) -> None:
            pass

        def run(self, *, once: bool = False) -> None:
            ran.append(once)

    monkeypatch.setattr("experiment_queue.cli_v5.V5SchedulerService", FakeScheduler)
    assert main(
        ["--state-dir", str(state), "serve", "--once", "--json"]
    ) == 0
    assert _json_output(capsys)["once"] is True
    assert ran == [True]

    migrated: list[dict[str, object]] = []

    def fake_migrate(**values: object) -> SimpleNamespace:
        migrated.append(values)
        receipt = SimpleNamespace(
            sha256="b" * 64,
            to_document=lambda: {
                "apiVersion": "experiment-queue/v1",
                "kind": "QueueMigrationReceipt",
            },
        )
        return SimpleNamespace(
            destination_state=values["destination_state"],
            receipt_path=values["receipt_path"],
            receipt=receipt,
            published=False,
        )

    monkeypatch.setattr("experiment_queue.cli_v5.migrate_legacy_state", fake_migrate)
    source = (tmp_path / "source-copy").resolve()
    destination = (tmp_path / "destination").resolve()
    checkout = (tmp_path / "legacy-checkout").resolve()
    receipt = (tmp_path / "migration-receipt.json").resolve()
    assert main(
        [
            "migrate",
            "--source-state",
            str(source),
            "--destination-state",
            str(destination),
            "--project-key",
            "legacy-project",
            "--legacy-checkout",
            str(checkout),
            "--actor",
            "cli:test",
            "--receipt",
            str(receipt),
            "--dry-run",
            "--confirm-source-is-copy",
            "--json",
        ]
    ) == 0
    output = _json_output(capsys)
    assert output["published"] is False
    assert migrated[0]["confirm_source_is_copy"] is True
