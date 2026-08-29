"""Prove admission reads only bounded regular blobs from a pinned Git tree."""

from __future__ import annotations

import copy
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess

import pytest

from experiment_queue.admission import Submission
from experiment_queue.authoring import Project
import experiment_queue.git_resolver as resolver_module
from experiment_queue.git_resolver import (
    GitBlobEvidence,
    GitResolvedAdmission,
    GitResolverError,
    compile_admission_from_revision,
    verify_git_ignored_checkout_descendants,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    ProjectRevision,
)
from experiment_queue.serialization import sha256_bytes


PROJECT_PATH = "config/project.yaml"
CARD_PATH = "cards/EXP-001.yaml"
EXTENSION_PATH = "schemas/extension.json"


@dataclass(frozen=True, slots=True)
class RepositoryFixture:
    """One temporary registered revision and its committed source bytes."""

    root: Path
    repository: Path
    project_source: bytes
    card_source: bytes
    extension_source: bytes | None
    project: Project
    enrollment: Enrollment
    revision: ProjectRevision


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"test Git command failed: {arguments!r}: "
            f"{result.stderr.decode(errors='replace')}"
        )
    return result.stdout.decode("ascii").strip()


def _source(document: dict[str, object]) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode()


def _project_document(*, with_extension: bool = False) -> dict[str, object]:
    spec: dict[str, object] = {
        "cardRoots": ["cards"],
        "volumes": [],
        "environments": [{"name": "python"}],
        "environmentPolicy": {"inherit": "none", "allowVariables": []},
        "supportedProtocols": [],
    }
    if with_extension:
        spec["extensionSchema"] = {"path": EXTENSION_PATH}
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "fixture-project",
            "displayName": "Git resolver fixture",
        },
        "spec": spec,
    }


def _card_document() -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "fixture-project",
            "experimentId": "EXP-001",
            "title": "Pinned Git admission",
        },
        "spec": {
            "parameters": {"epochs": 1},
            "jobs": [
                {
                    "id": "train",
                    "environment": "python",
                    "command": {
                        "type": "argv",
                        "argv": ["python", "train.py"],
                    },
                }
            ],
        },
    }


def _submission(**changes: object) -> Submission:
    values: dict[str, object] = {
        "project_key": "fixture-project",
        "card_path": CARD_PATH,
        "job_id": "train",
        "operator": "test:operator",
        "bindings": {"epochs": 2},
    }
    values.update(changes)
    return Submission(**values)  # type: ignore[arg-type]


def _create_enrollment(
    *,
    root: Path,
    repository: Path,
    project: Project,
) -> Enrollment:
    state_directory = root / "state"
    environment_directory = root / "environment-bin"
    state_directory.mkdir(exist_ok=True)
    environment_directory.mkdir(exist_ok=True)
    environment = EnvironmentBinding.create(
        name="python",
        executable_search_directories=[environment_directory],
    )
    return Enrollment.create(
        project=project,
        checkout_directory=repository,
        project_manifest_path=PROJECT_PATH,
        mounts=(),
        environments=(environment,),
        state_directory=state_directory,
    )


def _create_revision(
    fixture: RepositoryFixture,
    *,
    commit: str,
    enrollment: Enrollment | None = None,
    project_source: bytes | None = None,
    sequence: int = 2,
) -> ProjectRevision:
    return ProjectRevision.create(
        revision_id=sequence,
        project_id=17,
        sequence=sequence,
        project=fixture.project,
        project_source_path=PROJECT_PATH,
        project_source=(
            fixture.project_source if project_source is None else project_source
        ),
        git_commit=commit,
        enrollment=fixture.enrollment if enrollment is None else enrollment,
        created_actor="test:operator",
        created_at="2026-08-28T12:00:00Z",
        extension_schema_source=fixture.extension_source,
    )


def _make_repository_fixture(
    root: Path,
    *,
    with_extension: bool = False,
) -> RepositoryFixture:
    repository = root / "repository"
    repository.mkdir(parents=True)
    _git(repository, "init", "--quiet")

    project_source = _source(_project_document(with_extension=with_extension))
    card_source = _source(_card_document())
    extension_source = None
    (repository / PROJECT_PATH).parent.mkdir(parents=True)
    (repository / PROJECT_PATH).write_bytes(project_source)
    (repository / CARD_PATH).parent.mkdir(parents=True)
    (repository / CARD_PATH).write_bytes(card_source)
    if with_extension:
        extension_source = _source(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": True,
            }
        )
        (repository / EXTENSION_PATH).parent.mkdir(parents=True)
        (repository / EXTENSION_PATH).write_bytes(extension_source)
    _git(repository, "add", "--", PROJECT_PATH, CARD_PATH)
    if with_extension:
        _git(repository, "add", "--", EXTENSION_PATH)
    _git(
        repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    commit = _git(repository, "rev-parse", "HEAD")
    project = Project.from_yaml(project_source, source_name=PROJECT_PATH)
    enrollment = _create_enrollment(
        root=root,
        repository=repository,
        project=project,
    )
    revision = ProjectRevision.create(
        revision_id=1,
        project_id=17,
        sequence=1,
        project=project,
        project_source_path=PROJECT_PATH,
        project_source=project_source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at="2026-08-28T11:00:00Z",
        extension_schema_source=extension_source,
    )
    return RepositoryFixture(
        root=root,
        repository=repository,
        project_source=project_source,
        card_source=card_source,
        extension_source=extension_source,
        project=project,
        enrollment=enrollment,
        revision=revision,
    )


@pytest.fixture
def repository_fixture(tmp_path: Path) -> RepositoryFixture:
    return _make_repository_fixture(tmp_path)


def test_resolver_returns_factory_only_exact_git_evidence(
    repository_fixture: RepositoryFixture,
) -> None:
    first = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )
    second = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )

    assert first == second
    assert first.project_id == 17
    assert first.project_revision_id == 1
    assert first.project_key == "fixture-project"
    assert first.project_revision_label == "fixture-project:r1"
    assert first.repository_root == str(repository_fixture.repository.resolve())
    assert first.git_commit == repository_fixture.revision.git_commit
    assert first.project_blob.path == PROJECT_PATH
    assert first.project_blob.object_id == _git(
        repository_fixture.repository,
        "rev-parse",
        f"{first.git_commit}:{PROJECT_PATH}",
    )
    assert first.project_blob.mode == "100644"
    assert first.project_blob.size == len(repository_fixture.project_source)
    assert first.project_blob.source_sha256 == sha256_bytes(
        repository_fixture.project_source
    )
    assert first.card_blob.path == CARD_PATH
    assert first.card_blob.source_sha256 == first.snapshot.card_source_sha256
    assert first.extension_schema_blob is None
    assert first.snapshot.project_source == repository_fixture.project_source
    assert first.snapshot.card_source == repository_fixture.card_source
    assert first.snapshot.project_revision == "fixture-project:r1"
    assert first.snapshot.git_commit == first.git_commit
    assert first.admission_snapshot is first.snapshot

    with pytest.raises(TypeError, match="trusted Git evidence"):
        GitBlobEvidence()
    with pytest.raises(TypeError, match="trusted Git evidence"):
        GitResolvedAdmission()


def test_resolver_recomputes_tree_blob_identity_from_returned_bytes(
    repository_fixture: RepositoryFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tree metadata cannot authenticate different same-length blob bytes."""

    original = resolver_module._run_git

    def inconsistent_git(
        repository_root: Path,
        arguments: tuple[str, ...],
        **kwargs: object,
    ):
        result = original(repository_root, arguments, **kwargs)  # type: ignore[arg-type]
        if arguments[:2] == ("cat-file", "blob") and result.stdout:
            changed = bytearray(result.stdout)
            changed[0] ^= 1
            return resolver_module._GitResult(
                stdout=bytes(changed),
                stderr=result.stderr,
                returncode=result.returncode,
            )
        return result

    monkeypatch.setattr(resolver_module, "_run_git", inconsistent_git)
    with pytest.raises(GitResolverError, match="computed blob object ID"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(),
        )


def test_dirty_and_untracked_worktree_cannot_change_admission(
    repository_fixture: RepositoryFixture,
) -> None:
    expected = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )
    (repository_fixture.repository / PROJECT_PATH).write_text(
        "not: [the committed project\n",
        encoding="utf-8",
    )
    (repository_fixture.repository / CARD_PATH).write_text(
        "not: [the committed card\n",
        encoding="utf-8",
    )
    (repository_fixture.repository / "cards/untracked.yaml").write_text(
        "outside: committed evidence\n",
        encoding="utf-8",
    )

    actual = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )

    assert actual == expected
    assert actual.snapshot.project_source == repository_fixture.project_source
    assert actual.snapshot.card_source == repository_fixture.card_source


def test_git_routing_environment_cannot_select_another_repository(
    repository_fixture: RepositoryFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = tmp_path / "other"
    other.mkdir()
    _git(other, "init", "--quiet")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))

    resolved = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )

    assert resolved.repository_root == str(repository_fixture.repository.resolve())
    assert resolved.snapshot.git_commit == repository_fixture.revision.git_commit


def test_replacement_refs_cannot_change_the_pinned_commit_tree(
    repository_fixture: RepositoryFixture,
) -> None:
    changed = _card_document()
    spec = changed["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    command = job["command"]
    assert isinstance(command, dict)
    command["argv"] = ["python", "replacement.py"]
    (repository_fixture.repository / CARD_PATH).write_bytes(_source(changed))
    _git(repository_fixture.repository, "add", "--", CARD_PATH)
    _git(
        repository_fixture.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "replacement commit",
    )
    replacement = _git(repository_fixture.repository, "rev-parse", "HEAD")
    _git(
        repository_fixture.repository,
        "replace",
        repository_fixture.revision.git_commit,
        replacement,
    )

    resolved = compile_admission_from_revision(
        revision=repository_fixture.revision,
        submission=_submission(),
    )

    assert resolved.snapshot.card_source == repository_fixture.card_source
    assert resolved.snapshot.selected_command.to_document()["argv"] == [
        "python",
        "train.py",
    ]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("extensions.partialClone", "origin"),
        ("remote.cache.promisor", "true"),
        ("remote.cache.partialCloneFilter", "blob:none"),
    ],
)
def test_partial_clone_marker_is_rejected_before_object_resolution(
    repository_fixture: RepositoryFixture,
    key: str,
    value: str,
) -> None:
    _git(
        repository_fixture.repository,
        "config",
        key,
        value,
    )

    with pytest.raises(GitResolverError, match="partial clone.*promisor.*offline"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(),
        )


def test_wrong_registered_repository_fails_closed(
    repository_fixture: RepositoryFixture,
    tmp_path: Path,
) -> None:
    other_root = tmp_path / "wrong-root"
    other_repository = other_root / "repository"
    other_repository.mkdir(parents=True)
    _git(other_repository, "init", "--quiet")
    (other_repository / "unrelated.txt").write_text("unrelated\n", encoding="utf-8")
    _git(other_repository, "add", "--", "unrelated.txt")
    _git(
        other_repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "unrelated",
    )
    wrong_enrollment = _create_enrollment(
        root=other_root,
        repository=other_repository,
        project=repository_fixture.project,
    )
    wrong_revision = _create_revision(
        repository_fixture,
        commit=repository_fixture.revision.git_commit,
        enrollment=wrong_enrollment,
    )

    with pytest.raises(GitResolverError, match="find pinned object"):
        compile_admission_from_revision(
            revision=wrong_revision,
            submission=_submission(),
        )


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("abbreviated", "full 40- or 64-character"),
        ("uppercase", "exact lowercase full"),
        ("blob", "type 'blob'.*not 'commit'"),
        ("missing", "find pinned object"),
    ],
)
def test_only_an_exact_existing_commit_object_is_accepted(
    repository_fixture: RepositoryFixture,
    kind: str,
    message: str,
) -> None:
    if kind == "abbreviated":
        revision = copy.copy(repository_fixture.revision)
        object.__setattr__(
            revision,
            "git_commit",
            repository_fixture.revision.git_commit[:12],
        )
    elif kind == "uppercase":
        revision = copy.copy(repository_fixture.revision)
        object.__setattr__(
            revision,
            "git_commit",
            repository_fixture.revision.git_commit.upper(),
        )
    elif kind == "blob":
        blob = _git(
            repository_fixture.repository,
            "rev-parse",
            f"{repository_fixture.revision.git_commit}:{CARD_PATH}",
        )
        revision = _create_revision(repository_fixture, commit=blob)
    else:
        revision = _create_revision(repository_fixture, commit="f" * 40)

    with pytest.raises(GitResolverError, match=message):
        compile_admission_from_revision(
            revision=revision,
            submission=_submission(),
        )


def test_revision_label_and_source_evidence_are_revalidated(
    repository_fixture: RepositoryFixture,
) -> None:
    wrong_label = copy.copy(repository_fixture.revision)
    object.__setattr__(wrong_label, "label", "fixture-project:r99")
    with pytest.raises(GitResolverError, match="label.*immutable identity"):
        compile_admission_from_revision(
            revision=wrong_label,
            submission=_submission(),
        )

    wrong_hash = copy.copy(repository_fixture.revision)
    object.__setattr__(wrong_hash, "project_source_sha256", "0" * 64)
    with pytest.raises(GitResolverError, match="source SHA-256"):
        compile_admission_from_revision(
            revision=wrong_hash,
            submission=_submission(),
        )

    different_checkout = repository_fixture.root / "different-checkout"
    different_checkout.mkdir()
    wrong_enrollment = copy.copy(repository_fixture.enrollment)
    object.__setattr__(
        wrong_enrollment,
        "checkout_directory",
        different_checkout.resolve(),
    )
    wrong_checkout = copy.copy(repository_fixture.revision)
    object.__setattr__(wrong_checkout, "enrollment", wrong_enrollment)
    with pytest.raises(GitResolverError, match="Enrollment fields.*checkout"):
        compile_admission_from_revision(
            revision=wrong_checkout,
            submission=_submission(),
        )


def test_commit_tree_project_bytes_must_equal_revision_source(
    repository_fixture: RepositoryFixture,
) -> None:
    changed = _project_document()
    metadata = changed["metadata"]
    assert isinstance(metadata, dict)
    metadata["displayName"] = "Changed in a later commit"
    (repository_fixture.repository / PROJECT_PATH).write_bytes(_source(changed))
    _git(repository_fixture.repository, "add", "--", PROJECT_PATH)
    _git(
        repository_fixture.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "change project",
    )
    changed_commit = _git(repository_fixture.repository, "rev-parse", "HEAD")
    mismatched_revision = _create_revision(
        repository_fixture,
        commit=changed_commit,
    )

    with pytest.raises(GitResolverError, match="does not equal the exact bytes"):
        compile_admission_from_revision(
            revision=mismatched_revision,
            submission=_submission(),
        )


@pytest.mark.parametrize(
    "card_path",
    ["../outside.yaml", "/absolute/card.yaml", "cards/../outside.yaml"],
)
def test_submission_path_escape_is_rejected_before_git_lookup(
    repository_fixture: RepositoryFixture,
    card_path: str,
) -> None:
    with pytest.raises(GitResolverError, match="repository-relative POSIX path"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(card_path=card_path),
        )


def test_missing_directory_and_symlink_sources_are_never_followed(
    repository_fixture: RepositoryFixture,
) -> None:
    with pytest.raises(GitResolverError, match="does not contain ExperimentCard"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(card_path="cards/missing.yaml"),
        )
    with pytest.raises(GitResolverError, match="is a directory"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(card_path="cards"),
        )

    card = repository_fixture.repository / CARD_PATH
    card.unlink()
    card.symlink_to("../outside.yaml")
    _git(repository_fixture.repository, "add", "--", CARD_PATH)
    _git(
        repository_fixture.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "replace card with symlink",
    )
    symlink_commit = _git(repository_fixture.repository, "rev-parse", "HEAD")
    symlink_revision = _create_revision(
        repository_fixture,
        commit=symlink_commit,
    )
    with pytest.raises(GitResolverError, match="symbolic link.*never.*symlinks"):
        compile_admission_from_revision(
            revision=symlink_revision,
            submission=_submission(),
        )


def test_extension_schema_is_read_from_the_same_commit(tmp_path: Path) -> None:
    fixture = _make_repository_fixture(tmp_path, with_extension=True)
    assert fixture.extension_source is not None
    (fixture.repository / EXTENSION_PATH).write_text(
        "this is not committed JSON\n",
        encoding="utf-8",
    )

    resolved = compile_admission_from_revision(
        revision=fixture.revision,
        submission=_submission(),
    )

    assert resolved.extension_schema_blob is not None
    assert resolved.extension_schema_blob.path == EXTENSION_PATH
    assert resolved.extension_schema_blob.mode == "100644"
    assert resolved.extension_schema_blob.source_sha256 == sha256_bytes(
        fixture.extension_source
    )
    assert resolved.snapshot.extension_schema is not None
    assert resolved.snapshot.extension_schema.source == fixture.extension_source


def test_git_blob_reads_enforce_a_size_bound(
    repository_fixture: RepositoryFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resolver_module,
        "MAX_GIT_SOURCE_BYTES",
        len(repository_fixture.project_source) - 1,
    )

    with pytest.raises(GitResolverError, match="admission limit.*reduce or split"):
        compile_admission_from_revision(
            revision=repository_fixture.revision,
            submission=_submission(),
        )


def test_checkout_descendant_ignore_proof_uses_only_exact_pinned_tree(
    repository_fixture: RepositoryFixture,
) -> None:
    """Mutable checkout state cannot manufacture commit-pinned root authority."""

    root = repository_fixture.repository / "mutable" / "outputs"
    root.mkdir(parents=True)
    (repository_fixture.repository / ".git" / "info" / "exclude").write_text(
        "/mutable/\n",
        encoding="utf-8",
    )
    with pytest.raises(GitResolverError, match="not ignored by committed"):
        verify_git_ignored_checkout_descendants(
            repository_root=repository_fixture.repository,
            git_commit=repository_fixture.revision.git_commit,
            descendants=(root,),
        )

    (repository_fixture.repository / ".gitignore").write_text(
        "/mutable/\n",
        encoding="utf-8",
    )
    _git(repository_fixture.repository, "add", "--", ".gitignore")
    _git(
        repository_fixture.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "ignore mutable root",
    )
    ignored_commit = _git(repository_fixture.repository, "rev-parse", "HEAD")
    assert verify_git_ignored_checkout_descendants(
        repository_root=repository_fixture.repository,
        git_commit=ignored_commit,
        descendants=(root,),
    ) == (root.resolve(),)

    tracked = root / "already-tracked.txt"
    tracked.write_text("immutable tree content\n", encoding="utf-8")
    _git(repository_fixture.repository, "add", "--force", "--", str(tracked))
    _git(
        repository_fixture.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "tracked content under ignored root",
    )
    tracked_commit = _git(repository_fixture.repository, "rev-parse", "HEAD")
    with pytest.raises(GitResolverError, match="tracked content"):
        verify_git_ignored_checkout_descendants(
            repository_root=repository_fixture.repository,
            git_commit=tracked_commit,
            descendants=(root,),
        )
