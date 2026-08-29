"""Verify project-qualified Git ref, worktree, recovery, and cleanup isolation."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest

import experiment_queue.project_worktrees as worktree_module
from experiment_queue.authoring import Project
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
    ProjectRevision,
)
from experiment_queue.project_worktrees import (
    ProjectWorktreeError,
    ProjectWorktreeEvidence,
    ProjectWorktreeManager,
    QUEUE_REF_NAMESPACE,
)


NOW = "2026-08-28T18:00:00+00:00"


def git(repository: Path, *arguments: str) -> str:
    """Run one test-fixture Git command and return stripped stdout."""

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
            f"test Git command failed: {arguments!r}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def project_document(key: str, display_name: str) -> dict[str, object]:
    """Return a minimal portable Project/v1 for worktree fixtures."""

    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {"key": key, "displayName": display_name},
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
            "environmentPolicy": {"inherit": "none", "allowVariables": []},
            "supportedProtocols": [],
        },
    }


def project_source(project: Project) -> bytes:
    """Encode exact Project source committed into a fixture repository."""

    return (json.dumps(project.to_document(), indent=2) + "\n").encode()


@dataclass(slots=True)
class RepositoryFixture:
    """Paths and immutable revision for one temporary scientific repository."""

    base: Path
    repository: Path
    state_directory: Path
    worktree_root: Path
    scratch: Path
    environment_root: Path
    project: Project
    revision: ProjectRevision


def make_repository(
    base: Path,
    *,
    key: str = "alpha-project",
    project_id: int = 1,
    revision_id: int = 11,
    shared_state_directory: Path | None = None,
) -> RepositoryFixture:
    """Create one committed repo and a matching ProjectRevision."""

    base.mkdir(parents=True)
    repository = base / "repository"
    repository.mkdir()
    git(repository, "init", "--quiet")
    git(repository, "config", "user.name", "Queue Tests")
    git(repository, "config", "user.email", "queue-tests@example.invalid")

    project = Project.from_document(project_document(key, f"{key} display"))
    source = project_source(project)
    (repository / "Project.yaml").write_bytes(source)
    (repository / "program.py").write_text(
        "print('first revision')\n",
        encoding="utf-8",
    )
    git(repository, "add", "Project.yaml", "program.py")
    git(repository, "commit", "--quiet", "-m", "initial revision")
    commit = git(repository, "rev-parse", "HEAD")

    state = shared_state_directory or base / "state"
    state.mkdir(parents=True, exist_ok=True)
    worktree_root = state / "worktrees"
    worktree_root.mkdir(exist_ok=True)
    scratch = base / "scratch"
    scratch.mkdir()
    environment_root = base / "python-bin"
    environment_root.mkdir()
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
                executable_search_directories=[environment_root],
            )
        ],
        state_directory=state,
    )
    revision = ProjectRevision.create(
        revision_id=revision_id,
        project_id=project_id,
        sequence=1,
        project=project,
        project_source_path="Project.yaml",
        project_source=source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    return RepositoryFixture(
        base=base,
        repository=repository.resolve(),
        state_directory=state.resolve(),
        worktree_root=worktree_root.resolve(),
        scratch=scratch.resolve(),
        environment_root=environment_root.resolve(),
        project=project,
        revision=revision,
    )


def next_revision(
    fixture: RepositoryFixture,
    *,
    revision_id: int,
    sequence: int = 2,
) -> ProjectRevision:
    """Append and model a second committed Project revision in the same repo."""

    project = Project.from_document(
        project_document(
            fixture.revision.project_key,
            f"{fixture.revision.project_key} revision {sequence}",
        )
    )
    source = project_source(project)
    (fixture.repository / "Project.yaml").write_bytes(source)
    (fixture.repository / "program.py").write_text(
        f"print('revision {sequence}')\n",
        encoding="utf-8",
    )
    git(fixture.repository, "add", "Project.yaml", "program.py")
    git(fixture.repository, "commit", "--quiet", "-m", f"revision {sequence}")
    commit = git(fixture.repository, "rev-parse", "HEAD")
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=fixture.repository,
        project_manifest_path="Project.yaml",
        mounts=list(fixture.revision.enrollment.mounts),
        environments=list(fixture.revision.enrollment.environments),
        state_directory=fixture.state_directory,
    )
    return ProjectRevision.create(
        revision_id=revision_id,
        project_id=fixture.revision.project_id,
        sequence=sequence,
        project=project,
        project_source_path="Project.yaml",
        project_source=source,
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )


@pytest.mark.parametrize(
    "model",
    [ProjectWorktreeEvidence, ProjectWorktreeManager],
)
def test_worktree_models_are_factory_only(model: type[object]) -> None:
    """Trusted manager state and evidence cannot bypass public validation."""

    with pytest.raises(TypeError, match="validated-only"):
        model()  # type: ignore[call-arg]


def test_manager_requires_existing_absolute_dedicated_root(tmp_path: Path) -> None:
    """No cwd fallback, missing path, regular file, or filesystem root is allowed."""

    with pytest.raises(ProjectWorktreeError, match="must be absolute"):
        ProjectWorktreeManager.create("relative/worktrees")
    with pytest.raises(ProjectWorktreeError, match="existing directory"):
        ProjectWorktreeManager.create(tmp_path / "missing")
    regular = tmp_path / "regular"
    regular.write_text("not a directory", encoding="utf-8")
    with pytest.raises(ProjectWorktreeError, match="not a directory"):
        ProjectWorktreeManager.create(regular)
    with pytest.raises(ProjectWorktreeError, match="filesystem root"):
        ProjectWorktreeManager.create(Path("/"))


def test_registered_worktrees_preserve_non_utf8_unrelated_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legal POSIX path bytes in unrelated registrations remain inspectable."""

    output = (
        b"worktree /tmp/unrelated-\xff\0HEAD "
        + (b"a" * 40)
        + b"\0detached\0\0"
    )
    monkeypatch.setattr(
        worktree_module,
        "_run_git_bytes",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=output,
            stderr=b"",
        ),
    )

    records = worktree_module._registered_worktrees(tmp_path)  # noqa: SLF001

    assert len(records) == 1
    assert os.fsencode(records[0].path).endswith(b"unrelated-\xff")
    assert records[0].head == "a" * 40
    assert records[0].detached is True


def test_manager_rejects_insecure_or_replaced_worktree_root(
    tmp_path: Path,
) -> None:
    """The managed root stays a private, captured directory identity."""

    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    with pytest.raises(ProjectWorktreeError, match="non-symlink"):
        ProjectWorktreeManager.create(linked)

    writable = tmp_path / "writable"
    writable.mkdir(mode=0o700)
    writable.chmod(0o770)
    with pytest.raises(ProjectWorktreeError, match="not group/world writable"):
        ProjectWorktreeManager.create(writable)

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    original = fixture.worktree_root.with_name("original-worktrees")
    fixture.worktree_root.rename(original)
    fixture.worktree_root.mkdir(mode=0o700)
    with pytest.raises(ProjectWorktreeError, match="changed identity"):
        manager.expected_evidence(revision=fixture.revision, queue_item_id=100)


def test_prepare_returns_exact_immutable_evidence_and_is_idempotent(
    tmp_path: Path,
) -> None:
    """Repeated prepare verifies rather than replacing a correct ref/worktree."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=101)
    assert evidence.project_id == fixture.revision.project_id
    assert evidence.project_key == "alpha-project"
    assert evidence.project_revision_id == fixture.revision.id
    assert evidence.project_revision == "alpha-project:r1"
    assert evidence.queue_item_id == 101
    assert evidence.repository == fixture.repository
    assert evidence.git_ref == (
        f"{QUEUE_REF_NAMESPACE}/alpha-project/revisions/11/items/101"
    )
    assert evidence.worktree.parent == fixture.worktree_root
    assert evidence.git_commit == fixture.revision.git_commit
    assert git(fixture.repository, "show-ref", "--verify", "--hash", evidence.git_ref) == (
        evidence.git_commit
    )
    assert git(evidence.worktree, "rev-parse", "HEAD") == evidence.git_commit
    assert git(evidence.worktree, "rev-parse", "--show-toplevel") == str(
        evidence.worktree
    )
    assert git(evidence.worktree, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"

    recorded = ProjectWorktreeEvidence.from_document(evidence.to_document())
    assert recorded == evidence
    assert manager.prepare(
        revision=fixture.revision,
        queue_item_id=101,
        recorded_evidence=recorded,
    ) == evidence
    assert manager.recover(
        revision=fixture.revision,
        queue_item_id=101,
        recorded_evidence=recorded,
    ) == evidence


def test_recovery_is_strictly_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery runs inspection only and never repairs refs, trees, or metadata."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=102)
    calls: list[tuple[str, ...]] = []
    original = worktree_module._run_git

    def recording_run(repository: Path, *arguments: str):  # type: ignore[no-untyped-def]
        calls.append(tuple(arguments))
        return original(repository, *arguments)

    monkeypatch.setattr(worktree_module, "_run_git", recording_run)
    assert manager.recover(
        revision=fixture.revision,
        queue_item_id=102,
        recorded_evidence=evidence,
    ) == evidence
    mutating = {"update-ref", "add", "remove", "prune"}
    assert not any(mutating.intersection(arguments) for arguments in calls)


def test_all_manager_git_calls_use_structured_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No repository, ref, path, or commit ever crosses a shell parser."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    calls: list[tuple[object, dict[str, object]]] = []
    original = subprocess.run

    def recording_run(command: object, **kwargs: object):
        calls.append((command, dict(kwargs)))
        return original(command, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(worktree_module.subprocess, "run", recording_run)
    manager.prepare(revision=fixture.revision, queue_item_id=103)
    assert calls
    for command, kwargs in calls:
        assert type(command) is list
        assert command[0] == "git"  # type: ignore[index]
        assert kwargs.get("shell") is not True
        if kwargs.get("input") is None:
            assert kwargs["stdin"] is subprocess.DEVNULL
        else:
            assert kwargs["stdin"] is None
            assert type(kwargs["input"]) is bytes


def test_prepare_rejects_mutable_clean_smudge_filter_before_execution(
    tmp_path: Path,
) -> None:
    """A committed filter attribute cannot execute mutable repository config."""

    fixture = make_repository(tmp_path / "fixture")
    (fixture.repository / ".gitattributes").write_text(
        "program.py filter=evil\n",
        encoding="utf-8",
    )
    git(fixture.repository, "add", ".gitattributes")
    git(fixture.repository, "commit", "--quiet", "-m", "declare filter")
    commit = git(fixture.repository, "rev-parse", "HEAD")
    enrollment = Enrollment.create(
        project=fixture.project,
        checkout_directory=fixture.repository,
        project_manifest_path="Project.yaml",
        mounts=list(fixture.revision.enrollment.mounts),
        environments=list(fixture.revision.enrollment.environments),
        state_directory=fixture.state_directory,
    )
    revision = ProjectRevision.create(
        revision_id=12,
        project_id=fixture.revision.project_id,
        sequence=2,
        project=fixture.project,
        project_source_path="Project.yaml",
        project_source=project_source(fixture.project),
        git_commit=commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    marker = tmp_path / "filter-executed"
    filter_script = tmp_path / "filter.py"
    filter_script.write_text(
        "from pathlib import Path\n"
        "import sys\n"
        f"Path({str(marker)!r}).write_text('executed')\n"
        "sys.stdout.buffer.write(sys.stdin.buffer.read().replace("
        "b'first revision', b'SMUDGED'))\n",
        encoding="utf-8",
    )
    git(
        fixture.repository,
        "config",
        "filter.evil.smudge",
        shlex.join([sys.executable, str(filter_script)]),
    )
    git(fixture.repository, "config", "filter.evil.clean", "cat")
    git(fixture.repository, "config", "filter.evil.required", "true")

    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.expected_evidence(revision=revision, queue_item_id=901)
    with pytest.raises(ProjectWorktreeError, match="external checkout filter"):
        manager.prepare(revision=revision, queue_item_id=901)
    assert not marker.exists()
    assert not evidence.worktree.exists()
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        evidence.git_ref,
    ) == ""


def test_attribute_change_after_preflight_never_reaches_checkout_conversion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pinned blobs are materialized directly even if mutable attrs race."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    original_filter_check = worktree_module._reject_checkout_filters
    original_run_git = worktree_module._run_git
    checks = 0
    commands: list[tuple[str, ...]] = []

    def changing_filter_check(*args: object, **kwargs: object) -> None:
        nonlocal checks
        original_filter_check(*args, **kwargs)  # type: ignore[arg-type]
        checks += 1
        if checks == 2:
            (fixture.repository / ".git" / "info" / "attributes").write_text(
                "program.py filter=raced\n",
                encoding="utf-8",
            )

    def recording_run(repository: Path, *arguments: str):  # type: ignore[no-untyped-def]
        commands.append(tuple(arguments))
        return original_run_git(repository, *arguments)

    monkeypatch.setattr(
        worktree_module,
        "_reject_checkout_filters",
        changing_filter_check,
    )
    monkeypatch.setattr(worktree_module, "_run_git", recording_run)
    evidence = manager.expected_evidence(
        revision=fixture.revision,
        queue_item_id=902,
    )
    with pytest.raises(ProjectWorktreeError, match="external checkout filter"):
        manager.prepare(revision=fixture.revision, queue_item_id=902)

    assert evidence.worktree.is_dir()
    assert (evidence.worktree / "program.py").read_text(encoding="utf-8") == (
        "print('first revision')\n"
    )
    assert not any(command[:1] == ("checkout-index",) for command in commands)

    (fixture.repository / ".git" / "info" / "attributes").unlink()
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_committed_symbolic_link_materializes_recovers_and_cleans(
    tmp_path: Path,
) -> None:
    """POSIX 0777 symlink metadata does not make exact Git links dirty."""

    fixture = make_repository(tmp_path / "fixture")
    (fixture.repository / "program-link.py").symlink_to("program.py")
    git(fixture.repository, "add", "program-link.py")
    git(fixture.repository, "commit", "--quiet", "-m", "add symbolic link")
    commit = git(fixture.repository, "rev-parse", "HEAD")
    revision = ProjectRevision.create(
        revision_id=12,
        project_id=fixture.revision.project_id,
        sequence=2,
        project=fixture.project,
        project_source_path="Project.yaml",
        project_source=project_source(fixture.project),
        git_commit=commit,
        enrollment=fixture.revision.enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=revision, queue_item_id=903)

    link = evidence.worktree / "program-link.py"
    assert link.is_symlink()
    assert link.readlink() == Path("program.py")
    assert manager.recover(
        revision=revision,
        queue_item_id=903,
        recorded_evidence=evidence,
    ) == evidence
    manager.cleanup(revision=revision, recorded_evidence=evidence)
    assert not evidence.worktree.exists()


def test_new_worktree_modes_are_private_under_permissive_group_umask(
    tmp_path: Path,
) -> None:
    """Git-created metadata is normalized even when the service umask is 0002."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    prior_umask = os.umask(0o002)
    try:
        evidence = manager.prepare(revision=fixture.revision, queue_item_id=904)
    finally:
        os.umask(prior_umask)

    assert (evidence.worktree.stat().st_mode & 0o777) == 0o700
    assert ((evidence.worktree / ".git").stat().st_mode & 0o777) == 0o600
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_repository_is_derived_from_exact_enrollment_git_toplevel(
    tmp_path: Path,
) -> None:
    """A directory merely inside a repository cannot impersonate its checkout."""

    fixture = make_repository(tmp_path / "fixture")
    nested = fixture.repository / "nested"
    nested.mkdir()
    project = fixture.project
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=nested,
        project_manifest_path="Project.yaml",
        mounts=list(fixture.revision.enrollment.mounts),
        environments=list(fixture.revision.enrollment.environments),
        state_directory=fixture.state_directory,
    )
    revision = ProjectRevision.create(
        revision_id=99,
        project_id=fixture.revision.project_id,
        sequence=2,
        project=project,
        project_source_path="Project.yaml",
        project_source=project_source(project),
        git_commit=fixture.revision.git_commit,
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    with pytest.raises(ProjectWorktreeError, match="exact canonical Git top-level"):
        manager.prepare(revision=revision, queue_item_id=104)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("extensions.partialClone", "origin"),
        ("remote.cache.promisor", "true"),
        ("remote.cache.partialCloneFilter", "blob:none"),
    ],
)
def test_worktree_use_rejects_repository_newly_marked_partial_clone(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    """Dispatch never fetches a pinned object through mutable promisor config."""

    fixture = make_repository(tmp_path / "fixture")
    git(fixture.repository, "config", key, value)
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    with pytest.raises(
        ProjectWorktreeError,
        match="currently marked as a partial clone or has promisor configuration",
    ):
        manager.prepare(revision=fixture.revision, queue_item_id=905)
    assert not any(fixture.worktree_root.iterdir())


def test_revision_commit_must_exist_as_exact_commit_in_derived_repository(
    tmp_path: Path,
) -> None:
    """A full-looking object claim is not accepted unless Git verifies it."""

    fixture = make_repository(tmp_path / "fixture")
    revision = ProjectRevision.create(
        revision_id=98,
        project_id=fixture.revision.project_id,
        sequence=2,
        project=fixture.project,
        project_source_path="Project.yaml",
        project_source=project_source(fixture.project),
        git_commit="f" * 40,
        enrollment=fixture.revision.enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    with pytest.raises(ProjectWorktreeError, match="verify revision commit"):
        manager.prepare(revision=revision, queue_item_id=105)


def test_recorded_repository_or_path_substitution_fails_before_mutation(
    tmp_path: Path,
) -> None:
    """Persisted evidence must equal every recomputed revision/item field."""

    fixture = make_repository(tmp_path / "fixture")
    other = make_repository(
        tmp_path / "other",
        key="other-project",
        project_id=2,
        revision_id=22,
    )
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    expected = manager.expected_evidence(
        revision=fixture.revision,
        queue_item_id=106,
    )
    document = expected.to_document()
    document["repository"] = str(other.repository)
    substituted_repository = ProjectWorktreeEvidence.from_document(document)
    with pytest.raises(ProjectWorktreeError, match="fields.*repository"):
        manager.prepare(
            revision=fixture.revision,
            queue_item_id=106,
            recorded_evidence=substituted_repository,
    )
    assert not expected.worktree.exists()
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        QUEUE_REF_NAMESPACE,
    ) == ""

    document = expected.to_document()
    alternate_parent = tmp_path / "alternate"
    alternate_parent.mkdir()
    document["worktree"] = str(alternate_parent / expected.worktree.name)
    substituted_path = ProjectWorktreeEvidence.from_document(document)
    with pytest.raises(ProjectWorktreeError, match="fields.*worktree"):
        manager.prepare(
            revision=fixture.revision,
            queue_item_id=106,
            recorded_evidence=substituted_path,
        )
    assert not expected.worktree.exists()


def test_evidence_parser_rejects_ref_and_name_substitution(tmp_path: Path) -> None:
    """Recorded JSON cannot claim an arbitrary ref namespace or cleanup basename."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    expected = manager.expected_evidence(
        revision=fixture.revision,
        queue_item_id=107,
    )
    document = expected.to_document()
    document["gitRef"] = "refs/heads/main"
    with pytest.raises(ProjectWorktreeError, match="not exact queue-owned ref"):
        ProjectWorktreeEvidence.from_document(document)
    document = expected.to_document()
    document["worktree"] = str(fixture.worktree_root / "other")
    with pytest.raises(ProjectWorktreeError, match="basename"):
        ProjectWorktreeEvidence.from_document(document)


def test_wrong_ref_commit_blocks_recovery_and_cleanup_without_removal(
    tmp_path: Path,
) -> None:
    """A changed queue ref is evidence corruption, never permission to overwrite."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=108)
    later = next_revision(fixture, revision_id=12)
    git(fixture.repository, "update-ref", evidence.git_ref, later.git_commit)
    with pytest.raises(ProjectWorktreeError, match="points to"):
        manager.recover(
            revision=fixture.revision,
            queue_item_id=108,
            recorded_evidence=evidence,
        )
    with pytest.raises(ProjectWorktreeError, match="refused cleanup"):
        manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)
    assert evidence.worktree.is_dir()
    assert fixture.repository.is_dir()
    git(fixture.repository, "update-ref", evidence.git_ref, evidence.git_commit)
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_wrong_worktree_head_blocks_recovery_and_cleanup(tmp_path: Path) -> None:
    """A detached directory at another commit is never treated as queue identity."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=109)
    later = next_revision(fixture, revision_id=12)
    git(evidence.worktree, "reset", "--hard", later.git_commit)
    with pytest.raises(ProjectWorktreeError, match="HEAD is"):
        manager.recover(
            revision=fixture.revision,
            queue_item_id=109,
            recorded_evidence=evidence,
        )
    with pytest.raises(ProjectWorktreeError, match="HEAD is"):
        manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)
    assert evidence.worktree.is_dir()
    git(evidence.worktree, "reset", "--hard", evidence.git_commit)
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_attached_or_dirty_worktree_fails_read_only_recovery(tmp_path: Path) -> None:
    """Recovery requires detached committed code and changes no suspect tree."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=110)
    git(evidence.worktree, "switch", "--quiet", "-c", "unexpected-branch")
    with pytest.raises(ProjectWorktreeError, match="attached to branch"):
        manager.recover(
            revision=fixture.revision,
            queue_item_id=110,
            recorded_evidence=evidence,
        )
    git(evidence.worktree, "checkout", "--quiet", "--detach", evidence.git_commit)
    (evidence.worktree / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ProjectWorktreeError, match="is dirty"):
        manager.recover(
            revision=fixture.revision,
            queue_item_id=110,
            recorded_evidence=evidence,
        )
    assert (evidence.worktree / "untracked.txt").is_file()
    with pytest.raises(ProjectWorktreeError, match="is dirty"):
        manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)
    (evidence.worktree / "untracked.txt").unlink()
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_symlink_worktree_target_is_never_followed_or_cleaned(tmp_path: Path) -> None:
    """A pre-created target symlink cannot redirect prepare into artifacts."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.expected_evidence(
        revision=fixture.revision,
        queue_item_id=111,
    )
    marker = fixture.scratch / "scientific-result.bin"
    marker.write_bytes(b"retain me")
    evidence.worktree.symlink_to(fixture.scratch, target_is_directory=True)
    with pytest.raises(ProjectWorktreeError, match="resolves outside.*identity"):
        manager.prepare(revision=fixture.revision, queue_item_id=111)
    with pytest.raises(ProjectWorktreeError, match="resolves outside.*identity"):
        manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)
    assert evidence.worktree.is_symlink()
    assert marker.read_bytes() == b"retain me"
    assert fixture.repository.is_dir()


def test_plain_directory_at_target_cannot_authorize_ref_creation(
    tmp_path: Path,
) -> None:
    """Prepare authenticates an existing target before mutating repository refs."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.expected_evidence(
        revision=fixture.revision,
        queue_item_id=115,
    )
    evidence.worktree.mkdir()
    marker = evidence.worktree / "unrelated.txt"
    marker.write_text("do not remove\n", encoding="utf-8")
    with pytest.raises(ProjectWorktreeError, match="worktree top-level"):
        manager.prepare(revision=fixture.revision, queue_item_id=115)
    assert marker.read_text(encoding="utf-8") == "do not remove\n"
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        evidence.git_ref,
    ) == ""


def test_cleanup_removes_only_exact_tree_and_ref_and_is_idempotent(
    tmp_path: Path,
) -> None:
    """Dirty output is preserved until an operator makes the tree clean."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=112)
    marker = fixture.scratch / "artifact.bin"
    marker.write_bytes(b"scientific artifact")
    (evidence.worktree / "artifact-link").symlink_to(marker)
    (evidence.worktree / "temporary-output.txt").write_text(
        "queue-owned worktree output\n",
        encoding="utf-8",
    )
    with pytest.raises(ProjectWorktreeError, match="is dirty"):
        manager.cleanup(
            revision=fixture.revision,
            recorded_evidence=evidence,
        )
    assert evidence.worktree.is_dir()
    assert (evidence.worktree / "temporary-output.txt").is_file()
    assert (evidence.worktree / "artifact-link").is_symlink()
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        evidence.git_ref,
    ) == evidence.git_ref
    (evidence.worktree / "temporary-output.txt").unlink()
    (evidence.worktree / "artifact-link").unlink()
    assert manager.cleanup(
        revision=fixture.revision,
        recorded_evidence=evidence,
    ) == evidence
    assert not evidence.worktree.exists()
    assert not evidence.worktree.is_symlink()
    assert marker.read_bytes() == b"scientific artifact"
    assert fixture.repository.is_dir()
    assert (fixture.repository / "Project.yaml").is_file()
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        QUEUE_REF_NAMESPACE,
    ) == ""
    assert manager.cleanup(
        revision=fixture.revision,
        recorded_evidence=evidence,
    ) == evidence


def test_cleanup_refuses_recorded_path_mismatch(tmp_path: Path) -> None:
    """Even a valid queue-shaped alternate path is not a cleanup capability."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=113)
    document = evidence.to_document()
    alternate = tmp_path / "alternate"
    alternate.mkdir()
    document["worktree"] = str(alternate / evidence.worktree.name)
    substituted = ProjectWorktreeEvidence.from_document(document)
    with pytest.raises(ProjectWorktreeError, match="fields.*worktree"):
        manager.cleanup(
            revision=fixture.revision,
            recorded_evidence=substituted,
        )
    assert evidence.worktree.is_dir()
    assert git(fixture.repository, "show-ref", "--verify", evidence.git_ref)
    manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)


def test_cleanup_refuses_broad_prune_when_registered_path_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing files with live Git metadata require exact operator repair."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    evidence = manager.prepare(revision=fixture.revision, queue_item_id=114)
    shutil.rmtree(evidence.worktree)
    calls: list[tuple[str, ...]] = []
    original = worktree_module._run_git

    def recording_run(repository: Path, *arguments: str):  # type: ignore[no-untyped-def]
        calls.append(tuple(arguments))
        return original(repository, *arguments)

    monkeypatch.setattr(worktree_module, "_run_git", recording_run)
    with pytest.raises(ProjectWorktreeError, match="still registers"):
        manager.cleanup(revision=fixture.revision, recorded_evidence=evidence)
    assert not any("prune" in arguments for arguments in calls)
    assert git(fixture.repository, "show-ref", "--verify", evidence.git_ref)
    assert fixture.repository.is_dir()


def test_two_repositories_prepare_recover_and_cleanup_independently(
    tmp_path: Path,
) -> None:
    """One manager isolates repository refs and worktrees by Project identity."""

    shared_state = tmp_path / "state"
    alpha = make_repository(
        tmp_path / "alpha",
        key="alpha-project",
        project_id=1,
        revision_id=11,
        shared_state_directory=shared_state,
    )
    beta = make_repository(
        tmp_path / "beta",
        key="beta-project",
        project_id=2,
        revision_id=22,
        shared_state_directory=shared_state,
    )
    manager = ProjectWorktreeManager.create(shared_state / "worktrees")
    alpha_evidence = manager.prepare(revision=alpha.revision, queue_item_id=201)
    beta_evidence = manager.prepare(revision=beta.revision, queue_item_id=202)
    assert alpha_evidence.repository == alpha.repository
    assert beta_evidence.repository == beta.repository
    assert alpha_evidence.worktree != beta_evidence.worktree
    assert "alpha-project" in alpha_evidence.git_ref
    assert "beta-project" in beta_evidence.git_ref
    assert git(alpha.repository, "show-ref", "--verify", alpha_evidence.git_ref)
    assert git(beta.repository, "show-ref", "--verify", beta_evidence.git_ref)

    manager.cleanup(revision=alpha.revision, recorded_evidence=alpha_evidence)
    assert not alpha_evidence.worktree.exists()
    assert beta_evidence.worktree.is_dir()
    assert manager.recover(
        revision=beta.revision,
        queue_item_id=202,
        recorded_evidence=beta_evidence,
    ) == beta_evidence
    manager.cleanup(revision=beta.revision, recorded_evidence=beta_evidence)
    assert alpha.repository.is_dir()
    assert beta.repository.is_dir()


def test_changed_revision_cannot_recover_or_replace_old_item_identity(
    tmp_path: Path,
) -> None:
    """A later current revision never retargets an existing item worktree."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    old = manager.prepare(revision=fixture.revision, queue_item_id=301)
    changed = next_revision(fixture, revision_id=12, sequence=2)
    with pytest.raises(ProjectWorktreeError, match="fields"):
        manager.recover(
            revision=changed,
            queue_item_id=301,
            recorded_evidence=old,
        )
    with pytest.raises(ProjectWorktreeError, match="fields"):
        manager.prepare(
            revision=changed,
            queue_item_id=301,
            recorded_evidence=old,
        )
    changed_expected = manager.expected_evidence(
        revision=changed,
        queue_item_id=301,
    )
    with pytest.raises(
        ProjectWorktreeError,
        match="already has another revision-qualified identity",
    ):
        manager.prepare(revision=changed, queue_item_id=301)
    assert not changed_expected.worktree.exists()
    assert git(
        fixture.repository,
        "for-each-ref",
        "--format=%(refname)",
        f"{QUEUE_REF_NAMESPACE}/alpha-project/revisions/12/items/301",
    ) == ""
    assert manager.recover(
        revision=fixture.revision,
        queue_item_id=301,
        recorded_evidence=old,
    ) == old
    assert git(old.worktree, "rev-parse", "HEAD") == fixture.revision.git_commit
    manager.cleanup(revision=fixture.revision, recorded_evidence=old)


def test_cleanup_rejects_unvalidated_evidence_type_before_field_access(
    tmp_path: Path,
) -> None:
    """Cleanup's capability boundary never trusts duck-typed evidence."""

    fixture = make_repository(tmp_path / "fixture")
    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    with pytest.raises(TypeError, match="exactly ProjectWorktreeEvidence"):
        manager.cleanup(
            revision=fixture.revision,
            recorded_evidence=object(),  # type: ignore[arg-type]
        )


def test_state_root_overlap_and_changed_symlink_target_fail_closed(
    tmp_path: Path,
) -> None:
    """Manager root identity cannot move into checkout or artifact storage."""

    fixture = make_repository(tmp_path / "fixture")
    overlapping = fixture.repository / "worktrees"
    overlapping.mkdir()
    manager = ProjectWorktreeManager.create(overlapping)
    with pytest.raises(ProjectWorktreeError, match="overlaps revision checkout"):
        manager.expected_evidence(revision=fixture.revision, queue_item_id=401)

    manager = ProjectWorktreeManager.create(fixture.worktree_root)
    original_root = fixture.worktree_root
    moved_root = fixture.state_directory / "moved-worktrees"
    original_root.rename(moved_root)
    original_root.symlink_to(fixture.scratch, target_is_directory=True)
    with pytest.raises(ProjectWorktreeError, match="changed canonical target"):
        manager.expected_evidence(revision=fixture.revision, queue_item_id=402)
    assert fixture.scratch.is_dir()
