"""Prepare and verify project-qualified Git refs and scheduler worktrees.

The public manager derives repository identity only from an immutable
``ProjectRevision``.  It never accepts an arbitrary repository or cleanup path,
never invokes a shell, and never prunes unrelated Git worktree metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
from typing import Any, Final, Mapping, Self, TypeVar, cast

from experiment_queue.identity import validate_project_key
from experiment_queue.path_security import (
    PathBoundaryError,
    SecurePathBoundary,
    capture_secure_path_boundary,
    revalidate_secure_path_boundary,
)
from experiment_queue.project_lifecycle import ProjectRevision
from experiment_queue.serialization import JSONValue


QUEUE_REF_NAMESPACE: Final = "refs/experiment-queue/projects"
_FULL_GIT_OBJECT_PATTERN: Final = re.compile(
    r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z"
)
_REVISION_LABEL_PATTERN: Final = re.compile(
    r"(?P<project>[a-z][a-z0-9]*(?:-[a-z0-9]+)*):r(?P<sequence>[1-9][0-9]*)\Z"
)
_QUEUE_WORKTREE_NAME_PATTERN: Final = re.compile(
    r"(?P<project>[a-z][a-z0-9]*(?:-[a-z0-9]+)*)-"
    r"r(?P<revision>[1-9][0-9]*)-item-(?P<item>[1-9][0-9]*)-"
    r"(?P<commit>[0-9a-f]{12})\Z"
)
_GIT_TIMEOUT_SECONDS: Final = 30
_SIGNED_64_MAX: Final = (2**63) - 1
_GIT_REDIRECTION_VARIABLES: Final = frozenset(
    {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
    }
)


class ProjectWorktreeError(RuntimeError):
    """Raised when a queue-owned Git ref or worktree fails closed."""


class _FactoryOnly:
    """Prevent unvalidated construction of trusted worktree evidence."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is validated-only; use its documented factory"
        )


_FactoryModel = TypeVar("_FactoryModel", bound=_FactoryOnly)


def _construct(model: type[_FactoryModel], **values: object) -> _FactoryModel:
    instance = object.__new__(model)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def _positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0 or value > _SIGNED_64_MAX:
        raise ProjectWorktreeError(
            f"{field_name} must be a positive signed 64-bit integer, got {value!r}"
        )
    return value


def _text(value: object, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ProjectWorktreeError(
            f"{field_name} must be a non-empty string without surrounding "
            f"whitespace, got {value!r}"
        )
    if len(value) > maximum:
        raise ProjectWorktreeError(
            f"{field_name} must be {maximum} characters or fewer"
        )
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ProjectWorktreeError(
            f"{field_name} must contain valid Unicode scalar text"
        ) from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ProjectWorktreeError(
            f"{field_name} must not contain control characters"
        )
    return value


def _project_key(value: object) -> str:
    if type(value) is not str:
        raise ProjectWorktreeError(
            f"project_key must be a string, got {type(value).__name__}"
        )
    try:
        return validate_project_key(value)
    except ValueError as exc:
        raise ProjectWorktreeError(f"invalid project_key: {exc}") from exc


def _full_commit(value: object, *, field_name: str = "git_commit") -> str:
    if type(value) is not str or _FULL_GIT_OBJECT_PATTERN.fullmatch(value) is None:
        raise ProjectWorktreeError(
            f"{field_name} must be a lowercase full 40- or 64-character Git "
            "commit object ID"
        )
    return value


def _absolute_path(value: object, *, field_name: str) -> Path:
    if type(value) is str:
        path = Path(value)
    elif isinstance(value, Path):
        path = Path(value)
    else:
        raise ProjectWorktreeError(
            f"{field_name} must be an absolute path string or pathlib.Path, got "
            f"{type(value).__name__}"
        )
    if not path.is_absolute():
        raise ProjectWorktreeError(
            f"{field_name} must be absolute, got {str(path)!r}"
        )
    _text(str(path), field_name=field_name)
    return path


def _canonical_directory(value: object, *, field_name: str) -> Path:
    path = _absolute_path(value, field_name=field_name)
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ProjectWorktreeError(
            f"{field_name} {str(path)!r} does not resolve to an existing "
            "directory"
        ) from exc
    if not resolved.is_dir():
        raise ProjectWorktreeError(
            f"{field_name} {str(path)!r} resolves to {str(resolved)!r}, which is "
            "not a directory"
        )
    return resolved


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _queue_ref(revision: ProjectRevision, queue_item_id: int) -> str:
    return (
        f"{QUEUE_REF_NAMESPACE}/{revision.project_key}/revisions/"
        f"{revision.id}/items/{queue_item_id}"
    )


def _worktree_name(revision: ProjectRevision, queue_item_id: int) -> str:
    return (
        f"{revision.project_key}-r{revision.id}-item-{queue_item_id}-"
        f"{revision.git_commit[:12]}"
    )


def _git_environment() -> dict[str, str]:
    """Return a Git environment without caller-controlled repository redirects."""

    environment = {
        name: value
        for name, value in os.environ.items()
        if name not in _GIT_REDIRECTION_VARIABLES and not name.startswith("GIT_")
    }
    environment["GIT_TERMINAL_PROMPT"] = "0"
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    environment["GIT_NO_REPLACE_OBJECTS"] = "1"
    environment["GIT_NO_LAZY_FETCH"] = "1"
    environment["GIT_CONFIG_GLOBAL"] = os.devnull
    environment["GIT_CONFIG_SYSTEM"] = os.devnull
    environment["GIT_CONFIG_NOSYSTEM"] = "1"
    environment["GIT_ATTR_NOSYSTEM"] = "1"
    environment["GIT_PROTOCOL_FROM_USER"] = "0"
    environment["GIT_LITERAL_PATHSPECS"] = "1"
    environment["LC_ALL"] = "C"
    return environment


def _git_command(repository: Path, arguments: tuple[str, ...]) -> list[str]:
    """Build the common noninteractive Git argv with safe checkout defaults."""

    return [
        "git",
        "--no-pager",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "core.autocrlf=false",
        "-c",
        "core.eol=lf",
        "-c",
        "core.filemode=true",
        "-c",
        "core.symlinks=true",
        "-c",
        "core.sparseCheckout=false",
        "-c",
        "core.sparseCheckoutCone=false",
        "-C",
        str(repository),
        *arguments,
    ]


def _run_git(
    repository: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Run one structured Git argv without shell parsing or interactive input."""

    command = _git_command(repository, arguments)
    try:
        return subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
            env=_git_environment(),
        )
    except FileNotFoundError as exc:
        raise ProjectWorktreeError(
            "could not execute Git; install 'git' on the queue service PATH"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise ProjectWorktreeError(
            f"Git timed out after {_GIT_TIMEOUT_SECONDS} seconds while running "
            f"structured arguments {list(arguments)!r} in {repository}"
        ) from exc
    except OSError as exc:
        raise ProjectWorktreeError(
            f"could not run Git in {repository}: {exc}"
        ) from exc


def _run_git_bytes(
    repository: Path,
    *arguments: str,
    stdin_bytes: bytes | None = None,
    extra_environment: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run Git with byte-preserving paths and optional private index state."""

    environment = _git_environment()
    if extra_environment is not None:
        environment.update(extra_environment)
    try:
        return subprocess.run(
            _git_command(repository, arguments),
            input=stdin_bytes,
            stdin=subprocess.DEVNULL if stdin_bytes is None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
            env=environment,
        )
    except FileNotFoundError as exc:
        raise ProjectWorktreeError(
            "could not execute Git; install 'git' on the queue service PATH"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise ProjectWorktreeError(
            f"Git timed out after {_GIT_TIMEOUT_SECONDS} seconds while running "
            f"structured arguments {list(arguments)!r} in {repository}"
        ) from exc
    except OSError as exc:
        raise ProjectWorktreeError(
            f"could not run Git in {repository}: {exc}"
        ) from exc


def _git_bytes_detail(result: subprocess.CompletedProcess[bytes]) -> str:
    """Render bounded byte-mode Git diagnostics without losing path bytes."""

    detail = result.stderr.strip() or result.stdout.strip()
    if not detail:
        return f"exit code {result.returncode}"
    return detail[:4096].decode("utf-8", errors="backslashreplace")


def _git_detail(result: subprocess.CompletedProcess[str]) -> str:
    detail = result.stderr.strip() or result.stdout.strip()
    if not detail:
        detail = f"exit code {result.returncode}"
    return detail[:4096]


def _require_git(
    repository: Path,
    operation: str,
    *arguments: str,
) -> str:
    result = _run_git(repository, *arguments)
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not {operation} in repository {repository}: "
            f"{_git_detail(result)}"
        )
    return result.stdout.strip()


def _git_path(repository: Path, value: str, *, field_name: str) -> Path:
    raw = Path(_text(value, field_name=field_name))
    candidate = raw if raw.is_absolute() else repository / raw
    try:
        return candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ProjectWorktreeError(
            f"Git reported {field_name} {str(candidate)!r}, but it does not "
            "resolve to an existing path"
        ) from exc


@dataclass(frozen=True, slots=True, init=False)
class ProjectWorktreeEvidence(_FactoryOnly):
    """Immutable scheduler evidence for one project-qualified worktree."""

    project_id: int
    project_key: str
    project_revision_id: int
    project_revision: str
    project_revision_sequence: int
    queue_item_id: int
    repository: Path
    git_ref: str
    worktree: Path
    git_commit: str

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> Self:
        """Rehydrate recorded evidence for strict manager recovery checks."""

        if cls is not ProjectWorktreeEvidence:
            raise TypeError(
                "ProjectWorktreeEvidence.from_document() constructs exactly "
                "ProjectWorktreeEvidence"
            )
        if type(document) is not dict:
            raise ProjectWorktreeError(
                "ProjectWorktreeEvidence must be a plain JSON object"
            )
        non_text_keys = [key for key in document if type(key) is not str]
        if non_text_keys:
            raise ProjectWorktreeError(
                "ProjectWorktreeEvidence object keys must be strings, got "
                f"{non_text_keys!r}"
            )
        expected_fields = {
            "apiVersion",
            "kind",
            "projectId",
            "projectKey",
            "projectRevisionId",
            "projectRevision",
            "projectRevisionSequence",
            "queueItemId",
            "repository",
            "gitRef",
            "worktree",
            "gitCommit",
        }
        fields = set(document)
        missing = sorted(expected_fields - fields)
        unknown = sorted(fields - expected_fields)
        if missing or unknown:
            details: list[str] = []
            if missing:
                details.append(f"missing fields {missing}")
            if unknown:
                details.append(f"unknown fields {unknown}")
            raise ProjectWorktreeError(
                "ProjectWorktreeEvidence has invalid fields: " + "; ".join(details)
            )
        if (
            document["apiVersion"] != "experiment-queue/v1"
            or document["kind"] != "ProjectWorktreeEvidence"
        ):
            raise ProjectWorktreeError(
                "ProjectWorktreeEvidence requires apiVersion "
                "'experiment-queue/v1' and kind 'ProjectWorktreeEvidence'"
            )

        project_id = _positive_integer(document["projectId"], field_name="projectId")
        key = _project_key(document["projectKey"])
        revision_id = _positive_integer(
            document["projectRevisionId"],
            field_name="projectRevisionId",
        )
        revision_sequence = _positive_integer(
            document["projectRevisionSequence"],
            field_name="projectRevisionSequence",
        )
        revision_label = _text(
            document["projectRevision"],
            field_name="projectRevision",
            maximum=96,
        )
        label_match = _REVISION_LABEL_PATTERN.fullmatch(revision_label)
        if (
            label_match is None
            or label_match.group("project") != key
            or int(label_match.group("sequence")) != revision_sequence
        ):
            raise ProjectWorktreeError(
                f"projectRevision {revision_label!r} must equal "
                f"{key!r}:r{revision_sequence}"
            )
        item_id = _positive_integer(
            document["queueItemId"],
            field_name="queueItemId",
        )
        commit = _full_commit(document["gitCommit"], field_name="gitCommit")
        repository = _absolute_path(
            document["repository"],
            field_name="repository",
        )
        worktree = _absolute_path(
            document["worktree"],
            field_name="worktree",
        )
        git_ref = _text(document["gitRef"], field_name="gitRef", maximum=512)
        expected_ref = (
            f"{QUEUE_REF_NAMESPACE}/{key}/revisions/{revision_id}/items/{item_id}"
        )
        if git_ref != expected_ref:
            raise ProjectWorktreeError(
                f"gitRef {git_ref!r} is not exact queue-owned ref "
                f"{expected_ref!r} for the recorded identity"
            )
        expected_name = f"{key}-r{revision_id}-item-{item_id}-{commit[:12]}"
        if worktree.name != expected_name:
            raise ProjectWorktreeError(
                f"worktree basename {worktree.name!r} is not expected "
                f"queue-owned name {expected_name!r}"
            )
        return cast(
            Self,
            _construct(
                cls,
                project_id=project_id,
                project_key=key,
                project_revision_id=revision_id,
                project_revision=revision_label,
                project_revision_sequence=revision_sequence,
                queue_item_id=item_id,
                repository=repository,
                git_ref=git_ref,
                worktree=worktree,
                git_commit=commit,
            ),
        )

    def to_document(self) -> dict[str, JSONValue]:
        """Return fresh JSON-native evidence suitable for strict persistence."""

        return {
            "apiVersion": "experiment-queue/v1",
            "kind": "ProjectWorktreeEvidence",
            "projectId": self.project_id,
            "projectKey": self.project_key,
            "projectRevisionId": self.project_revision_id,
            "projectRevision": self.project_revision,
            "projectRevisionSequence": self.project_revision_sequence,
            "queueItemId": self.queue_item_id,
            "repository": str(self.repository),
            "gitRef": self.git_ref,
            "worktree": str(self.worktree),
            "gitCommit": self.git_commit,
        }


@dataclass(frozen=True, slots=True)
class _RegisteredWorktree:
    """Read-only identity parsed from ``git worktree list --porcelain -z``."""

    path: Path
    head: str | None
    detached: bool


def _registered_worktrees(repository: Path) -> tuple[_RegisteredWorktree, ...]:
    result = _run_git_bytes(
        repository,
        "worktree",
        "list",
        "--porcelain",
        "-z",
    )
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not list registered worktrees in repository "
            f"{repository}: {_git_bytes_detail(result)}"
        )
    output = result.stdout
    if not output or not output.endswith(b"\0\0"):
        raise ProjectWorktreeError(
            "Git worktree registry returned malformed porcelain output without "
            "a NUL-terminated record"
        )
    records: list[_RegisteredWorktree] = []
    fields: list[bytes] = []

    def finish_record() -> None:
        if not fields:
            return
        path_value: bytes | None = None
        head: str | None = None
        detached = False
        for field in fields:
            if field.startswith(b"worktree "):
                path_value = field.removeprefix(b"worktree ")
            elif field.startswith(b"HEAD "):
                try:
                    head = field.removeprefix(b"HEAD ").decode(
                        "ascii", errors="strict"
                    )
                except UnicodeDecodeError as exc:
                    raise ProjectWorktreeError(
                        "Git worktree registry returned a non-ASCII HEAD object ID"
                    ) from exc
            elif field == b"detached":
                detached = True
        if path_value is None:
            raise ProjectWorktreeError(
                "Git worktree registry returned a record without a worktree path"
            )
        path = Path(os.fsdecode(path_value))
        if not path.is_absolute():
            raise ProjectWorktreeError(
                "Git worktree registry returned non-absolute path bytes "
                f"{path_value!r}"
            )
        records.append(_RegisteredWorktree(path=path, head=head, detached=detached))
        fields.clear()

    for token in output.split(b"\0"):
        if token:
            fields.append(token)
        else:
            finish_record()
    return tuple(records)


def _registered_target(
    repository: Path,
    target: Path,
) -> _RegisteredWorktree | None:
    matches = [record for record in _registered_worktrees(repository) if record.path == target]
    if len(matches) > 1:
        raise ProjectWorktreeError(
            f"Git worktree registry repeats queue target {target}; repair repository "
            "metadata before retrying"
        )
    return None if not matches else matches[0]


def _read_ref(repository: Path, git_ref: str) -> str | None:
    # Apple Git 2.39 reports a missing full name from ``show-ref --verify`` as
    # fatal/128. ``rev-parse --verify --quiet`` has the portable tri-state we
    # need: zero with the exact object ID, one with no output when absent, and a
    # distinct failure for repository corruption.
    result = _run_git(repository, "rev-parse", "--verify", "--quiet", git_ref)
    if (
        result.returncode == 1
        and not result.stdout.strip()
        and not result.stderr.strip()
    ):
        return None
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not inspect queue-owned ref {git_ref!r} in {repository}: "
            f"{_git_detail(result)}"
        )
    value = result.stdout.strip()
    return _full_commit(value, field_name=f"Git ref {git_ref}")


def _reject_other_item_identity(
    evidence: ProjectWorktreeEvidence,
    *,
    state_worktree_root: Path,
) -> None:
    """Reject stale or concurrent identities for the same global queue item.

    Revision-qualified names make immutable identity inspectable, but the queue
    item ID remains globally unique.  Preparing a second revision for that item
    would leave two plausible execution locations.  Inspect both queue refs in
    the derived repository and queue-shaped direct children of the shared state
    root, without following or removing either one.
    """

    refs_text = _require_git(
        evidence.repository,
        "inspect project-qualified queue refs for item identity collisions",
        "for-each-ref",
        "--format=%(refname)",
        QUEUE_REF_NAMESPACE,
    )
    conflicting_refs: list[str] = []
    for git_ref in refs_text.splitlines():
        parts = git_ref.split("/")
        if (
            len(parts) == 8
            and parts[:3] == ["refs", "experiment-queue", "projects"]
            and parts[4] == "revisions"
            and parts[6] == "items"
            and parts[7] == str(evidence.queue_item_id)
            and git_ref != evidence.git_ref
        ):
            conflicting_refs.append(git_ref)

    conflicting_paths: list[Path] = []
    try:
        children = tuple(state_worktree_root.iterdir())
    except OSError as exc:
        raise ProjectWorktreeError(
            f"could not inspect state_worktree_root {state_worktree_root} for "
            f"queue item {evidence.queue_item_id} identity collisions: {exc}"
        ) from exc
    for child in children:
        match = _QUEUE_WORKTREE_NAME_PATTERN.fullmatch(child.name)
        if (
            match is not None
            and int(match.group("item")) == evidence.queue_item_id
            and child != evidence.worktree
        ):
            conflicting_paths.append(child)

    if conflicting_refs or conflicting_paths:
        details: list[str] = []
        if conflicting_refs:
            details.append(f"refs {sorted(conflicting_refs)!r}")
        if conflicting_paths:
            details.append(
                f"worktree paths {sorted(str(path) for path in conflicting_paths)!r}"
            )
        raise ProjectWorktreeError(
            f"global queue item {evidence.queue_item_id} already has another "
            f"revision-qualified identity ({'; '.join(details)}); refuse to "
            "prepare two plausible execution locations"
        )


def _ensure_ref(evidence: ProjectWorktreeEvidence) -> None:
    current = _read_ref(evidence.repository, evidence.git_ref)
    if current is not None:
        if current != evidence.git_commit:
            raise ProjectWorktreeError(
                f"queue-owned ref {evidence.git_ref!r} points to {current}, not "
                f"revision commit {evidence.git_commit}; refuse to overwrite it"
            )
        return
    zero = "0" * len(evidence.git_commit)
    result = _run_git(
        evidence.repository,
        "update-ref",
        evidence.git_ref,
        evidence.git_commit,
        zero,
    )
    if result.returncode == 0:
        return
    # A concurrent idempotent prepare may have created the same exact ref.
    current = _read_ref(evidence.repository, evidence.git_ref)
    if current == evidence.git_commit:
        return
    raise ProjectWorktreeError(
        f"Git could not create queue-owned ref {evidence.git_ref!r} for commit "
        f"{evidence.git_commit}: {_git_detail(result)}"
    )


def _delete_ref(evidence: ProjectWorktreeEvidence) -> None:
    current = _read_ref(evidence.repository, evidence.git_ref)
    if current is None:
        return
    if current != evidence.git_commit:
        raise ProjectWorktreeError(
            f"refused cleanup because {evidence.git_ref!r} points to {current}, not "
            f"the proven queue commit {evidence.git_commit}"
        )
    result = _run_git(
        evidence.repository,
        "update-ref",
        "-d",
        evidence.git_ref,
        evidence.git_commit,
    )
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not delete exact queue-owned ref {evidence.git_ref!r}: "
            f"{_git_detail(result)}"
        )


def _repository_identity(revision: ProjectRevision) -> tuple[Path, Path]:
    """Derive and authenticate repository/common-dir identity from Enrollment."""

    revision.enrollment.validate_current_paths()
    repository = revision.enrollment.checkout_directory
    current = _canonical_directory(
        repository,
        field_name=f"revision {revision.label!r} checkout",
    )
    if current != repository:
        raise ProjectWorktreeError(
            f"revision checkout changed canonical target from {repository} to "
            f"{current}; create a new ProjectRevision after revalidation"
        )
    top_text = _require_git(
        repository,
        "resolve exact repository top-level",
        "rev-parse",
        "--show-toplevel",
    )
    top = _git_path(repository, top_text, field_name="repository top-level")
    if top != repository:
        raise ProjectWorktreeError(
            f"revision checkout {repository} is inside Git repository {top}, but "
            "Enrollment must name the exact canonical Git top-level"
        )
    partial_clone = _run_git(
        repository,
        "config",
        "--local",
        "--null",
        "--name-only",
        "--get-regexp",
        r"^(extensions\.partialclone|remote\..*\."
        r"(promisor|partialclonefilter))$",
    )
    if partial_clone.returncode == 0:
        if not partial_clone.stdout.endswith("\0"):
            raise ProjectWorktreeError(
                f"Git returned malformed partial-clone/promisor marker output in "
                f"{repository}"
            )
        markers = sorted(
            value for value in partial_clone.stdout[:-1].split("\0") if value
        )
        if not markers:
            raise ProjectWorktreeError(
                f"Git reported partial-clone/promisor markers without names in "
                f"{repository}"
            )
        raise ProjectWorktreeError(
            f"revision repository {repository} is currently marked as a partial "
            f"clone or has promisor configuration {markers}; "
            "queue worktree use is offline, so materialize a complete clone and "
            "create a new ProjectRevision"
        )
    if partial_clone.returncode != 1:
        raise ProjectWorktreeError(
            f"Git could not inspect partial-clone/promisor state in {repository}: "
            f"{_git_detail(partial_clone)}"
        )
    common_text = _require_git(
        repository,
        "resolve repository common Git directory",
        "rev-parse",
        "--git-common-dir",
    )
    common = _git_path(repository, common_text, field_name="Git common directory")
    resolved_commit = _require_git(
        repository,
        f"verify revision commit {revision.git_commit}",
        "rev-parse",
        "--verify",
        f"{revision.git_commit}^{{commit}}",
    )
    if resolved_commit != revision.git_commit:
        raise ProjectWorktreeError(
            f"revision Git identity {revision.git_commit} resolves to "
            f"{resolved_commit!r} in repository {repository}; require the exact full "
            "commit object"
        )
    return repository, common


@dataclass(frozen=True, slots=True)
class _TreeEntry:
    """One exact leaf from the pinned commit tree."""

    mode: str
    object_id: str
    path: bytes


def _display_git_path(path: bytes) -> str:
    """Render an arbitrary Git path without assuming filesystem UTF-8."""

    return path.decode("utf-8", errors="backslashreplace")


def _pinned_tree_entries(
    repository: Path,
    commit: str,
) -> tuple[_TreeEntry, ...]:
    """Read exact leaf identities without checkout or attribute conversion."""

    result = _run_git_bytes(
        repository,
        "ls-tree",
        "-r",
        "-z",
        "--full-tree",
        commit,
    )
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not enumerate pinned tree {commit} in {repository}: "
            f"{_git_bytes_detail(result)}"
        )
    records = result.stdout.split(b"\0")
    if not records or records[-1] != b"":
        raise ProjectWorktreeError(
            f"Git returned an unterminated tree listing for pinned commit {commit}"
        )
    entries: list[_TreeEntry] = []
    seen: set[bytes] = set()
    for raw_record in records[:-1]:
        try:
            raw_header, raw_path = raw_record.split(b"\t", 1)
            raw_mode, raw_type, raw_object_id = raw_header.split(b" ", 2)
            mode = raw_mode.decode("ascii", errors="strict")
            object_type = raw_type.decode("ascii", errors="strict")
            object_id = raw_object_id.decode("ascii", errors="strict")
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProjectWorktreeError(
                f"Git returned a malformed leaf record for pinned commit {commit}"
            ) from exc
        if not raw_path or raw_path in seen:
            raise ProjectWorktreeError(
                f"pinned commit {commit} contains an empty or duplicate tree path"
            )
        components = raw_path.split(b"/")
        if any(
            not component
            or component in {b".", b".."}
            or component.lower() == b".git"
            for component in components
        ):
            raise ProjectWorktreeError(
                f"pinned commit {commit} contains unsafe worktree path "
                f"{_display_git_path(raw_path)!r}"
            )
        if object_type == "commit" or mode == "160000":
            raise ProjectWorktreeError(
                f"pinned commit {commit} contains unsupported Git submodule "
                f"{_display_git_path(raw_path)!r}; version 1 materializes only "
                "exact regular files and symbolic links"
            )
        if object_type != "blob" or mode not in {"100644", "100755", "120000"}:
            raise ProjectWorktreeError(
                f"pinned commit {commit} contains unsupported mode/type "
                f"{mode!r}/{object_type!r} at "
                f"{_display_git_path(raw_path)!r}"
            )
        if _FULL_GIT_OBJECT_PATTERN.fullmatch(object_id) is None:
            raise ProjectWorktreeError(
                f"pinned commit {commit} returned invalid blob object id "
                f"{object_id!r} at {_display_git_path(raw_path)!r}"
            )
        seen.add(raw_path)
        entries.append(
            _TreeEntry(mode=mode, object_id=object_id, path=raw_path)
        )
    entries.sort(key=lambda entry: entry.path)
    leaf_paths = {entry.path for entry in entries}
    for entry in entries:
        components = entry.path.split(b"/")
        for index in range(1, len(components)):
            parent = b"/".join(components[:index])
            if parent in leaf_paths:
                raise ProjectWorktreeError(
                    f"pinned commit {commit} uses leaf path "
                    f"{_display_git_path(parent)!r} as a directory"
                )
    return tuple(entries)


def _reject_checkout_filters(
    repository: Path,
    commit: str,
    entries: tuple[_TreeEntry, ...],
    *,
    scratch_directory: Path,
) -> None:
    """Reject effective external filters before any Git checkout operation.

    Git's clean/smudge/process drivers come from mutable repository config and
    can both execute commands and make transformed bytes appear clean.  A
    private temporary index lets ``check-attr --cached`` evaluate the pinned
    tree plus repository-local ``info/attributes`` without materializing or
    filtering a file first.
    """

    descriptor, index_name = tempfile.mkstemp(
        prefix=".experiment-queue-index-",
        dir=scratch_directory,
    )
    os.close(descriptor)
    index_path = Path(index_name)
    try:
        index_path.unlink()
        environment = {"GIT_INDEX_FILE": str(index_path)}
        populated = _run_git_bytes(
            repository,
            "read-tree",
            commit,
            extra_environment=environment,
        )
        if populated.returncode != 0:
            raise ProjectWorktreeError(
                f"Git could not create a private filter-inspection index for "
                f"pinned commit {commit}: {_git_bytes_detail(populated)}"
            )
        paths = b"".join(entry.path + b"\0" for entry in entries)
        checked = _run_git_bytes(
            repository,
            "check-attr",
            "--cached",
            "-z",
            "--stdin",
            "filter",
            stdin_bytes=paths,
            extra_environment=environment,
        )
        if checked.returncode != 0:
            raise ProjectWorktreeError(
                f"Git could not inspect checkout filters for pinned commit "
                f"{commit}: {_git_bytes_detail(checked)}"
            )
        tokens = checked.stdout.split(b"\0")
        if not tokens or tokens[-1] != b"" or (len(tokens) - 1) % 3 != 0:
            raise ProjectWorktreeError(
                f"Git returned malformed checkout-filter evidence for pinned "
                f"commit {commit}"
            )
        expected_paths = {entry.path for entry in entries}
        observed_paths: set[bytes] = set()
        for offset in range(0, len(tokens) - 1, 3):
            path, attribute, value = tokens[offset : offset + 3]
            if attribute != b"filter" or path not in expected_paths:
                raise ProjectWorktreeError(
                    f"Git returned mismatched checkout-filter evidence for pinned "
                    f"commit {commit}"
                )
            observed_paths.add(path)
            if value not in {b"unspecified", b"unset"}:
                rendered_value = value.decode("utf-8", errors="backslashreplace")
                raise ProjectWorktreeError(
                    f"pinned commit {commit} assigns external checkout filter "
                    f"{rendered_value!r} to {_display_git_path(path)!r}; version 1 "
                    "refuses clean/smudge/process filters because mutable Git "
                    "configuration could execute or transform unadmitted code"
                )
        if observed_paths != expected_paths:
            raise ProjectWorktreeError(
                f"Git omitted checkout-filter evidence for paths in pinned commit "
                f"{commit}"
            )
    finally:
        try:
            index_path.unlink(missing_ok=True)
        except OSError as exc:
            raise ProjectWorktreeError(
                f"could not remove private filter-inspection index {index_path}: "
                f"{exc}"
            ) from exc


def _stable_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Return fields that must not change during an authenticated file read."""

    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_private_entry(
    value: os.stat_result,
    *,
    label: str,
    allow_multiple_links: bool,
    enforce_write_bits: bool = True,
) -> None:
    """Require queue-owned, non-writable materialization metadata."""

    mode = stat.S_IMODE(value.st_mode)
    if value.st_uid != os.geteuid() or (enforce_write_bits and mode & 0o022):
        raise ProjectWorktreeError(
            f"queue worktree is dirty or unsafe: {label} must be owned by uid "
            f"{os.geteuid()} and not group/world writable; got uid "
            f"{value.st_uid} mode {mode:04o}"
        )
    if not allow_multiple_links and value.st_nlink != 1:
        raise ProjectWorktreeError(
            f"queue worktree is dirty or unsafe: {label} must have exactly one "
            f"filesystem link, got {value.st_nlink}"
        )


def _scan_materialized_tree(
    target: Path,
) -> tuple[set[bytes], dict[bytes, os.stat_result]]:
    """Enumerate the worktree without following a substituted directory link."""

    directory_paths: set[bytes] = set()
    leaf_paths: dict[bytes, os.stat_result] = {}
    root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    root_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    root_descriptor = os.open(target, root_flags)

    def visit(directory_descriptor: int, prefix: bytes) -> None:
        with os.scandir(directory_descriptor) as iterator:
            names = sorted(os.fsencode(entry.name) for entry in iterator)
        for name in names:
            relative = name if not prefix else prefix + b"/" + name
            if not prefix and name == b".git":
                continue
            try:
                entry_stat = os.stat(
                    name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise ProjectWorktreeError(
                    f"queue worktree is dirty or unstable at "
                    f"{_display_git_path(relative)!r}: {exc}"
                ) from exc
            if stat.S_ISDIR(entry_stat.st_mode):
                _require_private_entry(
                    entry_stat,
                    label=f"directory {_display_git_path(relative)!r}",
                    allow_multiple_links=True,
                )
                child_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                child_flags |= getattr(os, "O_NOFOLLOW", 0)
                child_flags |= getattr(os, "O_CLOEXEC", 0)
                try:
                    child = os.open(name, child_flags, dir_fd=directory_descriptor)
                except OSError as exc:
                    raise ProjectWorktreeError(
                        f"queue worktree directory "
                        f"{_display_git_path(relative)!r} changed during scan: {exc}"
                    ) from exc
                try:
                    directory_paths.add(relative)
                    visit(child, relative)
                finally:
                    os.close(child)
            else:
                if relative in leaf_paths:
                    raise ProjectWorktreeError(
                        f"queue worktree repeats materialized path "
                        f"{_display_git_path(relative)!r}"
                    )
                leaf_paths[relative] = entry_stat

    try:
        visit(root_descriptor, b"")
    finally:
        os.close(root_descriptor)
    return directory_paths, leaf_paths


def _open_parent_directory(root_descriptor: int, path: bytes) -> tuple[int, bytes]:
    """Open every parent component with ``O_NOFOLLOW`` beneath the worktree."""

    components = path.split(b"/")
    current = os.dup(root_descriptor)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        for component in components[:-1]:
            child = os.open(component, flags, dir_fd=current)
            os.close(current)
            current = child
        return current, components[-1]
    except BaseException:
        os.close(current)
        raise


def _git_blob_hasher(source_size: int, *, width: int) -> Any:
    """Initialize a streaming digest in the repository's Git object format."""

    if width == 40:
        digest = hashlib.sha1(usedforsecurity=False)
    elif width == 64:
        digest = hashlib.sha256()
    else:  # pragma: no cover - tree parser owns the closed object-id widths
        raise ProjectWorktreeError(f"unsupported Git object-id width {width}")
    digest.update(f"blob {source_size}\0".encode("ascii"))
    return digest


def _git_blob_size(repository: Path, object_id: str) -> int:
    """Read one pinned blob size before bounded or streaming materialization."""

    result = _run_git_bytes(repository, "cat-file", "-s", object_id)
    if result.returncode != 0:
        raise ProjectWorktreeError(
            f"Git could not read size for pinned blob {object_id}: "
            f"{_git_bytes_detail(result)}"
        )
    try:
        source = result.stdout.strip()
        if not source or b"\n" in source:
            raise ValueError
        size = int(source.decode("ascii", errors="strict"), 10)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProjectWorktreeError(
            f"Git returned invalid size {result.stdout[:128]!r} for pinned blob "
            f"{object_id}"
        ) from exc
    if size < 0:
        raise ProjectWorktreeError(
            f"Git returned negative size {size} for pinned blob {object_id}"
        )
    return size


def _ensure_materialized_directory(root_descriptor: int, path: bytes) -> None:
    """Create one expected directory without following a substituted symlink."""

    current = os.dup(root_descriptor)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        prefix: list[bytes] = []
        for component in path.split(b"/"):
            prefix.append(component)
            try:
                os.mkdir(component, 0o700, dir_fd=current)
            except FileExistsError:
                pass
            child = os.open(component, flags, dir_fd=current)
            try:
                details = os.fstat(child)
                _require_private_entry(
                    details,
                    label=f"directory {_display_git_path(b'/'.join(prefix))!r}",
                    allow_multiple_links=True,
                )
            except BaseException:
                os.close(child)
                raise
            os.close(current)
            current = child
    except BaseException:
        os.close(current)
        raise
    os.close(current)


def _stream_git_blob(
    repository: Path,
    *,
    object_id: str,
    descriptor: int,
) -> None:
    """Stream exact object bytes to an already-created regular file."""

    try:
        process = subprocess.Popen(
            _git_command(repository, ("cat-file", "blob", object_id)),
            stdin=subprocess.DEVNULL,
            stdout=descriptor,
            stderr=subprocess.PIPE,
            shell=False,
            env=_git_environment(),
        )
    except (OSError, ValueError) as exc:
        raise ProjectWorktreeError(
            f"could not start Git while materializing pinned blob {object_id}: {exc}"
        ) from exc
    try:
        _stdout, stderr = process.communicate(timeout=_GIT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.communicate()
        raise ProjectWorktreeError(
            f"Git timed out after {_GIT_TIMEOUT_SECONDS} seconds while "
            f"materializing pinned blob {object_id}"
        ) from exc
    if process.returncode != 0:
        detail = (stderr or b"").strip()[:4096].decode(
            "utf-8", errors="backslashreplace"
        )
        raise ProjectWorktreeError(
            f"Git could not materialize pinned blob {object_id}: "
            f"{detail or f'exit code {process.returncode}'}"
        )


def _materialize_pinned_tree(
    evidence: ProjectWorktreeEvidence,
    entries: tuple[_TreeEntry, ...],
) -> None:
    """Write exact Git objects without invoking checkout attribute conversion."""

    expected_directories = sorted(
        {
            b"/".join(components[:index])
            for entry in entries
            for components in (entry.path.split(b"/"),)
            for index in range(1, len(components))
        },
        key=lambda path: (path.count(b"/"), path),
    )
    root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    root_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    root_descriptor = os.open(evidence.worktree, root_flags)
    try:
        for directory in expected_directories:
            _ensure_materialized_directory(root_descriptor, directory)
        for entry in entries:
            size = _git_blob_size(evidence.repository, entry.object_id)
            parent, name = _open_parent_directory(root_descriptor, entry.path)
            try:
                if entry.mode == "120000":
                    if size > 4096:
                        raise ProjectWorktreeError(
                            f"committed symbolic-link target at "
                            f"{_display_git_path(entry.path)!r} is {size} bytes; "
                            "version 1 supports targets of at most 4096 bytes"
                        )
                    result = _run_git_bytes(
                        evidence.repository,
                        "cat-file",
                        "blob",
                        entry.object_id,
                    )
                    if result.returncode != 0:
                        raise ProjectWorktreeError(
                            f"Git could not read pinned symbolic-link blob "
                            f"{entry.object_id}: {_git_bytes_detail(result)}"
                        )
                    if len(result.stdout) != size or b"\0" in result.stdout:
                        raise ProjectWorktreeError(
                            f"Git returned invalid symbolic-link bytes for "
                            f"{_display_git_path(entry.path)!r}"
                        )
                    os.symlink(result.stdout, name, dir_fd=parent)
                else:
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(
                        os, "O_CLOEXEC", 0
                    )
                    descriptor = os.open(name, flags, 0o600, dir_fd=parent)
                    try:
                        _stream_git_blob(
                            evidence.repository,
                            object_id=entry.object_id,
                            descriptor=descriptor,
                        )
                        os.fchmod(descriptor, 0o755 if entry.mode == "100755" else 0o644)
                        if os.fstat(descriptor).st_size != size:
                            raise ProjectWorktreeError(
                                f"materialized size for "
                                f"{_display_git_path(entry.path)!r} differs from "
                                f"pinned blob {entry.object_id}"
                            )
                    finally:
                        os.close(descriptor)
            except OSError as exc:
                raise ProjectWorktreeError(
                    f"could not materialize pinned worktree path "
                    f"{_display_git_path(entry.path)!r}: {exc}"
                ) from exc
            finally:
                os.close(parent)
    finally:
        os.close(root_descriptor)


def _normalize_new_worktree_metadata(evidence: ProjectWorktreeEvidence) -> None:
    """Tighten Git-created root/metadata modes independently of service umask."""

    root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    root_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        root_descriptor = os.open(evidence.worktree, root_flags)
    except OSError as exc:
        raise ProjectWorktreeError(
            f"new queue worktree root {evidence.worktree} cannot be opened safely: "
            f"{exc}"
        ) from exc
    try:
        root_stat = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_uid != os.geteuid():
            raise ProjectWorktreeError(
                f"new queue worktree root {evidence.worktree} must be a real "
                f"directory owned by uid {os.geteuid()} before mode normalization"
            )
        os.fchmod(root_descriptor, 0o700)
        metadata_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        metadata_flags |= getattr(os, "O_CLOEXEC", 0)
        metadata_descriptor = os.open(
            b".git",
            metadata_flags,
            dir_fd=root_descriptor,
        )
        try:
            metadata_stat = os.fstat(metadata_descriptor)
            if (
                not stat.S_ISREG(metadata_stat.st_mode)
                or metadata_stat.st_uid != os.geteuid()
                or metadata_stat.st_nlink != 1
                or not 1 <= metadata_stat.st_size <= 8192
            ):
                raise ProjectWorktreeError(
                    f"new linked-worktree metadata {evidence.worktree / '.git'} "
                    "must be one bounded owner-controlled regular file"
                )
            os.fchmod(metadata_descriptor, 0o600)
        finally:
            os.close(metadata_descriptor)
    except OSError as exc:
        raise ProjectWorktreeError(
            f"could not normalize private metadata for new queue worktree "
            f"{evidence.worktree}: {exc}"
        ) from exc
    finally:
        os.close(root_descriptor)


def _verify_materialized_tree(
    evidence: ProjectWorktreeEvidence,
    entries: tuple[_TreeEntry, ...],
) -> None:
    """Compare every path, mode, and byte to the pinned Git tree without filters."""

    target_stat = os.stat(evidence.worktree, follow_symlinks=False)
    if not stat.S_ISDIR(target_stat.st_mode):
        raise ProjectWorktreeError(
            f"queue worktree is dirty or unsafe: {evidence.worktree} is not a "
            "real directory"
        )
    _require_private_entry(
        target_stat,
        label=f"worktree root {evidence.worktree}",
        allow_multiple_links=True,
    )
    try:
        git_entry = os.stat(
            evidence.worktree / ".git",
            follow_symlinks=False,
        )
    except OSError as exc:
        raise ProjectWorktreeError(
            f"queue worktree is dirty or unsafe: linked-worktree metadata file "
            f"{evidence.worktree / '.git'} is unavailable: {exc}"
        ) from exc
    if not stat.S_ISREG(git_entry.st_mode) or not 1 <= git_entry.st_size <= 8192:
        raise ProjectWorktreeError(
            f"queue worktree is dirty or unsafe: {evidence.worktree / '.git'} "
            "must be one bounded regular linked-worktree metadata file"
        )
    _require_private_entry(
        git_entry,
        label=f"linked-worktree metadata {evidence.worktree / '.git'}",
        allow_multiple_links=False,
    )

    expected_leaves = {entry.path: entry for entry in entries}
    expected_directories = {
        b"/".join(components[:index])
        for entry in entries
        for components in (entry.path.split(b"/"),)
        for index in range(1, len(components))
    }
    actual_directories, actual_leaves = _scan_materialized_tree(evidence.worktree)
    missing = sorted(set(expected_leaves) - set(actual_leaves))
    extra = sorted(set(actual_leaves) - set(expected_leaves))
    missing_directories = sorted(expected_directories - actual_directories)
    extra_directories = sorted(actual_directories - expected_directories)
    if missing or extra or missing_directories or extra_directories:
        details: list[str] = []
        if missing:
            details.append(
                "missing files "
                + repr([_display_git_path(path) for path in missing[:8]])
            )
        if extra:
            details.append(
                "untracked files "
                + repr([_display_git_path(path) for path in extra[:8]])
            )
        if missing_directories:
            details.append(
                "missing directories "
                + repr(
                    [_display_git_path(path) for path in missing_directories[:8]]
                )
            )
        if extra_directories:
            details.append(
                "untracked directories "
                + repr([_display_git_path(path) for path in extra_directories[:8]])
            )
        raise ProjectWorktreeError(
            f"queue worktree {evidence.worktree} is dirty or differs from pinned "
            f"Git tree: {'; '.join(details)}"
        )

    root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    root_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    root_descriptor = os.open(evidence.worktree, root_flags)
    try:
        for path, entry in expected_leaves.items():
            recorded_stat = actual_leaves[path]
            parent, name = _open_parent_directory(root_descriptor, path)
            try:
                if entry.mode == "120000":
                    if not stat.S_ISLNK(recorded_stat.st_mode):
                        raise ProjectWorktreeError(
                            f"queue worktree path {_display_git_path(path)!r} is "
                            "dirty: expected an exact committed symbolic link"
                        )
                    _require_private_entry(
                        recorded_stat,
                        label=f"symbolic link {_display_git_path(path)!r}",
                        allow_multiple_links=False,
                        # POSIX/Linux reports symlink mode 0777; those bits do
                        # not authorize writes through the link itself.
                        enforce_write_bits=False,
                    )
                    target = os.readlink(name, dir_fd=parent)
                    source = os.fsencode(target)
                    after = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if _stable_stat_identity(recorded_stat) != _stable_stat_identity(after):
                        raise ProjectWorktreeError(
                            f"queue worktree symbolic link "
                            f"{_display_git_path(path)!r} changed while verified"
                        )
                    digest = _git_blob_hasher(
                        len(source), width=len(entry.object_id)
                    )
                    digest.update(source)
                    actual_object_id = digest.hexdigest()
                else:
                    if not stat.S_ISREG(recorded_stat.st_mode):
                        raise ProjectWorktreeError(
                            f"queue worktree path {_display_git_path(path)!r} is "
                            "dirty: expected an exact committed regular file"
                        )
                    _require_private_entry(
                        recorded_stat,
                        label=f"file {_display_git_path(path)!r}",
                        allow_multiple_links=False,
                    )
                    executable = bool(stat.S_IMODE(recorded_stat.st_mode) & 0o111)
                    if executable != (entry.mode == "100755"):
                        raise ProjectWorktreeError(
                            f"queue worktree file {_display_git_path(path)!r} has "
                            f"executable={executable}, expected mode {entry.mode}"
                        )
                    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                    flags |= getattr(os, "O_NONBLOCK", 0)
                    flags |= getattr(os, "O_CLOEXEC", 0)
                    descriptor = os.open(name, flags, dir_fd=parent)
                    try:
                        opened_before = os.fstat(descriptor)
                        if not stat.S_ISREG(opened_before.st_mode):
                            raise ProjectWorktreeError(
                                f"queue worktree path {_display_git_path(path)!r} "
                                "changed from a regular file while opened"
                            )
                        digest = _git_blob_hasher(
                            opened_before.st_size,
                            width=len(entry.object_id),
                        )
                        remaining = opened_before.st_size
                        while remaining:
                            chunk = os.read(descriptor, min(1024 * 1024, remaining))
                            if not chunk:
                                break
                            digest.update(chunk)
                            remaining -= len(chunk)
                        opened_after = os.fstat(descriptor)
                    finally:
                        os.close(descriptor)
                    path_after = os.stat(
                        name,
                        dir_fd=parent,
                        follow_symlinks=False,
                    )
                    identities = {
                        _stable_stat_identity(recorded_stat),
                        _stable_stat_identity(opened_before),
                        _stable_stat_identity(opened_after),
                        _stable_stat_identity(path_after),
                    }
                    if len(identities) != 1 or remaining != 0:
                        raise ProjectWorktreeError(
                            f"queue worktree file {_display_git_path(path)!r} "
                            "changed or was truncated while verified"
                        )
                    actual_object_id = digest.hexdigest()
            except OSError as exc:
                raise ProjectWorktreeError(
                    f"queue worktree path {_display_git_path(path)!r} could not be "
                    f"verified without following links: {exc}"
                ) from exc
            finally:
                os.close(parent)
            if actual_object_id != entry.object_id:
                raise ProjectWorktreeError(
                    f"queue worktree file {_display_git_path(path)!r} is dirty: "
                    f"materialized Git blob {actual_object_id} differs from pinned "
                    f"blob {entry.object_id}"
                )
    finally:
        os.close(root_descriptor)


def _verify_worktree(
    evidence: ProjectWorktreeEvidence,
    *,
    repository_common_directory: Path,
    require_clean: bool,
) -> None:
    target = evidence.worktree
    if target.is_symlink():
        raise ProjectWorktreeError(
            f"queue worktree target {target} is a symlink; refuse redirected access"
        )
    if not target.exists():
        raise ProjectWorktreeError(
            f"queue worktree {target} is missing; prepare it before recovery"
        )
    if not target.is_dir():
        raise ProjectWorktreeError(
            f"queue worktree target {target} is not a directory"
        )
    try:
        canonical_target = target.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ProjectWorktreeError(
            f"queue worktree target {target} cannot be resolved safely"
        ) from exc
    if canonical_target != target:
        raise ProjectWorktreeError(
            f"queue worktree target {target} changed canonical identity to "
            f"{canonical_target}"
        )

    top_text = _require_git(
        target,
        "resolve worktree top-level",
        "rev-parse",
        "--show-toplevel",
    )
    top = _git_path(target, top_text, field_name="worktree top-level")
    if top != target:
        raise ProjectWorktreeError(
            f"expected queue worktree {target}, but Git reports top-level {top}"
        )
    common_text = _require_git(
        target,
        "resolve worktree common Git directory",
        "rev-parse",
        "--git-common-dir",
    )
    common = _git_path(target, common_text, field_name="worktree common Git directory")
    if common != repository_common_directory:
        raise ProjectWorktreeError(
            f"worktree {target} belongs to Git common directory {common}, not "
            f"revision repository common directory {repository_common_directory}"
        )
    head = _require_git(target, "read worktree HEAD", "rev-parse", "HEAD")
    if head != evidence.git_commit:
        raise ProjectWorktreeError(
            f"worktree {target} HEAD is {head!r}, not immutable revision commit "
            f"{evidence.git_commit}"
        )
    symbolic = _run_git(target, "symbolic-ref", "-q", "HEAD")
    if symbolic.returncode == 0:
        raise ProjectWorktreeError(
            f"worktree {target} is attached to branch {symbolic.stdout.strip()!r}; "
            "queue worktrees must remain detached"
        )
    if symbolic.returncode != 1:
        raise ProjectWorktreeError(
            f"Git could not verify detached HEAD in {target}: {_git_detail(symbolic)}"
        )
    registered = _registered_target(evidence.repository, target)
    if registered is None:
        raise ProjectWorktreeError(
            f"worktree {target} is not registered by revision repository "
            f"{evidence.repository}"
        )
    if registered.head != evidence.git_commit or not registered.detached:
        raise ProjectWorktreeError(
            f"Git worktree registry identity for {target} is head "
            f"{registered.head!r}, detached={registered.detached}; expected "
            f"{evidence.git_commit}, detached=True"
        )
    if require_clean:
        entries = _pinned_tree_entries(evidence.repository, evidence.git_commit)
        _reject_checkout_filters(
            evidence.repository,
            evidence.git_commit,
            entries,
            scratch_directory=evidence.worktree.parent,
        )
        _verify_materialized_tree(evidence, entries)


@dataclass(frozen=True, slots=True, init=False)
class ProjectWorktreeManager(_FactoryOnly):
    """Project-neutral manager rooted in one explicit scheduler state directory."""

    state_worktree_root: Path
    _root_device: int = field(repr=False)
    _root_inode: int = field(repr=False)
    _root_boundary: SecurePathBoundary = field(repr=False)

    @classmethod
    def create(cls, state_worktree_root: str | Path) -> Self:
        """Bind the manager to one existing canonical absolute worktree root."""

        if cls is not ProjectWorktreeManager:
            raise TypeError(
                "ProjectWorktreeManager.create() constructs exactly "
                "ProjectWorktreeManager"
            )
        selected = _absolute_path(
            state_worktree_root,
            field_name="state_worktree_root",
        )
        if selected.parent == selected:
            raise ProjectWorktreeError(
                "state_worktree_root may not be the filesystem root; choose a "
                "dedicated directory beneath queue state"
            )
        try:
            details = selected.stat(follow_symlinks=False)
        except OSError as exc:
            raise ProjectWorktreeError(
                f"state_worktree_root {selected!s} does not resolve to an existing "
                "directory"
            ) from exc
        mode = stat.S_IMODE(details.st_mode)
        if not stat.S_ISDIR(details.st_mode):
            if stat.S_ISLNK(details.st_mode):
                raise ProjectWorktreeError(
                    f"state_worktree_root {selected!s} must be a non-symlink "
                    "directory"
                )
            raise ProjectWorktreeError(
                f"state_worktree_root {selected!s} is not a directory"
            )
        if (
            selected.is_symlink()
            or details.st_uid != os.geteuid()
            or mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise ProjectWorktreeError(
                f"state_worktree_root {selected!s} must be a non-symlink directory "
                f"owned by uid {os.geteuid()} and not group/world writable; got "
                f"uid {details.st_uid} mode {mode:04o}"
            )
        try:
            root = selected.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ProjectWorktreeError(
                f"state_worktree_root {selected!s} cannot be resolved safely"
            ) from exc
        if root != selected:
            raise ProjectWorktreeError(
                f"state_worktree_root {selected!s} must already be canonical and "
                f"symlink-free, but resolves to {root!s}"
            )
        try:
            boundary = capture_secure_path_boundary(
                root,
                label="state_worktree_root",
            )
        except PathBoundaryError as exc:
            raise ProjectWorktreeError(str(exc)) from exc
        return cast(
            Self,
            _construct(
                cls,
                state_worktree_root=root,
                _root_device=details.st_dev,
                _root_inode=details.st_ino,
                _root_boundary=boundary,
            ),
        )

    def _validate_root(self, revision: ProjectRevision) -> None:
        try:
            revalidate_secure_path_boundary(self._root_boundary)
            details = self.state_worktree_root.stat(follow_symlinks=False)
        except (OSError, PathBoundaryError) as exc:
            raise ProjectWorktreeError(
                f"state_worktree_root {self.state_worktree_root} changed or cannot "
                "be authenticated; stop the scheduler and repair queue state"
            ) from exc
        mode = stat.S_IMODE(details.st_mode)
        if (
            not stat.S_ISDIR(details.st_mode)
            or self.state_worktree_root.is_symlink()
            or details.st_uid != os.geteuid()
            or mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (details.st_dev, details.st_ino)
            != (self._root_device, self._root_inode)
        ):
            raise ProjectWorktreeError(
                f"state_worktree_root {self.state_worktree_root} changed canonical "
                "target or changed identity/permissions after manager creation; "
                "stop the scheduler and repair queue state"
            )
        enrolled_roots: list[tuple[str, Path]] = [
            ("checkout", revision.enrollment.checkout_directory)
        ]
        enrolled_roots.extend(
            (f"mount {mount.name!r}", mount.path)
            for mount in revision.enrollment.mounts
        )
        for environment in revision.enrollment.environments:
            enrolled_roots.extend(
                (
                    f"environment {environment.name!r} search directory {index}",
                    directory,
                )
                for index, directory in enumerate(
                    environment.executable_search_directories
                )
            )
        for label, root in enrolled_roots:
            if _paths_overlap(self.state_worktree_root, root):
                raise ProjectWorktreeError(
                    f"state_worktree_root {self.state_worktree_root} overlaps "
                    f"revision {label} {root}; scheduler worktrees require a "
                    "dedicated state root outside all Project roots"
                )

    def expected_evidence(
        self,
        *,
        revision: ProjectRevision,
        queue_item_id: int,
    ) -> ProjectWorktreeEvidence:
        """Recompute exact scheduler identity from one immutable revision."""

        if type(revision) is not ProjectRevision:
            raise TypeError(
                f"revision must be exactly ProjectRevision, got "
                f"{type(revision).__name__}"
            )
        item_id = _positive_integer(queue_item_id, field_name="queue_item_id")
        self._validate_root(revision)
        repository, _common = _repository_identity(revision)
        target = self.state_worktree_root / _worktree_name(revision, item_id)
        if target.parent != self.state_worktree_root:
            raise ProjectWorktreeError(
                f"derived worktree target {target} escapes state root "
                f"{self.state_worktree_root}"
            )
        # resolve(strict=False) follows a pre-created symlink.  Requiring the
        # result to retain the exact direct-child identity catches both live and
        # broken symlink redirection before Git is allowed to create anything.
        try:
            resolved_target = target.resolve(strict=False)
        except (OSError, RuntimeError) as exc:
            raise ProjectWorktreeError(
                f"derived worktree target {target} cannot be resolved safely"
            ) from exc
        if resolved_target != target or resolved_target.parent != self.state_worktree_root:
            raise ProjectWorktreeError(
                f"derived worktree target {target} resolves outside its exact "
                f"queue-owned identity as {resolved_target}; remove the symlink or "
                "unexpected path without touching its target"
            )
        return cast(
            ProjectWorktreeEvidence,
            _construct(
                ProjectWorktreeEvidence,
                project_id=revision.project_id,
                project_key=revision.project_key,
                project_revision_id=revision.id,
                project_revision=revision.label,
                project_revision_sequence=revision.sequence,
                queue_item_id=item_id,
                repository=repository,
                git_ref=_queue_ref(revision, item_id),
                worktree=target,
                git_commit=revision.git_commit,
            ),
        )

    @staticmethod
    def _match_recorded(
        expected: ProjectWorktreeEvidence,
        recorded: ProjectWorktreeEvidence,
    ) -> None:
        if type(recorded) is not ProjectWorktreeEvidence:
            raise TypeError(
                f"recorded evidence must be exactly ProjectWorktreeEvidence, got "
                f"{type(recorded).__name__}"
            )
        if recorded != expected:
            fields = (
                "project_id",
                "project_key",
                "project_revision_id",
                "project_revision",
                "project_revision_sequence",
                "queue_item_id",
                "repository",
                "git_ref",
                "worktree",
                "git_commit",
            )
            mismatches = [
                field
                for field in fields
                if getattr(recorded, field) != getattr(expected, field)
            ]
            raise ProjectWorktreeError(
                f"recorded worktree evidence differs from immutable revision/item "
                f"identity in fields {mismatches}; refuse repository, ref, path, or "
                "commit substitution"
            )

    def prepare(
        self,
        *,
        revision: ProjectRevision,
        queue_item_id: int,
        recorded_evidence: ProjectWorktreeEvidence | None = None,
    ) -> ProjectWorktreeEvidence:
        """Idempotently pin and materialize one detached immutable worktree."""

        evidence = self.expected_evidence(
            revision=revision,
            queue_item_id=queue_item_id,
        )
        if recorded_evidence is not None:
            self._match_recorded(evidence, recorded_evidence)
        _repository, common = _repository_identity(revision)
        _reject_other_item_identity(
            evidence,
            state_worktree_root=self.state_worktree_root,
        )
        pinned_entries = _pinned_tree_entries(
            evidence.repository,
            evidence.git_commit,
        )
        _reject_checkout_filters(
            evidence.repository,
            evidence.git_commit,
            pinned_entries,
            scratch_directory=self.state_worktree_root,
        )
        target_exists = os.path.lexists(evidence.worktree)
        if target_exists:
            # Authenticate a pre-existing target before creating or repairing
            # its ref.  A plain directory at a queue-shaped path is not enough
            # authority to mutate repository state.
            _verify_worktree(
                evidence,
                repository_common_directory=common,
                require_clean=True,
            )
        elif _registered_target(evidence.repository, evidence.worktree) is not None:
            raise ProjectWorktreeError(
                f"queue worktree path {evidence.worktree} is missing while Git "
                "still registers it; refuse implicit pruning or ref repair and "
                "require exact operator repair"
            )
        _ensure_ref(evidence)
        # Recheck after compare-and-create so a racing prepare cannot silently
        # leave two plausible revision identities.  Ambiguity fails closed and
        # is left intact for explicit operator diagnosis.
        _reject_other_item_identity(
            evidence,
            state_worktree_root=self.state_worktree_root,
        )
        if not target_exists:
            added = _run_git(
                evidence.repository,
                "worktree",
                "add",
                "--no-checkout",
                "--detach",
                str(evidence.worktree),
                evidence.git_ref,
            )
            if added.returncode != 0:
                raise ProjectWorktreeError(
                    f"Git could not create exact queue worktree "
                    f"{evidence.worktree} for {evidence.git_ref!r}: "
                    f"{_git_detail(added)}"
                )
            _normalize_new_worktree_metadata(evidence)
            populated = _run_git(
                evidence.worktree,
                "read-tree",
                evidence.git_commit,
            )
            if populated.returncode != 0:
                raise ProjectWorktreeError(
                    f"Git could not populate the private queue worktree index "
                    f"for {evidence.git_commit}: {_git_detail(populated)}"
                )
            # Re-evaluate effective attributes after worktree registration and
            # immediately before materialization. Repository-local
            # info/attributes is mutable and is intentionally part of this
            # fail-closed check.
            _reject_checkout_filters(
                evidence.repository,
                evidence.git_commit,
                pinned_entries,
                scratch_directory=self.state_worktree_root,
            )
            _materialize_pinned_tree(evidence, pinned_entries)
        _verify_worktree(
            evidence,
            repository_common_directory=common,
            require_clean=True,
        )
        return evidence

    def recover(
        self,
        *,
        revision: ProjectRevision,
        queue_item_id: int,
        recorded_evidence: ProjectWorktreeEvidence,
    ) -> ProjectWorktreeEvidence:
        """Read-only verification of recorded ref and worktree identity."""

        expected = self.expected_evidence(
            revision=revision,
            queue_item_id=queue_item_id,
        )
        self._match_recorded(expected, recorded_evidence)
        _repository, common = _repository_identity(revision)
        current_ref = _read_ref(expected.repository, expected.git_ref)
        if current_ref is None:
            raise ProjectWorktreeError(
                f"recorded queue-owned ref {expected.git_ref!r} is missing; "
                "recovery is read-only, so use explicit prepare/repair policy"
            )
        if current_ref != expected.git_commit:
            raise ProjectWorktreeError(
                f"recorded queue-owned ref {expected.git_ref!r} points to "
                f"{current_ref}, expected {expected.git_commit}"
            )
        _verify_worktree(
            expected,
            repository_common_directory=common,
            require_clean=True,
        )
        return expected

    def cleanup(
        self,
        *,
        revision: ProjectRevision,
        recorded_evidence: ProjectWorktreeEvidence,
    ) -> ProjectWorktreeEvidence:
        """Idempotently remove only an exact proven queue worktree and ref.

        This operation never calls ``rm``, broad ``git worktree prune``, or
        forced removal. It refuses dirty trees, missing-path registry ambiguity,
        and uses compare-and-delete ref updates, so scientific output, a changed
        ref, or a substituted path is never removed.
        """

        if type(recorded_evidence) is not ProjectWorktreeEvidence:
            raise TypeError(
                f"recorded evidence must be exactly ProjectWorktreeEvidence, got "
                f"{type(recorded_evidence).__name__}"
            )
        expected = self.expected_evidence(
            revision=revision,
            queue_item_id=recorded_evidence.queue_item_id,
        )
        self._match_recorded(expected, recorded_evidence)
        _repository, common = _repository_identity(revision)
        current_ref = _read_ref(expected.repository, expected.git_ref)
        if current_ref is not None and current_ref != expected.git_commit:
            raise ProjectWorktreeError(
                f"refused cleanup because {expected.git_ref!r} points to "
                f"{current_ref}, expected {expected.git_commit}"
            )

        target_exists = os.path.lexists(expected.worktree)
        registered = _registered_target(expected.repository, expected.worktree)
        if target_exists:
            if current_ref is None:
                raise ProjectWorktreeError(
                    f"refused cleanup of present worktree {expected.worktree} because "
                    f"its exact queue-owned ref {expected.git_ref!r} is missing"
                )
            _verify_worktree(
                expected,
                repository_common_directory=common,
                require_clean=True,
            )
            removed = _run_git(
                expected.repository,
                "worktree",
                "remove",
                str(expected.worktree),
            )
            if removed.returncode != 0:
                raise ProjectWorktreeError(
                    f"Git could not remove exact queue worktree "
                    f"{expected.worktree}: {_git_detail(removed)}"
                )
            if os.path.lexists(expected.worktree):
                raise ProjectWorktreeError(
                    f"Git reported success but exact queue worktree "
                    f"{expected.worktree} still exists; refuse ref cleanup"
                )
            if _registered_target(expected.repository, expected.worktree) is not None:
                raise ProjectWorktreeError(
                    f"Git removed {expected.worktree} but still registers that exact "
                    "worktree; refuse ref cleanup"
                )
        elif registered is not None:
            raise ProjectWorktreeError(
                f"queue worktree path {expected.worktree} is missing while Git still "
                "registers it; refuse broad pruning and require exact operator repair"
            )

        _delete_ref(expected)
        return expected


__all__ = [
    "ProjectWorktreeError",
    "ProjectWorktreeEvidence",
    "ProjectWorktreeManager",
    "QUEUE_REF_NAMESPACE",
]
