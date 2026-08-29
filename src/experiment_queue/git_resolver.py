"""Resolve immutable admission sources from one pinned registered Git revision.

The pure admission compiler deliberately trusts its byte inputs.  This module
is the narrow bridge that earns that trust: it derives the repository only from
the frozen ``ProjectRevision``, reads regular blobs from the exact commit tree,
and returns factory-only evidence suitable for a database-v5 admission gate.
Working-tree files, indexes, refs, replacement objects, and Git hooks are never
consulted as authoring sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
from typing import Final, Sequence, TypeVar, cast

from experiment_queue.admission import (
    AdmissionSnapshot,
    Submission,
    compile_admission,
)
from experiment_queue.authoring import Project
from experiment_queue.project_lifecycle import Enrollment, ProjectRevision
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes


# Authoring documents are intentionally small control-plane inputs.  Checking
# the tree entry's declared size before reading prevents a committed giant blob
# from becoming an unbounded service-process allocation.
MAX_GIT_SOURCE_BYTES: Final = 8 * 1024 * 1024
_MAX_GIT_METADATA_BYTES: Final = 64 * 1024
_MAX_GIT_ERROR_BYTES: Final = 32 * 1024
_GIT_TIMEOUT_SECONDS: Final = 20.0
_MAX_REPOSITORY_PATH_CHARACTERS: Final = 4_096
_FULL_GIT_OBJECT_PATTERN: Final = re.compile(
    r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z"
)
_REGULAR_BLOB_MODES: Final = frozenset({"100644", "100755"})


class GitResolverError(ValueError):
    """Raised when pinned Git evidence cannot be resolved without ambiguity."""


class _ResolverEvidence:
    """Prevent callers from manufacturing resolver-authenticated values."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is trusted Git evidence produced only by "
            "compile_admission_from_revision()"
        )


_EvidenceT = TypeVar("_EvidenceT", bound=_ResolverEvidence)


def _construct_evidence(
    evidence_type: type[_EvidenceT],
    **values: object,
) -> _EvidenceT:
    instance = object.__new__(evidence_type)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


@dataclass(frozen=True, slots=True, init=False)
class GitBlobEvidence(_ResolverEvidence):
    """Detached identity of one regular file read from the pinned commit tree."""

    path: str
    object_id: str
    mode: str
    size: int
    source_sha256: str


@dataclass(frozen=True, slots=True, init=False)
class GitResolvedAdmission(_ResolverEvidence):
    """Admission snapshot authenticated against one immutable ProjectRevision.

    Database v5 should accept exactly this type, not a bare
    :class:`AdmissionSnapshot`, and should re-check the embedded hashes while
    decomposing the snapshot into rows.  Numeric ownership and the stable
    revision label are copied here so persistence need not infer them from
    mutable project state.
    """

    project_id: int
    project_revision_id: int
    project_key: str
    project_revision_label: str
    repository_root: str
    git_commit: str
    project_blob: GitBlobEvidence
    card_blob: GitBlobEvidence
    extension_schema_blob: GitBlobEvidence | None
    snapshot: AdmissionSnapshot = field(repr=False)

    @property
    def admission_snapshot(self) -> AdmissionSnapshot:
        """Alias the resolver-authenticated immutable compiler snapshot."""

        return self.snapshot


@dataclass(frozen=True, slots=True, init=False)
class GitResolvedProjectRevision(_ResolverEvidence):
    """Read-only proof that revision sources equal regular blobs at its commit."""

    project_id: int
    project_revision_id: int
    project_key: str
    project_revision_label: str
    repository_root: str
    git_commit: str
    project_blob: GitBlobEvidence
    extension_schema_blob: GitBlobEvidence | None
    revision: ProjectRevision = field(repr=False)


@dataclass(frozen=True, slots=True)
class _GitResult:
    """Bounded result from one structured Git plumbing invocation."""

    stdout: bytes
    stderr: bytes
    returncode: int


def _git_environment() -> dict[str, str]:
    """Return a deterministic read-only Git environment without caller routing.

    Ambient ``GIT_DIR``/object-directory/config variables could otherwise make
    ``git -C`` inspect a different repository.  Replacement refs could also
    change what a named object means, and partial-clone lazy fetch could turn a
    local admission into network activity.  Strip all Git controls and then add
    only the fail-closed read-only settings needed here.
    """

    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("GIT_")
    }
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PAGER": "cat",
            "GIT_TERMINAL_PROMPT": "0",
            "LC_ALL": "C",
        }
    )
    return environment


def _read_bounded(
    stream: object,
    *,
    maximum: int,
    operation: str,
    stream_name: str,
) -> bytes:
    """Read at most one byte beyond a declared subprocess-output bound."""

    assert hasattr(stream, "seek") and hasattr(stream, "read")
    stream.seek(0)  # type: ignore[attr-defined]
    value = stream.read(maximum + 1)  # type: ignore[attr-defined]
    assert type(value) is bytes
    if len(value) > maximum:
        raise GitResolverError(
            f"Git {operation} produced more than {maximum} bytes on {stream_name}; "
            "refusing unbounded repository output"
        )
    return value


def _run_git(
    repository_root: Path,
    arguments: tuple[str, ...],
    *,
    operation: str,
    maximum_stdout: int = _MAX_GIT_METADATA_BYTES,
    check: bool = True,
    literal_pathspecs: bool = True,
) -> _GitResult:
    """Run one fixed Git plumbing command with structured argv and bounded reads."""

    argv = (
        "git",
        "--no-pager",
        *(("--literal-pathspecs",) if literal_pathspecs else ()),
        "-C",
        str(repository_root),
        *arguments,
    )
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                env=_git_environment(),
                shell=False,
            )
        except FileNotFoundError as exc:
            raise GitResolverError(
                "Git executable was not found on the queue service PATH; install "
                "Git before registering or admitting a Project"
            ) from exc
        except OSError as exc:
            raise GitResolverError(
                f"could not start Git while {operation} in registered repository "
                f"{str(repository_root)!r}: {exc}"
            ) from exc
        try:
            returncode = process.wait(timeout=_GIT_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            process.wait()
            raise GitResolverError(
                f"Git timed out after {_GIT_TIMEOUT_SECONDS:g} seconds while "
                f"{operation} in registered repository {str(repository_root)!r}; "
                "check repository storage and object integrity"
            ) from exc

        stdout = _read_bounded(
            stdout_file,
            maximum=maximum_stdout,
            operation=operation,
            stream_name="stdout",
        )
        stderr = _read_bounded(
            stderr_file,
            maximum=_MAX_GIT_ERROR_BYTES,
            operation=operation,
            stream_name="stderr",
        )
    result = _GitResult(stdout=stdout, stderr=stderr, returncode=returncode)
    if check and result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        if not detail:
            detail = f"Git exited with status {result.returncode}"
        raise GitResolverError(
            f"Git could not {operation} in registered repository "
            f"{str(repository_root)!r}: {detail}"
        )
    return result


def _portable_repository_path(value: object, *, field_name: str) -> str:
    """Validate a tree path before it is passed as a literal Git pathspec."""

    if type(value) is not str:
        raise GitResolverError(
            f"{field_name} must be a repository-relative path string, got "
            f"{type(value).__name__}"
        )
    if not value or value != value.strip():
        raise GitResolverError(
            f"{field_name} must be non-empty and have no surrounding whitespace"
        )
    if len(value) > _MAX_REPOSITORY_PATH_CHARACTERS:
        raise GitResolverError(
            f"{field_name} must be {_MAX_REPOSITORY_PATH_CHARACTERS} characters "
            f"or fewer, got {len(value)}"
        )
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise GitResolverError(
            f"{field_name} must contain valid Unicode scalar text"
        ) from exc
    if any(
        ord(character) < 32
        or ord(character) in {127, 0x85, 0x2028, 0x2029}
        for character in value
    ):
        raise GitResolverError(
            f"{field_name} must not contain control or line characters"
        )
    components = value.split("/")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or re.match(r"[A-Za-z]:", value) is not None
        or value == "~"
        or value.startswith("~/")
        or "\\" in value
        or any(component in {"", ".", ".."} for component in components)
    ):
        raise GitResolverError(
            f"{field_name} must be one normalized repository-relative POSIX path "
            "without drive, tilde, backslash, empty, '.', or '..' components; "
            f"got {value!r}"
        )
    return path.as_posix()


def _validated_revision_context(revision: ProjectRevision) -> tuple[Path, str]:
    """Defensively verify the revision evidence needed before invoking Git."""

    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got "
            f"{type(revision).__name__}; load it through the validated lifecycle "
            "or database-v5 boundary"
        )
    for field_name, value in (
        ("revision.id", revision.id),
        ("revision.project_id", revision.project_id),
        ("revision.sequence", revision.sequence),
    ):
        if type(value) is not int or value <= 0:
            raise GitResolverError(
                f"{field_name} must be a positive integer, got {value!r}; reload "
                "the ProjectRevision from validated storage"
            )
    if type(revision.project) is not Project:
        raise GitResolverError(
            "revision.project is not an exact validated Project model; reload the "
            "ProjectRevision from validated source evidence"
        )
    if type(revision.enrollment) is not Enrollment:
        raise GitResolverError(
            "revision.enrollment is not an exact frozen Enrollment; reload the "
            "ProjectRevision from validated storage"
        )
    expected_label = f"{revision.project_key}:r{revision.sequence}"
    if revision.label != expected_label:
        raise GitResolverError(
            f"revision label {revision.label!r} does not match immutable identity "
            f"{expected_label!r}; repair or reject the stored revision"
        )
    if revision.project_key != revision.project.key:
        raise GitResolverError(
            f"revision project key {revision.project_key!r} does not match its "
            f"validated Project key {revision.project.key!r}"
        )
    if revision.enrollment.project_key != revision.project_key:
        raise GitResolverError(
            f"revision Enrollment belongs to {revision.enrollment.project_key!r}, "
            f"not Project {revision.project_key!r}"
        )
    source_path = _portable_repository_path(
        revision.project_source_path,
        field_name="revision.project_source_path",
    )
    if source_path != revision.enrollment.project_manifest_path:
        raise GitResolverError(
            f"revision Project source path {source_path!r} does not match frozen "
            f"Enrollment path {revision.enrollment.project_manifest_path!r}"
        )
    if type(revision.project_source) is not bytes:
        raise GitResolverError(
            "revision.project_source is not immutable bytes; reload the revision "
            "instead of forwarding caller-owned source data"
        )
    if sha256_bytes(revision.project_source) != revision.project_source_sha256:
        raise GitResolverError(
            "revision Project source SHA-256 does not match its stored bytes; "
            "reject the corrupted revision"
        )
    expected_normalized = canonical_json_bytes(revision.project.to_document())
    if revision.project_normalized_json != expected_normalized:
        raise GitResolverError(
            "revision normalized Project JSON does not match its validated Project "
            "model; reject the corrupted revision"
        )
    if sha256_bytes(expected_normalized) != revision.project_normalized_sha256:
        raise GitResolverError(
            "revision normalized Project SHA-256 does not match its canonical "
            "bytes; reject the corrupted revision"
        )
    extension_reference = revision.project.extension_schema
    extension_values = (
        revision.extension_schema_source_path,
        revision.extension_schema_source,
        revision.extension_schema_source_sha256,
        revision.extension_schema_canonical_json,
        revision.extension_schema_canonical_sha256,
    )
    if extension_reference is None:
        if any(value is not None for value in extension_values) or (
            revision.extension_schema_id is not None
        ):
            raise GitResolverError(
                "revision stores extension-schema evidence although its Project "
                "declares no extension schema; reject the corrupted revision"
            )
    else:
        if any(value is None for value in extension_values):
            raise GitResolverError(
                f"revision Project declares extension schema "
                f"{extension_reference.path!r}, but its immutable extension "
                "evidence is incomplete"
            )
        assert revision.extension_schema_source is not None
        assert revision.extension_schema_source_sha256 is not None
        assert revision.extension_schema_canonical_json is not None
        assert revision.extension_schema_canonical_sha256 is not None
        if revision.extension_schema_source_path != extension_reference.path:
            raise GitResolverError(
                "revision extension-schema source path does not match the Project "
                "reference path"
            )
        if (
            sha256_bytes(revision.extension_schema_source)
            != revision.extension_schema_source_sha256
            or sha256_bytes(revision.extension_schema_canonical_json)
            != revision.extension_schema_canonical_sha256
        ):
            raise GitResolverError(
                "revision extension-schema hashes do not match their immutable "
                "source/canonical bytes; reject the corrupted revision"
            )
    if (
        revision.enrollment.project_normalized_sha256
        != revision.project_normalized_sha256
    ):
        raise GitResolverError(
            "revision Enrollment was resolved for different Project semantics; "
            "create a new immutable revision with matching host bindings"
        )
    if (
        sha256_bytes(revision.enrollment.canonical_json)
        != revision.enrollment.sha256
    ):
        raise GitResolverError(
            "revision Enrollment digest does not match its canonical host-binding "
            "bytes; reject the corrupted revision"
        )
    try:
        enrollment_document = revision.enrollment.to_document()
        reencoded_enrollment = canonical_json_bytes(enrollment_document)
    except (AssertionError, TypeError, ValueError, UnicodeError) as exc:
        raise GitResolverError(
            "revision Enrollment canonical bytes are not a valid host-binding "
            "document; reject the corrupted revision"
        ) from exc
    if reencoded_enrollment != revision.enrollment.canonical_json:
        raise GitResolverError(
            "revision Enrollment document does not reproduce its canonical "
            "host-binding bytes; reject the corrupted revision"
        )

    commit = revision.git_commit
    if (
        type(commit) is not str
        or _FULL_GIT_OBJECT_PATTERN.fullmatch(commit) is None
        or commit != commit.lower()
    ):
        raise GitResolverError(
            "revision.git_commit must be an exact lowercase full 40- or "
            "64-character Git object ID, not a branch, tag, or abbreviation"
        )

    checkout = revision.enrollment.checkout_directory
    # pathlib's public factory returns the platform-specific PosixPath or
    # WindowsPath subclass, so exact ``type(...) is Path`` is never true.
    if not isinstance(checkout, Path) or not checkout.is_absolute():
        raise GitResolverError(
            "revision Enrollment checkout must be one canonical absolute pathlib.Path"
        )
    try:
        canonical_checkout = checkout.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise GitResolverError(
            f"registered checkout {str(checkout)!r} no longer resolves; restore "
            "the enrolled repository or create a new ProjectRevision"
        ) from exc
    if not canonical_checkout.is_dir():
        raise GitResolverError(
            f"registered checkout {str(canonical_checkout)!r} is not a directory"
        )
    if canonical_checkout != checkout:
        raise GitResolverError(
            f"registered checkout {str(checkout)!r} is not its current canonical "
            f"path {str(canonical_checkout)!r}; create a new ProjectRevision"
        )
    if (
        enrollment_document.get("projectKey") != revision.project_key
        or enrollment_document.get("projectManifestPath") != source_path
        or enrollment_document.get("checkoutDirectory") != str(canonical_checkout)
        or enrollment_document.get("projectNormalizedSha256")
        != revision.project_normalized_sha256
    ):
        raise GitResolverError(
            "revision Enrollment fields do not match its canonical project key, "
            "manifest path, checkout, or Project digest; reject the corrupted "
            "revision"
        )
    return canonical_checkout, source_path


def _canonical_git_toplevel(repository_root: Path) -> Path:
    result = _run_git(
        repository_root,
        ("rev-parse", "--show-toplevel"),
        operation="resolve the canonical Git toplevel",
        maximum_stdout=_MAX_REPOSITORY_PATH_CHARACTERS * 4 + 2,
    )
    raw = result.stdout
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if not raw or b"\n" in raw or b"\x00" in raw:
        raise GitResolverError(
            f"Git returned an invalid toplevel for registered checkout "
            f"{str(repository_root)!r}; ensure it is one non-bare worktree root"
        )
    try:
        reported = Path(os.fsdecode(raw)).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise GitResolverError(
            f"Git reported toplevel {os.fsdecode(raw)!r}, but it does not resolve "
            "to an existing canonical directory"
        ) from exc
    if reported != repository_root:
        raise GitResolverError(
            f"registered checkout {str(repository_root)!r} is not the Git "
            f"toplevel {str(reported)!r}; enroll the repository root itself, not "
            "a parent or subdirectory"
        )
    return reported


def _require_exact_commit(repository_root: Path, commit: str) -> None:
    object_type = _run_git(
        repository_root,
        ("cat-file", "-t", commit),
        operation=f"find pinned object {commit}",
        maximum_stdout=32,
    ).stdout.strip()
    if object_type != b"commit":
        rendered_type = object_type.decode("ascii", errors="replace") or "unknown"
        raise GitResolverError(
            f"pinned Git object {commit} has type {rendered_type!r}, not 'commit'; "
            "record the full object ID of a commit"
        )
    resolved = _run_git(
        repository_root,
        ("rev-parse", "--verify", f"{commit}^{{commit}}"),
        operation=f"verify pinned commit {commit}",
        maximum_stdout=80,
    ).stdout.strip()
    try:
        resolved_text = resolved.decode("ascii", errors="strict")
    except UnicodeDecodeError as exc:  # pragma: no cover - Git invariant
        raise GitResolverError(
            f"Git returned a non-ASCII object ID while verifying commit {commit}"
        ) from exc
    if resolved_text != commit:
        raise GitResolverError(
            f"pinned Git identity {commit} resolves to different commit "
            f"{resolved_text!r}; only an exact full commit object is admissible"
        )


def _optional_ignore_source(
    repository_root: Path,
    *,
    commit: str,
    path: str,
) -> bytes | None:
    """Read one regular committed ``.gitignore`` or return absent/non-regular."""

    result = _run_git(
        repository_root,
        ("ls-tree", "-z", "--full-tree", commit, "--", path),
        operation=f"inspect pinned ignore source {path!r}",
        maximum_stdout=_MAX_GIT_METADATA_BYTES,
    )
    if not result.stdout:
        return None
    mode, _object_id = _parse_tree_entry(
        result.stdout,
        source_path=path,
        purpose="Git-ignore source",
    )
    # A proof source is deliberately stricter than Git's ordinary worktree
    # behavior: only regular committed ignore files are admissible evidence.
    if mode not in _REGULAR_BLOB_MODES:
        return None
    blob, source = _read_regular_blob(
        repository_root,
        commit=commit,
        source_path=path,
        purpose="Git-ignore source",
    )
    assert blob.mode == mode
    return source


def verify_git_ignored_checkout_descendants(
    *,
    repository_root: str | Path,
    git_commit: str,
    descendants: Sequence[str | Path],
) -> tuple[Path, ...]:
    """Authenticate checkout-local mutable roots against one exact commit.

    Only committed ``.gitignore`` files participate. Global excludes, the
    checkout's mutable index and worktree, and ``.git/info/exclude`` are kept
    outside the decision by reproducing the relevant ignore hierarchy in a
    fresh temporary repository. The pinned tree must also contain no entry at
    or beneath a claimed mutable root, because ignore rules do not stop already
    tracked content from being changed.
    """

    if type(git_commit) is not str or (
        _FULL_GIT_OBJECT_PATTERN.fullmatch(git_commit) is None
        or git_commit != git_commit.lower()
    ):
        raise GitResolverError(
            "git_commit for Git-ignore proof must be an exact lowercase full "
            "40- or 64-character commit object ID"
        )
    if isinstance(descendants, (str, bytes)) or not isinstance(
        descendants, Sequence
    ):
        raise TypeError("descendants must be a sequence of absolute paths")
    try:
        checkout = Path(repository_root).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise GitResolverError(
            f"Git-ignore proof repository {str(repository_root)!r} cannot be "
            f"resolved: {exc}"
        ) from exc
    if not checkout.is_dir():
        raise GitResolverError(
            f"Git-ignore proof repository {str(checkout)!r} is not a directory"
        )
    _canonical_git_toplevel(checkout)
    _reject_partial_clone(checkout)
    _require_exact_commit(checkout, git_commit)

    canonical: list[tuple[Path, str]] = []
    for index, value in enumerate(descendants):
        if not isinstance(value, (str, Path)):
            raise TypeError(
                f"descendants[{index}] must be a string or pathlib.Path"
            )
        try:
            path = Path(value).resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise GitResolverError(
                f"Git-ignore proof path {str(value)!r} cannot be resolved: {exc}"
            ) from exc
        if not path.is_dir() or path == checkout or checkout not in path.parents:
            raise GitResolverError(
                f"Git-ignore proof path {str(path)!r} must be an existing strict "
                f"directory descendant of checkout {str(checkout)!r}"
            )
        relative = _portable_repository_path(
            path.relative_to(checkout).as_posix(),
            field_name=f"Git-ignore proof path {index}",
        )
        tracked = _run_git(
            checkout,
            ("ls-tree", "-z", "--full-tree", git_commit, "--", relative),
            operation=f"prove mutable root {relative!r} has no tracked content",
            maximum_stdout=_MAX_GIT_METADATA_BYTES,
        ).stdout
        if tracked:
            raise GitResolverError(
                f"checkout-descendant root {str(path)!r} contains tracked content "
                f"at pinned commit {git_commit}; move the mutable root or remove "
                "and commit the tracked content before enrollment"
            )
        canonical.append((path, relative))

    verified: list[Path] = []
    for path, relative in canonical:
        relative_path = PurePosixPath(relative)
        ignore_paths = [PurePosixPath(".gitignore")]
        parent = relative_path.parent
        accumulated = PurePosixPath()
        for component in parent.parts:
            accumulated /= component
            ignore_paths.append(accumulated / ".gitignore")

        with tempfile.TemporaryDirectory(prefix="experiment-queue-ignore-") as raw:
            proof_root = Path(raw)
            _run_git(
                proof_root,
                ("-c", "init.templateDir=", "init", "--quiet"),
                operation="initialize isolated Git-ignore proof repository",
            )
            for ignore_path in ignore_paths:
                ignore_source = _optional_ignore_source(
                    checkout,
                    commit=git_commit,
                    path=ignore_path.as_posix(),
                )
                if ignore_source is None:
                    continue
                target = proof_root.joinpath(*ignore_path.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    descriptor = os.open(
                        target,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600,
                    )
                except OSError as exc:
                    raise GitResolverError(
                        f"could not materialize isolated ignore source "
                        f"{ignore_path.as_posix()!r}: {exc}"
                    ) from exc
                try:
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(ignore_source)
                except BaseException:
                    target.unlink(missing_ok=True)
                    raise
            proof_candidate = proof_root.joinpath(*relative_path.parts)
            proof_candidate.mkdir(parents=True, exist_ok=True)
            result = _run_git(
                proof_root,
                (
                    "-c",
                    f"core.excludesFile={os.devnull}",
                    "check-ignore",
                    "--quiet",
                    "--no-index",
                    "--",
                    relative,
                ),
                operation=f"verify pinned ignore decision for {relative!r}",
                maximum_stdout=1,
                check=False,
                # check-ignore consumes a literal pathname rather than a Git
                # pathspec and rejects the global literal-pathspec switch.
                literal_pathspecs=False,
            )
        if result.returncode == 1:
            raise GitResolverError(
                f"checkout-descendant root {str(path)!r} is not ignored by "
                f"committed .gitignore rules at pinned commit {git_commit}; add "
                "and commit an ignore rule or move the root outside the checkout"
            )
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise GitResolverError(
                f"Git could not verify ignore status for {str(path)!r} at pinned "
                f"commit {git_commit}: {detail or f'exit status {result.returncode}'}"
            )
        verified.append(path)
    return tuple(sorted(set(verified), key=str))


def _reject_partial_clone(repository_root: Path) -> None:
    """Refuse repositories whose missing objects could trigger lazy fetching.

    ``GIT_NO_LAZY_FETCH`` is set for Git versions that implement it.  Rejecting
    the repository's durable partial-clone marker as well keeps the resolver
    offline on older supported host Git installations instead of depending on
    version-specific lazy-fetch behavior.
    """

    result = _run_git(
        repository_root,
        (
            "config",
            "--local",
            "--null",
            "--name-only",
            "--get-regexp",
            r"^(extensions\.partialclone|remote\..*\."
            r"(promisor|partialclonefilter))$",
        ),
        operation="inspect repository partial-clone and promisor markers",
        maximum_stdout=_MAX_GIT_METADATA_BYTES,
        check=False,
    )
    if result.returncode == 1:
        return
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise GitResolverError(
            "Git could not inspect the registered repository's partial-clone "
            f"or promisor markers: {detail or f'exit status {result.returncode}'}"
        )
    if not result.stdout.endswith(b"\0"):
        raise GitResolverError(
            "Git returned malformed partial-clone/promisor marker output"
        )
    markers = sorted(
        value.decode("utf-8", errors="backslashreplace")
        for value in result.stdout[:-1].split(b"\0")
        if value
    )
    if not markers:
        raise GitResolverError(
            "Git reported partial-clone/promisor markers but returned no names"
        )
    raise GitResolverError(
        f"registered repository {str(repository_root)!r} is a partial clone or "
        f"has promisor configuration {markers}; trusted "
        "admission is offline and will not lazily fetch missing objects, so "
        "materialize a complete local clone and create a new ProjectRevision"
    )


def _parse_tree_entry(
    output: bytes,
    *,
    source_path: str,
    purpose: str,
) -> tuple[str, str]:
    if not output:
        raise GitResolverError(
            f"pinned commit does not contain {purpose} at {source_path!r}; commit "
            "the file at the named path or submit a matching revision"
        )
    if not output.endswith(b"\x00"):
        raise GitResolverError(
            f"Git returned malformed tree evidence for {purpose} at "
            f"{source_path!r}; verify repository object integrity"
        )
    records = output[:-1].split(b"\x00")
    if len(records) != 1:
        raise GitResolverError(
            f"Git returned {len(records)} tree entries for exact {purpose} path "
            f"{source_path!r}; refusing ambiguous path evidence"
        )
    try:
        header, returned_path = records[0].split(b"\t", 1)
        mode_bytes, type_bytes, object_id_bytes = header.split(b" ", 2)
    except ValueError as exc:
        raise GitResolverError(
            f"Git returned malformed tree evidence for {purpose} at "
            f"{source_path!r}; verify repository object integrity"
        ) from exc
    if returned_path != os.fsencode(source_path):
        raise GitResolverError(
            f"Git returned path {os.fsdecode(returned_path)!r} while resolving "
            f"exact {purpose} path {source_path!r}; refusing mismatched evidence"
        )
    try:
        mode = mode_bytes.decode("ascii", errors="strict")
        object_type = type_bytes.decode("ascii", errors="strict")
        object_id = object_id_bytes.decode("ascii", errors="strict").lower()
    except UnicodeDecodeError as exc:
        raise GitResolverError(
            f"Git returned non-ASCII tree metadata for {purpose} at "
            f"{source_path!r}"
        ) from exc
    if mode not in _REGULAR_BLOB_MODES or object_type != "blob":
        if mode == "120000":
            detail = "is a symbolic link"
        elif mode == "160000" or object_type == "commit":
            detail = "is a Git submodule"
        elif object_type == "tree":
            detail = "is a directory"
        else:
            detail = f"has mode {mode!r} and type {object_type!r}"
        raise GitResolverError(
            f"{purpose} at {source_path!r} {detail} in the pinned commit; "
            "authoring sources must be committed regular files and are never "
            "followed through symlinks or submodules"
        )
    if _FULL_GIT_OBJECT_PATTERN.fullmatch(object_id) is None:
        raise GitResolverError(
            f"Git returned invalid blob object ID {object_id!r} for {purpose} at "
            f"{source_path!r}"
        )
    return mode, object_id


def _read_regular_blob(
    repository_root: Path,
    *,
    commit: str,
    source_path: str,
    purpose: str,
) -> tuple[GitBlobEvidence, bytes]:
    path = _portable_repository_path(source_path, field_name=f"{purpose} path")
    tree_output = _run_git(
        repository_root,
        ("ls-tree", "-z", "--full-tree", commit, "--", path),
        operation=f"resolve {purpose} tree entry {path!r} at {commit}",
        maximum_stdout=_MAX_GIT_METADATA_BYTES,
    ).stdout
    mode, object_id = _parse_tree_entry(
        tree_output,
        source_path=path,
        purpose=purpose,
    )
    size_output = _run_git(
        repository_root,
        ("cat-file", "-s", object_id),
        operation=f"read {purpose} blob size for {path!r}",
        maximum_stdout=64,
    ).stdout.strip()
    try:
        size = int(size_output.decode("ascii", errors="strict"), 10)
    except (UnicodeDecodeError, ValueError) as exc:
        raise GitResolverError(
            f"Git returned invalid blob size {size_output!r} for {purpose} at "
            f"{path!r}"
        ) from exc
    if size < 0 or size > MAX_GIT_SOURCE_BYTES:
        raise GitResolverError(
            f"{purpose} at {path!r} is {size} bytes in the pinned commit; the "
            f"admission limit is {MAX_GIT_SOURCE_BYTES} bytes, so reduce or split "
            "the authoring source"
        )
    source = _run_git(
        repository_root,
        ("cat-file", "blob", object_id),
        operation=f"read {purpose} blob {object_id} at {path!r}",
        maximum_stdout=size,
    ).stdout
    if len(source) != size:
        raise GitResolverError(
            f"Git reported {purpose} blob {object_id} at {path!r} as {size} "
            f"bytes but returned {len(source)}; retry after checking repository "
            "object integrity"
        )
    if len(object_id) == 40:
        object_digest = hashlib.sha1(usedforsecurity=False)
    elif len(object_id) == 64:
        object_digest = hashlib.sha256()
    else:  # pragma: no cover - tree parser owns the closed object-ID widths
        raise GitResolverError(
            f"Git returned unsupported object-ID width for {object_id!r}"
        )
    object_digest.update(f"blob {size}\0".encode("ascii"))
    object_digest.update(source)
    computed_object_id = object_digest.hexdigest()
    if computed_object_id != object_id:
        raise GitResolverError(
            f"Git returned bytes for {purpose} at {path!r} whose computed blob "
            f"object ID is {computed_object_id}, not tree identity {object_id}; "
            "check repository object storage and Git plumbing integrity"
        )
    evidence = cast(
        GitBlobEvidence,
        _construct_evidence(
            GitBlobEvidence,
            path=path,
            object_id=object_id,
            mode=mode,
            size=size,
            source_sha256=sha256_bytes(source),
        ),
    )
    return evidence, source


def verify_project_revision(
    revision: ProjectRevision,
) -> GitResolvedProjectRevision:
    """Authenticate one ProjectRevision against its exact committed tree.

    This is the registration/doctor boundary.  It reads only regular blobs from
    the full commit object and returns factory-only evidence; the index and
    working tree cannot change its result.
    """

    repository_root, project_source_path = _validated_revision_context(revision)
    canonical_toplevel = _canonical_git_toplevel(repository_root)
    _reject_partial_clone(canonical_toplevel)
    _require_exact_commit(canonical_toplevel, revision.git_commit)

    project_blob, project_source = _read_regular_blob(
        canonical_toplevel,
        commit=revision.git_commit,
        source_path=project_source_path,
        purpose="Project/v1 manifest",
    )
    if project_source != revision.project_source:
        raise GitResolverError(
            f"Project source at {project_source_path!r} in commit "
            f"{revision.git_commit} does not equal the exact bytes stored by "
            f"revision {revision.label!r}; reject the mismatched repository or "
            "revision"
        )
    if project_blob.source_sha256 != revision.project_source_sha256:
        raise GitResolverError(
            f"Project source hash at {project_source_path!r} in commit "
            f"{revision.git_commit} does not match revision {revision.label!r}"
        )

    extension_blob: GitBlobEvidence | None = None
    extension_reference = revision.project.extension_schema
    if extension_reference is not None:
        extension_blob, extension_source = _read_regular_blob(
            canonical_toplevel,
            commit=revision.git_commit,
            source_path=extension_reference.path,
            purpose="Project extension schema",
        )
        if (
            extension_source != revision.extension_schema_source
            or extension_blob.path != revision.extension_schema_source_path
            or extension_blob.source_sha256
            != revision.extension_schema_source_sha256
        ):
            raise GitResolverError(
                f"Project extension schema at {extension_reference.path!r} in "
                f"commit {revision.git_commit} does not equal the immutable bytes "
                f"stored by revision {revision.label!r}"
            )

    return cast(
        GitResolvedProjectRevision,
        _construct_evidence(
            GitResolvedProjectRevision,
            project_id=revision.project_id,
            project_revision_id=revision.id,
            project_key=revision.project_key,
            project_revision_label=revision.label,
            repository_root=str(canonical_toplevel),
            git_commit=revision.git_commit,
            project_blob=project_blob,
            extension_schema_blob=extension_blob,
            revision=revision,
        ),
    )


def compile_admission_from_revision(
    *,
    revision: ProjectRevision,
    submission: Submission,
) -> GitResolvedAdmission:
    """Authenticate pinned Git blobs and compile one immutable admission.

    The repository and Project source path come only from ``revision``.  The
    mutable submission contributes the already-bounded card path and policy;
    its card path is passed back to the pure compiler as ``card_source_name``,
    so mutation between resolution and compilation fails closed.  No database
    state is changed here.
    """

    if type(submission) is not Submission:
        raise TypeError(
            f"submission must be exactly Submission, got {type(submission).__name__}; "
            "copy proxy or subclass values into a plain Submission"
        )
    verified_revision = verify_project_revision(revision)
    canonical_toplevel = Path(verified_revision.repository_root)
    project_source_path = verified_revision.project_blob.path
    project_blob = verified_revision.project_blob
    project_source = revision.project_source

    # Read this mutable field once.  compile_admission() takes its own detached
    # snapshot and requires it to equal card_source_name, detecting a concurrent
    # change rather than compiling bytes under a different submitted name.
    card_path = _portable_repository_path(
        submission.card_path,
        field_name="submission.card_path",
    )
    card_blob, card_source = _read_regular_blob(
        canonical_toplevel,
        commit=revision.git_commit,
        source_path=card_path,
        purpose="ExperimentCard/v1",
    )

    extension_blob = verified_revision.extension_schema_blob
    extension_source = revision.extension_schema_source
    extension_reference = revision.project.extension_schema
    if extension_reference is None:
        extension_source = None

    snapshot = compile_admission(
        project_source=project_source,
        card_source=card_source,
        submission=submission,
        project_revision=revision.label,
        git_commit=revision.git_commit,
        extension_schema_source=extension_source,
        project_source_name=project_source_path,
        card_source_name=card_path,
    )
    if snapshot.project_source_sha256 != project_blob.source_sha256:
        raise GitResolverError(
            "compiled Project source hash differs from authenticated Git blob; "
            "refusing inconsistent admission evidence"
        )
    if snapshot.card_source_sha256 != card_blob.source_sha256:
        raise GitResolverError(
            "compiled ExperimentCard source hash differs from authenticated Git "
            "blob; refusing inconsistent admission evidence"
        )
    if extension_blob is None:
        if snapshot.extension_schema is not None:
            raise GitResolverError(
                "compiler produced extension-schema evidence without a resolved "
                "Git blob"
            )
    else:
        if (
            snapshot.extension_schema is None
            or snapshot.extension_schema.source_sha256
            != extension_blob.source_sha256
            or snapshot.extension_schema.reference_path != extension_blob.path
        ):
            raise GitResolverError(
                "compiled extension-schema evidence differs from the authenticated "
                "Git blob; refusing inconsistent admission evidence"
            )
    if (
        snapshot.project_revision != revision.label
        or snapshot.git_commit != revision.git_commit
    ):
        raise GitResolverError(
            "compiler returned different revision or commit identity than the "
            "authenticated Git context"
        )

    return cast(
        GitResolvedAdmission,
        _construct_evidence(
            GitResolvedAdmission,
            project_id=revision.project_id,
            project_revision_id=revision.id,
            project_key=revision.project_key,
            project_revision_label=revision.label,
            repository_root=str(canonical_toplevel),
            git_commit=revision.git_commit,
            project_blob=project_blob,
            card_blob=card_blob,
            extension_schema_blob=extension_blob,
            snapshot=snapshot,
        ),
    )


__all__ = [
    "GitBlobEvidence",
    "GitResolvedAdmission",
    "GitResolvedProjectRevision",
    "GitResolverError",
    "MAX_GIT_SOURCE_BYTES",
    "compile_admission_from_revision",
    "verify_git_ignored_checkout_descendants",
    "verify_project_revision",
]
