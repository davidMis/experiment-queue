"""Build project-qualified, path-authorized plans for structured admissions."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
from typing import Mapping, cast

from experiment_queue.admission import AdmissionSnapshot
from experiment_queue.authoring import (
    ArgvCommand,
    EnvironmentInheritance,
    ShellCommand,
    WrapperCommand,
    is_reserved_environment_variable,
)
from experiment_queue.project_lifecycle import (
    EnvironmentBinding,
    LifecycleValidationError,
    ProjectRevision,
)
from experiment_queue.serialization import JSONValue, canonical_json_bytes


_QUEUE_VARIABLE_PATTERN = re.compile(r"EXPERIMENT_QUEUE_[A-Z0-9_]+\Z")


class ExecutionValidationError(ValueError):
    """Raised when immutable admission evidence cannot be launched safely."""


@dataclass(frozen=True, slots=True)
class AuthorizedArtifact:
    """One declared artifact resolved beneath its revision-owned writable root."""

    name: str
    root_name: str
    relative_path: str
    path: Path
    artifact_type: str
    required: bool


@dataclass(frozen=True, slots=True)
class ObservedArtifact:
    """One post-segment observation bound to a declared authorized artifact."""

    name: str
    root_name: str
    relative_path: str
    path: Path
    artifact_type: str
    required: bool
    present: bool
    size_bytes: int | None


@dataclass(frozen=True, slots=True, init=False)
class ExecutionPlan:
    """A factory-only child-process plan derived from admitted revision evidence.

    The attempt publisher treats this object as an authorization result, not as
    a convenient bag of launch fields. Construction is therefore private and
    an integrity digest covers every executable input so ``dataclasses.replace``
    or an accidentally hand-built instance cannot substitute argv, environment,
    working-directory, or artifact authority after validation.
    """

    project_id: int
    project_key: str
    project_revision_id: int
    project_revision: str
    git_commit: str
    resolved_spec_sha256: str
    worktree_root: Path
    argv: tuple[str, ...]
    cwd: Path
    _environment_items: tuple[tuple[str, str], ...] = field(repr=False)
    artifacts: tuple[AuthorizedArtifact, ...]
    _integrity_sha256: str = field(repr=False)

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "ExecutionPlan is validated-only; use build_execution_plan()"
        )

    @property
    def environment(self) -> dict[str, str]:
        """Return a fresh child environment so callers cannot mutate this plan."""

        return dict(self._environment_items)

    def validate_integrity(self) -> None:
        """Reject any launch input changed after the factory authorized it."""

        try:
            expected = _execution_plan_integrity_sha256(
                project_id=self.project_id,
                project_key=self.project_key,
                project_revision_id=self.project_revision_id,
                project_revision=self.project_revision,
                git_commit=self.git_commit,
                resolved_spec_sha256=self.resolved_spec_sha256,
                worktree_root=self.worktree_root,
                argv=self.argv,
                cwd=self.cwd,
                environment_items=self._environment_items,
                artifacts=self.artifacts,
            )
            recorded = self._integrity_sha256
        except (AttributeError, TypeError, ValueError) as exc:
            raise ExecutionValidationError(
                f"ExecutionPlan has invalid factory-owned structure: {exc}"
            ) from exc
        if recorded != expected:
            raise ExecutionValidationError(
                "ExecutionPlan executable inputs changed after validation; rebuild "
                "the plan from its AdmissionSnapshot and ProjectRevision"
            )


def _execution_plan_integrity_sha256(
    *,
    project_id: int,
    project_key: str,
    project_revision_id: int,
    project_revision: str,
    git_commit: str,
    resolved_spec_sha256: str,
    worktree_root: Path,
    argv: tuple[str, ...],
    cwd: Path,
    environment_items: tuple[tuple[str, str], ...],
    artifacts: tuple[AuthorizedArtifact, ...],
) -> str:
    """Return the canonical digest for all fields that confer launch authority."""

    if type(argv) is not tuple or any(type(value) is not str for value in argv):
        raise TypeError("argv must be a tuple of strings")
    if type(environment_items) is not tuple or any(
        type(item) is not tuple
        or len(item) != 2
        or type(item[0]) is not str
        or type(item[1]) is not str
        for item in environment_items
    ):
        raise TypeError("environment items must be a tuple of string pairs")
    if tuple(sorted(environment_items)) != environment_items or len(
        {name for name, _value in environment_items}
    ) != len(environment_items):
        raise ValueError("environment items must be uniquely named and sorted")
    if type(artifacts) is not tuple or any(
        type(artifact) is not AuthorizedArtifact for artifact in artifacts
    ):
        raise TypeError("artifacts must be a tuple of AuthorizedArtifact values")
    if not isinstance(cwd, Path) or not cwd.is_absolute():
        raise TypeError("cwd must be an absolute pathlib.Path")
    if not isinstance(worktree_root, Path) or not worktree_root.is_absolute():
        raise TypeError("worktree_root must be an absolute pathlib.Path")

    artifact_documents: list[JSONValue] = []
    for artifact in artifacts:
        if not isinstance(artifact.path, Path) or not artifact.path.is_absolute():
            raise TypeError(
                f"artifact {artifact.name!r} path must be an absolute pathlib.Path"
            )
        artifact_documents.append(
            {
                "name": artifact.name,
                "root_name": artifact.root_name,
                "relative_path": artifact.relative_path,
                "path": str(artifact.path),
                "artifact_type": artifact.artifact_type,
                "required": artifact.required,
            }
        )
    document: JSONValue = {
        "schema_version": 1,
        "project_id": project_id,
        "project_key": project_key,
        "project_revision_id": project_revision_id,
        "project_revision": project_revision,
        "git_commit": git_commit,
        "resolved_spec_sha256": resolved_spec_sha256,
        "worktree_root": str(worktree_root),
        "argv": list(argv),
        "cwd": str(cwd),
        "environment": {name: value for name, value in environment_items},
        "artifacts": artifact_documents,
    }
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def _construct_execution_plan(**values: object) -> ExecutionPlan:
    """Construct an immutable plan only after the public builder validates it."""

    expected_names = {
        "project_id",
        "project_key",
        "project_revision_id",
        "project_revision",
        "git_commit",
        "resolved_spec_sha256",
        "worktree_root",
        "argv",
        "cwd",
        "_environment_items",
        "artifacts",
    }
    if set(values) != expected_names:
        raise AssertionError("internal ExecutionPlan construction fields changed")
    instance = object.__new__(ExecutionPlan)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    integrity = _execution_plan_integrity_sha256(
        project_id=instance.project_id,
        project_key=instance.project_key,
        project_revision_id=instance.project_revision_id,
        project_revision=instance.project_revision,
        git_commit=instance.git_commit,
        resolved_spec_sha256=instance.resolved_spec_sha256,
        worktree_root=instance.worktree_root,
        argv=instance.argv,
        cwd=instance.cwd,
        environment_items=instance._environment_items,
        artifacts=instance.artifacts,
    )
    object.__setattr__(instance, "_integrity_sha256", integrity)
    return instance


def _text(value: object, *, field_name: str, allow_empty: bool = False) -> str:
    if type(value) is not str or (not value and not allow_empty):
        qualifier = "a string" if allow_empty else "a non-empty string"
        raise ExecutionValidationError(
            f"{field_name} must be {qualifier}, got {value!r}"
        )
    if "\x00" in value:
        raise ExecutionValidationError(f"{field_name} must not contain a NUL byte")
    return value


def _portable_relative_path(value: object, *, field_name: str) -> str:
    text = _text(value, field_name=field_name)
    if (
        text.startswith(("/", "~"))
        or "\\" in text
        or "//" in text
        or re.match(r"^[A-Za-z]:", text) is not None
    ):
        raise ExecutionValidationError(
            f"{field_name} must be a normalized portable relative path, got {text!r}"
        )
    path = PurePosixPath(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ExecutionValidationError(
            f"{field_name} must not contain empty, '.', or '..' components, got "
            f"{text!r}"
        )
    return path.as_posix()


def _verified_root(root: Path, *, field_name: str) -> Path:
    """Re-resolve a frozen canonical root and reject replacement or retargeting."""

    try:
        resolved = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExecutionValidationError(
            f"{field_name} {root} is missing or cannot be resolved: {exc}; repair "
            "the enrolled path or create a new Project revision"
        ) from exc
    if not resolved.is_dir():
        raise ExecutionValidationError(
            f"{field_name} {root} is no longer a directory; repair it or create a "
            "new Project revision"
        )
    if resolved != root:
        raise ExecutionValidationError(
            f"{field_name} changed canonical target from {root} to {resolved}; "
            "refusing stale revision path authority"
        )
    return resolved


def _beneath(candidate: Path, root: Path) -> bool:
    return candidate != root and root in candidate.parents


def resolve_existing_project_path(
    root: Path,
    relative_path: str,
    *,
    field_name: str,
    require_directory: bool = False,
) -> Path:
    """Resolve an existing portable path and reject traversal or symlink escape."""

    canonical_root = _verified_root(root, field_name=f"{field_name} root")
    relative = _portable_relative_path(relative_path, field_name=field_name)
    try:
        resolved = (canonical_root / relative).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ExecutionValidationError(
            f"{field_name} {relative!r} does not resolve beneath {canonical_root}: "
            f"{exc}; commit/create the path or select the correct revision"
        ) from exc
    if not _beneath(resolved, canonical_root):
        raise ExecutionValidationError(
            f"{field_name} {relative!r} resolves outside authorized root "
            f"{canonical_root} to {resolved}"
        )
    if require_directory and not resolved.is_dir():
        raise ExecutionValidationError(
            f"{field_name} {relative!r} resolves to {resolved}, which is not a "
            "directory"
        )
    return resolved


def resolve_artifact_path(root: Path, relative_path: str, *, field_name: str) -> Path:
    """Resolve a possibly future artifact path beneath a canonical writable root."""

    canonical_root = _verified_root(root, field_name=f"{field_name} root")
    relative = _portable_relative_path(relative_path, field_name=field_name)
    try:
        resolved = (canonical_root / relative).resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise ExecutionValidationError(
            f"could not resolve {field_name} {relative!r} beneath {canonical_root}: "
            f"{exc}"
        ) from exc
    if not _beneath(resolved, canonical_root):
        raise ExecutionValidationError(
            f"{field_name} {relative!r} escapes authorized artifact root "
            f"{canonical_root} through traversal or a symlink (resolved to {resolved})"
        )
    return resolved


def construct_child_environment(
    *,
    revision: ProjectRevision,
    binding: EnvironmentBinding,
    ambient_environment: Mapping[str, str],
    assigned_gpu: str | None,
    queue_variables: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Construct an empty-baseline child environment and inject queue values last.

    Ambient values are copied only when their names occur in both the portable
    Project allowlist and the frozen host EnvironmentBinding. ``PATH`` is always
    built from the revision's canonical executable-search directories.
    """

    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got {type(revision).__name__}"
        )
    if type(binding) is not EnvironmentBinding:
        raise TypeError(
            f"binding must be exactly EnvironmentBinding, got {type(binding).__name__}"
        )
    if binding not in revision.enrollment.environments:
        raise ExecutionValidationError(
            f"environment binding {binding.name!r} does not belong to admitted "
            f"revision {revision.label!r}"
        )
    if not isinstance(ambient_environment, Mapping):
        raise TypeError("ambient_environment must be a mapping of names to values")
    if queue_variables is None:
        queue_variables = {}
    if not isinstance(queue_variables, Mapping):
        raise TypeError("queue_variables must be a mapping of queue-owned names")

    policy = revision.project.environment_policy
    portable_allowlist = set(policy.allow_variables)
    inherited_names = (
        ()
        if policy.inherit is EnvironmentInheritance.NONE
        else binding.inherit_variables
    )
    child: dict[str, str] = {}
    for name in inherited_names:
        if name not in portable_allowlist:
            raise ExecutionValidationError(
                f"revision environment {binding.name!r} attempts to inherit {name!r}, "
                "which is absent from the admitted Project allowlist"
            )
        if is_reserved_environment_variable(name) or name == "PATH":
            raise ExecutionValidationError(
                f"revision environment {binding.name!r} attempts to inherit "
                f"queue-owned variable {name!r}"
            )
        if name in ambient_environment:
            child[name] = _text(
                ambient_environment[name],
                field_name=f"ambient environment variable {name}",
                allow_empty=True,
            )

    search_directories = tuple(
        _verified_root(
            directory,
            field_name=(
                f"revision {revision.label!r} environment {binding.name!r} "
                "executable search directory"
            ),
        )
        for directory in binding.executable_search_directories
    )
    separated = [
        str(directory)
        for directory in search_directories
        if os.pathsep in str(directory)
    ]
    if separated:
        raise ExecutionValidationError(
            f"revision {revision.label!r} environment {binding.name!r} contains "
            f"executable search paths with PATH separator {os.pathsep!r}: "
            f"{separated}; create a new Enrollment with unambiguous directories"
        )
    child["PATH"] = os.pathsep.join(str(directory) for directory in search_directories)

    for name, value in queue_variables.items():
        if type(name) is not str or _QUEUE_VARIABLE_PATTERN.fullmatch(name) is None:
            raise ExecutionValidationError(
                f"queue variable name {name!r} must match EXPERIMENT_QUEUE_[A-Z0-9_]+"
            )
        child[name] = _text(
            value,
            field_name=f"queue environment variable {name}",
            allow_empty=True,
        )

    # Logical mount names are portable authoring identities. Their host paths
    # are queue-owned values injected only after ambient and caller-provided
    # variables, so project input cannot forge or override path authority.
    for mount in revision.enrollment.mounts:
        suffix = mount.name.upper().replace("-", "_")
        child[f"EXPERIMENT_QUEUE_MOUNT_{suffix}"] = str(
            _verified_root(
                mount.path,
                field_name=(
                    f"revision {revision.label!r} mount {mount.name!r}"
                ),
            )
        )

    child["EXPERIMENT_QUEUE_PROJECT_KEY"] = revision.project_key
    child["EXPERIMENT_QUEUE_PROJECT_REVISION"] = revision.label
    child["EXPERIMENT_QUEUE_GIT_COMMIT"] = revision.git_commit
    child["CUDA_VISIBLE_DEVICES"] = (
        ""
        if assigned_gpu is None
        else _text(assigned_gpu, field_name="assigned_gpu")
    )
    return child


def _command_argv(
    snapshot: AdmissionSnapshot,
    *,
    binding: EnvironmentBinding,
    worktree: Path,
) -> tuple[str, ...]:
    command = snapshot.command
    if type(command) is ArgvCommand:
        command_argv = command.argv
    elif type(command) is WrapperCommand:
        wrapper = resolve_existing_project_path(
            worktree,
            command.path,
            field_name="admitted wrapper path",
        )
        if not wrapper.is_file():
            raise ExecutionValidationError(
                f"admitted wrapper path {command.path!r} resolves to {wrapper}, "
                "which is not a regular file"
            )
        if not os.access(wrapper, os.X_OK):
            raise ExecutionValidationError(
                f"admitted wrapper {wrapper} is not executable; set its executable "
                "Git mode and create a new admission"
            )
        command_argv = (str(wrapper), *command.args)
    elif type(command) is ShellCommand:
        command_argv = ("sh", "-c", command.script)
    else:  # pragma: no cover - compiler-owned closed union
        raise ExecutionValidationError(
            f"admission has unsupported command model {type(command).__name__}"
        )
    if binding.command_prefix_argv is not None:
        return (*binding.command_prefix_argv, *command_argv)
    return command_argv


def build_execution_plan(
    *,
    snapshot: AdmissionSnapshot,
    revision: ProjectRevision,
    worktree: Path,
    ambient_environment: Mapping[str, str],
    assigned_gpu: str | None,
    queue_variables: Mapping[str, str] | None = None,
) -> ExecutionPlan:
    """Bind one structured snapshot to its immutable revision and worktree."""

    if type(snapshot) is not AdmissionSnapshot:
        raise TypeError(
            f"snapshot must be exactly AdmissionSnapshot, got {type(snapshot).__name__}"
        )
    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got {type(revision).__name__}"
        )
    mismatches: list[str] = []
    if snapshot.project_revision != revision.label:
        mismatches.append(
            f"snapshot revision {snapshot.project_revision!r} != {revision.label!r}"
        )
    if snapshot.git_commit != revision.git_commit:
        mismatches.append(
            f"snapshot commit {snapshot.git_commit!r} != {revision.git_commit!r}"
        )
    if snapshot.project_normalized_sha256 != revision.project_normalized_sha256:
        mismatches.append("Project normalized digest differs")
    if snapshot.project_source_sha256 != revision.project_source_sha256:
        mismatches.append("Project source digest differs")
    if snapshot.submission_policy.project_key != revision.project_key:
        mismatches.append("submission Project key differs")
    if mismatches:
        raise ExecutionValidationError(
            "admission snapshot does not belong to the supplied ProjectRevision: "
            + "; ".join(mismatches)
        )

    if not isinstance(worktree, Path) or not worktree.is_absolute():
        raise ExecutionValidationError(
            f"worktree must be an absolute pathlib.Path, got {worktree!r}"
        )
    canonical_worktree = _verified_root(
        worktree,
        field_name=f"revision {revision.label!r} worktree",
    )
    resolved = snapshot.resolved_document
    job_value = resolved.get("job")
    if type(job_value) is not dict:
        raise ExecutionValidationError("admission resolved evidence has no job object")
    job = cast(dict[str, object], job_value)
    environment_name = _text(
        job.get("environment"),
        field_name="admission job.environment",
    )
    try:
        binding = revision.enrollment.environment(environment_name)
    except LifecycleValidationError as exc:
        raise ExecutionValidationError(str(exc)) from exc

    working_directory = job.get("workingDirectory")
    cwd = (
        canonical_worktree
        if working_directory is None
        else resolve_existing_project_path(
            canonical_worktree,
            _text(working_directory, field_name="admission job.workingDirectory"),
            field_name="admission job.workingDirectory",
            require_directory=True,
        )
    )
    argv = _command_argv(
        snapshot,
        binding=binding,
        worktree=canonical_worktree,
    )
    environment = construct_child_environment(
        revision=revision,
        binding=binding,
        ambient_environment=ambient_environment,
        assigned_gpu=assigned_gpu,
        queue_variables=queue_variables,
    )

    artifact_values = job.get("artifacts", [])
    if type(artifact_values) is not list:
        raise ExecutionValidationError("admission job.artifacts must be a list")
    artifacts: list[AuthorizedArtifact] = []
    seen_names: set[str] = set()
    for index, value in enumerate(artifact_values):
        if type(value) is not dict:
            raise ExecutionValidationError(
                f"admission job.artifacts[{index}] must be an object"
            )
        artifact = cast(dict[str, object], value)
        name = _text(artifact.get("name"), field_name=f"artifact[{index}].name")
        if name in seen_names:
            raise ExecutionValidationError(f"admission repeats artifact name {name!r}")
        seen_names.add(name)
        root_name = _text(
            artifact.get("root"), field_name=f"artifact {name!r}.root"
        )
        try:
            root = revision.enrollment.artifact_root(root_name)
        except LifecycleValidationError as exc:
            raise ExecutionValidationError(str(exc)) from exc
        artifact_type = _text(
            artifact.get("type"), field_name=f"artifact {name!r}.type"
        )
        if artifact_type not in {"file", "directory"}:
            raise ExecutionValidationError(
                f"artifact {name!r}.type must be 'file' or 'directory', got "
                f"{artifact_type!r}"
            )
        required = artifact.get("required", False)
        if type(required) is not bool:
            raise ExecutionValidationError(
                f"artifact {name!r}.required must be a boolean"
            )
        relative_path = _portable_relative_path(
            artifact.get("path"), field_name=f"artifact {name!r}.path"
        )
        artifacts.append(
            AuthorizedArtifact(
                name=name,
                root_name=root_name,
                relative_path=relative_path,
                path=resolve_artifact_path(
                    root.path,
                    relative_path,
                    field_name=f"artifact {name!r}.path",
                ),
                artifact_type=artifact_type,
                required=required,
            )
        )

    # Exact declared artifact paths are also queue-owned child inputs. This
    # lets portable project code write its admitted outputs without learning a
    # host Enrollment path or reparsing queue state.
    for artifact in artifacts:
        suffix = artifact.name.upper().replace("-", "_")
        environment[f"EXPERIMENT_QUEUE_ARTIFACT_{suffix}"] = str(artifact.path)

    return _construct_execution_plan(
        project_id=revision.project_id,
        project_key=revision.project_key,
        project_revision_id=revision.id,
        project_revision=revision.label,
        git_commit=revision.git_commit,
        resolved_spec_sha256=snapshot.resolved_sha256,
        worktree_root=canonical_worktree,
        argv=argv,
        cwd=cwd,
        _environment_items=tuple(sorted(environment.items())),
        artifacts=tuple(artifacts),
    )


def observe_execution_artifacts(
    plan: ExecutionPlan,
    *,
    require_required: bool,
) -> tuple[ObservedArtifact, ...]:
    """Revalidate declared output paths and snapshot lightweight observations.

    General job artifacts may be arbitrarily large, so ordinary terminal
    collection records type, presence, and regular-file size without hashing
    their contents. Cooperative checkpoint artifacts use their separately
    versioned protocol and always carry digests.
    """

    if type(plan) is not ExecutionPlan:
        raise TypeError(
            f"plan must be exactly ExecutionPlan, got {type(plan).__name__}"
        )
    plan.validate_integrity()
    if type(require_required) is not bool:
        raise TypeError("require_required must be a boolean")
    observations: list[ObservedArtifact] = []
    missing: list[str] = []
    for artifact in plan.artifacts:
        path = artifact.path
        if path.is_symlink():
            raise ExecutionValidationError(
                f"declared artifact {artifact.name!r} became a symlink at {path}; "
                "refusing changed path authority"
            )
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError:
            resolved = None
        except (OSError, RuntimeError) as exc:
            raise ExecutionValidationError(
                f"could not inspect declared artifact {artifact.name!r} at "
                f"{path}: {exc}"
            ) from exc
        if resolved is None:
            if artifact.required and require_required:
                missing.append(f"{artifact.name} ({path})")
            observations.append(
                ObservedArtifact(
                    name=artifact.name,
                    root_name=artifact.root_name,
                    relative_path=artifact.relative_path,
                    path=path,
                    artifact_type=artifact.artifact_type,
                    required=artifact.required,
                    present=False,
                    size_bytes=None,
                )
            )
            continue
        if resolved != path:
            raise ExecutionValidationError(
                f"declared artifact {artifact.name!r} changed canonical target "
                f"from {path} to {resolved}"
            )
        if artifact.artifact_type == "file":
            if not resolved.is_file():
                raise ExecutionValidationError(
                    f"declared file artifact {artifact.name!r} is not a regular "
                    f"file at {resolved}"
                )
            try:
                size_bytes = resolved.stat().st_size
            except OSError as exc:
                raise ExecutionValidationError(
                    f"could not stat declared artifact {artifact.name!r} at "
                    f"{resolved}: {exc}"
                ) from exc
        else:
            if not resolved.is_dir():
                raise ExecutionValidationError(
                    f"declared directory artifact {artifact.name!r} is not a "
                    f"directory at {resolved}"
                )
            size_bytes = None
        observations.append(
            ObservedArtifact(
                name=artifact.name,
                root_name=artifact.root_name,
                relative_path=artifact.relative_path,
                path=path,
                artifact_type=artifact.artifact_type,
                required=artifact.required,
                present=True,
                size_bytes=size_bytes,
            )
        )
    if missing:
        raise ExecutionValidationError(
            "successful attempt did not produce required artifact(s): "
            + ", ".join(missing)
        )
    return tuple(observations)


__all__ = [
    "AuthorizedArtifact",
    "ExecutionPlan",
    "ExecutionValidationError",
    "ObservedArtifact",
    "build_execution_plan",
    "construct_child_environment",
    "observe_execution_artifacts",
    "resolve_artifact_path",
    "resolve_existing_project_path",
]
