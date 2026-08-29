"""Model immutable project enrollment, revisions, lifecycle, and health state.

This module is deliberately storage-neutral.  Its factories form the validated
boundary that database-v5 persistence may decompose into strict rows without
weakening the ownership, path, or immutability rules accepted in ADR 0009.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from importlib.metadata import PackageNotFoundError, version as package_version_for
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Final, Mapping, Self, Sequence, TypeVar, cast

from experiment_queue.authoring import (
    AuthoringValidationError,
    EnvironmentInheritance,
    Project,
    VolumeAccess,
    is_reserved_environment_variable,
)
from experiment_queue.identity import PROJECT_KEY_PATTERN, validate_project_key
from experiment_queue.extensions import ExtensionSchemaError, load_extension_schema
from experiment_queue.schema_registry import PROJECT_V1_SCHEMA
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


_LOGICAL_NAME_PATTERN: Final = PROJECT_KEY_PATTERN
_ENVIRONMENT_VARIABLE_PATTERN: Final = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_GIT_OBJECT_PATTERN: Final = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_ENVIRONMENT_ASSIGNMENT_PATTERN: Final = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*=.*\Z",
    flags=re.DOTALL,
)
_TIMESTAMP_PATTERN: Final = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.[0-9]+)?"
    r"(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)

ARCHIVE_BLOCKING_QUEUE_STATES: Final = frozenset(
    {
        "queued",
        "held",
        "blocked",
        "starting",
        "running",
        "yielding",
        "terminating",
        "force_killing",
    }
)
ARCHIVE_TERMINAL_QUEUE_STATES: Final = frozenset(
    {"succeeded", "failed", "interrupted", "force_killed", "removed"}
)
_KNOWN_QUEUE_STATES: Final = (
    ARCHIVE_BLOCKING_QUEUE_STATES | ARCHIVE_TERMINAL_QUEUE_STATES
)


class LifecycleValidationError(ValueError):
    """Raised when host enrollment or project lifecycle evidence is invalid."""


class ProjectLifecycle(StrEnum):
    """Permanent project lifecycle states accepted by version 1."""

    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class ProjectHealth(StrEnum):
    """Project-scoped dispatch circuit state, separate from lifecycle."""

    CLOSED = "closed"
    OPEN = "open"


class _FactoryOnly:
    """Prevent callers from manufacturing values without validation."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is validated-only; use its documented "
            "factory method"
        )


_FactoryModel = TypeVar("_FactoryModel", bound=_FactoryOnly)


def _construct(model: type[_FactoryModel], **values: object) -> _FactoryModel:
    instance = object.__new__(model)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def _require_positive_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise LifecycleValidationError(
            f"{field_name} must be a positive integer, got {value!r}"
        )
    return value


def _require_nonnegative_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise LifecycleValidationError(
            f"{field_name} must be a nonnegative integer, got {value!r}"
        )
    return value


def _require_text(value: object, *, field_name: str, maximum: int) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise LifecycleValidationError(
            f"{field_name} must be a non-empty string without surrounding "
            f"whitespace, got {value!r}"
        )
    if len(value) > maximum:
        raise LifecycleValidationError(
            f"{field_name} must be {maximum} characters or fewer"
        )
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise LifecycleValidationError(
            f"{field_name} must contain valid Unicode scalar text"
        ) from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise LifecycleValidationError(
            f"{field_name} must not contain control characters"
        )
    return value


def _require_logical_name(value: object, *, field_name: str) -> str:
    name = _require_text(value, field_name=field_name, maximum=63)
    if _LOGICAL_NAME_PATTERN.fullmatch(name) is None:
        raise LifecycleValidationError(
            f"{field_name} must start with a lowercase letter and contain only "
            f"lowercase letters, digits, and single hyphen-separated components; "
            f"got {name!r}"
        )
    return name


def _require_project_key(value: object, *, field_name: str) -> str:
    if type(value) is not str:
        raise LifecycleValidationError(
            f"{field_name} must be a project-key string, got {type(value).__name__}"
        )
    try:
        return validate_project_key(value)
    except ValueError as exc:
        raise LifecycleValidationError(f"invalid {field_name}: {exc}") from exc


def _require_timestamp(value: object, *, field_name: str) -> str:
    timestamp = _require_text(value, field_name=field_name, maximum=64)
    matched = _TIMESTAMP_PATTERN.fullmatch(timestamp)
    if matched is None:
        raise LifecycleValidationError(
            f"{field_name} must use RFC 3339 spelling "
            "YYYY-MM-DDTHH:MM:SS[.fraction](Z|+HH:MM|-HH:MM)"
        )
    if (
        int(matched.group("hour")) > 23
        or int(matched.group("minute")) > 59
        or int(matched.group("second")) > 59
    ):
        raise LifecycleValidationError(
            f"{field_name} must be a real date and time with a valid UTC offset"
        )
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise LifecycleValidationError(
            f"{field_name} must be a real RFC 3339 date-time with an explicit "
            "UTC offset"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LifecycleValidationError(
            f"{field_name} must include Z or an explicit UTC offset"
        )
    return timestamp


def _require_full_git_object(value: object) -> str:
    if type(value) is not str or _GIT_OBJECT_PATTERN.fullmatch(value) is None:
        raise LifecycleValidationError(
            "git_commit must be a full 40- or 64-character hexadecimal Git "
            "object ID, not a branch, tag, or abbreviated revision"
        )
    return value.lower()


def _require_sha256(value: object, *, field_name: str) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise LifecycleValidationError(
            f"{field_name} must be a lowercase 64-character SHA-256 digest"
        )
    return value


def _validated_package_version(value: object) -> str:
    return _require_text(
        value,
        field_name="validated_package_version",
        maximum=128,
    )


def _installed_package_version() -> str:
    """Read trusted revision-validator provenance from installed metadata."""

    try:
        value = package_version_for("experiment-queue")
    except PackageNotFoundError as exc:
        raise LifecycleValidationError(
            "experiment-queue package metadata is unavailable; install the package "
            "before creating a ProjectRevision"
        ) from exc
    return _validated_package_version(value)


def _portable_relative_path(value: object, *, field_name: str) -> str:
    path_text = _require_text(value, field_name=field_name, maximum=4096)
    if "\\" in path_text:
        raise LifecycleValidationError(
            f"{field_name} must use portable forward slashes, got {path_text!r}"
        )
    if "//" in path_text or path_text.startswith(("/", "~")):
        raise LifecycleValidationError(
            f"{field_name} must be one normalized repository-relative path, got "
            f"{path_text!r}"
        )
    if re.match(r"^[A-Za-z]:", path_text):
        raise LifecycleValidationError(
            f"{field_name} must not use a Windows drive path, got {path_text!r}"
        )
    raw_parts = path_text.split("/")
    path = PurePosixPath(path_text)
    if (
        path.is_absolute()
        or path_text.endswith("/")
        or any(part in ("", ".", "..") for part in raw_parts)
    ):
        raise LifecycleValidationError(
            f"{field_name} must be normalized and may not contain '.' or '..'; "
            f"got {path_text!r}"
        )
    return path.as_posix()


def _path_input(value: object, *, field_name: str) -> Path:
    if type(value) is str:
        path = Path(value)
    elif isinstance(value, Path):
        path = Path(value)
    else:
        raise LifecycleValidationError(
            f"{field_name} must be an absolute path string or pathlib.Path, got "
            f"{type(value).__name__}"
        )
    if not path.is_absolute():
        raise LifecycleValidationError(
            f"{field_name} must be absolute, got {str(path)!r}; resolve it on the "
            "enrolled host before registration"
        )
    return path


def _canonical_directory(value: object, *, field_name: str) -> Path:
    path = _path_input(value, field_name=field_name)
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise LifecycleValidationError(
            f"{field_name} {str(path)!r} does not resolve to an existing directory; "
            "create or repair the path and retry enrollment"
        ) from exc
    if not resolved.is_dir():
        raise LifecycleValidationError(
            f"{field_name} {str(path)!r} resolves to {str(resolved)!r}, which is "
            "not a directory"
        )
    _require_text(str(resolved), field_name=field_name, maximum=4096)
    return resolved


def _canonical_executable(value: object, *, field_name: str) -> str:
    path = _path_input(value, field_name=field_name)
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise LifecycleValidationError(
            f"{field_name} {str(path)!r} does not resolve to an existing file; "
            "install the command prefix before creating the revision"
        ) from exc
    if not resolved.is_file():
        raise LifecycleValidationError(
            f"{field_name} {str(path)!r} must resolve to a regular file, got "
            f"{str(resolved)!r}"
        )
    return _require_text(str(resolved), field_name=field_name, maximum=4096)


def _owned_sequence(value: object, *, field_name: str) -> tuple[object, ...]:
    if type(value) not in (list, tuple):
        raise LifecycleValidationError(
            f"{field_name} must be a list or tuple, got {type(value).__name__}"
        )
    try:
        return tuple(cast(Sequence[object], value))
    except RuntimeError as exc:
        raise LifecycleValidationError(
            f"{field_name} changed while it was being copied; stop mutating it "
            "and retry"
        ) from exc


def _overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _format_root(label: str, path: Path) -> str:
    return f"{label} ({str(path)!r})"


@dataclass(frozen=True, slots=True, init=False)
class MountBinding(_FactoryOnly):
    """One canonical host directory bound to a portable logical volume."""

    name: str
    path: Path
    access: VolumeAccess

    @classmethod
    def create(
        cls,
        *,
        name: str,
        path: str | Path,
        access: VolumeAccess | str,
    ) -> Self:
        """Validate one logical mount without yet trusting its Project relation."""

        if cls is not MountBinding:
            raise TypeError("MountBinding.create() constructs exactly MountBinding")
        logical_name = _require_logical_name(name, field_name="mount.name")
        try:
            parsed_access = (
                access if type(access) is VolumeAccess else VolumeAccess(access)
            )
        except (TypeError, ValueError) as exc:
            raise LifecycleValidationError(
                "mount.access must be 'readOnly' or 'readWrite'"
            ) from exc
        return cast(
            Self,
            _construct(
                cls,
                name=logical_name,
                path=_canonical_directory(path, field_name=f"mount {logical_name!r}"),
                access=parsed_access,
            ),
        )

    def to_document(self) -> dict[str, JSONValue]:
        """Return the exact normalized host-binding fields."""

        return {
            "name": self.name,
            "path": str(self.path),
            "access": self.access.value,
        }


@dataclass(frozen=True, slots=True, init=False)
class ArtifactRootBinding(_FactoryOnly):
    """Derived artifact-root view of one read-write MountBinding."""

    name: str
    path: Path

    @classmethod
    def from_mount(cls, mount: MountBinding) -> Self:
        """Derive an artifact root; callers cannot configure another path."""

        if cls is not ArtifactRootBinding:
            raise TypeError(
                "ArtifactRootBinding.from_mount() constructs exactly "
                "ArtifactRootBinding"
            )
        if type(mount) is not MountBinding:
            raise TypeError(
                f"mount must be exactly MountBinding, got {type(mount).__name__}"
            )
        if mount.access is not VolumeAccess.READ_WRITE:
            raise LifecycleValidationError(
                f"mount {mount.name!r} is {mount.access.value}; artifact roots may "
                "only be derived from readWrite mounts"
            )
        return cast(
            Self,
            _construct(cls, name=mount.name, path=mount.path),
        )

    def to_document(self) -> dict[str, JSONValue]:
        """Return the derived artifact-root identity."""

        return {"name": self.name, "path": str(self.path)}


@dataclass(frozen=True, slots=True, init=False)
class EnvironmentBinding(_FactoryOnly):
    """Frozen EnvironmentBinding/v1 containing names and paths, never values.

    Literal environment-variable mappings are intentionally absent.  Ambient
    values are looked up only at execution for names admitted by both the
    portable Project policy and this frozen binding.
    """

    name: str
    executable_search_directories: tuple[Path, ...]
    command_prefix_argv: tuple[str, ...] | None
    inherit_variables: tuple[str, ...]

    @classmethod
    def create(
        cls,
        *,
        name: str,
        executable_search_directories: Sequence[str | Path],
        inherit_variables: Sequence[str] = (),
        command_prefix_argv: Sequence[str] | None = None,
    ) -> Self:
        """Validate and freeze one host environment binding."""

        if cls is not EnvironmentBinding:
            raise TypeError(
                "EnvironmentBinding.create() constructs exactly EnvironmentBinding"
            )
        logical_name = _require_logical_name(name, field_name="environment.name")
        search_inputs = _owned_sequence(
            executable_search_directories,
            field_name=f"environment {logical_name!r} executable search directories",
        )
        if not search_inputs:
            raise LifecycleValidationError(
                f"environment {logical_name!r} requires at least one executable "
                "search directory so child PATH never depends on the service PATH"
            )
        search_directories = tuple(
            _canonical_directory(
                value,
                field_name=(
                    f"environment {logical_name!r} executable search directory "
                    f"{index}"
                ),
            )
            for index, value in enumerate(search_inputs)
        )
        separated = [
            str(directory)
            for directory in search_directories
            if os.pathsep in str(directory)
        ]
        if separated:
            raise LifecycleValidationError(
                f"environment {logical_name!r} executable search directories "
                f"contain the platform PATH separator {os.pathsep!r}: "
                f"{separated}; move or rename those directories so each frozen "
                "absolute path remains exactly one child PATH entry"
            )
        if len(set(search_directories)) != len(search_directories):
            raise LifecycleValidationError(
                f"environment {logical_name!r} repeats an executable search "
                "directory; list each canonical directory once"
            )

        inherited_inputs = _owned_sequence(
            inherit_variables,
            field_name=f"environment {logical_name!r} inherit variables",
        )
        inherited: list[str] = []
        for index, value in enumerate(inherited_inputs):
            variable = _require_text(
                value,
                field_name=(
                    f"environment {logical_name!r} inherit_variables[{index}]"
                ),
                maximum=256,
            )
            if _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(variable) is None:
                raise LifecycleValidationError(
                    f"environment {logical_name!r} inherit variable {variable!r} "
                    "must be an uppercase environment-variable name, not a name/value "
                    "assignment"
                )
            if variable == "PATH":
                raise LifecycleValidationError(
                    f"environment {logical_name!r} cannot inherit PATH; the queue "
                    "constructs child PATH only from its frozen executable search "
                    "directories"
                )
            if is_reserved_environment_variable(variable):
                raise LifecycleValidationError(
                    f"environment {logical_name!r} cannot inherit service-owned "
                    f"variable {variable!r}; CUDA_VISIBLE_DEVICES and all "
                    "EXPERIMENT_QUEUE_* values are injected by the queue"
                )
            inherited.append(variable)
        if len(set(inherited)) != len(inherited):
            raise LifecycleValidationError(
                f"environment {logical_name!r} repeats inherited variable names; "
                "list each name once"
            )

        prefix: tuple[str, ...] | None
        if command_prefix_argv is None:
            prefix = None
        else:
            prefix_inputs = _owned_sequence(
                command_prefix_argv,
                field_name=f"environment {logical_name!r} command prefix argv",
            )
            if not prefix_inputs:
                raise LifecycleValidationError(
                    f"environment {logical_name!r} command prefix argv cannot be "
                    "empty; omit it when no prefix is required"
                )
            prefix_values: list[str] = [
                _canonical_executable(
                    prefix_inputs[0],
                    field_name=(
                        f"environment {logical_name!r} command prefix executable"
                    ),
                )
            ]
            for index, value in enumerate(prefix_inputs[1:], start=1):
                argument = _require_text(
                    value,
                    field_name=(
                        f"environment {logical_name!r} command_prefix_argv[{index}]"
                    ),
                    maximum=4096,
                )
                if _ENVIRONMENT_ASSIGNMENT_PATTERN.fullmatch(argument):
                    raise LifecycleValidationError(
                        f"environment {logical_name!r} command prefix argument "
                        f"{argument!r} is a literal environment assignment; store "
                        "only inherited variable names and keep values or secrets out "
                        "of Enrollment"
                    )
                prefix_values.append(argument)
            prefix = tuple(prefix_values)

        return cast(
            Self,
            _construct(
                cls,
                name=logical_name,
                executable_search_directories=search_directories,
                command_prefix_argv=prefix,
                inherit_variables=tuple(sorted(inherited)),
            ),
        )

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> Self:
        """Parse strict EnvironmentBinding/v1 JSON without accepting secrets."""

        if cls is not EnvironmentBinding:
            raise TypeError(
                "EnvironmentBinding.from_document() constructs exactly "
                "EnvironmentBinding"
            )
        if type(document) is not dict:
            raise LifecycleValidationError(
                "EnvironmentBinding/v1 must be a plain JSON object"
            )
        non_text_fields = [field for field in document if type(field) is not str]
        if non_text_fields:
            raise LifecycleValidationError(
                "EnvironmentBinding/v1 object keys must be strings, got "
                f"{non_text_fields!r}"
            )
        expected = {
            "apiVersion",
            "kind",
            "name",
            "executableSearchDirectories",
            "inheritVariables",
        }
        optional = {"commandPrefixArgv"}
        fields = set(document)
        unknown = sorted(fields - expected - optional)
        missing = sorted(expected - fields)
        if unknown or missing:
            details: list[str] = []
            if missing:
                details.append(f"missing fields {missing}")
            if unknown:
                details.append(f"unknown fields {unknown}")
                if any(
                    name in {"environment", "variables", "values", "secrets"}
                    for name in unknown
                ):
                    details.append(
                        "literal variable values and secrets are forbidden; use "
                        "inheritVariables names only"
                    )
            raise LifecycleValidationError(
                "EnvironmentBinding/v1 has invalid fields: " + "; ".join(details)
            )
        if document["apiVersion"] != "experiment-queue/v1" or document["kind"] != (
            "EnvironmentBinding"
        ):
            raise LifecycleValidationError(
                "EnvironmentBinding/v1 requires apiVersion 'experiment-queue/v1' "
                "and kind 'EnvironmentBinding'"
            )
        return cls.create(
            name=cast(str, document["name"]),
            executable_search_directories=cast(
                Sequence[str | Path], document["executableSearchDirectories"]
            ),
            inherit_variables=cast(
                Sequence[str], document["inheritVariables"]
            ),
            command_prefix_argv=cast(
                Sequence[str] | None, document.get("commandPrefixArgv")
            ),
        )

    def to_document(self) -> dict[str, JSONValue]:
        """Return fresh versioned JSON containing no ambient values."""

        document: dict[str, JSONValue] = {
            "apiVersion": "experiment-queue/v1",
            "kind": "EnvironmentBinding",
            "name": self.name,
            "executableSearchDirectories": [
                str(path) for path in self.executable_search_directories
            ],
            "inheritVariables": list(self.inherit_variables),
        }
        if self.command_prefix_argv is not None:
            document["commandPrefixArgv"] = list(self.command_prefix_argv)
        return document


@dataclass(frozen=True, slots=True, init=False)
class HostRootClaim(_FactoryOnly):
    """Canonical root already owned by another registered Project."""

    project_key: str
    role: str
    path: Path

    @classmethod
    def create(
        cls,
        *,
        project_key: str,
        role: str,
        path: str | Path,
    ) -> Self:
        """Create one cross-project overlap claim for Enrollment validation."""

        if cls is not HostRootClaim:
            raise TypeError("HostRootClaim.create() constructs exactly HostRootClaim")
        key = _require_project_key(project_key, field_name="root claim project_key")
        role_text = _require_text(role, field_name="root claim role", maximum=200)
        return cast(
            Self,
            _construct(
                cls,
                project_key=key,
                role=role_text,
                path=_canonical_directory(
                    path,
                    field_name=f"root claim {key!r} {role_text!r}",
                ),
            ),
        )


def _validate_complete_bindings(
    project: Project,
    mounts: tuple[MountBinding, ...],
    environments: tuple[EnvironmentBinding, ...],
) -> None:
    declared_volumes = {volume.name: volume for volume in project.volumes}
    mount_names = [mount.name for mount in mounts]
    duplicate_mounts = sorted(
        name for name in set(mount_names) if mount_names.count(name) > 1
    )
    if duplicate_mounts:
        raise LifecycleValidationError(
            f"Enrollment for Project {project.key!r} repeats mount bindings "
            f"{duplicate_mounts}; bind each logical volume at most once"
        )
    unknown_mounts = sorted(set(mount_names) - set(declared_volumes))
    if unknown_mounts:
        raise LifecycleValidationError(
            f"Enrollment for Project {project.key!r} binds undeclared volumes "
            f"{unknown_mounts}; declare them in spec.volumes or remove the bindings"
        )
    missing_required = sorted(
        volume.name
        for volume in project.volumes
        if volume.required is True and volume.name not in mount_names
    )
    if missing_required:
        raise LifecycleValidationError(
            f"Enrollment for Project {project.key!r} is missing required volume "
            f"bindings {missing_required}; bind every spec.volumes entry with "
            "required: true"
        )
    for mount in mounts:
        declaration = declared_volumes[mount.name]
        if (
            declaration.access is VolumeAccess.READ_ONLY
            and mount.access is VolumeAccess.READ_WRITE
        ):
            raise LifecycleValidationError(
                f"mount {mount.name!r} widens Project access from readOnly to "
                "readWrite; Enrollment may narrow access but never widen it"
            )

    declared_environments = {item.name for item in project.environments}
    environment_names = [environment.name for environment in environments]
    duplicate_environments = sorted(
        name
        for name in set(environment_names)
        if environment_names.count(name) > 1
    )
    if duplicate_environments:
        raise LifecycleValidationError(
            f"Enrollment for Project {project.key!r} repeats environment bindings "
            f"{duplicate_environments}; bind each declared environment exactly once"
        )
    missing_environments = sorted(
        declared_environments - set(environment_names)
    )
    unknown_environments = sorted(
        set(environment_names) - declared_environments
    )
    if missing_environments or unknown_environments:
        details: list[str] = []
        if missing_environments:
            details.append(f"missing declared environments {missing_environments}")
        if unknown_environments:
            details.append(f"undeclared environments {unknown_environments}")
        raise LifecycleValidationError(
            f"Enrollment for Project {project.key!r} must bind every declared "
            "environment exactly once: " + "; ".join(details)
        )

    portable_allowlist = frozenset(project.environment_policy.allow_variables)
    for environment in environments:
        inherited = frozenset(environment.inherit_variables)
        outside_policy = sorted(inherited - portable_allowlist)
        if outside_policy:
            raise LifecycleValidationError(
                f"environment {environment.name!r} inherits variables "
                f"{outside_policy} outside Project spec.environmentPolicy."
                "allowVariables; remove them because Enrollment may only narrow "
                "portable policy"
            )
        if (
            project.environment_policy.inherit is EnvironmentInheritance.NONE
            and inherited
        ):
            raise LifecycleValidationError(
                f"environment {environment.name!r} inherits variables even though "
                "Project spec.environmentPolicy.inherit is 'none'; use an empty "
                "inheritVariables list"
            )


def _validate_root_relationships(
    *,
    checkout: Path,
    logical_roots: tuple[tuple[str, Path], ...],
    state_directory: Path,
    ignored_descendants: tuple[Path, ...],
    occupied_roots: tuple[HostRootClaim, ...],
) -> None:
    # Enrollment never treats the primary checkout as a scheduler-owned
    # worktree.  The state-containment exception is therefore not applicable.
    if _overlap(state_directory, checkout):
        raise LifecycleValidationError(
            f"queue state directory {str(state_directory)!r} overlaps checkout "
            f"{str(checkout)!r}; choose roots with no equality or ancestor relation"
        )

    for label, root in logical_roots:
        if _overlap(state_directory, root):
            raise LifecycleValidationError(
                f"queue state directory {str(state_directory)!r} overlaps "
                f"{_format_root(label, root)}; move the binding or state directory"
            )
        if root == checkout or root in checkout.parents:
            raise LifecycleValidationError(
                f"{_format_root(label, root)} equals or contains checkout "
                f"{str(checkout)!r}; a binding may only be a proven ignored "
                "descendant of its own checkout"
            )
        if checkout in root.parents:
            ignored = any(proof == root or proof in root.parents for proof in ignored_descendants)
            if not ignored:
                raise LifecycleValidationError(
                    f"{_format_root(label, root)} is inside checkout "
                    f"{str(checkout)!r}, but no Git-ignore proof covers it at the "
                    "pinned commit; supply a trusted ignored descendant or move it "
                    "outside the checkout"
                )

    for index, (first_label, first_root) in enumerate(logical_roots):
        for second_label, second_root in logical_roots[index + 1 :]:
            if _overlap(first_root, second_root):
                raise LifecycleValidationError(
                    f"logical roots {_format_root(first_label, first_root)} and "
                    f"{_format_root(second_label, second_root)} overlap; version 1 "
                    "requires distinct non-nested mount and environment roots"
                )

    local_roots = (("checkout", checkout),) + logical_roots
    for claim in occupied_roots:
        for local_label, local_root in local_roots:
            if _overlap(local_root, claim.path):
                raise LifecycleValidationError(
                    f"Project root {_format_root(local_label, local_root)} overlaps "
                    f"{claim.project_key!r} root "
                    f"{_format_root(claim.role, claim.path)}; version 1 forbids "
                    "cross-project equality and ancestor/descendant sharing"
                )


@dataclass(frozen=True, slots=True, init=False)
class Enrollment(_FactoryOnly):
    """Frozen host resolution of one validated portable Project/v1."""

    project_key: str
    project_normalized_sha256: str
    checkout_directory: Path
    project_manifest_path: str
    mounts: tuple[MountBinding, ...]
    artifact_roots: tuple[ArtifactRootBinding, ...]
    environments: tuple[EnvironmentBinding, ...]
    git_ignored_checkout_descendants: tuple[Path, ...]
    canonical_json: bytes = field(repr=False)
    sha256: str

    @classmethod
    def create(
        cls,
        *,
        project: Project,
        checkout_directory: str | Path,
        project_manifest_path: str,
        mounts: Sequence[MountBinding],
        environments: Sequence[EnvironmentBinding],
        state_directory: str | Path,
        git_ignored_checkout_descendants: Sequence[str | Path] = (),
        occupied_roots: Sequence[HostRootClaim] = (),
    ) -> Self:
        """Validate all host bindings and freeze their canonical JSON evidence.

        ``git_ignored_checkout_descendants`` is trusted evidence from Git at the
        pinned commit.  Merely existing beneath the working checkout is never
        treated as proof that a path is ignored.
        """

        if cls is not Enrollment:
            raise TypeError("Enrollment.create() constructs exactly Enrollment")
        if type(project) is not Project:
            raise TypeError(
                f"project must be exactly authoring.Project, got "
                f"{type(project).__name__}"
            )
        checkout = _canonical_directory(
            checkout_directory,
            field_name=f"Project {project.key!r} checkout_directory",
        )
        manifest_path = _portable_relative_path(
            project_manifest_path,
            field_name="project_manifest_path",
        )
        state = _canonical_directory(
            state_directory,
            field_name="queue state_directory",
        )

        mount_values = _owned_sequence(mounts, field_name="mounts")
        mount_models: list[MountBinding] = []
        for index, mount in enumerate(mount_values):
            if type(mount) is not MountBinding:
                raise TypeError(
                    f"mounts[{index}] must be exactly MountBinding, got "
                    f"{type(mount).__name__}"
                )
            mount_models.append(mount)

        environment_values = _owned_sequence(
            environments,
            field_name="environments",
        )
        environment_models: list[EnvironmentBinding] = []
        for index, environment in enumerate(environment_values):
            if type(environment) is not EnvironmentBinding:
                raise TypeError(
                    f"environments[{index}] must be exactly EnvironmentBinding, "
                    f"got {type(environment).__name__}"
                )
            environment_models.append(environment)

        ignored_values = _owned_sequence(
            git_ignored_checkout_descendants,
            field_name="git_ignored_checkout_descendants",
        )
        ignored: list[Path] = []
        for index, value in enumerate(ignored_values):
            proof = _canonical_directory(
                value,
                field_name=f"git_ignored_checkout_descendants[{index}]",
            )
            if proof == checkout or checkout not in proof.parents:
                raise LifecycleValidationError(
                    f"Git-ignore proof {str(proof)!r} must be a strict descendant "
                    f"of checkout {str(checkout)!r}; verify it with Git at the "
                    "pinned commit"
                )
            ignored.append(proof)
        ignored_tuple = tuple(sorted(set(ignored), key=str))

        occupied_values = _owned_sequence(
            occupied_roots,
            field_name="occupied_roots",
        )
        occupied: list[HostRootClaim] = []
        for index, claim in enumerate(occupied_values):
            if type(claim) is not HostRootClaim:
                raise TypeError(
                    f"occupied_roots[{index}] must be exactly HostRootClaim, got "
                    f"{type(claim).__name__}"
                )
            if claim.project_key == project.key:
                raise LifecycleValidationError(
                    f"occupied_roots[{index}] is labeled with enrolling Project "
                    f"{project.key!r}; provide only other-project root claims"
                )
            occupied.append(claim)

        mounts_tuple = tuple(mount_models)
        environments_tuple = tuple(environment_models)
        _validate_complete_bindings(project, mounts_tuple, environments_tuple)

        logical_roots: list[tuple[str, Path]] = [
            (f"mount {mount.name!r}", mount.path) for mount in mounts_tuple
        ]
        for environment in environments_tuple:
            logical_roots.extend(
                (
                    f"environment {environment.name!r} search directory {index}",
                    path,
                )
                for index, path in enumerate(
                    environment.executable_search_directories
                )
            )
        _validate_root_relationships(
            checkout=checkout,
            logical_roots=tuple(logical_roots),
            state_directory=state,
            ignored_descendants=ignored_tuple,
            occupied_roots=tuple(occupied),
        )

        mount_by_name = {mount.name: mount for mount in mounts_tuple}
        ordered_mounts = tuple(
            mount_by_name[volume.name]
            for volume in project.volumes
            if volume.name in mount_by_name
        )
        environment_by_name = {
            environment.name: environment for environment in environments_tuple
        }
        ordered_environments = tuple(
            environment_by_name[environment.name]
            for environment in project.environments
        )
        artifact_roots = tuple(
            ArtifactRootBinding.from_mount(mount)
            for mount in ordered_mounts
            if mount.access is VolumeAccess.READ_WRITE
        )
        project_normalized_sha256 = sha256_bytes(
            canonical_json_bytes(project.to_document())
        )
        document: dict[str, JSONValue] = {
            "apiVersion": "experiment-queue/v1",
            "kind": "Enrollment",
            "projectKey": project.key,
            "projectNormalizedSha256": project_normalized_sha256,
            "checkoutDirectory": str(checkout),
            "projectManifestPath": manifest_path,
            "mounts": [mount.to_document() for mount in ordered_mounts],
            "artifactRoots": [
                artifact_root.to_document() for artifact_root in artifact_roots
            ],
            "environments": [
                environment.to_document() for environment in ordered_environments
            ],
            "gitIgnoredCheckoutDescendants": [
                str(path) for path in ignored_tuple
            ],
        }
        encoded = canonical_json_bytes(document)
        return cast(
            Self,
            _construct(
                cls,
                project_key=project.key,
                project_normalized_sha256=project_normalized_sha256,
                checkout_directory=checkout,
                project_manifest_path=manifest_path,
                mounts=ordered_mounts,
                artifact_roots=artifact_roots,
                environments=ordered_environments,
                git_ignored_checkout_descendants=ignored_tuple,
                canonical_json=encoded,
                sha256=sha256_bytes(encoded),
            ),
        )

    @property
    def enrollment_json(self) -> bytes:
        """Alias the exact RFC 8785 host-resolution bytes for persistence."""

        return self.canonical_json

    @property
    def enrollment_sha256(self) -> str:
        """Alias the digest paired with :attr:`enrollment_json`."""

        return self.sha256

    def to_document(self) -> dict[str, JSONValue]:
        """Decode a fresh JSON-native copy of the exact frozen Enrollment."""

        value = json.loads(self.canonical_json)
        assert type(value) is dict
        return cast(dict[str, JSONValue], value)

    def mount(self, name: str) -> MountBinding:
        """Return a bound mount or raise with the admitted choices."""

        for mount in self.mounts:
            if mount.name == name:
                return mount
        raise LifecycleValidationError(
            f"Enrollment for Project {self.project_key!r} has no mount {name!r}; "
            f"choose one of {[mount.name for mount in self.mounts]}"
        )

    def environment(self, name: str) -> EnvironmentBinding:
        """Return a bound environment or raise with the admitted choices."""

        for environment in self.environments:
            if environment.name == name:
                return environment
        raise LifecycleValidationError(
            f"Enrollment for Project {self.project_key!r} has no environment "
            f"{name!r}; choose one of "
            f"{[environment.name for environment in self.environments]}"
        )

    def artifact_root(self, name: str) -> ArtifactRootBinding:
        """Return a derived writable artifact root or fail closed."""

        for root in self.artifact_roots:
            if root.name == name:
                return root
        raise LifecycleValidationError(
            f"Enrollment for Project {self.project_key!r} has no writable artifact "
            f"root {name!r}; choose one of "
            f"{[root.name for root in self.artifact_roots]}"
        )

    def validate_current_paths(self) -> None:
        """Re-resolve frozen roots and reject removal or symlink-target drift.

        Enrollment creation stores canonical targets.  Revision creation and
        later use-time authorization call this check so replacing a stored path
        with a symlink cannot silently redirect work outside the admitted root.
        Git-ignore truth and cross-project inventory remain responsibilities of
        their trusted callers because neither is derivable from path strings.
        """

        directories: list[tuple[str, Path]] = [
            ("checkout_directory", self.checkout_directory)
        ]
        directories.extend(
            (f"mount {mount.name!r}", mount.path) for mount in self.mounts
        )
        for environment in self.environments:
            directories.extend(
                (
                    f"environment {environment.name!r} search directory {index}",
                    path,
                )
                for index, path in enumerate(
                    environment.executable_search_directories
                )
            )
        directories.extend(
            (f"Git-ignore proof {index}", path)
            for index, path in enumerate(self.git_ignored_checkout_descendants)
        )
        for label, stored in directories:
            current = _canonical_directory(stored, field_name=label)
            if current != stored:
                raise LifecycleValidationError(
                    f"{label} changed canonical target from {str(stored)!r} to "
                    f"{str(current)!r}; hold affected work and create a new "
                    "ProjectRevision only after revalidation"
                )
        for environment in self.environments:
            if environment.command_prefix_argv is None:
                continue
            stored_executable = environment.command_prefix_argv[0]
            current_executable = _canonical_executable(
                stored_executable,
                field_name=(
                    f"environment {environment.name!r} command prefix executable"
                ),
            )
            if current_executable != stored_executable:
                raise LifecycleValidationError(
                    f"environment {environment.name!r} command prefix executable "
                    f"changed canonical target from {stored_executable!r} to "
                    f"{current_executable!r}; recreate the ProjectRevision"
                )


@dataclass(frozen=True, slots=True, init=False)
class ProjectRevision(_FactoryOnly):
    """Append-only immutable Project source plus one frozen Enrollment."""

    id: int
    project_id: int
    project_key: str
    sequence: int
    label: str
    display_name: str
    git_commit: str
    project_source_path: str
    project_source: bytes = field(repr=False)
    project_source_sha256: str
    project_normalized_json: bytes = field(repr=False)
    project_normalized_sha256: str
    project_schema_api_version: str
    project_schema_kind: str
    project_schema_id: str
    project_schema_sha256: str
    extension_schema_source_path: str | None
    extension_schema_source: bytes | None = field(repr=False)
    extension_schema_source_sha256: str | None
    extension_schema_canonical_json: bytes | None = field(repr=False)
    extension_schema_canonical_sha256: str | None
    extension_schema_id: str | None
    validated_package_version: str
    project: Project = field(repr=False, hash=False)
    enrollment: Enrollment = field(repr=False, hash=False)
    created_actor: str
    created_at: str

    @classmethod
    def create(
        cls,
        *,
        revision_id: int,
        project_id: int,
        sequence: int,
        project: Project,
        project_source_path: str,
        project_source: bytes,
        git_commit: str,
        enrollment: Enrollment,
        created_actor: str,
        created_at: str,
        extension_schema_source: bytes | None = None,
    ) -> Self:
        """Create a revision using authenticated installed validator metadata."""

        if cls is not ProjectRevision:
            raise TypeError(
                "ProjectRevision.create() constructs exactly ProjectRevision"
            )
        return cls._create_validated(
            revision_id=revision_id,
            project_id=project_id,
            sequence=sequence,
            project=project,
            project_source_path=project_source_path,
            project_source=project_source,
            git_commit=git_commit,
            enrollment=enrollment,
            created_actor=created_actor,
            created_at=created_at,
            extension_schema_source=extension_schema_source,
            validated_package_version=_installed_package_version(),
        )

    @classmethod
    def _create_validated(
        cls,
        *,
        revision_id: int,
        project_id: int,
        sequence: int,
        project: Project,
        project_source_path: str,
        project_source: bytes,
        git_commit: str,
        enrollment: Enrollment,
        created_actor: str,
        created_at: str,
        extension_schema_source: bytes | None,
        validated_package_version: str,
    ) -> Self:
        """Validate exact source/evidence with already authenticated provenance."""

        if cls is not ProjectRevision:
            raise TypeError(
                "ProjectRevision validation constructs exactly ProjectRevision"
            )
        revision_identifier = _require_positive_integer(
            revision_id,
            field_name="revision_id",
        )
        registered_project_id = _require_positive_integer(
            project_id,
            field_name="project_id",
        )
        revision_sequence = _require_positive_integer(
            sequence,
            field_name="sequence",
        )
        if type(project) is not Project:
            raise TypeError(
                f"project must be exactly authoring.Project, got "
                f"{type(project).__name__}"
            )
        if type(enrollment) is not Enrollment:
            raise TypeError(
                f"enrollment must be exactly Enrollment, got "
                f"{type(enrollment).__name__}"
            )
        enrollment.validate_current_paths()
        source_path = _portable_relative_path(
            project_source_path,
            field_name="project_source_path",
        )
        if source_path != enrollment.project_manifest_path:
            raise LifecycleValidationError(
                f"project_source_path {source_path!r} does not equal Enrollment "
                f"projectManifestPath {enrollment.project_manifest_path!r}; the "
                "trusted Git resolver must read the named manifest path"
            )
        if type(project_source) is not bytes:
            raise TypeError(
                f"project_source must be exact bytes from the pinned Git tree, got "
                f"{type(project_source).__name__}"
            )
        try:
            parsed_project = Project.from_yaml(
                project_source,
                source_name=source_path,
            )
        except AuthoringValidationError as exc:
            raise LifecycleValidationError(
                f"project_source at {source_path!r} is not a valid Project/v1: "
                f"{exc}"
            ) from exc
        expected_normalized = canonical_json_bytes(project.to_document())
        parsed_normalized = canonical_json_bytes(parsed_project.to_document())
        if parsed_normalized != expected_normalized:
            raise LifecycleValidationError(
                "project_source does not normalize to the authoring.Project used "
                "for Enrollment; resolve and validate both from the same pinned "
                "Git object"
            )
        normalized_sha256 = sha256_bytes(parsed_normalized)
        if enrollment.project_key != parsed_project.key:
            raise LifecycleValidationError(
                f"Enrollment belongs to Project {enrollment.project_key!r}, but "
                f"project_source declares {parsed_project.key!r}; rebuild Enrollment "
                "for the resolved Project"
            )
        if enrollment.project_normalized_sha256 != normalized_sha256:
            raise LifecycleValidationError(
                "Enrollment projectNormalizedSha256 does not match project_source; "
                "host bindings must be recreated for this exact Project revision"
            )

        extension_reference = parsed_project.extension_schema
        extension_source_path: str | None = None
        extension_source: bytes | None = None
        extension_source_sha256: str | None = None
        extension_canonical_json: bytes | None = None
        extension_canonical_sha256: str | None = None
        extension_schema_id: str | None = None
        if extension_reference is None:
            if extension_schema_source is not None:
                raise LifecycleValidationError(
                    f"Project {parsed_project.key!r} does not declare "
                    "spec.extensionSchema, but extension_schema_source bytes were "
                    "supplied; omit them or commit a reference in the same revision"
                )
        else:
            if extension_schema_source is None:
                raise LifecycleValidationError(
                    f"Project {parsed_project.key!r} declares extension schema "
                    f"{extension_reference.path!r}, but exact source bytes are "
                    "missing; read that path from the pinned Git tree"
                )
            try:
                extension = load_extension_schema(
                    extension_schema_source,
                    extension_reference,
                    source_name=extension_reference.path,
                )
            except (ExtensionSchemaError, TypeError) as exc:
                raise LifecycleValidationError(
                    f"Project extension schema {extension_reference.path!r} is "
                    f"invalid: {exc}"
                ) from exc
            extension_source_path = extension.reference_path
            extension_source = extension.source_bytes
            extension_source_sha256 = extension.source_sha256
            extension_canonical_json = extension.canonical_bytes
            extension_canonical_sha256 = extension.canonical_sha256
            extension_schema_id = extension.schema_id

        actor = _require_text(
            created_actor,
            field_name="created_actor",
            maximum=256,
        )
        timestamp = _require_timestamp(created_at, field_name="created_at")
        key = parsed_project.key
        return cast(
            Self,
            _construct(
                cls,
                id=revision_identifier,
                project_id=registered_project_id,
                project_key=key,
                sequence=revision_sequence,
                label=f"{key}:r{revision_sequence}",
                display_name=parsed_project.display_name,
                git_commit=_require_full_git_object(git_commit),
                project_source_path=source_path,
                project_source=project_source,
                project_source_sha256=sha256_bytes(project_source),
                project_normalized_json=parsed_normalized,
                project_normalized_sha256=normalized_sha256,
                project_schema_api_version=PROJECT_V1_SCHEMA.protocol.api_version,
                project_schema_kind=PROJECT_V1_SCHEMA.protocol.kind.value,
                project_schema_id=PROJECT_V1_SCHEMA.schema_id,
                project_schema_sha256=_require_sha256(
                    PROJECT_V1_SCHEMA.sha256,
                    field_name="bundled Project/v1 schema digest",
                ),
                extension_schema_source_path=extension_source_path,
                extension_schema_source=extension_source,
                extension_schema_source_sha256=extension_source_sha256,
                extension_schema_canonical_json=extension_canonical_json,
                extension_schema_canonical_sha256=extension_canonical_sha256,
                extension_schema_id=extension_schema_id,
                validated_package_version=_validated_package_version(
                    validated_package_version
                ),
                project=parsed_project,
                enrollment=enrollment,
                created_actor=actor,
                created_at=timestamp,
            ),
        )

    @classmethod
    def from_recorded_evidence(
        cls,
        *,
        revision_id: int,
        project_id: int,
        sequence: int,
        recorded_revision_label: str,
        recorded_display_name: str,
        project: Project,
        project_source_path: str,
        project_source: bytes,
        project_source_sha256: str,
        project_normalized_json: bytes,
        project_normalized_sha256: str,
        project_schema_api_version: str,
        project_schema_kind: str,
        project_schema_id: str,
        project_schema_sha256: str,
        git_commit: str,
        enrollment: Enrollment,
        enrollment_json: bytes,
        enrollment_sha256: str,
        extension_schema_source: bytes | None,
        extension_schema_source_path: str | None,
        extension_schema_source_sha256: str | None,
        extension_schema_canonical_json: bytes | None,
        extension_schema_canonical_sha256: str | None,
        extension_schema_id: str | None,
        validated_package_version: str,
        created_actor: str,
        created_at: str,
    ) -> Self:
        """Rehydrate old-version rows only after exact evidence comparison.

        Unlike :meth:`create`, this narrow persistence boundary retains the
        authenticated validator version recorded when the revision was made.
        It recomputes every digest and schema field from exact source bytes and
        rejects a row that differs before returning a trusted model.
        """

        if cls is not ProjectRevision:
            raise TypeError(
                "ProjectRevision.from_recorded_evidence() constructs exactly "
                "ProjectRevision"
            )
        candidate = cls._create_validated(
            revision_id=revision_id,
            project_id=project_id,
            sequence=sequence,
            project=project,
            project_source_path=project_source_path,
            project_source=project_source,
            git_commit=git_commit,
            enrollment=enrollment,
            created_actor=created_actor,
            created_at=created_at,
            extension_schema_source=extension_schema_source,
            validated_package_version=validated_package_version,
        )
        recorded: dict[str, object] = {
            "revision_label": recorded_revision_label,
            "display_name": recorded_display_name,
            "project_source_sha256": project_source_sha256,
            "project_normalized_json": project_normalized_json,
            "project_normalized_sha256": project_normalized_sha256,
            "project_schema_api_version": project_schema_api_version,
            "project_schema_kind": project_schema_kind,
            "project_schema_id": project_schema_id,
            "project_schema_sha256": project_schema_sha256,
            "enrollment_json": enrollment_json,
            "enrollment_sha256": enrollment_sha256,
            "extension_schema_source_path": extension_schema_source_path,
            "extension_schema_source_sha256": extension_schema_source_sha256,
            "extension_schema_canonical_json": extension_schema_canonical_json,
            "extension_schema_canonical_sha256": (
                extension_schema_canonical_sha256
            ),
            "extension_schema_id": extension_schema_id,
        }
        recomputed: dict[str, object] = {
            "revision_label": candidate.label,
            "display_name": candidate.display_name,
            "project_source_sha256": candidate.project_source_sha256,
            "project_normalized_json": candidate.project_normalized_json,
            "project_normalized_sha256": candidate.project_normalized_sha256,
            "project_schema_api_version": candidate.project_schema_api_version,
            "project_schema_kind": candidate.project_schema_kind,
            "project_schema_id": candidate.project_schema_id,
            "project_schema_sha256": candidate.project_schema_sha256,
            "enrollment_json": candidate.enrollment.canonical_json,
            "enrollment_sha256": candidate.enrollment.sha256,
            "extension_schema_source_path": candidate.extension_schema_source_path,
            "extension_schema_source_sha256": (
                candidate.extension_schema_source_sha256
            ),
            "extension_schema_canonical_json": (
                candidate.extension_schema_canonical_json
            ),
            "extension_schema_canonical_sha256": (
                candidate.extension_schema_canonical_sha256
            ),
            "extension_schema_id": candidate.extension_schema_id,
        }
        mismatches = [
            name for name, value in recorded.items() if value != recomputed[name]
        ]
        if mismatches:
            raise LifecycleValidationError(
                "recorded ProjectRevision evidence differs from exact recomputed "
                f"source/schema/Enrollment evidence in fields {mismatches}; refuse "
                "to load the row without reinterpretation"
            )
        return candidate

    def to_document(self) -> dict[str, JSONValue]:
        """Return revision identity and digests without embedding source bytes."""

        document: dict[str, JSONValue] = {
            "id": self.id,
            "projectId": self.project_id,
            "projectKey": self.project_key,
            "sequence": self.sequence,
            "label": self.label,
            "displayName": self.display_name,
            "gitCommit": self.git_commit,
            "projectSourcePath": self.project_source_path,
            "projectSourceSha256": self.project_source_sha256,
            "projectNormalizedSha256": self.project_normalized_sha256,
            "projectSchema": {
                "apiVersion": self.project_schema_api_version,
                "kind": self.project_schema_kind,
                "id": self.project_schema_id,
                "sha256": self.project_schema_sha256,
            },
            "enrollmentSha256": self.enrollment.sha256,
            "validatedPackageVersion": self.validated_package_version,
            "createdActor": self.created_actor,
            "createdAt": self.created_at,
        }
        if self.extension_schema_source_path is not None:
            extension: dict[str, JSONValue] = {
                "path": self.extension_schema_source_path,
                "sourceSha256": self.extension_schema_source_sha256,
                "canonicalSha256": self.extension_schema_canonical_sha256,
            }
            if self.extension_schema_id is not None:
                extension["schemaId"] = self.extension_schema_id
            document["extensionSchema"] = extension
        return document


def _lifecycle(value: ProjectLifecycle | str, *, field_name: str) -> ProjectLifecycle:
    try:
        return value if type(value) is ProjectLifecycle else ProjectLifecycle(value)
    except (TypeError, ValueError) as exc:
        raise LifecycleValidationError(
            f"{field_name} must be one of {[state.value for state in ProjectLifecycle]}, "
            f"got {value!r}"
        ) from exc


def validate_lifecycle_transition(
    current: ProjectLifecycle | str,
    target: ProjectLifecycle | str,
    *,
    queue_item_states: Sequence[str] = (),
    incomplete_cleanup: bool = False,
) -> ProjectLifecycle:
    """Validate one explicit lifecycle edge and archival preconditions.

    The caller supplies every queue-item state for archival.  Unknown states
    fail closed so a new database state cannot accidentally bypass the archive
    gate.  This function never mutates state.
    """

    current_state = _lifecycle(current, field_name="current lifecycle")
    target_state = _lifecycle(target, field_name="target lifecycle")
    allowed = {
        ProjectLifecycle.ACTIVE: {ProjectLifecycle.PAUSED},
        ProjectLifecycle.PAUSED: {
            ProjectLifecycle.ACTIVE,
            ProjectLifecycle.ARCHIVED,
        },
        ProjectLifecycle.ARCHIVED: set(),
    }
    if target_state not in allowed[current_state]:
        if current_state is ProjectLifecycle.ARCHIVED:
            detail = "archival is permanent in version 1"
        elif target_state is current_state:
            detail = "no-op lifecycle transitions are not audit events"
        else:
            detail = "projects must be paused before archival"
        raise LifecycleValidationError(
            f"cannot transition Project from {current_state.value!r} to "
            f"{target_state.value!r}: {detail}"
        )

    if target_state is not ProjectLifecycle.ARCHIVED:
        return target_state
    if type(incomplete_cleanup) is not bool:
        raise LifecycleValidationError(
            "incomplete_cleanup must be a boolean derived from ref/worktree "
            "cleanup evidence"
        )
    states = _owned_sequence(queue_item_states, field_name="queue_item_states")
    normalized_states: list[str] = []
    for index, state in enumerate(states):
        state_name = _require_text(
            state,
            field_name=f"queue_item_states[{index}]",
            maximum=64,
        )
        if state_name not in _KNOWN_QUEUE_STATES:
            raise LifecycleValidationError(
                f"queue_item_states[{index}] has unknown state {state_name!r}; "
                "update lifecycle validation before treating a new queue state as "
                "safe for archival"
            )
        normalized_states.append(state_name)
    blocking = sorted(
        state for state in normalized_states if state in ARCHIVE_BLOCKING_QUEUE_STATES
    )
    if blocking:
        raise LifecycleValidationError(
            f"cannot archive Project while nonterminal queue items remain in states "
            f"{blocking}; wait for terminal states and completed cleanup"
        )
    if incomplete_cleanup:
        raise LifecycleValidationError(
            "cannot archive Project while ref or worktree cleanup is incomplete; "
            "finish or explicitly repair cleanup evidence first"
        )
    return target_state


@dataclass(frozen=True, slots=True, init=False)
class RegisteredProject(_FactoryOnly):
    """Stable database identity and current revision for one registered Project."""

    id: int
    key: str
    display_name: str
    lifecycle: ProjectLifecycle
    current_revision_id: int
    current_revision_sequence: int
    lifecycle_reason: str
    lifecycle_actor: str
    lifecycle_changed_at: str

    @classmethod
    def register(
        cls,
        *,
        revision: ProjectRevision,
        initial_lifecycle: ProjectLifecycle | str = ProjectLifecycle.ACTIVE,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> Self:
        """Create a registered Project from its complete first revision."""

        if cls is not RegisteredProject:
            raise TypeError(
                "RegisteredProject.register() constructs exactly RegisteredProject"
            )
        if type(revision) is not ProjectRevision:
            raise TypeError(
                f"revision must be exactly ProjectRevision, got "
                f"{type(revision).__name__}"
            )
        state = _lifecycle(initial_lifecycle, field_name="initial_lifecycle")
        if state is ProjectLifecycle.ARCHIVED:
            raise LifecycleValidationError(
                "registration may start active or explicitly paused, not archived; "
                "archival requires a later audited transition from paused"
            )
        if revision.sequence != 1:
            raise LifecycleValidationError(
                f"registration requires the first ProjectRevision sequence to be 1, "
                f"got {revision.sequence}; later sequences are gap-tolerant"
            )
        return cast(
            Self,
            _construct(
                cls,
                id=revision.project_id,
                key=revision.project_key,
                display_name=revision.display_name,
                lifecycle=state,
                current_revision_id=revision.id,
                current_revision_sequence=revision.sequence,
                lifecycle_reason=_require_text(
                    reason,
                    field_name="lifecycle reason",
                    maximum=4000,
                ),
                lifecycle_actor=_require_text(
                    actor,
                    field_name="lifecycle actor",
                    maximum=256,
                ),
                lifecycle_changed_at=_require_timestamp(
                    changed_at,
                    field_name="lifecycle changed_at",
                ),
            ),
        )

    @classmethod
    def adopt_imported_history(
        cls,
        *,
        revision: ProjectRevision,
        lifecycle: ProjectLifecycle | str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> Self:
        """Represent a typed current revision appended after imported history.

        A legacy-v4 import owns sequence 1 but cannot be reinterpreted as a
        portable ProjectRevision.  The first resolver-authenticated Project/v1
        revision therefore has a sequence greater than one.  This narrow
        factory records that adoption boundary without pretending the typed
        revision was the Project's original registration revision.
        """

        if cls is not RegisteredProject:
            raise TypeError(
                "RegisteredProject.adopt_imported_history() constructs exactly "
                "RegisteredProject"
            )
        if type(revision) is not ProjectRevision:
            raise TypeError(
                f"revision must be exactly ProjectRevision, got "
                f"{type(revision).__name__}"
            )
        if revision.sequence <= 1:
            raise LifecycleValidationError(
                "adopting imported history requires a typed ProjectRevision "
                f"sequence greater than 1, got {revision.sequence}; use "
                "RegisteredProject.register() for a new Project"
            )
        state = _lifecycle(lifecycle, field_name="lifecycle")
        return cast(
            Self,
            _construct(
                cls,
                id=revision.project_id,
                key=revision.project_key,
                display_name=revision.display_name,
                lifecycle=state,
                current_revision_id=revision.id,
                current_revision_sequence=revision.sequence,
                lifecycle_reason=_require_text(
                    reason,
                    field_name="lifecycle reason",
                    maximum=4000,
                ),
                lifecycle_actor=_require_text(
                    actor,
                    field_name="lifecycle actor",
                    maximum=256,
                ),
                lifecycle_changed_at=_require_timestamp(
                    changed_at,
                    field_name="lifecycle changed_at",
                ),
            ),
        )

    def with_current_revision(self, revision: ProjectRevision) -> Self:
        """Activate a newer append-only revision without changing lifecycle."""

        if type(revision) is not ProjectRevision:
            raise TypeError(
                f"revision must be exactly ProjectRevision, got "
                f"{type(revision).__name__}"
            )
        if self.lifecycle is ProjectLifecycle.ARCHIVED:
            raise LifecycleValidationError(
                f"Project {self.key!r} is archived; version 1 forbids revision "
                "creation or activation"
            )
        if revision.project_id != self.id or revision.project_key != self.key:
            raise LifecycleValidationError(
                f"revision {revision.label!r} belongs to Project id/key "
                f"({revision.project_id}, {revision.project_key!r}), not registered "
                f"Project ({self.id}, {self.key!r})"
            )
        if revision.sequence <= self.current_revision_sequence:
            raise LifecycleValidationError(
                f"revision {revision.label!r} sequence must be greater than current "
                f"sequence {self.current_revision_sequence}; reverting content "
                "creates a new gap-tolerant sequence instead of reactivating a row"
            )
        if revision.id == self.current_revision_id:
            raise LifecycleValidationError(
                f"revision {revision.label!r} reuses current revision id "
                f"{revision.id}; append a distinct immutable revision row"
            )
        return cast(
            Self,
            _construct(
                RegisteredProject,
                id=self.id,
                key=self.key,
                display_name=revision.display_name,
                lifecycle=self.lifecycle,
                current_revision_id=revision.id,
                current_revision_sequence=revision.sequence,
                lifecycle_reason=self.lifecycle_reason,
                lifecycle_actor=self.lifecycle_actor,
                lifecycle_changed_at=self.lifecycle_changed_at,
            ),
        )

    def transition(
        self,
        target: ProjectLifecycle | str,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        queue_item_states: Sequence[str] = (),
        incomplete_cleanup: bool = False,
    ) -> Self:
        """Return a new registered-project value after an authorized transition."""

        target_state = validate_lifecycle_transition(
            self.lifecycle,
            target,
            queue_item_states=queue_item_states,
            incomplete_cleanup=incomplete_cleanup,
        )
        return cast(
            Self,
            _construct(
                RegisteredProject,
                id=self.id,
                key=self.key,
                display_name=self.display_name,
                lifecycle=target_state,
                current_revision_id=self.current_revision_id,
                current_revision_sequence=self.current_revision_sequence,
                lifecycle_reason=_require_text(
                    reason,
                    field_name="lifecycle reason",
                    maximum=4000,
                ),
                lifecycle_actor=_require_text(
                    actor,
                    field_name="lifecycle actor",
                    maximum=256,
                ),
                lifecycle_changed_at=_require_timestamp(
                    changed_at,
                    field_name="lifecycle changed_at",
                ),
            ),
        )

    @property
    def admission_allowed(self) -> bool:
        """Paused Projects remain admissible; archived Projects do not."""

        return self.lifecycle is not ProjectLifecycle.ARCHIVED

    @property
    def revision_creation_allowed(self) -> bool:
        """Paused Projects may append revisions; archived Projects may not."""

        return self.lifecycle is not ProjectLifecycle.ARCHIVED

    @property
    def dispatch_allowed_by_lifecycle(self) -> bool:
        """Only active Projects may start new work."""

        return self.lifecycle is ProjectLifecycle.ACTIVE

    def dispatch_allowed(self, runtime_state: ProjectRuntimeState) -> bool:
        """Combine lifecycle and the separate Project health circuit."""

        if type(runtime_state) is not ProjectRuntimeState:
            raise TypeError(
                f"runtime_state must be exactly ProjectRuntimeState, got "
                f"{type(runtime_state).__name__}"
            )
        if runtime_state.project_id != self.id or runtime_state.project_key != self.key:
            raise LifecycleValidationError(
                "runtime_state identity does not match the registered Project; "
                "never use another Project's circuit state for dispatch"
            )
        return self.dispatch_allowed_by_lifecycle and not runtime_state.blocks_dispatch


@dataclass(frozen=True, slots=True, init=False)
class ProjectRuntimeState(_FactoryOnly):
    """Project-scoped health circuit state, independent of operator lifecycle."""

    project_id: int
    project_key: str
    health: ProjectHealth
    circuit_failure_count: int
    health_reason: str
    health_actor: str
    health_changed_at: str

    @classmethod
    def create(
        cls,
        *,
        project_id: int,
        project_key: str,
        health: ProjectHealth | str = ProjectHealth.CLOSED,
        circuit_failure_count: int = 0,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> Self:
        """Create validated project-scoped circuit evidence."""

        if cls is not ProjectRuntimeState:
            raise TypeError(
                "ProjectRuntimeState.create() constructs exactly ProjectRuntimeState"
            )
        try:
            parsed_health = (
                health if type(health) is ProjectHealth else ProjectHealth(health)
            )
        except (TypeError, ValueError) as exc:
            raise LifecycleValidationError(
                "health must be 'closed' or 'open'"
            ) from exc
        failure_count = _require_nonnegative_integer(
            circuit_failure_count,
            field_name="circuit_failure_count",
        )
        if parsed_health is ProjectHealth.OPEN and failure_count == 0:
            raise LifecycleValidationError(
                "an open Project health circuit requires a positive "
                "circuit_failure_count"
            )
        return cast(
            Self,
            _construct(
                cls,
                project_id=_require_positive_integer(
                    project_id,
                    field_name="project_id",
                ),
                project_key=_require_project_key(
                    project_key,
                    field_name="project_key",
                ),
                health=parsed_health,
                circuit_failure_count=failure_count,
                health_reason=_require_text(
                    reason,
                    field_name="health reason",
                    maximum=4000,
                ),
                health_actor=_require_text(
                    actor,
                    field_name="health actor",
                    maximum=256,
                ),
                health_changed_at=_require_timestamp(
                    changed_at,
                    field_name="health changed_at",
                ),
            ),
        )

    @property
    def blocks_dispatch(self) -> bool:
        """Return whether this Project's circuit blocks only its new dispatch."""

        return self.health is ProjectHealth.OPEN

    def record_failure(
        self,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        open_circuit: bool,
    ) -> Self:
        """Increment failure evidence and optionally open the circuit."""

        if type(open_circuit) is not bool:
            raise LifecycleValidationError("open_circuit must be a boolean")
        return ProjectRuntimeState.create(
            project_id=self.project_id,
            project_key=self.project_key,
            health=(ProjectHealth.OPEN if open_circuit else self.health),
            circuit_failure_count=self.circuit_failure_count + 1,
            reason=reason,
            actor=actor,
            changed_at=changed_at,
        )

    def close_circuit(
        self,
        *,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> Self:
        """Close the Project circuit and reset its consecutive failure count."""

        return ProjectRuntimeState.create(
            project_id=self.project_id,
            project_key=self.project_key,
            health=ProjectHealth.CLOSED,
            circuit_failure_count=0,
            reason=reason,
            actor=actor,
            changed_at=changed_at,
        )


__all__ = [
    "ARCHIVE_BLOCKING_QUEUE_STATES",
    "ARCHIVE_TERMINAL_QUEUE_STATES",
    "ArtifactRootBinding",
    "Enrollment",
    "EnvironmentBinding",
    "HostRootClaim",
    "LifecycleValidationError",
    "MountBinding",
    "ProjectHealth",
    "ProjectLifecycle",
    "ProjectRevision",
    "ProjectRuntimeState",
    "RegisteredProject",
    "validate_lifecycle_transition",
]
