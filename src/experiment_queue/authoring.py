"""Expose validated, immutable Project/v1 and ExperimentCard/v1 authoring models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from types import MappingProxyType
from typing import Final, Mapping, Self, TypeAlias, TypeVar, cast

from jsonschema.exceptions import ValidationError

from experiment_queue.protocols import (
    EXPERIMENT_CARD_V1,
    PROJECT_V1,
    ProtocolVersion,
)
from experiment_queue.schema_registry import (
    SemanticValidationError,
    validate_bundled_document,
)
from experiment_queue.serialization import (
    CanonicalJSONError,
    JSONScalar,
    JSONValue,
    MAX_NESTING_DEPTH,
    StrictYAMLError,
    load_strict_yaml,
)


FrozenJSONValue: TypeAlias = (
    JSONScalar | tuple["FrozenJSONValue", ...] | Mapping[str, "FrozenJSONValue"]
)
FrozenJSONObject: TypeAlias = Mapping[str, FrozenJSONValue]

# The service owns GPU selection and every queue-prefixed variable. Project
# environment inheritance may therefore never admit these host values.
RESERVED_ENVIRONMENT_VARIABLES: Final = frozenset({"CUDA_VISIBLE_DEVICES"})
RESERVED_ENVIRONMENT_PREFIX: Final = "EXPERIMENT_QUEUE_"
_PLACEHOLDER_TOKEN_PATTERN: Final = re.compile(
    r"\$\{[^}]*\}?|\{\{.*?(?:\}\}|$)",
    flags=re.DOTALL,
)


class AuthoringValidationError(ValueError):
    """Raised when a portable authoring document cannot form a valid model."""


_DocumentViewT = TypeVar("_DocumentViewT", bound="_DocumentView")


class VolumeAccess(StrEnum):
    """Portable access requested for a logical Project volume."""

    READ_ONLY = "readOnly"
    READ_WRITE = "readWrite"


class EnvironmentInheritance(StrEnum):
    """Host-environment inheritance admitted by a Project."""

    NONE = "none"
    ALLOWLIST = "allowlist"


class JobRole(StrEnum):
    """A job's explicit role in an independently schedulable card."""

    INDEPENDENT = "independent"
    COORDINATOR = "coordinator"
    WORKER = "worker"


class ArtifactType(StrEnum):
    """Portable artifact shape declared by a job."""

    FILE = "file"
    DIRECTORY = "directory"


def is_reserved_environment_variable(name: str) -> bool:
    """Return whether child-environment construction must own ``name``."""

    if type(name) is not str:
        raise TypeError(
            f"environment variable name must be a string, got {type(name).__name__}"
        )
    return (
        name in RESERVED_ENVIRONMENT_VARIABLES
        or name.startswith(RESERVED_ENVIRONMENT_PREFIX)
    )


def _freeze_json(value: JSONValue) -> FrozenJSONValue:
    """Detach and recursively freeze an already validated JSON value."""

    if type(value) is dict:
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if type(value) is list:
        return tuple(_freeze_json(item) for item in value)
    return cast(JSONScalar, value)


def _freeze_object(value: dict[str, JSONValue]) -> FrozenJSONObject:
    frozen = _freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _thaw_json(value: FrozenJSONValue) -> JSONValue:
    """Return a fresh JSON-native copy of a recursively frozen value."""

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _fresh_document(document: FrozenJSONObject) -> dict[str, JSONValue]:
    result = _thaw_json(document)
    assert type(result) is dict
    return result


def _copy_untrusted_json(
    value: object,
    *,
    path: str = "$",
    depth: int = 0,
    active_container_ids: set[int] | None = None,
) -> object:
    """Take an owned snapshot before validating a caller's mutable containers."""

    if depth > MAX_NESTING_DEPTH:
        raise AuthoringValidationError(
            f"authoring document {path} exceeds the maximum nesting depth of "
            f"{MAX_NESTING_DEPTH}; reduce its nesting"
        )
    value_type = type(value)
    if value_type not in (dict, list):
        return value

    if active_container_ids is None:
        active_container_ids = set()
    container_id = id(value)
    if container_id in active_container_ids:
        raise AuthoringValidationError(
            f"authoring document {path} contains a recursive container; replace it "
            "with an acyclic JSON value"
        )
    active_container_ids.add(container_id)
    try:
        if value_type is list:
            # Snapshot this level before descending so later caller mutation
            # cannot alter which children form the owned document.
            items = tuple(cast(list[object], value))
            return [
                _copy_untrusted_json(
                    item,
                    path=f"{path}[{index}]",
                    depth=depth + 1,
                    active_container_ids=active_container_ids,
                )
                for index, item in enumerate(items)
            ]

        items = tuple(cast(dict[object, object], value).items())
        return {
            key: _copy_untrusted_json(
                item,
                path=_json_member_path(path, key) if type(key) is str else path,
                depth=depth + 1,
                active_container_ids=active_container_ids,
            )
            for key, item in items
        }
    finally:
        active_container_ids.remove(container_id)


def _object(value: object) -> dict[str, JSONValue]:
    """Narrow a value whose object shape was established by the schema."""

    assert type(value) is dict
    return cast(dict[str, JSONValue], value)


def _array(value: object) -> list[JSONValue]:
    """Narrow a value whose array shape was established by the schema."""

    assert type(value) is list
    return cast(list[JSONValue], value)


def _string(value: object) -> str:
    """Narrow a value whose string shape was established by the schema."""

    assert type(value) is str
    return value


def _optional_string(document: Mapping[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is None:
        return None
    return _string(value)


def _format_schema_error(document_name: str, error: ValidationError) -> str:
    path = error.json_path or "$"
    return (
        f"{document_name} is invalid at {path}: {error.message}; "
        f"fix the document to satisfy the bundled {document_name}/v1 schema"
    )


def _validated_snapshot(
    protocol: ProtocolVersion,
    document: Mapping[str, object],
    *,
    document_name: str,
) -> dict[str, JSONValue]:
    """Detach caller-owned data, then validate the exact owned snapshot."""

    if type(document) is not dict:
        raise AuthoringValidationError(
            f"{document_name} must be a JSON object, got {type(document).__name__}; "
            "load or supply one complete authoring document"
        )
    try:
        snapshot = _copy_untrusted_json(document)
    except RuntimeError as exc:
        raise AuthoringValidationError(
            f"{document_name} changed while its owned validation snapshot was being "
            "copied; stop mutating the input and retry construction"
        ) from exc
    assert type(snapshot) is dict
    try:
        validate_bundled_document(protocol, snapshot)
    except ValidationError as exc:
        raise AuthoringValidationError(
            _format_schema_error(document_name, exc)
        ) from exc
    except SemanticValidationError as exc:
        raise AuthoringValidationError(
            f"{document_name} has invalid logical references or identities: {exc}; "
            "make the referenced names unique and internally consistent"
        ) from exc
    except CanonicalJSONError as exc:
        raise AuthoringValidationError(
            f"{document_name} is outside the portable JSON domain: {exc}; "
            "use only finite JSON-native values, string keys, and safe integers"
        ) from exc

    # Validation ran against this exact owned dict; no caller reference can
    # alter the model between the trust decision and recursive freezing.
    return cast(dict[str, JSONValue], snapshot)


def _load_yaml_document(
    source: bytes,
    *,
    source_name: str,
    document_name: str,
) -> Mapping[str, object]:
    try:
        document = load_strict_yaml(source, source_name=source_name)
    except (StrictYAMLError, TypeError) as exc:
        raise AuthoringValidationError(
            f"could not load {document_name} from {source_name}: {exc}"
        ) from exc
    if type(document) is not dict:
        raise AuthoringValidationError(
            f"could not load {document_name} from {source_name}: the YAML root "
            f"must be an object, got {type(document).__name__}"
        )
    return cast(dict[str, object], document)


def _json_member_path(path: str, key: str) -> str:
    if key.isidentifier():
        return f"{path}.{key}"
    return f"{path}[{key!r}]"


def _placeholder_token(value: str) -> str | None:
    match = _PLACEHOLDER_TOKEN_PATTERN.search(value)
    return None if match is None else match.group(0)


def _parameter_placeholder(
    value: JSONValue,
    *,
    path: str,
) -> tuple[str, str] | None:
    """Find the first parameter sentinel or token without recursive calls."""

    pending: list[tuple[JSONValue, str]] = [(value, path)]
    while pending:
        current, current_path = pending.pop()
        if type(current) is dict:
            if "$binding" in current:
                return _json_member_path(current_path, "$binding"), "$binding"
            for key, item in reversed(tuple(current.items())):
                item_path = _json_member_path(current_path, key)
                token = _placeholder_token(key)
                if token is not None:
                    return item_path, token
                pending.append((item, item_path))
        elif type(current) is list:
            for index in range(len(current) - 1, -1, -1):
                pending.append((current[index], f"{current_path}[{index}]"))
        elif type(current) is str:
            token = _placeholder_token(current)
            if token is not None:
                return current_path, token
    return None


def _reject_unresolved_placeholders(document: dict[str, JSONValue]) -> None:
    """Reject tokens in parameter trees and structured execution fields."""

    spec = _object(document["spec"])
    fields: list[tuple[JSONValue, str]] = [
        (spec["parameters"], "$.spec.parameters")
    ]
    for index, job_value in enumerate(_array(spec["jobs"])):
        job = _object(job_value)
        if "parameters" in job:
            fields.append(
                (job["parameters"], f"$.spec.jobs[{index}].parameters")
            )
        job_path = f"$.spec.jobs[{index}]"
        if "workingDirectory" in job:
            fields.append((job["workingDirectory"], f"{job_path}.workingDirectory"))

        command = _object(job["command"])
        command_type = _string(command["type"])
        if command_type == "argv":
            for argument_index, argument in enumerate(_array(command["argv"])):
                fields.append(
                    (argument, f"{job_path}.command.argv[{argument_index}]")
                )
        elif command_type == "wrapper":
            fields.append((command["path"], f"{job_path}.command.path"))
            for argument_index, argument in enumerate(
                _array(command.get("args", []))
            ):
                fields.append(
                    (argument, f"{job_path}.command.args[{argument_index}]")
                )
        # Shell scripts are the explicit compatibility escape hatch. Their
        # expansion syntax is owned by the invoked shell, not this authoring API.

        for artifact_index, artifact_value in enumerate(
            _array(job.get("artifacts", []))
        ):
            artifact = _object(artifact_value)
            fields.append(
                (
                    artifact["path"],
                    f"{job_path}.artifacts[{artifact_index}].path",
                )
            )

    for value, path in fields:
        placeholder = _parameter_placeholder(value, path=path)
        if placeholder is None:
            continue
        placeholder_path, token = placeholder
        if token == "$binding":
            raise AuthoringValidationError(
                f"ExperimentCard contains unsupported '$binding' interpolation at "
                f"{placeholder_path}; version 1 supports whole-parameter submission "
                "overrides, so store a literal value in the card and override that "
                "complete parameter at submission"
            )
        raise AuthoringValidationError(
            f"ExperimentCard contains unresolved placeholder token {token!r} at "
            f"{placeholder_path}; version 1 does not interpolate parameter, argv, "
            "wrapper, working-directory, or artifact-path strings, so store the "
            "resolved literal or use a whole-parameter submission override for "
            "spec.parameters"
        )


def _reject_reserved_environment_variables(
    document: dict[str, JSONValue],
) -> None:
    """Keep queue-owned and GPU-selection variables out of Project policy."""

    spec = _object(document["spec"])
    policy = _object(spec["environmentPolicy"])
    allow_variables = tuple(
        _string(value) for value in _array(policy["allowVariables"])
    )
    reserved = [
        name for name in allow_variables if is_reserved_environment_variable(name)
    ]
    if reserved:
        raise AuthoringValidationError(
            "Project spec.environmentPolicy.allowVariables contains service-reserved "
            f"variables {reserved}; remove CUDA_VISIBLE_DEVICES and all "
            "EXPERIMENT_QUEUE_* names because the queue service owns GPU assignment "
            "and execution metadata"
        )


class _DocumentView:
    """Shared fresh-document behavior for immutable typed views."""

    __slots__ = ()
    _document: FrozenJSONObject

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is a validated view obtained from "
            "Project or ExperimentCard; construct the root with "
            "from_document() or from_yaml()"
        )

    def to_document(self) -> dict[str, JSONValue]:
        """Return a fresh JSON-native copy with the original normalized shape."""

        return _fresh_document(self._document)


def _construct_view(
    model_type: type[_DocumentViewT],
    **values: object,
) -> _DocumentViewT:
    """Populate a frozen nested view after its owning root was validated."""

    instance = object.__new__(model_type)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


@dataclass(frozen=True, slots=True, init=False)
class LogicalVolume(_DocumentView):
    """Immutable typed view of one Project logical-volume declaration."""

    name: str
    access: VolumeAccess
    required: bool | None
    description: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class ProjectEnvironment(_DocumentView):
    """Immutable typed view of one portable execution environment name."""

    name: str
    description: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class EnvironmentPolicy(_DocumentView):
    """Immutable Project policy for service-environment inheritance."""

    inherit: EnvironmentInheritance
    allow_variables: tuple[str, ...]
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class ExtensionSchemaReference(_DocumentView):
    """Immutable portable reference to a project's extension schema."""

    path: str
    sha256: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class ArgvCommand(_DocumentView):
    """Preferred structured argv command; no shell parsing is implied."""

    argv: tuple[str, ...]
    _document: FrozenJSONObject = field(repr=False, hash=False)

    @property
    def type(self) -> str:
        """Return the stable serialized command discriminator."""

        return "argv"


@dataclass(frozen=True, slots=True, init=False)
class WrapperCommand(_DocumentView):
    """Portable repository-relative wrapper command."""

    path: str
    args: tuple[str, ...]
    _document: FrozenJSONObject = field(repr=False, hash=False)

    @property
    def type(self) -> str:
        """Return the stable serialized command discriminator."""

        return "wrapper"


@dataclass(frozen=True, slots=True, init=False)
class ShellCommand(_DocumentView):
    """Explicit compatibility-only shell command and its required rationale."""

    script: str
    compatibility_reason: str
    _document: FrozenJSONObject = field(repr=False, hash=False)

    @property
    def type(self) -> str:
        """Return the stable serialized command discriminator."""

        return "shell"


Command: TypeAlias = ArgvCommand | WrapperCommand | ShellCommand


@dataclass(frozen=True, slots=True, init=False)
class JobResources(_DocumentView):
    """Immutable optional resource requests for one independently scheduled job."""

    gpus: int | None
    cpus: int | None
    memory_bytes: int | None
    wall_time_seconds: int | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class Artifact(_DocumentView):
    """Immutable typed view of one logical job artifact."""

    name: str
    root: str
    path: str
    type: ArtifactType
    required: bool | None
    description: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class CooperativeYieldCapability(_DocumentView):
    """Protocols and checkpoint evidence declared for cooperative yielding."""

    request_protocol: ProtocolVersion
    receipt_protocol: ProtocolVersion
    checkpoint_artifacts: tuple[str, ...]
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class JobCapabilities(_DocumentView):
    """Immutable optional capabilities declared by one job."""

    cooperative_yield: CooperativeYieldCapability | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class ProvenanceInput(_DocumentView):
    """Immutable named scientific input evidence recorded by a card."""

    name: str
    source: str
    sha256: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class Provenance(_DocumentView):
    """Immutable scientific provenance attached to an ExperimentCard."""

    inputs: tuple[ProvenanceInput, ...]
    notes: str | None
    _document: FrozenJSONObject = field(repr=False, hash=False)


@dataclass(frozen=True, slots=True, init=False)
class Job(_DocumentView):
    """Immutable, validated portable job definition from an ExperimentCard."""

    id: str
    role: JobRole | None
    description: str | None
    environment: str
    working_directory: str | None
    command: Command
    parameters: FrozenJSONObject = field(hash=False)
    resources: JobResources | None
    artifacts: tuple[Artifact, ...]
    capabilities: JobCapabilities | None
    extensions: FrozenJSONObject = field(hash=False)
    _document: FrozenJSONObject = field(repr=False, hash=False)


def _logical_volume(document: dict[str, JSONValue]) -> LogicalVolume:
    required = document.get("required")
    assert required is None or type(required) is bool
    return _construct_view(
        LogicalVolume,
        name=_string(document["name"]),
        access=VolumeAccess(_string(document["access"])),
        required=required,
        description=_optional_string(document, "description"),
        _document=_freeze_object(document),
    )


def _project_environment(document: dict[str, JSONValue]) -> ProjectEnvironment:
    return _construct_view(
        ProjectEnvironment,
        name=_string(document["name"]),
        description=_optional_string(document, "description"),
        _document=_freeze_object(document),
    )


def _environment_policy(document: dict[str, JSONValue]) -> EnvironmentPolicy:
    return _construct_view(
        EnvironmentPolicy,
        inherit=EnvironmentInheritance(_string(document["inherit"])),
        allow_variables=tuple(
            _string(value) for value in _array(document["allowVariables"])
        ),
        _document=_freeze_object(document),
    )


def _extension_schema_reference(
    document: dict[str, JSONValue],
) -> ExtensionSchemaReference:
    return _construct_view(
        ExtensionSchemaReference,
        path=_string(document["path"]),
        sha256=_optional_string(document, "sha256"),
        _document=_freeze_object(document),
    )


def _command(document: dict[str, JSONValue]) -> Command:
    command_type = _string(document["type"])
    if command_type == "argv":
        return _construct_view(
            ArgvCommand,
            argv=tuple(_string(value) for value in _array(document["argv"])),
            _document=_freeze_object(document),
        )
    if command_type == "wrapper":
        args = document.get("args", [])
        return _construct_view(
            WrapperCommand,
            path=_string(document["path"]),
            args=tuple(_string(value) for value in _array(args)),
            _document=_freeze_object(document),
        )
    assert command_type == "shell"
    return _construct_view(
        ShellCommand,
        script=_string(document["script"]),
        compatibility_reason=_string(document["compatibilityReason"]),
        _document=_freeze_object(document),
    )


def _optional_integer(document: Mapping[str, object], key: str) -> int | None:
    value = document.get(key)
    if value is None:
        return None
    # JSON Schema treats finite integral floats as integers. The typed model
    # exposes their mathematical value while the document snapshot retains the
    # exact validated JSON number supplied by the author.
    assert type(value) in (int, float)
    return int(value)


def _job_resources(document: dict[str, JSONValue]) -> JobResources:
    return _construct_view(
        JobResources,
        gpus=_optional_integer(document, "gpus"),
        cpus=_optional_integer(document, "cpus"),
        memory_bytes=_optional_integer(document, "memoryBytes"),
        wall_time_seconds=_optional_integer(document, "wallTimeSeconds"),
        _document=_freeze_object(document),
    )


def _artifact(document: dict[str, JSONValue]) -> Artifact:
    required = document.get("required")
    assert required is None or type(required) is bool
    return _construct_view(
        Artifact,
        name=_string(document["name"]),
        root=_string(document["root"]),
        path=_string(document["path"]),
        type=ArtifactType(_string(document["type"])),
        required=required,
        description=_optional_string(document, "description"),
        _document=_freeze_object(document),
    )


def _cooperative_yield(
    document: dict[str, JSONValue],
) -> CooperativeYieldCapability:
    return _construct_view(
        CooperativeYieldCapability,
        request_protocol=ProtocolVersion.from_document(
            _object(document["requestProtocol"])
        ),
        receipt_protocol=ProtocolVersion.from_document(
            _object(document["receiptProtocol"])
        ),
        checkpoint_artifacts=tuple(
            _string(value) for value in _array(document["checkpointArtifacts"])
        ),
        _document=_freeze_object(document),
    )


def _job_capabilities(document: dict[str, JSONValue]) -> JobCapabilities:
    cooperative_value = document.get("cooperativeYield")
    cooperative_yield = (
        None
        if cooperative_value is None
        else _cooperative_yield(_object(cooperative_value))
    )
    return _construct_view(
        JobCapabilities,
        cooperative_yield=cooperative_yield,
        _document=_freeze_object(document),
    )


def _provenance_input(document: dict[str, JSONValue]) -> ProvenanceInput:
    return _construct_view(
        ProvenanceInput,
        name=_string(document["name"]),
        source=_string(document["source"]),
        sha256=_optional_string(document, "sha256"),
        _document=_freeze_object(document),
    )


def _provenance(document: dict[str, JSONValue]) -> Provenance:
    inputs = document.get("inputs", [])
    return _construct_view(
        Provenance,
        inputs=tuple(
            _provenance_input(_object(value)) for value in _array(inputs)
        ),
        notes=_optional_string(document, "notes"),
        _document=_freeze_object(document),
    )


def _job(document: dict[str, JSONValue]) -> Job:
    role_value = document.get("role")
    role = None if role_value is None else JobRole(_string(role_value))
    resources_value = document.get("resources")
    capabilities_value = document.get("capabilities")
    parameters = _object(document.get("parameters", {}))
    extensions = _object(document.get("extensions", {}))
    artifacts = document.get("artifacts", [])
    return _construct_view(
        Job,
        id=_string(document["id"]),
        role=role,
        description=_optional_string(document, "description"),
        environment=_string(document["environment"]),
        working_directory=_optional_string(document, "workingDirectory"),
        command=_command(_object(document["command"])),
        parameters=_freeze_object(parameters),
        resources=(
            None
            if resources_value is None
            else _job_resources(_object(resources_value))
        ),
        artifacts=tuple(_artifact(_object(value)) for value in _array(artifacts)),
        capabilities=(
            None
            if capabilities_value is None
            else _job_capabilities(_object(capabilities_value))
        ),
        extensions=_freeze_object(extensions),
        _document=_freeze_object(document),
    )


@dataclass(frozen=True, slots=True, init=False)
class Project(_DocumentView):
    """Validated, deeply immutable typed view of one portable Project/v1.

    Direct construction is intentionally unavailable: use :meth:`from_document`
    or :meth:`from_yaml` so the bundled structural and semantic validators run
    before any model exists.
    """

    key: str
    display_name: str
    description: str | None
    card_roots: tuple[str, ...]
    volumes: tuple[LogicalVolume, ...]
    environments: tuple[ProjectEnvironment, ...]
    environment_policy: EnvironmentPolicy
    supported_protocols: tuple[ProtocolVersion, ...]
    extension_schema: ExtensionSchemaReference | None
    extensions: FrozenJSONObject = field(hash=False)
    _document: FrozenJSONObject = field(repr=False, hash=False)

    def __init__(self) -> None:
        raise TypeError(
            "Project must be constructed with from_document() or from_yaml()"
        )

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> Self:
        """Validate and detach one JSON-native Project/v1 document."""

        if cls is not Project:
            raise TypeError(
                "Project.from_document() constructs exactly Project, not "
                f"subclass {cls.__name__}; use Project.from_document() so typed "
                "fields cannot diverge from the validated document"
            )
        snapshot = _validated_snapshot(
            PROJECT_V1,
            document,
            document_name="Project",
        )
        _reject_reserved_environment_variables(snapshot)
        metadata = _object(snapshot["metadata"])
        spec = _object(snapshot["spec"])
        extension_schema_value = spec.get("extensionSchema")
        instance = object.__new__(cls)
        values = {
            "key": _string(metadata["key"]),
            "display_name": _string(metadata["displayName"]),
            "description": _optional_string(metadata, "description"),
            "card_roots": tuple(
                _string(value) for value in _array(spec["cardRoots"])
            ),
            "volumes": tuple(
                _logical_volume(_object(value))
                for value in _array(spec["volumes"])
            ),
            "environments": tuple(
                _project_environment(_object(value))
                for value in _array(spec["environments"])
            ),
            "environment_policy": _environment_policy(
                _object(spec["environmentPolicy"])
            ),
            "supported_protocols": tuple(
                ProtocolVersion.from_document(_object(value))
                for value in _array(spec["supportedProtocols"])
            ),
            "extension_schema": (
                None
                if extension_schema_value is None
                else _extension_schema_reference(_object(extension_schema_value))
            ),
            "extensions": _freeze_object(_object(snapshot.get("extensions", {}))),
            "_document": _freeze_object(snapshot),
        }
        for name, value in values.items():
            object.__setattr__(instance, name, value)
        return instance

    @classmethod
    def from_yaml(cls, source: bytes, *, source_name: str = "<bytes>") -> Self:
        """Load strict YAML bytes, validate them as Project/v1, and detach them."""

        if cls is not Project:
            raise TypeError(
                "Project.from_yaml() constructs exactly Project, not "
                f"subclass {cls.__name__}; use Project.from_yaml() so typed fields "
                "cannot diverge from the validated document"
            )
        document = _load_yaml_document(
            source,
            source_name=source_name,
            document_name="Project",
        )
        return cls.from_document(document)

    def to_document(self) -> dict[str, JSONValue]:
        """Return a fresh JSON-native Project with exact normalized semantics."""

        return _fresh_document(self._document)


@dataclass(frozen=True, slots=True, init=False)
class ExperimentCard(_DocumentView):
    """Validated, deeply immutable typed view of one ExperimentCard/v1.

    Project-dependent logical references are checked separately by
    :func:`validate_card_for_project`, before admission selects runnable work.
    """

    project_key: str
    experiment_id: str
    title: str
    description: str | None
    tags: tuple[str, ...]
    parameters: FrozenJSONObject = field(hash=False)
    jobs: tuple[Job, ...]
    provenance: Provenance | None
    extensions: FrozenJSONObject = field(hash=False)
    _document: FrozenJSONObject = field(repr=False, hash=False)

    def __init__(self) -> None:
        raise TypeError(
            "ExperimentCard must be constructed with from_document() or from_yaml()"
        )

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> Self:
        """Validate and detach one JSON-native ExperimentCard/v1 document."""

        if cls is not ExperimentCard:
            raise TypeError(
                "ExperimentCard.from_document() constructs exactly ExperimentCard, "
                f"not subclass {cls.__name__}; use ExperimentCard.from_document() "
                "so typed fields cannot diverge from the validated document"
            )
        snapshot = _validated_snapshot(
            EXPERIMENT_CARD_V1,
            document,
            document_name="ExperimentCard",
        )
        _reject_unresolved_placeholders(snapshot)
        metadata = _object(snapshot["metadata"])
        spec = _object(snapshot["spec"])
        provenance_value = spec.get("provenance")
        instance = object.__new__(cls)
        values = {
            "project_key": _string(metadata["projectKey"]),
            "experiment_id": _string(metadata["experimentId"]),
            "title": _string(metadata["title"]),
            "description": _optional_string(metadata, "description"),
            "tags": tuple(
                _string(value) for value in _array(metadata.get("tags", []))
            ),
            "parameters": _freeze_object(_object(spec["parameters"])),
            "jobs": tuple(_job(_object(value)) for value in _array(spec["jobs"])),
            "provenance": (
                None
                if provenance_value is None
                else _provenance(_object(provenance_value))
            ),
            "extensions": _freeze_object(_object(snapshot.get("extensions", {}))),
            "_document": _freeze_object(snapshot),
        }
        for name, value in values.items():
            object.__setattr__(instance, name, value)
        return instance

    @classmethod
    def from_yaml(cls, source: bytes, *, source_name: str = "<bytes>") -> Self:
        """Load strict YAML bytes, validate as ExperimentCard/v1, and detach."""

        if cls is not ExperimentCard:
            raise TypeError(
                "ExperimentCard.from_yaml() constructs exactly ExperimentCard, not "
                f"subclass {cls.__name__}; use ExperimentCard.from_yaml() so typed "
                "fields cannot diverge from the validated document"
            )
        document = _load_yaml_document(
            source,
            source_name=source_name,
            document_name="ExperimentCard",
        )
        return cls.from_document(document)

    def to_document(self) -> dict[str, JSONValue]:
        """Return a fresh JSON-native card with exact normalized semantics."""

        return _fresh_document(self._document)

    def job(self, job_id: str) -> Job:
        """Return the uniquely identified job or fail with available choices."""

        for job in self.jobs:
            if job.id == job_id:
                return job
        available = [job.id for job in self.jobs]
        raise AuthoringValidationError(
            f"ExperimentCard {self.experiment_id!r} has no job {job_id!r}; "
            f"choose one of {available}"
        )


def _protocol_label(protocol: ProtocolVersion) -> str:
    return f"{protocol.kind.value}/v{protocol.major}"


def validate_card_for_project(project: Project, card: ExperimentCard) -> None:
    """Validate Project-dependent card references before admission.

    This check owns only relationships between the two already validated
    protocol documents. Structural and within-document invariants remain owned
    by their immutable bundled schemas and semantic validators.
    """

    if type(project) is not Project:
        raise TypeError(
            f"project must be exactly a Project, got {type(project).__name__}"
        )
    if type(card) is not ExperimentCard:
        raise TypeError(
            f"card must be exactly an ExperimentCard, got {type(card).__name__}"
        )

    if card.project_key != project.key:
        raise AuthoringValidationError(
            f"ExperimentCard {card.experiment_id!r} declares projectKey "
            f"{card.project_key!r}, but the selected Project key is {project.key!r}; "
            "set metadata.projectKey to the selected Project"
        )

    environment_names: Final = frozenset(
        environment.name for environment in project.environments
    )
    volume_access: Final = {
        volume.name: volume.access for volume in project.volumes
    }
    supported_protocols: Final = frozenset(project.supported_protocols)

    for job in card.jobs:
        if job.environment not in environment_names:
            raise AuthoringValidationError(
                f"job {job.id!r} references environment {job.environment!r}, but "
                f"Project {project.key!r} declares {sorted(environment_names)}; add "
                "the environment to spec.environments or select a declared name"
            )
        for artifact in job.artifacts:
            if artifact.root not in volume_access:
                raise AuthoringValidationError(
                    f"artifact {artifact.name!r} in job {job.id!r} references root "
                    f"{artifact.root!r}, but Project {project.key!r} declares "
                    f"{sorted(volume_access)}; add the logical root to spec.volumes "
                    "or select a declared root"
                )
            if volume_access[artifact.root] is not VolumeAccess.READ_WRITE:
                raise AuthoringValidationError(
                    f"artifact {artifact.name!r} in job {job.id!r} declares execution "
                    f"output on readOnly Project volume {artifact.root!r}; artifact "
                    "roots must reference a readWrite spec.volumes declaration, while "
                    "read-only scientific inputs belong in spec.provenance.inputs"
                )

        cooperative_yield = (
            None
            if job.capabilities is None
            else job.capabilities.cooperative_yield
        )
        if cooperative_yield is None:
            continue
        for field_name, protocol in (
            ("requestProtocol", cooperative_yield.request_protocol),
            ("receiptProtocol", cooperative_yield.receipt_protocol),
        ):
            if protocol not in supported_protocols:
                raise AuthoringValidationError(
                    f"job {job.id!r} declares cooperative-yield {field_name} "
                    f"{_protocol_label(protocol)}, but Project {project.key!r} does "
                    "not declare that identity in spec.supportedProtocols; add "
                    f"{protocol.document_identity()!r} to the Project or remove the "
                    "unsupported capability"
                )


__all__ = [
    "ArgvCommand",
    "Artifact",
    "ArtifactType",
    "AuthoringValidationError",
    "Command",
    "CooperativeYieldCapability",
    "EnvironmentInheritance",
    "EnvironmentPolicy",
    "ExperimentCard",
    "ExtensionSchemaReference",
    "FrozenJSONObject",
    "FrozenJSONValue",
    "Job",
    "JobCapabilities",
    "JobResources",
    "JobRole",
    "LogicalVolume",
    "Project",
    "ProjectEnvironment",
    "Provenance",
    "ProvenanceInput",
    "RESERVED_ENVIRONMENT_PREFIX",
    "RESERVED_ENVIRONMENT_VARIABLES",
    "ShellCommand",
    "VolumeAccess",
    "WrapperCommand",
    "is_reserved_environment_variable",
    "validate_card_for_project",
]
