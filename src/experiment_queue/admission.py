"""Compile mutable submissions into immutable, content-addressed execution evidence.

Admission is a one-way boundary: authoring sources and scheduling policy are
revalidated on every call, while the returned snapshot owns immutable bytes and
never consults later source-file or ``Submission`` mutations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version as package_version_for
import json
from pathlib import PurePosixPath
import re
from typing import Final, TypeVar, cast

from experiment_queue.authoring import (
    AuthoringValidationError,
    Command,
    ExperimentCard,
    Project,
    validate_card_for_project,
)
from experiment_queue.extensions import validate_namespaced_extensions
from experiment_queue.identity import validate_project_key
from experiment_queue.schema_registry import (
    EXPERIMENT_CARD_V1_SCHEMA,
    PROJECT_V1_SCHEMA,
    BundledSchema,
)
from experiment_queue.serialization import (
    CanonicalJSONError,
    JSONValue,
    MAX_NESTING_DEPTH,
    canonical_json_bytes,
    sha256_bytes,
)


_SIGNED_64_MIN: Final = -(2**63)
_SIGNED_64_MAX: Final = (2**63) - 1
_MAX_CARD_PATH_CHARACTERS: Final = 4_096
_MAX_OPERATOR_CHARACTERS: Final = 256
_MAX_HOLD_REASON_CHARACTERS: Final = 4_000
_MAX_REVISION_CHARACTERS: Final = 256
_MAX_PACKAGE_VERSION_CHARACTERS: Final = 128
_FULL_GIT_OBJECT_PATTERN = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
_PLACEHOLDER_TOKEN_PATTERN: Final = re.compile(
    r"\$\{[^}]*\}?|\{\{.*?(?:\}\}|$)",
    flags=re.DOTALL,
)


class AdmissionError(ValueError):
    """Raised when mutable submission data cannot produce safe execution evidence."""


class _CompilerEvidence:
    """Factory-only base for values whose type asserts compiler validation."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is trusted admission evidence produced only by "
            "compile_admission()"
        )


_EvidenceT = TypeVar("_EvidenceT", bound=_CompilerEvidence)


def _construct_evidence(
    evidence_type: type[_EvidenceT],
    **values: object,
) -> _EvidenceT:
    """Populate one frozen evidence value after all compiler checks pass."""

    instance = object.__new__(evidence_type)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


@dataclass(slots=True)
class Submission:
    """Mutable operator policy kept separate from a committed ExperimentCard.

    Construction intentionally does not confer trust. All fields are copied and
    revalidated by :func:`compile_admission`, including after caller mutation.
    """

    project_key: str
    card_path: str
    job_id: str
    operator: str = ""
    bindings: dict[str, JSONValue] = field(default_factory=dict)
    priority: int = 0
    hold_reason: str | None = None
    dependencies: list[int] = field(default_factory=list)
    preemption_authorized: bool = False


@dataclass(frozen=True, slots=True)
class _SubmissionCandidate:
    """Detached one-read snapshot of caller-owned mutable Submission state."""

    project_key: object
    card_path: object
    job_id: object
    operator: object
    bindings: dict[str, JSONValue]
    bindings_json: bytes
    priority: object
    hold_reason: object
    dependencies: tuple[int, ...]
    preemption_authorized: object


@dataclass(frozen=True, slots=True, init=False)
class SubmissionPolicy(_CompilerEvidence):
    """Immutable copy of the mutable scheduling policy at admission time.

    Binding values are retained as canonical JSON bytes. The ``bindings``
    property and :meth:`to_document` return fresh containers, so a caller can
    never mutate policy owned by the snapshot.
    """

    project_key: str
    card_path: str
    job_id: str
    priority: int
    hold_reason: str | None
    dependencies: tuple[int, ...]
    operator: str
    preemption_authorized: bool
    _bindings_json: bytes

    @property
    def bindings(self) -> dict[str, JSONValue]:
        """Return a fresh JSON-native copy of admitted parameter bindings."""

        value = _decode_owned_json(self._bindings_json)
        assert type(value) is dict
        return value

    @property
    def bindings_json(self) -> bytes:
        """Return immutable RFC 8785 bytes for the admitted bindings."""

        return self._bindings_json

    def to_document(self) -> dict[str, JSONValue]:
        """Return a fresh audit document, including policy excluded from execution."""

        return {
            "projectKey": self.project_key,
            "cardPath": self.card_path,
            "jobId": self.job_id,
            "bindings": self.bindings,
            "priority": self.priority,
            "holdReason": self.hold_reason,
            "dependencies": list(self.dependencies),
            "operator": self.operator,
            "preemptionAuthorized": self.preemption_authorized,
        }


@dataclass(frozen=True, slots=True, init=False)
class SchemaEvidence(_CompilerEvidence):
    """Authenticated identity and canonical digest of one bundled schema."""

    api_version: str
    kind: str
    schema_id: str
    sha256: str


@dataclass(frozen=True, slots=True, init=False)
class ExtensionSchemaEvidence(_CompilerEvidence):
    """Exact and canonical evidence for a project-owned extension schema."""

    source_name: str
    reference_path: str
    source: bytes
    source_sha256: str
    canonical_json: bytes
    canonical_sha256: str
    schema_id: str | None


@dataclass(frozen=True, slots=True, init=False)
class AdmissionSnapshot(_CompilerEvidence):
    """Frozen evidence consumed by later persistence and execution layers.

    Every document field is immutable bytes or an immutable typed value.
    Convenience document properties always decode fresh containers.
    """

    project_source_name: str
    project_source: bytes
    project_source_sha256: str
    project_normalized_json: bytes
    project_normalized_sha256: str
    project_schema: SchemaEvidence
    card_source_name: str
    card_source: bytes
    card_source_sha256: str
    card_normalized_json: bytes
    card_normalized_sha256: str
    card_schema: SchemaEvidence
    extension_schema: ExtensionSchemaEvidence | None
    resolved_json: bytes
    resolved_sha256: str
    command: Command
    project_revision: str
    git_commit: str
    package_version: str
    submission_policy: SubmissionPolicy

    @property
    def project_document(self) -> dict[str, JSONValue]:
        """Return a fresh normalized Project document."""

        value = _decode_owned_json(self.project_normalized_json)
        assert type(value) is dict
        return value

    @property
    def card_document(self) -> dict[str, JSONValue]:
        """Return a fresh normalized ExperimentCard document."""

        value = _decode_owned_json(self.card_normalized_json)
        assert type(value) is dict
        return value

    @property
    def resolved_document(self) -> dict[str, JSONValue]:
        """Return a fresh resolved execution document."""

        value = _decode_owned_json(self.resolved_json)
        assert type(value) is dict
        return value

    @property
    def selected_command(self) -> Command:
        """Alias naming the command selected from the admitted job."""

        return self.command


def _decode_owned_json(source: bytes) -> JSONValue:
    """Decode bytes emitted by our canonicalizer into a fresh JSON value."""

    return cast(JSONValue, json.loads(source.decode("utf-8")))


def _schema_evidence(descriptor: BundledSchema) -> SchemaEvidence:
    return _construct_evidence(
        SchemaEvidence,
        api_version=descriptor.protocol.api_version,
        kind=descriptor.protocol.kind.value,
        schema_id=descriptor.schema_id,
        sha256=descriptor.sha256,
    )


def _require_bounded_text(
    value: object,
    *,
    field_name: str,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str:
        raise AdmissionError(
            f"{field_name} must be text, got {type(value).__name__}"
        )
    if value != value.strip():
        raise AdmissionError(f"{field_name} must not have surrounding whitespace")
    if not value and not allow_empty:
        raise AdmissionError(f"{field_name} must not be empty")
    if len(value) > maximum:
        raise AdmissionError(
            f"{field_name} must be {maximum} characters or fewer, got {len(value)}"
        )
    if any(
        ord(character) < 32
        or ord(character) in {127, 0x85, 0x2028, 0x2029}
        for character in value
    ):
        raise AdmissionError(f"{field_name} must not contain control or line characters")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise AdmissionError(
            f"{field_name} must contain only Unicode scalar text"
        ) from exc
    return value


def _portable_card_path(
    value: object,
    *,
    field_name: str = "submission.card_path",
) -> str:
    card_path = _require_bounded_text(
        value,
        field_name=field_name,
        maximum=_MAX_CARD_PATH_CHARACTERS,
    )
    components = card_path.split("/")
    if (
        card_path.startswith("/")
        or re.match(r"[A-Za-z]:", card_path) is not None
        or card_path == "~"
        or card_path.startswith("~/")
        or "\\" in card_path
        or any(component in {"", ".", ".."} for component in components)
    ):
        raise AdmissionError(
            f"{field_name} must be a portable project-relative POSIX path "
            f"without drive, tilde, backslash, empty, '.', or '..' components; got "
            f"{card_path!r}"
        )
    return card_path


def _require_card_beneath_root(card_path: str, roots: tuple[str, ...]) -> None:
    candidate = PurePosixPath(card_path)
    for root_value in roots:
        root = PurePosixPath(root_value)
        try:
            relative = candidate.relative_to(root)
        except ValueError:
            continue
        if relative.parts:
            return
    raise AdmissionError(
        f"submission.card_path {card_path!r} must be beneath one of Project "
        f"spec.cardRoots {list(roots)!r}"
    )


def _full_git_commit(value: object) -> str:
    if type(value) is not str or _FULL_GIT_OBJECT_PATTERN.fullmatch(value) is None:
        raise AdmissionError(
            "git_commit must be a full 40- or 64-character hexadecimal Git object ID"
        )
    # Git object IDs are case-insensitive input, but stored evidence has one
    # deliberate lowercase spelling so equivalent operator input hashes alike.
    return value.lower()


def _package_version() -> str:
    """Return authenticated local compiler provenance from installed metadata."""

    try:
        value = package_version_for("experiment-queue")
    except PackageNotFoundError as exc:
        raise AdmissionError(
            "experiment-queue package metadata is unavailable; install the package "
            "before compiling admission evidence"
        ) from exc
    return _require_bounded_text(
        value,
        field_name="package_version",
        maximum=_MAX_PACKAGE_VERSION_CHARACTERS,
    )


def _detach_binding_json(
    value: object,
    *,
    path: str = "$",
    depth: int = 0,
    active_container_ids: set[int] | None = None,
) -> JSONValue:
    """Detach one caller-owned binding value before canonical validation."""

    if depth > MAX_NESTING_DEPTH:
        raise AdmissionError(
            f"submission.bindings {path} exceeds the maximum nesting depth of "
            f"{MAX_NESTING_DEPTH}; reduce its nesting"
        )
    value_type = type(value)
    if value is None or value_type in (bool, int, float, str):
        return cast(JSONValue, value)
    if value_type not in (dict, list):
        raise AdmissionError(
            f"submission.bindings {path} has unsupported non-JSON type "
            f"{value_type.__name__}; use only JSON-native values"
        )

    if active_container_ids is None:
        active_container_ids = set()
    container_id = id(value)
    if container_id in active_container_ids:
        raise AdmissionError(
            f"submission.bindings {path} contains a recursive JSON container; "
            "replace it with an acyclic JSON value"
        )
    active_container_ids.add(container_id)
    try:
        if value_type is list:
            items = tuple(cast(list[object], value))
            return [
                _detach_binding_json(
                    item,
                    path=f"{path}[{index}]",
                    depth=depth + 1,
                    active_container_ids=active_container_ids,
                )
                for index, item in enumerate(items)
            ]

        items = tuple(cast(dict[object, object], value).items())
        copied: dict[str, JSONValue] = {}
        for key, item in items:
            if type(key) is not str:
                raise AdmissionError(
                    f"submission.bindings {path} object key {key!r} is not a string; "
                    "use JSON object string keys"
                )
            copied[key] = _detach_binding_json(
                item,
                path=_json_member_path(path, key),
                depth=depth + 1,
                active_container_ids=active_container_ids,
            )
        return copied
    finally:
        active_container_ids.remove(container_id)


def _copy_bindings(value: object) -> tuple[dict[str, JSONValue], bytes]:
    if type(value) is not dict:
        raise AdmissionError(
            f"submission.bindings must be an object, got {type(value).__name__}"
        )
    try:
        copied_value = _detach_binding_json(value)
        assert type(copied_value) is dict
        encoded = canonical_json_bytes(copied_value)
    except RuntimeError as exc:
        raise AdmissionError(
            "submission.bindings changed while its detached canonical snapshot was "
            "being copied; stop mutating the bindings and retry admission"
        ) from exc
    except CanonicalJSONError as exc:
        raise AdmissionError(
            f"submission.bindings must contain only canonical JSON values: {exc}"
        ) from exc
    return copied_value, encoded


def _copy_dependencies(value: object) -> tuple[int, ...]:
    if type(value) is not list:
        raise AdmissionError(
            f"submission.dependencies must be a list, got {type(value).__name__}"
        )
    # Detach membership before validating so later caller mutation cannot alter
    # the policy being compiled.
    candidates = tuple(value)
    copied: list[int] = []
    seen: set[int] = set()
    for index, dependency in enumerate(candidates):
        if (
            type(dependency) is not int
            or dependency < 1
            or dependency > _SIGNED_64_MAX
        ):
            raise AdmissionError(
                f"submission.dependencies[{index}] must be a positive signed "
                f"64-bit queue item ID, got {dependency!r}"
            )
        if dependency in seen:
            raise AdmissionError(
                f"submission.dependencies contains duplicate queue item ID {dependency}"
            )
        seen.add(dependency)
        copied.append(dependency)
    return tuple(copied)


def _json_member_path(path: str, key: str) -> str:
    if key.isidentifier():
        return f"{path}.{key}"
    return f"{path}[{key!r}]"


def _resolved_parameter_placeholder(
    value: JSONValue,
) -> tuple[str, str] | None:
    """Find reserved objects or obvious template tokens without recursion."""

    pending: list[tuple[JSONValue, str]] = [(value, "$")]
    while pending:
        current, path = pending.pop()
        if type(current) is dict:
            if "$binding" in current:
                return _json_member_path(path, "$binding"), "$binding"
            for key, item in reversed(tuple(current.items())):
                item_path = _json_member_path(path, key)
                key_match = _PLACEHOLDER_TOKEN_PATTERN.search(key)
                if key_match is not None:
                    return item_path, key_match.group(0)
                pending.append((item, item_path))
        elif type(current) is list:
            for index in range(len(current) - 1, -1, -1):
                pending.append((current[index], f"{path}[{index}]"))
        elif type(current) is str:
            match = _PLACEHOLDER_TOKEN_PATTERN.search(current)
            if match is not None:
                return path, match.group(0)
    return None


def _snapshot_submission(submission: Submission) -> _SubmissionCandidate:
    """Read each mutable field once and detach its container-valued state."""

    project_key = submission.project_key
    card_path = submission.card_path
    job_id = submission.job_id
    operator = submission.operator
    bindings_value = submission.bindings
    priority = submission.priority
    hold_reason = submission.hold_reason
    dependencies_value = submission.dependencies
    preemption_authorized = submission.preemption_authorized

    bindings, bindings_json = _copy_bindings(bindings_value)
    dependencies = _copy_dependencies(dependencies_value)
    return _SubmissionCandidate(
        project_key=project_key,
        card_path=card_path,
        job_id=job_id,
        operator=operator,
        bindings=bindings,
        bindings_json=bindings_json,
        priority=priority,
        hold_reason=hold_reason,
        dependencies=dependencies,
        preemption_authorized=preemption_authorized,
    )


def _extension_payloads(
    project_document: dict[str, JSONValue],
    card_document: dict[str, JSONValue],
    job_document: dict[str, JSONValue],
    *,
    project_key: str,
    job_id: str,
) -> dict[str, JSONValue]:
    """Extract the validated project-owned payloads relevant to this execution."""

    payloads: dict[str, JSONValue] = {}
    for location, document in (
        ("project", project_document),
        ("card", card_document),
    ):
        extensions = document.get("extensions")
        if type(extensions) is dict and project_key in extensions:
            payloads[location] = extensions[project_key]
    job_extensions = job_document.get("extensions")
    if type(job_extensions) is dict and project_key in job_extensions:
        payloads["jobs"] = {job_id: job_extensions[project_key]}
    return payloads


def _submission_policy(
    submission: _SubmissionCandidate,
    *,
    project: Project,
    card: ExperimentCard,
    card_document: dict[str, JSONValue],
) -> tuple[SubmissionPolicy, dict[str, JSONValue]]:
    """Copy and validate every mutable field before deriving execution identity."""

    try:
        project_key = validate_project_key(submission.project_key)
    except ValueError as exc:
        raise AdmissionError(f"submission.project_key is invalid: {exc}") from exc
    if project_key != project.key:
        raise AdmissionError(
            f"submission.project_key {project_key!r} does not match Project key "
            f"{project.key!r}"
        )
    if project_key != card.project_key:
        raise AdmissionError(
            f"submission.project_key {project_key!r} does not match ExperimentCard "
            f"project key {card.project_key!r}"
        )

    card_path = _portable_card_path(submission.card_path)
    _require_card_beneath_root(card_path, tuple(project.card_roots))
    job_id = _require_bounded_text(
        submission.job_id,
        field_name="submission.job_id",
        maximum=128,
    )
    operator = _require_bounded_text(
        submission.operator,
        field_name="submission.operator",
        maximum=_MAX_OPERATOR_CHARACTERS,
    )
    hold_reason = submission.hold_reason
    if hold_reason is not None:
        hold_reason = _require_bounded_text(
            hold_reason,
            field_name="submission.hold_reason",
            maximum=_MAX_HOLD_REASON_CHARACTERS,
        )
    priority = submission.priority
    if (
        type(priority) is not int
        or priority < _SIGNED_64_MIN
        or priority > _SIGNED_64_MAX
    ):
        raise AdmissionError(
            "submission.priority must be a signed 64-bit integer, "
            f"got {priority!r}"
        )
    dependencies = submission.dependencies
    preemption_authorized = submission.preemption_authorized
    if type(preemption_authorized) is not bool:
        raise AdmissionError(
            "submission.preemption_authorized must be true or false, "
            f"got {preemption_authorized!r}"
        )

    bindings = submission.bindings
    bindings_json = submission.bindings_json
    spec = card_document["spec"]
    assert type(spec) is dict
    declared_parameters = spec["parameters"]
    assert type(declared_parameters) is dict
    unknown_bindings = sorted(set(bindings) - set(declared_parameters))
    if unknown_bindings:
        raise AdmissionError(
            "submission.bindings may replace only declared top-level "
            f"card.spec.parameters; unknown names: {unknown_bindings}"
        )
    resolved_parameters = cast(dict[str, JSONValue], _decode_owned_json(
        canonical_json_bytes(declared_parameters)
    ))
    resolved_parameters.update(bindings)
    placeholder = _resolved_parameter_placeholder(resolved_parameters)
    if placeholder is not None:
        placeholder_path, token = placeholder
        if token != "$binding":
            raise AdmissionError(
                f"resolved parameters contain unresolved placeholder token "
                f"{token!r} at {placeholder_path}; bindings replace whole literal "
                "values and do not interpolate ${...} or {{...}} syntax"
            )
        raise AdmissionError(
            f"resolved parameters contain reserved '$binding' placeholder data at "
            f"{placeholder_path}; ExperimentCard/v1 bindings replace whole values "
            "and do not interpolate"
        )

    policy = _construct_evidence(
        SubmissionPolicy,
        project_key=project_key,
        card_path=card_path,
        job_id=job_id,
        priority=priority,
        hold_reason=hold_reason,
        dependencies=dependencies,
        operator=operator,
        preemption_authorized=preemption_authorized,
        _bindings_json=bindings_json,
    )
    return policy, resolved_parameters


def compile_admission(
    *,
    project_source: bytes,
    card_source: bytes,
    submission: Submission,
    project_revision: str,
    git_commit: str,
    extension_schema_source: bytes | None = None,
    project_source_name: str = "project.yaml",
    card_source_name: str | None = None,
) -> AdmissionSnapshot:
    """Compile exact authoring bytes and mutable policy into frozen evidence.

    The compiler deliberately reparses trusted model inputs on every call.
    Bindings replace only complete declared top-level parameter values; no
    command, path, wrapper, or shell interpolation occurs.

    This pure compiler does not read Git. Its caller must be the queue's trusted
    Git/ProjectRevision resolver and must supply the Project, card, and any
    declared extension-schema bytes read from ``git_commit``'s tree at their
    named paths. A database or API admission boundary must not forward
    arbitrary client bytes, revision names, or commit claims directly. The
    compiler binds those resolver-supplied inputs into immutable bytes and
    digests so later source edits cannot change the resulting evidence.
    """

    if type(project_source) is not bytes:
        raise TypeError(
            "project_source must be immutable bytes, "
            f"got {type(project_source).__name__}"
        )
    if type(card_source) is not bytes:
        raise TypeError(
            f"card_source must be immutable bytes, got {type(card_source).__name__}"
        )
    if extension_schema_source is not None and type(extension_schema_source) is not bytes:
        raise TypeError(
            "extension_schema_source must be immutable bytes or None, "
            f"got {type(extension_schema_source).__name__}"
        )
    if type(submission) is not Submission:
        raise TypeError(
            f"submission must be exactly a Submission, got "
            f"{type(submission).__name__}; copy subclass or proxy values into a "
            "plain Submission before admission"
        )
    submission_candidate = _snapshot_submission(submission)
    project_source_name = _portable_card_path(
        project_source_name,
        field_name="project_source_name",
    )
    submitted_card_path = _portable_card_path(submission_candidate.card_path)
    if card_source_name is None:
        card_source_name = submitted_card_path
    else:
        card_source_name = _portable_card_path(
            card_source_name,
            field_name="card_source_name",
        )
    if card_source_name != submitted_card_path:
        raise AdmissionError(
            f"card_source_name {card_source_name!r} must equal normalized "
            f"submission.card_path {submitted_card_path!r}; the trusted Git "
            "resolver must read and name the card at the submitted path"
        )
    revision = _require_bounded_text(
        project_revision,
        field_name="project_revision",
        maximum=_MAX_REVISION_CHARACTERS,
    )
    commit = _full_git_commit(git_commit)
    compiler_version = _package_version()

    project = Project.from_yaml(project_source, source_name=project_source_name)
    card = ExperimentCard.from_yaml(card_source, source_name=card_source_name)
    validate_card_for_project(project, card)
    extension = validate_namespaced_extensions(
        project,
        card,
        schema_source=extension_schema_source,
    )

    project_document = project.to_document()
    card_document = card.to_document()
    policy, resolved_parameters = _submission_policy(
        submission_candidate,
        project=project,
        card=card,
        card_document=card_document,
    )
    try:
        selected_job = card.job(policy.job_id)
    except AuthoringValidationError as exc:
        available_jobs = [job.id for job in card.jobs]
        raise AdmissionError(
            f"submission.job_id {policy.job_id!r} does not name a job in "
            f"ExperimentCard {card.experiment_id!r}; choose one of {available_jobs}"
        ) from exc
    selected_job_document = selected_job.to_document()
    capabilities = selected_job_document.get("capabilities", {})
    cooperative_yield_declared = (
        type(capabilities) is dict and "cooperativeYield" in capabilities
    )
    if policy.preemption_authorized and not cooperative_yield_declared:
        raise AdmissionError(
            f"submission preemption authorization requires selected job "
            f"{policy.job_id!r} to declare capabilities.cooperativeYield"
        )

    project_normalized = canonical_json_bytes(project_document)
    card_normalized = canonical_json_bytes(card_document)
    extension_evidence: ExtensionSchemaEvidence | None = None
    if extension is not None:
        assert extension_schema_source is not None
        extension_evidence = _construct_evidence(
            ExtensionSchemaEvidence,
            source_name=extension.source_name,
            reference_path=extension.reference_path,
            source=extension_schema_source,
            source_sha256=extension.source_sha256,
            canonical_json=extension.canonical_bytes,
            canonical_sha256=extension.canonical_sha256,
            schema_id=extension.schema_id,
        )

    card_identity: dict[str, JSONValue] = {
        "apiVersion": card_document["apiVersion"],
        "kind": card_document["kind"],
        "metadata": card_document["metadata"],
    }
    card_spec = card_document["spec"]
    assert type(card_spec) is dict
    if "provenance" in card_spec:
        card_identity["provenance"] = card_spec["provenance"]
    resolved: dict[str, JSONValue] = {
        "project": project_document,
        "card": card_identity,
        "cardPath": policy.card_path,
        "job": selected_job_document,
        "parameters": resolved_parameters,
        "environmentPolicy": project_document["spec"]["environmentPolicy"],  # type: ignore[index]
        "extensions": _extension_payloads(
            project_document,
            card_document,
            selected_job_document,
            project_key=project.key,
            job_id=selected_job.id,
        ),
        "projectRevision": revision,
        "gitCommit": commit,
        "preemptionAuthorized": policy.preemption_authorized,
        "compiler": {
            "package": "experiment-queue",
            "version": compiler_version,
        },
    }
    if extension_evidence is not None:
        extension_descriptor: dict[str, JSONValue] = {
            "path": extension_evidence.reference_path,
            "sha256": extension_evidence.canonical_sha256,
        }
        if extension_evidence.schema_id is not None:
            extension_descriptor["schemaId"] = extension_evidence.schema_id
        resolved["extensionSchema"] = extension_descriptor
    resolved_json = canonical_json_bytes(resolved)

    return _construct_evidence(
        AdmissionSnapshot,
        project_source_name=project_source_name,
        project_source=project_source,
        project_source_sha256=sha256_bytes(project_source),
        project_normalized_json=project_normalized,
        project_normalized_sha256=sha256_bytes(project_normalized),
        project_schema=_schema_evidence(PROJECT_V1_SCHEMA),
        card_source_name=card_source_name,
        card_source=card_source,
        card_source_sha256=sha256_bytes(card_source),
        card_normalized_json=card_normalized,
        card_normalized_sha256=sha256_bytes(card_normalized),
        card_schema=_schema_evidence(EXPERIMENT_CARD_V1_SCHEMA),
        extension_schema=extension_evidence,
        resolved_json=resolved_json,
        resolved_sha256=sha256_bytes(resolved_json),
        command=selected_job.command,
        project_revision=revision,
        git_commit=commit,
        package_version=compiler_version,
        submission_policy=policy,
    )


__all__ = [
    "AdmissionError",
    "AdmissionSnapshot",
    "ExtensionSchemaEvidence",
    "SchemaEvidence",
    "Submission",
    "SubmissionPolicy",
    "compile_admission",
]
