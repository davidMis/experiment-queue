"""Load and validate immutable bundled Project and ExperimentCard schemas."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.resources import files
import json
from types import MappingProxyType
from typing import Callable, Final, Mapping

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from referencing import Registry, Resource

from experiment_queue.protocols import (
    EXPERIMENT_CARD_V1,
    PROJECT_V1,
    ProtocolVersion,
)
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


JSON_SCHEMA_DIALECT: Final = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_RESOURCE_PACKAGE: Final = "experiment_queue.schema_resources"


class BundledSchemaError(RuntimeError):
    """Raised when an installed schema resource is missing, changed, or invalid."""


class SemanticValidationError(ValueError):
    """Raised when a structurally valid Project/Card violates cross-item rules."""


@dataclass(frozen=True, slots=True)
class BundledSchema:
    """Expected immutable identity and digest for one packaged protocol schema."""

    protocol: ProtocolVersion
    resource_name: str
    schema_id: str
    sha256: str


# Digests are filled from RFC 8785 canonical schema bytes, not pretty-printed
# resource bytes. Any schema edit therefore requires an explicit new protocol
# version rather than silently updating these v1 identities.
PROJECT_V1_SCHEMA: Final = BundledSchema(
    protocol=PROJECT_V1,
    resource_name="project-v1.schema.json",
    schema_id="urn:experiment-queue:schema:project:v1",
    sha256="f654e4cd57f6939113c3e8ec32093d00b318b84dd94b653d7d37300e6bcfb23e",
)
EXPERIMENT_CARD_V1_SCHEMA: Final = BundledSchema(
    protocol=EXPERIMENT_CARD_V1,
    resource_name="experiment-card-v1.schema.json",
    schema_id="urn:experiment-queue:schema:experiment-card:v1",
    sha256="0b5c9091d71c727428f92de45818fd6e4eb6d60f0e3a9a81d772255908794e83",
)

BUNDLED_SCHEMAS: Final = MappingProxyType(
    {
        PROJECT_V1: PROJECT_V1_SCHEMA,
        EXPERIMENT_CARD_V1: EXPERIMENT_CARD_V1_SCHEMA,
    }
)


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant {value!r} is not allowed")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _descriptor(protocol: ProtocolVersion) -> BundledSchema:
    try:
        return BUNDLED_SCHEMAS[protocol]
    except KeyError as exc:
        raise BundledSchemaError(
            f"no bundled JSON Schema owns {protocol.kind.value}/v{protocol.major}"
        ) from exc


def load_bundled_schema(protocol: ProtocolVersion) -> dict[str, JSONValue]:
    """Load, authenticate, and meta-validate a fresh installed schema document."""

    descriptor = _descriptor(protocol)
    try:
        resource = files(SCHEMA_RESOURCE_PACKAGE).joinpath(descriptor.resource_name)
        raw = resource.read_bytes()
    except (FileNotFoundError, ModuleNotFoundError, OSError) as exc:
        raise BundledSchemaError(
            f"could not read installed schema resource {descriptor.resource_name!r}: {exc}"
        ) from exc
    try:
        document = json.loads(
            raw.decode("utf-8", errors="strict"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise BundledSchemaError(
            f"installed schema resource {descriptor.resource_name!r} is not strict JSON: "
            f"{exc}"
        ) from exc
    if type(document) is not dict:
        raise BundledSchemaError(
            f"installed schema resource {descriptor.resource_name!r} must be an object"
        )

    try:
        canonical = canonical_json_bytes(document)
    except ValueError as exc:
        raise BundledSchemaError(
            f"installed schema resource {descriptor.resource_name!r} is outside "
            f"the canonical JSON domain: {exc}"
        ) from exc
    actual_digest = sha256_bytes(canonical)
    if actual_digest != descriptor.sha256:
        raise BundledSchemaError(
            f"installed schema resource {descriptor.resource_name!r} has digest "
            f"{actual_digest}, expected {descriptor.sha256}; reinstall an untampered package"
        )
    if document.get("$schema") != JSON_SCHEMA_DIALECT:
        raise BundledSchemaError(
            f"installed schema {descriptor.resource_name!r} must declare "
            f"{JSON_SCHEMA_DIALECT!r}"
        )
    if document.get("$id") != descriptor.schema_id:
        raise BundledSchemaError(
            f"installed schema {descriptor.resource_name!r} has unexpected $id "
            f"{document.get('$id')!r}"
        )
    try:
        Draft202012Validator.check_schema(document)
    except SchemaError as exc:
        raise BundledSchemaError(
            f"installed schema {descriptor.resource_name!r} is not valid Draft 2020-12: "
            f"{exc}"
        ) from exc
    return document


@cache
def bundled_schema_registry() -> Registry:
    """Return an offline registry containing only authenticated bundled schemas."""

    registry = Registry()
    for descriptor in BUNDLED_SCHEMAS.values():
        document = load_bundled_schema(descriptor.protocol)
        registry = registry.with_resource(
            descriptor.schema_id,
            Resource.from_contents(document),
        )
    return registry


def validator_for(protocol: ProtocolVersion) -> Draft202012Validator:
    """Return the exact Draft 2020-12 validator for a bundled protocol schema."""

    return offline_validator(load_bundled_schema(protocol))


def offline_validator(schema: Mapping[str, object]) -> Draft202012Validator:
    """Build a Draft 2020-12 validator with no network retrieval fallback."""

    if schema.get("$schema") != JSON_SCHEMA_DIALECT:
        raise BundledSchemaError(
            "JSON Schema must declare exactly "
            f"{JSON_SCHEMA_DIALECT!r}, got {schema.get('$schema')!r}"
        )
    try:
        canonical_json_bytes(schema)  # type: ignore[arg-type]
    except ValueError as exc:
        raise BundledSchemaError(
            f"JSON Schema is outside the strict canonical JSON domain: {exc}"
        ) from exc
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise BundledSchemaError(f"invalid Draft 2020-12 JSON Schema: {exc}") from exc
    return Draft202012Validator(schema, registry=bundled_schema_registry())


def validate_bundled_document(
    protocol: ProtocolVersion,
    document: Mapping[str, object],
) -> None:
    """Validate a JSON-native document without schema or network fallback."""

    # JSON Schema's Python protocol can traverse arbitrary Python objects in
    # permissive subschemas. Authenticate the exact JSON/JCS domain first so a
    # caller cannot bypass the strict loader with a date, tuple, or large int.
    canonical_json_bytes(document)  # type: ignore[arg-type]
    validator_for(protocol).validate(document)
    semantic_validator = _SEMANTIC_VALIDATORS.get(protocol)
    if semantic_validator is not None:
        semantic_validator(document)


def _require_unique_named_objects(
    values: object,
    *,
    field: str,
    identity_field: str = "name",
) -> None:
    """Enforce identity uniqueness that JSON Schema arrays cannot express."""

    if not isinstance(values, list):
        return
    seen: set[object] = set()
    for value in values:
        if not isinstance(value, Mapping):
            continue
        identity = value.get(identity_field)
        if identity in seen:
            raise SemanticValidationError(
                f"{field} contains duplicate {identity_field} {identity!r}"
            )
        seen.add(identity)


def _validate_project_v1_semantics(document: Mapping[str, object]) -> None:
    """Apply Project/v1 cross-item invariants after structural validation."""

    spec = document["spec"]
    assert isinstance(spec, Mapping)
    _require_unique_named_objects(spec["volumes"], field="spec.volumes")
    _require_unique_named_objects(spec["environments"], field="spec.environments")


def _validate_experiment_card_v1_semantics(document: Mapping[str, object]) -> None:
    """Apply ExperimentCard/v1 identity and local-reference invariants."""

    spec = document["spec"]
    assert isinstance(spec, Mapping)
    jobs = spec["jobs"]
    _require_unique_named_objects(jobs, field="spec.jobs", identity_field="id")
    assert isinstance(jobs, list)
    for job_index, job in enumerate(jobs):
        assert isinstance(job, Mapping)
        artifacts = job.get("artifacts", [])
        artifact_field = f"spec.jobs[{job_index}].artifacts"
        _require_unique_named_objects(artifacts, field=artifact_field)
        artifact_types = {
            artifact["name"]: artifact["type"]
            for artifact in artifacts
            if isinstance(artifact, Mapping)
        }
        capabilities = job.get("capabilities")
        if not isinstance(capabilities, Mapping):
            continue
        cooperative_yield = capabilities.get("cooperativeYield")
        if not isinstance(cooperative_yield, Mapping):
            continue
        checkpoint_names = cooperative_yield.get("checkpointArtifacts", [])
        assert isinstance(checkpoint_names, list)
        missing = sorted(set(checkpoint_names) - set(artifact_types))
        if missing:
            raise SemanticValidationError(
                f"spec.jobs[{job_index}].capabilities.cooperativeYield."
                f"checkpointArtifacts references undeclared job artifacts {missing}"
            )
        non_files = sorted(
            name for name in checkpoint_names if artifact_types[name] != "file"
        )
        if non_files:
            raise SemanticValidationError(
                f"spec.jobs[{job_index}].capabilities.cooperativeYield."
                f"checkpointArtifacts requires file artifacts, got {non_files}"
            )


_SEMANTIC_VALIDATORS: Final[
    Mapping[ProtocolVersion, Callable[[Mapping[str, object]], None]]
] = MappingProxyType(
    {
        PROJECT_V1: _validate_project_v1_semantics,
        EXPERIMENT_CARD_V1: _validate_experiment_card_v1_semantics,
    }
)


def schema_canonical_bytes(protocol: ProtocolVersion) -> bytes:
    """Return authenticated RFC 8785 bytes for an installed bundled schema."""

    return canonical_json_bytes(load_bundled_schema(protocol))


def editor_schema_bytes(protocol: ProtocolVersion) -> bytes:
    """Export an authenticated bundled schema as stable editor-friendly JSON.

    These indented bytes are presentation output, not protocol evidence;
    callers must use :func:`schema_canonical_bytes` for digest material.
    """

    document = load_bundled_schema(protocol)
    return (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def schema_sha256(protocol: ProtocolVersion) -> str:
    """Return the immutable checked-in digest for a bundled schema identity."""

    return _descriptor(protocol).sha256


__all__ = [
    "BUNDLED_SCHEMAS",
    "BundledSchema",
    "BundledSchemaError",
    "EXPERIMENT_CARD_V1_SCHEMA",
    "JSON_SCHEMA_DIALECT",
    "PROJECT_V1_SCHEMA",
    "SemanticValidationError",
    "bundled_schema_registry",
    "editor_schema_bytes",
    "load_bundled_schema",
    "offline_validator",
    "schema_canonical_bytes",
    "schema_sha256",
    "validate_bundled_document",
    "validator_for",
]
