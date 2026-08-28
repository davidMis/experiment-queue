"""Validate project-namespaced extension payloads and their schema evidence."""

from __future__ import annotations

from codecs import BOM_UTF8
from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from referencing import Resource
from referencing.exceptions import Unresolvable

from experiment_queue.authoring import (
    ExperimentCard,
    ExtensionSchemaReference,
    Project,
)
from experiment_queue.schema_registry import (
    BundledSchemaError,
    JSON_SCHEMA_DIALECT,
    bundled_schema_registry,
    offline_validator,
)
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


_MISSING: Final = object()


class ExtensionSchemaError(ValueError):
    """Raised when extension-schema bytes or their evidence are invalid."""


class ExtensionValidationError(ValueError):
    """Raised when extension namespaces or payloads violate their contract."""


@dataclass(frozen=True, slots=True)
class ExtensionSchema:
    """Immutable source and canonical evidence for one extension schema."""

    source_name: str
    reference_path: str
    source_bytes: bytes
    source_sha256: str
    canonical_bytes: bytes
    canonical_sha256: str
    schema_id: str | None

    @property
    def path(self) -> str:
        """Return the portable Project reference path."""

        return self.reference_path

    @property
    def sha256(self) -> str:
        """Return the RFC 8785 canonical schema digest."""

        return self.canonical_sha256


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant {value!r} is not allowed")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _strict_json_object(source: bytes, *, source_name: str) -> dict[str, JSONValue]:
    """Decode one exact UTF-8 JSON object without Python JSON extensions."""

    if type(source) is not bytes:
        raise TypeError(
            f"{source_name}: extension schema source must be bytes, got "
            f"{type(source).__name__}; read the declared schema path in binary mode"
        )
    if source.startswith(BOM_UTF8):
        raise ExtensionSchemaError(
            f"{source_name}: extension schema has a UTF-8 byte-order mark at byte 0; "
            "save it as strict UTF-8 JSON without a BOM"
        )
    try:
        text = source.decode("utf-8", errors="strict")
        document = json.loads(
            text,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except UnicodeDecodeError as exc:
        raise ExtensionSchemaError(
            f"{source_name}: extension schema is not valid UTF-8 at byte "
            f"{exc.start}; save it as strict UTF-8 JSON"
        ) from exc
    except RecursionError as exc:
        raise ExtensionSchemaError(
            f"{source_name}: extension schema nesting is too deep at $; reduce its "
            "nesting before validation"
        ) from exc
    except (json.JSONDecodeError, ValueError) as exc:
        raise ExtensionSchemaError(
            f"{source_name}: extension schema is not strict JSON at $: {exc}; "
            "remove duplicate keys and non-finite numbers and fix the JSON syntax"
        ) from exc
    if type(document) is not dict:
        raise ExtensionSchemaError(
            f"{source_name}: extension schema root at $ must be a JSON object, got "
            f"{type(document).__name__}; wrap the schema keywords in an object"
        )
    return document


def load_extension_schema(
    source: bytes,
    reference: ExtensionSchemaReference,
    *,
    source_name: str | None = None,
) -> ExtensionSchema:
    """Authenticate and meta-validate one declared Draft 2020-12 schema.

    The optional reference digest authenticates RFC 8785 canonical schema
    bytes, not the presentation bytes retained in ``source_bytes``.
    """

    if type(reference) is not ExtensionSchemaReference:
        raise TypeError(
            "extension schema reference must be an exact validated "
            f"ExtensionSchemaReference, got {type(reference).__name__}; use "
            "Project.from_document() or Project.from_yaml()"
        )
    effective_name = reference.path if source_name is None else source_name
    if type(effective_name) is not str or not effective_name:
        raise TypeError("extension schema source_name must be a non-empty string")
    document = _strict_json_object(source, source_name=effective_name)
    try:
        canonical = canonical_json_bytes(document)
    except ValueError as exc:
        raise ExtensionSchemaError(
            f"{effective_name}: extension schema at $ is outside the canonical JSON "
            f"domain: {exc}; use finite JSON numbers, strings, arrays, and objects"
        ) from exc

    canonical_sha256 = sha256_bytes(canonical)
    if reference.sha256 is not None and canonical_sha256 != reference.sha256:
        raise ExtensionSchemaError(
            f"{effective_name}: extension schema at $ has canonical SHA-256 "
            f"{canonical_sha256}, but Project reference {reference.path!r} expects "
            f"{reference.sha256}; restore the referenced schema or update the Project "
            "digest in the same committed revision"
        )

    dialect = document.get("$schema")
    if dialect != JSON_SCHEMA_DIALECT:
        raise ExtensionSchemaError(
            f"{effective_name}: extension schema keyword $.$schema must be exactly "
            f"{JSON_SCHEMA_DIALECT!r}, got {dialect!r}; declare Draft 2020-12 "
            "explicitly"
        )
    try:
        # The registry behind this public helper contains only authenticated,
        # packaged schemas and has no network-retrieval callback.
        offline_validator(document)
    except BundledSchemaError as exc:
        raise ExtensionSchemaError(
            f"{effective_name}: invalid Draft 2020-12 extension schema at $: {exc}; "
            "fix the reported schema keyword before admission"
        ) from exc

    schema_id = document.get("$id")
    return ExtensionSchema(
        source_name=effective_name,
        reference_path=reference.path,
        source_bytes=source,
        source_sha256=sha256_bytes(source),
        canonical_bytes=canonical,
        canonical_sha256=canonical_sha256,
        schema_id=schema_id if type(schema_id) is str else None,
    )


def _json_copy(value: object, *, source: str, path: str) -> JSONValue:
    """Copy immutable authoring values into exact JSON-native containers."""

    value_type = type(value)
    if value is None or value_type in (bool, int, float, str):
        return value  # type: ignore[return-value]
    if isinstance(value, Mapping):
        copied: dict[str, JSONValue] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ExtensionValidationError(
                    f"{source}: extension value at {path} has non-string key "
                    f"{key!r}; use JSON object string keys"
                )
            copied[key] = _json_copy(
                item,
                source=source,
                path=f"{path}/{_pointer_token(key)}",
            )
        return copied
    if isinstance(value, (list, tuple)):
        return [
            _json_copy(item, source=source, path=f"{path}/{index}")
            for index, item in enumerate(value)
        ]
    raise ExtensionValidationError(
        f"{source}: extension value at {path} has non-JSON type "
        f"{value_type.__name__}; replace it with JSON-native data"
    )


def _pointer_token(value: object) -> str:
    return str(value).replace("~", "~0").replace("/", "~1")


def _namespace_payload(
    extensions: Mapping[str, object],
    *,
    namespace: str,
    source: str,
    path: str,
) -> object:
    for candidate in extensions:
        if candidate != namespace:
            raise ExtensionValidationError(
                f"{source}: extension namespace at "
                f"{path}/{_pointer_token(candidate)} is {candidate!r}, but only "
                f"extensions.{namespace} is allowed in this project context; move "
                f"the payload under {namespace!r} or remove it"
            )
    if namespace not in extensions:
        return _MISSING
    payload = extensions[namespace]
    if not isinstance(payload, Mapping):
        raise ExtensionValidationError(
            f"{source}: extension namespace {namespace!r} at "
            f"{path}/{_pointer_token(namespace)} must contain a JSON object; wrap "
            "the project-specific fields in an object"
        )
    return _json_copy(
        payload,
        source=source,
        path=f"{path}/{_pointer_token(namespace)}",
    )


def _validation_path(error: ValidationError) -> str:
    if not error.absolute_path:
        return "$"
    return "$/" + "/".join(_pointer_token(part) for part in error.absolute_path)


def _validator_from_evidence(evidence: ExtensionSchema) -> Draft202012Validator:
    document = _strict_json_object(
        evidence.canonical_bytes,
        source_name=evidence.source_name,
    )
    try:
        return offline_validator(document)
    except BundledSchemaError as exc:  # pragma: no cover - authenticated invariant
        raise ExtensionSchemaError(
            f"{evidence.source_name}: authenticated extension schema at $ could not "
            f"be reconstructed: {exc}; reload the original schema source"
        ) from exc


def _schema_object_paths(document: dict[str, JSONValue]) -> dict[int, str]:
    """Index exact schema-object identities by deterministic JSON Pointer path."""

    paths: dict[int, str] = {}

    def visit(value: JSONValue, path: str) -> None:
        if type(value) is dict:
            paths[id(value)] = path
            for key in sorted(value):
                visit(value[key], f"{path}/{_pointer_token(key)}")
        elif type(value) is list:
            for index, item in enumerate(value):
                visit(item, f"{path}/{index}")

    visit(document, "$")
    return paths


def _preflight_schema_references(
    document: dict[str, JSONValue],
    *,
    source_name: str,
    namespace: str,
) -> None:
    """Resolve every schema reference against the admitted offline registry.

    ``jsonschema`` resolves references lazily, so a remote reference beneath an
    unused ``$defs`` entry or an unselected conditional would otherwise survive
    admission. ``Resource.subresources()`` supplies Draft 2020-12's actual
    subschema locations (excluding data under keywords such as ``const``), and
    ``Resolver.in_subresource()`` preserves nested ``$id`` base-URI semantics.
    """

    paths = _schema_object_paths(document)
    root = Resource.from_contents(document)
    root_resolver = bundled_schema_registry().resolver_with_root(root)
    resources: list[tuple[str, Resource[Any], Any]] = []
    visited: set[int] = set()

    def collect(resource: Resource[Any], resolver: Any) -> None:
        contents = resource.contents
        contents_id = id(contents)
        if contents_id in visited:
            return
        visited.add(contents_id)
        path = paths.get(contents_id, "$")
        resources.append((path, resource, resolver))
        for subresource in resource.subresources():
            collect(subresource, resolver.in_subresource(subresource))

    collect(root, root_resolver)
    for path, resource, resolver in sorted(resources, key=lambda item: item[0]):
        contents = resource.contents
        if type(contents) is not dict:
            continue
        for keyword in ("$ref", "$dynamicRef"):
            reference = contents.get(keyword)
            if reference is None:
                continue
            # Draft 2020-12 meta-validation establishes this before preflight.
            assert type(reference) is str
            try:
                resolved = resolver.lookup(reference)
            except RecursionError as exc:
                raise ExtensionValidationError(
                    f"{source_name}: extensions.{namespace} schema {keyword} "
                    f"{reference!r} at schema path {path} exceeded bounded offline "
                    "reference resolution; replace the recursive reference chain "
                    "with a finite local target"
                ) from exc
            except Unresolvable as exc:
                raise ExtensionValidationError(
                    f"{source_name}: extensions.{namespace} schema could not resolve "
                    f"a {keyword} {reference!r} declared at schema path {path}: {exc}; "
                    "use a same-resource fragment or an authenticated bundled schema "
                    "identity (network retrieval is disabled)"
                ) from exc
            if type(resolved.contents) not in (dict, bool):
                raise ExtensionValidationError(
                    f"{source_name}: extensions.{namespace} schema {keyword} "
                    f"{reference!r} declared at schema path {path} resolves to "
                    f"{type(resolved.contents).__name__}, not a JSON Schema object "
                    "or boolean; point the reference at a schema resource"
                )


def validate_namespaced_extensions(
    project: Project,
    card: ExperimentCard | None = None,
    schema_source: bytes | None = None,
) -> ExtensionSchema | None:
    """Validate all present project/card/job payloads in one schema envelope.

    Only the active Project key is an admitted namespace. When a schema is
    declared, a single envelope allows requirements to span ``project``,
    ``card``, and job-id-keyed ``jobs`` payload locations.
    """

    if type(project) is not Project:
        raise TypeError(
            f"project must be exactly a validated Project, got "
            f"{type(project).__name__}; "
            "use Project.from_document() or Project.from_yaml()"
        )
    if card is not None and type(card) is not ExperimentCard:
        raise TypeError(
            f"card must be exactly a validated ExperimentCard, got "
            f"{type(card).__name__}; "
            "use ExperimentCard.from_document() or ExperimentCard.from_yaml()"
        )
    namespace = project.key
    envelope: dict[str, JSONValue] = {}
    project_payload = _namespace_payload(
        project.extensions,
        namespace=namespace,
        source=f"Project {project.key!r}",
        path="$/project/extensions",
    )
    if project_payload is not _MISSING:
        envelope["project"] = project_payload  # type: ignore[assignment]

    if card is not None:
        if card.project_key != namespace:
            raise ExtensionValidationError(
                f"ExperimentCard for {card.project_key!r}: project identity at "
                f"$/card/projectKey does not match Project {namespace!r}; validate "
                "the card with its owning Project"
            )
        card_payload = _namespace_payload(
            card.extensions,
            namespace=namespace,
            source=f"ExperimentCard for Project {namespace!r}",
            path="$/card/extensions",
        )
        if card_payload is not _MISSING:
            envelope["card"] = card_payload  # type: ignore[assignment]

        job_payloads: dict[str, JSONValue] = {}
        for job in card.jobs:
            job_payload = _namespace_payload(
                job.extensions,
                namespace=namespace,
                source=f"ExperimentCard job {job.id!r} for Project {namespace!r}",
                path=f"$/jobs/{_pointer_token(job.id)}/extensions",
            )
            if job_payload is not _MISSING:
                job_payloads[job.id] = job_payload  # type: ignore[assignment]
        if job_payloads:
            envelope["jobs"] = job_payloads

    try:
        canonical_json_bytes(envelope)
    except ValueError as exc:
        raise ExtensionValidationError(
            f"Project {namespace!r}: extensions.{namespace} envelope contains "
            f"invalid JSON data at {exc}; replace it with finite, portable "
            "JSON-native values"
        ) from exc

    reference = project.extension_schema
    if reference is None:
        if schema_source is not None:
            raise ExtensionSchemaError(
                f"Project {namespace!r}: unexpected extension schema bytes at $; "
                "declare spec.extensionSchema in the Project or omit schema_source"
            )
        return None
    if schema_source is None:
        raise ExtensionSchemaError(
            f"Project {namespace!r}: declared extension schema source "
            f"{reference.path!r} is required but missing at $; provide its exact "
            "bytes during validation and admission"
        )

    evidence = load_extension_schema(
        schema_source,
        reference,
        source_name=reference.path,
    )
    schema_document = _strict_json_object(
        evidence.canonical_bytes,
        source_name=evidence.source_name,
    )
    _preflight_schema_references(
        schema_document,
        source_name=evidence.source_name,
        namespace=namespace,
    )
    validator = _validator_from_evidence(evidence)
    try:
        validator.validate(envelope)
    except RecursionError as exc:
        raise ExtensionValidationError(
            f"{evidence.source_name}: extensions.{namespace} schema exhausted bounded "
            "validation through a recursive $ref or $dynamicRef at envelope path $; "
            "replace every unconditional reference cycle with a schema that has a "
            "finite base case"
        ) from exc
    except Unresolvable as exc:
        raise ExtensionValidationError(
            f"{evidence.source_name}: extensions.{namespace} schema could not resolve "
            f"a $ref while validating envelope path $: {exc}; vendor the referenced "
            "schema into the admitted offline registry or replace it with a local "
            "reference (network retrieval is disabled)"
        ) from exc
    except ValidationError as exc:
        path = _validation_path(exc)
        raise ExtensionValidationError(
            f"{evidence.source_name}: extensions.{namespace} envelope failed schema "
            f"validation at {path}: {exc.message}; fix that extension payload or "
            "the declared schema requirement"
        ) from exc
    return evidence


__all__ = [
    "ExtensionSchema",
    "ExtensionSchemaError",
    "ExtensionValidationError",
    "load_extension_schema",
    "validate_namespaced_extensions",
]
