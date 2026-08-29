"""Define the bounded, canonical QueueExport/v1 evidence envelope."""

from __future__ import annotations

from base64 import b64decode, b64encode
from binascii import Error as Base64Error
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import PurePath, PurePosixPath
import re
from typing import Final, Mapping, Self, cast
from uuid import RFC_4122, UUID

from experiment_queue.cooperative_yield import (
    CooperativeYieldError,
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    validate_receipt_for_request,
)
from experiment_queue.protocols import (
    DATABASE_V5,
    QUEUE_EXPORT_V1,
    ProtocolIdentityError,
    ProtocolVersion,
)
from experiment_queue.serialization import (
    CanonicalJSONError,
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)
from experiment_queue.schema_registry import (
    EXPERIMENT_CARD_V1_SCHEMA,
    PROJECT_V1_SCHEMA,
)


MAX_QUEUE_EXPORT_BYTES: Final = 64 * 1024 * 1024
MAX_QUEUE_EXPORT_RECORDS: Final = 100_000
MAX_QUEUE_EXPORT_TOTAL_RECORDS: Final = 100_000
MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES: Final = 32 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class QueueExportError(ValueError):
    """Raised when evidence cannot form an exact QueueExport/v1 document."""


def _exact(value: object, *, name: str, fields: set[str]) -> dict[str, JSONValue]:
    if type(value) is not dict:
        raise QueueExportError(f"{name} must be a JSON object")
    result = cast(dict[str, JSONValue], value)
    missing = sorted(fields - set(result))
    unknown = sorted(set(result) - fields)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if unknown:
            details.append(f"unknown fields {unknown}")
        raise QueueExportError(f"{name} has invalid fields: {'; '.join(details)}")
    return result


def _allowed(
    value: object,
    *,
    name: str,
    required: set[str],
    optional: set[str],
) -> dict[str, JSONValue]:
    if type(value) is not dict:
        raise QueueExportError(f"{name} must be a JSON object")
    result = cast(dict[str, JSONValue], value)
    missing = sorted(required - set(result))
    unknown = sorted(set(result) - required - optional)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if unknown:
            details.append(f"unknown fields {unknown}")
        raise QueueExportError(f"{name} has invalid fields: {'; '.join(details)}")
    return result


def _text(value: object, *, name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise QueueExportError(
            f"{name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) in {127, 0x85, 0x2028, 0x2029}
        for character in value
    ):
        raise QueueExportError(f"{name} must be log-safe text of at most {maximum} characters")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise QueueExportError(f"{name} must be Unicode scalar text") from exc
    return value


def _positive(value: object, *, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise QueueExportError(f"{name} must be a positive integer")
    return value


def _timestamp(value: object, *, name: str) -> str:
    text = _text(value, name=name, maximum=64)
    try:
        parsed = datetime.fromisoformat(
            f"{text[:-1]}+00:00" if text.endswith("Z") else text
        )
    except (ValueError, Base64Error) as exc:
        raise QueueExportError(f"{name} must be a real RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QueueExportError(f"{name} must include an explicit UTC offset")
    return text


def _array(value: object, *, name: str) -> list[JSONValue]:
    if type(value) is not list:
        raise QueueExportError(f"{name} must be a JSON array")
    result = cast(list[JSONValue], value)
    if len(result) > MAX_QUEUE_EXPORT_RECORDS:
        raise QueueExportError(
            f"{name} exceeds the {MAX_QUEUE_EXPORT_RECORDS} record limit"
        )
    return result


def _sha(value: object, *, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise QueueExportError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _portable_path(value: object, *, name: str) -> str:
    """Validate one normalized repository-relative POSIX evidence path."""

    text = _text(value, name=name)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or "\\" in text
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != text
    ):
        raise QueueExportError(f"{name} must be a normalized repository-relative path")
    return text


def _nullable_text(value: object, *, name: str, maximum: int = 4096) -> str | None:
    return None if value is None else _text(value, name=name, maximum=maximum)


def _nonnegative(value: object, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise QueueExportError(f"{name} must be a nonnegative integer")
    return value


def _boolean(value: object, *, name: str) -> bool:
    if type(value) is not bool:
        raise QueueExportError(f"{name} must be true or false")
    return value


def _decode_evidence(
    value: object, *, name: str, json_document: bool
) -> tuple[bytes, JSONValue | None]:
    fields = {"sourceSha256", "sourceBytes", "sourceBase64"}
    if json_document:
        fields.add("document")
    evidence = _exact(value, name=name, fields=fields)
    digest = _sha(evidence["sourceSha256"], name=f"{name}.sourceSha256")
    length = _positive(evidence["sourceBytes"], name=f"{name}.sourceBytes")
    if length > MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES:
        raise QueueExportError(f"{name}.sourceBytes exceeds the exact-source limit")
    encoded = _text(
        evidence["sourceBase64"], name=f"{name}.sourceBase64",
        maximum=MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES * 2,
    )
    try:
        source = b64decode(encoded, validate=True)
    except (ValueError, Base64Error) as exc:
        raise QueueExportError(f"{name}.sourceBase64 is not strict base64") from exc
    if len(source) != length or sha256_bytes(source) != digest:
        raise QueueExportError(f"{name} exact source length or SHA-256 differs")
    if not json_document:
        return source, None
    try:
        parsed = json.loads(source.decode("utf-8", errors="strict"))
        canonical = canonical_json_bytes(cast(JSONValue, parsed))
    except (UnicodeDecodeError, json.JSONDecodeError, CanonicalJSONError, ValueError) as exc:
        raise QueueExportError(f"{name} source is not canonical UTF-8 JSON") from exc
    if source != canonical or parsed != evidence["document"]:
        raise QueueExportError(f"{name}.document differs from exact canonical source")
    return source, cast(JSONValue, parsed)


def database_instance_document(
    *, state_directory: str, database_path: str, instance_identity: str
) -> dict[str, JSONValue]:
    """Return stable identity for one Database/v5 location and schema."""

    state = _text(state_directory, name="database.stateDirectory")
    database = _text(database_path, name="database.databasePath")
    if not PurePath(state).is_absolute() or not PurePath(database).is_absolute():
        raise QueueExportError("database state and file paths must be absolute")
    instance = _text(instance_identity, name="database.instanceIdentity", maximum=36)
    try:
        parsed_instance = UUID(instance)
    except ValueError as exc:
        raise QueueExportError("database.instanceIdentity must be an RFC 4122 UUID") from exc
    if (
        str(parsed_instance) != instance
        or parsed_instance.version != 4
        or parsed_instance.variant != RFC_4122
    ):
        raise QueueExportError("database.instanceIdentity must be canonical lowercase UUIDv4")
    identity: dict[str, JSONValue] = {
        **DATABASE_V5.document_identity(),
        "schemaIdentity": "experiment-queue/database-v5",
        "instanceIdentity": instance,
        "stateDirectory": state,
        "databasePath": database,
    }
    return {
        **identity,
        "instanceIdentitySha256": sha256_bytes(canonical_json_bytes(identity)),
    }


def binary_evidence_document(
    *, source: bytes, source_sha256: str
) -> dict[str, JSONValue]:
    """Encode exact immutable bytes with independently checked length and digest."""

    if type(source) is not bytes:
        raise TypeError("evidence source must be bytes")
    return {
        "sourceSha256": source_sha256,
        "sourceBytes": len(source),
        "sourceBase64": b64encode(source).decode("ascii"),
    }


def json_evidence_document(
    *, source: bytes, source_sha256: str, document: JSONValue
) -> dict[str, JSONValue]:
    """Encode exact canonical JSON together with its detached parsed document."""

    return {**binary_evidence_document(source=source, source_sha256=source_sha256), "document": document}


def wire_evidence_document(
    *,
    queue_item_id: int,
    project_id: int,
    revision_id: int,
    request_id: str,
    source: bytes,
    source_sha256: str,
    document: Mapping[str, object],
) -> dict[str, JSONValue]:
    """Encode exact persisted cooperative-yield source without losing bytes."""

    if type(source) is not bytes:
        raise TypeError("cooperative-yield source must be bytes")
    return {
        "queueItemId": queue_item_id,
        "projectId": project_id,
        "revisionId": revision_id,
        "requestId": request_id,
        "sourceSha256": source_sha256,
        "sourceBytes": len(source),
        "sourceBase64": b64encode(source).decode("ascii"),
        "document": cast(JSONValue, dict(document)),
    }


def _cooperative_yield_wire_bytes(document: Mapping[str, object]) -> bytes:
    """Match the deterministic CooperativeYield/v1 on-disk JSON encoder."""

    try:
        return (
            json.dumps(
                document,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise QueueExportError(
            f"cooperative-yield document is not finite UTF-8 JSON: {exc}"
        ) from exc


def _git_blob_object_id(source: bytes, recorded_object_id: object) -> str:
    """Recompute a SHA-1 or SHA-256 Git blob OID from exact source bytes."""

    if type(recorded_object_id) is not str or re.fullmatch(
        r"[0-9a-f]{40}|[0-9a-f]{64}", recorded_object_id
    ) is None:
        raise QueueExportError("Git blob objectId must be lowercase SHA-1 or SHA-256")
    framed = b"blob " + str(len(source)).encode("ascii") + b"\0" + source
    algorithm = hashlib.sha1 if len(recorded_object_id) == 40 else hashlib.sha256
    return algorithm(framed).hexdigest()


def _validate_wire_evidence(
    value: object,
    *,
    name: str,
    project_id: int,
    item_ids: set[int],
    revision_ids: set[int],
    receipt: bool,
) -> CooperativeYieldRequest | CooperativeYieldReceipt:
    evidence = _exact(
        value,
        name=name,
        fields={
            "queueItemId", "projectId", "revisionId", "requestId",
            "sourceSha256", "sourceBytes", "sourceBase64", "document",
        },
    )
    item_id = _positive(evidence["queueItemId"], name=f"{name}.queueItemId")
    revision_id = _positive(evidence["revisionId"], name=f"{name}.revisionId")
    if evidence["projectId"] != project_id or item_id not in item_ids:
        raise QueueExportError(f"{name} escapes exported Project/item ownership")
    if revision_id not in revision_ids:
        raise QueueExportError(f"{name} names an unexported revision")
    request_id = _text(evidence["requestId"], name=f"{name}.requestId", maximum=256)
    digest = _sha(evidence["sourceSha256"], name=f"{name}.sourceSha256")
    byte_count = evidence["sourceBytes"]
    if type(byte_count) is not int or byte_count <= 0 or byte_count > MAX_QUEUE_EXPORT_BYTES:
        raise QueueExportError(f"{name}.sourceBytes is outside the bounded range")
    encoded = _text(evidence["sourceBase64"], name=f"{name}.sourceBase64", maximum=MAX_QUEUE_EXPORT_BYTES * 2)
    try:
        source = b64decode(encoded, validate=True)
    except ValueError as exc:
        raise QueueExportError(f"{name}.sourceBase64 is not strict base64") from exc
    if len(source) != byte_count or sha256_bytes(source) != digest:
        raise QueueExportError(f"{name} exact source length or SHA-256 differs")
    def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, nested_value in pairs:
            if key in result:
                raise QueueExportError(
                    f"{name} exact source repeats JSON key {key!r}"
                )
            result[key] = nested_value
        return result

    try:
        parsed_document = json.loads(
            source.decode("utf-8", errors="strict"),
            object_pairs_hook=no_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"unsupported JSON constant {token}")
            ),
        )
    except QueueExportError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise QueueExportError(f"{name} exact source is not UTF-8 JSON") from exc
    if parsed_document != evidence["document"]:
        raise QueueExportError(f"{name}.document differs from its exact source")
    if type(parsed_document) is not dict or _cooperative_yield_wire_bytes(
        cast(Mapping[str, object], parsed_document)
    ) != source:
        raise QueueExportError(
            f"{name} exact source does not use the deterministic "
            "CooperativeYield/v1 wire encoding"
        )
    try:
        parsed = (
            CooperativeYieldReceipt.from_document(cast(Mapping[str, object], parsed_document))
            if receipt
            else CooperativeYieldRequest.from_document(cast(Mapping[str, object], parsed_document))
        )
    except (CooperativeYieldError, TypeError, ValueError) as exc:
        raise QueueExportError(f"{name} protocol evidence is invalid: {exc}") from exc
    if parsed.queue_item_id != item_id or parsed.request_id != request_id:
        raise QueueExportError(f"{name} decomposed identity differs from its document")
    return parsed


def _validate_admission_snapshot(
    value: object,
    *,
    name: str,
    snapshot_id: int,
    project_key: str,
    revision_label: str,
    git_commit: str,
    card_path: str,
    card_sha256: str,
    job_id: str,
    priority: int,
    preemptible: bool,
    command_text: str,
    runner_name: str,
    revision: Mapping[str, JSONValue],
) -> tuple[int, ...]:
    snapshot = _exact(
        value, name=name,
        fields={
            "id", "projectRevision", "gitCommit", "packageVersion",
            "projectSourceName", "projectSource", "projectNormalized", "projectSchema",
            "cardSourceName", "cardSource", "cardNormalized", "cardSchema",
            "extensionSchema", "resolved", "command", "submissionPolicy",
            "policyBindings", "policyDependencies",
        },
    )
    if snapshot["id"] != snapshot_id:
        raise QueueExportError(f"{name}.id differs from queue item snapshotId")
    if snapshot["projectRevision"] != revision_label or snapshot["gitCommit"] != git_commit:
        raise QueueExportError(f"{name} revision/Git identity differs from queue item")
    _text(snapshot["packageVersion"], name=f"{name}.packageVersion", maximum=128)
    _text(snapshot["projectSourceName"], name=f"{name}.projectSourceName")
    _decode_evidence(snapshot["projectSource"], name=f"{name}.projectSource", json_document=False)
    _decode_evidence(snapshot["projectNormalized"], name=f"{name}.projectNormalized", json_document=True)
    _text(snapshot["cardSourceName"], name=f"{name}.cardSourceName")
    card_source, _ = _decode_evidence(snapshot["cardSource"], name=f"{name}.cardSource", json_document=False)
    if sha256_bytes(card_source) != card_sha256 or snapshot["cardSourceName"] != card_path:
        raise QueueExportError(f"{name} card evidence differs from queue item")
    _decode_evidence(snapshot["cardNormalized"], name=f"{name}.cardNormalized", json_document=True)
    for schema_name in ("projectSchema", "cardSchema"):
        schema = _exact(
            snapshot[schema_name], name=f"{name}.{schema_name}",
            fields={"apiVersion", "kind", "schemaId", "sha256"},
        )
        _text(schema["apiVersion"], name=f"{name}.{schema_name}.apiVersion", maximum=128)
        _text(schema["kind"], name=f"{name}.{schema_name}.kind", maximum=128)
        _text(schema["schemaId"], name=f"{name}.{schema_name}.schemaId")
        _sha(schema["sha256"], name=f"{name}.{schema_name}.sha256")
        expected_schema = PROJECT_V1_SCHEMA if schema_name == "projectSchema" else EXPERIMENT_CARD_V1_SCHEMA
        if (
            schema["apiVersion"] != expected_schema.protocol.api_version
            or schema["kind"] != expected_schema.protocol.kind.value
            or schema["schemaId"] != expected_schema.schema_id
            or schema["sha256"] != expected_schema.sha256
        ):
            raise QueueExportError(f"{name}.{schema_name} is not the installed v1 schema")
    extension = snapshot["extensionSchema"]
    if extension is not None:
        extension_doc = _exact(
            extension, name=f"{name}.extensionSchema",
            fields={"sourceName", "referencePath", "source", "canonical", "schemaId"},
        )
        _text(extension_doc["sourceName"], name=f"{name}.extensionSchema.sourceName")
        _text(extension_doc["referencePath"], name=f"{name}.extensionSchema.referencePath")
        _decode_evidence(extension_doc["source"], name=f"{name}.extensionSchema.source", json_document=False)
        _decode_evidence(extension_doc["canonical"], name=f"{name}.extensionSchema.canonical", json_document=True)
        if extension_doc["schemaId"] is not None:
            _text(extension_doc["schemaId"], name=f"{name}.extensionSchema.schemaId")
    typed_revision = cast(dict[str, JSONValue], revision["typedEvidence"])
    revision_identity = cast(dict[str, JSONValue], typed_revision["identity"])
    if (
        snapshot["projectSource"] != typed_revision["projectSource"]
        or snapshot["projectNormalized"] != typed_revision["projectNormalized"]
        or snapshot["projectSourceName"] != revision_identity["projectSourcePath"]
    ):
        raise QueueExportError(f"{name} Project source differs from owning revision")
    revision_schema = cast(dict[str, JSONValue], revision_identity["projectSchema"])
    snapshot_project_schema = cast(dict[str, JSONValue], snapshot["projectSchema"])
    if any(
        snapshot_project_schema[left] != revision_schema[right]
        for left, right in (("apiVersion", "apiVersion"), ("kind", "kind"), ("schemaId", "id"), ("sha256", "sha256"))
    ):
        raise QueueExportError(f"{name} Project schema differs from owning revision")
    revision_extension = revision_identity.get("extensionSchema")
    if (extension is None) != (revision_extension is None):
        raise QueueExportError(f"{name} extension evidence differs from owning revision")
    if extension is not None:
        extension_doc = cast(dict[str, JSONValue], extension)
        revision_extension_doc = cast(dict[str, JSONValue], revision_extension)
        if (
            extension_doc["source"] != typed_revision["extensionSource"]
            or extension_doc["canonical"] != typed_revision["extensionCanonical"]
            or extension_doc["sourceName"] != revision_extension_doc["path"]
            or extension_doc["referencePath"] != revision_extension_doc["path"]
            or cast(dict[str, JSONValue], extension_doc["source"])["sourceSha256"] != revision_extension_doc["sourceSha256"]
            or cast(dict[str, JSONValue], extension_doc["canonical"])["sourceSha256"] != revision_extension_doc["canonicalSha256"]
            or extension_doc["schemaId"] != revision_extension_doc.get("schemaId")
        ):
            raise QueueExportError(f"{name} extension source differs from owning revision")
    _decode_evidence(snapshot["resolved"], name=f"{name}.resolved", json_document=True)
    command_source, command = _decode_evidence(snapshot["command"], name=f"{name}.command", json_document=True)
    if type(command) is not dict:
        raise QueueExportError(f"{name}.command.document must be an object")
    if command_source.decode("utf-8") != command_text or runner_name != "run-experiment":
        raise QueueExportError(f"{name} command/runner evidence differs from queue item")
    _, policy = _decode_evidence(snapshot["submissionPolicy"], name=f"{name}.submissionPolicy", json_document=True)
    if type(policy) is not dict or (
        policy.get("projectKey") != project_key
        or policy.get("cardPath") != card_path
        or policy.get("jobId") != job_id
        or policy.get("priority") != priority
        or policy.get("preemptionAuthorized") is not preemptible
    ):
        raise QueueExportError(f"{name}.submissionPolicy differs from queue item")
    _, bindings = _decode_evidence(snapshot["policyBindings"], name=f"{name}.policyBindings", json_document=True)
    _, dependencies = _decode_evidence(snapshot["policyDependencies"], name=f"{name}.policyDependencies", json_document=True)
    if policy.get("bindings") != bindings or policy.get("dependencies") != dependencies:
        raise QueueExportError(f"{name} policy component evidence differs from policy")
    if type(dependencies) is not list or any(type(value) is not int or value <= 0 for value in dependencies):
        raise QueueExportError(f"{name}.policyDependencies must contain positive item IDs")
    return tuple(cast(list[int], dependencies))


@dataclass(frozen=True, slots=True, init=False)
class QueueExport:
    """One immutable QueueExport/v1 document backed by canonical JSON bytes."""

    _canonical_json: bytes = field(repr=False)

    @classmethod
    def create(
        cls,
        *,
        package_version: str,
        database: Mapping[str, object],
        exported_at: str,
        actor: str,
        host_state: Mapping[str, object],
        project: Mapping[str, object],
        revisions: list[dict[str, JSONValue]],
        items: list[dict[str, JSONValue]],
        events: list[dict[str, JSONValue]],
        artifacts: list[dict[str, JSONValue]],
        yield_requests: list[dict[str, JSONValue]],
        yield_receipts: list[dict[str, JSONValue]],
    ) -> Self:
        document: dict[str, JSONValue] = {
            **QUEUE_EXPORT_V1.document_identity(),
            "producer": {"package": "experiment-queue", "version": package_version},
            "database": cast(JSONValue, dict(database)),
            "exportedAt": exported_at,
            "actor": actor,
            "hostState": cast(JSONValue, dict(host_state)),
            "project": cast(JSONValue, dict(project)),
            "revisions": revisions,
            "items": items,
            "events": events,
            "artifacts": artifacts,
            "cooperativeYieldRequests": yield_requests,
            "cooperativeYieldReceipts": yield_receipts,
            "executorReceipts": {
                "exactSourceAvailable": False,
                "records": [],
                "reason": (
                    "Database/v5 stores authenticated terminal outcome columns but "
                    "not exact ExecutorReceipt source bytes; no protocol document "
                    "was reconstructed"
                ),
            },
        }
        return cls.from_document(document)

    @classmethod
    def from_document(cls, value: Mapping[str, object]) -> Self:
        document = _exact(
            dict(value),
            name="QueueExport/v1",
            fields={
                "apiVersion", "kind", "producer", "database", "exportedAt",
                "actor", "project", "revisions", "items", "events",
                "hostState",
                "artifacts", "cooperativeYieldRequests",
                "cooperativeYieldReceipts", "executorReceipts",
            },
        )
        try:
            identity = ProtocolVersion.from_document(document)
        except ProtocolIdentityError as exc:
            raise QueueExportError(f"unsupported QueueExport protocol: {exc}") from exc
        if identity != QUEUE_EXPORT_V1:
            raise QueueExportError("document is not QueueExport/v1")
        producer = _exact(document["producer"], name="producer", fields={"package", "version"})
        if producer["package"] != "experiment-queue":
            raise QueueExportError("producer.package must be 'experiment-queue'")
        _text(producer["version"], name="producer.version", maximum=128)
        database = _exact(
            document["database"], name="database",
            fields={"apiVersion", "kind", "schemaIdentity", "instanceIdentity", "stateDirectory", "databasePath", "instanceIdentitySha256"},
        )
        try:
            database_protocol = ProtocolVersion.from_document(database)
        except ProtocolIdentityError as exc:
            raise QueueExportError(f"database protocol identity is invalid: {exc}") from exc
        if database_protocol != DATABASE_V5:
            raise QueueExportError("database identity must be exactly Database/v5")
        expected_database = database_instance_document(
            state_directory=cast(str, database["stateDirectory"]),
            database_path=cast(str, database["databasePath"]),
            instance_identity=cast(str, database["instanceIdentity"]),
        )
        if database != expected_database:
            raise QueueExportError("database instance identity is invalid")
        _timestamp(document["exportedAt"], name="exportedAt")
        _text(document["actor"], name="actor", maximum=256)
        host = _exact(
            document["hostState"], name="hostState",
            fields={"dispatchPaused", "reason", "provenance"},
        )
        paused = _boolean(host["dispatchPaused"], name="hostState.dispatchPaused")
        reason = host["reason"]
        if paused:
            _text(reason, name="hostState.reason")
        elif reason != "":
            raise QueueExportError("unpaused hostState.reason must be empty")
        provenance = host["provenance"]
        if provenance is not None:
            host_event = _exact(
                provenance, name="hostState.provenance",
                fields={"eventId", "createdAt", "actor", "eventType", "payload"},
            )
            _positive(host_event["eventId"], name="hostState.provenance.eventId")
            _timestamp(host_event["createdAt"], name="hostState.provenance.createdAt")
            _text(host_event["actor"], name="hostState.provenance.actor", maximum=256)
            expected_type = "HOST_DISPATCH_PAUSED" if paused else "HOST_DISPATCH_RESUMED"
            if host_event["eventType"] != expected_type:
                raise QueueExportError("hostState provenance event does not match gate state")
            payload = _exact(
                host_event["payload"], name="hostState.provenance.payload",
                fields={"reason"} if paused else set(),
            )
            if paused and payload["reason"] != reason:
                raise QueueExportError("hostState provenance reason differs from metadata")
        project = _exact(
            document["project"],
            name="project",
            fields={
                "id", "key", "displayName", "lifecycle", "lifecycleReason",
                "lifecycleActor", "lifecycleChangedAt", "health",
                "circuitFailureCount", "healthReason", "healthActor",
                "healthChangedAt", "currentRevision", "hostDispatchPaused",
                "hostPauseReason", "dispatchAllowed", "queueCounts",
            },
        )
        project_id = _positive(project.get("id"), name="project.id")
        project_key = _text(project.get("key"), name="project.key", maximum=63)
        _text(project["displayName"], name="project.displayName", maximum=500)
        lifecycle = project["lifecycle"]
        if lifecycle not in {"active", "paused", "archived"}:
            raise QueueExportError("project.lifecycle is outside the Database/v5 domain")
        _text(project["lifecycleReason"], name="project.lifecycleReason")
        _text(project["lifecycleActor"], name="project.lifecycleActor", maximum=256)
        _timestamp(project["lifecycleChangedAt"], name="project.lifecycleChangedAt")
        health = project["health"]
        if health not in {"closed", "open"}:
            raise QueueExportError("project.health is outside the Database/v5 domain")
        failures = _nonnegative(project["circuitFailureCount"], name="project.circuitFailureCount")
        if health == "open" and failures == 0:
            raise QueueExportError("open Project health requires a positive failure count")
        _text(project["healthReason"], name="project.healthReason")
        _text(project["healthActor"], name="project.healthActor", maximum=256)
        _timestamp(project["healthChangedAt"], name="project.healthChangedAt")
        if project["hostDispatchPaused"] is not paused or project["hostPauseReason"] != reason:
            raise QueueExportError("Project host gate copy differs from hostState")
        dispatch_allowed = _boolean(project["dispatchAllowed"], name="project.dispatchAllowed")
        if dispatch_allowed != (not paused and lifecycle == "active" and health == "closed"):
            raise QueueExportError("project.dispatchAllowed differs from gate inputs")
        counts = project["queueCounts"]
        if type(counts) is not dict:
            raise QueueExportError("project.queueCounts must be an object")
        for state, count in counts.items():
            if state not in {"queued", "held", "blocked", "starting", "running", "yielding", "terminating", "force_killing", "succeeded", "failed", "interrupted", "force_killed", "removed"}:
                raise QueueExportError(f"project.queueCounts has unknown state {state!r}")
            _positive(count, name=f"project.queueCounts.{state}")
        current = _exact(
            project["currentRevision"],
            name="project.currentRevision",
            fields={"id", "sequence", "label", "kind", "gitCommit"},
        )
        revisions = _array(document["revisions"], name="revisions")
        revision_ids: set[int] = set()
        revision_by_id: dict[int, dict[str, JSONValue]] = {}
        previous_revision_sequence = 0
        for index, raw in enumerate(revisions):
            revision = _allowed(
                raw,
                name=f"revisions[{index}]",
                required={
                    "id", "projectId", "sequence", "label", "kind",
                    "displayName", "gitCommit", "checkoutPath",
                    "enrollmentSha256", "enrollmentEvidence", "createdAt", "createdActor",
                },
                optional={"typedEvidence", "gitEvidence"},
            )
            revision_id = _positive(revision.get("id"), name=f"revisions[{index}].id")
            if revision.get("projectId") != project_id or revision_id in revision_ids:
                raise QueueExportError("revision ownership or uniqueness is invalid")
            revision_ids.add(revision_id)
            sequence = _positive(revision["sequence"], name=f"revisions[{index}].sequence")
            if sequence <= previous_revision_sequence:
                raise QueueExportError("revisions must be strictly ordered by sequence")
            previous_revision_sequence = sequence
            label = _text(revision["label"], name=f"revisions[{index}].label", maximum=256)
            kind = revision["kind"]
            if kind not in {"project-v1", "legacy-v4"}:
                raise QueueExportError(f"revisions[{index}].kind is invalid")
            expected_label = f"{project_key}:r{sequence}" if kind == "project-v1" else f"{project_key}:legacy-r{sequence}"
            if label != expected_label:
                raise QueueExportError(f"revisions[{index}].label is not canonical for its Project")
            _text(revision["displayName"], name=f"revisions[{index}].displayName", maximum=500)
            commit = revision["gitCommit"]
            if kind == "project-v1":
                if type(commit) is not str or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit) is None:
                    raise QueueExportError(f"revisions[{index}].gitCommit is invalid")
                if "typedEvidence" not in revision or "gitEvidence" not in revision:
                    raise QueueExportError(f"typed revisions[{index}] lacks typed/Git evidence")
                typed = _exact(
                    revision["typedEvidence"], name=f"revisions[{index}].typedEvidence",
                    fields={"identity", "projectSource", "projectNormalized", "extensionSource", "extensionCanonical"},
                )
                identity_doc = _allowed(
                    typed["identity"], name=f"revisions[{index}].typedEvidence.identity",
                    required={"id", "projectId", "projectKey", "sequence", "label", "displayName", "gitCommit", "projectSourcePath", "projectSourceSha256", "projectNormalizedSha256", "projectSchema", "enrollmentSha256", "validatedPackageVersion", "createdActor", "createdAt"},
                    optional={"extensionSchema"},
                )
                if any(identity_doc[name] != revision[source] for name, source in (("id", "id"), ("projectId", "projectId"), ("sequence", "sequence"), ("label", "label"), ("displayName", "displayName"), ("gitCommit", "gitCommit"), ("enrollmentSha256", "enrollmentSha256"), ("createdActor", "createdActor"), ("createdAt", "createdAt"))) or identity_doc["projectKey"] != project_key:
                    raise QueueExportError(f"revisions[{index}].typedEvidence identity differs")
                revision_schema = _exact(
                    identity_doc["projectSchema"], name=f"revisions[{index}].typedEvidence.identity.projectSchema",
                    fields={"apiVersion", "kind", "id", "sha256"},
                )
                if revision_schema != {
                    "apiVersion": PROJECT_V1_SCHEMA.protocol.api_version,
                    "kind": PROJECT_V1_SCHEMA.protocol.kind.value,
                    "id": PROJECT_V1_SCHEMA.schema_id,
                    "sha256": PROJECT_V1_SCHEMA.sha256,
                }:
                    raise QueueExportError(f"revisions[{index}] Project schema identity is invalid")
                project_source_path = _portable_path(
                    identity_doc["projectSourcePath"],
                    name=f"revisions[{index}].typedEvidence.identity.projectSourcePath",
                )
                project_source, _ = _decode_evidence(typed["projectSource"], name=f"revisions[{index}].typedEvidence.projectSource", json_document=False)
                _, project_document = _decode_evidence(typed["projectNormalized"], name=f"revisions[{index}].typedEvidence.projectNormalized", json_document=True)
                if cast(dict[str, JSONValue], typed["projectSource"])["sourceSha256"] != identity_doc["projectSourceSha256"] or cast(dict[str, JSONValue], typed["projectNormalized"])["sourceSha256"] != identity_doc["projectNormalizedSha256"] or type(project_document) is not dict:
                    raise QueueExportError(f"revisions[{index}] typed Project source evidence differs")
                if (typed["extensionSource"] is None) != (typed["extensionCanonical"] is None):
                    raise QueueExportError(f"revisions[{index}] extension evidence is incomplete")
                extension_identity = identity_doc.get("extensionSchema")
                extension_source: bytes | None = None
                if typed["extensionSource"] is not None:
                    extension_source, _ = _decode_evidence(typed["extensionSource"], name=f"revisions[{index}].typedEvidence.extensionSource", json_document=False)
                    _decode_evidence(typed["extensionCanonical"], name=f"revisions[{index}].typedEvidence.extensionCanonical", json_document=True)
                if (extension_identity is None) != (extension_source is None):
                    raise QueueExportError(
                        f"revisions[{index}] extension identity/source evidence is incomplete"
                    )
                extension_identity_doc: dict[str, JSONValue] | None = None
                if extension_identity is not None:
                    extension_identity_doc = _allowed(
                        extension_identity,
                        name=f"revisions[{index}].typedEvidence.identity.extensionSchema",
                        required={"path", "sourceSha256", "canonicalSha256"},
                        optional={"schemaId"},
                    )
                    _portable_path(
                        extension_identity_doc["path"],
                        name=(
                            f"revisions[{index}].typedEvidence.identity."
                            "extensionSchema.path"
                        ),
                    )
                    _sha(
                        extension_identity_doc["sourceSha256"],
                        name=(
                            f"revisions[{index}].typedEvidence.identity."
                            "extensionSchema.sourceSha256"
                        ),
                    )
                    _sha(
                        extension_identity_doc["canonicalSha256"],
                        name=(
                            f"revisions[{index}].typedEvidence.identity."
                            "extensionSchema.canonicalSha256"
                        ),
                    )
                    if extension_identity_doc.get("schemaId") is not None:
                        _text(
                            extension_identity_doc["schemaId"],
                            name=(
                                f"revisions[{index}].typedEvidence.identity."
                                "extensionSchema.schemaId"
                            ),
                        )
                    if (
                        cast(dict[str, JSONValue], typed["extensionSource"])["sourceSha256"]
                        != extension_identity_doc["sourceSha256"]
                        or cast(dict[str, JSONValue], typed["extensionCanonical"])["sourceSha256"]
                        != extension_identity_doc["canonicalSha256"]
                    ):
                        raise QueueExportError(
                            f"revisions[{index}] extension digests differ from identity"
                        )
                git = _exact(
                    revision["gitEvidence"], name=f"revisions[{index}].gitEvidence",
                    fields={"repositoryRoot", "gitCommit", "projectBlob", "extensionSchemaBlob"},
                )
                if git["gitCommit"] != commit:
                    raise QueueExportError(f"revisions[{index}] Git evidence commit differs")
                repository_root = _text(
                    git["repositoryRoot"],
                    name=f"revisions[{index}].gitEvidence.repositoryRoot",
                )
                checkout_path = _text(
                    revision["checkoutPath"],
                    name=f"revisions[{index}].checkoutPath",
                )
                if (
                    repository_root != checkout_path
                    or not PurePath(repository_root).is_absolute()
                    or ".." in PurePath(repository_root).parts
                ):
                    raise QueueExportError(
                        f"revisions[{index}] Git repository root differs from checkout"
                    )
                project_blob = _exact(git["projectBlob"], name=f"revisions[{index}].gitEvidence.projectBlob", fields={"path", "objectId", "mode", "size", "sourceSha256"})
                if project_blob["sourceSha256"] != sha256_bytes(project_source) or project_blob["path"] != project_source_path:
                    raise QueueExportError(f"revisions[{index}] Project Git blob differs from source")
                _nonnegative(project_blob["size"], name=f"revisions[{index}].gitEvidence.projectBlob.size")
                if project_blob["size"] != len(project_source) or project_blob["mode"] not in {"100644", "100755"} or type(project_blob["objectId"]) is not str or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", cast(str, project_blob["objectId"])) is None:
                    raise QueueExportError(f"revisions[{index}] Project Git blob fields are invalid")
                if _git_blob_object_id(project_source, project_blob["objectId"]) != project_blob["objectId"]:
                    raise QueueExportError(
                        f"revisions[{index}] Project Git blob objectId differs "
                        "from exact source"
                    )
                if (git["extensionSchemaBlob"] is None) != (extension_source is None):
                    raise QueueExportError(
                        f"revisions[{index}] extension Git/source evidence is incomplete"
                    )
                if git["extensionSchemaBlob"] is not None:
                    assert extension_identity_doc is not None
                    assert extension_source is not None
                    extension_blob = _exact(git["extensionSchemaBlob"], name=f"revisions[{index}].gitEvidence.extensionSchemaBlob", fields={"path", "objectId", "mode", "size", "sourceSha256"})
                    _nonnegative(
                        extension_blob["size"],
                        name=f"revisions[{index}].gitEvidence.extensionSchemaBlob.size",
                    )
                    if (
                        extension_blob["path"] != extension_identity_doc["path"]
                        or extension_blob["sourceSha256"]
                        != extension_identity_doc["sourceSha256"]
                        or extension_blob["size"] != len(extension_source)
                        or extension_blob["mode"] not in {"100644", "100755"}
                        or type(extension_blob["objectId"]) is not str
                        or re.fullmatch(
                            r"[0-9a-f]{40}|[0-9a-f]{64}",
                            cast(str, extension_blob["objectId"]),
                        )
                        is None
                    ):
                        raise QueueExportError(f"revisions[{index}] extension Git blob differs from source")
                    if _git_blob_object_id(extension_source, extension_blob["objectId"]) != extension_blob["objectId"]:
                        raise QueueExportError(
                            f"revisions[{index}] extension Git blob objectId differs "
                            "from exact source"
                        )
            elif commit is not None and (type(commit) is not str or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit) is None):
                raise QueueExportError(f"revisions[{index}].gitCommit is invalid")
            elif "typedEvidence" in revision or "gitEvidence" in revision:
                raise QueueExportError(f"legacy revisions[{index}] must not claim typed evidence")
            checkout = PurePath(
                _text(
                    revision["checkoutPath"],
                    name=f"revisions[{index}].checkoutPath",
                )
            )
            if not checkout.is_absolute() or ".." in checkout.parts:
                raise QueueExportError(
                    f"revisions[{index}].checkoutPath must be absolute and non-traversing"
                )
            _sha(revision["enrollmentSha256"], name=f"revisions[{index}].enrollmentSha256")
            if "enrollmentEvidence" not in revision:
                raise QueueExportError(f"revisions[{index}] lacks exact Enrollment evidence")
            _, enrollment = _decode_evidence(revision["enrollmentEvidence"], name=f"revisions[{index}].enrollmentEvidence", json_document=True)
            if cast(dict[str, JSONValue], revision["enrollmentEvidence"])["sourceSha256"] != revision["enrollmentSha256"]:
                raise QueueExportError(f"revisions[{index}] Enrollment digest differs")
            if type(enrollment) is not dict:
                raise QueueExportError(f"revisions[{index}] Enrollment must be an object")
            enrollment_kind = "Enrollment" if kind == "project-v1" else "LegacyEnrollment"
            if (
                enrollment.get("kind") != enrollment_kind
                or enrollment.get("projectKey") != project_key
                or enrollment.get("checkoutDirectory") != revision["checkoutPath"]
                or (kind == "legacy-v4" and enrollment.get("gitCommit") != commit)
            ):
                raise QueueExportError(f"revisions[{index}] Enrollment identity differs")
            if kind == "project-v1" and enrollment.get("projectNormalizedSha256") != cast(dict[str, JSONValue], revision["typedEvidence"])["identity"]["projectNormalizedSha256"]:  # type: ignore[index]
                raise QueueExportError(f"revisions[{index}] Enrollment Project digest differs")
            _timestamp(revision["createdAt"], name=f"revisions[{index}].createdAt")
            _text(revision["createdActor"], name=f"revisions[{index}].createdActor", maximum=256)
            revision_by_id[revision_id] = revision
        current_id = _positive(current["id"], name="project.currentRevision.id")
        if current_id not in revision_by_id:
            raise QueueExportError("project.currentRevision names an unexported revision")
        current_revision = revision_by_id[current_id]
        for field_name, revision_name in (("sequence", "sequence"), ("label", "label"), ("kind", "kind"), ("gitCommit", "gitCommit")):
            if current[field_name] != current_revision[revision_name]:
                raise QueueExportError("project.currentRevision differs from exported revision")
        items = _array(document["items"], name="items")
        item_ids: set[int] = set()
        used_snapshot_ids: set[int] = set()
        snapshot_dependencies: dict[int, tuple[int, ...]] = {}
        previous_item_id = 0
        for index, raw in enumerate(items):
            item = _exact(
                raw,
                name=f"items[{index}]",
                fields={
                    "id", "projectId", "projectKey", "revisionId",
                    "revisionLabel", "admissionKind", "snapshotId", "jobId",
                    "experimentId", "attempt", "segment", "state",
                    "stateDetail", "priority", "resumeFront", "preemptible",
                    "cardPath", "cardSha256", "gitCommit", "addedAt",
                    "addedBy", "commandText", "runnerName", "admissionSnapshot",
                    "dependencies", "runtime",
                },
            )
            item_id = _positive(item.get("id"), name=f"items[{index}].id")
            if item_id <= previous_item_id:
                raise QueueExportError("items must be strictly ordered by id")
            previous_item_id = item_id
            revision_id = item.get("revisionId")
            if item.get("projectId") != project_id or revision_id not in revision_ids or item_id in item_ids:
                raise QueueExportError("item ownership, revision, or uniqueness is invalid")
            revision = revision_by_id[cast(int, revision_id)]
            if item["projectKey"] != project_key or item["revisionLabel"] != revision["label"] or item["gitCommit"] != revision["gitCommit"]:
                raise QueueExportError(f"items[{index}] Project/revision identity differs")
            admission_kind = item["admissionKind"]
            if admission_kind not in {"ExperimentCard/v1", "LegacyMarkdownCard/v0"}:
                raise QueueExportError(f"items[{index}].admissionKind is invalid")
            snapshot_id = item["snapshotId"]
            job_id_value = item["jobId"]
            if admission_kind == "ExperimentCard/v1":
                typed_snapshot_id = _positive(snapshot_id, name=f"items[{index}].snapshotId")
                if typed_snapshot_id in used_snapshot_ids:
                    raise QueueExportError("typed queue items reuse one AdmissionSnapshot id")
                used_snapshot_ids.add(typed_snapshot_id)
                job_id = _text(job_id_value, name=f"items[{index}].jobId", maximum=256)
                if revision["kind"] != "project-v1":
                    raise QueueExportError(f"typed items[{index}] must reference a project-v1 revision")
            elif snapshot_id is not None or job_id_value is not None or item["admissionSnapshot"] is not None:
                raise QueueExportError(f"legacy items[{index}] must not claim typed admission evidence")
            elif revision["kind"] != "legacy-v4":
                raise QueueExportError(f"legacy items[{index}] must reference a legacy-v4 revision")
            _text(item["experimentId"], name=f"items[{index}].experimentId", maximum=512)
            _positive(item["attempt"], name=f"items[{index}].attempt")
            segment = _positive(item["segment"], name=f"items[{index}].segment")
            state = item["state"]
            if state not in {"queued", "held", "blocked", "starting", "running", "yielding", "terminating", "force_killing", "succeeded", "failed", "interrupted", "force_killed", "removed"}:
                raise QueueExportError(f"items[{index}].state is invalid")
            _nullable_text(item["stateDetail"], name=f"items[{index}].stateDetail")
            if type(item["priority"]) is not int or not -(2**63) <= cast(int, item["priority"]) <= 2**63 - 1:
                raise QueueExportError(f"items[{index}].priority must be signed 64-bit")
            _boolean(item["resumeFront"], name=f"items[{index}].resumeFront")
            preemptible = _boolean(item["preemptible"], name=f"items[{index}].preemptible")
            card_path = _text(item["cardPath"], name=f"items[{index}].cardPath")
            card_sha = _sha(item["cardSha256"], name=f"items[{index}].cardSha256")
            _timestamp(item["addedAt"], name=f"items[{index}].addedAt")
            _text(item["addedBy"], name=f"items[{index}].addedBy", maximum=256)
            _text(item["commandText"], name=f"items[{index}].commandText", maximum=1_000_000)
            _text(item["runnerName"], name=f"items[{index}].runnerName", maximum=256)
            runtime = _exact(
                item["runtime"],
                name=f"items[{index}].runtime",
                fields={
                    "assignedGpuUuid", "assignedGpuIndex", "pid", "pgid",
                    "runtimeGpuLeaseHeld", "runtimeGpuLeaseReleasedAt",
                    "processStartTicks", "startedAt", "finishedAt",
                    "returnCode", "terminateRequestedAt", "terminateReason",
                    "terminationStage", "terminationSignalEpoch", "contentionDetected",
                    "repoDriftDetected", "runnerRunDirectory",
                    "runnerManifestPath", "rsyncPullCommand", "yieldRequestId",
                    "yieldRequestedAt", "yieldRequestedBy", "yieldNote", "yieldDurationHours",
                    "continuationCheckpoint", "continuationCheckpointSha256",
                    "continuationCheckpointMetadata", "continuationCheckpointMetadataSha256",
                    "continuationStep", "continuationWandbId", "historicalGitRef",
                    "historicalWorktreePath", "historicalWorktreeCreatedAt",
                    "historicalWorktreeRemovedAt", "historicalWorktreeCleanupError",
                    "runtimeGitRef", "runtimeWorktreePath", "runtimeWorktreeCreatedAt",
                    "runtimeWorktreeRemovedAt", "runtimeWorktreeCleanupError",
                },
            )
            for pid_field in ("pid", "pgid"):
                if runtime[pid_field] is not None:
                    _positive(runtime[pid_field], name=f"items[{index}].runtime.{pid_field}")
            for timestamp_field in ("runtimeGpuLeaseReleasedAt", "startedAt", "finishedAt", "terminateRequestedAt", "yieldRequestedAt", "historicalWorktreeCreatedAt", "historicalWorktreeRemovedAt", "runtimeWorktreeCreatedAt", "runtimeWorktreeRemovedAt"):
                if runtime[timestamp_field] is not None:
                    _timestamp(runtime[timestamp_field], name=f"items[{index}].runtime.{timestamp_field}")
            if runtime["terminationStage"] not in {None, "interrupt", "terminate", "kill"}:
                raise QueueExportError(f"items[{index}].runtime.terminationStage is invalid")
            if runtime["returnCode"] is not None and type(runtime["returnCode"]) is not int:
                raise QueueExportError(f"items[{index}].runtime.returnCode must be an integer or null")
            signal_epoch = runtime["terminationSignalEpoch"]
            if signal_epoch is not None and (
                type(signal_epoch) not in {int, float}
                or cast(float, signal_epoch) < 0
                or cast(float, signal_epoch) != cast(float, signal_epoch)
                or abs(cast(float, signal_epoch)) == float("inf")
            ):
                raise QueueExportError(f"items[{index}].runtime.terminationSignalEpoch is invalid")
            _boolean(runtime["contentionDetected"], name=f"items[{index}].runtime.contentionDetected")
            _boolean(runtime["repoDriftDetected"], name=f"items[{index}].runtime.repoDriftDetected")
            lease_held = _boolean(
                runtime["runtimeGpuLeaseHeld"],
                name=f"items[{index}].runtime.runtimeGpuLeaseHeld",
            )
            if (runtime["assignedGpuUuid"] is None) != (runtime["assignedGpuIndex"] is None):
                raise QueueExportError(f"items[{index}] assigned GPU identity is incomplete")
            active_state = state in {
                "starting", "running", "yielding", "terminating", "force_killing"
            }
            if active_state and not lease_held:
                raise QueueExportError(
                    f"items[{index}] active state must retain its runtime GPU lease"
                )
            if state == "queued" and lease_held:
                raise QueueExportError(
                    f"items[{index}] queued state cannot retain a runtime GPU lease"
                )
            if lease_held and state not in {
                "starting", "running", "yielding", "terminating", "force_killing",
                "succeeded", "failed", "interrupted", "force_killed",
            }:
                raise QueueExportError(
                    f"items[{index}] state {state!r} cannot retain a runtime GPU lease"
                )
            if lease_held and runtime["assignedGpuUuid"] is None:
                raise QueueExportError(
                    f"items[{index}] held runtime GPU lease lacks assigned identity"
                )
            if lease_held and runtime["runtimeGpuLeaseReleasedAt"] is not None:
                raise QueueExportError(
                    f"items[{index}] held runtime GPU lease claims a release time"
                )
            if runtime["runtimeGpuLeaseReleasedAt"] is not None and (
                lease_held or runtime["assignedGpuUuid"] is None
            ):
                raise QueueExportError(
                    f"items[{index}] runtime GPU lease release lacks complete "
                    "historical assignment or remains held"
                )
            for text_field in (
                "assignedGpuUuid", "assignedGpuIndex", "processStartTicks",
                "terminateReason", "runnerRunDirectory", "runnerManifestPath",
                "rsyncPullCommand", "yieldRequestId", "yieldRequestedBy", "yieldNote",
                "continuationCheckpoint", "continuationCheckpointMetadata",
                "continuationWandbId", "historicalGitRef", "historicalWorktreePath",
                "historicalWorktreeCleanupError", "runtimeGitRef", "runtimeWorktreePath",
                "runtimeWorktreeCleanupError",
            ):
                _nullable_text(runtime[text_field], name=f"items[{index}].runtime.{text_field}", maximum=1_000_000)
            for path_field in (
                "runnerRunDirectory", "runnerManifestPath", "historicalWorktreePath",
                "runtimeWorktreePath",
            ):
                path_value = runtime[path_field]
                if path_value is not None and (
                    not PurePath(cast(str, path_value)).is_absolute()
                    or ".." in PurePath(cast(str, path_value)).parts
                ):
                    raise QueueExportError(f"items[{index}].runtime.{path_field} must be absolute and non-traversing")
            for digest_field in ("continuationCheckpointSha256", "continuationCheckpointMetadataSha256"):
                if runtime[digest_field] is not None:
                    _sha(runtime[digest_field], name=f"items[{index}].runtime.{digest_field}")
            if (runtime["continuationCheckpoint"] is None) != (runtime["continuationCheckpointSha256"] is None):
                raise QueueExportError(f"items[{index}] continuation checkpoint/hash pair is incomplete")
            if (runtime["continuationCheckpointMetadata"] is None) != (runtime["continuationCheckpointMetadataSha256"] is None):
                raise QueueExportError(f"items[{index}] continuation metadata/hash pair is incomplete")
            for prefix in ("historical", "runtime"):
                ref = runtime[f"{prefix}GitRef"]
                path = runtime[f"{prefix}WorktreePath"]
                created = runtime[f"{prefix}WorktreeCreatedAt"]
                removed = runtime[f"{prefix}WorktreeRemovedAt"]
                cleanup_error = runtime[f"{prefix}WorktreeCleanupError"]
                if (ref is None) != (path is None) or (created is not None and path is None) or (removed is not None and created is None) or (cleanup_error is not None and path is None):
                    raise QueueExportError(f"items[{index}] {prefix} worktree identity/timestamps are inconsistent")
            if runtime["yieldDurationHours"] is not None and (type(runtime["yieldDurationHours"]) is not int or not 1 <= cast(int, runtime["yieldDurationHours"]) <= 24):
                raise QueueExportError(f"items[{index}].runtime.yieldDurationHours is invalid")
            if runtime["continuationStep"] is not None:
                _nonnegative(runtime["continuationStep"], name=f"items[{index}].runtime.continuationStep")
            yield_presence = tuple(
                runtime[field] is not None
                for field in ("yieldRequestId", "yieldRequestedAt", "yieldRequestedBy", "yieldNote")
            )
            if len(set(yield_presence)) != 1 or (
                runtime["yieldDurationHours"] is not None and runtime["yieldRequestId"] is None
            ):
                raise QueueExportError(f"items[{index}] cooperative-yield runtime identity is incomplete")
            dependencies = _array(item["dependencies"], name=f"items[{index}].dependencies")
            dependency_ids: set[int] = set()
            for dep_index, raw_dependency in enumerate(dependencies):
                dependency = _exact(raw_dependency, name=f"items[{index}].dependencies[{dep_index}]", fields={"itemId", "projectId", "projectKey", "revisionId", "revisionLabel", "state", "external"})
                dependency_id = _positive(dependency["itemId"], name=f"items[{index}].dependencies[{dep_index}].itemId")
                if dependency_id == item_id or dependency_id in dependency_ids:
                    raise QueueExportError(f"items[{index}] has invalid duplicate/self dependency")
                dependency_ids.add(dependency_id)
                _positive(dependency["projectId"], name=f"items[{index}].dependencies[{dep_index}].projectId")
                _text(dependency["projectKey"], name=f"items[{index}].dependencies[{dep_index}].projectKey", maximum=63)
                _positive(dependency["revisionId"], name=f"items[{index}].dependencies[{dep_index}].revisionId")
                _text(dependency["revisionLabel"], name=f"items[{index}].dependencies[{dep_index}].revisionLabel", maximum=256)
                if dependency["state"] not in {"queued", "held", "blocked", "starting", "running", "yielding", "terminating", "force_killing", "succeeded", "failed", "interrupted", "force_killed", "removed"}:
                    raise QueueExportError(f"items[{index}] dependency state is invalid")
                external = _boolean(dependency["external"], name=f"items[{index}].dependencies[{dep_index}].external")
                if external != (dependency["projectId"] != project_id):
                    raise QueueExportError(f"items[{index}] dependency external marker is false")
            if admission_kind == "ExperimentCard/v1":
                snapshot_dependencies[item_id] = _validate_admission_snapshot(
                    item["admissionSnapshot"], name=f"items[{index}].admissionSnapshot",
                    snapshot_id=typed_snapshot_id, project_key=project_key,
                    revision_label=cast(str, item["revisionLabel"]), git_commit=cast(str, item["gitCommit"]),
                    card_path=card_path, card_sha256=card_sha, job_id=job_id,
                    priority=cast(int, item["priority"]), preemptible=preemptible,
                    command_text=cast(str, item["commandText"]),
                    runner_name=cast(str, item["runnerName"]),
                    revision=revision,
                )
            item_ids.add(item_id)
        item_by_id = {cast(int, cast(dict[str, JSONValue], raw)["id"]): cast(dict[str, JSONValue], raw) for raw in items}
        for index, raw in enumerate(items):
            item = cast(dict[str, JSONValue], raw)
            for dependency in cast(list[JSONValue], item["dependencies"]):
                target = cast(dict[str, JSONValue], dependency)
                if target["external"] is False:
                    exported_target = item_by_id.get(cast(int, target["itemId"]))
                    if exported_target is None or any(target[key] != exported_target[source] for key, source in (("projectId", "projectId"), ("projectKey", "projectKey"), ("revisionId", "revisionId"), ("revisionLabel", "revisionLabel"), ("state", "state"))):
                        raise QueueExportError(f"items[{index}] internal dependency identity differs from exported target")
            emitted_dependency_ids = tuple(
                cast(int, cast(dict[str, JSONValue], dependency)["itemId"])
                for dependency in cast(list[JSONValue], item["dependencies"])
            )
            if emitted_dependency_ids != tuple(sorted(emitted_dependency_ids)):
                raise QueueExportError(f"items[{index}] dependencies must be ordered by itemId")
            if item["admissionKind"] == "ExperimentCard/v1" and snapshot_dependencies[cast(int, item["id"])] != emitted_dependency_ids:
                raise QueueExportError(f"items[{index}] typed policy dependencies differ from emitted targets")
        derived_counts: dict[str, int] = {}
        for raw in items:
            state = cast(str, cast(dict[str, JSONValue], raw)["state"])
            derived_counts[state] = derived_counts.get(state, 0) + 1
        if counts != derived_counts:
            raise QueueExportError("project.queueCounts differs from emitted items")
        events = _array(document["events"], name="events")
        event_ids: set[int] = set()
        previous_event_id = 0
        for index, raw in enumerate(events):
            event = _exact(raw, name=f"events[{index}]", fields={"id", "createdAt", "actor", "eventType", "scope", "projectId", "queueItemId", "payload"})
            event_id = _positive(event["id"], name=f"events[{index}].id")
            if event_id <= previous_event_id:
                raise QueueExportError("events must be strictly ordered by id")
            previous_event_id = event_id
            if event_id in event_ids or event["scope"] != "project" or event["projectId"] != project_id:
                raise QueueExportError("event identity or Project failure scope is invalid")
            if event["queueItemId"] is not None and event["queueItemId"] not in item_ids:
                raise QueueExportError("event names an unexported queue item")
            _timestamp(event["createdAt"], name=f"events[{index}].createdAt")
            _text(event["actor"], name=f"events[{index}].actor", maximum=256)
            _text(event["eventType"], name=f"events[{index}].eventType", maximum=256)
            if type(event["payload"]) is not dict:
                raise QueueExportError(f"events[{index}].payload must be an object")
            event_ids.add(event_id)
        artifacts = _array(document["artifacts"], name="artifacts")
        artifact_ids: set[int] = set()
        previous_artifact_id = 0
        for index, raw in enumerate(artifacts):
            artifact = _exact(
                raw,
                name=f"artifacts[{index}]",
                fields={
                    "id", "queueItemId", "projectId", "revisionId", "segment",
                    "evidenceKind", "name", "type", "root", "relativePath",
                    "absolutePath", "sizeBytes", "sha256", "recordedAt",
                    "metadata",
                },
            )
            artifact_id = _positive(artifact["id"], name=f"artifacts[{index}].id")
            if artifact_id <= previous_artifact_id:
                raise QueueExportError("artifacts must be strictly ordered by id")
            previous_artifact_id = artifact_id
            item_id = artifact.get("queueItemId")
            if artifact_id in artifact_ids or artifact.get("projectId") != project_id or item_id not in item_ids or artifact.get("revisionId") not in revision_ids:
                raise QueueExportError("artifact ownership is invalid")
            owner = item_by_id[cast(int, item_id)]
            if artifact["revisionId"] != owner["revisionId"]:
                raise QueueExportError("artifact revision differs from its queue item")
            artifact_segment = _positive(artifact["segment"], name=f"artifacts[{index}].segment")
            if artifact_segment > owner["segment"]:
                raise QueueExportError("artifact segment is newer than its queue item")
            if artifact["evidenceKind"] not in {"declared-v1", "legacy-v4"}:
                raise QueueExportError(f"artifacts[{index}].evidenceKind is invalid")
            expected_artifact_kind = (
                "declared-v1" if owner["admissionKind"] == "ExperimentCard/v1" else "legacy-v4"
            )
            if artifact["evidenceKind"] != expected_artifact_kind:
                raise QueueExportError("artifact evidence kind differs from its queue item")
            _text(artifact["name"], name=f"artifacts[{index}].name")
            if artifact["type"] not in {"file", "directory"}:
                raise QueueExportError(f"artifacts[{index}].type is invalid")
            absolute = _text(artifact["absolutePath"], name=f"artifacts[{index}].absolutePath")
            if not PurePath(absolute).is_absolute() or ".." in PurePath(absolute).parts:
                raise QueueExportError(f"artifacts[{index}].absolutePath is invalid")
            if artifact["sizeBytes"] is not None:
                _nonnegative(artifact["sizeBytes"], name=f"artifacts[{index}].sizeBytes")
            if artifact["sha256"] is not None:
                _sha(artifact["sha256"], name=f"artifacts[{index}].sha256")
            if artifact["evidenceKind"] == "declared-v1":
                root = _text(artifact["root"], name=f"artifacts[{index}].root", maximum=63)
                relative = _text(artifact["relativePath"], name=f"artifacts[{index}].relativePath")
                relative_path = PurePath(relative)
                if relative_path.is_absolute() or ".." in relative_path.parts or "\\" in relative:
                    raise QueueExportError(f"artifacts[{index}].relativePath is invalid")
                if not root:
                    raise QueueExportError(f"artifacts[{index}].root is invalid")
            elif artifact["root"] is not None or artifact["relativePath"] is not None:
                raise QueueExportError(f"legacy artifacts[{index}] must not claim declared-root evidence")
            _timestamp(artifact["recordedAt"], name=f"artifacts[{index}].recordedAt")
            artifact_ids.add(artifact_id)
        requests = _array(document["cooperativeYieldRequests"], name="cooperativeYieldRequests")
        request_ids: set[str] = set()
        requests_by_id: dict[str, CooperativeYieldRequest] = {}
        requests_by_item: dict[int, set[str]] = {}
        previous_request_order: tuple[int, int] = (0, 0)
        for index, raw in enumerate(requests):
            parsed_request = cast(CooperativeYieldRequest, _validate_wire_evidence(raw, name=f"cooperativeYieldRequests[{index}]", project_id=project_id, item_ids=item_ids, revision_ids=revision_ids, receipt=False))
            wrapper = cast(dict[str, JSONValue], raw)
            owner = item_by_id[cast(int, wrapper["queueItemId"])]
            order = (parsed_request.queue_item_id, parsed_request.segment)
            if order <= previous_request_order:
                raise QueueExportError("cooperative-yield requests must be ordered by item/segment")
            previous_request_order = order
            if wrapper["revisionId"] != owner["revisionId"] or parsed_request.segment > owner["segment"] or parsed_request.continuation.project_revision != owner["revisionLabel"] or parsed_request.continuation.git_commit != owner["gitCommit"]:
                raise QueueExportError("cooperative-yield request identity differs from owning item")
            if owner["admissionKind"] != "ExperimentCard/v1":
                raise QueueExportError("cooperative-yield request belongs to a legacy item")
            request_id = cast(dict[str, JSONValue], raw)["requestId"]
            if cast(str, request_id) in request_ids:
                raise QueueExportError("cooperative-yield request IDs must be unique")
            request_ids.add(cast(str, request_id))
            requests_by_id[cast(str, request_id)] = parsed_request
            requests_by_item.setdefault(parsed_request.queue_item_id, set()).add(cast(str, request_id))
        for item_id, owner in item_by_id.items():
            runtime_request_id = cast(dict[str, JSONValue], owner["runtime"])["yieldRequestId"]
            if runtime_request_id is not None and runtime_request_id not in requests_by_item.get(item_id, set()):
                raise QueueExportError("queue item yieldRequestId has no exported request evidence")
        receipts = _array(document["cooperativeYieldReceipts"], name="cooperativeYieldReceipts")
        receipt_ids: set[str] = set()
        previous_receipt_order: tuple[int, int] = (0, 0)
        for index, raw in enumerate(receipts):
            parsed_receipt = cast(CooperativeYieldReceipt, _validate_wire_evidence(raw, name=f"cooperativeYieldReceipts[{index}]", project_id=project_id, item_ids=item_ids, revision_ids=revision_ids, receipt=True))
            wrapper = cast(dict[str, JSONValue], raw)
            owner = item_by_id[cast(int, wrapper["queueItemId"])]
            order = (parsed_receipt.queue_item_id, parsed_receipt.segment)
            if order <= previous_receipt_order:
                raise QueueExportError("cooperative-yield receipts must be ordered by item/segment")
            previous_receipt_order = order
            if wrapper["revisionId"] != owner["revisionId"] or parsed_receipt.segment > owner["segment"]:
                raise QueueExportError("cooperative-yield receipt identity differs from owning item")
            if owner["admissionKind"] != "ExperimentCard/v1":
                raise QueueExportError("cooperative-yield receipt belongs to a legacy item")
            receipt_id = cast(str, cast(dict[str, JSONValue], raw)["requestId"])
            if receipt_id not in request_ids:
                raise QueueExportError("cooperative-yield receipt has no exported request")
            if receipt_id in receipt_ids:
                raise QueueExportError("cooperative-yield receipt IDs must be unique")
            receipt_ids.add(receipt_id)
            try:
                validate_receipt_for_request(parsed_receipt, requests_by_id[receipt_id])
            except CooperativeYieldError as exc:
                raise QueueExportError(f"cooperative-yield receipt/request binding is invalid: {exc}") from exc
        total_records = (
            len(revisions) + len(items) + len(events) + len(artifacts)
            + len(requests) + len(receipts)
            + sum(len(cast(list[JSONValue], cast(dict[str, JSONValue], raw)["dependencies"])) for raw in items)
            + 2
        )
        if total_records > MAX_QUEUE_EXPORT_TOTAL_RECORDS:
            raise QueueExportError(
                f"QueueExport has {total_records} total records; limit is {MAX_QUEUE_EXPORT_TOTAL_RECORDS}"
            )
        executor = _exact(document["executorReceipts"], name="executorReceipts", fields={"exactSourceAvailable", "records", "reason"})
        if executor["exactSourceAvailable"] is not False or executor["records"] != []:
            raise QueueExportError("ExecutorReceipt absence metadata must remain truthful")
        _text(executor["reason"], name="executorReceipts.reason")
        try:
            source = canonical_json_bytes(cast(JSONValue, document))
        except CanonicalJSONError as exc:
            raise QueueExportError(f"QueueExport is not bounded canonical JSON: {exc}") from exc
        if len(source) > MAX_QUEUE_EXPORT_BYTES:
            raise QueueExportError(f"QueueExport exceeds {MAX_QUEUE_EXPORT_BYTES} bytes")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_canonical_json", source)
        return instance

    @classmethod
    def from_bytes(cls, source: bytes) -> Self:
        if type(source) is not bytes or not source or len(source) > MAX_QUEUE_EXPORT_BYTES:
            raise QueueExportError("QueueExport source is empty, non-bytes, or oversized")
        def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise QueueExportError(f"QueueExport repeats JSON key {key!r}")
                result[key] = value
            return result
        try:
            value = json.loads(
                source.decode("utf-8", errors="strict"),
                object_pairs_hook=no_duplicates,
                parse_constant=lambda token: (_ for _ in ()).throw(ValueError(f"unsupported JSON constant {token}")),
            )
        except QueueExportError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise QueueExportError(f"QueueExport is not strict UTF-8 JSON: {exc}") from exc
        if type(value) is not dict:
            raise QueueExportError("QueueExport must contain one JSON object")
        export = cls.from_document(cast(dict[str, object], value))
        if source != export.canonical_json:
            raise QueueExportError("QueueExport source is not exact RFC 8785 canonical JSON")
        return export

    @property
    def canonical_json(self) -> bytes:
        return self._canonical_json

    @property
    def sha256(self) -> str:
        return sha256_bytes(self._canonical_json)

    def to_document(self) -> dict[str, JSONValue]:
        return cast(dict[str, JSONValue], json.loads(self._canonical_json))


__all__ = [
    "MAX_QUEUE_EXPORT_BYTES",
    "MAX_QUEUE_EXPORT_EXACT_SOURCE_BYTES",
    "MAX_QUEUE_EXPORT_TOTAL_RECORDS",
    "QueueExport",
    "QueueExportError",
    "binary_evidence_document",
    "database_instance_document",
    "json_evidence_document",
    "wire_evidence_document",
]
