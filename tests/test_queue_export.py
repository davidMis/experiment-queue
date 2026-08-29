"""Verify strict canonical QueueExport/v1 parsing and evidence binding."""

from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json

import pytest

from experiment_queue.cooperative_yield import (
    ContinuationIdentity,
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    YieldRequestKind,
)
from experiment_queue.queue_export import (
    QueueExport,
    QueueExportError,
    binary_evidence_document,
    database_instance_document,
    json_evidence_document,
    wire_evidence_document,
)
from experiment_queue.serialization import sha256_bytes
from experiment_queue.schema_registry import EXPERIMENT_CARD_V1_SCHEMA, PROJECT_V1_SCHEMA


NOW = "2026-08-28T20:00:00+00:00"


def _wire(document: dict[str, object]) -> bytes:
    return (
        json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def _git_blob_oid(source: bytes, *, sha256: bool = False) -> str:
    """Return the Git blob identity for exact fixture bytes."""

    framed = b"blob " + str(len(source)).encode("ascii") + b"\0" + source
    return (hashlib.sha256 if sha256 else hashlib.sha1)(framed).hexdigest()


def _replace_wire_source(wrapper: dict[str, object], source: bytes) -> None:
    """Keep an evidence envelope internally hashed while changing wire bytes."""

    wrapper["sourceBase64"] = base64.b64encode(source).decode("ascii")
    wrapper["sourceBytes"] = len(source)
    wrapper["sourceSha256"] = sha256_bytes(source)


def _project() -> dict[str, object]:
    return {
        "id": 1,
        "key": "project-one",
        "displayName": "Project One",
        "lifecycle": "active",
        "lifecycleReason": "registered",
        "lifecycleActor": "operator:test",
        "lifecycleChangedAt": NOW,
        "health": "closed",
        "circuitFailureCount": 0,
        "healthReason": "healthy",
        "healthActor": "operator:test",
        "healthChangedAt": NOW,
        "currentRevision": {
            "id": 11,
            "sequence": 1,
            "label": "project-one:legacy-r1",
            "kind": "legacy-v4",
            "gitCommit": None,
        },
        "hostDispatchPaused": False,
        "hostPauseReason": "",
        "dispatchAllowed": True,
        "queueCounts": {"failed": 1},
    }


def _revision() -> dict[str, object]:
    enrollment_document = {
        "kind": "LegacyEnrollment", "projectKey": "project-one",
        "checkoutDirectory": "/project-one", "gitCommit": None,
    }
    enrollment = json.dumps(enrollment_document, separators=(",", ":"), sort_keys=True).encode()
    return {
        "id": 11,
        "projectId": 1,
        "sequence": 1,
        "label": "project-one:legacy-r1",
        "kind": "legacy-v4",
        "displayName": "Project One",
        "gitCommit": None,
        "checkoutPath": "/project-one",
        "enrollmentSha256": sha256_bytes(enrollment),
        "enrollmentEvidence": json_evidence_document(source=enrollment, source_sha256=sha256_bytes(enrollment), document=enrollment_document),
        "createdAt": NOW,
        "createdActor": "operator:test",
    }


def _item() -> dict[str, object]:
    return {
        "id": 7,
        "projectId": 1,
        "projectKey": "project-one",
        "revisionId": 11,
        "revisionLabel": "project-one:legacy-r1",
        "admissionKind": "LegacyMarkdownCard/v0",
        "snapshotId": None,
        "jobId": None,
        "experimentId": "EXP-001",
        "attempt": 1,
        "segment": 1,
        "state": "failed",
        "stateDetail": "checkpoint declined",
        "priority": 0,
        "resumeFront": False,
        "preemptible": False,
        "cardPath": "cards/EXP-001.yaml",
        "cardSha256": "5" * 64,
        "gitCommit": None,
        "addedAt": NOW,
        "addedBy": "operator:test",
        "commandText": "python train.py",
        "runnerName": "legacy-runner",
        "admissionSnapshot": None,
        "dependencies": [],
        "runtime": {
            "assignedGpuUuid": None,
            "assignedGpuIndex": None,
            "runtimeGpuLeaseHeld": False,
            "runtimeGpuLeaseReleasedAt": None,
            "pid": None,
            "pgid": None,
            "processStartTicks": None,
            "startedAt": NOW,
            "finishedAt": NOW,
            "returnCode": 75,
            "terminateRequestedAt": None,
            "terminateReason": None,
            "terminationStage": None,
            "terminationSignalEpoch": None,
            "contentionDetected": False,
            "repoDriftDetected": False,
            "runnerRunDirectory": "/runs/7",
            "runnerManifestPath": "/runs/7/manifest.json",
            "rsyncPullCommand": None,
            "yieldRequestId": None,
            "yieldRequestedAt": None,
            "yieldRequestedBy": None,
            "yieldNote": None,
            "yieldDurationHours": None,
            "continuationCheckpoint": None,
            "continuationCheckpointSha256": None,
            "continuationCheckpointMetadata": None,
            "continuationCheckpointMetadataSha256": None,
            "continuationStep": None,
            "continuationWandbId": None,
            "historicalGitRef": None,
            "historicalWorktreePath": None,
            "historicalWorktreeCreatedAt": None,
            "historicalWorktreeRemovedAt": None,
            "historicalWorktreeCleanupError": None,
            "runtimeGitRef": None,
            "runtimeWorktreePath": None,
            "runtimeWorktreeCreatedAt": None,
            "runtimeWorktreeRemovedAt": None,
            "runtimeWorktreeCleanupError": None,
        },
    }


def _artifact() -> dict[str, object]:
    return {
        "id": 4,
        "queueItemId": 7,
        "projectId": 1,
        "revisionId": 11,
        "segment": 1,
        "evidenceKind": "legacy-v4",
        "name": "checkpoint",
        "type": "file",
        "root": None,
        "relativePath": None,
        "absolutePath": "/scratch/checkpoint.bin",
        "sizeBytes": 10,
        "sha256": "6" * 64,
        "recordedAt": NOW,
        "metadata": None,
    }


def _export() -> QueueExport:
    return QueueExport.create(
        package_version="0.1.0-test",
        database=database_instance_document(
            state_directory="/state",
            database_path="/state/queue.sqlite3",
            instance_identity="12345678-1234-4234-8234-123456789abc",
        ),
        exported_at=NOW,
        actor="operator:test",
        host_state={"dispatchPaused": False, "reason": "", "provenance": None},
        project=_project(),
        revisions=[_revision()],
        items=[_item()],
        events=[{
            "id": 91, "createdAt": NOW, "actor": "scheduler",
            "eventType": "cooperative_yield_requested", "scope": "project",
            "projectId": 1, "queueItemId": 7,
            "payload": {"requestId": "request-1"},
        }],
        artifacts=[_artifact()],
        yield_requests=[],
        yield_receipts=[],
    )


def _json_evidence(document: object) -> dict[str, object]:
    source = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
    return json_evidence_document(
        source=source,
        source_sha256=sha256_bytes(source),
        document=document,  # type: ignore[arg-type]
    )


def _typed_export_document(*, receipt_segment: int = 1) -> dict[str, object]:
    document = _export().to_document()
    commit = "2" * 40
    project_source = b"project source"
    card_source = b"card source"
    command = {"type": "argv", "argv": ["python"]}
    command_source = json.dumps(command, separators=(",", ":"), sort_keys=True).encode()
    policy = {
        "projectKey": "project-one", "cardPath": "cards/EXP-001.yaml",
        "jobId": "train", "bindings": {}, "priority": 0,
        "holdReason": None, "dependencies": [], "operator": "operator:test",
        "preemptionAuthorized": True,
    }
    enrollment = {
        "kind": "Enrollment", "projectKey": "project-one",
        "checkoutDirectory": "/project-one", "projectNormalizedSha256": sha256_bytes(b"{}"),
    }
    enrollment_evidence = _json_evidence(enrollment)
    revision = document["revisions"][0]  # type: ignore[index]
    revision.update({  # type: ignore[union-attr]
        "label": "project-one:r1", "kind": "project-v1", "gitCommit": commit,
        "enrollmentSha256": enrollment_evidence["sourceSha256"],
        "enrollmentEvidence": enrollment_evidence,
        "typedEvidence": {
            "identity": {
                "id": 11, "projectId": 1, "projectKey": "project-one", "sequence": 1,
                "label": "project-one:r1", "displayName": "Project One", "gitCommit": commit,
                "projectSourcePath": "Project.yaml", "projectSourceSha256": sha256_bytes(project_source),
                "projectNormalizedSha256": sha256_bytes(b"{}"),
                "projectSchema": {"apiVersion": PROJECT_V1_SCHEMA.protocol.api_version, "kind": PROJECT_V1_SCHEMA.protocol.kind.value, "id": PROJECT_V1_SCHEMA.schema_id, "sha256": PROJECT_V1_SCHEMA.sha256},
                "enrollmentSha256": enrollment_evidence["sourceSha256"],
                "validatedPackageVersion": "test", "createdActor": "operator:test", "createdAt": NOW,
            },
            "projectSource": binary_evidence_document(source=project_source, source_sha256=sha256_bytes(project_source)),
            "projectNormalized": _json_evidence({}), "extensionSource": None,
            "extensionCanonical": None,
        },
        "gitEvidence": {
            "repositoryRoot": "/project-one", "gitCommit": commit,
            "projectBlob": {"path": "Project.yaml", "objectId": _git_blob_oid(project_source), "mode": "100644", "size": len(project_source), "sourceSha256": sha256_bytes(project_source)},
            "extensionSchemaBlob": None,
        },
    })
    document["project"]["currentRevision"].update({"label": "project-one:r1", "kind": "project-v1", "gitCommit": commit})  # type: ignore[index]
    item = document["items"][0]  # type: ignore[index]
    item.update({  # type: ignore[union-attr]
        "revisionLabel": "project-one:r1", "admissionKind": "ExperimentCard/v1",
        "snapshotId": 1, "jobId": "train", "segment": 2,
        "preemptible": True, "cardSha256": sha256_bytes(card_source), "gitCommit": commit,
        "commandText": command_source.decode(), "runnerName": "run-experiment",
        "admissionSnapshot": {
            "id": 1, "projectRevision": "project-one:r1", "gitCommit": commit,
            "packageVersion": "test", "projectSourceName": "Project.yaml",
            "projectSource": binary_evidence_document(source=project_source, source_sha256=sha256_bytes(project_source)),
            "projectNormalized": _json_evidence({}),
            "projectSchema": {"apiVersion": PROJECT_V1_SCHEMA.protocol.api_version, "kind": PROJECT_V1_SCHEMA.protocol.kind.value, "schemaId": PROJECT_V1_SCHEMA.schema_id, "sha256": PROJECT_V1_SCHEMA.sha256},
            "cardSourceName": "cards/EXP-001.yaml",
            "cardSource": binary_evidence_document(source=card_source, source_sha256=sha256_bytes(card_source)),
            "cardNormalized": _json_evidence({}),
            "cardSchema": {"apiVersion": EXPERIMENT_CARD_V1_SCHEMA.protocol.api_version, "kind": EXPERIMENT_CARD_V1_SCHEMA.protocol.kind.value, "schemaId": EXPERIMENT_CARD_V1_SCHEMA.schema_id, "sha256": EXPERIMENT_CARD_V1_SCHEMA.sha256},
            "extensionSchema": None, "resolved": _json_evidence({}),
            "command": _json_evidence(command), "submissionPolicy": _json_evidence(policy),
            "policyBindings": _json_evidence({}), "policyDependencies": _json_evidence([]),
        },
    })
    document["artifacts"][0].update({"evidenceKind": "declared-v1", "root": "scratch", "relativePath": "checkpoint.bin"})  # type: ignore[index]
    continuation = ContinuationIdentity.create(
        resolved_spec_sha256="1" * 64, project_revision="project-one:r1",
        git_commit=commit, run_id="run-1", prior_receipt_sha256="3" * 64,
    )
    request = CooperativeYieldRequest(
        request_id="request-1", queue_item_id=7, segment=1,
        request_kind=YieldRequestKind.MANUAL_PREEMPTION, requested_at=NOW,
        requested_by="operator:test", note="checkpoint", continuation=continuation,
    )
    receipt_request = CooperativeYieldRequest(
        request_id="request-1", queue_item_id=7, segment=receipt_segment,
        request_kind=YieldRequestKind.MANUAL_PREEMPTION, requested_at=NOW,
        requested_by="operator:test", note="checkpoint", continuation=continuation,
    )
    receipt = CooperativeYieldReceipt.failed(receipt_request, error="declined", written_at=NOW)
    request_source = _wire(request.to_document()); receipt_source = _wire(receipt.to_document())
    document["cooperativeYieldRequests"] = [wire_evidence_document(queue_item_id=7, project_id=1, revision_id=11, request_id="request-1", source=request_source, source_sha256=sha256_bytes(request_source), document=request.to_document())]
    document["cooperativeYieldReceipts"] = [wire_evidence_document(queue_item_id=7, project_id=1, revision_id=11, request_id="request-1", source=receipt_source, source_sha256=sha256_bytes(receipt_source), document=receipt.to_document())]
    item["runtime"].update({"yieldRequestId": "request-1", "yieldRequestedAt": NOW, "yieldRequestedBy": "operator:test", "yieldNote": "checkpoint"})  # type: ignore[index]
    return document


def _typed_export_with_extension_document() -> dict[str, object]:
    document = _typed_export_document()
    extension_document = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.invalid/project-one-extension-v1",
        "type": "object",
    }
    extension_source = json.dumps(
        extension_document, separators=(",", ":"), sort_keys=True
    ).encode()
    extension_source_evidence = binary_evidence_document(
        source=extension_source,
        source_sha256=sha256_bytes(extension_source),
    )
    extension_canonical_evidence = _json_evidence(extension_document)
    extension_identity = {
        "path": "schemas/project-extension.json",
        "sourceSha256": extension_source_evidence["sourceSha256"],
        "canonicalSha256": extension_canonical_evidence["sourceSha256"],
        "schemaId": extension_document["$id"],
    }
    revision = document["revisions"][0]  # type: ignore[index]
    typed = revision["typedEvidence"]  # type: ignore[index]
    typed["identity"]["extensionSchema"] = extension_identity  # type: ignore[index]
    typed["extensionSource"] = extension_source_evidence  # type: ignore[index]
    typed["extensionCanonical"] = extension_canonical_evidence  # type: ignore[index]
    revision["gitEvidence"]["extensionSchemaBlob"] = {  # type: ignore[index]
        "path": extension_identity["path"],
        "objectId": _git_blob_oid(extension_source),
        "mode": "100644",
        "size": extension_source_evidence["sourceBytes"],
        "sourceSha256": extension_source_evidence["sourceSha256"],
    }
    snapshot = document["items"][0]["admissionSnapshot"]  # type: ignore[index]
    snapshot["extensionSchema"] = {  # type: ignore[index]
        "sourceName": "schemas/project-extension.json",
        "referencePath": extension_identity["path"],
        "source": extension_source_evidence,
        "canonical": extension_canonical_evidence,
        "schemaId": extension_identity["schemaId"],
    }
    return document


def test_queue_export_round_trips_exact_canonical_protocol() -> None:
    export = _export()
    document = export.to_document()

    assert document["apiVersion"] == "experiment-queue/v1"
    assert document["kind"] == "QueueExport"
    assert document["database"]["kind"] == "Database"  # type: ignore[index]
    assert document["events"][0]["actor"] == "scheduler"  # type: ignore[index]
    assert document["events"][0]["scope"] == "project"  # type: ignore[index]
    assert document["executorReceipts"]["exactSourceAvailable"] is False  # type: ignore[index]
    assert QueueExport.from_bytes(export.canonical_json) == export


def test_queue_export_rejects_unknown_fields_and_noncanonical_source() -> None:
    export = _export()
    document = export.to_document()
    document["guessedReceipt"] = {}
    with pytest.raises(QueueExportError, match="unknown fields"):
        QueueExport.from_document(document)

    document = export.to_document()
    document["items"][0]["guessedOutcome"] = "succeeded"  # type: ignore[index]
    with pytest.raises(QueueExportError, match=r"items\[0\].*unknown fields"):
        QueueExport.from_document(document)

    pretty = json.dumps(export.to_document(), indent=2).encode("utf-8")
    with pytest.raises(QueueExportError, match="not exact RFC 8785 canonical"):
        QueueExport.from_bytes(pretty)


def test_queue_export_rejects_duplicate_keys_and_tampered_exact_enrollment_source() -> None:
    export = _export()
    duplicated = export.canonical_json.replace(
        b'"apiVersion":',
        b'"apiVersion":"experiment-queue/v1","apiVersion":',
        1,
    )
    with pytest.raises(QueueExportError, match="repeats JSON key"):
        QueueExport.from_bytes(duplicated)

    document = export.to_document()
    enrollment = document["revisions"][0]["enrollmentEvidence"]  # type: ignore[index]
    enrollment["sourceSha256"] = "0" * 64  # type: ignore[index]
    with pytest.raises(QueueExportError, match="length or SHA-256 differs"):
        QueueExport.from_document(document)


def test_queue_export_rejects_cross_project_scope_and_invented_executor_receipt() -> None:
    document = _export().to_document()
    document["events"][0]["scope"] = "host"  # type: ignore[index]
    with pytest.raises(QueueExportError, match="failure scope"):
        QueueExport.from_document(document)

    document = _export().to_document()
    document["executorReceipts"]["exactSourceAvailable"] = True  # type: ignore[index]
    with pytest.raises(QueueExportError, match="absence metadata"):
        QueueExport.from_document(document)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value["project"]["currentRevision"].__setitem__("id", 999), "unexported revision"),
        (lambda value: value["project"].__setitem__("queueCounts", {"failed": 2}), "queueCounts differs"),
        (lambda value: value["items"][0].__setitem__("state", "invented"), "state is invalid"),
        (lambda value: value["artifacts"][0].__setitem__("segment", 2), "newer than"),
        (lambda value: value["items"][0]["runtime"].__setitem__("continuationCheckpoint", "/tmp/checkpoint"), "checkpoint/hash pair"),
        (lambda value: value["items"][0]["runtime"].__setitem__("runtimeWorktreePath", "relative/path"), "absolute"),
        (lambda value: value["items"][0]["runtime"].__setitem__("runtimeGpuLeaseHeld", "yes"), "runtimeGpuLeaseHeld must be true or false"),
        (lambda value: value["items"][0]["runtime"].update({"runtimeGpuLeaseHeld": True}), "held runtime GPU lease lacks assigned identity"),
        (lambda value: value["items"][0]["runtime"].update({"runtimeGpuLeaseHeld": True, "assignedGpuUuid": "GPU-1", "assignedGpuIndex": "0", "runtimeGpuLeaseReleasedAt": NOW}), "held runtime GPU lease claims a release time"),
        (lambda value: value["items"][0]["runtime"].update({"runtimeGpuLeaseReleasedAt": NOW}), "release lacks complete historical assignment"),
        (lambda value: (value["items"][0].update({"state": "removed"}), value["items"][0]["runtime"].update({"runtimeGpuLeaseHeld": True, "assignedGpuUuid": "GPU-1", "assignedGpuIndex": "0"})), "state 'removed' cannot retain"),
    ],
)
def test_queue_export_rejects_cross_record_and_runtime_inconsistency(
    mutate: object, message: str
) -> None:
    document = _export().to_document()
    mutate(document)  # type: ignore[operator]
    with pytest.raises(QueueExportError, match=message):
        QueueExport.from_document(document)


def test_queue_export_validates_yield_receipt_against_exact_request() -> None:
    assert QueueExport.from_document(_typed_export_document()).to_document()["kind"] == "QueueExport"

    with pytest.raises(QueueExportError, match="receipt/request binding"):
        QueueExport.from_document(_typed_export_document(receipt_segment=2))


@pytest.mark.parametrize("evidence_key", ["cooperativeYieldRequests", "cooperativeYieldReceipts"])
def test_queue_export_rejects_non_deterministic_cooperative_yield_wire(
    evidence_key: str,
) -> None:
    """Rehashed whitespace variants cannot masquerade as persisted v1 wire."""

    document = _typed_export_document()
    wrapper = document[evidence_key][0]  # type: ignore[index]
    noncanonical = json.dumps(wrapper["document"], indent=2, sort_keys=False).encode()
    _replace_wire_source(wrapper, noncanonical)
    with pytest.raises(QueueExportError, match="deterministic CooperativeYield/v1"):
        QueueExport.from_document(document)


def test_queue_export_binds_typed_snapshot_revision_schema_and_unique_id() -> None:
    document = _typed_export_document()
    document["items"][0]["admissionSnapshot"]["projectSource"] = binary_evidence_document(source=b"other", source_sha256=sha256_bytes(b"other"))  # type: ignore[index]
    with pytest.raises(QueueExportError, match="differs from owning revision"):
        QueueExport.from_document(document)

    document = _typed_export_document()
    document["items"][0]["admissionSnapshot"]["cardSchema"]["sha256"] = "0" * 64  # type: ignore[index]
    with pytest.raises(QueueExportError, match="installed v1 schema"):
        QueueExport.from_document(document)

    document = _typed_export_document()
    duplicate = deepcopy(document["items"][0])  # type: ignore[index]
    duplicate["id"] = 8
    duplicate["experimentId"] = "EXP-002"
    duplicate["runtime"].update({"yieldRequestId": None, "yieldRequestedAt": None, "yieldRequestedBy": None, "yieldNote": None})
    document["items"].append(duplicate)  # type: ignore[union-attr]
    document["project"]["queueCounts"] = {"failed": 2}  # type: ignore[index]
    with pytest.raises(QueueExportError, match="reuse one AdmissionSnapshot"):
        QueueExport.from_document(document)


def test_queue_export_rejects_artifact_path_and_admission_revision_mismatch() -> None:
    document = _typed_export_document()
    document["artifacts"][0]["relativePath"] = "../escape"  # type: ignore[index]
    with pytest.raises(QueueExportError, match="relativePath is invalid"):
        QueueExport.from_document(document)

    document = _typed_export_document()
    item = document["items"][0]  # type: ignore[index]
    item.update({"admissionKind": "LegacyMarkdownCard/v0", "snapshotId": None, "jobId": None, "admissionSnapshot": None})  # type: ignore[union-attr]
    with pytest.raises(QueueExportError, match="legacy.*legacy-v4 revision"):
        QueueExport.from_document(document)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["revisions"][0]["gitEvidence"].__setitem__(
                "repositoryRoot", "/different-checkout"
            ),
            "repository root differs",
        ),
        (
            lambda value: value["revisions"][0]["gitEvidence"].__setitem__(
                "extensionSchemaBlob", None
            ),
            "extension Git/source evidence is incomplete",
        ),
        (
            lambda value: value["revisions"][0]["gitEvidence"][
                "extensionSchemaBlob"
            ].__setitem__("path", "schemas/other.json"),
            "extension Git blob differs",
        ),
        (
            lambda value: value["revisions"][0]["typedEvidence"]["identity"][
                "extensionSchema"
            ].__setitem__("unexpected", True),
            "unknown fields",
        ),
    ],
)
def test_queue_export_binds_complete_extension_git_evidence(
    mutate: object, message: str
) -> None:
    document = _typed_export_with_extension_document()
    assert QueueExport.from_document(document).to_document()["kind"] == "QueueExport"
    mutate(document)  # type: ignore[operator]
    with pytest.raises(QueueExportError, match=message):
        QueueExport.from_document(document)


@pytest.mark.parametrize(
    ("blob_name", "message"),
    [
        ("projectBlob", "Project Git blob objectId differs"),
        ("extensionSchemaBlob", "extension Git blob objectId differs"),
    ],
)
def test_queue_export_recomputes_git_blob_object_ids(
    blob_name: str,
    message: str,
) -> None:
    """Well-shaped but false blob IDs are rejected against exact source bytes."""

    document = _typed_export_with_extension_document()
    document["revisions"][0]["gitEvidence"][blob_name]["objectId"] = "a" * 40  # type: ignore[index]
    with pytest.raises(QueueExportError, match=message):
        QueueExport.from_document(document)
