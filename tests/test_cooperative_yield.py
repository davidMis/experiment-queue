"""Conformance tests for generic cooperative-yield requests and continuations."""

from __future__ import annotations

import base64
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

import pytest

import experiment_queue.cooperative_yield as cooperative_yield
from experiment_queue.cooperative_yield import (
    CONTINUATION_RECEIPT_ENV,
    CheckpointArtifact,
    ContinuationIdentity,
    CooperativeYieldHelper,
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    OpaqueResumeContext,
    YIELD_RECEIPT_ENV,
    YIELD_REQUEST_ENV,
    YieldDocumentError,
    YieldIntegrityError,
    YieldProgress,
    YieldReceiptStatus,
    YieldRequestKind,
    read_yield_receipt,
    read_continuation_receipt_from_environment,
    read_yield_request,
    sha256_bytes,
    sha256_file,
    utc_now_iso,
    validate_continuation_identity,
    validate_ready_continuation,
    validate_receipt_for_request,
    verify_checkpoint_artifacts,
    write_yield_receipt,
    write_yield_request,
)


WRITTEN_AT = "2026-08-27T12:34:56+00:00"
RESOLVED_SPEC_SHA256 = "1" * 64
PROJECT_REVISION = "flowers-3d-helmholtz:revision-7"
GIT_COMMIT = "2" * 40
RUN_ID = "run-0198"
PRIOR_RECEIPT_SHA256 = "3" * 64
FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "cooperative_yield_project.py"


def continuation_identity(**changes: str) -> ContinuationIdentity:
    """Return a complete synthetic continuation binding for one fixture run."""

    values = {
        "resolved_spec_sha256": RESOLVED_SPEC_SHA256,
        "project_revision": PROJECT_REVISION,
        "git_commit": GIT_COMMIT,
        "run_id": RUN_ID,
        "prior_receipt_sha256": PRIOR_RECEIPT_SHA256,
    }
    values.update(changes)
    return ContinuationIdentity.create(**values)


def yield_request(**changes: object) -> CooperativeYieldRequest:
    """Return a strict synthetic manual-preemption request."""

    values: dict[str, object] = {
        "request_id": "request-001",
        "queue_item_id": 41,
        "segment": 2,
        "request_kind": YieldRequestKind.MANUAL_PREEMPTION,
        "requested_at": WRITTEN_AT,
        "requested_by": "test:operator",
        "note": "release the GPU for a higher-priority run",
        "continuation": continuation_identity(),
    }
    values.update(changes)
    return CooperativeYieldRequest(**values)  # type: ignore[arg-type]


def ready_receipt(
    temporary_path: Path,
) -> tuple[CooperativeYieldRequest, CooperativeYieldReceipt, Path]:
    """Create one valid ready receipt and its checkpoint beneath a run root."""

    request = yield_request()
    checkpoint = temporary_path / "run" / "checkpoint.bin"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"checkpoint version one\n")
    receipt = CooperativeYieldReceipt.ready(
        request,
        progress=YieldProgress(unit="settled_rows", completed=7, total=12),
        checkpoint_artifacts=(
            CheckpointArtifact.from_file("solver_state", checkpoint),
        ),
        resume_context=OpaqueResumeContext.from_json(
            {"checkpoint_name": "solver_state", "next_row": 7}
        ),
        written_at=WRITTEN_AT,
    )
    return request, receipt, checkpoint


def test_typed_request_round_trips_with_independent_protocol_identity(
    tmp_path: Path,
) -> None:
    request_path = tmp_path / "control" / "request.json"
    request = yield_request()

    write_yield_request(request_path, request)

    wire = json.loads(request_path.read_text(encoding="utf-8"))
    assert wire["apiVersion"] == "experiment-queue/v1"
    assert wire["kind"] == "CooperativeYieldRequest"
    assert wire["continuation"]["identity_sha256"] == (
        request.continuation.identity_sha256
    )
    assert read_yield_request(request_path) == request
    assert list(request_path.parent.glob(f".{request_path.name}.*.tmp")) == []


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda document: document.update({"unexpected": True}), "unknown fields"),
        (
            lambda document: document.update({"kind": "CooperativeYieldReceipt"}),
            "unsupported CooperativeYieldReceipt/v1",
        ),
        (
            lambda document: document.update({"request_kind": "automatic_priority"}),
            "request_kind is unsupported",
        ),
        (
            lambda document: document.update({"queue_item_id": True}),
            "queue_item_id must be an integer",
        ),
    ],
)
def test_request_parser_fails_closed(
    mutation: object,
    message: str,
) -> None:
    document = yield_request().to_document()
    mutation(document)  # type: ignore[operator]

    with pytest.raises(YieldDocumentError, match=message):
        CooperativeYieldRequest.from_document(document)


def test_json_reader_rejects_duplicate_keys_and_non_finite_constants(
    tmp_path: Path,
) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(
        '{"apiVersion":"experiment-queue/v1",'
        '"kind":"CooperativeYieldRequest",'
        '"kind":"CooperativeYieldRequest"}\n',
        encoding="utf-8",
    )
    with pytest.raises(YieldDocumentError, match="repeats object key 'kind'"):
        read_yield_request(request_path)

    request_path.write_text('{"value":NaN}\n', encoding="utf-8")
    with pytest.raises(YieldDocumentError, match="unsupported JSON constant"):
        read_yield_request(request_path)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            b'{"queue_item_id":' + (b"9" * 5_000) + b"}",
            "invalid JSON integer",
        ),
        (
            (b"[" * 5_000) + b"null" + (b"]" * 5_000),
            "JSON nesting depth",
        ),
    ],
)
def test_protocol_json_reader_contains_extreme_integer_and_nesting_failures(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_bytes(payload)

    with pytest.raises(YieldDocumentError, match=message):
        read_yield_request(request_path)


def test_project_helper_hashes_checkpoints_and_writes_atomic_ready_receipt(
    tmp_path: Path,
) -> None:
    request_path = tmp_path / "control" / "request.json"
    receipt_path = tmp_path / "control" / "receipt.json"
    request = yield_request()
    write_yield_request(request_path, request)
    helper = CooperativeYieldHelper.from_environment(
        {
            YIELD_REQUEST_ENV: str(request_path),
            YIELD_RECEIPT_ENV: str(receipt_path),
        }
    )
    assert helper is not None
    assert helper.request_if_present() == request
    checkpoint = tmp_path / "run" / "checkpoint.bin"
    metadata = tmp_path / "run" / "checkpoint.metadata.json"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"binary checkpoint\x00\x01")
    metadata.write_text('{"format":"fixture/v1"}\n', encoding="utf-8")
    context = OpaqueResumeContext.from_json(
        {
            "primary": "solver_state",
            "project_extension": {"tracker": "opaque-run-id"},
        }
    )

    receipt = helper.write_ready(
        request,
        checkpoint_files={
            "solver_state": checkpoint,
            "checkpoint_metadata": metadata,
        },
        media_types={"checkpoint_metadata": "application/json"},
        progress=YieldProgress(unit="optimizer_steps", completed=19, total=100),
        resume_context=context,
        written_at=WRITTEN_AT,
    )

    parsed = read_yield_receipt(receipt_path)
    assert parsed == receipt
    assert parsed.status is YieldReceiptStatus.READY
    assert {artifact.name for artifact in parsed.checkpoint_artifacts} == {
        "solver_state",
        "checkpoint_metadata",
    }
    assert parsed.resume_context is not None
    assert parsed.resume_context.json_value() == {
        "primary": "solver_state",
        "project_extension": {"tracker": "opaque-run-id"},
    }
    validate_ready_continuation(
        parsed,
        request,
        resolved_spec_sha256=RESOLVED_SPEC_SHA256,
        project_revision=PROJECT_REVISION,
        git_commit=GIT_COMMIT,
        run_id=RUN_ID,
        prior_receipt_sha256=PRIOR_RECEIPT_SHA256,
        allowed_artifact_roots=(tmp_path / "run",),
        expected_checkpoint_names=("solver_state", "checkpoint_metadata"),
    )
    assert list(receipt_path.parent.glob(f".{receipt_path.name}.*.tmp")) == []


def test_project_helper_writes_small_failed_receipt(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    receipt_path = tmp_path / "receipt.json"
    request = yield_request(request_kind=YieldRequestKind.GPU_RESERVATION)
    write_yield_request(request_path, request)
    helper = CooperativeYieldHelper(request_path, receipt_path)

    receipt = helper.write_failed(
        request,
        error="checkpoint backend is temporarily unavailable",
        progress=YieldProgress(unit="samples", completed=32),
        written_at=WRITTEN_AT,
    )

    parsed = read_yield_receipt(receipt_path)
    assert parsed == receipt
    assert parsed.status is YieldReceiptStatus.FAILED
    assert parsed.error == "checkpoint backend is temporarily unavailable"
    wire = parsed.to_document()
    assert wire["kind"] == "CooperativeYieldReceipt"
    assert "continuation" not in wire
    assert "checkpoint_artifacts" not in wire
    assert "resume_context" not in wire
    validate_receipt_for_request(parsed, request)


def test_resumed_segment_reads_prior_ready_receipt_from_queue_environment(
    tmp_path: Path,
) -> None:
    """Project code gets exact prior continuation evidence, never an inferred path."""

    _request, receipt, _checkpoint = ready_receipt(tmp_path)
    receipt_path = tmp_path / "prior-receipt.json"
    write_yield_receipt(receipt_path, receipt)
    assert read_continuation_receipt_from_environment(
        {CONTINUATION_RECEIPT_ENV: str(receipt_path)}
    ) == receipt
    assert read_continuation_receipt_from_environment({}) is None


@pytest.mark.parametrize("failed", [False, True])
def test_fixture_project_needs_no_copied_environment_or_hashing_logic(
    tmp_path: Path,
    failed: bool,
) -> None:
    request_path = tmp_path / "control" / "request.json"
    receipt_path = tmp_path / "control" / "receipt.json"
    checkpoint_root = tmp_path / "project-checkpoints"
    request = yield_request()
    write_yield_request(request_path, request)
    environment = dict(os.environ)
    environment.update(
        {
            YIELD_REQUEST_ENV: str(request_path),
            YIELD_RECEIPT_ENV: str(receipt_path),
        }
    )
    command = [
        sys.executable,
        str(FIXTURE_PROJECT),
        "--checkpoint-dir",
        str(checkpoint_root),
    ]
    if failed:
        command.append("--fail")

    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    receipt = read_yield_receipt(receipt_path)
    validate_receipt_for_request(receipt, request)
    if failed:
        assert receipt.status is YieldReceiptStatus.FAILED
        assert receipt.checkpoint_artifacts == ()
    else:
        assert receipt.status is YieldReceiptStatus.READY
        validate_ready_continuation(
            receipt,
            request,
            resolved_spec_sha256=RESOLVED_SPEC_SHA256,
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            run_id=RUN_ID,
            prior_receipt_sha256=PRIOR_RECEIPT_SHA256,
            allowed_artifact_roots=(checkpoint_root,),
            expected_checkpoint_names=("fixture_state",),
        )


def test_atomic_receipt_failure_preserves_the_previous_complete_document(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "receipt.json"
    request = yield_request()
    previous = CooperativeYieldReceipt.failed(
        request,
        error="first failure",
        written_at=WRITTEN_AT,
    )
    replacement = CooperativeYieldReceipt.failed(
        request,
        error="second failure",
        written_at=WRITTEN_AT,
    )
    write_yield_receipt(receipt_path, previous)
    original_bytes = receipt_path.read_bytes()

    with mock.patch(
        "experiment_queue.cooperative_yield.os.replace",
        side_effect=OSError("simulated rename failure"),
    ):
        with pytest.raises(OSError, match="simulated rename failure"):
            write_yield_receipt(receipt_path, replacement)

    assert receipt_path.read_bytes() == original_bytes
    assert read_yield_receipt(receipt_path) == previous
    assert list(tmp_path.glob(f".{receipt_path.name}.*.tmp")) == []


def test_checkpoint_hash_or_path_escape_invalidates_continuation(
    tmp_path: Path,
) -> None:
    request, receipt, checkpoint = ready_receipt(tmp_path)

    checkpoint.write_bytes(b"mutated checkpoint\n")
    with pytest.raises(YieldIntegrityError, match="size differs|SHA-256 differs"):
        validate_ready_continuation(
            receipt,
            request,
            resolved_spec_sha256=RESOLVED_SPEC_SHA256,
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            run_id=RUN_ID,
            prior_receipt_sha256=PRIOR_RECEIPT_SHA256,
            allowed_artifact_roots=(tmp_path / "run",),
            expected_checkpoint_names=("solver_state",),
        )


def test_ready_continuation_requires_the_exact_admitted_checkpoint_names(
    tmp_path: Path,
) -> None:
    request, receipt, _checkpoint = ready_receipt(tmp_path)
    common = {
        "resolved_spec_sha256": RESOLVED_SPEC_SHA256,
        "project_revision": PROJECT_REVISION,
        "git_commit": GIT_COMMIT,
        "run_id": RUN_ID,
        "prior_receipt_sha256": PRIOR_RECEIPT_SHA256,
        "allowed_artifact_roots": (tmp_path / "run",),
    }

    with pytest.raises(YieldIntegrityError, match=r"missing \['metadata'\]"):
        validate_ready_continuation(
            receipt,
            request,
            expected_checkpoint_names=("solver_state", "metadata"),
            **common,  # type: ignore[arg-type]
        )

    extra_checkpoint = tmp_path / "run" / "extra.bin"
    extra_checkpoint.write_bytes(b"extra checkpoint")
    receipt_with_extra = replace(
        receipt,
        checkpoint_artifacts=(
            *receipt.checkpoint_artifacts,
            CheckpointArtifact.from_file("extra", extra_checkpoint),
        ),
    )
    with pytest.raises(YieldIntegrityError, match=r"unexpected \['extra'\]"):
        validate_ready_continuation(
            receipt_with_extra,
            request,
            expected_checkpoint_names=("solver_state",),
            **common,  # type: ignore[arg-type]
        )

    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    outside_artifact = CheckpointArtifact.from_file("outside", outside)
    with pytest.raises(YieldIntegrityError, match="outside allowed roots"):
        verify_checkpoint_artifacts(
            (outside_artifact,),
            allowed_roots=(tmp_path / "run",),
        )


def test_checkpoint_symlink_is_rejected_at_creation_and_verification(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "run" / "checkpoint.bin"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"original")
    artifact = CheckpointArtifact.from_file("state", checkpoint)
    checkpoint.unlink()
    replacement = tmp_path / "run" / "replacement.bin"
    replacement.write_bytes(b"original")
    checkpoint.symlink_to(replacement)

    with pytest.raises(YieldIntegrityError, match="must not be a symlink"):
        verify_checkpoint_artifacts(
            (artifact,),
            allowed_roots=(tmp_path / "run",),
        )
    with pytest.raises(YieldIntegrityError, match="must not be a symlink"):
        CheckpointArtifact.from_file("state", checkpoint)


def test_checkpoint_path_swap_between_inspection_and_open_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint = tmp_path / "checkpoint.bin"
    checkpoint.write_bytes(b"approved checkpoint")
    replacement = tmp_path / "replacement.bin"
    replacement.write_bytes(b"different checkpoint")
    displaced = tmp_path / "displaced.bin"
    real_open = os.open

    def swap_then_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        checkpoint.replace(displaced)
        replacement.replace(checkpoint)
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(cooperative_yield.os, "open", swap_then_open)
    with pytest.raises(YieldIntegrityError, match="changed before it was opened"):
        CheckpointArtifact.from_file("state", checkpoint)


def test_stale_request_and_regressed_progress_fail_closed(tmp_path: Path) -> None:
    request, receipt, _checkpoint = ready_receipt(tmp_path)
    stale = replace(receipt, request_id="request-older")
    with pytest.raises(YieldIntegrityError, match="request_id"):
        validate_receipt_for_request(stale, request)

    with pytest.raises(YieldIntegrityError, match="regressed"):
        validate_receipt_for_request(
            receipt,
            request,
            previous_progress=YieldProgress(
                unit="settled_rows", completed=8, total=12
            ),
        )
    with pytest.raises(YieldIntegrityError, match="progress unit changed"):
        validate_receipt_for_request(
            receipt,
            request,
            previous_progress=YieldProgress(
                unit="optimizer_steps", completed=1, total=12
            ),
        )


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("resolved_spec_sha256", "a" * 64),
        ("project_revision", "flowers-3d-helmholtz:revision-8"),
        ("git_commit", "b" * 40),
        ("run_id", "run-0200"),
        ("prior_receipt_sha256", "c" * 64),
    ],
)
def test_every_continuation_evidence_field_is_bound(
    field: str,
    changed_value: str,
) -> None:
    identity = continuation_identity()
    expected = {
        "resolved_spec_sha256": RESOLVED_SPEC_SHA256,
        "project_revision": PROJECT_REVISION,
        "git_commit": GIT_COMMIT,
        "run_id": RUN_ID,
        "prior_receipt_sha256": PRIOR_RECEIPT_SHA256,
    }
    expected[field] = changed_value

    with pytest.raises(YieldIntegrityError, match=field):
        validate_continuation_identity(identity, **expected)


def test_changed_prior_receipt_bytes_invalidate_the_binding(tmp_path: Path) -> None:
    prior_receipt = tmp_path / "prior-receipt.json"
    prior_receipt.write_bytes(b'{"status":"running"}\n')
    original_sha256 = sha256_file(prior_receipt)
    identity = continuation_identity(prior_receipt_sha256=original_sha256)
    prior_receipt.write_bytes(b'{"status":"yielded"}\n')

    with pytest.raises(YieldIntegrityError, match="prior_receipt_sha256"):
        validate_continuation_identity(
            identity,
            resolved_spec_sha256=RESOLVED_SPEC_SHA256,
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            run_id=RUN_ID,
            prior_receipt_sha256=sha256_file(prior_receipt),
        )


def test_corrupt_resume_payload_or_digest_is_rejected(tmp_path: Path) -> None:
    _request, receipt, _checkpoint = ready_receipt(tmp_path)
    document = receipt.to_document()
    resume_context = document["resume_context"]
    assert isinstance(resume_context, dict)
    replacement = b'{"next_row":999}'
    resume_context["data"] = base64.b64encode(replacement).decode("ascii")
    resume_context["bytes"] = len(replacement)

    with pytest.raises(YieldIntegrityError, match="SHA-256"):
        CooperativeYieldReceipt.from_document(document)

    document = receipt.to_document()
    resume_context = document["resume_context"]
    assert isinstance(resume_context, dict)
    resume_context["sha256"] = "f" * 64
    with pytest.raises(YieldIntegrityError, match="SHA-256"):
        CooperativeYieldReceipt.from_document(document)


def test_tampered_continuation_digest_is_rejected(tmp_path: Path) -> None:
    _request, receipt, _checkpoint = ready_receipt(tmp_path)
    document = receipt.to_document()
    continuation = document["continuation"]
    assert isinstance(continuation, dict)
    continuation["identity_sha256"] = "0" * 64

    with pytest.raises(YieldIntegrityError, match="identity SHA-256"):
        CooperativeYieldReceipt.from_document(document)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda document: document["checkpoint_artifacts"].clear(),
            "at least one checkpoint artifact",
        ),
        (
            lambda document: document.update({"error": "ambiguous"}),
            "unknown fields",
        ),
        (
            lambda document: document["progress"].update({"completed": False}),
            "progress.completed must be an integer",
        ),
    ],
)
def test_ready_receipt_status_shape_is_strict(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    _request, receipt, _checkpoint = ready_receipt(tmp_path)
    document = receipt.to_document()
    mutation(document)  # type: ignore[operator]

    with pytest.raises(YieldDocumentError, match=message):
        CooperativeYieldReceipt.from_document(document)


def test_helper_environment_is_optional_but_never_half_configured(
    tmp_path: Path,
) -> None:
    assert CooperativeYieldHelper.from_environment({}) is None
    with pytest.raises(YieldDocumentError, match="must be set together"):
        CooperativeYieldHelper.from_environment(
            {YIELD_REQUEST_ENV: str(tmp_path / "request.json")}
        )
    with pytest.raises(YieldDocumentError, match="absolute Path"):
        CooperativeYieldHelper.from_environment(
            {
                YIELD_REQUEST_ENV: "relative-request.json",
                YIELD_RECEIPT_ENV: "relative-receipt.json",
            }
        )


def test_protocol_paths_reject_nul_and_log_control_characters(tmp_path: Path) -> None:
    artifact_document = {
        "name": "state",
        "path": "/tmp/bad\x00path",
        "bytes": 1,
        "sha256": "0" * 64,
        "media_type": "application/octet-stream",
    }
    with pytest.raises(YieldDocumentError, match="control"):
        CheckpointArtifact.from_document(artifact_document)

    with pytest.raises(YieldDocumentError, match="control"):
        CooperativeYieldHelper(
            Path(f"{tmp_path}/request\x00.json"),
            tmp_path / "receipt.json",
        )


def test_helper_rejects_lexical_and_hardlink_aliases(tmp_path: Path) -> None:
    request_path = tmp_path / "control" / "request.json"
    lexical_alias = tmp_path / "control" / "nested" / ".." / "request.json"
    with pytest.raises(YieldDocumentError, match="different files"):
        CooperativeYieldHelper(request_path, lexical_alias)

    request_path.parent.mkdir(parents=True)
    request_path.write_text("request", encoding="utf-8")
    hardlink_alias = tmp_path / "control" / "request-hardlink.json"
    os.link(request_path, hardlink_alias)
    with pytest.raises(YieldDocumentError, match="different files"):
        CooperativeYieldHelper(request_path, hardlink_alias)


def test_helper_polling_surfaces_request_path_inspection_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = tmp_path / "request.json"
    helper = CooperativeYieldHelper(request_path, tmp_path / "receipt.json")
    real_lstat = Path.lstat

    def fail_request_lstat(path: Path) -> os.stat_result:
        if path == request_path:
            raise PermissionError("synthetic permission failure")
        return real_lstat(path)

    monkeypatch.setattr(Path, "lstat", fail_request_lstat)
    with pytest.raises(YieldDocumentError, match="cannot be inspected"):
        helper.request_if_present()


def test_resume_context_accepts_arbitrary_bytes_and_rejects_non_finite_json() -> None:
    payload = b"\x00project-owned\xff"
    context = OpaqueResumeContext.from_bytes(payload, media_type="application/x-fixture")
    assert context.payload == payload
    assert context.sha256 == sha256_bytes(payload)
    assert OpaqueResumeContext.from_document(context.to_document()) == context

    with pytest.raises(YieldDocumentError, match="finite JSON-native"):
        OpaqueResumeContext.from_json({"loss": float("nan")})


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-08-27T12:34:56Z",
        "2026-08-27T12:34:56+05:30",
        "2026-08-27T12:34:56.123456789-04:00",
    ],
)
def test_yield_timestamps_accept_exact_rfc3339_spelling(timestamp: str) -> None:
    assert yield_request(requested_at=timestamp).requested_at == timestamp
    assert yield_request(requested_at=utc_now_iso()).requested_at.endswith("+00:00")


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-08-27 12:34:56+00:00",
        "2026-08-27T12:34:56+0000",
        "2026-08-27T12:34:56",
        "2026-08-27T12:34:56z",
        "20260827T123456Z",
        "2026-08-27T24:00:00Z",
    ],
)
def test_yield_timestamps_reject_ambiguous_or_impossible_spellings(
    timestamp: str,
) -> None:
    with pytest.raises(YieldDocumentError, match="RFC 3339|real date and time"):
        yield_request(requested_at=timestamp)


def test_protocol_text_rejects_lone_surrogates_before_utf8_encoding() -> None:
    with pytest.raises(YieldDocumentError, match="Unicode scalar values"):
        yield_request(requested_by="operator:\ud800")

    with pytest.raises(YieldDocumentError, match="Unicode scalar values"):
        continuation_identity(project_revision="project:revision-\udfff")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"coordinates": (1, 2)}, "non-JSON-native type tuple"),
        ({1: "value"}, "object key 1 is not a string"),
        ({"loss": float("inf")}, "finite JSON-native"),
        ({"offset": 2**53}, "outside the interoperable JSON range"),
        ({"offset": -(2**53)}, "outside the interoperable JSON range"),
        ({"text": "\ud800"}, "Unicode scalar values"),
        ({"\udfff": "value"}, "Unicode scalar values"),
    ],
)
def test_resume_context_encoder_requires_exact_interoperable_json(
    payload: object,
    message: str,
) -> None:
    with pytest.raises(YieldDocumentError, match=message):
        OpaqueResumeContext.from_json(payload)  # type: ignore[arg-type]


def test_resume_context_json_safe_boundaries_round_trip() -> None:
    payload = {
        "supplementary": "\U0001f642",
        "values": [-((2**53) - 1), (2**53) - 1, -0.0, True, None],
    }

    assert OpaqueResumeContext.from_json(payload).json_value() == payload


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b'{"value":1,"value":2}', "repeats object key 'value'"),
        (b'{"nested":{"value":1,"value":2}}', "repeats object key 'value'"),
        (b'{"value":NaN}', "unsupported JSON constant 'NaN'"),
        (b'{"value":1e400}', "finite JSON-native"),
        (b'{"value":9007199254740992}', "outside the interoperable JSON range"),
        (b'{"value":"\\ud800"}', "Unicode scalar values"),
        (b'"\xff"', "not valid interoperable JSON"),
    ],
)
def test_resume_context_decoder_rejects_non_interoperable_json(
    payload: bytes,
    message: str,
) -> None:
    context = OpaqueResumeContext.from_bytes(
        payload,
        media_type="application/json",
    )

    with pytest.raises(YieldDocumentError, match=message):
        context.json_value()


def test_allowed_artifact_roots_must_be_absolute_existing_directories(
    tmp_path: Path,
) -> None:
    checkpoint_root = tmp_path / "checkpoints"
    checkpoint_root.mkdir()
    checkpoint = checkpoint_root / "checkpoint.bin"
    checkpoint.write_bytes(b"checkpoint")
    artifact = CheckpointArtifact.from_file("state", checkpoint)

    with pytest.raises(YieldIntegrityError, match="must be absolute"):
        verify_checkpoint_artifacts((artifact,), allowed_roots=(Path("relative"),))
    with pytest.raises(YieldIntegrityError, match="cannot be resolved.*existing directory"):
        verify_checkpoint_artifacts(
            (artifact,),
            allowed_roots=(tmp_path / "missing",),
        )
    root_file = tmp_path / "not-a-directory"
    root_file.write_bytes(b"not a directory")
    with pytest.raises(YieldIntegrityError, match="must resolve to a directory"):
        verify_checkpoint_artifacts((artifact,), allowed_roots=(root_file,))


def test_allowed_artifact_root_resolution_failure_is_an_integrity_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint_root = tmp_path / "checkpoints"
    checkpoint_root.mkdir()
    checkpoint = checkpoint_root / "checkpoint.bin"
    checkpoint.write_bytes(b"checkpoint")
    artifact = CheckpointArtifact.from_file("state", checkpoint)
    real_resolve = Path.resolve

    def fail_root_resolution(
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> Path:
        if path == checkpoint_root:
            raise RuntimeError("synthetic symlink loop")
        return real_resolve(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "resolve", fail_root_resolution)
    with pytest.raises(YieldIntegrityError, match="allowed artifact root.*cannot be resolved"):
        verify_checkpoint_artifacts(
            (artifact,),
            allowed_roots=(checkpoint_root,),
        )


def test_checkpoint_resolution_failure_is_an_integrity_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint_root = tmp_path / "checkpoints"
    checkpoint_root.mkdir()
    checkpoint = checkpoint_root / "checkpoint.bin"
    checkpoint.write_bytes(b"checkpoint")
    artifact = CheckpointArtifact.from_file("state", checkpoint)
    artifact_path = Path(artifact.path)
    real_resolve = Path.resolve

    def fail_checkpoint_resolution(
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> Path:
        if path == artifact_path:
            raise RuntimeError("synthetic symlink loop")
        return real_resolve(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "resolve", fail_checkpoint_resolution)
    with pytest.raises(YieldIntegrityError, match="checkpoint artifact.*cannot be resolved"):
        verify_checkpoint_artifacts(
            (artifact,),
            allowed_roots=(checkpoint_root,),
        )
