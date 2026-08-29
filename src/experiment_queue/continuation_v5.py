"""Coordinate safe manual CooperativeYield/v1 continuation on schema-v5."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import secrets
import signal
import stat
import time
from typing import Final, Mapping, cast

from experiment_queue.attempt_runtime import (
    PreparedAttempt,
    signal_recorded_process,
)
from experiment_queue.cooperative_yield import (
    ContinuationIdentity,
    CooperativeYieldError,
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    YieldReceiptStatus,
    YieldRequestKind,
    validate_ready_continuation,
)
from experiment_queue.serialization import sha256_bytes
from experiment_queue.scheduler_v5 import V5SchedulerError, V5SchedulingController
from experiment_queue.v5_repository import (
    V5ProjectRepository,
    V5QueueItem,
    V5RepositoryError,
    V5YieldReceiptRecord,
    V5YieldRequestRecord,
)


_MAX_CONTROL_BYTES: Final = 8 * 1024 * 1024
_SIGNAL_ATTEMPT_LEASE_SECONDS: Final = 5.0


class V5ContinuationError(RuntimeError):
    """Raised when one manual continuation cannot proceed without ambiguity."""


class _CoordinatorEvidence:
    """Prevent callers from manufacturing an authenticated pending operation."""

    __slots__ = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            f"{type(self).__name__} is created only by "
            "V5ContinuationCoordinator.request_manual_yield()"
        )


@dataclass(frozen=True, slots=True, init=False)
class V5PendingContinuation(_CoordinatorEvidence):
    """Persisted and published request awaiting one project-owned receipt."""

    project_id: int
    revision_id: int
    queue_item_id: int
    segment: int
    run_id: str
    prior_runner_receipt_sha256: str
    request_path: Path
    receipt_path: Path
    request: CooperativeYieldRequest
    request_source: bytes = field(repr=False)
    request_sha256: str


@dataclass(frozen=True, slots=True)
class V5ContinuationOutcome:
    """Final Project/item result after consuming a strict yield receipt."""

    item: V5QueueItem
    request_record: V5YieldRequestRecord
    receipt_record: V5YieldReceiptRecord
    requeued: bool


def _construct_pending(**values: object) -> V5PendingContinuation:
    pending = object.__new__(V5PendingContinuation)
    for name, value in values.items():
        object.__setattr__(pending, name, value)
    return cast(V5PendingContinuation, pending)


def _text(value: object, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5ContinuationError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise V5ContinuationError(
            f"{field_name} must be log-safe text of at most {maximum} characters"
        )
    return value


def _timestamp(value: object, *, field_name: str) -> str:
    timestamp = _text(value, field_name=field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(
            f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
        )
    except ValueError as exc:
        raise V5ContinuationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V5ContinuationError(
            f"{field_name} must be a timezone-aware ISO 8601 timestamp"
        )
    return timestamp


def _wire_bytes(document: Mapping[str, object]) -> bytes:
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
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise V5ContinuationError(
            f"cooperative-yield document is not finite UTF-8 JSON: {exc}"
        ) from exc


def _decode_document(source: bytes, *, label: str) -> dict[str, object]:
    def without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise V5ContinuationError(
                    f"{label} repeats JSON object key {key!r}"
                )
            document[key] = value
        return document

    try:
        value = json.loads(
            source.decode("utf-8", errors="strict"),
            object_pairs_hook=without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON token {token}")
            ),
        )
    except V5ContinuationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise V5ContinuationError(f"{label} is not strict UTF-8 JSON: {exc}") from exc
    if type(value) is not dict:
        raise V5ContinuationError(f"{label} must contain one JSON object")
    return cast(dict[str, object], value)


def _read_regular(path: Path, *, label: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        raise V5ContinuationError(f"{label} is missing: {path}") from exc
    except OSError as exc:
        raise V5ContinuationError(f"could not open {label} {path}: {exc}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise V5ContinuationError(f"{label} is not a regular file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            source = stream.read(_MAX_CONTROL_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(source) > _MAX_CONTROL_BYTES:
        raise V5ContinuationError(
            f"{label} exceeds {_MAX_CONTROL_BYTES} bytes: {path}"
        )
    if not source:
        raise V5ContinuationError(f"{label} is empty: {path}")
    return source


def _atomic_create_or_verify(path: Path, source: bytes, *, root: Path) -> None:
    """Publish once beneath the immutable segment root; exact retry is safe."""

    if not path.is_absolute() or path.parent != root:
        raise V5ContinuationError(
            f"yield request path {path} is not directly under segment root {root}"
        )
    try:
        resolved_root = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise V5ContinuationError(
            f"segment root {root} cannot be resolved before publication: {exc}"
        ) from exc
    if resolved_root != root or not root.is_dir():
        raise V5ContinuationError(
            f"segment root changed canonical identity before publication: {root}"
        )
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
    except FileExistsError:
        if _read_regular(path, label="existing yield request") != source:
            raise V5ContinuationError(
                f"refused to overwrite changed yield request evidence {path}"
            )
        return
    except OSError as exc:
        raise V5ContinuationError(
            f"could not publish yield request {path}: {exc}"
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source)
            stream.flush()
            os.fsync(stream.fileno())
        directory_descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _checkpoint_names(item: V5QueueItem) -> tuple[str, ...]:
    if item.snapshot is None:
        raise V5ContinuationError(
            f"queue item {item.id} lacks typed admission evidence"
        )
    resolved = item.snapshot.resolved_document
    job = resolved.get("job")
    if type(job) is not dict:
        raise V5ContinuationError("resolved execution has no typed job")
    resources = job.get("resources")
    if type(resources) is dict:
        gpus = resources.get("gpus", 0)
        if type(gpus) is not int or gpus not in {0, 1}:
            raise V5ContinuationError(
                "manual continuation supports one independently schedulable GPU "
                "job only; DDP/gang continuation is not implemented"
            )
    capabilities = job.get("capabilities")
    cooperative = (
        capabilities.get("cooperativeYield")
        if type(capabilities) is dict
        else None
    )
    values = (
        cooperative.get("checkpointArtifacts")
        if type(cooperative) is dict
        else None
    )
    if type(values) is not list or not values or not all(
        type(value) is str for value in values
    ):
        raise V5ContinuationError(
            "admitted job does not declare cooperative-yield checkpoint artifacts"
        )
    return tuple(cast(list[str], values))


class V5ContinuationCoordinator:
    """Two-phase manual-yield coordinator with Project-scoped failure isolation."""

    def __init__(self, repository: V5ProjectRepository):
        if type(repository) is not V5ProjectRepository:
            raise TypeError(
                f"repository must be exactly V5ProjectRepository, got "
                f"{type(repository).__name__}"
            )
        self.repository = repository

    def _runner_receipt(
        self,
        prepared: PreparedAttempt,
    ) -> tuple[str, bytes, str]:
        path = prepared.paths.segment_root / "runner.json"
        source = _read_regular(path, label="runner receipt")
        document = _decode_document(source, label="runner receipt")
        required = {
            "apiVersion",
            "kind",
            "run_id",
            "queue_item_id",
            "segment",
            "status",
            "return_code",
            "run_directory",
            "manifest",
            "logs",
            "sync",
            "written_at",
        }
        if set(document) != required:
            raise V5ContinuationError(
                "runner receipt has invalid fields; expected exact "
                f"{sorted(required)}, got {sorted(document)}"
            )
        if (
            document["apiVersion"] != "experiment-queue/v1"
            or document["kind"] != "RunnerReceipt"
            or document["status"] != "running"
            or document["return_code"] is not None
            or document["queue_item_id"] != prepared.queue_item_id
            or document["segment"] != prepared.segment
        ):
            raise V5ContinuationError(
                "runner receipt is not the running receipt for this exact queue "
                "item and segment"
            )
        run_id = _text(document["run_id"], field_name="runner receipt run_id", maximum=256)
        return run_id, source, sha256_bytes(source)

    def _process_identity(
        self,
        *,
        item_id: int,
        segment: int,
    ) -> tuple[int, int, str | None]:
        try:
            with self.repository.store.connect() as connection:
                row = connection.execute(
                    """
                    SELECT state, segment, pid, pgid, proc_start_ticks
                    FROM queue_items WHERE id = ?
                    """,
                    (item_id,),
                ).fetchone()
        except Exception as exc:
            raise V5ContinuationError(
                f"could not load process identity for queue item {item_id}: {exc}"
            ) from exc
        if row is None:
            raise V5ContinuationError(f"queue item {item_id} does not exist")
        if str(row["state"]) not in {"running", "yielding"} or int(row["segment"]) != segment:
            raise V5ContinuationError(
                f"queue item {item_id} is not active in segment {segment}"
            )
        if row["pid"] is None or row["pgid"] is None:
            raise V5ContinuationError(
                f"queue item {item_id} lacks persisted PID/process-group evidence"
            )
        return (
            int(row["pid"]),
            int(row["pgid"]),
            None if row["proc_start_ticks"] is None else str(row["proc_start_ticks"]),
        )

    def _raise_signal_uncertainty(
        self,
        *,
        item: V5QueueItem,
        request_id: str,
        actor: str,
        changed_at: str,
        detail: str,
        cause: BaseException,
    ) -> None:
        """Quarantine dispatch without destroying an ambiguous active process.

        Once the request is durable and published, a signaling exception or
        false return cannot prove that SIGINT was not delivered.  The process
        may already be writing a valid receipt or exiting, so the item must
        remain ``yielding`` with its PID, process group, and GPU assignment for
        normal receipt/process recovery.  Only this Project's dispatch circuit
        is opened while that evidence is reconciled.
        """

        reason = (
            f"manual-yield request {request_id!r} has uncertain SIGINT delivery: "
            f"{detail}; the active yielding state is preserved for recovery"
        )
        try:
            V5SchedulingController(self.repository.store).quarantine_project(
                item.project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
                queue_item_id=item.id,
            )
        except V5SchedulerError as quarantine_error:
            raise V5ContinuationError(
                f"{reason}; Project quarantine failed: {quarantine_error}"
            ) from cause
        raise V5ContinuationError(reason) from cause

    def _raise_publication_uncertainty(
        self,
        *,
        item: V5QueueItem,
        request_id: str,
        actor: str,
        changed_at: str,
        cause: BaseException,
    ) -> None:
        """Retain a live yielding identity so restart can safely republish."""

        reason = (
            f"manual-yield request {request_id!r} is durable but publication "
            f"failed: {cause}; the live yielding process identity, GPU assignment, "
            "and immutable request are retained for startup recovery"
        )
        try:
            V5SchedulingController(self.repository.store).quarantine_project(
                item.project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
                queue_item_id=item.id,
            )
        except V5SchedulerError as quarantine_error:
            raise V5ContinuationError(
                f"{reason}; Project quarantine failed: {quarantine_error}"
            ) from cause
        raise V5ContinuationError(reason) from cause

    def request_manual_yield(
        self,
        prepared: PreparedAttempt,
        *,
        note: str,
        actor: str,
        requested_at: str,
        request_id: str | None = None,
    ) -> V5PendingContinuation:
        """Persist, publish, then signal one explicitly preemptible running item."""

        if type(prepared) is not PreparedAttempt:
            raise TypeError(
                f"prepared must be exactly PreparedAttempt, got "
                f"{type(prepared).__name__}"
            )
        event_actor = _text(actor, field_name="actor", maximum=256)
        reason = _text(note, field_name="note", maximum=1000)
        timestamp = _timestamp(requested_at, field_name="requested_at")
        item = self.repository.get_queue_item(prepared.queue_item_id)
        if (
            item.state != "running"
            or item.admission_kind != "ExperimentCard/v1"
            or item.snapshot is None
            or not item.preemptible
        ):
            raise V5ContinuationError(
                f"queue item {item.id} must be a running, typed, explicitly "
                "preemptible admission before manual yield"
            )
        expected = (
            item.project_id,
            item.revision_id,
            item.experiment_id,
            item.attempt,
            item.segment,
            item.git_commit,
            item.snapshot.resolved_sha256,
        )
        actual = (
            prepared.project_id,
            prepared.project_revision_id,
            prepared.experiment_id,
            prepared.attempt,
            prepared.segment,
            prepared.git_commit,
            prepared.resolved_spec_sha256,
        )
        if actual != expected:
            raise V5ContinuationError(
                "PreparedAttempt identity differs from the current queue item and "
                "immutable admission snapshot"
            )
        _checkpoint_names(item)
        pid, pgid, process_start_ticks = self._process_identity(
            item_id=item.id,
            segment=item.segment,
        )
        run_id, _runner_source, prior_digest = self._runner_receipt(prepared)
        request_key = (
            f"manual:{item.id}:{item.segment}:{secrets.token_hex(12)}"
            if request_id is None
            else _text(request_id, field_name="request_id", maximum=256)
        )
        continuation = ContinuationIdentity.create(
            resolved_spec_sha256=item.snapshot.resolved_sha256,
            project_revision=item.snapshot.project_revision,
            git_commit=item.git_commit,
            run_id=run_id,
            prior_receipt_sha256=prior_digest,
        )
        request = CooperativeYieldRequest(
            request_id=request_key,
            queue_item_id=item.id,
            segment=item.segment,
            request_kind=YieldRequestKind.MANUAL_PREEMPTION,
            requested_at=timestamp,
            requested_by=event_actor,
            note=reason,
            continuation=continuation,
        )
        source = _wire_bytes(request.to_document())
        # Durable database evidence intentionally precedes both filesystem
        # publication and process signaling.
        request_record = self.repository.record_yield_request(
            request,
            source=source,
        )
        try:
            _atomic_create_or_verify(
                prepared.paths.yield_request,
                source,
                root=prepared.paths.segment_root,
            )
        except Exception as exc:
            self._raise_publication_uncertainty(
                item=item,
                request_id=request.request_id,
                actor=event_actor,
                changed_at=timestamp,
                cause=exc,
            )
        pending = _construct_pending(
            project_id=item.project_id,
            revision_id=item.revision_id,
            queue_item_id=item.id,
            segment=item.segment,
            run_id=run_id,
            prior_runner_receipt_sha256=prior_digest,
            request_path=prepared.paths.yield_request,
            receipt_path=prepared.paths.yield_receipt,
            request=request,
            request_source=request_record.source,
            request_sha256=request_record.sha256,
        )
        controller = V5SchedulingController(self.repository.store)
        signal_epoch = time.time()
        claim = controller.claim_manual_yield_signal_attempt(
            item.id,
            request_id=request.request_id,
            attempt_token=secrets.token_hex(16),
            signal_epoch=signal_epoch,
            retry_after_seconds=_SIGNAL_ATTEMPT_LEASE_SECONDS,
            actor=event_actor,
            changed_at=timestamp,
        )
        if claim is None:
            return pending
        try:
            signaled = signal_recorded_process(
                pid=pid,
                pgid=pgid,
                process_start_ticks=process_start_ticks,
                signum=signal.SIGINT,
            )
        except Exception as exc:
            try:
                controller.record_manual_yield_signal_result(
                    claim,
                    delivered=False,
                    detail=f"signal operation raised {exc}",
                    result_epoch=time.time(),
                    actor=event_actor,
                    changed_at=timestamp,
                )
            except V5SchedulerError as audit_error:
                exc = V5ContinuationError(
                    f"signal operation raised {exc}; result audit failed: "
                    f"{audit_error}"
                )
            self._raise_signal_uncertainty(
                item=item,
                request_id=request.request_id,
                actor=event_actor,
                changed_at=timestamp,
                detail=f"signal operation raised {exc}",
                cause=exc,
            )
        if not signaled:
            cause = V5ContinuationError(
                "authenticated signal delivery returned false"
            )
            try:
                controller.record_manual_yield_signal_result(
                    claim,
                    delivered=False,
                    detail=str(cause),
                    result_epoch=time.time(),
                    actor=event_actor,
                    changed_at=timestamp,
                )
            except V5SchedulerError as audit_error:
                cause = V5ContinuationError(
                    f"{cause}; result audit failed: {audit_error}"
                )
            self._raise_signal_uncertainty(
                item=item,
                request_id=request.request_id,
                actor=event_actor,
                changed_at=timestamp,
                detail=str(cause),
                cause=cause,
            )
        try:
            controller.record_manual_yield_signal_result(
                claim,
                delivered=True,
                detail="authenticated SIGINT was delivered",
                result_epoch=time.time(),
                actor=event_actor,
                changed_at=timestamp,
            )
        except V5SchedulerError as exc:
            self._raise_signal_uncertainty(
                item=item,
                request_id=request.request_id,
                actor=event_actor,
                changed_at=timestamp,
                detail=f"signal was delivered but result audit failed: {exc}",
                cause=exc,
            )
        return pending

    def recover_pending(
        self,
        prepared: PreparedAttempt,
    ) -> V5PendingContinuation:
        """Rehydrate a persisted yielding operation after process/service restart."""

        if type(prepared) is not PreparedAttempt:
            raise TypeError(
                f"prepared must be exactly PreparedAttempt, got "
                f"{type(prepared).__name__}"
            )
        item = self.repository.get_queue_item(prepared.queue_item_id)
        try:
            with self.repository.store.connect() as connection:
                row = connection.execute(
                    "SELECT yield_request_id FROM queue_items WHERE id = ?",
                    (item.id,),
                ).fetchone()
        except Exception as exc:
            raise V5ContinuationError(
                f"could not recover yield request identity for queue item "
                f"{item.id}: {exc}"
            ) from exc
        if item.state != "yielding" or row is None or row["yield_request_id"] is None:
            raise V5ContinuationError(
                f"queue item {item.id} is not a persisted yielding operation"
            )
        request_record = self.repository.get_yield_request(
            str(row["yield_request_id"])
        )
        request = request_record.request
        expected = (
            item.project_id,
            item.revision_id,
            item.id,
            item.segment,
            item.experiment_id,
            item.attempt,
            item.git_commit,
            None if item.snapshot is None else item.snapshot.resolved_sha256,
        )
        actual = (
            prepared.project_id,
            prepared.project_revision_id,
            prepared.queue_item_id,
            prepared.segment,
            prepared.experiment_id,
            prepared.attempt,
            prepared.git_commit,
            prepared.resolved_spec_sha256,
        )
        if actual != expected:
            raise V5ContinuationError(
                "recovered PreparedAttempt identity differs from the yielding "
                "queue item"
            )
        if (
            request.queue_item_id != item.id
            or request.segment != item.segment
            or request_record.project_id != item.project_id
            or request_record.revision_id != item.revision_id
        ):
            raise V5ContinuationError(
                "persisted yield request ownership differs from the yielding item"
            )
        try:
            _atomic_create_or_verify(
                prepared.paths.yield_request,
                request_record.source,
                root=prepared.paths.segment_root,
            )
            published = _read_regular(
                prepared.paths.yield_request, label="published yield request"
            )
        except V5ContinuationError as exc:
            raise V5ContinuationError(
                f"persisted yield request cannot be recovered: {exc}"
            ) from exc
        if published != request_record.source:
            raise V5ContinuationError(
                "published yield request differs from immutable repository evidence"
            )
        prior_receipt_sha256 = request.continuation.prior_receipt_sha256
        if prior_receipt_sha256 is None:
            raise V5ContinuationError(
                "manual-yield continuation lacks prior runner-receipt identity"
            )
        return _construct_pending(
            project_id=item.project_id,
            revision_id=item.revision_id,
            queue_item_id=item.id,
            segment=item.segment,
            run_id=request.continuation.run_id,
            prior_runner_receipt_sha256=prior_receipt_sha256,
            request_path=prepared.paths.yield_request,
            receipt_path=prepared.paths.yield_receipt,
            request=request,
            request_source=request_record.source,
            request_sha256=request_record.sha256,
        )

    def _isolate_and_raise(
        self,
        pending: V5PendingContinuation,
        *,
        reason: str,
        actor: str,
        changed_at: str,
        cause: BaseException,
    ) -> None:
        try:
            self.repository.isolate_continuation_failure(
                pending.queue_item_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
                terminal=False,
            )
        except V5RepositoryError as isolation_error:
            raise V5ContinuationError(
                f"{reason}; item isolation did not override a concurrent state "
                f"transition: {isolation_error}"
            ) from cause
        raise V5ContinuationError(reason) from cause

    def finalize_manual_yield(
        self,
        pending: V5PendingContinuation,
        *,
        actor: str,
        changed_at: str,
    ) -> V5ContinuationOutcome:
        """Validate/persist one strict receipt, then requeue or isolate its item."""

        if type(pending) is not V5PendingContinuation:
            raise TypeError(
                f"pending must be exactly V5PendingContinuation, got "
                f"{type(pending).__name__}"
            )
        event_actor = _text(actor, field_name="actor", maximum=256)
        timestamp = _timestamp(changed_at, field_name="changed_at")
        request_record = self.repository.get_yield_request(
            pending.request.request_id
        )
        if (
            request_record.request != pending.request
            or request_record.source != pending.request_source
            or request_record.sha256 != pending.request_sha256
        ):
            raise V5ContinuationError(
                "pending manual-yield evidence differs from immutable repository "
                "request evidence"
            )
        try:
            source = _read_regular(pending.receipt_path, label="yield receipt")
            document = _decode_document(source, label="yield receipt")
            if _wire_bytes(document) != source:
                raise V5ContinuationError(
                    "yield receipt does not use the deterministic "
                    "CooperativeYield/v1 wire encoding"
                )
            receipt = CooperativeYieldReceipt.from_document(document)
        except (V5ContinuationError, CooperativeYieldError, TypeError, ValueError) as exc:
            self._isolate_and_raise(
                pending,
                reason=f"manual-yield receipt is missing or invalid: {exc}",
                actor=event_actor,
                changed_at=timestamp,
                cause=exc,
            )
        assert isinstance(receipt, CooperativeYieldReceipt)
        if receipt.status is YieldReceiptStatus.FAILED:
            try:
                receipt_record = self.repository.record_yield_receipt(
                    receipt,
                    source=source,
                    actor=event_actor,
                )
            except V5RepositoryError as exc:
                self._isolate_and_raise(
                    pending,
                    reason=f"failed manual-yield receipt is stale or invalid: {exc}",
                    actor=event_actor,
                    changed_at=timestamp,
                    cause=exc,
                )
            item = self.repository.isolate_continuation_failure(
                pending.queue_item_id,
                reason=f"project declined manual continuation: {receipt.error}",
                actor=event_actor,
                changed_at=timestamp,
                terminal=True,
            )
            return V5ContinuationOutcome(
                item=item,
                request_record=request_record,
                receipt_record=receipt_record,
                requeued=False,
            )

        item = self.repository.get_queue_item(pending.queue_item_id)
        revision = self.repository.get_revision(pending.revision_id)
        if item.snapshot is None:
            raise V5ContinuationError(
                f"queue item {item.id} lost typed admission evidence"
            )
        try:
            validate_ready_continuation(
                receipt,
                pending.request,
                resolved_spec_sha256=item.snapshot.resolved_sha256,
                project_revision=revision.label,
                git_commit=revision.git_commit,
                run_id=pending.run_id,
                prior_receipt_sha256=pending.prior_runner_receipt_sha256,
                allowed_artifact_roots=(
                    root.path for root in revision.enrollment.artifact_roots
                ),
                expected_checkpoint_names=_checkpoint_names(item),
            )
            receipt_record = self.repository.record_yield_receipt(
                receipt,
                source=source,
                actor=event_actor,
            )
            requeued = self.repository.requeue_ready_continuation(
                pending.request.request_id,
                actor=event_actor,
                changed_at=timestamp,
            )
        except (CooperativeYieldError, V5RepositoryError, V5ContinuationError) as exc:
            self._isolate_and_raise(
                pending,
                reason=f"manual continuation failed validation: {exc}",
                actor=event_actor,
                changed_at=timestamp,
                cause=exc,
            )
        return V5ContinuationOutcome(
            item=requeued,
            request_record=request_record,
            receipt_record=receipt_record,
            requeued=True,
        )


__all__ = [
    "V5ContinuationCoordinator",
    "V5ContinuationError",
    "V5ContinuationOutcome",
    "V5PendingContinuation",
]
