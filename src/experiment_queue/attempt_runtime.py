"""Prepare and launch one project-qualified durable schema-v5 attempt."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
from typing import Mapping, Self

from experiment_queue.admission import AdmissionSnapshot
from experiment_queue.execution import ExecutionPlan, ExecutionValidationError
from experiment_queue.executor import (
    ExecutorError,
    ExecutorLaunchReceipt,
    ExecutorReceipt,
    confirm_immutable_evidence_for_read,
)
from experiment_queue.project_lifecycle import ProjectRevision
from experiment_queue.project_worktrees import ProjectWorktreeEvidence
from experiment_queue.serialization import JSONValue, canonical_json_bytes


_PROJECT_KEY_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OBJECT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_MAX_CONTROL_FILE_BYTES = 1024 * 1024


class AttemptRuntimeError(RuntimeError):
    """Raised when an attempt cannot be prepared, launched, or authenticated."""


class AttemptLaunchUncertainError(AttemptRuntimeError):
    """A failed launch whose complete process-group termination is unproven."""

    def __init__(
        self,
        message: str,
        *,
        pid: int | None,
        pgid: int | None,
        process_start_ticks: str | None,
    ) -> None:
        super().__init__(message)
        self.pid = pid
        self.pgid = pgid
        self.process_start_ticks = process_start_ticks


def _positive_integer(value: int, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise AttemptRuntimeError(f"{field_name} must be a positive integer")
    return value


def _text(value: str, *, field_name: str, maximum: int = 4096) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise AttemptRuntimeError(
            f"{field_name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > maximum or "\x00" in value:
        raise AttemptRuntimeError(
            f"{field_name} must be NUL-free text of at most {maximum} characters"
        )
    return value


def _canonical_directory(path: Path, *, field_name: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise AttemptRuntimeError(f"{field_name} must be an absolute pathlib.Path")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise AttemptRuntimeError(f"{field_name} {path} cannot be resolved: {exc}") from exc
    if resolved != path or not resolved.is_dir():
        raise AttemptRuntimeError(
            f"{field_name} must remain the recorded canonical directory {path}"
        )
    return resolved


def _create_private_directory_chain(
    state: Path,
    components: tuple[str, ...],
) -> Path:
    """Create and authenticate queue-owned control directories without links.

    Every descent is relative to an already-open directory descriptor. New
    components are normalized to mode 0700 independently of the service umask;
    pre-existing components must already be owned by the service account and
    must not grant group or world write access.
    """

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        parent_descriptor = os.open(state, flags)
    except OSError as exc:
        raise AttemptRuntimeError(
            f"could not open state_directory {state} without following links: {exc}"
        ) from exc
    try:
        state_details = os.fstat(parent_descriptor)
        state_mode = stat.S_IMODE(state_details.st_mode)
        if (
            not stat.S_ISDIR(state_details.st_mode)
            or state_details.st_uid != os.geteuid()
            or state_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise AttemptRuntimeError(
                f"state_directory {state} must be owned by uid {os.geteuid()} "
                "and not group/world writable before attempt control creation; "
                f"got uid {state_details.st_uid} mode {state_mode:04o}"
            )

        current = state
        for component in components:
            created = False
            try:
                os.mkdir(component, mode=0o700, dir_fd=parent_descriptor)
                created = True
            except FileExistsError:
                pass
            except OSError as exc:
                raise AttemptRuntimeError(
                    f"could not create attempt control directory "
                    f"{current / component}: {exc}"
                ) from exc
            try:
                child_descriptor = os.open(
                    component,
                    flags,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                raise AttemptRuntimeError(
                    f"attempt control component {current / component} must be "
                    f"a real directory and not a symlink: {exc}"
                ) from exc
            try:
                if created:
                    os.fchmod(child_descriptor, 0o700)
                details = os.fstat(child_descriptor)
                mode = stat.S_IMODE(details.st_mode)
                if (
                    not stat.S_ISDIR(details.st_mode)
                    or details.st_uid != os.geteuid()
                    or mode & (stat.S_IWGRP | stat.S_IWOTH)
                ):
                    raise AttemptRuntimeError(
                        f"attempt control component {current / component} must "
                        f"be owned by uid {os.geteuid()} and not group/world "
                        f"writable; got uid {details.st_uid} mode {mode:04o}"
                    )
            except BaseException:
                os.close(child_descriptor)
                raise
            os.close(parent_descriptor)
            parent_descriptor = child_descriptor
            current /= component
    finally:
        os.close(parent_descriptor)

    try:
        resolved = current.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise AttemptRuntimeError(
            f"attempt control directory {current} cannot be resolved after "
            f"creation: {exc}"
        ) from exc
    if resolved != current or state not in resolved.parents:
        raise AttemptRuntimeError(
            f"attempt control directory {current} escaped canonical state root "
            f"{state} through a path replacement"
        )
    return resolved


def _sha256(source: bytes) -> str:
    return hashlib.sha256(source).hexdigest()


def _read_regular(path: Path) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise AttemptRuntimeError(f"could not open control file {path}: {exc}") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise AttemptRuntimeError(f"control file must be regular: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            source = stream.read(_MAX_CONTROL_FILE_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(source) > _MAX_CONTROL_FILE_BYTES:
        raise AttemptRuntimeError(
            f"control file {path} exceeds {_MAX_CONTROL_FILE_BYTES} bytes"
        )
    return source


def _atomic_create_or_verify(path: Path, source: bytes) -> None:
    """Publish immutable control evidence, accepting an exact prior write only."""

    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = _read_regular(path)
        if existing != source:
            raise AttemptRuntimeError(
                f"refused to overwrite changed scheduler control evidence {path}"
            )
        return
    except OSError as exc:
        raise AttemptRuntimeError(f"could not create control file {path}: {exc}") from exc
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source)
            stream.flush()
            os.fsync(stream.fileno())
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


@dataclass(frozen=True, slots=True)
class AttemptPaths:
    """Scheduler-owned paths for one immutable queue segment."""

    control_root: Path
    segment_root: Path
    payload: Path
    launch_receipt: Path
    exit_receipt: Path
    launcher_log: Path
    yield_request: Path
    yield_receipt: Path
    continuation_receipt: Path

    @classmethod
    def create(
        cls,
        *,
        state_directory: Path,
        project_key: str,
        queue_item_id: int,
        segment: int,
    ) -> Self:
        """Create a canonical, non-symlinked control directory beneath state."""

        state = _canonical_directory(
            state_directory, field_name="state_directory"
        )
        key = _text(project_key, field_name="project_key", maximum=63)
        if _PROJECT_KEY_PATTERN.fullmatch(key) is None:
            raise AttemptRuntimeError(f"project_key has invalid syntax: {key!r}")
        item_id = _positive_integer(queue_item_id, field_name="queue_item_id")
        segment_value = _positive_integer(segment, field_name="segment")
        components = (
            "attempts",
            "projects",
            key,
            "items",
            str(item_id),
            "segments",
            str(segment_value),
        )
        resolved = _create_private_directory_chain(state, components)
        return cls(
            control_root=state,
            segment_root=resolved,
            payload=resolved / "executor.json",
            launch_receipt=resolved / "launch.json",
            exit_receipt=resolved / "exit.json",
            launcher_log=resolved / "launcher.log",
            yield_request=resolved / "yield-request.json",
            yield_receipt=resolved / "yield-receipt.json",
            continuation_receipt=resolved / "continuation-receipt.json",
        )


@dataclass(frozen=True, slots=True, init=False)
class PreparedAttempt:
    """Frozen launch inputs derived from admitted revision and snapshot evidence."""

    queue_item_id: int
    project_id: int
    project_key: str
    project_revision_id: int
    project_revision: str
    experiment_id: str
    attempt: int
    segment: int
    git_commit: str
    resolved_spec_sha256: str | None
    admission_kind: str
    command_kind: str
    worktree: Path
    cwd: Path
    argv: tuple[str, ...]
    paths: AttemptPaths
    payload_sha256: str
    command_sha256: str
    gpu_uuid: str
    gpu_index: str
    _environment_items: tuple[tuple[str, str], ...] = field(repr=False)

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "PreparedAttempt is validated-only; use prepare_structured_attempt()"
        )

    @property
    def environment(self) -> dict[str, str]:
        """Return a detached child environment copy."""

        return dict(self._environment_items)

    def read_exit_receipt(self) -> ExecutorReceipt:
        """Authenticate terminal executor evidence against this launch plan."""

        return ExecutorReceipt.read(
            self.paths.exit_receipt,
            queue_item_id=self.queue_item_id,
            project_id=self.project_id,
            project_revision_id=self.project_revision_id,
            project_key=self.project_key,
            project_revision=self.project_revision,
            experiment_id=self.experiment_id,
            attempt=self.attempt,
            resolved_spec_sha256=self.resolved_spec_sha256,
            admission_kind=self.admission_kind,
            segment=self.segment,
            git_commit=self.git_commit,
            worktree=self.worktree,
            command_kind=self.command_kind,
            command_sha256=self.command_sha256,
            gpu_uuid=self.gpu_uuid,
        )

    def read_launch_receipt(
        self,
        *,
        pid: int | None = None,
        pgid: int | None = None,
        process_start_ticks: str | None = None,
    ) -> ExecutorLaunchReceipt:
        """Authenticate durable executor identity against this exact payload."""

        return ExecutorLaunchReceipt.read(
            self.paths.launch_receipt,
            queue_item_id=self.queue_item_id,
            project_id=self.project_id,
            project_key=self.project_key,
            project_revision_id=self.project_revision_id,
            project_revision=self.project_revision,
            experiment_id=self.experiment_id,
            attempt=self.attempt,
            segment=self.segment,
            payload_sha256=self.payload_sha256,
            gpu_uuid=self.gpu_uuid,
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        )


def _construct_prepared(**values: object) -> PreparedAttempt:
    instance = object.__new__(PreparedAttempt)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def prepare_structured_attempt(
    *,
    state_directory: Path,
    queue_item_id: int,
    experiment_id: str,
    attempt: int,
    segment: int,
    revision: ProjectRevision,
    snapshot: AdmissionSnapshot,
    execution_plan: ExecutionPlan,
    worktree_evidence: ProjectWorktreeEvidence,
    gpu_uuid: str,
    gpu_index: str,
    allow_existing_exit_receipt: bool = False,
    prior_yield_receipt_source: bytes | None = None,
) -> PreparedAttempt:
    """Persist or rehydrate an exact payload after checking typed identity.

    ``allow_existing_exit_receipt`` is only for scheduler recovery: it permits
    reconstruction of immutable attempt identity after exit, but the launcher
    still independently refuses any duplicate segment execution.
    """

    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got {type(revision).__name__}"
        )
    if type(snapshot) is not AdmissionSnapshot:
        raise TypeError(
            f"snapshot must be exactly AdmissionSnapshot, got {type(snapshot).__name__}"
        )
    if type(execution_plan) is not ExecutionPlan:
        raise TypeError(
            "execution_plan must be exactly ExecutionPlan, got "
            f"{type(execution_plan).__name__}"
        )
    try:
        execution_plan.validate_integrity()
    except ExecutionValidationError as exc:
        raise AttemptRuntimeError(
            f"execution_plan failed factory-integrity validation: {exc}"
        ) from exc
    if type(worktree_evidence) is not ProjectWorktreeEvidence:
        raise TypeError(
            "worktree_evidence must be exactly ProjectWorktreeEvidence, got "
            f"{type(worktree_evidence).__name__}"
        )
    item_id = _positive_integer(queue_item_id, field_name="queue_item_id")
    attempt_value = _positive_integer(attempt, field_name="attempt")
    segment_value = _positive_integer(segment, field_name="segment")
    experiment = _text(experiment_id, field_name="experiment_id", maximum=256)
    gpu = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
    gpu_host_index = _text(gpu_index, field_name="gpu_index", maximum=64)
    if type(allow_existing_exit_receipt) is not bool:
        raise TypeError("allow_existing_exit_receipt must be a boolean")
    if segment_value == 1 and prior_yield_receipt_source is not None:
        raise AttemptRuntimeError(
            "first structured segment cannot carry a prior yield receipt"
        )
    if segment_value > 1:
        if type(prior_yield_receipt_source) is not bytes:
            raise AttemptRuntimeError(
                "continued structured segment requires exact prior yield receipt bytes"
            )
        if (
            not prior_yield_receipt_source
            or len(prior_yield_receipt_source) > _MAX_CONTROL_FILE_BYTES
        ):
            raise AttemptRuntimeError(
                "prior yield receipt must contain 1 through "
                f"{_MAX_CONTROL_FILE_BYTES} exact bytes"
            )

    mismatches: list[str] = []
    expected = {
        "plan.project_id": (execution_plan.project_id, revision.project_id),
        "plan.project_key": (execution_plan.project_key, revision.project_key),
        "plan.revision_id": (execution_plan.project_revision_id, revision.id),
        "plan.revision": (execution_plan.project_revision, revision.label),
        "plan.commit": (execution_plan.git_commit, revision.git_commit),
        "plan.resolved_spec_sha256": (
            execution_plan.resolved_spec_sha256,
            snapshot.resolved_sha256,
        ),
        "snapshot.revision": (snapshot.project_revision, revision.label),
        "snapshot.commit": (snapshot.git_commit, revision.git_commit),
        "worktree.project_id": (worktree_evidence.project_id, revision.project_id),
        "worktree.project_key": (worktree_evidence.project_key, revision.project_key),
        "worktree.revision_id": (worktree_evidence.project_revision_id, revision.id),
        "worktree.revision": (worktree_evidence.project_revision, revision.label),
        "worktree.commit": (worktree_evidence.git_commit, revision.git_commit),
        "worktree.item_id": (worktree_evidence.queue_item_id, item_id),
    }
    for label, (actual, wanted) in expected.items():
        if actual != wanted:
            mismatches.append(f"{label} {actual!r} != {wanted!r}")
    metadata = snapshot.card_document.get("metadata")
    snapshot_experiment = (
        metadata.get("experimentId") if type(metadata) is dict else None
    )
    if snapshot_experiment != experiment:
        mismatches.append(
            f"snapshot experiment {snapshot_experiment!r} != {experiment!r}"
        )
    worktree = _canonical_directory(
        worktree_evidence.worktree, field_name="worktree_evidence.worktree"
    )
    if execution_plan.worktree_root != worktree:
        mismatches.append(
            f"plan.worktree_root {execution_plan.worktree_root!r} != {worktree!r}"
        )
    if execution_plan.cwd != worktree and worktree not in execution_plan.cwd.parents:
        mismatches.append(
            f"execution cwd {execution_plan.cwd} is outside worktree {worktree}"
        )
    if mismatches:
        raise AttemptRuntimeError(
            "attempt identity does not match admitted evidence: " + "; ".join(mismatches)
        )

    paths = AttemptPaths.create(
        state_directory=state_directory,
        project_key=revision.project_key,
        queue_item_id=item_id,
        segment=segment_value,
    )
    if (
        not allow_existing_exit_receipt
        and (paths.exit_receipt.exists() or paths.exit_receipt.is_symlink())
    ):
        raise AttemptRuntimeError(
            f"attempt exit receipt already exists at {paths.exit_receipt}; recover "
            "the recorded segment rather than launching it twice"
        )
    environment = execution_plan.environment
    queue_values = {
        "EXPERIMENT_QUEUE_ITEM_ID": str(item_id),
        "EXPERIMENT_QUEUE_PROJECT_ID": str(revision.project_id),
        "EXPERIMENT_QUEUE_PROJECT_KEY": revision.project_key,
        "EXPERIMENT_QUEUE_PROJECT_REVISION_ID": str(revision.id),
        "EXPERIMENT_QUEUE_PROJECT_REVISION": revision.label,
        "EXPERIMENT_QUEUE_GIT_COMMIT": revision.git_commit,
        "EXPERIMENT_QUEUE_EXPERIMENT_ID": experiment,
        "EXPERIMENT_QUEUE_ATTEMPT": str(attempt_value),
        "EXPERIMENT_QUEUE_SEGMENT": str(segment_value),
        "EXPERIMENT_QUEUE_GPU_UUID": gpu,
        "EXPERIMENT_QUEUE_WORKTREE": str(worktree),
        "EXPERIMENT_QUEUE_PRIMARY_REPO": str(
            revision.enrollment.checkout_directory
        ),
        "EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH": str(
            paths.segment_root / "runner.json"
        ),
        "CUDA_VISIBLE_DEVICES": gpu,
    }
    if snapshot.submission_policy.preemption_authorized:
        queue_values.update(
            {
                "EXPERIMENT_QUEUE_YIELD_REQUEST_PATH": str(paths.yield_request),
                "EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH": str(paths.yield_receipt),
            }
        )
    if prior_yield_receipt_source is not None:
        _atomic_create_or_verify(
            paths.continuation_receipt, prior_yield_receipt_source
        )
        queue_values["EXPERIMENT_QUEUE_CONTINUATION_RECEIPT_PATH"] = str(
            paths.continuation_receipt
        )
    environment.update(queue_values)
    command = list(execution_plan.argv)
    command_sha256 = _sha256(canonical_json_bytes(command))
    payload: dict[str, JSONValue] = {
        "schema_version": 1,
        "queue_item_id": item_id,
        "project_id": revision.project_id,
        "project_revision_id": revision.id,
        "project_key": revision.project_key,
        "project_revision": revision.label,
        "experiment_id": experiment,
        "attempt": attempt_value,
        "resolved_spec_sha256": snapshot.resolved_sha256,
        "admission_kind": "ExperimentCard/v1",
        "segment": segment_value,
        "git_commit": revision.git_commit,
        "worktree": str(worktree),
        "cwd": str(execution_plan.cwd),
        "command_kind": "argv",
        "command": command,
        "control_root": str(paths.control_root),
        "receipt_path": str(paths.exit_receipt),
    }
    encoded = canonical_json_bytes(payload) + b"\n"
    _atomic_create_or_verify(paths.payload, encoded)
    return _construct_prepared(
        queue_item_id=item_id,
        project_id=revision.project_id,
        project_key=revision.project_key,
        project_revision_id=revision.id,
        project_revision=revision.label,
        experiment_id=experiment,
        attempt=attempt_value,
        segment=segment_value,
        git_commit=revision.git_commit,
        resolved_spec_sha256=snapshot.resolved_sha256,
        admission_kind="ExperimentCard/v1",
        command_kind="argv",
        worktree=worktree,
        cwd=execution_plan.cwd,
        argv=execution_plan.argv,
        paths=paths,
        payload_sha256=_sha256(encoded),
        command_sha256=command_sha256,
        gpu_uuid=gpu,
        gpu_index=gpu_host_index,
        _environment_items=tuple(sorted(environment.items())),
    )


def prepare_legacy_attempt(
    *,
    state_directory: Path,
    queue_item_id: int,
    project_id: int,
    project_key: str,
    project_revision_id: int,
    project_revision: str,
    experiment_id: str,
    attempt: int,
    segment: int,
    git_commit: str,
    execution_root: Path,
    primary_checkout: Path,
    command_text: str,
    ambient_environment: Mapping[str, str],
    gpu_uuid: str,
    gpu_index: str,
    preemptible: bool = False,
    continuation_run_directory: Path | None = None,
    continuation_checkpoint: Path | None = None,
    continuation_wandb_id: str | None = None,
    allow_existing_exit_receipt: bool = False,
) -> PreparedAttempt:
    """Prepare one exact imported LegacyMarkdownCard/v0 compatibility launch.

    The caller must first authenticate the recorded checkout/worktree, commit,
    and card digest. This function never reparses Markdown and never rewrites
    the recorded shell text; `/bin/bash -lc` exists only behind the explicit
    legacy admission discriminator enforced by the durable executor.
    ``allow_existing_exit_receipt`` is reserved for restart recovery and never
    authorizes launching the same segment twice.
    """

    item_id = _positive_integer(queue_item_id, field_name="queue_item_id")
    owner_id = _positive_integer(project_id, field_name="project_id")
    revision_id = _positive_integer(
        project_revision_id, field_name="project_revision_id"
    )
    attempt_value = _positive_integer(attempt, field_name="attempt")
    segment_value = _positive_integer(segment, field_name="segment")
    key = _text(project_key, field_name="project_key", maximum=63)
    if _PROJECT_KEY_PATTERN.fullmatch(key) is None:
        raise AttemptRuntimeError(f"project_key has invalid syntax: {key!r}")
    revision_label = _text(
        project_revision, field_name="project_revision", maximum=256
    )
    experiment = _text(experiment_id, field_name="experiment_id", maximum=256)
    commit = _text(git_commit, field_name="git_commit", maximum=64)
    if _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
        raise AttemptRuntimeError(
            "git_commit must be a full lowercase 40- or 64-character object ID"
        )
    command = _text(command_text, field_name="legacy command", maximum=262_144)
    root = _canonical_directory(execution_root, field_name="legacy execution_root")
    primary = _canonical_directory(primary_checkout, field_name="legacy primary_checkout")
    gpu = _text(gpu_uuid, field_name="gpu_uuid", maximum=256)
    gpu_host_index = _text(gpu_index, field_name="gpu_index", maximum=64)
    if not isinstance(ambient_environment, Mapping):
        raise TypeError("ambient_environment must be a mapping of names to values")
    if type(preemptible) is not bool:
        raise TypeError("preemptible must be a boolean")
    if type(allow_existing_exit_receipt) is not bool:
        raise TypeError("allow_existing_exit_receipt must be a boolean")
    environment: dict[str, str] = {}
    for name, value in ambient_environment.items():
        if type(name) is not str or not name or "=" in name or "\x00" in name:
            raise AttemptRuntimeError(
                f"legacy ambient environment has invalid variable name {name!r}"
            )
        if type(value) is not str or "\x00" in value:
            raise AttemptRuntimeError(
                f"legacy ambient environment variable {name!r} is not NUL-free text"
            )
        environment[name] = value
    paths = AttemptPaths.create(
        state_directory=state_directory,
        project_key=key,
        queue_item_id=item_id,
        segment=segment_value,
    )
    if (
        not allow_existing_exit_receipt
        and (paths.exit_receipt.exists() or paths.exit_receipt.is_symlink())
    ):
        raise AttemptRuntimeError(
            f"attempt exit receipt already exists at {paths.exit_receipt}; recover "
            "the recorded segment rather than launching it twice"
        )
    queue_values = {
            "EXPERIMENT_QUEUE_ITEM_ID": str(item_id),
            "EXPERIMENT_QUEUE_PROJECT_ID": str(owner_id),
            "EXPERIMENT_QUEUE_PROJECT_KEY": key,
            "EXPERIMENT_QUEUE_PROJECT_REVISION_ID": str(revision_id),
            "EXPERIMENT_QUEUE_PROJECT_REVISION": revision_label,
            "EXPERIMENT_QUEUE_GIT_COMMIT": commit,
            "EXPERIMENT_QUEUE_EXPERIMENT_ID": experiment,
            "EXPERIMENT_QUEUE_ATTEMPT": str(attempt_value),
            "EXPERIMENT_QUEUE_SEGMENT": str(segment_value),
            "EXPERIMENT_QUEUE_GPU_UUID": gpu,
            "EXPERIMENT_QUEUE_WORKTREE": str(root),
            "EXPERIMENT_QUEUE_PRIMARY_REPO": str(primary),
            "EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH": str(
                paths.segment_root / "runner.json"
            ),
            "CUDA_VISIBLE_DEVICES": gpu,
    }
    if preemptible:
        queue_values.update(
            {
                "EXPERIMENT_QUEUE_YIELD_REQUEST_PATH": str(paths.yield_request),
                "EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH": str(paths.yield_receipt),
            }
        )
    if segment_value > 1:
        if continuation_run_directory is None or continuation_checkpoint is None:
            raise AttemptRuntimeError(
                "legacy continuation segment requires its recorded run directory "
                "and checkpoint"
            )
        run_directory = _canonical_directory(
            continuation_run_directory,
            field_name="legacy continuation_run_directory",
        )
        checkpoint_source = Path(continuation_checkpoint)
        if not checkpoint_source.is_absolute() or checkpoint_source.is_symlink():
            raise AttemptRuntimeError(
                "legacy continuation_checkpoint must be an absolute non-symlink file"
            )
        try:
            checkpoint = checkpoint_source.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise AttemptRuntimeError(
                f"legacy continuation checkpoint cannot be resolved: {exc}"
            ) from exc
        if checkpoint != checkpoint_source or not checkpoint.is_file():
            raise AttemptRuntimeError(
                "legacy continuation_checkpoint changed canonical target or is not a file"
            )
        queue_values["EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR"] = str(run_directory)
        queue_values["EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT"] = str(checkpoint)
        if continuation_wandb_id is not None:
            queue_values["EXPERIMENT_QUEUE_WANDB_ID"] = _text(
                continuation_wandb_id,
                field_name="legacy continuation_wandb_id",
                maximum=256,
            )
    elif any(
        value is not None
        for value in (
            continuation_run_directory,
            continuation_checkpoint,
            continuation_wandb_id,
        )
    ):
        raise AttemptRuntimeError(
            "legacy first segment cannot carry continuation-only evidence"
        )
    environment.update(queue_values)
    command_sha256 = _sha256(command.encode("utf-8"))
    payload: dict[str, JSONValue] = {
        "schema_version": 1,
        "queue_item_id": item_id,
        "project_id": owner_id,
        "project_revision_id": revision_id,
        "project_key": key,
        "project_revision": revision_label,
        "experiment_id": experiment,
        "attempt": attempt_value,
        "resolved_spec_sha256": None,
        "admission_kind": "LegacyMarkdownCard/v0",
        "segment": segment_value,
        "git_commit": commit,
        "worktree": str(root),
        "cwd": str(root),
        "command_kind": "legacy-shell",
        "command": command,
        "control_root": str(paths.control_root),
        "receipt_path": str(paths.exit_receipt),
    }
    encoded = canonical_json_bytes(payload) + b"\n"
    _atomic_create_or_verify(paths.payload, encoded)
    return _construct_prepared(
        queue_item_id=item_id,
        project_id=owner_id,
        project_key=key,
        project_revision_id=revision_id,
        project_revision=revision_label,
        experiment_id=experiment,
        attempt=attempt_value,
        segment=segment_value,
        git_commit=commit,
        resolved_spec_sha256=None,
        admission_kind="LegacyMarkdownCard/v0",
        command_kind="legacy-shell",
        worktree=root,
        cwd=root,
        argv=("/bin/bash", "-lc", command),
        paths=paths,
        payload_sha256=_sha256(encoded),
        command_sha256=command_sha256,
        gpu_uuid=gpu,
        gpu_index=gpu_host_index,
        _environment_items=tuple(sorted(environment.items())),
    )


@dataclass(slots=True)
class LaunchedAttempt:
    """Live process plus persistent identity needed for scheduler recovery."""

    prepared: PreparedAttempt
    process: subprocess.Popen[bytes] = field(repr=False)
    pid: int
    pgid: int
    process_start_ticks: str | None


def _linux_process_start_ticks(pid: int) -> str | None:
    """Read Linux /proc start time; macOS development returns no stable token."""

    try:
        source = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    closing = source.rfind(")")
    if closing < 0:
        return None
    fields_after_comm = source[closing + 1 :].split()
    return fields_after_comm[19] if len(fields_after_comm) > 19 else None


def _linux_process_identity_required() -> bool:
    """Return whether this host exposes the production process-token contract."""

    return Path("/proc").is_dir()


def process_identity_matches(
    *,
    pid: int,
    pgid: int,
    process_start_ticks: str | None,
) -> bool:
    """Authenticate a recorded process before recovery monitoring or signaling."""

    _positive_integer(pid, field_name="pid")
    _positive_integer(pgid, field_name="pgid")
    try:
        os.kill(pid, 0)
        actual_pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return False
    if actual_pgid != pgid:
        return False
    if process_start_ticks is None:
        # Linux production must have a /proc token. macOS development cannot
        # safely authenticate a process across restart and therefore fails
        # closed instead of trusting a potentially reused PID.
        return False
    actual_ticks = _linux_process_start_ticks(pid)
    return actual_ticks is not None and actual_ticks == process_start_ticks


def signal_recorded_process(
    *,
    pid: int,
    pgid: int,
    process_start_ticks: str | None,
    signum: int,
) -> bool:
    """Signal an authenticated process group; never signal a reused identity."""

    if signum not in {signal.SIGINT, signal.SIGTERM, signal.SIGKILL}:
        raise AttemptRuntimeError(
            "scheduler may signal attempts only with SIGINT, SIGTERM, or SIGKILL"
        )
    if not process_identity_matches(
        pid=pid,
        pgid=pgid,
        process_start_ticks=process_start_ticks,
    ):
        return False
    try:
        # Graceful signals target only the authenticated executor.  It is the
        # sole per-signum coalescing broadcaster to the scientific process group,
        # including signals received after launch publication but before the
        # first child exists. SIGKILL cannot be forwarded, so escalation still
        # kills the complete group directly.
        if signum == signal.SIGKILL:
            os.killpg(pgid, signum)
        else:
            os.kill(pid, signum)
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise AttemptRuntimeError(
            f"permission denied signaling authenticated process group {pgid}"
        ) from exc
    return True


def _named_process_group_exists(pgid: int) -> bool:
    """Probe one freshly spawned or durably recorded process group by identity."""

    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        raise AttemptRuntimeError(
            f"could not inspect named process group {pgid}: {exc}"
        ) from exc
    return True


def _stop_failed_launch_group(
    process: subprocess.Popen[bytes],
    *,
    process_start_ticks: str | None,
) -> None:
    """Kill a failed fresh launch and prove its complete process group absent."""

    pid = process.pid
    pgid = process.pid
    try:
        group_exists = _named_process_group_exists(pgid)
    except AttemptRuntimeError as exc:
        raise AttemptLaunchUncertainError(
            f"failed launch process-group state is uncertain: {exc}",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        ) from exc
    if not group_exists:
        if process.poll() is None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired as exc:
                raise AttemptLaunchUncertainError(
                    "failed launch group appears absent but its executor leader "
                    "could not be reaped",
                    pid=pid,
                    pgid=pgid,
                    process_start_ticks=process_start_ticks,
                ) from exc
        return

    delivered = False
    try:
        if process_start_ticks is not None:
            delivered = signal_recorded_process(
                pid=pid,
                pgid=pgid,
                process_start_ticks=process_start_ticks,
                signum=signal.SIGKILL,
            )
        elif process.poll() is None and os.getpgid(pid) == pgid:
            # This is the exact Popen child created above with
            # start_new_session=True, not a discovered or guessed PID.
            os.killpg(pgid, signal.SIGKILL)
            delivered = True
    except (AttemptRuntimeError, OSError) as exc:
        raise AttemptLaunchUncertainError(
            f"could not kill failed launch process group {pgid}: {exc}",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        ) from exc
    if not delivered:
        raise AttemptLaunchUncertainError(
            f"authenticated SIGKILL was not delivered to failed launch group {pgid}",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        )
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as exc:
        raise AttemptLaunchUncertainError(
            f"failed launch executor {pid} did not exit after group SIGKILL",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        ) from exc
    try:
        group_exists = _named_process_group_exists(pgid)
    except AttemptRuntimeError as exc:
        raise AttemptLaunchUncertainError(
            f"could not verify failed launch group {pgid} absent: {exc}",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        ) from exc
    if group_exists:
        raise AttemptLaunchUncertainError(
            f"failed launch process group {pgid} still exists after SIGKILL",
            pid=pid,
            pgid=pgid,
            process_start_ticks=process_start_ticks,
        )


def stop_launched_attempt(launched: LaunchedAttempt) -> None:
    """Stop a launched attempt only when complete group absence can be proven."""

    if type(launched) is not LaunchedAttempt:
        raise TypeError(
            f"launched must be exactly LaunchedAttempt, got {type(launched).__name__}"
        )
    _stop_failed_launch_group(
        launched.process,
        process_start_ticks=launched.process_start_ticks,
    )


def launch_prepared_attempt(prepared: PreparedAttempt) -> LaunchedAttempt:
    """Launch only after the executor fsyncs its authenticated identity."""

    if type(prepared) is not PreparedAttempt:
        raise TypeError(
            f"prepared must be exactly PreparedAttempt, got {type(prepared).__name__}"
        )
    payload = _read_regular(prepared.paths.payload)
    if _sha256(payload) != prepared.payload_sha256:
        raise AttemptRuntimeError(
            f"executor payload changed after preparation: {prepared.paths.payload}"
        )
    if prepared.paths.exit_receipt.exists() or prepared.paths.exit_receipt.is_symlink():
        raise AttemptRuntimeError(
            f"attempt receipt already exists: {prepared.paths.exit_receipt}; refuse "
            "duplicate segment launch"
        )
    try:
        confirm_immutable_evidence_for_read(prepared.paths.launch_receipt)
    except ExecutorError as exc:
        raise AttemptLaunchUncertainError(
            f"launch evidence requires operator inspection: {exc}",
            pid=None,
            pgid=None,
            process_start_ticks=None,
        ) from exc
    if os.path.lexists(prepared.paths.launch_receipt):
        raise AttemptRuntimeError(
            f"attempt launch receipt already exists: {prepared.paths.launch_receipt}; "
            "refuse duplicate or stale segment launch"
        )
    try:
        log_descriptor = os.open(
            prepared.paths.launcher_log,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise AttemptRuntimeError(
            f"could not open launcher log {prepared.paths.launcher_log}: {exc}"
        ) from exc
    try:
        if not stat.S_ISREG(os.fstat(log_descriptor).st_mode):
            raise AttemptRuntimeError(
                f"launcher log must be a regular file: {prepared.paths.launcher_log}"
            )
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-m",
                    "experiment_queue.executor",
                    str(prepared.paths.payload),
                ],
                cwd=prepared.cwd,
                env=prepared.environment,
                stdin=subprocess.DEVNULL,
                stdout=log_descriptor,
                stderr=subprocess.STDOUT,
                shell=False,
                start_new_session=True,
            )
        except OSError as exc:
            raise AttemptRuntimeError(f"could not launch durable executor: {exc}") from exc
    finally:
        os.close(log_descriptor)
    observed_ticks = _linux_process_start_ticks(process.pid)
    if _linux_process_identity_required() and observed_ticks is None:
        try:
            _stop_failed_launch_group(
                process,
                process_start_ticks=None,
            )
        except AttemptLaunchUncertainError:
            raise
        raise AttemptRuntimeError(
            f"could not authenticate Linux executor process start time for PID "
            f"{process.pid}"
        )
    deadline = time.monotonic() + 10.0
    try:
        while not os.path.lexists(prepared.paths.launch_receipt):
            return_code = process.poll()
            if return_code is not None:
                raise AttemptRuntimeError(
                    f"durable executor exited with {return_code} before publishing "
                    f"launch receipt {prepared.paths.launch_receipt}"
                )
            if time.monotonic() >= deadline:
                raise AttemptRuntimeError(
                    f"durable executor did not publish launch receipt "
                    f"{prepared.paths.launch_receipt} within 10 seconds"
                )
            time.sleep(0.01)
        receipt = prepared.read_launch_receipt(
            pid=process.pid,
            pgid=process.pid,
            process_start_ticks=observed_ticks,
        )
        if receipt.process_start_ticks != observed_ticks:
            raise AttemptRuntimeError(
                "executor launch receipt process_start_ticks does not match the "
                "newly launched process"
            )
    except (AttemptRuntimeError, ExecutorError) as exc:
        try:
            _stop_failed_launch_group(
                process,
                process_start_ticks=observed_ticks,
            )
        except AttemptLaunchUncertainError as uncertainty:
            raise uncertainty from exc
        raise
    return LaunchedAttempt(
        prepared=prepared,
        process=process,
        pid=receipt.pid,
        pgid=receipt.pgid,
        process_start_ticks=receipt.process_start_ticks,
    )


__all__ = [
    "AttemptPaths",
    "AttemptLaunchUncertainError",
    "AttemptRuntimeError",
    "LaunchedAttempt",
    "PreparedAttempt",
    "launch_prepared_attempt",
    "prepare_legacy_attempt",
    "prepare_structured_attempt",
    "process_identity_matches",
    "signal_recorded_process",
    "stop_launched_attempt",
]
