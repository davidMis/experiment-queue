"""Run the project-aware schema-v5 GPU scheduler and durable attempts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import signal
import subprocess
import time
from typing import Callable, Mapping, Sequence

from experiment_queue.attempt_runtime import (
    AttemptPaths,
    AttemptLaunchUncertainError,
    AttemptRuntimeError,
    LaunchedAttempt,
    PreparedAttempt,
    launch_prepared_attempt,
    prepare_legacy_attempt,
    prepare_structured_attempt,
    process_identity_matches,
    signal_recorded_process,
    stop_launched_attempt,
)
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.continuation_v5 import (
    V5ContinuationCoordinator,
    V5ContinuationError,
    V5PendingContinuation,
)
from experiment_queue.execution import (
    ExecutionPlan,
    ExecutionValidationError,
    build_execution_plan,
    observe_execution_artifacts,
)
from experiment_queue.executor import (
    ExecutorError,
    ExecutorLaunchReceipt,
    ExecutorReceipt,
)
from experiment_queue.identity import validate_project_key
from experiment_queue.host_locks import HostGpuLockError, acquire_host_gpu_lock
from experiment_queue.legacy import LegacyCardError, legacy_command_for_worktree
from experiment_queue.legacy_continuation_v0 import (
    LegacyV0ContinuationCoordinator,
    LegacyV0ContinuationError,
    LegacyV0PendingContinuation,
)
from experiment_queue.project_lifecycle import ProjectRevision
from experiment_queue.project_worktrees import (
    ProjectWorktreeError,
    ProjectWorktreeEvidence,
    ProjectWorktreeManager,
)
from experiment_queue.queue import GpuSnapshot, query_gpus
from experiment_queue.reservation_v5 import V5ReservationService
from experiment_queue.scheduler_v5 import (
    FailureScope,
    V5AbandonedLaunchResolution,
    V5ActiveAttempt,
    V5DispatchCandidate,
    V5SchedulerError,
    V5SchedulingController,
    V5TerminationAction,
)
from experiment_queue.v5_repository import (
    V5ProjectRepository,
    V5QueueItem,
    V5RepositoryError,
)


_GIT_OBJECT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_LEGACY_SHARED_PATHS = (
    ".env",
    ".venv",
    "data",
    "experiments",
    "figures",
    "logs",
    "model",
    "out",
    "outputs",
    "oxy_updates",
    "papers",
    "runs",
)
_MAX_LEGACY_CARD_BYTES = 8 * 1024 * 1024
_QUEUE_RUNTIME_NAME_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*-r[1-9][0-9]*-"
    r"item-(?P<item>[1-9][0-9]*)-[0-9a-f]{12}\Z"
)


class V5SchedulerServiceError(RuntimeError):
    """Raised when the runnable v5 scheduler must stop or isolate a failure."""


def utc_now_iso() -> str:
    """Return one second-precision UTC scheduler timestamp."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _finite_float(value: float, *, field_name: str, minimum: float) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise V5SchedulerServiceError(f"{field_name} must be a number")
    converted = float(value)
    if converted != converted or converted == float("inf") or converted < minimum:
        raise V5SchedulerServiceError(
            f"{field_name} must be finite and at least {minimum}"
        )
    return converted


def _positive_integer(value: int, *, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise V5SchedulerServiceError(f"{field_name} must be a positive integer")
    return value


def _git_environment() -> dict[str, str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("GIT_")
    }
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_PAGER": "cat",
            "LC_ALL": "C",
        }
    )
    return environment


def _git_completed(
    repository: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Run one noninteractive structured Git command without interpreting status."""

    try:
        return subprocess.run(
            [
                "git",
                "--no-pager",
                "-c",
                "core.hooksPath=/dev/null",
                "-C",
                str(repository),
                *arguments,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=30,
            env=_git_environment(),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        raise V5SchedulerServiceError(
            f"could not run Git in legacy checkout {repository}: {exc}"
        ) from exc


def _git(repository: Path, *arguments: str) -> str:
    result = _git_completed(repository, *arguments)
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip() or "no detail")[:4096]
        raise V5SchedulerServiceError(
            f"Git {list(arguments)!r} failed in legacy checkout {repository}: {detail}"
        )
    return result.stdout.strip()


def _git_blob(repository: Path, *, commit: str, path: str) -> bytes:
    """Read one bounded regular Git blob without consulting checkout bytes."""

    listing = _git(repository, "ls-tree", "-z", commit, "--", path)
    records = [record for record in listing.split("\0") if record]
    if len(records) != 1 or "\t" not in records[0]:
        raise V5SchedulerServiceError(
            f"legacy card {path!r} is not one exact tracked Git entry at {commit}"
        )
    metadata, recorded_path = records[0].split("\t", 1)
    fields = metadata.split(" ")
    if recorded_path != path or len(fields) != 3:
        raise V5SchedulerServiceError(
            f"legacy card {path!r} has ambiguous Git tree evidence at {commit}"
        )
    mode, object_type, object_id = fields
    if mode not in {"100644", "100755"} or object_type != "blob":
        raise V5SchedulerServiceError(
            f"legacy card {path!r} must be a regular Git blob, got "
            f"mode/type {mode!r}/{object_type!r}"
        )
    size_text = _git(repository, "cat-file", "-s", object_id)
    try:
        size = int(size_text)
    except ValueError as exc:
        raise V5SchedulerServiceError(
            f"Git returned invalid size {size_text!r} for legacy card {path!r}"
        ) from exc
    if size < 0 or size > _MAX_LEGACY_CARD_BYTES:
        raise V5SchedulerServiceError(
            f"legacy card {path!r} is {size} bytes; maximum is "
            f"{_MAX_LEGACY_CARD_BYTES}"
        )
    command = [
        "git",
        "--no-pager",
        "-c",
        "core.hooksPath=/dev/null",
        "-C",
        str(repository),
        "cat-file",
        "blob",
        object_id,
    ]
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=30,
            env=_git_environment(),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        raise V5SchedulerServiceError(
            f"could not read Git blob for legacy card {path!r}: {exc}"
        ) from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()[:4096]
        raise V5SchedulerServiceError(
            f"Git could not read blob {object_id} for legacy card {path!r}: "
            f"{detail or f'exit code {result.returncode}'}"
        )
    if len(result.stdout) != size:
        raise V5SchedulerServiceError(
            f"legacy card {path!r} Git blob size changed from {size} to "
            f"{len(result.stdout)} bytes while reading"
        )
    return bytes(result.stdout)


def _canonical_directory(value: object, *, field_name: str) -> Path:
    if type(value) is not str or not value or not value.startswith("/"):
        raise V5SchedulerServiceError(f"{field_name} must be an absolute path")
    path = Path(value)
    if path.is_symlink():
        raise V5SchedulerServiceError(f"{field_name} must not be a symlink: {path}")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise V5SchedulerServiceError(f"{field_name} cannot be resolved: {path}: {exc}") from exc
    if resolved != path or not resolved.is_dir():
        raise V5SchedulerServiceError(
            f"{field_name} changed canonical target or is not a directory: {path}"
        )
    return resolved


def _portable_card_path(value: object) -> str:
    if type(value) is not str or not value or value.startswith(("/", "~")):
        raise V5SchedulerServiceError("legacy card path must be portable and relative")
    if len(value) > 4096:
        raise V5SchedulerServiceError("legacy card path must be 4096 characters or fewer")
    if (
        "\\" in value
        or "//" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise V5SchedulerServiceError("legacy card path has non-portable syntax")
    path = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise V5SchedulerServiceError("legacy card path contains traversal components")
    return path.as_posix()


def _sha256_regular(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise V5SchedulerServiceError(f"required evidence is not a regular file: {path}")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise V5SchedulerServiceError(f"could not hash evidence file {path}: {exc}") from exc
    return digest.hexdigest()


def _canonical_regular_file(value: object, *, field_name: str) -> Path:
    """Require exact absolute non-symlink file identity for legacy evidence."""

    if type(value) is not str or not value or not value.startswith("/"):
        raise V5SchedulerServiceError(f"{field_name} must be an absolute path")
    path = Path(value)
    if path.is_symlink():
        raise V5SchedulerServiceError(f"{field_name} must not be a symlink: {path}")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise V5SchedulerServiceError(
            f"{field_name} cannot be resolved: {path}: {exc}"
        ) from exc
    if resolved != path or not resolved.is_file():
        raise V5SchedulerServiceError(
            f"{field_name} changed canonical target or is not a file: {path}"
        )
    return resolved


@dataclass(frozen=True, slots=True)
class _LegacyContext:
    item: V5QueueItem
    project_key: str
    revision_label: str
    primary_checkout: Path
    execution_root: Path
    git_ref: str
    worktree_path: Path
    command_text: str
    continuation_run_directory: Path | None
    continuation_checkpoint: Path | None
    continuation_wandb_id: str | None


@dataclass(frozen=True, slots=True)
class _PreparedDispatch:
    prepared: PreparedAttempt
    candidate: V5DispatchCandidate
    revision: ProjectRevision | None = field(default=None, repr=False)
    worktree_evidence: ProjectWorktreeEvidence | None = field(default=None, repr=False)
    execution_plan: ExecutionPlan | None = field(default=None, repr=False)
    legacy_context: _LegacyContext | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class V5TerminationOutcome:
    """Committed termination intent and whether its first signal was delivered."""

    action: V5TerminationAction
    signal_delivered: bool


@dataclass(frozen=True, slots=True)
class V5AbandonedLaunchOutcome:
    """Guarded abandoned-launch transition and authenticated cleanup result."""

    resolution: V5AbandonedLaunchResolution
    launch_receipt_status: str
    worktree_cleanup_error: str | None


class V5SchedulerService:
    """Single-host foreground dispatcher with Project-local failure quarantine."""

    def __init__(
        self,
        store: V5QueueStore,
        *,
        poll_seconds: float = 60.0,
        control_seconds: float = 1.0,
        termination_grace_seconds: float = 30.0,
        manual_yield_signal_retry_seconds: float = 5.0,
        min_free_memory_fraction: float = 0.95,
        max_utilization_percent: float = 5.0,
        min_free_disk_gib: float = 50.0,
        gpu_provider: Callable[[], list[GpuSnapshot]] = query_gpus,
        ambient_environment: Mapping[str, str] | None = None,
        clock: Callable[[], str] = utc_now_iso,
        epoch_clock: Callable[[], float] = time.time,
    ):
        if type(store) is not V5QueueStore:
            raise TypeError(
                f"store must be exactly V5QueueStore, got {type(store).__name__}"
            )
        self.store = store
        self.repository = V5ProjectRepository(store)
        self.controller = V5SchedulingController(store)
        self.continuations = V5ContinuationCoordinator(self.repository)
        self.legacy_continuations = LegacyV0ContinuationCoordinator(self.repository)
        self.reservations = V5ReservationService(store)
        self.poll_seconds = _finite_float(
            poll_seconds, field_name="poll_seconds", minimum=0.001
        )
        self.control_seconds = _finite_float(
            control_seconds, field_name="control_seconds", minimum=0.001
        )
        self.termination_grace_seconds = _finite_float(
            termination_grace_seconds,
            field_name="termination_grace_seconds",
            minimum=0.0,
        )
        self.manual_yield_signal_retry_seconds = _finite_float(
            manual_yield_signal_retry_seconds,
            field_name="manual_yield_signal_retry_seconds",
            minimum=0.0,
        )
        self.min_free_memory_fraction = _finite_float(
            min_free_memory_fraction,
            field_name="min_free_memory_fraction",
            minimum=0.0,
        )
        if self.min_free_memory_fraction > 1.0:
            raise V5SchedulerServiceError(
                "min_free_memory_fraction must be between 0 and 1"
            )
        self.max_utilization_percent = _finite_float(
            max_utilization_percent,
            field_name="max_utilization_percent",
            minimum=0.0,
        )
        if self.max_utilization_percent > 100.0:
            raise V5SchedulerServiceError(
                "max_utilization_percent must be between 0 and 100"
            )
        self.min_free_disk_gib = _finite_float(
            min_free_disk_gib, field_name="min_free_disk_gib", minimum=0.0
        )
        if not callable(gpu_provider):
            raise TypeError("gpu_provider must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not callable(epoch_clock):
            raise TypeError("epoch_clock must be callable")
        self.gpu_provider = gpu_provider
        self.clock = clock
        self.epoch_clock = epoch_clock
        source_environment = os.environ if ambient_environment is None else ambient_environment
        if not isinstance(source_environment, Mapping):
            raise TypeError("ambient_environment must be a mapping")
        self.ambient_environment = dict(source_environment)
        worktree_root = store.state_dir / "worktrees"
        try:
            worktree_root.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise V5SchedulerServiceError(
                f"could not create scheduler worktree root {worktree_root}: {exc}"
            ) from exc
        try:
            self.worktrees = ProjectWorktreeManager.create(worktree_root)
        except ProjectWorktreeError as exc:
            raise V5SchedulerServiceError(
                f"scheduler worktree root is unsafe: {exc}"
            ) from exc
        self.processes: dict[int, LaunchedAttempt] = {}
        self.dispatch_contexts: dict[int, _PreparedDispatch] = {}
        self.gpu_locks: dict[str, object] = {}
        self._scheduler_lock: object | None = None
        self._last_gpu_poll = 0.0
        self._replayed_termination_signals: set[
            tuple[int, int, str, int | None, int | None, str | None, float]
        ] = set()
        self._stop = False

    def _lock_scheduler(self) -> None:
        lock_path = self.store.state_dir / "scheduler.lock"
        lock_file = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise V5SchedulerServiceError(
                f"another scheduler already holds {lock_path}; stop it before "
                "starting a second writer"
            ) from exc
        self._scheduler_lock = lock_file

    def _global_gpu_lock(self, uuid: str) -> bool:
        if uuid in self.gpu_locks:
            return True
        try:
            lock_file = acquire_host_gpu_lock(uuid)
        except HostGpuLockError as exc:
            raise V5SchedulerServiceError(
                f"cannot authenticate host-wide lock for GPU {uuid!r}: {exc}"
            ) from exc
        if lock_file is None:
            return False
        self.gpu_locks[uuid] = lock_file
        return True

    def _release_gpu_lock(self, uuid: str | None) -> None:
        if uuid is None:
            return
        lock = self.gpu_locks.pop(uuid, None)
        if lock is not None:
            lock.close()  # type: ignore[attr-defined]

    def _idle(self, gpu: GpuSnapshot) -> bool:
        return (
            not gpu.compute_pids
            and gpu.free_memory_fraction >= self.min_free_memory_fraction
            and gpu.utilization_percent <= self.max_utilization_percent
        )

    @staticmethod
    def _validated_gpu_snapshots(value: object) -> list[GpuSnapshot]:
        """Authenticate one complete, unambiguous telemetry snapshot set."""

        if type(value) is not list or any(
            type(snapshot) is not GpuSnapshot for snapshot in value
        ):
            raise V5SchedulerServiceError(
                "GPU telemetry must contain only exact GpuSnapshot records"
            )
        snapshots = list(value)
        uuids: set[str] = set()
        indices: set[str] = set()
        for snapshot in snapshots:
            assert type(snapshot) is GpuSnapshot
            if (
                type(snapshot.uuid) is not str
                or not snapshot.uuid
                or snapshot.uuid != snapshot.uuid.strip()
                or type(snapshot.index) is not str
                or not snapshot.index
                or snapshot.index != snapshot.index.strip()
                or type(snapshot.name) is not str
                or not snapshot.name
                or snapshot.name != snapshot.name.strip()
            ):
                raise V5SchedulerServiceError(
                    "GPU telemetry identity fields must be non-empty text without "
                    "surrounding whitespace"
                )
            if snapshot.uuid in uuids or snapshot.index in indices:
                raise V5SchedulerServiceError(
                    f"GPU telemetry contains duplicate UUID or host index for "
                    f"{snapshot.uuid!r}/{snapshot.index!r}"
                )
            uuids.add(snapshot.uuid)
            indices.add(snapshot.index)
            numbers = (
                snapshot.memory_total_mib,
                snapshot.memory_used_mib,
                snapshot.utilization_percent,
            )
            if any(
                type(number) not in {int, float}
                or isinstance(number, bool)
                or float(number) != float(number)
                or abs(float(number)) == float("inf")
                for number in numbers
            ) or not (
                float(snapshot.memory_total_mib) > 0
                and 0
                <= float(snapshot.memory_used_mib)
                <= float(snapshot.memory_total_mib)
                and 0 <= float(snapshot.utilization_percent) <= 100
            ):
                raise V5SchedulerServiceError(
                    f"GPU telemetry for {snapshot.uuid!r} has invalid memory or "
                    "utilization metrics"
                )
            if type(snapshot.compute_pids) is not tuple or any(
                type(pid) is not int or pid <= 0 for pid in snapshot.compute_pids
            ) or len(set(snapshot.compute_pids)) != len(snapshot.compute_pids):
                raise V5SchedulerServiceError(
                    f"GPU telemetry for {snapshot.uuid!r} has invalid or duplicate "
                    "compute process IDs"
                )
        return snapshots

    def _require_current_idle_gpu(
        self,
        *,
        item_id: int,
        gpu_uuid: str,
        actor: str,
    ) -> GpuSnapshot:
        """Authenticate one exact live idle GPU while retaining its host lock.

        A terminal executor leader is not proof that detached scientific work
        stopped using the GPU. Missing, duplicate, or malformed telemetry
        pauses all dispatch. A valid busy observation simply retains the
        durable runtime lease plus host-wide lock for the next control pass.
        """

        if not self._global_gpu_lock(gpu_uuid):
            reason = (
                f"cannot release queue item {item_id} GPU lease: host-wide lock "
                f"for {gpu_uuid!r} is owned by another scheduler/process"
            )
            self.controller.pause_host(
                reason=reason,
                actor=actor,
                changed_at=self.clock(),
            )
            raise V5SchedulerServiceError(reason)
        try:
            snapshots = self._validated_gpu_snapshots(self.gpu_provider())
        except Exception as exc:
            reason = (
                f"cannot release queue item {item_id} GPU lease: current GPU "
                f"telemetry failed: {exc}"
            )
            self.controller.pause_host(
                reason=reason,
                actor=actor,
                changed_at=self.clock(),
            )
            raise V5SchedulerServiceError(reason) from exc
        matches = [snapshot for snapshot in snapshots if snapshot.uuid == gpu_uuid]
        if len(matches) != 1:
            reason = (
                f"cannot release queue item {item_id} GPU lease: telemetry returned "
                f"{len(matches)} exact records for assigned GPU {gpu_uuid!r}; "
                "exactly one is required"
            )
            self.controller.pause_host(
                reason=reason,
                actor=actor,
                changed_at=self.clock(),
            )
            raise V5SchedulerServiceError(reason)
        gpu = matches[0]
        if not self._idle(gpu):
            reason = (
                f"cannot release queue item {item_id} GPU lease: assigned GPU "
                f"{gpu_uuid!r} is still busy (compute PIDs "
                f"{list(gpu.compute_pids)}, free memory "
                f"{gpu.free_memory_fraction:.4f}, utilization "
                f"{gpu.utilization_percent:.2f}%)"
            )
            raise V5SchedulerServiceError(reason)
        return gpu

    def _release_finalized_gpu_lease(
        self,
        item_id: int,
        *,
        gpu: GpuSnapshot,
        actor: str,
    ) -> bool:
        """Commit the durable lease release authenticated by ``gpu``."""

        return self.controller.release_gpu_lease(
            item_id,
            gpu_uuid=gpu.uuid,
            observed_gpu_index=gpu.index,
            memory_total_mib=gpu.memory_total_mib,
            memory_used_mib=gpu.memory_used_mib,
            utilization_percent=gpu.utilization_percent,
            compute_pids=gpu.compute_pids,
            minimum_free_memory_fraction=self.min_free_memory_fraction,
            maximum_utilization_percent=self.max_utilization_percent,
            actor=actor,
            observed_at=self.clock(),
        )

    def _refresh_allowlist(self, snapshots: Sequence[GpuSnapshot]) -> None:
        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        changed: list[tuple[str, str, str]] = []
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for row in connection.execute("SELECT * FROM gpu_allowlist"):
                gpu = by_uuid.get(str(row["uuid"]))
                if gpu is None:
                    continue
                old_index = str(row["last_index"])
                connection.execute(
                    """
                    UPDATE gpu_allowlist SET last_index = ?, name = ?, updated_at = ?
                    WHERE uuid = ?
                    """,
                    (gpu.index, gpu.name, self.clock(), gpu.uuid),
                )
                if old_index != gpu.index:
                    changed.append((gpu.uuid, old_index, gpu.index))
        for uuid, old_index, new_index in changed:
            self.repository.record_event(
                created_at=self.clock(),
                actor="scheduler",
                event_type="gpu_host_index_changed",
                scope="host",
                payload={
                    "gpuUuid": uuid,
                    "oldIndex": old_index,
                    "newIndex": new_index,
                },
            )

    def _available_gpus(self) -> list[GpuSnapshot]:
        try:
            snapshots = self._validated_gpu_snapshots(self.gpu_provider())
        except Exception as exc:
            reason = f"GPU telemetry failed: {exc}"
            self.controller.pause_host(
                reason=reason, actor="scheduler", changed_at=self.clock()
            )
            return []
        self._refresh_allowlist(snapshots)
        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        with self.store.connect() as connection:
            allowed = list(
                connection.execute(
                    """
                    SELECT * FROM gpu_allowlist
                    WHERE enabled = 1 AND draining = 0
                    ORDER BY CAST(last_index AS INTEGER), uuid
                    """
                )
            )
            assigned = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT assigned_gpu_uuid FROM queue_items
                    WHERE runtime_gpu_lease_held = 1
                      AND assigned_gpu_uuid IS NOT NULL
                    """
                )
            }
        reserved = self.reservations.open_gpu_uuids()
        return [
            by_uuid[str(row["uuid"])]
            for row in allowed
            if str(row["uuid"]) in by_uuid
            and str(row["uuid"]) not in assigned
            and str(row["uuid"]) not in reserved
            and self._idle(by_uuid[str(row["uuid"])])
        ]

    def _legacy_runtime_identity(
        self,
        *,
        project_key: str,
        revision_id: int,
        item_id: int,
        commit: str,
    ) -> tuple[str, Path]:
        """Derive the sole destination-owned identity for an imported item."""

        try:
            key = validate_project_key(project_key)
        except ValueError as exc:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} has invalid project key: {exc}"
            ) from exc
        revision_key = _positive_integer(revision_id, field_name="revision_id")
        item_key = _positive_integer(item_id, field_name="item_id")
        if _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} has invalid full commit {commit!r}"
            )
        root = self.worktrees.state_worktree_root
        target = root / (
            f"{key}-r{revision_key}-item-{item_key}-{commit[:12]}"
        )
        if target.parent != root:
            raise V5SchedulerServiceError(
                f"derived legacy runtime worktree {target} escapes {root}"
            )
        try:
            resolved = target.resolve(strict=False)
        except (OSError, RuntimeError) as exc:
            raise V5SchedulerServiceError(
                f"derived legacy runtime worktree {target} cannot be resolved: {exc}"
            ) from exc
        if resolved != target:
            raise V5SchedulerServiceError(
                f"legacy runtime worktree {target} resolves as {resolved}; refuse "
                "path substitution"
            )
        return (
            f"refs/experiment-queue/projects/{key}/revisions/"
            f"{revision_key}/items/{item_key}",
            target,
        )

    @staticmethod
    def _legacy_common_directory(repository: Path) -> Path:
        value = Path(_git(repository, "rev-parse", "--git-common-dir"))
        candidate = value if value.is_absolute() else repository / value
        try:
            return candidate.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise V5SchedulerServiceError(
                f"legacy repository {repository} Git common directory cannot be "
                f"resolved: {exc}"
            ) from exc

    @staticmethod
    def _legacy_registered_worktree(repository: Path, target: Path) -> bool:
        output = _git(repository, "worktree", "list", "--porcelain", "-z")
        paths = [
            Path(field.removeprefix("worktree "))
            for field in output.split("\0")
            if field.startswith("worktree ")
        ]
        matches = [path for path in paths if path == target]
        if len(matches) > 1:
            raise V5SchedulerServiceError(
                f"Git worktree registry repeats legacy runtime target {target}"
            )
        return bool(matches)

    def _legacy_excludes_file(self) -> Path:
        """Create or verify the exact v4 shared-path ignore overlay."""

        path = self.store.state_dir / "legacy_worktree_shared_paths.exclude"
        source = (
            "# Scheduler-managed shared paths in imported legacy worktrees.\n"
            + "".join(f"/{name}\n" for name in _LEGACY_SHARED_PATHS)
        ).encode("utf-8")
        if os.path.lexists(path):
            if path.is_symlink() or not path.is_file():
                raise V5SchedulerServiceError(
                    f"legacy excludes path must be a regular non-symlink file: {path}"
                )
            try:
                existing = path.read_bytes()
            except OSError as exc:
                raise V5SchedulerServiceError(
                    f"could not read legacy excludes file {path}: {exc}"
                ) from exc
            if existing != source:
                raise V5SchedulerServiceError(
                    f"legacy excludes file {path} differs from the frozen v4 "
                    "shared-path policy; remove it only after inspection"
                )
            return path
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(source)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            return self._legacy_excludes_file()
        except OSError as exc:
            raise V5SchedulerServiceError(
                f"could not create legacy excludes file {path}: {exc}"
            ) from exc
        return path

    def _legacy_worktree_git_completed(
        self,
        repository: Path,
        *arguments: str,
    ) -> subprocess.CompletedProcess[str]:
        return _git_completed(
            repository,
            "-c",
            f"core.excludesFile={self._legacy_excludes_file()}",
            *arguments,
        )

    def _legacy_worktree_git(self, repository: Path, *arguments: str) -> str:
        result = self._legacy_worktree_git_completed(repository, *arguments)
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip() or "no detail")[:4096]
            raise V5SchedulerServiceError(
                f"Git {list(arguments)!r} failed in legacy runtime {repository}: "
                f"{detail}"
            )
        return result.stdout.strip()

    def _legacy_child_environment(self) -> dict[str, str]:
        """Reproduce the frozen v4 command-scoped shared-path Git policy."""

        environment = dict(self.ambient_environment)
        raw_count = environment.get("GIT_CONFIG_COUNT", "0")
        try:
            count = int(raw_count)
        except ValueError as exc:
            raise V5SchedulerServiceError(
                f"legacy ambient GIT_CONFIG_COUNT must be an integer, got "
                f"{raw_count!r}"
            ) from exc
        if count < 0:
            raise V5SchedulerServiceError(
                f"legacy ambient GIT_CONFIG_COUNT must be nonnegative, got {count}"
            )
        environment[f"GIT_CONFIG_KEY_{count}"] = "core.excludesFile"
        environment[f"GIT_CONFIG_VALUE_{count}"] = str(
            self._legacy_excludes_file()
        )
        environment["GIT_CONFIG_COUNT"] = str(count + 1)
        return environment

    @staticmethod
    def _legacy_shared_link_is_exact(
        *, checkout: Path, worktree: Path, name: str
    ) -> bool:
        """Recognize only the compatibility symlink created for one shared path."""

        source = checkout / name
        target = worktree / name
        if not target.is_symlink():
            return False
        try:
            return os.readlink(target) == str(source)
        except OSError:
            return False

    def _legacy_worktree_status_changes(
        self,
        *,
        checkout: Path,
        worktree: Path,
    ) -> tuple[str, ...]:
        """Return every non-compatibility tracked, untracked, or ignored entry."""

        status = self._legacy_worktree_git(
            worktree,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
            "--ignore-submodules=none",
        )
        changes: list[str] = []
        for record in status.split("\0"):
            if not record:
                continue
            if record.startswith("!! "):
                name = record[3:]
                if name in _LEGACY_SHARED_PATHS and self._legacy_shared_link_is_exact(
                    checkout=checkout,
                    worktree=worktree,
                    name=name,
                ):
                    continue
            changes.append(record)
        return tuple(changes)

    @staticmethod
    def _legacy_read_ref(repository: Path, git_ref: str) -> str | None:
        result = _git_completed(
            repository,
            "rev-parse",
            "--verify",
            "--quiet",
            git_ref,
        )
        if result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip():
            return None
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip() or "no detail")[:4096]
            raise V5SchedulerServiceError(
                f"Git could not inspect legacy runtime ref {git_ref!r}: {detail}"
            )
        value = result.stdout.strip()
        if _GIT_OBJECT_PATTERN.fullmatch(value) is None:
            raise V5SchedulerServiceError(
                f"legacy runtime ref {git_ref!r} resolved to invalid object {value!r}"
            )
        return value

    def _verify_legacy_worktree(
        self,
        *,
        checkout: Path,
        worktree: Path,
        git_ref: str,
        commit: str,
        require_clean: bool,
    ) -> None:
        """Authenticate one exact detached worktree without repairing it."""

        if worktree.is_symlink():
            raise V5SchedulerServiceError(
                f"legacy runtime worktree must not be a symlink: {worktree}"
            )
        try:
            resolved = worktree.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise V5SchedulerServiceError(
                f"legacy runtime worktree {worktree} is missing or inaccessible: {exc}"
            ) from exc
        if resolved != worktree or not worktree.is_dir():
            raise V5SchedulerServiceError(
                f"legacy runtime worktree changed canonical identity: {worktree}"
            )
        top = Path(_git(worktree, "rev-parse", "--show-toplevel")).resolve(strict=True)
        if top != worktree:
            raise V5SchedulerServiceError(
                f"legacy runtime worktree top-level is {top}, expected {worktree}"
            )
        if self._legacy_common_directory(worktree) != self._legacy_common_directory(checkout):
            raise V5SchedulerServiceError(
                f"legacy runtime worktree {worktree} belongs to another repository"
            )
        if _git(worktree, "rev-parse", "HEAD") != commit:
            raise V5SchedulerServiceError(
                f"legacy runtime worktree {worktree} HEAD differs from {commit}"
            )
        symbolic = _git_completed(worktree, "symbolic-ref", "-q", "HEAD")
        if symbolic.returncode == 0:
            raise V5SchedulerServiceError(
                f"legacy runtime worktree {worktree} is attached to branch "
                f"{symbolic.stdout.strip()!r}; expected detached HEAD"
            )
        if symbolic.returncode != 1:
            detail = (symbolic.stderr.strip() or symbolic.stdout.strip() or "no detail")[:4096]
            raise V5SchedulerServiceError(
                f"Git could not verify detached HEAD in {worktree}: {detail}"
            )
        if not self._legacy_registered_worktree(checkout, worktree):
            raise V5SchedulerServiceError(
                f"Git does not register exact legacy runtime worktree {worktree}"
            )
        if self._legacy_read_ref(checkout, git_ref) != commit:
            raise V5SchedulerServiceError(
                f"legacy runtime ref {git_ref!r} is missing or differs from {commit}"
            )
        if require_clean:
            dirty = self._legacy_worktree_status_changes(
                checkout=checkout,
                worktree=worktree,
            )
            if dirty:
                detail = repr(dirty)[:2048]
                raise V5SchedulerServiceError(
                    f"legacy runtime worktree {worktree} is dirty; contains tracked, "
                    f"untracked, or ignored non-compatibility content: "
                    f"{detail}"
                )

    def _link_legacy_shared_paths(self, checkout: Path, worktree: Path) -> None:
        """Recreate only the frozen v4 ignored shared-path compatibility links."""

        outputs = checkout / "outputs"
        try:
            outputs.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise V5SchedulerServiceError(
                f"could not prepare legacy shared outputs directory {outputs}: {exc}"
            ) from exc
        for name in _LEGACY_SHARED_PATHS:
            source = checkout / name
            if not source.exists() and not source.is_symlink():
                continue
            target = worktree / name
            if os.path.lexists(target):
                if self._legacy_shared_link_is_exact(
                    checkout=checkout,
                    worktree=worktree,
                    name=name,
                ):
                    continue
                raise V5SchedulerServiceError(
                    f"legacy runtime path {target} already exists and cannot be "
                    f"bound to exact shared path {source}"
                )
            ignore_candidate = f"{name}/" if source.is_dir() else name
            ignored = self._legacy_worktree_git_completed(
                worktree,
                "check-ignore",
                "-q",
                "--no-index",
                "--",
                ignore_candidate,
            )
            if ignored.returncode != 0:
                raise V5SchedulerServiceError(
                    f"legacy shared path {name!r} is not ignored by commit; add "
                    ".gitignore coverage before cutover or resolve the item"
                )
            try:
                target.symlink_to(source, target_is_directory=source.is_dir())
            except OSError as exc:
                raise V5SchedulerServiceError(
                    f"could not link legacy shared path {target} to {source}: {exc}"
                ) from exc

    def _prepare_legacy_runtime_worktree(
        self,
        *,
        checkout: Path,
        worktree: Path,
        git_ref: str,
        commit: str,
    ) -> None:
        """Crash-safely create or verify a destination-owned legacy worktree."""

        if checkout == self.worktrees.state_worktree_root or (
            checkout in self.worktrees.state_worktree_root.parents
            or self.worktrees.state_worktree_root in checkout.parents
        ):
            raise V5SchedulerServiceError(
                "legacy checkout overlaps the scheduler state worktree root"
            )
        self._reject_legacy_runtime_collisions(
            checkout=checkout,
            worktree=worktree,
            git_ref=git_ref,
            item_id=int(git_ref.rsplit("/", 1)[-1]),
        )
        current_ref = self._legacy_read_ref(checkout, git_ref)
        target_exists = os.path.lexists(worktree)
        registered = self._legacy_registered_worktree(checkout, worktree)
        if target_exists:
            self._verify_legacy_worktree(
                checkout=checkout,
                worktree=worktree,
                git_ref=git_ref,
                commit=commit,
                require_clean=True,
            )
        elif registered:
            raise V5SchedulerServiceError(
                f"legacy runtime path {worktree} is missing while Git registers it; "
                "explicit repair is required"
            )
        if current_ref is None:
            zero = "0" * len(commit)
            created = _git_completed(checkout, "update-ref", git_ref, commit, zero)
            if created.returncode != 0:
                current_ref = self._legacy_read_ref(checkout, git_ref)
                if current_ref != commit:
                    detail = (
                        created.stderr.strip()
                        or created.stdout.strip()
                        or f"exit code {created.returncode}"
                    )[:4096]
                    raise V5SchedulerServiceError(
                        f"could not create exact legacy runtime ref {git_ref!r}: {detail}"
                    )
        elif current_ref != commit:
            raise V5SchedulerServiceError(
                f"legacy runtime ref {git_ref!r} points to {current_ref}, not {commit}"
            )
        self._reject_legacy_runtime_collisions(
            checkout=checkout,
            worktree=worktree,
            git_ref=git_ref,
            item_id=int(git_ref.rsplit("/", 1)[-1]),
        )
        if not target_exists:
            added = _git_completed(
                checkout,
                "worktree",
                "add",
                "--detach",
                str(worktree),
                git_ref,
            )
            if added.returncode != 0:
                detail = (added.stderr.strip() or added.stdout.strip() or "no detail")[:4096]
                raise V5SchedulerServiceError(
                    f"could not create legacy runtime worktree {worktree}: {detail}"
                )
        self._link_legacy_shared_paths(checkout, worktree)
        self._verify_legacy_worktree(
            checkout=checkout,
            worktree=worktree,
            git_ref=git_ref,
            commit=commit,
            require_clean=True,
        )

    def _reject_legacy_runtime_collisions(
        self,
        *,
        checkout: Path,
        worktree: Path,
        git_ref: str,
        item_id: int,
    ) -> None:
        """Reject any second plausible v5 identity for one global item ID."""

        refs = _git(
            checkout,
            "for-each-ref",
            "--format=%(refname)",
            "refs/experiment-queue/projects",
        )
        conflicting_refs: list[str] = []
        for reference in refs.splitlines():
            parts = reference.split("/")
            if (
                len(parts) == 8
                and parts[:3] == ["refs", "experiment-queue", "projects"]
                and parts[4] == "revisions"
                and parts[6] == "items"
                and parts[7] == str(item_id)
                and reference != git_ref
            ):
                conflicting_refs.append(reference)
        try:
            children = tuple(self.worktrees.state_worktree_root.iterdir())
        except OSError as exc:
            raise V5SchedulerServiceError(
                "could not inspect scheduler worktree root for legacy runtime "
                f"identity collisions: {exc}"
            ) from exc
        conflicting_paths = [
            child
            for child in children
            if (
                (match := _QUEUE_RUNTIME_NAME_PATTERN.fullmatch(child.name))
                is not None
                and int(match.group("item")) == item_id
                and child != worktree
            )
        ]
        if conflicting_refs or conflicting_paths:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} has another plausible v5 runtime "
                f"identity: refs={sorted(conflicting_refs)!r}, "
                f"paths={sorted(str(path) for path in conflicting_paths)!r}"
            )

    def _legacy_context(
        self,
        item_id: int,
        *,
        allow_active_dirty: bool = False,
        prepare_runtime: bool = True,
        verify_runtime: bool = True,
    ) -> _LegacyContext:
        """Authenticate import evidence and use only a v5-owned runtime worktree."""

        if type(allow_active_dirty) is not bool:
            raise TypeError("allow_active_dirty must be a boolean")
        if type(prepare_runtime) is not bool:
            raise TypeError("prepare_runtime must be a boolean")
        if type(verify_runtime) is not bool:
            raise TypeError("verify_runtime must be a boolean")
        if prepare_runtime and not verify_runtime:
            raise ValueError("prepare_runtime requires verify_runtime")
        with self.store.connect() as connection:
            row = connection.execute(
                """
                SELECT item.*, project.project_key, revision.revision_label,
                       revision.revision_kind, revision.checkout_path
                FROM queue_items AS item
                JOIN projects AS project ON project.id = item.project_id
                JOIN project_revisions AS revision
                  ON revision.id = item.revision_id
                 AND revision.project_id = item.project_id
                WHERE item.id = ?
                """,
                (item_id,),
            ).fetchone()
            source_versions = (
                []
                if row is None
                else [
                    int(source["source_schema_version"])
                    for source in connection.execute(
                        "SELECT source_schema_version FROM migration_sources "
                        "WHERE project_id = ? ORDER BY id",
                        (int(row["project_id"]),),
                    )
                ]
            )
        if row is None or row["admission_kind"] != "LegacyMarkdownCard/v0":
            raise V5SchedulerServiceError(
                f"queue item {item_id} is not an imported legacy admission"
            )
        if len(source_versions) != 1:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} Project has {len(source_versions)} "
                "migration-source records; exact source schema cannot be "
                "authenticated"
            )
        source_schema_version = source_versions[0]
        if row["revision_kind"] != "legacy-v4":
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} is not owned by a legacy-v4 revision"
            )
        checkout = _canonical_directory(
            row["checkout_path"], field_name="legacy checkout_path"
        )
        top = Path(_git(checkout, "rev-parse", "--show-toplevel")).resolve(strict=True)
        if top != checkout:
            raise V5SchedulerServiceError(
                f"legacy checkout {checkout} is not the exact Git top-level {top}"
            )
        commit = str(row["git_commit"])
        if _GIT_OBJECT_PATTERN.fullmatch(commit) is None:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} has invalid full commit {commit!r}"
            )
        if _git(checkout, "rev-parse", "--verify", f"{commit}^{{commit}}") != commit:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} commit {commit} is not exact"
            )

        # Historical v4 identity is immutable provenance. Authenticate live,
        # unremoved evidence but never execute from or delete it under v5.
        historical_ref = row["git_ref"]
        historical_worktree = row["worktree_path"]
        historical_removed = row["worktree_removed_at"]
        if historical_ref is not None:
            expected_historical_ref = f"refs/experiment-queue/items/{item_id}"
            if str(historical_ref) != expected_historical_ref:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} records historical ref {historical_ref!r}; "
                    f"expected {expected_historical_ref!r}"
                )
        if historical_worktree is not None and historical_ref is None:
            raise V5SchedulerServiceError(
                f"legacy item {item_id} has historical worktree evidence without "
                "its pinned ref"
            )
        if historical_removed is None and historical_ref is not None:
            if self._legacy_read_ref(checkout, str(historical_ref)) != commit:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} historical ref is missing or changed"
                )
        if historical_removed is None and historical_worktree is not None:
            historical_path = _canonical_directory(
                historical_worktree,
                field_name="legacy historical worktree_path",
            )
            historical_top = Path(
                _git(historical_path, "rev-parse", "--show-toplevel")
            ).resolve(strict=True)
            if historical_top != historical_path:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} historical worktree top-level changed"
                )
            if self._legacy_common_directory(historical_path) != self._legacy_common_directory(checkout):
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} historical worktree belongs to another repository"
                )
            if _git(historical_path, "rev-parse", "HEAD") != commit:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} historical worktree HEAD changed"
                )

        card_relative = _portable_card_path(row["card_path"])
        card_source = _git_blob(checkout, commit=commit, path=card_relative)
        actual_card_hash = hashlib.sha256(card_source).hexdigest()
        if actual_card_hash != row["card_sha256"]:
            raise V5SchedulerServiceError(
                f"legacy queue item {item_id} committed card digest changed"
            )

        runtime_ref, runtime_worktree = self._legacy_runtime_identity(
            project_key=str(row["project_key"]),
            revision_id=int(row["revision_id"]),
            item_id=item_id,
            commit=commit,
        )
        command_text = legacy_command_for_worktree(
            str(row["command_text"]),
            runtime_worktree,
        )
        continuation_run: Path | None = None
        continuation_checkpoint: Path | None = None
        if int(row["segment"]) > 1:
            run_value = row["runner_run_dir"]
            checkpoint_value = row["continuation_checkpoint"]
            checkpoint_hash = row["continuation_checkpoint_sha256"]
            metadata_value = row["continuation_checkpoint_metadata"]
            metadata_hash = row["continuation_checkpoint_metadata_sha256"]
            if source_schema_version < 2:
                raise V5SchedulerServiceError(
                    f"legacy continuation item {item_id} cannot originate from "
                    f"schema v{source_schema_version}"
                )
            if not all(
                value is not None
                for value in (run_value, checkpoint_value, checkpoint_hash)
            ):
                raise V5SchedulerServiceError(
                    f"legacy continuation item {item_id} has incomplete checkpoint evidence"
                )
            continuation_run = _canonical_directory(
                run_value, field_name="legacy runner_run_dir"
            )
            continuation_checkpoint = _canonical_regular_file(
                checkpoint_value,
                field_name="legacy continuation_checkpoint",
            )
            try:
                continuation_checkpoint.relative_to(continuation_run)
            except ValueError as exc:
                raise V5SchedulerServiceError(
                    f"legacy continuation item {item_id} checkpoint is outside "
                    f"runner directory {continuation_run}: "
                    f"{continuation_checkpoint}"
                ) from exc
            if _sha256_regular(continuation_checkpoint) != checkpoint_hash:
                raise V5SchedulerServiceError(
                    f"legacy continuation item {item_id} checkpoint digest changed"
                )
            if source_schema_version >= 4:
                if metadata_value is None or metadata_hash is None:
                    raise V5SchedulerServiceError(
                        f"legacy continuation item {item_id} has incomplete v4 "
                        "checkpoint metadata evidence"
                    )
                metadata = _canonical_regular_file(
                    metadata_value,
                    field_name="legacy continuation_checkpoint_metadata",
                )
                try:
                    metadata.relative_to(continuation_run)
                except ValueError as exc:
                    raise V5SchedulerServiceError(
                        f"legacy continuation item {item_id} metadata is outside "
                        f"runner directory {continuation_run}: {metadata}"
                    ) from exc
                if _sha256_regular(metadata) != metadata_hash:
                    raise V5SchedulerServiceError(
                        f"legacy continuation item {item_id} metadata digest changed"
                    )
            elif metadata_value is not None or metadata_hash is not None:
                raise V5SchedulerServiceError(
                    f"legacy continuation item {item_id} schema-v"
                    f"{source_schema_version} import unexpectedly records v4 "
                    "checkpoint metadata"
                )

        # Validate every immutable command/continuation input before creating
        # filesystem state. A rejected pending continuation must not strand an
        # unrecorded destination-owned ref/worktree behind an open circuit.
        recorded_runtime = (
            row["runtime_git_ref"],
            row["runtime_worktree_path"],
        )
        expected_runtime = (runtime_ref, str(runtime_worktree))
        if prepare_runtime:
            if recorded_runtime not in {(None, None), expected_runtime}:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} persisted runtime identity "
                    f"{recorded_runtime!r} differs from {expected_runtime!r}"
                )
            self._prepare_legacy_runtime_worktree(
                checkout=checkout,
                worktree=runtime_worktree,
                git_ref=runtime_ref,
                commit=commit,
            )
        else:
            if recorded_runtime != expected_runtime:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} lacks exact persisted runtime identity; "
                    f"recorded {recorded_runtime!r}, expected {expected_runtime!r}"
                )
            if row["runtime_worktree_created_at"] is None:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} runtime identity lacks creation evidence"
                )
            if row["runtime_worktree_removed_at"] is not None:
                raise V5SchedulerServiceError(
                    f"legacy item {item_id} runtime worktree is recorded removed"
                )
            if verify_runtime:
                self._verify_legacy_worktree(
                    checkout=checkout,
                    worktree=runtime_worktree,
                    git_ref=runtime_ref,
                    commit=commit,
                    require_clean=not allow_active_dirty,
                )
        item = self.repository.get_queue_item(item_id)
        return _LegacyContext(
            item=item,
            project_key=str(row["project_key"]),
            revision_label=str(row["revision_label"]),
            primary_checkout=checkout,
            execution_root=runtime_worktree,
            git_ref=runtime_ref,
            worktree_path=runtime_worktree,
            command_text=command_text,
            continuation_run_directory=continuation_run,
            continuation_checkpoint=continuation_checkpoint,
            continuation_wandb_id=(
                None
                if row["continuation_wandb_id"] is None
                else str(row["continuation_wandb_id"])
            ),
        )

    def _prepare_dispatch(
        self,
        candidate: V5DispatchCandidate,
        gpu: GpuSnapshot,
    ) -> _PreparedDispatch:
        item = self.repository.get_queue_item(candidate.id)
        if item.admission_kind == "ExperimentCard/v1":
            if item.snapshot is None:
                raise V5SchedulerServiceError(
                    f"typed queue item {item.id} has no authenticated snapshot"
                )
            revision = self.repository.get_revision(item.revision_id)
            evidence = self.worktrees.prepare(
                revision=revision, queue_item_id=item.id
            )
            self.controller.record_worktree_prepared(
                evidence, actor="scheduler", changed_at=self.clock()
            )
            try:
                prior_receipt_source = self._typed_prior_receipt_source(item)
                plan = build_execution_plan(
                    snapshot=item.snapshot,
                    revision=revision,
                    worktree=evidence.worktree,
                    ambient_environment=self.ambient_environment,
                    assigned_gpu=gpu.uuid,
                )
                prepared = prepare_structured_attempt(
                    state_directory=self.store.state_dir,
                    queue_item_id=item.id,
                    experiment_id=item.experiment_id,
                    attempt=item.attempt,
                    segment=item.segment,
                    revision=revision,
                    snapshot=item.snapshot,
                    execution_plan=plan,
                    worktree_evidence=evidence,
                    gpu_uuid=gpu.uuid,
                    gpu_index=gpu.index,
                    prior_yield_receipt_source=prior_receipt_source,
                )
            except BaseException:
                try:
                    self.worktrees.cleanup(
                        revision=revision, recorded_evidence=evidence
                    )
                    self.controller.record_worktree_cleanup(
                        evidence, actor="scheduler", changed_at=self.clock()
                    )
                except (ProjectWorktreeError, V5SchedulerError) as cleanup_error:
                    try:
                        self.controller.record_worktree_cleanup(
                            evidence,
                            actor="scheduler",
                            changed_at=self.clock(),
                            error=str(cleanup_error),
                        )
                    except V5SchedulerError:
                        pass
                raise
            return _PreparedDispatch(
                prepared=prepared,
                candidate=candidate,
                revision=revision,
                worktree_evidence=evidence,
                execution_plan=plan,
            )
        legacy = self._legacy_context(item.id)
        self.controller.record_legacy_worktree_adopted(
            item.id,
            git_ref=legacy.git_ref,
            worktree_path=legacy.worktree_path,
            actor="scheduler",
            changed_at=self.clock(),
        )
        prepared = prepare_legacy_attempt(
            state_directory=self.store.state_dir,
            queue_item_id=item.id,
            project_id=item.project_id,
            project_key=legacy.project_key,
            project_revision_id=item.revision_id,
            project_revision=legacy.revision_label,
            experiment_id=item.experiment_id,
            attempt=item.attempt,
            segment=item.segment,
            git_commit=item.git_commit,
            execution_root=legacy.execution_root,
            primary_checkout=legacy.primary_checkout,
            command_text=legacy.command_text,
            ambient_environment=self._legacy_child_environment(),
            gpu_uuid=gpu.uuid,
            gpu_index=gpu.index,
            preemptible=item.preemptible,
            continuation_run_directory=legacy.continuation_run_directory,
            continuation_checkpoint=legacy.continuation_checkpoint,
            continuation_wandb_id=legacy.continuation_wandb_id,
        )
        return _PreparedDispatch(
            prepared=prepared,
            candidate=candidate,
            legacy_context=legacy,
        )

    def _typed_prior_receipt_source(self, item: V5QueueItem) -> bytes | None:
        """Bind a resumed typed segment to its exact authenticated prior receipt."""

        if item.admission_kind != "ExperimentCard/v1":
            raise V5SchedulerServiceError(
                f"queue item {item.id} is not a typed admission"
            )
        if item.segment == 1:
            return None
        record = self.repository.get_ready_yield_receipt_for_segment(
            item.id,
            completed_segment=item.segment - 1,
        )
        if (record.project_id, record.revision_id) != (
            item.project_id,
            item.revision_id,
        ):
            raise V5SchedulerServiceError(
                f"queue item {item.id} prior continuation receipt changed "
                "ProjectRevision ownership"
            )
        return record.source

    def _cleanup_dispatch(
        self,
        context: _PreparedDispatch,
        *,
        terminal: bool = False,
    ) -> None:
        if context.legacy_context is not None:
            self._cleanup_legacy_worktree(context.legacy_context)
            return
        if context.revision is None or context.worktree_evidence is None:
            return
        self._cleanup_structured_worktree(
            revision=context.revision,
            evidence=context.worktree_evidence,
        )

    def _cleanup_legacy_worktree(self, legacy: _LegacyContext) -> None:
        """Remove only a clean, exact v5-owned runtime; preserve v4 history."""

        try:
            with self.store.connect() as connection:
                row = connection.execute(
                    """
                    SELECT runtime_git_ref, runtime_worktree_path,
                           runtime_worktree_removed_at
                    FROM queue_items WHERE id = ?
                    """,
                    (legacy.item.id,),
                ).fetchone()
            if row is None or (
                row["runtime_git_ref"],
                row["runtime_worktree_path"],
            ) != (legacy.git_ref, str(legacy.worktree_path)):
                raise V5SchedulerServiceError(
                    f"legacy item {legacy.item.id} persisted runtime identity "
                    "changed before cleanup"
                )
            if row["runtime_worktree_removed_at"] is not None:
                return
            current_ref = self._legacy_read_ref(
                legacy.primary_checkout,
                legacy.git_ref,
            )
            target_exists = os.path.lexists(legacy.worktree_path)
            registered = self._legacy_registered_worktree(
                legacy.primary_checkout,
                legacy.worktree_path,
            )
            if target_exists:
                if current_ref != legacy.item.git_commit:
                    raise V5SchedulerServiceError(
                        f"legacy runtime worktree {legacy.worktree_path} exists but "
                        "its exact destination-owned ref is missing or changed"
                    )
                self._verify_legacy_worktree(
                    checkout=legacy.primary_checkout,
                    worktree=legacy.worktree_path,
                    git_ref=legacy.git_ref,
                    commit=legacy.item.git_commit,
                    require_clean=True,
                )
                removed = self._legacy_worktree_git_completed(
                    legacy.primary_checkout,
                    "worktree",
                    "remove",
                    str(legacy.worktree_path),
                )
                if removed.returncode != 0:
                    detail = (
                        removed.stderr.strip()
                        or removed.stdout.strip()
                        or "no detail"
                    )[:4096]
                    raise V5SchedulerServiceError(
                        f"could not remove clean legacy runtime worktree "
                        f"{legacy.worktree_path}: {detail}"
                    )
                if os.path.lexists(legacy.worktree_path) or self._legacy_registered_worktree(
                    legacy.primary_checkout,
                    legacy.worktree_path,
                ):
                    raise V5SchedulerServiceError(
                        f"legacy runtime worktree {legacy.worktree_path} remains after "
                        "Git reported successful removal; preserving its ref"
                    )
            elif registered:
                raise V5SchedulerServiceError(
                    f"legacy runtime path {legacy.worktree_path} is missing while Git "
                    "still registers it; explicit repair is required"
                )
            if current_ref is not None:
                if current_ref != legacy.item.git_commit:
                    raise V5SchedulerServiceError(
                        f"legacy runtime ref {legacy.git_ref!r} changed before cleanup"
                    )
                deleted = _git_completed(
                    legacy.primary_checkout,
                    "update-ref",
                    "-d",
                    legacy.git_ref,
                    legacy.item.git_commit,
                )
                if deleted.returncode != 0:
                    detail = (
                        deleted.stderr.strip()
                        or deleted.stdout.strip()
                        or "no detail"
                    )[:4096]
                    raise V5SchedulerServiceError(
                        f"could not delete exact legacy runtime ref "
                        f"{legacy.git_ref!r}: {detail}"
                    )
            self.controller.record_legacy_worktree_cleanup(
                legacy.item.id,
                git_ref=legacy.git_ref,
                worktree_path=legacy.worktree_path,
                actor="scheduler",
                changed_at=self.clock(),
            )
        except (V5SchedulerServiceError, V5SchedulerError) as exc:
            try:
                self.controller.record_legacy_worktree_cleanup(
                    legacy.item.id,
                    git_ref=legacy.git_ref,
                    worktree_path=legacy.worktree_path,
                    actor="scheduler",
                    changed_at=self.clock(),
                    error=str(exc),
                )
                self.controller.quarantine_project(
                    legacy.item.project_id,
                    reason=f"legacy worktree cleanup failed: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                    queue_item_id=legacy.item.id,
                )
            except V5SchedulerError:
                pass

    def _cleanup_structured_worktree(
        self,
        *,
        revision: ProjectRevision,
        evidence: ProjectWorktreeEvidence,
    ) -> None:
        """Remove one clean typed worktree/ref and record any preserved failure."""

        try:
            self.worktrees.cleanup(
                revision=revision,
                recorded_evidence=evidence,
            )
            self.controller.record_worktree_cleanup(
                evidence,
                actor="scheduler",
                changed_at=self.clock(),
            )
        except (ProjectWorktreeError, V5SchedulerError) as exc:
            try:
                self.controller.record_worktree_cleanup(
                    evidence,
                    actor="scheduler",
                    changed_at=self.clock(),
                    error=str(exc),
                )
                self.controller.quarantine_project(
                    evidence.project_id,
                    reason=f"worktree cleanup failed: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                    queue_item_id=evidence.queue_item_id,
                )
            except V5SchedulerError:
                pass

    def _dispatch_one(self, gpu: GpuSnapshot) -> bool:
        candidates = self.controller.list_dispatch_candidates(limit=1)
        if not candidates:
            return False
        candidate = candidates[0]
        checks = self.controller.enforce_disk_capacity(
            candidate.project_id,
            revision_id=candidate.revision_id,
            minimum_gib=self.min_free_disk_gib,
            actor="scheduler",
            changed_at=self.clock(),
        )
        if any(not check.sufficient for check in checks):
            return False
        if not self._global_gpu_lock(gpu.uuid):
            return False
        context: _PreparedDispatch | None = None
        launched: LaunchedAttempt | None = None
        claimed = False
        handed_off = False
        try:
            try:
                refreshed_snapshots = self._validated_gpu_snapshots(
                    self.gpu_provider()
                )
            except Exception as exc:
                self.controller.pause_host(
                    reason=f"GPU telemetry failed before dispatch claim: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                )
                self._release_gpu_lock(gpu.uuid)
                return False
            refreshed = {
                item.uuid: item for item in refreshed_snapshots
            }.get(gpu.uuid)
            if refreshed is None or not self._idle(refreshed):
                self._release_gpu_lock(gpu.uuid)
                return False
            try:
                context = self._prepare_dispatch(candidate, refreshed)
            except (
                AttemptRuntimeError,
                ExecutionValidationError,
                LegacyCardError,
                ProjectWorktreeError,
                V5RepositoryError,
                V5SchedulerError,
                V5SchedulerServiceError,
            ) as exc:
                self.controller.quarantine_project(
                    candidate.project_id,
                    reason=f"dispatch preparation failed: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                    queue_item_id=candidate.id,
                )
                self._release_gpu_lock(gpu.uuid)
                return False
            claim = self.controller.claim(
                candidate.id,
                gpu_uuid=refreshed.uuid,
                gpu_index=refreshed.index,
                actor="scheduler",
                changed_at=self.clock(),
            )
            if claim is None:
                self._cleanup_dispatch(context)
                self._release_gpu_lock(gpu.uuid)
                return False
            claimed = True
            try:
                launched = launch_prepared_attempt(context.prepared)
                launch_state = self.controller.record_launched(
                    candidate.id,
                    segment=candidate.segment,
                    gpu_uuid=refreshed.uuid,
                    pid=launched.pid,
                    pgid=launched.pgid,
                    process_start_ticks=launched.process_start_ticks,
                    actor="scheduler",
                    started_at=self.clock(),
                )
            except AttemptLaunchUncertainError as exc:
                # No terminal transition, worktree cleanup, or GPU release is
                # safe until an operator proves the named attempt is absent.
                self.controller.pause_host(
                    reason=f"durable executor launch is ambiguous: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                )
                return False
            except (AttemptRuntimeError, V5SchedulerError) as exc:
                if launched is not None:
                    try:
                        stop_launched_attempt(launched)
                    except AttemptLaunchUncertainError as uncertainty:
                        self.controller.pause_host(
                            reason=(
                                f"launch identity could not be recorded ({exc}); "
                                f"process-group teardown is ambiguous: {uncertainty}"
                            ),
                            actor="scheduler",
                            changed_at=self.clock(),
                        )
                        return False
                self.controller.fail_active_item(
                    candidate.id,
                    reason=f"durable executor launch failed: {exc}",
                    actor="scheduler",
                    finished_at=self.clock(),
                    return_code=127,
                )
                self.controller.pause_host(
                    reason=f"durable executor launch failed: {exc}",
                    actor="scheduler",
                    changed_at=self.clock(),
                )
                self._cleanup_dispatch(context, terminal=claimed)
                self._release_gpu_lock(gpu.uuid)
                return False
            self.processes[candidate.id] = launched
            self.dispatch_contexts[candidate.id] = context
            handed_off = True
            if launch_state in {"terminating", "force_killing"}:
                try:
                    active = next(
                        (
                            attempt
                            for attempt in self.controller.active_attempts()
                            if attempt.id == candidate.id
                        ),
                        None,
                    )
                    if active is None:
                        raise V5SchedulerServiceError(
                            f"queue item {candidate.id} lost its raced termination "
                            "state after launch identity was persisted"
                        )
                    self._deliver_termination_action(
                        self._termination_action_from_active(active),
                        actor="scheduler:launch",
                    )
                except (V5SchedulerError, V5SchedulerServiceError) as exc:
                    self.controller.pause_host(
                        reason=f"could not deliver raced termination: {exc}",
                        actor="scheduler",
                        changed_at=self.clock(),
                    )
            return True
        except BaseException as exc:
            # An unexpected exception must not leave a detached executor, an
            # active database row, or a host-global GPU lease behind. Expected
            # preparation/launch failures return above after their scoped
            # transitions, so this path is deliberately fail-closed.
            if launched is not None and not handed_off:
                try:
                    stop_launched_attempt(launched)
                except AttemptLaunchUncertainError as uncertainty:
                    self.controller.pause_host(
                        reason=(
                            f"unexpected dispatch failure ({exc}); process-group "
                            f"teardown is ambiguous: {uncertainty}"
                        ),
                        actor="scheduler",
                        changed_at=self.clock(),
                    )
                    raise V5SchedulerServiceError(
                        "unexpected dispatch failure left an assigned ambiguous "
                        "attempt under a paused host"
                    ) from exc
            if claimed and not handed_off:
                try:
                    self.controller.fail_active_item(
                        candidate.id,
                        reason=f"unexpected dispatch failure: {exc}",
                        actor="scheduler",
                        finished_at=self.clock(),
                    )
                    self.controller.pause_host(
                        reason=f"unexpected dispatch failure: {exc}",
                        actor="scheduler",
                        changed_at=self.clock(),
                    )
                except V5SchedulerError:
                    pass
            if context is not None and not handed_off:
                self._cleanup_dispatch(context, terminal=claimed)
            if not handed_off:
                self._release_gpu_lock(gpu.uuid)
            raise

    def _finish_local_process(self, item_id: int, launched: LaunchedAttempt) -> None:
        context = self.dispatch_contexts[item_id]
        idle_gpu: GpuSnapshot | None = None
        current_before = self.repository.get_queue_item(item_id)
        if current_before.state == "yielding":
            # A successful yield requeues and releases its lease in one CAS, so
            # unlike terminal finalization it needs idle proof first.
            try:
                idle_gpu = self._require_current_idle_gpu(
                    item_id=item_id,
                    gpu_uuid=launched.prepared.gpu_uuid,
                    actor="scheduler",
                )
            except V5SchedulerServiceError:
                return
        receipt_path = launched.prepared.paths.exit_receipt
        receipt_present = receipt_path.exists() or receipt_path.is_symlink()
        try:
            receipt = launched.prepared.read_exit_receipt()
            self._finalize_authenticated_receipt(
                receipt=receipt,
                context=context,
                actor="scheduler",
            )
        except (
            AttemptRuntimeError,
            ExecutionValidationError,
            ExecutorError,
            LegacyV0ContinuationError,
            V5ContinuationError,
            V5RepositoryError,
            V5SchedulerError,
            OSError,
        ) as exc:
            current = self.repository.get_queue_item(item_id)
            if current.state in {
                "succeeded",
                "failed",
                "interrupted",
                "force_killed",
            }:
                pass
            elif not receipt_present and current.state in {
                "terminating",
                "force_killing",
            }:
                raw_return_code = launched.process.returncode
                return_code = (
                    None
                    if raw_return_code is None
                    else raw_return_code
                    if raw_return_code >= 0
                    else 128 + abs(raw_return_code)
                )
                self.controller.record_termination_completion(
                    item_id,
                    actor="scheduler",
                    finished_at=self.clock(),
                    return_code=return_code,
                )
            else:
                self._isolate_terminal_evidence_failure(
                    item_id=item_id,
                    project_id=context.candidate.project_id,
                    reason=f"executor terminal evidence rejected: {exc}",
                    actor="scheduler",
                )
        if idle_gpu is None:
            # Terminal state is truthful immediately, but the separate lease
            # remains held until detached GPU work is proven absent.
            try:
                idle_gpu = self._require_current_idle_gpu(
                    item_id=item_id,
                    gpu_uuid=launched.prepared.gpu_uuid,
                    actor="scheduler",
                )
            except V5SchedulerServiceError:
                return
        self._release_finalized_gpu_lease(
            item_id,
            gpu=idle_gpu,
            actor="scheduler",
        )
        self._cleanup_dispatch(context, terminal=True)
        self._release_gpu_lock(launched.prepared.gpu_uuid)
        self.processes.pop(item_id, None)
        self.dispatch_contexts.pop(item_id, None)

    def _reconcile_local_processes(self) -> None:
        active_by_id = {
            active.id: active for active in self.controller.active_attempts()
        }
        for item_id, launched in tuple(self.processes.items()):
            if launched.process.poll() is None:
                active = active_by_id.get(item_id)
                if active is not None and active.state == "yielding":
                    self._reconcile_manual_yield_signal(active)
                    self._reconcile_live_legacy_yield(
                        active,
                        context=self.dispatch_contexts.get(item_id),
                    )
                if active is not None and active.state in {
                    "terminating",
                    "force_killing",
                }:
                    try:
                        self._reconcile_termination_action(
                            active,
                            actor="scheduler",
                        )
                    except (V5SchedulerError, V5SchedulerServiceError) as exc:
                        self.controller.pause_host(
                            reason=(
                                f"could not reconcile termination for queue item "
                                f"{item_id}: {exc}"
                            ),
                            actor="scheduler",
                            changed_at=self.clock(),
                        )
                continue
            self._finish_local_process(item_id, launched)

    def _reconcile_live_legacy_yield(
        self,
        active: V5ActiveAttempt,
        *,
        context: _PreparedDispatch | None = None,
    ) -> None:
        """Accept a v0 failed receipt while its imported process keeps running."""

        if active.state != "yielding":
            return
        item = self.repository.get_queue_item(active.id)
        if item.admission_kind != "LegacyMarkdownCard/v0":
            return
        try:
            recovered = context
            if recovered is None:
                recovered = self._legacy_context_from_active(
                    active,
                    # A cooperative child can publish its v0 receipt and exit
                    # between SIGINT delivery and this same reconciliation
                    # pass.  Reconstructing the already-recorded segment is
                    # safe; the terminal executor receipt is consumed by the
                    # normal authenticated finalizer on the next pass.
                    allow_existing_exit_receipt=True,
                )
            pending = self.legacy_continuations.recover_pending(recovered.prepared)
            self.legacy_continuations.reconcile_live_failure(
                pending,
                actor="scheduler",
                changed_at=self.clock(),
            )
        except (AttemptRuntimeError, LegacyV0ContinuationError) as exc:
            reason = f"legacy cooperative-yield evidence rejected: {exc}"
            try:
                self.request_termination(
                    active.id,
                    reason=reason,
                    actor="scheduler",
                    requested_at=self.clock(),
                )
                self.controller.quarantine_project(
                    active.project_id,
                    reason=reason,
                    actor="scheduler",
                    changed_at=self.clock(),
                    queue_item_id=active.id,
                )
            except (V5SchedulerError, V5SchedulerServiceError):
                self.controller.pause_host(
                    reason=(
                        f"could not safely terminate queue item {active.id} after "
                        f"legacy continuation rejection: {exc}"
                    ),
                    actor="scheduler",
                    changed_at=self.clock(),
                )

    def _reconcile_manual_yield_signal(
        self,
        active: V5ActiveAttempt,
    ) -> None:
        """Claim and attempt any durable unacknowledged manual-yield signal."""

        if active.state != "yielding":
            return
        try:
            if active.admission_kind == "ExperimentCard/v1":
                context = self._structured_context_from_active(
                    active,
                    allow_existing_exit_receipt=False,
                )
                pending = self.continuations.recover_pending(context.prepared)
                recovered_request_id = pending.request.request_id
            else:
                context = self._legacy_context_from_active(
                    active,
                    allow_existing_exit_receipt=False,
                )
                pending = self.legacy_continuations.recover_pending(context.prepared)
                recovered_request_id = pending.request_id
            if active.pid is None or active.pgid is None:
                raise V5SchedulerServiceError(
                    f"yielding queue item {active.id} lacks persisted process identity"
                )
            timestamp = self.clock()
            claim = self.controller.claim_manual_yield_signal_attempt(
                active.id,
                request_id=recovered_request_id,
                attempt_token=secrets.token_hex(16),
                signal_epoch=self.epoch_clock(),
                retry_after_seconds=self.manual_yield_signal_retry_seconds,
                actor="scheduler:recovery",
                changed_at=timestamp,
            )
            if claim is None:
                return
            try:
                delivered = signal_recorded_process(
                    pid=active.pid,
                    pgid=active.pgid,
                    process_start_ticks=active.process_start_ticks,
                    signum=signal.SIGINT,
                )
                detail = (
                    "authenticated SIGINT was delivered"
                    if delivered
                    else "authenticated SIGINT delivery returned false"
                )
            except (AttemptRuntimeError, OSError) as exc:
                delivered = False
                detail = f"authenticated SIGINT operation failed: {exc}"
            self.controller.record_manual_yield_signal_result(
                claim,
                delivered=delivered,
                detail=detail,
                result_epoch=self.epoch_clock(),
                actor="scheduler:recovery",
                changed_at=timestamp,
            )
            if not delivered:
                self.controller.quarantine_project(
                    active.project_id,
                    reason=(
                        f"manual-yield request {recovered_request_id!r} signal "
                        f"attempt {claim.attempt} is uncertain: {detail}"
                    ),
                    actor="scheduler:recovery",
                    changed_at=timestamp,
                    queue_item_id=active.id,
                )
        except (
            AttemptRuntimeError,
            ExecutionValidationError,
            LegacyV0ContinuationError,
            ProjectWorktreeError,
            V5ContinuationError,
            V5RepositoryError,
            V5SchedulerError,
            V5SchedulerServiceError,
            OSError,
        ) as exc:
            self.controller.pause_host(
                reason=(
                    f"could not safely reconcile manual-yield signal for queue "
                    f"item {active.id}: {exc}"
                ),
                actor="scheduler:recovery",
                changed_at=self.clock(),
            )

    def _attempt_paths(self, active: V5ActiveAttempt) -> AttemptPaths:
        return AttemptPaths.create(
            state_directory=self.store.state_dir,
            project_key=active.project_key,
            queue_item_id=active.id,
            segment=active.segment,
        )

    def _recorded_structured_worktree_evidence(
        self,
        *,
        item: V5QueueItem,
        revision: ProjectRevision,
    ) -> ProjectWorktreeEvidence:
        """Load persisted runtime identity before any typed Git/filesystem action.

        Immutable ProjectRevision data determines the only acceptable identity,
        but recovery must first rehydrate the identity actually committed in the
        queue database.  Passing recomputed expected values directly to the
        worktree manager would silently bypass detection of database ref/path
        substitution before cleanup or process recovery.
        """

        with self.store.connect() as connection:
            row = connection.execute(
                """
                SELECT runtime_git_ref, runtime_worktree_path,
                       runtime_worktree_created_at, runtime_worktree_removed_at
                FROM queue_items
                WHERE id = ? AND project_id = ? AND revision_id = ?
                """,
                (item.id, item.project_id, item.revision_id),
            ).fetchone()
        if row is None:
            raise V5SchedulerServiceError(
                f"typed queue item {item.id} disappeared while loading runtime "
                "worktree evidence"
            )
        git_ref = row["runtime_git_ref"]
        worktree_path = row["runtime_worktree_path"]
        if git_ref is None or worktree_path is None:
            raise V5SchedulerServiceError(
                f"typed queue item {item.id} lacks complete persisted runtime "
                "ref/worktree identity"
            )
        if row["runtime_worktree_created_at"] is None:
            raise V5SchedulerServiceError(
                f"typed queue item {item.id} has runtime ref/worktree identity "
                "without a creation timestamp"
            )
        if row["runtime_worktree_removed_at"] is not None:
            raise V5SchedulerServiceError(
                f"typed queue item {item.id} runtime worktree was already recorded "
                "as removed"
            )
        document = self.worktrees.expected_evidence(
            revision=revision,
            queue_item_id=item.id,
        ).to_document()
        document["gitRef"] = str(git_ref)
        document["worktree"] = str(worktree_path)
        try:
            return ProjectWorktreeEvidence.from_document(document)
        except ProjectWorktreeError as exc:
            raise V5SchedulerServiceError(
                f"typed queue item {item.id} persisted runtime worktree evidence "
                f"is invalid: {exc}"
            ) from exc

    def _structured_context_from_active(
        self,
        active: V5ActiveAttempt,
        *,
        allow_existing_exit_receipt: bool,
    ) -> _PreparedDispatch:
        """Rehydrate one exact typed attempt from database and worktree evidence."""

        item = self.repository.get_queue_item(active.id)
        if item.admission_kind != "ExperimentCard/v1" or item.snapshot is None:
            raise V5SchedulerServiceError(
                f"active item {active.id} is not a complete typed admission"
            )
        if active.assigned_gpu_uuid is None or active.assigned_gpu_index is None:
            raise V5SchedulerServiceError(
                f"typed active item {active.id} lacks assigned GPU identity"
            )
        revision = self.repository.get_revision(item.revision_id)
        evidence = self._recorded_structured_worktree_evidence(
            item=item,
            revision=revision,
        )
        self.worktrees.recover(
            revision=revision,
            queue_item_id=item.id,
            recorded_evidence=evidence,
        )
        plan = build_execution_plan(
            snapshot=item.snapshot,
            revision=revision,
            worktree=evidence.worktree,
            ambient_environment=self.ambient_environment,
            assigned_gpu=active.assigned_gpu_uuid,
        )
        prepared = prepare_structured_attempt(
            state_directory=self.store.state_dir,
            queue_item_id=item.id,
            experiment_id=item.experiment_id,
            attempt=item.attempt,
            segment=item.segment,
            revision=revision,
            snapshot=item.snapshot,
            execution_plan=plan,
            worktree_evidence=evidence,
            gpu_uuid=active.assigned_gpu_uuid,
            gpu_index=active.assigned_gpu_index,
            allow_existing_exit_receipt=allow_existing_exit_receipt,
            prior_yield_receipt_source=self._typed_prior_receipt_source(item),
        )
        candidate = V5DispatchCandidate(
            id=item.id,
            project_id=item.project_id,
            project_key=active.project_key,
            revision_id=item.revision_id,
            revision_label=revision.label,
            revision_kind="typed-v1",
            admission_kind=item.admission_kind,
            snapshot_id=item.snapshot_id,
            experiment_id=item.experiment_id,
            attempt=item.attempt,
            priority=item.priority,
            resume_front=item.resume_front,
            segment=item.segment,
            git_commit=item.git_commit,
        )
        return _PreparedDispatch(
            prepared=prepared,
            candidate=candidate,
            revision=revision,
            worktree_evidence=evidence,
            execution_plan=plan,
        )

    def _legacy_context_from_active(
        self,
        active: V5ActiveAttempt,
        *,
        allow_existing_exit_receipt: bool,
    ) -> _PreparedDispatch:
        """Rehydrate one exact imported legacy attempt for control/recovery."""

        item = self.repository.get_queue_item(active.id)
        if item.admission_kind != "LegacyMarkdownCard/v0" or item.snapshot is not None:
            raise V5SchedulerServiceError(
                f"active item {active.id} is not an imported legacy admission"
            )
        if active.assigned_gpu_uuid is None or active.assigned_gpu_index is None:
            raise V5SchedulerServiceError(
                f"legacy active item {active.id} lacks assigned GPU identity"
            )
        legacy = self._legacy_context(
            item.id,
            allow_active_dirty=True,
            prepare_runtime=False,
        )
        prepared = prepare_legacy_attempt(
            state_directory=self.store.state_dir,
            queue_item_id=item.id,
            project_id=item.project_id,
            project_key=legacy.project_key,
            project_revision_id=item.revision_id,
            project_revision=legacy.revision_label,
            experiment_id=item.experiment_id,
            attempt=item.attempt,
            segment=item.segment,
            git_commit=item.git_commit,
            execution_root=legacy.execution_root,
            primary_checkout=legacy.primary_checkout,
            command_text=legacy.command_text,
            ambient_environment=self._legacy_child_environment(),
            gpu_uuid=active.assigned_gpu_uuid,
            gpu_index=active.assigned_gpu_index,
            preemptible=item.preemptible,
            continuation_run_directory=legacy.continuation_run_directory,
            continuation_checkpoint=legacy.continuation_checkpoint,
            continuation_wandb_id=legacy.continuation_wandb_id,
            allow_existing_exit_receipt=allow_existing_exit_receipt,
        )
        return _PreparedDispatch(
            prepared=prepared,
            candidate=V5DispatchCandidate(
                id=item.id,
                project_id=item.project_id,
                project_key=legacy.project_key,
                revision_id=item.revision_id,
                revision_label=legacy.revision_label,
                revision_kind="legacy-v4",
                admission_kind=item.admission_kind,
                snapshot_id=item.snapshot_id,
                experiment_id=item.experiment_id,
                attempt=item.attempt,
                priority=item.priority,
                resume_front=item.resume_front,
                segment=item.segment,
                git_commit=item.git_commit,
            ),
            legacy_context=legacy,
        )

    @staticmethod
    def _termination_action_from_active(
        active: V5ActiveAttempt,
    ) -> V5TerminationAction:
        """Validate one active row before selecting a process-group signal."""

        if active.state not in {"terminating", "force_killing"}:
            raise V5SchedulerServiceError(
                f"queue item {active.id} is {active.state!r}, not terminating"
            )
        if (
            active.terminate_requested_at is None
            or active.terminate_reason is None
            or active.termination_stage is None
            or active.termination_signal_epoch is None
        ):
            raise V5SchedulerServiceError(
                f"queue item {active.id} has incomplete persisted termination "
                "evidence; refuse to guess a signal"
            )
        if active.termination_stage not in {"interrupt", "terminate", "kill"}:
            raise V5SchedulerServiceError(
                f"queue item {active.id} has unsupported termination stage "
                f"{active.termination_stage!r}"
            )
        if active.state == "force_killing" and active.termination_stage != "kill":
            raise V5SchedulerServiceError(
                f"queue item {active.id} force-killing state does not carry kill stage"
            )
        if active.state == "terminating" and active.termination_stage == "kill":
            raise V5SchedulerServiceError(
                f"queue item {active.id} terminating state cannot carry kill stage"
            )
        if (active.pid is None) != (active.pgid is None):
            raise V5SchedulerServiceError(
                f"queue item {active.id} has incomplete PID/process-group evidence"
            )
        return V5TerminationAction(
            item_id=active.id,
            project_id=active.project_id,
            segment=active.segment,
            state=active.state,
            stage=active.termination_stage,
            requested_at=active.terminate_requested_at,
            reason=active.terminate_reason,
            signal_epoch=active.termination_signal_epoch,
            pid=active.pid,
            pgid=active.pgid,
            process_start_ticks=active.process_start_ticks,
        )

    @staticmethod
    def _termination_signal(action: V5TerminationAction) -> signal.Signals:
        """Map a validated durable stage to its only authorized POSIX signal."""

        return {
            "interrupt": signal.SIGINT,
            "terminate": signal.SIGTERM,
            "kill": signal.SIGKILL,
        }[action.stage]

    @staticmethod
    def _termination_replay_key(
        action: V5TerminationAction,
    ) -> tuple[int, int, str, int | None, int | None, str | None, float]:
        return (
            action.item_id,
            action.segment,
            action.stage,
            action.pid,
            action.pgid,
            action.process_start_ticks,
            action.signal_epoch,
        )

    def _deliver_termination_action(
        self,
        action: V5TerminationAction,
        *,
        actor: str,
    ) -> bool:
        """Authenticate and signal one committed stage, then append its audit."""

        signum = self._termination_signal(action)
        delivered = False
        signal_error: AttemptRuntimeError | None = None
        if action.pid is not None and action.pgid is not None:
            try:
                delivered = signal_recorded_process(
                    pid=action.pid,
                    pgid=action.pgid,
                    process_start_ticks=action.process_start_ticks,
                    signum=signum,
                )
            except AttemptRuntimeError as exc:
                signal_error = exc
        self.controller.record_termination_signal_attempt(
            action,
            signal_name=signum.name,
            delivered=delivered,
            actor=actor,
            attempted_at=self.clock(),
        )
        self._replayed_termination_signals.add(
            self._termination_replay_key(action)
        )
        if signal_error is not None:
            raise V5SchedulerServiceError(
                f"termination for queue item {action.item_id} is persisted, but "
                f"{signum.name} could not be delivered: {signal_error}"
            ) from signal_error
        return delivered

    def _reconcile_termination_action(
        self,
        active: V5ActiveAttempt,
        *,
        actor: str,
    ) -> None:
        """Replay or advance one requested signal using only durable deadlines."""

        action = self._termination_action_from_active(active)
        now = _finite_float(
            self.epoch_clock(), field_name="epoch_clock result", minimum=0.0
        )
        key = self._termination_replay_key(action)
        if key not in self._replayed_termination_signals:
            # A fresh scheduler cannot prove that its predecessor reached the
            # post-commit signal call. Replay the persisted stage once before
            # considering its deadline; the next control pass may escalate
            # without resetting the durable clock.
            self._deliver_termination_action(action, actor=actor)
            return
        if (
            action.stage in {"interrupt", "terminate"}
            and now - action.signal_epoch >= self.termination_grace_seconds
        ):
            escalated = self.controller.escalate_termination(
                action.item_id,
                expected_stage=action.stage,
                expected_signal_epoch=action.signal_epoch,
                actor=actor,
                changed_at=self.clock(),
                signal_epoch=now,
            )
            if escalated is None:
                return
            action = escalated
            key = self._termination_replay_key(action)
        if key not in self._replayed_termination_signals:
            self._deliver_termination_action(action, actor=actor)

    def request_termination(
        self,
        item_id: int,
        *,
        reason: str,
        actor: str,
        force: bool = False,
        requested_at: str | None = None,
    ) -> V5TerminationOutcome:
        """Persist then signal graceful termination or an explicit force kill.

        This entry point is safe to call from a short-lived CLI/web process; it
        relies only on database process evidence and the same authenticated
        process-group primitive used by scheduler recovery.
        """

        item_key = _positive_integer(item_id, field_name="item_id")
        action = self.controller.request_termination(
            item_key,
            reason=reason,
            force=force,
            actor=actor,
            requested_at=self.clock() if requested_at is None else requested_at,
            signal_epoch=_finite_float(
                self.epoch_clock(), field_name="epoch_clock result", minimum=0.0
            ),
        )
        return V5TerminationOutcome(
            action=action,
            signal_delivered=self._deliver_termination_action(
                action,
                actor=actor,
            ),
        )

    @staticmethod
    def _named_process_group_exists(pgid: int) -> bool:
        """Probe only a sidecar-named group; never discover or guess PIDs."""

        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            raise V5SchedulerServiceError(
                f"could not inspect named process group {pgid}: {exc}"
            ) from exc
        return True

    def resolve_abandoned_launch(
        self,
        item_id: int,
        *,
        project_id: int,
        gpu_uuid: str,
        reason: str,
        actor: str,
        confirm: str,
        changed_at: str | None = None,
    ) -> V5AbandonedLaunchOutcome:
        """Resolve an operator-proven pre-launch or recorded-dead active attempt."""

        item_key = _positive_integer(item_id, field_name="item_id")
        project_key = _positive_integer(project_id, field_name="project_id")
        if confirm != "RESOLVE-ABANDONED-LAUNCH":
            raise V5SchedulerServiceError(
                "abandoned-launch resolution requires exact confirmation "
                "RESOLVE-ABANDONED-LAUNCH"
            )
        for value, label in (
            (gpu_uuid, "gpu_uuid"),
            (reason, "reason"),
            (actor, "actor"),
        ):
            if type(value) is not str or not value or value != value.strip():
                raise V5SchedulerServiceError(
                    f"{label} must be non-empty text without surrounding whitespace"
                )
        timestamp = self.clock() if changed_at is None else changed_at
        owns_scheduler_lock = self._scheduler_lock is None
        context: _PreparedDispatch | None = None
        launch_status = "absent"
        cleanup_authentication_error: str | None = None
        lease_released = False
        try:
            if owns_scheduler_lock:
                self._lock_scheduler()
            active = next(
                (
                    attempt
                    for attempt in self.controller.active_attempts()
                    if attempt.id == item_key
                ),
                None,
            )
            if active is None:
                raise V5SchedulerServiceError(
                    f"queue item {item_key} is not an active attempt"
                )
            if active.project_id != project_key:
                raise V5SchedulerServiceError(
                    f"queue item {item_key} belongs to Project id "
                    f"{active.project_id}, not authorized Project id {project_key}"
                )
            with self.store.connect() as connection:
                paused = connection.execute(
                    "SELECT value FROM metadata WHERE key = 'dispatch_paused'"
                ).fetchone()
            if paused is None or str(paused[0]) != "1":
                raise V5SchedulerServiceError(
                    "host dispatch must already be paused before resolving an "
                    "abandoned launch"
                )
            allowed_states = {
                "starting",
                "running",
                "yielding",
                "terminating",
                "force_killing",
            }
            if active.state not in allowed_states:
                raise V5SchedulerServiceError(
                    f"queue item {item_key} is {active.state!r}; abandoned-attempt "
                    f"resolution requires an active state in {sorted(allowed_states)!r}"
                )
            if active.assigned_gpu_uuid != gpu_uuid or active.assigned_gpu_index is None:
                raise V5SchedulerServiceError(
                    f"queue item {item_key} assigned GPU identity "
                    f"{(active.assigned_gpu_uuid, active.assigned_gpu_index)!r} "
                    f"does not match confirmed GPU {gpu_uuid!r}"
                )
            if (active.pid is None) != (active.pgid is None):
                raise V5SchedulerServiceError(
                    f"queue item {item_key} has incomplete persisted PID/process-"
                    "group evidence; resolution is forbidden"
                )
            recorded_process = active.pid is not None
            if not recorded_process and active.state != "starting":
                raise V5SchedulerServiceError(
                    f"queue item {item_key} has no process identity in state "
                    f"{active.state!r}; only a pre-launch 'starting' claim may be "
                    "resolved without recorded identity"
                )
            if recorded_process and active.state == "starting":
                raise V5SchedulerServiceError(
                    f"queue item {item_key} records process identity while still "
                    "'starting'; this inconsistent crash evidence requires repair, "
                    "not abandoned-attempt resolution"
                )
            if (
                recorded_process
                and Path("/proc").is_dir()
                and active.process_start_ticks is None
            ):
                raise V5SchedulerServiceError(
                    f"queue item {item_key} lacks the Linux process start-time "
                    "token required to prove a recorded process is dead"
                )
            if not self._global_gpu_lock(gpu_uuid):
                raise V5SchedulerServiceError(
                    f"GPU {gpu_uuid!r} is locked by another scheduler/process; "
                    "stop the writer and verify the GPU before resolution"
                )
            paths = self._attempt_paths(active)
            if os.path.lexists(paths.exit_receipt):
                raise V5SchedulerServiceError(
                    f"queue item {item_key} has terminal executor evidence at "
                    f"{paths.exit_receipt}; reconcile that receipt instead"
                )
            if recorded_process:
                assert active.pid is not None and active.pgid is not None
                if process_identity_matches(
                    pid=active.pid,
                    pgid=active.pgid,
                    process_start_ticks=active.process_start_ticks,
                ):
                    raise V5SchedulerServiceError(
                        f"queue item {item_key} records live authenticated executor "
                        f"PID {active.pid}; use termination, not abandoned-attempt "
                        "resolution"
                    )
                if self._named_process_group_exists(active.pgid):
                    raise V5SchedulerServiceError(
                        f"queue item {item_key} records extant process group "
                        f"{active.pgid}; resolution is forbidden"
                    )
            unbound_launch: ExecutorLaunchReceipt | None = None
            if os.path.lexists(paths.launch_receipt):
                launch_status = "rejected"
                try:
                    unbound_launch = ExecutorLaunchReceipt.inspect(
                        paths.launch_receipt
                    )
                except ExecutorError:
                    unbound_launch = None
                if unbound_launch is not None:
                    if process_identity_matches(
                        pid=unbound_launch.pid,
                        pgid=unbound_launch.pgid,
                        process_start_ticks=unbound_launch.process_start_ticks,
                    ):
                        raise V5SchedulerServiceError(
                            f"launch receipt names live authenticated executor PID "
                            f"{unbound_launch.pid}; use termination, not abandoned-"
                            "launch resolution"
                        )
                    if self._named_process_group_exists(unbound_launch.pgid):
                        raise V5SchedulerServiceError(
                            f"launch receipt names extant process group "
                            f"{unbound_launch.pgid}; resolution is forbidden"
                        )
            idle_gpu = self._require_current_idle_gpu(
                item_id=item_key,
                gpu_uuid=gpu_uuid,
                actor=actor,
            )
            try:
                context = (
                    self._structured_context_from_active(
                        active,
                        allow_existing_exit_receipt=False,
                    )
                    if active.admission_kind == "ExperimentCard/v1"
                    else self._legacy_context_from_active(
                        active,
                        allow_existing_exit_receipt=False,
                    )
                )
            except (
                AttemptRuntimeError,
                ExecutionValidationError,
                ProjectWorktreeError,
                V5RepositoryError,
                V5SchedulerError,
                V5SchedulerServiceError,
            ):
                context = None
            if unbound_launch is not None and context is not None:
                try:
                    context.prepared.read_launch_receipt(
                        pid=active.pid,
                        pgid=active.pgid,
                        process_start_ticks=active.process_start_ticks,
                    )
                except ExecutorError:
                    launch_status = "rejected"
                else:
                    launch_status = "valid-inactive"
            resolution = self.controller.resolve_abandoned_launch(
                item_key,
                project_id=project_key,
                gpu_uuid=gpu_uuid,
                pid=active.pid,
                pgid=active.pgid,
                process_start_ticks=active.process_start_ticks,
                reason=reason,
                actor=actor,
                changed_at=timestamp,
            )
            self._release_finalized_gpu_lease(
                item_key,
                gpu=idle_gpu,
                actor=actor,
            )
            lease_released = True
            if context is not None:
                self._cleanup_dispatch(context, terminal=True)
            else:
                try:
                    item = self.repository.get_queue_item(item_key)
                    if item.admission_kind == "ExperimentCard/v1":
                        revision = self.repository.get_revision(item.revision_id)
                        evidence = self._recorded_structured_worktree_evidence(
                            item=item,
                            revision=revision,
                        )
                        self._cleanup_structured_worktree(
                            revision=revision,
                            evidence=evidence,
                        )
                    else:
                        legacy = self._legacy_context(
                            item_key,
                            allow_active_dirty=True,
                            prepare_runtime=False,
                            verify_runtime=False,
                        )
                        self._cleanup_legacy_worktree(legacy)
                except (
                    ProjectWorktreeError,
                    V5RepositoryError,
                    V5SchedulerError,
                    V5SchedulerServiceError,
                ) as exc:
                    cleanup_authentication_error = str(exc)
                    self.controller.quarantine_project(
                        project_key,
                        reason=(
                            "abandoned-launch runtime cleanup could not be "
                            f"authenticated: {exc}"
                        ),
                        actor=actor,
                        changed_at=timestamp,
                        queue_item_id=item_key,
                    )
            with self.store.connect() as connection:
                cleanup_row = connection.execute(
                    "SELECT runtime_worktree_cleanup_error FROM queue_items "
                    "WHERE id = ? AND project_id = ?",
                    (item_key, project_key),
                ).fetchone()
            return V5AbandonedLaunchOutcome(
                resolution=resolution,
                launch_receipt_status=launch_status,
                worktree_cleanup_error=(
                    cleanup_authentication_error
                    if cleanup_row is None or cleanup_row[0] is None
                    else str(cleanup_row[0])
                ),
            )
        finally:
            if lease_released:
                self._release_gpu_lock(gpu_uuid)
            if owns_scheduler_lock and self._scheduler_lock is not None:
                self._scheduler_lock.close()  # type: ignore[attr-defined]
                self._scheduler_lock = None

    def request_manual_preemption(
        self,
        item_id: int,
        *,
        note: str,
        actor: str,
        requested_at: str | None = None,
    ) -> V5PendingContinuation | LegacyV0PendingContinuation:
        """Persist, publish, and signal one admitted manual preemption."""

        item_key = _positive_integer(item_id, field_name="item_id")
        active = next(
            (
                attempt
                for attempt in self.controller.active_attempts()
                if attempt.id == item_key
            ),
            None,
        )
        if active is None:
            raise V5SchedulerServiceError(
                f"queue item {item_key} is not an active attempt"
            )
        context = self.dispatch_contexts.get(item_key)
        if context is None:
            item = self.repository.get_queue_item(item_key)
            if item.admission_kind == "ExperimentCard/v1":
                context = self._structured_context_from_active(
                    active,
                    allow_existing_exit_receipt=False,
                )
            else:
                context = self._legacy_context_from_active(
                    active,
                    allow_existing_exit_receipt=False,
                )
        timestamp = self.clock() if requested_at is None else requested_at
        if context.prepared.admission_kind == "ExperimentCard/v1":
            return self.continuations.request_manual_yield(
                context.prepared,
                note=note,
                actor=actor,
                requested_at=timestamp,
            )
        return self.legacy_continuations.request_manual_yield(
            context.prepared,
            note=note,
            actor=actor,
            requested_at=timestamp,
        )

    def _finalize_authenticated_receipt(
        self,
        *,
        receipt: ExecutorReceipt,
        context: _PreparedDispatch | None,
        actor: str,
    ) -> None:
        item = self.repository.get_queue_item(receipt.queue_item_id)
        if item.state == "yielding":
            if context is None:
                raise V5SchedulerServiceError(
                    "yielding completion lacks recovered attempt context"
                )
            if item.admission_kind == "LegacyMarkdownCard/v0":
                pending = self.legacy_continuations.recover_pending(
                    context.prepared
                )
                self.legacy_continuations.finalize_manual_yield(
                    pending,
                    executor_return_code=receipt.return_code,
                    actor=actor,
                    changed_at=receipt.finished_at,
                )
                return
            try:
                pending = self.continuations.recover_pending(context.prepared)
                self.continuations.finalize_manual_yield(
                    pending,
                    actor=actor,
                    changed_at=receipt.finished_at,
                )
            except V5ContinuationError:
                current = self.repository.get_queue_item(item.id)
                if current.state == "yielding":
                    self.repository.isolate_continuation_failure(
                        item.id,
                        reason="manual continuation terminal evidence was rejected",
                        actor=actor,
                        changed_at=receipt.finished_at,
                        # The authenticated executor receipt proves this process
                        # ended. Record a truthful terminal state while the
                        # separate GPU lease remains held for telemetry release.
                        terminal=True,
                    )
                return
            return
        if context is not None and context.execution_plan is not None:
            observations = observe_execution_artifacts(
                context.execution_plan,
                require_required=receipt.return_code == 0,
            )
            self.controller.record_job_artifacts(
                item.id,
                segment=receipt.segment,
                observations=observations,
                actor=actor,
                recorded_at=receipt.finished_at,
            )
        self.controller.record_executor_completion(receipt, actor=actor)

    def _isolate_terminal_evidence_failure(
        self,
        *,
        item_id: int,
        project_id: int,
        reason: str,
        actor: str,
    ) -> None:
        current = self.repository.get_queue_item(item_id)
        if current.state in {
            "succeeded",
            "failed",
            "interrupted",
            "force_killed",
            "removed",
        }:
            # A valid terminal/continuation transition that committed first is
            # authoritative; stale recovery must never reopen its Project.
            return
        if current.state not in {
            "starting",
            "running",
            "yielding",
            "terminating",
            "force_killing",
        }:
            # In particular, a valid cooperative continuation may have already
            # requeued this same item at its next segment.
            return
        if current.state == "yielding":
            self.repository.isolate_continuation_failure(
                item_id,
                reason=reason,
                actor=actor,
                changed_at=self.clock(),
                # This path is reached only while finalizing an ended local or
                # recovered executor. Its GPU lease remains held separately.
                terminal=True,
            )
            return
        self.controller.fail_active_item(
            item_id,
            reason=reason,
            actor=actor,
            finished_at=self.clock(),
        )
        self.controller.quarantine_project(
            project_id,
            reason=reason,
            actor=actor,
            changed_at=self.clock(),
            queue_item_id=item_id,
        )

    def _read_recovered_receipt(
        self,
        active: V5ActiveAttempt,
    ) -> tuple[ExecutorReceipt, _PreparedDispatch | None]:
        paths = self._attempt_paths(active)
        item = self.repository.get_queue_item(active.id)
        if item.admission_kind == "ExperimentCard/v1":
            context = self._structured_context_from_active(
                active,
                allow_existing_exit_receipt=True,
            )
            return context.prepared.read_exit_receipt(), context
        context = self._legacy_context_from_active(
            active,
            allow_existing_exit_receipt=True,
        )
        return context.prepared.read_exit_receipt(), context

    def _pause_ambiguous_launch(
        self,
        active: V5ActiveAttempt,
        *,
        reason: str,
    ) -> None:
        """Keep an assigned GPU leased when executor identity is not conclusive."""

        if active.assigned_gpu_uuid is not None:
            self._global_gpu_lock(active.assigned_gpu_uuid)
        self.controller.pause_host(
            reason=f"cannot safely reconcile queue item {active.id} launch: {reason}",
            actor="scheduler:recovery",
            changed_at=self.clock(),
        )

    def _recover_durable_launch_identity(
        self,
        active: V5ActiveAttempt,
    ) -> V5ActiveAttempt | None:
        """Adopt an exact launch sidecar into an incomplete active database row."""

        paths = self._attempt_paths(active)
        if not os.path.lexists(paths.launch_receipt):
            self._pause_ambiguous_launch(
                active,
                reason=(
                    f"database process identity is incomplete and launch receipt "
                    f"{paths.launch_receipt} is absent"
                ),
            )
            return None
        try:
            if active.admission_kind == "ExperimentCard/v1":
                context = self._structured_context_from_active(
                    active,
                    allow_existing_exit_receipt=True,
                )
            else:
                context = self._legacy_context_from_active(
                    active,
                    allow_existing_exit_receipt=True,
                )
            receipt = context.prepared.read_launch_receipt()
            if receipt.pid != receipt.pgid:
                raise V5SchedulerServiceError(
                    f"executor launch receipt PID {receipt.pid} differs from "
                    f"process group {receipt.pgid}"
                )
            if Path("/proc").is_dir() and receipt.process_start_ticks is None:
                raise V5SchedulerServiceError(
                    "Linux executor launch receipt lacks process start-time token"
                )
            self.controller.record_launched(
                active.id,
                segment=active.segment,
                gpu_uuid=str(active.assigned_gpu_uuid),
                pid=receipt.pid,
                pgid=receipt.pgid,
                process_start_ticks=receipt.process_start_ticks,
                actor="scheduler:recovery",
                started_at=receipt.published_at,
            )
            refreshed = next(
                (
                    attempt
                    for attempt in self.controller.active_attempts()
                    if attempt.id == active.id
                ),
                None,
            )
            if refreshed is None:
                raise V5SchedulerServiceError(
                    "launch identity committed but active row disappeared"
                )
            return refreshed
        except (
            AttemptRuntimeError,
            ExecutionValidationError,
            ExecutorError,
            ProjectWorktreeError,
            V5RepositoryError,
            V5SchedulerError,
            V5SchedulerServiceError,
            OSError,
        ) as exc:
            self._pause_ambiguous_launch(active, reason=str(exc))
            return None

    def _reconcile_restarted_processes(self) -> None:
        for active in self.controller.active_attempts():
            if active.id in self.processes:
                continue
            if active.pid is None or active.pgid is None or (
                Path("/proc").is_dir()
                and active.process_start_ticks is None
            ):
                recovered = self._recover_durable_launch_identity(active)
                if recovered is None:
                    continue
                active = recovered
            paths = self._attempt_paths(active)
            if paths.exit_receipt.exists() or paths.exit_receipt.is_symlink():
                context: _PreparedDispatch | None = None
                if active.assigned_gpu_uuid is None:
                    self._pause_ambiguous_launch(
                        active,
                        reason="active row lacks assigned GPU identity for release",
                    )
                    continue
                idle_gpu: GpuSnapshot | None = None
                if active.state == "yielding":
                    try:
                        idle_gpu = self._require_current_idle_gpu(
                            item_id=active.id,
                            gpu_uuid=active.assigned_gpu_uuid,
                            actor="scheduler:recovery",
                        )
                    except V5SchedulerServiceError:
                        continue
                try:
                    receipt, context = self._read_recovered_receipt(active)
                    self._finalize_authenticated_receipt(
                        receipt=receipt,
                        context=context,
                        actor="scheduler:recovery",
                    )
                except (
                    AttemptRuntimeError,
                    ExecutionValidationError,
                    ExecutorError,
                    LegacyV0ContinuationError,
                    ProjectWorktreeError,
                    V5ContinuationError,
                    V5RepositoryError,
                    V5SchedulerError,
                    V5SchedulerServiceError,
                    OSError,
                ) as exc:
                    self._isolate_terminal_evidence_failure(
                        item_id=active.id,
                        project_id=active.project_id,
                        reason=f"recovered executor evidence rejected: {exc}",
                        actor="scheduler:recovery",
                    )
                if idle_gpu is None:
                    try:
                        idle_gpu = self._require_current_idle_gpu(
                            item_id=active.id,
                            gpu_uuid=active.assigned_gpu_uuid,
                            actor="scheduler:recovery",
                        )
                    except V5SchedulerServiceError:
                        continue
                self._release_finalized_gpu_lease(
                    active.id,
                    gpu=idle_gpu,
                    actor="scheduler:recovery",
                )
                if context is not None:
                    self._cleanup_dispatch(context, terminal=True)
                self._release_gpu_lock(active.assigned_gpu_uuid)
                continue
            alive = (
                active.pid is not None
                and active.pgid is not None
                and process_identity_matches(
                    pid=active.pid,
                    pgid=active.pgid,
                    process_start_ticks=active.process_start_ticks,
                )
            )
            if alive:
                if active.assigned_gpu_uuid is not None:
                    if not self._global_gpu_lock(active.assigned_gpu_uuid):
                        reason = (
                            f"could not reacquire host GPU lock for live queue "
                            f"item {active.id} on {active.assigned_gpu_uuid}; "
                            "another process may control the same GPU"
                        )
                        self.controller.pause_host(
                            reason=reason,
                            actor="scheduler:recovery",
                            changed_at=self.clock(),
                        )
                        self.controller.quarantine_project(
                            active.project_id,
                            reason=reason,
                            actor="scheduler:recovery",
                            changed_at=self.clock(),
                            queue_item_id=active.id,
                        )
                        continue
                if active.state == "yielding":
                    self._reconcile_manual_yield_signal(active)
                    self._reconcile_live_legacy_yield(active)
                if active.state in {"terminating", "force_killing"}:
                    try:
                        self._reconcile_termination_action(
                            active,
                            actor="scheduler:recovery",
                        )
                    except (V5SchedulerError, V5SchedulerServiceError) as exc:
                        self.controller.pause_host(
                            reason=(
                                "could not safely replay persisted termination for "
                                f"queue item {active.id}: {exc}"
                            ),
                            actor="scheduler:recovery",
                            changed_at=self.clock(),
                        )
                continue
            if active.state in {"terminating", "force_killing"}:
                self._pause_ambiguous_launch(
                    active,
                    reason=(
                        "authenticated executor leader is absent without a terminal "
                        "receipt; its process group may still contain project work"
                    ),
                )
                continue
            self._pause_ambiguous_launch(
                active,
                reason=(
                    "authenticated executor leader is absent without a terminal "
                    "receipt; its process group may still contain project work"
                ),
            )

    def _cleanup_persisted_runtime(self, row: Mapping[str, object]) -> None:
        """Clean one authenticated ended runtime without guessing its identity."""

        item_id = int(row["id"])
        try:
            if row["admission_kind"] == "ExperimentCard/v1":
                item = self.repository.get_queue_item(item_id)
                revision = self.repository.get_revision(item.revision_id)
                evidence = self._recorded_structured_worktree_evidence(
                    item=item,
                    revision=revision,
                )
                self._cleanup_structured_worktree(
                    revision=revision,
                    evidence=evidence,
                )
            else:
                legacy = self._legacy_context(
                    item_id,
                    allow_active_dirty=True,
                    prepare_runtime=False,
                    verify_runtime=False,
                )
                self._cleanup_legacy_worktree(legacy)
        except (
            ProjectWorktreeError,
            V5RepositoryError,
            V5SchedulerError,
            V5SchedulerServiceError,
        ) as exc:
            # Never guess at changed runtime identity. Preserve the evidence and
            # isolate only its Project after the GPU resource itself is safe.
            self.controller.quarantine_project(
                int(row["project_id"]),
                reason=f"orphaned worktree cleanup could not be recovered: {exc}",
                actor="scheduler:recovery",
                changed_at=self.clock(),
                queue_item_id=item_id,
            )

    def _reconcile_pending_gpu_lease_releases(self) -> None:
        """Release ended leases only after fresh exact idle GPU telemetry."""

        with self.store.connect() as connection:
            rows = list(
                connection.execute(
                    """
                    SELECT id, project_id, admission_kind, state,
                           assigned_gpu_uuid, assigned_gpu_index
                    FROM queue_items
                    WHERE runtime_gpu_lease_held = 1
                      AND state NOT IN (
                          'starting', 'running', 'yielding',
                          'terminating', 'force_killing'
                      )
                    ORDER BY id
                    """
                )
            )
        for row in rows:
            item_id = int(row["id"])
            gpu_uuid = str(row["assigned_gpu_uuid"])
            try:
                idle_gpu = self._require_current_idle_gpu(
                    item_id=item_id,
                    gpu_uuid=gpu_uuid,
                    actor="scheduler:recovery",
                )
                self._release_finalized_gpu_lease(
                    item_id,
                    gpu=idle_gpu,
                    actor="scheduler:recovery",
                )
            except (V5SchedulerError, V5SchedulerServiceError) as exc:
                if isinstance(exc, V5SchedulerError):
                    self.controller.pause_host(
                        reason=(
                            f"cannot commit queue item {item_id} GPU lease release "
                            f"after idle telemetry: {exc}"
                        ),
                        actor="scheduler:recovery",
                        changed_at=self.clock(),
                    )
                continue
            # The durable release event/CAS precedes both cleanup and lock close.
            self._cleanup_persisted_runtime(row)
            self._release_gpu_lock(gpu_uuid)

    def _reconcile_orphaned_worktree_cleanup(self) -> None:
        """Finish cleanup after a crash committed state before removing a worktree."""

        with self.store.connect() as connection:
            rows = list(
                connection.execute(
                    """
                    SELECT id, project_id, admission_kind
                    FROM queue_items
                    WHERE state NOT IN (
                        'starting', 'running', 'yielding',
                        'terminating', 'force_killing'
                    )
                      AND runtime_gpu_lease_held = 0
                      AND runtime_worktree_removed_at IS NULL
                      AND (
                          runtime_git_ref IS NOT NULL
                          OR runtime_worktree_path IS NOT NULL
                      )
                    ORDER BY id
                    """
                )
            )
        for row in rows:
            item_id = int(row["id"])
            try:
                if row["admission_kind"] == "ExperimentCard/v1":
                    item = self.repository.get_queue_item(item_id)
                    revision = self.repository.get_revision(item.revision_id)
                    evidence = self._recorded_structured_worktree_evidence(
                        item=item,
                        revision=revision,
                    )
                    self._cleanup_structured_worktree(
                        revision=revision,
                        evidence=evidence,
                    )
                else:
                    legacy = self._legacy_context(
                        item_id,
                        allow_active_dirty=True,
                        prepare_runtime=False,
                        verify_runtime=False,
                    )
                    self._cleanup_legacy_worktree(legacy)
            except (
                ProjectWorktreeError,
                V5RepositoryError,
                V5SchedulerError,
                V5SchedulerServiceError,
            ) as exc:
                # Never guess at a changed repository/worktree identity. Keep
                # the evidence in place and stop only this Project's dispatch.
                self.controller.quarantine_project(
                    int(row["project_id"]),
                    reason=f"orphaned worktree cleanup could not be recovered: {exc}",
                    actor="scheduler:recovery",
                    changed_at=self.clock(),
                    queue_item_id=item_id,
                )

    def run_iteration(
        self,
        *,
        force_gpu_poll: bool = False,
        allow_dispatch: bool = True,
    ) -> None:
        """Run reconciliation and, when authorized, one dispatch pass."""

        self._reconcile_local_processes()
        self._reconcile_restarted_processes()
        self._reconcile_pending_gpu_lease_releases()
        self._reconcile_orphaned_worktree_cleanup()
        self.reservations.reconcile(
            reconciled_at=self.clock(), actor="scheduler"
        )
        self.controller.reconcile_failed_dependencies(
            actor="scheduler", changed_at=self.clock()
        )
        if not allow_dispatch:
            return
        now = time.monotonic()
        if not force_gpu_poll and now - self._last_gpu_poll < self.poll_seconds:
            return
        self._last_gpu_poll = now
        if self.controller.host_dispatch_state()[0]:
            return
        for gpu in self._available_gpus():
            if not self._dispatch_one(gpu) and self.controller.host_dispatch_state()[0]:
                break

    def run(self, *, once: bool = False) -> None:
        """Hold the singleton scheduler lease and serve until interrupted."""

        self._lock_scheduler()
        previous_handlers: dict[int, object] = {}

        def stop_handler(_signum: int, _frame: object) -> None:
            self._stop = True

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, stop_handler)
        try:
            while not self._stop:
                self.run_iteration(
                    force_gpu_poll=once,
                    allow_dispatch=not once,
                )
                if once:
                    break
                time.sleep(self.control_seconds)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)  # type: ignore[arg-type]
            for lock in self.gpu_locks.values():
                lock.close()  # type: ignore[attr-defined]
            self.gpu_locks.clear()
            if self._scheduler_lock is not None:
                self._scheduler_lock.close()  # type: ignore[attr-defined]
                self._scheduler_lock = None


__all__ = [
    "V5AbandonedLaunchOutcome",
    "V5SchedulerService",
    "V5SchedulerServiceError",
    "V5TerminationOutcome",
    "utc_now_iso",
]
