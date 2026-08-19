"""Operate an explicit, durable experiment queue on an unmanaged GPU host.

The queue never discovers work from experiment cards or project status.  An
operator must explicitly add each experiment.  Added items snapshot the exact
manual command from the committed run card, and a foreground scheduler later
dispatches those commands through the existing experiment runner when an
operator-selected GPU is observed idle.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import getpass
import hashlib
import hmac
import json
import math
import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from experiment_queue.config import StateDirectoryError, resolve_state_dir
from experiment_queue.runner import collect_git_context


SCHEMA_VERSION = 4
WORKTREE_ROOT_NAME = "worktrees"
SHARED_WORKTREE_PATHS = (
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
ACTIVE_STATES = {
    "queued",
    "held",
    "blocked",
    "starting",
    "running",
    "yielding",
    "terminating",
    "force_killing",
}
PENDING_STATES = {"queued", "held", "blocked"}
RUNNING_STATES = {"starting", "running", "yielding", "terminating", "force_killing"}
PRIORITY_MUTABLE_STATES = PENDING_STATES | {"starting", "running", "yielding"}
TERMINAL_STATES = {"succeeded", "failed", "interrupted", "force_killed", "removed"}
SUCCESS_STATE = "succeeded"
CARD_COMMAND_HEADING = "## Exact Manual Command On Mutton2"
YIELD_EXIT_CODE = 75
YIELD_PROGRESS_UNIT_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,31}")
MIN_RESERVATION_HOURS = 1
MAX_RESERVATION_HOURS = 24
WORKTREE_CLEANUP_RETRY_SECONDS = 60

_DURABLE_EXECUTOR_SOURCE = r'''# Minimal immutable queue executor; standard library only.
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
signals_received = []
child = None

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def forward(signum, _frame):
    signals_received.append(signal.Signals(signum).name)
    if child is not None and child.poll() is None:
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass

previous = {}
for signum in (signal.SIGINT, signal.SIGTERM):
    previous[signum] = signal.signal(signum, forward)
started_at = now()
try:
    child = subprocess.Popen(
        ["/bin/bash", "-lc", payload["command"]],
        cwd=payload["cwd"],
    )
    raw_return_code = child.wait()
    return_code = 128 + abs(raw_return_code) if raw_return_code < 0 else raw_return_code
except OSError as exc:
    return_code = 127
    signals_received.append(f"launch_error:{exc}")
finally:
    for signum, handler in previous.items():
        signal.signal(signum, handler)
receipt = dict(payload["receipt"])
receipt.update({
    "started_at": started_at,
    "finished_at": now(),
    "return_code": return_code,
    "signals_received": signals_received,
})
receipt_path = Path(payload["receipt_path"])
receipt_path.parent.mkdir(parents=True, exist_ok=True)
temporary = receipt_path.with_name(f".{receipt_path.name}.{os.getpid()}.tmp")
temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.replace(temporary, receipt_path)
raise SystemExit(return_code)
'''


class QueueError(RuntimeError):
    """Raised when a queue operation cannot be completed safely."""


class ContinuationIntegrityError(QueueError):
    """Raised when a queued continuation no longer matches its sealed files."""


class _ClosingConnection(sqlite3.Connection):
    """Commit or roll back a context-managed connection, then always close it."""

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc, traceback))
        finally:
            self.close()


@dataclass(frozen=True)
class GpuSnapshot:
    """One physical GPU observation from ``nvidia-smi``."""

    index: str
    uuid: str
    name: str
    memory_total_mib: float
    memory_used_mib: float
    utilization_percent: float
    compute_pids: tuple[int, ...] = ()

    @property
    def free_memory_fraction(self) -> float:
        """Return the observed free-memory fraction, or zero for invalid totals."""

        if self.memory_total_mib <= 0:
            return 0.0
        return max(0.0, 1.0 - self.memory_used_mib / self.memory_total_mib)


@dataclass(frozen=True)
class CardCommand:
    """Frozen executable identity extracted from an immutable run card."""

    experiment_id: str
    card_path: Path
    card_sha256: str
    command_text: str
    runner_name: str


def utc_now_iso() -> str:
    """Return a compact UTC timestamp suitable for durable records."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _actor() -> str:
    """Identify the local operator without relying on external services."""

    return f"{getpass.getuser()}@{socket.gethostname()}"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON through an adjacent temporary file and atomic rename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _atomic_write_text(path: Path, value: str) -> None:
    """Write text through an adjacent temporary file and atomic rename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _parse_csv_number(value: str, *, field: str) -> float:
    cleaned = value.strip()
    if cleaned in {"", "N/A", "[N/A]"}:
        raise QueueError(f"nvidia-smi did not report {field}: {value!r}")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise QueueError(f"nvidia-smi returned invalid {field}: {value!r}") from exc


def _scheduler_float(value: Any, *, field: str) -> float:
    """Normalize one finite scheduler scalar with an actionable queue error."""

    if isinstance(value, bool):
        raise QueueError(f"{field} must be a finite number, got {value!r}")
    try:
        normalized = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise QueueError(f"{field} must be a finite number, got {value!r}") from exc
    if not math.isfinite(normalized):
        raise QueueError(f"{field} must be finite, got {value!r}")
    return normalized


def _validated_yield_progress(
    receipt: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize optional workflow progress without trusting display text."""

    if "progress" not in receipt:
        return None, None
    progress = receipt.get("progress")
    if not isinstance(progress, Mapping):
        return None, "yield receipt progress must be an object"
    unit = progress.get("unit")
    if not isinstance(unit, str) or not YIELD_PROGRESS_UNIT_PATTERN.fullmatch(unit):
        return None, (
            "yield receipt progress unit must be a 1-32 character ASCII token "
            "starting with a letter"
        )
    completed = progress.get("completed")
    if isinstance(completed, bool) or not isinstance(completed, int) or completed < 0:
        return None, "yield receipt progress completed must be an integer >= 0"
    normalized = {"unit": unit, "completed": completed}
    if "total" in progress:
        total = progress["total"]
        if isinstance(total, bool) or not isinstance(total, int) or total < completed:
            return None, "yield receipt progress total must be an integer >= completed"
        normalized["total"] = total
    return normalized, None


class QueueStore:
    """SQLite-backed queue state and append-only operational event log."""

    def __init__(self, state_dir: Path, repo_root: Path):
        self.state_dir = state_dir.resolve()
        self.repo_root = repo_root.resolve()
        self.database_path = self.state_dir / "queue.sqlite3"
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise QueueError(
                f"could not create scheduler state directory {self.state_dir}: {exc}. "
                "Create it as the scheduler user and verify that its parent is writable."
            ) from exc
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        """Open a connection that closes after a ``with`` block.

        ``sqlite3.Connection`` normally commits or rolls back on context exit
        without closing the database handle. The scheduler opens several
        short transactions per control pass, so its connection subclass must
        close deterministically to avoid exhausting the process file limit.
        """

        try:
            connection = sqlite3.connect(
                self.database_path,
                timeout=30.0,
                factory=_ClosingConnection,
            )
        except sqlite3.Error as exc:
            if not self.state_dir.exists():
                state_detail = "the state directory no longer exists"
            elif not self.state_dir.is_dir():
                state_detail = "the configured state path is not a directory"
            else:
                access = "/".join(
                    label
                    for label, allowed in (
                        ("readable", os.access(self.state_dir, os.R_OK)),
                        ("writable", os.access(self.state_dir, os.W_OK)),
                        ("searchable", os.access(self.state_dir, os.X_OK)),
                    )
                    if allowed
                ) or "no effective access"
                state_detail = f"effective directory access: {access}"
            raise QueueError(
                f"could not open scheduler database {self.database_path}: {exc}; "
                f"{state_detail}. Verify that gpu_scheduler_state remains present and "
                "writable by the scheduler user."
            ) from exc
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS queue_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    card_path TEXT NOT NULL,
                    card_sha256 TEXT NOT NULL,
                    command_text TEXT NOT NULL,
                    runner_name TEXT NOT NULL,
                    git_commit TEXT NOT NULL,
                    added_at TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    state_detail TEXT,
                    assigned_gpu_uuid TEXT,
                    assigned_gpu_index TEXT,
                    pid INTEGER,
                    pgid INTEGER,
                    proc_start_ticks TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    return_code INTEGER,
                    terminate_requested_at TEXT,
                    terminate_reason TEXT,
                    termination_stage TEXT,
                    termination_signal_epoch REAL,
                    contention_detected INTEGER NOT NULL DEFAULT 0,
                    repo_drift_detected INTEGER NOT NULL DEFAULT 0,
                    runner_run_dir TEXT,
                    runner_manifest_path TEXT,
                    rsync_pull_command TEXT,
                    preemptible INTEGER NOT NULL DEFAULT 0,
                    segment INTEGER NOT NULL DEFAULT 1,
                    resume_front INTEGER NOT NULL DEFAULT 0,
                    yield_requested_at TEXT,
                    yield_requested_by TEXT,
                    yield_request_id TEXT,
                    yield_note TEXT,
                    yield_duration_hours INTEGER,
                    continuation_checkpoint TEXT,
                    continuation_checkpoint_sha256 TEXT,
                    continuation_checkpoint_metadata TEXT,
                    continuation_checkpoint_metadata_sha256 TEXT,
                    continuation_step INTEGER,
                    continuation_wandb_id TEXT,
                    git_ref TEXT,
                    worktree_path TEXT,
                    worktree_created_at TEXT,
                    worktree_removed_at TEXT,
                    worktree_cleanup_error TEXT,
                    UNIQUE(experiment_id, attempt)
                );

                CREATE INDEX IF NOT EXISTS queue_items_state_order
                    ON queue_items(state, priority DESC, id ASC);

                CREATE TABLE IF NOT EXISTS dependencies (
                    queue_item_id INTEGER NOT NULL,
                    dependency_item_id INTEGER NOT NULL,
                    PRIMARY KEY(queue_item_id, dependency_item_id),
                    FOREIGN KEY(queue_item_id) REFERENCES queue_items(id),
                    FOREIGN KEY(dependency_item_id) REFERENCES queue_items(id)
                );

                CREATE TABLE IF NOT EXISTS gpu_allowlist (
                    uuid TEXT PRIMARY KEY,
                    requested_identifier TEXT NOT NULL,
                    last_index TEXT NOT NULL,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    queue_item_id INTEGER,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(queue_item_id) REFERENCES queue_items(id)
                );

                CREATE TABLE IF NOT EXISTS gpu_reservations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gpu_uuid TEXT NOT NULL,
                    queue_item_id INTEGER,
                    status TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    note TEXT NOT NULL,
                    duration_hours INTEGER NOT NULL,
                    starts_at TEXT,
                    expires_at TEXT,
                    released_at TEXT,
                    released_by TEXT,
                    state_detail TEXT,
                    FOREIGN KEY(queue_item_id) REFERENCES queue_items(id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_gpu_reservations_open_gpu
                    ON gpu_reservations(gpu_uuid)
                    WHERE status IN ('pending', 'active');

                CREATE INDEX IF NOT EXISTS idx_gpu_reservations_status_expiry
                    ON gpu_reservations(status, expires_at);
                """
            )
            existing_version = connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            if existing_version is None:
                self._set_meta(connection, "schema_version", str(SCHEMA_VERSION))
                self._set_meta(connection, "repo_root", str(self.repo_root))
                self._set_meta(connection, "dispatch_paused", "0")
                self._set_meta(connection, "pause_reason", "")
                self._set_meta(connection, "consecutive_failures", "0")
                self._event(connection, "QUEUE_INITIALIZED", payload={"repo_root": str(self.repo_root)})
            else:
                version = int(existing_version["value"])
                if version not in {1, 2, 3, SCHEMA_VERSION}:
                    raise QueueError(
                        f"queue schema {existing_version['value']} is not supported; "
                        f"expected 1, 2, 3, or {SCHEMA_VERSION}"
                    )
                recorded_root = self.get_meta("repo_root", connection=connection)
                if Path(recorded_root).resolve() != self.repo_root:
                    raise QueueError(
                        f"queue belongs to repository {recorded_root}, not {self.repo_root}"
                    )
                if version == 1:
                    self._migrate_v1_to_v2(connection)
                    version = 2
                if version == 2:
                    self._migrate_v2_to_v3(connection)
                    version = 3
                if version == 3:
                    self._migrate_v3_to_v4(connection)
            connection.execute("PRAGMA optimize")

    def _migrate_v1_to_v2(self, connection: sqlite3.Connection) -> None:
        """Add cooperative-yield state without discarding a live v1 queue."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(queue_items)")
        }
        additions = {
            "preemptible": "INTEGER NOT NULL DEFAULT 0",
            "segment": "INTEGER NOT NULL DEFAULT 1",
            "resume_front": "INTEGER NOT NULL DEFAULT 0",
            "yield_requested_at": "TEXT",
            "yield_requested_by": "TEXT",
            "yield_request_id": "TEXT",
            "yield_note": "TEXT",
            "yield_duration_hours": "INTEGER",
            "continuation_checkpoint": "TEXT",
            "continuation_checkpoint_sha256": "TEXT",
            "continuation_step": "INTEGER",
            "continuation_wandb_id": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE queue_items ADD COLUMN {name} {declaration}"
                )
        self._set_meta(connection, "schema_version", "2")
        self._event(connection, "QUEUE_SCHEMA_MIGRATED", payload={"from": 1, "to": 2})

    def _migrate_v2_to_v3(self, connection: sqlite3.Connection) -> None:
        """Pin pending commits and add isolated-worktree lifecycle fields."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(queue_items)")
        }
        additions = {
            "git_ref": "TEXT",
            "worktree_path": "TEXT",
            "worktree_created_at": "TEXT",
            "worktree_removed_at": "TEXT",
            "worktree_cleanup_error": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE queue_items ADD COLUMN {name} {declaration}"
                )
        if "git_commit" not in columns:
            item_count = int(connection.execute("SELECT COUNT(*) FROM queue_items").fetchone()[0])
            if item_count:
                raise QueueError(
                    "queue schema v2 has items but no git_commit column; restore the original "
                    "queue database before migrating"
                )
            active: list[sqlite3.Row] = []
        else:
            active = list(
                connection.execute(
                    "SELECT id, git_commit, state FROM queue_items WHERE state IN "
                    "('queued','held','blocked','starting','running','yielding','terminating','force_killing')"
                )
            )
        for item in active:
            item_id = int(item["id"])
            if item["state"] in RUNNING_STATES:
                detail = (
                    "legacy shared-checkout attempt created before isolated worktrees; "
                    "do not update the primary checkout until this process is terminal"
                )
                connection.execute(
                    "UPDATE queue_items SET state_detail = COALESCE(state_detail, ?) "
                    "WHERE id = ?",
                    (detail, item_id),
                )
                continue
            git_ref = _queue_git_ref(item_id)
            pinned = _git_completed(
                self.repo_root,
                "update-ref",
                git_ref,
                str(item["git_commit"]),
            )
            if pinned.returncode == 0:
                connection.execute(
                    "UPDATE queue_items SET git_ref = ? WHERE id = ?",
                    (git_ref, item_id),
                )
            else:
                detail = (
                    f"could not pin queued commit {item['git_commit']}: "
                    f"{pinned.stderr.strip() or pinned.stdout.strip()}"
                )
                connection.execute(
                    "UPDATE queue_items SET state = 'held', state_detail = ? WHERE id = ?",
                    (detail, item_id),
                )
        self._set_meta(connection, "schema_version", "3")
        self._event(
            connection,
            "QUEUE_SCHEMA_MIGRATED",
            payload={"from": 2, "to": 3},
        )

    def _migrate_v3_to_v4(self, connection: sqlite3.Connection) -> None:
        """Bind continuation metadata so accepted yields cannot rot before resume."""

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(queue_items)")
        }
        additions = {
            "continuation_checkpoint_metadata": "TEXT",
            "continuation_checkpoint_metadata_sha256": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE queue_items ADD COLUMN {name} {declaration}"
                )
        required_columns = {
            "id",
            "state",
            "state_detail",
            "segment",
            "runner_run_dir",
            "continuation_checkpoint",
            "continuation_checkpoint_sha256",
            *additions,
        }
        if required_columns.issubset(columns | set(additions)):
            candidates = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE segment > 1 "
                    "AND continuation_checkpoint IS NOT NULL "
                    "AND continuation_checkpoint_metadata IS NULL "
                    "AND state IN ('queued','held','blocked')"
                )
            )
            for item in candidates:
                item_id = int(item["id"])
                prior_segment = int(item["segment"]) - 1
                receipt_path = (
                    self.state_dir
                    / "attempts"
                    / str(item_id)
                    / "segments"
                    / str(prior_segment)
                    / "yield"
                    / "receipt.json"
                )
                try:
                    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    if not isinstance(receipt, dict):
                        raise QueueError("yield receipt is not a JSON object")
                    runner_value = item["runner_run_dir"]
                    if not runner_value:
                        raise QueueError("runner directory is missing")
                    runner = Path(str(runner_value)).resolve()
                    checkpoint_source = Path(str(receipt.get("checkpoint", "")))
                    metadata_source = Path(
                        str(receipt.get("checkpoint_metadata", ""))
                    )
                    if checkpoint_source.is_symlink() or metadata_source.is_symlink():
                        raise QueueError("yield continuation uses a symlink")
                    checkpoint = checkpoint_source.resolve()
                    metadata = metadata_source.resolve()
                    checkpoint.relative_to(runner)
                    metadata.relative_to(runner)
                    if (
                        not checkpoint.is_file()
                        or not metadata.is_file()
                        or checkpoint
                        != Path(str(item["continuation_checkpoint"])).resolve()
                        or not hmac.compare_digest(
                            _sha256_file(checkpoint),
                            str(item["continuation_checkpoint_sha256"]),
                        )
                    ):
                        raise QueueError("yield continuation files changed")
                except (OSError, ValueError, json.JSONDecodeError, QueueError) as exc:
                    detail = (
                        "queue schema v4 could not bind the legacy continuation "
                        f"metadata; inspect segment {prior_segment} yield evidence: "
                        f"{exc}"
                    )
                    connection.execute(
                        "UPDATE queue_items SET state = 'held', state_detail = ? "
                        "WHERE id = ?",
                        (detail, item_id),
                    )
                    self._event(
                        connection,
                        "QUEUE_CONTINUATION_MIGRATION_HELD",
                        queue_item_id=item_id,
                        payload={"detail": detail},
                    )
                    continue
                connection.execute(
                    "UPDATE queue_items SET continuation_checkpoint_metadata = ?, "
                    "continuation_checkpoint_metadata_sha256 = ? WHERE id = ?",
                    (str(metadata), _sha256_file(metadata), item_id),
                )
                self._event(
                    connection,
                    "QUEUE_CONTINUATION_METADATA_BOUND",
                    queue_item_id=item_id,
                    payload={
                        "segment": int(item["segment"]),
                        "checkpoint_metadata": str(metadata),
                        "checkpoint_metadata_sha256": _sha256_file(metadata),
                    },
                )
        self._set_meta(connection, "schema_version", str(SCHEMA_VERSION))
        self._event(
            connection,
            "QUEUE_SCHEMA_MIGRATED",
            payload={"from": 3, "to": SCHEMA_VERSION},
        )

    @staticmethod
    def _set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def set_meta(self, key: str, value: str) -> None:
        with self.connect() as connection:
            self._set_meta(connection, key, value)

    def get_meta(self, key: str, *, connection: sqlite3.Connection | None = None) -> str:
        owns_connection = connection is None
        active = connection or self.connect()
        try:
            row = active.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
            if row is None:
                raise QueueError(f"queue metadata is missing required key {key!r}")
            return str(row["value"])
        finally:
            if owns_connection:
                active.close()

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        event_type: str,
        *,
        queue_item_id: int | None = None,
        payload: dict[str, Any] | None = None,
        actor: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO events(created_at, actor, event_type, queue_item_id, payload_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                utc_now_iso(),
                actor or _actor(),
                event_type,
                queue_item_id,
                json.dumps(payload or {}, sort_keys=True),
            ),
        )

    def event(
        self,
        event_type: str,
        *,
        queue_item_id: int | None = None,
        payload: dict[str, Any] | None = None,
        actor: str | None = None,
    ) -> None:
        with self.connect() as connection:
            self._event(
                connection,
                event_type,
                queue_item_id=queue_item_id,
                payload=payload,
                actor=actor,
            )

    def item(self, item_id: int, *, connection: sqlite3.Connection | None = None) -> sqlite3.Row:
        owns_connection = connection is None
        active = connection or self.connect()
        try:
            row = active.execute("SELECT * FROM queue_items WHERE id = ?", (item_id,)).fetchone()
            if row is None:
                raise QueueError(f"queue item {item_id} does not exist")
            return row
        finally:
            if owns_connection:
                active.close()

    def list_items(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(connection.execute("SELECT * FROM queue_items ORDER BY id"))


def _queue_git_ref(item_id: int) -> str:
    """Return the private Git ref that keeps one queued commit reachable."""

    if item_id < 1:
        raise QueueError(f"queue item ID must be positive, got {item_id}")
    return f"refs/experiment-queue/items/{item_id}"


def _item_value(item: Mapping[str, Any], key: str) -> Any:
    """Read a field from either a dictionary or ``sqlite3.Row``."""

    try:
        return item[key]
    except (KeyError, IndexError):
        return None


def _expected_worktree_path(store: QueueStore, item: Mapping[str, Any]) -> Path:
    """Return and validate the scheduler-owned worktree path for one item."""

    commit = str(item["git_commit"])
    if not re.fullmatch(r"[0-9a-fA-F]{7,64}", commit):
        raise QueueError(f"queue item {item['id']} has invalid Git commit {commit!r}")
    root = (store.state_dir / WORKTREE_ROOT_NAME).resolve()
    path = (root / f"item-{int(item['id'])}-{commit[:12].lower()}").resolve()
    if path.parent != root:
        raise QueueError(f"unsafe queue worktree path resolved outside {root}: {path}")
    return path


def _git_completed(repo_root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
        check=False,
    )


def _git_error(operation: str, result: subprocess.CompletedProcess[str]) -> QueueError:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return QueueError(f"Git could not {operation}: {detail}")


def _worktree_excludes_file(store: QueueStore) -> Path:
    """Create the scheduler-only excludes needed for shared root symlinks."""

    path = store.state_dir / "worktree_shared_paths.exclude"
    content = "# Scheduler-managed shared paths in detached experiment worktrees.\n" + "".join(
        f"/{name}\n" for name in SHARED_WORKTREE_PATHS
    )
    try:
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current != content:
            _atomic_write_text(path, content)
    except OSError as exc:
        raise QueueError(f"could not prepare worktree excludes file {path}: {exc}") from exc
    return path


def _environment_with_git_excludes(
    environment: Mapping[str, str],
    excludes_file: Path,
) -> dict[str, str]:
    """Append one command-scoped Git configuration entry to an environment."""

    updated = dict(environment)
    raw_count = updated.get("GIT_CONFIG_COUNT", "0")
    try:
        count = int(raw_count)
    except ValueError as exc:
        raise QueueError(f"GIT_CONFIG_COUNT must be an integer, got {raw_count!r}") from exc
    updated[f"GIT_CONFIG_KEY_{count}"] = "core.excludesFile"
    updated[f"GIT_CONFIG_VALUE_{count}"] = str(excludes_file)
    updated["GIT_CONFIG_COUNT"] = str(count + 1)
    return updated


def _worktree_git_completed(
    store: QueueStore,
    worktree: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Run Git with scheduler-owned shared symlinks excluded from status."""

    environment = _environment_with_git_excludes(
        os.environ,
        _worktree_excludes_file(store),
    )
    return subprocess.run(
        ["git", *arguments],
        cwd=worktree,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
        check=False,
    )


def _pin_item_commit(repo_root: Path, item_id: int, commit: str) -> str:
    """Create the queue-owned ref that protects an admitted commit from pruning."""

    git_ref = _queue_git_ref(item_id)
    result = _git_completed(repo_root, "update-ref", git_ref, commit)
    if result.returncode != 0:
        raise _git_error(f"pin commit {commit} for queue item {item_id}", result)
    return git_ref


def _delete_item_ref(repo_root: Path, item_id: int, commit: str) -> None:
    """Delete only the exact queue-owned ref for one terminal item."""

    result = _git_completed(
        repo_root,
        "update-ref",
        "-d",
        _queue_git_ref(item_id),
        commit,
    )
    if result.returncode != 0:
        raise _git_error(f"delete pinned ref for queue item {item_id}", result)


def _worktree_identity(
    store: QueueStore,
    item: Mapping[str, Any],
    worktree: Path,
) -> tuple[bool, str | None]:
    """Verify commit, cleanliness, and card bytes inside an item worktree."""

    head = _worktree_git_completed(store, worktree, "rev-parse", "HEAD")
    if head.returncode != 0:
        return False, str(_git_error(f"read HEAD in {worktree}", head))
    if head.stdout.strip() != str(item["git_commit"]):
        return False, (
            f"worktree HEAD {head.stdout.strip()} differs from queued commit "
            f"{item['git_commit']}"
        )
    status = _worktree_git_completed(
        store,
        worktree,
        "status",
        "--porcelain",
        "--untracked-files=normal",
    )
    if status.returncode != 0:
        return False, str(_git_error(f"inspect worktree {worktree}", status))
    if status.stdout.strip():
        return False, f"isolated worktree is dirty: {status.stdout.strip()}"
    card_path = worktree / str(item["card_path"])
    if not card_path.is_file():
        return False, f"queued card is missing from isolated worktree: {card_path}"
    if _sha256_bytes(card_path.read_bytes()) != str(item["card_sha256"]):
        return False, f"queued card hash changed inside isolated worktree: {card_path}"
    return True, None


def _link_shared_worktree_paths(store: QueueStore, worktree: Path) -> list[str]:
    """Link ignored runtime/data/artifact roots into an isolated code worktree."""

    (store.repo_root / "outputs").mkdir(parents=True, exist_ok=True)
    linked: list[str] = []
    for name in SHARED_WORKTREE_PATHS:
        source = store.repo_root / name
        if not source.exists() and not source.is_symlink():
            continue
        target = worktree / name
        if os.path.lexists(target):
            if target.is_symlink() and target.resolve() == source.resolve():
                linked.append(name)
                continue
            raise QueueError(
                f"isolated worktree path {target} already exists and cannot link shared {source}"
            )
        ignore_candidate = f"{name}/" if source.is_dir() else name
        ignored = _worktree_git_completed(
            store,
            worktree,
            "check-ignore",
            "-q",
            "--no-index",
            "--",
            ignore_candidate,
        )
        if ignored.returncode != 0:
            raise QueueError(
                f"shared runtime path {name!r} is not ignored by queued commit "
                f"{worktree.name}; add it to .gitignore before admitting the experiment"
            )
        target.symlink_to(source, target_is_directory=source.is_dir())
        linked.append(name)
    return linked


def prepare_item_worktree(store: QueueStore, item: Mapping[str, Any]) -> Path:
    """Materialize or verify one detached immutable code worktree."""

    git_ref = str(_item_value(item, "git_ref") or "")
    expected_ref = _queue_git_ref(int(item["id"]))
    if git_ref != expected_ref:
        raise QueueError(
            f"queue item {item['id']} lacks its expected pinned ref {expected_ref}; "
            "remove and explicitly re-add the item"
        )
    worktree = _expected_worktree_path(store, item)
    recorded = _item_value(item, "worktree_path")
    if recorded and Path(str(recorded)).resolve() != worktree:
        raise QueueError(
            f"queue item {item['id']} records unexpected worktree {recorded}; expected {worktree}"
        )
    if not worktree.exists():
        worktree.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        added = _git_completed(
            store.repo_root,
            "worktree",
            "add",
            "--detach",
            str(worktree),
            git_ref,
        )
        if added.returncode != 0:
            _git_completed(store.repo_root, "worktree", "prune")
            added = _git_completed(
                store.repo_root,
                "worktree",
                "add",
                "--detach",
                str(worktree),
                git_ref,
            )
        if added.returncode != 0:
            raise _git_error(f"create isolated worktree {worktree}", added)
    linked = _link_shared_worktree_paths(store, worktree)
    valid, detail = _worktree_identity(store, item, worktree)
    if not valid:
        raise QueueError(detail or f"isolated worktree validation failed: {worktree}")
    if not recorded:
        with store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET worktree_path = ?, worktree_created_at = ?, "
                "worktree_removed_at = NULL, worktree_cleanup_error = NULL WHERE id = ?",
                (str(worktree), utc_now_iso(), item["id"]),
            )
            store._event(
                connection,
                "EXPERIMENT_WORKTREE_CREATED",
                queue_item_id=int(item["id"]),
                payload={
                    "path": str(worktree),
                    "git_commit": item["git_commit"],
                    "git_ref": git_ref,
                    "shared_paths": linked,
                },
            )
    return worktree


def cleanup_item_worktree(
    store: QueueStore,
    item: Mapping[str, Any],
    *,
    actor: str | None = None,
) -> bool:
    """Remove one exact terminal worktree and its pinned ref, retaining artifacts."""

    git_ref = _item_value(item, "git_ref")
    if not git_ref:
        return True
    item_id = int(item["id"])
    worktree = _expected_worktree_path(store, item)
    recorded = _item_value(item, "worktree_path")
    if recorded and Path(str(recorded)).resolve() != worktree:
        detail = f"refused unexpected worktree cleanup target {recorded}; expected {worktree}"
    else:
        detail = None
        if worktree.exists() or os.path.lexists(worktree):
            removed = _git_completed(
                store.repo_root,
                "worktree",
                "remove",
                "--force",
                str(worktree),
            )
            if removed.returncode != 0:
                detail = str(_git_error(f"remove isolated worktree {worktree}", removed))
        if detail is None:
            try:
                _delete_item_ref(store.repo_root, item_id, str(item["git_commit"]))
            except QueueError as exc:
                detail = str(exc)
    with store.connect() as connection:
        if detail is None:
            connection.execute(
                "UPDATE queue_items SET worktree_removed_at = ?, "
                "worktree_cleanup_error = NULL WHERE id = ?",
                (utc_now_iso(), item_id),
            )
            store._event(
                connection,
                "EXPERIMENT_WORKTREE_REMOVED",
                queue_item_id=item_id,
                payload={"path": str(worktree), "git_ref": git_ref},
                actor=actor,
            )
        else:
            connection.execute(
                "UPDATE queue_items SET worktree_cleanup_error = ? WHERE id = ?",
                (detail, item_id),
            )
            store._event(
                connection,
                "EXPERIMENT_WORKTREE_CLEANUP_FAILED",
                queue_item_id=item_id,
                payload={"path": str(worktree), "git_ref": git_ref, "error": detail},
                actor=actor,
            )
    if detail is None:
        _git_completed(store.repo_root, "worktree", "prune")
        return True
    return False


def _command_for_worktree(command_text: str, worktree: Path) -> str:
    """Redirect the card's canonical checkout line into its isolated worktree."""

    replacement = 'cd -- "$EXPERIMENT_QUEUE_WORKTREE"'
    lines = command_text.splitlines()
    replaced = 0
    for index, line in enumerate(lines):
        if re.fullmatch(r"\s*cd\s+~/3D_Helmholtz\s*", line):
            lines[index] = replacement
            replaced += 1
    transformed = "\n".join(lines)
    if replaced > 1:
        raise QueueError("card command changes to ~/3D_Helmholtz more than once")
    if "~/3D_Helmholtz" in transformed:
        raise QueueError(
            "card command contains an unsupported primary-checkout reference; "
            f"use one standalone 'cd ~/3D_Helmholtz' line: {worktree}"
        )
    return transformed


def _validated_continuation_checkpoint(item: Mapping[str, Any]) -> Path:
    """Revalidate both files accepted by the prior yield before resuming."""

    required = {
        "runner directory": item["runner_run_dir"],
        "continuation checkpoint": item["continuation_checkpoint"],
        "checkpoint SHA-256": item["continuation_checkpoint_sha256"],
        "continuation checkpoint metadata": item[
            "continuation_checkpoint_metadata"
        ],
        "checkpoint metadata SHA-256": item[
            "continuation_checkpoint_metadata_sha256"
        ],
    }
    missing = [label for label, value in required.items() if not value]
    if missing:
        raise ContinuationIntegrityError(
            f"queue item {item['id']} segment {item['segment']} lacks "
            + ", ".join(missing)
        )
    try:
        runner = Path(str(item["runner_run_dir"])).resolve()
        checkpoint_source = Path(str(item["continuation_checkpoint"]))
        metadata_source = Path(str(item["continuation_checkpoint_metadata"]))
        checkpoint = checkpoint_source.resolve()
        metadata = metadata_source.resolve()
    except OSError as error:
        raise ContinuationIntegrityError(
            f"queue item {item['id']} continuation paths cannot be resolved: {error}"
        ) from error
    for path, label in (
        (checkpoint, "continuation checkpoint"),
        (metadata, "continuation checkpoint metadata"),
    ):
        try:
            path.relative_to(runner)
        except ValueError as error:
            raise ContinuationIntegrityError(
                f"queue item {item['id']} {label} is outside runner directory: {path}"
            ) from error
    try:
        checkpoint_valid = (
            not checkpoint_source.is_symlink()
            and checkpoint.is_file()
            and hmac.compare_digest(
                _sha256_file(checkpoint),
                str(item["continuation_checkpoint_sha256"]),
            )
        )
        metadata_valid = (
            not metadata_source.is_symlink()
            and metadata.is_file()
            and hmac.compare_digest(
                _sha256_file(metadata),
                str(item["continuation_checkpoint_metadata_sha256"]),
            )
        )
    except OSError as error:
        raise ContinuationIntegrityError(
            f"queue item {item['id']} continuation files cannot be verified: {error}"
        ) from error
    if not checkpoint_valid:
        raise ContinuationIntegrityError(
            f"queue item {item['id']} continuation checkpoint is missing or changed: "
            f"{checkpoint}"
        )
    if not metadata_valid:
        raise ContinuationIntegrityError(
            "queue item "
            f"{item['id']} continuation checkpoint metadata is missing or "
            f"changed: {metadata}"
        )
    return checkpoint


def _item_execution_context(
    store: QueueStore,
    item: Mapping[str, Any],
    environment: Mapping[str, str],
) -> tuple[Path, str, dict[str, str]]:
    """Validate an item's immutable checkout and construct its child environment."""

    if _item_value(item, "git_ref"):
        execution_root = _expected_worktree_path(store, item)
        valid, detail = _worktree_identity(store, item, execution_root)
        if not valid:
            raise QueueError(
                detail or f"queue item {item['id']} isolated worktree failed validation"
            )
        command_text = _command_for_worktree(str(item["command_text"]), execution_root)
    else:
        # Legacy attempts admitted before schema v3 retain shared-checkout behavior.
        execution_root = store.repo_root
        command_text = str(item["command_text"])
    child_environment = dict(environment)
    if _item_value(item, "git_ref"):
        child_environment = _environment_with_git_excludes(
            child_environment,
            _worktree_excludes_file(store),
        )
    segment = int(item["segment"])
    child_environment["EXPERIMENT_QUEUE_SEGMENT"] = str(segment)
    child_environment["EXPERIMENT_QUEUE_WORKTREE"] = str(execution_root)
    child_environment["EXPERIMENT_QUEUE_PRIMARY_REPO"] = str(store.repo_root)
    if item["preemptible"]:
        child_environment["EXPERIMENT_QUEUE_YIELD_REQUEST_PATH"] = str(
            _yield_request_path(store, int(item["id"]), segment)
        )
        child_environment["EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"] = str(
            _yield_receipt_path(store, int(item["id"]), segment)
        )
    if segment > 1:
        checkpoint = _validated_continuation_checkpoint(item)
        child_environment["EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR"] = str(
            item["runner_run_dir"]
        )
        child_environment["EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT"] = str(checkpoint)
        if item["continuation_wandb_id"]:
            child_environment["EXPERIMENT_QUEUE_WANDB_ID"] = str(
                item["continuation_wandb_id"]
            )
    return execution_root, command_text, child_environment


def require_clean_git(repo_root: Path) -> str:
    """Return the exact commit after verifying an available clean worktree."""

    context = collect_git_context(repo_root)
    if not context.get("available") or not context.get("commit"):
        raise QueueError(
            f"experiment queue requires a Git worktree with a resolved commit: {context.get('error')}"
        )
    if context.get("dirty"):
        detail = str(context.get("status") or "").strip()
        suffix = f"\nDirty files:\n{detail}" if detail else ""
        raise QueueError(
            "experiment queue requires a clean worktree before adding or launching work; "
            f"commit or restore the intended state first.{suffix}"
        )
    return str(context["commit"])


def _path_inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def read_card_command(repo_root: Path, experiment_id: str, card_path: Path | None = None) -> CardCommand:
    """Read exactly one executable mutton2 command from an explicitly named card."""

    repo_root = repo_root.resolve()
    normalized_id = experiment_id.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_-]*-[0-9]+", normalized_id):
        raise QueueError(
            f"invalid experiment ID {experiment_id!r}; expected a phase-scoped ID such as WCG-017"
        )
    selected = card_path or Path("docs/experiments") / f"{normalized_id}.md"
    absolute = selected if selected.is_absolute() else repo_root / selected
    absolute = absolute.resolve()
    if not _path_inside(absolute, repo_root):
        raise QueueError(f"experiment card must be inside the repository: {absolute}")
    if not absolute.is_file():
        raise QueueError(f"experiment card does not exist: {absolute}")

    tracked = _git_completed(repo_root, "ls-files", "--error-unmatch", str(absolute.relative_to(repo_root)))
    if tracked.returncode != 0:
        raise QueueError(
            f"experiment card is not tracked by Git: {absolute}; commit the exact card before adding it"
        )

    raw = absolute.read_bytes()
    text = raw.decode("utf-8")
    first_heading = text.splitlines()[0] if text.splitlines() else ""
    if not first_heading.startswith(f"# {normalized_id}:"):
        raise QueueError(
            f"experiment card heading must start with '# {normalized_id}:': {absolute}"
        )
    heading_offset = text.find(CARD_COMMAND_HEADING)
    if heading_offset < 0:
        raise QueueError(
            f"experiment card lacks the required {CARD_COMMAND_HEADING!r} section: {absolute}"
        )
    section_start = heading_offset + len(CARD_COMMAND_HEADING)
    next_heading = text.find("\n## ", section_start)
    section = text[section_start:] if next_heading < 0 else text[section_start:next_heading]
    blocks = re.findall(r"^```(?:bash|sh)\s*\n(.*?)^```\s*$", section, flags=re.MULTILINE | re.DOTALL)
    if len(blocks) != 1:
        raise QueueError(
            f"expected exactly one bash command block under {CARD_COMMAND_HEADING!r} in {absolute}; "
            f"found {len(blocks)}"
        )
    command_text = blocks[0].strip()
    if re.search(r"\\\\[ \t]*$", command_text, flags=re.MULTILINE):
        raise QueueError(
            "card command contains a doubled trailing backslash; use exactly "
            "one backslash for each shell line continuation in "
            f"{absolute}"
        )
    required_fragments = ("scripts/run_experiment.py", "--require-clean", "--remote mutton2")
    missing = [fragment for fragment in required_fragments if fragment not in command_text]
    if missing:
        raise QueueError(
            f"card command is not queue-compatible; missing {', '.join(missing)} in {absolute}"
        )
    name_match = re.search(r"--name\s+([A-Za-z0-9_.-]+)", command_text)
    if name_match is None:
        raise QueueError(f"card command does not contain a simple --name value: {absolute}")
    return CardCommand(
        experiment_id=normalized_id,
        card_path=absolute,
        card_sha256=_sha256_bytes(raw),
        command_text=command_text,
        runner_name=name_match.group(1),
    )


def add_experiment(
    store: QueueStore,
    experiment_id: str,
    *,
    card_path: Path | None = None,
    priority: int = 0,
    dependency_ids: Sequence[int] = (),
    held: bool = False,
    new_attempt: bool = False,
    preemptible: bool = False,
    actor: str | None = None,
) -> int:
    """Explicitly snapshot one committed card command into queue membership."""

    commit = require_clean_git(store.repo_root)
    card = read_card_command(store.repo_root, experiment_id, card_path)
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        active = connection.execute(
            "SELECT id, state FROM queue_items WHERE experiment_id = ? "
            "AND state IN ('queued','held','blocked','starting','running','yielding','terminating','force_killing')",
            (card.experiment_id,),
        ).fetchone()
        if active is not None:
            raise QueueError(
                f"{card.experiment_id} already has active queue item {active['id']} "
                f"in state {active['state']}"
            )
        prior = list(
            connection.execute(
                "SELECT id, attempt, state, started_at FROM queue_items "
                "WHERE experiment_id = ? ORDER BY attempt",
                (card.experiment_id,),
            )
        )
        previously_launched = any(row["started_at"] is not None for row in prior)
        if previously_launched and not new_attempt:
            raise QueueError(
                f"{card.experiment_id} has a prior launched attempt; pass --new-attempt "
                "to authorize another run"
            )
        for dependency_id in dependency_ids:
            dependency = connection.execute(
                "SELECT id FROM queue_items WHERE id = ?", (dependency_id,)
            ).fetchone()
            if dependency is None:
                raise QueueError(f"dependency queue item {dependency_id} does not exist")

        attempt = max((int(row["attempt"]) for row in prior), default=0) + 1
        state = "held" if held else "queued"
        relative_card = card.card_path.relative_to(store.repo_root)
        cursor = connection.execute(
            """
            INSERT INTO queue_items(
                experiment_id, attempt, state, priority, card_path, card_sha256,
                command_text, runner_name, git_commit, added_at, added_by, state_detail
                , preemptible
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card.experiment_id,
                attempt,
                state,
                priority,
                str(relative_card),
                card.card_sha256,
                card.command_text,
                card.runner_name,
                commit,
                utc_now_iso(),
                actor or _actor(),
                "explicitly held at admission" if held else None,
                int(preemptible),
            ),
        )
        item_id = int(cursor.lastrowid)
        git_ref = _pin_item_commit(store.repo_root, item_id, commit)
        try:
            connection.execute(
                "UPDATE queue_items SET git_ref = ? WHERE id = ?",
                (git_ref, item_id),
            )
            for dependency_id in dependency_ids:
                connection.execute(
                    "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (?, ?)",
                    (item_id, dependency_id),
                )
            store._event(
                connection,
                "EXPERIMENT_ADDED",
                queue_item_id=item_id,
                payload={
                    "experiment_id": card.experiment_id,
                    "attempt": attempt,
                    "priority": priority,
                    "held": held,
                    "dependencies": list(dependency_ids),
                    "card_path": str(relative_card),
                    "card_sha256": card.card_sha256,
                    "git_commit": commit,
                    "git_ref": git_ref,
                    "preemptible": bool(preemptible),
                },
                actor=actor,
            )
        except BaseException:
            try:
                _delete_item_ref(store.repo_root, item_id, commit)
            except QueueError:
                pass
            raise
    return item_id


def _transition_pending_item(
    store: QueueStore,
    item_id: int,
    *,
    target_state: str,
    event_type: str,
    detail: str | None = None,
    allowed_states: Iterable[str] = PENDING_STATES,
    actor: str | None = None,
) -> None:
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in set(allowed_states):
            raise QueueError(
                f"queue item {item_id} is {item['state']}; {event_type.lower()} requires "
                f"one of {sorted(set(allowed_states))}"
            )
        connection.execute(
            "UPDATE queue_items SET state = ?, state_detail = ? WHERE id = ?",
            (target_state, detail, item_id),
        )
        store._event(
            connection,
            event_type,
            queue_item_id=item_id,
            payload={"detail": detail},
            actor=actor,
        )


def remove_item(
    store: QueueStore,
    item_id: int,
    reason: str | None = None,
    *,
    actor: str | None = None,
) -> None:
    """Remove a pending item without deleting its operational history."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in PENDING_STATES:
            raise QueueError(
                f"queue item {item_id} is {item['state']}; remove requires a pending item"
            )
        connection.execute(
            "UPDATE queue_items SET state = 'removed', state_detail = ? WHERE id = ?",
            (reason, item_id),
        )
        store._event(
            connection,
            "EXPERIMENT_REMOVED",
            queue_item_id=item_id,
            payload={"detail": reason},
            actor=actor,
        )
        dependents = list(
            connection.execute(
                """
                SELECT child.id
                FROM dependencies AS link
                JOIN queue_items AS child ON child.id = link.queue_item_id
                WHERE link.dependency_item_id = ? AND child.state IN ('queued','blocked')
                """,
                (item_id,),
            )
        )
        for dependent in dependents:
            detail = f"dependency queue item {item_id} was removed"
            connection.execute(
                "UPDATE queue_items SET state = 'held', state_detail = ? WHERE id = ?",
                (detail, dependent["id"]),
            )
            store._event(
                connection,
                "DEPENDENT_HELD",
                queue_item_id=int(dependent["id"]),
                payload={"dependency_item_id": item_id, "reason": detail},
                actor=actor,
            )
    cleanup_item_worktree(store, store.item(item_id), actor=actor)


def hold_item(
    store: QueueStore,
    item_id: int,
    reason: str | None = None,
    *,
    actor: str | None = None,
) -> None:
    """Prevent a pending item from dispatching until explicitly released."""

    _transition_pending_item(
        store,
        item_id,
        target_state="held",
        event_type="EXPERIMENT_HELD",
        detail=reason,
        allowed_states={"queued", "blocked"},
        actor=actor,
    )


def release_item(
    store: QueueStore,
    item_id: int,
    *,
    actor: str | None = None,
) -> None:
    """Return a held or blocked item to explicit queue membership."""

    _transition_pending_item(
        store,
        item_id,
        target_state="queued",
        event_type="EXPERIMENT_RELEASED",
        allowed_states={"held", "blocked"},
        actor=actor,
    )


def set_priority(
    store: QueueStore,
    item_id: int,
    priority: int,
    *,
    actor: str | None = None,
) -> None:
    """Change the priority of queued work or a resumable active attempt."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in PRIORITY_MUTABLE_STATES:
            raise QueueError(
                f"queue item {item_id} is {item['state']}; priority can change only "
                "for pending, starting, running, or yielding work"
            )
        connection.execute("UPDATE queue_items SET priority = ? WHERE id = ?", (priority, item_id))
        store._event(
            connection,
            "PRIORITY_CHANGED",
            queue_item_id=item_id,
            payload={"old": item["priority"], "new": priority},
            actor=actor,
        )


def set_dispatch_paused(
    store: QueueStore,
    paused: bool,
    reason: str | None = None,
    *,
    actor: str | None = None,
) -> None:
    """Pause or resume new dispatch without changing running jobs."""

    with store.connect() as connection:
        store._set_meta(connection, "dispatch_paused", "1" if paused else "0")
        store._set_meta(connection, "pause_reason", reason or "")
        store._event(
            connection,
            "DISPATCH_PAUSED" if paused else "DISPATCH_RESUMED",
            payload={"reason": reason},
            actor=actor,
        )


def query_gpus(nvidia_smi: str = "nvidia-smi") -> list[GpuSnapshot]:
    """Query physical GPUs and active compute PIDs through ``nvidia-smi``."""

    inventory_command = [
        nvidia_smi,
        "--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu",
        "--format=csv,noheader,nounits",
    ]
    try:
        inventory = subprocess.run(
            inventory_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=20,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise QueueError(f"could not query GPUs with {nvidia_smi!r}: {exc}") from exc
    if inventory.returncode != 0:
        raise QueueError(
            f"nvidia-smi GPU inventory failed with exit code {inventory.returncode}: "
            f"{inventory.stderr.strip()}"
        )

    process_command = [
        nvidia_smi,
        "--query-compute-apps=gpu_uuid,pid",
        "--format=csv,noheader,nounits",
    ]
    try:
        processes = subprocess.run(
            process_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=20,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise QueueError(f"could not query GPU compute processes with {nvidia_smi!r}: {exc}") from exc
    if processes.returncode != 0:
        raise QueueError(
            f"nvidia-smi compute-process query failed with exit code {processes.returncode}: "
            f"{processes.stderr.strip()}"
        )
    pids_by_uuid: dict[str, list[int]] = {}
    process_lines = [
        line
        for line in processes.stdout.splitlines()
        if line.strip() and "no running processes" not in line.lower()
    ]
    for row in csv.reader(process_lines):
        if len(row) != 2:
            raise QueueError(f"unexpected nvidia-smi compute-process row: {row!r}")
        uuid = row[0].strip()
        if uuid in {"N/A", "[N/A]"} or row[1].strip() in {"N/A", "[N/A]"}:
            continue
        try:
            pid = int(row[1].strip())
        except ValueError as exc:
            raise QueueError(f"invalid compute PID from nvidia-smi: {row[1]!r}") from exc
        pids_by_uuid.setdefault(uuid, []).append(pid)

    snapshots: list[GpuSnapshot] = []
    for row in csv.reader(line for line in inventory.stdout.splitlines() if line.strip()):
        if len(row) != 6:
            raise QueueError(f"unexpected nvidia-smi GPU row: {row!r}")
        uuid = row[1].strip()
        snapshots.append(
            GpuSnapshot(
                index=row[0].strip(),
                uuid=uuid,
                name=row[2].strip(),
                memory_total_mib=_parse_csv_number(row[3], field="total GPU memory"),
                memory_used_mib=_parse_csv_number(row[4], field="used GPU memory"),
                utilization_percent=_parse_csv_number(row[5], field="GPU utilization"),
                compute_pids=tuple(sorted(pids_by_uuid.get(uuid, []))),
            )
        )
    if not snapshots:
        raise QueueError("nvidia-smi reported no GPUs")
    return snapshots


def _expand_identifiers(identifiers: Sequence[str]) -> list[str]:
    expanded: list[str] = []
    for value in identifiers:
        expanded.extend(part.strip() for part in value.split(",") if part.strip())
    return expanded


def resolve_gpu_identifiers(
    snapshots: Sequence[GpuSnapshot], identifiers: Sequence[str]
) -> list[tuple[str, GpuSnapshot]]:
    """Resolve operator-provided indices or unique UUID prefixes."""

    resolved: list[tuple[str, GpuSnapshot]] = []
    seen: set[str] = set()
    for identifier in _expand_identifiers(identifiers):
        matches = [
            gpu
            for gpu in snapshots
            if gpu.index == identifier or gpu.uuid == identifier or gpu.uuid.startswith(identifier)
        ]
        if not matches:
            raise QueueError(f"GPU identifier {identifier!r} did not match an observed index or UUID")
        unique = {gpu.uuid: gpu for gpu in matches}
        if len(unique) != 1:
            raise QueueError(f"GPU identifier {identifier!r} is ambiguous; use a longer UUID prefix")
        gpu = next(iter(unique.values()))
        if gpu.uuid not in seen:
            resolved.append((identifier, gpu))
            seen.add(gpu.uuid)
    return resolved


def _running_gpu_uuids(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["assigned_gpu_uuid"])
        for row in connection.execute(
            "SELECT assigned_gpu_uuid FROM queue_items WHERE state IN "
            "('starting','running','yielding','terminating','force_killing') AND assigned_gpu_uuid IS NOT NULL"
        )
    }


def update_gpu_allowlist(
    store: QueueStore,
    action: str,
    identifiers: Sequence[str],
    *,
    snapshots: Sequence[GpuSnapshot],
    actor: str | None = None,
) -> None:
    """Atomically set, add, or remove operator-owned GPU eligibility."""

    if action not in {"set", "add", "remove"}:
        raise QueueError(f"unsupported GPU allowlist action: {action}")
    now = utc_now_iso()
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        running = _running_gpu_uuids(connection)
        existing = {
            str(row["uuid"]): row
            for row in connection.execute("SELECT * FROM gpu_allowlist")
        }
        if action == "remove":
            requested_uuids: set[str] = set()
            for identifier in _expand_identifiers(identifiers):
                observed_matches = {
                    gpu.uuid
                    for gpu in snapshots
                    if gpu.index == identifier
                    or gpu.uuid == identifier
                    or gpu.uuid.startswith(identifier)
                }
                stored_matches = {
                    uuid
                    for uuid, row in existing.items()
                    if row["last_index"] == identifier
                    or row["requested_identifier"] == identifier
                    or uuid == identifier
                    or uuid.startswith(identifier)
                }
                matches = observed_matches | stored_matches
                if not matches:
                    raise QueueError(
                        f"GPU identifier {identifier!r} did not match an observed or allowed GPU"
                    )
                if len(matches) != 1:
                    raise QueueError(
                        f"GPU identifier {identifier!r} is ambiguous; use the full GPU UUID"
                    )
                requested_uuids.update(matches)
            resolved: list[tuple[str, GpuSnapshot]] = []
        else:
            resolved = resolve_gpu_identifiers(snapshots, identifiers)
            requested_uuids = {gpu.uuid for _, gpu in resolved}
        if action == "set":
            removals = set(existing) - requested_uuids
            additions = resolved
        elif action == "add":
            removals = set()
            additions = resolved
        else:
            removals = requested_uuids
            additions = []

        for uuid in removals:
            if uuid in running:
                connection.execute(
                    "UPDATE gpu_allowlist SET enabled = 0, draining = 1, updated_at = ? WHERE uuid = ?",
                    (now, uuid),
                )
            else:
                connection.execute("DELETE FROM gpu_allowlist WHERE uuid = ?", (uuid,))
        for requested, gpu in additions:
            connection.execute(
                """
                INSERT INTO gpu_allowlist(
                    uuid, requested_identifier, last_index, name, enabled, draining, updated_at
                ) VALUES (?, ?, ?, ?, 1, 0, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    requested_identifier = excluded.requested_identifier,
                    last_index = excluded.last_index,
                    name = excluded.name,
                    enabled = 1,
                    draining = 0,
                    updated_at = excluded.updated_at
                """,
                (gpu.uuid, requested, gpu.index, gpu.name, now),
            )
        store._event(
            connection,
            f"GPU_ALLOWLIST_{action.upper()}",
            payload={
                "identifiers": _expand_identifiers(identifiers),
                "resolved_uuids": sorted(requested_uuids),
                "draining_uuids": sorted(removals & running),
            },
            actor=actor,
        )


def _segment_dir(store: QueueStore, item_id: int, segment: int) -> Path:
    return store.state_dir / "attempts" / str(item_id) / "segments" / str(segment)


def _yield_request_path(store: QueueStore, item_id: int, segment: int) -> Path:
    return _segment_dir(store, item_id, segment) / "yield" / "request.json"


def _yield_receipt_path(store: QueueStore, item_id: int, segment: int) -> Path:
    return _segment_dir(store, item_id, segment) / "yield" / "receipt.json"


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def expire_reservations(
    store: QueueStore,
    *,
    now: datetime | None = None,
) -> list[int]:
    """Expire active reservations without mutating the permanent GPU allowlist."""

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expired: list[int] = []
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = list(
            connection.execute(
                "SELECT * FROM gpu_reservations WHERE status = 'active' "
                "AND expires_at IS NOT NULL"
            )
        )
        for row in rows:
            if _parse_timestamp(str(row["expires_at"])) > current:
                continue
            reservation_id = int(row["id"])
            connection.execute(
                "UPDATE gpu_reservations SET status = 'expired', state_detail = ? WHERE id = ?",
                ("reservation duration elapsed", reservation_id),
            )
            store._event(
                connection,
                "GPU_RESERVATION_EXPIRED",
                queue_item_id=row["queue_item_id"],
                payload={
                    "reservation_id": reservation_id,
                    "gpu_uuid": row["gpu_uuid"],
                    "note": row["note"],
                    "expires_at": row["expires_at"],
                },
                actor="scheduler",
            )
            expired.append(reservation_id)
    return expired


def _open_reserved_gpu_uuids(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["gpu_uuid"])
        for row in connection.execute(
            "SELECT gpu_uuid FROM gpu_reservations "
            "WHERE status IN ('pending', 'active')"
        )
    }


def list_reservations(store: QueueStore) -> list[sqlite3.Row]:
    """Return reservations newest first for operator and coworker interfaces."""

    with store.connect() as connection:
        return list(
            connection.execute("SELECT * FROM gpu_reservations ORDER BY id DESC")
        )


def request_gpu_reservation(
    store: QueueStore,
    gpu_uuid: str,
    *,
    duration_hours: int,
    note: str,
    actor: str,
    snapshots: Sequence[GpuSnapshot] | None = None,
) -> int:
    """Reserve an idle GPU or cooperatively yield one preemptible queue job."""

    if isinstance(duration_hours, bool) or not isinstance(duration_hours, int) or not (
        MIN_RESERVATION_HOURS <= duration_hours <= MAX_RESERVATION_HOURS
    ):
        raise QueueError(
            f"reservation duration must be a whole number from {MIN_RESERVATION_HOURS} "
            f"through {MAX_RESERVATION_HOURS} hours"
        )
    cleaned_note = " ".join(note.split()).strip()
    if not cleaned_note:
        raise QueueError("reservation note is required; include who the GPU is for")
    if len(cleaned_note) > 200:
        raise QueueError("reservation note must be 200 characters or fewer")
    now = datetime.now(timezone.utc)
    requested_at = now.isoformat(timespec="seconds")
    request_id = uuid_module.uuid4().hex
    item_id: int | None = None
    segment: int | None = None
    reservation_id: int
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        allow = connection.execute(
            "SELECT * FROM gpu_allowlist WHERE uuid = ? AND enabled = 1",
            (gpu_uuid,),
        ).fetchone()
        if allow is None:
            raise QueueError(
                f"GPU {gpu_uuid} is not currently enabled in the scheduler pool"
            )
        existing = connection.execute(
            "SELECT * FROM gpu_reservations WHERE gpu_uuid = ? "
            "AND status IN ('pending', 'active')",
            (gpu_uuid,),
        ).fetchone()
        if existing is not None:
            raise QueueError(
                f"GPU {gpu_uuid} already has open reservation {existing['id']} "
                f"for {existing['note']}"
            )
        running = connection.execute(
            "SELECT * FROM queue_items WHERE assigned_gpu_uuid = ? AND state IN "
            "('starting','running','yielding','terminating','force_killing')",
            (gpu_uuid,),
        ).fetchone()
        if running is not None:
            if running["state"] != "running":
                raise QueueError(
                    f"queue item {running['id']} is {running['state']}; wait until it is "
                    "stably running before requesting a cooperative yield"
                )
            if not running["preemptible"]:
                raise QueueError(
                    f"queue item {running['id']} is not marked checkpoint-and-requeue capable"
                )
            item_id = int(running["id"])
            segment = int(running["segment"])
            status = "pending"
            starts_at = None
            expires_at = None
        else:
            if snapshots is not None:
                observed = {gpu.uuid: gpu for gpu in snapshots}.get(gpu_uuid)
                if observed is None:
                    raise QueueError(
                        f"GPU {gpu_uuid} is not currently visible in GPU telemetry"
                    )
                if observed.compute_pids:
                    raise QueueError(
                        f"GPU {gpu_uuid} is used by external process IDs "
                        f"{list(observed.compute_pids)}; the scheduler cannot yield those processes"
                    )
            status = "active"
            starts_at = requested_at
            expires_at = (now + timedelta(hours=duration_hours)).isoformat(
                timespec="seconds"
            )
        cursor = connection.execute(
            """
            INSERT INTO gpu_reservations(
                gpu_uuid, queue_item_id, status, requested_at, requested_by,
                note, duration_hours, starts_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gpu_uuid,
                item_id,
                status,
                requested_at,
                actor,
                cleaned_note,
                duration_hours,
                starts_at,
                expires_at,
            ),
        )
        reservation_id = int(cursor.lastrowid)
        if item_id is not None:
            connection.execute(
                """
                UPDATE queue_items SET state = 'yielding', yield_requested_at = ?,
                    yield_requested_by = ?, yield_request_id = ?, yield_note = ?,
                    yield_duration_hours = ?, state_detail = ?
                WHERE id = ? AND state = 'running'
                """,
                (
                    requested_at,
                    actor,
                    request_id,
                    cleaned_note,
                    duration_hours,
                    f"checkpointing to yield GPU for {cleaned_note}",
                    item_id,
                ),
            )
        store._event(
            connection,
            "GPU_RESERVATION_REQUESTED",
            queue_item_id=item_id,
            payload={
                "reservation_id": reservation_id,
                "gpu_uuid": gpu_uuid,
                "duration_hours": duration_hours,
                "note": cleaned_note,
                "status": status,
                "request_id": request_id if item_id is not None else None,
            },
            actor=actor,
        )
    if item_id is not None and segment is not None:
        request_path = _yield_request_path(store, item_id, segment)
        try:
            _atomic_write_json(
                request_path,
                {
                    "schema_version": 1,
                    "request_id": request_id,
                    "queue_item_id": item_id,
                    "segment": segment,
                    "gpu_uuid": gpu_uuid,
                    "requested_at": requested_at,
                    "requested_by": actor,
                    "note": cleaned_note,
                    "duration_hours": duration_hours,
                },
            )
        except OSError as exc:
            with store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                restored = connection.execute(
                    """
                    UPDATE queue_items SET state = 'running', state_detail = ?,
                        yield_requested_at = NULL, yield_requested_by = NULL,
                        yield_request_id = NULL, yield_note = NULL,
                        yield_duration_hours = NULL
                    WHERE id = ? AND state = 'yielding' AND yield_request_id = ?
                    """,
                    (f"yield request could not be written: {exc}", item_id, request_id),
                )
                connection.execute(
                    "UPDATE gpu_reservations SET status = 'failed', state_detail = ? WHERE id = ?",
                    (str(exc), reservation_id),
                )
                store._event(
                    connection,
                    "GPU_RESERVATION_FAILED",
                    queue_item_id=item_id,
                    payload={
                        "reservation_id": reservation_id,
                        "error": str(exc),
                        "item_state_restored": restored.rowcount == 1,
                    },
                    actor=actor,
                )
            raise QueueError(f"could not deliver yield request: {exc}") from exc
    return reservation_id


def request_preemption(
    store: QueueStore,
    item_id: int,
    *,
    reason: str | None = None,
    actor: str | None = None,
) -> str:
    """Ask one capable running item to checkpoint, exit, and rejoin its priority band."""

    cleaned_reason = " ".join((reason or "manual operator preemption").split()).strip()
    if not cleaned_reason:
        cleaned_reason = "manual operator preemption"
    if len(cleaned_reason) > 200:
        raise QueueError("preemption reason must be 200 characters or fewer")
    requested_by = actor or _actor()
    requested_at = utc_now_iso()
    request_id = uuid_module.uuid4().hex
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] != "running":
            raise QueueError(
                f"queue item {item_id} is {item['state']}; cooperative preemption "
                "requires a stably running item"
            )
        if not item["preemptible"]:
            raise QueueError(
                f"queue item {item_id} is not marked checkpoint-and-requeue capable"
            )
        gpu_uuid = str(item["assigned_gpu_uuid"] or "").strip()
        if not gpu_uuid:
            raise QueueError(
                f"queue item {item_id} has no assigned GPU; cannot request preemption"
            )
        segment = int(item["segment"])
        connection.execute(
            """
            UPDATE queue_items SET state = 'yielding', yield_requested_at = ?,
                yield_requested_by = ?, yield_request_id = ?, yield_note = ?,
                yield_duration_hours = NULL, state_detail = ?
            WHERE id = ? AND state = 'running'
            """,
            (
                requested_at,
                requested_by,
                request_id,
                cleaned_reason,
                f"checkpointing for manual preemption: {cleaned_reason}",
                item_id,
            ),
        )
        store._event(
            connection,
            "MANUAL_PREEMPTION_REQUESTED",
            queue_item_id=item_id,
            payload={
                "request_id": request_id,
                "gpu_uuid": gpu_uuid,
                "segment": segment,
                "reason": cleaned_reason,
            },
            actor=actor,
        )

    request_path = _yield_request_path(store, item_id, segment)
    try:
        _atomic_write_json(
            request_path,
            {
                "schema_version": 1,
                "request_kind": "manual_preemption",
                "request_id": request_id,
                "queue_item_id": item_id,
                "segment": segment,
                "gpu_uuid": gpu_uuid,
                "requested_at": requested_at,
                "requested_by": requested_by,
                "note": cleaned_reason,
            },
        )
    except OSError as exc:
        with store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            restored = connection.execute(
                """
                UPDATE queue_items SET state = 'running', state_detail = ?,
                    yield_requested_at = NULL, yield_requested_by = NULL,
                    yield_request_id = NULL, yield_note = NULL,
                    yield_duration_hours = NULL
                WHERE id = ? AND state = 'yielding' AND yield_request_id = ?
                """,
                (f"manual preemption request could not be written: {exc}", item_id, request_id),
            )
            store._event(
                connection,
                "MANUAL_PREEMPTION_REQUEST_FAILED",
                queue_item_id=item_id,
                payload={
                    "request_id": request_id,
                    "error": str(exc),
                    "item_state_restored": restored.rowcount == 1,
                },
                actor=actor,
            )
        raise QueueError(f"could not deliver manual preemption request: {exc}") from exc
    return request_id


def release_gpu_reservation(
    store: QueueStore,
    reservation_id: int,
    *,
    actor: str,
) -> None:
    """Release an active reservation without changing the allowlist."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM gpu_reservations WHERE id = ?", (reservation_id,)
        ).fetchone()
        if row is None:
            raise QueueError(f"GPU reservation {reservation_id} does not exist")
        if row["status"] == "pending":
            raise QueueError(
                f"GPU reservation {reservation_id} is still checkpointing; wait until "
                "the reservation becomes active before releasing it"
            )
        if row["status"] != "active":
            raise QueueError(
                f"GPU reservation {reservation_id} is already {row['status']}"
            )
        connection.execute(
            "UPDATE gpu_reservations SET status = 'released', released_at = ?, "
            "released_by = ?, state_detail = ? WHERE id = ?",
            (utc_now_iso(), actor, "released early", reservation_id),
        )
        store._event(
            connection,
            "GPU_RESERVATION_RELEASED",
            queue_item_id=row["queue_item_id"],
            payload={
                "reservation_id": reservation_id,
                "gpu_uuid": row["gpu_uuid"],
                "note": row["note"],
            },
            actor=actor,
        )


def _process_start_ticks(pid: int) -> str | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    except (FileNotFoundError, PermissionError, OSError):
        return None
    return fields[21] if len(fields) > 21 else None


def _pid_matches(item: sqlite3.Row) -> bool:
    pid = item["pid"]
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return False
    expected_ticks = item["proc_start_ticks"]
    current_ticks = _process_start_ticks(int(pid))
    if expected_ticks is not None and current_ticks is not None and current_ticks != expected_ticks:
        return False
    try:
        return os.getpgid(int(pid)) == int(item["pgid"])
    except (ProcessLookupError, PermissionError):
        return False


def _signal_item(item: sqlite3.Row, signum: int) -> bool:
    if not _pid_matches(item):
        return False
    try:
        os.killpg(int(item["pgid"]), signum)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def request_termination(
    store: QueueStore,
    item_id: int,
    *,
    reason: str | None,
    force: bool,
    actor: str | None = None,
) -> bool:
    """Record and signal a graceful termination or explicit force kill."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in RUNNING_STATES:
            raise QueueError(
                f"queue item {item_id} is {item['state']}; remove pending work or target a running item"
            )
        if force:
            target_state = "force_killing"
            stage = "kill"
            signum = signal.SIGKILL
            event_type = "FORCE_KILL_REQUESTED"
        else:
            if item["state"] == "force_killing":
                raise QueueError(f"queue item {item_id} already has a force-kill request")
            target_state = "terminating"
            stage = "interrupt"
            signum = signal.SIGINT
            event_type = "TERMINATION_REQUESTED"
        now = utc_now_iso()
        epoch = time.time()
        connection.execute(
            """
            UPDATE queue_items SET state = ?, terminate_requested_at = ?, terminate_reason = ?,
                termination_stage = ?, termination_signal_epoch = ?
            WHERE id = ?
            """,
            (target_state, now, reason, stage, epoch, item_id),
        )
        if item["state"] == "yielding":
            connection.execute(
                "UPDATE gpu_reservations SET status = 'failed', state_detail = ? "
                "WHERE queue_item_id = ? AND status = 'pending'",
                ("yield superseded by explicit termination", item_id),
            )
        store._event(
            connection,
            event_type,
            queue_item_id=item_id,
            payload={"reason": reason, "signal": signal.Signals(signum).name},
            actor=actor,
        )
        refreshed = store.item(item_id, connection=connection)
    signaled = _signal_item(refreshed, signum)
    store.event(
        "TERMINATION_SIGNAL_SENT" if signaled else "TERMINATION_SIGNAL_PENDING",
        queue_item_id=item_id,
        payload={"signal": signal.Signals(signum).name},
        actor=actor,
    )
    return signaled


def _read_runner_receipt(log_path: Path) -> dict[str, str]:
    """Extract the existing runner's final paths and pull command from its tee log."""

    if not log_path.is_file():
        return {}
    text = log_path.read_text(encoding="utf-8", errors="replace")
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text).replace("\r", "\n")
    patterns = {
        "run_directory": r"^run directory:\s*(.+?)\s*$",
        "manifest": r"^manifest:\s*(.+?)\s*$",
        "rsync_pull_command": r"^pull outputs with:\s*(.+?)\s*$",
    }
    result: dict[str, str] = {}
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        if matches:
            result[key] = matches[-1]
    return result


class Scheduler:
    """Foreground dispatcher with persistent subprocess recovery."""

    def __init__(
        self,
        store: QueueStore,
        *,
        poll_seconds: float = 60.0,
        control_seconds: float = 1.0,
        min_free_memory_fraction: float = 0.95,
        max_utilization_percent: float = 5.0,
        min_free_disk_gib: float = 50.0,
        termination_grace_seconds: float = 30.0,
        max_consecutive_failures: int = 2,
        gpu_provider: Callable[[], list[GpuSnapshot]] = query_gpus,
    ):
        poll_seconds = _scheduler_float(poll_seconds, field="poll_seconds")
        control_seconds = _scheduler_float(control_seconds, field="control_seconds")
        min_free_memory_fraction = _scheduler_float(
            min_free_memory_fraction,
            field="min_free_memory_fraction",
        )
        max_utilization_percent = _scheduler_float(
            max_utilization_percent,
            field="max_utilization_percent",
        )
        min_free_disk_gib = _scheduler_float(
            min_free_disk_gib,
            field="min_free_disk_gib",
        )
        termination_grace_seconds = _scheduler_float(
            termination_grace_seconds,
            field="termination_grace_seconds",
        )
        if poll_seconds <= 0.0:
            raise QueueError(f"poll_seconds must be finite and positive, got {poll_seconds!r}")
        if control_seconds <= 0.0:
            raise QueueError(
                f"control_seconds must be finite and positive, got {control_seconds!r}"
            )
        if not 0.0 <= min_free_memory_fraction <= 1.0:
            raise QueueError(
                "min_free_memory_fraction must be finite and between 0 and 1, got "
                f"{min_free_memory_fraction!r}"
            )
        if not 0.0 <= max_utilization_percent <= 100.0:
            raise QueueError(
                "max_utilization_percent must be finite and between 0 and 100, got "
                f"{max_utilization_percent!r}"
            )
        if min_free_disk_gib < 0.0:
            raise QueueError(
                f"min_free_disk_gib must be finite and nonnegative, got {min_free_disk_gib!r}"
            )
        if termination_grace_seconds < 0.0:
            raise QueueError(
                "termination_grace_seconds must be finite and nonnegative, got "
                f"{termination_grace_seconds!r}"
            )
        if (
            isinstance(max_consecutive_failures, bool)
            or not isinstance(max_consecutive_failures, int)
            or max_consecutive_failures < 1
        ):
            raise QueueError(
                "max_consecutive_failures must be a positive integer, got "
                f"{max_consecutive_failures!r}"
            )
        self.store = store
        self.poll_seconds = float(poll_seconds)
        self.control_seconds = float(control_seconds)
        self.min_free_memory_fraction = float(min_free_memory_fraction)
        self.max_utilization_percent = float(max_utilization_percent)
        self.min_free_disk_gib = float(min_free_disk_gib)
        self.termination_grace_seconds = float(termination_grace_seconds)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.gpu_provider = gpu_provider
        self.processes: dict[int, subprocess.Popen[bytes]] = {}
        self.gpu_locks: dict[str, Any] = {}
        self._stop = False
        self._last_gpu_poll = 0.0
        self._scheduler_lock: Any | None = None

    def _lock_scheduler(self) -> None:
        lock_path = self.store.state_dir / "scheduler.lock"
        lock_file = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise QueueError(
                f"another scheduler already holds {lock_path}; use the status command to inspect it"
            ) from exc
        self._scheduler_lock = lock_file

    def _global_gpu_lock(self, uuid: str) -> Any | None:
        if uuid in self.gpu_locks:
            return self.gpu_locks[uuid]
        lock_root = Path(tempfile.gettempdir()) / f"helmholtz-experiment-queue-locks-{os.getuid()}"
        lock_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        filename = hashlib.sha256(uuid.encode("utf-8")).hexdigest() + ".lock"
        lock_file = (lock_root / filename).open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock_file.close()
            return None
        self.gpu_locks[uuid] = lock_file
        return lock_file

    def _release_gpu_lock(self, uuid: str | None) -> None:
        if not uuid:
            return
        lock_file = self.gpu_locks.pop(uuid, None)
        if lock_file is not None:
            lock_file.close()

    def _scheduler_identity(self, head: str) -> None:
        _atomic_write_json(
            self.store.state_dir / "scheduler.json",
            {
                "pid": os.getpid(),
                "started_at": utc_now_iso(),
                "git_commit": head,
                "poll_seconds": self.poll_seconds,
                "control_seconds": self.control_seconds,
            },
        )

    def _recover_gpu_locks(self) -> None:
        with self.store.connect() as connection:
            rows = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state IN "
                    "('starting','running','yielding','terminating','force_killing')"
                )
            )
        for item in rows:
            uuid = item["assigned_gpu_uuid"]
            if uuid and self._global_gpu_lock(str(uuid)) is None:
                self.store.event(
                    "GPU_LOCK_RECOVERY_FAILED",
                    queue_item_id=int(item["id"]),
                    payload={"gpu_uuid": uuid},
                )

    def _receipt_path(self, item_id: int, segment: int) -> Path:
        return _segment_dir(self.store, item_id, segment) / "exit.json"

    def _validated_yield_receipt(
        self,
        item: sqlite3.Row,
        *,
        runner_run_dir: str | None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Verify a workflow checkpoint receipt before allowing requeue."""

        receipt_path = _yield_receipt_path(
            self.store,
            int(item["id"]),
            int(item["segment"]),
        )
        if not receipt_path.is_file():
            return None, f"yield receipt is missing: {receipt_path}"
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return None, f"yield receipt is invalid: {exc}"
        if receipt.get("status") != "ready":
            return None, f"yield checkpoint reported {receipt.get('status')!r}"
        if receipt.get("request_id") != item["yield_request_id"]:
            return None, "yield receipt request identity does not match the queue item"
        try:
            receipt_item_id = int(receipt.get("queue_item_id", -1))
        except (TypeError, ValueError):
            return None, "yield receipt has no valid queue-item identity"
        if receipt_item_id != int(item["id"]):
            return None, "yield receipt queue-item identity does not match"
        checkpoint_value = receipt.get("checkpoint")
        metadata_value = receipt.get("checkpoint_metadata")
        if not checkpoint_value or not metadata_value:
            return None, "yield receipt omits checkpoint paths"
        checkpoint_source = Path(str(checkpoint_value))
        metadata_source = Path(str(metadata_value))
        try:
            if checkpoint_source.is_symlink():
                return None, f"yield checkpoint is missing or unsafe: {checkpoint_source}"
            if metadata_source.is_symlink():
                return None, (
                    "yield checkpoint metadata is missing or unsafe: "
                    f"{metadata_source}"
                )
            checkpoint = checkpoint_source.resolve()
            metadata = metadata_source.resolve()
            if not checkpoint.is_file():
                return None, f"yield checkpoint is missing or unsafe: {checkpoint}"
            if not metadata.is_file():
                return None, f"yield checkpoint metadata is missing or unsafe: {metadata}"
            if runner_run_dir:
                run_dir = Path(runner_run_dir)
                if not run_dir.is_absolute():
                    run_dir = self.store.repo_root / run_dir
                try:
                    checkpoint.relative_to(run_dir.resolve())
                except ValueError:
                    return None, f"yield checkpoint is outside runner directory {run_dir}"
                try:
                    metadata.relative_to(run_dir.resolve())
                except ValueError:
                    return None, (
                        "yield checkpoint metadata is outside runner directory "
                        f"{run_dir}"
                    )
            expected_bytes = receipt.get("checkpoint_bytes")
            try:
                expected_size = int(expected_bytes)
            except (TypeError, ValueError):
                return None, "yield receipt has no valid checkpoint size"
            if checkpoint.stat().st_size != expected_size:
                return None, "yield checkpoint size differs from its receipt"
            expected_hash = str(receipt.get("checkpoint_sha256") or "")
            actual_hash = _sha256_file(checkpoint)
            if not expected_hash or not hmac.compare_digest(actual_hash, expected_hash):
                return None, "yield checkpoint SHA-256 differs from its receipt"
            receipt["checkpoint"] = str(checkpoint)
            receipt["checkpoint_metadata"] = str(metadata)
            receipt["checkpoint_metadata_bytes"] = metadata.stat().st_size
            receipt["checkpoint_metadata_sha256"] = _sha256_file(metadata)
        except (OSError, RuntimeError) as exc:
            return None, f"yield checkpoint files could not be verified: {exc}"
        has_progress = "progress" in receipt
        progress, progress_error = _validated_yield_progress(receipt)
        if progress_error is not None:
            return None, progress_error
        if progress is not None:
            receipt["progress"] = progress
        step = receipt.get("step")
        step_label = "continuation" if has_progress else "optimizer"
        if isinstance(step, bool) or not isinstance(step, int):
            return None, f"yield receipt has no valid {step_label} step"
        if step < 0 or (progress is None and step < 1):
            return None, f"yield receipt has invalid {step_label} step {step}"
        return receipt, None

    @staticmethod
    def _yield_progress_text(yield_receipt: Mapping[str, Any]) -> str:
        """Format validated generic progress or the legacy training step."""

        progress = yield_receipt.get("progress")
        if isinstance(progress, Mapping):
            completed = f"{int(progress['completed']):,}"
            total = progress.get("total")
            amount = completed if total is None else f"{completed}/{int(total):,}"
            return f"{amount} {progress['unit']}"
        return f"step {int(yield_receipt['step']):,}"

    def _activate_pending_reservation(
        self,
        connection: sqlite3.Connection,
        item: sqlite3.Row,
    ) -> sqlite3.Row | None:
        reservation = connection.execute(
            "SELECT * FROM gpu_reservations WHERE queue_item_id = ? AND status = 'pending'",
            (item["id"],),
        ).fetchone()
        if reservation is None:
            return None
        starts = datetime.now(timezone.utc)
        expires = starts + timedelta(hours=int(reservation["duration_hours"]))
        connection.execute(
            "UPDATE gpu_reservations SET status = 'active', starts_at = ?, expires_at = ?, "
            "state_detail = ? WHERE id = ?",
            (
                starts.isoformat(timespec="seconds"),
                expires.isoformat(timespec="seconds"),
                "scheduler job checkpointed and GPU process exited",
                reservation["id"],
            ),
        )
        return connection.execute(
            "SELECT * FROM gpu_reservations WHERE id = ?", (reservation["id"],)
        ).fetchone()

    def _finalize_yield(
        self,
        item: sqlite3.Row,
        executor_receipt: dict[str, Any],
        runner_receipt: dict[str, str],
        yield_receipt: dict[str, Any],
    ) -> bool:
        """Requeue a still-current yield without clobbering a later termination."""

        wandb = yield_receipt.get("wandb") or {}
        progress_text = self._yield_progress_text(yield_receipt)
        event_payload = {
            "segment_finished": int(item["segment"]),
            "next_segment": int(item["segment"]) + 1,
            "checkpoint": yield_receipt["checkpoint"],
            "checkpoint_sha256": yield_receipt["checkpoint_sha256"],
            "step": int(yield_receipt["step"]),
            "wandb_id": wandb.get("id"),
            "reservation_id": None,
            "reservation_expires_at": None,
            "executor_receipt": executor_receipt,
        }
        if yield_receipt.get("progress") is not None:
            event_payload.update(
                {
                    "progress": yield_receipt["progress"],
                    "progress_text": progress_text,
                }
            )
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self.store.item(int(item["id"]), connection=connection)
            if (
                current["state"] != "yielding"
                or int(current["segment"]) != int(item["segment"])
                or current["yield_request_id"] != item["yield_request_id"]
            ):
                return False
            reservation = self._activate_pending_reservation(connection, current)
            if reservation is not None:
                event_payload["reservation_id"] = int(reservation["id"])
                event_payload["reservation_expires_at"] = reservation["expires_at"]
            connection.execute(
                """
                UPDATE queue_items SET state = 'queued', state_detail = ?, segment = segment + 1,
                    resume_front = 1, assigned_gpu_uuid = NULL, assigned_gpu_index = NULL,
                    pid = NULL, pgid = NULL, proc_start_ticks = NULL, return_code = NULL,
                    continuation_checkpoint = ?, continuation_checkpoint_sha256 = ?,
                    continuation_checkpoint_metadata = ?,
                    continuation_checkpoint_metadata_sha256 = ?, continuation_step = ?,
                    continuation_wandb_id = ?, runner_run_dir = ?,
                    runner_manifest_path = ?, rsync_pull_command = ?
                WHERE id = ?
                """,
                (
                    f"resume from verified {progress_text}",
                    str(yield_receipt["checkpoint"]),
                    str(yield_receipt["checkpoint_sha256"]),
                    str(yield_receipt["checkpoint_metadata"]),
                    str(yield_receipt["checkpoint_metadata_sha256"]),
                    int(yield_receipt["step"]),
                    wandb.get("id"),
                    runner_receipt.get("run_directory") or item["runner_run_dir"],
                    runner_receipt.get("manifest") or item["runner_manifest_path"],
                    runner_receipt.get("rsync_pull_command") or item["rsync_pull_command"],
                    item["id"],
                ),
            )
            self.store._set_meta(connection, "consecutive_failures", "0")
            self.store._event(
                connection,
                "EXPERIMENT_YIELDED_AND_REQUEUED",
                queue_item_id=int(item["id"]),
                payload=event_payload,
            )
        self._release_gpu_lock(item["assigned_gpu_uuid"])
        print(
            f"[{utc_now_iso()}] queue item {item['id']} yielded at {progress_text} "
            "and returned to the front of its priority band",
            flush=True,
        )
        return True

    def _finalize_item(self, item: sqlite3.Row, receipt: dict[str, Any] | None) -> None:
        prior_state = str(item["state"])
        return_code = receipt.get("return_code") if receipt is not None else None
        segment_dir = _segment_dir(
            self.store,
            int(item["id"]),
            int(item["segment"]),
        )
        runner_receipt = _read_runner_receipt(segment_dir / "launcher.log")
        if prior_state == "yielding" and receipt is not None:
            run_dir = runner_receipt.get("run_directory") or item["runner_run_dir"]
            yield_receipt, yield_error = self._validated_yield_receipt(
                item,
                runner_run_dir=run_dir,
            )
            if return_code == YIELD_EXIT_CODE and yield_receipt is not None:
                if self._finalize_yield(item, receipt, runner_receipt, yield_receipt):
                    return
                item = self.store.item(int(item["id"]))
                prior_state = str(item["state"])
        else:
            yield_error = None
        if prior_state == "force_killing":
            final_state = "force_killed"
        elif prior_state == "terminating" or return_code == 130:
            final_state = "interrupted"
        elif return_code == 0:
            final_state = "succeeded"
        else:
            final_state = "failed"
        detail = (
            yield_error
            if yield_error
            else None if receipt is not None else "process disappeared without an exit receipt"
        )
        with self.store.connect() as connection:
            connection.execute(
                """
                UPDATE queue_items SET state = ?, finished_at = ?, return_code = ?,
                    state_detail = ?, pid = NULL, pgid = NULL, proc_start_ticks = NULL,
                    runner_run_dir = ?, runner_manifest_path = ?, rsync_pull_command = ?
                WHERE id = ?
                """,
                (
                    final_state,
                    utc_now_iso(),
                    return_code,
                    detail,
                    runner_receipt.get("run_directory") or item["runner_run_dir"],
                    runner_receipt.get("manifest") or item["runner_manifest_path"],
                    runner_receipt.get("rsync_pull_command")
                    or item["rsync_pull_command"],
                    item["id"],
                ),
            )
            failures = int(self.store.get_meta("consecutive_failures", connection=connection))
            if final_state == "failed":
                failures += 1
            else:
                failures = 0
            self.store._set_meta(connection, "consecutive_failures", str(failures))
            self.store._event(
                connection,
                "EXPERIMENT_FINISHED",
                queue_item_id=int(item["id"]),
                payload={"state": final_state, "return_code": return_code, "receipt": receipt},
            )
            pending_reservation = connection.execute(
                "SELECT * FROM gpu_reservations WHERE queue_item_id = ? AND status = 'pending'",
                (item["id"],),
            ).fetchone()
            if pending_reservation is not None:
                connection.execute(
                    "UPDATE gpu_reservations SET status = 'failed', state_detail = ? WHERE id = ?",
                    (
                        detail or f"queue item finished as {final_state} before safe yield",
                        pending_reservation["id"],
                    ),
                )
                self.store._event(
                    connection,
                    "GPU_RESERVATION_FAILED",
                    queue_item_id=int(item["id"]),
                    payload={
                        "reservation_id": int(pending_reservation["id"]),
                        "reason": detail or final_state,
                    },
                )
            if final_state != "succeeded":
                dependents = list(
                    connection.execute(
                        """
                        SELECT child.id
                        FROM dependencies AS link
                        JOIN queue_items AS child ON child.id = link.queue_item_id
                        WHERE link.dependency_item_id = ? AND child.state IN ('queued','blocked')
                        """,
                        (item["id"],),
                    )
                )
                for dependent in dependents:
                    dependent_detail = (
                        f"dependency queue item {item['id']} finished as {final_state}"
                    )
                    connection.execute(
                        "UPDATE queue_items SET state = 'held', state_detail = ? WHERE id = ?",
                        (dependent_detail, dependent["id"]),
                    )
                    self.store._event(
                        connection,
                        "DEPENDENT_HELD",
                        queue_item_id=int(dependent["id"]),
                        payload={
                            "dependency_item_id": item["id"],
                            "dependency_state": final_state,
                        },
                    )
            if final_state == "failed" and failures >= self.max_consecutive_failures:
                reason = f"circuit breaker after {failures} consecutive child failures"
                self.store._set_meta(connection, "dispatch_paused", "1")
                self.store._set_meta(connection, "pause_reason", reason)
                self.store._event(connection, "CIRCUIT_BREAKER_OPENED", payload={"reason": reason})
            uuid = item["assigned_gpu_uuid"]
            if uuid:
                running_on_gpu = connection.execute(
                    "SELECT 1 FROM queue_items WHERE assigned_gpu_uuid = ? AND state IN "
                    "('starting','running','yielding','terminating','force_killing') LIMIT 1",
                    (uuid,),
                ).fetchone()
                allow = connection.execute(
                    "SELECT enabled, draining FROM gpu_allowlist WHERE uuid = ?", (uuid,)
                ).fetchone()
                if (
                    running_on_gpu is None
                    and allow is not None
                    and not allow["enabled"]
                    and allow["draining"]
                ):
                    connection.execute("DELETE FROM gpu_allowlist WHERE uuid = ?", (uuid,))
        self._release_gpu_lock(item["assigned_gpu_uuid"])
        cleanup_item_worktree(
            self.store,
            self.store.item(int(item["id"])),
            actor="scheduler",
        )
        print(
            f"[{utc_now_iso()}] queue item {item['id']} {item['experiment_id']}/a{item['attempt']} "
            f"finished as {final_state} (return_code={return_code})",
            flush=True,
        )

    def _reconcile_processes(self) -> None:
        with self.store.connect() as connection:
            items = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state IN "
                    "('starting','running','yielding','terminating','force_killing')"
                )
            )
        for item in items:
            item_id = int(item["id"])
            process = self.processes.get(item_id)
            if process is not None:
                process.poll()
            receipt_path = self._receipt_path(item_id, int(item["segment"]))
            if int(item["segment"]) == 1 and not receipt_path.exists():
                legacy = self.store.state_dir / "attempts" / str(item_id) / "exit.json"
                if legacy.exists():
                    receipt_path = legacy
            if receipt_path.is_file():
                try:
                    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if process is not None:
                    try:
                        process.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        # The receipt is durable and authoritative. A later
                        # parent-process exit will still reap this rare straggler.
                        pass
                self.processes.pop(item_id, None)
                self._finalize_item(item, receipt)
            elif not _pid_matches(item):
                self.processes.pop(item_id, None)
                self._finalize_item(item, None)

    def _reconcile_worktree_cleanup(self) -> None:
        """Retry exact cleanup left incomplete by a prior stop or Git error."""

        placeholders = ",".join("?" for _ in TERMINAL_STATES)
        with self.store.connect() as connection:
            items = list(
                connection.execute(
                    "SELECT queue_items.*, (SELECT created_at FROM events "
                    "WHERE events.queue_item_id = queue_items.id "
                    "AND events.event_type = 'EXPERIMENT_WORKTREE_CLEANUP_FAILED' "
                    "ORDER BY events.id DESC LIMIT 1) AS cleanup_last_attempt_at "
                    f"FROM queue_items WHERE state IN ({placeholders}) "
                    "AND git_ref IS NOT NULL AND worktree_removed_at IS NULL",
                    tuple(sorted(TERMINAL_STATES)),
                )
            )
        for item in items:
            last_attempt = item["cleanup_last_attempt_at"]
            if last_attempt:
                elapsed = datetime.now(timezone.utc) - datetime.fromisoformat(
                    str(last_attempt)
                )
                if elapsed.total_seconds() < WORKTREE_CLEANUP_RETRY_SECONDS:
                    continue
            cleanup_item_worktree(self.store, item, actor="scheduler")

    def _reconcile_yield_failures(self) -> None:
        """Return a still-running job to normal state when checkpointing failed."""

        with self.store.connect() as connection:
            items = list(
                connection.execute("SELECT * FROM queue_items WHERE state = 'yielding'")
            )
        for item in items:
            receipt_path = _yield_receipt_path(
                self.store,
                int(item["id"]),
                int(item["segment"]),
            )
            if not receipt_path.is_file() or not _pid_matches(item):
                continue
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if receipt.get("status") != "failed":
                continue
            progress, progress_error = _validated_yield_progress(receipt)
            if progress is not None and progress_error is None:
                receipt["progress"] = progress
                progress_text = self._yield_progress_text(receipt)
            else:
                progress_text = f"step {receipt.get('step')}"
            detail = (
                f"cooperative yield failed at {progress_text}: "
                f"{receipt.get('error', 'unknown checkpoint error')}"
            )
            with self.store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = self.store.item(int(item["id"]), connection=connection)
                if current["state"] != "yielding":
                    continue
                connection.execute(
                    """
                    UPDATE queue_items SET state = 'running', state_detail = ?,
                        yield_requested_at = NULL, yield_requested_by = NULL,
                        yield_request_id = NULL, yield_note = NULL,
                        yield_duration_hours = NULL
                    WHERE id = ?
                    """,
                    (detail, item["id"]),
                )
                reservation = connection.execute(
                    "SELECT * FROM gpu_reservations WHERE queue_item_id = ? "
                    "AND status = 'pending'",
                    (item["id"],),
                ).fetchone()
                if reservation is not None:
                    connection.execute(
                        "UPDATE gpu_reservations SET status = 'failed', state_detail = ? WHERE id = ?",
                        (detail, reservation["id"]),
                    )
                event_payload = {
                    "reservation_id": int(reservation["id"]) if reservation else None,
                    "receipt": receipt,
                }
                if progress is not None and progress_error is None:
                    event_payload["progress_text"] = progress_text
                self.store._event(
                    connection,
                    "COOPERATIVE_YIELD_FAILED",
                    queue_item_id=int(item["id"]),
                    payload=event_payload,
                )

    def _escalate_terminations(self) -> None:
        now = time.time()
        with self.store.connect() as connection:
            items = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state = 'terminating' "
                    "AND termination_stage = 'interrupt'"
                )
            )
        for item in items:
            sent_at = item["termination_signal_epoch"]
            if sent_at is None or now - float(sent_at) < self.termination_grace_seconds:
                continue
            signaled = _signal_item(item, signal.SIGTERM)
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET termination_stage = 'terminate', "
                    "termination_signal_epoch = ? WHERE id = ?",
                    (now, item["id"]),
                )
                self.store._event(
                    connection,
                    "TERMINATION_ESCALATED",
                    queue_item_id=int(item["id"]),
                    payload={"signal": "SIGTERM", "signal_sent": signaled},
                )

    def _repo_identity_matches(self, item: sqlite3.Row) -> tuple[bool, str | None]:
        if item["git_ref"]:
            worktree = _expected_worktree_path(self.store, item)
            if not worktree.is_dir():
                return False, f"isolated worktree is missing: {worktree}"
            return _worktree_identity(self.store, item, worktree)
        # A process admitted before schema v3 still belongs to the shared
        # checkout and retains the old safety rule until it is terminal.
        try:
            head = require_clean_git(self.store.repo_root)
        except QueueError as exc:
            return False, str(exc)
        if head != item["git_commit"]:
            return False, f"current HEAD {head} differs from queued commit {item['git_commit']}"
        card_path = self.store.repo_root / str(item["card_path"])
        if not card_path.is_file():
            return False, f"queued card no longer exists: {card_path}"
        current_hash = _sha256_bytes(card_path.read_bytes())
        if current_hash != item["card_sha256"]:
            return False, f"queued card hash changed: {card_path}"
        return True, None

    def _dependencies_satisfied(self, connection: sqlite3.Connection, item_id: int) -> bool:
        blocking = connection.execute(
            """
            SELECT dependency.id, dependency.state
            FROM dependencies AS link
            JOIN queue_items AS dependency ON dependency.id = link.dependency_item_id
            WHERE link.queue_item_id = ? AND dependency.state != 'succeeded'
            LIMIT 1
            """,
            (item_id,),
        ).fetchone()
        return blocking is None

    def _next_item(self) -> sqlite3.Row | None:
        with self.store.connect() as connection:
            candidates = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state = 'queued' "
                    "ORDER BY priority DESC, resume_front DESC, id ASC"
                )
            )
            for item in candidates:
                if self._dependencies_satisfied(connection, int(item["id"])):
                    return item
        return None

    def _idle(self, gpu: GpuSnapshot) -> bool:
        return (
            not gpu.compute_pids
            and gpu.free_memory_fraction >= self.min_free_memory_fraction
            and gpu.utilization_percent <= self.max_utilization_percent
        )

    def _launch(self, item: sqlite3.Row, gpu: GpuSnapshot) -> bool:
        try:
            worktree = prepare_item_worktree(self.store, item)
            item = self.store.item(int(item["id"]))
            execution_root, effective_command, execution_environment = (
                _item_execution_context(self.store, item, os.environ)
            )
        except ContinuationIntegrityError as exc:
            detail = str(exc)
            with self.store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                cursor = connection.execute(
                    "UPDATE queue_items SET state = 'held', state_detail = ? "
                    "WHERE id = ? AND state = 'queued'",
                    (detail, item["id"]),
                )
                if cursor.rowcount == 1:
                    self.store._event(
                        connection,
                        "QUEUE_CONTINUATION_INTEGRITY_HELD",
                        queue_item_id=int(item["id"]),
                        payload={"reason": detail},
                    )
            print(
                f"[{utc_now_iso()}] continuation held: {detail}",
                file=sys.stderr,
                flush=True,
            )
            return False
        except QueueError as exc:
            detail = str(exc)
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state_detail = ? WHERE id = ? AND state = 'queued'",
                    (detail, item["id"]),
                )
                self.store._event(
                    connection,
                    "EXPERIMENT_WORKTREE_PREPARATION_FAILED",
                    queue_item_id=int(item["id"]),
                    payload={"reason": detail},
                )
            print(
                f"[{utc_now_iso()}] worktree preparation deferred: {detail}",
                file=sys.stderr,
                flush=True,
            )
            return False
        matches, detail = self._repo_identity_matches(item)
        if not matches:
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state = 'blocked', state_detail = ? WHERE id = ?",
                    (detail, item["id"]),
                )
                self.store._set_meta(connection, "dispatch_paused", "1")
                self.store._set_meta(connection, "pause_reason", detail or "repository identity mismatch")
                self.store._event(
                    connection,
                    "EXPERIMENT_BLOCKED",
                    queue_item_id=int(item["id"]),
                    payload={"reason": detail},
                )
            print(f"[{utc_now_iso()}] dispatch paused: {detail}", file=sys.stderr, flush=True)
            return False
        free_gib = shutil.disk_usage(self.store.repo_root).free / (1024**3)
        if free_gib < self.min_free_disk_gib:
            reason = (
                f"only {free_gib:.2f} GiB free under {self.store.repo_root}; "
                f"minimum is {self.min_free_disk_gib:.2f} GiB"
            )
            set_dispatch_paused(self.store, True, reason)
            print(f"[{utc_now_iso()}] dispatch paused: {reason}", file=sys.stderr, flush=True)
            return False
        if self._global_gpu_lock(gpu.uuid) is None:
            self.store.event(
                "GPU_ADVISORY_LOCK_BUSY",
                payload={"gpu_uuid": gpu.uuid, "gpu_index": gpu.index},
            )
            return False
        refreshed = {snapshot.uuid: snapshot for snapshot in self.gpu_provider()}.get(gpu.uuid)
        if refreshed is None or not self._idle(refreshed):
            self._release_gpu_lock(gpu.uuid)
            return False

        item_id = int(item["id"])
        segment = int(item["segment"])
        attempt_dir = _segment_dir(self.store, item_id, segment)
        attempt_dir.mkdir(parents=True, exist_ok=True)
        launcher_log = (attempt_dir / "launcher.log").open("ab", buffering=0)
        env = execution_environment
        env["CUDA_VISIBLE_DEVICES"] = gpu.uuid
        env["EXPERIMENT_QUEUE_ITEM_ID"] = str(item_id)
        env["EXPERIMENT_QUEUE_GPU_UUID"] = gpu.uuid
        executor_payload_path = attempt_dir / "executor.json"
        _atomic_write_json(
            executor_payload_path,
            {
                "command": effective_command,
                "cwd": str(execution_root),
                "receipt_path": str(self._receipt_path(item_id, segment)),
                "receipt": {
                    "schema_version": 2,
                    "queue_item_id": item_id,
                    "experiment_id": item["experiment_id"],
                    "attempt": item["attempt"],
                    "segment": segment,
                    "git_commit": item["git_commit"],
                    "git_ref": item["git_ref"],
                    "worktree": str(execution_root),
                    "command_sha256": _sha256_bytes(effective_command.encode("utf-8")),
                    "gpu_uuid": gpu.uuid,
                },
            },
        )
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE queue_items SET state = 'starting', assigned_gpu_uuid = ?,
                    assigned_gpu_index = ?, state_detail = NULL, resume_front = 0 WHERE id = ?
                    AND state = 'queued'
                """,
                (gpu.uuid, gpu.index, item_id),
            )
            if cursor.rowcount != 1:
                self._release_gpu_lock(gpu.uuid)
                return False
            self.store._event(
                connection,
                "EXPERIMENT_STARTING",
                queue_item_id=item_id,
                payload={
                    "gpu_uuid": gpu.uuid,
                    "gpu_index": gpu.index,
                    "gpu_name": gpu.name,
                    "free_memory_fraction": gpu.free_memory_fraction,
                    "utilization_percent": gpu.utilization_percent,
                    "segment": segment,
                },
            )
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    _DURABLE_EXECUTOR_SOURCE,
                    str(executor_payload_path),
                ],
                cwd=execution_root,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=launcher_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as exc:
            self._release_gpu_lock(gpu.uuid)
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state = 'failed', finished_at = ?, state_detail = ? WHERE id = ?",
                    (utc_now_iso(), f"could not launch queue executor: {exc}", item_id),
                )
                self.store._set_meta(connection, "dispatch_paused", "1")
                self.store._set_meta(connection, "pause_reason", str(exc))
                self.store._event(
                    connection,
                    "EXECUTOR_LAUNCH_FAILED",
                    queue_item_id=item_id,
                    payload={"error": str(exc)},
                )
            cleanup_item_worktree(self.store, self.store.item(item_id), actor="scheduler")
            return False
        finally:
            launcher_log.close()
        start_ticks = _process_start_ticks(process.pid)
        signal_after_launch: int | None = None
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self.store.item(item_id, connection=connection)
            current_state = str(current["state"])
            if current_state == "starting":
                next_state = "running"
            elif current_state == "terminating":
                next_state = "terminating"
                signal_after_launch = signal.SIGINT
            elif current_state == "force_killing":
                next_state = "force_killing"
                signal_after_launch = signal.SIGKILL
            else:
                process.kill()
                process.wait()
                self._release_gpu_lock(gpu.uuid)
                raise QueueError(
                    f"queue item {item_id} changed to unexpected state {current_state!r} during launch"
                )
            connection.execute(
                """
                UPDATE queue_items SET state = ?, pid = ?, pgid = ?, proc_start_ticks = ?,
                    started_at = ? WHERE id = ?
                """,
                (next_state, process.pid, process.pid, start_ticks, utc_now_iso(), item_id),
            )
            self.store._event(
                connection,
                "EXPERIMENT_LAUNCHED",
                queue_item_id=item_id,
                payload={
                    "pid": process.pid,
                    "pgid": process.pid,
                    "gpu_uuid": gpu.uuid,
                    "git_commit": item["git_commit"],
                    "worktree": str(worktree),
                },
            )
        self.processes[item_id] = process
        if signal_after_launch is not None:
            refreshed_item = self.store.item(item_id)
            signaled = _signal_item(refreshed_item, signal_after_launch)
            self.store.event(
                "DEFERRED_TERMINATION_SIGNAL_SENT",
                queue_item_id=item_id,
                payload={
                    "signal": signal.Signals(signal_after_launch).name,
                    "signal_sent": signaled,
                },
            )
        print(
            f"[{utc_now_iso()}] launched queue item {item_id} {item['experiment_id']}/a{item['attempt']} "
            f"on GPU {gpu.index} ({gpu.uuid}) pid={process.pid}",
            flush=True,
        )
        return True

    def _detect_contention(self, snapshots: Sequence[GpuSnapshot]) -> None:
        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        with self.store.connect() as connection:
            running = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state IN "
                    "('starting','running','yielding','terminating','force_killing') "
                    "AND assigned_gpu_uuid IS NOT NULL"
                )
            )
            for item in running:
                gpu = by_uuid.get(str(item["assigned_gpu_uuid"]))
                if gpu is None or not gpu.compute_pids:
                    continue
                pgid = int(item["pgid"]) if item["pgid"] is not None else None
                foreign: list[int] = []
                for pid in gpu.compute_pids:
                    try:
                        process_group = os.getpgid(pid)
                    except (ProcessLookupError, PermissionError):
                        process_group = None
                    if pgid is None or process_group != pgid:
                        foreign.append(pid)
                if foreign and not item["contention_detected"]:
                    connection.execute(
                        "UPDATE queue_items SET contention_detected = 1, state_detail = ? WHERE id = ?",
                        (f"foreign GPU process IDs observed: {foreign}", item["id"]),
                    )
                    self.store._event(
                        connection,
                        "GPU_CONTENTION_DETECTED",
                        queue_item_id=int(item["id"]),
                        payload={
                            "gpu_uuid": gpu.uuid,
                            "foreign_pids": foreign,
                            "memory_used_mib": gpu.memory_used_mib,
                            "utilization_percent": gpu.utilization_percent,
                        },
                    )

    def _poll_and_dispatch(self) -> None:
        self._check_running_repo_identity()
        try:
            snapshots = self.gpu_provider()
        except QueueError as exc:
            set_dispatch_paused(self.store, True, f"GPU telemetry failed: {exc}")
            print(f"[{utc_now_iso()}] GPU telemetry failed; dispatch paused: {exc}", file=sys.stderr)
            return
        self._refresh_allowlist_identities(snapshots)
        self._detect_contention(snapshots)
        if self.store.get_meta("dispatch_paused") == "1":
            return
        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        with self.store.connect() as connection:
            allow = list(
                connection.execute(
                    "SELECT * FROM gpu_allowlist WHERE enabled = 1 ORDER BY CAST(last_index AS INTEGER), uuid"
                )
            )
            assigned = _running_gpu_uuids(connection)
            reserved = _open_reserved_gpu_uuids(connection)
        available = [
            by_uuid[row["uuid"]]
            for row in allow
            if row["uuid"] in by_uuid
            and row["uuid"] not in assigned
            and row["uuid"] not in reserved
            and self._idle(by_uuid[row["uuid"]])
        ]
        for gpu in available:
            item = self._next_item()
            if item is None:
                break
            if not self._launch(item, gpu):
                if self.store.get_meta("dispatch_paused") == "1":
                    break

    def _check_running_repo_identity(self) -> None:
        """Pause only when an active item's own execution checkout changes."""

        with self.store.connect() as connection:
            running = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state IN "
                    "('starting','running','yielding','terminating','force_killing')"
                )
            )
        if not running:
            return
        mismatched = [
            (item, detail)
            for item in running
            for matches, detail in (self._repo_identity_matches(item),)
            if not matches
        ]
        if not mismatched:
            return
        reason = "; ".join(
            f"item {item['id']}: {detail or 'execution checkout identity mismatch'}"
            for item, detail in mismatched
        )
        with self.store.connect() as connection:
            self.store._set_meta(connection, "dispatch_paused", "1")
            self.store._set_meta(connection, "pause_reason", reason)
            for item, item_reason in mismatched:
                if not item["repo_drift_detected"]:
                    connection.execute(
                        "UPDATE queue_items SET repo_drift_detected = 1, "
                        "state_detail = COALESCE(state_detail, ?) WHERE id = ?",
                        (item_reason or reason, item["id"]),
                    )
                    self.store._event(
                        connection,
                        "REPOSITORY_DRIFT_DETECTED",
                        queue_item_id=int(item["id"]),
                        payload={"reason": item_reason or reason},
                    )

    def _refresh_allowlist_identities(self, snapshots: Sequence[GpuSnapshot]) -> None:
        """Keep host indices current while retaining UUIDs as stable identities."""

        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        with self.store.connect() as connection:
            rows = list(connection.execute("SELECT * FROM gpu_allowlist"))
            for row in rows:
                gpu = by_uuid.get(str(row["uuid"]))
                if gpu is None:
                    continue
                old_index = str(row["last_index"])
                connection.execute(
                    "UPDATE gpu_allowlist SET last_index = ?, name = ?, updated_at = ? WHERE uuid = ?",
                    (gpu.index, gpu.name, utc_now_iso(), gpu.uuid),
                )
                if old_index != gpu.index:
                    self.store._event(
                        connection,
                        "GPU_HOST_INDEX_CHANGED",
                        payload={"gpu_uuid": gpu.uuid, "old_index": old_index, "new_index": gpu.index},
                    )

    def run_iteration(self, *, force_gpu_poll: bool = False) -> None:
        """Run one recovery/control cycle and optionally one GPU scheduling pass."""

        expire_reservations(self.store)
        self._reconcile_processes()
        self._reconcile_worktree_cleanup()
        self._reconcile_yield_failures()
        self._escalate_terminations()
        now = time.monotonic()
        if force_gpu_poll or now - self._last_gpu_poll >= self.poll_seconds:
            self._last_gpu_poll = now
            self._poll_and_dispatch()

    def run(self, *, once: bool = False) -> None:
        """Hold the scheduler lock and serve until interrupted."""

        head = require_clean_git(self.store.repo_root)
        self._lock_scheduler()
        stop_signal: str | None = None
        scheduler_started = False

        def stop_handler(signum: int, _frame: Any) -> None:
            nonlocal stop_signal
            self._stop = True
            stop_signal = signal.Signals(signum).name

        previous_handlers: dict[int, Any] = {}
        try:
            self.store.set_meta(
                "termination_grace_seconds", str(self.termination_grace_seconds)
            )
            self._scheduler_identity(head)
            self._recover_gpu_locks()
            self.store.event(
                "SCHEDULER_STARTED", payload={"pid": os.getpid(), "git_commit": head}
            )
            scheduler_started = True
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.signal(signum, stop_handler)
            while not self._stop:
                self.run_iteration(force_gpu_poll=once)
                if once:
                    break
                time.sleep(self.control_seconds)
        finally:
            had_active_error = sys.exc_info()[0] is not None
            cleanup_error: BaseException | None = None
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
            if scheduler_started and stop_signal is not None:
                try:
                    self.store.event(
                        "SCHEDULER_STOP_REQUESTED", payload={"signal": stop_signal}
                    )
                except (OSError, QueueError, sqlite3.Error) as exc:
                    cleanup_error = exc
            if scheduler_started:
                try:
                    self.store.event("SCHEDULER_STOPPED", payload={"pid": os.getpid()})
                except (OSError, QueueError, sqlite3.Error) as exc:
                    cleanup_error = cleanup_error or exc
            identity_path = self.store.state_dir / "scheduler.json"
            try:
                if identity_path.exists():
                    identity_path.unlink()
            except OSError as exc:
                cleanup_error = cleanup_error or exc
            for lock_file in self.gpu_locks.values():
                lock_file.close()
            self.gpu_locks.clear()
            if self._scheduler_lock is not None:
                self._scheduler_lock.close()
                self._scheduler_lock = None
            if cleanup_error is not None:
                if had_active_error:
                    print(
                        f"warning: scheduler cleanup could not update state: {cleanup_error}",
                        file=sys.stderr,
                    )
                else:
                    raise cleanup_error


def execute_item(store: QueueStore, item_id: int) -> int:
    """Internal durable executor that writes an exit receipt even if the scheduler stops."""

    item = store.item(item_id)
    if item["state"] not in RUNNING_STATES:
        raise QueueError(f"queue item {item_id} is {item['state']}; executor requires a running state")
    execution_root, command_text, child_environment = _item_execution_context(
        store,
        item,
        os.environ,
    )
    segment = int(item["segment"])
    attempt_dir = _segment_dir(store, item_id, segment)
    attempt_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = attempt_dir / "exit.json"
    signals_received: list[str] = []
    child: subprocess.Popen[bytes] | None = None

    def forward(signum: int, _frame: Any) -> None:
        signals_received.append(signal.Signals(signum).name)
        if child is not None and child.poll() is None:
            try:
                child.send_signal(signum)
            except ProcessLookupError:
                pass

    started_at = utc_now_iso()
    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, forward)
    try:
        child = subprocess.Popen(
            ["/bin/bash", "-lc", command_text],
            cwd=execution_root,
            env=child_environment,
        )
        raw_return_code = child.wait()
        return_code = 128 + abs(raw_return_code) if raw_return_code < 0 else raw_return_code
    except OSError as exc:
        return_code = 127
        signals_received.append(f"launch_error:{exc}")
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    receipt = {
        "schema_version": 2,
        "queue_item_id": item_id,
        "experiment_id": item["experiment_id"],
        "attempt": item["attempt"],
        "segment": segment,
        "git_commit": item["git_commit"],
        "git_ref": item["git_ref"],
        "worktree": str(execution_root),
        "command_sha256": _sha256_bytes(command_text.encode("utf-8")),
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "return_code": return_code,
        "signals_received": signals_received,
        "gpu_uuid": os.environ.get("EXPERIMENT_QUEUE_GPU_UUID"),
    }
    _atomic_write_json(receipt_path, receipt)
    return int(return_code)


def _format_gpu_status(store: QueueStore, snapshots: Sequence[GpuSnapshot] | None) -> str:
    by_uuid = {gpu.uuid: gpu for gpu in snapshots or []}
    with store.connect() as connection:
        rows = list(connection.execute("SELECT * FROM gpu_allowlist ORDER BY last_index, uuid"))
        assigned = {
            str(row["assigned_gpu_uuid"]): row
            for row in connection.execute(
                "SELECT id, experiment_id, assigned_gpu_uuid FROM queue_items WHERE state IN "
                "('starting','running','yielding','terminating','force_killing')"
            )
            if row["assigned_gpu_uuid"]
        }
        reservations = {
            str(row["gpu_uuid"]): row
            for row in connection.execute(
                "SELECT * FROM gpu_reservations WHERE status IN ('pending', 'active')"
            )
        }
    lines = ["INDEX  UUID                  STATE           JOB       MEMORY-FREE  UTIL  COMPUTE-PIDS"]
    for row in rows:
        gpu = by_uuid.get(str(row["uuid"]))
        job = assigned.get(str(row["uuid"]))
        reservation = reservations.get(str(row["uuid"]))
        if row["draining"]:
            state = "draining"
        elif reservation and reservation["status"] == "active":
            state = "reserved"
        elif reservation:
            state = "yield-pending"
        elif job:
            state = "scheduler-busy"
        elif gpu is None:
            state = "unobserved"
        elif gpu.compute_pids:
            state = "externally-busy"
        else:
            state = "no-compute-pid"
        lines.append(
            f"{row['last_index']:<6} {str(row['uuid'])[:20]:<20} {state:<15} "
            f"{str(job['id']) if job else '-':<9} "
            f"{f'{gpu.free_memory_fraction:.1%}' if gpu else '-':<12} "
            f"{f'{gpu.utilization_percent:.0f}%' if gpu else '-':<5} "
            f"{','.join(str(pid) for pid in gpu.compute_pids) if gpu and gpu.compute_pids else '-'}"
        )
    return "\n".join(lines)


def format_status(store: QueueStore, *, as_json: bool = False) -> str:
    """Render queue membership without reading project STATUS.md."""

    items = store.list_items()
    with store.connect() as connection:
        dependency_rows = list(
            connection.execute(
                """
                SELECT link.queue_item_id, dependency.id, dependency.state
                FROM dependencies AS link
                JOIN queue_items AS dependency ON dependency.id = link.dependency_item_id
                ORDER BY link.queue_item_id, dependency.id
                """
            )
        )
    dependencies: dict[int, list[dict[str, Any]]] = {}
    for row in dependency_rows:
        dependencies.setdefault(int(row["queue_item_id"]), []).append(
            {"item_id": int(row["id"]), "state": str(row["state"])}
        )
    item_payloads: list[dict[str, Any]] = []
    for item in items:
        item_payload = dict(item)
        item_payload["dependencies"] = dependencies.get(int(item["id"]), [])
        item_payloads.append(item_payload)
    payload = {
        "dispatch_paused": store.get_meta("dispatch_paused") == "1",
        "pause_reason": store.get_meta("pause_reason"),
        "consecutive_failures": int(store.get_meta("consecutive_failures")),
        "items": item_payloads,
        "reservations": [dict(row) for row in list_reservations(store)],
    }
    if as_json:
        return json.dumps(payload, indent=2, sort_keys=True)
    lines = [
        f"dispatch: {'PAUSED' if payload['dispatch_paused'] else 'active'}"
        + (f" ({payload['pause_reason']})" if payload["pause_reason"] else ""),
        "ID   EXPERIMENT/ATTEMPT  STATE          PRIORITY  GPU    PID      DETAIL",
    ]
    for item in item_payloads:
        label = f"{item['experiment_id']}/a{item['attempt']}"
        waiting = [
            f"{dependency['item_id']}:{dependency['state']}"
            for dependency in item["dependencies"]
            if dependency["state"] != "succeeded"
        ]
        detail = str(item["state_detail"] or "")
        if item["worktree_cleanup_error"]:
            cleanup_detail = f"worktree cleanup pending: {item['worktree_cleanup_error']}"
            detail = f"{detail}; {cleanup_detail}" if detail else cleanup_detail
        elif item["worktree_removed_at"]:
            isolation_detail = f"commit {str(item['git_commit'])[:12]} · worktree cleaned"
            detail = f"{detail}; {isolation_detail}" if detail else isolation_detail
        elif item["worktree_path"]:
            isolation_detail = f"commit {str(item['git_commit'])[:12]} · isolated worktree ready"
            detail = f"{detail}; {isolation_detail}" if detail else isolation_detail
        elif item["git_ref"]:
            isolation_detail = f"commit {str(item['git_commit'])[:12]} · pinned"
            detail = f"{detail}; {isolation_detail}" if detail else isolation_detail
        if waiting:
            dependency_detail = "waiting on " + ",".join(waiting)
            detail = f"{detail}; {dependency_detail}" if detail else dependency_detail
        lines.append(
            f"{item['id']:<4} {label:<19} {item['state']:<14} {item['priority']:<9} "
            f"{str(item['assigned_gpu_index'] or '-'):<6} {str(item['pid'] or '-'):<8} "
            f"{detail}"
        )
    return "\n".join(lines)


def export_receipt(store: QueueStore, destination: Path | None = None) -> Path:
    """Export a consistent JSON summary suitable for synchronization and audit."""

    target = destination or store.state_dir / "queue_receipt.json"
    with store.connect() as connection:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now_iso(),
            "repo_root": str(store.repo_root),
            "metadata": {
                row["key"]: row["value"] for row in connection.execute("SELECT * FROM metadata")
            },
            "gpu_allowlist": [
                dict(row) for row in connection.execute("SELECT * FROM gpu_allowlist ORDER BY uuid")
            ],
            "gpu_reservations": [
                dict(row)
                for row in connection.execute("SELECT * FROM gpu_reservations ORDER BY id")
            ],
            "queue_items": [
                dict(row) for row in connection.execute("SELECT * FROM queue_items ORDER BY id")
            ],
            "dependencies": [
                dict(row) for row in connection.execute("SELECT * FROM dependencies ORDER BY queue_item_id")
            ],
            "events": [dict(row) for row in connection.execute("SELECT * FROM events ORDER BY id")],
        }
    _atomic_write_json(target, payload)
    return target


def format_pull_commands(store: QueueStore) -> str:
    """Print completed runner-generated rsync commands without executing them."""

    with store.connect() as connection:
        rows = list(
            connection.execute(
                "SELECT id, experiment_id, attempt, rsync_pull_command FROM queue_items "
                "WHERE rsync_pull_command IS NOT NULL ORDER BY id"
            )
        )
    if not rows:
        return "no runner-generated pull commands are recorded yet"
    lines: list[str] = []
    for row in rows:
        lines.append(f"# queue item {row['id']}: {row['experiment_id']}/a{row['attempt']}")
        lines.append(str(row["rsync_pull_command"]))
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the explicit queue-control command-line interface."""

    parser = argparse.ArgumentParser(
        description=(
            "Explicitly manage and run approved experiment cards on operator-selected, "
            "unmanaged GPUs. No card or STATUS.md entry is enqueued automatically."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Clean repository checkout containing cards and runner code. Default: current directory.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help=(
            "Absolute durable state directory for the SQLite database, logs, and receipts. "
            "Required unless EXPERIMENT_QUEUE_STATE_DIR is set."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    add = subparsers.add_parser("add", help="Explicitly add one committed experiment card.")
    add.add_argument("experiment_id", help="Stable experiment ID, for example WCG-017.")
    add.add_argument("--card", type=Path, help="Optional card path; defaults to docs/experiments/<ID>.md.")
    add.add_argument("--priority", type=int, default=0, help="Higher integers dispatch first. Default: 0.")
    add.add_argument(
        "--after",
        type=int,
        action="append",
        default=[],
        help="Require the named queue item to succeed first. Repeat for multiple dependencies.",
    )
    add.add_argument("--hold", action="store_true", help="Add the item held instead of dispatchable.")
    add.add_argument(
        "--new-attempt",
        action="store_true",
        help="Explicitly authorize another attempt after this experiment was previously launched.",
    )
    add.add_argument(
        "--preemptible",
        action="store_true",
        help=(
            "Allow the cooperative checkpoint-and-requeue yield workflow. Use only "
            "for commands whose trainer supports experiment-queue yield receipts."
        ),
    )

    for name, help_text in (
        ("remove", "Remove a pending queue item without deleting history."),
        ("hold", "Temporarily hold a queued or blocked item."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
        command.add_argument("--reason", help="Reason recorded in the event history.")
    release = subparsers.add_parser("release", help="Release a held or blocked item.")
    release.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
    priority = subparsers.add_parser(
        "priority",
        help="Change a pending or resumable active item's dispatch priority.",
    )
    priority.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
    priority.add_argument("value", type=int, help="New integer priority; larger values dispatch first.")

    preempt = subparsers.add_parser(
        "preempt",
        help="Cooperatively checkpoint and requeue one preemptible running item.",
    )
    preempt.add_argument("item_id", type=int, help="Exact numeric running queue item ID.")
    preempt.add_argument(
        "--reason",
        help=(
            "Operator reason preserved in queue history. The request does not reserve "
            "the released GPU."
        ),
    )

    pause = subparsers.add_parser("pause", help="Pause new dispatch without stopping running work.")
    pause.add_argument("--reason", help="Reason recorded in queue state.")
    subparsers.add_parser("resume", help="Resume new dispatch after an explicit review.")

    status = subparsers.add_parser("status", help="Show explicit queue membership and state.")
    status.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")

    gpu = subparsers.add_parser("gpus", help="Manage the mutable operator-owned GPU allowlist.")
    gpu_subparsers = gpu.add_subparsers(dest="gpu_action", required=True)
    for action in ("set", "add", "remove"):
        command = gpu_subparsers.add_parser(action, help=f"{action.title()} allowed GPUs.")
        command.add_argument(
            "identifiers",
            nargs="*" if action == "set" else "+",
            help="GPU indices, UUIDs, or unambiguous UUID prefixes; comma-separated values are accepted.",
        )
        command.add_argument(
            "--nvidia-smi",
            default="nvidia-smi",
            help="nvidia-smi executable used to resolve identities. Default: nvidia-smi.",
        )
    gpu_show = gpu_subparsers.add_parser("show", help="Show allowed and draining GPU states.")
    gpu_show.add_argument(
        "--nvidia-smi",
        default="nvidia-smi",
        help="nvidia-smi executable used for live state. Default: nvidia-smi.",
    )

    terminate = subparsers.add_parser("terminate", help="Gracefully interrupt a running queue item.")
    terminate.add_argument("item_id", type=int, help="Exact numeric running queue item ID.")
    terminate.add_argument("--reason", help="Operator reason preserved in the event history.")
    kill = subparsers.add_parser("kill", help="Force-kill an unresponsive running queue item.")
    kill.add_argument("item_id", type=int, help="Exact numeric running queue item ID.")
    kill.add_argument("--reason", help="Operator reason preserved in the event history.")
    kill.add_argument(
        "--yes",
        action="store_true",
        help="Required acknowledgement that SIGKILL prevents graceful cleanup.",
    )

    serve = subparsers.add_parser("serve", help="Run the foreground GPU polling scheduler.")
    serve.add_argument(
        "--poll-seconds",
        type=float,
        default=60.0,
        help="GPU poll interval. Default: 60 seconds.",
    )
    serve.add_argument(
        "--control-seconds",
        type=float,
        default=1.0,
        help="Queue-control poll interval. Default: 1 second.",
    )
    serve.add_argument(
        "--min-free-memory-fraction",
        type=float,
        default=0.95,
        help="Minimum observed free GPU-memory fraction required to launch. Default: 0.95.",
    )
    serve.add_argument(
        "--max-utilization-percent",
        type=float,
        default=5.0,
        help="Maximum observed GPU utilization permitted at launch. Default: 5.",
    )
    serve.add_argument(
        "--min-free-disk-gib",
        type=float,
        default=50.0,
        help="Pause before launch when repository filesystem free space is lower. Default: 50 GiB.",
    )
    serve.add_argument(
        "--termination-grace-seconds",
        type=float,
        default=30.0,
        help="Wait after SIGINT before escalating to SIGTERM. Default: 30 seconds.",
    )
    serve.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=2,
        help="Pause after this many consecutive failed children. Default: 2.",
    )
    serve.add_argument(
        "--nvidia-smi",
        default="nvidia-smi",
        help="nvidia-smi executable used for polling. Default: nvidia-smi.",
    )
    serve.add_argument("--once", action="store_true", help="Run one recovery and scheduling pass, then exit.")

    receipt = subparsers.add_parser("receipt", help="Export the complete queue and event history as JSON.")
    receipt.add_argument(
        "--output",
        type=Path,
        help="Destination path. Default: <state-dir>/queue_receipt.json.",
    )

    subparsers.add_parser(
        "pull-commands",
        help="Print, but do not execute, every recorded runner-generated rsync pull command.",
    )

    internal = subparsers.add_parser(
        "_execute", help="Internal durable attempt executor; do not invoke manually."
    )
    internal.add_argument("item_id", type=int)
    return parser


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    repo_root = args.repo_root.resolve()
    return repo_root, resolve_state_dir(args.state_dir)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one queue control action and return a process exit code."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        repo_root, state_dir = _resolve_paths(args)
        store = QueueStore(state_dir, repo_root)
        if args.action == "add":
            item_id = add_experiment(
                store,
                args.experiment_id,
                card_path=args.card,
                priority=args.priority,
                dependency_ids=args.after,
                held=args.hold,
                new_attempt=args.new_attempt,
                preemptible=args.preemptible,
            )
            item = store.item(item_id)
            print(f"added queue item {item_id}: {item['experiment_id']}/a{item['attempt']} ({item['state']})")
        elif args.action == "remove":
            remove_item(store, args.item_id, args.reason)
            print(f"removed pending queue item {args.item_id}")
        elif args.action == "hold":
            hold_item(store, args.item_id, args.reason)
            print(f"held queue item {args.item_id}")
        elif args.action == "release":
            release_item(store, args.item_id)
            print(f"released queue item {args.item_id}")
        elif args.action == "priority":
            set_priority(store, args.item_id, args.value)
            print(f"queue item {args.item_id} priority is now {args.value}")
        elif args.action == "preempt":
            request_id = request_preemption(
                store,
                args.item_id,
                reason=args.reason,
            )
            print(
                f"manual preemption {request_id} recorded for queue item {args.item_id}; "
                "the job will checkpoint, exit, and rejoin the front of its priority band"
            )
        elif args.action == "pause":
            set_dispatch_paused(store, True, args.reason)
            print("new dispatch paused; running jobs were not changed")
        elif args.action == "resume":
            set_dispatch_paused(store, False)
            print("new dispatch resumed")
        elif args.action == "status":
            print(format_status(store, as_json=args.json))
        elif args.action == "gpus":
            if args.gpu_action == "show":
                try:
                    snapshots = query_gpus(args.nvidia_smi)
                except QueueError as exc:
                    snapshots = None
                    print(f"warning: live GPU state unavailable: {exc}", file=sys.stderr)
                print(_format_gpu_status(store, snapshots))
            else:
                if args.gpu_action == "set" and not args.identifiers:
                    snapshots = []
                else:
                    try:
                        snapshots = query_gpus(args.nvidia_smi)
                    except QueueError:
                        if args.gpu_action != "remove":
                            raise
                        snapshots = []
                update_gpu_allowlist(
                    store,
                    args.gpu_action,
                    args.identifiers,
                    snapshots=snapshots,
                )
                print(_format_gpu_status(store, snapshots))
        elif args.action == "terminate":
            signaled = request_termination(
                store, args.item_id, reason=args.reason, force=False
            )
            print(
                f"termination recorded for queue item {args.item_id}; "
                f"SIGINT {'sent' if signaled else 'will be reconciled by the scheduler'}"
            )
        elif args.action == "kill":
            if not args.yes:
                raise QueueError("force-kill requires --yes because graceful cleanup will not run")
            signaled = request_termination(store, args.item_id, reason=args.reason, force=True)
            print(
                f"force-kill recorded for queue item {args.item_id}; "
                f"SIGKILL {'sent' if signaled else 'will be reconciled by the scheduler'}"
            )
        elif args.action == "serve":
            if args.poll_seconds <= 0 or args.control_seconds <= 0:
                raise QueueError("poll and control intervals must be positive")
            if not 0.0 <= args.min_free_memory_fraction <= 1.0:
                raise QueueError("--min-free-memory-fraction must be between 0 and 1")
            if args.max_consecutive_failures < 1:
                raise QueueError("--max-consecutive-failures must be at least 1")
            scheduler = Scheduler(
                store,
                poll_seconds=args.poll_seconds,
                control_seconds=args.control_seconds,
                min_free_memory_fraction=args.min_free_memory_fraction,
                max_utilization_percent=args.max_utilization_percent,
                min_free_disk_gib=args.min_free_disk_gib,
                termination_grace_seconds=args.termination_grace_seconds,
                max_consecutive_failures=args.max_consecutive_failures,
                gpu_provider=lambda: query_gpus(args.nvidia_smi),
            )
            scheduler.run(once=args.once)
        elif args.action == "receipt":
            destination = args.output
            if destination is not None and not destination.is_absolute():
                destination = repo_root / destination
            print(export_receipt(store, destination))
        elif args.action == "pull-commands":
            print(format_pull_commands(store))
        elif args.action == "_execute":
            return execute_item(store, args.item_id)
        else:
            parser.error(f"unsupported action {args.action!r}")
    except (QueueError, StateDirectoryError) as exc:
        print(f"experiment queue error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
