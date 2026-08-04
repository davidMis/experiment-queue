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
import json
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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from helmholtz_shared.experiment_runner import collect_git_context


SCHEMA_VERSION = 1
DEFAULT_STATE_DIR = Path("gpu_scheduler_state")
ACTIVE_STATES = {"queued", "held", "blocked", "starting", "running", "terminating", "force_killing"}
PENDING_STATES = {"queued", "held", "blocked"}
RUNNING_STATES = {"starting", "running", "terminating", "force_killing"}
TERMINAL_STATES = {"succeeded", "failed", "interrupted", "force_killed", "removed"}
SUCCESS_STATE = "succeeded"
CARD_COMMAND_HEADING = "## Exact Manual Command On Mutton2"


class QueueError(RuntimeError):
    """Raised when a queue operation cannot be completed safely."""


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


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON through an adjacent temporary file and atomic rename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _parse_csv_number(value: str, *, field: str) -> float:
    cleaned = value.strip()
    if cleaned in {"", "N/A", "[N/A]"}:
        raise QueueError(f"nvidia-smi did not report {field}: {value!r}")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise QueueError(f"nvidia-smi returned invalid {field}: {value!r}") from exc


class QueueStore:
    """SQLite-backed queue state and append-only operational event log."""

    def __init__(self, state_dir: Path, repo_root: Path):
        self.state_dir = state_dir.resolve()
        self.repo_root = repo_root.resolve()
        self.database_path = self.state_dir / "queue.sqlite3"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        """Open one configured SQLite connection."""

        connection = sqlite3.connect(self.database_path, timeout=30.0)
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
                if int(existing_version["value"]) != SCHEMA_VERSION:
                    raise QueueError(
                        f"queue schema {existing_version['value']} is not supported; "
                        f"expected {SCHEMA_VERSION}"
                    )
                recorded_root = self.get_meta("repo_root", connection=connection)
                if Path(recorded_root).resolve() != self.repo_root:
                    raise QueueError(
                        f"queue belongs to repository {recorded_root}, not {self.repo_root}"
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
    ) -> None:
        with self.connect() as connection:
            self._event(connection, event_type, queue_item_id=queue_item_id, payload=payload)

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
) -> int:
    """Explicitly snapshot one committed card command into queue membership."""

    commit = require_clean_git(store.repo_root)
    card = read_card_command(store.repo_root, experiment_id, card_path)
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        active = connection.execute(
            "SELECT id, state FROM queue_items WHERE experiment_id = ? "
            "AND state IN ('queued','held','blocked','starting','running','terminating','force_killing')",
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                _actor(),
                "explicitly held at admission" if held else None,
            ),
        )
        item_id = int(cursor.lastrowid)
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
            },
        )
    return item_id


def _transition_pending_item(
    store: QueueStore,
    item_id: int,
    *,
    target_state: str,
    event_type: str,
    detail: str | None = None,
    allowed_states: Iterable[str] = PENDING_STATES,
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
        store._event(connection, event_type, queue_item_id=item_id, payload={"detail": detail})


def remove_item(store: QueueStore, item_id: int, reason: str | None = None) -> None:
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
            )


def hold_item(store: QueueStore, item_id: int, reason: str | None = None) -> None:
    """Prevent a pending item from dispatching until explicitly released."""

    _transition_pending_item(
        store,
        item_id,
        target_state="held",
        event_type="EXPERIMENT_HELD",
        detail=reason,
        allowed_states={"queued", "blocked"},
    )


def release_item(store: QueueStore, item_id: int) -> None:
    """Return a held or blocked item to explicit queue membership."""

    _transition_pending_item(
        store,
        item_id,
        target_state="queued",
        event_type="EXPERIMENT_RELEASED",
        allowed_states={"held", "blocked"},
    )


def set_priority(store: QueueStore, item_id: int, priority: int) -> None:
    """Change the dispatch priority of a pending item."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in PENDING_STATES:
            raise QueueError(f"queue item {item_id} is {item['state']}; only pending priority can change")
        connection.execute("UPDATE queue_items SET priority = ? WHERE id = ?", (priority, item_id))
        store._event(
            connection,
            "PRIORITY_CHANGED",
            queue_item_id=item_id,
            payload={"old": item["priority"], "new": priority},
        )


def set_dispatch_paused(store: QueueStore, paused: bool, reason: str | None = None) -> None:
    """Pause or resume new dispatch without changing running jobs."""

    with store.connect() as connection:
        store._set_meta(connection, "dispatch_paused", "1" if paused else "0")
        store._set_meta(connection, "pause_reason", reason or "")
        store._event(
            connection,
            "DISPATCH_PAUSED" if paused else "DISPATCH_RESUMED",
            payload={"reason": reason},
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
            "('starting','running','terminating','force_killing') AND assigned_gpu_uuid IS NOT NULL"
        )
    }


def update_gpu_allowlist(
    store: QueueStore,
    action: str,
    identifiers: Sequence[str],
    *,
    snapshots: Sequence[GpuSnapshot],
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
) -> bool:
    """Record and signal a graceful termination or explicit force kill."""

    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        item = store.item(item_id, connection=connection)
        if item["state"] not in {"starting", "running", "terminating", "force_killing"}:
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
        store._event(
            connection,
            event_type,
            queue_item_id=item_id,
            payload={"reason": reason, "signal": signal.Signals(signum).name},
        )
        refreshed = store.item(item_id, connection=connection)
    signaled = _signal_item(refreshed, signum)
    store.event(
        "TERMINATION_SIGNAL_SENT" if signaled else "TERMINATION_SIGNAL_PENDING",
        queue_item_id=item_id,
        payload={"signal": signal.Signals(signum).name},
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
        self.store = store
        self.poll_seconds = poll_seconds
        self.control_seconds = control_seconds
        self.min_free_memory_fraction = min_free_memory_fraction
        self.max_utilization_percent = max_utilization_percent
        self.min_free_disk_gib = min_free_disk_gib
        self.termination_grace_seconds = termination_grace_seconds
        self.max_consecutive_failures = max_consecutive_failures
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
                    "('starting','running','terminating','force_killing')"
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

    def _receipt_path(self, item_id: int) -> Path:
        return self.store.state_dir / "attempts" / str(item_id) / "exit.json"

    def _finalize_item(self, item: sqlite3.Row, receipt: dict[str, Any] | None) -> None:
        prior_state = str(item["state"])
        return_code = receipt.get("return_code") if receipt is not None else None
        if prior_state == "force_killing":
            final_state = "force_killed"
        elif prior_state == "terminating" or return_code == 130:
            final_state = "interrupted"
        elif return_code == 0:
            final_state = "succeeded"
        else:
            final_state = "failed"
        detail = None if receipt is not None else "process disappeared without an exit receipt"
        runner_receipt = _read_runner_receipt(
            self.store.state_dir / "attempts" / str(item["id"]) / "launcher.log"
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
                    runner_receipt.get("run_directory"),
                    runner_receipt.get("manifest"),
                    runner_receipt.get("rsync_pull_command"),
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
                    "('starting','running','terminating','force_killing') LIMIT 1",
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
                    "('starting','running','terminating','force_killing')"
                )
            )
        for item in items:
            item_id = int(item["id"])
            process = self.processes.get(item_id)
            if process is not None:
                process.poll()
            receipt_path = self._receipt_path(item_id)
            if receipt_path.is_file():
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                self.processes.pop(item_id, None)
                self._finalize_item(item, receipt)
            elif not _pid_matches(item):
                self.processes.pop(item_id, None)
                self._finalize_item(item, None)

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
                    "ORDER BY priority DESC, id ASC"
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
        attempt_dir = self.store.state_dir / "attempts" / str(item_id)
        attempt_dir.mkdir(parents=True, exist_ok=True)
        launcher_log = (attempt_dir / "launcher.log").open("ab", buffering=0)
        executor_script = Path(__file__).resolve().parents[1] / "scripts" / "run_experiment_queue.py"
        command = [
            sys.executable,
            str(executor_script),
            "--repo-root",
            str(self.store.repo_root),
            "--state-dir",
            str(self.store.state_dir),
            "_execute",
            str(item_id),
        ]
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = gpu.uuid
        env["EXPERIMENT_QUEUE_ITEM_ID"] = str(item_id)
        env["EXPERIMENT_QUEUE_GPU_UUID"] = gpu.uuid
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE queue_items SET state = 'starting', assigned_gpu_uuid = ?,
                    assigned_gpu_index = ?, state_detail = NULL WHERE id = ?
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
                },
            )
        try:
            process = subprocess.Popen(
                command,
                cwd=self.store.repo_root,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=launcher_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as exc:
            launcher_log.close()
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
                payload={"pid": process.pid, "pgid": process.pid, "gpu_uuid": gpu.uuid},
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
                    "('starting','running','terminating','force_killing') "
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
        available = [
            by_uuid[row["uuid"]]
            for row in allow
            if row["uuid"] in by_uuid
            and row["uuid"] not in assigned
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
        """Pause new dispatch if the shared checkout changes beneath running work."""

        with self.store.connect() as connection:
            running = list(
                connection.execute(
                    "SELECT * FROM queue_items WHERE state IN "
                    "('starting','running','terminating','force_killing')"
                )
            )
        if not running:
            return
        try:
            head = require_clean_git(self.store.repo_root)
            base_reason = None
        except QueueError as exc:
            head = None
            base_reason = str(exc)
        mismatched = [
            item
            for item in running
            if base_reason is not None or head != str(item["git_commit"])
        ]
        if not mismatched:
            return
        reason = base_reason or (
            f"repository HEAD changed to {head} while queue jobs from another commit are running"
        )
        with self.store.connect() as connection:
            self.store._set_meta(connection, "dispatch_paused", "1")
            self.store._set_meta(connection, "pause_reason", reason)
            for item in mismatched:
                if not item["repo_drift_detected"]:
                    connection.execute(
                        "UPDATE queue_items SET repo_drift_detected = 1, "
                        "state_detail = COALESCE(state_detail, ?) WHERE id = ?",
                        (reason, item["id"]),
                    )
                    self.store._event(
                        connection,
                        "REPOSITORY_DRIFT_DETECTED",
                        queue_item_id=int(item["id"]),
                        payload={"reason": reason},
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

        self._reconcile_processes()
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
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
            if scheduler_started and stop_signal is not None:
                self.store.event(
                    "SCHEDULER_STOP_REQUESTED", payload={"signal": stop_signal}
                )
            if scheduler_started:
                self.store.event("SCHEDULER_STOPPED", payload={"pid": os.getpid()})
            identity_path = self.store.state_dir / "scheduler.json"
            if identity_path.exists():
                identity_path.unlink()
            for lock_file in self.gpu_locks.values():
                lock_file.close()
            self.gpu_locks.clear()
            if self._scheduler_lock is not None:
                self._scheduler_lock.close()
                self._scheduler_lock = None


def execute_item(store: QueueStore, item_id: int) -> int:
    """Internal durable executor that writes an exit receipt even if the scheduler stops."""

    item = store.item(item_id)
    if item["state"] not in {"starting", "running", "terminating", "force_killing"}:
        raise QueueError(f"queue item {item_id} is {item['state']}; executor requires a running state")
    attempt_dir = store.state_dir / "attempts" / str(item_id)
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

    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, forward)
    started_at = utc_now_iso()
    try:
        child = subprocess.Popen(
            ["/bin/bash", "-lc", str(item["command_text"])],
            cwd=store.repo_root,
            env=os.environ.copy(),
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
        "schema_version": 1,
        "queue_item_id": item_id,
        "experiment_id": item["experiment_id"],
        "attempt": item["attempt"],
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
                "('starting','running','terminating','force_killing')"
            )
            if row["assigned_gpu_uuid"]
        }
    lines = ["INDEX  UUID                  STATE           JOB       MEMORY-FREE  UTIL  COMPUTE-PIDS"]
    for row in rows:
        gpu = by_uuid.get(str(row["uuid"]))
        job = assigned.get(str(row["uuid"]))
        if row["draining"]:
            state = "draining"
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
            "schema_version": 1,
            "generated_at": utc_now_iso(),
            "repo_root": str(store.repo_root),
            "metadata": {
                row["key"]: row["value"] for row in connection.execute("SELECT * FROM metadata")
            },
            "gpu_allowlist": [
                dict(row) for row in connection.execute("SELECT * FROM gpu_allowlist ORDER BY uuid")
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
        default=DEFAULT_STATE_DIR,
        help=(
            "Ignored durable state directory for the SQLite database, logs, and receipts. "
            "Relative paths are resolved from --repo-root. Default: gpu_scheduler_state."
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

    for name, help_text in (
        ("remove", "Remove a pending queue item without deleting history."),
        ("hold", "Temporarily hold a queued or blocked item."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
        command.add_argument("--reason", help="Reason recorded in the event history.")
    release = subparsers.add_parser("release", help="Release a held or blocked item.")
    release.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
    priority = subparsers.add_parser("priority", help="Change a pending item's dispatch priority.")
    priority.add_argument("item_id", type=int, help="Exact numeric queue item ID from status.")
    priority.add_argument("value", type=int, help="New integer priority; larger values dispatch first.")

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
    state_dir = args.state_dir if args.state_dir.is_absolute() else repo_root / args.state_dir
    return repo_root, state_dir.resolve()


def main(argv: Sequence[str] | None = None) -> int:
    """Run one queue control action and return a process exit code."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    repo_root, state_dir = _resolve_paths(args)
    try:
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
    except QueueError as exc:
        print(f"experiment queue error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
