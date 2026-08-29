"""Offline, receipt-producing import of authenticated schema-v1 through v4 copies.

The importer never opens the source database read-write and never mutates a
requested destination in place.  It constructs a fresh schema-v5 candidate in
an adjacent directory, performs field-by-field verification, and publishes the
complete directory with one rename only after every check succeeds.
"""

from __future__ import annotations

import argparse
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
from typing import Final, Mapping, Sequence, cast

from experiment_queue import __version__
from experiment_queue.database_v5 import (
    DATABASE_FILENAME,
    SCHEMA_IDENTITY,
    SCHEMA_VERSION,
    V5QueueStore,
)
from experiment_queue.legacy_state import (
    LegacySourceProbe,
    LegacyStateError,
    LegacyStateSnapshot,
    V4_QUEUE_COLUMNS,
    load_legacy_state,
    sqlite_rows_evidence_bytes,
)
from experiment_queue.migration_receipt import (
    MigrationMode,
    MigrationResult,
    QueueMigrationReceipt,
)
from experiment_queue.path_security import (
    PathBoundaryError,
    SecurePathBoundary,
    capture_secure_path_boundary,
    revalidate_secure_path_boundary,
)
from experiment_queue.serialization import JSONValue, canonical_json_bytes, sha256_bytes


_PROJECT_KEY = re.compile(r"[a-z](?:[a-z0-9]|-(?=[a-z0-9])){0,62}\Z")
_OPERATION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
_MIGRATED_TABLES: Final = (
    "queue_items",
    "dependencies",
    "gpu_allowlist",
    "events",
    "gpu_reservations",
)
_SOURCE_SEQUENCE_TABLES: Final = frozenset(
    {"queue_items", "events", "gpu_reservations"}
)


class V5MigrationError(RuntimeError):
    """Raised after an offline import fails closed.

    When source probing was possible, ``receipt`` contains the exact failed
    QueueMigrationReceipt/v1 evidence also written to the requested external
    receipt path.
    """

    def __init__(
        self,
        message: str,
        *,
        receipt: QueueMigrationReceipt | None = None,
    ) -> None:
        super().__init__(message)
        self.receipt = receipt


class _ReceiptPublicationIndeterminate(V5MigrationError):
    """A receipt is visible but its directory durability could not be proven."""


class _StatePublicationIndeterminate(V5MigrationError):
    """A state rename occurred but its durable final name is not knowable."""


@dataclass(frozen=True, slots=True)
class V5MigrationOutcome:
    """Completed dry-run or published import result."""

    destination_state: Path
    receipt_path: Path
    receipt: QueueMigrationReceipt
    published: bool


@dataclass(slots=True)
class _BuildEvidence:
    """Mutable internal evidence accumulated before receipt finalization."""

    project_id: int | None = None
    revision_ids: tuple[int, ...] = ()
    current_revision_id: int | None = None
    comparison: dict[str, object] | None = None
    integrity_check: str = "not run"
    foreign_key_violations: int = 0
    database_instance_id: str | None = None
    candidate_database_sha256: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _absolute_existing_directory(value: Path, *, label: str) -> Path:
    supplied = Path(value)
    if not supplied.is_absolute():
        raise V5MigrationError(f"{label} must be an absolute directory, got {supplied}")
    if supplied.is_symlink():
        raise V5MigrationError(f"{label} {supplied} must not be a symlink")
    try:
        resolved = supplied.resolve(strict=True)
    except OSError as exc:
        raise V5MigrationError(f"{label} {supplied} does not resolve: {exc}") from exc
    if not resolved.is_dir():
        raise V5MigrationError(f"{label} {resolved} is not a directory")
    return resolved


def _validate_project_key(value: str) -> str:
    if type(value) is not str or _PROJECT_KEY.fullmatch(value) is None:
        raise V5MigrationError(
            f"project key {value!r} must be 1-63 lowercase letters, digits, and "
            "single interior hyphens, beginning with a letter"
        )
    return value


def _validate_actor(value: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise V5MigrationError("actor must be non-empty text without surrounding whitespace")
    if len(value) > 256 or any(ord(character) < 32 for character in value):
        raise V5MigrationError("actor must be at most 256 control-free characters")
    return value


def _validate_operation_id(value: str) -> str:
    if _OPERATION_ID.fullmatch(value) is None:
        raise V5MigrationError(
            "operation ID must use 1-256 letters, digits, '.', '_', ':', or '-'"
        )
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _secure_path_boundary(path: Path, *, label: str) -> SecurePathBoundary:
    """Capture a trusted root/service-owned ancestor chain for publication."""

    try:
        return capture_secure_path_boundary(path, label=label)
    except PathBoundaryError as exc:
        raise V5MigrationError(str(exc)) from exc


def _revalidate_path_boundary(boundary: SecurePathBoundary) -> None:
    try:
        revalidate_secure_path_boundary(boundary)
    except PathBoundaryError as exc:
        raise V5MigrationError(str(exc)) from exc


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_remove_candidate(candidate: Path, destination: Path) -> None:
    """Remove only our validated sibling candidate after a failed/unpublished run."""

    if candidate.parent != destination.parent or candidate == destination:
        raise V5MigrationError(
            f"refused unsafe candidate cleanup target {candidate}; expected a distinct "
            f"sibling of {destination}"
        )
    if candidate.exists():
        shutil.rmtree(candidate)


def _atomic_write_receipt(path: Path, receipt: QueueMigrationReceipt) -> None:
    """Publish an external receipt atomically without replacing prior evidence."""

    parent = path.parent
    if not parent.is_dir() or parent.is_symlink():
        raise V5MigrationError(
            f"receipt parent {parent} must be an existing non-symlink directory"
        )
    boundary = _secure_path_boundary(path, label="migration receipt path")
    temporary = parent / f".{path.name}.{receipt.operation_id}.tmp"
    descriptor: int | None = None
    temporary_created = False
    linked = False
    publication_durable = False
    preserve_temporary = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        temporary_created = True
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = None
            stream.write(receipt.canonical_json)
            stream.flush()
            os.fsync(stream.fileno())
        # First make the fsynced staging name directory-durable. It is the
        # recoverable evidence name if the final-link fsync is indeterminate.
        _fsync_directory(parent)
        _revalidate_path_boundary(boundary)
        # A hard-link publication is atomic and fails if evidence already exists.
        os.link(temporary, path)
        linked = True
        _fsync_directory(parent)
        publication_durable = True
        _revalidate_path_boundary(boundary)
        try:
            temporary.unlink()
        except OSError:
            # The final receipt is already directory-durable. A stale staging
            # hard link is cleanup work, not grounds to roll back state.
            preserve_temporary = True
            return
        _fsync_directory(parent)
        _revalidate_path_boundary(boundary)
    except BaseException as exc:
        publication_visible = linked
        if not publication_visible:
            try:
                publication_visible = (
                    not path.is_symlink()
                    and path.is_file()
                    and temporary.exists()
                    and os.path.samefile(temporary, path)
                )
            except OSError:
                publication_visible = False
        if publication_visible:
            preserve_temporary = not publication_durable
            durability = (
                "is durably published but staging-name cleanup could not be "
                "confirmed"
                if publication_durable
                else "is visible but final-link directory durability could not "
                "be confirmed; its durable staging hard link is preserved"
            )
            raise _ReceiptPublicationIndeterminate(
                f"receipt {path} {durability}: {exc}; preserve the receipt, any "
                "staging hard link, and any published destination, then inspect "
                "them before retrying",
                receipt=receipt,
            ) from exc
        if isinstance(exc, (KeyboardInterrupt, SystemExit, V5MigrationError)):
            raise
        if isinstance(exc, FileExistsError):
            raise V5MigrationError(
                f"receipt path or staging file already exists ({path}); choose a "
                "fresh absolute --receipt path and retain earlier evidence"
            ) from exc
        raise V5MigrationError(f"could not atomically write receipt {path}: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_created and temporary.exists() and not preserve_temporary:
            try:
                temporary.unlink()
            except OSError:
                pass


def _publish_state(candidate: Path, destination: Path) -> None:
    """Atomically publish a verified sibling without replacing any destination."""

    boundary = _secure_path_boundary(
        destination,
        label="migration destination state",
    )
    if destination.exists() or destination.is_symlink():
        raise V5MigrationError(
            f"destination {destination} appeared before publish; it was not changed"
        )
    try:
        candidate_details = candidate.stat(follow_symlinks=False)
    except OSError as exc:
        raise V5MigrationError(
            f"verified migration candidate {candidate} cannot be inspected: {exc}"
        ) from exc
    candidate_identity = (candidate_details.st_dev, candidate_details.st_ino)
    try:
        _atomic_rename_noreplace(candidate, destination)
    except FileExistsError as exc:
        raise V5MigrationError(
            f"destination {destination} appeared during atomic publish; it was "
            "not changed and candidate {candidate} remains unpublished"
        ) from exc
    except OSError as exc:
        current_candidate_identity: tuple[int, int] | None = None
        destination_identity: tuple[int, int] | None = None
        try:
            current = candidate.stat(follow_symlinks=False)
            current_candidate_identity = (current.st_dev, current.st_ino)
        except OSError:
            pass
        try:
            current = destination.stat(follow_symlinks=False)
            destination_identity = (current.st_dev, current.st_ino)
        except OSError:
            pass
        if (
            destination_identity == candidate_identity
            and current_candidate_identity is None
        ):
            _rollback_published_state(
                destination,
                candidate,
                reason=f"exclusive rename reported an error after moving state ({exc})",
                boundary=boundary,
            )
            raise V5MigrationError(
                f"atomic publish syscall reported an error after moving state, "
                f"and publication was durably rolled back to {candidate}: {exc}"
            ) from exc
        if (
            destination_identity is not None
            or current_candidate_identity != candidate_identity
        ):
            raise _StatePublicationIndeterminate(
                f"state publication outcome is indeterminate after exclusive "
                f"rename error ({exc}); candidate identity was "
                f"{candidate_identity}, current candidate identity is "
                f"{current_candidate_identity}, and destination identity is "
                f"{destination_identity}. Preserve and inspect both paths before "
                "any retry"
            ) from exc
        raise V5MigrationError(
            f"atomic publish from {candidate} to {destination} failed: {exc}"
        ) from exc
    try:
        _fsync_directory(destination.parent)
        _revalidate_path_boundary(boundary)
    except BaseException as exc:
        _rollback_published_state(
            destination,
            candidate,
            reason=f"parent-directory fsync after atomic publication failed ({exc})",
            boundary=boundary,
        )
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise V5MigrationError(
            f"state rename parent fsync failed and publication was durably rolled "
            f"back to {candidate}: {exc}"
        ) from exc


def _rollback_published_state(
    destination: Path,
    candidate: Path,
    *,
    reason: str,
    boundary: SecurePathBoundary | None = None,
) -> None:
    """Durably restore an unpublished candidate or declare uncertainty.

    Once the publication rename has occurred, neither a failed rollback nor a
    failed parent-directory fsync permits ordinary cleanup or a receipt claiming
    ``published: false``. In those cases exact visible names are preserved for
    operator inspection.
    """

    selected_boundary = boundary or _secure_path_boundary(
        destination,
        label="migration destination state",
    )
    try:
        _revalidate_path_boundary(selected_boundary)
    except V5MigrationError as boundary_exc:
        raise _StatePublicationIndeterminate(
            f"state publication durability is indeterminate after {reason}; "
            f"destination ancestor identity changed ({boundary_exc}). Preserve "
            "and inspect both paths before any retry"
        ) from boundary_exc
    if (
        not destination.is_dir()
        or destination.is_symlink()
        or candidate.exists()
        or candidate.is_symlink()
    ):
        raise _StatePublicationIndeterminate(
            f"state publication durability is indeterminate after {reason}; "
            f"cannot exclusively roll {destination} back to absent {candidate}. "
            "Preserve and inspect both paths before any retry"
        )
    try:
        _atomic_rename_noreplace(destination, candidate)
    except OSError as rollback_exc:
        raise _StatePublicationIndeterminate(
            f"state publication durability is indeterminate after {reason}; "
            f"rollback from {destination} to {candidate} failed ({rollback_exc}). "
            "Preserve and inspect both paths before any retry"
        ) from rollback_exc
    try:
        _fsync_directory(destination.parent)
        _revalidate_path_boundary(selected_boundary)
    except (OSError, V5MigrationError) as rollback_fsync_exc:
        raise _StatePublicationIndeterminate(
            f"state publication durability is indeterminate after {reason}; "
            f"rollback is visible at {candidate}, but its parent-directory fsync "
            f"failed ({rollback_fsync_exc}). Preserve and inspect both paths "
            "before any retry"
        ) from rollback_fsync_exc


def _atomic_rename_noreplace(source: Path, destination: Path) -> None:
    """Use the host's atomic exclusive-rename primitive or fail closed.

    Linux production uses ``renameat2(RENAME_NOREPLACE)``. macOS development
    and migration validation use ``renamex_np(RENAME_EXCL)``. There is no plain
    ``os.rename`` fallback because it can replace an empty directory created in
    the destination-absence race window.
    """

    library = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform.startswith("linux"):
        rename = getattr(library, "renameat2", None)
        if rename is None:
            raise OSError(
                errno.ENOSYS,
                "libc does not expose renameat2(RENAME_NOREPLACE); atomic "
                "copy-only migration publication is unavailable",
                str(destination),
            )
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, destination_bytes, 1)
    elif sys.platform == "darwin":
        rename = getattr(library, "renamex_np", None)
        if rename is None:
            raise OSError(
                errno.ENOSYS,
                "libSystem does not expose renamex_np(RENAME_EXCL); atomic "
                "copy-only migration publication is unavailable",
                str(destination),
            )
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 0x00000004)
    else:
        raise OSError(
            errno.ENOTSUP,
            f"atomic no-replace directory publication is unsupported on {sys.platform}",
            str(destination),
        )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(
            error_number, os.strerror(error_number), str(destination)
        )
    raise OSError(error_number, os.strerror(error_number), str(destination))


def _validate_paths(
    *,
    source: LegacyStateSnapshot,
    destination_state: Path,
    legacy_checkout: Path,
    receipt_path: Path,
    protected_roots: Sequence[Path],
) -> tuple[Path, Path, tuple[Path, ...]]:
    destination = Path(destination_state)
    if not destination.is_absolute():
        raise V5MigrationError(
            f"destination state must be an absolute path, got {destination}"
        )
    if destination.exists() or destination.is_symlink():
        raise V5MigrationError(
            f"destination {destination} must be absent; the importer never merges, "
            "repairs, or overwrites state"
        )
    parent = destination.parent.resolve(strict=True)
    if not parent.is_dir() or parent.is_symlink():
        raise V5MigrationError(
            f"destination parent {parent} must be an existing non-symlink directory"
        )
    destination = parent / destination.name
    _secure_path_boundary(
        destination,
        label="migration destination state",
    )
    checkout = _absolute_existing_directory(legacy_checkout, label="legacy checkout")
    roots = tuple(
        _absolute_existing_directory(root, label=f"protected legacy root {index}")
        for index, root in enumerate(protected_roots)
    )
    protected = (("source state", source.state_path), ("legacy checkout", checkout)) + tuple(
        (f"protected legacy root {index}", root) for index, root in enumerate(roots)
    )
    protected += tuple(
        (
            f"recorded legacy {entry['kind']} for item {entry['item_id']}",
            Path(cast(str, entry["path"])).resolve(strict=False),
        )
        for entry in source.path_inventory
        if entry["kind"] != "git_ref"
        and type(entry.get("path")) is str
        and Path(cast(str, entry["path"])).is_absolute()
    )
    for label, root in protected:
        if _paths_overlap(destination, root):
            raise V5MigrationError(
                f"destination {destination} overlaps {label} {root}; choose an absent "
                "sibling outside every source, checkout, artifact, and worktree root"
            )
    receipt = Path(receipt_path)
    if not receipt.is_absolute():
        raise V5MigrationError(f"receipt path must be absolute, got {receipt}")
    if receipt.exists() or receipt.is_symlink():
        raise V5MigrationError(
            f"receipt path {receipt} already exists; retain it and choose a fresh path"
        )
    receipt_parent = receipt.parent.resolve(strict=True)
    receipt = receipt_parent / receipt.name
    _secure_path_boundary(receipt, label="migration receipt path")
    for label, root in protected + (("destination state", destination),):
        if _paths_overlap(receipt, root):
            raise V5MigrationError(
                f"external receipt {receipt} overlaps {label} {root}; place it in a "
                "separate operator-controlled directory"
            )
    return destination, receipt, roots


def _safe_failure_receipt_path(
    path: Path,
    *,
    probe: LegacySourceProbe,
    destination: Path,
    legacy_checkout: Path,
    protected_roots: Sequence[Path],
    path_inventory: Sequence[Mapping[str, object]],
) -> Path:
    """Resolve a failure receipt target without ever writing into input roots."""

    supplied = Path(path)
    if not supplied.is_absolute() or supplied.exists() or supplied.is_symlink():
        raise V5MigrationError(
            f"failure receipt path {supplied} must be absolute and absent"
        )
    try:
        parent = supplied.parent.resolve(strict=True)
    except OSError as exc:
        raise V5MigrationError(
            f"failure receipt parent {supplied.parent} does not resolve: {exc}"
        ) from exc
    target = parent / supplied.name
    roots: list[tuple[str, Path]] = [("source state", probe.state_path)]
    for label, raw in (
        ("legacy checkout", legacy_checkout),
        ("destination state", destination),
    ):
        if Path(raw).is_absolute():
            roots.append((label, Path(raw).resolve(strict=False)))
    roots.extend(
        (f"protected legacy root {index}", Path(root).resolve(strict=False))
        for index, root in enumerate(protected_roots)
        if Path(root).is_absolute()
    )
    roots.extend(
        (
            f"recorded legacy {entry['kind']} for item {entry['item_id']}",
            Path(cast(str, entry["path"])).resolve(strict=False),
        )
        for entry in path_inventory
        if entry.get("kind") != "git_ref"
        and type(entry.get("path")) is str
        and Path(cast(str, entry["path"])).is_absolute()
    )
    for label, root in roots:
        if _paths_overlap(target, root):
            raise V5MigrationError(
                f"failure receipt {target} overlaps {label} {root}; source state "
                "was not changed and no unsafe receipt was written"
            )
    return target


def _legacy_enrollment(
    *,
    project_key: str,
    checkout: Path,
    snapshot: LegacyStateSnapshot,
    git_commit: str | None,
) -> bytes:
    return canonical_json_bytes(
        cast(
            JSONValue,
            {
                "apiVersion": "experiment-queue/v1",
                "kind": "LegacyEnrollment",
                "projectKey": project_key,
                "checkoutDirectory": str(checkout),
                "projectManifestPath": None,
                "sourceSchemaVersion": snapshot.schema_version,
                "sourceStateIdentitySha256": snapshot.probe.state_identity_sha256,
                "gitCommit": git_commit,
                "mounts": [],
                "artifactRoots": [],
                "environments": [],
            },
        )
    )


def _insert_project_and_revisions(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_key: str,
    checkout: Path,
    actor: str,
    created_at: str,
) -> tuple[int, tuple[int, ...], dict[str, int]]:
    commits: tuple[str | None, ...] = (
        cast(tuple[str | None, ...], snapshot.commits)
        if snapshot.commits
        else (None,)
    )
    project_id = 1
    revision_ids = tuple(range(1, len(commits) + 1))
    current_revision_id = revision_ids[-1]
    current_sequence = len(revision_ids)
    connection.execute(
        """
        INSERT INTO projects(
            id, project_key, display_name, lifecycle, current_revision_id,
            current_revision_sequence, created_at, created_by,
            lifecycle_changed_at, lifecycle_actor, lifecycle_reason
        ) VALUES (?, ?, ?, 'paused', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            project_id,
            project_key,
            project_key,
            current_revision_id,
            current_sequence,
            created_at,
            actor,
            created_at,
            actor,
            "offline legacy import; operator activation required",
        ),
    )
    revision_by_commit: dict[str, int] = {}
    for sequence, (revision_id, commit) in enumerate(zip(revision_ids, commits), start=1):
        enrollment = _legacy_enrollment(
            project_key=project_key,
            checkout=checkout,
            snapshot=snapshot,
            git_commit=commit,
        )
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, checkout_path, project_manifest_path,
                enrollment_json, enrollment_sha256, created_at, created_actor
            ) VALUES (?, ?, ?, ?, 'legacy-v4', ?, ?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                revision_id,
                project_id,
                sequence,
                f"{project_key}:legacy-r{sequence}",
                project_key,
                commit,
                str(checkout),
                enrollment,
                sha256_bytes(enrollment),
                created_at,
                actor,
            ),
        )
        if commit is not None:
            revision_by_commit[commit] = revision_id
    connection.execute(
        """
        INSERT INTO project_runtime_state(
            project_id, health, circuit_failure_count, health_reason,
            health_actor, health_changed_at
        ) VALUES (?, 'closed', 0, ?, ?, ?)
        """,
        (
            project_id,
            "imported Project is paused pending operator verification",
            actor,
            created_at,
        ),
    )
    return project_id, revision_ids, revision_by_commit


def _insert_queue_rows(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_id: int,
    revision_by_commit: Mapping[str, int],
) -> None:
    columns = (
        "id",
        "project_id",
        "revision_id",
        "admission_kind",
        "snapshot_id",
        "job_id",
    ) + tuple(column for column in V4_QUEUE_COLUMNS if column != "id") + (
        "runtime_gpu_lease_held",
        "runtime_gpu_lease_released_at",
    )
    sql = (
        f"INSERT INTO queue_items({', '.join(columns)}) VALUES "
        f"({', '.join('?' for _ in columns)})"
    )
    for row in snapshot.normalized_queue_items():
        commit = cast(str, row["git_commit"])
        # An imported active attempt already owns its historical assignment;
        # binding the new v5 lease to it prevents fresh dispatch from reusing
        # that GPU before restart recovery authenticates the runtime.  Inactive
        # legacy rows carry historical assignment only and start unheld.
        runtime_gpu_lease_held = int(
            row["state"]
            in {
                "starting",
                "running",
                "yielding",
                "terminating",
                "force_killing",
            }
        )
        values = (
            row["id"],
            project_id,
            revision_by_commit[commit],
            "LegacyMarkdownCard/v0",
            None,
            None,
        ) + tuple(row[column] for column in V4_QUEUE_COLUMNS if column != "id") + (
            runtime_gpu_lease_held,
            None,
        )
        connection.execute(sql, values)


def _insert_dependencies(
    connection: sqlite3.Connection,
    snapshot: LegacyStateSnapshot,
) -> None:
    connection.executemany(
        "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (?, ?)",
        (
            (row["queue_item_id"], row["dependency_item_id"])
            for row in snapshot.dependencies
        ),
    )


def _insert_allowlist(
    connection: sqlite3.Connection,
    snapshot: LegacyStateSnapshot,
) -> None:
    columns = (
        "uuid",
        "requested_identifier",
        "last_index",
        "name",
        "enabled",
        "draining",
        "updated_at",
    )
    connection.executemany(
        f"INSERT INTO gpu_allowlist({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        (tuple(row[column] for column in columns) for row in snapshot.gpu_allowlist),
    )


def _insert_events(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_id: int,
) -> None:
    for row in snapshot.events:
        item_id = row["queue_item_id"]
        scope = "project" if item_id is not None else "host"
        event_project_id = project_id if item_id is not None else None
        connection.execute(
            """
            INSERT INTO events(
                id, created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["id"],
                row["created_at"],
                row["actor"],
                row["event_type"],
                item_id,
                row["payload_json"],
                scope,
                event_project_id,
            ),
        )


def _insert_reservations(
    connection: sqlite3.Connection,
    snapshot: LegacyStateSnapshot,
) -> None:
    columns = (
        "id",
        "gpu_uuid",
        "queue_item_id",
        "status",
        "requested_at",
        "requested_by",
        "note",
        "duration_hours",
        "starts_at",
        "expires_at",
        "released_at",
        "released_by",
        "state_detail",
    )
    connection.executemany(
        f"INSERT INTO gpu_reservations({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        (tuple(row[column] for column in columns) for row in snapshot.gpu_reservations),
    )


def _insert_artifacts(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_id: int,
    revision_by_commit: Mapping[str, int],
) -> None:
    item_by_id = {int(row["id"]): row for row in snapshot.normalized_queue_items()}
    check_hashes = {
        (int(check["item_id"]), cast(str, evidence["kind"])): cast(str, evidence["sha256"])
        for check in snapshot.continuation_checks
        for evidence in cast(list[dict[str, object]], check["files"])
    }
    for entry in snapshot.path_inventory:
        if entry["kind"] == "git_ref":
            continue
        item_id = int(entry["item_id"])
        item = item_by_id[item_id]
        kind = cast(str, entry["kind"])
        metadata = canonical_json_bytes(cast(JSONValue, entry))
        connection.execute(
            """
            INSERT INTO job_artifacts(
                queue_item_id, project_id, revision_id, segment, evidence_kind,
                artifact_name, artifact_type, absolute_path, size_bytes, sha256,
                recorded_at, metadata_json
            ) VALUES (?, ?, ?, ?, 'legacy-v4', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                project_id,
                revision_by_commit[cast(str, item["git_commit"])],
                item["segment"],
                kind,
                entry["expected_type"],
                entry["path"],
                (
                    int(cast(str, entry["size_bytes"]))
                    if entry.get("size_bytes") is not None
                    else None
                ),
                check_hashes.get((item_id, kind)),
                item["finished_at"] or item["added_at"],
                metadata,
            ),
        )


def _restore_source_sequences(
    connection: sqlite3.Connection,
    snapshot: LegacyStateSnapshot,
) -> None:
    source = dict(snapshot.sequences)
    for table in _SOURCE_SEQUENCE_TABLES:
        connection.execute("DELETE FROM sqlite_sequence WHERE name = ?", (table,))
        if table in source:
            connection.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)",
                (table, source[table]),
            )


def _insert_migration_source(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_id: int,
    current_revision_id: int,
    actor: str,
    imported_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO migration_sources(
            source_schema_version, source_state_path, source_database_path,
            source_database_sha256, source_database_size_bytes,
            source_database_mtime_ns, source_state_identity_json,
            source_state_identity_sha256, project_id, revision_id,
            importer_package_version, imported_at, imported_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot.schema_version,
            str(snapshot.state_path),
            str(snapshot.database_path),
            snapshot.probe.database_sha256,
            snapshot.probe.database_size_bytes,
            snapshot.probe.database_mtime_ns,
            snapshot.probe.state_identity_json,
            snapshot.probe.state_identity_sha256,
            project_id,
            current_revision_id,
            __version__,
            imported_at,
            actor,
        ),
    )
    migration_source_id = int(cursor.lastrowid)
    connection.executemany(
        """
        INSERT INTO legacy_metadata(migration_source_id, source_key, source_value)
        VALUES (?, ?, ?)
        """,
        ((migration_source_id, key, value) for key, value in snapshot.metadata),
    )
    return migration_source_id


def _destination_projection(
    connection: sqlite3.Connection,
) -> dict[str, object]:
    queue_columns = ", ".join(V4_QUEUE_COLUMNS)
    return {
        "queue_items": [
            dict(row)
            for row in connection.execute(
                f"SELECT {queue_columns} FROM queue_items ORDER BY id"
            )
        ],
        "dependencies": [
            dict(row)
            for row in connection.execute(
                "SELECT queue_item_id, dependency_item_id FROM dependencies "
                "ORDER BY queue_item_id, dependency_item_id"
            )
        ],
        "gpu_allowlist": [
            dict(row)
            for row in connection.execute("SELECT * FROM gpu_allowlist ORDER BY uuid")
        ],
        "events": [
            {
                key: row[key]
                for key in (
                    "id",
                    "created_at",
                    "actor",
                    "event_type",
                    "queue_item_id",
                    "payload_json",
                )
            }
            for row in connection.execute("SELECT * FROM events ORDER BY id")
        ],
        "gpu_reservations": [
            dict(row)
            for row in connection.execute("SELECT * FROM gpu_reservations ORDER BY id")
        ],
    }


def _source_projection(snapshot: LegacyStateSnapshot) -> dict[str, object]:
    return {
        "queue_items": snapshot.normalized_queue_items(),
        "dependencies": snapshot.dependencies,
        "gpu_allowlist": snapshot.gpu_allowlist,
        "events": snapshot.events,
        "gpu_reservations": snapshot.gpu_reservations,
    }


def _verify_destination(
    connection: sqlite3.Connection,
    *,
    snapshot: LegacyStateSnapshot,
    project_id: int,
    revision_ids: tuple[int, ...],
    revision_by_commit: Mapping[str, int],
) -> dict[str, object]:
    integrity_rows = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    integrity = "; ".join(integrity_rows) or "no result"
    if integrity != "ok":
        raise V5MigrationError(f"candidate SQLite integrity_check failed: {integrity}")
    violations = list(connection.execute("PRAGMA foreign_key_check"))
    if violations:
        raise V5MigrationError(
            f"candidate SQLite foreign_key_check found {len(violations)} violation(s)"
        )
    source_projection = _source_projection(snapshot)
    destination_projection = _destination_projection(connection)
    if destination_projection != source_projection:
        for table in _MIGRATED_TABLES:
            if destination_projection[table] != source_projection[table]:
                raise V5MigrationError(
                    f"field-by-field comparison failed for migrated table {table}"
                )
        raise V5MigrationError("field-by-field legacy projection comparison failed")
    source_sequences = {
        name: value
        for name, value in snapshot.sequences
        if name in _SOURCE_SEQUENCE_TABLES
    }
    destination_sequences = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT name, seq FROM sqlite_sequence WHERE name IN "
            "('queue_items', 'events', 'gpu_reservations') ORDER BY name"
        )
    }
    if destination_sequences != source_sequences:
        raise V5MigrationError(
            "destination sqlite_sequence high-water values differ from the source: "
            f"source={source_sequences}, destination={destination_sequences}"
        )
    item_events = sum(row["queue_item_id"] is not None for row in snapshot.events)
    itemless_events = len(snapshot.events) - item_events
    scope_counts = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT scope, COUNT(*) FROM events GROUP BY scope ORDER BY scope"
        )
    }
    expected_scopes = {
        key: value
        for key, value in {"project": item_events, "host": itemless_events}.items()
        if value
    }
    if scope_counts != expected_scopes:
        raise V5MigrationError(
            f"event scope mapping differs: expected {expected_scopes}, got {scope_counts}"
        )
    imported_metadata = [
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT source_key, source_value FROM legacy_metadata ORDER BY source_key"
        )
    ]
    if imported_metadata != sorted(snapshot.metadata):
        raise V5MigrationError("legacy metadata text was not preserved exactly")
    revision_rows = list(
        connection.execute(
            "SELECT id, git_commit FROM project_revisions ORDER BY sequence"
        )
    )
    expected_commits = list(snapshot.commits) if snapshot.commits else [None]
    if [row["git_commit"] for row in revision_rows] != expected_commits:
        raise V5MigrationError("legacy revisions do not map one-to-one to distinct commits")
    if any(row["project_manifest_path"] is not None for row in connection.execute(
        "SELECT project_manifest_path FROM project_revisions"
    )):
        raise V5MigrationError("legacy revisions fabricated a Project manifest path")
    if connection.execute("SELECT COUNT(*) FROM admission_snapshots").fetchone()[0] != 0:
        raise V5MigrationError("legacy import fabricated typed admission evidence")
    counts = {
        table: {
            "source": len(cast(list[object], source_projection[table])),
            "destination": len(cast(list[object], destination_projection[table])),
        }
        for table in _MIGRATED_TABLES
    }
    queue_rows = cast(list[dict[str, object]], source_projection["queue_items"])
    event_rows = cast(list[dict[str, object]], source_projection["events"])
    reservation_rows = cast(
        list[dict[str, object]], source_projection["gpu_reservations"]
    )
    dependency_rows = cast(
        list[dict[str, object]], source_projection["dependencies"]
    )
    allowlist_rows = cast(
        list[dict[str, object]], source_projection["gpu_allowlist"]
    )
    state_counts: dict[str, int] = {}
    for row in queue_rows:
        state = cast(str, row["state"])
        state_counts[state] = state_counts.get(state, 0) + 1
    source_table_digests = {
        table: sha256_bytes(
            sqlite_rows_evidence_bytes(
                cast(list[dict[str, object]], source_projection[table])
            )
        )
        for table in _MIGRATED_TABLES
    }
    destination_table_digests = {
        table: sha256_bytes(
            sqlite_rows_evidence_bytes(
                cast(list[dict[str, object]], destination_projection[table])
            )
        )
        for table in _MIGRATED_TABLES
    }
    return {
        "verified": True,
        "importer_package_version": __version__,
        "source_schema_version": snapshot.schema_version,
        "source_state_entry_count": len(
            cast(list[object], json.loads(snapshot.probe.state_identity_json))
        ),
        "legacy_defaults": dict(snapshot.legacy_defaults),
        "row_counts": counts,
        "state_counts": state_counts,
        "queue_item_ids": [str(row["id"]) for row in queue_rows],
        "event_ids": [str(row["id"]) for row in event_rows],
        "reservation_ids": [str(row["id"]) for row in reservation_rows],
        "dependency_pairs": [
            {
                "queue_item_id": str(row["queue_item_id"]),
                "dependency_item_id": str(row["dependency_item_id"]),
            }
            for row in dependency_rows
        ],
        "gpu_allowlist_uuids": [cast(str, row["uuid"]) for row in allowlist_rows],
        "source_sequences": {
            name: str(value) for name, value in source_sequences.items()
        },
        "destination_sequences": {
            name: str(value) for name, value in destination_sequences.items()
        },
        "source_table_sha256": source_table_digests,
        "destination_table_sha256": destination_table_digests,
        "event_scope_mapping": {
            "queue_item_event": "project",
            "itemless_event": "host",
        },
        "revision_by_commit": [
            {"git_commit": commit, "revision_id": revision_by_commit[commit]}
            for commit in snapshot.commits
        ],
        "project_id": project_id,
        "revision_ids": list(revision_ids),
    }


def _build_candidate(
    *,
    candidate: Path,
    snapshot: LegacyStateSnapshot,
    project_key: str,
    checkout: Path,
    actor: str,
    imported_at: str,
) -> tuple[V5QueueStore, _BuildEvidence, int]:
    store = V5QueueStore(candidate)
    store.initialize()
    evidence = _BuildEvidence(database_instance_id=store.instance_identity())
    migration_source_id: int
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        created_at = (
            cast(str, snapshot.queue_items[0]["added_at"])
            if snapshot.queue_items
            else imported_at
        )
        project_id, revision_ids, revision_by_commit = _insert_project_and_revisions(
            connection,
            snapshot=snapshot,
            project_key=project_key,
            checkout=checkout,
            actor=actor,
            created_at=created_at,
        )
        evidence.project_id = project_id
        evidence.revision_ids = revision_ids
        evidence.current_revision_id = revision_ids[-1]
        _insert_queue_rows(
            connection,
            snapshot=snapshot,
            project_id=project_id,
            revision_by_commit=revision_by_commit,
        )
        _insert_dependencies(connection, snapshot)
        _insert_allowlist(connection, snapshot)
        _insert_events(connection, snapshot=snapshot, project_id=project_id)
        _insert_reservations(connection, snapshot)
        _insert_artifacts(
            connection,
            snapshot=snapshot,
            project_id=project_id,
            revision_by_commit=revision_by_commit,
        )
        migration_source_id = _insert_migration_source(
            connection,
            snapshot=snapshot,
            project_id=project_id,
            current_revision_id=revision_ids[-1],
            actor=actor,
            imported_at=imported_at,
        )
        _restore_source_sequences(connection, snapshot)
        evidence.comparison = _verify_destination(
            connection,
            snapshot=snapshot,
            project_id=project_id,
            revision_ids=revision_ids,
            revision_by_commit=revision_by_commit,
        )
        evidence.integrity_check = "ok"
        evidence.foreign_key_violations = 0
        connection.commit()
    return store, evidence, migration_source_id


def _insert_published_receipt(
    store: V5QueueStore,
    *,
    migration_source_id: int,
    evidence: _BuildEvidence,
    receipt: QueueMigrationReceipt,
) -> None:
    assert evidence.project_id is not None
    assert evidence.current_revision_id is not None
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO migration_receipts(
                migration_source_id, project_id, revision_id,
                protocol_api_version, protocol_kind, result, receipt_json,
                receipt_sha256, started_at, finished_at, actor
            ) VALUES (?, ?, ?, 'experiment-queue/v1', 'QueueMigrationReceipt',
                      'succeeded', ?, ?, ?, ?, ?)
            """,
            (
                migration_source_id,
                evidence.project_id,
                evidence.current_revision_id,
                receipt.canonical_json,
                receipt.sha256,
                receipt.started_at,
                receipt.finished_at,
                receipt.actor,
            ),
        )
        connection.commit()


def _final_database_checks(store: V5QueueStore) -> tuple[str, int]:
    with store.connect() as connection:
        integrity = "; ".join(
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        )
        violations = len(list(connection.execute("PRAGMA foreign_key_check")))
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is None or int(checkpoint[0]) != 0:
            raise V5MigrationError(
                f"candidate WAL checkpoint could not complete: {checkpoint!r}"
            )
    if integrity != "ok" or violations:
        raise V5MigrationError(
            f"final candidate verification failed: integrity={integrity!r}, "
            f"foreign_key_violations={violations}"
        )
    return integrity, violations


def _verify_published_destination(
    destination: Path,
    *,
    expected_instance_id: str,
    expected_receipt: QueueMigrationReceipt,
    boundary: SecurePathBoundary | None = None,
) -> None:
    """Reopen the committed name before exposing its staged success receipt."""

    published_store = V5QueueStore(destination)
    actual_instance_id = published_store.instance_identity()
    if actual_instance_id != expected_instance_id:
        raise V5MigrationError(
            "published destination database instance differs from the verified "
            f"candidate: expected {expected_instance_id}, got {actual_instance_id}"
        )
    with published_store.connect() as connection:
        integrity = "; ".join(
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        )
        violations = list(connection.execute("PRAGMA foreign_key_check"))
        row = connection.execute(
            "SELECT result, receipt_json, receipt_sha256 FROM migration_receipts"
        ).fetchone()
    if integrity != "ok" or violations:
        raise V5MigrationError(
            "published destination failed post-rename verification: "
            f"integrity={integrity!r}, foreign_key_violations={len(violations)}"
        )
    if (
        row is None
        or row["result"] != "succeeded"
        or row["receipt_json"] != expected_receipt.canonical_json
        or row["receipt_sha256"] != expected_receipt.sha256
    ):
        raise V5MigrationError(
            "published destination does not contain the exact staged success receipt"
        )
    if boundary is not None:
        _revalidate_path_boundary(boundary)


def _checks(*, dry_run: bool, continuation_count: int) -> list[dict[str, str]]:
    return [
        {"name": "source-identity", "status": "passed", "detail": "complete source tree hashed before and after mapping"},
        {"name": "source-schema", "status": "passed", "detail": "authentic historical schema and every row were inspected immutable/read-only"},
        {"name": "queue-quiescent", "status": "passed", "detail": "no starting, running, yielding, terminating, or force_killing items"},
        {"name": "git-evidence", "status": "passed", "detail": "every distinct commit and exact committed legacy card were authenticated"},
        {
            "name": "continuation-evidence",
            "status": "passed" if continuation_count else "not-applicable",
            "detail": (
                f"verified {continuation_count} continuation evidence file(s)"
                if continuation_count
                else "source contains no continuation checkpoint evidence"
            ),
        },
        {"name": "destination-integrity", "status": "passed", "detail": "SQLite integrity_check is ok and foreign_key_check is empty"},
        {"name": "field-comparison", "status": "passed", "detail": "every legacy column, row, metadata value, and source sequence matched field-by-field"},
        {
            "name": "atomic-publish",
            "status": "not-applicable" if dry_run else "passed",
            "detail": (
                "dry run discarded its verified sibling candidate"
                if dry_run
                else "verified sibling candidate published with one atomic rename"
            ),
        },
    ]


def _make_receipt(
    *,
    operation_id: str,
    dry_run: bool,
    result: MigrationResult,
    project_key: str,
    actor: str,
    started_at: str,
    finished_at: str,
    probe: LegacySourceProbe,
    destination: Path,
    evidence: _BuildEvidence,
    snapshot: LegacyStateSnapshot | None,
    published: bool,
    error: str | None,
    failed_check_name: str = "migration-operation",
) -> QueueMigrationReceipt:
    successful = result is MigrationResult.SUCCEEDED
    continuation_count = (
        sum(len(cast(list[object], row["files"])) for row in snapshot.continuation_checks)
        if snapshot is not None
        else 0
    )
    checks = (
        _checks(dry_run=dry_run, continuation_count=continuation_count)
        if successful
        else [
            {
                "name": failed_check_name,
                "status": "failed",
                "detail": (error or "migration failed")[:16_384],
            }
        ]
    )
    comparison = evidence.comparison or {
        "verified": False,
        "source_schema_version": probe.schema_version,
    }
    return QueueMigrationReceipt.create(
        operation_id=operation_id,
        mode=MigrationMode.DRY_RUN if dry_run else MigrationMode.IMPORT,
        result=result,
        project_key=project_key,
        actor=actor,
        started_at=started_at,
        finished_at=finished_at,
        source=probe.receipt_document(),
        destination={
            "state_path": str(destination),
            "schema_version": SCHEMA_VERSION,
            "schema_identity": SCHEMA_IDENTITY,
            "database_instance_id": evidence.database_instance_id,
            # Published receipts live inside the database they authenticate, so
            # the whole-file digest would be self-referential.  Dry runs can
            # record their discarded candidate hash directly.
            "database_sha256": (
                evidence.candidate_database_sha256 if dry_run else None
            ),
            "integrity_check": evidence.integrity_check,
            "foreign_key_violations": evidence.foreign_key_violations,
            "published": published,
        },
        project={
            "key": project_key,
            "id": evidence.project_id,
            "revision_ids": list(evidence.revision_ids),
            "lifecycle": "paused",
        },
        comparison=cast(Mapping[str, object], comparison),
        path_inventory=(
            cast(list[object], snapshot.path_inventory) if snapshot is not None else []
        ),
        continuation_checks=(
            cast(list[object], snapshot.continuation_checks)
            if snapshot is not None
            else []
        ),
        checks=cast(list[object], checks),
        error=error,
    )


def migrate_legacy_state(
    *,
    source_state_copy: Path,
    destination_state: Path,
    project_key: str,
    legacy_checkout: Path,
    actor: str,
    receipt_path: Path,
    dry_run: bool = False,
    protected_roots: Sequence[Path] = (),
    operation_id: str | None = None,
    confirm_source_is_copy: bool = False,
) -> V5MigrationOutcome:
    """Fully verify and dry-run or atomically import one legacy state copy.

    ``confirm_source_is_copy`` is deliberately mandatory: callers must attest
    that they supplied an offline copy, not the operator's production state.
    The source is still opened only through immutable read-only SQLite handles.
    """

    started_at = _utc_now()
    key = _validate_project_key(project_key)
    migration_actor = _validate_actor(actor)
    if not confirm_source_is_copy:
        raise V5MigrationError(
            "refused legacy import without confirm_source_is_copy=True; stop all "
            "legacy writers, create a complete copy, and import only that copy"
        )
    snapshot: LegacyStateSnapshot | None = None
    probe: LegacySourceProbe | None = None
    candidate: Path | None = None
    destination_boundary: SecurePathBoundary | None = None
    destination = Path(destination_state)
    external_receipt = Path(receipt_path)
    evidence = _BuildEvidence()
    published = False
    resolved_operation_id = operation_id
    current_check = "source-authentication-and-quiescence"
    try:
        snapshot = load_legacy_state(
            Path(source_state_copy), legacy_checkout=Path(legacy_checkout)
        )
        probe = snapshot.probe
        current_check = "path-isolation"
        destination, external_receipt, _ = _validate_paths(
            source=snapshot,
            destination_state=Path(destination_state),
            legacy_checkout=Path(legacy_checkout),
            receipt_path=Path(receipt_path),
            protected_roots=protected_roots,
        )
        destination_boundary = _secure_path_boundary(
            destination,
            label="migration destination state",
        )
        if resolved_operation_id is None:
            resolved_operation_id = (
                f"migrate-v5-{snapshot.probe.state_identity_sha256[:24]}"
            )
        resolved_operation_id = _validate_operation_id(resolved_operation_id)
        candidate = destination.parent / (
            f".{destination.name}.{resolved_operation_id}.candidate"
        )
        if candidate.exists() or candidate.is_symlink():
            raise V5MigrationError(
                f"candidate path {candidate} already exists; inspect and remove only "
                "stale unpublished migration state before retrying"
            )
        checkout = _absolute_existing_directory(
            Path(legacy_checkout), label="legacy checkout"
        )
        current_check = "candidate-build-and-field-comparison"
        store, evidence, migration_source_id = _build_candidate(
            candidate=candidate,
            snapshot=snapshot,
            project_key=key,
            checkout=checkout,
            actor=migration_actor,
            imported_at=started_at,
        )
        current_check = "candidate-final-integrity"
        _final_database_checks(store)
        evidence.integrity_check = "ok"
        evidence.foreign_key_violations = 0
        evidence.candidate_database_sha256 = _sha256_file(
            candidate / DATABASE_FILENAME
        )
        assert evidence.comparison is not None
        evidence.comparison["pre_receipt_candidate_database_sha256"] = (
            evidence.candidate_database_sha256
        )
        snapshot.assert_unchanged()
        finished_at = _utc_now()
        current_check = "success-receipt-construction"
        # For a real import this is a staged commit record: ``published: true``
        # becomes truthful only when the complete directory (including these
        # exact bytes) is atomically renamed to the requested destination. The
        # record is never emitted externally until that name is reopened and
        # its database instance plus embedded receipt are re-authenticated.
        success_receipt = _make_receipt(
            operation_id=resolved_operation_id,
            dry_run=dry_run,
            result=MigrationResult.SUCCEEDED,
            project_key=key,
            actor=migration_actor,
            started_at=started_at,
            finished_at=finished_at,
            probe=probe,
            destination=destination,
            evidence=evidence,
            snapshot=snapshot,
            published=not dry_run,
            error=None,
        )
        if dry_run:
            current_check = "dry-run-candidate-cleanup"
            _safe_remove_candidate(candidate, destination)
            candidate = None
            snapshot.assert_unchanged()
            current_check = "external-receipt-publication"
            _atomic_write_receipt(external_receipt, success_receipt)
            return V5MigrationOutcome(
                destination_state=destination,
                receipt_path=external_receipt,
                receipt=success_receipt,
                published=False,
            )

        current_check = "destination-receipt-persistence"
        _insert_published_receipt(
            store,
            migration_source_id=migration_source_id,
            evidence=evidence,
            receipt=success_receipt,
        )
        _final_database_checks(store)
        snapshot.assert_unchanged()
        current_check = "atomic-publish"
        _revalidate_path_boundary(destination_boundary)
        _publish_state(candidate, destination)
        published = True
        candidate = None
        try:
            current_check = "published-destination-parent-identity"
            _revalidate_path_boundary(destination_boundary)
            current_check = "published-destination-identity-and-receipt"
            assert evidence.database_instance_id is not None
            _verify_published_destination(
                destination,
                expected_instance_id=evidence.database_instance_id,
                expected_receipt=success_receipt,
                boundary=destination_boundary,
            )
            current_check = "source-post-publish-identity"
            snapshot.assert_unchanged()
            current_check = "external-receipt-publication"
            _revalidate_path_boundary(destination_boundary)
            _atomic_write_receipt(external_receipt, success_receipt)
            try:
                _revalidate_path_boundary(destination_boundary)
            except V5MigrationError as boundary_exc:
                raise _ReceiptPublicationIndeterminate(
                    "successful external receipt is durable, but the migration "
                    "destination ancestor identity changed during receipt "
                    f"publication ({boundary_exc}); preserve all names and inspect "
                    "them before retrying",
                    receipt=success_receipt,
                ) from boundary_exc
        except _ReceiptPublicationIndeterminate:
            # The final receipt name already exists. Preserve the matching
            # destination so the visible succeeded/published receipt can never
            # attest to state this error path subsequently removed.
            raise
        except BaseException:
            # Publication is reversible until its required external receipt is
            # durable. Move our new destination back to its known sibling and
            # fsync that rollback before ordinary candidate cleanup is allowed.
            rollback_candidate = destination.parent / (
                f".{destination.name}.{resolved_operation_id}.candidate"
            )
            _rollback_published_state(
                destination,
                rollback_candidate,
                reason=f"post-publication check {current_check!r} failed",
                boundary=destination_boundary,
            )
            candidate = rollback_candidate
            published = False
            raise
        return V5MigrationOutcome(
            destination_state=destination,
            receipt_path=external_receipt,
            receipt=success_receipt,
            published=True,
        )
    except BaseException as exc:
        if isinstance(
            exc,
            (_ReceiptPublicationIndeterminate, _StatePublicationIndeterminate),
        ):
            raise
        if candidate is not None and candidate.exists():
            try:
                _safe_remove_candidate(candidate, destination)
            except BaseException:
                # Preserve the original failure and explicitly report cleanup in
                # the combined error rather than widening deletion scope.
                exc = V5MigrationError(
                    f"{exc}; unpublished candidate cleanup also failed at {candidate}"
                )
        if probe is None and isinstance(exc, LegacyStateError):
            probe = exc.probe
        failure_receipt: QueueMigrationReceipt | None = None
        rendered = str(exc) or type(exc).__name__
        if probe is not None:
            try:
                if resolved_operation_id is None:
                    resolved_operation_id = (
                        f"migrate-v5-{probe.state_identity_sha256[:24]}"
                    )
                failure_receipt = _make_receipt(
                    operation_id=_validate_operation_id(resolved_operation_id),
                    dry_run=dry_run,
                    result=MigrationResult.FAILED,
                    project_key=key,
                    actor=migration_actor,
                    started_at=started_at,
                    finished_at=_utc_now(),
                    probe=probe,
                    destination=(
                        destination
                        if destination.is_absolute()
                        else Path(destination_state).absolute()
                    ),
                    evidence=evidence,
                    snapshot=snapshot,
                        published=published,
                        error=rendered[:16_384],
                        failed_check_name=current_check,
                )
                safe_failure_receipt = _safe_failure_receipt_path(
                    external_receipt,
                    probe=probe,
                    destination=destination,
                    legacy_checkout=Path(legacy_checkout),
                    protected_roots=protected_roots,
                    path_inventory=(
                        snapshot.path_inventory if snapshot is not None else ()
                    ),
                )
                _atomic_write_receipt(safe_failure_receipt, failure_receipt)
            except BaseException as receipt_exc:
                rendered = f"{rendered}; could not write failure receipt: {receipt_exc}"
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise V5MigrationError(rendered, receipt=failure_receipt) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="experiment-queue-migrate-v5",
        description=(
            "Offline import of a complete, quiescent schema-v1 through v4 state "
            "copy into a new absent schema-v5 directory."
        ),
    )
    parser.add_argument(
        "--source-state",
        required=True,
        type=Path,
        help="absolute path to a complete offline legacy state copy containing queue.sqlite3",
    )
    parser.add_argument(
        "--destination-state",
        required=True,
        type=Path,
        help="absolute absent destination directory; never merged or overwritten",
    )
    parser.add_argument(
        "--project-key",
        required=True,
        help="new lowercase schema-v5 Project key owning every imported legacy row",
    )
    parser.add_argument(
        "--legacy-checkout",
        required=True,
        type=Path,
        help="absolute Git checkout matching legacy metadata.repo_root and all recorded commits",
    )
    parser.add_argument(
        "--actor",
        required=True,
        help="operator identity recorded in Project, migration source, and receipt evidence",
    )
    parser.add_argument(
        "--receipt",
        required=True,
        type=Path,
        help="fresh absolute external QueueMigrationReceipt/v1 path outside source and destination",
    )
    parser.add_argument(
        "--legacy-root",
        action="append",
        type=Path,
        default=[],
        help="absolute protected artifact/worktree root the destination must not overlap; repeat as needed",
    )
    parser.add_argument(
        "--operation-id",
        help="optional stable receipt operation ID; defaults deterministically from source identity",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build and fully verify a temporary v5 candidate, write a receipt, then discard it without creating the destination",
    )
    parser.add_argument(
        "--confirm-source-is-copy",
        action="store_true",
        help="required attestation that --source-state is an offline copy, never live operator state",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the offline importer CLI and return a process-style status code."""

    arguments = _parser().parse_args(argv)
    try:
        outcome = migrate_legacy_state(
            source_state_copy=arguments.source_state,
            destination_state=arguments.destination_state,
            project_key=arguments.project_key,
            legacy_checkout=arguments.legacy_checkout,
            actor=arguments.actor,
            receipt_path=arguments.receipt,
            dry_run=arguments.dry_run,
            protected_roots=arguments.legacy_root,
            operation_id=arguments.operation_id,
            confirm_source_is_copy=arguments.confirm_source_is_copy,
        )
    except V5MigrationError as exc:
        print(f"migration failed: {exc}", file=sys.stderr)
        return 2
    action = "validated without publication" if not outcome.published else "published"
    print(
        f"schema-v5 migration {action}: destination={outcome.destination_state} "
        f"receipt={outcome.receipt_path} sha256={outcome.receipt.sha256}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "V5MigrationError",
    "V5MigrationOutcome",
    "main",
    "migrate_legacy_state",
]
