"""Exercise the copy-only, receipt-producing legacy-to-v5 importer boundary."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
from typing import cast
import uuid

import pytest

from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.legacy import LegacyMarkdownCard
from experiment_queue.legacy_state import (
    LEGACY_QUEUE_COLUMNS,
    LEGACY_SCHEMA_SOURCE_COMMITS,
)
from experiment_queue.migrate_v5 import (
    V5MigrationError,
    main,
    migrate_legacy_state,
)
from experiment_queue.migration_receipt import QueueMigrationReceipt


NOW = "2026-08-28T12:00:00+00:00"

# Each fixture DDL below is a direct semantic transcription of the corresponding
# locally extracted historical definition, whose commit remains part of the
# durable compatibility evidence rather than a runtime test dependency.
assert LEGACY_SCHEMA_SOURCE_COMMITS == {
    1: "eb7d0c5d16e40643ee2554eaea1970c6217fa126",
    2: "0f8b98d3d0006ae8918d3caf1c518ca72d320178",
    3: "cc68ce9a10b6e9979ebfffedb036be6c502ce36e",
    4: "4569a86a75d559ba99378e54fce301a7415ee57e",
}

_BASE_QUEUE_DEFINITIONS = (
    "id INTEGER PRIMARY KEY AUTOINCREMENT",
    "experiment_id TEXT NOT NULL",
    "attempt INTEGER NOT NULL",
    "state TEXT NOT NULL",
    "priority INTEGER NOT NULL DEFAULT 0",
    "card_path TEXT NOT NULL",
    "card_sha256 TEXT NOT NULL",
    "command_text TEXT NOT NULL",
    "runner_name TEXT NOT NULL",
    "git_commit TEXT NOT NULL",
    "added_at TEXT NOT NULL",
    "added_by TEXT NOT NULL",
    "state_detail TEXT",
    "assigned_gpu_uuid TEXT",
    "assigned_gpu_index TEXT",
    "pid INTEGER",
    "pgid INTEGER",
    "proc_start_ticks TEXT",
    "started_at TEXT",
    "finished_at TEXT",
    "return_code INTEGER",
    "terminate_requested_at TEXT",
    "terminate_reason TEXT",
    "termination_stage TEXT",
    "termination_signal_epoch REAL",
    "contention_detected INTEGER NOT NULL DEFAULT 0",
    "repo_drift_detected INTEGER NOT NULL DEFAULT 0",
    "runner_run_dir TEXT",
    "runner_manifest_path TEXT",
    "rsync_pull_command TEXT",
)

_V2_QUEUE_DEFINITIONS = (
    "preemptible INTEGER NOT NULL DEFAULT 0",
    "segment INTEGER NOT NULL DEFAULT 1",
    "resume_front INTEGER NOT NULL DEFAULT 0",
    "yield_requested_at TEXT",
    "yield_requested_by TEXT",
    "yield_request_id TEXT",
    "yield_note TEXT",
    "yield_duration_hours INTEGER",
    "continuation_checkpoint TEXT",
    "continuation_checkpoint_sha256 TEXT",
    "continuation_step INTEGER",
    "continuation_wandb_id TEXT",
)

_V3_QUEUE_DEFINITIONS = (
    "git_ref TEXT",
    "worktree_path TEXT",
    "worktree_created_at TEXT",
    "worktree_removed_at TEXT",
    "worktree_cleanup_error TEXT",
)

_V4_QUEUE_DEFINITIONS = (
    "continuation_checkpoint_metadata TEXT",
    "continuation_checkpoint_metadata_sha256 TEXT",
)


@dataclass(frozen=True, slots=True)
class LegacyFixture:
    state: Path
    checkout: Path
    external_root: Path
    database: Path
    commits: tuple[str, ...]
    source_database_bytes: bytes
    source_database_mtime_ns: int


def _run_git(checkout: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(checkout), *arguments],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _card_source(experiment_id: str, runner: str) -> bytes:
    return (
        f"# {experiment_id}: Import fixture\n\n"
        "## Exact Manual Command On Mutton2\n\n"
        "```bash\n"
        "python scripts/run_experiment.py --require-clean --remote mutton2 "
        f"--name {runner}\n"
        "```\n"
    ).encode()


def _create_checkout(root: Path, *, commits: int) -> tuple[Path, tuple[dict[str, object], ...]]:
    checkout = root / "legacy-checkout"
    checkout.mkdir()
    _run_git(checkout, "init", "-q")
    _run_git(checkout, "config", "user.email", "fixture@example.invalid")
    _run_git(checkout, "config", "user.name", "Migration Fixture")
    cards: list[dict[str, object]] = []
    for index in range(1, commits + 1):
        experiment_id = f"WCG-{index:03d}"
        runner = f"fixture-{index}"
        relative = Path("cards") / f"{experiment_id}.md"
        source = _card_source(experiment_id, runner)
        target = checkout / relative
        target.parent.mkdir(exist_ok=True)
        target.write_bytes(source)
        _run_git(checkout, "add", relative.as_posix())
        _run_git(checkout, "commit", "-q", "-m", f"card {index}")
        commit = _run_git(checkout, "rev-parse", "HEAD")
        parsed = LegacyMarkdownCard.from_source(
            source,
            experiment_id=experiment_id,
            source_name=relative.as_posix(),
        )
        cards.append(
            {
                "experiment_id": experiment_id,
                "runner_name": runner,
                "card_path": relative.as_posix(),
                "card_sha256": parsed.source_sha256,
                "command_text": parsed.command_text,
                "git_commit": commit,
            }
        )
    return checkout, tuple(cards)


def _queue_definitions(version: int) -> tuple[str, ...]:
    definitions = _BASE_QUEUE_DEFINITIONS
    if version >= 2:
        definitions += _V2_QUEUE_DEFINITIONS
    if version == 3:
        definitions += _V3_QUEUE_DEFINITIONS
    elif version == 4:
        # Schema-v4 inserted metadata evidence before step/wandb and Git fields.
        definitions = (
            _BASE_QUEUE_DEFINITIONS
            + _V2_QUEUE_DEFINITIONS[:10]
            + _V4_QUEUE_DEFINITIONS
            + _V2_QUEUE_DEFINITIONS[10:]
            + _V3_QUEUE_DEFINITIONS
        )
    return definitions


def _create_authentic_schema(connection: sqlite3.Connection, version: int) -> None:
    queue_definitions = ",\n".join(
        _queue_definitions(version) + ("UNIQUE(experiment_id, attempt)",)
    )
    reservation = """
        CREATE TABLE gpu_reservations (
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
        CREATE UNIQUE INDEX idx_gpu_reservations_open_gpu
            ON gpu_reservations(gpu_uuid)
            WHERE status IN ('pending', 'active');
        CREATE INDEX idx_gpu_reservations_status_expiry
            ON gpu_reservations(status, expires_at);
    """ if version >= 2 else ""
    connection.executescript(
        f"""
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE queue_items ({queue_definitions});
        CREATE INDEX queue_items_state_order
            ON queue_items(state, priority DESC, id ASC);
        CREATE TABLE dependencies (
            queue_item_id INTEGER NOT NULL,
            dependency_item_id INTEGER NOT NULL,
            PRIMARY KEY(queue_item_id, dependency_item_id),
            FOREIGN KEY(queue_item_id) REFERENCES queue_items(id),
            FOREIGN KEY(dependency_item_id) REFERENCES queue_items(id)
        );
        CREATE TABLE gpu_allowlist (
            uuid TEXT PRIMARY KEY,
            requested_identifier TEXT NOT NULL,
            last_index TEXT NOT NULL,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            draining INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            actor TEXT NOT NULL,
            event_type TEXT NOT NULL,
            queue_item_id INTEGER,
            payload_json TEXT NOT NULL,
            FOREIGN KEY(queue_item_id) REFERENCES queue_items(id)
        );
        {reservation}
        """
    )


def _row_for_card(
    *,
    version: int,
    card: dict[str, object],
    item_id: int,
    state: str,
    external_root: Path,
) -> dict[str, object]:
    run_dir = external_root / f"run-{item_id}"
    run_dir.mkdir()
    manifest = run_dir / "runner-manifest.json"
    manifest.write_text('{"fixture":true}\n')
    checkpoint = external_root / f"checkpoint-{item_id}.bin"
    checkpoint.write_bytes(f"checkpoint-{item_id}".encode())
    metadata = external_root / f"checkpoint-{item_id}.json"
    metadata.write_text('{"step":42}\n')
    worktree = external_root / f"worktree-{item_id}"
    row: dict[str, object] = {
        "id": item_id,
        **card,
        "attempt": 1,
        "state": state,
        "priority": item_id,
        "added_at": NOW,
        "added_by": "fixture",
        "state_detail": "historical exact detail",
        "assigned_gpu_uuid": "GPU-fixture",
        "assigned_gpu_index": "0",
        "pid": 12345,
        "pgid": 12345,
        "proc_start_ticks": "777",
        "started_at": NOW,
        "finished_at": NOW if state not in {"queued", "held", "blocked"} else None,
        "return_code": 0 if state == "succeeded" else None,
        "terminate_requested_at": None,
        "terminate_reason": None,
        "termination_stage": None,
        "termination_signal_epoch": None,
        "contention_detected": 1,
        "repo_drift_detected": 1,
        "runner_run_dir": str(run_dir),
        "runner_manifest_path": str(manifest),
        "rsync_pull_command": "rsync exact historical command",
    }
    if version >= 2:
        row.update(
            {
                "preemptible": 1,
                "segment": 2,
                "resume_front": 1,
                "yield_requested_at": NOW,
                "yield_requested_by": "fixture",
                "yield_request_id": f"yield-{item_id}",
                "yield_note": "historical yield",
                "yield_duration_hours": 3,
                "continuation_checkpoint": str(checkpoint),
                "continuation_checkpoint_sha256": hashlib.sha256(
                    checkpoint.read_bytes()
                ).hexdigest(),
                "continuation_step": 42,
                "continuation_wandb_id": f"wandb-{item_id}",
            }
        )
    if version >= 3:
        row.update(
            {
                "git_ref": f"refs/experiment-queue/items/{item_id}",
                "worktree_path": str(worktree),
                "worktree_created_at": NOW,
                "worktree_removed_at": None,
                "worktree_cleanup_error": (
                    None
                    if state in {"queued", "held", "blocked"}
                    else "historical cleanup failure"
                ),
            }
        )
    if version >= 4:
        row.update(
            {
                "continuation_checkpoint_metadata": str(metadata),
                "continuation_checkpoint_metadata_sha256": hashlib.sha256(
                    metadata.read_bytes()
                ).hexdigest(),
            }
        )
    return row


def _legacy_fixture(
    tmp_path: Path,
    *,
    version: int,
    state: str = "succeeded",
    item_count: int = 1,
) -> LegacyFixture:
    checkout, cards = _create_checkout(tmp_path, commits=item_count)
    external = tmp_path / "legacy-artifacts"
    external.mkdir()
    state_dir = tmp_path / "legacy-state"
    state_dir.mkdir()
    database = state_dir / "queue.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = DELETE")
        _create_authentic_schema(connection, version)
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            (
                ("schema_version", str(version)),
                ("repo_root", str(checkout.resolve())),
                ("unknown.future.key", "  exact unknown text  "),
            ),
        )
        rows = [
            _row_for_card(
                version=version,
                card=dict(card),
                item_id=10 + index,
                state=state if index == 0 else "held",
                external_root=external,
            )
            for index, card in enumerate(cards)
        ]
        if version >= 3:
            for row in rows:
                reference = cast(str, row["git_ref"])
                commit = cast(str, row["git_commit"])
                worktree = Path(cast(str, row["worktree_path"]))
                _run_git(checkout, "update-ref", reference, commit)
                _run_git(
                    checkout,
                    "worktree",
                    "add",
                    "--detach",
                    str(worktree),
                    commit,
                )
        columns = LEGACY_QUEUE_COLUMNS[version]
        connection.executemany(
            f"INSERT INTO queue_items({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            (tuple(row[column] for column in columns) for row in rows),
        )
        if len(rows) > 1:
            connection.execute(
                "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (?, ?)",
                (rows[1]["id"], rows[0]["id"]),
            )
        connection.execute(
            """
            INSERT INTO gpu_allowlist(
                uuid, requested_identifier, last_index, name, enabled,
                draining, updated_at
            ) VALUES ('GPU-fixture', '0', '0', 'Fixture GPU', 1, 1, ?)
            """,
            (NOW,),
        )
        if rows:
            connection.execute(
                """
                INSERT INTO events(
                    id, created_at, actor, event_type, queue_item_id, payload_json
                ) VALUES (20, ?, 'fixture', 'ITEM_HISTORY', ?, '{"exact":true}')
                """,
                (NOW, rows[0]["id"]),
            )
        connection.execute(
            """
            INSERT INTO events(
                id, created_at, actor, event_type, queue_item_id, payload_json
            ) VALUES (?, ?, 'fixture', 'HOST_HISTORY', NULL, '{"host":true}')
            """,
            (21 if rows else 20, NOW),
        )
        if version >= 2:
            connection.execute(
                """
                INSERT INTO gpu_reservations(
                    id, gpu_uuid, queue_item_id, status, requested_at,
                    requested_by, note, duration_hours, starts_at, expires_at,
                    released_at, released_by, state_detail
                ) VALUES (6, 'GPU-fixture', ?, 'released', ?, 'fixture',
                          'exact reservation', 4, ?, ?, ?, 'fixture', 'done')
                """,
                (rows[0]["id"] if rows else None, NOW, NOW, NOW, NOW),
            )
        connection.execute(
            "UPDATE sqlite_sequence SET seq = 99 WHERE name = 'queue_items'"
        )
        connection.execute("UPDATE sqlite_sequence SET seq = 88 WHERE name = 'events'")
        if version >= 2:
            connection.execute(
                "UPDATE sqlite_sequence SET seq = 77 WHERE name = 'gpu_reservations'"
            )
        connection.commit()
    details = database.stat()
    return LegacyFixture(
        state=state_dir.resolve(),
        checkout=checkout.resolve(),
        external_root=external.resolve(),
        database=database.resolve(),
        commits=tuple(cast(str, card["git_commit"]) for card in cards),
        source_database_bytes=database.read_bytes(),
        source_database_mtime_ns=details.st_mtime_ns,
    )


def _migrate(
    fixture: LegacyFixture,
    tmp_path: Path,
    *,
    dry_run: bool = False,
    destination: Path | None = None,
    receipt: Path | None = None,
):
    return migrate_legacy_state(
        source_state_copy=fixture.state,
        destination_state=(destination or (tmp_path / "v5-state")).resolve(),
        project_key="flowers-legacy",
        legacy_checkout=fixture.checkout,
        actor="migration-test",
        receipt_path=(receipt or (tmp_path / "migration-receipt.json")).resolve(),
        dry_run=dry_run,
        protected_roots=(fixture.external_root,),
        confirm_source_is_copy=True,
    )


@pytest.mark.parametrize("version", [1, 2, 3, 4])
def test_authentic_v1_through_v4_import_preserves_rows_sequences_and_source(
    tmp_path: Path,
    version: int,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=version)
    outcome = _migrate(fixture, tmp_path)
    assert outcome.published is True
    assert outcome.destination_state.is_dir()
    external = QueueMigrationReceipt.from_bytes(outcome.receipt_path.read_bytes())
    assert external.sha256 == outcome.receipt.sha256
    receipt_document = external.to_document()
    assert receipt_document["result"] == "succeeded"
    assert fixture.database.read_bytes() == fixture.source_database_bytes
    assert fixture.database.stat().st_mtime_ns == fixture.source_database_mtime_ns
    imported_store = V5QueueStore(outcome.destination_state)
    assert (
        imported_store.instance_identity()
        == receipt_document["destination"]["database_instance_id"]
    )
    with imported_store.connect() as connection:
        project = connection.execute("SELECT * FROM projects").fetchone()
        assert project["project_key"] == "flowers-legacy"
        assert project["lifecycle"] == "paused"
        item = connection.execute("SELECT * FROM queue_items").fetchone()
        assert item["id"] == 10
        assert item["admission_kind"] == "LegacyMarkdownCard/v0"
        assert item["snapshot_id"] is None
        assert item["runtime_git_ref"] is None
        assert connection.execute(
            "SELECT source_value FROM legacy_metadata "
            "WHERE source_key = 'unknown.future.key'"
        ).fetchone()[0] == "  exact unknown text  "
        sequences = dict(
            connection.execute(
                "SELECT name, seq FROM sqlite_sequence WHERE name IN "
                "('queue_items', 'events', 'gpu_reservations')"
            )
        )
        assert sequences["queue_items"] == 99
        assert sequences["events"] == 88
        assert ("gpu_reservations" in sequences) is (version >= 2)
        scopes = dict(
            connection.execute("SELECT event_type, scope FROM events ORDER BY id")
        )
        assert scopes == {"ITEM_HISTORY": "project", "HOST_HISTORY": "host"}
        assert connection.execute("SELECT COUNT(*) FROM migration_receipts").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM admission_snapshots").fetchone()[0] == 0
        revision = connection.execute("SELECT * FROM project_revisions").fetchone()
        assert revision["project_manifest_path"] is None
        assert revision["project_blob_object_id"] is None


def test_v4_import_preserves_distinct_commit_revisions_dependencies_and_artifacts(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=2)
    outcome = _migrate(fixture, tmp_path)
    with V5QueueStore(outcome.destination_state).connect() as connection:
        revisions = list(
            connection.execute(
                "SELECT id, git_commit FROM project_revisions ORDER BY sequence"
            )
        )
        assert [row["git_commit"] for row in revisions] == list(fixture.commits)
        assert connection.execute("SELECT COUNT(*) FROM dependencies").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM job_artifacts").fetchone()[0] == 10
        item = connection.execute("SELECT * FROM queue_items WHERE id = 10").fetchone()
        assert item["continuation_step"] == 42
        assert item["worktree_cleanup_error"] == "historical cleanup failure"
        assert item["runtime_worktree_cleanup_error"] is None


def test_empty_legacy_queue_gets_one_explicit_null_commit_revision(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=0)
    outcome = _migrate(fixture, tmp_path)
    with V5QueueStore(outcome.destination_state).connect() as connection:
        revision = connection.execute("SELECT * FROM project_revisions").fetchone()
        assert revision["revision_kind"] == "legacy-v4"
        assert revision["git_commit"] is None
        assert revision["project_manifest_path"] is None
        assert connection.execute("SELECT COUNT(*) FROM queue_items").fetchone()[0] == 0
        assert connection.execute("SELECT scope FROM events").fetchone()[0] == "host"


def test_dry_run_builds_and_verifies_without_creating_destination(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination = (tmp_path / "v5-dry-run").resolve()
    outcome = _migrate(fixture, tmp_path, dry_run=True, destination=destination)
    assert outcome.published is False
    assert not destination.exists()
    document = outcome.receipt.to_document()
    assert document["mode"] == "dry-run"
    assert document["destination"]["published"] is False
    assert document["destination"]["database_sha256"] is not None
    dry_run_instance_id = document["destination"]["database_instance_id"]
    assert str(uuid.UUID(dry_run_instance_id)) == dry_run_instance_id
    assert not list(tmp_path.glob(".v5-dry-run.*.candidate"))


def test_migration_receipt_distinguishes_same_path_database_replacement(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    outcome = _migrate(fixture, tmp_path)
    receipt_instance_id = outcome.receipt.to_document()["destination"][
        "database_instance_id"
    ]
    imported = V5QueueStore(outcome.destination_state)
    assert imported.instance_identity() == receipt_instance_id

    replacement_state = (tmp_path / "replacement-v5").resolve()
    replacement = V5QueueStore(replacement_state)
    replacement.initialize()
    replacement_instance_id = replacement.instance_identity()
    assert replacement_instance_id != receipt_instance_id
    os.replace(replacement.database_path, imported.database_path)

    current = V5QueueStore(outcome.destination_state)
    assert current.instance_identity() == replacement_instance_id
    assert current.instance_identity() != receipt_instance_id


@pytest.mark.parametrize(
    "active_state",
    ["starting", "running", "yielding", "terminating", "force_killing"],
)
def test_active_legacy_states_fail_with_external_receipt(
    tmp_path: Path,
    active_state: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, state=active_state)
    receipt = (tmp_path / f"failed-{active_state}.json").resolve()
    with pytest.raises(V5MigrationError, match="idle legacy queue") as raised:
        _migrate(fixture, tmp_path, receipt=receipt)
    assert raised.value.receipt is not None
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()["result"] == "failed"
    assert not (tmp_path / "v5-state").exists()
    assert fixture.database.read_bytes() == fixture.source_database_bytes


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_any_sqlite_sidecar_fails_closed_with_receipt(
    tmp_path: Path,
    suffix: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    Path(str(fixture.database) + suffix).write_bytes(b"unresolved")
    receipt = (tmp_path / f"sidecar{suffix}.json").resolve()
    with pytest.raises(V5MigrationError, match="sidecars"):
        _migrate(fixture, tmp_path, receipt=receipt)
    document = QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()
    assert document["result"] == "failed"
    assert document["source"]["sidecars"]


def test_corrupt_card_or_continuation_evidence_is_rejected(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            "UPDATE queue_items SET continuation_checkpoint_sha256 = ?",
            ("f" * 64,),
        )
    receipt = (tmp_path / "corrupt-receipt.json").resolve()
    with pytest.raises(V5MigrationError, match="digest mismatch"):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("missing", "does not exist"),
        ("symlink", "must not be a symlink"),
        ("wrong-type", "must be a file"),
    ],
)
def test_required_referenced_runner_evidence_must_be_present_and_exact_type(
    tmp_path: Path,
    replacement: str,
    message: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    run_dir = fixture.external_root / "run-10"
    manifest = run_dir / "runner-manifest.json"
    if replacement == "missing":
        recorded = fixture.external_root / "missing-runner-manifest.json"
    elif replacement == "symlink":
        recorded = fixture.external_root / "runner-manifest-link.json"
        recorded.symlink_to(manifest)
    else:
        recorded = run_dir
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            "UPDATE queue_items SET runner_manifest_path = ? WHERE id = 10",
            (str(recorded),),
        )
    receipt = (tmp_path / f"runner-evidence-{replacement}.json").resolve()
    with pytest.raises(V5MigrationError, match=message):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()[
        "result"
    ] == "failed"


def test_partial_runner_path_evidence_is_rejected(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            "UPDATE queue_items SET runner_manifest_path = NULL WHERE id = 10"
        )
    receipt = (tmp_path / "partial-runner-evidence.json").resolve()
    with pytest.raises(V5MigrationError, match="partial runner"):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()


@pytest.mark.parametrize(
    "idle_state",
    ["queued", "held", "blocked", "interrupted", "failed", "removed"],
)
def test_idle_and_historical_states_are_importable(
    tmp_path: Path,
    idle_state: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, state=idle_state)
    outcome = _migrate(fixture, tmp_path)
    with V5QueueStore(outcome.destination_state).connect() as connection:
        assert connection.execute(
            "SELECT state FROM queue_items WHERE id = 10"
        ).fetchone()[0] == idle_state


def test_live_legacy_ref_must_exist_and_resolve_to_recorded_commit(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=2)
    reference = "refs/experiment-queue/items/10"
    _run_git(fixture.checkout, "update-ref", "-d", reference)
    with pytest.raises(V5MigrationError, match="live recorded worktree.*ref.*missing"):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "missing-ref.json").resolve(),
        )

    moved_root = tmp_path / "moved"
    moved_root.mkdir()
    fixture = _legacy_fixture(moved_root, version=4, item_count=2)
    _run_git(
        fixture.checkout,
        "update-ref",
        reference,
        fixture.commits[1],
    )
    with pytest.raises(V5MigrationError, match="points to .*not recorded commit"):
        _migrate(
            fixture,
            moved_root,
            receipt=(tmp_path / "moved-ref.json").resolve(),
        )


def test_legacy_worktree_head_must_equal_recorded_commit(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=2)
    worktree = fixture.external_root / "worktree-10"
    _run_git(worktree, "checkout", "--detach", fixture.commits[1])
    with pytest.raises(V5MigrationError, match="worktree .* HEAD .* differs"):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "wrong-head.json").resolve(),
        )


def test_legacy_worktree_must_belong_to_supplied_repository(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    foreign = (tmp_path / "foreign-repository").resolve()
    subprocess.run(
        ["git", "clone", "-q", str(fixture.checkout), str(foreign)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            "UPDATE queue_items SET worktree_path = ? WHERE id = 10",
            (str(foreign),),
        )
    with pytest.raises(V5MigrationError, match="not registered by the supplied"):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "foreign-worktree.json").resolve(),
        )


def test_plain_directory_cannot_substitute_for_legacy_worktree(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    substituted = (tmp_path / "plain-directory").resolve()
    substituted.mkdir()
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            "UPDATE queue_items SET worktree_path = ? WHERE id = 10",
            (str(substituted),),
        )
    with pytest.raises(V5MigrationError, match="not registered by the supplied"):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "plain-worktree.json").resolve(),
        )


@pytest.mark.parametrize(
    ("assignment", "message"),
    [
        ("git_ref = NULL", "lifecycle evidence without its legacy Git ref"),
        (
            "worktree_created_at = NULL",
            "worktree_path and worktree_created_at must be recorded together",
        ),
    ],
)
def test_inconsistent_legacy_ref_worktree_shapes_are_rejected(
    tmp_path: Path,
    assignment: str,
    message: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(f"UPDATE queue_items SET {assignment} WHERE id = 10")
    with pytest.raises(V5MigrationError, match=message):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "inconsistent-runtime.json").resolve(),
        )


def test_pending_item_with_missing_recorded_worktree_is_rejected(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, state="queued")
    worktree = fixture.external_root / "worktree-10"
    _run_git(fixture.checkout, "worktree", "remove", "--force", str(worktree))
    with pytest.raises(V5MigrationError, match="pending queue item 10 recorded worktree.*missing"):
        _migrate(
            fixture,
            tmp_path,
            receipt=(tmp_path / "missing-worktree.json").resolve(),
        )


def test_completed_legacy_cleanup_is_verified_as_historical_absence(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    reference = "refs/experiment-queue/items/10"
    worktree = fixture.external_root / "worktree-10"
    _run_git(fixture.checkout, "worktree", "remove", "--force", str(worktree))
    _run_git(fixture.checkout, "update-ref", "-d", reference)
    with sqlite3.connect(fixture.database) as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET worktree_removed_at = ?, worktree_cleanup_error = NULL
            WHERE id = 10
            """,
            (NOW,),
        )

    outcome = _migrate(fixture, tmp_path)
    inventory = cast(list[dict[str, object]], outcome.receipt.to_document()["path_inventory"])
    ref = next(entry for entry in inventory if entry["kind"] == "git_ref")
    worktree_entry = next(
        entry for entry in inventory if entry["kind"] == "worktree_path"
    )
    assert ref["exists"] is False
    assert ref["disposition"] == "removed"
    assert ref["verified"] is True
    assert worktree_entry["exists"] is False
    assert worktree_entry["registered"] is False
    assert worktree_entry["disposition"] == "removed"


def test_read_only_source_database_is_never_opened_for_writing(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    fixture.database.chmod(0o444)
    try:
        outcome = _migrate(fixture, tmp_path, dry_run=True)
        assert outcome.receipt.to_document()["result"] == "succeeded"
        assert fixture.database.read_bytes() == fixture.source_database_bytes
    finally:
        fixture.database.chmod(0o600)


def test_legacy_git_verification_ignores_ambient_repository_routing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "wrong.git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path / "wrong-worktree"))
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", str(tmp_path / "wrong-objects"))
    outcome = _migrate(fixture, tmp_path, dry_run=True)
    assert outcome.receipt.to_document()["result"] == "succeeded"


def test_failure_receipt_is_never_written_inside_source_state(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, state="running")
    unsafe_receipt = fixture.state / "failure.json"
    with pytest.raises(V5MigrationError, match="no unsafe receipt was written"):
        _migrate(fixture, tmp_path, receipt=unsafe_receipt)
    assert not unsafe_receipt.exists()
    assert fixture.database.read_bytes() == fixture.source_database_bytes


def test_sqlite_64_bit_values_are_preserved_without_unsafe_json_numbers(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    large = 2**60
    with sqlite3.connect(fixture.database) as connection:
        connection.execute("UPDATE queue_items SET priority = ? WHERE id = 10", (large,))
        connection.execute(
            "UPDATE sqlite_sequence SET seq = ? WHERE name = 'queue_items'",
            (large,),
        )
    outcome = _migrate(fixture, tmp_path)
    with V5QueueStore(outcome.destination_state).connect() as connection:
        assert connection.execute(
            "SELECT priority FROM queue_items WHERE id = 10"
        ).fetchone()[0] == large
        assert connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'queue_items'"
        ).fetchone()[0] == large
    comparison = outcome.receipt.to_document()["comparison"]
    assert comparison["source_sequences"]["queue_items"] == str(large)  # type: ignore[index]


def test_schema_with_matching_names_but_spoofed_declaration_is_rejected(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    with sqlite3.connect(fixture.database) as connection:
        connection.execute("PRAGMA writable_schema = ON")
        connection.execute(
            """
            UPDATE sqlite_master
            SET sql = replace(sql, 'card_sha256 TEXT NOT NULL',
                                   'card_sha256 BLOB NOT NULL')
            WHERE type = 'table' AND name = 'queue_items'
            """
        )
        connection.commit()
        connection.execute("PRAGMA writable_schema = OFF")
        connection.execute("PRAGMA schema_version = 999")
    receipt = (tmp_path / "spoofed-schema.json").resolve()
    with pytest.raises(V5MigrationError, match="authentic historical DDL"):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()["result"] == "failed"


@pytest.mark.parametrize("overlap", ["source", "checkout", "root"])
def test_destination_overlap_is_rejected_without_mutating_source(
    tmp_path: Path,
    overlap: str,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    roots = {
        "source": fixture.state / "new-v5",
        "checkout": fixture.checkout / "new-v5",
        "root": fixture.external_root / "new-v5",
    }
    receipt = (tmp_path / f"overlap-{overlap}.json").resolve()
    with pytest.raises(V5MigrationError, match="overlaps"):
        _migrate(fixture, tmp_path, destination=roots[overlap], receipt=receipt)
    assert fixture.database.read_bytes() == fixture.source_database_bytes


def test_destination_rejects_writable_nonsticky_ancestor(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    insecure = tmp_path / "insecure-destination-root"
    insecure.mkdir(mode=0o700)
    insecure.chmod(0o777)
    trusted = insecure / "trusted"
    trusted.mkdir(mode=0o700)
    destination = (trusted / "v5-state").resolve()
    receipt = (tmp_path / "insecure-destination.json").resolve()
    try:
        with pytest.raises(
            V5MigrationError,
            match="ancestor .*insecure-destination-root.*writable without the sticky bit",
        ):
            _migrate(
                fixture,
                tmp_path,
                destination=destination,
                receipt=receipt,
            )
        assert not destination.exists()
    finally:
        insecure.chmod(0o700)


def test_receipt_rejects_writable_nonsticky_ancestor(tmp_path: Path) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    insecure = tmp_path / "insecure-receipt-root"
    insecure.mkdir(mode=0o700)
    insecure.chmod(0o777)
    trusted = insecure / "trusted"
    trusted.mkdir(mode=0o700)
    receipt = (trusted / "receipt.json").resolve()
    try:
        with pytest.raises(
            V5MigrationError,
            match="ancestor .*insecure-receipt-root.*writable without the sticky bit",
        ):
            _migrate(fixture, tmp_path, receipt=receipt)
        assert not receipt.exists()
        assert not (tmp_path / "v5-state").exists()
    finally:
        insecure.chmod(0o700)


def test_failed_atomic_publish_cleans_candidate_and_retry_is_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=2)
    from experiment_queue import migrate_v5 as module

    original_publish = module._publish_state

    def fail_publish(_candidate: Path, _destination: Path) -> None:
        raise V5MigrationError("injected atomic publish failure")

    monkeypatch.setattr(module, "_publish_state", fail_publish)
    failed_receipt = (tmp_path / "publish-failed.json").resolve()
    with pytest.raises(V5MigrationError, match="injected atomic publish failure"):
        _migrate(fixture, tmp_path, receipt=failed_receipt)
    assert not (tmp_path / "v5-state").exists()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    monkeypatch.setattr(module, "_publish_state", original_publish)
    outcome = _migrate(
        fixture,
        tmp_path,
        receipt=(tmp_path / "retry-succeeded.json").resolve(),
    )
    assert outcome.published
    with V5QueueStore(outcome.destination_state).connect() as connection:
        assert [
            tuple(row)
            for row in connection.execute(
                "SELECT id, sequence, git_commit FROM project_revisions ORDER BY id"
            )
        ] == [(1, 1, fixture.commits[0]), (2, 2, fixture.commits[1])]


def test_atomic_publish_never_replaces_destination_created_in_race_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from experiment_queue import migrate_v5 as module

    candidate = tmp_path / ".verified.candidate"
    candidate.mkdir()
    (candidate / "evidence").write_text("verified\n")
    destination = tmp_path / "v5-state"
    original_rename = module._atomic_rename_noreplace

    def destination_appears(source: Path, target: Path) -> None:
        target.mkdir()
        original_rename(source, target)

    monkeypatch.setattr(module, "_atomic_rename_noreplace", destination_appears)
    with pytest.raises(V5MigrationError, match="appeared during atomic publish"):
        module._publish_state(candidate, destination)

    assert candidate.is_dir()
    assert (candidate / "evidence").read_text() == "verified\n"
    assert destination.is_dir()
    assert list(destination.iterdir()) == []


def test_rename_error_after_move_is_detected_and_durably_rolled_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from experiment_queue import migrate_v5 as module

    candidate = tmp_path / ".verified.candidate"
    candidate.mkdir()
    (candidate / "evidence").write_text("verified\n")
    destination = tmp_path / "v5-state"
    original_rename = module._atomic_rename_noreplace
    rename_calls = 0

    def move_then_report_error(source: Path, target: Path) -> None:
        nonlocal rename_calls
        rename_calls += 1
        original_rename(source, target)
        if rename_calls == 1:
            raise OSError("injected uncertain rename return")

    monkeypatch.setattr(module, "_atomic_rename_noreplace", move_then_report_error)
    with pytest.raises(
        V5MigrationError,
        match="error after moving state.*durably rolled back",
    ):
        module._publish_state(candidate, destination)

    assert candidate.is_dir()
    assert (candidate / "evidence").read_text() == "verified\n"
    assert not destination.exists()


def test_state_publish_fsync_and_rollback_failure_is_indeterminate_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination = (tmp_path / "v5-state").resolve()
    receipt = (tmp_path / "indeterminate-state.json").resolve()
    from experiment_queue import migrate_v5 as module

    original_rename = module._atomic_rename_noreplace
    original_fsync = module.os.fsync
    rename_calls = 0

    def publish_then_refuse_rollback(source: Path, target: Path) -> None:
        nonlocal rename_calls
        rename_calls += 1
        if rename_calls == 1:
            original_rename(source, target)
            return
        raise OSError("injected exclusive rollback failure")

    def fail_after_publication(descriptor: int) -> None:
        if rename_calls:
            raise OSError("injected state parent fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(
        module, "_atomic_rename_noreplace", publish_then_refuse_rollback
    )
    monkeypatch.setattr(module.os, "fsync", fail_after_publication)
    with pytest.raises(
        V5MigrationError,
        match="state publication durability is indeterminate.*rollback.*failed",
    ) as raised:
        _migrate(fixture, tmp_path, destination=destination, receipt=receipt)

    assert raised.value.receipt is None
    assert destination.is_dir()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    assert not receipt.exists()
    assert fixture.database.read_bytes() == fixture.source_database_bytes


def test_state_publish_rollback_fsync_failure_preserves_candidate_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination = (tmp_path / "v5-state").resolve()
    receipt = (tmp_path / "indeterminate-rollback.json").resolve()
    from experiment_queue import migrate_v5 as module

    original_rename = module._atomic_rename_noreplace
    original_fsync = module.os.fsync
    rename_calls = 0

    def record_state_rename(source: Path, target: Path) -> None:
        nonlocal rename_calls
        rename_calls += 1
        original_rename(source, target)

    def fail_publication_and_rollback_fsync(descriptor: int) -> None:
        if rename_calls:
            raise OSError("injected state directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(module, "_atomic_rename_noreplace", record_state_rename)
    monkeypatch.setattr(module.os, "fsync", fail_publication_and_rollback_fsync)
    with pytest.raises(
        V5MigrationError,
        match="state publication durability is indeterminate.*rollback is visible",
    ) as raised:
        _migrate(fixture, tmp_path, destination=destination, receipt=receipt)

    candidates = list(tmp_path.glob(".v5-state.*.candidate"))
    assert raised.value.receipt is None
    assert not destination.exists()
    assert len(candidates) == 1
    assert (candidates[0] / "queue.sqlite3").is_file()
    assert not receipt.exists()
    assert fixture.database.read_bytes() == fixture.source_database_bytes


def test_published_destination_verification_failure_rolls_back_durably(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination = (tmp_path / "v5-state").resolve()
    receipt = (tmp_path / "published-verification-failed.json").resolve()
    from experiment_queue import migrate_v5 as module

    def fail_final_verification(*_args: object, **_kwargs: object) -> None:
        raise V5MigrationError("injected published destination verification failure")

    monkeypatch.setattr(
        module, "_verify_published_destination", fail_final_verification
    )
    with pytest.raises(
        V5MigrationError,
        match="injected published destination verification failure",
    ):
        _migrate(fixture, tmp_path, destination=destination, receipt=receipt)

    assert not destination.exists()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    failed = QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()
    assert failed["result"] == "failed"
    assert failed["destination"]["published"] is False  # type: ignore[index]
    assert fixture.database.read_bytes() == fixture.source_database_bytes


def test_migration_receipt_identity_rejects_database_replaced_at_same_path(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    outcome = _migrate(fixture, tmp_path)
    receipt_document = outcome.receipt.to_document()
    expected_instance_id = receipt_document["destination"][  # type: ignore[index]
        "database_instance_id"
    ]
    replacement = V5QueueStore((tmp_path / "replacement-state").resolve())
    replacement.initialize()
    assert replacement.instance_identity() != expected_instance_id

    os.replace(
        replacement.database_path,
        outcome.destination_state / "queue.sqlite3",
    )
    from experiment_queue import migrate_v5 as module

    with pytest.raises(
        V5MigrationError,
        match="published destination database instance differs from the verified candidate",
    ):
        module._verify_published_destination(
            outcome.destination_state,
            expected_instance_id=expected_instance_id,
            expected_receipt=outcome.receipt,
        )


def test_destination_parent_substitution_after_receipt_preserves_success_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination_parent = (tmp_path / "destination-parent").resolve()
    destination_parent.mkdir(mode=0o700)
    destination = destination_parent / "v5-state"
    moved_parent = (tmp_path / "moved-destination-parent").resolve()
    receipt = (tmp_path / "substituted-parent-receipt.json").resolve()
    from experiment_queue import migrate_v5 as module

    original_write = module._atomic_write_receipt

    def publish_receipt_then_substitute(
        path: Path,
        migration_receipt: QueueMigrationReceipt,
    ) -> None:
        original_write(path, migration_receipt)
        if migration_receipt.to_document()["result"] == "succeeded":
            destination_parent.rename(moved_parent)
            destination_parent.mkdir(mode=0o700)

    monkeypatch.setattr(
        module,
        "_atomic_write_receipt",
        publish_receipt_then_substitute,
    )
    with pytest.raises(
        V5MigrationError,
        match="destination ancestor identity changed during receipt publication",
    ) as raised:
        _migrate(
            fixture,
            tmp_path,
            destination=destination,
            receipt=receipt,
        )

    assert receipt.is_file()
    succeeded = QueueMigrationReceipt.from_bytes(receipt.read_bytes())
    assert succeeded.to_document()["result"] == "succeeded"
    assert raised.value.receipt == succeeded
    assert not destination.exists()
    assert (moved_parent / "v5-state" / "queue.sqlite3").is_file()


def test_post_link_receipt_fsync_failure_preserves_published_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    destination = (tmp_path / "v5-state").resolve()
    receipt = (tmp_path / "post-link-receipt.json").resolve()
    from experiment_queue import migrate_v5 as module

    original_fsync = module.os.fsync

    def fail_after_final_link(descriptor: int) -> None:
        if receipt.exists():
            raise OSError("injected receipt directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", fail_after_final_link)
    with pytest.raises(
        V5MigrationError,
        match="durable staging hard link is preserved",
    ) as raised:
        _migrate(
            fixture,
            tmp_path,
            destination=destination,
            receipt=receipt,
        )

    assert destination.is_dir()
    assert receipt.is_file()
    staging_links = list(tmp_path.glob(".post-link-receipt.json.*.tmp"))
    assert len(staging_links) == 1
    assert os.path.samefile(staging_links[0], receipt)
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    published = QueueMigrationReceipt.from_bytes(receipt.read_bytes())
    assert published.to_document()["result"] == "succeeded"
    assert published.to_document()["destination"]["published"] is True
    assert raised.value.receipt == published
    with V5QueueStore(destination).connect() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_preexisting_receipt_staging_name_is_never_deleted(
    tmp_path: Path,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    receipt = (tmp_path / "owned-staging-receipt.json").resolve()
    staging = tmp_path / ".owned-staging-receipt.json.fixed-operation.tmp"
    prior_evidence = b"preserved prior staging evidence\n"
    staging.write_bytes(prior_evidence)

    with pytest.raises(
        V5MigrationError,
        match="receipt path or staging file already exists",
    ):
        migrate_legacy_state(
            source_state_copy=fixture.state,
            destination_state=(tmp_path / "v5-state").resolve(),
            project_key="flowers-legacy",
            legacy_checkout=fixture.checkout,
            actor="migration-test",
            receipt_path=receipt,
            protected_roots=(fixture.external_root,),
            operation_id="fixed-operation",
            confirm_source_is_copy=True,
        )

    assert staging.read_bytes() == prior_evidence
    assert not receipt.exists()
    assert not (tmp_path / "v5-state").exists()


def test_source_tree_change_during_mapping_prevents_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    from experiment_queue import migrate_v5 as module

    original_build = module._build_candidate

    def build_then_race(**kwargs: object):
        result = original_build(**kwargs)  # type: ignore[arg-type]
        (fixture.state / "concurrent-writer-evidence").write_text("changed\n")
        return result

    monkeypatch.setattr(module, "_build_candidate", build_then_race)
    receipt = (tmp_path / "source-race.json").resolve()
    with pytest.raises(V5MigrationError, match="changed during migration"):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()["result"] == "failed"


def test_external_continuation_change_after_receipt_build_prevents_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4)
    checkpoint = fixture.external_root / "checkpoint-10.bin"
    from experiment_queue import migrate_v5 as module

    original_insert = module._insert_published_receipt

    def insert_then_race(*args: object, **kwargs: object) -> None:
        original_insert(*args, **kwargs)  # type: ignore[arg-type]
        checkpoint.write_bytes(b"concurrently changed continuation")

    monkeypatch.setattr(module, "_insert_published_receipt", insert_then_race)
    receipt = (tmp_path / "continuation-race.json").resolve()
    with pytest.raises(
        V5MigrationError,
        match="external migration evidence changed.*continuation_checkpoint digest mismatch",
    ):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()[
        "result"
    ] == "failed"


def test_legacy_ref_change_after_receipt_build_prevents_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _legacy_fixture(tmp_path, version=4, item_count=2)
    from experiment_queue import migrate_v5 as module

    original_insert = module._insert_published_receipt

    def insert_then_move_ref(*args: object, **kwargs: object) -> None:
        original_insert(*args, **kwargs)  # type: ignore[arg-type]
        _run_git(
            fixture.checkout,
            "update-ref",
            "refs/experiment-queue/items/10",
            fixture.commits[1],
        )

    monkeypatch.setattr(module, "_insert_published_receipt", insert_then_move_ref)
    receipt = (tmp_path / "git-ref-race.json").resolve()
    with pytest.raises(
        V5MigrationError,
        match="external migration evidence changed.*legacy ref.*points to",
    ):
        _migrate(fixture, tmp_path, receipt=receipt)
    assert not (tmp_path / "v5-state").exists()
    assert not list(tmp_path.glob(".v5-state.*.candidate"))
    assert QueueMigrationReceipt.from_bytes(receipt.read_bytes()).to_document()[
        "result"
    ] == "failed"


def test_cli_requires_copy_attestation_and_supports_full_dry_run(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _legacy_fixture(tmp_path, version=1)
    common = [
        "--source-state", str(fixture.state),
        "--destination-state", str((tmp_path / "cli-v5").resolve()),
        "--project-key", "flowers-legacy",
        "--legacy-checkout", str(fixture.checkout),
        "--actor", "migration-test",
        "--receipt", str((tmp_path / "cli-receipt.json").resolve()),
        "--legacy-root", str(fixture.external_root),
        "--dry-run",
    ]
    assert main(common) == 2
    assert "confirm_source_is_copy" in capsys.readouterr().err
    assert main(common + ["--confirm-source-is-copy"]) == 0
    assert "validated without publication" in capsys.readouterr().out
    assert not (tmp_path / "cli-v5").exists()
