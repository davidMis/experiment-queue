"""Exercise fresh schema-v5 creation, ownership, and no-migration boundaries."""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import stat
import uuid

import pytest

import experiment_queue.database_v5 as database_v5_module
from experiment_queue.database_v5 import (
    DATABASE_INSTANCE_ID_KEY,
    SCHEMA_DDL_SHA256,
    SCHEMA_IDENTITY,
    V4_QUEUE_ITEM_COLUMNS,
    V5DatabaseError,
    V5QueueStore,
    V5SchemaVersionError,
)
from experiment_queue.path_security import (
    PathBoundaryError,
    capture_secure_path_boundary,
    revalidate_secure_path_boundary,
)


SHA = "0" * 64
COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
NOW = "2026-08-28T12:00:00+00:00"


@pytest.fixture
def store(tmp_path: Path) -> V5QueueStore:
    value = V5QueueStore(tmp_path.resolve())
    value.initialize()
    return value


def _insert_project_revision(
    connection: sqlite3.Connection,
    *,
    project_id: int,
    revision_id: int,
    key: str,
    commit: str = COMMIT_A,
    typed: bool = False,
    project_blob_size: int | None = None,
) -> None:
    """Insert one cyclic Project/revision pair in the deferred transaction."""

    connection.execute(
        """
        INSERT INTO projects(
            id, project_key, display_name, lifecycle, current_revision_id,
            current_revision_sequence,
            created_at, created_by, lifecycle_changed_at, lifecycle_actor,
            lifecycle_reason
        ) VALUES (?, ?, ?, 'active', ?, 1, ?, 'tester', ?, 'tester', 'registered')
        """,
        (project_id, key, key, revision_id, NOW, NOW),
    )
    values: dict[str, object] = {
        "id": revision_id,
        "project_id": project_id,
        "sequence": 1,
        "revision_label": f"{key}:r1",
        "revision_kind": "project-v1" if typed else "legacy-v4",
        "display_name": key,
        "git_commit": commit,
        "checkout_path": f"/tmp/{key}",
        "project_manifest_path": "project.yaml" if typed else None,
        "enrollment_json": b"{}",
        "enrollment_sha256": SHA,
        "created_at": NOW,
        "created_actor": "tester",
    }
    if typed:
        values.update(
            {
                "project_source_path": "project.yaml",
                "project_source": b"apiVersion: experiment-queue.openai/v1\n",
                "project_source_sha256": SHA,
                "project_blob_object_id": "c" * 40,
                "project_blob_mode": "100644",
                "project_blob_size": (
                    len(b"apiVersion: experiment-queue.openai/v1\n")
                    if project_blob_size is None
                    else project_blob_size
                ),
                "project_normalized_json": b"{}",
                "project_normalized_sha256": SHA,
                "project_schema_api_version": "experiment-queue.openai/v1",
                "project_schema_kind": "Project",
                "project_schema_id": "https://example.invalid/project-v1",
                "project_schema_sha256": SHA,
                "validated_package_version": "0.1.0",
            }
        )
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    connection.execute(
        f"INSERT INTO project_revisions({columns}) VALUES ({placeholders})",
        tuple(values.values()),
    )
    connection.execute(
        """
        INSERT INTO project_runtime_state(
            project_id, health_reason, health_actor, health_changed_at
        ) VALUES (?, 'healthy', 'tester', ?)
        """,
        (project_id, NOW),
    )


def _insert_snapshot(
    connection: sqlite3.Connection,
    *,
    snapshot_id: int,
    project_id: int,
    revision_id: int,
    project_key: str,
    commit: str = COMMIT_A,
    revision_label: str | None = None,
    policy_project_key: str | None = None,
    card_blob_size: int | None = None,
) -> None:
    """Insert a complete typed snapshot without optional extension evidence."""

    values: dict[str, object] = {
        "id": snapshot_id,
        "project_id": project_id,
        "revision_id": revision_id,
        "project_revision_label": revision_label or f"{project_key}:r1",
        "git_commit": commit,
        "project_source_name": "project.yaml",
        "project_source": b"project",
        "project_source_sha256": SHA,
        "project_blob_object_id": "c" * 40,
        "project_blob_mode": "100644",
        "project_blob_size": len(b"project"),
        "project_normalized_json": b"{}",
        "project_normalized_sha256": SHA,
        "project_schema_api_version": "experiment-queue.openai/v1",
        "project_schema_kind": "Project",
        "project_schema_id": "https://example.invalid/project-v1",
        "project_schema_sha256": SHA,
        "card_source_name": "cards/example.yaml",
        "card_source": b"card",
        "card_source_sha256": SHA,
        "card_blob_object_id": "d" * 40,
        "card_blob_mode": "100644",
        "card_blob_size": len(b"card") if card_blob_size is None else card_blob_size,
        "card_normalized_json": b"{}",
        "card_normalized_sha256": SHA,
        "card_schema_api_version": "experiment-queue.openai/v1",
        "card_schema_kind": "ExperimentCard",
        "card_schema_id": "https://example.invalid/card-v1",
        "card_schema_sha256": SHA,
        "resolved_json": b"{}",
        "resolved_sha256": SHA,
        "command_kind": "argv",
        "command_json": b'{"type":"argv","argv":["true"]}',
        "command_sha256": SHA,
        "package_version": "0.1.0",
        "policy_project_key": policy_project_key or project_key,
        "policy_card_path": "cards/example.yaml",
        "policy_job_id": "main",
        "policy_priority": 0,
        "policy_operator": "tester",
        "policy_preemption_authorized": 0,
        "policy_bindings_json": b"{}",
        "policy_bindings_sha256": SHA,
        "policy_dependencies_json": b"[]",
        "policy_dependencies_sha256": SHA,
        "policy_json": b"{}",
        "policy_sha256": SHA,
        "created_at": NOW,
    }
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    connection.execute(
        f"INSERT INTO admission_snapshots({columns}) VALUES ({placeholders})",
        tuple(values.values()),
    )


def _insert_queue_item(
    connection: sqlite3.Connection,
    *,
    item_id: int,
    project_id: int,
    revision_id: int,
    experiment_id: str = "same-experiment",
    commit: str = COMMIT_A,
    snapshot_id: int | None = None,
) -> None:
    typed = snapshot_id is not None
    connection.execute(
        """
        INSERT INTO queue_items(
            id, project_id, revision_id, admission_kind, snapshot_id, job_id,
            experiment_id, attempt, state, card_path, card_sha256, command_text,
            runner_name, git_commit, added_at, added_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'queued', 'cards/example.yaml', ?,
                  'true', 'example', ?, ?, 'tester')
        """,
        (
            item_id,
            project_id,
            revision_id,
            "ExperimentCard/v1" if typed else "LegacyMarkdownCard/v0",
            snapshot_id,
            "main" if typed else None,
            experiment_id,
            SHA,
            commit,
            NOW,
        ),
    )


def _insert_yield_request(connection: sqlite3.Connection) -> None:
    """Insert one exact typed CooperativeYieldRequest/v1 evidence row."""

    connection.execute(
        """
        INSERT INTO cooperative_yield_requests(
            request_id, queue_item_id, project_id, revision_id, segment,
            protocol_api_version, protocol_kind, request_kind, requested_at,
            requested_by, note, request_json, request_sha256,
            resolved_spec_sha256, project_revision_label, git_commit, run_id,
            prior_receipt_sha256, continuation_identity_sha256
        ) VALUES (
            'yield-101-1', 101, 1, 11, 1, 'experiment-queue/v1',
            'CooperativeYieldRequest', 'manual_preemption', ?, 'tester',
            'operator requested checkpoint', X'7b7d', ?, ?,
            'project-one:r1', ?, 'run-101', ?, ?
        )
        """,
        (NOW, SHA, SHA, COMMIT_A, SHA, SHA),
    )


def _legacy_database(path: Path, version: str | None) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        if version is not None:
            connection.execute(
                "INSERT INTO metadata(key, value) VALUES ('schema_version', ?)",
                (version,),
            )


def test_construction_is_side_effect_free_and_connect_requires_initialize(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir.resolve())
    assert not state_dir.exists()
    with pytest.raises(V5DatabaseError, match=r"call initialize\(\)"):
        store.connect()
    assert not state_dir.exists()


def test_state_directory_must_be_absolute() -> None:
    with pytest.raises(V5DatabaseError, match="must be absolute"):
        V5QueueStore(Path("relative-state"))


def test_selected_state_directory_symlink_is_refused(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    selected = tmp_path / "selected"
    selected.symlink_to(target, target_is_directory=True)
    with pytest.raises(V5DatabaseError, match="must not be a symlink"):
        V5QueueStore(selected)


def test_existing_state_directory_rejects_unsafe_write_permissions(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    state.chmod(0o770)
    try:
        with pytest.raises(V5DatabaseError, match="group/world writable"):
            V5QueueStore(state.resolve()).initialize()
        assert list(state.iterdir()) == []
    finally:
        state.chmod(0o700)


def test_existing_state_directory_preserves_reasonable_shared_read_mode(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    state.chmod(0o750)
    V5QueueStore(state.resolve()).initialize()
    assert stat.S_IMODE(state.stat().st_mode) == 0o750


def test_state_directory_rejects_writable_nonsticky_ancestor(
    tmp_path: Path,
) -> None:
    insecure = tmp_path / "insecure"
    insecure.mkdir(mode=0o700)
    insecure.chmod(0o777)
    trusted_parent = insecure / "trusted"
    trusted_parent.mkdir(mode=0o700)
    state = trusted_parent / "state"
    try:
        with pytest.raises(
            V5DatabaseError,
            match="ancestor .*insecure.*writable without the sticky bit",
        ):
            V5QueueStore(state.resolve()).initialize()
        assert not state.exists()
    finally:
        insecure.chmod(0o700)


def test_state_directory_accepts_sticky_shared_ancestor(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o700)
    shared.chmod(0o1777)
    store = V5QueueStore((shared / "state").resolve())
    store.initialize()
    assert store.instance_identity()


def test_secure_path_boundary_detects_ancestor_inode_substitution(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "selected-parent"
    parent.mkdir(mode=0o700)
    selected = parent / "state"
    boundary = capture_secure_path_boundary(selected, label="test state")
    moved = tmp_path / "moved-parent"
    parent.rename(moved)
    parent.mkdir(mode=0o700)

    with pytest.raises(PathBoundaryError, match="changed identity"):
        revalidate_secure_path_boundary(boundary)


def test_existing_state_directory_must_be_owned_by_service_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    actual_uid = os.geteuid()
    monkeypatch.setattr(database_v5_module.os, "geteuid", lambda: actual_uid + 1)
    with pytest.raises(V5DatabaseError, match="must be owned"):
        V5QueueStore(state.resolve()).initialize()


def test_existing_v5_database_requires_exact_private_file_mode(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "state").resolve())
    store.initialize()
    store.database_path.chmod(0o640)
    before = store.database_path.read_bytes()
    try:
        with pytest.raises(V5DatabaseError, match="mode 0600"):
            V5QueueStore(store.state_dir).initialize()
        assert store.database_path.read_bytes() == before
    finally:
        store.database_path.chmod(0o600)


def test_initialize_creates_complete_strict_v5_and_connect_closes(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        assert metadata["schema_version"] == "5"
        assert metadata["schema_identity"] == SCHEMA_IDENTITY
        assert metadata["schema_ddl_sha256"] == SCHEMA_DDL_SHA256
        instance_id = metadata[DATABASE_INSTANCE_ID_KEY]
        parsed_instance_id = uuid.UUID(instance_id)
        assert str(parsed_instance_id) == instance_id
        assert parsed_instance_id.version == 4
        assert parsed_instance_id.variant == uuid.RFC_4122
        assert store.instance_identity() == instance_id
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert list(connection.execute("PRAGMA foreign_key_check")) == []
        tables = {
            str(row[1]): int(row[5])
            for row in connection.execute("PRAGMA table_list")
            if str(row[2]) == "table" and not str(row[1]).startswith("sqlite_")
        }
        assert tables
        assert set(tables.values()) == {1}
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connection.execute("SELECT 1")


def test_fresh_initialize_is_atomic_private_and_leaves_no_candidate_sidecars(
    tmp_path: Path,
) -> None:
    state = (tmp_path / "new" / "private-state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    assert stat.S_IMODE(state.stat().st_mode) == 0o700
    assert stat.S_IMODE(store.database_path.stat().st_mode) == 0o600
    assert [path.name for path in state.iterdir()] == ["queue.sqlite3"]


def test_fresh_initialize_failure_cleans_private_candidate_without_final_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    monkeypatch.setattr(
        database_v5_module,
        "_SCHEMA_STATEMENTS",
        database_v5_module._SCHEMA_STATEMENTS + ("INVALID SCHEMA STATEMENT",),
    )
    with pytest.raises(V5DatabaseError, match="could not create fresh"):
        store.initialize()
    assert state.is_dir()
    assert list(state.iterdir()) == []


def test_fresh_initialize_never_clobbers_a_concurrent_valid_creator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    winner = V5QueueStore((tmp_path / "winner").resolve())
    winner.initialize()
    winner_id = winner.instance_identity()
    selected = V5QueueStore((tmp_path / "selected").resolve())
    original_link = os.link

    def publish_winner_then_collide(source: Path, destination: Path) -> None:
        original_link(winner.database_path, destination)
        original_link(source, destination)

    monkeypatch.setattr(database_v5_module.os, "link", publish_winner_then_collide)
    selected.initialize()
    assert selected.instance_identity() == winner_id
    names = [path.name for path in selected.state_dir.iterdir()]
    assert "queue.sqlite3" in names
    assert not any(".candidate" in name for name in names)
    assert stat.S_IMODE(selected.database_path.stat().st_mode) == 0o600


def test_atomic_publication_error_leaves_no_final_or_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)

    def fail_link(_source: Path, _destination: Path) -> None:
        raise OSError("simulated no-link filesystem")

    monkeypatch.setattr(database_v5_module.os, "link", fail_link)
    with pytest.raises(V5DatabaseError, match="simulated no-link filesystem"):
        store.initialize()
    assert list(state.iterdir()) == []


def test_fresh_publication_fsyncs_state_directory_before_and_after_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = (tmp_path / "state").resolve()
    calls: list[Path] = []
    original_fsync_directory = database_v5_module._fsync_directory

    def record_fsync(path: Path) -> None:
        calls.append(path)
        original_fsync_directory(path)

    monkeypatch.setattr(database_v5_module, "_fsync_directory", record_fsync)
    V5QueueStore(state).initialize()
    assert calls.count(state) == 4
    assert state.parent in calls


def test_post_link_fsync_failure_preserves_durable_candidate_for_observer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = (tmp_path / "state").resolve()
    publisher = V5QueueStore(state)
    original_fsync_directory = database_v5_module._fsync_directory
    observer_id: list[str] = []
    observing = False

    def observe_then_fail(path: Path) -> None:
        nonlocal observing
        candidates = list(state.glob(".queue.sqlite3.*.candidate"))
        if (
            path == state
            and publisher.database_path.exists()
            and candidates
            and not observing
            and not observer_id
        ):
            observing = True
            try:
                observer = V5QueueStore(state)
                observer.initialize()
                observer_id.append(observer.instance_identity())
            finally:
                observing = False
            raise OSError("injected final-link directory fsync failure")
        original_fsync_directory(path)

    monkeypatch.setattr(
        database_v5_module, "_fsync_directory", observe_then_fail
    )
    with pytest.raises(
        V5DatabaseError,
        match="publication durability is indeterminate.*preserve both",
    ):
        publisher.initialize()

    candidates = list(state.glob(".queue.sqlite3.*.candidate"))
    assert len(observer_id) == 1
    assert len(candidates) == 1
    assert publisher.database_path.is_file()
    assert os.path.samefile(candidates[0], publisher.database_path)
    assert V5QueueStore(state).instance_identity() == observer_id[0]


def test_initialize_existing_v5_does_not_rewrite_database(store: V5QueueStore) -> None:
    before = (
        store.database_path.read_bytes(),
        store.database_path.stat().st_mtime_ns,
    )
    store.initialize()
    after = (
        store.database_path.read_bytes(),
        store.database_path.stat().st_mtime_ns,
    )
    assert after == before


def test_database_identity_metadata_is_immutable(store: V5QueueStore) -> None:
    with store.connect() as connection:
        for statement in (
            "UPDATE metadata SET value = 'changed' "
            "WHERE key = 'database_instance_id'",
            "DELETE FROM metadata WHERE key = 'database_instance_id'",
            "UPDATE metadata SET key = 'database_instance_id' "
            "WHERE key = 'pause_reason'",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="identity metadata is immutable"):
                connection.execute(statement)


def test_malformed_database_instance_identity_is_refused_on_open(
    tmp_path: Path,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    with sqlite3.connect(store.database_path) as connection:
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'metadata_database_identity_immutable_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER metadata_database_identity_immutable_update")
        connection.execute(
            "UPDATE metadata SET value = '00000000-0000-0000-0000-000000000000' "
            "WHERE key = ?",
            (DATABASE_INSTANCE_ID_KEY,),
        )
        connection.execute(trigger_sql)
    before = store.database_path.read_bytes()
    replacement_store = V5QueueStore(state)
    with pytest.raises(V5DatabaseError, match="canonical lowercase UUIDv4"):
        replacement_store.connect()
    assert store.database_path.read_bytes() == before


def test_missing_database_identity_protection_trigger_is_refused_on_open(
    tmp_path: Path,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    with sqlite3.connect(store.database_path) as connection:
        connection.execute(
            "DROP TRIGGER metadata_database_identity_immutable_delete"
        )
    before = store.database_path.read_bytes()
    with pytest.raises(
        V5DatabaseError,
        match="missing trigger 'metadata_database_identity_immutable_delete'",
    ):
        V5QueueStore(state).connect()
    assert store.database_path.read_bytes() == before


@pytest.mark.parametrize(
    ("tamper_sql", "expected_difference"),
    (
        (
            "DROP TRIGGER queue_items_no_delete",
            "missing trigger 'queue_items_no_delete'",
        ),
        (
            "DROP INDEX queue_items_state_order",
            "missing index 'queue_items_state_order'",
        ),
        (
            "CREATE TABLE unexpected_state(value TEXT) STRICT",
            "unexpected table 'unexpected_state'",
        ),
        (
            "ALTER TABLE metadata ADD COLUMN unexpected_value TEXT",
            "changed table 'metadata'",
        ),
    ),
)
def test_every_application_schema_object_is_authenticated_on_open(
    tmp_path: Path,
    tamper_sql: str,
    expected_difference: str,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    with sqlite3.connect(store.database_path) as connection:
        connection.execute(tamper_sql)

    before = store.database_path.read_bytes()
    with pytest.raises(V5DatabaseError, match=expected_difference):
        V5QueueStore(state).connect()
    assert store.database_path.read_bytes() == before


def test_unexpected_reserved_prefix_schema_object_is_refused_on_open(
    tmp_path: Path,
) -> None:
    state = (tmp_path / "state").resolve()
    store = V5QueueStore(state)
    store.initialize()
    with sqlite3.connect(store.database_path) as connection:
        connection.execute("PRAGMA writable_schema = ON")
        connection.execute(
            """
            INSERT INTO sqlite_schema(type, name, tbl_name, rootpage, sql)
            VALUES (
                'table', 'sqlite_unexpected', 'sqlite_unexpected', 0,
                'CREATE TABLE sqlite_unexpected(value TEXT)'
            )
            """
        )
        connection.execute("PRAGMA writable_schema = OFF")
        connection.execute("PRAGMA schema_version = 999")

    before = store.database_path.read_bytes()
    with pytest.raises(
        V5DatabaseError,
        match="unexpected table 'sqlite_unexpected'",
    ):
        V5QueueStore(state).connect()
    assert store.database_path.read_bytes() == before


def test_store_detects_a_different_database_instance_replaced_at_same_path(
    tmp_path: Path,
) -> None:
    selected_state = (tmp_path / "selected").resolve()
    replacement_state = (tmp_path / "replacement").resolve()
    selected = V5QueueStore(selected_state)
    replacement = V5QueueStore(replacement_state)
    selected.initialize()
    replacement.initialize()
    selected_id = selected.instance_identity()
    replacement_id = replacement.instance_identity()
    assert replacement_id != selected_id

    os.replace(replacement.database_path, selected.database_path)
    with pytest.raises(V5DatabaseError, match="database instance .* changed"):
        selected.connect()

    deliberately_reselected = V5QueueStore(selected_state)
    assert deliberately_reselected.instance_identity() == replacement_id


@pytest.mark.parametrize("version", ["1", "2", "3", "4", "6", "bogus", None])
def test_existing_non_v5_database_is_refused_without_mutation(
    tmp_path: Path,
    version: str | None,
) -> None:
    database = tmp_path / "queue.sqlite3"
    _legacy_database(database, version)
    before = (
        database.read_bytes(),
        database.stat().st_mtime_ns,
        sorted(path.name for path in tmp_path.iterdir()),
    )
    store = V5QueueStore(tmp_path.resolve())
    with pytest.raises(V5SchemaVersionError):
        store.initialize()
    with pytest.raises(V5SchemaVersionError):
        store.connect()
    after = (
        database.read_bytes(),
        database.stat().st_mtime_ns,
        sorted(path.name for path in tmp_path.iterdir()),
    )
    assert after == before


def test_spoofed_v5_version_without_v5_structure_is_refused_without_mutation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "queue.sqlite3"
    _legacy_database(database, "5")
    before = (
        database.read_bytes(),
        database.stat().st_mtime_ns,
        sorted(path.name for path in tmp_path.iterdir()),
    )
    store = V5QueueStore(tmp_path.resolve())
    with pytest.raises(V5DatabaseError, match="identity or DDL digest"):
        store.initialize()
    after = (
        database.read_bytes(),
        database.stat().st_mtime_ns,
        sorted(path.name for path in tmp_path.iterdir()),
    )
    assert after == before


def test_database_symlink_is_refused_without_touching_target(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = target_dir / "legacy.sqlite3"
    _legacy_database(target, "4")
    before = target.read_bytes()
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "queue.sqlite3").symlink_to(target)
    store = V5QueueStore(state_dir.resolve())
    with pytest.raises(V5SchemaVersionError, match="non-symlink"):
        store.initialize()
    assert target.read_bytes() == before


def test_queue_items_preserve_every_v4_column_and_add_v5_identity(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(queue_items)")
        }
    assert V4_QUEUE_ITEM_COLUMNS <= columns
    assert {
        "project_id", "revision_id", "admission_kind", "snapshot_id", "job_id",
        "runtime_gpu_lease_held", "runtime_gpu_lease_released_at",
        "runtime_git_ref", "runtime_worktree_path", "runtime_worktree_created_at",
        "runtime_worktree_removed_at", "runtime_worktree_cleanup_error",
    } <= columns


def test_typed_git_blob_provenance_is_required_exact_and_legacy_is_null(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        revision_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(project_revisions)")
        }
        snapshot_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(admission_snapshots)")
        }
        assert {
            "project_blob_object_id",
            "project_blob_mode",
            "project_blob_size",
            "extension_schema_blob_object_id",
            "extension_schema_blob_mode",
            "extension_schema_blob_size",
        } <= revision_columns
        assert {
            "project_blob_object_id",
            "project_blob_mode",
            "project_blob_size",
            "card_blob_object_id",
            "card_blob_mode",
            "card_blob_size",
            "extension_schema_blob_object_id",
            "extension_schema_blob_mode",
            "extension_schema_blob_size",
        } <= snapshot_columns

        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="legacy-project"
        )
        legacy = connection.execute(
            "SELECT project_blob_object_id, project_blob_mode, project_blob_size "
            "FROM project_revisions WHERE id = 11"
        ).fetchone()
        assert tuple(legacy) == (None, None, None)

        connection.execute("SAVEPOINT malformed_typed_revision")
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            _insert_project_revision(
                connection,
                project_id=2,
                revision_id=22,
                key="typed-project",
                typed=True,
                project_blob_size=999,
            )
        connection.execute("ROLLBACK TO malformed_typed_revision")
        connection.execute("RELEASE malformed_typed_revision")

        _insert_project_revision(
            connection,
            project_id=2,
            revision_id=22,
            key="typed-project",
            typed=True,
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            _insert_snapshot(
                connection,
                snapshot_id=31,
                project_id=2,
                revision_id=22,
                project_key="typed-project",
                card_blob_size=999,
            )


def test_v4_autoincrement_sequences_remain_available(store: V5QueueStore) -> None:
    with store.connect() as connection:
        definitions = {
            str(row[0]): str(row[1]).upper()
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            )
        }
    for table in ("queue_items", "events", "gpu_reservations"):
        assert "AUTOINCREMENT" in definitions[table]
    assert "sqlite_sequence" in definitions


def test_typed_cooperative_yield_evidence_is_distinct_strict_and_append_only(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        strict_tables = {
            str(row[1]): int(row[5])
            for row in connection.execute("PRAGMA table_list")
        }
        assert strict_tables["cooperative_yield_requests"] == 1
        assert strict_tables["cooperative_yield_receipts"] == 1

        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one", typed=True
        )
        _insert_snapshot(
            connection,
            snapshot_id=31,
            project_id=1,
            revision_id=11,
            project_key="project-one",
        )
        _insert_queue_item(
            connection,
            item_id=101,
            project_id=1,
            revision_id=11,
            snapshot_id=31,
        )
        _insert_yield_request(connection)
        connection.execute(
            """
            INSERT INTO cooperative_yield_receipts(
                request_id, queue_item_id, project_id, revision_id, segment,
                bound_continuation_identity_sha256, protocol_api_version,
                protocol_kind, status, written_at, receipt_json, receipt_sha256,
                progress_json, progress_sha256, checkpoint_artifacts_json,
                checkpoint_artifacts_sha256, resume_context,
                resume_context_bytes, resume_context_media_type,
                resume_context_sha256
            ) VALUES (
                'yield-101-1', 101, 1, 11, 1, ?, 'experiment-queue/v1',
                'CooperativeYieldReceipt', 'ready', ?, X'7b7d', ?, X'7b7d', ?,
                X'5b5d', ?, X'00ff', 2, 'application/octet-stream', ?
            )
            """,
            (SHA, NOW, SHA, SHA, SHA, SHA),
        )
        for statement in (
            "UPDATE cooperative_yield_requests SET note = 'changed'",
            "DELETE FROM cooperative_yield_requests",
            "UPDATE cooperative_yield_receipts SET status = 'failed'",
            "DELETE FROM cooperative_yield_receipts",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(statement)


def test_cooperative_yield_ownership_collision_and_status_checks(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one", typed=True
        )
        _insert_snapshot(
            connection,
            snapshot_id=31,
            project_id=1,
            revision_id=11,
            project_key="project-one",
        )
        _insert_queue_item(
            connection,
            item_id=101,
            project_id=1,
            revision_id=11,
            snapshot_id=31,
        )
        _insert_yield_request(connection)
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            connection.execute(
                """
                INSERT INTO cooperative_yield_requests(
                    request_id, queue_item_id, project_id, revision_id, segment,
                    protocol_api_version, protocol_kind, request_kind,
                    requested_at, requested_by, note, request_json,
                    request_sha256, resolved_spec_sha256,
                    project_revision_label, git_commit, run_id,
                    prior_receipt_sha256, continuation_identity_sha256
                ) SELECT
                    'different-request', queue_item_id, project_id, revision_id,
                    segment, protocol_api_version, protocol_kind, request_kind,
                    requested_at, requested_by, note, request_json,
                    request_sha256, resolved_spec_sha256,
                    project_revision_label, git_commit, run_id,
                    prior_receipt_sha256, continuation_identity_sha256
                FROM cooperative_yield_requests
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO cooperative_yield_receipts(
                    request_id, queue_item_id, project_id, revision_id, segment,
                    bound_continuation_identity_sha256, protocol_api_version,
                    protocol_kind, status, written_at, receipt_json,
                    receipt_sha256, checkpoint_artifacts_json,
                    checkpoint_artifacts_sha256, resume_context,
                    resume_context_bytes, resume_context_media_type,
                    resume_context_sha256, error
                ) VALUES (
                    'yield-101-1', 101, 1, 11, 1, ?, 'experiment-queue/v1',
                    'CooperativeYieldReceipt', 'failed', ?, X'7b7d', ?, X'5b5d',
                    ?, X'00', 1, 'application/octet-stream', ?, 'failed'
                )
                """,
                (SHA, NOW, SHA, SHA, SHA),
            )


def test_legacy_items_cannot_claim_typed_cooperative_yield_evidence(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_yield_request(connection)


def test_every_foreign_key_is_explicit_restrict(store: V5QueueStore) -> None:
    with store.connect() as connection:
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        ]
        foreign_keys = [
            (table, str(row[5]), str(row[6]))
            for table in tables
            for row in connection.execute(f"PRAGMA foreign_key_list({table})")
        ]
    assert foreign_keys
    assert {(on_update, on_delete) for _, on_update, on_delete in foreign_keys} == {
        ("RESTRICT", "RESTRICT")
    }


def test_two_projects_can_reuse_experiment_attempt_but_one_project_cannot(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_project_revision(
            connection, project_id=2, revision_id=22, key="project-two", commit=COMMIT_B
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE queue_items SET git_ref = 'refs/changed' WHERE id = 101"
            )
        connection.execute(
            """
            UPDATE queue_items
            SET runtime_git_ref = 'refs/experiment-queue/projects/project-one/items/101',
                runtime_worktree_path = '/tmp/v5-worktree',
                runtime_worktree_created_at = ?
            WHERE id = 101
            """,
            (NOW,),
        )
        _insert_queue_item(
            connection, item_id=102, project_id=2, revision_id=22, commit=COMMIT_B
        )
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_queue_item(
                connection, item_id=103, project_id=1, revision_id=11
            )


def test_queue_item_revision_ownership_and_commit_are_relationally_enforced(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_project_revision(
            connection, project_id=2, revision_id=22, key="project-two", commit=COMMIT_B
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_queue_item(
                connection, item_id=101, project_id=1, revision_id=22, commit=COMMIT_B
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_queue_item(
                connection, item_id=102, project_id=1, revision_id=11, commit=COMMIT_B
            )


def test_legacy_revision_cannot_fabricate_a_project_manifest(store: V5QueueStore) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="imported-project"
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO project_revisions(
                    id, project_id, sequence, revision_label, revision_kind,
                    display_name, git_commit, checkout_path, project_manifest_path,
                    enrollment_json, enrollment_sha256, created_at, created_actor
                ) VALUES (
                    12, 1, 2, 'imported-project:r2', 'legacy-v4',
                    'imported-project', ?, '/tmp/imported-project', 'project.yaml',
                    X'7b7d', ?, ?, 'tester'
                )
                """,
                (COMMIT_B, SHA, NOW),
            )


def test_typed_and_legacy_admission_shapes_cannot_be_confused(store: V5QueueStore) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="typed-project", typed=True
        )
        _insert_snapshot(
            connection,
            snapshot_id=50,
            project_id=1,
            revision_id=11,
            project_key="typed-project",
        )
        _insert_queue_item(
            connection,
            item_id=101,
            project_id=1,
            revision_id=11,
            snapshot_id=50,
        )
        with pytest.raises(sqlite3.IntegrityError, match="legacy admission requires"):
            connection.execute(
                """
                INSERT INTO queue_items(
                    project_id, revision_id, admission_kind, snapshot_id, job_id,
                    experiment_id, attempt, state, card_path, card_sha256,
                    command_text, runner_name, git_commit, added_at, added_by
                ) VALUES (1, 11, 'LegacyMarkdownCard/v0', 50, NULL, 'bad', 1, 'queued',
                          'card', ?, 'true', 'bad', ?, ?, 'tester')
                """,
                (SHA, COMMIT_A, NOW),
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO queue_items(
                    project_id, revision_id, admission_kind, snapshot_id, job_id,
                    experiment_id, attempt, state, card_path, card_sha256,
                    command_text, runner_name, git_commit, added_at, added_by
                ) VALUES (1, 11, 'ExperimentCard/v1', NULL, 'main', 'bad-two', 1,
                          'queued', 'card', ?, 'true', 'bad', ?, ?, 'tester')
                """,
                (SHA, COMMIT_A, NOW),
            )
        with pytest.raises(sqlite3.IntegrityError, match="legacy admission requires"):
            connection.execute(
                """
                INSERT INTO queue_items(
                    project_id, revision_id, admission_kind, snapshot_id, job_id,
                    experiment_id, attempt, state, card_path, card_sha256,
                    command_text, runner_name, git_commit, added_at, added_by
                ) VALUES (1, 11, 'LegacyMarkdownCard/v0', NULL, NULL, 'bad-three', 1,
                          'queued', 'card', ?, 'true', 'bad', ?, ?, 'tester')
                """,
                (SHA, COMMIT_A, NOW),
            )


def test_structured_snapshot_cannot_be_attached_to_legacy_revision(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="imported-project"
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_snapshot(
                connection,
                snapshot_id=1,
                project_id=1,
                revision_id=11,
                project_key="imported-project",
            )


def test_snapshot_project_key_revision_label_and_commit_must_match_owners(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="typed-project", typed=True
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_snapshot(
                connection,
                snapshot_id=1,
                project_id=1,
                revision_id=11,
                project_key="typed-project",
                policy_project_key="wrong-project",
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_snapshot(
                connection,
                snapshot_id=2,
                project_id=1,
                revision_id=11,
                project_key="typed-project",
                revision_label="typed-project:r9",
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_snapshot(
                connection,
                snapshot_id=3,
                project_id=1,
                revision_id=11,
                project_key="typed-project",
                commit=COMMIT_B,
            )


def test_artifact_roots_are_only_readwrite_mount_references_without_paths(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        connection.executemany(
            """
            INSERT INTO project_mounts(
                project_id, revision_id, mount_name, mount_path,
                declared_access, access, required
            ) VALUES (1, 11, ?, ?, ?, ?, 1)
            """,
            (
                ("outputs", "/tmp/project-one-outputs", "readWrite", "readWrite"),
                ("inputs", "/tmp/project-one-inputs", "readOnly", "readOnly"),
            ),
        )
        connection.execute(
            """
            INSERT INTO project_artifact_roots(project_id, revision_id, mount_name)
            VALUES (1, 11, 'outputs')
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO project_artifact_roots(
                    project_id, revision_id, mount_name, mount_access
                ) VALUES (1, 11, 'inputs', 'readOnly')
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                """
                INSERT INTO project_artifact_roots(project_id, revision_id, mount_name)
                VALUES (1, 11, 'missing')
                """
            )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(project_artifact_roots)")
        }
        assert "path" not in columns
        assert "mount_name" in columns


def test_event_scope_and_queue_item_project_ownership_are_enforced(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_project_revision(
            connection, project_id=2, revision_id=22, key="project-two", commit=COMMIT_B
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        connection.execute(
            """
            INSERT INTO events(created_at, actor, event_type, payload_json, scope)
            VALUES (?, 'scheduler', 'HOST_EVENT', '{}', 'host')
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, 'scheduler', 'ITEM_EVENT', 101, '{}', 'project', 1)
            """,
            (NOW,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO events(
                    created_at, actor, event_type, payload_json, scope, project_id
                ) VALUES (?, 'scheduler', 'BAD_HOST', '{}', 'host', 1)
                """,
                (NOW,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                """
                INSERT INTO events(
                    created_at, actor, event_type, queue_item_id, payload_json,
                    scope, project_id
                ) VALUES (?, 'scheduler', 'WRONG_PROJECT', 101, '{}', 'project', 2)
                """,
                (NOW,),
            )


def test_historical_rows_are_restrict_owned_and_append_only(store: V5QueueStore) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, 'tester', 'ITEM_ADDED', 101, '{}', 'project', 1)
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO gpu_reservations(
                gpu_uuid, queue_item_id, status, requested_at, requested_by,
                note, duration_hours
            ) VALUES ('GPU-1', 101, 'failed', ?, 'tester', 'history', 1)
            """,
            (NOW,),
        )
        connection.execute(
            """
            INSERT INTO job_artifacts(
                queue_item_id, project_id, revision_id, segment, evidence_kind,
                artifact_name, artifact_type, absolute_path, recorded_at
            ) VALUES (101, 1, 11, 1, 'legacy-v4', 'output', 'file',
                      '/tmp/project-one-output', ?)
            """,
            (NOW,),
        )
        for statement in (
            "DELETE FROM queue_items WHERE id = 101",
            "DELETE FROM project_revisions WHERE id = 11",
            "DELETE FROM projects WHERE id = 1",
            "UPDATE events SET event_type = 'changed' WHERE id = 1",
            "DELETE FROM job_artifacts WHERE id = 1",
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement)
        assert connection.execute(
            "SELECT COUNT(*) FROM queue_items WHERE id = 101"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM events WHERE queue_item_id = 101"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM job_artifacts WHERE queue_item_id = 101"
        ).fetchone()[0] == 1


def test_published_migration_evidence_is_owned_complete_and_append_only(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="imported-project"
        )
        connection.execute(
            """
            INSERT INTO migration_sources(
                id, source_schema_version, source_state_path,
                source_database_path, source_database_sha256,
                source_database_size_bytes, source_database_mtime_ns,
                source_state_identity_json, source_state_identity_sha256,
                project_id, revision_id, importer_package_version,
                imported_at, imported_by
            ) VALUES (
                7, 4, '/tmp/legacy-state', '/tmp/legacy-state/queue.sqlite3', ?,
                4096, 123456789, X'7b7d', ?, 1, 11, '0.1.0', ?, 'tester'
            )
            """,
            (SHA, SHA, NOW),
        )
        connection.executemany(
            """
            INSERT INTO legacy_metadata(
                migration_source_id, source_key, source_value
            ) VALUES (7, ?, ?)
            """,
            (
                ("schema_version", "4"),
                ("unknown-empty-value", ""),
                ("unknown-key", " exact legacy text "),
            ),
        )
        connection.execute(
            """
            INSERT INTO migration_receipts(
                migration_source_id, project_id, revision_id,
                protocol_api_version, protocol_kind, result, receipt_json,
                receipt_sha256, started_at, finished_at, actor
            ) VALUES (
                7, 1, 11, 'experiment-queue/v1', 'QueueMigrationReceipt',
                'succeeded', X'7b7d', ?, ?, ?, 'tester'
            )
            """,
            (SHA, NOW, NOW),
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE migration_sources SET source_schema_version = 3 WHERE id = 7"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE migration_receipts SET actor = 'other' WHERE migration_source_id = 7"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                """
                UPDATE legacy_metadata SET source_value = 'normalized'
                WHERE migration_source_id = 7 AND source_key = 'unknown-key'
                """
            )
        assert connection.execute(
            """
            SELECT source_value FROM legacy_metadata
            WHERE migration_source_id = 7 AND source_key = 'unknown-key'
            """
        ).fetchone()[0] == " exact legacy text "
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                """
                INSERT INTO migration_receipts(
                    migration_source_id, project_id, revision_id,
                    protocol_api_version, protocol_kind, result, receipt_json,
                    receipt_sha256, started_at, finished_at, actor
                ) VALUES (
                    999, 1, 11, 'experiment-queue/v1', 'QueueMigrationReceipt',
                    'succeeded', X'7b7d', ?, ?, ?, 'tester'
                )
                """,
                (SHA, NOW, NOW),
            )


def test_destination_receipt_table_accepts_only_successful_real_import_evidence(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="imported-project"
        )
        connection.execute(
            """
            INSERT INTO migration_sources(
                source_schema_version, source_state_path, source_database_path,
                source_database_sha256, source_database_size_bytes,
                source_database_mtime_ns, source_state_identity_json,
                source_state_identity_sha256, project_id, revision_id,
                importer_package_version, imported_at, imported_by
            ) VALUES (4, '/tmp/source', '/tmp/source/queue.sqlite3', ?, 4096,
                      123456789, X'7b7d', ?, 1, 11, '0.1.0', ?, 'tester')
            """,
            (SHA, SHA, NOW),
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO migration_receipts(
                    migration_source_id, project_id, revision_id,
                    protocol_api_version, protocol_kind, result, receipt_json,
                    receipt_sha256, started_at, finished_at, actor
                ) VALUES (1, 1, 11, 'experiment-queue/v1',
                          'QueueMigrationReceipt', 'failed', X'7b7d', ?, ?, ?, 'tester')
                """,
                (SHA, NOW, NOW),
            )


def test_revision_bindings_and_admission_snapshots_are_immutable(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="typed-project", typed=True
        )
        connection.execute(
            """
            INSERT INTO project_mounts(
                project_id, revision_id, mount_name, mount_path,
                declared_access, access, required
            ) VALUES (1, 11, 'outputs', '/tmp/typed-outputs',
                      'readWrite', 'readWrite', 1)
            """
        )
        connection.execute(
            """
            INSERT INTO project_environments(
                project_id, revision_id, environment_name,
                search_directories_json, search_directories_sha256,
                inherit_variables_json, inherit_variables_sha256
            ) VALUES (1, 11, 'python', X'5b5d', ?, X'5b5d', ?)
            """,
            (SHA, SHA),
        )
        _insert_snapshot(
            connection,
            snapshot_id=50,
            project_id=1,
            revision_id=11,
            project_key="typed-project",
        )
        for statement in (
            "UPDATE project_revisions SET display_name = 'changed' WHERE id = 11",
            "UPDATE project_mounts SET mount_path = '/tmp/other' WHERE revision_id = 11",
            "UPDATE project_environments SET environment_name = 'other' WHERE revision_id = 11",
            "UPDATE admission_snapshots SET package_version = 'other' WHERE id = 50",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="immutable|append-only"):
                connection.execute(statement)


def test_project_archival_requires_pause_no_active_work_and_is_permanent(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        with pytest.raises(sqlite3.IntegrityError, match="paused"):
            connection.execute("UPDATE projects SET lifecycle = 'archived' WHERE id = 1")
        connection.execute("UPDATE projects SET lifecycle = 'paused' WHERE id = 1")
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        with pytest.raises(sqlite3.IntegrityError, match="active work"):
            connection.execute("UPDATE projects SET lifecycle = 'archived' WHERE id = 1")
        connection.execute(
            "UPDATE queue_items SET state = 'removed' WHERE id = 101",
        )
        connection.execute(
            """
            UPDATE queue_items
            SET runtime_worktree_path = '/tmp/v5-worktree',
                runtime_worktree_created_at = ?
            WHERE id = 101
            """,
            (NOW,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="incomplete checkout"):
            connection.execute("UPDATE projects SET lifecycle = 'archived' WHERE id = 1")
        connection.execute(
            "UPDATE queue_items SET runtime_worktree_removed_at = ? WHERE id = 101",
            (NOW,),
        )
        connection.execute("UPDATE projects SET lifecycle = 'archived' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match="cannot be restored"):
            connection.execute("UPDATE projects SET lifecycle = 'paused' WHERE id = 1")


def test_strict_checks_reject_invalid_flags_hashes_and_self_dependencies(
    store: V5QueueStore,
) -> None:
    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                "UPDATE queue_items SET preemptible = 2 WHERE id = 101"
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                "INSERT INTO dependencies(queue_item_id, dependency_item_id) VALUES (101, 101)"
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                INSERT INTO gpu_allowlist(
                    uuid, requested_identifier, last_index, name,
                    enabled, draining, updated_at
                ) VALUES ('GPU-1', '0', '0', 'GPU', 3, 0, ?)
                """,
                (NOW,),
            )


def test_runtime_gpu_lease_checks_reject_malformed_assignment_and_state(
    store: V5QueueStore,
) -> None:
    """Scheduler-critical GPU leases remain complete and state-bound in SQLite."""

    with store.connect() as connection:
        _insert_project_revision(
            connection, project_id=1, revision_id=11, key="project-one"
        )
        _insert_queue_item(connection, item_id=101, project_id=1, revision_id=11)

        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                "UPDATE queue_items SET assigned_gpu_uuid = 'GPU-1' WHERE id = 101"
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                "UPDATE queue_items SET runtime_gpu_lease_released_at = ? WHERE id = 101",
                (NOW,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                UPDATE queue_items
                SET state = 'starting', assigned_gpu_uuid = 'GPU-1',
                    assigned_gpu_index = '0'
                WHERE id = 101
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                """
                UPDATE queue_items
                SET state = 'held', assigned_gpu_uuid = 'GPU-1',
                    assigned_gpu_index = '0', runtime_gpu_lease_held = 1
                WHERE id = 101
                """
            )

        connection.execute(
            """
            UPDATE queue_items
            SET state = 'failed', assigned_gpu_uuid = 'GPU-1',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1
            WHERE id = 101
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            connection.execute(
                "UPDATE queue_items SET state = 'removed' WHERE id = 101"
            )
        connection.execute(
            """
            UPDATE queue_items
            SET runtime_gpu_lease_held = 0,
                runtime_gpu_lease_released_at = ?
            WHERE id = 101
            """,
            (NOW,),
        )
        row = connection.execute(
            """
            SELECT assigned_gpu_uuid, assigned_gpu_index,
                   runtime_gpu_lease_held, runtime_gpu_lease_released_at
            FROM queue_items WHERE id = 101
            """
        ).fetchone()
    assert tuple(row) == ("GPU-1", "0", 0, NOW)


def test_foreign_keys_are_enabled_on_every_public_connection(store: V5QueueStore) -> None:
    for _ in range(2):
        with store.connect() as connection:
            assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
            with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
                connection.execute(
                    """
                    INSERT INTO project_runtime_state(
                        project_id, health_reason, health_actor, health_changed_at
                    ) VALUES (999, 'healthy', 'tester', '2026-08-28T12:00:00+00:00')
                    """
                )
