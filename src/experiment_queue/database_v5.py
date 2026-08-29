"""Create and safely open the fresh multi-project SQLite schema version 5.

This module is deliberately separate from the schema-v4 ``QueueStore``.  It
does not migrate, repair, or reinterpret an existing database: callers must use
the future explicit offline importer for versions 1 through 4.  Existing files
are inspected read-only before any connection pragma or DDL is allowed.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import stat
from typing import Final
import uuid

from experiment_queue.path_security import (
    PathBoundaryError,
    SecurePathBoundary,
    capture_secure_path_boundary,
    revalidate_secure_path_boundary,
)


SCHEMA_VERSION: Final = 5
DATABASE_FILENAME: Final = "queue.sqlite3"
SCHEMA_IDENTITY: Final = "experiment-queue/database-v5"
DATABASE_INSTANCE_ID_KEY: Final = "database_instance_id"


class V5DatabaseError(RuntimeError):
    """Raised when schema-v5 state cannot be created or opened safely."""


class V5SchemaVersionError(V5DatabaseError):
    """Raised when an existing database is not exactly schema version 5."""


class _FreshPublicationIndeterminate(V5DatabaseError):
    """A fresh final link is visible but its directory fsync did not complete."""


class _ClosingConnection(sqlite3.Connection):
    """Commit or roll back a context-managed transaction, then close it."""

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc, traceback))
        finally:
            self.close()


# These statements are an immutable fresh-v5 definition, not startup migration
# steps.  They intentionally omit IF NOT EXISTS so a partially initialized or
# externally modified database fails closed instead of being repaired in place.
_TABLE_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TABLE metadata (
        key TEXT PRIMARY KEY CHECK(length(key) > 0),
        value TEXT NOT NULL
    ) STRICT
    """,
    """
    CREATE TABLE projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        project_key TEXT NOT NULL UNIQUE
            CHECK(length(project_key) BETWEEN 1 AND 63)
            CHECK(substr(project_key, 1, 1) GLOB '[a-z]')
            CHECK(project_key NOT GLOB '*[^a-z0-9-]*')
            CHECK(project_key NOT LIKE '%--%')
            CHECK(substr(project_key, -1, 1) GLOB '[a-z0-9]'),
        display_name TEXT NOT NULL CHECK(length(display_name) > 0),
        lifecycle TEXT NOT NULL CHECK(lifecycle IN ('active', 'paused', 'archived')),
        current_revision_id INTEGER NOT NULL CHECK(current_revision_id > 0),
        current_revision_sequence INTEGER NOT NULL CHECK(current_revision_sequence > 0),
        created_at TEXT NOT NULL CHECK(length(created_at) > 0),
        created_by TEXT NOT NULL CHECK(length(created_by) > 0),
        lifecycle_changed_at TEXT NOT NULL CHECK(length(lifecycle_changed_at) > 0),
        lifecycle_actor TEXT NOT NULL CHECK(length(lifecycle_actor) > 0),
        lifecycle_reason TEXT NOT NULL CHECK(length(lifecycle_reason) > 0),
        UNIQUE(id, project_key),
        UNIQUE(id, current_revision_id, current_revision_sequence),
        FOREIGN KEY(
            id, current_revision_id, current_revision_sequence, display_name
        ) REFERENCES project_revisions(project_id, id, sequence, display_name)
            ON UPDATE RESTRICT ON DELETE RESTRICT
            DEFERRABLE INITIALLY DEFERRED
    ) STRICT
    """,
    """
    CREATE TABLE project_revisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        sequence INTEGER NOT NULL CHECK(sequence > 0),
        revision_label TEXT NOT NULL UNIQUE CHECK(length(revision_label) > 0),
        revision_kind TEXT NOT NULL
            CHECK(revision_kind IN ('project-v1', 'legacy-v4')),
        display_name TEXT NOT NULL CHECK(length(display_name) > 0),
        git_commit TEXT
            CHECK(git_commit IS NULL OR (
                length(git_commit) IN (40, 64)
                AND git_commit NOT GLOB '*[^0-9a-f]*'
            )),
        project_source_path TEXT,
        project_source BLOB,
        project_source_sha256 TEXT
            CHECK(project_source_sha256 IS NULL OR (
                length(project_source_sha256) = 64
                AND project_source_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        project_blob_object_id TEXT
            CHECK(project_blob_object_id IS NULL OR (
                length(project_blob_object_id) IN (40, 64)
                AND project_blob_object_id NOT GLOB '*[^0-9a-f]*'
            )),
        project_blob_mode TEXT
            CHECK(project_blob_mode IS NULL OR project_blob_mode IN ('100644', '100755')),
        project_blob_size INTEGER
            CHECK(project_blob_size IS NULL OR project_blob_size >= 0),
        project_normalized_json BLOB,
        project_normalized_sha256 TEXT
            CHECK(project_normalized_sha256 IS NULL OR (
                length(project_normalized_sha256) = 64
                AND project_normalized_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        project_schema_api_version TEXT,
        project_schema_kind TEXT,
        project_schema_id TEXT,
        project_schema_sha256 TEXT
            CHECK(project_schema_sha256 IS NULL OR (
                length(project_schema_sha256) = 64
                AND project_schema_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_source_path TEXT,
        extension_schema_source BLOB,
        extension_schema_source_sha256 TEXT
            CHECK(extension_schema_source_sha256 IS NULL OR (
                length(extension_schema_source_sha256) = 64
                AND extension_schema_source_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_blob_object_id TEXT
            CHECK(extension_schema_blob_object_id IS NULL OR (
                length(extension_schema_blob_object_id) IN (40, 64)
                AND extension_schema_blob_object_id NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_blob_mode TEXT
            CHECK(extension_schema_blob_mode IS NULL OR
                  extension_schema_blob_mode IN ('100644', '100755')),
        extension_schema_blob_size INTEGER
            CHECK(extension_schema_blob_size IS NULL OR extension_schema_blob_size >= 0),
        extension_schema_canonical_json BLOB,
        extension_schema_canonical_sha256 TEXT
            CHECK(extension_schema_canonical_sha256 IS NULL OR (
                length(extension_schema_canonical_sha256) = 64
                AND extension_schema_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_id TEXT,
        checkout_path TEXT NOT NULL
            CHECK(length(checkout_path) > 1 AND substr(checkout_path, 1, 1) = '/'),
        project_manifest_path TEXT
            CHECK(project_manifest_path IS NULL OR (
                length(project_manifest_path) > 0
                AND substr(project_manifest_path, 1, 1) <> '/'
                AND instr(project_manifest_path, '\\') = 0
            )),
        enrollment_json BLOB NOT NULL CHECK(length(enrollment_json) > 0),
        enrollment_sha256 TEXT NOT NULL
            CHECK(length(enrollment_sha256) = 64)
            CHECK(enrollment_sha256 NOT GLOB '*[^0-9a-f]*'),
        validated_package_version TEXT,
        created_at TEXT NOT NULL CHECK(length(created_at) > 0),
        created_actor TEXT NOT NULL CHECK(length(created_actor) > 0),
        UNIQUE(project_id, sequence),
        UNIQUE(project_id, id),
        UNIQUE(project_id, id, git_commit),
        UNIQUE(project_id, id, sequence, display_name),
        UNIQUE(project_id, id, revision_kind, revision_label, git_commit),
        UNIQUE(project_id, id, revision_label, git_commit),
        CHECK(project_source_path IS NULL OR project_manifest_path = project_source_path),
        CHECK(
            (revision_kind = 'project-v1'
                AND git_commit IS NOT NULL
                AND project_manifest_path IS NOT NULL
                AND project_source_path IS NOT NULL
                AND length(project_source_path) > 0
                AND project_source IS NOT NULL
                AND length(project_source) > 0
                AND project_source_sha256 IS NOT NULL
                AND project_blob_object_id IS NOT NULL
                AND project_blob_mode IS NOT NULL
                AND project_blob_size = length(project_source)
                AND project_normalized_json IS NOT NULL
                AND length(project_normalized_json) > 0
                AND project_normalized_sha256 IS NOT NULL
                AND project_schema_api_version IS NOT NULL
                AND length(project_schema_api_version) > 0
                AND project_schema_kind IS NOT NULL
                AND length(project_schema_kind) > 0
                AND project_schema_id IS NOT NULL
                AND length(project_schema_id) > 0
                AND project_schema_sha256 IS NOT NULL
                AND validated_package_version IS NOT NULL
                AND length(validated_package_version) > 0)
            OR
            (revision_kind = 'legacy-v4'
                AND project_manifest_path IS NULL
                AND project_source_path IS NULL
                AND project_source IS NULL
                AND project_source_sha256 IS NULL
                AND project_blob_object_id IS NULL
                AND project_blob_mode IS NULL
                AND project_blob_size IS NULL
                AND project_normalized_json IS NULL
                AND project_normalized_sha256 IS NULL
                AND project_schema_api_version IS NULL
                AND project_schema_kind IS NULL
                AND project_schema_id IS NULL
                AND project_schema_sha256 IS NULL
                AND validated_package_version IS NULL)
        ),
        CHECK(
            (extension_schema_source_path IS NULL
                AND extension_schema_source IS NULL
                AND extension_schema_source_sha256 IS NULL
                AND extension_schema_blob_object_id IS NULL
                AND extension_schema_blob_mode IS NULL
                AND extension_schema_blob_size IS NULL
                AND extension_schema_canonical_json IS NULL
                AND extension_schema_canonical_sha256 IS NULL
                AND extension_schema_id IS NULL)
            OR
            (revision_kind = 'project-v1'
                AND extension_schema_source_path IS NOT NULL
                AND length(extension_schema_source_path) > 0
                AND extension_schema_source IS NOT NULL
                AND length(extension_schema_source) > 0
                AND extension_schema_source_sha256 IS NOT NULL
                AND extension_schema_blob_object_id IS NOT NULL
                AND extension_schema_blob_mode IS NOT NULL
                AND extension_schema_blob_size = length(extension_schema_source)
                AND extension_schema_canonical_json IS NOT NULL
                AND length(extension_schema_canonical_json) > 0
                AND extension_schema_canonical_sha256 IS NOT NULL
                AND (extension_schema_id IS NULL OR length(extension_schema_id) > 0))
        ),
        FOREIGN KEY(project_id) REFERENCES projects(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE project_mounts (
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        mount_name TEXT NOT NULL
            CHECK(length(mount_name) BETWEEN 1 AND 63)
            CHECK(substr(mount_name, 1, 1) GLOB '[a-z]')
            CHECK(mount_name NOT GLOB '*[^a-z0-9-]*')
            CHECK(mount_name NOT LIKE '%--%')
            CHECK(substr(mount_name, -1, 1) GLOB '[a-z0-9]'),
        mount_path TEXT NOT NULL
            CHECK(length(mount_path) > 1 AND substr(mount_path, 1, 1) = '/'),
        declared_access TEXT NOT NULL
            CHECK(declared_access IN ('readOnly', 'readWrite')),
        access TEXT NOT NULL CHECK(access IN ('readOnly', 'readWrite')),
        required INTEGER NOT NULL CHECK(required IN (0, 1)),
        checkout_descendant INTEGER NOT NULL DEFAULT 0
            CHECK(checkout_descendant IN (0, 1)),
        git_ignored INTEGER NOT NULL DEFAULT 0 CHECK(git_ignored IN (0, 1)),
        PRIMARY KEY(project_id, revision_id, mount_name),
        UNIQUE(project_id, revision_id, mount_name, access),
        CHECK(NOT (declared_access = 'readOnly' AND access = 'readWrite')),
        CHECK(checkout_descendant = 0 OR git_ignored = 1),
        FOREIGN KEY(project_id, revision_id)
            REFERENCES project_revisions(project_id, id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE project_artifact_roots (
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        mount_name TEXT NOT NULL CHECK(length(mount_name) > 0),
        mount_access TEXT NOT NULL DEFAULT 'readWrite'
            CHECK(mount_access = 'readWrite'),
        PRIMARY KEY(project_id, revision_id, mount_name),
        FOREIGN KEY(project_id, revision_id, mount_name, mount_access)
            REFERENCES project_mounts(project_id, revision_id, mount_name, access)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE project_environments (
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        environment_name TEXT NOT NULL
            CHECK(length(environment_name) BETWEEN 1 AND 63)
            CHECK(substr(environment_name, 1, 1) GLOB '[a-z]')
            CHECK(environment_name NOT GLOB '*[^a-z0-9-]*')
            CHECK(environment_name NOT LIKE '%--%')
            CHECK(substr(environment_name, -1, 1) GLOB '[a-z0-9]'),
        search_directories_json BLOB NOT NULL
            CHECK(length(search_directories_json) > 0),
        search_directories_sha256 TEXT NOT NULL
            CHECK(length(search_directories_sha256) = 64)
            CHECK(search_directories_sha256 NOT GLOB '*[^0-9a-f]*'),
        command_prefix_json BLOB,
        command_prefix_sha256 TEXT
            CHECK(command_prefix_sha256 IS NULL OR (
                length(command_prefix_sha256) = 64
                AND command_prefix_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        inherit_variables_json BLOB NOT NULL
            CHECK(length(inherit_variables_json) > 0),
        inherit_variables_sha256 TEXT NOT NULL
            CHECK(length(inherit_variables_sha256) = 64)
            CHECK(inherit_variables_sha256 NOT GLOB '*[^0-9a-f]*'),
        PRIMARY KEY(project_id, revision_id, environment_name),
        CHECK((command_prefix_json IS NULL) = (command_prefix_sha256 IS NULL)),
        CHECK(command_prefix_json IS NULL OR length(command_prefix_json) > 0),
        FOREIGN KEY(project_id, revision_id)
            REFERENCES project_revisions(project_id, id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE project_runtime_state (
        project_id INTEGER PRIMARY KEY CHECK(project_id > 0),
        health TEXT NOT NULL DEFAULT 'closed' CHECK(health IN ('closed', 'open')),
        circuit_failure_count INTEGER NOT NULL DEFAULT 0
            CHECK(circuit_failure_count >= 0),
        health_reason TEXT NOT NULL CHECK(length(health_reason) > 0),
        health_actor TEXT NOT NULL CHECK(length(health_actor) > 0),
        health_changed_at TEXT NOT NULL CHECK(length(health_changed_at) > 0),
        CHECK(health = 'closed' OR circuit_failure_count > 0),
        FOREIGN KEY(project_id) REFERENCES projects(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE admission_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        revision_kind TEXT NOT NULL DEFAULT 'project-v1'
            CHECK(revision_kind = 'project-v1'),
        project_revision_label TEXT NOT NULL CHECK(length(project_revision_label) > 0),
        git_commit TEXT NOT NULL
            CHECK(length(git_commit) IN (40, 64))
            CHECK(git_commit NOT GLOB '*[^0-9a-f]*'),
        project_source_name TEXT NOT NULL CHECK(length(project_source_name) > 0),
        project_source BLOB NOT NULL CHECK(length(project_source) > 0),
        project_source_sha256 TEXT NOT NULL
            CHECK(length(project_source_sha256) = 64)
            CHECK(project_source_sha256 NOT GLOB '*[^0-9a-f]*'),
        project_blob_object_id TEXT NOT NULL
            CHECK(length(project_blob_object_id) IN (40, 64))
            CHECK(project_blob_object_id NOT GLOB '*[^0-9a-f]*'),
        project_blob_mode TEXT NOT NULL
            CHECK(project_blob_mode IN ('100644', '100755')),
        project_blob_size INTEGER NOT NULL
            CHECK(project_blob_size >= 0),
        project_normalized_json BLOB NOT NULL
            CHECK(length(project_normalized_json) > 0),
        project_normalized_sha256 TEXT NOT NULL
            CHECK(length(project_normalized_sha256) = 64)
            CHECK(project_normalized_sha256 NOT GLOB '*[^0-9a-f]*'),
        project_schema_api_version TEXT NOT NULL
            CHECK(length(project_schema_api_version) > 0),
        project_schema_kind TEXT NOT NULL CHECK(length(project_schema_kind) > 0),
        project_schema_id TEXT NOT NULL CHECK(length(project_schema_id) > 0),
        project_schema_sha256 TEXT NOT NULL
            CHECK(length(project_schema_sha256) = 64)
            CHECK(project_schema_sha256 NOT GLOB '*[^0-9a-f]*'),
        card_source_name TEXT NOT NULL CHECK(length(card_source_name) > 0),
        card_source BLOB NOT NULL CHECK(length(card_source) > 0),
        card_source_sha256 TEXT NOT NULL
            CHECK(length(card_source_sha256) = 64)
            CHECK(card_source_sha256 NOT GLOB '*[^0-9a-f]*'),
        card_blob_object_id TEXT NOT NULL
            CHECK(length(card_blob_object_id) IN (40, 64))
            CHECK(card_blob_object_id NOT GLOB '*[^0-9a-f]*'),
        card_blob_mode TEXT NOT NULL
            CHECK(card_blob_mode IN ('100644', '100755')),
        card_blob_size INTEGER NOT NULL CHECK(card_blob_size >= 0),
        card_normalized_json BLOB NOT NULL CHECK(length(card_normalized_json) > 0),
        card_normalized_sha256 TEXT NOT NULL
            CHECK(length(card_normalized_sha256) = 64)
            CHECK(card_normalized_sha256 NOT GLOB '*[^0-9a-f]*'),
        card_schema_api_version TEXT NOT NULL
            CHECK(length(card_schema_api_version) > 0),
        card_schema_kind TEXT NOT NULL CHECK(length(card_schema_kind) > 0),
        card_schema_id TEXT NOT NULL CHECK(length(card_schema_id) > 0),
        card_schema_sha256 TEXT NOT NULL
            CHECK(length(card_schema_sha256) = 64)
            CHECK(card_schema_sha256 NOT GLOB '*[^0-9a-f]*'),
        extension_schema_source_name TEXT,
        extension_schema_reference_path TEXT,
        extension_schema_source BLOB,
        extension_schema_source_sha256 TEXT
            CHECK(extension_schema_source_sha256 IS NULL OR (
                length(extension_schema_source_sha256) = 64
                AND extension_schema_source_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_blob_object_id TEXT
            CHECK(extension_schema_blob_object_id IS NULL OR (
                length(extension_schema_blob_object_id) IN (40, 64)
                AND extension_schema_blob_object_id NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_blob_mode TEXT
            CHECK(extension_schema_blob_mode IS NULL OR
                  extension_schema_blob_mode IN ('100644', '100755')),
        extension_schema_blob_size INTEGER
            CHECK(extension_schema_blob_size IS NULL OR extension_schema_blob_size >= 0),
        extension_schema_canonical_json BLOB,
        extension_schema_canonical_sha256 TEXT
            CHECK(extension_schema_canonical_sha256 IS NULL OR (
                length(extension_schema_canonical_sha256) = 64
                AND extension_schema_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        extension_schema_id TEXT,
        resolved_json BLOB NOT NULL CHECK(length(resolved_json) > 0),
        resolved_sha256 TEXT NOT NULL
            CHECK(length(resolved_sha256) = 64)
            CHECK(resolved_sha256 NOT GLOB '*[^0-9a-f]*'),
        command_kind TEXT NOT NULL CHECK(command_kind IN ('argv', 'wrapper', 'shell')),
        command_json BLOB NOT NULL CHECK(length(command_json) > 0),
        command_sha256 TEXT NOT NULL
            CHECK(length(command_sha256) = 64)
            CHECK(command_sha256 NOT GLOB '*[^0-9a-f]*'),
        package_version TEXT NOT NULL CHECK(length(package_version) > 0),
        policy_project_key TEXT NOT NULL CHECK(length(policy_project_key) > 0),
        policy_card_path TEXT NOT NULL CHECK(length(policy_card_path) > 0),
        policy_job_id TEXT NOT NULL CHECK(length(policy_job_id) > 0),
        policy_priority INTEGER NOT NULL,
        policy_hold_reason TEXT,
        policy_operator TEXT NOT NULL,
        policy_preemption_authorized INTEGER NOT NULL
            CHECK(policy_preemption_authorized IN (0, 1)),
        policy_bindings_json BLOB NOT NULL CHECK(length(policy_bindings_json) > 0),
        policy_bindings_sha256 TEXT NOT NULL
            CHECK(length(policy_bindings_sha256) = 64)
            CHECK(policy_bindings_sha256 NOT GLOB '*[^0-9a-f]*'),
        policy_dependencies_json BLOB NOT NULL
            CHECK(length(policy_dependencies_json) > 0),
        policy_dependencies_sha256 TEXT NOT NULL
            CHECK(length(policy_dependencies_sha256) = 64)
            CHECK(policy_dependencies_sha256 NOT GLOB '*[^0-9a-f]*'),
        policy_json BLOB NOT NULL CHECK(length(policy_json) > 0),
        policy_sha256 TEXT NOT NULL
            CHECK(length(policy_sha256) = 64)
            CHECK(policy_sha256 NOT GLOB '*[^0-9a-f]*'),
        created_at TEXT NOT NULL CHECK(length(created_at) > 0),
        UNIQUE(id, project_id, revision_id),
        CHECK(project_blob_size = length(project_source)),
        CHECK(card_blob_size = length(card_source)),
        CHECK(
            (extension_schema_source_name IS NULL
                AND extension_schema_reference_path IS NULL
                AND extension_schema_source IS NULL
                AND extension_schema_source_sha256 IS NULL
                AND extension_schema_blob_object_id IS NULL
                AND extension_schema_blob_mode IS NULL
                AND extension_schema_blob_size IS NULL
                AND extension_schema_canonical_json IS NULL
                AND extension_schema_canonical_sha256 IS NULL
                AND extension_schema_id IS NULL)
            OR
            (extension_schema_source_name IS NOT NULL
                AND length(extension_schema_source_name) > 0
                AND extension_schema_reference_path IS NOT NULL
                AND length(extension_schema_reference_path) > 0
                AND extension_schema_source IS NOT NULL
                AND length(extension_schema_source) > 0
                AND extension_schema_source_sha256 IS NOT NULL
                AND extension_schema_blob_object_id IS NOT NULL
                AND extension_schema_blob_mode IS NOT NULL
                AND extension_schema_blob_size = length(extension_schema_source)
                AND extension_schema_canonical_json IS NOT NULL
                AND length(extension_schema_canonical_json) > 0
                AND extension_schema_canonical_sha256 IS NOT NULL
                AND (extension_schema_id IS NULL OR length(extension_schema_id) > 0))
        ),
        FOREIGN KEY(
            project_id, revision_id, revision_kind,
            project_revision_label, git_commit
        ) REFERENCES project_revisions(
            project_id, id, revision_kind, revision_label, git_commit
        )
            ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY(project_id, policy_project_key)
            REFERENCES projects(id, project_key)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE queue_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        admission_kind TEXT NOT NULL
            CHECK(admission_kind IN ('ExperimentCard/v1', 'LegacyMarkdownCard/v0')),
        snapshot_id INTEGER CHECK(snapshot_id IS NULL OR snapshot_id > 0),
        job_id TEXT,
        experiment_id TEXT NOT NULL CHECK(length(experiment_id) > 0),
        attempt INTEGER NOT NULL CHECK(attempt > 0),
        state TEXT NOT NULL CHECK(state IN (
            'queued', 'held', 'blocked', 'starting', 'running', 'yielding',
            'terminating', 'force_killing', 'succeeded', 'failed',
            'interrupted', 'force_killed', 'removed'
        )),
        priority INTEGER NOT NULL DEFAULT 0,
        card_path TEXT NOT NULL CHECK(length(card_path) > 0),
        card_sha256 TEXT NOT NULL
            CHECK(length(card_sha256) = 64)
            CHECK(card_sha256 NOT GLOB '*[^0-9a-f]*'),
        command_text TEXT NOT NULL CHECK(length(command_text) > 0),
        runner_name TEXT NOT NULL CHECK(length(runner_name) > 0),
        git_commit TEXT NOT NULL
            CHECK(length(git_commit) IN (40, 64))
            CHECK(git_commit NOT GLOB '*[^0-9a-f]*'),
        added_at TEXT NOT NULL CHECK(length(added_at) > 0),
        added_by TEXT NOT NULL CHECK(length(added_by) > 0),
        state_detail TEXT,
        assigned_gpu_uuid TEXT,
        assigned_gpu_index TEXT,
        runtime_gpu_lease_held INTEGER NOT NULL DEFAULT 0
            CHECK(runtime_gpu_lease_held IN (0, 1)),
        runtime_gpu_lease_released_at TEXT
            CHECK(runtime_gpu_lease_released_at IS NULL
                  OR length(runtime_gpu_lease_released_at) > 0),
        pid INTEGER CHECK(pid IS NULL OR pid > 0),
        pgid INTEGER CHECK(pgid IS NULL OR pgid > 0),
        proc_start_ticks TEXT,
        started_at TEXT,
        finished_at TEXT,
        return_code INTEGER,
        terminate_requested_at TEXT,
        terminate_reason TEXT,
        termination_stage TEXT
            CHECK(termination_stage IS NULL OR termination_stage IN (
                'interrupt', 'terminate', 'kill'
            )),
        termination_signal_epoch REAL,
        contention_detected INTEGER NOT NULL DEFAULT 0
            CHECK(contention_detected IN (0, 1)),
        repo_drift_detected INTEGER NOT NULL DEFAULT 0
            CHECK(repo_drift_detected IN (0, 1)),
        runner_run_dir TEXT,
        runner_manifest_path TEXT,
        rsync_pull_command TEXT,
        preemptible INTEGER NOT NULL DEFAULT 0 CHECK(preemptible IN (0, 1)),
        segment INTEGER NOT NULL DEFAULT 1 CHECK(segment > 0),
        resume_front INTEGER NOT NULL DEFAULT 0 CHECK(resume_front IN (0, 1)),
        yield_requested_at TEXT,
        yield_requested_by TEXT,
        yield_request_id TEXT,
        yield_note TEXT,
        yield_duration_hours INTEGER
            CHECK(yield_duration_hours IS NULL OR yield_duration_hours BETWEEN 1 AND 24),
        continuation_checkpoint TEXT,
        continuation_checkpoint_sha256 TEXT
            CHECK(continuation_checkpoint_sha256 IS NULL OR (
                length(continuation_checkpoint_sha256) = 64
                AND continuation_checkpoint_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        continuation_checkpoint_metadata TEXT,
        continuation_checkpoint_metadata_sha256 TEXT
            CHECK(continuation_checkpoint_metadata_sha256 IS NULL OR (
                length(continuation_checkpoint_metadata_sha256) = 64
                AND continuation_checkpoint_metadata_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        continuation_step INTEGER CHECK(continuation_step IS NULL OR continuation_step >= 0),
        continuation_wandb_id TEXT,
        git_ref TEXT,
        worktree_path TEXT,
        worktree_created_at TEXT,
        worktree_removed_at TEXT,
        worktree_cleanup_error TEXT,
        runtime_git_ref TEXT,
        runtime_worktree_path TEXT,
        runtime_worktree_created_at TEXT,
        runtime_worktree_removed_at TEXT,
        runtime_worktree_cleanup_error TEXT,
        UNIQUE(project_id, experiment_id, attempt),
        UNIQUE(snapshot_id),
        UNIQUE(id, project_id),
        UNIQUE(id, project_id, revision_id),
        UNIQUE(id, project_id, revision_id, admission_kind),
        CHECK(
            (admission_kind = 'ExperimentCard/v1'
                AND snapshot_id IS NOT NULL
                AND job_id IS NOT NULL AND length(job_id) > 0)
            OR
            (admission_kind = 'LegacyMarkdownCard/v0'
                AND snapshot_id IS NULL
                AND job_id IS NULL)
        ),
        CHECK((continuation_checkpoint IS NULL) =
              (continuation_checkpoint_sha256 IS NULL)),
        CHECK((continuation_checkpoint_metadata IS NULL) =
              (continuation_checkpoint_metadata_sha256 IS NULL)),
        CHECK((assigned_gpu_uuid IS NULL) = (assigned_gpu_index IS NULL)),
        CHECK(
            runtime_gpu_lease_held = 0
            OR (
                assigned_gpu_uuid IS NOT NULL
                AND assigned_gpu_index IS NOT NULL
                AND runtime_gpu_lease_released_at IS NULL
            )
        ),
        CHECK(
            runtime_gpu_lease_released_at IS NULL
            OR (
                runtime_gpu_lease_held = 0
                AND assigned_gpu_uuid IS NOT NULL
                AND assigned_gpu_index IS NOT NULL
            )
        ),
        CHECK(
            runtime_gpu_lease_held = 0
            OR state IN (
                'starting', 'running', 'yielding', 'terminating', 'force_killing',
                'succeeded', 'failed', 'interrupted', 'force_killed'
            )
        ),
        CHECK(
            state NOT IN (
                'starting', 'running', 'yielding', 'terminating', 'force_killing'
            )
            OR runtime_gpu_lease_held = 1
        ),
        CHECK(state <> 'queued' OR runtime_gpu_lease_held = 0),
        FOREIGN KEY(project_id, revision_id, git_commit)
            REFERENCES project_revisions(project_id, id, git_commit)
            ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY(snapshot_id, project_id, revision_id)
            REFERENCES admission_snapshots(id, project_id, revision_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE cooperative_yield_requests (
        request_id TEXT PRIMARY KEY
            CHECK(length(request_id) BETWEEN 1 AND 256),
        queue_item_id INTEGER NOT NULL CHECK(queue_item_id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        admission_kind TEXT NOT NULL DEFAULT 'ExperimentCard/v1'
            CHECK(admission_kind = 'ExperimentCard/v1'),
        segment INTEGER NOT NULL CHECK(segment > 0),
        protocol_api_version TEXT NOT NULL
            CHECK(protocol_api_version = 'experiment-queue/v1'),
        protocol_kind TEXT NOT NULL
            CHECK(protocol_kind = 'CooperativeYieldRequest'),
        request_kind TEXT NOT NULL
            CHECK(request_kind IN ('manual_preemption', 'gpu_reservation')),
        requested_at TEXT NOT NULL CHECK(length(requested_at) > 0),
        requested_by TEXT NOT NULL CHECK(length(requested_by) > 0),
        note TEXT NOT NULL CHECK(length(note) BETWEEN 1 AND 1000),
        request_json BLOB NOT NULL CHECK(length(request_json) > 0),
        request_sha256 TEXT NOT NULL
            CHECK(length(request_sha256) = 64)
            CHECK(request_sha256 NOT GLOB '*[^0-9a-f]*'),
        resolved_spec_sha256 TEXT NOT NULL
            CHECK(length(resolved_spec_sha256) = 64)
            CHECK(resolved_spec_sha256 NOT GLOB '*[^0-9a-f]*'),
        project_revision_label TEXT NOT NULL
            CHECK(length(project_revision_label) BETWEEN 1 AND 256),
        git_commit TEXT NOT NULL
            CHECK(length(git_commit) IN (40, 64))
            CHECK(git_commit NOT GLOB '*[^0-9a-f]*'),
        run_id TEXT NOT NULL CHECK(length(run_id) BETWEEN 1 AND 256),
        prior_receipt_sha256 TEXT NOT NULL
            CHECK(length(prior_receipt_sha256) = 64)
            CHECK(prior_receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
        continuation_identity_sha256 TEXT NOT NULL
            CHECK(length(continuation_identity_sha256) = 64)
            CHECK(continuation_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
        UNIQUE(queue_item_id, segment),
        UNIQUE(
            queue_item_id, project_id, revision_id, segment, request_id,
            continuation_identity_sha256
        ),
        FOREIGN KEY(queue_item_id, project_id, revision_id, admission_kind)
            REFERENCES queue_items(id, project_id, revision_id, admission_kind)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE cooperative_yield_receipts (
        request_id TEXT PRIMARY KEY
            CHECK(length(request_id) BETWEEN 1 AND 256),
        queue_item_id INTEGER NOT NULL CHECK(queue_item_id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        segment INTEGER NOT NULL CHECK(segment > 0),
        bound_continuation_identity_sha256 TEXT NOT NULL
            CHECK(length(bound_continuation_identity_sha256) = 64)
            CHECK(bound_continuation_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
        protocol_api_version TEXT NOT NULL
            CHECK(protocol_api_version = 'experiment-queue/v1'),
        protocol_kind TEXT NOT NULL
            CHECK(protocol_kind = 'CooperativeYieldReceipt'),
        status TEXT NOT NULL CHECK(status IN ('ready', 'failed')),
        written_at TEXT NOT NULL CHECK(length(written_at) > 0),
        receipt_json BLOB NOT NULL CHECK(length(receipt_json) > 0),
        receipt_sha256 TEXT NOT NULL
            CHECK(length(receipt_sha256) = 64)
            CHECK(receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
        progress_json BLOB,
        progress_sha256 TEXT
            CHECK(progress_sha256 IS NULL OR (
                length(progress_sha256) = 64
                AND progress_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        checkpoint_artifacts_json BLOB,
        checkpoint_artifacts_sha256 TEXT
            CHECK(checkpoint_artifacts_sha256 IS NULL OR (
                length(checkpoint_artifacts_sha256) = 64
                AND checkpoint_artifacts_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        resume_context BLOB,
        resume_context_bytes INTEGER
            CHECK(resume_context_bytes IS NULL OR resume_context_bytes >= 0),
        resume_context_media_type TEXT,
        resume_context_sha256 TEXT
            CHECK(resume_context_sha256 IS NULL OR (
                length(resume_context_sha256) = 64
                AND resume_context_sha256 NOT GLOB '*[^0-9a-f]*'
            )),
        error TEXT CHECK(error IS NULL OR length(error) BETWEEN 1 AND 4096),
        CHECK((progress_json IS NULL) = (progress_sha256 IS NULL)),
        CHECK(
            (resume_context IS NULL)
                = (resume_context_bytes IS NULL)
            AND (resume_context IS NULL)
                = (resume_context_media_type IS NULL)
            AND (resume_context IS NULL)
                = (resume_context_sha256 IS NULL)
        ),
        CHECK(resume_context IS NULL OR length(resume_context) = resume_context_bytes),
        CHECK(
            (checkpoint_artifacts_json IS NULL)
                = (checkpoint_artifacts_sha256 IS NULL)
        ),
        CHECK(
            (status = 'ready'
                AND progress_json IS NOT NULL
                AND checkpoint_artifacts_json IS NOT NULL
                AND length(checkpoint_artifacts_json) > 0
                AND resume_context IS NOT NULL
                AND length(resume_context_media_type) > 0
                AND error IS NULL)
            OR
            (status = 'failed'
                AND checkpoint_artifacts_json IS NULL
                AND resume_context IS NULL
                AND error IS NOT NULL)
        ),
        FOREIGN KEY(
            queue_item_id, project_id, revision_id, segment, request_id,
            bound_continuation_identity_sha256
        ) REFERENCES cooperative_yield_requests(
            queue_item_id, project_id, revision_id, segment, request_id,
            continuation_identity_sha256
        )
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE dependencies (
        queue_item_id INTEGER NOT NULL CHECK(queue_item_id > 0),
        dependency_item_id INTEGER NOT NULL CHECK(dependency_item_id > 0),
        PRIMARY KEY(queue_item_id, dependency_item_id),
        CHECK(queue_item_id <> dependency_item_id),
        FOREIGN KEY(queue_item_id) REFERENCES queue_items(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY(dependency_item_id) REFERENCES queue_items(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
    """
    CREATE TABLE gpu_allowlist (
        uuid TEXT PRIMARY KEY CHECK(length(uuid) > 0),
        requested_identifier TEXT NOT NULL CHECK(length(requested_identifier) > 0),
        last_index TEXT NOT NULL CHECK(length(last_index) > 0),
        name TEXT NOT NULL CHECK(length(name) > 0),
        enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
        draining INTEGER NOT NULL CHECK(draining IN (0, 1)),
        updated_at TEXT NOT NULL CHECK(length(updated_at) > 0)
    ) STRICT
    """,
    """
    CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        created_at TEXT NOT NULL CHECK(length(created_at) > 0),
        actor TEXT NOT NULL CHECK(length(actor) > 0),
        event_type TEXT NOT NULL CHECK(length(event_type) > 0),
        queue_item_id INTEGER,
        payload_json TEXT NOT NULL CHECK(length(payload_json) > 0),
        scope TEXT NOT NULL CHECK(scope IN ('host', 'project')),
        project_id INTEGER,
        CHECK(
            (scope = 'host' AND project_id IS NULL AND queue_item_id IS NULL)
            OR
            (scope = 'project' AND project_id IS NOT NULL)
        ),
        FOREIGN KEY(project_id) REFERENCES projects(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY(queue_item_id, project_id)
            REFERENCES queue_items(id, project_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE gpu_reservations (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        gpu_uuid TEXT NOT NULL CHECK(length(gpu_uuid) > 0),
        queue_item_id INTEGER,
        status TEXT NOT NULL
            CHECK(status IN ('pending', 'active', 'expired', 'released', 'failed')),
        requested_at TEXT NOT NULL CHECK(length(requested_at) > 0),
        requested_by TEXT NOT NULL CHECK(length(requested_by) > 0),
        note TEXT NOT NULL CHECK(length(note) BETWEEN 1 AND 200),
        duration_hours INTEGER NOT NULL CHECK(duration_hours BETWEEN 1 AND 24),
        starts_at TEXT,
        expires_at TEXT,
        released_at TEXT,
        released_by TEXT,
        state_detail TEXT,
        FOREIGN KEY(queue_item_id) REFERENCES queue_items(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE job_artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        queue_item_id INTEGER NOT NULL CHECK(queue_item_id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        segment INTEGER NOT NULL CHECK(segment > 0),
        evidence_kind TEXT NOT NULL
            CHECK(evidence_kind IN ('declared-v1', 'legacy-v4')),
        artifact_name TEXT NOT NULL CHECK(length(artifact_name) > 0),
        artifact_type TEXT NOT NULL CHECK(artifact_type IN ('file', 'directory')),
        root_name TEXT,
        root_access TEXT CHECK(root_access IS NULL OR root_access = 'readWrite'),
        relative_path TEXT,
        absolute_path TEXT NOT NULL
            CHECK(length(absolute_path) > 1 AND substr(absolute_path, 1, 1) = '/'),
        size_bytes INTEGER CHECK(size_bytes IS NULL OR size_bytes >= 0),
        sha256 TEXT CHECK(sha256 IS NULL OR (
            length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'
        )),
        recorded_at TEXT NOT NULL CHECK(length(recorded_at) > 0),
        metadata_json BLOB,
        CHECK(
            (evidence_kind = 'declared-v1'
                AND root_name IS NOT NULL AND length(root_name) > 0
                AND root_access = 'readWrite'
                AND relative_path IS NOT NULL AND length(relative_path) > 0
                AND substr(relative_path, 1, 1) <> '/'
                AND instr(relative_path, '\\') = 0
                AND relative_path <> '..'
                AND relative_path NOT LIKE '../%'
                AND relative_path NOT LIKE '%/../%'
                AND relative_path NOT LIKE '%/..')
            OR
            (evidence_kind = 'legacy-v4'
                AND root_name IS NULL
                AND root_access IS NULL
                AND relative_path IS NULL)
        ),
        FOREIGN KEY(queue_item_id, project_id, revision_id)
            REFERENCES queue_items(id, project_id, revision_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY(project_id, revision_id, root_name)
            REFERENCES project_artifact_roots(project_id, revision_id, mount_name)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE migration_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        source_schema_version INTEGER NOT NULL
            CHECK(source_schema_version BETWEEN 1 AND 4),
        source_state_path TEXT NOT NULL
            CHECK(length(source_state_path) > 1 AND substr(source_state_path, 1, 1) = '/'),
        source_database_path TEXT NOT NULL
            CHECK(length(source_database_path) > 1 AND substr(source_database_path, 1, 1) = '/'),
        source_database_sha256 TEXT NOT NULL
            CHECK(length(source_database_sha256) = 64)
            CHECK(source_database_sha256 NOT GLOB '*[^0-9a-f]*'),
        source_database_size_bytes INTEGER NOT NULL
            CHECK(source_database_size_bytes > 0),
        source_database_mtime_ns INTEGER NOT NULL
            CHECK(source_database_mtime_ns > 0),
        source_state_identity_json BLOB NOT NULL
            CHECK(length(source_state_identity_json) > 0),
        source_state_identity_sha256 TEXT NOT NULL
            CHECK(length(source_state_identity_sha256) = 64)
            CHECK(source_state_identity_sha256 NOT GLOB '*[^0-9a-f]*'),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        importer_package_version TEXT NOT NULL
            CHECK(length(importer_package_version) > 0),
        imported_at TEXT NOT NULL CHECK(length(imported_at) > 0),
        imported_by TEXT NOT NULL CHECK(length(imported_by) > 0),
        UNIQUE(id, project_id, revision_id),
        FOREIGN KEY(project_id, revision_id)
            REFERENCES project_revisions(project_id, id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE migration_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
        migration_source_id INTEGER NOT NULL CHECK(migration_source_id > 0),
        project_id INTEGER NOT NULL CHECK(project_id > 0),
        revision_id INTEGER NOT NULL CHECK(revision_id > 0),
        protocol_api_version TEXT NOT NULL CHECK(length(protocol_api_version) > 0),
        protocol_kind TEXT NOT NULL CHECK(protocol_kind = 'QueueMigrationReceipt'),
        result TEXT NOT NULL CHECK(result = 'succeeded'),
        receipt_json BLOB NOT NULL CHECK(length(receipt_json) > 0),
        receipt_sha256 TEXT NOT NULL
            CHECK(length(receipt_sha256) = 64)
            CHECK(receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
        started_at TEXT NOT NULL CHECK(length(started_at) > 0),
        finished_at TEXT NOT NULL CHECK(length(finished_at) > 0),
        actor TEXT NOT NULL CHECK(length(actor) > 0),
        UNIQUE(migration_source_id),
        FOREIGN KEY(migration_source_id, project_id, revision_id)
            REFERENCES migration_sources(id, project_id, revision_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT
    """,
    """
    CREATE TABLE legacy_metadata (
        migration_source_id INTEGER NOT NULL CHECK(migration_source_id > 0),
        source_key TEXT NOT NULL,
        source_value TEXT NOT NULL,
        PRIMARY KEY(migration_source_id, source_key),
        FOREIGN KEY(migration_source_id) REFERENCES migration_sources(id)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    ) STRICT, WITHOUT ROWID
    """,
)


_INDEX_STATEMENTS: Final[tuple[str, ...]] = (
    "CREATE INDEX queue_items_state_order "
    "ON queue_items(state, priority DESC, resume_front DESC, id ASC)",
    "CREATE INDEX queue_items_project_state ON queue_items(project_id, state, id)",
    "CREATE INDEX queue_items_runtime_gpu_lease "
    "ON queue_items(runtime_gpu_lease_held, assigned_gpu_uuid, id)",
    "CREATE INDEX cooperative_yield_requests_item_order "
    "ON cooperative_yield_requests(queue_item_id, segment)",
    "CREATE INDEX events_scope_order ON events(scope, project_id, id)",
    "CREATE INDEX job_artifacts_item_segment "
    "ON job_artifacts(queue_item_id, segment, id)",
    "CREATE INDEX migration_sources_project "
    "ON migration_sources(project_id, revision_id, id)",
    "CREATE UNIQUE INDEX gpu_reservations_open_gpu ON gpu_reservations(gpu_uuid) "
    "WHERE status IN ('pending', 'active')",
    "CREATE INDEX gpu_reservations_status_expiry ON gpu_reservations(status, expires_at)",
)


_TRIGGER_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TRIGGER metadata_database_identity_immutable_update
    BEFORE UPDATE ON metadata
    WHEN OLD.key IN (
        'schema_version', 'schema_identity', 'schema_ddl_sha256',
        'database_instance_id'
    ) OR NEW.key IN (
        'schema_version', 'schema_identity', 'schema_ddl_sha256',
        'database_instance_id'
    )
    BEGIN SELECT RAISE(ABORT, 'database identity metadata is immutable'); END
    """,
    """
    CREATE TRIGGER metadata_database_identity_immutable_delete
    BEFORE DELETE ON metadata
    WHEN OLD.key IN (
        'schema_version', 'schema_identity', 'schema_ddl_sha256',
        'database_instance_id'
    )
    BEGIN SELECT RAISE(ABORT, 'database identity metadata is immutable'); END
    """,
    """
    CREATE TRIGGER projects_identity_immutable
    BEFORE UPDATE OF id, project_key ON projects
    BEGIN SELECT RAISE(ABORT, 'project identity is immutable'); END
    """,
    """
    CREATE TRIGGER projects_no_delete
    BEFORE DELETE ON projects
    BEGIN SELECT RAISE(ABORT, 'projects are archived, never deleted'); END
    """,
    """
    CREATE TRIGGER projects_archival_permanent
    BEFORE UPDATE OF lifecycle ON projects
    WHEN OLD.lifecycle = 'archived' AND NEW.lifecycle <> 'archived'
    BEGIN SELECT RAISE(ABORT, 'archived projects cannot be restored in v1'); END
    """,
    """
    CREATE TRIGGER projects_archive_from_paused
    BEFORE UPDATE OF lifecycle ON projects
    WHEN NEW.lifecycle = 'archived' AND OLD.lifecycle <> 'paused'
    BEGIN SELECT RAISE(ABORT, 'project must be paused before archival'); END
    """,
    """
    CREATE TRIGGER projects_archive_requires_cleanup
    BEFORE UPDATE OF lifecycle ON projects
    WHEN NEW.lifecycle = 'archived' AND (
        EXISTS(
            SELECT 1 FROM queue_items
            WHERE project_id = OLD.id
              AND state IN ('queued', 'held', 'blocked', 'starting', 'running',
                            'yielding', 'terminating', 'force_killing')
        )
        OR EXISTS(
            SELECT 1 FROM queue_items
            WHERE project_id = OLD.id
              AND (((worktree_cleanup_error IS NOT NULL
                     OR (worktree_path IS NOT NULL
                         AND worktree_removed_at IS NULL))
                    AND runtime_worktree_removed_at IS NULL)
                   OR runtime_worktree_cleanup_error IS NOT NULL
                   OR (runtime_worktree_path IS NOT NULL
                       AND runtime_worktree_removed_at IS NULL))
        )
    )
    BEGIN SELECT RAISE(ABORT, 'project has active work or incomplete checkout cleanup'); END
    """,
    """
    CREATE TRIGGER projects_revision_activation_moves_forward
    BEFORE UPDATE OF current_revision_id, current_revision_sequence ON projects
    WHEN NEW.lifecycle = 'archived'
         OR NEW.current_revision_sequence <= OLD.current_revision_sequence
    BEGIN SELECT RAISE(ABORT, 'current Project revision must move forward and cannot change after archive'); END
    """,
    """
    CREATE TRIGGER project_revisions_immutable_update
    BEFORE UPDATE ON project_revisions
    BEGIN SELECT RAISE(ABORT, 'project revisions are append-only'); END
    """,
    """
    CREATE TRIGGER project_revisions_immutable_delete
    BEFORE DELETE ON project_revisions
    BEGIN SELECT RAISE(ABORT, 'project revisions are append-only'); END
    """,
    """
    CREATE TRIGGER project_mounts_immutable_update
    BEFORE UPDATE ON project_mounts
    BEGIN SELECT RAISE(ABORT, 'revision mounts are immutable'); END
    """,
    """
    CREATE TRIGGER project_mounts_immutable_delete
    BEFORE DELETE ON project_mounts
    BEGIN SELECT RAISE(ABORT, 'revision mounts are immutable'); END
    """,
    """
    CREATE TRIGGER project_artifact_roots_immutable_update
    BEFORE UPDATE ON project_artifact_roots
    BEGIN SELECT RAISE(ABORT, 'revision artifact roots are immutable'); END
    """,
    """
    CREATE TRIGGER project_artifact_roots_immutable_delete
    BEFORE DELETE ON project_artifact_roots
    BEGIN SELECT RAISE(ABORT, 'revision artifact roots are immutable'); END
    """,
    """
    CREATE TRIGGER project_environments_immutable_update
    BEFORE UPDATE ON project_environments
    BEGIN SELECT RAISE(ABORT, 'revision environments are immutable'); END
    """,
    """
    CREATE TRIGGER project_environments_immutable_delete
    BEFORE DELETE ON project_environments
    BEGIN SELECT RAISE(ABORT, 'revision environments are immutable'); END
    """,
    """
    CREATE TRIGGER admission_snapshots_immutable_update
    BEFORE UPDATE ON admission_snapshots
    BEGIN SELECT RAISE(ABORT, 'admission snapshots are immutable'); END
    """,
    """
    CREATE TRIGGER admission_snapshots_immutable_delete
    BEFORE DELETE ON admission_snapshots
    BEGIN SELECT RAISE(ABORT, 'admission snapshots are immutable'); END
    """,
    """
    CREATE TRIGGER queue_items_identity_immutable
    BEFORE UPDATE OF id, project_id, revision_id, admission_kind, snapshot_id,
        job_id, experiment_id, attempt, card_path, card_sha256, command_text,
        runner_name, git_commit, added_at, added_by, git_ref, worktree_path,
        worktree_created_at, worktree_removed_at, worktree_cleanup_error
        ON queue_items
    BEGIN SELECT RAISE(ABORT, 'admitted queue identity and evidence are immutable'); END
    """,
    """
    CREATE TRIGGER queue_items_legacy_requires_legacy_revision
    BEFORE INSERT ON queue_items
    WHEN NEW.admission_kind = 'LegacyMarkdownCard/v0'
         AND EXISTS(
             SELECT 1 FROM project_revisions
             WHERE id = NEW.revision_id
               AND project_id = NEW.project_id
               AND revision_kind <> 'legacy-v4'
         )
    BEGIN SELECT RAISE(ABORT, 'legacy admission requires an explicit legacy-v4 revision'); END
    """,
    """
    CREATE TRIGGER queue_items_no_delete
    BEFORE DELETE ON queue_items
    BEGIN SELECT RAISE(ABORT, 'queue history is never deleted'); END
    """,
    """
    CREATE TRIGGER cooperative_yield_requests_immutable_update
    BEFORE UPDATE ON cooperative_yield_requests
    BEGIN SELECT RAISE(ABORT, 'cooperative yield requests are append-only evidence'); END
    """,
    """
    CREATE TRIGGER cooperative_yield_requests_immutable_delete
    BEFORE DELETE ON cooperative_yield_requests
    BEGIN SELECT RAISE(ABORT, 'cooperative yield requests are append-only evidence'); END
    """,
    """
    CREATE TRIGGER cooperative_yield_receipts_immutable_update
    BEFORE UPDATE ON cooperative_yield_receipts
    BEGIN SELECT RAISE(ABORT, 'cooperative yield receipts are append-only evidence'); END
    """,
    """
    CREATE TRIGGER cooperative_yield_receipts_immutable_delete
    BEFORE DELETE ON cooperative_yield_receipts
    BEGIN SELECT RAISE(ABORT, 'cooperative yield receipts are append-only evidence'); END
    """,
    """
    CREATE TRIGGER dependencies_immutable_update
    BEFORE UPDATE ON dependencies
    BEGIN SELECT RAISE(ABORT, 'admitted dependencies are immutable'); END
    """,
    """
    CREATE TRIGGER dependencies_immutable_delete
    BEFORE DELETE ON dependencies
    BEGIN SELECT RAISE(ABORT, 'admitted dependencies are immutable'); END
    """,
    """
    CREATE TRIGGER events_immutable_update
    BEFORE UPDATE ON events
    BEGIN SELECT RAISE(ABORT, 'events are append-only'); END
    """,
    """
    CREATE TRIGGER events_immutable_delete
    BEFORE DELETE ON events
    BEGIN SELECT RAISE(ABORT, 'events are append-only'); END
    """,
    """
    CREATE TRIGGER job_artifacts_immutable_update
    BEFORE UPDATE ON job_artifacts
    BEGIN SELECT RAISE(ABORT, 'job artifacts are append-only evidence'); END
    """,
    """
    CREATE TRIGGER job_artifacts_immutable_delete
    BEFORE DELETE ON job_artifacts
    BEGIN SELECT RAISE(ABORT, 'job artifacts are append-only evidence'); END
    """,
    """
    CREATE TRIGGER migration_sources_immutable_update
    BEFORE UPDATE ON migration_sources
    BEGIN SELECT RAISE(ABORT, 'migration source evidence is immutable'); END
    """,
    """
    CREATE TRIGGER migration_sources_immutable_delete
    BEFORE DELETE ON migration_sources
    BEGIN SELECT RAISE(ABORT, 'migration source evidence is immutable'); END
    """,
    """
    CREATE TRIGGER migration_receipts_immutable_update
    BEFORE UPDATE ON migration_receipts
    BEGIN SELECT RAISE(ABORT, 'migration receipts are immutable'); END
    """,
    """
    CREATE TRIGGER migration_receipts_immutable_delete
    BEFORE DELETE ON migration_receipts
    BEGIN SELECT RAISE(ABORT, 'migration receipts are immutable'); END
    """,
    """
    CREATE TRIGGER legacy_metadata_immutable_update
    BEFORE UPDATE ON legacy_metadata
    BEGIN SELECT RAISE(ABORT, 'legacy metadata evidence is immutable'); END
    """,
    """
    CREATE TRIGGER legacy_metadata_immutable_delete
    BEFORE DELETE ON legacy_metadata
    BEGIN SELECT RAISE(ABORT, 'legacy metadata evidence is immutable'); END
    """,
)


_SCHEMA_STATEMENTS: Final = _TABLE_STATEMENTS + _INDEX_STATEMENTS + _TRIGGER_STATEMENTS
SCHEMA_DDL_SHA256: Final = hashlib.sha256(
    "\n".join(statement.strip() for statement in _SCHEMA_STATEMENTS).encode("utf-8")
).hexdigest()


def _normalized_schema_sql(value: object) -> str | None:
    """Normalize insignificant whitespace in SQLite's stored schema SQL."""

    if value is None:
        return None
    if type(value) is not str:
        raise V5DatabaseError(
            "schema-v5 schema object has no readable SQL definition"
        )
    return " ".join(value.split())


def _schema_objects(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], tuple[str, str | None]]:
    """Return the complete ``sqlite_schema`` surface.

    The expected surface includes SQLite's deterministic implicit constraint
    indexes and ``sqlite_sequence``. Checking them as well as application-owned
    objects prevents writable-schema tampering from hiding behind SQLite's
    reserved ``sqlite_`` prefix.
    """

    objects: dict[tuple[str, str], tuple[str, str | None]] = {}
    for object_type, name, table_name, sql in connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name"
    ):
        if type(object_type) is not str or type(name) is not str:
            raise V5DatabaseError(
                "schema-v5 sqlite_schema contains a malformed object identity"
            )
        if type(table_name) is not str:
            raise V5DatabaseError(
                f"schema-v5 {object_type} {name!r} has a malformed owner"
            )
        objects[(object_type, name)] = (
            table_name,
            _normalized_schema_sql(sql),
        )
    return objects


def _build_expected_schema_objects(
) -> dict[tuple[str, str], tuple[str, str | None]]:
    """Materialize immutable expected SQLite DDL using this SQLite runtime."""

    connection = sqlite3.connect(":memory:")
    try:
        for statement in _SCHEMA_STATEMENTS:
            connection.execute(statement)
        return _schema_objects(connection)
    finally:
        connection.close()


# Constructed once from the immutable statement tuple. Building expected SQL
# through SQLite avoids treating harmless formatting performed by SQLite itself
# as schema drift while still authenticating every application-owned object.
_EXPECTED_SCHEMA_OBJECTS: Final = _build_expected_schema_objects()

_REQUIRED_TABLES: Final = frozenset(
    {
        "metadata",
        "projects",
        "project_revisions",
        "project_mounts",
        "project_artifact_roots",
        "project_environments",
        "project_runtime_state",
        "admission_snapshots",
        "queue_items",
        "cooperative_yield_requests",
        "cooperative_yield_receipts",
        "dependencies",
        "gpu_allowlist",
        "events",
        "gpu_reservations",
        "job_artifacts",
        "migration_sources",
        "migration_receipts",
        "legacy_metadata",
    }
)

V4_QUEUE_ITEM_COLUMNS: Final = frozenset(
    {
        "id", "experiment_id", "attempt", "state", "priority", "card_path",
        "card_sha256", "command_text", "runner_name", "git_commit", "added_at",
        "added_by", "state_detail", "assigned_gpu_uuid", "assigned_gpu_index",
        "pid", "pgid", "proc_start_ticks", "started_at", "finished_at",
        "return_code", "terminate_requested_at", "terminate_reason",
        "termination_stage", "termination_signal_epoch", "contention_detected",
        "repo_drift_detected", "runner_run_dir", "runner_manifest_path",
        "rsync_pull_command", "preemptible", "segment", "resume_front",
        "yield_requested_at", "yield_requested_by", "yield_request_id",
        "yield_note", "yield_duration_hours", "continuation_checkpoint",
        "continuation_checkpoint_sha256", "continuation_checkpoint_metadata",
        "continuation_checkpoint_metadata_sha256", "continuation_step",
        "continuation_wandb_id", "git_ref", "worktree_path",
        "worktree_created_at", "worktree_removed_at", "worktree_cleanup_error",
    }
)


def _database_uri(path: Path, mode: str, *, immutable: bool = False) -> str:
    """Return an encoded SQLite URI that never falls back to creating a file."""

    immutable_option = "&immutable=1" if immutable else ""
    return f"{path.as_uri()}?mode={mode}{immutable_option}"


def _metadata_value(connection: sqlite3.Connection, key: str) -> str | None:
    """Read one metadata value without assuming the row factory or schema trust."""

    try:
        row = connection.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
    except sqlite3.Error as exc:
        raise V5SchemaVersionError(
            "existing database has no readable metadata schema; it will not be "
            "created, repaired, or migrated at startup"
        ) from exc
    if row is None or type(row[0]) is not str:
        return None
    return row[0]


def _validate_instance_id(value: object) -> str:
    """Validate the canonical random identity of one Database/v5 instance."""

    if type(value) is not str or len(value) != 36:
        raise V5DatabaseError(
            "schema-v5 database_instance_id must be a canonical lowercase UUIDv4"
        )
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise V5DatabaseError(
            "schema-v5 database_instance_id must be a canonical lowercase UUIDv4"
        ) from exc
    if (
        str(parsed) != value
        or parsed.version != 4
        or parsed.variant != uuid.RFC_4122
    ):
        raise V5DatabaseError(
            "schema-v5 database_instance_id must be a canonical lowercase UUIDv4"
        )
    return value


def _inspect_version_read_only(database_path: Path) -> None:
    """Reject every existing non-v5 file before a mutable connection is opened."""

    if database_path.is_symlink() or not database_path.is_file():
        raise V5SchemaVersionError(
            f"existing database path {database_path} must be a regular, non-symlink file"
        )
    try:
        connection = sqlite3.connect(
            _database_uri(database_path, "ro", immutable=True),
            uri=True,
            timeout=30.0,
        )
    except sqlite3.Error as exc:
        raise V5SchemaVersionError(
            f"could not inspect existing database {database_path} read-only: {exc}"
        ) from exc
    try:
        version = _metadata_value(connection, "schema_version")
    finally:
        connection.close()
    if version == str(SCHEMA_VERSION):
        return
    if version in {"1", "2", "3", "4"}:
        raise V5SchemaVersionError(
            f"database {database_path} is schema v{version}; V5QueueStore never "
            "migrates legacy state at startup. Run the explicit offline importer "
            "against a copy and retain the source unchanged for rollback."
        )
    rendered = "missing" if version is None else repr(version)
    raise V5SchemaVersionError(
        f"database {database_path} has unsupported schema version {rendered}; "
        "expected exactly '5' and refused to mutate the file"
    )


def _validate_v5_structure(connection: sqlite3.Connection) -> str:
    """Verify the schema and return its authenticated immutable instance ID."""

    version = _metadata_value(connection, "schema_version")
    identity = _metadata_value(connection, "schema_identity")
    ddl_digest = _metadata_value(connection, "schema_ddl_sha256")
    if version != str(SCHEMA_VERSION):
        raise V5SchemaVersionError(
            f"database changed while opening: expected schema '5', got {version!r}"
        )
    if identity != SCHEMA_IDENTITY or ddl_digest != SCHEMA_DDL_SHA256:
        raise V5DatabaseError(
            "schema-v5 metadata identity or DDL digest does not match this build; "
            "refusing startup repair or reinterpretation"
        )
    instance_id = _validate_instance_id(
        _metadata_value(connection, DATABASE_INSTANCE_ID_KEY)
    )
    actual_schema_objects = _schema_objects(connection)
    if actual_schema_objects != _EXPECTED_SCHEMA_OBJECTS:
        expected_keys = set(_EXPECTED_SCHEMA_OBJECTS)
        actual_keys = set(actual_schema_objects)
        missing = sorted(expected_keys - actual_keys)
        unexpected = sorted(actual_keys - expected_keys)
        changed = sorted(
            key
            for key in expected_keys & actual_keys
            if _EXPECTED_SCHEMA_OBJECTS[key] != actual_schema_objects[key]
        )
        differences = [
            *(f"missing {kind} {name!r}" for kind, name in missing),
            *(f"unexpected {kind} {name!r}" for kind, name in unexpected),
            *(f"changed {kind} {name!r}" for kind, name in changed),
        ]
        raise V5DatabaseError(
            "schema-v5 sqlite_schema does not exactly match this "
            "build: " + "; ".join(differences)
        )
    table_rows = list(connection.execute("PRAGMA table_list"))
    tables = {str(row[1]) for row in table_rows if str(row[2]) == "table"}
    missing = sorted(_REQUIRED_TABLES - tables)
    if missing:
        raise V5DatabaseError(
            f"schema-v5 database is missing required tables: {', '.join(missing)}"
        )
    strict_by_name = {
        str(row[1]): int(row[5]) for row in table_rows if str(row[2]) == "table"
    }
    non_strict = sorted(
        table for table in _REQUIRED_TABLES if strict_by_name.get(table) != 1
    )
    if non_strict:
        raise V5DatabaseError(
            f"schema-v5 required tables are not STRICT: {', '.join(non_strict)}"
        )
    queue_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(queue_items)")
    }
    missing_v4 = sorted(V4_QUEUE_ITEM_COLUMNS - queue_columns)
    if missing_v4:
        raise V5DatabaseError(
            "schema-v5 queue_items does not preserve v4 columns: "
            + ", ".join(missing_v4)
        )
    violations = list(connection.execute("PRAGMA foreign_key_check"))
    if violations:
        first = violations[0]
        raise V5DatabaseError(
            "schema-v5 foreign-key integrity check failed at "
            f"table {first[0]!r}, row {first[1]!r}"
        )
    return instance_id


def _validate_existing_read_only(database_path: Path) -> str:
    """Validate base shape and WAL state, returning one stable instance ID."""

    try:
        immutable = sqlite3.connect(
            _database_uri(database_path, "ro", immutable=True),
            uri=True,
            timeout=30.0,
        )
        try:
            base_instance_id = _validate_v5_structure(immutable)
        finally:
            immutable.close()
        current = sqlite3.connect(
            _database_uri(database_path, "ro"), uri=True, timeout=30.0
        )
        try:
            current_instance_id = _validate_v5_structure(current)
        finally:
            current.close()
        if current_instance_id != base_instance_id:
            raise V5DatabaseError(
                "schema-v5 database instance identity differs between the base "
                "database and accepted WAL state"
            )
        _validate_database_permissions(database_path)
        return current_instance_id
    except V5DatabaseError:
        raise
    except sqlite3.Error as exc:
        raise V5DatabaseError(
            f"could not validate accepted schema-v5 database {database_path}: {exc}"
        ) from exc


def _candidate_sidecars(database_path: Path) -> tuple[Path, ...]:
    """Return every SQLite sidecar name scoped to one private candidate."""

    return tuple(
        database_path.with_name(database_path.name + suffix)
        for suffix in ("-journal", "-wal", "-shm")
    )


def _validate_database_permissions(database_path: Path) -> None:
    details = database_path.stat(follow_symlinks=False)
    mode = stat.S_IMODE(details.st_mode)
    if details.st_uid != os.geteuid() or mode != 0o600:
        raise V5DatabaseError(
            f"schema-v5 database {database_path} must be owned by uid "
            f"{os.geteuid()} with mode 0600, got uid {details.st_uid} and "
            f"mode {mode:04o}"
        )


def _validate_state_directory(state_dir: Path) -> SecurePathBoundary:
    """Require a stable POSIX owner boundary without forbidding shared reads."""

    if state_dir.is_symlink() or not state_dir.is_dir():
        raise V5DatabaseError(
            f"schema-v5 state directory {state_dir} must be a non-symlink directory"
        )
    details = state_dir.stat(follow_symlinks=False)
    mode = stat.S_IMODE(details.st_mode)
    if details.st_uid != os.geteuid():
        raise V5DatabaseError(
            f"schema-v5 state directory {state_dir} must be owned by uid "
            f"{os.geteuid()}, got uid {details.st_uid}"
        )
    if mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise V5DatabaseError(
            f"schema-v5 state directory {state_dir} must not be group/world "
            f"writable, got mode {mode:04o}; remove group/other write permission"
        )
    return _capture_state_boundary(state_dir)


def _capture_state_boundary(selected_path: Path) -> SecurePathBoundary:
    """Capture the trusted ancestor chain for an existing or planned state leaf."""

    try:
        return capture_secure_path_boundary(
            selected_path,
            label="schema-v5 state directory",
        )
    except PathBoundaryError as exc:
        raise V5DatabaseError(str(exc)) from exc


def _revalidate_state_boundary(boundary: SecurePathBoundary) -> None:
    """Translate ancestor substitution into the Database/v5 error boundary."""

    try:
        revalidate_secure_path_boundary(boundary)
    except PathBoundaryError as exc:
        raise V5DatabaseError(str(exc)) from exc


def _ensure_state_directory(state_dir: Path) -> bool:
    """Create only the selected leaf as 0700, or validate an existing leaf."""

    first_absent = state_dir
    while not first_absent.parent.exists():
        first_absent = first_absent.parent
    _capture_state_boundary(first_absent)
    try:
        state_dir.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise V5DatabaseError(
            f"could not create parent directories for schema-v5 state {state_dir}: {exc}"
        ) from exc
    _capture_state_boundary(state_dir)
    created = False
    try:
        os.mkdir(state_dir, 0o700)
        created = True
        os.chmod(state_dir, 0o700, follow_symlinks=False)
    except FileExistsError:
        pass
    except OSError as exc:
        raise V5DatabaseError(
            f"could not create schema-v5 state directory {state_dir}: {exc}"
        ) from exc
    _validate_state_directory(state_dir)
    return created


def _cleanup_fresh_candidate(database_path: Path) -> None:
    """Remove only an unpublished UUID-named candidate and its SQLite sidecars."""

    failures: list[str] = []
    for path in (*_candidate_sidecars(database_path), database_path):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            failures.append(f"{path}: {exc}")
    if failures:
        raise V5DatabaseError(
            "could not clean unpublished schema-v5 candidate files: "
            + "; ".join(failures)
        )


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _build_fresh_candidate(database_path: Path, *, instance_id: str) -> None:
    """Build and durably validate one private database before publication."""

    connection = sqlite3.connect(
        _database_uri(database_path, "rw"), uri=True, timeout=30.0
    )
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")
        for statement in _SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            (
                ("schema_version", str(SCHEMA_VERSION)),
                ("schema_identity", SCHEMA_IDENTITY),
                ("schema_ddl_sha256", SCHEMA_DDL_SHA256),
                (DATABASE_INSTANCE_ID_KEY, instance_id),
                ("dispatch_paused", "0"),
                ("pause_reason", ""),
                ("consecutive_failures", "0"),
            ),
        )
        connection.commit()
        created_instance_id = _validate_v5_structure(connection)
        if created_instance_id != instance_id:
            raise V5DatabaseError(
                "fresh schema-v5 database did not retain its generated instance "
                "identity"
            )
        journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise V5DatabaseError(
                f"could not enable WAL journaling, SQLite returned {journal_mode!r}"
            )
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    remaining_sidecars = [
        path for path in _candidate_sidecars(database_path) if path.exists() or path.is_symlink()
    ]
    if remaining_sidecars:
        raise V5DatabaseError(
            "fresh schema-v5 candidate retained SQLite sidecars after close: "
            + ", ".join(str(path) for path in remaining_sidecars)
        )
    _fsync_file(database_path)


class V5QueueStore:
    """Minimal fresh-v5 SQLite owner with fail-closed version boundaries.

    Constructing the object is side-effect free. :meth:`initialize` creates a
    complete v5 database only when ``queue.sqlite3`` does not exist, or verifies
    an existing v5 database without applying DDL. :meth:`connect` repeats the
    read-only version/shape check before opening the file read-write.
    """

    def __init__(self, state_dir: Path):
        candidate = Path(state_dir)
        if not candidate.is_absolute():
            raise V5DatabaseError(
                f"schema-v5 state directory must be absolute, got {candidate}"
            )
        if candidate.is_symlink():
            raise V5DatabaseError(
                f"schema-v5 state directory {candidate} must not be a symlink"
            )
        self.state_dir = candidate.resolve()
        self.database_path = self.state_dir / DATABASE_FILENAME
        self._expected_instance_id: str | None = None

    def _bind_instance_identity(self, instance_id: str) -> str:
        """Bind this store object to one database even if its path is replaced."""

        expected = self._expected_instance_id
        if expected is None:
            self._expected_instance_id = instance_id
        elif instance_id != expected:
            raise V5DatabaseError(
                f"schema-v5 database instance at {self.database_path} changed "
                f"from {expected} to {instance_id}; construct a new store only "
                "after intentionally selecting the replacement state"
            )
        return instance_id

    def instance_identity(self) -> str:
        """Return the immutable UUIDv4 identity after exact read-only validation."""

        if not self.database_path.exists():
            raise V5DatabaseError(
                f"schema-v5 database {self.database_path} does not exist; call "
                "initialize() explicitly before reading its instance identity"
            )
        boundary = _validate_state_directory(self.state_dir)
        _inspect_version_read_only(self.database_path)
        instance_id = _validate_existing_read_only(self.database_path)
        _revalidate_state_boundary(boundary)
        return self._bind_instance_identity(instance_id)

    def initialize(self) -> None:
        """Create fresh v5 state or validate an existing exact-v5 database."""

        if self.database_path.exists():
            boundary = _validate_state_directory(self.state_dir)
            _inspect_version_read_only(self.database_path)
            instance_id = _validate_existing_read_only(self.database_path)
            try:
                # An initializer racing a fresh publisher must not return based
                # only on a visible, not-yet-durable hard link.
                _fsync_directory(self.state_dir)
            except OSError as exc:
                raise V5DatabaseError(
                    f"could not synchronize existing schema-v5 state directory "
                    f"{self.state_dir}: {exc}"
                ) from exc
            _revalidate_state_boundary(boundary)
            self._bind_instance_identity(instance_id)
            return
        state_dir_created = _ensure_state_directory(self.state_dir)
        boundary = _validate_state_directory(self.state_dir)
        try:
            _fsync_directory(self.state_dir)
            if state_dir_created:
                _fsync_directory(self.state_dir.parent)
        except OSError as exc:
            raise V5DatabaseError(
                f"could not synchronize schema-v5 state directory {self.state_dir}: {exc}"
            ) from exc

        instance_id = str(uuid.uuid4())
        candidate = self.state_dir / (
            f".{DATABASE_FILENAME}.{instance_id}.candidate"
        )
        try:
            descriptor = os.open(
                candidate,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except OSError as exc:
            raise V5DatabaseError(
                f"could not reserve private schema-v5 candidate {candidate}: {exc}"
            ) from exc
        else:
            try:
                os.fchmod(descriptor, 0o600)
            except OSError as exc:
                try:
                    _cleanup_fresh_candidate(candidate)
                finally:
                    raise V5DatabaseError(
                        f"could not secure private schema-v5 candidate {candidate}: {exc}"
                    ) from exc
            finally:
                os.close(descriptor)

        published = False
        publication_durable = False
        try:
            _build_fresh_candidate(candidate, instance_id=instance_id)
            # Make the fully fsynced candidate's directory entry durable before
            # adding a final link. It remains the recoverable name if publication
            # visibility cannot subsequently be synchronized.
            _fsync_directory(self.state_dir)
            try:
                os.link(candidate, self.database_path)
                published = True
            except FileExistsError:
                # Another fully built instance won atomic no-clobber publication.
                # Discard only our private candidate, then authenticate the winner.
                _cleanup_fresh_candidate(candidate)
                _fsync_directory(self.state_dir)
                _inspect_version_read_only(self.database_path)
                winner_instance_id = _validate_existing_read_only(
                    self.database_path
                )
                _revalidate_state_boundary(boundary)
                self._bind_instance_identity(winner_instance_id)
                return
            _fsync_directory(self.state_dir)
            publication_durable = True
            _cleanup_fresh_candidate(candidate)
            _fsync_directory(self.state_dir)
            _revalidate_state_boundary(boundary)
            self._bind_instance_identity(instance_id)
        except BaseException as exc:
            same_published_inode = False
            if candidate.exists() and self.database_path.exists():
                try:
                    same_published_inode = os.path.samefile(
                        candidate, self.database_path
                    )
                except OSError:
                    pass
            if (published or same_published_inode) and not publication_durable:
                # The pre-link directory fsync made the candidate durable. Never
                # unlink that fallback while the final name's durability is
                # unknown, including when another initializer already observed it.
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                detail = str(exc) or type(exc).__name__
                raise _FreshPublicationIndeterminate(
                    f"fresh schema-v5 publication durability is indeterminate: "
                    f"{detail}; preserve both {candidate} and "
                    f"{self.database_path}, which name the same complete database, "
                    "and inspect the state directory before retrying"
                ) from exc
            cleanup_error: V5DatabaseError | None = None
            if candidate.exists() or candidate.is_symlink() or any(
                path.exists() or path.is_symlink()
                for path in _candidate_sidecars(candidate)
            ):
                try:
                    _cleanup_fresh_candidate(candidate)
                except V5DatabaseError as candidate_error:
                    cleanup_error = candidate_error
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            detail = str(exc) or type(exc).__name__
            if published:
                detail = (
                    f"{detail}; a complete database is visible at {self.database_path}, "
                    "but publication durability or candidate-name cleanup was not "
                    "confirmed. Retry initialize() to authenticate it."
                )
            if cleanup_error is not None:
                detail = f"{detail}; {cleanup_error}"
            raise V5DatabaseError(
                f"could not create fresh schema-v5 database {self.database_path}: {detail}"
            ) from exc

    def connect(self) -> sqlite3.Connection:
        """Open an exact-v5 connection with foreign keys enforced.

        The first pass is read-only. The second connection uses ``mode=rw`` so
        a deletion or race can never recreate an empty database implicitly.
        Both passes verify v5 identity before connection-mutating pragmas run.
        """

        if not self.database_path.exists():
            raise V5DatabaseError(
                f"schema-v5 database {self.database_path} does not exist; call "
                "initialize() explicitly before connecting"
            )
        boundary = _validate_state_directory(self.state_dir)
        _inspect_version_read_only(self.database_path)
        inspection: sqlite3.Connection | None = None
        connection: sqlite3.Connection | None = None
        try:
            immutable = sqlite3.connect(
                _database_uri(self.database_path, "ro", immutable=True),
                uri=True,
                timeout=30.0,
            )
            try:
                immutable_instance_id = _validate_v5_structure(immutable)
            finally:
                immutable.close()
            inspection = sqlite3.connect(
                _database_uri(self.database_path, "ro"), uri=True, timeout=30.0
            )
            inspection_instance_id = _validate_v5_structure(inspection)
            if inspection_instance_id != immutable_instance_id:
                raise V5DatabaseError(
                    "schema-v5 database instance changed between immutable and "
                    "WAL-aware inspection"
                )
            _validate_database_permissions(self.database_path)
            inspection.close()
            inspection = None
            connection = sqlite3.connect(
                _database_uri(self.database_path, "rw"),
                uri=True,
                timeout=30.0,
                factory=_ClosingConnection,
            )
            # Recheck through the exact handle that will mutate. This closes the
            # inspection/open replacement race before enabling any pragma.
            writable_instance_id = _validate_v5_structure(connection)
            if writable_instance_id != inspection_instance_id:
                raise V5DatabaseError(
                    "schema-v5 database instance changed while opening the "
                    "read-write handle"
                )
            self._bind_instance_identity(writable_instance_id)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 30000")
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise V5DatabaseError(
                    f"could not enable WAL journaling, SQLite returned {journal_mode!r}"
                )
            if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise V5DatabaseError("could not enable SQLite foreign-key enforcement")
            _revalidate_state_boundary(boundary)
            return connection
        except (sqlite3.Error, OSError, V5DatabaseError) as exc:
            if inspection is not None:
                inspection.close()
            if connection is not None:
                connection.close()
            if isinstance(exc, V5DatabaseError):
                raise
            raise V5DatabaseError(
                f"could not open schema-v5 database {self.database_path}: {exc}"
            ) from exc


__all__ = [
    "DATABASE_FILENAME",
    "DATABASE_INSTANCE_ID_KEY",
    "SCHEMA_DDL_SHA256",
    "SCHEMA_IDENTITY",
    "SCHEMA_VERSION",
    "V4_QUEUE_ITEM_COLUMNS",
    "V5DatabaseError",
    "V5QueueStore",
    "V5SchemaVersionError",
]
