"""Exercise the authenticated service boundary above fresh schema-v5 state."""

from __future__ import annotations

from dataclasses import dataclass, fields
import json
from pathlib import Path
import subprocess

import pytest

from experiment_queue.admission import AdmissionSnapshot, Submission
from experiment_queue.authoring import Project
from experiment_queue.cooperative_yield import (
    ContinuationIdentity,
    CooperativeYieldReceipt,
    CooperativeYieldRequest,
    YieldRequestKind,
)
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.git_resolver import (
    GitResolvedAdmission,
    compile_admission_from_revision,
    verify_project_revision,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    MountBinding,
    ProjectRevision,
    ProjectRuntimeState,
    RegisteredProject,
)
from experiment_queue.serialization import sha256_bytes
from experiment_queue.serialization import canonical_json_bytes
from experiment_queue.v5_repository import (
    V5EvidenceError,
    V5ProjectRepository,
    V5RepositoryError,
)


PROJECT_PATH = "config/project.yaml"
CARD_PATH = "cards/shared.yaml"
EXTENSION_PATH = "schemas/extensions.json"
NOW = "2026-08-28T15:00:00Z"


@dataclass(frozen=True, slots=True)
class ProjectBundle:
    """Temporary committed Project and complete host enrollment."""

    repository: Path
    project_source: bytes
    card_source: bytes
    extension_source: bytes | None
    project: Project
    enrollment: Enrollment
    revision: ProjectRevision
    registered: RegisteredProject
    runtime: ProjectRuntimeState


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"test Git command failed: {arguments!r}: "
            f"{result.stderr.decode(errors='replace')}"
        )
    return result.stdout.decode("ascii").strip()


def _source(document: dict[str, object]) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode()


def _wire(document: dict[str, object]) -> bytes:
    return (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _project_document(
    key: str,
    *,
    display_name: str,
    with_extension: bool,
) -> dict[str, object]:
    spec: dict[str, object] = {
        "cardRoots": ["cards"],
        "volumes": [
            {"name": "scratch", "access": "readWrite", "required": True}
        ],
        "environments": [{"name": "python"}],
        "environmentPolicy": {"inherit": "none", "allowVariables": []},
        "supportedProtocols": [
            {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldRequest",
            },
            {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldReceipt",
            },
        ],
    }
    if with_extension:
        spec["extensionSchema"] = {"path": EXTENSION_PATH}
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {"key": key, "displayName": display_name},
        "spec": spec,
    }


def _card_document(key: str, experiment_id: str = "shared-experiment") -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": key,
            "experimentId": experiment_id,
            "title": "V5 repository fixture",
        },
        "spec": {
            "parameters": {"epochs": 1},
            "jobs": [
                {
                    "id": "train",
                    "environment": "python",
                    "command": {"type": "argv", "argv": ["python", "train.py"]},
                    "artifacts": [
                        {
                            "name": "checkpoint",
                            "root": "scratch",
                            "path": "checkpoints/latest.bin",
                            "type": "file",
                        }
                    ],
                    "capabilities": {
                        "cooperativeYield": {
                            "requestProtocol": {
                                "apiVersion": "experiment-queue/v1",
                                "kind": "CooperativeYieldRequest",
                            },
                            "receiptProtocol": {
                                "apiVersion": "experiment-queue/v1",
                                "kind": "CooperativeYieldReceipt",
                            },
                            "checkpointArtifacts": ["checkpoint"],
                        }
                    },
                }
            ],
        },
    }


def _make_bundle(
    root: Path,
    *,
    state_dir: Path,
    project_id: int,
    revision_id: int,
    key: str,
    with_extension: bool = False,
) -> ProjectBundle:
    root.mkdir(parents=True)
    repository = root / "repository"
    repository.mkdir()
    _git(repository, "init", "--quiet")
    project_source = _source(
        _project_document(
            key,
            display_name=f"Project {key}",
            with_extension=with_extension,
        )
    )
    card_source = _source(_card_document(key))
    extension_source: bytes | None = None
    (repository / PROJECT_PATH).parent.mkdir(parents=True)
    (repository / PROJECT_PATH).write_bytes(project_source)
    (repository / CARD_PATH).parent.mkdir(parents=True)
    (repository / CARD_PATH).write_bytes(card_source)
    if with_extension:
        extension_source = _source(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": True,
            }
        )
        (repository / EXTENSION_PATH).parent.mkdir(parents=True)
        (repository / EXTENSION_PATH).write_bytes(extension_source)
    _git(repository, "add", "--", PROJECT_PATH, CARD_PATH)
    if with_extension:
        _git(repository, "add", "--", EXTENSION_PATH)
    _git(
        repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    project = Project.from_yaml(project_source, source_name=PROJECT_PATH)
    environment_directory = root / "environment-bin"
    scratch_directory = root / "scratch"
    environment_directory.mkdir()
    scratch_directory.mkdir()
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=repository,
        project_manifest_path=PROJECT_PATH,
        mounts=(
            MountBinding.create(
                name="scratch",
                path=scratch_directory,
                access="readWrite",
            ),
        ),
        environments=(
            EnvironmentBinding.create(
                name="python",
                executable_search_directories=(environment_directory,),
            ),
        ),
        state_directory=state_dir,
    )
    revision = ProjectRevision.create(
        revision_id=revision_id,
        project_id=project_id,
        sequence=1,
        project=project,
        project_source_path=PROJECT_PATH,
        project_source=project_source,
        git_commit=_git(repository, "rev-parse", "HEAD"),
        enrollment=enrollment,
        created_actor="test:operator",
        created_at=NOW,
        extension_schema_source=extension_source,
    )
    registered = RegisteredProject.register(
        revision=revision,
        reason="initial registration",
        actor="test:operator",
        changed_at=NOW,
    )
    runtime = ProjectRuntimeState.create(
        project_id=project_id,
        project_key=key,
        reason="healthy",
        actor="test:operator",
        changed_at=NOW,
    )
    return ProjectBundle(
        repository=repository,
        project_source=project_source,
        card_source=card_source,
        extension_source=extension_source,
        project=project,
        enrollment=enrollment,
        revision=revision,
        registered=registered,
        runtime=runtime,
    )


@pytest.fixture
def repository(tmp_path: Path) -> tuple[V5ProjectRepository, ProjectBundle]:
    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir)
    store.initialize()
    bundle = _make_bundle(
        tmp_path / "one",
        state_dir=state_dir,
        project_id=1,
        revision_id=1,
        key="project-one",
    )
    service = V5ProjectRepository(store)
    service.register_project(
        bundle.registered,
        verify_project_revision(bundle.revision),
        bundle.runtime,
    )
    return service, bundle


def _resolved(
    bundle: ProjectBundle,
    **changes: object,
) -> GitResolvedAdmission:
    values: dict[str, object] = {
        "project_key": bundle.project.key,
        "card_path": CARD_PATH,
        "job_id": "train",
        "operator": "test:operator",
        "bindings": {"epochs": 2},
    }
    values.update(changes)
    return compile_admission_from_revision(
        revision=bundle.revision,
        submission=Submission(**values),  # type: ignore[arg-type]
    )


def _forged_resolved(
    resolved: GitResolvedAdmission,
    **changes: object,
) -> GitResolvedAdmission:
    forged = object.__new__(GitResolvedAdmission)
    for definition in fields(GitResolvedAdmission):
        object.__setattr__(
            forged,
            definition.name,
            changes.get(definition.name, getattr(resolved, definition.name)),
        )
    return forged


def test_registration_round_trips_all_revision_and_git_evidence(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository

    view = service.get_project(project_key="project-one")
    revision = service.get_revision(bundle.revision.id)
    git_evidence = service.get_revision_git_evidence(bundle.revision.id)

    assert view.project == bundle.registered
    assert view.current_revision == bundle.revision
    assert view.runtime_state == bundle.runtime
    assert revision == bundle.revision
    assert git_evidence.project_blob.path == PROJECT_PATH
    assert git_evidence.project_blob.size == len(bundle.project_source)
    assert git_evidence.project_blob.object_id == _git(
        bundle.repository,
        "rev-parse",
        f"HEAD:{PROJECT_PATH}",
    )
    assert service.list_projects() == (view,)
    assert service.list_events()[0].event_type == "project_registered"


def test_imported_legacy_project_atomically_adopts_first_typed_revision(
    tmp_path: Path,
) -> None:
    """Cutover can append+activate typed evidence without fabricating sequence 1."""

    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir)
    store.initialize()
    bundle = _make_bundle(
        tmp_path / "portable",
        state_dir=state_dir,
        project_id=1,
        revision_id=2,
        key="imported-project",
    )
    typed_revision = ProjectRevision.create(
        revision_id=2,
        project_id=1,
        sequence=2,
        project=bundle.project,
        project_source_path=PROJECT_PATH,
        project_source=bundle.project_source,
        git_commit=bundle.revision.git_commit,
        enrollment=bundle.enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )
    legacy_enrollment = canonical_json_bytes(
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "LegacyEnrollment",
            "projectKey": "imported-project",
            "checkoutDirectory": str(bundle.repository),
            "projectManifestPath": None,
            "sourceSchemaVersion": 4,
            "sourceStateIdentitySha256": "1" * 64,
            "gitCommit": typed_revision.git_commit,
            "mounts": [],
            "artifactRoots": [],
            "environments": [],
        }
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO projects(
                id, project_key, display_name, lifecycle, current_revision_id,
                current_revision_sequence, created_at, created_by,
                lifecycle_changed_at, lifecycle_actor, lifecycle_reason
            ) VALUES (1, 'imported-project', 'imported-project', 'paused', 1, 1,
                      ?, 'importer', ?, 'importer', 'offline import')
            """,
            (NOW, NOW),
        )
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, checkout_path, enrollment_json,
                enrollment_sha256, created_at, created_actor
            ) VALUES (1, 1, 1, 'imported-project:legacy-r1', 'legacy-v4',
                      'imported-project', ?, ?, ?, ?, ?, 'importer')
            """,
            (
                typed_revision.git_commit,
                str(bundle.repository),
                legacy_enrollment,
                sha256_bytes(legacy_enrollment),
                NOW,
            ),
        )
        connection.execute(
            """
            INSERT INTO project_runtime_state(
                project_id, health, circuit_failure_count, health_reason,
                health_actor, health_changed_at
            ) VALUES (1, 'closed', 0, 'import verified', 'importer', ?)
            """,
            (NOW,),
        )
        connection.commit()

    service = V5ProjectRepository(store)
    with pytest.raises(V5EvidenceError, match="only imported legacy-v4"):
        service.get_project(project_id=1)
    with pytest.raises(V5RepositoryError, match="appended and activated atomically"):
        service.append_revision(verify_project_revision(typed_revision), activate=False)
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM project_revisions"
        ).fetchone()[0] == 1

    adopted = service.append_revision(
        verify_project_revision(typed_revision), activate=True
    )
    assert adopted.project.current_revision_id == 2
    assert adopted.project.current_revision_sequence == 2
    assert adopted.current_revision == typed_revision
    assert adopted.project.lifecycle.value == "paused"
    assert service.list_projects() == (adopted,)

    active = service.transition_project(
        project_id=1,
        target="active",
        reason="portable Project verified",
        actor="test:operator",
        changed_at=NOW,
    )
    assert active.project.lifecycle.value == "active"


def test_admission_requires_git_wrapper_and_round_trips_every_hash(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository
    resolved = _resolved(bundle, priority=7)

    with pytest.raises(TypeError, match="GitResolvedAdmission"):
        service.admit(resolved.snapshot, added_at=NOW)  # type: ignore[arg-type]
    with pytest.raises(V5RepositoryError, match="no registered Project"):
        service.admit(
            _forged_resolved(resolved, project_id=99),
            added_at=NOW,
        )

    item = service.admit(resolved, added_at=NOW)

    assert item.id == 1
    assert item.attempt == 1
    assert item.priority == 7
    assert item.snapshot == resolved.snapshot
    assert service.get_admission_snapshot(item.snapshot_id or 0) == resolved.snapshot
    assert service.get_queue_item(item.id) == item
    with service.store.connect() as connection:
        row = connection.execute(
            "SELECT * FROM admission_snapshots WHERE id = ?",
            (item.snapshot_id,),
        ).fetchone()
        assert row is not None
        assert row["project_blob_object_id"] == resolved.project_blob.object_id
        assert row["card_blob_object_id"] == resolved.card_blob.object_id
        assert row["command_sha256"] == sha256_bytes(row["command_json"])
        assert row["policy_sha256"] == sha256_bytes(row["policy_json"])


def test_global_ids_project_attempts_and_cross_project_dependencies(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir)
    store.initialize()
    service = V5ProjectRepository(store)
    first = _make_bundle(
        tmp_path / "one",
        state_dir=state_dir,
        project_id=1,
        revision_id=1,
        key="project-one",
    )
    second = _make_bundle(
        tmp_path / "two",
        state_dir=state_dir,
        project_id=2,
        revision_id=2,
        key="project-two",
    )
    for bundle in (first, second):
        service.register_project(
            bundle.registered,
            verify_project_revision(bundle.revision),
            bundle.runtime,
        )

    first_item = service.admit(_resolved(first, priority=1), added_at=NOW)
    second_item = service.admit(
        _resolved(second, dependencies=[first_item.id], priority=20),
        added_at=NOW,
    )
    retry = service.admit(_resolved(first, priority=5), added_at=NOW)

    assert [first_item.id, second_item.id, retry.id] == [1, 2, 3]
    assert first_item.experiment_id == second_item.experiment_id
    assert [first_item.attempt, second_item.attempt, retry.attempt] == [1, 1, 2]
    assert [item.id for item in service.list_dispatch_candidates()] == [3, 1]

    with service.store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'succeeded' WHERE id = ?",
            (first_item.id,),
        )
        connection.commit()
    assert [item.id for item in service.list_dispatch_candidates()] == [2, 3]

    with service.store.connect() as connection:
        connection.execute(
            "UPDATE queue_items SET state = 'failed' WHERE id = ?",
            (retry.id,),
        )
        connection.commit()
    with pytest.raises(V5RepositoryError, match="terminal.*non-success"):
        service.admit(
            _resolved(second, dependencies=[retry.id]),
            added_at=NOW,
        )


def test_pause_and_health_block_dispatch_but_pause_allows_admission(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository
    service.transition_project(
        project_id=bundle.registered.id,
        target="paused",
        reason="maintenance",
        actor="test:operator",
        changed_at=NOW,
    )
    item = service.admit(_resolved(bundle), added_at=NOW)
    assert service.list_dispatch_candidates() == ()

    service.transition_project(
        project_id=bundle.registered.id,
        target="active",
        reason="maintenance complete",
        actor="test:operator",
        changed_at=NOW,
    )
    assert [candidate.id for candidate in service.list_dispatch_candidates()] == [item.id]
    service.record_project_failure(
        project_id=bundle.registered.id,
        reason="project setup failed",
        actor="scheduler",
        changed_at=NOW,
        open_circuit=True,
    )
    assert service.list_dispatch_candidates() == ()
    service.close_project_circuit(
        project_id=bundle.registered.id,
        reason="operator repaired setup",
        actor="test:operator",
        changed_at=NOW,
    )
    assert [candidate.id for candidate in service.list_dispatch_candidates()] == [item.id]


def test_append_then_activate_new_revision(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository
    updated_document = _project_document(
        bundle.project.key,
        display_name="Project one revision two",
        with_extension=False,
    )
    updated_source = _source(updated_document)
    (bundle.repository / PROJECT_PATH).write_bytes(updated_source)
    _git(bundle.repository, "add", "--", PROJECT_PATH)
    _git(
        bundle.repository,
        "-c",
        "user.name=Experiment Queue Tests",
        "-c",
        "user.email=queue-tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "revision two",
    )
    updated_project = Project.from_yaml(updated_source, source_name=PROJECT_PATH)
    updated_enrollment = Enrollment.create(
        project=updated_project,
        checkout_directory=bundle.repository,
        project_manifest_path=PROJECT_PATH,
        mounts=bundle.enrollment.mounts,
        environments=bundle.enrollment.environments,
        state_directory=service.store.state_dir,
    )
    revision = ProjectRevision.create(
        revision_id=2,
        project_id=bundle.registered.id,
        sequence=2,
        project=updated_project,
        project_source_path=PROJECT_PATH,
        project_source=updated_source,
        git_commit=_git(bundle.repository, "rev-parse", "HEAD"),
        enrollment=updated_enrollment,
        created_actor="test:operator",
        created_at=NOW,
    )

    view = service.append_revision(verify_project_revision(revision), activate=False)
    assert view.project.current_revision_id == bundle.revision.id
    activated = service.activate_revision(
        project_id=bundle.registered.id,
        revision_id=revision.id,
        actor="test:operator",
        changed_at=NOW,
    )
    assert activated.project.current_revision_id == revision.id
    assert activated.project.display_name == "Project one revision two"
    assert service.get_revision(revision.id) == revision


def test_archive_is_permanent_preserves_history_and_blocks_admission(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository
    service.transition_project(
        project_id=bundle.registered.id,
        target="paused",
        reason="retiring",
        actor="test:operator",
        changed_at=NOW,
    )
    archived = service.transition_project(
        project_id=bundle.registered.id,
        target="archived",
        reason="retired",
        actor="test:operator",
        changed_at=NOW,
    )

    assert archived.project.lifecycle.value == "archived"
    assert service.get_revision(bundle.revision.id) == bundle.revision
    assert archived.runtime_state == bundle.runtime
    assert len(service.list_events(project_id=bundle.registered.id)) == 3
    with pytest.raises(V5RepositoryError, match="archived"):
        service.admit(_resolved(bundle), added_at=NOW)


def test_corrupt_snapshot_blob_fails_closed_after_trigger_is_restored(
    repository: tuple[V5ProjectRepository, ProjectBundle],
) -> None:
    service, bundle = repository
    item = service.admit(_resolved(bundle), added_at=NOW)
    with service.store.connect() as connection:
        trigger = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'trigger' AND name = 'admission_snapshots_immutable_update'
            """
        ).fetchone()[0]
        connection.execute("DROP TRIGGER admission_snapshots_immutable_update")
        connection.execute(
            "UPDATE admission_snapshots SET resolved_json = ? WHERE id = ?",
            (b"{}", item.snapshot_id),
        )
        connection.execute(trigger)
        connection.commit()

    with pytest.raises(V5EvidenceError, match="failed exact rehydration"):
        service.get_admission_snapshot(item.snapshot_id or 0)


def test_extension_and_cooperative_yield_wire_evidence_round_trip(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    store = V5QueueStore(state_dir)
    store.initialize()
    bundle = _make_bundle(
        tmp_path / "project",
        state_dir=state_dir,
        project_id=1,
        revision_id=1,
        key="yield-project",
        with_extension=True,
    )
    service = V5ProjectRepository(store)
    service.register_project(
        bundle.registered,
        verify_project_revision(bundle.revision),
        bundle.runtime,
    )
    item = service.admit(
        _resolved(bundle, preemption_authorized=True),
        added_at=NOW,
    )
    with service.store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = 'GPU-fixture',
                assigned_gpu_index = '0', runtime_gpu_lease_held = 1
            WHERE id = ?
            """,
            (item.id,),
        )
        connection.commit()
    assert item.snapshot is not None
    identity = ContinuationIdentity.create(
        resolved_spec_sha256=item.snapshot.resolved_sha256,
        project_revision=item.snapshot.project_revision,
        git_commit=item.git_commit,
        run_id="run-1",
        prior_receipt_sha256="0" * 64,
    )
    request = CooperativeYieldRequest(
        request_id="request-1",
        queue_item_id=item.id,
        segment=item.segment,
        request_kind=YieldRequestKind.MANUAL_PREEMPTION,
        requested_at=NOW,
        requested_by="test:operator",
        note="test cooperative checkpoint",
        continuation=identity,
    )
    request_source = _wire(request.to_document())
    request_record = service.record_yield_request(request, source=request_source)
    assert request_record.source == request_source
    assert request_record.sha256 == sha256_bytes(request_source)
    assert service.get_yield_request(request.request_id) == request_record

    receipt = CooperativeYieldReceipt.failed(
        request,
        error="fixture declined checkpoint",
        written_at=NOW,
    )
    receipt_source = _wire(receipt.to_document())
    receipt_record = service.record_yield_receipt(
        receipt,
        source=receipt_source,
        actor="project:fixture",
    )
    assert receipt_record.source == receipt_source
    assert receipt_record.sha256 == sha256_bytes(receipt_source)
    assert service.get_yield_receipt(request.request_id) == receipt_record

    git_evidence = service.get_revision_git_evidence(bundle.revision.id)
    assert git_evidence.extension_schema_blob is not None
    assert git_evidence.extension_schema_blob.path == EXTENSION_PATH
