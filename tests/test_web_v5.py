"""Verify schema-v5 web authentication and project disclosure boundaries."""

from __future__ import annotations

from dataclasses import replace
from http import HTTPStatus
import json
import os
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest

import experiment_queue.web_v5 as web_v5_module
from experiment_queue.database_v5 import V5QueueStore
from experiment_queue.reservation_v5 import (
    V5ReservationError,
    V5ReservationService,
    V5ReservationStatus,
)
from experiment_queue.scheduler_service_v5 import V5SchedulerService
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes
from experiment_queue.v5_operator_repository import (
    V5OperatorNotFoundError,
    V5OperatorRepository,
)
from experiment_queue.web import initialize_web_auth as initialize_legacy_web_auth
from experiment_queue.web_v5 import (
    ROLE_HOST_ADMIN,
    ROLE_OPERATOR,
    ROLE_RESERVER,
    ROLE_VIEWER,
    V5AuthManager,
    V5WebApplication,
    V5WebArtifactSummary,
    V5WebAuthorizationError,
    V5WebError,
    V5WebEventSummary,
    V5WebGpuSummary,
    V5WebHandler,
    V5WebItemSummary,
    V5WebNotFoundError,
    V5WebProjectSummary,
    V5WebRateLimitError,
    V5WebRepositoryAdapter,
    V5WebReservationSummary,
    V5WebSession,
    V5WebTerminationSummary,
    V5WebYieldSummary,
    build_arg_parser,
    initialize_v5_web_auth,
    main,
    serve_v5_web,
)


def test_v5_auth_signs_role_and_project_scope_without_storing_passwords(
    tmp_path: Path,
) -> None:
    path = initialize_v5_web_auth(
        tmp_path,
        role_passwords={
            ROLE_HOST_ADMIN: "host-administrator-secret",
            ROLE_OPERATOR: "project-operator-secret",
            ROLE_VIEWER: "project-viewer-secret",
            ROLE_RESERVER: "gpu-reserver-secret",
        },
        project_scopes={
            ROLE_OPERATOR: ["project-one"],
            ROLE_VIEWER: ["project-two"],
        },
    )
    source = path.read_text(encoding="utf-8")
    assert path.stat().st_mode & 0o077 == 0
    assert "host-administrator-secret" not in source
    assert "project-viewer-secret" not in source

    auth = V5AuthManager(path)
    assert auth.verify_password(ROLE_HOST_ADMIN, "host-administrator-secret")
    assert auth.verify_password(ROLE_OPERATOR, "project-operator-secret")
    assert auth.verify_password(ROLE_VIEWER, "project-viewer-secret")
    assert auth.verify_password(ROLE_RESERVER, "gpu-reserver-secret")
    token, viewer = auth.issue_session(ROLE_VIEWER, now_epoch=100)
    assert viewer.project_keys == ("project-two",)
    assert viewer.can_read_project("project-two")
    assert not viewer.can_read_project("project-one")
    assert auth.verify_session(token, now_epoch=101) == viewer
    assert auth.verify_session(token + "changed", now_epoch=101) is None
    assert auth.verify_session(token, now_epoch=viewer.expires_epoch) is None

    _token, administrator = auth.issue_session(ROLE_HOST_ADMIN, now_epoch=100)
    assert administrator.project_keys is None
    assert administrator.can_read_project("any-valid-project")
    _token, reserver = auth.issue_session(ROLE_RESERVER, now_epoch=100)
    assert reserver.project_keys == ()
    assert not reserver.can_read_project("project-two")
    _token, later_reserver = auth.issue_session(ROLE_RESERVER, now_epoch=200)
    assert later_reserver.subject == reserver.subject
    assert later_reserver.csrf != reserver.csrf


def test_v5_auth_reads_legacy_admin_and_reservation_credentials(tmp_path: Path) -> None:
    path = initialize_legacy_web_auth(
        tmp_path,
        admin_password="administrator-secret",
        reservation_password="coworker-shared-secret",
    )
    auth = V5AuthManager(path)

    assert auth.verify_password("admin", "administrator-secret")
    assert auth.verify_password(ROLE_HOST_ADMIN, "administrator-secret")
    assert auth.verify_password("reservation", "coworker-shared-secret")
    assert auth.verify_password(ROLE_RESERVER, "coworker-shared-secret")
    _token, administrator = auth.issue_session("admin")
    _token, reserver = auth.issue_session("reservation")
    assert administrator.role == ROLE_HOST_ADMIN
    assert administrator.project_keys is None
    assert reserver.role == ROLE_RESERVER
    assert reserver.project_keys == ()
    assert not auth.verify_password(ROLE_OPERATOR, "administrator-secret")


def test_v5_auth_rejects_ambiguous_roles_and_reserver_project_scope(
    tmp_path: Path,
) -> None:
    with pytest.raises(V5WebError, match="different password"):
        initialize_v5_web_auth(
            tmp_path,
            role_passwords={
                ROLE_HOST_ADMIN: "shared-password-value",
                ROLE_VIEWER: "shared-password-value",
            },
        )
    with pytest.raises(V5WebError, match="reserver role"):
        initialize_v5_web_auth(
            tmp_path,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_RESERVER: "gpu-reserver-secret",
            },
            project_scopes={ROLE_RESERVER: ["project-one"]},
        )


def test_v5_auth_rejects_symlink_nonregular_and_multiply_linked_files(
    tmp_path: Path,
) -> None:
    external = initialize_v5_web_auth(
        tmp_path / "external",
        role_passwords={ROLE_HOST_ADMIN: "host-administrator-secret"},
    )
    state = tmp_path / "state"
    state.mkdir()
    linked = state / "web_auth.json"
    linked.symlink_to(external)
    with pytest.raises(V5WebError, match="must not be a symlink"):
        V5AuthManager(linked)

    directory = state / "directory-auth.json"
    directory.mkdir()
    with pytest.raises(V5WebError, match="regular file"):
        V5AuthManager(directory)

    fifo = state / "fifo-auth.json"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(V5WebError, match="regular file"):
        V5AuthManager(fifo)

    hardlink = state / "hardlink-auth.json"
    os.link(external, hardlink)
    with pytest.raises(V5WebError, match="exactly one filesystem link"):
        V5AuthManager(hardlink)


def test_v5_auth_rejects_wrong_owner_or_nonexact_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = initialize_v5_web_auth(
        tmp_path,
        role_passwords={ROLE_HOST_ADMIN: "host-administrator-secret"},
    )
    path.chmod(0o400)
    with pytest.raises(V5WebError, match="mode 0600"):
        V5AuthManager(path)
    path.chmod(0o600)

    actual_uid = os.geteuid()
    monkeypatch.setattr(
        "experiment_queue.web_v5.os.geteuid", lambda: actual_uid + 1
    )
    with pytest.raises(V5WebError, match="owned by uid"):
        V5AuthManager(path)


def _project(project_id: int, key: str) -> V5WebProjectSummary:
    return V5WebProjectSummary(
        id=project_id,
        key=key,
        display_name=f"{key} display",
        lifecycle="active",
        revision_id=project_id,
        revision_sequence=1,
        revision_label=f"{key}:r1",
        revision_kind="project-v1",
        git_commit=str(project_id) * 40,
        health="closed",
        health_reason=f"{key} healthy",
        circuit_failure_count=0,
        dispatch_allowed=True,
        host_dispatch_paused=False,
        host_pause_reason="",
        queue_counts=(("queued", 3),),
    )


def _item(item_id: int, project: V5WebProjectSummary) -> V5WebItemSummary:
    return V5WebItemSummary(
        id=item_id,
        project_id=project.id,
        project_key=project.key,
        revision_id=project.revision_id,
        revision_label=project.revision_label,
        admission_kind="ExperimentCard/v1",
        experiment_id=f"{project.key}-experiment-{item_id}",
        job_id="train",
        attempt=1,
        segment=1,
        state="queued",
        priority=20,
        resume_front=False,
        preemptible=False,
        git_commit=project.git_commit or "",
        card_path=f"experiments/{item_id}.yaml",
        added_at="2026-08-28T12:00:00+00:00",
        added_by="test:operator",
        state_detail=None,
        dependencies=(),
    )


class FakeWebService:
    """Strict two-Project service double that records every authorized selector."""

    def __init__(self) -> None:
        self.one = _project(1, "project-one")
        self.two = _project(2, "project-two")
        self.items = {
            1: (_item(1, self.one), _item(3, self.one), _item(5, self.one)),
            2: (_item(2, self.two),),
        }
        self.calls: list[tuple[object, ...]] = []

    def list_projects(
        self, *, project_keys: tuple[str, ...] | None
    ) -> tuple[V5WebProjectSummary, ...]:
        self.calls.append(("list_projects", project_keys))
        values = (self.one, self.two)
        if project_keys is None:
            return values
        return tuple(project for project in values if project.key in project_keys)

    def get_project(self, project_key: str) -> V5WebProjectSummary:
        self.calls.append(("get_project", project_key))
        for project in (self.one, self.two):
            if project.key == project_key:
                return project
        raise V5WebNotFoundError("missing Project")

    def list_items(
        self,
        *,
        project_id: int,
        states: tuple[str, ...],
        after_id: int,
        limit: int,
    ) -> tuple[V5WebItemSummary, ...]:
        self.calls.append(("list_items", project_id, states, after_id, limit))
        values = tuple(item for item in self.items[project_id] if item.id > after_id)
        if states:
            values = tuple(item for item in values if item.state in states)
        return values[:limit]

    def get_item(self, *, project_id: int, item_id: int) -> V5WebItemSummary:
        self.calls.append(("get_item", project_id, item_id))
        for item in self.items[project_id]:
            if item.id == item_id:
                return item
        raise V5WebNotFoundError(
            f"Project id {project_id} has no item with global id {item_id}"
        )

    def list_events(
        self, *, project_id: int, after_id: int, limit: int
    ) -> tuple[V5WebEventSummary, ...]:
        self.calls.append(("list_events", project_id, after_id, limit))
        events = tuple(
            V5WebEventSummary(
                id=project_id * 10 + index,
                project_id=project_id,
                queue_item_id=self.items[project_id][0].id,
                created_at="2026-08-28T12:00:00+00:00",
                actor="test:operator",
                event_type=f"project-{project_id}-event-{index}",
                payload={"privateProject": project_id},
            )
            for index in range(1, 4)
            if project_id * 10 + index > after_id
        )
        return events[:limit]

    def list_artifacts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebArtifactSummary, ...]:
        self.get_item(project_id=project_id, item_id=item_id)
        return (
            V5WebArtifactSummary(
                id=1,
                queue_item_id=item_id,
                revision_id=project_id,
                segment=1,
                evidence_kind="declared-v1",
                artifact_name="model",
                artifact_type="file",
                root_name="artifacts",
                relative_path="model.bin",
                absolute_path=f"/authorized/{project_id}/model.bin",
                size_bytes=10,
                sha256="a" * 64,
                recorded_at="2026-08-28T12:00:00+00:00",
                metadata=None,
            ),
        )

    def list_yield_receipts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebYieldSummary, ...]:
        self.get_item(project_id=project_id, item_id=item_id)
        return (
            V5WebYieldSummary(
                request_id="yield-request-one",
                queue_item_id=item_id,
                segment=1,
                status="ready",
                written_at="2026-08-28T12:00:00+00:00",
                receipt_sha256="b" * 64,
                continuation_identity_sha256="c" * 64,
                progress={"unit": "step", "completed": 2},
                checkpoint_artifacts=[],
                resume_context_media_type="application/json",
                resume_context_bytes=2,
                resume_context_sha256="d" * 64,
                error=None,
            ),
        )

    def mutate_item(
        self,
        *,
        project_id: int,
        item_id: int,
        operation: str,
        reason: str,
        priority: int | None,
        actor: str,
        changed_at: str,
    ) -> V5WebItemSummary:
        self.calls.append(
            (
                "mutate_item",
                project_id,
                item_id,
                operation,
                reason,
                priority,
                actor,
                changed_at,
            )
        )
        item = self.get_item(project_id=project_id, item_id=item_id)
        return replace(
            item,
            state="held" if operation == "hold" else item.state,
            priority=priority if priority is not None else item.priority,
        )

    def request_termination(
        self,
        *,
        item_id: int,
        reason: str,
        actor: str,
        force: bool,
        requested_at: str,
    ) -> V5WebTerminationSummary:
        self.calls.append(
            (
                "request_termination",
                item_id,
                reason,
                actor,
                force,
                requested_at,
            )
        )
        for project_id, items in self.items.items():
            if any(item.id == item_id for item in items):
                return V5WebTerminationSummary(
                    item_id=item_id,
                    project_id=project_id,
                    state="force_killing" if force else "terminating",
                    stage="kill" if force else "interrupt",
                    requested_at=requested_at,
                    signal_delivered=not force,
                )
        raise V5WebNotFoundError(f"queue item {item_id} does not exist")

    def mutate_project(
        self,
        *,
        project_id: int,
        operation: str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5WebProjectSummary:
        self.calls.append(
            (
                "mutate_project",
                project_id,
                operation,
                reason,
                actor,
                changed_at,
            )
        )
        project = self.one if project_id == 1 else self.two
        return replace(project, lifecycle="paused" if operation == "pause" else "active")

    def list_reserver_gpus(
        self, *, actor: str, include_all: bool
    ) -> tuple[V5WebGpuSummary, ...]:
        self.calls.append(("list_reserver_gpus", actor, include_all))
        return (
            V5WebGpuSummary(
                uuid="GPU-private-uuid",
                index="0",
                name="Test GPU",
                schedulable=True,
                busy=True,
            ),
        )

    def request_reservation(
        self,
        *,
        gpu_uuid: str,
        duration_hours: int,
        note: str,
        actor: str,
        requested_at: str,
    ) -> V5WebReservationSummary:
        self.calls.append(
            (
                "request_reservation",
                gpu_uuid,
                duration_hours,
                note,
                actor,
                requested_at,
            )
        )
        return V5WebReservationSummary(
            id=1,
            gpu_uuid=gpu_uuid,
            status="pending",
            note=note,
            duration_hours=duration_hours,
            requested_at=requested_at,
            starts_at=None,
            expires_at=None,
            released_at=None,
            open=True,
        )

    def release_reservation(
        self,
        *,
        reservation_id: int,
        actor: str,
        released_at: str,
        allow_any: bool,
    ) -> V5WebReservationSummary:
        self.calls.append(
            (
                "release_reservation",
                reservation_id,
                actor,
                released_at,
                allow_any,
            )
        )
        return V5WebReservationSummary(
            id=reservation_id,
            gpu_uuid="GPU-private-uuid",
            status="released",
            note="released fixture",
            duration_hours=2,
            requested_at="2026-08-28T12:00:00+00:00",
            starts_at=None,
            expires_at=None,
            released_at=released_at,
            open=False,
        )


@pytest.fixture
def scoped_web(
    tmp_path: Path,
) -> tuple[V5WebApplication, FakeWebService, V5WebSession, V5WebSession, V5WebSession]:
    auth = V5AuthManager(
        initialize_v5_web_auth(
            tmp_path,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_OPERATOR: "project-operator-secret",
                ROLE_VIEWER: "project-viewer-secret",
                ROLE_RESERVER: "gpu-reserver-secret",
            },
            project_scopes={
                ROLE_OPERATOR: ["project-one"],
                ROLE_VIEWER: ["project-one"],
            },
        )
    )
    service = FakeWebService()
    app = V5WebApplication(service, auth)
    _token, operator = auth.issue_session(ROLE_OPERATOR)
    _token, viewer = auth.issue_session(ROLE_VIEWER)
    _token, reserver = auth.issue_session(ROLE_RESERVER)
    return app, service, operator, viewer, reserver


def test_project_filter_and_history_pagination_are_server_side(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, service, _operator, viewer, _reserver = scoped_web
    page = app.render_projects(viewer, {}).decode("utf-8")
    assert "project-one display" in page
    assert "project-two display" not in page
    assert ("list_projects", ("project-one",)) in service.calls

    document = json.loads(
        app.queue_document(
            viewer,
            "project-one",
            {"queue_size": ["2"], "queue_after": ["0"], "state": ["queued"]},
        )
    )
    assert document["project"] == "project-one"
    assert [item["id"] for item in document["items"]] == [1, 3]
    assert document["hasMore"] is True
    assert document["nextAfterId"] == 3
    assert ("list_items", 1, ("queued",), 0, 3) in service.calls
    assert all(call[1] != 2 for call in service.calls if call[0] == "list_items")

    events = json.loads(
        app.events_document(
            viewer,
            "project-one",
            {"event_size": ["1"], "event_after": ["10"]},
        )
    )
    assert len(events["events"]) == 1
    assert events["events"][0]["payload"] == {"privateProject": 1}
    assert ("list_events", 1, 10, 2) in service.calls


def test_direct_routes_hide_other_project_and_mismatched_global_item(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, service, _operator, viewer, _reserver = scoped_web
    with pytest.raises(V5WebNotFoundError, match="not found"):
        app.render_project(viewer, "project-two", {})
    assert ("get_project", "project-two") not in service.calls

    with pytest.raises(V5WebNotFoundError, match="global id 2"):
        app.render_item(viewer, "project-one", 2)
    assert ("get_item", 1, 2) in service.calls
    assert ("get_item", 2, 2) not in service.calls

    sent: list[tuple[int, bytes]] = []
    handler = V5WebHandler.__new__(V5WebHandler)
    handler.path = "/projects/project-two/items/2"
    handler.server = SimpleNamespace(app=app)
    handler._session = lambda: viewer  # type: ignore[method-assign]
    handler._send = lambda status, body, **_kwargs: sent.append(  # type: ignore[method-assign]
        (status, body)
    )
    handler.do_GET()
    assert sent[0][0] == HTTPStatus.NOT_FOUND
    assert b"project-two" not in sent[0][1]
    assert b"experiment-2" not in sent[0][1]


def test_item_detail_shows_project_revision_artifact_and_yield_evidence(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, _service, _operator, viewer, _reserver = scoped_web
    page = app.render_item(viewer, "project-one", 1).decode("utf-8")
    assert "project-one:r1 (id 1)" in page
    assert "ExperimentCard/v1" in page
    assert "/authorized/1/model.bin" in page
    assert "yield-request-one" in page
    assert "b" * 64 in page
    assert "Queue controls" not in page


def test_terminal_gpu_lease_is_visible_in_scoped_detail_list_and_api(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    """A terminal state cannot be mistaken for a reusable assigned GPU."""

    app, service, _operator, viewer, _reserver = scoped_web
    first, *remaining = service.items[1]
    service.items[1] = (
        replace(
            first,
            state="force_killed",
            assigned_gpu_uuid="GPU-held-private",
            assigned_gpu_index="3",
            runtime_gpu_lease_held=True,
            runtime_gpu_lease_released_at=None,
        ),
        *remaining,
    )

    detail = app.render_item(viewer, "project-one", 1).decode("utf-8")
    assert "GPU-held-private (host index 3)" in detail
    assert "GPU runtime lease" in detail
    assert "not reusable until current idle telemetry is authenticated" in detail
    project = app.render_project(viewer, "project-one", {}).decode("utf-8")
    assert "GPU runtime lease held; current idle telemetry required" in project

    document = json.loads(app.queue_document(viewer, "project-one", {}))
    exported = document["items"][0]
    assert exported["assignedGpuUuid"] == "GPU-held-private"
    assert exported["assignedGpuIndex"] == "3"
    assert exported["runtimeGpuLeaseHeld"] is True
    assert exported["runtimeGpuLeaseReleasedAt"] is None
    assert "project-two" not in json.dumps(document)


def test_item_termination_controls_require_scope_ownership_and_exact_force_token(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, service, operator, viewer, _reserver = scoped_web
    queued_page = app.render_item(operator, "project-one", 1).decode("utf-8")
    assert "Terminate gracefully" not in queued_page
    assert "Force kill" not in queued_page

    first, *remaining = service.items[1]
    service.items[1] = (replace(first, state="starting"), *remaining)
    starting_page = app.render_item(operator, "project-one", 1).decode("utf-8")
    assert "Terminate gracefully" not in starting_page
    assert "Force kill" not in starting_page
    with pytest.raises(V5WebError, match="committed running process identity"):
        app.item_action(
            operator,
            "project-one",
            1,
            {"operation": ["terminate"], "reason": ["pre-launch race"]},
        )
    assert not any(call[0] == "request_termination" for call in service.calls)

    service.items[1] = (replace(first, state="running", pid=4312, pgid=4312), *remaining)
    confirmation = app.force_kill_confirmation("project-one", 1)
    running_page = app.render_item(operator, "project-one", 1).decode("utf-8")
    assert "Terminate gracefully" in running_page
    assert "Force kill" in running_page
    assert confirmation in running_page
    assert 'name="confirmation"' in running_page
    assert 'autocomplete="off"' in running_page

    with pytest.raises(V5WebAuthorizationError, match="project.mutate"):
        app.item_action(
            viewer,
            "project-one",
            1,
            {"operation": ["terminate"], "reason": ["not authorized"]},
        )
    with pytest.raises(V5WebNotFoundError, match="global id 2"):
        app.item_action(
            operator,
            "project-one",
            2,
            {"operation": ["terminate"], "reason": ["wrong Project route"]},
        )
    assert not any(call[0] == "request_termination" for call in service.calls)

    for invalid in ("", confirmation.lower(), confirmation + " ", "FORCE KILL #1"):
        with pytest.raises(V5WebError, match="exact token"):
            app.item_action(
                operator,
                "project-one",
                1,
                {
                    "operation": ["force-kill"],
                    "reason": ["immediate operator stop"],
                    "confirmation": [invalid],
                },
            )
    assert not any(call[0] == "request_termination" for call in service.calls)

    graceful = app.item_action(
        operator,
        "project-one",
        1,
        {
            "operation": ["terminate"],
            "reason": ["graceful operator stop"],
        },
    )
    assert graceful == (
        "queue item #1 graceful termination recorded; "
        "stage interrupt; signal delivered"
    )
    force = app.item_action(
        operator,
        "project-one",
        1,
        {
            "operation": ["force-kill"],
            "reason": ["immediate operator stop"],
            "confirmation": [confirmation],
        },
    )
    assert force == "queue item #1 force kill recorded; stage kill; signal pending"
    calls = [call for call in service.calls if call[0] == "request_termination"]
    assert len(calls) == 2
    assert calls[0][1:3] == (1, "graceful operator stop")
    assert calls[0][3].startswith("web:operator:")
    assert calls[0][4] is False
    assert calls[0][5].endswith("+00:00")
    assert calls[1][1:3] == (1, "immediate operator stop")
    assert calls[1][3] == calls[0][3]
    assert calls[1][4] is True
    assert calls[1][5].endswith("+00:00")


def test_force_kill_http_action_rejects_invalid_csrf_before_scheduler_call(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, service, operator, _viewer, _reserver = scoped_web
    first, *remaining = service.items[1]
    service.items[1] = (replace(first, state="running"), *remaining)
    failures: list[tuple[BaseException, int]] = []
    redirects: list[str] = []
    handler = V5WebHandler.__new__(V5WebHandler)
    handler.path = "/projects/project-one/items/1/actions"
    handler.server = SimpleNamespace(app=app)
    handler._form = lambda: {  # type: ignore[method-assign]
        "csrf": ["invalid-form-token"],
        "operation": ["force-kill"],
        "reason": ["immediate operator stop"],
        "confirmation": [app.force_kill_confirmation("project-one", 1)],
    }
    handler._require_session = lambda **_kwargs: operator  # type: ignore[method-assign]
    handler._bad_request = (  # type: ignore[method-assign]
        lambda error, *, status=400: failures.append((error, status))
    )
    handler._redirect = (  # type: ignore[method-assign]
        lambda location, **_kwargs: redirects.append(location)
    )

    handler.do_POST()

    assert len(failures) == 1
    assert failures[0][1] == HTTPStatus.FORBIDDEN
    assert redirects == []
    assert not any(call[0] == "request_termination" for call in service.calls)


def test_roles_enforce_read_mutation_and_minimized_reserver_views(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, service, operator, viewer, reserver = scoped_web
    with pytest.raises(V5WebAuthorizationError, match="project.mutate"):
        app.item_action(
            viewer,
            "project-one",
            1,
            {"operation": ["hold"], "reason": ["review"]},
        )
    message = app.item_action(
        operator,
        "project-one",
        1,
        {"operation": ["hold"], "reason": ["review"]},
    )
    assert message == "queue item #1 hold recorded"
    assert any(call[0] == "mutate_item" and call[1:5] == (1, 1, "hold", "review") for call in service.calls)

    page = app.render_reserve(reserver).decode("utf-8")
    assert "Test GPU" in page
    assert "busy" in page
    assert "project-one" not in page
    assert "project-two-experiment-2" not in page
    assert "other actors’ reservation notes" in page
    with pytest.raises(V5WebAuthorizationError, match="reservation.mutate"):
        app.reservation_action(viewer, "/reserve/request", {})
    with pytest.raises(V5WebAuthorizationError, match="project.read"):
        app.render_projects(reserver, {})


def _insert_imported_project(
    store: V5QueueStore,
    *,
    project_id: int,
    project_key: str,
    item_id: int,
    checkout: Path,
) -> None:
    """Insert one exact legacy-v4-shaped Project without using operator state."""

    checkout.mkdir()
    commit = f"{project_id:x}" * 40
    enrollment = canonical_json_bytes(
        {
            "apiVersion": "experiment-queue/v1",
            "kind": "LegacyEnrollment",
            "projectKey": project_key,
            "checkoutDirectory": str(checkout),
            "projectManifestPath": None,
            "sourceSchemaVersion": 4,
            "sourceStateIdentitySha256": f"{project_id + 7:x}" * 64,
            "gitCommit": commit,
            "mounts": [],
            "artifactRoots": [],
            "environments": [],
        }
    )
    now = "2026-08-28T12:00:00+00:00"
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO projects(
                id, project_key, display_name, lifecycle, current_revision_id,
                current_revision_sequence, created_at, created_by,
                lifecycle_changed_at, lifecycle_actor, lifecycle_reason
            ) VALUES (?, ?, ?, 'paused', ?, 1, ?, 'test:importer', ?,
                      'test:importer', 'imported pending typed adoption')
            """,
            (project_id, project_key, f"{project_key} private", project_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO project_revisions(
                id, project_id, sequence, revision_label, revision_kind,
                display_name, git_commit, checkout_path, project_manifest_path,
                enrollment_json, enrollment_sha256, created_at, created_actor
            ) VALUES (?, ?, 1, ?, 'legacy-v4', ?, ?, ?, NULL, ?, ?, ?,
                      'test:importer')
            """,
            (
                project_id,
                project_id,
                f"{project_key}:legacy-r1",
                f"{project_key} private",
                commit,
                str(checkout),
                enrollment,
                sha256_bytes(enrollment),
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO project_runtime_state(
                project_id, health, circuit_failure_count, health_reason,
                health_actor, health_changed_at
            ) VALUES (?, 'closed', 0, 'healthy imported state',
                      'test:importer', ?)
            """,
            (project_id, now),
        )
        connection.execute(
            """
            INSERT INTO queue_items(
                id, project_id, revision_id, admission_kind, snapshot_id,
                job_id, experiment_id, attempt, state, priority, card_path,
                card_sha256, command_text, runner_name, git_commit, added_at,
                added_by
            ) VALUES (?, ?, ?, 'LegacyMarkdownCard/v0', NULL, NULL, ?, 1,
                      'queued', 20, ?, ?, ?, ?, ?, ?, 'test:importer')
            """,
            (
                item_id,
                project_id,
                project_id,
                f"{project_key}-scientific-secret",
                f"docs/{project_key}.md",
                f"{project_id + 2:x}" * 64,
                f"python run-{project_key}.py",
                f"{project_key}-runner",
                commit,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO events(
                created_at, actor, event_type, queue_item_id, payload_json,
                scope, project_id
            ) VALUES (?, 'test:importer', 'legacy_imported', ?, ?, 'project', ?)
            """,
            (
                now,
                item_id,
                canonical_json_bytes({"privateProject": project_key}).decode("utf-8"),
                project_id,
            ),
        )
        connection.execute(
            """
            INSERT INTO job_artifacts(
                queue_item_id, project_id, revision_id, segment, evidence_kind,
                artifact_name, artifact_type, absolute_path, recorded_at
            ) VALUES (?, ?, ?, 1, 'legacy-v4', 'legacy-output', 'directory', ?, ?)
            """,
            (
                item_id,
                project_id,
                project_id,
                str(checkout / "private-output"),
                now,
            ),
        )


def test_actual_v5_repository_keeps_imported_projects_isolated_through_web(
    tmp_path: Path,
) -> None:
    store = V5QueueStore(tmp_path / "state")
    store.initialize()
    _insert_imported_project(
        store,
        project_id=1,
        project_key="project-one",
        item_id=1,
        checkout=tmp_path / "checkout-one",
    )
    _insert_imported_project(
        store,
        project_id=2,
        project_key="project-two",
        item_id=2,
        checkout=tmp_path / "checkout-two",
    )
    auth = V5AuthManager(
        initialize_v5_web_auth(
            store.state_dir,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_OPERATOR: "project-operator-secret",
                ROLE_VIEWER: "project-viewer-secret",
            },
            project_scopes={
                ROLE_OPERATOR: ["project-one"],
                ROLE_VIEWER: ["project-one"],
            },
        )
    )
    repository = V5OperatorRepository(store)
    app = V5WebApplication(
        V5WebRepositoryAdapter(
            repository,
            V5ReservationService(store),
            V5SchedulerService(store, gpu_provider=lambda: []),
        ),
        auth,
    )
    _token, viewer = auth.issue_session(ROLE_VIEWER)
    _token, operator = auth.issue_session(ROLE_OPERATOR)

    projects = app.render_projects(viewer, {}).decode("utf-8")
    assert "project-one private" in projects
    assert "project-two private" not in projects
    first = app.render_project(viewer, "project-one", {}).decode("utf-8")
    assert "project-one-scientific-secret" in first
    assert "project-two-scientific-secret" not in first
    assert "project-one:legacy-r1" in first
    detail = app.render_item(viewer, "project-one", 1).decode("utf-8")
    assert str(tmp_path / "checkout-one" / "private-output") in detail
    assert str(tmp_path / "checkout-two") not in detail

    with pytest.raises(V5WebNotFoundError, match="not found"):
        app.render_project(viewer, "project-two", {})
    with pytest.raises(V5OperatorNotFoundError, match="global id 2"):
        app.render_item(viewer, "project-one", 2)

    assert "hold recorded" in app.item_action(
        operator,
        "project-one",
        1,
        {"operation": ["hold"], "reason": ["scoped maintenance"]},
    )
    assert repository.get_item(1, project_id=1).item.state == "held"
    assert repository.get_item(2, project_id=2).item.state == "queued"


def test_repository_adapter_delegates_termination_to_scheduler_service(
    tmp_path: Path,
) -> None:
    store = V5QueueStore((tmp_path / "termination-state").resolve())
    store.initialize()
    _insert_imported_project(
        store,
        project_id=1,
        project_key="project-one",
        item_id=51,
        checkout=tmp_path / "termination-checkout",
    )
    started_at = "2026-08-28T12:00:00+00:00"
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', started_at = ?,
                assigned_gpu_uuid = 'GPU-fixture', assigned_gpu_index = '0',
                runtime_gpu_lease_held = 1
            WHERE id = 51
            """,
            (started_at,),
        )
    auth = V5AuthManager(
        initialize_v5_web_auth(
            store.state_dir,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_OPERATOR: "project-operator-secret",
            },
            project_scopes={ROLE_OPERATOR: ["project-one"]},
        )
    )
    repository = V5OperatorRepository(store)
    scheduler = V5SchedulerService(
        store,
        gpu_provider=lambda: [],
        ambient_environment={},
        clock=lambda: "2026-08-28T12:05:00+00:00",
        epoch_clock=lambda: 1_787_920_700.0,
    )
    app = V5WebApplication(
        V5WebRepositoryAdapter(
            repository,
            V5ReservationService(store),
            scheduler,
        ),
        auth,
    )
    _token, operator = auth.issue_session(ROLE_OPERATOR)

    message = app.item_action(
        operator,
        "project-one",
        51,
        {
            "operation": ["terminate"],
            "reason": ["controlled web shutdown"],
        },
    )
    assert message == (
        "queue item #51 graceful termination recorded; "
        "stage interrupt; signal pending"
    )
    item = repository.get_item(51, project_id=1)
    assert item.item.state == "terminating"
    assert item.terminate_reason == "controlled web shutdown"
    assert item.termination_stage == "interrupt"
    events = repository.list_events(project_id=1, after_id=0, limit=100)
    assert [event.event_type for event in events[-2:]] == [
        "TERMINATION_REQUESTED",
        "TERMINATION_SIGNAL_PENDING",
    ]
    assert all(event.actor == app.actor(operator) for event in events[-2:])


def _reservation_web(
    tmp_path: Path,
) -> tuple[
    V5QueueStore,
    V5OperatorRepository,
    V5ReservationService,
    V5WebApplication,
    V5AuthManager,
    V5WebSession,
    V5WebSession,
]:
    """Build isolated typed reservation state without starting an HTTP server."""

    store = V5QueueStore((tmp_path / "reservation-state").resolve())
    store.initialize()
    _insert_imported_project(
        store,
        project_id=1,
        project_key="private-project",
        item_id=41,
        checkout=tmp_path / "reservation-checkout",
    )
    repository = V5OperatorRepository(store)
    changed_at = "2026-08-28T00:00:00+00:00"
    for index, uuid, name in (
        ("0", "GPU-busy-private-uuid", "Busy Fixture GPU"),
        ("1", "GPU-other-private-uuid", "Reserved Fixture GPU"),
        ("2", "GPU-one-hour-private-uuid", "One Hour Fixture GPU"),
        ("3", "GPU-day-private-uuid", "Day Fixture GPU"),
        ("4", "GPU-rate-private-uuid", "Rate Fixture GPU"),
        ("5", "GPU-disabled-private-uuid", "Disabled Fixture GPU"),
        ("6", "GPU-draining-private-uuid", "Draining Fixture GPU"),
    ):
        repository.add_gpu(
            uuid=uuid,
            requested_identifier=uuid,
            last_index=index,
            name=name,
            actor="test:host-admin",
            changed_at=changed_at,
        )
    repository.disable_gpu(
        "GPU-disabled-private-uuid",
        actor="test:host-admin",
        changed_at=changed_at,
    )
    repository.drain_gpu(
        "GPU-draining-private-uuid",
        actor="test:host-admin",
        changed_at=changed_at,
    )
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE queue_items
            SET state = 'running', assigned_gpu_uuid = ?,
                assigned_gpu_index = '0', pid = 9321, pgid = 9321,
                started_at = ?, runtime_gpu_lease_held = 1
            WHERE id = 41
            """,
            ("GPU-busy-private-uuid", changed_at),
        )
    auth = V5AuthManager(
        initialize_v5_web_auth(
            store.state_dir,
            role_passwords={
                ROLE_HOST_ADMIN: "host-administrator-secret",
                ROLE_RESERVER: "gpu-reserver-secret",
            },
        )
    )
    reservations = V5ReservationService(store)
    app = V5WebApplication(
        V5WebRepositoryAdapter(
            repository,
            reservations,
            V5SchedulerService(store, gpu_provider=lambda: []),
        ),
        auth,
    )
    _token, reserver = auth.issue_session(ROLE_RESERVER)
    _token, administrator = auth.issue_session(ROLE_HOST_ADMIN)
    return (
        store,
        repository,
        reservations,
        app,
        auth,
        reserver,
        administrator,
    )


def test_typed_reservation_page_hides_other_and_process_details_and_scopes_release(
    tmp_path: Path,
) -> None:
    (
        _store,
        _repository,
        reservations,
        app,
        auth,
        reserver,
        administrator,
    ) = _reservation_web(tmp_path)
    other = reservations.request_reservation(
        "GPU-other-private-uuid",
        duration_hours=3,
        note="OTHER ACTOR PRIVATE NOTE",
        requested_by="outside:private-person",
        requested_at="2026-08-28T00:01:00+00:00",
    )

    before = app.render_reserve(reserver).decode("utf-8")
    assert "Busy Fixture GPU" in before
    assert "Reserved Fixture GPU" in before
    assert "busy" in before
    assert "reserved" in before
    assert "OTHER ACTOR PRIVATE NOTE" not in before
    assert f"Reservation #{other.id}" not in before
    assert "GPU-other-private-uuid" not in before
    assert "private-project" not in before
    assert "private-project-scientific-secret" not in before
    assert "9321" not in before

    request_token = app._reservation_request_token(  # noqa: SLF001
        reserver, "GPU-busy-private-uuid"
    )
    message = app.reservation_action(
        reserver,
        "/reserve/request",
        {
            "reservation_token": [request_token],
            "hours": ["24"],
            "note": ["Signed reserver overnight work"],
        },
    )
    assert "pending" in message
    actor = app.actor(reserver)
    owned = reservations.list_reservations(requested_by=actor)
    assert len(owned) == 1
    assert owned[0].duration_hours == 24
    assert owned[0].queue_item_id == 41
    assert owned[0].status is V5ReservationStatus.PENDING

    after = app.render_reserve(reserver).decode("utf-8")
    assert "Signed reserver overnight work" in after
    assert f"Reservation #{owned[0].id} · pending" in after
    assert owned[0].requested_at in after
    assert "when GPU clears" in after
    assert "queue item 41" not in after
    assert "GPU-busy-private-uuid" not in after
    assert "outside:private-person" not in after

    _token, relogged_reserver = auth.issue_session(ROLE_RESERVER)
    assert app.actor(relogged_reserver) == actor
    assert "Signed reserver overnight work" in app.render_reserve(
        relogged_reserver
    ).decode("utf-8")
    with pytest.raises(V5WebNotFoundError, match="not found"):
        app.reservation_action(
            relogged_reserver,
            "/reserve/release",
            {"reservation_id": [str(other.id)]},
        )
    assert reservations.get_reservation(other.id).is_open

    assert "released" in app.reservation_action(
        administrator,
        "/reserve/release",
        {"reservation_id": [str(other.id)]},
    )
    assert reservations.get_reservation(other.id).status is V5ReservationStatus.RELEASED
    assert "released" in app.reservation_action(
        relogged_reserver,
        "/reserve/release",
        {"reservation_id": [str(owned[0].id)]},
    )
    assert (
        reservations.get_reservation(owned[0].id).status
        is V5ReservationStatus.RELEASED
    )


def test_reservation_actions_enforce_duration_schedulability_and_rate_limit(
    tmp_path: Path,
) -> None:
    (
        _store,
        _repository,
        reservations,
        app,
        _auth,
        reserver,
        _administrator,
    ) = _reservation_web(tmp_path)

    for uuid, hours in (
        ("GPU-one-hour-private-uuid", 1),
        ("GPU-day-private-uuid", 24),
    ):
        token = app._reservation_request_token(reserver, uuid)  # noqa: SLF001
        app.reservation_action(
            reserver,
            "/reserve/request",
            {
                "reservation_token": [token],
                "hours": [str(hours)],
                "note": [f"duration boundary {hours}"],
            },
        )
    boundary_records = reservations.list_reservations(
        requested_by=app.actor(reserver)
    )
    assert {record.duration_hours for record in boundary_records} == {1, 24}

    invalid_token = app._reservation_request_token(  # noqa: SLF001
        reserver, "GPU-rate-private-uuid"
    )
    with pytest.raises(V5ReservationError, match="whole number"):
        app.reservation_action(
            reserver,
            "/reserve/request",
            {
                "reservation_token": [invalid_token],
                "hours": ["25"],
                "note": ["invalid duration"],
            },
        )
    for uuid in ("GPU-disabled-private-uuid", "GPU-draining-private-uuid"):
        token = app._reservation_request_token(reserver, uuid)  # noqa: SLF001
        with pytest.raises(V5ReservationError, match="disabled|draining"):
            app.reservation_action(
                reserver,
                "/reserve/request",
                {
                    "reservation_token": [token],
                    "hours": ["2"],
                    "note": ["must remain unavailable"],
                },
            )

    limited = V5WebApplication(app.service, app.auth)
    retry_token = limited._reservation_request_token(  # noqa: SLF001
        reserver, "GPU-rate-private-uuid"
    )
    retry_form = {
        "reservation_token": [retry_token],
        "hours": ["2"],
        "note": ["one idempotent browser request"],
    }
    for _index in range(12):
        limited.reservation_action(reserver, "/reserve/request", retry_form)
    with pytest.raises(V5WebRateLimitError, match="too many"):
        limited.reservation_action(reserver, "/reserve/request", retry_form)
    assert len(
        [
            record
            for record in reservations.list_reservations(
                requested_by=app.actor(reserver)
            )
            if record.gpu_uuid == "GPU-rate-private-uuid"
        ]
    ) == 1


def test_reservation_http_csrf_and_errors_do_not_disclose_typed_service_detail(
    tmp_path: Path,
) -> None:
    (
        _store,
        _repository,
        reservations,
        app,
        _auth,
        reserver,
        _administrator,
    ) = _reservation_web(tmp_path)
    other = reservations.request_reservation(
        "GPU-other-private-uuid",
        duration_hours=2,
        note="DO NOT DISCLOSE THIS NOTE",
        requested_by="outside:private-person",
        requested_at="2026-08-28T00:01:00+00:00",
    )
    conflict_token = app._reservation_request_token(  # noqa: SLF001
        reserver, "GPU-other-private-uuid"
    )

    redirects: list[str] = []
    handler = V5WebHandler.__new__(V5WebHandler)
    handler.path = "/reserve/request"
    handler.server = SimpleNamespace(app=app)
    handler._form = lambda: {  # type: ignore[method-assign]
        "csrf": [reserver.csrf],
        "reservation_token": [conflict_token],
        "hours": ["2"],
        "note": ["conflicting request"],
    }
    handler._require_session = lambda **_kwargs: reserver  # type: ignore[method-assign]
    handler._redirect = (  # type: ignore[method-assign]
        lambda location, **_kwargs: redirects.append(location)
    )
    handler.do_POST()
    assert len(redirects) == 1
    assert redirects[0].startswith("/reserve?error=")
    assert "GPU-other-private-uuid" not in redirects[0]
    assert str(other.id) not in redirects[0]
    assert "DO+NOT+DISCLOSE" not in redirects[0]
    assert "outside%3Aprivate-person" not in redirects[0]

    failures: list[tuple[BaseException, int]] = []
    csrf_handler = V5WebHandler.__new__(V5WebHandler)
    csrf_handler.path = "/reserve/release"
    csrf_handler.server = SimpleNamespace(app=app)
    csrf_handler._form = lambda: {  # type: ignore[method-assign]
        "csrf": ["wrong-csrf"],
        "reservation_id": [str(other.id)],
    }
    csrf_handler._require_session = (  # type: ignore[method-assign]
        lambda **_kwargs: reserver
    )
    csrf_handler._bad_request = (  # type: ignore[method-assign]
        lambda error, *, status=400: failures.append((error, status))
    )
    csrf_handler.do_POST()
    assert len(failures) == 1
    assert failures[0][1] == HTTPStatus.FORBIDDEN
    assert reservations.get_reservation(other.id).is_open

    def fail_gpu_read(*, actor: str, include_all: bool):
        del actor, include_all
        raise V5ReservationError(
            "GPU-other-private-uuid is tied to queue item 41 for outside:private-person"
        )

    app.service.list_reserver_gpus = fail_gpu_read  # type: ignore[method-assign]
    responses: list[tuple[int, bytes]] = []
    get_handler = V5WebHandler.__new__(V5WebHandler)
    get_handler.path = "/reserve"
    get_handler.server = SimpleNamespace(app=app)
    get_handler._require_session = (  # type: ignore[method-assign]
        lambda **_kwargs: reserver
    )
    get_handler._send = (  # type: ignore[method-assign]
        lambda status, body, **_kwargs: responses.append((status, body))
    )
    get_handler.do_GET()
    assert responses[0][0] == HTTPStatus.SERVICE_UNAVAILABLE
    assert b"GPU-other-private-uuid" not in responses[0][1]
    assert b"queue item 41" not in responses[0][1]
    assert b"outside:private-person" not in responses[0][1]


def test_v5_web_cli_help_and_startup_refuse_implicit_state_creation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_arg_parser()
    help_text = parser.format_help()
    assert "existing schema-v5 queue" in help_text
    assert "serving never" in help_text
    assert "creates or migrates queue.sqlite3" in help_text
    with pytest.raises(SystemExit) as help_exit:
        parser.parse_args(["serve", "--help"])
    assert help_exit.value.code == 0

    absent = tmp_path / "absent-state"
    assert main(
        [
            "--state-dir",
            str(absent),
            "serve",
            "--host",
            "127.0.0.1",
            "--insecure-http",
        ]
    ) == 2
    assert not absent.exists()
    assert "does not exist" in capsys.readouterr().err
    assert main(
        ["--state-dir", str(absent), "auth-setup"]
    ) == 2
    assert not absent.exists()
    assert "does not exist" in capsys.readouterr().err


def test_v5_web_requires_https_outside_loopback(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
) -> None:
    app, _service, _operator, _viewer, _reserver = scoped_web
    with pytest.raises(V5WebError, match="HTTPS requires"):
        serve_v5_web(
            app,
            host="127.0.0.1",
            port=8443,
            tls_cert=None,
            tls_key=None,
        )
    with pytest.raises(V5WebError, match="loopback"):
        serve_v5_web(
            app,
            host="0.0.0.0",
            port=8443,
            tls_cert=None,
            tls_key=None,
            insecure_http=True,
        )


def test_v5_web_reports_occupied_bind_address(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bind failure is an actionable web error, never a raw traceback."""

    app, _service, _operator, _viewer, _reserver = scoped_web

    class OccupiedServer:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise OSError(48, "Address already in use")

    monkeypatch.setattr(web_v5_module, "V5WebServer", OccupiedServer)
    with pytest.raises(V5WebError, match="could not bind.*port is available"):
        serve_v5_web(
            app,
            host="127.0.0.1",
            port=8443,
            tls_cert=None,
            tls_key=None,
            insecure_http=True,
        )


def test_v5_web_serves_ipv6_loopback_with_ipv6_socket_family(
    scoped_web: tuple[
        V5WebApplication,
        FakeWebService,
        V5WebSession,
        V5WebSession,
        V5WebSession,
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The documented ::1 development listener uses AF_INET6."""

    if not socket.has_ipv6:
        pytest.skip("host Python lacks IPv6 support")
    app, _service, _operator, _viewer, _reserver = scoped_web
    observed: list[tuple[tuple[str, int], int]] = []

    class IPv6Server:
        address_family = socket.AF_INET6

        def __init__(
            self,
            address: tuple[str, int],
            _app: object,
            *,
            secure_cookies: bool,
        ) -> None:
            assert secure_cookies is False
            self.address = address

        def serve_forever(self, *, poll_interval: float) -> None:
            del poll_interval
            observed.append((self.address, self.address_family))

        def server_close(self) -> None:
            pass

    monkeypatch.setattr(
        web_v5_module,
        "V5IPv6WebServer",
        IPv6Server,
    )
    serve_v5_web(
        app,
        host="::1",
        port=8443,
        tls_cert=None,
        tls_key=None,
        insecure_http=True,
    )
    assert observed == [(('::1', 8443), socket.AF_INET6)]
