"""Verify private scheduler-web authentication, roles, and safe actions."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from helmholtz_shared.experiment_queue import GpuSnapshot, QueueError, QueueStore, update_gpu_allowlist
from helmholtz_shared.experiment_queue_web import (
    AUTH_FILENAME,
    AuthManager,
    CLIENT_SCRIPT,
    QueueWebHandler,
    SchedulerWebApp,
    initialize_web_auth,
    serve_web,
)


def _gpu() -> GpuSnapshot:
    return GpuSnapshot(
        index="0",
        uuid="GPU-web-0000",
        name="Web Test GPU",
        memory_total_mib=100_000,
        memory_used_mib=100,
        utilization_percent=0,
        compute_pids=(),
    )


def test_two_role_auth_hashes_passwords_and_signs_expiring_sessions(tmp_path: Path) -> None:
    path = initialize_web_auth(
        tmp_path,
        admin_password="administrator-secret",
        reservation_password="coworker-shared-secret",
    )
    assert path.name == AUTH_FILENAME
    assert path.stat().st_mode & 0o077 == 0
    assert "administrator-secret" not in path.read_text(encoding="utf-8")
    auth = AuthManager(path)
    assert auth.verify_password("admin", "administrator-secret")
    assert auth.verify_password("reservation", "coworker-shared-secret")
    assert not auth.verify_password("reservation", "administrator-secret")

    token, session = auth.issue_session("reservation", now_epoch=100)
    assert auth.verify_session(token, now_epoch=101) == session
    assert auth.verify_session(token + "changed", now_epoch=101) is None
    assert auth.verify_session(token, now_epoch=session.expires_epoch) is None


def test_reservation_page_and_admin_actions_use_same_durable_queue(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    auth_path = initialize_web_auth(
        state_dir,
        admin_password="administrator-secret",
        reservation_password="coworker-shared-secret",
    )
    auth = AuthManager(auth_path)
    app = SchedulerWebApp(store, auth)
    app.gpu_snapshots = lambda: ([_gpu()], None)  # type: ignore[method-assign]
    update_gpu_allowlist(store, "set", ["0"], snapshots=[_gpu()])
    _token, reservation_session = auth.issue_session("reservation")

    page = app.render_reserve(reservation_session, {}).decode("utf-8")
    assert "GPU 0" in page
    assert "24 hours" in page
    assert "Reserved for / note" in page
    assert 'http-equiv="refresh"' not in page
    assert 'data-live-view="reserve"' in page
    assert 'data-live-section="reserve"' in page
    assert 'id="theme-toggle"' in page
    assert 'src="/static/scheduler.js"' in page
    assert "new EventSource" in CLIENT_SCRIPT
    assert "localStorage" in CLIENT_SCRIPT
    reservation_message = app.reservation_action(
        "/reserve/request",
        {
            "gpu_uuid": [_gpu().uuid],
            "hours": ["24"],
            "note": ["Taylor — overnight evaluation"],
        },
    )
    assert "recorded" in reservation_message
    with store.connect() as connection:
        reservation = connection.execute("SELECT * FROM gpu_reservations").fetchone()
    assert reservation["status"] == "active"
    assert reservation["note"] == "Taylor — overnight evaluation"

    assert "paused" in app.admin_action(
        "/admin/dispatch", {"operation": ["pause"], "reason": ["maintenance"]}
    )
    assert store.get_meta("dispatch_paused") == "1"


def test_live_sections_change_with_durable_queue_revision_and_enforce_role(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    app.gpu_snapshots = lambda: ([_gpu()], None)  # type: ignore[method-assign]
    update_gpu_allowlist(store, "set", ["0"], snapshots=[_gpu()])
    _admin_token, admin_session = auth.issue_session("admin")
    _reservation_token, reservation_session = auth.issue_session("reservation")

    before = app.live_revision()
    sections = app.live_sections("admin", admin_session)
    assert set(sections) == {"dispatch", "gpus", "queue", "reservations", "events"}
    assert "Dispatch active" in sections["dispatch"]
    app.admin_action(
        "/admin/dispatch", {"operation": ["pause"], "reason": ["live test"]}
    )
    assert app.live_revision() > before
    assert "Dispatch paused" in app.live_sections("admin", admin_session)["dispatch"]
    with pytest.raises(QueueError, match="cannot subscribe"):
        app.live_sections("admin", reservation_session)


def test_authenticated_event_stream_pushes_status_without_page_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    _token, session = auth.issue_session("reservation")

    class OneUpdateApp:
        calls = 0

        def live_revision(self) -> int:
            self.calls += 1
            if self.calls > 1:
                raise QueueError("end test stream")
            return 7

        @staticmethod
        def live_sections(view: str, current_session: object) -> dict[str, str]:
            assert view == "reserve"
            assert current_session == session
            return {"reserve": "<section>live status</section>"}

    monkeypatch.setattr(
        "helmholtz_shared.experiment_queue_web.LIVE_POLL_SECONDS", 0.0
    )
    handler = QueueWebHandler.__new__(QueueWebHandler)
    handler.server = SimpleNamespace(app=OneUpdateApp())
    handler.wfile = BytesIO()
    handler.send_response = lambda _status: None
    handler.send_header = lambda _name, _value: None
    handler.end_headers = lambda: None
    handler._send_live_events("reserve", session)
    event_stream = handler.wfile.getvalue()
    assert b"retry: 2000\n\n" in event_stream
    assert b"event: status\n" in event_stream
    assert b"id: 7\n" in event_stream
    assert b'\"reserve\":\"<section>live status</section>\"' in event_stream
    assert handler.close_connection is True


def test_https_is_required_except_for_explicit_loopback_testing(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    with pytest.raises(QueueError, match="HTTPS requires"):
        serve_web(
            app,
            host="127.0.0.1",
            port=8443,
            tls_cert=None,
            tls_key=None,
        )
    with pytest.raises(QueueError, match="loopback"):
        serve_web(
            app,
            host="0.0.0.0",
            port=8443,
            tls_cert=None,
            tls_key=None,
            insecure_http=True,
        )
