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


def _insert_run(
    store: QueueStore,
    *,
    item_id: int,
    state: str,
    run_dir: Path | None = None,
    rsync_command: str | None = None,
) -> None:
    """Insert a minimal durable queue item for read-only web rendering tests."""

    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO queue_items(
                id, experiment_id, attempt, state, priority, card_path, card_sha256,
                command_text, runner_name, git_commit, added_at, added_by,
                runner_run_dir, runner_manifest_path, rsync_pull_command
            ) VALUES (?, ?, 1, ?, 20, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                f"TST-{item_id:03d}",
                state,
                f"docs/experiments/TST-{item_id:03d}.md",
                "a" * 64,
                "python scripts/run_experiment.py --name test",
                f"test-{item_id}",
                "b" * 40,
                "2026-08-09T12:00:00+00:00",
                "test",
                str(run_dir) if run_dir is not None else None,
                str(run_dir / "manifest.json") if run_dir is not None else None,
                rsync_command,
            ),
        )
        store._event(
            connection,
            "EXPERIMENT_TEST_EVENT",
            queue_item_id=item_id,
            payload={"state": state},
            actor="test",
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
    assert 'data-dispatch-paused="false"' in sections["dispatch"]
    app.admin_action(
        "/admin/dispatch", {"operation": ["pause"], "reason": ["live test"]}
    )
    assert app.live_revision() > before
    paused_dispatch = app.live_sections("admin", admin_session)["dispatch"]
    assert "Dispatch paused" in paused_dispatch
    assert 'data-dispatch-paused="true"' in paused_dispatch
    with pytest.raises(QueueError, match="cannot subscribe"):
        app.live_sections("admin", reservation_session)


def test_admin_run_page_shows_logs_and_copyable_rsync_command(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    run_dir = repo_root / "outputs" / "experiments" / "test-run"
    run_dir.mkdir(parents=True)
    (run_dir / "stdout.log").write_text(
        "progress\r\x1b[32mfinished\x1b[0m\n<script>unsafe</script>\n",
        encoding="utf-8",
    )
    (run_dir / "stderr.log").write_text(
        "stderr should stay hidden\n",
        encoding="utf-8",
    )
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    command = "rsync -avh mutton2:'/remote/run & data/' '/local/output/'"
    _insert_run(
        store,
        item_id=1,
        state="succeeded",
        run_dir=run_dir,
        rsync_command=command,
    )
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    app.gpu_snapshots = lambda: ([], None)  # type: ignore[method-assign]
    _token, admin_session = auth.issue_session("admin")
    _reservation_token, reservation_session = auth.issue_session("reservation")

    admin_page = app.render_admin(admin_session, {}).decode("utf-8")
    assert 'href="/admin/runs/1"' in admin_page
    assert 'id="queue-search"' in admin_page
    assert 'id="queue-state-filter"' in admin_page
    assert 'id="queue-gpu-filter"' in admin_page
    assert 'id="queue-sort"' in admin_page
    assert 'id="queue-reset"' in admin_page
    assert 'data-queue-row' in admin_page
    assert 'data-state="succeeded"' in admin_page
    assert 'data-state-group="terminal"' in admin_page
    assert "scheduler priority and dispatch order are unchanged" in admin_page
    assert "body[data-dispatch-paused=true]" in admin_page
    assert "--paused-bg:#200b0b" in admin_page
    assert "--paused-bg:#fff0ef" in admin_page
    page = app.render_run(admin_session, 1).decode("utf-8")
    assert 'data-live-view="run-1"' in page
    assert "finished" in page
    assert "&lt;script&gt;unsafe&lt;/script&gt;" in page
    assert "stderr should stay hidden" not in page
    assert ">Stderr<" not in page
    assert page.count('class="panel log-card"') == 1
    assert "\x1b[32m" not in page
    assert "Copy rsync command to clipboard" in page
    assert "data-copy-target=\"rsync-command\"" in page
    assert "&amp; data" in page
    assert "EXPERIMENT_TEST_EVENT" in page
    assert "navigator.clipboard" in CLIENT_SCRIPT
    assert "/events/admin/runs/" in CLIENT_SCRIPT
    assert "applyQueueView({ refreshGpus: true })" in CLIENT_SCRIPT
    assert 'name === "queue"' in CLIENT_SCRIPT
    assert "syncDispatchAppearance" in CLIENT_SCRIPT
    assert 'name === "dispatch"' in CLIENT_SCRIPT
    assert set(app.live_sections("run-1", admin_session)) == {"run"}
    with pytest.raises(QueueError, match="cannot subscribe"):
        app.live_sections("run-1", reservation_session)


def test_running_run_page_falls_back_to_combined_launcher_output(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    _insert_run(store, item_id=2, state="running")
    launcher = state_dir / "attempts" / "2" / "segments" / "1" / "launcher.log"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("live startup output\n", encoding="utf-8")
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    _token, admin_session = auth.issue_session("admin")

    page = app.render_run(admin_session, 2).decode("utf-8")
    assert "live startup output" in page
    assert "Combined queue launcher output" in page
    assert "The runner has not recorded an rsync command yet." in page
    assert "Copy rsync command to clipboard" in page
    assert "disabled" in page
    assert 'data-state-group="active"' in app.live_sections(
        "admin", admin_session
    )["queue"]


def test_authenticated_run_detail_route_renders_the_requested_item(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    _insert_run(store, item_id=3, state="queued")
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    _token, admin_session = auth.issue_session("admin")
    sent: list[tuple[int, bytes]] = []
    handler = QueueWebHandler.__new__(QueueWebHandler)
    handler.path = "/admin/runs/3"
    handler.server = SimpleNamespace(app=app)
    handler._require = lambda role: admin_session if role == "admin" else None  # type: ignore[method-assign]
    handler._send = lambda status, body, **_kwargs: sent.append((status, body))  # type: ignore[method-assign]

    handler.do_GET()

    assert len(sent) == 1
    assert sent[0][0] == 200
    assert b"TST-003" in sent[0][1]
    assert b'data-live-view="run-3"' in sent[0][1]


def test_run_page_does_not_read_logs_outside_the_queue_repository(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    outside = tmp_path / "outside-run"
    outside.mkdir()
    (outside / "stdout.log").write_text("host secret\n", encoding="utf-8")
    state_dir = repo_root / "gpu_scheduler_state"
    store = QueueStore(state_dir, repo_root)
    _insert_run(store, item_id=4, state="failed", run_dir=outside)
    auth = AuthManager(
        initialize_web_auth(
            state_dir,
            admin_password="administrator-secret",
            reservation_password="coworker-shared-secret",
        )
    )
    app = SchedulerWebApp(store, auth)
    _token, admin_session = auth.issue_session("admin")

    page = app.render_run(admin_session, 4).decode("utf-8")

    assert "outside this repository" in page
    assert "host secret" not in page


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
