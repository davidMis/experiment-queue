"""Serve the private HTTPS control surface for the unmanaged-GPU queue."""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import getpass
import hashlib
import hmac
import html
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import ssl
import tempfile
import time
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlencode, urlparse

from helmholtz_shared.experiment_queue import (
    MAX_RESERVATION_HOURS,
    MIN_RESERVATION_HOURS,
    PENDING_STATES,
    RUNNING_STATES,
    GpuSnapshot,
    QueueError,
    QueueStore,
    add_experiment,
    expire_reservations,
    hold_item,
    query_gpus,
    release_gpu_reservation,
    release_item,
    remove_item,
    request_gpu_reservation,
    request_termination,
    set_dispatch_paused,
    set_priority,
    update_gpu_allowlist,
    utc_now_iso,
)


AUTH_FILENAME = "web_auth.json"
SESSION_COOKIE = "mutton_scheduler_session"
SESSION_SECONDS = 12 * 60 * 60
MAX_FORM_BYTES = 32 * 1024
LOGIN_WINDOW_SECONDS = 5 * 60
LOGIN_MAX_FAILURES = 8
LIVE_POLL_SECONDS = 0.5
LIVE_TELEMETRY_SECONDS = 10.0
LIVE_KEEPALIVE_SECONDS = 15.0


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _atomic_write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write authentication state atomically with owner-only permissions."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary = Path(name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _password_record(password: str) -> dict[str, Any]:
    if len(password) < 12:
        raise QueueError("web passwords must contain at least 12 characters")
    salt = secrets.token_bytes(16)
    n, r, p = 2**14, 8, 1
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p)
    return {
        "algorithm": "scrypt",
        "salt": _b64encode(salt),
        "digest": _b64encode(digest),
        "n": n,
        "r": r,
        "p": p,
    }


def initialize_web_auth(
    state_dir: Path,
    *,
    admin_password: str,
    reservation_password: str,
) -> Path:
    """Create or replace the two-role password and session-signing configuration."""

    if hmac.compare_digest(admin_password, reservation_password):
        raise QueueError("administrator and coworker passwords must be different")
    path = state_dir.resolve() / AUTH_FILENAME
    payload = {
        "schema_version": 1,
        "created_at": utc_now_iso(),
        "auth_version": secrets.token_hex(16),
        "session_secret": _b64encode(secrets.token_bytes(32)),
        "roles": {
            "admin": _password_record(admin_password),
            "reservation": _password_record(reservation_password),
        },
    }
    _atomic_write_private_json(path, payload)
    return path


@dataclass(frozen=True)
class WebSession:
    """Authenticated role and CSRF identity carried by a signed cookie."""

    role: str
    csrf: str
    expires_epoch: int


class AuthManager:
    """Verify password hashes and issue stateless, signed browser sessions."""

    def __init__(self, path: Path):
        self.path = path.resolve()
        try:
            self.config = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise QueueError(
                f"web authentication is not configured at {self.path}; run auth setup first"
            ) from exc
        if self.config.get("schema_version") != 1:
            raise QueueError(f"unsupported web-auth schema in {self.path}")
        if self.path.stat().st_mode & 0o077:
            raise QueueError(
                f"web-auth file must be owner-only (chmod 600): {self.path}"
            )
        try:
            self.secret = _b64decode(str(self.config["session_secret"]))
            self.auth_version = str(self.config["auth_version"])
        except (KeyError, ValueError) as exc:
            raise QueueError(f"web-auth configuration is incomplete: {self.path}") from exc

    def verify_password(self, role: str, password: str) -> bool:
        record = self.config.get("roles", {}).get(role)
        if not isinstance(record, dict) or record.get("algorithm") != "scrypt":
            return False
        try:
            actual = hashlib.scrypt(
                password.encode("utf-8"),
                salt=_b64decode(str(record["salt"])),
                n=int(record["n"]),
                r=int(record["r"]),
                p=int(record["p"]),
            )
            expected = _b64decode(str(record["digest"]))
        except (KeyError, TypeError, ValueError):
            return False
        return hmac.compare_digest(actual, expected)

    def issue_session(self, role: str, *, now_epoch: int | None = None) -> tuple[str, WebSession]:
        if role not in {"admin", "reservation"}:
            raise QueueError(f"unknown web role {role!r}")
        now = int(time.time()) if now_epoch is None else int(now_epoch)
        session = WebSession(
            role=role,
            csrf=secrets.token_urlsafe(24),
            expires_epoch=now + SESSION_SECONDS,
        )
        payload = {
            "role": session.role,
            "csrf": session.csrf,
            "exp": session.expires_epoch,
            "version": self.auth_version,
        }
        encoded = _b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        signature = _b64encode(
            hmac.new(self.secret, encoded.encode("ascii"), hashlib.sha256).digest()
        )
        return f"{encoded}.{signature}", session

    def verify_session(
        self,
        token: str | None,
        *,
        now_epoch: int | None = None,
    ) -> WebSession | None:
        if not token or "." not in token:
            return None
        encoded, signature = token.rsplit(".", 1)
        expected = _b64encode(
            hmac.new(self.secret, encoded.encode("ascii"), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            payload = json.loads(_b64decode(encoded))
            expires = int(payload["exp"])
            role = str(payload["role"])
            csrf = str(payload["csrf"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        now = int(time.time()) if now_epoch is None else int(now_epoch)
        if (
            expires <= now
            or role not in {"admin", "reservation"}
            or payload.get("version") != self.auth_version
            or not csrf
        ):
            return None
        return WebSession(role=role, csrf=csrf, expires_epoch=expires)


def _escape(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _field(form: Mapping[str, list[str]], name: str, default: str = "") -> str:
    values = form.get(name)
    return values[-1] if values else default


def _integer(value: str, *, label: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise QueueError(f"{label} must be a whole number") from exc


def _format_remaining(expires_at: str | None) -> str:
    if not expires_at:
        return "starts when the GPU is clear"
    remaining = _parse_web_timestamp(expires_at).timestamp() - time.time()
    if remaining <= 0:
        return "expiring now"
    hours, remainder = divmod(int(remaining), 3600)
    minutes = remainder // 60
    return f"{hours}h {minutes:02d}m remaining"


def _parse_web_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


STYLE = """
:root{color-scheme:dark;--bg:#101311;--glow:#1d2a22;--panel:#171c19;--panel2:#1d2420;--field:#0e120f;--line:#334039;--text:#f2f5f0;--muted:#aab5ae;--green:#9ee37d;--amber:#f0c36a;--red:#ff8c82;--blue:#8dc7ff;--button:#263d2d;--button-hover:#31503a;--secondary:#202824;--danger:#4b2525;--danger-line:#83403b;--pill:#253029;--ok-bg:#17301d;--ok-line:#315c3b;--error-text:#ffd1cd;--error-bg:#3a2020;--error-line:#78403d;--shadow:#0004}
:root[data-theme=light]{color-scheme:light;--bg:#f4f7f2;--glow:#dfeee2;--panel:#fff;--panel2:#f7faf6;--field:#fff;--line:#c8d4ca;--text:#172019;--muted:#5d6b61;--green:#347a2e;--amber:#9a6414;--red:#b23932;--blue:#1769aa;--button:#dcecdf;--button-hover:#cce3d1;--secondary:#edf2ed;--danger:#f7dddd;--danger-line:#d8a09c;--pill:#e7eee8;--ok-bg:#e3f3e4;--ok-line:#a8cea9;--error-text:#7d211d;--error-bg:#f9e1df;--error-line:#d9a4a0;--shadow:#253a2b1c}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top left,var(--glow) 0,var(--bg) 42%);color:var(--text);font:15px/1.45 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;transition:background-color .18s,color .18s}
a{color:var(--blue)}.shell{max-width:1180px;margin:0 auto;padding:28px 20px 64px}.top{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:24px}.eyebrow{text-transform:uppercase;letter-spacing:.14em;color:var(--green);font-size:12px;font-weight:700}.title{font-size:clamp(28px,5vw,48px);line-height:1.02;margin:8px 0}.subtitle{color:var(--muted);max-width:680px}.nav{display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap}.nav form{margin:0}.panel,.gpu,.login{background:linear-gradient(145deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:16px;box-shadow:0 18px 50px var(--shadow)}.panel{padding:20px;margin:18px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:14px}.gpu{padding:18px;position:relative;overflow:hidden}.gpu:before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--line)}.gpu.available:before{background:var(--green)}.gpu.busy:before{background:var(--amber)}.gpu.reserved:before{background:var(--blue)}.gpu.danger:before{background:var(--red)}.gpu h3{margin:0 0 4px;font-size:20px}.meta{color:var(--muted);font-size:13px}.status{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:999px;padding:4px 9px;margin:10px 0;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.06em}.dot{width:7px;height:7px;border-radius:50%;background:currentColor}.available .status{color:var(--green)}.busy .status{color:var(--amber)}.reserved .status{color:var(--blue)}.danger .status{color:var(--red)}form{margin:12px 0 0}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:end}.field{display:flex;flex-direction:column;gap:5px;min-width:120px;flex:1}.field label{font-size:12px;color:var(--muted);font-weight:700}input,select,button,textarea{font:inherit}input,select,textarea{width:100%;color:var(--text);background:var(--field);border:1px solid var(--line);border-radius:9px;padding:9px 10px}textarea{min-height:72px;resize:vertical}button,.button{border:1px solid var(--line);background:var(--button);color:var(--text);border-radius:9px;padding:9px 13px;font-weight:750;cursor:pointer;text-decoration:none;display:inline-block}button:hover,.button:hover{background:var(--button-hover)}button.secondary,.button.secondary{background:var(--secondary);border-color:var(--line)}button.danger{background:var(--danger);border-color:var(--danger-line)}button:disabled{opacity:.5;cursor:not-allowed}.flash{padding:12px 14px;border-radius:10px;margin:14px 0;border:1px solid}.flash.ok{color:var(--green);background:var(--ok-bg);border-color:var(--ok-line)}.flash.error{color:var(--error-text);background:var(--error-bg);border-color:var(--error-line)}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}.actions{display:flex;gap:6px;flex-wrap:wrap}.actions form{margin:0}.actions button{padding:6px 9px;font-size:12px}.pill{display:inline-block;padding:3px 7px;border-radius:999px;background:var(--pill);color:var(--muted);font-size:11px}.login{max-width:440px;margin:12vh auto 0;padding:28px}.login h1{margin-top:0}.split{display:grid;grid-template-columns:1fr 1fr;gap:18px}.muted{color:var(--muted)}.tiny{font-size:12px}.event{display:grid;grid-template-columns:150px 210px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid var(--line);font-size:12px}.theme-corner{position:fixed;right:18px;top:18px;z-index:2}.live-indicator{display:inline-flex;align-items:center;gap:7px;color:var(--amber);border:1px solid var(--line);border-radius:999px;padding:8px 11px;font-size:12px;font-weight:700;background:var(--panel)}.live-indicator.connected{color:var(--green)}.live-indicator.disconnected{color:var(--red)}.live-indicator .dot{box-shadow:0 0 0 3px color-mix(in srgb,currentColor 18%,transparent)}[data-live-section]{scroll-margin-top:16px}@media(max-width:760px){.top{display:block}.nav{margin-top:18px}.split{grid-template-columns:1fr}table{display:block;overflow-x:auto}.event{grid-template-columns:1fr}.shell{padding:20px 14px 50px}.theme-corner{position:static;margin:14px}}
"""


CLIENT_SCRIPT = r"""
(() => {
  const root = document.documentElement;
  const storageKey = "mutton-scheduler-theme";
  const preferredTheme = () => {
    let saved = null;
    try { saved = localStorage.getItem(storageKey); } catch (_error) { /* preference is optional */ }
    if (saved === "light" || saved === "dark") return saved;
    return matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  };
  const setTheme = (theme) => {
    root.dataset.theme = theme;
    const toggle = document.getElementById("theme-toggle");
    if (toggle) {
      const target = theme === "dark" ? "Light" : "Dark";
      toggle.textContent = `${target} mode`;
      toggle.setAttribute("aria-label", `Switch to ${target.toLowerCase()} mode`);
    }
  };
  setTheme(preferredTheme());
  document.getElementById("theme-toggle")?.addEventListener("click", () => {
    const next = root.dataset.theme === "dark" ? "light" : "dark";
    try { localStorage.setItem(storageKey, next); } catch (_error) { /* keep this page themed */ }
    setTheme(next);
  });

  const view = document.body.dataset.liveView;
  if (!view || !window.EventSource) return;
  const indicator = document.getElementById("live-connection");
  const pending = new Map();
  const setConnection = (state, label) => {
    if (!indicator) return;
    indicator.className = `live-indicator ${state}`;
    const labelNode = indicator.querySelector("span:last-child");
    if (labelNode) labelNode.textContent = label;
  };
  const applySection = (name, markup) => {
    const section = document.querySelector(`[data-live-section="${CSS.escape(name)}"]`);
    if (!section) return;
    if (section.contains(document.activeElement)) {
      pending.set(name, markup);
      return;
    }
    section.innerHTML = markup;
    pending.delete(name);
  };
  document.addEventListener("focusout", () => setTimeout(() => {
    for (const [name, markup] of pending) applySection(name, markup);
  }, 0));

  const stream = new EventSource(`/events/${encodeURIComponent(view)}`);
  stream.onopen = () => setConnection("connected", "Live");
  stream.onerror = () => setConnection("disconnected", "Reconnecting…");
  stream.addEventListener("status", (event) => {
    try {
      const payload = JSON.parse(event.data);
      for (const [name, markup] of Object.entries(payload.sections || {})) {
        applySection(name, String(markup));
      }
      setConnection("connected", "Live");
    } catch (_error) {
      setConnection("disconnected", "Update error");
    }
  });
  stream.addEventListener("session-expired", () => {
    location.assign(view === "admin" ? "/login/admin" : "/login/reservation");
  });
  addEventListener("beforeunload", () => stream.close(), { once: true });
})();
"""


class SchedulerWebApp:
    """Render role-specific pages and execute narrowly scoped scheduler actions."""

    def __init__(self, store: QueueStore, auth: AuthManager, *, nvidia_smi: str = "nvidia-smi"):
        self.store = store
        self.auth = auth
        self.nvidia_smi = nvidia_smi
        self.login_failures: dict[str, list[float]] = {}

    def gpu_snapshots(self) -> tuple[list[GpuSnapshot], str | None]:
        try:
            return query_gpus(self.nvidia_smi), None
        except QueueError as exc:
            return [], str(exc)

    def _data(self) -> dict[str, Any]:
        expire_reservations(self.store)
        snapshots, telemetry_error = self.gpu_snapshots()
        by_uuid = {gpu.uuid: gpu for gpu in snapshots}
        with self.store.connect() as connection:
            allow = [dict(row) for row in connection.execute(
                "SELECT * FROM gpu_allowlist ORDER BY CAST(last_index AS INTEGER), uuid"
            )]
            items = [dict(row) for row in connection.execute(
                "SELECT * FROM queue_items ORDER BY resume_front DESC, priority DESC, id DESC"
            )]
            reservations = [dict(row) for row in connection.execute(
                "SELECT * FROM gpu_reservations ORDER BY id DESC"
            )]
            events = [dict(row) for row in connection.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 50"
            )]
        running_by_gpu = {
            str(item["assigned_gpu_uuid"]): item
            for item in items
            if item["state"] in RUNNING_STATES and item["assigned_gpu_uuid"]
        }
        open_reservations = {
            str(row["gpu_uuid"]): row
            for row in reservations
            if row["status"] in {"pending", "active"}
        }
        return {
            "allow": allow,
            "items": items,
            "reservations": reservations,
            "events": events,
            "by_uuid": by_uuid,
            "running_by_gpu": running_by_gpu,
            "open_reservations": open_reservations,
            "telemetry_error": telemetry_error,
            "dispatch_paused": self.store.get_meta("dispatch_paused") == "1",
            "pause_reason": self.store.get_meta("pause_reason"),
        }

    @staticmethod
    def _page(title: str, body: str, *, live_view: str | None = None) -> bytes:
        live_attribute = f' data-live-view="{_escape(live_view)}"' if live_view else ""
        document = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_escape(title)}</title><style>{STYLE}</style><script src="/static/scheduler.js" defer></script></head>
<body{live_attribute}>{body}</body></html>"""
        return document.encode("utf-8")

    @staticmethod
    def _theme_toggle(*, corner: bool = False) -> str:
        wrapper = ' class="theme-corner"' if corner else ""
        return (
            f"<div{wrapper}><button id=\"theme-toggle\" class=\"secondary\" "
            'type="button" aria-label="Switch color theme">Theme</button></div>'
        )

    @staticmethod
    def _live_indicator() -> str:
        return (
            '<div id="live-connection" class="live-indicator" role="status" '
            'aria-live="polite"><span class="dot"></span><span>Connecting…</span></div>'
        )

    @staticmethod
    def _flash(query: Mapping[str, list[str]]) -> str:
        if query.get("ok"):
            return f'<div class="flash ok">{_escape(query["ok"][-1])}</div>'
        if query.get("error"):
            return f'<div class="flash error">{_escape(query["error"][-1])}</div>'
        return ""

    def render_login(self, role: str, *, error: str | None = None) -> bytes:
        label = "Scheduler administrator" if role == "admin" else "GPU reservation desk"
        error_html = f'<div class="flash error">{_escape(error)}</div>' if error else ""
        body = f"""{self._theme_toggle(corner=True)}<main class="shell"><section class="login"><div class="eyebrow">Mutton2</div>
<h1>{label}</h1><p class="muted">Enter the shared password David provided.</p>{error_html}
<form method="post" action="/login/{role}"><div class="field"><label for="password">Password</label>
<input id="password" name="password" type="password" autocomplete="current-password" required autofocus></div>
<button type="submit">Sign in</button></form></section></main>"""
        return self._page(label, body)

    def _reservation_form(self, csrf: str, gpu_uuid: str, *, label: str) -> str:
        options = "".join(
            f'<option value="{hour}"{" selected" if hour == 2 else ""}>{hour} hour{"s" if hour != 1 else ""}</option>'
            for hour in range(MIN_RESERVATION_HOURS, MAX_RESERVATION_HOURS + 1)
        )
        return f"""<form method="post" action="/reserve/request"><input type="hidden" name="csrf" value="{_escape(csrf)}">
<input type="hidden" name="gpu_uuid" value="{_escape(gpu_uuid)}"><div class="row">
<div class="field"><label>Duration</label><select name="hours">{options}</select></div>
<div class="field"><label>Reserved for / note</label><input name="note" maxlength="200" placeholder="Name — short reason" required></div>
<button type="submit">{_escape(label)}</button></div></form>"""

    def _reserve_sections(self, session: WebSession) -> dict[str, str]:
        """Render the coworker status fragment pushed over the live stream."""

        data = self._data()
        cards: list[str] = []
        for row in data["allow"]:
            gpu_uuid = str(row["uuid"])
            gpu = data["by_uuid"].get(gpu_uuid)
            job = data["running_by_gpu"].get(gpu_uuid)
            reservation = data["open_reservations"].get(gpu_uuid)
            if row["draining"]:
                card_class, status = "danger", "leaving scheduler pool"
                action = '<p class="muted">This device is not accepting reservations.</p>'
            elif reservation:
                card_class, status = "reserved", (
                    "checkpointing" if reservation["status"] == "pending" else "reserved"
                )
                action = f"""<p><strong>{_escape(reservation['note'])}</strong><br>
<span class="muted">{_escape(_format_remaining(reservation['expires_at']))}</span></p>
<form method="post" action="/reserve/release"><input type="hidden" name="csrf" value="{_escape(session.csrf)}">
<input type="hidden" name="reservation_id" value="{reservation['id']}"><button class="secondary" type="submit">Release early</button></form>"""
            elif job:
                card_class, status = "busy", f"running {job['experiment_id']}"
                if job["preemptible"] and job["state"] == "running":
                    action = self._reservation_form(session.csrf, gpu_uuid, label="Yield this GPU")
                else:
                    action = '<p class="muted">This job cannot currently be checkpointed and requeued.</p>'
            elif gpu is None:
                card_class, status = "danger", "telemetry unavailable"
                action = '<p class="muted">Reservation is disabled until the device is observed.</p>'
            elif gpu.compute_pids:
                card_class, status = "busy", "used outside David’s scheduler"
                action = '<p class="muted">The reservation desk cannot stop or reserve another user’s process.</p>'
            else:
                card_class, status = "available", "available"
                action = self._reservation_form(session.csrf, gpu_uuid, label="Reserve GPU")
            telemetry = (
                f"{gpu.memory_used_mib:,.0f} MiB used · {gpu.utilization_percent:.0f}% utilization"
                if gpu else "No current device reading"
            )
            cards.append(f"""<article class="gpu {card_class}"><h3>GPU {_escape(row['last_index'])}</h3>
<div class="meta">{_escape(row['name'])}<br>{_escape(gpu_uuid)}</div><div class="status"><span class="dot"></span>{_escape(status)}</div>
<div class="meta">{_escape(telemetry)}</div>{action}</article>""")
        telemetry_warning = (
            f'<div class="flash error">GPU telemetry is unavailable: {_escape(data["telemetry_error"])}</div>'
            if data["telemetry_error"] else ""
        )
        return {
            "reserve": (
                f'{telemetry_warning}<section class="grid">'
                f"{''.join(cards) or '<p>No GPUs are currently in David’s scheduler pool.</p>'}"
                "</section>"
            )
        }

    def render_reserve(
        self,
        session: WebSession,
        query: Mapping[str, list[str]],
    ) -> bytes:
        sections = self._reserve_sections(session)
        admin_link = '<a class="button secondary" href="/admin">Administrator</a>' if session.role == "admin" else ""
        body = f"""<main class="shell"><header class="top"><div><div class="eyebrow">Shared GPU courtesy desk</div>
<h1 class="title">Need a GPU? Take a window.</h1><p class="subtitle">A running preemptible job will save a continuation checkpoint, leave the device, and return to the front of David’s queue on another available GPU.</p></div>
<nav class="nav">{self._live_indicator()}{self._theme_toggle()}{admin_link}<form method="post" action="/logout"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><button class="secondary">Sign out</button></form></nav></header>
{self._flash(query)}<div data-live-section="reserve">{sections['reserve']}</div>
<section class="panel"><strong>Reservation rules</strong><p class="muted">Choose 1–24 whole hours. The timer for a yielded job starts only after its checkpoint is verified and its GPU process exits. Expiry removes the temporary reservation; normal GPU polling still prevents launch while another process is present.</p></section></main>"""
        return self._page("Mutton2 GPU reservation desk", body, live_view="reserve")

    def _admin_sections(self, session: WebSession) -> dict[str, str]:
        """Render administrator status fragments without replacing data-entry forms."""

        data = self._data()
        pause_action = "resume" if data["dispatch_paused"] else "pause"
        dispatch_text = "Dispatch paused" if data["dispatch_paused"] else "Dispatch active"
        gpu_cards: list[str] = []
        for row in data["allow"]:
            gpu_uuid = str(row["uuid"])
            gpu = data["by_uuid"].get(gpu_uuid)
            job = data["running_by_gpu"].get(gpu_uuid)
            reservation = data["open_reservations"].get(gpu_uuid)
            state = "reserved" if reservation else "busy" if job or (gpu and gpu.compute_pids) else "available"
            detail = reservation["note"] if reservation else job["experiment_id"] if job else "idle"
            gpu_cards.append(f"""<article class="gpu {state}"><h3>GPU {_escape(row['last_index'])}</h3><div class="meta">{_escape(gpu_uuid)}</div>
<div class="status"><span class="dot"></span>{_escape(detail)}</div>
<form method="post" action="/admin/gpu"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="operation" value="remove"><input type="hidden" name="identifiers" value="{_escape(gpu_uuid)}"><button class="danger" type="submit">Remove from pool</button></form></article>""")
        queue_rows: list[str] = []
        for item in data["items"]:
            item_id = int(item["id"])
            actions: list[str] = []
            base = f'<input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="item_id" value="{item_id}">'
            if item["state"] in PENDING_STATES:
                if item["state"] == "held":
                    actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="release"><button>Release</button></form>')
                else:
                    actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="hold"><button class="secondary">Hold</button></form>')
                actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="remove"><button class="danger">Remove</button></form>')
                actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="priority"><input style="width:76px" name="priority" type="number" value="{item["priority"]}" aria-label="Priority"><button>Set</button></form>')
            if item["state"] in RUNNING_STATES:
                actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="terminate"><button class="secondary">Terminate</button></form>')
                actions.append(f'<form method="post" action="/admin/item">{base}<input type="hidden" name="operation" value="kill"><input style="width:88px" name="confirm" placeholder="type KILL" aria-label="Type KILL to confirm force kill" required><button class="danger">Force kill</button></form>')
            queue_rows.append(f"""<tr><td>#{item_id}</td><td><strong>{_escape(item['experiment_id'])}</strong><br><span class="tiny muted">attempt {item['attempt']} · segment {item['segment']}</span></td>
<td><span class="pill">{_escape(item['state'])}</span></td><td>{item['priority']}{' · front' if item['resume_front'] else ''}</td><td>{_escape(item['assigned_gpu_index'] or '—')}</td><td>{_escape(item['state_detail'] or '')}</td><td><div class="actions">{''.join(actions)}</div></td></tr>""")
        event_rows = "".join(
            f'<div class="event"><span>{_escape(row["created_at"])}</span><strong>{_escape(row["event_type"])}</strong><span>{_escape(row["actor"])} · item {_escape(row["queue_item_id"] or "—")}<br>{_escape(row["payload_json"])}</span></div>'
            for row in data["events"]
        )
        reservation_rows = "".join(
            f"<tr><td>#{row['id']}</td><td>{_escape(row['note'])}</td><td><span class=\"pill\">{_escape(row['status'])}</span></td><td>{row['duration_hours']}h</td><td>{_escape(row['starts_at'] or 'waiting for clear')}</td><td>{_escape(row['expires_at'] or '—')}</td></tr>"
            for row in data["reservations"][:30]
        )
        telemetry_warning = (
            f'<div class="flash error">GPU telemetry is unavailable: {_escape(data["telemetry_error"])}</div>'
            if data["telemetry_error"] else ""
        )
        return {
            "dispatch": f"""<section class="panel"><div class="row"><div><strong>{dispatch_text}</strong><div class="muted">{_escape(data['pause_reason'])}</div></div>
<form method="post" action="/admin/dispatch"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="operation" value="{pause_action}"><input name="reason" placeholder="Reason (optional)"><button>{pause_action.title()} dispatch</button></form></div></section>""",
            "gpus": f"{telemetry_warning}<div class=\"grid\">{''.join(gpu_cards) or '<p>No GPUs are enabled.</p>'}</div>",
            "queue": f"""<section class="panel"><h2>Queue</h2><table><thead><tr><th>ID</th><th>Experiment</th><th>State</th><th>Priority</th><th>GPU</th><th>Detail</th><th>Controls</th></tr></thead><tbody>{''.join(queue_rows)}</tbody></table></section>""",
            "reservations": f"""<section class="panel"><h2>Reservation history</h2><table><thead><tr><th>ID</th><th>Reserved for / note</th><th>Status</th><th>Duration</th><th>Started</th><th>Expires</th></tr></thead><tbody>{reservation_rows}</tbody></table></section>""",
            "events": f"""<section class="panel"><h2>Recent audit history</h2>{event_rows or '<p>No events recorded.</p>'}</section>""",
        }

    def live_sections(self, view: str, session: WebSession) -> dict[str, str]:
        """Return role-checked live sections for one authenticated dashboard."""

        if view == "reserve":
            return self._reserve_sections(session)
        if view == "admin" and session.role == "admin":
            return self._admin_sections(session)
        raise QueueError(f"role {session.role!r} cannot subscribe to {view!r} status")

    def live_revision(self) -> int:
        """Return the latest durable event ID used for prompt change detection."""

        with self.store.connect() as connection:
            row = connection.execute("SELECT COALESCE(MAX(id), 0) AS id FROM events").fetchone()
        return int(row["id"])

    def render_admin(
        self,
        session: WebSession,
        query: Mapping[str, list[str]],
    ) -> bytes:
        sections = self._admin_sections(session)
        body = f"""<main class="shell"><header class="top"><div><div class="eyebrow">Mutton2 scheduler</div><h1 class="title">Queue control</h1>
<p class="subtitle">Explicit experiment admission, mutable GPU pool, safe yield reservations, and complete operational history.</p></div><nav class="nav">{self._live_indicator()}{self._theme_toggle()}<a class="button secondary" href="/reserve">Reservation page</a><form method="post" action="/logout"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><button class="secondary">Sign out</button></form></nav></header>
{self._flash(query)}<div data-live-section="dispatch">{sections['dispatch']}</div>
<section><h2>GPU pool</h2><div data-live-section="gpus">{sections['gpus']}</div>
<div class="panel"><form method="post" action="/admin/gpu"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><div class="row"><div class="field"><label>Indices, UUIDs, or UUID prefixes</label><input name="identifiers" placeholder="0 2 GPU-…"></div><button name="operation" value="add">Add GPUs</button><button class="secondary" name="operation" value="set">Replace pool</button><button class="danger" name="operation" value="clear">Clear pool</button></div></form></div></section>
<section class="panel"><h2>Add experiment explicitly</h2><form method="post" action="/admin/add"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><div class="row">
<div class="field"><label>Experiment ID</label><input name="experiment_id" placeholder="WCG-023" required></div><div class="field"><label>Card path (optional)</label><input name="card_path" placeholder="docs/experiments/WCG-023.md"></div><div class="field"><label>Priority</label><input name="priority" type="number" value="0"></div><div class="field"><label>After item IDs</label><input name="dependencies" placeholder="1, 2"></div></div>
<div class="row"><label><input style="width:auto" type="checkbox" name="preemptible" value="1"> Checkpoint and requeue capable</label><label><input style="width:auto" type="checkbox" name="held" value="1"> Add held</label><label><input style="width:auto" type="checkbox" name="new_attempt" value="1"> Authorize new attempt</label><button type="submit">Add to queue</button></div></form></section>
<div data-live-section="queue">{sections['queue']}</div>
<div data-live-section="reservations">{sections['reservations']}</div>
<div data-live-section="events">{sections['events']}</div></main>"""
        return self._page("Mutton2 scheduler", body, live_view="admin")

    def admin_action(self, route: str, form: Mapping[str, list[str]]) -> str:
        actor = "web:admin"
        if route == "/admin/add":
            dependencies = [
                _integer(value.strip(), label="dependency ID")
                for value in _field(form, "dependencies").replace(",", " ").split()
            ]
            card_value = _field(form, "card_path").strip()
            item_id = add_experiment(
                self.store,
                _field(form, "experiment_id"),
                card_path=Path(card_value) if card_value else None,
                priority=_integer(_field(form, "priority", "0"), label="priority"),
                dependency_ids=dependencies,
                held=_field(form, "held") == "1",
                new_attempt=_field(form, "new_attempt") == "1",
                preemptible=_field(form, "preemptible") == "1",
                actor=actor,
            )
            return f"Added queue item {item_id}"
        if route == "/admin/item":
            item_id = _integer(_field(form, "item_id"), label="queue item ID")
            operation = _field(form, "operation")
            if operation == "hold":
                hold_item(self.store, item_id, "held from web dashboard", actor=actor)
            elif operation == "release":
                release_item(self.store, item_id, actor=actor)
            elif operation == "remove":
                remove_item(self.store, item_id, "removed from web dashboard", actor=actor)
            elif operation == "priority":
                set_priority(
                    self.store,
                    item_id,
                    _integer(_field(form, "priority"), label="priority"),
                    actor=actor,
                )
            elif operation == "terminate":
                request_termination(
                    self.store,
                    item_id,
                    reason="administrator web request",
                    force=False,
                    actor=actor,
                )
            elif operation == "kill":
                if _field(form, "confirm") != "KILL":
                    raise QueueError("force kill requires explicit KILL confirmation")
                request_termination(
                    self.store,
                    item_id,
                    reason="administrator force-kill web request",
                    force=True,
                    actor=actor,
                )
            else:
                raise QueueError(f"unsupported queue operation {operation!r}")
            return f"Queue item {item_id}: {operation} recorded"
        if route == "/admin/gpu":
            operation = _field(form, "operation")
            identifiers = _field(form, "identifiers").replace(",", " ").split()
            queue_operation = "set" if operation == "clear" else operation
            if queue_operation not in {"set", "add", "remove"}:
                raise QueueError(f"unsupported GPU-pool operation {operation!r}")
            if operation == "add" and not identifiers:
                raise QueueError("enter at least one GPU before choosing Add GPUs")
            snapshots, error = self.gpu_snapshots()
            if error and operation not in {"remove", "clear"}:
                raise QueueError(f"cannot change GPU pool without telemetry: {error}")
            update_gpu_allowlist(
                self.store,
                queue_operation,
                identifiers,
                snapshots=snapshots,
                actor=actor,
            )
            return f"GPU pool {operation} completed"
        if route == "/admin/dispatch":
            operation = _field(form, "operation")
            if operation not in {"pause", "resume"}:
                raise QueueError(f"unsupported dispatch operation {operation!r}")
            set_dispatch_paused(
                self.store,
                operation == "pause",
                _field(form, "reason").strip() or None,
                actor=actor,
            )
            return f"Dispatch {operation}d"
        raise QueueError(f"unknown administrator action {route}")

    def reservation_action(self, route: str, form: Mapping[str, list[str]]) -> str:
        if route == "/reserve/request":
            snapshots, error = self.gpu_snapshots()
            if error:
                raise QueueError(f"cannot reserve a GPU without telemetry: {error}")
            note = _field(form, "note")
            reservation_id = request_gpu_reservation(
                self.store,
                _field(form, "gpu_uuid"),
                duration_hours=_integer(_field(form, "hours"), label="duration"),
                note=note,
                actor="web:reservation",
                snapshots=snapshots,
            )
            return f"Reservation {reservation_id} recorded for {note.strip()}"
        if route == "/reserve/release":
            reservation_id = _integer(
                _field(form, "reservation_id"), label="reservation ID"
            )
            release_gpu_reservation(
                self.store,
                reservation_id,
                actor="web:reservation",
            )
            return f"Reservation {reservation_id} released"
        raise QueueError(f"unknown reservation action {route}")

    def login_allowed(self, client: str) -> bool:
        now = time.monotonic()
        recent = [stamp for stamp in self.login_failures.get(client, []) if now - stamp < LOGIN_WINDOW_SECONDS]
        self.login_failures[client] = recent
        return len(recent) < LOGIN_MAX_FAILURES

    def login_failed(self, client: str) -> None:
        self.login_failures.setdefault(client, []).append(time.monotonic())

    def login_succeeded(self, client: str) -> None:
        self.login_failures.pop(client, None)


class QueueWebServer(ThreadingHTTPServer):
    """Threaded HTTP server carrying the scheduler web application."""

    daemon_threads = True

    def __init__(self, address: tuple[str, int], app: SchedulerWebApp):
        self.app = app
        super().__init__(address, QueueWebHandler)


class QueueWebHandler(BaseHTTPRequestHandler):
    """Route authenticated HTML requests without exposing shell execution."""

    server: QueueWebServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        print(
            f"[{utc_now_iso()}] web {self.client_address[0]} " + format % args,
            flush=True,
        )

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; "
            "connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Strict-Transport-Security", "max-age=31536000")

    def _send(self, status: int, body: bytes, *, content_type: str = "text/html; charset=utf-8") -> None:
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str, *, cookie: str | None = None) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self._security_headers()
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _session(self) -> WebSession | None:
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        morsel = cookie.get(SESSION_COOKIE)
        return self.server.app.auth.verify_session(morsel.value if morsel else None)

    @staticmethod
    def _cookie(token: str, *, max_age: int = SESSION_SECONDS) -> str:
        return (
            f"{SESSION_COOKIE}={token}; Path=/; Max-Age={max_age}; "
            "Secure; HttpOnly; SameSite=Strict"
        )

    def _require(self, role: str) -> WebSession | None:
        session = self._authorized_session(role)
        if session is None:
            self._redirect(f"/login/{role}")
        return session

    def _authorized_session(self, role: str) -> WebSession | None:
        """Return a session authorized for a page or read-only event stream."""

        session = self._session()
        allowed = session is not None and (
            session.role == "admin" or (role == "reservation" and session.role == "reservation")
        )
        return session if allowed else None

    def _send_live_events(self, view: str, session: WebSession) -> None:
        """Push changed dashboard sections over an authenticated SSE connection."""

        self.send_response(HTTPStatus.OK)
        self._security_headers()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        last_revision: int | None = None
        last_digest: str | None = None
        last_render = 0.0
        last_keepalive = time.monotonic()
        try:
            self.wfile.write(b"retry: 2000\n\n")
            self.wfile.flush()
            while time.time() < session.expires_epoch:
                now = time.monotonic()
                revision = self.server.app.live_revision()
                if (
                    last_revision is None
                    or revision != last_revision
                    or now - last_render >= LIVE_TELEMETRY_SECONDS
                ):
                    sections = self.server.app.live_sections(view, session)
                    encoded = json.dumps(
                        {"sections": sections},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    digest = hashlib.sha256(encoded).hexdigest()
                    if digest != last_digest:
                        self.wfile.write(f"event: status\nid: {revision}\ndata: ".encode("utf-8"))
                        self.wfile.write(encoded)
                        self.wfile.write(b"\n\n")
                        self.wfile.flush()
                        last_digest = digest
                    last_revision = revision
                    last_render = now
                    last_keepalive = now
                elif now - last_keepalive >= LIVE_KEEPALIVE_SECONDS:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    last_keepalive = now
                time.sleep(LIVE_POLL_SECONDS)
            self.wfile.write(b"event: session-expired\ndata: {}\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError, QueueError):
            pass
        finally:
            self.close_connection = True

    def _form(self) -> dict[str, list[str]]:
        content_type = self.headers.get("Content-Type", "")
        if not content_type.startswith("application/x-www-form-urlencoded"):
            raise QueueError("web actions require form-encoded requests")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise QueueError("invalid request length") from exc
        if length <= 0 or length > MAX_FORM_BYTES:
            raise QueueError(f"request body must be between 1 and {MAX_FORM_BYTES} bytes")
        return parse_qs(self.rfile.read(length).decode("utf-8"), keep_blank_values=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path == "/healthz":
            self._send(HTTPStatus.OK, b"ok\n", content_type="text/plain; charset=utf-8")
            return
        if parsed.path == "/static/scheduler.js":
            self._send(
                HTTPStatus.OK,
                CLIENT_SCRIPT.encode("utf-8"),
                content_type="text/javascript; charset=utf-8",
            )
            return
        if parsed.path in {"/events/admin", "/events/reserve"}:
            view = parsed.path.rsplit("/", 1)[-1]
            role = "admin" if view == "admin" else "reservation"
            session = self._authorized_session(role)
            if session is None:
                self._send(
                    HTTPStatus.UNAUTHORIZED,
                    b"authentication required\n",
                    content_type="text/plain; charset=utf-8",
                )
                return
            self._send_live_events(view, session)
            return
        if parsed.path == "/":
            session = self._session()
            self._redirect("/admin" if session and session.role == "admin" else "/reserve")
            return
        if parsed.path in {"/login/admin", "/login/reservation"}:
            role = parsed.path.rsplit("/", 1)[-1]
            self._send(HTTPStatus.OK, self.server.app.render_login(role))
            return
        if parsed.path == "/reserve":
            session = self._require("reservation")
            if session:
                self._send(HTTPStatus.OK, self.server.app.render_reserve(session, query))
            return
        if parsed.path == "/admin":
            session = self._require("admin")
            if session:
                self._send(HTTPStatus.OK, self.server.app.render_admin(session, query))
            return
        self._send(HTTPStatus.NOT_FOUND, self.server.app._page("Not found", '<main class="shell"><h1>Not found</h1></main>'))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            form = self._form()
        except QueueError as exc:
            self._send(HTTPStatus.BAD_REQUEST, self.server.app._page("Bad request", f'<main class="shell"><div class="flash error">{_escape(exc)}</div></main>'))
            return
        if parsed.path in {"/login/admin", "/login/reservation"}:
            role = parsed.path.rsplit("/", 1)[-1]
            client = self.client_address[0]
            if not self.server.app.login_allowed(client):
                self._send(HTTPStatus.TOO_MANY_REQUESTS, self.server.app.render_login(role, error="Too many failed attempts; wait five minutes."))
                return
            if not self.server.app.auth.verify_password(role, _field(form, "password")):
                self.server.app.login_failed(client)
                self._send(HTTPStatus.UNAUTHORIZED, self.server.app.render_login(role, error="Incorrect password."))
                return
            self.server.app.login_succeeded(client)
            token, _session = self.server.app.auth.issue_session(role)
            self._redirect("/admin" if role == "admin" else "/reserve", cookie=self._cookie(token))
            return
        session = self._session()
        if session is None:
            self._redirect("/login/reservation")
            return
        if not hmac.compare_digest(_field(form, "csrf"), session.csrf):
            self._send(HTTPStatus.FORBIDDEN, self.server.app._page("Forbidden", '<main class="shell"><div class="flash error">Invalid form token. Refresh the page and try again.</div></main>'))
            return
        if parsed.path == "/logout":
            self._redirect("/login/reservation", cookie=self._cookie("deleted", max_age=0))
            return
        try:
            if parsed.path.startswith("/admin/"):
                if session.role != "admin":
                    raise QueueError("administrator access is required")
                message = self.server.app.admin_action(parsed.path, form)
                destination = "/admin"
            elif parsed.path.startswith("/reserve/"):
                message = self.server.app.reservation_action(parsed.path, form)
                destination = "/reserve"
            else:
                raise QueueError(f"unknown web action {parsed.path}")
        except (QueueError, OSError, ValueError) as exc:
            destination = "/admin" if parsed.path.startswith("/admin/") else "/reserve"
            self._redirect(destination + "?" + urlencode({"error": str(exc)[:500]}))
            return
        self._redirect(destination + "?" + urlencode({"ok": message[:500]}))


def serve_web(
    app: SchedulerWebApp,
    *,
    host: str,
    port: int,
    tls_cert: Path | None,
    tls_key: Path | None,
    insecure_http: bool = False,
) -> None:
    """Serve until interrupted, requiring HTTPS outside explicit loopback testing."""

    if not 1 <= port <= 65535:
        raise QueueError("web port must be between 1 and 65535")
    if insecure_http:
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise QueueError("--insecure-http is restricted to a loopback host")
    elif tls_cert is None or tls_key is None:
        raise QueueError("HTTPS requires both --tls-cert and --tls-key")
    server = QueueWebServer((host, port), app)
    if not insecure_http:
        assert tls_cert is not None and tls_key is not None
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        try:
            context.load_cert_chain(str(tls_cert.resolve()), str(tls_key.resolve()))
        except (OSError, ssl.SSLError) as exc:
            server.server_close()
            raise QueueError(f"could not load HTTPS certificate and key: {exc}") from exc
        server.socket = context.wrap_socket(server.socket, server_side=True)
    scheme = "http" if insecure_http else "https"
    app.store.event(
        "WEB_SERVER_STARTED",
        payload={"host": host, "port": port, "scheme": scheme, "pid": os.getpid()},
        actor="web-server",
    )
    print(f"scheduler web app listening at {scheme}://{host}:{port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("scheduler web app stopping", flush=True)
    finally:
        server.server_close()
        app.store.event(
            "WEB_SERVER_STOPPED",
            payload={"host": host, "port": port, "pid": os.getpid()},
            actor="web-server",
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the private scheduler-web command-line interface."""

    parser = argparse.ArgumentParser(
        description="Configure or serve the private HTTPS interface for the GPU scheduler."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository checkout associated with the scheduler state. Default: current directory.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("gpu_scheduler_state"),
        help="Ignored scheduler state directory. Relative paths resolve from --repo-root.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser(
        "auth-setup",
        help="Interactively set separate administrator and shared coworker passwords.",
    )
    serve = subparsers.add_parser("serve", help="Serve the private scheduler web app.")
    serve.add_argument(
        "--host",
        default="0.0.0.0",
        help="Private-network interface to listen on. Default: 0.0.0.0.",
    )
    serve.add_argument("--port", type=int, default=8443, help="HTTPS port. Default: 8443.")
    serve.add_argument("--tls-cert", type=Path, help="PEM certificate chain for the private hostname.")
    serve.add_argument("--tls-key", type=Path, help="PEM private key matching --tls-cert.")
    serve.add_argument(
        "--nvidia-smi",
        default="nvidia-smi",
        help="GPU telemetry executable used by the dashboard. Default: nvidia-smi.",
    )
    serve.add_argument(
        "--insecure-http",
        action="store_true",
        help="Allow HTTP only on a loopback host for local testing; never use on the private network.",
    )
    return parser


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    repo_root = args.repo_root.resolve()
    state_dir = args.state_dir if args.state_dir.is_absolute() else repo_root / args.state_dir
    return repo_root, state_dir.resolve()


def _prompt_password(label: str) -> str:
    first = getpass.getpass(f"{label} password (minimum 12 characters): ")
    second = getpass.getpass(f"Confirm {label.lower()} password: ")
    if not hmac.compare_digest(first, second):
        raise QueueError(f"{label} password confirmation did not match")
    return first


def main(argv: Sequence[str] | None = None) -> int:
    """Configure credentials or run the private web service."""

    args = build_arg_parser().parse_args(argv)
    repo_root, state_dir = _resolve_paths(args)
    try:
        store = QueueStore(state_dir, repo_root)
        if args.action == "auth-setup":
            path = initialize_web_auth(
                state_dir,
                admin_password=_prompt_password("Administrator"),
                reservation_password=_prompt_password("Coworker reservation"),
            )
            print(f"web authentication configured at {path}")
            return 0
        auth = AuthManager(state_dir / AUTH_FILENAME)
        app = SchedulerWebApp(store, auth, nvidia_smi=args.nvidia_smi)
        serve_web(
            app,
            host=args.host,
            port=args.port,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            insecure_http=args.insecure_http,
        )
        return 0
    except QueueError as exc:
        print(f"scheduler web error: {exc}", file=os.sys.stderr)
        return 2


__all__ = [
    "AUTH_FILENAME",
    "AuthManager",
    "SchedulerWebApp",
    "WebSession",
    "initialize_web_auth",
    "main",
    "serve_web",
]
