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
import re
import secrets
import ssl
import tempfile
import threading
import time
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlencode, urlparse

from helmholtz_shared.experiment_queue import (
    MAX_RESERVATION_HOURS,
    MIN_RESERVATION_HOURS,
    PENDING_STATES,
    RUNNING_STATES,
    TERMINAL_STATES,
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
LOG_TAIL_BYTES = 128 * 1024
RUN_PAGE_PATTERN = re.compile(r"^/admin/runs/([1-9][0-9]*)$")
RUN_EVENT_PATTERN = re.compile(r"^/events/admin/runs/([1-9][0-9]*)$")
ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\))|(?:\x1b\[[0-?]*[ -/]*[@-~])"
)
TERMINAL_CONTROL_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
QUEUE_STATES = PENDING_STATES | RUNNING_STATES | TERMINAL_STATES


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


@dataclass(frozen=True)
class LogSnapshot:
    """Bounded, browser-safe view of one durable runner or launcher log."""

    source: str
    text: str
    size_bytes: int | None
    truncated: bool
    available: bool
    note: str


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


def _state_pill(state: Any) -> str:
    """Render a text-labeled, theme-aware badge for one queue job state."""

    value = str(state)
    css_state = value.replace("_", "-") if value in QUEUE_STATES else "unknown"
    return (
        f'<span class="pill state-pill state-{css_state}" '
        f'aria-label="State: {_escape(value)}">'
        f'<span class="state-dot" aria-hidden="true"></span>{_escape(value)}</span>'
    )


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


def _format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024**2:
        return f"{value / 1024:.1f} KiB"
    return f"{value / 1024**2:.1f} MiB"


def _read_log_tail(path: Path, *, source: str, note: str) -> LogSnapshot:
    """Read a bounded terminal-log tail without rendering control sequences."""

    try:
        size = path.stat().st_size
        offset = max(0, size - LOG_TAIL_BYTES)
        with path.open("rb") as handle:
            handle.seek(offset)
            payload = handle.read(LOG_TAIL_BYTES)
    except OSError as exc:
        return LogSnapshot(
            source=source,
            text="",
            size_bytes=None,
            truncated=False,
            available=False,
            note=f"Could not read {source}: {exc}",
        )
    text = payload.decode("utf-8", errors="replace")
    text = ANSI_ESCAPE_PATTERN.sub("", text)
    text = TERMINAL_CONTROL_PATTERN.sub(
        "", text.replace("\r\n", "\n").replace("\r", "\n")
    )
    return LogSnapshot(
        source=source,
        text=text,
        size_bytes=size,
        truncated=offset > 0,
        available=True,
        note=note,
    )


STYLE = """
:root{color-scheme:dark;--bg:#101311;--glow:#1d2a22;--paused-bg:#200b0b;--paused-glow:#7d2823;--panel:#171c19;--panel2:#1d2420;--field:#0e120f;--line:#334039;--text:#f2f5f0;--muted:#aab5ae;--green:#9ee37d;--amber:#f0c36a;--red:#ff8c82;--blue:#8dc7ff;--button:#263d2d;--button-hover:#31503a;--secondary:#202824;--danger:#4b2525;--danger-line:#83403b;--pill:#253029;--ok-bg:#17301d;--ok-line:#315c3b;--error-text:#ffd1cd;--error-bg:#3a2020;--error-line:#78403d;--shadow:#0004}
:root[data-theme=light]{color-scheme:light;--bg:#f4f7f2;--glow:#dfeee2;--paused-bg:#fff0ef;--paused-glow:#f5afa9;--panel:#fff;--panel2:#f7faf6;--field:#fff;--line:#c8d4ca;--text:#172019;--muted:#5d6b61;--green:#347a2e;--amber:#9a6414;--red:#b23932;--blue:#1769aa;--button:#dcecdf;--button-hover:#cce3d1;--secondary:#edf2ed;--danger:#f7dddd;--danger-line:#d8a09c;--pill:#e7eee8;--ok-bg:#e3f3e4;--ok-line:#a8cea9;--error-text:#7d211d;--error-bg:#f9e1df;--error-line:#d9a4a0;--shadow:#253a2b1c}
:root{--state-queued:#a9c7ff;--state-queued-bg:#172942;--state-queued-line:#3e6495;--state-held:#c7cdd1;--state-held-bg:#242a2d;--state-held-line:#59636a;--state-blocked:#ffb0d7;--state-blocked-bg:#3b1d2d;--state-blocked-line:#824768;--state-starting:#86e5ef;--state-starting-bg:#113239;--state-starting-line:#2c7580;--state-running:#9ee37d;--state-running-bg:#17301d;--state-running-line:#315c3b;--state-yielding:#f0c36a;--state-yielding-bg:#3a2d13;--state-yielding-line:#806426;--state-terminating:#ffb56b;--state-terminating-bg:#402711;--state-terminating-line:#87542b;--state-force:#ff8c82;--state-force-bg:#3a2020;--state-force-line:#78403d;--state-succeeded:#7fe0c3;--state-succeeded-bg:#13342c;--state-succeeded-line:#2f7562;--state-interrupted:#ffd07b;--state-interrupted-bg:#3a2c15;--state-interrupted-line:#806529;--state-removed:#b7bec2;--state-removed-bg:#24292b;--state-removed-line:#555e62}
:root[data-theme=light]{--state-queued:#1553a3;--state-queued-bg:#e4efff;--state-queued-line:#98b9e8;--state-held:#536069;--state-held-bg:#edf0f2;--state-held-line:#b9c1c6;--state-blocked:#96265c;--state-blocked-bg:#fbe4ef;--state-blocked-line:#dda4bf;--state-starting:#0c6e78;--state-starting-bg:#ddf6f8;--state-starting-line:#93cdd2;--state-running:#276b2f;--state-running-bg:#e3f3e4;--state-running-line:#a8cea9;--state-yielding:#855b0b;--state-yielding-bg:#fff2cf;--state-yielding-line:#d9bb68;--state-terminating:#9b4c0b;--state-terminating-bg:#ffead7;--state-terminating-line:#dfa676;--state-force:#a52c25;--state-force-bg:#f9e1df;--state-force-line:#d9a4a0;--state-succeeded:#08745e;--state-succeeded-bg:#dcf4ed;--state-succeeded-line:#91cdbd;--state-interrupted:#87580a;--state-interrupted-bg:#fff0d1;--state-interrupted-line:#d9b969;--state-removed:#60676b;--state-removed-bg:#eef0f1;--state-removed-line:#bcc2c5}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top left,var(--glow) 0,var(--bg) 42%);color:var(--text);font:15px/1.45 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;transition:background .22s,color .18s}body[data-dispatch-paused=true]{background:radial-gradient(circle at top left,var(--paused-glow) 0,var(--paused-bg) 48%)}
a{color:var(--blue)}.shell{max-width:1180px;margin:0 auto;padding:28px 20px 64px}.top{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:24px}.eyebrow{text-transform:uppercase;letter-spacing:.14em;color:var(--green);font-size:12px;font-weight:700}.title{font-size:clamp(28px,5vw,48px);line-height:1.02;margin:8px 0}.subtitle{color:var(--muted);max-width:680px}.nav{display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap}.nav form{margin:0}.panel,.gpu,.login{background:linear-gradient(145deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:16px;box-shadow:0 18px 50px var(--shadow)}.panel{padding:20px;margin:18px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:14px}.gpu{padding:18px;position:relative;overflow:hidden}.gpu:before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--line)}.gpu.available:before{background:var(--green)}.gpu.busy:before{background:var(--amber)}.gpu.reserved:before{background:var(--blue)}.gpu.danger:before{background:var(--red)}.gpu h3{margin:0 0 4px;font-size:20px}.meta{color:var(--muted);font-size:13px}.status{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:999px;padding:4px 9px;margin:10px 0;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.06em}.dot{width:7px;height:7px;border-radius:50%;background:currentColor}.available .status{color:var(--green)}.busy .status{color:var(--amber)}.reserved .status{color:var(--blue)}.danger .status{color:var(--red)}form{margin:12px 0 0}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:end}.field{display:flex;flex-direction:column;gap:5px;min-width:120px;flex:1}.field label{font-size:12px;color:var(--muted);font-weight:700}input,select,button,textarea{font:inherit}input,select,textarea{width:100%;color:var(--text);background:var(--field);border:1px solid var(--line);border-radius:9px;padding:9px 10px}textarea{min-height:72px;resize:vertical}button,.button{border:1px solid var(--line);background:var(--button);color:var(--text);border-radius:9px;padding:9px 13px;font-weight:750;cursor:pointer;text-decoration:none;display:inline-block}button:hover,.button:hover{background:var(--button-hover)}button.secondary,.button.secondary{background:var(--secondary);border-color:var(--line)}button.danger{background:var(--danger);border-color:var(--danger-line)}button:disabled{opacity:.5;cursor:not-allowed}.flash{padding:12px 14px;border-radius:10px;margin:14px 0;border:1px solid}.flash.ok{color:var(--green);background:var(--ok-bg);border-color:var(--ok-line)}.flash.error{color:var(--error-text);background:var(--error-bg);border-color:var(--error-line)}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}.actions{display:flex;gap:6px;flex-wrap:wrap}.actions form{margin:0}.actions button{padding:6px 9px;font-size:12px}.pill{display:inline-block;padding:3px 7px;border-radius:999px;background:var(--pill);color:var(--muted);font-size:11px}.login{max-width:440px;margin:12vh auto 0;padding:28px}.login h1{margin-top:0}.split{display:grid;grid-template-columns:1fr 1fr;gap:18px}.muted{color:var(--muted)}.tiny{font-size:12px}.event{display:grid;grid-template-columns:150px 210px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid var(--line);font-size:12px}.theme-corner{position:fixed;right:18px;top:18px;z-index:2}.live-indicator{display:inline-flex;align-items:center;gap:7px;color:var(--amber);border:1px solid var(--line);border-radius:999px;padding:8px 11px;font-size:12px;font-weight:700;background:var(--panel)}.live-indicator.connected{color:var(--green)}.live-indicator.disconnected{color:var(--red)}.live-indicator .dot{box-shadow:0 0 0 3px color-mix(in srgb,currentColor 18%,transparent)}[data-live-section]{scroll-margin-top:16px}.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px;margin:0}.facts div{min-width:0}.facts dt{color:var(--muted);font-size:11px;font-weight:750;letter-spacing:.08em;text-transform:uppercase}.facts dd{margin:4px 0 0;overflow-wrap:anywhere}.log-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.log-card{min-width:0}.log-card h3{margin:0}.log-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:10px}.log{height:420px;margin:0;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;background:var(--field);border:1px solid var(--line);border-radius:10px;padding:14px;color:var(--text);font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;tab-size:4}.command{margin:10px 0;white-space:pre-wrap;overflow-wrap:anywhere;background:var(--field);border:1px solid var(--line);border-radius:10px;padding:12px;font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}.copy-status{display:inline-block;min-height:1.4em;margin-left:8px;color:var(--green);font-size:12px}.run-link{font-weight:750;text-decoration:none}.run-link:hover{text-decoration:underline}@media(max-width:760px){.top{display:block}.nav{margin-top:18px}.split,.log-grid{grid-template-columns:1fr}table{display:block;overflow-x:auto}.event{grid-template-columns:1fr}.shell{padding:20px 14px 50px}.theme-corner{position:static;margin:14px}.log{height:320px}}
.state-pill{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--state-line,var(--line));background:var(--state-bg,var(--pill));color:var(--state-fg,var(--muted));font-weight:750;white-space:nowrap}.state-dot{width:6px;height:6px;border-radius:50%;background:currentColor;box-shadow:0 0 0 2px color-mix(in srgb,currentColor 16%,transparent)}.state-queued{--state-fg:var(--state-queued);--state-bg:var(--state-queued-bg);--state-line:var(--state-queued-line)}.state-held{--state-fg:var(--state-held);--state-bg:var(--state-held-bg);--state-line:var(--state-held-line)}.state-blocked{--state-fg:var(--state-blocked);--state-bg:var(--state-blocked-bg);--state-line:var(--state-blocked-line)}.state-starting{--state-fg:var(--state-starting);--state-bg:var(--state-starting-bg);--state-line:var(--state-starting-line)}.state-running{--state-fg:var(--state-running);--state-bg:var(--state-running-bg);--state-line:var(--state-running-line)}.state-yielding{--state-fg:var(--state-yielding);--state-bg:var(--state-yielding-bg);--state-line:var(--state-yielding-line)}.state-terminating{--state-fg:var(--state-terminating);--state-bg:var(--state-terminating-bg);--state-line:var(--state-terminating-line)}.state-force-killing,.state-failed{--state-fg:var(--state-force);--state-bg:var(--state-force-bg);--state-line:var(--state-force-line)}.state-succeeded{--state-fg:var(--state-succeeded);--state-bg:var(--state-succeeded-bg);--state-line:var(--state-succeeded-line)}.state-interrupted{--state-fg:var(--state-interrupted);--state-bg:var(--state-interrupted-bg);--state-line:var(--state-interrupted-line)}.state-force-killed{--state-fg:var(--state-force);--state-bg:var(--state-force-bg);--state-line:var(--state-force-line)}.state-removed{--state-fg:var(--state-removed);--state-bg:var(--state-removed-bg);--state-line:var(--state-removed-line)}
.queue-heading{display:flex;align-items:baseline;justify-content:space-between;gap:14px;flex-wrap:wrap}.queue-heading h2{margin:0}.queue-toolbar{display:grid;grid-template-columns:minmax(210px,2fr) repeat(3,minmax(140px,1fr)) auto;gap:10px;align-items:end;margin:16px 0}.queue-toolbar .field{min-width:0}.queue-summary{color:var(--muted);font-size:13px;font-weight:700}.queue-table-wrap{overflow-x:auto}.queue-empty{margin:18px 0 4px;text-align:center}[hidden]{display:none!important}@media(max-width:900px){.queue-toolbar{grid-template-columns:repeat(2,minmax(0,1fr))}.queue-toolbar .search-field{grid-column:1/-1}}@media(max-width:560px){.queue-toolbar{grid-template-columns:1fr}.queue-toolbar .search-field{grid-column:auto}.queue-toolbar button{width:100%}}
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

  const copyText = async (value) => {
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(value);
        return;
      } catch (_error) { /* fall through to selection-based copying */ }
    }
    const helper = document.createElement("textarea");
    helper.value = value;
    helper.setAttribute("readonly", "");
    helper.style.position = "fixed";
    helper.style.opacity = "0";
    document.body.appendChild(helper);
    helper.select();
    const copied = document.execCommand("copy");
    helper.remove();
    if (!copied) throw new Error("copy command was unavailable");
  };
  document.addEventListener("click", async (event) => {
    const button = event.target.closest?.("[data-copy-target]");
    if (!button || button.disabled) return;
    const source = document.getElementById(button.dataset.copyTarget || "");
    const status = document.getElementById(button.dataset.copyStatus || "");
    if (!source) return;
    const original = button.textContent;
    button.disabled = true;
    try {
      await copyText(source.textContent || "");
      button.textContent = "Copied";
      if (status) status.textContent = "Command copied to clipboard.";
    } catch (_error) {
      button.textContent = "Copy failed";
      if (status) status.textContent = "Select and copy the command above.";
    }
    setTimeout(() => {
      if (button.isConnected) {
        button.textContent = original;
        button.disabled = false;
      }
      if (status?.isConnected) status.textContent = "";
    }, 2200);
  });

  const queueCollator = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: "base",
  });
  const rebuildQueueGpuOptions = (rows) => {
    const select = document.getElementById("queue-gpu-filter");
    if (!select) return;
    const selected = select.value;
    while (select.options.length > 2) select.remove(2);
    const gpuIndices = [...new Set(rows.map((row) => row.dataset.gpu).filter(Boolean))]
      .sort((left, right) => queueCollator.compare(left, right));
    for (const gpu of gpuIndices) {
      const option = document.createElement("option");
      option.value = gpu;
      option.textContent = `GPU ${gpu}`;
      select.appendChild(option);
    }
    select.value = [...select.options].some((option) => option.value === selected)
      ? selected
      : "all";
  };
  const applyQueueView = ({ refreshGpus = false } = {}) => {
    const body = document.querySelector("[data-queue-body]");
    if (!body) return;
    const rows = [...body.querySelectorAll("[data-queue-row]")];
    if (refreshGpus) rebuildQueueGpuOptions(rows);
    const search = document.getElementById("queue-search")?.value || "";
    const terms = search.toLocaleLowerCase().trim().split(/\s+/).filter(Boolean);
    const state = document.getElementById("queue-state-filter")?.value || "all";
    const gpu = document.getElementById("queue-gpu-filter")?.value || "all";
    const sort = document.getElementById("queue-sort")?.value || "queue";
    for (const row of rows) {
      const matchesSearch = terms.every((term) =>
        (row.dataset.search || "").toLocaleLowerCase().includes(term)
      );
      const matchesState = state === "all"
        || row.dataset.state === state
        || row.dataset.stateGroup === state;
      const matchesGpu = gpu === "all"
        || (gpu === "unassigned" ? !row.dataset.gpu : row.dataset.gpu === gpu);
      row.hidden = !(matchesSearch && matchesState && matchesGpu);
    }
    const number = (row, key) => Number(row.dataset[key] || 0);
    const text = (row, key) => row.dataset[key] || "";
    const byNewest = (left, right) => number(right, "id") - number(left, "id");
    rows.sort((left, right) => {
      let compared = 0;
      if (sort === "queue" || sort === "id-desc") compared = byNewest(left, right);
      else if (sort === "id-asc") compared = number(left, "id") - number(right, "id");
      else if (sort === "experiment") {
        compared = queueCollator.compare(text(left, "experiment"), text(right, "experiment"));
      } else if (sort === "state") {
        compared = queueCollator.compare(text(left, "state"), text(right, "state"));
      } else if (sort === "priority-desc") {
        compared = number(right, "priority") - number(left, "priority");
      } else if (sort === "priority-asc") {
        compared = number(left, "priority") - number(right, "priority");
      } else if (sort === "gpu") {
        compared = queueCollator.compare(text(left, "gpu") || "zzzz", text(right, "gpu") || "zzzz");
      }
      return compared || byNewest(left, right);
    });
    for (const row of rows) body.appendChild(row);
    const visible = rows.filter((row) => !row.hidden).length;
    const summary = document.getElementById("queue-summary");
    if (summary) {
      summary.textContent = `Showing ${visible} of ${rows.length} queue item${rows.length === 1 ? "" : "s"}`;
    }
    const empty = document.getElementById("queue-empty");
    if (empty) {
      empty.textContent = rows.length
        ? "No queue items match these filters."
        : "No queue items have been added.";
      empty.hidden = visible > 0;
    }
  };
  document.getElementById("queue-search")?.addEventListener("input", () => applyQueueView());
  for (const id of ["queue-state-filter", "queue-gpu-filter", "queue-sort"]) {
    document.getElementById(id)?.addEventListener("change", () => applyQueueView());
  }
  document.getElementById("queue-reset")?.addEventListener("click", () => {
    const search = document.getElementById("queue-search");
    const state = document.getElementById("queue-state-filter");
    const gpu = document.getElementById("queue-gpu-filter");
    const sort = document.getElementById("queue-sort");
    if (search) search.value = "";
    if (state) state.value = "all";
    if (gpu) gpu.value = "all";
    if (sort) sort.value = "queue";
    applyQueueView();
    search?.focus();
  });
  applyQueueView({ refreshGpus: true });
  const syncDispatchAppearance = () => {
    const dispatch = document.querySelector("[data-dispatch-paused]");
    if (dispatch) document.body.dataset.dispatchPaused = dispatch.dataset.dispatchPaused;
  };
  syncDispatchAppearance();

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
    if (name === "queue") applyQueueView({ refreshGpus: true });
    if (name === "dispatch") syncDispatchAppearance();
  };
  document.addEventListener("focusout", () => setTimeout(() => {
    for (const [name, markup] of pending) applySection(name, markup);
  }, 0));

  const eventUrl = view.startsWith("run-")
    ? `/events/admin/runs/${encodeURIComponent(view.slice(4))}`
    : `/events/${encodeURIComponent(view)}`;
  const stream = new EventSource(eventUrl);
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
    location.assign(
      view === "admin" || view.startsWith("run-")
        ? "/login/admin"
        : "/login/reservation"
    );
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
        self.login_failures: dict[tuple[str, str], list[float]] = {}
        self._login_failure_lock = threading.Lock()

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
                "SELECT * FROM queue_items ORDER BY id DESC"
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

    def _runner_log_path(
        self,
        item: Mapping[str, Any],
        filename: str,
    ) -> tuple[Path | None, str | None]:
        """Resolve a fixed runner log name without exposing arbitrary host files."""

        if filename != "stdout.log":
            raise QueueError(f"unsupported runner log {filename!r}")
        run_directory = str(item.get("runner_run_dir") or "").strip()
        if not run_directory:
            return None, "The runner has not published its run directory yet."
        repo_root = self.store.repo_root.resolve()
        candidate = Path(run_directory)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        try:
            resolved_directory = candidate.resolve()
            log_path = resolved_directory / filename
            if log_path.is_symlink():
                return None, f"The recorded {filename} path is a symbolic link."
            resolved_log = log_path.resolve()
        except OSError as exc:
            return None, f"Could not resolve the recorded runner directory: {exc}"
        if resolved_directory != repo_root and repo_root not in resolved_directory.parents:
            return None, "The recorded runner directory is outside this repository."
        if resolved_log.parent != resolved_directory:
            return None, f"The recorded {filename} path is not a safe regular log path."
        if not resolved_log.is_file():
            return None, f"{filename} is not available in the recorded runner directory."
        return resolved_log, None

    def _launcher_log_path(self, item: Mapping[str, Any]) -> Path | None:
        """Return the current segment's scheduler-owned combined launcher log."""

        segment = max(1, int(item["segment"]))
        path = (
            self.store.state_dir
            / "attempts"
            / str(int(item["id"]))
            / "segments"
            / str(segment)
            / "launcher.log"
        )
        return path if path.is_file() and not path.is_symlink() else None

    def _log_snapshot(self, item: Mapping[str, Any], filename: str) -> LogSnapshot:
        """Prefer canonical runner output and fall back to startup output when useful."""

        path, error = self._runner_log_path(item, filename)
        if path is not None:
            return _read_log_tail(
                path,
                source=filename,
                note=f"Runner log · {path}",
            )
        launcher = self._launcher_log_path(item)
        if launcher is not None:
            return _read_log_tail(
                launcher,
                source="launcher.log",
                note=(
                    "Combined queue launcher output while the canonical runner logs "
                    "are not available"
                ),
            )
        return LogSnapshot(
            source=filename,
            text="",
            size_bytes=None,
            truncated=False,
            available=False,
            note=error or f"{filename} is not available yet.",
        )

    def _run_data(self, item_id: int) -> dict[str, Any]:
        """Read one queue item and its audit context without polling GPU telemetry."""

        with self.store.connect() as connection:
            item = dict(self.store.item(item_id, connection=connection))
            dependencies = [
                dict(row)
                for row in connection.execute(
                    "SELECT dependency_item_id, experiment_id, state FROM dependencies "
                    "JOIN queue_items ON queue_items.id = dependencies.dependency_item_id "
                    "WHERE queue_item_id = ? ORDER BY dependency_item_id",
                    (item_id,),
                )
            ]
            events = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM events WHERE queue_item_id = ? ORDER BY id DESC LIMIT 100",
                    (item_id,),
                )
            ]
        return {"item": item, "dependencies": dependencies, "events": events}

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

    @staticmethod
    def _log_card(title: str, snapshot: LogSnapshot) -> str:
        if snapshot.available:
            assert snapshot.size_bytes is not None
            extent = (
                f"Showing the last {_format_bytes(LOG_TAIL_BYTES)} of "
                f"{_format_bytes(snapshot.size_bytes)}"
                if snapshot.truncated
                else f"Complete log · {_format_bytes(snapshot.size_bytes)}"
            )
            content = snapshot.text or "(empty log)"
        else:
            extent = "Not available"
            content = snapshot.note
        return f"""<article class="panel log-card"><div class="log-head"><div><h3>{_escape(title)}</h3>
<div class="tiny muted">{_escape(snapshot.source)} · {_escape(extent)}</div></div></div>
<p class="tiny muted">{_escape(snapshot.note)}</p><pre class="log" tabindex="0">{_escape(content)}</pre></article>"""

    def _run_sections(self, session: WebSession, item_id: int) -> dict[str, str]:
        """Render the live administrator-only detail fragment for one queue run."""

        if session.role != "admin":
            raise QueueError("administrator access is required for run details")
        data = self._run_data(item_id)
        item = data["item"]
        stdout = self._log_snapshot(item, "stdout.log")
        dependencies = " · ".join(
            f'<a href="/admin/runs/{row["dependency_item_id"]}">'
            f'#{row["dependency_item_id"]} {_escape(row["experiment_id"])}</a> '
            f'{_state_pill(row["state"])}'
            for row in data["dependencies"]
        ) or "None"
        facts = [
            ("Queue item", f"#{item_id}"),
            ("State", str(item["state"])),
            ("Attempt / segment", f'{item["attempt"]} / {item["segment"]}'),
            ("Priority", str(item["priority"])),
            ("GPU", str(item["assigned_gpu_index"] or "—")),
            ("Return code", str(item["return_code"] if item["return_code"] is not None else "—")),
            ("Started", str(item["started_at"] or "—")),
            ("Finished", str(item["finished_at"] or "—")),
            ("Commit", str(item["git_commit"])),
            ("Card", str(item["card_path"])),
            ("Run directory", str(item["runner_run_dir"] or "Not published yet")),
            ("Manifest", str(item["runner_manifest_path"] or "Not published yet")),
        ]
        facts_html = "".join(
            f"<div><dt>{_escape(label)}</dt><dd>{_escape(value)}</dd></div>"
            for label, value in facts
        )
        rsync_command = str(item["rsync_pull_command"] or "").strip()
        if rsync_command:
            rsync_html = f"""<pre class="command"><code id="rsync-command">{_escape(rsync_command)}</code></pre>
<button type="button" data-copy-target="rsync-command" data-copy-status="copy-status">Copy rsync command to clipboard</button>
<span id="copy-status" class="copy-status" role="status" aria-live="polite"></span>"""
        else:
            rsync_html = """<p class="muted">The runner has not recorded an rsync command yet.</p>
<button type="button" disabled>Copy rsync command to clipboard</button>"""
        detail_html = (
            f'<section class="panel"><strong>State detail</strong>'
            f'<p class="muted">{_escape(item["state_detail"])}</p></section>'
            if item["state_detail"]
            else ""
        )
        event_rows = "".join(
            f'<div class="event"><span>{_escape(row["created_at"])}</span>'
            f'<strong>{_escape(row["event_type"])}</strong>'
            f'<span>{_escape(row["actor"])}<br>{_escape(row["payload_json"])}</span></div>'
            for row in data["events"]
        )
        return {
            "run": f"""{detail_html}<section class="panel"><div class="row"><div><h2 style="margin:0">{_escape(item['experiment_id'])}</h2>
<p class="muted">Queue item #{item_id} · {_state_pill(item['state'])}</p></div></div>
<dl class="facts">{facts_html}</dl><p class="tiny muted">Dependencies: {dependencies}</p></section>
<section><h2>Output</h2><p class="muted">The live page shows bounded log tails so very large training output stays responsive.</p>
{self._log_card('Stdout', stdout)}</section>
<section class="panel"><h2>Synchronize this run</h2>{rsync_html}</section>
<section class="panel"><h2>Run audit history</h2>{event_rows or '<p>No run events recorded.</p>'}</section>"""
        }

    def render_run(self, session: WebSession, item_id: int) -> bytes:
        """Render an administrator run page with live logs and sync instructions."""

        sections = self._run_sections(session, item_id)
        body = f"""<main class="shell"><header class="top"><div><div class="eyebrow">Mutton2 scheduler</div>
<h1 class="title">Run details</h1><p class="subtitle">Status and output refresh automatically while this page is open.</p></div>
<nav class="nav">{self._live_indicator()}{self._theme_toggle()}<a class="button secondary" href="/admin">Back to queue</a>
<form method="post" action="/logout"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><button class="secondary">Sign out</button></form></nav></header>
<div data-live-section="run">{sections['run']}</div></main>"""
        return self._page(
            f"Run {item_id} · Mutton2 scheduler",
            body,
            live_view=f"run-{item_id}",
        )

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
                reservation_detail = f"""<p><strong>{_escape(reservation['note'])}</strong><br>
<span class="muted">{_escape(_format_remaining(reservation['expires_at']))}</span></p>"""
                if reservation["status"] == "pending":
                    action = reservation_detail + (
                        '<p class="muted">Release is available after checkpointing finishes.</p>'
                    )
                else:
                    action = reservation_detail + f"""
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
            if item["worktree_cleanup_error"]:
                isolation = f"cleanup pending: {item['worktree_cleanup_error']}"
            elif item["worktree_removed_at"]:
                isolation = "isolated worktree cleaned"
            elif item["worktree_path"]:
                isolation = "isolated worktree ready"
            elif item["git_ref"]:
                isolation = "commit pinned; worktree pending"
            else:
                isolation = "legacy shared checkout"
            detail = str(item["state_detail"] or "")
            if isolation:
                detail = f"{detail} · {isolation}" if detail else isolation
            gpu_index = str(item["assigned_gpu_index"] or "")
            state = str(item["state"])
            state_group = "terminal" if state in TERMINAL_STATES else "active"
            search_value = " ".join(
                str(value or "")
                for value in (
                    item_id,
                    item["experiment_id"],
                    state,
                    item["priority"],
                    gpu_index,
                    item["assigned_gpu_uuid"],
                    item["git_commit"],
                    item["card_path"],
                    detail,
                )
            )
            search_value = " ".join(search_value.split())
            queue_rows.append(f"""<tr data-queue-row data-id="{item_id}" data-experiment="{_escape(item['experiment_id'])}" data-state="{_escape(state)}" data-state-group="{state_group}" data-priority="{item['priority']}" data-gpu="{_escape(gpu_index)}" data-search="{_escape(search_value)}"><td>#{item_id}</td><td><a class="run-link" href="/admin/runs/{item_id}">{_escape(item['experiment_id'])}</a><br><span class="tiny muted">attempt {item['attempt']} · segment {item['segment']} · commit {_escape(str(item['git_commit'])[:12])}</span></td>
<td>{_state_pill(item['state'])}</td><td>{item['priority']}{' · front' if item['resume_front'] else ''}</td><td>{_escape(item['assigned_gpu_index'] or '—')}</td><td>{_escape(detail)}</td><td><div class="actions">{''.join(actions)}</div></td></tr>""")
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
            "dispatch": f"""<section class="panel" data-dispatch-paused="{str(data['dispatch_paused']).lower()}"><div class="row"><div><strong>{dispatch_text}</strong><div class="muted">{_escape(data['pause_reason'])}</div></div>
<form method="post" action="/admin/dispatch"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="operation" value="{pause_action}"><input name="reason" placeholder="Reason (optional)"><button>{pause_action.title()} dispatch</button></form></div></section>""",
            "gpus": f"{telemetry_warning}<div class=\"grid\">{''.join(gpu_cards) or '<p>No GPUs are enabled.</p>'}</div>",
            "queue": f"""<div class="queue-table-wrap"><table><thead><tr><th>ID</th><th>Experiment</th><th>State</th><th>Priority</th><th>GPU</th><th>Detail</th><th>Controls</th></tr></thead><tbody data-queue-body>{''.join(queue_rows)}</tbody></table></div>""",
            "reservations": f"""<section class="panel"><h2>Reservation history</h2><table><thead><tr><th>ID</th><th>Reserved for / note</th><th>Status</th><th>Duration</th><th>Started</th><th>Expires</th></tr></thead><tbody>{reservation_rows}</tbody></table></section>""",
            "events": f"""<section class="panel"><h2>Recent audit history</h2>{event_rows or '<p>No events recorded.</p>'}</section>""",
        }

    def live_sections(self, view: str, session: WebSession) -> dict[str, str]:
        """Return role-checked live sections for one authenticated dashboard."""

        if view == "reserve":
            return self._reserve_sections(session)
        if view == "admin" and session.role == "admin":
            return self._admin_sections(session)
        if view.startswith("run-") and session.role == "admin":
            item_id = _integer(view.removeprefix("run-"), label="queue item ID")
            return self._run_sections(session, item_id)
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
<section class="panel" aria-labelledby="queue-heading"><div class="queue-heading"><h2 id="queue-heading">Queue</h2><div id="queue-summary" class="queue-summary" role="status" aria-live="polite"></div></div>
<div class="queue-toolbar" aria-label="Queue filters and sorting">
<div class="field search-field"><label for="queue-search">Search queue</label><input id="queue-search" type="search" placeholder="Experiment, ID, detail, or commit" autocomplete="off"></div>
<div class="field"><label for="queue-state-filter">State</label><select id="queue-state-filter"><option value="all">All states</option><option value="active">Active</option><option value="terminal">Finished</option><optgroup label="Active states"><option value="queued">Queued</option><option value="held">Held</option><option value="blocked">Blocked</option><option value="starting">Starting</option><option value="running">Running</option><option value="yielding">Yielding</option><option value="terminating">Terminating</option><option value="force_killing">Force killing</option></optgroup><optgroup label="Finished states"><option value="succeeded">Succeeded</option><option value="failed">Failed</option><option value="interrupted">Interrupted</option><option value="force_killed">Force killed</option><option value="removed">Removed</option></optgroup></select></div>
<div class="field"><label for="queue-gpu-filter">GPU</label><select id="queue-gpu-filter"><option value="all">All GPUs</option><option value="unassigned">Unassigned</option></select></div>
<div class="field"><label for="queue-sort">Sort by</label><select id="queue-sort"><option value="queue">Default order</option><option value="priority-desc">Priority: high to low</option><option value="priority-asc">Priority: low to high</option><option value="id-desc">Newest first</option><option value="id-asc">Oldest first</option><option value="experiment">Experiment A–Z</option><option value="state">State A–Z</option><option value="gpu">GPU</option></select></div>
<button id="queue-reset" class="secondary" type="button">Reset</button></div>
<div data-live-section="queue">{sections['queue']}</div><p id="queue-empty" class="queue-empty muted" hidden></p>
<p class="tiny muted">Filters and sorting change only this browser view; scheduler priority and dispatch order are unchanged.</p></section>
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

    def begin_login_attempt(self, client: str, role: str) -> bool:
        """Atomically reserve one attempt within the per-client, per-role window."""

        if role not in {"admin", "reservation"}:
            raise QueueError(f"unknown web role {role!r}")
        now = time.monotonic()
        key = (client, role)
        with self._login_failure_lock:
            recent = [
                stamp
                for stamp in self.login_failures.get(key, [])
                if now - stamp < LOGIN_WINDOW_SECONDS
            ]
            if len(recent) >= LOGIN_MAX_FAILURES:
                self.login_failures[key] = recent
                return False
            recent.append(now)
            self.login_failures[key] = recent
            return True

    def login_succeeded(self, client: str, role: str) -> None:
        with self._login_failure_lock:
            self.login_failures.pop((client, role), None)


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
        body = self.rfile.read(length)
        if len(body) != length:
            raise QueueError(
                f"incomplete request body: expected {length} bytes, received {len(body)}"
            )
        try:
            decoded = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise QueueError("request body must contain valid UTF-8") from exc
        return parse_qs(decoded, keep_blank_values=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        run_event = RUN_EVENT_PATTERN.fullmatch(parsed.path)
        run_page = RUN_PAGE_PATTERN.fullmatch(parsed.path)
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
        if run_event:
            session = self._authorized_session("admin")
            if session is None:
                self._send(
                    HTTPStatus.UNAUTHORIZED,
                    b"authentication required\n",
                    content_type="text/plain; charset=utf-8",
                )
                return
            self._send_live_events(f"run-{run_event.group(1)}", session)
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
        if run_page:
            session = self._require("admin")
            if session:
                try:
                    page = self.server.app.render_run(session, int(run_page.group(1)))
                except QueueError as exc:
                    self._send(
                        HTTPStatus.NOT_FOUND,
                        self.server.app._page(
                            "Run not found",
                            f'<main class="shell"><h1>Run not found</h1>'
                            f'<div class="flash error">{_escape(exc)}</div>'
                            '<a class="button secondary" href="/admin">Back to queue</a></main>',
                        ),
                    )
                else:
                    self._send(HTTPStatus.OK, page)
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
            if not self.server.app.begin_login_attempt(client, role):
                self._send(HTTPStatus.TOO_MANY_REQUESTS, self.server.app.render_login(role, error="Too many failed attempts; wait five minutes."))
                return
            if not self.server.app.auth.verify_password(role, _field(form, "password")):
                self._send(HTTPStatus.UNAUTHORIZED, self.server.app.render_login(role, error="Incorrect password."))
                return
            self.server.app.login_succeeded(client, role)
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
