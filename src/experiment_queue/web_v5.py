"""Project-aware authenticated web surface for schema-v5 queue state.

The request handler in this module performs authentication, authorization,
bounded input parsing, and presentation only. Durable reads and mutations are
delegated to the typed
:class:`experiment_queue.v5_operator_repository.V5OperatorRepository` and
:class:`experiment_queue.reservation_v5.V5ReservationService` boundaries, with
process control delegated to
:class:`experiment_queue.scheduler_service_v5.V5SchedulerService`, so no HTTP
route owns SQL or scheduler state transitions.
"""

from __future__ import annotations

import argparse
import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
import socket
import ssl
import stat
import tempfile
import threading
import time
from typing import Any, Final, Protocol
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse

from experiment_queue.config import StateDirectoryError, resolve_state_dir
from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.identity import validate_project_key
from experiment_queue.reservation_v5 import (
    MAX_RESERVATION_HOURS,
    MIN_RESERVATION_HOURS,
    V5GpuReservation,
    V5ReservationError,
    V5ReservationService,
)
from experiment_queue.scheduler_service_v5 import (
    V5SchedulerService,
    V5SchedulerServiceError,
)
from experiment_queue.scheduler_v5 import V5SchedulerError
from experiment_queue.v5_operator_repository import (
    V5ArtifactRecord,
    V5OperatorError,
    V5OperatorItemView,
    V5OperatorNotFoundError,
    V5OperatorRepository,
    V5ProjectSummary,
)


AUTH_FILENAME: Final = "web_auth.json"
SESSION_COOKIE: Final = "experiment_queue_v5_session"
SESSION_SECONDS: Final = 12 * 60 * 60
MAX_FORM_BYTES: Final = 32 * 1024
MAX_QUERY_BYTES: Final = 8 * 1024
MAX_PAGE_SIZE: Final = 100
DEFAULT_PAGE_SIZE: Final = 25
MAX_AUTH_BYTES: Final = 1024 * 1024
LOGIN_WINDOW_SECONDS: Final = 5 * 60
LOGIN_MAX_FAILURES: Final = 8
RESERVATION_ACTION_WINDOW_SECONDS: Final = 60
RESERVATION_ACTION_MAX_FAILURES: Final = 12

ROLE_HOST_ADMIN: Final = "host-admin"
ROLE_OPERATOR: Final = "operator"
ROLE_VIEWER: Final = "viewer"
ROLE_RESERVER: Final = "reserver"
WEB_ROLES: Final = frozenset(
    {ROLE_HOST_ADMIN, ROLE_OPERATOR, ROLE_VIEWER, ROLE_RESERVER}
)
_LEGACY_ROLE_ALIASES: Final = {
    "admin": ROLE_HOST_ADMIN,
    "reservation": ROLE_RESERVER,
}
_ROLE_CAPABILITIES: Final = {
    ROLE_HOST_ADMIN: frozenset(
        {
            "project.read",
            "project.mutate",
            "host.read",
            "host.mutate",
            "reservation.read",
            "reservation.mutate",
        }
    ),
    ROLE_OPERATOR: frozenset({"project.read", "project.mutate", "host.read"}),
    ROLE_VIEWER: frozenset({"project.read", "host.read"}),
    ROLE_RESERVER: frozenset({"reservation.read", "reservation.mutate"}),
}
_PROJECT_ROUTE = re.compile(r"^/projects/([^/]+)$")
_ITEM_ROUTE = re.compile(r"^/projects/([^/]+)/items/([1-9][0-9]*)$")
_PROJECT_ACTION_ROUTE = re.compile(r"^/projects/([^/]+)/actions$")
_ITEM_ACTION_ROUTE = re.compile(
    r"^/projects/([^/]+)/items/([1-9][0-9]*)/actions$"
)
_API_QUEUE_ROUTE = re.compile(r"^/api/projects/([^/]+)/queue$")
_API_EVENTS_ROUTE = re.compile(r"^/api/projects/([^/]+)/events$")
_TERMINABLE_ITEM_STATES: Final = frozenset(
    {"running", "yielding", "terminating", "force_killing"}
)


class V5WebError(ValueError):
    """Raised when a web request cannot be served safely."""


class V5WebAuthorizationError(V5WebError):
    """Raised when an authenticated principal lacks an endpoint capability."""


class V5WebNotFoundError(V5WebError):
    """Raised for absent or deliberately concealed direct-route resources."""


class V5WebRateLimitError(V5WebError):
    """Raised when one signed session exceeds a bounded mutation rate."""


def _read_private_auth_json(path: Path) -> object:
    """Read one stable owner-only regular auth file without following links."""

    supplied = Path(path)
    if not supplied.is_absolute():
        raise V5WebError(f"web-auth file path must be absolute, got {supplied}")
    try:
        canonical_parent = supplied.parent.resolve(strict=True)
        requested = canonical_parent / supplied.name
        entry_before = os.stat(requested, follow_symlinks=False)
    except (OSError, RuntimeError) as exc:
        raise V5WebError(
            f"web authentication is not configured at {supplied}; run "
            "auth-setup first"
        ) from exc
    if stat.S_ISLNK(entry_before.st_mode):
        raise V5WebError(f"web-auth file must not be a symlink: {requested}")
    if not stat.S_ISREG(entry_before.st_mode):
        raise V5WebError(f"web-auth file must be a regular file: {requested}")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(requested, flags)
        opened_before = os.fstat(descriptor)
        if not stat.S_ISREG(opened_before.st_mode):
            raise V5WebError(f"web-auth file must be a regular file: {requested}")
        if (entry_before.st_dev, entry_before.st_ino) != (
            opened_before.st_dev,
            opened_before.st_ino,
        ):
            raise V5WebError(
                f"web-auth file changed before it was opened: {requested}"
            )
        mode = stat.S_IMODE(opened_before.st_mode)
        if opened_before.st_uid != os.geteuid() or mode != 0o600:
            raise V5WebError(
                f"web-auth file must be owned by uid {os.geteuid()} with mode "
                f"0600: {requested}; got uid {opened_before.st_uid} and mode "
                f"{mode:04o}"
            )
        if opened_before.st_nlink != 1:
            raise V5WebError(
                f"web-auth file must have exactly one filesystem link: {requested}"
            )
        if not 1 <= opened_before.st_size <= MAX_AUTH_BYTES:
            raise V5WebError(
                f"web-auth file must contain 1 through {MAX_AUTH_BYTES} bytes: "
                f"{requested}"
            )
        chunks: list[bytes] = []
        remaining = MAX_AUTH_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        source = b"".join(chunks)
        opened_after = os.fstat(descriptor)
        entry_after = os.stat(requested, follow_symlinks=False)
    except V5WebError:
        raise
    except OSError as exc:
        raise V5WebError(f"cannot securely read web-auth file {requested}: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)

    before_identity = (
        opened_before.st_dev,
        opened_before.st_ino,
        opened_before.st_mode,
        opened_before.st_uid,
        opened_before.st_nlink,
        opened_before.st_size,
        opened_before.st_mtime_ns,
        opened_before.st_ctime_ns,
    )
    after_identity = (
        opened_after.st_dev,
        opened_after.st_ino,
        opened_after.st_mode,
        opened_after.st_uid,
        opened_after.st_nlink,
        opened_after.st_size,
        opened_after.st_mtime_ns,
        opened_after.st_ctime_ns,
    )
    entry_identity = (
        entry_after.st_dev,
        entry_after.st_ino,
        entry_after.st_mode,
        entry_after.st_uid,
        entry_after.st_nlink,
        entry_after.st_size,
        entry_after.st_mtime_ns,
        entry_after.st_ctime_ns,
    )
    if before_identity != after_identity or after_identity != entry_identity:
        raise V5WebError(f"web-auth file changed while it was read: {requested}")
    if not source or len(source) > MAX_AUTH_BYTES or len(source) != opened_after.st_size:
        raise V5WebError(
            f"web-auth file must contain one stable document no larger than "
            f"{MAX_AUTH_BYTES} bytes: {requested}"
        )
    try:
        return json.loads(source.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise V5WebError(
            f"web authentication is not valid UTF-8 JSON at {requested}: {exc}"
        ) from exc


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _atomic_write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish and synchronize authentication configuration with mode ``0600``."""

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
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        temporary = None
    except OSError as exc:
        raise V5WebError(
            f"could not durably publish web-auth configuration {path}: {exc}; "
            "if the final path is visible, inspect it before retrying"
        ) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _password_record(password: str) -> dict[str, Any]:
    if type(password) is not str or len(password) < 12:
        raise V5WebError("web passwords must contain at least 12 characters")
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


def _canonical_role(role: str) -> str:
    canonical = _LEGACY_ROLE_ALIASES.get(role, role)
    if canonical not in WEB_ROLES:
        raise V5WebError(
            f"unknown web role {role!r}; expected one of {sorted(WEB_ROLES)}"
        )
    return canonical


def _project_scope(value: object, *, role: str) -> tuple[str, ...] | None:
    """Validate a role's project scope; ``None`` means every Project."""

    if role == ROLE_RESERVER:
        if value not in (None, [], ()):
            raise V5WebError("the reserver role cannot receive Project access")
        return ()
    if value in (None, "*"):
        return None
    if type(value) not in {list, tuple}:
        raise V5WebError(
            f"Project scope for role {role!r} must be '*' or a list of keys"
        )
    result: list[str] = []
    for raw in value:
        if type(raw) is not str:
            raise V5WebError(
                f"Project scope for role {role!r} contains a non-string key"
            )
        try:
            key = validate_project_key(raw)
        except (TypeError, ValueError) as exc:
            raise V5WebError(
                f"Project scope for role {role!r} contains invalid key {raw!r}: {exc}"
            ) from exc
        if key in result:
            raise V5WebError(
                f"Project scope for role {role!r} repeats key {key!r}"
            )
        result.append(key)
    return tuple(sorted(result))


def initialize_v5_web_auth(
    state_dir: Path,
    *,
    role_passwords: Mapping[str, str],
    project_scopes: Mapping[str, Sequence[str] | str | None] | None = None,
) -> Path:
    """Create schema-v2 credentials for one or more compatibility roles.

    A role entry may be omitted when that shared login is not deployed.  The
    host-admin role is mandatory.  Operator and viewer roles default to all
    Projects; a finite key list can narrow either shared role.  Reservers never
    receive Project visibility.
    """

    state_path = Path(state_dir)
    if not state_path.is_absolute():
        raise V5WebError(
            f"web authentication state directory must be absolute, got {state_path}"
        )
    if type(role_passwords) is not dict:
        raise V5WebError("role_passwords must be a plain mapping")
    normalized_passwords: dict[str, str] = {}
    for supplied_role, password in role_passwords.items():
        if type(supplied_role) is not str:
            raise V5WebError("web role names must be strings")
        role = _canonical_role(supplied_role)
        if role in normalized_passwords:
            raise V5WebError(f"web role {role!r} was configured more than once")
        normalized_passwords[role] = password
    if ROLE_HOST_ADMIN not in normalized_passwords:
        raise V5WebError("web authentication requires a host-admin password")
    passwords = list(normalized_passwords.values())
    for index, password in enumerate(passwords):
        for other in passwords[index + 1 :]:
            if hmac.compare_digest(password, other):
                raise V5WebError("each configured web role must use a different password")
    supplied_scopes = {} if project_scopes is None else dict(project_scopes)
    normalized_scopes: dict[str, object] = {}
    for supplied_role, scope in supplied_scopes.items():
        role = _canonical_role(supplied_role)
        if role not in normalized_passwords:
            raise V5WebError(
                f"Project scope was supplied for unconfigured role {role!r}"
            )
        normalized_scopes[role] = scope

    roles: dict[str, Any] = {}
    for role, password in normalized_passwords.items():
        if role == ROLE_HOST_ADMIN:
            scope: tuple[str, ...] | None = None
        else:
            scope = _project_scope(normalized_scopes.get(role), role=role)
        roles[role] = {
            **_password_record(password),
            "projects": "*" if scope is None else list(scope),
        }
    path = state_path.resolve() / AUTH_FILENAME
    _atomic_write_private_json(
        path,
        {
            "schema_version": 2,
            "created_at": _utc_now(),
            "auth_version": secrets.token_hex(16),
            "session_secret": _b64encode(secrets.token_bytes(32)),
            "roles": roles,
        },
    )
    return path


@dataclass(frozen=True, slots=True)
class V5WebSession:
    """Signed compatibility-role session with an explicit Project scope."""

    role: str
    csrf: str
    subject: str
    expires_epoch: int
    project_keys: tuple[str, ...] | None

    def has(self, capability: str) -> bool:
        """Return whether this compatibility role owns ``capability``."""

        return capability in _ROLE_CAPABILITIES[self.role]

    def can_read_project(self, project_key: str) -> bool:
        """Check the signed role scope before any direct Project data read."""

        return self.has("project.read") and (
            self.project_keys is None or project_key in self.project_keys
        )


class V5AuthManager:
    """Verify v2 credentials and legacy v1 admin/reservation credentials."""

    def __init__(self, path: Path):
        supplied = Path(path)
        if not supplied.is_absolute():
            raise V5WebError(f"web-auth file path must be absolute, got {supplied}")
        self.path = supplied.parent.resolve(strict=True) / supplied.name
        self.config = _read_private_auth_json(self.path)
        if type(self.config) is not dict or self.config.get("schema_version") not in {
            1,
            2,
        }:
            raise V5WebError(f"unsupported web-auth schema in {self.path}")
        try:
            self.secret = _b64decode(str(self.config["session_secret"]))
            self.auth_version = str(self.config["auth_version"])
        except (KeyError, TypeError, ValueError) as exc:
            raise V5WebError(
                f"web-auth configuration is incomplete: {self.path}"
            ) from exc
        if len(self.secret) < 32 or not self.auth_version:
            raise V5WebError(f"web-auth configuration is incomplete: {self.path}")

    def _record(self, role: str) -> Mapping[str, object] | None:
        canonical = _canonical_role(role)
        configured = self.config.get("roles")
        if type(configured) is not dict:
            return None
        if self.config["schema_version"] == 1:
            legacy_name = "admin" if canonical == ROLE_HOST_ADMIN else (
                "reservation" if canonical == ROLE_RESERVER else ""
            )
            record = configured.get(legacy_name)
        else:
            record = configured.get(canonical)
        return record if type(record) is dict else None

    def verify_password(self, role: str, password: str) -> bool:
        """Verify one configured canonical or legacy role password."""

        try:
            record = self._record(role)
        except V5WebError:
            return False
        if record is None or record.get("algorithm") != "scrypt":
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

    def _scope_for_role(self, role: str) -> tuple[str, ...] | None:
        if role == ROLE_HOST_ADMIN:
            return None
        if role == ROLE_RESERVER:
            return ()
        record = self._record(role)
        if record is None:
            raise V5WebError(f"web role {role!r} is not configured")
        return _project_scope(record.get("projects"), role=role)

    def issue_session(
        self,
        role: str,
        *,
        now_epoch: int | None = None,
    ) -> tuple[str, V5WebSession]:
        """Issue a signed session carrying immutable role and Project scope."""

        canonical = _canonical_role(role)
        if self._record(canonical) is None:
            raise V5WebError(f"web role {canonical!r} is not configured")
        now = int(time.time()) if now_epoch is None else int(now_epoch)
        subject = _b64encode(
            hmac.new(
                self.secret,
                f"subject:{self.auth_version}:{canonical}".encode("utf-8"),
                hashlib.sha256,
            ).digest()[:18]
        )
        session = V5WebSession(
            role=canonical,
            csrf=secrets.token_urlsafe(24),
            # Compatibility roles are shared principals. Stable ownership lets
            # a later authenticated session for the same role release its own
            # 24-hour reservation without disclosing other actors' rows.
            subject=subject,
            expires_epoch=now + SESSION_SECONDS,
            project_keys=self._scope_for_role(canonical),
        )
        payload: dict[str, object] = {
            "role": session.role,
            "csrf": session.csrf,
            "subject": session.subject,
            "exp": session.expires_epoch,
            "projects": (
                "*" if session.project_keys is None else list(session.project_keys)
            ),
            "version": self.auth_version,
        }
        encoded = _b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
                "utf-8"
            )
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
    ) -> V5WebSession | None:
        """Verify and rehydrate a signed, unexpired role session."""

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
            role = _canonical_role(str(payload["role"]))
            expires = int(payload["exp"])
            csrf = str(payload["csrf"])
            subject = str(payload["subject"])
            scope = _project_scope(payload["projects"], role=role)
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            V5WebError,
        ):
            return None
        now = int(time.time()) if now_epoch is None else int(now_epoch)
        if (
            expires <= now
            or payload.get("version") != self.auth_version
            or not csrf
            or not subject
            or self._record(role) is None
        ):
            return None
        return V5WebSession(
            role=role,
            csrf=csrf,
            subject=subject,
            expires_epoch=expires,
            project_keys=scope,
        )


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _escape(value: object) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _field(form: Mapping[str, list[str]], name: str, default: str = "") -> str:
    values = form.get(name)
    return values[-1] if values else default


@dataclass(frozen=True, slots=True)
class V5WebProjectSummary:
    """Legacy-safe presentation view of one registered Project."""

    id: int
    key: str
    display_name: str
    lifecycle: str
    revision_id: int
    revision_sequence: int
    revision_label: str
    revision_kind: str
    git_commit: str | None
    health: str
    health_reason: str
    circuit_failure_count: int
    dispatch_allowed: bool
    host_dispatch_paused: bool
    host_pause_reason: str
    queue_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class V5WebItemSummary:
    """Project-qualified queue row safe for list and direct-route rendering."""

    id: int
    project_id: int
    project_key: str
    revision_id: int
    revision_label: str
    admission_kind: str
    experiment_id: str
    job_id: str | None
    attempt: int
    segment: int
    state: str
    priority: int
    resume_front: bool
    preemptible: bool
    git_commit: str
    card_path: str
    added_at: str
    added_by: str
    state_detail: str | None
    dependencies: tuple[int, ...]
    assigned_gpu_index: str | None = None
    assigned_gpu_uuid: str | None = None
    runtime_gpu_lease_held: bool = False
    runtime_gpu_lease_released_at: str | None = None
    pid: int | None = None
    pgid: int | None = None
    started_at: str | None = None
    finished_at: str | None = None
    return_code: int | None = None
    runner_run_dir: str | None = None
    runner_manifest_path: str | None = None
    yield_request_id: str | None = None
    yield_requested_at: str | None = None
    continuation_checkpoint: str | None = None
    continuation_checkpoint_sha256: str | None = None
    runtime_git_ref: str | None = None
    runtime_worktree_path: str | None = None
    runtime_worktree_cleanup_error: str | None = None


@dataclass(frozen=True, slots=True)
class V5WebEventSummary:
    """One Project-scoped append-only event for paginated presentation."""

    id: int
    project_id: int
    queue_item_id: int | None
    created_at: str
    actor: str
    event_type: str
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class V5WebArtifactSummary:
    """Authenticated artifact evidence; paths are never dereferenced by HTTP."""

    id: int
    queue_item_id: int
    revision_id: int
    segment: int
    evidence_kind: str
    artifact_name: str
    artifact_type: str
    root_name: str | None
    relative_path: str | None
    absolute_path: str
    size_bytes: int | None
    sha256: str | None
    recorded_at: str
    metadata: object


@dataclass(frozen=True, slots=True)
class V5WebYieldSummary:
    """Authenticated cooperative-yield receipt without opaque resume bytes."""

    request_id: str
    queue_item_id: int
    segment: int
    status: str
    written_at: str
    receipt_sha256: str
    continuation_identity_sha256: str
    progress: object
    checkpoint_artifacts: object
    resume_context_media_type: str | None
    resume_context_bytes: int | None
    resume_context_sha256: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class V5WebGpuSummary:
    """Minimized GPU availability shown to the shared reserver role."""

    uuid: str
    index: str
    name: str
    schedulable: bool
    busy: bool
    reserved: bool = False
    own_reservations: tuple["V5WebReservationSummary", ...] = ()


@dataclass(frozen=True, slots=True)
class V5WebReservationSummary:
    """Owner-filtered reservation evidence safe for the reserver page."""

    id: int
    gpu_uuid: str
    status: str
    note: str
    duration_hours: int
    requested_at: str
    starts_at: str | None
    expires_at: str | None
    released_at: str | None
    open: bool


@dataclass(frozen=True, slots=True)
class V5WebTerminationSummary:
    """Minimal durable termination audit returned to the presentation layer."""

    item_id: int
    project_id: int
    state: str
    stage: str
    requested_at: str
    signal_delivered: bool


class V5WebService(Protocol):
    """Typed data/mutation boundary consumed by :class:`V5WebApplication`."""

    def list_projects(
        self, *, project_keys: tuple[str, ...] | None
    ) -> tuple[V5WebProjectSummary, ...]: ...

    def get_project(self, project_key: str) -> V5WebProjectSummary: ...

    def list_items(
        self,
        *,
        project_id: int,
        states: tuple[str, ...],
        after_id: int,
        limit: int,
    ) -> tuple[V5WebItemSummary, ...]: ...

    def get_item(self, *, project_id: int, item_id: int) -> V5WebItemSummary: ...

    def list_events(
        self, *, project_id: int, after_id: int, limit: int
    ) -> tuple[V5WebEventSummary, ...]: ...

    def list_artifacts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebArtifactSummary, ...]: ...

    def list_yield_receipts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebYieldSummary, ...]: ...

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
    ) -> V5WebItemSummary: ...

    def request_termination(
        self,
        *,
        item_id: int,
        reason: str,
        actor: str,
        force: bool,
        requested_at: str,
    ) -> V5WebTerminationSummary: ...

    def mutate_project(
        self,
        *,
        project_id: int,
        operation: str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5WebProjectSummary: ...

    def list_reserver_gpus(
        self, *, actor: str, include_all: bool
    ) -> tuple[V5WebGpuSummary, ...]: ...

    def request_reservation(
        self,
        *,
        gpu_uuid: str,
        duration_hours: int,
        note: str,
        actor: str,
        requested_at: str,
    ) -> V5WebReservationSummary: ...

    def release_reservation(
        self,
        *,
        reservation_id: int,
        actor: str,
        released_at: str,
        allow_any: bool,
    ) -> V5WebReservationSummary: ...


class V5WebRepositoryAdapter:
    """Convert authenticated operator-repository records into web views."""

    def __init__(
        self,
        repository: V5OperatorRepository,
        reservations: V5ReservationService,
        scheduler: V5SchedulerService,
    ):
        if type(repository) is not V5OperatorRepository:
            raise TypeError(
                "repository must be exactly V5OperatorRepository, got "
                f"{type(repository).__name__}"
            )
        self.repository = repository
        if type(reservations) is not V5ReservationService:
            raise TypeError(
                "reservations must be exactly V5ReservationService, got "
                f"{type(reservations).__name__}"
            )
        self.reservations = reservations
        if type(scheduler) is not V5SchedulerService:
            raise TypeError(
                "scheduler must be exactly V5SchedulerService, got "
                f"{type(scheduler).__name__}"
            )
        self.scheduler = scheduler

    @staticmethod
    def _project(summary: V5ProjectSummary) -> V5WebProjectSummary:
        return V5WebProjectSummary(
            id=summary.id,
            key=summary.key,
            display_name=summary.display_name,
            lifecycle=summary.lifecycle.value,
            revision_id=summary.current_revision_id,
            revision_sequence=summary.current_revision_sequence,
            revision_label=summary.current_revision_label,
            revision_kind=summary.current_revision_kind,
            git_commit=summary.current_git_commit,
            health=summary.health.value,
            health_reason=summary.health_reason,
            circuit_failure_count=summary.circuit_failure_count,
            dispatch_allowed=summary.dispatch_allowed,
            host_dispatch_paused=summary.host_dispatch_paused,
            host_pause_reason=summary.host_pause_reason,
            queue_counts=summary.queue_counts,
        )

    @staticmethod
    def _item(view: V5OperatorItemView) -> V5WebItemSummary:
        item = view.item
        return V5WebItemSummary(
            id=item.id,
            project_id=item.project_id,
            project_key=view.project_key,
            revision_id=item.revision_id,
            revision_label=view.revision_label,
            admission_kind=item.admission_kind,
            experiment_id=item.experiment_id,
            job_id=item.job_id,
            attempt=item.attempt,
            segment=item.segment,
            state=item.state,
            priority=item.priority,
            resume_front=item.resume_front,
            preemptible=item.preemptible,
            git_commit=item.git_commit,
            card_path=item.card_path,
            added_at=item.added_at,
            added_by=item.added_by,
            state_detail=item.state_detail,
            dependencies=view.dependencies,
            assigned_gpu_index=view.assigned_gpu_index,
            assigned_gpu_uuid=view.assigned_gpu_uuid,
            runtime_gpu_lease_held=view.runtime_gpu_lease_held,
            runtime_gpu_lease_released_at=view.runtime_gpu_lease_released_at,
            pid=view.pid,
            pgid=view.pgid,
            started_at=view.started_at,
            finished_at=view.finished_at,
            return_code=view.return_code,
            runner_run_dir=view.runner_run_dir,
            runner_manifest_path=view.runner_manifest_path,
            yield_request_id=view.yield_request_id,
            yield_requested_at=view.yield_requested_at,
            continuation_checkpoint=view.continuation_checkpoint,
            continuation_checkpoint_sha256=view.continuation_checkpoint_sha256,
            runtime_git_ref=view.runtime_git_ref,
            runtime_worktree_path=view.runtime_worktree_path,
            runtime_worktree_cleanup_error=view.runtime_worktree_cleanup_error,
        )

    @staticmethod
    def _artifact(record: V5ArtifactRecord) -> V5WebArtifactSummary:
        return V5WebArtifactSummary(
            id=record.id,
            queue_item_id=record.queue_item_id,
            revision_id=record.revision_id,
            segment=record.segment,
            evidence_kind=record.evidence_kind,
            artifact_name=record.artifact_name,
            artifact_type=record.artifact_type,
            root_name=record.root_name,
            relative_path=record.relative_path,
            absolute_path=str(record.absolute_path),
            size_bytes=record.size_bytes,
            sha256=record.sha256,
            recorded_at=record.recorded_at,
            metadata=record.metadata,
        )

    @staticmethod
    def _reservation(record: V5GpuReservation) -> V5WebReservationSummary:
        return V5WebReservationSummary(
            id=record.id,
            gpu_uuid=record.gpu_uuid,
            status=record.status.value,
            note=record.note,
            duration_hours=record.duration_hours,
            requested_at=record.requested_at,
            starts_at=record.starts_at,
            expires_at=record.expires_at,
            released_at=record.released_at,
            open=record.is_open,
        )

    def list_projects(
        self, *, project_keys: tuple[str, ...] | None
    ) -> tuple[V5WebProjectSummary, ...]:
        if project_keys == ():
            return ()
        summaries = self.repository.list_project_summaries(
            project_keys=() if project_keys is None else project_keys,
            limit=10_000,
        )
        return tuple(self._project(summary) for summary in summaries)

    def get_project(self, project_key: str) -> V5WebProjectSummary:
        return self._project(
            self.repository.get_project_summary(project_key=project_key)
        )

    def list_items(
        self,
        *,
        project_id: int,
        states: tuple[str, ...],
        after_id: int,
        limit: int,
    ) -> tuple[V5WebItemSummary, ...]:
        return tuple(
            self._item(view)
            for view in self.repository.list_items(
                project_id=project_id,
                states=states,
                after_id=after_id,
                limit=limit,
            )
        )

    def get_item(self, *, project_id: int, item_id: int) -> V5WebItemSummary:
        return self._item(
            self.repository.get_item(item_id, project_id=project_id)
        )

    def list_events(
        self, *, project_id: int, after_id: int, limit: int
    ) -> tuple[V5WebEventSummary, ...]:
        return tuple(
            V5WebEventSummary(
                id=event.id,
                project_id=project_id,
                queue_item_id=event.queue_item_id,
                created_at=event.created_at,
                actor=event.actor,
                event_type=event.event_type,
                payload=event.payload,
            )
            for event in self.repository.list_events(
                project_id=project_id,
                after_id=after_id,
                limit=limit,
            )
        )

    def list_artifacts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebArtifactSummary, ...]:
        return tuple(
            self._artifact(record)
            for record in self.repository.list_artifacts(
                project_id=project_id,
                queue_item_id=item_id,
                limit=10_000,
            )
        )

    def list_yield_receipts(
        self, *, project_id: int, item_id: int
    ) -> tuple[V5WebYieldSummary, ...]:
        records = self.repository.list_yield_receipts(
            project_id=project_id,
            queue_item_id=item_id,
        )
        result: list[V5WebYieldSummary] = []
        for record in records:
            receipt = record.receipt
            context = receipt.resume_context
            continuation = receipt.continuation
            result.append(
                V5WebYieldSummary(
                    request_id=receipt.request_id,
                    queue_item_id=receipt.queue_item_id,
                    segment=receipt.segment,
                    status=receipt.status.value,
                    written_at=receipt.written_at,
                    receipt_sha256=record.sha256,
                    continuation_identity_sha256=(
                        continuation.identity_sha256
                        if continuation is not None
                        else ""
                    ),
                    progress=(
                        receipt.progress.to_document()
                        if receipt.progress is not None
                        else None
                    ),
                    checkpoint_artifacts=[
                        artifact.to_document()
                        for artifact in receipt.checkpoint_artifacts
                    ],
                    resume_context_media_type=(
                        None if context is None else context.media_type
                    ),
                    resume_context_bytes=(
                        None if context is None else len(context.payload)
                    ),
                    resume_context_sha256=(
                        None if context is None else context.sha256
                    ),
                    error=receipt.error,
                )
            )
        return tuple(result)

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
        if operation == "hold":
            view = self.repository.hold_item(
                item_id,
                project_id=project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
            )
        elif operation == "release":
            view = self.repository.release_item(
                item_id,
                project_id=project_id,
                actor=actor,
                changed_at=changed_at,
            )
        elif operation == "priority":
            if priority is None:
                raise V5WebError("priority operation requires a value")
            view = self.repository.set_item_priority(
                item_id,
                project_id=project_id,
                priority=priority,
                actor=actor,
                changed_at=changed_at,
            )
        elif operation == "remove":
            view = self.repository.remove_item(
                item_id,
                project_id=project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
            )
        else:  # guarded by the request service; retained for direct callers
            raise V5WebError(f"unsupported queue-item operation {operation!r}")
        return self._item(view)

    def request_termination(
        self,
        *,
        item_id: int,
        reason: str,
        actor: str,
        force: bool,
        requested_at: str,
    ) -> V5WebTerminationSummary:
        """Delegate durable process control to the schema-v5 scheduler service."""

        outcome = self.scheduler.request_termination(
            item_id,
            reason=reason,
            actor=actor,
            force=force,
            requested_at=requested_at,
        )
        action = outcome.action
        return V5WebTerminationSummary(
            item_id=action.item_id,
            project_id=action.project_id,
            state=action.state,
            stage=action.stage,
            requested_at=action.requested_at,
            signal_delivered=outcome.signal_delivered,
        )

    def mutate_project(
        self,
        *,
        project_id: int,
        operation: str,
        reason: str,
        actor: str,
        changed_at: str,
    ) -> V5WebProjectSummary:
        if operation == "repair":
            summary = self.repository.close_project_circuit(
                project_id=project_id,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
            )
        else:
            target = {
                "pause": "paused",
                "resume": "active",
                "archive": "archived",
            }.get(operation)
            if target is None:
                raise V5WebError(f"unsupported Project operation {operation!r}")
            summary = self.repository.transition_project(
                project_id=project_id,
                target=target,
                reason=reason,
                actor=actor,
                changed_at=changed_at,
            )
        return self._project(summary)

    def list_reserver_gpus(
        self, *, actor: str, include_all: bool
    ) -> tuple[V5WebGpuSummary, ...]:
        if type(actor) is not str or not actor:
            raise V5WebError("reservation actor is required")
        if type(include_all) is not bool:
            raise TypeError("include_all must be boolean")
        visible = self.reservations.list_reservations(
            requested_by=None if include_all else actor
        )
        by_gpu: dict[str, list[V5WebReservationSummary]] = {}
        for record in visible:
            by_gpu.setdefault(record.gpu_uuid, []).append(self._reservation(record))
        open_uuids = self.reservations.open_gpu_uuids()
        return tuple(
            V5WebGpuSummary(
                uuid=entry.uuid,
                index=entry.last_index,
                name=entry.name,
                schedulable=entry.enabled and not entry.draining,
                busy=bool(entry.assigned_queue_item_ids),
                reserved=entry.uuid in open_uuids,
                own_reservations=tuple(by_gpu.get(entry.uuid, ()))[:5],
            )
            for entry in self.repository.list_gpus()
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
        return self._reservation(
            self.reservations.request_reservation(
                gpu_uuid,
                duration_hours=duration_hours,
                note=note,
                requested_by=actor,
                requested_at=requested_at,
            )
        )

    def release_reservation(
        self,
        *,
        reservation_id: int,
        actor: str,
        released_at: str,
        allow_any: bool,
    ) -> V5WebReservationSummary:
        if type(allow_any) is not bool:
            raise TypeError("allow_any must be boolean")
        if not allow_any:
            owned = self.reservations.list_reservations(requested_by=actor)
            if all(record.id != reservation_id for record in owned):
                raise V5WebNotFoundError("reservation route not found")
        return self._reservation(
            self.reservations.release_reservation(
                reservation_id,
                released_by=actor,
                released_at=released_at,
            )
        )


_STYLE = """
:root{color-scheme:dark;--bg:#111512;--panel:#19201b;--line:#354139;--text:#f2f5f2;--muted:#aeb8b1;--green:#99df80;--amber:#f0c36a;--red:#ff9188;--blue:#8fc9ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,sans-serif}.shell{max-width:1180px;margin:auto;padding:28px 20px 64px}header,.row,.nav{display:flex;gap:12px;align-items:center;flex-wrap:wrap}header{justify-content:space-between;margin-bottom:24px}.nav{justify-content:flex-end}h1{margin:.2rem 0}h2{margin-top:0}.muted{color:var(--muted)}.panel,.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:18px}.panel{margin:16px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:14px}.card{text-decoration:none;color:inherit}.card:hover{border-color:var(--blue)}.pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:3px 8px;font-size:12px}.ok{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}a{color:var(--blue)}button,.button,input,select{font:inherit;border-radius:8px;padding:8px 10px;border:1px solid var(--line)}button,.button{background:#263c2d;color:var(--text);font-weight:700;text-decoration:none;cursor:pointer}input,select{background:#0e120f;color:var(--text)}form{margin:0}.field{display:flex;flex-direction:column;gap:4px}.flash{padding:12px;border-radius:9px;border:1px solid var(--line)}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted)}.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px}.facts dt{color:var(--muted);font-size:12px}.facts dd{margin:3px 0;overflow-wrap:anywhere}.actions{display:flex;gap:6px;flex-wrap:wrap}.actions form{display:flex;gap:5px}.mono{font-family:ui-monospace,monospace;overflow-wrap:anywhere}.login{max-width:440px;margin:12vh auto}.pagination{display:flex;justify-content:space-between;align-items:center;margin-top:12px}@media(max-width:720px){table{display:block;overflow:auto}header{align-items:flex-start}.shell{padding:18px 12px 44px}}
"""


def _page(title: str, body: str) -> bytes:
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{_escape(title)}</title><style>{_STYLE}</style></head>"
        f"<body>{body}</body></html>"
    ).encode("utf-8")


def _integer_query(
    query: Mapping[str, list[str]],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    values = query.get(name)
    if not values:
        return default
    if len(values) != 1:
        raise V5WebError(f"query parameter {name!r} may be supplied only once")
    try:
        value = int(values[0])
    except ValueError as exc:
        raise V5WebError(f"query parameter {name!r} must be a whole number") from exc
    if not minimum <= value <= maximum:
        raise V5WebError(
            f"query parameter {name!r} must be from {minimum} through {maximum}"
        )
    return value


def _cursor_query(query: Mapping[str, list[str]], prefix: str) -> tuple[int, int]:
    after = _integer_query(
        query,
        f"{prefix}_after",
        default=0,
        minimum=0,
        maximum=2**63 - 1,
    )
    size = _integer_query(
        query,
        f"{prefix}_size",
        default=DEFAULT_PAGE_SIZE,
        minimum=1,
        maximum=MAX_PAGE_SIZE,
    )
    return after, size


def _state_filter(query: Mapping[str, list[str]]) -> tuple[str, ...]:
    values = query.get("state", [])
    if len(values) > 20:
        raise V5WebError("at most 20 queue-state filters may be supplied")
    states: list[str] = []
    for raw in values:
        for value in raw.split(","):
            if value and value not in states:
                if not re.fullmatch(r"[a-z_]{1,32}", value):
                    raise V5WebError(f"invalid queue-state filter {value!r}")
                states.append(value)
    return tuple(states)


def _next_query(
    query: Mapping[str, list[str]], *, name: str, value: int
) -> str:
    values = {key: list(parts) for key, parts in query.items()}
    values[name] = [str(value)]
    return urlencode(values, doseq=True)


class V5WebApplication:
    """Role-aware renderer whose every Project read is explicitly scoped."""

    def __init__(self, service: V5WebService, auth: V5AuthManager):
        self.service = service
        self.auth = auth
        self.login_failures: dict[tuple[str, str], list[float]] = {}
        self._login_failure_lock = threading.Lock()
        self.reservation_actions: dict[str, list[float]] = {}
        self._reservation_action_lock = threading.Lock()

    @staticmethod
    def actor(session: V5WebSession) -> str:
        """Return a bounded pseudonymous audit actor for one shared-role session."""

        digest = hashlib.sha256(session.subject.encode("utf-8")).hexdigest()[:16]
        return f"web:{session.role}:{digest}"

    @staticmethod
    def force_kill_confirmation(project_key: str, item_id: int) -> str:
        """Return the exact human-entered token bound to one Project/item route."""

        return f"FORCE KILL {project_key} #{item_id}"

    def begin_login_attempt(self, client: str, role: str) -> bool:
        canonical = _canonical_role(role)
        now = time.monotonic()
        key = (client, canonical)
        with self._login_failure_lock:
            recent = [
                value
                for value in self.login_failures.get(key, [])
                if now - value < LOGIN_WINDOW_SECONDS
            ]
            if len(recent) >= LOGIN_MAX_FAILURES:
                self.login_failures[key] = recent
                return False
            recent.append(now)
            self.login_failures[key] = recent
            return True

    def login_succeeded(self, client: str, role: str) -> None:
        canonical = _canonical_role(role)
        with self._login_failure_lock:
            self.login_failures.pop((client, canonical), None)

    def _admit_reservation_action(self, session: V5WebSession) -> None:
        """Bound reservation mutations per signed shared principal."""

        now = time.monotonic()
        with self._reservation_action_lock:
            recent = [
                value
                for value in self.reservation_actions.get(session.subject, [])
                if now - value < RESERVATION_ACTION_WINDOW_SECONDS
            ]
            if len(recent) >= RESERVATION_ACTION_MAX_FAILURES:
                self.reservation_actions[session.subject] = recent
                raise V5WebRateLimitError(
                    "too many reservation actions; wait one minute and retry"
                )
            recent.append(now)
            self.reservation_actions[session.subject] = recent

    def _reservation_request_token(
        self, session: V5WebSession, gpu_uuid: str
    ) -> str:
        """Bind an exact GPU and retry timestamp to this signed principal."""

        now = int(time.time())
        payload = {
            "gpu": gpu_uuid,
            "subject": session.subject,
            "requested_at": _utc_now(),
            "exp": now + 15 * 60,
            "version": self.auth.auth_version,
        }
        encoded = _b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
                "utf-8"
            )
        )
        signature = _b64encode(
            hmac.new(
                self.auth.secret,
                f"reservation:{encoded}".encode("ascii"),
                hashlib.sha256,
            ).digest()
        )
        return f"{encoded}.{signature}"

    def _verify_reservation_request_token(
        self, session: V5WebSession, token: str
    ) -> tuple[str, str]:
        """Return exact GPU/timestamp or reject tampering, expiry, and replay scope."""

        if not token or "." not in token or len(token) > 4096:
            raise V5WebAuthorizationError("invalid reservation request token")
        encoded, signature = token.rsplit(".", 1)
        expected = _b64encode(
            hmac.new(
                self.auth.secret,
                f"reservation:{encoded}".encode("ascii"),
                hashlib.sha256,
            ).digest()
        )
        if not hmac.compare_digest(signature, expected):
            raise V5WebAuthorizationError("invalid reservation request token")
        try:
            payload = json.loads(_b64decode(encoded))
            gpu_uuid = str(payload["gpu"])
            requested_at = str(payload["requested_at"])
            expires = int(payload["exp"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise V5WebAuthorizationError(
                "invalid reservation request token"
            ) from exc
        if (
            payload.get("subject") != session.subject
            or payload.get("version") != self.auth.auth_version
            or expires <= int(time.time())
            or not gpu_uuid
            or not requested_at
        ):
            raise V5WebAuthorizationError("invalid reservation request token")
        return gpu_uuid, requested_at

    @staticmethod
    def _require(session: V5WebSession, capability: str) -> None:
        if not session.has(capability):
            raise V5WebAuthorizationError(
                f"role {session.role!r} does not have {capability!r} access"
            )

    def _project(
        self, session: V5WebSession, project_key: str
    ) -> V5WebProjectSummary:
        """Authorize the URL key before asking the repository for any data."""

        try:
            key = validate_project_key(project_key)
        except (TypeError, ValueError) as exc:
            raise V5WebNotFoundError("Project route not found") from exc
        if not session.can_read_project(key):
            # Deliberately conceal whether the stable key exists.
            raise V5WebNotFoundError("Project route not found")
        project = self.service.get_project(key)
        if project.key != key:
            raise V5WebError("operator repository returned mismatched Project identity")
        return project

    def render_login(self, role: str, *, error: str | None = None) -> bytes:
        canonical = _canonical_role(role)
        problem = (
            f'<div class="flash bad">{_escape(error)}</div>' if error else ""
        )
        body = f'''<main class="shell"><section class="panel login"><p class="muted">experiment-queue · schema v5</p>
<h1>{_escape(canonical)} sign in</h1>{problem}<form method="post" action="/login/{quote(canonical)}">
<div class="field"><label for="password">Password</label><input id="password" name="password" type="password" autocomplete="current-password" required autofocus></div>
<p><button type="submit">Sign in</button></p></form></section></main>'''
        return _page(f"{canonical} sign in", body)

    def _header(self, session: V5WebSession, title: str) -> str:
        projects = (
            '<a class="button" href="/projects">Projects</a>'
            if session.has("project.read")
            else ""
        )
        reserve = (
            '<a class="button" href="/reserve">GPU availability</a>'
            if session.has("reservation.read")
            else ""
        )
        return f'''<header><div><p class="muted">experiment-queue · {_escape(session.role)}</p><h1>{_escape(title)}</h1></div>
<nav class="nav">{projects}{reserve}<form method="post" action="/logout"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><button>Sign out</button></form></nav></header>'''

    def render_projects(
        self, session: V5WebSession, query: Mapping[str, list[str]]
    ) -> bytes:
        self._require(session, "project.read")
        projects = self.service.list_projects(project_keys=session.project_keys)
        if session.project_keys is not None and any(
            project.key not in session.project_keys for project in projects
        ):
            raise V5WebError("operator repository returned an unauthorized Project")
        cards: list[str] = []
        for project in projects:
            counts = " · ".join(
                f"{_escape(state)} {count}" for state, count in project.queue_counts
            ) or "no queue items"
            health_class = "ok" if project.dispatch_allowed else "warn"
            commit = project.git_commit[:12] if project.git_commit else "legacy unknown"
            cards.append(
                f'''<a class="card" href="/projects/{quote(project.key)}"><h2>{_escape(project.display_name)}</h2>
<p class="mono">{_escape(project.key)}</p><p><span class="pill">{_escape(project.lifecycle)}</span> <span class="pill {health_class}">{_escape(project.health)}</span></p>
<p class="muted">{_escape(project.revision_label)} · {_escape(project.revision_kind)} · {_escape(commit)}</p><p>{counts}</p></a>'''
            )
        flash = ""
        if query.get("ok"):
            flash = f'<div class="flash ok">{_escape(query["ok"][-1])}</div>'
        body = f'''<main class="shell">{self._header(session, "Projects")}{flash}<section class="grid">
{''.join(cards) or '<p class="panel">No Projects are visible to this role.</p>'}</section></main>'''
        return _page("Projects · experiment-queue", body)

    @staticmethod
    def _queue_rows(project: V5WebProjectSummary, items: Sequence[V5WebItemSummary]) -> str:
        rows: list[str] = []
        for item in items:
            if item.project_id != project.id or item.project_key != project.key:
                raise V5WebError("operator repository returned a cross-Project queue row")
            detail = item.state_detail or ""
            if item.runtime_gpu_lease_held:
                lease_detail = "GPU runtime lease held; current idle telemetry required"
                detail = f"{detail} · {lease_detail}" if detail else lease_detail
            rows.append(
                f'''<tr><td>#{item.id}</td><td><a href="/projects/{quote(project.key)}/items/{item.id}">{_escape(item.experiment_id)}</a><br><span class="muted">attempt {item.attempt} · segment {item.segment}</span></td>
<td><span class="pill">{_escape(item.state)}</span></td><td>{item.priority}{' · front' if item.resume_front else ''}</td><td>{_escape(item.revision_label)}</td><td>{_escape(detail)}</td></tr>'''
            )
        return "".join(rows)

    @staticmethod
    def _event_rows(project: V5WebProjectSummary, events: Sequence[V5WebEventSummary]) -> str:
        rows: list[str] = []
        for event in events:
            if event.project_id != project.id:
                raise V5WebError("operator repository returned a cross-Project event")
            payload = json.dumps(event.payload, ensure_ascii=False, sort_keys=True)
            rows.append(
                f'''<tr><td>#{event.id}</td><td>{_escape(event.created_at)}</td><td>{_escape(event.event_type)}</td><td>{_escape(event.queue_item_id or '—')}</td><td>{_escape(event.actor)}<br><span class="mono muted">{_escape(payload)}</span></td></tr>'''
            )
        return "".join(rows)

    def render_project(
        self,
        session: V5WebSession,
        project_key: str,
        query: Mapping[str, list[str]],
    ) -> bytes:
        project = self._project(session, project_key)
        item_after, item_size = _cursor_query(query, "queue")
        event_after, event_size = _cursor_query(query, "event")
        states = _state_filter(query)
        items = self.service.list_items(
            project_id=project.id,
            states=states,
            after_id=item_after,
            limit=item_size + 1,
        )
        events = self.service.list_events(
            project_id=project.id,
            after_id=event_after,
            limit=event_size + 1,
        )
        has_more_items = len(items) > item_size
        has_more_events = len(events) > event_size
        visible_items = items[:item_size]
        visible_events = events[:event_size]
        state_value = ",".join(states)
        item_next = (
            ""
            if not has_more_items
            else '<a class="button" href="?'
            + _escape(
                _next_query(
                    query,
                    name="queue_after",
                    value=visible_items[-1].id,
                )
            )
            + '">Next queue page</a>'
        )
        event_next = (
            ""
            if not has_more_events
            else '<a class="button" href="?'
            + _escape(
                _next_query(
                    query,
                    name="event_after",
                    value=visible_events[-1].id,
                )
            )
            + '">Next event page</a>'
        )
        revision_commit = project.git_commit or "not recorded by legacy source"
        counts = " · ".join(
            f"{_escape(state)} {count}" for state, count in project.queue_counts
        ) or "no queue items"
        mutation = ""
        if session.has("project.mutate"):
            if project.revision_kind == "legacy-v4":
                mutation = '''<section class="panel"><h2>Project controls</h2><p class="muted">This imported legacy Project remains paused and read-only at the lifecycle boundary until an authenticated Project/v1 revision is adopted through the operator CLI.</p></section>'''
            else:
                mutation = f'''<section class="panel"><h2>Project controls</h2><form class="row" method="post" action="/projects/{quote(project.key)}/actions">
<input type="hidden" name="csrf" value="{_escape(session.csrf)}"><select name="operation"><option value="pause">Pause</option><option value="resume">Resume</option><option value="archive">Archive</option><option value="repair">Close health circuit</option></select>
<input name="reason" maxlength="1000" placeholder="required reason" required><button type="submit">Apply</button></form></section>'''
        body = f'''<main class="shell">{self._header(session, project.display_name)}<p><a href="/projects">← all Projects</a></p>
<section class="panel"><dl class="facts"><div><dt>Project key</dt><dd class="mono">{_escape(project.key)}</dd></div><div><dt>Lifecycle / health</dt><dd>{_escape(project.lifecycle)} / {_escape(project.health)}</dd></div><div><dt>Dispatch</dt><dd>{'allowed' if project.dispatch_allowed else 'blocked'}</dd></div><div><dt>Revision</dt><dd>{_escape(project.revision_label)} · {_escape(project.revision_kind)}</dd></div><div><dt>Commit</dt><dd class="mono">{_escape(revision_commit)}</dd></div><div><dt>Queue</dt><dd>{counts}</dd></div></dl>
<p class="muted">{_escape(project.health_reason)}{' · host: ' + _escape(project.host_pause_reason) if project.host_dispatch_paused else ''}</p></section>{mutation}
<section class="panel"><h2>Queue</h2><form class="row" method="get"><div class="field"><label for="state">State filters (comma-separated)</label><input id="state" name="state" value="{_escape(state_value)}"></div><input type="hidden" name="queue_size" value="{item_size}"><input type="hidden" name="event_size" value="{event_size}"><button>Filter on server</button></form>
<table><thead><tr><th>ID</th><th>Experiment</th><th>State</th><th>Priority</th><th>Revision</th><th>Detail</th></tr></thead><tbody>{self._queue_rows(project, visible_items) or '<tr><td colspan="6">No matching queue items.</td></tr>'}</tbody></table><div class="pagination"><span>{len(visible_items)} rows after ID {item_after}</span>{item_next}</div></section>
<section class="panel"><h2>Events</h2><table><thead><tr><th>ID</th><th>Time</th><th>Type</th><th>Item</th><th>Evidence</th></tr></thead><tbody>{self._event_rows(project, visible_events) or '<tr><td colspan="5">No Project events.</td></tr>'}</tbody></table><div class="pagination"><span>{len(visible_events)} rows after ID {event_after}</span>{event_next}</div></section></main>'''
        return _page(f"{project.display_name} · experiment-queue", body)

    def render_item(
        self, session: V5WebSession, project_key: str, item_id: int
    ) -> bytes:
        project = self._project(session, project_key)
        item = self.service.get_item(project_id=project.id, item_id=item_id)
        if item.project_id != project.id or item.project_key != project.key:
            raise V5WebNotFoundError("queue item route not found")
        artifacts = self.service.list_artifacts(
            project_id=project.id, item_id=item.id
        )
        yields = self.service.list_yield_receipts(
            project_id=project.id, item_id=item.id
        )
        for artifact in artifacts:
            if artifact.queue_item_id != item.id:
                raise V5WebError("operator repository returned cross-item artifacts")
        for receipt in yields:
            if receipt.queue_item_id != item.id:
                raise V5WebError("operator repository returned cross-item yield evidence")
        artifact_rows = "".join(
            f'''<tr><td>#{record.id}</td><td>{_escape(record.artifact_name)}</td><td>{_escape(record.artifact_type)}</td><td class="mono">{_escape(record.absolute_path)}</td><td>{_escape(record.sha256 or '—')}</td></tr>'''
            for record in artifacts
        )
        yield_rows = "".join(
            f'''<tr><td class="mono">{_escape(record.request_id)}</td><td>{record.segment}</td><td>{_escape(record.status)}</td><td>{_escape(record.written_at)}</td><td class="mono">{_escape(record.receipt_sha256)}</td><td>{_escape(record.resume_context_media_type or '—')} · {_escape(record.resume_context_bytes if record.resume_context_bytes is not None else '—')} bytes</td></tr>'''
            for record in yields
        )
        actions = ""
        if session.has("project.mutate"):
            base = f'''<input type="hidden" name="csrf" value="{_escape(session.csrf)}">'''
            action_url = f"/projects/{quote(project.key)}/items/{item.id}/actions"
            termination_actions = ""
            if item.state in _TERMINABLE_ITEM_STATES:
                confirmation = self.force_kill_confirmation(project.key, item.id)
                termination_actions = f'''
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="terminate"><input name="reason" value="operator requested graceful termination" required><button>Terminate gracefully</button></form>
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="force-kill"><input name="reason" value="operator requested immediate force kill" required><label class="field">Type <span class="mono">{_escape(confirmation)}</span><input name="confirmation" autocomplete="off" required></label><button>Force kill</button></form>'''
            actions = f'''<section class="panel"><h2>Queue controls</h2><div class="actions">
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="hold"><input name="reason" value="operator hold" required><button>Hold</button></form>
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="release"><button>Release</button></form>
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="priority"><input name="priority" type="number" value="{item.priority}" required><button>Set priority</button></form>
<form method="post" action="{action_url}">{base}<input type="hidden" name="operation" value="remove"><input name="reason" value="operator removed pending item" required><button>Remove</button></form>{termination_actions}</div></section>'''
        dependencies = ", ".join(f"#{value}" for value in item.dependencies) or "none"
        gpu_identity = (
            "—"
            if item.assigned_gpu_uuid is None
            else f"{item.assigned_gpu_uuid} (host index {item.assigned_gpu_index or '—'})"
        )
        if item.runtime_gpu_lease_held:
            gpu_lease = (
                "held — this GPU is not reusable until current idle telemetry "
                "is authenticated"
            )
        elif item.runtime_gpu_lease_released_at is not None:
            gpu_lease = f"released at {item.runtime_gpu_lease_released_at}"
        else:
            gpu_lease = "not held"
        body = f'''<main class="shell">{self._header(session, item.experiment_id)}<p><a href="/projects/{quote(project.key)}">← {_escape(project.display_name)}</a></p>
<section class="panel"><dl class="facts"><div><dt>Global queue item</dt><dd>#{item.id}</dd></div><div><dt>Project</dt><dd class="mono">{_escape(project.key)}</dd></div><div><dt>Revision</dt><dd>{_escape(item.revision_label)} (id {item.revision_id})</dd></div><div><dt>Admission</dt><dd>{_escape(item.admission_kind)}</dd></div><div><dt>Experiment / job</dt><dd>{_escape(item.experiment_id)} / {_escape(item.job_id or 'legacy')}</dd></div><div><dt>Attempt / segment</dt><dd>{item.attempt} / {item.segment}</dd></div><div><dt>State</dt><dd>{_escape(item.state)}</dd></div><div><dt>Priority</dt><dd>{item.priority}</dd></div><div><dt>Commit</dt><dd class="mono">{_escape(item.git_commit)}</dd></div><div><dt>Card</dt><dd class="mono">{_escape(item.card_path)}</dd></div><div><dt>Dependencies</dt><dd>{dependencies}</dd></div><div><dt>GPU assignment</dt><dd>{_escape(gpu_identity)}</dd></div><div><dt>GPU runtime lease</dt><dd>{_escape(gpu_lease)}</dd></div></dl><p class="muted">{_escape(item.state_detail or '')}</p></section>{actions}
<section class="panel"><h2>Artifacts</h2><table><thead><tr><th>ID</th><th>Name</th><th>Type</th><th>Authorized path evidence</th><th>SHA-256</th></tr></thead><tbody>{artifact_rows or '<tr><td colspan="5">No artifact evidence recorded.</td></tr>'}</tbody></table></section>
<section class="panel"><h2>Cooperative-yield evidence</h2><table><thead><tr><th>Request</th><th>Segment</th><th>Status</th><th>Written</th><th>Receipt SHA-256</th><th>Opaque context</th></tr></thead><tbody>{yield_rows or '<tr><td colspan="6">No typed yield receipts recorded.</td></tr>'}</tbody></table></section></main>'''
        return _page(f"Queue item {item.id} · experiment-queue", body)

    def render_reserve(
        self,
        session: V5WebSession,
        query: Mapping[str, list[str]] | None = None,
    ) -> bytes:
        self._require(session, "reservation.read")
        actor = self.actor(session)
        devices = self.service.list_reserver_gpus(
            actor=actor,
            include_all=session.role == ROLE_HOST_ADMIN,
        )
        cards: list[str] = []
        options = "".join(
            f'<option value="{hours}"{" selected" if hours == 2 else ""}>'
            f'{hours} hour{"s" if hours != 1 else ""}</option>'
            for hours in range(MIN_RESERVATION_HOURS, MAX_RESERVATION_HOURS + 1)
        )
        for device in devices:
            if device.reserved:
                status, css = "reserved", "warn"
            elif device.busy:
                status, css = "busy", "warn"
            elif device.schedulable:
                status, css = "available", "ok"
            else:
                status, css = "unavailable", "bad"
            history: list[str] = []
            for reservation in device.own_reservations:
                times = (
                    f"requested {_escape(reservation.requested_at)} · starts "
                    f"{_escape(reservation.starts_at or 'when GPU clears')} · "
                    f"expires {_escape(reservation.expires_at or 'after activation')}"
                )
                if reservation.released_at is not None:
                    times += f" · released {_escape(reservation.released_at)}"
                release = ""
                if reservation.open and session.has("reservation.mutate"):
                    release = f'''<form method="post" action="/reserve/release"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="reservation_id" value="{reservation.id}"><button type="submit">Release reservation</button></form>'''
                history.append(
                    f'''<section class="panel"><strong>Reservation #{reservation.id} · {_escape(reservation.status)}</strong><p>{_escape(reservation.note)}</p><p class="muted">{times}</p>{release}</section>'''
                )
            request = ""
            if (
                device.schedulable
                and not device.reserved
                and session.has("reservation.mutate")
            ):
                request_token = self._reservation_request_token(
                    session, device.uuid
                )
                request = f'''<form method="post" action="/reserve/request"><input type="hidden" name="csrf" value="{_escape(session.csrf)}"><input type="hidden" name="reservation_token" value="{_escape(request_token)}"><div class="field"><label>Duration</label><select name="hours">{options}</select></div><div class="field"><label>Reservation note</label><input name="note" maxlength="200" required placeholder="who or what needs this GPU"></div><p><button type="submit">Request reservation</button></p></form>'''
            cards.append(
                f'''<article class="card"><h2>GPU {_escape(device.index)}</h2><p>{_escape(device.name)}</p><p><span class="pill {css}">{status}</span></p>{''.join(history)}{request}</article>'''
            )
        messages = {} if query is None else query
        flash = ""
        if messages.get("ok"):
            flash = f'<div class="flash ok">{_escape(messages["ok"][-1])}</div>'
        elif messages.get("error"):
            flash = (
                f'<div class="flash bad">{_escape(messages["error"][-1])}</div>'
            )
        role_note = (
            "Host administrators may release any displayed reservation."
            if session.role == ROLE_HOST_ADMIN
            else "Only reservations owned by this signed reserver role are shown and releasable."
        )
        body = f'''<main class="shell">{self._header(session, "GPU availability")}{flash}<p class="muted">This shared view intentionally omits Project names, experiment identities, other actors’ reservation notes, process IDs, and queue history. {_escape(role_note)}</p><section class="grid">{''.join(cards) or '<p class="panel">No GPUs are in the scheduler allowlist.</p>'}</section></main>'''
        return _page("GPU availability · experiment-queue", body)

    def reservation_action(
        self,
        session: V5WebSession,
        path: str,
        form: Mapping[str, list[str]],
    ) -> str:
        """Perform one owner-scoped typed reservation request or release."""

        self._require(session, "reservation.mutate")
        self._admit_reservation_action(session)
        actor = self.actor(session)
        if path == "/reserve/request":
            gpu_uuid, requested_at = self._verify_reservation_request_token(
                session, _field(form, "reservation_token")
            )
            try:
                duration = int(_field(form, "hours"))
            except ValueError as exc:
                raise V5WebError(
                    "reservation duration must be a whole number from 1 through 24"
                ) from exc
            note = _field(form, "note")
            reservation = self.service.request_reservation(
                gpu_uuid=gpu_uuid,
                duration_hours=duration,
                note=note,
                actor=actor,
                requested_at=requested_at,
            )
            return f"reservation #{reservation.id} {reservation.status}"
        if path == "/reserve/release":
            try:
                reservation_id = int(_field(form, "reservation_id"))
            except ValueError as exc:
                raise V5WebError("reservation ID must be a positive whole number") from exc
            if reservation_id <= 0:
                raise V5WebError("reservation ID must be a positive whole number")
            reservation = self.service.release_reservation(
                reservation_id=reservation_id,
                actor=actor,
                released_at=_utc_now(),
                allow_any=session.role == ROLE_HOST_ADMIN,
            )
            return f"reservation #{reservation.id} released"
        raise V5WebNotFoundError("reservation action route not found")

    def queue_document(
        self,
        session: V5WebSession,
        project_key: str,
        query: Mapping[str, list[str]],
    ) -> bytes:
        project = self._project(session, project_key)
        after, size = _cursor_query(query, "queue")
        states = _state_filter(query)
        items = self.service.list_items(
            project_id=project.id, states=states, after_id=after, limit=size + 1
        )
        for item in items:
            if item.project_id != project.id or item.project_key != project.key:
                raise V5WebError("operator repository returned a cross-Project queue row")
        visible = items[:size]
        document = {
            "project": project.key,
            "afterId": after,
            "limit": size,
            "states": list(states),
            "hasMore": len(items) > size,
            "nextAfterId": visible[-1].id if len(items) > size and visible else None,
            "items": [
                {
                    "id": item.id,
                    "projectKey": item.project_key,
                    "revisionId": item.revision_id,
                    "revisionLabel": item.revision_label,
                    "experimentId": item.experiment_id,
                    "jobId": item.job_id,
                    "attempt": item.attempt,
                    "segment": item.segment,
                    "state": item.state,
                    "priority": item.priority,
                    "gitCommit": item.git_commit,
                    "assignedGpuUuid": item.assigned_gpu_uuid,
                    "assignedGpuIndex": item.assigned_gpu_index,
                    "runtimeGpuLeaseHeld": item.runtime_gpu_lease_held,
                    "runtimeGpuLeaseReleasedAt": item.runtime_gpu_lease_released_at,
                }
                for item in visible
            ],
        }
        return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )

    def events_document(
        self,
        session: V5WebSession,
        project_key: str,
        query: Mapping[str, list[str]],
    ) -> bytes:
        project = self._project(session, project_key)
        after, size = _cursor_query(query, "event")
        events = self.service.list_events(
            project_id=project.id, after_id=after, limit=size + 1
        )
        if any(event.project_id != project.id for event in events):
            raise V5WebError("operator repository returned a cross-Project event")
        visible = events[:size]
        document = {
            "project": project.key,
            "afterId": after,
            "limit": size,
            "hasMore": len(events) > size,
            "nextAfterId": visible[-1].id if len(events) > size and visible else None,
            "events": [
                {
                    "id": event.id,
                    "projectId": event.project_id,
                    "queueItemId": event.queue_item_id,
                    "createdAt": event.created_at,
                    "actor": event.actor,
                    "eventType": event.event_type,
                    "payload": dict(event.payload),
                }
                for event in visible
            ],
        }
        return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )

    def item_action(
        self,
        session: V5WebSession,
        project_key: str,
        item_id: int,
        form: Mapping[str, list[str]],
    ) -> str:
        self._require(session, "project.mutate")
        project = self._project(session, project_key)
        # This project-qualified read deliberately precedes mutation, while the
        # repository repeats ownership inside its write transaction.
        item = self.service.get_item(project_id=project.id, item_id=item_id)
        if (
            item.project_id != project.id
            or item.project_key != project.key
            or item.id != item_id
        ):
            raise V5WebNotFoundError("queue item action route not found")
        operation = _field(form, "operation")
        if operation not in {
            "hold",
            "release",
            "priority",
            "remove",
            "terminate",
            "force-kill",
        }:
            raise V5WebError(f"unsupported queue-item operation {operation!r}")
        priority: int | None = None
        if operation == "priority":
            try:
                priority = int(_field(form, "priority"))
            except ValueError as exc:
                raise V5WebError("priority must be a signed whole number") from exc
        reason = _field(form, "reason").strip()
        if operation in {"hold", "remove", "terminate", "force-kill"} and not reason:
            raise V5WebError(f"{operation} requires a non-empty reason")
        if operation in {"terminate", "force-kill"}:
            if item.state not in _TERMINABLE_ITEM_STATES:
                raise V5WebError(
                    f"queue item {item_id} is {item.state!r}; termination requires "
                    "a committed running process identity"
                )
            force = operation == "force-kill"
            if force:
                expected = self.force_kill_confirmation(project.key, item_id)
                if not hmac.compare_digest(_field(form, "confirmation"), expected):
                    raise V5WebError(
                        "force-kill confirmation did not match the exact token "
                        f"{expected!r}"
                    )
            outcome = self.service.request_termination(
                item_id=item_id,
                reason=reason,
                actor=self.actor(session),
                force=force,
                requested_at=_utc_now(),
            )
            if outcome.project_id != project.id or outcome.item_id != item_id:
                raise V5WebError(
                    "scheduler service returned mismatched termination identity"
                )
            mode = "force kill" if force else "graceful termination"
            delivery = "signal delivered" if outcome.signal_delivered else "signal pending"
            return (
                f"queue item #{item_id} {mode} recorded; "
                f"stage {outcome.stage}; {delivery}"
            )
        updated = self.service.mutate_item(
            project_id=project.id,
            item_id=item_id,
            operation=operation,
            reason=reason,
            priority=priority,
            actor=self.actor(session),
            changed_at=_utc_now(),
        )
        if updated.project_id != project.id or updated.id != item_id:
            raise V5WebError("operator repository returned mismatched mutation identity")
        return f"queue item #{item_id} {operation} recorded"

    def project_action(
        self,
        session: V5WebSession,
        project_key: str,
        form: Mapping[str, list[str]],
    ) -> str:
        self._require(session, "project.mutate")
        project = self._project(session, project_key)
        if project.revision_kind == "legacy-v4":
            raise V5WebError(
                f"imported Project {project.key!r} cannot change lifecycle or "
                "health through the web until a Project/v1 revision is adopted"
            )
        operation = _field(form, "operation")
        if operation not in {"pause", "resume", "archive", "repair"}:
            raise V5WebError(f"unsupported Project operation {operation!r}")
        reason = _field(form, "reason").strip()
        if not reason:
            raise V5WebError(f"Project {operation} requires a non-empty reason")
        updated = self.service.mutate_project(
            project_id=project.id,
            operation=operation,
            reason=reason,
            actor=self.actor(session),
            changed_at=_utc_now(),
        )
        if updated.id != project.id or updated.key != project.key:
            raise V5WebError("operator repository returned mismatched Project mutation")
        return f"Project {project.key} {operation} recorded"


class V5WebServer(ThreadingHTTPServer):
    """Threaded server carrying one schema-v5 web application."""

    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        app: V5WebApplication,
        *,
        secure_cookies: bool = True,
    ):
        self.app = app
        self.secure_cookies = secure_cookies
        super().__init__(address, V5WebHandler)


class V5IPv6WebServer(V5WebServer):
    """Schema-v5 web server bound through the IPv6 socket family."""

    address_family = socket.AF_INET6


class V5WebHandler(BaseHTTPRequestHandler):
    """Authenticate and route requests without reading queue SQL or files."""

    server: V5WebServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        print(
            f"[{_utc_now()}] schema-v5 web {self.client_address[0]} "
            + format % args,
            flush=True,
        )

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        if self.server.secure_cookies:
            self.send_header("Strict-Transport-Security", "max-age=31536000")

    def _send(
        self,
        status: int,
        body: bytes,
        *,
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str, *, cookie: str | None = None) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self._security_headers()
        if cookie is not None:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _session(self) -> V5WebSession | None:
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        morsel = cookie.get(SESSION_COOKIE)
        return self.server.app.auth.verify_session(
            morsel.value if morsel is not None else None
        )

    def _cookie(self, token: str, *, max_age: int = SESSION_SECONDS) -> str:
        secure = " Secure;" if self.server.secure_cookies else ""
        return (
            f"{SESSION_COOKIE}={token}; Path=/; Max-Age={max_age};{secure} "
            "HttpOnly; SameSite=Strict"
        )

    def _form(self) -> dict[str, list[str]]:
        content_type = self.headers.get("Content-Type", "")
        if not content_type.startswith("application/x-www-form-urlencoded"):
            raise V5WebError("web actions require form-encoded requests")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise V5WebError("invalid request length") from exc
        if not 1 <= length <= MAX_FORM_BYTES:
            raise V5WebError(
                f"request body must be between 1 and {MAX_FORM_BYTES} bytes"
            )
        body = self.rfile.read(length)
        if len(body) != length:
            raise V5WebError(
                f"incomplete request body: expected {length} bytes, "
                f"received {len(body)}"
            )
        try:
            decoded = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise V5WebError("request body must contain valid UTF-8") from exc
        return parse_qs(decoded, keep_blank_values=True)

    def _query(self, parsed_query: str) -> dict[str, list[str]]:
        if len(parsed_query.encode("utf-8")) > MAX_QUERY_BYTES:
            raise V5WebError(
                f"query string exceeds the {MAX_QUERY_BYTES}-byte limit"
            )
        return parse_qs(parsed_query, keep_blank_values=True)

    @staticmethod
    def _project_segment(value: str) -> str:
        try:
            decoded = unquote(value, encoding="utf-8", errors="strict")
            return validate_project_key(decoded)
        except (UnicodeError, TypeError, ValueError) as exc:
            raise V5WebNotFoundError("Project route not found") from exc

    def _require_session(self, *, api: bool = False) -> V5WebSession | None:
        session = self._session()
        if session is not None:
            return session
        if api:
            self._send(
                HTTPStatus.UNAUTHORIZED,
                b"authentication required\n",
                content_type="text/plain; charset=utf-8",
            )
        else:
            self._redirect(f"/login/{ROLE_HOST_ADMIN}")
        return None

    def _not_found(self) -> None:
        self._send(
            HTTPStatus.NOT_FOUND,
            _page(
                "Not found",
                '<main class="shell"><section class="panel"><h1>Not found</h1>'
                "<p>The requested resource is unavailable.</p></section></main>",
            ),
        )

    def _bad_request(self, error: BaseException, *, status: int = 400) -> None:
        self._send(
            status,
            _page(
                "Request failed",
                '<main class="shell"><section class="panel"><h1>Request failed</h1>'
                f'<div class="flash bad">{_escape(str(error)[:1000])}</div>'
                '<p><a href="/">Return to the queue</a></p></section></main>',
            ),
        )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            query = self._query(parsed.query)
        except V5WebError as exc:
            self._bad_request(exc)
            return
        if parsed.path == "/healthz":
            self._send(
                HTTPStatus.OK,
                b"ok\n",
                content_type="text/plain; charset=utf-8",
            )
            return
        if parsed.path.startswith("/login/"):
            role = parsed.path.removeprefix("/login/")
            try:
                self._send(HTTPStatus.OK, self.server.app.render_login(role))
            except V5WebError:
                self._not_found()
            return
        if parsed.path == "/":
            session = self._session()
            if session is None:
                self._redirect(f"/login/{ROLE_HOST_ADMIN}")
            elif session.role == ROLE_RESERVER:
                self._redirect("/reserve")
            else:
                self._redirect("/projects")
            return
        session = self._require_session(api=parsed.path.startswith("/api/"))
        if session is None:
            return
        reservation_route = parsed.path == "/reserve"
        try:
            if parsed.path == "/projects":
                self._send(
                    HTTPStatus.OK,
                    self.server.app.render_projects(session, query),
                )
                return
            if parsed.path == "/reserve":
                self._send(
                    HTTPStatus.OK,
                    self.server.app.render_reserve(session, query),
                )
                return
            item_match = _ITEM_ROUTE.fullmatch(parsed.path)
            if item_match is not None:
                key = self._project_segment(item_match.group(1))
                self._send(
                    HTTPStatus.OK,
                    self.server.app.render_item(
                        session, key, int(item_match.group(2))
                    ),
                )
                return
            project_match = _PROJECT_ROUTE.fullmatch(parsed.path)
            if project_match is not None:
                key = self._project_segment(project_match.group(1))
                self._send(
                    HTTPStatus.OK,
                    self.server.app.render_project(session, key, query),
                )
                return
            queue_match = _API_QUEUE_ROUTE.fullmatch(parsed.path)
            if queue_match is not None:
                key = self._project_segment(queue_match.group(1))
                self._send(
                    HTTPStatus.OK,
                    self.server.app.queue_document(session, key, query),
                    content_type="application/json; charset=utf-8",
                )
                return
            events_match = _API_EVENTS_ROUTE.fullmatch(parsed.path)
            if events_match is not None:
                key = self._project_segment(events_match.group(1))
                self._send(
                    HTTPStatus.OK,
                    self.server.app.events_document(session, key, query),
                    content_type="application/json; charset=utf-8",
                )
                return
        except (V5WebNotFoundError, V5OperatorNotFoundError):
            self._not_found()
            return
        except V5WebAuthorizationError as exc:
            if reservation_route:
                exc = V5WebAuthorizationError(
                    "GPU availability is not authorized for this signed role"
                )
            self._bad_request(exc, status=HTTPStatus.FORBIDDEN)
            return
        except (
            V5ReservationError,
            V5WebError,
            V5OperatorError,
            V5DatabaseError,
            V5SchedulerError,
            V5SchedulerServiceError,
            OSError,
        ) as exc:
            if reservation_route:
                exc = V5WebError(
                    "GPU availability could not be loaded; retry or ask the "
                    "host administrator"
                )
                self._bad_request(exc, status=HTTPStatus.SERVICE_UNAVAILABLE)
                return
            self._bad_request(exc)
            return
        self._not_found()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            form = self._form()
        except V5WebError as exc:
            self._bad_request(exc)
            return
        if parsed.path.startswith("/login/"):
            supplied_role = parsed.path.removeprefix("/login/")
            try:
                role = _canonical_role(supplied_role)
            except V5WebError:
                self._not_found()
                return
            client = self.client_address[0]
            if not self.server.app.begin_login_attempt(client, role):
                self._send(
                    HTTPStatus.TOO_MANY_REQUESTS,
                    self.server.app.render_login(
                        role,
                        error="Too many failed attempts; wait five minutes.",
                    ),
                )
                return
            if not self.server.app.auth.verify_password(
                role, _field(form, "password")
            ):
                self._send(
                    HTTPStatus.UNAUTHORIZED,
                    self.server.app.render_login(role, error="Incorrect password."),
                )
                return
            self.server.app.login_succeeded(client, role)
            token, session = self.server.app.auth.issue_session(role)
            destination = "/reserve" if session.role == ROLE_RESERVER else "/projects"
            self._redirect(destination, cookie=self._cookie(token))
            return
        session = self._require_session()
        if session is None:
            return
        if not hmac.compare_digest(_field(form, "csrf"), session.csrf):
            self._bad_request(
                V5WebAuthorizationError(
                    "invalid form token; refresh the page and try again"
                ),
                status=HTTPStatus.FORBIDDEN,
            )
            return
        if parsed.path == "/logout":
            self._redirect(
                f"/login/{ROLE_HOST_ADMIN}",
                cookie=self._cookie("deleted", max_age=0),
            )
            return
        reservation_route = parsed.path in {
            "/reserve/request",
            "/reserve/release",
        }
        try:
            item_match = _ITEM_ACTION_ROUTE.fullmatch(parsed.path)
            if item_match is not None:
                key = self._project_segment(item_match.group(1))
                item_id = int(item_match.group(2))
                message = self.server.app.item_action(
                    session, key, item_id, form
                )
                destination = f"/projects/{quote(key)}/items/{item_id}"
            else:
                project_match = _PROJECT_ACTION_ROUTE.fullmatch(parsed.path)
                if project_match is None:
                    if reservation_route:
                        message = self.server.app.reservation_action(
                            session, parsed.path, form
                        )
                        destination = "/reserve"
                    else:
                        raise V5WebNotFoundError("action route not found")
                else:
                    key = self._project_segment(project_match.group(1))
                    message = self.server.app.project_action(session, key, form)
                    destination = f"/projects/{quote(key)}"
        except V5WebRateLimitError:
            self._send(
                HTTPStatus.TOO_MANY_REQUESTS,
                _page(
                    "Reservation request limited",
                    '<main class="shell"><section class="panel"><h1>Try again later</h1>'
                    "<p>Too many reservation actions were attempted. Wait one "
                    "minute, refresh the reservation page, and retry.</p></section></main>",
                ),
            )
            return
        except V5WebAuthorizationError as exc:
            if reservation_route:
                exc = V5WebAuthorizationError(
                    "reservation action is not authorized; refresh the page "
                    "and sign in again"
                )
            self._bad_request(exc, status=HTTPStatus.FORBIDDEN)
            return
        except (
            V5ReservationError,
            V5WebError,
            V5OperatorError,
            V5DatabaseError,
            OSError,
        ) as exc:
            if reservation_route:
                # Never echo typed-service details: they can contain exact GPU,
                # reservation, assigned-item, or other-principal identities.
                self._redirect(
                    "/reserve?"
                    + urlencode(
                        {
                            "error": (
                                "Reservation action could not be completed. "
                                "Refresh the page and retry."
                            )
                        }
                    )
                )
                return
            if isinstance(exc, (V5WebNotFoundError, V5OperatorNotFoundError)):
                self._not_found()
                return
            self._bad_request(exc, status=HTTPStatus.CONFLICT)
            return
        self._redirect(destination + "?" + urlencode({"ok": message[:500]}))


def serve_v5_web(
    app: V5WebApplication,
    *,
    host: str,
    port: int,
    tls_cert: Path | None,
    tls_key: Path | None,
    insecure_http: bool = False,
) -> None:
    """Serve schema-v5 state, requiring HTTPS except loopback test mode."""

    if type(port) is not int or not 1 <= port <= 65535:
        raise V5WebError("web port must be between 1 and 65535")
    if insecure_http:
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise V5WebError("--insecure-http is restricted to a loopback host")
    elif tls_cert is None or tls_key is None:
        raise V5WebError("HTTPS requires both --tls-cert and --tls-key")
    server_type = V5IPv6WebServer if ":" in host else V5WebServer
    try:
        server = server_type(
            (host, port),
            app,
            secure_cookies=not insecure_http,
        )
    except OSError as exc:
        raise V5WebError(
            f"could not bind schema-v5 web server to {host}:{port}: {exc}; "
            "verify the address, permissions, and that the port is available"
        ) from exc
    if not insecure_http:
        assert tls_cert is not None and tls_key is not None
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        try:
            context.load_cert_chain(str(tls_cert.resolve()), str(tls_key.resolve()))
        except (OSError, ssl.SSLError) as exc:
            server.server_close()
            raise V5WebError(f"could not load HTTPS certificate and key: {exc}") from exc
        try:
            server.socket = context.wrap_socket(server.socket, server_side=True)
        except (OSError, ssl.SSLError) as exc:
            server.server_close()
            raise V5WebError(
                f"could not initialize HTTPS listener on {host}:{port}: {exc}"
            ) from exc
    scheme = "http" if insecure_http else "https"
    print(f"schema-v5 web app listening at {scheme}://{host}:{port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("schema-v5 web app stopping", flush=True)
    finally:
        server.server_close()


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the standalone schema-v5 private-web command interface."""

    parser = argparse.ArgumentParser(
        prog="python -m experiment_queue.web_v5",
        description=(
            "Configure credentials or serve the project-aware private HTTPS "
            "interface for an existing schema-v5 queue."
        )
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help=(
            "Absolute schema-v5 state directory. Required unless "
            "EXPERIMENT_QUEUE_STATE_DIR is set; serving never creates or migrates "
            "queue.sqlite3."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    setup = subparsers.add_parser(
        "auth-setup",
        help=(
            "Replace shared compatibility-role credentials while the web service "
            "is stopped."
        ),
        description=(
            "With the web service stopped, atomically replace web_auth.json after "
            "prompting for all four shared compatibility roles. Restart the web "
            "service to load the new file; that restart invalidates every prior "
            "session. A running server retains its securely opened in-memory "
            "credentials and is not rotated by this command."
        ),
    )
    setup.add_argument(
        "--operator-project",
        action="append",
        default=None,
        metavar="KEY",
        help=(
            "Restrict the shared operator role to this Project key; repeat for "
            "additional Projects. Omit for all Projects."
        ),
    )
    setup.add_argument(
        "--viewer-project",
        action="append",
        default=None,
        metavar="KEY",
        help=(
            "Restrict the shared viewer role to this Project key; repeat for "
            "additional Projects. Omit for all Projects."
        ),
    )
    serve = subparsers.add_parser(
        "serve",
        help="Serve an existing exact schema-v5 database without migration.",
    )
    serve.add_argument(
        "--host",
        default="0.0.0.0",
        help="Private-network interface to listen on. Default: 0.0.0.0.",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=8443,
        help="HTTPS port from 1 through 65535. Default: 8443.",
    )
    serve.add_argument(
        "--tls-cert",
        type=Path,
        help="Path to a PEM certificate chain; required unless --insecure-http.",
    )
    serve.add_argument(
        "--tls-key",
        type=Path,
        help="Path to the PEM private key matching --tls-cert.",
    )
    serve.add_argument(
        "--insecure-http",
        action="store_true",
        help=(
            "Allow unencrypted HTTP only on 127.0.0.1, localhost, or ::1 for "
            "local tests; never use on a private or public network."
        ),
    )
    return parser


def _prompt_password(label: str) -> str:
    first = getpass.getpass(f"{label} password (minimum 12 characters): ")
    second = getpass.getpass(f"Confirm {label.lower()} password: ")
    if not hmac.compare_digest(first, second):
        raise V5WebError(f"{label} password confirmation did not match")
    return first


def main(argv: Sequence[str] | None = None) -> int:
    """Configure credentials or serve one existing schema-v5 queue."""

    args = build_arg_parser().parse_args(argv)
    try:
        state_dir = resolve_state_dir(args.state_dir)
        if args.action == "auth-setup":
            # Credential setup belongs to one already-created exact-v5 queue;
            # a typo must not leave a plausible auth-only state directory.
            store = V5QueueStore(state_dir)
            connection = store.connect()
            connection.close()
            path = initialize_v5_web_auth(
                state_dir,
                role_passwords={
                    ROLE_HOST_ADMIN: _prompt_password("Host administrator"),
                    ROLE_OPERATOR: _prompt_password("Project operator"),
                    ROLE_VIEWER: _prompt_password("Read-only viewer"),
                    ROLE_RESERVER: _prompt_password("GPU reserver"),
                },
                project_scopes={
                    ROLE_OPERATOR: (
                        "*"
                        if args.operator_project is None
                        else args.operator_project
                    ),
                    ROLE_VIEWER: (
                        "*" if args.viewer_project is None else args.viewer_project
                    ),
                },
            )
            print(f"schema-v5 web authentication configured at {path}")
            return 0
        store = V5QueueStore(state_dir)
        # Validate existing state through the read-only schema gate. Web startup
        # never calls initialize(), so a missing path cannot become fresh state.
        with store.connect():
            pass
        repository = V5OperatorRepository(store)
        reservations = V5ReservationService(store)
        scheduler = V5SchedulerService(store)
        app = V5WebApplication(
            V5WebRepositoryAdapter(repository, reservations, scheduler),
            V5AuthManager(state_dir / AUTH_FILENAME),
        )
        serve_v5_web(
            app,
            host=args.host,
            port=args.port,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            insecure_http=args.insecure_http,
        )
        return 0
    except (
        StateDirectoryError,
        V5DatabaseError,
        V5OperatorError,
        V5SchedulerError,
        V5SchedulerServiceError,
        V5WebError,
    ) as exc:
        print(f"schema-v5 web error: {exc}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUTH_FILENAME",
    "ROLE_HOST_ADMIN",
    "ROLE_OPERATOR",
    "ROLE_RESERVER",
    "ROLE_VIEWER",
    "V5AuthManager",
    "V5WebApplication",
    "V5WebAuthorizationError",
    "V5WebError",
    "V5IPv6WebServer",
    "V5WebHandler",
    "V5WebNotFoundError",
    "V5WebRepositoryAdapter",
    "V5WebSession",
    "V5WebTerminationSummary",
    "build_arg_parser",
    "initialize_v5_web_auth",
    "main",
    "serve_v5_web",
]
