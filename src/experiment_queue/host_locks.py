"""Acquire host-wide GPU locks through one hardened POSIX file namespace."""

from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
import stat
from typing import BinaryIO, Final


# This path is deliberately independent of TMPDIR and queue state. Packaged v4
# and v5 schedulers must contend in one namespace for a shared physical GPU.
HOST_LOCK_PARENT: Final = Path("/tmp")
_ROOT_PREFIX: Final = "experiment-queue-host-gpu-locks-v1"


class HostGpuLockError(RuntimeError):
    """Raised when the host lock namespace cannot be authenticated safely."""


def _root_identity(path: Path) -> tuple[int, int, int, int]:
    """Validate and return stable identity for the private lock directory."""

    try:
        details = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise HostGpuLockError(f"cannot inspect host GPU lock root {path}: {exc}") from exc
    mode = stat.S_IMODE(details.st_mode)
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise HostGpuLockError(
            f"host GPU lock root must be a non-symlink directory: {path}"
        )
    if details.st_uid != os.geteuid() or mode != 0o700:
        raise HostGpuLockError(
            f"host GPU lock root {path} must be owned by uid {os.geteuid()} "
            f"with mode 0700; got uid {details.st_uid} and mode {mode:04o}"
        )
    return details.st_dev, details.st_ino, details.st_uid, details.st_mode


def _lock_root() -> Path:
    """Create or validate the fixed euid-owned host lock namespace."""

    try:
        parent = HOST_LOCK_PARENT.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise HostGpuLockError(
            f"fixed host GPU lock parent {HOST_LOCK_PARENT} is unavailable: {exc}"
        ) from exc
    if not parent.is_dir():
        raise HostGpuLockError(
            f"fixed host GPU lock parent is not a directory: {parent}"
        )
    root = parent / f"{_ROOT_PREFIX}-{os.geteuid()}"
    created = False
    try:
        os.mkdir(root, 0o700)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise HostGpuLockError(f"cannot create host GPU lock root {root}: {exc}") from exc
    if created:
        try:
            os.chmod(root, 0o700, follow_symlinks=False)
        except OSError as exc:
            raise HostGpuLockError(
                f"cannot secure new host GPU lock root {root}: {exc}"
            ) from exc
    before = _root_identity(root)
    try:
        resolved = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise HostGpuLockError(f"host GPU lock root {root} cannot resolve: {exc}") from exc
    if resolved != root or _root_identity(root) != before:
        raise HostGpuLockError(
            f"host GPU lock root changed while it was authenticated: {root}"
        )
    return root


def acquire_host_gpu_lock(gpu_uuid: str) -> BinaryIO | None:
    """Acquire one nonblocking lock shared by packaged v4 and v5 schedulers.

    ``None`` means another process holds the exact GPU lock. Every namespace or
    file-integrity problem raises instead of allowing an unsafe dispatch.
    """

    if (
        type(gpu_uuid) is not str
        or not gpu_uuid
        or gpu_uuid != gpu_uuid.strip()
        or len(gpu_uuid) > 256
    ):
        raise HostGpuLockError(
            "GPU UUID for host locking must be 1-256 characters without "
            "surrounding whitespace"
        )
    root = _lock_root()
    root_before = _root_identity(root)
    filename = hashlib.sha256(gpu_uuid.encode("utf-8")).hexdigest() + ".lock"
    path = root / filename
    base_flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    base_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor: int | None = None
    created = False
    try:
        try:
            descriptor = os.open(path, base_flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
            os.fchmod(descriptor, 0o600)
        except FileExistsError:
            descriptor = os.open(path, base_flags)
        opened = os.fstat(descriptor)
        mode = stat.S_IMODE(opened.st_mode)
        if not stat.S_ISREG(opened.st_mode):
            raise HostGpuLockError(f"host GPU lock must be a regular file: {path}")
        if opened.st_uid != os.geteuid() or mode != 0o600:
            raise HostGpuLockError(
                f"host GPU lock {path} must be owned by uid {os.geteuid()} with "
                f"mode 0600; got uid {opened.st_uid} and mode {mode:04o}"
            )
        if opened.st_nlink != 1:
            raise HostGpuLockError(
                f"host GPU lock must have exactly one filesystem link: {path}"
            )
        entry = os.stat(path, follow_symlinks=False)
        if (
            entry.st_dev,
            entry.st_ino,
            entry.st_mode,
            entry.st_uid,
            entry.st_nlink,
        ) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_nlink,
        ):
            raise HostGpuLockError(
                f"host GPU lock path changed while it was opened: {path}"
            )
        if _root_identity(root) != root_before:
            raise HostGpuLockError(
                f"host GPU lock root changed while opening {path}"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            descriptor = None
            return None
        handle = os.fdopen(descriptor, "a+b", closefd=True)
        descriptor = None
        return handle
    except HostGpuLockError:
        raise
    except OSError as exc:
        action = "create" if created else "open"
        raise HostGpuLockError(
            f"cannot safely {action} host GPU lock {path}: {exc}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


__all__ = ["HostGpuLockError", "acquire_host_gpu_lock"]
