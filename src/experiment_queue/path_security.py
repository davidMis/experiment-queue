"""Authenticate POSIX ancestor directories used for durable state publication."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat


class PathBoundaryError(ValueError):
    """Raised when an absolute path can be renamed by an untrusted account."""


@dataclass(frozen=True, slots=True)
class SecurePathBoundary:
    """Captured device/inode identities for one canonical ancestor chain."""

    selected_path: Path
    label: str
    ancestors: tuple[tuple[Path, int, int], ...]


def _validate_ancestor(path: Path, *, label: str) -> os.stat_result:
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise PathBoundaryError(
            f"{label} ancestor {path} cannot be inspected: {exc}"
        ) from exc
    if not stat.S_ISDIR(details.st_mode) or path.is_symlink():
        raise PathBoundaryError(
            f"{label} ancestor {path} must be a non-symlink directory"
        )
    allowed_owners = {0, os.geteuid()}
    if details.st_uid not in allowed_owners:
        raise PathBoundaryError(
            f"{label} ancestor {path} must be owned by root or service uid "
            f"{os.geteuid()}, got uid {details.st_uid}"
        )
    mode = stat.S_IMODE(details.st_mode)
    writable_by_others = mode & (stat.S_IWGRP | stat.S_IWOTH)
    if writable_by_others and not details.st_mode & stat.S_ISVTX:
        raise PathBoundaryError(
            f"{label} ancestor {path} is group/world writable without the sticky "
            f"bit (mode {mode:04o}); another account could replace the selected path"
        )
    return details


def capture_secure_path_boundary(
    selected_path: Path,
    *,
    label: str,
) -> SecurePathBoundary:
    """Validate and capture every canonical ancestor from the parent through ``/``.

    Root/service-owned non-writable directories are trusted. A group/world-
    writable ancestor is accepted only with sticky semantics, where the selected
    leaf (when present) and every child ancestor are themselves root/service-owned.
    """

    selected = Path(selected_path)
    if not selected.is_absolute():
        raise PathBoundaryError(f"{label} must be absolute, got {selected}")
    if selected.exists() or selected.is_symlink():
        try:
            leaf = selected.stat(follow_symlinks=False)
        except OSError as exc:
            raise PathBoundaryError(f"{label} {selected} cannot be inspected: {exc}") from exc
        if stat.S_ISLNK(leaf.st_mode):
            raise PathBoundaryError(f"{label} {selected} must not be a symlink")
        if leaf.st_uid not in {0, os.geteuid()}:
            raise PathBoundaryError(
                f"{label} {selected} must be owned by root or service uid "
                f"{os.geteuid()}, got uid {leaf.st_uid}"
            )
    try:
        canonical_parent = selected.parent.resolve(strict=True)
    except OSError as exc:
        raise PathBoundaryError(
            f"{label} parent {selected.parent} cannot be resolved: {exc}"
        ) from exc
    if canonical_parent != selected.parent:
        raise PathBoundaryError(
            f"{label} parent must already be canonical and symlink-free, got "
            f"{selected.parent} resolving to {canonical_parent}"
        )

    identities: list[tuple[Path, int, int]] = []
    current = canonical_parent
    while True:
        details = _validate_ancestor(current, label=label)
        identities.append((current, details.st_dev, details.st_ino))
        if current.parent == current:
            break
        current = current.parent
    return SecurePathBoundary(
        selected_path=selected,
        label=label,
        ancestors=tuple(identities),
    )


def revalidate_secure_path_boundary(boundary: SecurePathBoundary) -> None:
    """Reject ancestor substitution or newly unsafe permissions after capture."""

    for path, expected_device, expected_inode in boundary.ancestors:
        details = _validate_ancestor(path, label=boundary.label)
        if (details.st_dev, details.st_ino) != (expected_device, expected_inode):
            raise PathBoundaryError(
                f"{boundary.label} ancestor {path} changed identity during the "
                "operation; preserve evidence and inspect the path before retrying"
            )


__all__ = [
    "PathBoundaryError",
    "SecurePathBoundary",
    "capture_secure_path_boundary",
    "revalidate_secure_path_boundary",
]
