"""Validate stable public identifiers used by multi-project queue state."""

from __future__ import annotations

import re


PROJECT_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
MAX_PROJECT_KEY_LENGTH = 63


def validate_project_key(value: str) -> str:
    """Return a valid immutable project key or raise an actionable error."""

    if not isinstance(value, str):
        raise ValueError(f"project key must be a string, got {type(value).__name__}")
    if len(value) > MAX_PROJECT_KEY_LENGTH or PROJECT_KEY_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "project key must contain at most 63 ASCII characters, start with a "
            "lowercase letter, and contain lowercase letters, digits, or single "
            f"hyphen-separated components; got {value!r}"
        )
    return value


__all__ = ["MAX_PROJECT_KEY_LENGTH", "PROJECT_KEY_PATTERN", "validate_project_key"]
