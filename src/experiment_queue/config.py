"""Resolve host-level configuration shared by queue command-line tools."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


STATE_DIR_ENV = "EXPERIMENT_QUEUE_STATE_DIR"


class StateDirectoryError(ValueError):
    """Raised when an operator has not selected a safe state directory."""


def resolve_state_dir(
    cli_value: Path | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Return the explicit absolute state directory selected by the operator.

    A command-line value takes precedence over ``EXPERIMENT_QUEUE_STATE_DIR``.
    There is intentionally no current-directory or project-relative fallback.
    """

    values = os.environ if environ is None else environ
    raw_value: str | Path | None = cli_value
    source = "--state-dir"
    if raw_value is None:
        raw_value = values.get(STATE_DIR_ENV)
        source = STATE_DIR_ENV
    if raw_value is None or not str(raw_value).strip():
        raise StateDirectoryError(
            "experiment queue state directory is required; pass --state-dir "
            f"/absolute/path or set {STATE_DIR_ENV}"
        )

    expanded = Path(raw_value).expanduser()
    if not expanded.is_absolute():
        raise StateDirectoryError(
            f"{source} must be an absolute path, got {str(raw_value)!r}"
        )
    return expanded.resolve()
