#!/usr/bin/env python
"""Expose the private HTTPS experiment-queue web app from this checkout."""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from helmholtz_shared.experiment_queue_web import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
