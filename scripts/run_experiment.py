# This script is a thin CLI wrapper around the reusable experiment runner.
# It adds the repository root to the import path so the command works before
# the package is installed, then delegates all behavior to the library.

from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from experiment_queue.runner import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
