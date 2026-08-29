# This thin development wrapper exposes the primary queue CLI from a checkout.
# Dispatch still requires the repository-local package installation so the
# isolated durable-executor interpreter can authenticate queue control code.

from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from experiment_queue.cli_v5 import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
