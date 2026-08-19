# This script exposes the explicit unmanaged-GPU experiment queue from the
# repository checkout without requiring the package to be installed first.

from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from experiment_queue.queue import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
