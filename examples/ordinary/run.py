"""Write the exact result artifact path authorized by the queue."""

from __future__ import annotations

import json
import os
from pathlib import Path


target = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_RESULT"])
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(
    json.dumps({"status": "ok", "project": os.environ["EXPERIMENT_QUEUE_PROJECT_KEY"]})
    + "\n",
    encoding="utf-8",
)
