"""Demonstrate logical dataset access and one declared model artifact."""

from __future__ import annotations

import json
import os
from pathlib import Path


dataset_root = Path(os.environ["EXPERIMENT_QUEUE_MOUNT_DATASET"])
target = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_MODEL"])
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(
    json.dumps({"dataset": str(dataset_root), "epochs": 3, "weights": [0.25, 0.75]})
    + "\n",
    encoding="utf-8",
)
