"""Represent one independent SPECFEM worker without invoking a real solver."""

from __future__ import annotations

import json
import os
from pathlib import Path


dataset = Path(os.environ["EXPERIMENT_QUEUE_MOUNT_DATASETS"])
target = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_RESULT"])
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(
    json.dumps({"fixture": "specfem-worker", "datasetRoot": str(dataset)}) + "\n",
    encoding="utf-8",
)
