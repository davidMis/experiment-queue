"""Write a local Flowers compatibility result without scientific dependencies."""

from __future__ import annotations

import json
import os
from pathlib import Path


target = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_RESULT"])
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps({"fixture": "simple"}) + "\n", encoding="utf-8")
