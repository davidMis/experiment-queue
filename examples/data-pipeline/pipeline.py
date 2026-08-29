"""Write a deterministic inventory into the admitted output directory."""

from __future__ import annotations

import json
import os
from pathlib import Path


source = Path(os.environ["EXPERIMENT_QUEUE_MOUNT_INPUTS"])
output = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_TRANSFORMED"])
output.mkdir(parents=True, exist_ok=True)
entries = sorted(path.name for path in source.iterdir())
(output / "inventory.json").write_text(
    json.dumps({"entries": entries}) + "\n", encoding="utf-8"
)
