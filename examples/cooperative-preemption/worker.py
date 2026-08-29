"""Poll for a typed request and atomically publish a resumable checkpoint."""

from __future__ import annotations

import json
import os
from pathlib import Path
import time

from experiment_queue.cooperative_yield import (
    CooperativeYieldHelper,
    OpaqueResumeContext,
    YieldProgress,
)


checkpoint = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_CHECKPOINT"])
result = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_RESULT"])
checkpoint.parent.mkdir(parents=True, exist_ok=True)
start = 0
if checkpoint.is_file():
    start = int(json.loads(checkpoint.read_text(encoding="utf-8"))["next_step"])
helper = CooperativeYieldHelper.from_environment()

for step in range(start, 25):
    request = None if helper is None else helper.request_if_present()
    if request is not None:
        checkpoint.write_text(
            json.dumps({"next_step": step}) + "\n", encoding="utf-8"
        )
        helper.write_ready(
            request,
            checkpoint_files={"checkpoint": checkpoint},
            media_types={"checkpoint": "application/json"},
            progress=YieldProgress(unit="steps", completed=step, total=25),
            resume_context=OpaqueResumeContext.from_json({"next_step": step}),
        )
        raise SystemExit(0)
    time.sleep(0.01)

result.parent.mkdir(parents=True, exist_ok=True)
result.write_text(json.dumps({"completed_steps": 25}) + "\n", encoding="utf-8")
