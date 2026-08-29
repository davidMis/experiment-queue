"""Model a tracker-aware checkpoint while keeping tracker context project-owned."""

from __future__ import annotations

import json
import os
from pathlib import Path

from experiment_queue.cooperative_yield import (
    CooperativeYieldHelper,
    OpaqueResumeContext,
    YieldProgress,
)


checkpoint = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_CHECKPOINT"])
result = Path(os.environ["EXPERIMENT_QUEUE_ARTIFACT_RESULT"])
helper = CooperativeYieldHelper.from_environment()
request = None if helper is None else helper.request_if_present()
if request is not None:
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps({"next_step": 4}) + "\n", encoding="utf-8")
    helper.write_ready(
        request,
        checkpoint_files={"checkpoint": checkpoint},
        progress=YieldProgress(unit="steps", completed=4, total=10),
        resume_context=OpaqueResumeContext.from_json(
            {"checkpoint": "checkpoint", "tracker": "project-owned"}
        ),
    )
else:
    result.parent.mkdir(parents=True, exist_ok=True)
    result.write_text(json.dumps({"fixture": "tracked"}) + "\n", encoding="utf-8")
