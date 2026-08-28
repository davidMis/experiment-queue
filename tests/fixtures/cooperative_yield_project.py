"""Tiny project-owned checkpoint adapter used by yield conformance tests."""

from __future__ import annotations

import argparse
from pathlib import Path

from experiment_queue.cooperative_yield import (
    CooperativeYieldHelper,
    OpaqueResumeContext,
    YieldProgress,
)


def main() -> int:
    """Respond to one queue request using only the optional public helper."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--fail", action="store_true")
    arguments = parser.parse_args()

    helper = CooperativeYieldHelper.from_environment()
    if helper is None:
        raise RuntimeError("fixture was not launched with cooperative-yield paths")
    request = helper.request_if_present()
    if request is None:
        raise RuntimeError("fixture did not receive a cooperative-yield request")
    progress = YieldProgress(unit="fixture_steps", completed=4, total=9)
    if arguments.fail:
        helper.write_failed(
            request,
            error="fixture intentionally could not checkpoint",
            progress=progress,
        )
        return 0

    arguments.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = arguments.checkpoint_dir / "fixture-state.json"
    checkpoint.write_text('{"next_step":4}\n', encoding="utf-8")
    helper.write_ready(
        request,
        checkpoint_files={"fixture_state": checkpoint},
        media_types={"fixture_state": "application/json"},
        progress=progress,
        resume_context=OpaqueResumeContext.from_json(
            {"checkpoint_name": "fixture_state", "next_step": 4}
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
