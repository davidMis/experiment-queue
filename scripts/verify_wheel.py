"""Verify that a built wheel exposes authenticated bundled schema resources."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import zipfile


REQUIRED_RESOURCES = (
    "experiment_queue/schema_resources/__init__.py",
    "experiment_queue/schema_resources/project-v1.schema.json",
    "experiment_queue/schema_resources/experiment-card-v1.schema.json",
)


def verify_wheel(wheel: Path) -> None:
    """Fail unless an isolated import from ``wheel`` authenticates both schemas."""

    if not wheel.is_absolute():
        raise ValueError(f"wheel path must be absolute, got {wheel}")
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError(f"wheel path must name an existing .whl file, got {wheel}")
    try:
        with zipfile.ZipFile(wheel) as archive:
            members = set(archive.namelist())
    except (OSError, zipfile.BadZipFile) as exc:
        raise ValueError(f"could not read wheel archive {wheel}: {exc}") from exc
    missing = sorted(set(REQUIRED_RESOURCES) - members)
    if missing:
        raise ValueError(f"wheel {wheel} is missing schema resources: {missing}")

    probe = """
import json
from pathlib import Path
import sys

wheel = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(wheel))
import experiment_queue
from experiment_queue.protocols import EXPERIMENT_CARD_V1, PROJECT_V1
from experiment_queue.schema_registry import (
    editor_schema_bytes,
    load_bundled_schema,
    schema_canonical_bytes,
    schema_sha256,
)
if not str(experiment_queue.__file__).startswith(str(wheel)):
    raise SystemExit(f"import did not come from wheel: {experiment_queue.__file__}")
for protocol in (PROJECT_V1, EXPERIMENT_CARD_V1):
    document = load_bundled_schema(protocol)
    if json.loads(editor_schema_bytes(protocol)) != document:
        raise SystemExit(f"editor export differs for {protocol}")
    import hashlib
    if hashlib.sha256(schema_canonical_bytes(protocol)).hexdigest() != schema_sha256(protocol):
        raise SystemExit(f"canonical digest differs for {protocol}")
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", probe, str(wheel)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ValueError(f"isolated schema import from {wheel} failed: {detail}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify schema package data, authenticated loading, canonical digests, "
            "and editor export from one already-built experiment-queue wheel."
        )
    )
    parser.add_argument(
        "wheel",
        type=Path,
        help="absolute path to the .whl artifact to verify; the wheel is not modified",
    )
    return parser


def main() -> int:
    """Run the wheel verifier as a development/release command."""

    arguments = _parser().parse_args()
    try:
        verify_wheel(arguments.wheel)
    except ValueError as exc:
        print(f"wheel verification failed: {exc}", file=sys.stderr)
        return 1
    print(f"verified wheel schema resources: {arguments.wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
