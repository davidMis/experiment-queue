"""Verify that a built wheel exposes the authoring API and schema resources."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import zipfile


REQUIRED_RESOURCES = (
    "experiment_queue/admission.py",
    "experiment_queue/authoring.py",
    "experiment_queue/extensions.py",
    "experiment_queue/schema_resources/__init__.py",
    "experiment_queue/schema_resources/project-v1.schema.json",
    "experiment_queue/schema_resources/experiment-card-v1.schema.json",
)


def verify_wheel(wheel: Path) -> None:
    """Fail unless ``wheel`` contains the API and authenticates both schemas."""

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
        raise ValueError(f"wheel {wheel} is missing required package files: {missing}")

    probe = """
import json
from importlib.metadata import distributions
from pathlib import Path
import sys

wheel = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(wheel))
import experiment_queue
from experiment_queue import admission, authoring, extensions
from experiment_queue.protocols import EXPERIMENT_CARD_V1, PROJECT_V1
from experiment_queue.schema_registry import (
    editor_schema_bytes,
    load_bundled_schema,
    schema_canonical_bytes,
    schema_sha256,
)
if not str(experiment_queue.__file__).startswith(str(wheel)):
    raise SystemExit(f"import did not come from wheel: {experiment_queue.__file__}")
for module in (admission, authoring, extensions):
    if not str(module.__file__).startswith(str(wheel)):
        raise SystemExit(f"authoring module import did not come from wheel: {module.__file__}")
wheel_distributions = [
    distribution
    for distribution in distributions(path=[str(wheel)])
    if distribution.metadata["Name"] == "experiment-queue"
]
if len(wheel_distributions) != 1:
    raise SystemExit(
        f"expected one experiment-queue distribution in wheel, got {len(wheel_distributions)}"
    )
wheel_version = wheel_distributions[0].version
if experiment_queue.__version__ != wheel_version:
    raise SystemExit(
        f"package version {experiment_queue.__version__!r} differs from wheel metadata "
        f"{wheel_version!r}"
    )
if admission._package_version() != wheel_version:
    raise SystemExit("admission compiler provenance differs from wheel metadata")
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
            "Verify typed authoring modules, compiler metadata, schema package data, "
            "authenticated loading, canonical digests, and editor export from one "
            "already-built experiment-queue wheel."
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
    print(f"verified wheel authoring API and schema resources: {arguments.wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
