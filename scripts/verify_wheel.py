"""Verify that a built wheel exposes the complete supported runtime surface."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import zipfile


REQUIRED_RESOURCES = (
    "experiment_queue/__init__.py",
    "experiment_queue/__main__.py",
    "experiment_queue/admission.py",
    "experiment_queue/attempt_runtime.py",
    "experiment_queue/authoring.py",
    "experiment_queue/cli_v5.py",
    "experiment_queue/config.py",
    "experiment_queue/continuation_v5.py",
    "experiment_queue/cooperative_yield.py",
    "experiment_queue/database_v5.py",
    "experiment_queue/execution.py",
    "experiment_queue/executor.py",
    "experiment_queue/extensions.py",
    "experiment_queue/git_resolver.py",
    "experiment_queue/host_locks.py",
    "experiment_queue/identity.py",
    "experiment_queue/legacy.py",
    "experiment_queue/legacy_continuation_v0.py",
    "experiment_queue/legacy_state.py",
    "experiment_queue/migrate_v5.py",
    "experiment_queue/migration_receipt.py",
    "experiment_queue/operator_cli.py",
    "experiment_queue/operator_services.py",
    "experiment_queue/path_security.py",
    "experiment_queue/project_lifecycle.py",
    "experiment_queue/project_worktrees.py",
    "experiment_queue/protocols.py",
    "experiment_queue/queue.py",
    "experiment_queue/queue_export.py",
    "experiment_queue/reservation_v5.py",
    "experiment_queue/runner.py",
    "experiment_queue/scheduler_service_v5.py",
    "experiment_queue/scheduler_v5.py",
    "experiment_queue/schema_registry.py",
    "experiment_queue/serialization.py",
    "experiment_queue/v5_operator_repository.py",
    "experiment_queue/v5_repository.py",
    "experiment_queue/web.py",
    "experiment_queue/web_v5.py",
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
    metadata_members = sorted(
        name for name in members if name.endswith(".dist-info/METADATA")
    )
    if len(metadata_members) != 1:
        raise ValueError(
            f"wheel {wheel} must contain exactly one dist-info METADATA file, "
            f"got {metadata_members}"
        )
    dist_info = metadata_members[0].removesuffix("METADATA")
    license_member = f"{dist_info}licenses/LICENSE"
    if license_member not in members:
        raise ValueError(
            f"wheel {wheel} is missing required PEP 639 license file "
            f"{license_member}"
        )

    probe = """
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import importlib
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
runtime_modules = (
    "__main__",
    "admission",
    "attempt_runtime",
    "authoring",
    "cli_v5",
    "config",
    "continuation_v5",
    "cooperative_yield",
    "database_v5",
    "execution",
    "executor",
    "extensions",
    "git_resolver",
    "host_locks",
    "identity",
    "legacy",
    "legacy_continuation_v0",
    "legacy_state",
    "migrate_v5",
    "migration_receipt",
    "operator_cli",
    "operator_services",
    "path_security",
    "project_lifecycle",
    "project_worktrees",
    "protocols",
    "queue",
    "queue_export",
    "reservation_v5",
    "runner",
    "scheduler_service_v5",
    "scheduler_v5",
    "schema_registry",
    "serialization",
    "v5_operator_repository",
    "v5_repository",
    "web",
    "web_v5",
)
modules = [
    importlib.import_module(f"experiment_queue.{name}") for name in runtime_modules
]
for module in modules:
    if not str(module.__file__).startswith(str(wheel)):
        raise SystemExit(f"runtime module import did not come from wheel: {module.__file__}")
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
metadata = wheel_distributions[0].metadata
if metadata.get("License-Expression") != "MIT":
    raise SystemExit(
        "wheel metadata must declare the exact SPDX License-Expression MIT"
    )
if metadata.get_all("License-File") != ["LICENSE"]:
    raise SystemExit(
        "wheel metadata must declare exactly one License-File named LICENSE"
    )
console_entry_points = {
    point.name: point
    for point in wheel_distributions[0].entry_points
    if point.group == "console_scripts"
}
console_scripts = {
    name: point.value for name, point in console_entry_points.items()
}
expected_entry_points = {
    "experiment-queue": "experiment_queue.cli_v5:main",
    "experiment-queue-web": "experiment_queue.web_v5:main",
    "experiment-queue-migrate-v5": "experiment_queue.migrate_v5:main",
    "experiment-queue-legacy-v4": "experiment_queue.queue:main",
    "experiment-queue-web-legacy-v4": "experiment_queue.web:main",
    "run-experiment": "experiment_queue.runner:main",
}
if console_scripts != expected_entry_points:
    raise SystemExit(
        f"console entry points differ: expected {expected_entry_points}, got {console_scripts}"
    )
for name in sorted(expected_entry_points):
    output = StringIO()
    original_argv = sys.argv
    sys.argv = [name, "--help"]
    try:
        with redirect_stdout(output), redirect_stderr(output):
            try:
                result = console_entry_points[name].load()()
            except SystemExit as exc:
                if exc.code in (None, 0):
                    status = 0
                elif type(exc.code) is int:
                    status = exc.code
                else:
                    status = 1
            else:
                status = 0 if result is None else result
    finally:
        sys.argv = original_argv
    if status != 0:
        raise SystemExit(
            f"console entry point {name!r} --help returned status {status}: "
            f"{output.getvalue().strip()}"
        )
    if "usage:" not in output.getvalue().lower():
        raise SystemExit(
            f"console entry point {name!r} --help produced no argparse usage text"
        )
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
        raise ValueError(f"isolated wheel runtime probe for {wheel} failed: {detail}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the complete runtime module surface, console scripts and their "
            "--help behavior, compiler "
            "metadata, schema package data, authenticated loading, canonical digests, "
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
    print(
        "verified wheel runtime API, entry-point help, and schema resources: "
        f"{arguments.wheel}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
