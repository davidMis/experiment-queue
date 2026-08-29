"""Generate exact local Enrollment/v1 evidence from operator-supplied host paths."""

from __future__ import annotations

import argparse
from pathlib import Path

from experiment_queue.authoring import Project
from experiment_queue.project_lifecycle import Enrollment, EnvironmentBinding, MountBinding


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a Flowers compatibility Enrollment for existing local paths."
    )
    parser.add_argument(
        "--checkout",
        required=True,
        type=Path,
        help="Existing absolute Flowers fixture checkout containing Project.yaml.",
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        type=Path,
        help="Existing absolute queue state directory used for overlap validation.",
    )
    parser.add_argument(
        "--datasets",
        required=True,
        type=Path,
        help="Existing absolute read-only dataset directory for the datasets mount.",
    )
    parser.add_argument(
        "--outputs",
        required=True,
        type=Path,
        help="Existing absolute writable output directory for the outputs mount.",
    )
    parser.add_argument(
        "--scratch",
        required=True,
        type=Path,
        help="Existing absolute writable scratch directory for checkpoint artifacts.",
    )
    parser.add_argument(
        "--environment-bin",
        required=True,
        type=Path,
        help="Existing absolute executable-search directory for the scientific environment.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Enrollment JSON file to create or replace; parent directory must exist.",
    )
    arguments = parser.parse_args()
    checkout = arguments.checkout.resolve(strict=True)
    project = Project.from_yaml(
        (checkout / "Project.yaml").read_bytes(), source_name="Project.yaml"
    )
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=checkout,
        project_manifest_path="Project.yaml",
        mounts=(
            MountBinding.create(name="datasets", path=arguments.datasets.resolve(strict=True), access="readOnly"),
            MountBinding.create(name="outputs", path=arguments.outputs.resolve(strict=True), access="readWrite"),
            MountBinding.create(name="scratch", path=arguments.scratch.resolve(strict=True), access="readWrite"),
        ),
        environments=(
            EnvironmentBinding.create(
                name="scientific",
                executable_search_directories=(arguments.environment_bin.resolve(strict=True),),
                inherit_variables=("LANG",),
            ),
        ),
        state_directory=arguments.state_dir.resolve(strict=True),
    )
    arguments.output.write_bytes(enrollment.canonical_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
