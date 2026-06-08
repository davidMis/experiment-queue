# This test module verifies the lightweight experiment runner's core behavior.
# It uses temporary directories and subprocesses so tests exercise real manifest,
# log, config-copy, and rsync-command generation without requiring a GPU.

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from helmholtz_shared.experiment_runner import (  # noqa: E402
    ExperimentError,
    ExperimentRequest,
    run_experiment,
    slugify,
)


class ExperimentRunnerTests(unittest.TestCase):
    """Tests for the reusable experiment runner module."""

    def test_slugify_keeps_run_names_filesystem_friendly(self) -> None:
        self.assertEqual(slugify("Baseline GPU Run"), "baseline-gpu-run")
        self.assertEqual(slugify("  weird/value!!  "), "weird-value")
        self.assertEqual(slugify("!!!"), "run")

    def test_run_experiment_creates_manifest_logs_configs_and_rsync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "config.yaml"
            config_path.write_text("learning_rate: 0.001\n", encoding="utf-8")

            request = ExperimentRequest(
                name="Smoke Run",
                command=[
                    sys.executable,
                    "-c",
                    (
                        "import os; "
                        "print('run=' + os.environ['EXPERIMENT_RUN_ID']); "
                        "print('out=' + os.environ['EXPERIMENT_OUTPUT_DIR'])"
                    ),
                ],
                output_root=root / "outputs",
                config_paths=[config_path],
                notes="unit test smoke run",
                remote="mutton2",
                cwd=root,
            )

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                result = run_experiment(request)

            self.assertEqual(result.return_code, 0)
            self.assertIn("smoke-run", result.run_id)
            self.assertTrue(result.manifest_path.exists())
            self.assertTrue((result.run_dir / "stdout.log").exists())
            self.assertTrue((result.run_dir / "stderr.log").exists())
            self.assertEqual(
                (result.run_dir / "configs" / "config.yaml").read_text(encoding="utf-8"),
                "learning_rate: 0.001\n",
            )

            stdout = (result.run_dir / "stdout.log").read_text(encoding="utf-8")
            self.assertIn(f"run={result.run_id}", stdout)
            self.assertIn(f"out={result.run_dir}", stdout)

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["run"]["status"], "succeeded")
            self.assertEqual(manifest["run"]["return_code"], 0)
            self.assertEqual(manifest["run"]["notes"], "unit test smoke run")
            self.assertEqual(manifest["configs"][0]["copy"], "configs/config.yaml")
            self.assertFalse(manifest["git"]["available"])
            self.assertIn("mutton2:", manifest["sync"]["rsync_pull_command"])
            self.assertEqual(result.rsync_pull_command, manifest["sync"]["rsync_pull_command"])

    def test_missing_config_path_raises_helpful_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            request = ExperimentRequest(
                name="Missing Config",
                command=[sys.executable, "-c", "print('should not run')"],
                output_root=root / "outputs",
                config_paths=[root / "missing.yaml"],
                cwd=root,
            )

            with self.assertRaisesRegex(ExperimentError, "config path does not exist"):
                run_experiment(request)

            self.assertFalse((root / "outputs").exists())

    def test_require_clean_requires_git_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            request = ExperimentRequest(
                name="Exact Run",
                command=[sys.executable, "-c", "print('should not run')"],
                output_root=root / "outputs",
                require_clean=True,
                cwd=root,
            )

            with self.assertRaisesRegex(ExperimentError, "requires a git worktree"):
                run_experiment(request)

            self.assertFalse((root / "outputs").exists())


if __name__ == "__main__":
    unittest.main()
