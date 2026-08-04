# This test module verifies the lightweight experiment runner's core behavior.
# It uses temporary directories and subprocesses so tests exercise real manifest,
# log, config-copy, and rsync-command generation without requiring a GPU.

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from helmholtz_shared.experiment_runner import (  # noqa: E402
    DEFAULT_USE_PTY,
    ExperimentError,
    ExperimentRequest,
    build_arg_parser,
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
            self.assertEqual(manifest["command"]["pty"], DEFAULT_USE_PTY)
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

    @unittest.skipIf(os.name == "nt", "PTY support is POSIX-only")
    def test_run_experiment_pty_gives_child_tty_and_records_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            request = ExperimentRequest(
                name="PTY Progress",
                command=[
                    sys.executable,
                    "-c",
                    (
                        "import sys; "
                        "print('stdout_isatty=' + str(sys.stdout.isatty())); "
                        "print('stderr_isatty=' + str(sys.stderr.isatty()), file=sys.stderr)"
                    ),
                ],
                output_root=root / "outputs",
                cwd=root,
                use_pty=True,
            )

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                result = run_experiment(request)

            self.assertEqual(result.return_code, 0)
            stdout = (result.run_dir / "stdout.log").read_text(encoding="utf-8")
            stderr = (result.run_dir / "stderr.log").read_text(encoding="utf-8")
            self.assertIn("stdout_isatty=True", stdout)
            self.assertIn("stderr_isatty=True", stdout)
            self.assertIn("PTY mode merges child stdout and stderr", stderr)

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["command"]["pty"])

    def test_no_pty_keeps_stdout_and_stderr_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            request = ExperimentRequest(
                name="No PTY",
                command=[
                    sys.executable,
                    "-c",
                    "import sys; print('out'); print('err', file=sys.stderr)",
                ],
                output_root=root / "outputs",
                cwd=root,
                use_pty=False,
            )

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                result = run_experiment(request)

            self.assertEqual(result.return_code, 0)
            stdout = (result.run_dir / "stdout.log").read_text(encoding="utf-8")
            stderr = (result.run_dir / "stderr.log").read_text(encoding="utf-8")
            self.assertIn("out", stdout)
            self.assertNotIn("err", stdout)
            self.assertIn("err", stderr)

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertFalse(manifest["command"]["pty"])

    def test_cli_pty_defaults_can_be_disabled(self) -> None:
        parser = build_arg_parser()

        default_args = parser.parse_args(["--", sys.executable, "-c", "print('x')"])
        self.assertEqual(default_args.use_pty, DEFAULT_USE_PTY)

        no_pty_args = parser.parse_args(
            ["--no-pty", "--", sys.executable, "-c", "print('x')"]
        )
        self.assertFalse(no_pty_args.use_pty)

    @unittest.skipIf(os.name != "posix", "signal handling is POSIX-specific")
    def test_sigterm_records_interrupted_manifest(self) -> None:
        for mode in ("--no-pty", "--pty"):
            for signum in (signal.SIGINT, signal.SIGTERM):
                with self.subTest(mode=mode, signal=signum), tempfile.TemporaryDirectory() as temp_dir:
                    self._assert_signal_records_interrupted(mode, signum, Path(temp_dir))

    def _assert_signal_records_interrupted(
        self, mode: str, signum: int, root: Path
    ) -> None:
        output_root = root / "outputs"
        runner = REPO_ROOT / "scripts" / "run_experiment.py"
        process = subprocess.Popen(
            [
                sys.executable,
                str(runner),
                "--output-root",
                str(output_root),
                mode,
                "--",
                sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            manifests = list(output_root.glob("*/manifest.json"))
            if manifests:
                break
            time.sleep(0.05)
        else:
            process.kill()
            self.fail("runner did not create a manifest before signal timeout")

        process.send_signal(signum)
        stdout, stderr = process.communicate(timeout=15)

        self.assertEqual(process.returncode, 130, (stdout, stderr))
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "interrupted")
        self.assertEqual(manifest["run"]["return_code"], 130)


if __name__ == "__main__":
    unittest.main()
