"""Exercise runner manifests, subprocess logs, configs, and sync commands."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import contextlib
import io
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

from helmholtz_shared.experiment_runner import (  # noqa: E402
    DEFAULT_USE_PTY,
    ExperimentError,
    ExperimentRequest,
    _create_run_dir,
    build_rsync_pull_command,
    build_arg_parser,
    run_experiment,
    slugify,
    write_manifest,
)


class ExperimentRunnerTests(unittest.TestCase):
    """Tests for the reusable experiment runner module."""

    def test_slugify_keeps_run_names_filesystem_friendly(self) -> None:
        self.assertEqual(slugify("Baseline GPU Run"), "baseline-gpu-run")
        self.assertEqual(slugify("  weird/value!!  "), "weird-value")
        self.assertEqual(slugify("!!!"), "run")

    def test_write_manifest_atomically_replaces_existing_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text('{"status": "old"}\n', encoding="utf-8")
            path.chmod(0o640)

            write_manifest(path, {"status": "new", "attempt": 2})

            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                {"attempt": 2, "status": "new"},
            )
            self.assertEqual(path.stat().st_mode & 0o777, 0o640)
            self.assertEqual(list(path.parent.glob(".manifest.json.*.tmp")), [])

    def test_new_manifest_keeps_private_temporary_file_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            previous_umask = os.umask(0o077)
            try:
                write_manifest(path, {"status": "new"})
            finally:
                os.umask(previous_umask)

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_write_manifest_supports_concurrent_same_process_writers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(
                    executor.map(
                        lambda index: write_manifest(path, {"writer": index}),
                        range(8),
                    )
                )

            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn(payload["writer"], range(8))
            self.assertEqual(list(path.parent.glob(".manifest.json.*.tmp")), [])

    def test_rsync_command_quotes_remote_and_local_operands_as_whole_arguments(self) -> None:
        command = build_rsync_pull_command(
            remote="host;echo unsafe",
            remote_run_dir=Path("/remote/run with spaces"),
            local_output_root=Path("local output"),
            run_id="run-id",
        )

        self.assertIsNotNone(command)
        arguments = shlex.split(str(command))
        self.assertEqual(arguments[-3], "--")
        self.assertEqual(arguments[-2], "host;echo unsafe:/remote/run with spaces/")
        self.assertEqual(arguments[-1], "local output/run-id/")

    def test_run_directory_claim_skips_an_already_claimed_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir)

            first_id, first_path = _create_run_dir(
                output_root=output_root,
                timestamp="20260811_120000",
                name="same run",
                short_commit="abc12345",
            )
            second_id, second_path = _create_run_dir(
                output_root=output_root,
                timestamp="20260811_120000",
                name="same run",
                short_commit="abc12345",
            )

            self.assertEqual(first_id, "20260811_120000_same-run_abc12345")
            self.assertEqual(second_id, "20260811_120000_same-run_abc12345_002")
            self.assertTrue(first_path.is_dir())
            self.assertTrue(second_path.is_dir())

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

    def test_config_directory_cannot_contain_the_output_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            request = ExperimentRequest(
                name="Recursive Config",
                command=[sys.executable, "-c", "print('should not run')"],
                output_root=root / "outputs",
                config_paths=[root],
                cwd=root,
            )

            with self.assertRaisesRegex(ExperimentError, "recursively copying"):
                run_experiment(request)

            self.assertFalse((root / "outputs").exists())

    @unittest.skipIf(os.name == "nt", "symlink behavior is POSIX-specific")
    def test_config_directory_copies_symlink_without_following_output_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_dir = root / "config"
            config_dir.mkdir()
            (config_dir / "output-link").symlink_to(
                root / "outputs",
                target_is_directory=True,
            )
            request = ExperimentRequest(
                name="Symlink Config",
                command=[sys.executable, "-c", "print('done')"],
                output_root=root / "outputs",
                config_paths=[config_dir],
                cwd=root,
            )

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                result = run_experiment(request)

            copied_link = result.run_dir / "configs" / "config" / "output-link"
            self.assertTrue(copied_link.is_symlink())
            self.assertEqual(copied_link.readlink(), root / "outputs")

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

    def test_yielded_runner_continues_in_same_directory_and_appends_segment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            command = [
                sys.executable,
                "-c",
                (
                    "import os; "
                    "print('segment=' + os.environ.get('EXPERIMENT_QUEUE_SEGMENT', '1')); "
                    "raise SystemExit(0 if os.environ.get("
                    "'EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR') else 75)"
                ),
            ]
            request = ExperimentRequest(
                name="Yield Resume",
                command=command,
                output_root=root / "outputs",
                cwd=root,
                use_pty=False,
            )
            previous_receipt = os.environ.get("EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH")
            previous_run = os.environ.get("EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR")
            try:
                os.environ["EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"] = str(
                    root / "yield-receipt.json"
                )
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                    io.StringIO()
                ):
                    first = run_experiment(request)
                self.assertEqual(first.return_code, 75)
                os.environ["EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR"] = str(first.run_dir)
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                    io.StringIO()
                ):
                    second = run_experiment(request)
            finally:
                if previous_receipt is None:
                    os.environ.pop("EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH", None)
                else:
                    os.environ["EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"] = previous_receipt
                if previous_run is None:
                    os.environ.pop("EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR", None)
                else:
                    os.environ["EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR"] = previous_run

            self.assertEqual(second.return_code, 0)
            self.assertEqual(second.run_dir.resolve(), first.run_dir.resolve())
            manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["run"]["status"], "succeeded")
            self.assertEqual([row["status"] for row in manifest["segments"]], ["yielded", "succeeded"])
            self.assertIn("continuation started", (first.run_dir / "stdout.log").read_text())

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
