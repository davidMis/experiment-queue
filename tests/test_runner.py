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
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]

from experiment_queue.runner import (  # noqa: E402
    DEFAULT_USE_PTY,
    RUNNER_RECEIPT_ENV,
    YIELD_RECEIPT_ENV,
    ExperimentError,
    ExperimentRequest,
    _create_run_dir,
    _load_continuation_manifest,
    build_manifest,
    build_rsync_pull_command,
    build_arg_parser,
    collect_git_context,
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

    def test_runner_publishes_complete_running_and_terminal_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            receipt_path = root / "queue-state" / "runner-receipt.json"
            receipt_path.parent.mkdir()
            captured_running = root / "captured-running.json"
            request = ExperimentRequest(
                name="Structured Receipt",
                command=[
                    sys.executable,
                    "-c",
                    (
                        "import os, pathlib; "
                        "source = pathlib.Path(os.environ["
                        "'EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH']); "
                        "pathlib.Path(__import__('sys').argv[1]).write_bytes(source.read_bytes())"
                    ),
                    str(captured_running),
                ],
                output_root=root / "outputs",
                remote="mutton2",
                cwd=root,
                use_pty=False,
            )

            with mock.patch.dict(
                os.environ,
                {
                    RUNNER_RECEIPT_ENV: str(receipt_path),
                    "EXPERIMENT_QUEUE_ITEM_ID": "41",
                },
            ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                result = run_experiment(request)

            running = json.loads(captured_running.read_text(encoding="utf-8"))
            terminal = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(
                (running["apiVersion"], running["kind"]),
                ("experiment-queue/v1", "RunnerReceipt"),
            )
            self.assertEqual(running["status"], "running")
            self.assertIsNone(running["return_code"])
            self.assertEqual(running["queue_item_id"], 41)
            self.assertEqual(running["segment"], 1)
            self.assertEqual(terminal["status"], "succeeded")
            self.assertEqual(terminal["return_code"], 0)
            self.assertEqual(terminal["run_id"], result.run_id)
            self.assertEqual(Path(terminal["run_directory"]), result.run_dir.resolve())
            self.assertEqual(Path(terminal["manifest"]), result.manifest_path.resolve())
            self.assertEqual(
                Path(terminal["logs"]["stdout"]),
                (result.run_dir / "stdout.log").resolve(),
            )
            self.assertEqual(terminal["sync"]["type"], "rsync-pull")
            self.assertIn("mutton2:", terminal["sync"]["command"])
            self.assertEqual(result.receipt_path, receipt_path)
            self.assertEqual(list(receipt_path.parent.glob(".runner-receipt.json.*.tmp")), [])

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                (manifest["apiVersion"], manifest["kind"]),
                ("experiment-queue/v1", "RunnerManifest"),
            )
            self.assertEqual(manifest["schema_version"], 1)

    @unittest.skipIf(os.name != "posix", "signal exit codes are POSIX-specific")
    def test_cli_receipt_matches_signal_terminated_child_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            receipt_path = root / "runner-receipt.json"
            environment = os.environ.copy()
            environment[RUNNER_RECEIPT_ENV] = str(receipt_path)
            environment.pop("EXPERIMENT_QUEUE_ITEM_ID", None)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "run_experiment.py"),
                    "--output-root",
                    str(root / "outputs"),
                    "--no-pty",
                    "--",
                    sys.executable,
                    "-c",
                    "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
                ],
                cwd=root,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
            )

            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            manifest = json.loads(Path(receipt["manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(completed.returncode, 128 + signal.SIGTERM)
            self.assertEqual(receipt["return_code"], completed.returncode)
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual(manifest["run"]["return_code"], -signal.SIGTERM)

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

    def test_continuation_accepts_legacy_v1_manifest_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            run_dir = root / "legacy-run"
            run_dir.mkdir()
            request = ExperimentRequest(
                name="Legacy Continuation",
                command=[sys.executable, "-c", "print('continued')"],
                cwd=root,
                use_pty=False,
            )
            git_context = collect_git_context(root)
            manifest = build_manifest(
                request=request,
                cwd=root,
                run_id=run_dir.name,
                run_dir=run_dir,
                git_context=git_context,
                copied_configs=[],
                rsync_pull_command=None,
            )
            manifest.pop("apiVersion")
            manifest.pop("kind")
            manifest["run"]["status"] = "yielded"
            write_manifest(run_dir / "manifest.json", manifest)

            with mock.patch.dict(
                os.environ,
                {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(run_dir)},
            ):
                loaded = _load_continuation_manifest(
                    request,
                    cwd=root,
                    git_context=git_context,
                )

            self.assertIsNotNone(loaded)
            self.assertEqual(loaded[0], run_dir.name)

    def test_continuation_rejects_undeclared_manifest_major(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            run_dir = root / "future-run"
            run_dir.mkdir()
            request = ExperimentRequest(
                name="Future Continuation",
                command=[sys.executable, "-c", "print('must not run')"],
                cwd=root,
                use_pty=False,
            )
            git_context = collect_git_context(root)
            manifest = build_manifest(
                request=request,
                cwd=root,
                run_id=run_dir.name,
                run_dir=run_dir,
                git_context=git_context,
                copied_configs=[],
                rsync_pull_command=None,
            )
            manifest["apiVersion"] = "experiment-queue/v999"
            manifest["run"]["status"] = "yielded"
            write_manifest(run_dir / "manifest.json", manifest)

            with mock.patch.dict(
                os.environ,
                {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(run_dir)},
            ), self.assertRaisesRegex(
                ExperimentError,
                "invalid protocol identity",
            ):
                _load_continuation_manifest(
                    request,
                    cwd=root,
                    git_context=git_context,
                )

    def test_continuation_manifest_identity_is_unambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            request = ExperimentRequest(
                name="Strict Continuation",
                command=[sys.executable, "-c", "print('must not run')"],
                cwd=root,
                use_pty=False,
            )
            git_context = collect_git_context(root)
            for typed in (True, False):
                with self.subTest(typed=typed):
                    run_dir = root / ("typed-run" if typed else "legacy-run")
                    run_dir.mkdir()
                    manifest = build_manifest(
                        request=request,
                        cwd=root,
                        run_id=run_dir.name,
                        run_dir=run_dir,
                        git_context=git_context,
                        copied_configs=[],
                        rsync_pull_command=None,
                    )
                    if not typed:
                        manifest.pop("apiVersion")
                        manifest.pop("kind")
                    manifest["schema_version"] = True
                    manifest["run"]["status"] = "yielded"
                    write_manifest(run_dir / "manifest.json", manifest)

                    with mock.patch.dict(
                        os.environ,
                        {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(run_dir)},
                    ), self.assertRaisesRegex(ExperimentError, "schema_version"):
                        _load_continuation_manifest(
                            request,
                            cwd=root,
                            git_context=git_context,
                        )

            duplicate_dir = root / "duplicate-run"
            duplicate_dir.mkdir()
            (duplicate_dir / "manifest.json").write_text(
                '{"apiVersion":"experiment-queue/v1",'
                '"kind":"RunnerManifest","kind":"RunnerManifest"}',
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(duplicate_dir)},
            ), self.assertRaisesRegex(ExperimentError, "repeats JSON key 'kind'"):
                _load_continuation_manifest(
                    request,
                    cwd=root,
                    git_context=git_context,
                )

            (duplicate_dir / "manifest.json").write_text(
                "[" * 100_000 + "0" + "]" * 100_000,
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(duplicate_dir)},
            ), self.assertRaisesRegex(ExperimentError, "could not read continuation"):
                _load_continuation_manifest(
                    request,
                    cwd=root,
                    git_context=git_context,
                )

            (duplicate_dir / "manifest.json").write_text(
                '{"schema_version":' + ("9" * 5000) + "}",
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR": str(duplicate_dir)},
            ), self.assertRaisesRegex(ExperimentError, "could not read continuation"):
                _load_continuation_manifest(
                    request,
                    cwd=root,
                    git_context=git_context,
                )

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

    @unittest.skipIf(os.name != "posix", "process-group signals are POSIX-specific")
    def test_queue_group_signal_is_not_duplicated_by_runner_cleanup(self) -> None:
        for mode in ("--no-pty", "--pty"):
            for signum in (signal.SIGINT, signal.SIGTERM):
                with self.subTest(mode=mode, signal=signum), tempfile.TemporaryDirectory() as temp_dir:
                    self._assert_queue_group_signal_is_not_duplicated(
                        mode,
                        signum,
                        Path(temp_dir),
                    )

    @unittest.skipIf(os.name != "posix", "process-group signals are POSIX-specific")
    def test_queue_group_sigint_preserves_cooperative_yield_exit(self) -> None:
        for mode in ("--no-pty", "--pty"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp_dir:
                self._assert_queue_group_sigint_preserves_yield(
                    mode,
                    Path(temp_dir),
                )

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

    def _assert_queue_group_signal_is_not_duplicated(
        self,
        mode: str,
        signum: int,
        root: Path,
    ) -> None:
        output_root = root / "outputs"
        signal_log = root / "child-signals.jsonl"
        child_ready = root / "child-ready"
        runner_receipt = root / "queue-runner-receipt.json"
        runner = REPO_ROOT / "scripts" / "run_experiment.py"
        child_source = (
            "import json, os, signal, time\n"
            f"log = {str(signal_log)!r}\n"
            f"ready = {str(child_ready)!r}\n"
            "def handle(signum, _frame):\n"
            "    fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)\n"
            "    try:\n"
            "        os.write(fd, (json.dumps({'signal': signum}) + '\\n').encode())\n"
            "    finally:\n"
            "        os.close(fd)\n"
            "    signal.signal(signum, signal.SIG_DFL)\n"
            "    os.kill(os.getpid(), signum)\n"
            "signal.signal(signal.SIGINT, handle)\n"
            "signal.signal(signal.SIGTERM, handle)\n"
            "open(ready, 'x').close()\n"
            "while True:\n"
            "    time.sleep(60)\n"
        )
        environment = os.environ.copy()
        environment["EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"] = str(runner_receipt)
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
                child_source,
            ],
            cwd=root,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            manifests = list(output_root.glob("*/manifest.json"))
            if manifests and child_ready.exists():
                break
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                self.fail(f"runner exited before child readiness: {stdout!r} {stderr!r}")
            time.sleep(0.05)
        else:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            self.fail("queue runner child did not become signal-ready")

        os.killpg(process.pid, signum)
        stdout, stderr = process.communicate(timeout=15)

        self.assertEqual(process.returncode, 128 + signum, (stdout, stderr))
        observed = [
            json.loads(line)["signal"]
            for line in signal_log.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(observed, [signum])
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "failed")
        self.assertEqual(manifest["run"]["return_code"], -signum)

    def _assert_queue_group_sigint_preserves_yield(
        self,
        mode: str,
        root: Path,
    ) -> None:
        output_root = root / "outputs"
        signal_log = root / "yield-child-signals.jsonl"
        child_ready = root / "yield-child-ready"
        yield_receipt = root / "yield-receipt.json"
        runner_receipt = root / "queue-runner-receipt.json"
        runner = REPO_ROOT / "scripts" / "run_experiment.py"
        child_source = (
            "import json, os, signal, time\n"
            f"log = {str(signal_log)!r}\n"
            f"ready = {str(child_ready)!r}\n"
            f"receipt = {str(yield_receipt)!r}\n"
            "def handle(signum, _frame):\n"
            "    fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)\n"
            "    try:\n"
            "        os.write(fd, (json.dumps({'signal': signum}) + '\\n').encode())\n"
            "    finally:\n"
            "        os.close(fd)\n"
            "    with open(receipt, 'x', encoding='utf-8') as stream:\n"
            "        json.dump({'status': 'ready'}, stream)\n"
            "    raise SystemExit(75)\n"
            "signal.signal(signal.SIGINT, handle)\n"
            "open(ready, 'x').close()\n"
            "while True:\n"
            "    time.sleep(60)\n"
        )
        environment = os.environ.copy()
        environment[RUNNER_RECEIPT_ENV] = str(runner_receipt)
        environment[YIELD_RECEIPT_ENV] = str(yield_receipt)
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
                child_source,
            ],
            cwd=root,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            manifests = list(output_root.glob("*/manifest.json"))
            if manifests and child_ready.exists():
                break
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                self.fail(f"runner exited before yield readiness: {stdout!r} {stderr!r}")
            time.sleep(0.05)
        else:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            self.fail("queue runner yield child did not become signal-ready")

        os.killpg(process.pid, signal.SIGINT)
        stdout, stderr = process.communicate(timeout=15)

        self.assertEqual(process.returncode, 75, (stdout, stderr))
        observed = [
            json.loads(line)["signal"]
            for line in signal_log.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(observed, [signal.SIGINT])
        receipt = json.loads(runner_receipt.read_text(encoding="utf-8"))
        self.assertEqual(receipt["status"], "yielded")
        self.assertEqual(receipt["return_code"], 75)
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "yielded")
        self.assertEqual(manifest["run"]["return_code"], 75)


if __name__ == "__main__":
    unittest.main()
