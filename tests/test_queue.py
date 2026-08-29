"""Verify explicit admission, GPU controls, dispatch, and queue termination."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import experiment_queue.queue as queue_module


REPO_ROOT = Path(__file__).resolve().parents[1]


def time_to_datetime(value: str, *, seconds: int = 0):
    return datetime.fromisoformat(value) + timedelta(seconds=seconds)

from experiment_queue.queue import (  # noqa: E402
    GpuSnapshot,
    QueueError,
    QueueStore,
    Scheduler,
    add_experiment,
    format_pull_commands,
    format_status,
    expire_reservations,
    list_reservations,
    query_gpus,
    read_card_command,
    release_gpu_reservation,
    remove_item,
    request_gpu_reservation,
    request_preemption,
    request_termination,
    set_priority,
    update_gpu_allowlist,
)


class TemporaryQueueRepository:
    """Small committed repository whose cards invoke a controllable fake runner."""

    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "docs" / "experiments").mkdir(parents=True)
        (self.root / "scripts").mkdir()
        (self.root / ".gitignore").write_text("/outputs/\n", encoding="utf-8")
        (self.root / "STATUS.md").write_text(
            "# Status\n\nTST-999 is prepared_locally but must not enter the queue.\n",
            encoding="utf-8",
        )
        (self.root / "scripts" / "run_experiment.py").write_text(
            "# Fake runner used only by queue unit tests.\n"
            "import argparse\n"
            "import json\n"
            "import os\n"
            "import subprocess\n"
            "import time\n"
            "from pathlib import Path\n"
            "parser = argparse.ArgumentParser()\n"
            "parser.add_argument('--name')\n"
            "parser.add_argument('--require-clean', action='store_true')\n"
            "parser.add_argument('--remote')\n"
            "parser.add_argument('--sleep', type=float, default=0.0)\n"
            "parser.add_argument('--exit-code', type=int, default=0)\n"
            "parser.add_argument('--yield-aware', action='store_true')\n"
            "parser.add_argument('--generic-progress', action='store_true')\n"
            "args = parser.parse_args()\n"
            "marker = Path(os.environ['QUEUE_TEST_MARKER'])\n"
            "head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()\n"
            "marker.write_text(json.dumps({'gpu': os.environ.get('CUDA_VISIBLE_DEVICES'), 'cwd': str(Path.cwd()), 'head': head, 'worktree': os.environ.get('EXPERIMENT_QUEUE_WORKTREE'), 'continuation_checkpoint': os.environ.get('EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT')}))\n"
            "if args.yield_aware and os.environ.get('EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT'):\n"
            "    print('run directory: ' + os.environ['EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR'])\n"
            "    print('manifest: ' + os.environ['EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR'] + '/manifest.json')\n"
            "    print('pull outputs with: rsync -av mutton2:fake-run local-output')\n"
            "    raise SystemExit(0)\n"
            "deadline = time.monotonic() + args.sleep\n"
            "while time.monotonic() < deadline:\n"
            "    request_value = os.environ.get('EXPERIMENT_QUEUE_YIELD_REQUEST_PATH')\n"
            "    if args.yield_aware and request_value and Path(request_value).is_file():\n"
            "        request = json.loads(Path(request_value).read_text())\n"
            "        run_dir = (Path.cwd() / 'outputs' / 'experiments' / 'fake-run').resolve()\n"
            "        checkpoint_dir = run_dir / 'training' / 'checkpoints'\n"
            "        checkpoint_dir.mkdir(parents=True, exist_ok=True)\n"
            "        step = 0 if args.generic_progress else 5\n"
            "        checkpoint = checkpoint_dir / f'preempt_step_{step:08d}.msgpack'\n"
            "        checkpoint.write_bytes(b'fake complete train state')\n"
            "        metadata = checkpoint.with_suffix('.json')\n"
            "        metadata.write_text(json.dumps({'step': step}))\n"
            "        import hashlib\n"
            "        receipt = {'schema_version': 1, 'status': 'ready', 'request_id': request['request_id'], 'queue_item_id': request['queue_item_id'], 'step': step, 'checkpoint': str(checkpoint), 'checkpoint_metadata': str(metadata), 'checkpoint_bytes': checkpoint.stat().st_size, 'checkpoint_sha256': hashlib.sha256(checkpoint.read_bytes()).hexdigest(), 'wandb': None if args.generic_progress else {'id': 'fake-wandb-id'}}\n"
            "        if args.generic_progress:\n"
            "            receipt['progress'] = {'unit': 'settled_rows', 'completed': 0, 'total': 12}\n"
            "        receipt_path = Path(os.environ['EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH'])\n"
            "        receipt_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "        receipt_path.write_text(json.dumps(receipt))\n"
            "        print('run directory: ' + str(run_dir))\n"
            "        print('manifest: ' + str(run_dir / 'manifest.json'))\n"
            "        print('pull outputs with: rsync -av mutton2:fake-run local-output')\n"
            "        raise SystemExit(75)\n"
            "    time.sleep(0.02)\n"
            "print('run directory: outputs/experiments/fake-run')\n"
            "print('manifest: outputs/experiments/fake-run/manifest.json')\n"
            "print('pull outputs with: rsync -av mutton2:fake-run local-output')\n"
            "raise SystemExit(args.exit_code)\n",
            encoding="utf-8",
        )
        self.add_card("TST-001", sleep=0.0)
        self.add_card("TST-002", sleep=0.0)
        self.add_card("TST-003", sleep=30.0)
        self.add_card("TST-004", sleep=0.0, exit_code=4)
        self.add_card("TST-005", sleep=0.0, exit_code=5)
        self.add_card("TST-007", sleep=30.0, yield_aware=True)
        self.add_card(
            "TST-009",
            sleep=30.0,
            yield_aware=True,
            generic_progress=True,
        )
        self.add_real_runner_card()
        self.add_real_runner_launch_failure_card()
        self._git("init", "-q")
        self._git("config", "user.email", "queue-test@example.invalid")
        self._git("config", "user.name", "Queue Test")
        self._git("add", ".")
        self._git("commit", "-qm", "queue fixture")
        self.state_dir = self.root / "outputs" / "experiment_queue"
        self.store = QueueStore(self.state_dir, self.root)

    def add_card(
        self,
        experiment_id: str,
        *,
        sleep: float,
        exit_code: int = 0,
        yield_aware: bool = False,
        generic_progress: bool = False,
    ) -> None:
        card = self.root / "docs" / "experiments" / f"{experiment_id}.md"
        card.write_text(
            f"# {experiment_id}: Queue Test\n\n"
            "## Exact Manual Command On Mutton2\n\n"
            "```bash\n"
            "(\n"
            "set -euo pipefail\n"
            "cd ~/3D_Helmholtz\n"
            f"python3 scripts/run_experiment.py --name {experiment_id.lower()} "
            f"--require-clean --remote mutton2 --sleep {sleep} --exit-code {exit_code} "
            f"{'--yield-aware' if yield_aware else ''} "
            f"{'--generic-progress' if generic_progress else ''}\n"
            ")\n"
            "```\n\n"
            "## Expected Artifacts\n\nTest-only marker.\n",
            encoding="utf-8",
        )

    def add_real_runner_card(self) -> None:
        experiment_id = "TST-006"
        card = self.root / "docs" / "experiments" / f"{experiment_id}.md"
        runner = REPO_ROOT / "scripts" / "run_experiment.py"
        local_output = self.root / "local-output"
        command = [
            sys.executable,
            str(runner),
            "--name",
            "tst-006-real-runner",
            "--output-root",
            "outputs/experiments",
            "--config",
            f"docs/experiments/{experiment_id}.md",
            "--require-clean",
            "--remote",
            "mutton2",
            "--local-output-root",
            str(local_output),
            "--no-pty",
            "--",
            sys.executable,
            "-c",
            "import os; print('gpu=' + os.environ['CUDA_VISIBLE_DEVICES'])",
        ]
        card.write_text(
            f"# {experiment_id}: Real Runner Integration\n\n"
            "## Exact Manual Command On Mutton2\n\n"
            "```bash\n"
            + " ".join(shlex.quote(part) for part in command)
            + "\n```\n",
            encoding="utf-8",
        )

    def add_real_runner_launch_failure_card(self) -> None:
        """Add a real runner whose requested child executable does not exist."""

        experiment_id = "TST-010"
        runner = REPO_ROOT / "scripts" / "run_experiment.py"
        command = [
            sys.executable,
            str(runner),
            "--name",
            "tst-010-launch-failure",
            "--output-root",
            "outputs/experiments",
            "--require-clean",
            "--remote",
            "mutton2",
            "--no-pty",
            "--",
            str(self.root / "missing-child-executable"),
        ]
        card = self.root / "docs" / "experiments" / f"{experiment_id}.md"
        card.write_text(
            f"# {experiment_id}: Real Runner Launch Failure\n\n"
            "## Exact Manual Command On Mutton2\n\n"
            "```bash\n"
            + " ".join(shlex.quote(part) for part in command)
            + "\n```\n",
            encoding="utf-8",
        )

    def _git(self, *arguments: str) -> None:
        result = subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)

    def close(self) -> None:
        self.temporary.cleanup()


class ExperimentQueueTests(unittest.TestCase):
    """Exercise the queue without requiring NVIDIA hardware."""

    def setUp(self) -> None:
        self.repo = TemporaryQueueRepository()
        self.marker = self.repo.root / "outputs" / "marker.json"
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        self.previous_marker = os.environ.get("QUEUE_TEST_MARKER")
        os.environ["QUEUE_TEST_MARKER"] = str(self.marker)

    def tearDown(self) -> None:
        if self.previous_marker is None:
            os.environ.pop("QUEUE_TEST_MARKER", None)
        else:
            os.environ["QUEUE_TEST_MARKER"] = self.previous_marker
        self.repo.close()

    def test_context_managed_connections_close_database_handles(self) -> None:
        connection = self.repo.store.connect()
        with connection as active:
            self.assertEqual(active.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed database"):
            connection.execute("SELECT 1")

    def test_scheduler_rejects_invalid_tuning_values(self) -> None:
        invalid_options = (
            ("poll_seconds", 0.0),
            ("poll_seconds", "not-a-number"),
            ("control_seconds", float("nan")),
            ("control_seconds", True),
            ("min_free_memory_fraction", 1.1),
            ("max_utilization_percent", float("inf")),
            ("max_utilization_percent", 101.0),
            ("min_free_disk_gib", -1.0),
            ("termination_grace_seconds", -1.0),
            ("max_consecutive_failures", 0),
            ("max_consecutive_failures", 1.5),
            ("max_consecutive_failures", True),
        )
        for field, value in invalid_options:
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(QueueError, field):
                    Scheduler(self.repo.store, **{field: value})

    @staticmethod
    def gpu(index: str = "0", uuid: str = "GPU-test-0000") -> GpuSnapshot:
        return GpuSnapshot(
            index=index,
            uuid=uuid,
            name="Test GPU",
            memory_total_mib=100_000,
            memory_used_mib=100,
            utilization_percent=0,
            compute_pids=(),
        )

    def _structured_receipt_payload(
        self,
        item_id: int,
        run_dir: Path,
        *,
        status: str = "running",
        return_code: int | None = None,
    ) -> dict[str, object]:
        """Build one scheduler-owned RunnerReceipt/v1 test document."""

        return {
            **queue_module.RUNNER_RECEIPT_V1.document_identity(),
            "run_id": run_dir.name,
            "queue_item_id": item_id,
            "segment": 1,
            "status": status,
            "return_code": return_code,
            "run_directory": str(run_dir.resolve()),
            "manifest": str((run_dir / "manifest.json").resolve()),
            "logs": {
                "stdout": str((run_dir / "stdout.log").resolve()),
                "stderr": str((run_dir / "stderr.log").resolve()),
            },
            "sync": None,
            "written_at": queue_module.utc_now_iso(),
        }

    def test_legacy_stdout_runner_receipt_parser_is_exact_and_nonguessing(self) -> None:
        log_path = self.repo.root / "legacy-launcher.log"
        log_path.write_text(
            "child output\n"
            "\x1b[32mrun directory: outputs/experiments/legacy-run\x1b[0m\r"
            "manifest: outputs/experiments/legacy-run/manifest.json\n"
            "pull outputs with: rsync -av host:legacy-run local-output\n",
            encoding="utf-8",
        )

        self.assertEqual(
            queue_module._read_legacy_stdout_runner_receipt(log_path),  # noqa: SLF001
            {
                "run_directory": "outputs/experiments/legacy-run",
                "manifest": "outputs/experiments/legacy-run/manifest.json",
                "rsync_pull_command": "rsync -av host:legacy-run local-output",
            },
        )

        malformed_values = (
            "run directory: outputs/experiments/partial\n",
            (
                "run directory: outputs/experiments/one\n"
                "run directory: outputs/experiments/two\n"
                "manifest: outputs/experiments/two/manifest.json\n"
            ),
        )
        for value in malformed_values:
            with self.subTest(value=value):
                log_path.write_text(value, encoding="utf-8")
                with self.assertRaises(queue_module.RunnerReceiptError):
                    queue_module._read_legacy_stdout_runner_receipt(  # noqa: SLF001
                        log_path
                    )

    @unittest.skipIf(os.name != "posix", "receipt symlink checks are POSIX-specific")
    def test_structured_runner_receipt_never_falls_back_when_present_but_invalid(
        self,
    ) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        segment_dir = queue_module._segment_dir(self.repo.store, item_id, 1)  # noqa: SLF001
        segment_dir.mkdir(parents=True)
        (segment_dir / "launcher.log").write_text(
            "run directory: outputs/experiments/legacy\n"
            "manifest: outputs/experiments/legacy/manifest.json\n",
            encoding="utf-8",
        )
        receipt_path = queue_module._runner_receipt_path(  # noqa: SLF001
            self.repo.store,
            item_id,
            1,
        )
        receipt_path.write_text('{"status":', encoding="utf-8")
        scheduler = Scheduler(self.repo.store, min_free_disk_gib=0, gpu_provider=lambda: [])

        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )

        self.assertEqual(receipt, {})
        self.assertIn("invalid JSON", str(error))
        self.assertTrue(structured_present)

        receipt_path.write_bytes(b"\xff")
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("invalid JSON", str(error))
        self.assertTrue(structured_present)

        receipt_path.write_text("[" * 100_000 + "0" + "]" * 100_000, encoding="utf-8")
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("invalid JSON", str(error))
        self.assertTrue(structured_present)

        receipt_path.write_text(
            '{"queue_item_id":' + ("9" * 5000) + "}",
            encoding="utf-8",
        )
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("invalid JSON", str(error))
        self.assertTrue(structured_present)

        invalid_status = self._structured_receipt_payload(
            item_id,
            self.repo.root / "outputs" / "experiments" / "invalid-status",
        )
        invalid_status["status"] = []
        queue_module._atomic_write_json(receipt_path, invalid_status)  # noqa: SLF001
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("invalid status", str(error))
        self.assertTrue(structured_present)

        valid = json.dumps(
            self._structured_receipt_payload(
                item_id,
                self.repo.root / "outputs" / "experiments" / "duplicate-key",
            )
        )
        receipt_path.write_text(
            valid.replace(
                '"status": "running"',
                '"status": "failed", "status": "running"',
                1,
            ),
            encoding="utf-8",
        )
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("repeats JSON key 'status'", str(error))
        self.assertTrue(structured_present)

        outside_run_dir = self.repo.root.parent / "outside-run"
        queue_module._atomic_write_json(  # noqa: SLF001
            receipt_path,
            self._structured_receipt_payload(item_id, outside_run_dir),
        )
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("outside authorized roots", str(error))
        self.assertTrue(structured_present)

        receipt_path.unlink()
        receipt_path.symlink_to(segment_dir / "missing-receipt-target.json")
        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )
        self.assertEqual(receipt, {})
        self.assertIn("not a regular file", str(error))
        self.assertTrue(structured_present)

        receipt_path.unlink()
        unresolved_run_dir = (
            self.repo.root / "outputs" / "experiments" / "unresolved"
        )
        payload = self._structured_receipt_payload(
            item_id,
            unresolved_run_dir,
        )
        failing_value = Path(str(payload["run_directory"]))
        queue_module._atomic_write_json(receipt_path, payload)  # noqa: SLF001
        original_resolve = Path.resolve

        def fail_selected_resolve(path: Path, *args, **kwargs) -> Path:
            if path == failing_value:
                raise RuntimeError("simulated symlink loop")
            return original_resolve(path, *args, **kwargs)

        with mock.patch.object(Path, "resolve", new=fail_selected_resolve):
            receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
                self.repo.store.item(item_id)
            )
        self.assertEqual(receipt, {})
        self.assertIn("cannot be resolved", str(error))
        self.assertTrue(structured_present)

    def test_incomplete_atomic_temp_receipt_leaves_legacy_fallback_available(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        segment_dir = queue_module._segment_dir(self.repo.store, item_id, 1)  # noqa: SLF001
        segment_dir.mkdir(parents=True)
        (segment_dir / ".runner-receipt.json.partial.tmp").write_text(
            '{"incomplete":',
            encoding="utf-8",
        )
        (segment_dir / "launcher.log").write_text(
            "run directory: outputs/experiments/legacy\n"
            "manifest: outputs/experiments/legacy/manifest.json\n",
            encoding="utf-8",
        )
        scheduler = Scheduler(self.repo.store, min_free_disk_gib=0, gpu_provider=lambda: [])

        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )

        self.assertIsNone(error)
        self.assertFalse(structured_present)
        self.assertEqual(receipt["run_directory"], "outputs/experiments/legacy")

    @unittest.skipIf(os.name != "posix", "shared artifact symlinks are POSIX-specific")
    def test_structured_receipt_accepts_a_configured_shared_symlink_root(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        external_directory = tempfile.TemporaryDirectory(
            prefix="experiment-queue-external-runs-"
        )
        self.addCleanup(external_directory.cleanup)
        external_root = Path(external_directory.name)
        run_dir = external_root / "accepted-run"
        run_dir.mkdir(parents=True)
        (self.repo.root / "runs").symlink_to(external_root, target_is_directory=True)
        receipt_path = queue_module._runner_receipt_path(  # noqa: SLF001
            self.repo.store,
            item_id,
            1,
        )
        queue_module._atomic_write_json(  # noqa: SLF001
            receipt_path,
            self._structured_receipt_payload(item_id, run_dir),
        )
        scheduler = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )

        receipt, error, structured_present = scheduler._read_item_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )

        self.assertIsNone(error)
        self.assertTrue(structured_present)
        self.assertEqual(receipt["run_directory"], str(run_dir.resolve()))

    def test_restarted_scheduler_ingests_initial_structured_runner_receipt(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        run_dir = self.repo.root / "outputs" / "experiments" / "restart-run"
        run_dir.mkdir(parents=True)
        receipt_path = queue_module._runner_receipt_path(  # noqa: SLF001
            self.repo.store,
            item_id,
            1,
        )
        queue_module._atomic_write_json(  # noqa: SLF001
            receipt_path,
            self._structured_receipt_payload(item_id, run_dir),
        )
        restarted = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )

        restarted._ingest_structured_runner_receipt(  # noqa: SLF001
            self.repo.store.item(item_id)
        )

        item = self.repo.store.item(item_id)
        self.assertEqual(item["runner_run_dir"], str(run_dir.resolve()))
        self.assertEqual(
            item["runner_manifest_path"],
            str((run_dir / "manifest.json").resolve()),
        )
        with self.repo.store.connect() as connection:
            latest_event = connection.execute(
                "SELECT event_type FROM events WHERE queue_item_id = ? ORDER BY id DESC LIMIT 1",
                (item_id,),
            ).fetchone()
        self.assertEqual(latest_event["event_type"], "RUNNER_RECEIPT_INGESTED")

    def test_card_command_is_read_only_after_explicit_selection(self) -> None:
        card = read_card_command(self.repo.root, "TST-001")

        self.assertEqual(card.experiment_id, "TST-001")
        self.assertIn("scripts/run_experiment.py", card.command_text)
        self.assertEqual(self.repo.store.list_items(), [])

    def test_card_rejects_doubled_shell_line_continuation(self) -> None:
        experiment_id = "TST-008"
        card = self.repo.root / "docs" / "experiments" / f"{experiment_id}.md"
        card.write_text(
            f"# {experiment_id}: Invalid Continuation\n\n"
            "## Exact Manual Command On Mutton2\n\n"
            "```bash\n"
            "cd ~/3D_Helmholtz\n"
            "python3 scripts/run_experiment.py \\\\\n"
            "  --name invalid --require-clean --remote mutton2 -- python3 -V\n"
            "```\n",
            encoding="utf-8",
        )
        self.repo._git("add", str(card.relative_to(self.repo.root)))
        self.repo._git("commit", "-qm", "add invalid continuation card")

        with self.assertRaisesRegex(QueueError, "doubled trailing backslash"):
            read_card_command(self.repo.root, experiment_id)

    def test_v1_database_migrates_in_place_for_reservations_and_segments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            state_dir.mkdir()
            database = state_dir / "queue.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.executescript(
                    """
                    CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE queue_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        experiment_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        priority INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(experiment_id, attempt)
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)",
                    [
                        ("schema_version", "1"),
                        ("repo_root", str(root)),
                        ("dispatch_paused", "0"),
                        ("pause_reason", ""),
                        ("consecutive_failures", "0"),
                    ],
                )

            migrated = QueueStore(state_dir, root)

            self.assertEqual(migrated.get_meta("schema_version"), "4")
            with migrated.connect() as connection:
                columns = {
                    row["name"] for row in connection.execute("PRAGMA table_info(queue_items)")
                }
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table'"
                    )
                }
            self.assertIn("preemptible", columns)
            self.assertIn("segment", columns)
            self.assertIn("git_ref", columns)
            self.assertIn("worktree_path", columns)
            self.assertIn("gpu_reservations", tables)

    def test_future_schema_is_rejected_before_database_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            state_dir.mkdir()
            database = state_dir / "queue.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.executescript(
                    """
                    CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE future_only (proof TEXT NOT NULL);
                    INSERT INTO metadata(key, value) VALUES ('schema_version', '5');
                    INSERT INTO future_only(proof) VALUES ('must survive');
                    """
                )
            before = {
                path.name: path.read_bytes()
                for path in state_dir.glob("queue.sqlite3*")
            }

            with self.assertRaisesRegex(QueueError, "schema.*5.*not supported"):
                QueueStore(state_dir, root)

            after = {
                path.name: path.read_bytes()
                for path in state_dir.glob("queue.sqlite3*")
            }
            self.assertEqual(after, before)
            database_uri = f"{database.as_uri()}?mode=ro&immutable=1"
            with sqlite3.connect(database_uri, uri=True) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT value FROM metadata WHERE key = 'schema_version'"
                    ).fetchone()[0],
                    "5",
                )
                self.assertEqual(
                    connection.execute("SELECT proof FROM future_only").fetchone()[0],
                    "must survive",
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT name FROM sqlite_schema WHERE name = 'queue_items'"
                    ).fetchone()
                )

    def test_unknown_schema_is_rejected_before_database_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            state_dir.mkdir()
            database = state_dir / "queue.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES ('schema_version', 'unknown')"
                )
            before = {
                path.name: path.read_bytes()
                for path in state_dir.glob("queue.sqlite3*")
            }

            with self.assertRaisesRegex(QueueError, "schema version.*unknown.*supported"):
                QueueStore(state_dir, root)

            after = {
                path.name: path.read_bytes()
                for path in state_dir.glob("queue.sqlite3*")
            }
            self.assertEqual(after, before)
            database_uri = f"{database.as_uri()}?mode=ro&immutable=1"
            with sqlite3.connect(database_uri, uri=True) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT value FROM metadata WHERE key = 'schema_version'"
                    ).fetchone()[0],
                    "unknown",
                )
                self.assertIsNone(
                    connection.execute(
                        "SELECT name FROM sqlite_schema WHERE name = 'queue_items'"
                    ).fetchone()
                )

    def test_future_schema_in_wal_is_rejected_without_touching_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            state_dir.mkdir()
            database = state_dir / "queue.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.executescript(
                    """
                    CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    INSERT INTO metadata(key, value) VALUES ('schema_version', '4');
                    """
                )
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA wal_autocheckpoint = 0")
                connection.execute(
                    "UPDATE metadata SET value = '5' WHERE key = 'schema_version'"
                )
                connection.commit()
                before = {
                    path.name: path.read_bytes()
                    for path in state_dir.glob("queue.sqlite3*")
                }
                self.assertIn("queue.sqlite3-wal", before)
                self.assertIn("queue.sqlite3-shm", before)

                with self.assertRaisesRegex(QueueError, "schema.*5.*not supported"):
                    QueueStore(state_dir, root)

                after = {
                    path.name: path.read_bytes()
                    for path in state_dir.glob("queue.sqlite3*")
                }
                self.assertEqual(after, before)
                self.assertEqual(
                    connection.execute(
                        "SELECT value FROM metadata WHERE key = 'schema_version'"
                    ).fetchone()[0],
                    "5",
                )
            finally:
                connection.close()

    def test_add_does_not_scan_other_cards_or_status(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001", priority=7)

        items = self.repo.store.list_items()
        self.assertEqual([item["experiment_id"] for item in items], ["TST-001"])
        self.assertEqual(items[0]["priority"], 7)
        self.assertEqual(items[0]["id"], item_id)
        self.assertNotIn("TST-002", format_status(self.repo.store))
        self.assertNotIn("TST-999", format_status(self.repo.store))

    def test_v2_migration_pins_an_existing_pending_item(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        item = self.repo.store.item(item_id)
        subprocess.run(
            ["git", "update-ref", "-d", str(item["git_ref"])],
            cwd=self.repo.root,
            check=True,
        )
        with self.repo.store.connect() as connection:
            connection.execute("UPDATE metadata SET value = '2' WHERE key = 'schema_version'")
            connection.execute(
                "UPDATE queue_items SET git_ref = NULL WHERE id = ?",
                (item_id,),
            )

        migrated = QueueStore(self.repo.state_dir, self.repo.root)

        item = migrated.item(item_id)
        self.assertEqual(migrated.get_meta("schema_version"), "4")
        self.assertEqual(item["git_ref"], f"refs/experiment-queue/items/{item_id}")
        pinned = subprocess.check_output(
            ["git", "rev-parse", str(item["git_ref"])],
            cwd=self.repo.root,
            text=True,
        ).strip()
        self.assertEqual(pinned, item["git_commit"])

    def test_v3_migration_binds_legacy_continuation_metadata_or_holds(
        self,
    ) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        runner = self.repo.root / "outputs" / "legacy-yield"
        runner.mkdir(parents=True)
        checkpoint = runner / "checkpoint.json"
        metadata = runner / "checkpoint.metadata.json"
        checkpoint.write_text('{"step": 4}\n', encoding="utf-8")
        metadata.write_text('{"checkpoint": "bound"}\n', encoding="utf-8")
        receipt = (
            self.repo.state_dir
            / "attempts"
            / str(item_id)
            / "segments"
            / "1"
            / "yield"
            / "receipt.json"
        )
        receipt.parent.mkdir(parents=True)
        receipt.write_text(
            json.dumps(
                {
                    "checkpoint": str(checkpoint),
                    "checkpoint_metadata": str(metadata),
                }
            ),
            encoding="utf-8",
        )
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE metadata SET value = '3' WHERE key = 'schema_version'"
            )
            connection.execute(
                "UPDATE queue_items SET state = 'queued', segment = 2, "
                "runner_run_dir = ?, continuation_checkpoint = ?, "
                "continuation_checkpoint_sha256 = ?, "
                "continuation_checkpoint_metadata = NULL, "
                "continuation_checkpoint_metadata_sha256 = NULL WHERE id = ?",
                (
                    str(runner),
                    str(checkpoint),
                    hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                    item_id,
                ),
            )

        migrated = QueueStore(self.repo.state_dir, self.repo.root)
        migrated_item = migrated.item(item_id)
        self.assertEqual(migrated_item["state"], "queued")
        self.assertEqual(
            migrated_item["continuation_checkpoint_metadata"], str(metadata.resolve())
        )
        self.assertEqual(
            migrated_item["continuation_checkpoint_metadata_sha256"],
            hashlib.sha256(metadata.read_bytes()).hexdigest(),
        )

        metadata.unlink()
        with migrated.connect() as connection:
            connection.execute(
                "UPDATE metadata SET value = '3' WHERE key = 'schema_version'"
            )
            connection.execute(
                "UPDATE queue_items SET continuation_checkpoint_metadata = NULL, "
                "continuation_checkpoint_metadata_sha256 = NULL WHERE id = ?",
                (item_id,),
            )
        held = QueueStore(self.repo.state_dir, self.repo.root).item(item_id)
        self.assertEqual(held["state"], "held")
        self.assertIn("could not bind", held["state_detail"])

    def test_remove_preserves_history_and_explicit_readd_creates_new_membership(self) -> None:
        first = add_experiment(self.repo.store, "TST-001")
        first_ref = str(self.repo.store.item(first)["git_ref"])
        remove_item(self.repo.store, first, "operator changed ordering")
        second = add_experiment(self.repo.store, "TST-001")

        self.assertNotEqual(first, second)
        items = self.repo.store.list_items()
        self.assertEqual([item["state"] for item in items], ["removed", "queued"])
        self.assertEqual([item["attempt"] for item in items], [1, 2])
        self.assertIsNotNone(items[0]["worktree_removed_at"])
        self.assertNotEqual(
            subprocess.run(
                ["git", "show-ref", "--verify", first_ref],
                cwd=self.repo.root,
                check=False,
            ).returncode,
            0,
        )

    def test_removing_dependency_holds_its_pending_dependent(self) -> None:
        prerequisite = add_experiment(self.repo.store, "TST-001")
        dependent = add_experiment(
            self.repo.store, "TST-002", dependency_ids=[prerequisite]
        )

        remove_item(self.repo.store, prerequisite, "operator removed prerequisite")

        child = self.repo.store.item(dependent)
        self.assertEqual(child["state"], "held")
        self.assertIn("dependency", child["state_detail"])
        self.assertIn(f"{prerequisite}:removed", format_status(self.repo.store))

    def test_prior_launched_attempt_requires_explicit_new_attempt(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'failed', started_at = ?, finished_at = ? WHERE id = ?",
                ("2026-08-03T00:00:00+00:00", "2026-08-03T00:01:00+00:00", item_id),
            )

        with self.assertRaisesRegex(QueueError, "--new-attempt"):
            add_experiment(self.repo.store, "TST-001")
        second = add_experiment(self.repo.store, "TST-001", new_attempt=True)
        self.assertEqual(self.repo.store.item(second)["attempt"], 2)

    def test_priority_changes_pending_and_resumable_active_items(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        for priority, state in enumerate(
            ("queued", "held", "blocked", "starting", "running", "yielding"),
            start=40,
        ):
            with self.repo.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state = ? WHERE id = ?",
                    (state, item_id),
                )
            set_priority(self.repo.store, item_id, priority)
            self.assertEqual(self.repo.store.item(item_id)["priority"], priority)

        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'succeeded' WHERE id = ?", (item_id,)
            )
        with self.assertRaisesRegex(QueueError, "priority can change only"):
            set_priority(self.repo.store, item_id, 1)

    def test_resume_front_applies_only_within_the_same_priority_band(self) -> None:
        high = add_experiment(self.repo.store, "TST-001", priority=20)
        ordinary = add_experiment(self.repo.store, "TST-002", priority=10)
        resumed = add_experiment(self.repo.store, "TST-003", priority=10)
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET resume_front = 1 WHERE id = ?", (resumed,)
            )
        scheduler = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )

        self.assertEqual(scheduler._next_item()["id"], high)  # noqa: SLF001
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'held' WHERE id = ?", (high,)
            )
        self.assertEqual(scheduler._next_item()["id"], resumed)  # noqa: SLF001
        self.assertNotEqual(resumed, ordinary)

    def test_gpu_set_drains_removed_running_gpu(self) -> None:
        gpu0 = self.gpu("0", "GPU-test-0000")
        gpu1 = self.gpu("1", "GPU-test-1111")
        update_gpu_allowlist(
            self.repo.store, "set", ["0", "1"], snapshots=[gpu0, gpu1]
        )
        item_id = add_experiment(self.repo.store, "TST-001")
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ? WHERE id = ?",
                (gpu0.uuid, item_id),
            )

        update_gpu_allowlist(self.repo.store, "set", ["1"], snapshots=[gpu0, gpu1])

        with self.repo.store.connect() as connection:
            rows = {row["uuid"]: row for row in connection.execute("SELECT * FROM gpu_allowlist")}
        self.assertFalse(rows[gpu0.uuid]["enabled"])
        self.assertTrue(rows[gpu0.uuid]["draining"])
        self.assertTrue(rows[gpu1.uuid]["enabled"])

    def test_gpu_remove_can_disable_an_unobserved_allowed_uuid(self) -> None:
        gpu = self.gpu("0", "GPU-test-0000")
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])

        update_gpu_allowlist(
            self.repo.store, "remove", [gpu.uuid], snapshots=[]
        )

        with self.repo.store.connect() as connection:
            rows = list(connection.execute("SELECT * FROM gpu_allowlist"))
        self.assertEqual(rows, [])

    def test_scheduler_launches_explicit_item_and_records_success(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-001")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )

        scheduler.run_iteration(force_gpu_poll=True)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=False)
            if self.repo.store.item(item_id)["state"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "succeeded")
        self.assertEqual(item["return_code"], 0)
        self.assertEqual(item["runner_run_dir"], "outputs/experiments/fake-run")
        self.assertIn("mutton2:fake-run", item["rsync_pull_command"])
        self.assertIn("queue item 1", format_pull_commands(self.repo.store))
        self.assertEqual(json.loads(self.marker.read_text())["gpu"], gpu.uuid)
        self.assertTrue(
            (
                self.repo.state_dir
                / "attempts"
                / str(item_id)
                / "segments"
                / "1"
                / "exit.json"
            ).is_file()
        )

    def test_scheduler_invokes_existing_experiment_runner_end_to_end(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-006")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )

        deadline = time.monotonic() + 10
        with mock.patch(
            "experiment_queue.queue._read_legacy_stdout_runner_receipt",
            side_effect=AssertionError(
                "new runner jobs must not depend on the legacy stdout parser"
            ),
        ):
            while time.monotonic() < deadline:
                scheduler.run_iteration(force_gpu_poll=True)
                if self.repo.store.item(item_id)["state"] == "succeeded":
                    break
                time.sleep(0.05)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "succeeded")
        manifest_path = Path(item["runner_manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "succeeded")
        stdout = (manifest_path.parent / "stdout.log").read_text(encoding="utf-8")
        self.assertIn(f"gpu={gpu.uuid}", stdout)
        self.assertIn("mutton2:", item["rsync_pull_command"])
        receipt_path = queue_module._runner_receipt_path(  # noqa: SLF001
            self.repo.store,
            item_id,
            1,
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["status"], "succeeded")
        self.assertEqual(receipt["queue_item_id"], item_id)

    def test_runner_launch_failure_receipt_matches_observed_cli_exit(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-010")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            if self.repo.store.item(item_id)["state"] == "failed":
                break
            time.sleep(0.05)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "failed")
        self.assertEqual(item["return_code"], 2)
        self.assertIsNone(item["state_detail"])
        receipt_path = queue_module._runner_receipt_path(  # noqa: SLF001
            self.repo.store,
            item_id,
            1,
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["status"], "launch_failed")
        self.assertEqual(receipt["return_code"], 2)
        manifest = json.loads(Path(receipt["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "launch_failed")
        self.assertEqual(manifest["run"]["return_code"], 127)

    @unittest.skipIf(os.name != "posix", "process-group termination is POSIX-specific")
    def test_terminate_interrupts_entire_running_attempt(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-003")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=100,
            min_free_disk_gib=0,
            termination_grace_seconds=1,
            gpu_provider=lambda: [gpu],
        )
        scheduler.run_iteration(force_gpu_poll=True)
        self.assertEqual(self.repo.store.item(item_id)["state"], "running")

        self.assertTrue(
            request_termination(
                self.repo.store, item_id, reason="unit-test stop", force=False
            )
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=False)
            if self.repo.store.item(item_id)["state"] == "interrupted":
                break
            time.sleep(0.05)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "interrupted")
        self.assertEqual(item["terminate_reason"], "unit-test stop")

    @unittest.skipIf(os.name != "posix", "process-group termination is POSIX-specific")
    def test_force_kill_is_recorded_separately(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-003")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=100,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )
        scheduler.run_iteration(force_gpu_poll=True)

        self.assertTrue(
            request_termination(
                self.repo.store, item_id, reason="unit-test force stop", force=True
            )
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=False)
            if self.repo.store.item(item_id)["state"] == "force_killed":
                break
            time.sleep(0.05)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "force_killed")
        self.assertEqual(item["terminate_reason"], "unit-test force stop")

    @unittest.skipIf(os.name != "posix", "process-group termination is POSIX-specific")
    def test_pinned_worktrees_ignore_primary_updates_and_cleanup_when_terminal(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        first_id = add_experiment(self.repo.store, "TST-003")
        first = self.repo.store.item(first_id)
        first_commit = str(first["git_commit"])
        first_ref = str(first["git_ref"])
        ref_head = subprocess.check_output(
            ["git", "rev-parse", first_ref], cwd=self.repo.root, text=True
        ).strip()
        self.assertEqual(ref_head, first_commit)
        self.assertIsNone(first["worktree_path"])

        (self.repo.root / "STATUS.md").write_text("# committed after first admission\n", encoding="utf-8")
        self.repo._git("add", "STATUS.md")
        self.repo._git("commit", "-qm", "primary checkout update")
        second_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=self.repo.root, text=True
        ).strip()
        self.assertNotEqual(second_commit, first_commit)
        second_id = add_experiment(self.repo.store, "TST-001")

        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )
        scheduler.run_iteration(force_gpu_poll=True)
        first = self.repo.store.item(first_id)
        self.assertEqual(first["state"], "running")
        first_worktree = Path(str(first["worktree_path"]))
        self.assertTrue(first_worktree.is_dir())

        marker_deadline = time.monotonic() + 5
        while time.monotonic() < marker_deadline and not self.marker.is_file():
            time.sleep(0.02)
        marker = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertEqual(marker["head"], first_commit)
        self.assertEqual(Path(marker["cwd"]), first_worktree)
        self.assertEqual(marker["worktree"], str(first_worktree))

        (self.repo.root / "STATUS.md").write_text("# uncommitted during run\n", encoding="utf-8")
        scheduler.run_iteration(force_gpu_poll=True)

        first = self.repo.store.item(first_id)
        self.assertEqual(first["state"], "running")
        self.assertFalse(first["repo_drift_detected"])
        self.assertEqual(self.repo.store.get_meta("dispatch_paused"), "0")

        request_termination(
            self.repo.store, first_id, reason="finish worktree-isolation test", force=True
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            first_state = self.repo.store.item(first_id)["state"]
            second_state = self.repo.store.item(second_id)["state"]
            if first_state == "force_killed" and second_state == "succeeded":
                break
            time.sleep(0.05)
        first = self.repo.store.item(first_id)
        second = self.repo.store.item(second_id)
        self.assertEqual(first["state"], "force_killed")
        self.assertEqual(second["state"], "succeeded")
        self.assertIsNotNone(first["worktree_removed_at"])
        self.assertIsNotNone(second["worktree_removed_at"])
        self.assertFalse(first_worktree.exists())
        self.assertFalse(Path(str(second["worktree_path"])).exists())
        self.assertNotEqual(
            subprocess.run(
                ["git", "show-ref", "--verify", first_ref],
                cwd=self.repo.root,
                check=False,
            ).returncode,
            0,
        )
        marker = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertEqual(marker["head"], second_commit)

    def test_two_consecutive_child_failures_pause_dispatch(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        first = add_experiment(self.repo.store, "TST-004")
        second = add_experiment(self.repo.store, "TST-005")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            max_consecutive_failures=2,
            gpu_provider=lambda: [gpu],
        )

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            states = [self.repo.store.item(item_id)["state"] for item_id in (first, second)]
            if states == ["failed", "failed"]:
                break
            time.sleep(0.05)

        self.assertEqual(self.repo.store.item(first)["state"], "failed")
        self.assertEqual(self.repo.store.item(second)["state"], "failed")
        self.assertEqual(self.repo.store.get_meta("dispatch_paused"), "1")
        self.assertIn("circuit breaker", self.repo.store.get_meta("pause_reason"))

    def test_idle_gpu_reservation_expires_without_changing_allowlist(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        reservation_id = request_gpu_reservation(
            self.repo.store,
            gpu.uuid,
            duration_hours=1,
            note="Alex — short benchmark",
            actor="web:reservation",
            snapshots=[gpu],
        )
        reservation = list_reservations(self.repo.store)[0]
        self.assertEqual(reservation["id"], reservation_id)
        self.assertEqual(reservation["status"], "active")

        expire_reservations(
            self.repo.store,
            now=time_to_datetime(reservation["expires_at"], seconds=1),
        )

        self.assertEqual(list_reservations(self.repo.store)[0]["status"], "expired")
        with self.repo.store.connect() as connection:
            allowed = connection.execute(
                "SELECT enabled FROM gpu_allowlist WHERE uuid = ?", (gpu.uuid,)
            ).fetchone()
        self.assertEqual(allowed["enabled"], 1)

    def test_gpu_reservation_duration_rejects_boolean_values(self) -> None:
        with self.assertRaisesRegex(QueueError, "whole number"):
            request_gpu_reservation(
                self.repo.store,
                "GPU-test-0000",
                duration_hours=True,
                note="Alex — short benchmark",
                actor="web:reservation",
            )

    def test_pending_gpu_reservation_cannot_be_released_mid_checkpoint(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(
            self.repo.store,
            "TST-007",
            preemptible=True,
        )
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ?, "
                "assigned_gpu_index = ? WHERE id = ?",
                (gpu.uuid, gpu.index, item_id),
            )
        reservation_id = request_gpu_reservation(
            self.repo.store,
            gpu.uuid,
            duration_hours=2,
            note="Alex — short benchmark",
            actor="web:reservation",
            snapshots=[gpu],
        )

        with self.assertRaisesRegex(QueueError, "still checkpointing"):
            release_gpu_reservation(
                self.repo.store,
                reservation_id,
                actor="web:reservation",
            )

        reservation = list_reservations(self.repo.store)[0]
        self.assertEqual(reservation["status"], "pending")
        self.assertEqual(self.repo.store.item(item_id)["state"], "yielding")

    def test_failed_yield_delivery_does_not_clobber_concurrent_termination(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        item_id = add_experiment(self.repo.store, "TST-007", preemptible=True)
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ?, "
                "assigned_gpu_index = ? WHERE id = ?",
                (gpu.uuid, gpu.index, item_id),
            )

        def fail_after_termination(_path, _payload) -> None:
            with self.repo.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state = 'terminating', state_detail = ? WHERE id = ?",
                    ("admin requested termination", item_id),
                )
            raise OSError("simulated request write failure")

        with mock.patch(
            "experiment_queue.queue._atomic_write_json",
            side_effect=fail_after_termination,
        ):
            with self.assertRaisesRegex(QueueError, "could not deliver yield request"):
                request_gpu_reservation(
                    self.repo.store,
                    gpu.uuid,
                    duration_hours=2,
                    note="Alex — short benchmark",
                    actor="web:reservation",
                    snapshots=[gpu],
                )

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "terminating")
        self.assertEqual(item["state_detail"], "admin requested termination")
        self.assertEqual(list_reservations(self.repo.store)[0]["status"], "failed")

    def test_manual_preemption_requires_a_capable_stably_running_gpu_item(self) -> None:
        gpu = self.gpu()
        item_id = add_experiment(self.repo.store, "TST-007")
        with self.assertRaisesRegex(QueueError, "stably running"):
            request_preemption(self.repo.store, item_id)

        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ?, "
                "assigned_gpu_index = ? WHERE id = ?",
                (gpu.uuid, gpu.index, item_id),
            )
        with self.assertRaisesRegex(QueueError, "not marked"):
            request_preemption(self.repo.store, item_id)

        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET preemptible = 1, assigned_gpu_uuid = NULL "
                "WHERE id = ?",
                (item_id,),
            )
        with self.assertRaisesRegex(QueueError, "no assigned GPU"):
            request_preemption(self.repo.store, item_id)

    def test_failed_manual_preemption_delivery_preserves_concurrent_termination(self) -> None:
        gpu = self.gpu()
        item_id = add_experiment(self.repo.store, "TST-007", preemptible=True)
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'running', assigned_gpu_uuid = ?, "
                "assigned_gpu_index = ? WHERE id = ?",
                (gpu.uuid, gpu.index, item_id),
            )

        def fail_after_termination(_path, _payload) -> None:
            with self.repo.store.connect() as connection:
                connection.execute(
                    "UPDATE queue_items SET state = 'terminating', state_detail = ? "
                    "WHERE id = ?",
                    ("admin requested termination", item_id),
                )
            raise OSError("simulated request write failure")

        with mock.patch(
            "experiment_queue.queue._atomic_write_json",
            side_effect=fail_after_termination,
        ):
            with self.assertRaisesRegex(
                QueueError, "could not deliver manual preemption request"
            ):
                request_preemption(self.repo.store, item_id)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "terminating")
        self.assertEqual(item["state_detail"], "admin requested termination")
        self.assertEqual(list_reservations(self.repo.store), [])

    def test_terminal_continuation_preserves_original_runner_receipt_paths(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        run_dir = self.repo.root / "outputs" / "experiments" / "original-run"
        manifest = run_dir / "manifest.json"
        pull_command = "rsync -av mutton2:original-run local-output"
        with self.repo.store.connect() as connection:
            connection.execute(
                """
                UPDATE queue_items SET state = 'terminating', segment = 2,
                    runner_run_dir = ?, runner_manifest_path = ?, rsync_pull_command = ?
                WHERE id = ?
                """,
                (str(run_dir), str(manifest), pull_command, item_id),
            )
        scheduler = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )

        scheduler._finalize_item(  # noqa: SLF001 - focused recovery regression test
            self.repo.store.item(item_id),
            {"return_code": 130},
        )

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "interrupted")
        self.assertEqual(item["runner_run_dir"], str(run_dir))
        self.assertEqual(item["runner_manifest_path"], str(manifest))
        self.assertEqual(item["rsync_pull_command"], pull_command)

    def test_yield_receipt_rejects_invalid_generic_progress(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-009", preemptible=True)
        request_id = "test-yield-request"
        run_dir = self.repo.root / "outputs" / "experiments" / "validation-run"
        checkpoint = run_dir / "shared_state.json"
        metadata = run_dir / "shared_state.metadata.json"
        checkpoint.parent.mkdir(parents=True)
        checkpoint.write_text('{"next_row": 0}\n', encoding="utf-8")
        metadata.write_text('{"schema_version": 1}\n', encoding="utf-8")
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'yielding', yield_request_id = ?, "
                "runner_run_dir = ? WHERE id = ?",
                (request_id, str(run_dir), item_id),
            )
        receipt_path = (
            self.repo.state_dir
            / "attempts"
            / str(item_id)
            / "segments"
            / "1"
            / "yield"
            / "receipt.json"
        )
        receipt_path.parent.mkdir(parents=True)
        base_receipt = {
            "schema_version": 1,
            "status": "ready",
            "request_id": request_id,
            "queue_item_id": item_id,
            "step": 0,
            "checkpoint": str(checkpoint),
            "checkpoint_metadata": str(metadata),
            "checkpoint_bytes": checkpoint.stat().st_size,
            "checkpoint_sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
        }
        invalid_cases = (
            (None, 0, "progress must be an object"),
            ({"unit": "settled rows", "completed": 0}, 0, "progress unit"),
            ({"unit": "settled_rows", "completed": False}, 0, "progress completed"),
            ({"unit": "settled_rows", "completed": -1}, 0, "progress completed"),
            (
                {"unit": "settled_rows", "completed": 2, "total": 1},
                2,
                "progress total",
            ),
            ({"unit": "settled_rows", "completed": 0}, -1, "continuation step"),
        )
        scheduler = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )

        for progress, step, expected_error in invalid_cases:
            with self.subTest(progress=progress, step=step):
                receipt = dict(base_receipt, progress=progress, step=step)
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
                validated, error = scheduler._validated_yield_receipt(  # noqa: SLF001
                    self.repo.store.item(item_id),
                    runner_run_dir=str(run_dir),
                )
                self.assertIsNone(validated)
                self.assertIn(expected_error, str(error))

        receipt_path.write_text(
            json.dumps(
                dict(
                    base_receipt,
                    progress={"unit": "settled_rows", "completed": 0},
                    step=0,
                )
            ),
            encoding="utf-8",
        )
        validated, error = scheduler._validated_yield_receipt(  # noqa: SLF001
            self.repo.store.item(item_id),
            runner_run_dir=str(run_dir),
        )
        self.assertIsNone(error)
        self.assertEqual(
            validated["progress"],
            {"unit": "settled_rows", "completed": 0},
        )

        legacy_step_zero = dict(base_receipt, step=0)
        receipt_path.write_text(json.dumps(legacy_step_zero), encoding="utf-8")
        validated, error = scheduler._validated_yield_receipt(  # noqa: SLF001
            self.repo.store.item(item_id),
            runner_run_dir=str(run_dir),
        )
        self.assertIsNone(validated)
        self.assertIn("invalid optimizer step 0", str(error))

        with mock.patch(
            "experiment_queue.queue._sha256_file",
            side_effect=OSError("checkpoint disappeared during hashing"),
        ):
            validated, error = scheduler._validated_yield_receipt(  # noqa: SLF001
                self.repo.store.item(item_id),
                runner_run_dir=str(run_dir),
            )
        self.assertIsNone(validated)
        self.assertIn("could not be verified", str(error))

    def test_corrupt_continuation_is_held_without_blocking_next_item(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(self.repo.store, "set", ["0"], snapshots=[gpu])
        corrupt_id = add_experiment(self.repo.store, "TST-001")
        healthy_id = add_experiment(self.repo.store, "TST-002")
        runner = self.repo.root / "outputs" / "corrupt-continuation"
        runner.mkdir(parents=True)
        checkpoint = runner / "checkpoint.json"
        metadata = runner / "checkpoint.metadata.json"
        checkpoint.write_text('{"step": 4}\n', encoding="utf-8")
        metadata.write_text('{"metadata": "original"}\n', encoding="utf-8")
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET segment = 2, resume_front = 1, "
                "runner_run_dir = ?, continuation_checkpoint = ?, "
                "continuation_checkpoint_sha256 = ?, "
                "continuation_checkpoint_metadata = ?, "
                "continuation_checkpoint_metadata_sha256 = ?, "
                "continuation_step = 4 WHERE id = ?",
                (
                    str(runner.resolve()),
                    str(checkpoint.resolve()),
                    hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                    str(metadata.resolve()),
                    hashlib.sha256(metadata.read_bytes()).hexdigest(),
                    corrupt_id,
                ),
            )
        metadata.write_text('{"metadata": "changed"}\n', encoding="utf-8")
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=0.01,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )

        scheduler.run_iteration(force_gpu_poll=True)
        corrupt = self.repo.store.item(corrupt_id)
        self.assertEqual(corrupt["state"], "held")
        self.assertIn("metadata is missing or changed", corrupt["state_detail"])

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            if self.repo.store.item(healthy_id)["state"] == "succeeded":
                break
            time.sleep(0.05)
        self.assertEqual(self.repo.store.item(healthy_id)["state"], "succeeded")
        with self.repo.store.connect() as connection:
            events = list(
                connection.execute(
                    "SELECT event_type FROM events WHERE queue_item_id = ?",
                    (corrupt_id,),
                )
            )
        self.assertIn(
            "QUEUE_CONTINUATION_INTEGRITY_HELD",
            {row["event_type"] for row in events},
        )

    def test_stale_yield_finalization_preserves_newer_termination(self) -> None:
        gpu = self.gpu()
        item_id = add_experiment(self.repo.store, "TST-007", preemptible=True)
        request_id = "stale-yield-request"
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'yielding', yield_request_id = ?, "
                "assigned_gpu_uuid = ?, assigned_gpu_index = ? WHERE id = ?",
                (request_id, gpu.uuid, gpu.index, item_id),
            )
        stale = self.repo.store.item(item_id)
        with self.repo.store.connect() as connection:
            connection.execute(
                "UPDATE queue_items SET state = 'force_killing', state_detail = ? "
                "WHERE id = ?",
                ("operator force kill", item_id),
            )
        scheduler = Scheduler(
            self.repo.store,
            min_free_disk_gib=0,
            gpu_provider=lambda: [],
        )
        finalized = scheduler._finalize_yield(  # noqa: SLF001
            stale,
            executor_receipt={"return_code": 75},
            runner_receipt={},
            yield_receipt={
                "checkpoint": "/tmp/checkpoint",
                "checkpoint_sha256": "a" * 64,
                "checkpoint_metadata": "/tmp/checkpoint.metadata",
                "checkpoint_metadata_sha256": "b" * 64,
                "step": 1,
                "wandb": None,
            },
        )
        self.assertFalse(finalized)
        current = self.repo.store.item(item_id)
        self.assertEqual(current["state"], "force_killing")
        self.assertEqual(current["state_detail"], "operator force kill")

    @unittest.skipIf(os.name != "posix", "cooperative subprocess yield is POSIX-specific")
    def test_manual_preemption_requeues_without_a_reservation_and_obeys_priority(self) -> None:
        gpu = self.gpu()
        update_gpu_allowlist(
            self.repo.store,
            "set",
            ["0"],
            snapshots=[gpu],
        )
        low = add_experiment(
            self.repo.store,
            "TST-007",
            priority=10,
            preemptible=True,
        )
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=100,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu],
        )
        scheduler.run_iteration(force_gpu_poll=True)
        self.assertEqual(self.repo.store.item(low)["state"], "running")
        high = add_experiment(self.repo.store, "TST-002", priority=20)

        request_id = request_preemption(
            self.repo.store,
            low,
            reason="stakeholder overnight run",
            actor="test:operator",
        )

        yielding = self.repo.store.item(low)
        self.assertEqual(yielding["state"], "yielding")
        request_path = (
            self.repo.state_dir
            / "attempts"
            / str(low)
            / "segments"
            / "1"
            / "yield"
            / "request.json"
        )
        request = json.loads(request_path.read_text(encoding="utf-8"))
        self.assertEqual(request["request_id"], request_id)
        self.assertEqual(request["request_kind"], "manual_preemption")
        self.assertEqual(request["note"], "stakeholder overnight run")
        self.assertEqual(list_reservations(self.repo.store), [])

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            if (
                self.repo.store.item(high)["state"] == "succeeded"
                and self.repo.store.item(low)["state"] == "succeeded"
            ):
                break
            time.sleep(0.03)

        self.assertEqual(self.repo.store.item(high)["state"], "succeeded")
        self.assertEqual(self.repo.store.item(low)["state"], "succeeded")
        self.assertEqual(self.repo.store.item(low)["segment"], 2)
        with self.repo.store.connect() as connection:
            starts = [
                int(row["queue_item_id"])
                for row in connection.execute(
                    "SELECT queue_item_id FROM events "
                    "WHERE event_type = 'EXPERIMENT_STARTING' ORDER BY id"
                )
            ]
            events = {
                str(row["event_type"])
                for row in connection.execute(
                    "SELECT event_type FROM events WHERE queue_item_id = ?", (low,)
                )
            }
        self.assertEqual(starts, [low, high, low])
        self.assertIn("MANUAL_PREEMPTION_REQUESTED", events)
        self.assertIn("EXPERIMENT_YIELDED_AND_REQUEUED", events)

    @unittest.skipIf(os.name != "posix", "cooperative subprocess yield is POSIX-specific")
    def test_yield_checkpoints_reserves_old_gpu_and_resumes_at_priority_front(self) -> None:
        gpu0 = self.gpu("0", "GPU-test-0000")
        gpu1 = self.gpu("1", "GPU-test-1111")
        update_gpu_allowlist(
            self.repo.store,
            "set",
            ["0", "1"],
            snapshots=[gpu0, gpu1],
        )
        item_id = add_experiment(
            self.repo.store,
            "TST-007",
            preemptible=True,
        )
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=100,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu0, gpu1],
        )
        scheduler.run_iteration(force_gpu_poll=True)
        self.assertEqual(self.repo.store.item(item_id)["state"], "running")
        reservation_id = request_gpu_reservation(
            self.repo.store,
            gpu0.uuid,
            duration_hours=24,
            note="Morgan — model sweep",
            actor="web:reservation",
            snapshots=[gpu0, gpu1],
        )

        deadline = time.monotonic() + 15
        observed_requeue = False
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=False)
            item = self.repo.store.item(item_id)
            if item["state"] == "queued" and item["segment"] == 2:
                observed_requeue = True
                break
            time.sleep(0.03)
        self.assertTrue(observed_requeue)
        reservation = next(
            row for row in list_reservations(self.repo.store) if row["id"] == reservation_id
        )
        self.assertEqual(reservation["status"], "active")
        self.assertIsNotNone(reservation["starts_at"])
        item = self.repo.store.item(item_id)
        self.assertEqual(item["continuation_step"], 5)
        self.assertEqual(item["continuation_wandb_id"], "fake-wandb-id")
        self.assertEqual(item["resume_front"], 1)
        self.assertEqual(item["state_detail"], "resume from verified step 5")
        with self.repo.store.connect() as connection:
            event = connection.execute(
                "SELECT payload_json FROM events WHERE queue_item_id = ? "
                "AND event_type = 'EXPERIMENT_YIELDED_AND_REQUEUED'",
                (item_id,),
            ).fetchone()
        legacy_payload = json.loads(event["payload_json"])
        self.assertEqual(legacy_payload["step"], 5)
        self.assertNotIn("progress", legacy_payload)
        self.assertNotIn("progress_text", legacy_payload)
        worktree = Path(str(item["worktree_path"]))
        git_ref = str(item["git_ref"])
        self.assertTrue(worktree.is_dir())
        self.assertIsNone(item["worktree_removed_at"])
        self.assertEqual(
            subprocess.check_output(
                ["git", "rev-parse", git_ref], cwd=self.repo.root, text=True
            ).strip(),
            item["git_commit"],
        )

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            if self.repo.store.item(item_id)["state"] == "succeeded":
                break
            time.sleep(0.03)
        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "succeeded")
        self.assertEqual(item["segment"], 2)
        self.assertEqual(item["assigned_gpu_uuid"], gpu1.uuid)
        self.assertIsNotNone(item["worktree_removed_at"])
        self.assertFalse(worktree.exists())

    @unittest.skipIf(os.name != "posix", "cooperative subprocess yield is POSIX-specific")
    def test_generic_zero_row_progress_yields_and_resumes(self) -> None:
        gpu0 = self.gpu("0", "GPU-test-0000")
        gpu1 = self.gpu("1", "GPU-test-1111")
        update_gpu_allowlist(
            self.repo.store,
            "set",
            ["0", "1"],
            snapshots=[gpu0, gpu1],
        )
        item_id = add_experiment(
            self.repo.store,
            "TST-009",
            preemptible=True,
        )
        scheduler = Scheduler(
            self.repo.store,
            poll_seconds=100,
            min_free_disk_gib=0,
            gpu_provider=lambda: [gpu0, gpu1],
        )
        scheduler.run_iteration(force_gpu_poll=True)
        self.assertEqual(self.repo.store.item(item_id)["state"], "running")
        request_gpu_reservation(
            self.repo.store,
            gpu0.uuid,
            duration_hours=24,
            note="Morgan — model sweep",
            actor="web:reservation",
            snapshots=[gpu0, gpu1],
        )

        console = io.StringIO()
        deadline = time.monotonic() + 15
        with contextlib.redirect_stdout(console):
            while time.monotonic() < deadline:
                scheduler.run_iteration(force_gpu_poll=False)
                item = self.repo.store.item(item_id)
                if item["state"] == "queued" and item["segment"] == 2:
                    break
                time.sleep(0.03)
        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "queued")
        self.assertEqual(item["segment"], 2)
        self.assertEqual(item["continuation_step"], 0)
        self.assertEqual(item["state_detail"], "resume from verified 0/12 settled_rows")
        self.assertIn(
            "yielded at 0/12 settled_rows and returned to the front of its priority band",
            console.getvalue(),
        )
        with self.repo.store.connect() as connection:
            event = connection.execute(
                "SELECT payload_json FROM events WHERE queue_item_id = ? "
                "AND event_type = 'EXPERIMENT_YIELDED_AND_REQUEUED'",
                (item_id,),
            ).fetchone()
        payload = json.loads(event["payload_json"])
        self.assertEqual(payload["step"], 0)
        self.assertEqual(
            payload["progress"],
            {"unit": "settled_rows", "completed": 0, "total": 12},
        )
        self.assertEqual(payload["progress_text"], "0/12 settled_rows")

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=True)
            if self.repo.store.item(item_id)["state"] == "succeeded":
                break
            time.sleep(0.03)
        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "succeeded")
        self.assertEqual(item["segment"], 2)
        self.assertEqual(item["assigned_gpu_uuid"], gpu1.uuid)
        marker = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertEqual(marker["continuation_checkpoint"], item["continuation_checkpoint"])

    def test_query_gpus_parses_inventory_and_processes(self) -> None:
        executable = self.repo.root / "fake-nvidia-smi"
        executable.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  --query-gpu=*) echo '0, GPU-a, Test A, 100000, 1000, 3' ;;\n"
            "  --query-compute-apps=*) echo 'GPU-a, 1234' ;;\n"
            "  *) exit 2 ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)

        snapshots = query_gpus(str(executable))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].uuid, "GPU-a")
        self.assertEqual(snapshots[0].compute_pids, (1234,))
        self.assertAlmostEqual(snapshots[0].free_memory_fraction, 0.99)


if __name__ == "__main__":
    unittest.main()
