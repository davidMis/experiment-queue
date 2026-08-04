"""Verify explicit admission, GPU controls, dispatch, and queue termination."""

from __future__ import annotations

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


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def time_to_datetime(value: str, *, seconds: int = 0):
    return datetime.fromisoformat(value) + timedelta(seconds=seconds)

from helmholtz_shared.experiment_queue import (  # noqa: E402
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
    remove_item,
    request_termination,
    request_gpu_reservation,
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
            "import time\n"
            "from pathlib import Path\n"
            "parser = argparse.ArgumentParser()\n"
            "parser.add_argument('--name')\n"
            "parser.add_argument('--require-clean', action='store_true')\n"
            "parser.add_argument('--remote')\n"
            "parser.add_argument('--sleep', type=float, default=0.0)\n"
            "parser.add_argument('--exit-code', type=int, default=0)\n"
            "parser.add_argument('--yield-aware', action='store_true')\n"
            "args = parser.parse_args()\n"
            "marker = Path(os.environ['QUEUE_TEST_MARKER'])\n"
            "marker.write_text(json.dumps({'gpu': os.environ.get('CUDA_VISIBLE_DEVICES')}))\n"
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
            "        run_dir = Path.cwd() / 'outputs' / 'experiments' / 'fake-run'\n"
            "        checkpoint_dir = run_dir / 'training' / 'checkpoints'\n"
            "        checkpoint_dir.mkdir(parents=True, exist_ok=True)\n"
            "        checkpoint = checkpoint_dir / 'preempt_step_00000005.msgpack'\n"
            "        checkpoint.write_bytes(b'fake complete train state')\n"
            "        metadata = checkpoint.with_suffix('.json')\n"
            "        metadata.write_text(json.dumps({'step': 5}))\n"
            "        import hashlib\n"
            "        receipt = {'schema_version': 1, 'status': 'ready', 'request_id': request['request_id'], 'queue_item_id': request['queue_item_id'], 'step': 5, 'checkpoint': str(checkpoint), 'checkpoint_metadata': str(metadata), 'checkpoint_bytes': checkpoint.stat().st_size, 'checkpoint_sha256': hashlib.sha256(checkpoint.read_bytes()).hexdigest(), 'wandb': {'id': 'fake-wandb-id'}}\n"
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
        self.add_real_runner_card()
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
    ) -> None:
        card = self.root / "docs" / "experiments" / f"{experiment_id}.md"
        card.write_text(
            f"# {experiment_id}: Queue Test\n\n"
            "## Exact Manual Command On Mutton2\n\n"
            "```bash\n"
            f"python3 scripts/run_experiment.py --name {experiment_id.lower()} "
            f"--require-clean --remote mutton2 --sleep {sleep} --exit-code {exit_code} "
            f"{'--yield-aware' if yield_aware else ''}\n"
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

    def test_card_command_is_read_only_after_explicit_selection(self) -> None:
        card = read_card_command(self.repo.root, "TST-001")

        self.assertEqual(card.experiment_id, "TST-001")
        self.assertIn("scripts/run_experiment.py", card.command_text)
        self.assertEqual(self.repo.store.list_items(), [])

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

            self.assertEqual(migrated.get_meta("schema_version"), "2")
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
            self.assertIn("gpu_reservations", tables)

    def test_current_queue_candidate_cards_have_exact_runner_commands(self) -> None:
        for experiment_id in (
            "WCG-008",
            "WCG-017",
            "WCG-019",
            "WCG-020",
            "WCG-021",
            "WCG-022",
            "WCG-023",
            "WCG-024",
        ):
            with self.subTest(experiment_id=experiment_id):
                card = read_card_command(REPO_ROOT, experiment_id)
                self.assertIn("scripts/run_experiment.py", card.command_text)
                self.assertIn("--require-clean", card.command_text)

    def test_add_does_not_scan_other_cards_or_status(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001", priority=7)

        items = self.repo.store.list_items()
        self.assertEqual([item["experiment_id"] for item in items], ["TST-001"])
        self.assertEqual(items[0]["priority"], 7)
        self.assertEqual(items[0]["id"], item_id)
        self.assertNotIn("TST-002", format_status(self.repo.store))
        self.assertNotIn("TST-999", format_status(self.repo.store))

    def test_remove_preserves_history_and_explicit_readd_creates_new_membership(self) -> None:
        first = add_experiment(self.repo.store, "TST-001")
        remove_item(self.repo.store, first, "operator changed ordering")
        second = add_experiment(self.repo.store, "TST-001")

        self.assertNotEqual(first, second)
        items = self.repo.store.list_items()
        self.assertEqual([item["state"] for item in items], ["removed", "queued"])
        self.assertEqual([item["attempt"] for item in items], [1, 2])

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

    def test_priority_changes_only_pending_items(self) -> None:
        item_id = add_experiment(self.repo.store, "TST-001")
        set_priority(self.repo.store, item_id, 42)
        self.assertEqual(self.repo.store.item(item_id)["priority"], 42)
        with self.repo.store.connect() as connection:
            connection.execute("UPDATE queue_items SET state = 'running' WHERE id = ?", (item_id,))
        with self.assertRaisesRegex(QueueError, "only pending"):
            set_priority(self.repo.store, item_id, 1)

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
    def test_repository_drift_pauses_future_dispatch_without_killing_job(self) -> None:
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
        self.assertEqual(self.repo.store.item(item_id)["state"], "running")

        (self.repo.root / "STATUS.md").write_text("# changed during run\n", encoding="utf-8")
        scheduler.run_iteration(force_gpu_poll=True)

        item = self.repo.store.item(item_id)
        self.assertEqual(item["state"], "running")
        self.assertTrue(item["repo_drift_detected"])
        self.assertEqual(self.repo.store.get_meta("dispatch_paused"), "1")

        request_termination(
            self.repo.store, item_id, reason="clean up repo-drift test", force=True
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            scheduler.run_iteration(force_gpu_poll=False)
            if self.repo.store.item(item_id)["state"] == "force_killed":
                break
            time.sleep(0.05)
        self.assertEqual(self.repo.store.item(item_id)["state"], "force_killed")

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

    @unittest.skipIf(os.name != "posix", "cooperative subprocess yield is POSIX-specific")
    def test_yield_checkpoints_reserves_old_gpu_and_resumes_at_queue_front(self) -> None:
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
