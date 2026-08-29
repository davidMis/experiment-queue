"""Verify the durable executor launches structured argv without shell rewriting."""

from __future__ import annotations

from collections.abc import Callable
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

import experiment_queue.executor as executor_module
from experiment_queue.executor import (
    ExecutorError,
    ExecutorEvidencePublicationError,
    ExecutorEvidencePublicationUncertainError,
    ExecutorLaunchReceipt,
    ExecutorReceipt,
    confirm_immutable_evidence_for_read,
    execute_payload,
    main,
)
from experiment_queue.serialization import canonical_json_bytes


def _payload(tmp_path: Path, **changes: object) -> tuple[Path, dict[str, object]]:
    worktree = tmp_path / "worktree"
    control = tmp_path / "state" / "attempts" / "1" / "segment-1"
    worktree.mkdir(parents=True)
    control.mkdir(parents=True)
    document: dict[str, object] = {
        "schema_version": 1,
        "queue_item_id": 1,
        "project_id": 2,
        "project_revision_id": 3,
        "project_key": "executor-fixture",
        "project_revision": "executor-fixture:r1",
        "experiment_id": "EXEC-001",
        "attempt": 1,
        "resolved_spec_sha256": "a" * 64,
        "admission_kind": "ExperimentCard/v1",
        "segment": 1,
        "git_commit": "b" * 40,
        "worktree": str(worktree),
        "cwd": str(worktree),
        "command_kind": "argv",
        "command": [sys.executable, "-c", "raise SystemExit(0)"],
        "control_root": str(tmp_path / "state"),
        "receipt_path": str(control / "exit.json"),
    }
    document.update(changes)
    path = control / "executor.json"
    path.write_bytes(canonical_json_bytes(document) + b"\n")
    return path, document


def test_structured_argv_preserves_literal_arguments_and_receipt_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "worktree" / "marker.json"
    shell_side_effect = tmp_path / "must-not-exist"
    literal = f"; touch {shell_side_effect}"
    program = (
        "import json, os, pathlib, sys; "
        "pathlib.Path(sys.argv[1]).write_text(json.dumps({"
        "'literal': sys.argv[2], "
        "'item': os.environ.get('EXPERIMENT_QUEUE_ITEM_ID')}))"
    )
    path, document = _payload(
        tmp_path,
        command=[sys.executable, "-c", program, str(marker), literal],
    )
    monkeypatch.setenv("EXPERIMENT_QUEUE_ITEM_ID", "1")
    monkeypatch.setenv("EXPERIMENT_QUEUE_GPU_UUID", "GPU-fixture")

    assert execute_payload(path) == 0

    assert json.loads(marker.read_text()) == {"literal": literal, "item": "1"}
    assert not shell_side_effect.exists()
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["schema_version"] == 3
    assert receipt["queue_item_id"] == 1
    assert receipt["project_id"] == 2
    assert receipt["project_revision_id"] == 3
    assert receipt["project_revision"] == "executor-fixture:r1"
    assert receipt["experiment_id"] == "EXEC-001"
    assert receipt["resolved_spec_sha256"] == "a" * 64
    assert receipt["admission_kind"] == "ExperimentCard/v1"
    assert receipt["command_kind"] == "argv"
    assert receipt["gpu_uuid"] == "GPU-fixture"
    assert receipt["return_code"] == 0
    assert receipt["signals_received"] == []
    assert list(Path(document["receipt_path"]).parent.glob(".exit.json.*.tmp")) == []


def test_legacy_shell_is_an_explicit_compatibility_discriminator(tmp_path: Path) -> None:
    marker = tmp_path / "worktree" / "legacy.txt"
    path, document = _payload(
        tmp_path,
        admission_kind="LegacyMarkdownCard/v0",
        resolved_spec_sha256=None,
        command_kind="legacy-shell",
        command=f"printf legacy > {marker}",
    )

    assert execute_payload(path) == 0
    assert marker.read_text() == "legacy"
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["command_kind"] == "legacy-shell"
    assert receipt["resolved_spec_sha256"] is None


def test_launch_failure_is_a_durable_terminal_receipt(tmp_path: Path) -> None:
    path, document = _payload(
        tmp_path,
        command=[str(tmp_path / "missing-executable")],
    )
    assert execute_payload(path) == 127
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["return_code"] == 127
    assert receipt["signals_received"][0].startswith("launch_error:")


def test_scientific_command_never_starts_before_launch_receipt_publication(
    tmp_path: Path,
) -> None:
    """Stale sidecar evidence blocks the project-controlled Popen entirely."""

    marker = tmp_path / "worktree" / "must-not-start.txt"
    path, document = _payload(
        tmp_path,
        command=[
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
        ],
    )
    launch_receipt = path.with_name("launch.json")
    launch_receipt.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ExecutorError, match="launch receipt already exists"):
        execute_payload(path)

    assert not marker.exists()
    assert not Path(document["receipt_path"]).exists()


def test_exit_receipt_publication_never_replaces_prior_exact_evidence(
    tmp_path: Path,
) -> None:
    """A retry refuses the final name without changing its bytes or inode."""

    path = tmp_path / "exit.json"
    executor_module._atomic_write_json(path, {"result": "first"})  # noqa: SLF001
    before = (path.read_bytes(), path.stat().st_ino)

    with pytest.raises(ExecutorError, match="exit receipt already exists"):
        executor_module._atomic_write_json(  # noqa: SLF001
            path,
            {"result": "replacement"},
        )

    assert (path.read_bytes(), path.stat().st_ino) == before
    assert list(tmp_path.glob(".exit.json.*.tmp")) == []


def test_pre_link_failure_retains_durable_staging_and_allows_safe_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete pre-link receipt survives while a later retry publishes."""

    path = tmp_path / "exit.json"
    document = {"result": "complete"}
    original_link = executor_module.os.link

    def fail_link(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected hard-link failure")

    monkeypatch.setattr(executor_module.os, "link", fail_link)
    with pytest.raises(
        ExecutorEvidencePublicationError,
        match="complete durable staging evidence is preserved",
    ) as raised:
        executor_module._atomic_write_json(path, document)  # noqa: SLF001

    staging = raised.value.staging_path
    assert staging is not None and staging.is_file()
    assert raised.value.final_visible is False
    assert raised.value.final_durable is False
    assert not path.exists()

    monkeypatch.setattr(executor_module.os, "link", original_link)
    executor_module._atomic_write_json(path, document)  # noqa: SLF001
    assert json.loads(path.read_text()) == document
    assert staging.is_file()
    assert not os.path.samefile(staging, path)


def test_staging_directory_fsync_failure_retains_complete_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain staging-name commit is not erased during error cleanup."""

    path = tmp_path / "launch.json"

    def fail_staging_fsync(_path: Path) -> None:
        raise OSError("injected staging directory fsync failure")

    monkeypatch.setattr(executor_module, "_fsync_directory", fail_staging_fsync)
    with pytest.raises(
        ExecutorEvidencePublicationError,
        match="complete staging evidence is preserved",
    ) as raised:
        executor_module._atomic_create_json(path, {"pid": 17})  # noqa: SLF001

    assert not path.exists()
    assert raised.value.staging_path is not None
    assert raised.value.staging_path.is_file()
    assert json.loads(raised.value.staging_path.read_text()) == {"pid": 17}


@pytest.mark.parametrize(
    ("filename", "publisher", "existing_message"),
    [
        ("launch.json", executor_module._atomic_create_json, "launch receipt"),  # noqa: SLF001
        ("exit.json", executor_module._atomic_write_json, "exit receipt"),  # noqa: SLF001
    ],
)
def test_post_link_fsync_failure_preserves_both_names_and_refuses_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    publisher: Callable[[Path, dict[str, object]], None],
    existing_message: str,
) -> None:
    """Visible-but-uncertain evidence keeps its durable fallback hard link."""

    path = tmp_path / filename
    original_fsync_directory = executor_module._fsync_directory  # noqa: SLF001
    fsync_calls = 0

    def fail_final_link_fsync(directory: Path) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("injected final-link directory fsync failure")
        original_fsync_directory(directory)

    monkeypatch.setattr(
        executor_module,
        "_fsync_directory",
        fail_final_link_fsync,
    )
    with pytest.raises(
        ExecutorEvidencePublicationUncertainError,
        match="preserve both.*same complete evidence",
    ) as raised:
        publisher(path, {"pid": 19})

    staging = raised.value.staging_path
    assert staging is not None and staging.is_file()
    assert path.is_file()
    assert os.path.samefile(staging, path)
    assert raised.value.final_visible is True
    assert raised.value.final_durable is False

    with pytest.raises(ExecutorError, match=f"{existing_message} already exists"):
        publisher(path, {"pid": 19})
    assert staging.is_file()
    assert os.path.samefile(staging, path)

    confirm_immutable_evidence_for_read(path)
    assert path.is_file()
    assert not staging.exists()


def test_evidence_confirmation_rejects_staging_only_and_changed_companion(
    tmp_path: Path,
) -> None:
    """Recovery never promotes staging or trusts a different companion inode."""

    final = tmp_path / "exit.json"
    staging = tmp_path / ".exit.json.fixture.tmp"
    staging.write_text('{"status":"staging-only"}\n', encoding="utf-8")
    with pytest.raises(ExecutorError, match="final.*absent or unsafe"):
        confirm_immutable_evidence_for_read(final)
    assert staging.is_file()
    assert not final.exists()

    final.write_text('{"status":"final"}\n', encoding="utf-8")
    with pytest.raises(ExecutorError, match="not a regular hard link"):
        confirm_immutable_evidence_for_read(final)
    assert final.is_file()
    assert staging.is_file()


def test_launch_receipt_reader_confirms_same_inode_staging_residue(
    tmp_path: Path,
) -> None:
    """Launch handoff fsync-confirms an uncertain same-inode final before use."""

    path, document = _payload(tmp_path)
    assert execute_payload(path) == 0
    launch = path.with_name("launch.json")
    staging = launch.with_name(f".{launch.name}.fixture.tmp")
    os.link(launch, staging)

    receipt = ExecutorLaunchReceipt.inspect(launch)

    assert receipt.queue_item_id == document["queue_item_id"]
    assert launch.is_file()
    assert not staging.exists()


def test_durable_exit_receipt_survives_staging_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup is non-authoritative once scheduler-consumable evidence is durable."""

    path, document = _payload(tmp_path)
    original_unlink = Path.unlink

    def fail_exit_staging_unlink(target: Path, *args: object, **kwargs: object) -> None:
        if target.name.startswith(".exit.json."):
            raise OSError("injected staging cleanup failure")
        original_unlink(target, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_exit_staging_unlink)
    assert execute_payload(path) == 0

    receipt_path = Path(document["receipt_path"])
    staging_links = list(receipt_path.parent.glob(".exit.json.*.tmp"))
    assert len(staging_links) == 1
    assert os.path.samefile(staging_links[0], receipt_path)
    assert _read_receipt(document).return_code == 0


def test_cleanup_directory_fsync_failure_keeps_final_receipt_authoritative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed staging-removal fsync cannot revoke a durable final hard link."""

    path = tmp_path / "exit.json"
    original_fsync_directory = executor_module._fsync_directory  # noqa: SLF001
    fsync_calls = 0

    def fail_cleanup_fsync(directory: Path) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 3:
            raise OSError("injected staging-cleanup directory fsync failure")
        original_fsync_directory(directory)

    monkeypatch.setattr(executor_module, "_fsync_directory", fail_cleanup_fsync)
    executor_module._atomic_write_json(path, {"result": "durable"})  # noqa: SLF001

    assert json.loads(path.read_text()) == {"result": "durable"}
    assert list(tmp_path.glob(".exit.json.*.tmp")) == []


def test_signal_after_launch_publication_before_popen_is_deferred_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable launch-window signal is delivered once after child assignment."""

    path, document = _payload(tmp_path)
    original_publish = executor_module._publish_launch_receipt  # noqa: SLF001
    broadcasts: list[tuple[int, int]] = []

    def publish_then_signal(**kwargs: object) -> None:
        original_publish(**kwargs)  # type: ignore[arg-type]
        os.kill(os.getpid(), signal.SIGTERM)

    class FakeChild:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.return_code: int | None = None

        def poll(self) -> int | None:
            return self.return_code

        def send_signal(self, signum: int) -> None:
            self.return_code = -signum

        def wait(self) -> int:
            return 0 if self.return_code is None else self.return_code

    monkeypatch.setattr(executor_module, "_publish_launch_receipt", publish_then_signal)
    monkeypatch.setattr(executor_module.subprocess, "Popen", FakeChild)
    monkeypatch.setattr(
        executor_module.os,
        "killpg",
        lambda pgid, signum: broadcasts.append((pgid, signum)),
    )

    assert execute_payload(path) == 0
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["signals_received"] == ["SIGTERM"]
    assert broadcasts == [(os.getpgrp(), signal.SIGTERM)]


def test_graceful_executor_signal_reaches_scientific_child_once(
    tmp_path: Path,
) -> None:
    """Leader-only scheduler signaling cannot double-deliver to the shared group."""

    ready = tmp_path / "worktree" / "ready"
    deliveries = tmp_path / "worktree" / "deliveries"
    program = (
        "import pathlib, signal, sys, time; "
        f"ready=pathlib.Path({str(ready)!r}); "
        f"deliveries=pathlib.Path({str(deliveries)!r}); "
        "handler=lambda _s,_f: (deliveries.write_text(str(int(deliveries.read_text())+1) if deliveries.exists() else '1'), time.sleep(0.1), sys.exit(0)); "
        "signal.signal(signal.SIGTERM, handler); ready.write_text('ready'); "
        "time.sleep(30)"
    )
    path, document = _payload(
        tmp_path,
        command=[sys.executable, "-c", program],
    )
    executor = subprocess.Popen(
        [sys.executable, "-m", "experiment_queue.executor", str(path)],
        start_new_session=True,
    )
    try:
        for _ in range(500):
            if ready.exists():
                break
            if executor.poll() is not None:
                pytest.fail(f"executor exited before child readiness: {executor.returncode}")
            time.sleep(0.01)
        assert ready.exists()
        os.kill(executor.pid, signal.SIGTERM)
        assert executor.wait(timeout=5) == 0
    finally:
        if executor.poll() is None:
            os.killpg(executor.pid, signal.SIGKILL)
            executor.wait(timeout=5)

    assert deliveries.read_text() == "1"
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["signals_received"] == ["SIGTERM"]


def test_executor_coalesces_repeated_graceful_signals_per_segment(
    tmp_path: Path,
) -> None:
    """At-least-once senders cannot rebroadcast one graceful stage repeatedly."""

    ready = tmp_path / "worktree" / "ready"
    interrupts = tmp_path / "worktree" / "interrupts"
    terminations = tmp_path / "worktree" / "terminations"
    program = f"""
from pathlib import Path
import signal
import time
ready = Path({str(ready)!r})
interrupts = Path({str(interrupts)!r})
terminations = Path({str(terminations)!r})
def record(path):
    count = int(path.read_text()) + 1 if path.exists() else 1
    path.write_text(str(count))
def interrupt(_signum, _frame):
    record(interrupts)
def terminate(_signum, _frame):
    record(terminations)
    raise SystemExit(0)
signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, terminate)
ready.write_text("ready")
while True:
    time.sleep(0.01)
"""
    path, document = _payload(
        tmp_path,
        command=[sys.executable, "-c", program],
    )
    executor = subprocess.Popen(
        [sys.executable, "-m", "experiment_queue.executor", str(path)],
        start_new_session=True,
    )
    try:
        for _ in range(500):
            if ready.exists():
                break
            if executor.poll() is not None:
                pytest.fail(f"executor exited before child readiness: {executor.returncode}")
            time.sleep(0.01)
        assert ready.exists()
        for _ in range(3):
            os.kill(executor.pid, signal.SIGINT)
            time.sleep(0.02)
        for _ in range(200):
            if interrupts.exists():
                break
            time.sleep(0.01)
        assert interrupts.read_text() == "1"
        os.kill(executor.pid, signal.SIGTERM)
        assert executor.wait(timeout=5) == 0
    finally:
        if executor.poll() is None:
            os.killpg(executor.pid, signal.SIGKILL)
            executor.wait(timeout=5)

    assert interrupts.read_text() == "1"
    assert terminations.read_text() == "1"
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["signals_received"] == ["SIGINT", "SIGTERM"]


def test_process_group_scan_skips_unreadable_unrelated_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Host-wide unreadable /proc entries cannot block queue-owned group drain."""

    proc = tmp_path / "proc"
    bad_stat = proc / "12" / "stat"
    member_stat = proc / "13" / "stat"
    bad_stat.parent.mkdir(parents=True)
    member_stat.parent.mkdir()
    bad_stat.write_text("unrelated", encoding="ascii")
    member_stat.write_text("13 (child) S 1 77 0 0\n", encoding="ascii")
    original_read_text = Path.read_text

    def read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path == bad_stat:
            raise PermissionError("fixture unrelated process")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    assert executor_module._scientific_process_group_has_members(  # noqa: SLF001
        pgid=77,
        executor_pid=11,
        proc_root=proc,
    )
    member_stat.unlink()
    assert not executor_module._scientific_process_group_has_members(  # noqa: SLF001
        pgid=77,
        executor_pid=11,
        proc_root=proc,
    )


@pytest.mark.skipif(
    not Path("/proc").is_dir(),
    reason="terminal process-group drain is a Linux production invariant",
)
def test_executor_waits_for_background_descendant_and_signals_its_group(
    tmp_path: Path,
) -> None:
    """A child exit cannot publish terminal evidence while its descendant lives."""

    ready = tmp_path / "worktree" / "grandchild-ready"
    deliveries = tmp_path / "worktree" / "grandchild-deliveries"
    grandchild = f"""
from pathlib import Path
import signal
import sys
import time
ready = Path({str(ready)!r})
deliveries = Path({str(deliveries)!r})
def stop(_signum, _frame):
    count = int(deliveries.read_text()) + 1 if deliveries.exists() else 1
    deliveries.write_text(str(count))
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
ready.write_text("ready")
while True:
    time.sleep(0.05)
"""
    parent = (
        "import pathlib, subprocess, sys, time; "
        f"ready=pathlib.Path({str(ready)!r}); "
        f"subprocess.Popen([sys.executable, '-c', {grandchild!r}]); "
        "[(time.sleep(0.01)) for _ in range(500) if not ready.exists()]"
    )
    path, document = _payload(
        tmp_path,
        command=[sys.executable, "-c", parent],
    )
    executor = subprocess.Popen(
        [sys.executable, "-m", "experiment_queue.executor", str(path)],
        start_new_session=True,
    )
    try:
        for _ in range(500):
            if ready.exists():
                break
            if executor.poll() is not None:
                pytest.fail(f"executor exited before descendant readiness: {executor.returncode}")
            time.sleep(0.01)
        assert ready.exists()
        time.sleep(0.1)
        assert executor.poll() is None
        assert not Path(document["receipt_path"]).exists()

        os.kill(executor.pid, signal.SIGTERM)
        assert executor.wait(timeout=5) == 0
    finally:
        if executor.poll() is None:
            os.killpg(executor.pid, signal.SIGKILL)
            executor.wait(timeout=5)

    assert deliveries.read_text() == "1"
    receipt = json.loads(Path(document["receipt_path"]).read_text())
    assert receipt["signals_received"] == ["SIGTERM"]


def _read_receipt(document: dict[str, object]) -> ExecutorReceipt:
    receipt_document = json.loads(Path(document["receipt_path"]).read_text())
    return ExecutorReceipt.read(
        Path(document["receipt_path"]),
        queue_item_id=int(document["queue_item_id"]),
        project_id=int(document["project_id"]),
        project_revision_id=int(document["project_revision_id"]),
        project_key=str(document["project_key"]),
        project_revision=str(document["project_revision"]),
        experiment_id=str(document["experiment_id"]),
        attempt=int(document["attempt"]),
        resolved_spec_sha256=document["resolved_spec_sha256"],  # type: ignore[arg-type]
        admission_kind=str(document["admission_kind"]),
        segment=int(document["segment"]),
        git_commit=str(document["git_commit"]),
        worktree=Path(document["worktree"]),
        command_kind=str(document["command_kind"]),
        command_sha256=str(receipt_document["command_sha256"]),
        gpu_uuid=receipt_document["gpu_uuid"],
    )


def test_receipt_reader_authenticates_all_scheduler_identity(tmp_path: Path) -> None:
    path, document = _payload(tmp_path)
    assert execute_payload(path) == 0

    receipt = _read_receipt(document)

    assert receipt.project_key == "executor-fixture"
    assert receipt.project_revision == "executor-fixture:r1"
    assert receipt.return_code == 0
    assert receipt.finished_at >= receipt.started_at
    with pytest.raises(TypeError):
        ExecutorReceipt()  # type: ignore[call-arg]


def test_receipt_reader_rejects_identity_drift_and_unknown_evidence(
    tmp_path: Path,
) -> None:
    path, document = _payload(tmp_path)
    assert execute_payload(path) == 0
    receipt_path = Path(document["receipt_path"])
    receipt_document = json.loads(receipt_path.read_text())
    receipt_document["project_id"] = 99
    receipt_path.write_text(json.dumps(receipt_document), encoding="utf-8")
    with pytest.raises(ExecutorError, match="project_id 99 does not match"):
        _read_receipt(document)

    receipt_document["project_id"] = 2
    receipt_document["unexpected"] = "evidence"
    receipt_path.write_text(json.dumps(receipt_document), encoding="utf-8")
    with pytest.raises(ExecutorError, match="unknown fields"):
        _read_receipt(document)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"schema_version": 2}, "schema_version"),
        ({"resolved_spec_sha256": None}, "requires resolved_spec_sha256"),
        (
            {
                "admission_kind": "LegacyMarkdownCard/v0",
                "command_kind": "legacy-shell",
                "command": "true",
            },
            "must use null",
        ),
        ({"git_commit": "short"}, "full lowercase Git object"),
        ({"command_kind": "shell"}, "command_kind"),
        ({"command": []}, "non-empty JSON array"),
    ],
)
def test_payload_identity_and_command_shape_fail_closed(
    tmp_path: Path,
    changes: dict[str, object],
    message: str,
) -> None:
    path, _document = _payload(tmp_path, **changes)
    with pytest.raises(ExecutorError, match=message):
        execute_payload(path)


def test_cwd_and_control_paths_cannot_escape_authorized_roots(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    path, _document = _payload(tmp_path, cwd=str(outside))
    with pytest.raises(ExecutorError, match="outside admitted worktree"):
        execute_payload(path)

    # Build a fresh fixture because the helper intentionally creates its roots.
    other = tmp_path / "other"
    path, _document = _payload(
        other,
        receipt_path=str(outside / "exit.json"),
    )
    with pytest.raises(ExecutorError, match="outside scheduler control root"):
        execute_payload(path)


def test_payload_must_itself_be_scheduler_owned_and_not_a_symlink(tmp_path: Path) -> None:
    path, _document = _payload(tmp_path)
    outside = tmp_path / "outside-payload.json"
    outside.write_bytes(path.read_bytes())
    with pytest.raises(ExecutorError, match="outside scheduler control root"):
        execute_payload(outside)

    link = path.with_name("executor-link.json")
    link.symlink_to(path)
    with pytest.raises(ExecutorError, match="must not be a symlink"):
        execute_payload(link)


def test_payload_parser_rejects_duplicate_keys_and_unknown_fields(tmp_path: Path) -> None:
    path, document = _payload(tmp_path)
    path.write_text(
        json.dumps(document).replace(
            '"schema_version": 1',
            '"schema_version": 1, "schema_version": 1',
            1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ExecutorError, match="repeats JSON key"):
        execute_payload(path)

    path, document = _payload(tmp_path / "unknown")
    document["unexpected"] = True
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ExecutorError, match="unknown fields"):
        execute_payload(path)


def test_internal_cli_requires_one_absolute_payload_path(tmp_path: Path) -> None:
    assert main([]) == 2
    assert main(["relative.json"]) == 2
    path, _document = _payload(tmp_path)
    assert main([str(path)]) == 0
