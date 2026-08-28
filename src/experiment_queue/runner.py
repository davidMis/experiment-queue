# This module implements a lightweight experiment runner for remote GPU work.
# It creates a timestamped run directory, records provenance in a manifest,
# streams command output to both the terminal and log files, and prints a
# ready-to-use rsync command when a remote host alias is supplied.

from __future__ import annotations

import argparse
import codecs
import contextlib
import json
import os
import platform
import re
import select
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterable, Sequence

from experiment_queue.protocols import (
    RUNNER_MANIFEST_V1,
    RUNNER_RECEIPT_V1,
    ProtocolIdentityError,
    ProtocolVersion,
)


DEFAULT_OUTPUT_ROOT = Path("outputs/experiments")
MANIFEST_NAME = "manifest.json"
RUNNER_RECEIPT_NAME = "runner-receipt.json"
DEFAULT_USE_PTY = True
YIELD_EXIT_CODE = 75
CONTINUATION_RUN_DIR_ENV = "EXPERIMENT_QUEUE_CONTINUATION_RUN_DIR"
YIELD_RECEIPT_ENV = "EXPERIMENT_QUEUE_YIELD_RECEIPT_PATH"
RUNNER_RECEIPT_ENV = "EXPERIMENT_QUEUE_RUNNER_RECEIPT_PATH"


class ExperimentError(RuntimeError):
    """Raised when the runner cannot prepare or launch an experiment."""


@dataclass(frozen=True)
class ExperimentRequest:
    """User-facing request for a single experiment run."""

    name: str
    command: Sequence[str]
    output_root: Path = DEFAULT_OUTPUT_ROOT
    config_paths: Sequence[Path] = ()
    notes: str | None = None
    require_clean: bool = False
    remote: str | None = None
    local_output_root: Path | None = None
    cwd: Path = field(default_factory=Path.cwd)
    use_pty: bool = DEFAULT_USE_PTY


@dataclass(frozen=True)
class ExperimentResult:
    """Summary of a completed experiment process."""

    run_id: str
    run_dir: Path
    manifest_path: Path
    receipt_path: Path
    return_code: int
    rsync_pull_command: str | None


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser used by the thin wrapper in scripts/."""

    parser = argparse.ArgumentParser(
        description=(
            "Run an experiment command in a timestamped output directory while "
            "recording logs, copied configs, git metadata, and a manifest."
        )
    )
    parser.add_argument(
        "--name",
        default="run",
        help=(
            "Short human-readable run name used in the output directory. "
            "It is slugified and combined with a UTC timestamp and git SHA."
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=(
            "Directory where experiment run directories are created. "
            "Default: outputs/experiments."
        ),
    )
    parser.add_argument(
        "--config",
        action="append",
        type=Path,
        default=[],
        dest="config_paths",
        help=(
            "Config file or directory to copy into the run directory before "
            "launching the command. Repeat this option for multiple configs."
        ),
    )
    parser.add_argument(
        "--notes",
        help=(
            "Short free-form note recorded in the manifest, such as the "
            "hypothesis or comparison this run is meant to test."
        ),
    )
    parser.add_argument(
        "--require-clean",
        action="store_true",
        help=(
            "Refuse to start if the git worktree has uncommitted changes. "
            "Use this for expensive runs that must be tied to an exact commit."
        ),
    )
    parser.add_argument(
        "--remote",
        help=(
            "Optional SSH host alias, such as mutton2 or user@gpu-host. When "
            "provided, the runner prints an rsync command for pulling outputs "
            "from this machine back to the local workstation."
        ),
    )
    parser.add_argument(
        "--local-output-root",
        type=Path,
        help=(
            "Local destination root used in the printed rsync pull command. "
            "Defaults to the same value as --output-root."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Show a Python traceback for runner setup errors. The default is "
            "to print concise, actionable error messages."
        ),
    )
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument(
        "--pty",
        dest="use_pty",
        action="store_true",
        default=DEFAULT_USE_PTY,
        help=(
            "Run the child command in a pseudo-terminal so nested progress bars "
            "and other TTY-aware output render interactively. This is the "
            "default. stdout and stderr are merged into stdout.log in this mode."
        ),
    )
    progress_group.add_argument(
        "--no-pty",
        dest="use_pty",
        action="store_false",
        help=(
            "Run the child command with separate stdout/stderr pipes instead "
            "of a pseudo-terminal. Use this for plain line-oriented logs or "
            "when stderr separation matters."
        ),
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help=(
            "Experiment command to run. Place it after --, for example: "
            "-- python scripts/benchmark_flowers.py --preset tiny --warmup 0 "
            "--iterations 1."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    command = _normalize_remainder(args.command)
    if not command:
        parser.error(
            "the experiment command is required; put it after --, for example: "
            "-- python scripts/benchmark_flowers.py --preset tiny --warmup 0 "
            "--iterations 1"
        )

    request = ExperimentRequest(
        name=args.name,
        command=command,
        output_root=args.output_root,
        config_paths=args.config_paths,
        notes=args.notes,
        require_clean=args.require_clean,
        remote=args.remote,
        local_output_root=args.local_output_root,
        cwd=Path.cwd(),
        use_pty=args.use_pty,
    )

    try:
        result = run_experiment(request)
    except ExperimentError as exc:
        if args.debug:
            raise
        print(f"experiment runner error: {exc}", file=sys.stderr)
        return 2

    print(f"run directory: {result.run_dir}")
    print(f"manifest: {result.manifest_path}")
    if result.rsync_pull_command:
        print(f"pull outputs with: {result.rsync_pull_command}")
    if result.return_code != 0:
        print(
            f"experiment command failed with exit code {result.return_code}; "
            f"inspect logs in {result.run_dir}",
            file=sys.stderr,
        )
    return _runner_process_return_code(result.return_code)


def _runner_process_return_code(command_return_code: int) -> int:
    """Map a signal-terminated child to the runner CLI's observable exit code."""

    if command_return_code < 0:
        return 128 + abs(command_return_code)
    return command_return_code


def _load_continuation_manifest(
    request: ExperimentRequest,
    *,
    cwd: Path,
    git_context: dict[str, Any],
) -> tuple[str, Path, Path, dict[str, Any]] | None:
    """Load and strictly validate a scheduler-authorized yielded runner."""

    value = os.environ.get(CONTINUATION_RUN_DIR_ENV)
    if not value:
        return None
    run_dir = Path(value).resolve()
    if not run_dir.is_dir() or run_dir.is_symlink():
        raise ExperimentError(
            f"continuation runner directory is missing or not a regular directory: {run_dir}"
        )
    manifest_path = run_dir / MANIFEST_NAME
    def reject_duplicate_keys(
        pairs: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ExperimentError(
                    f"continuation manifest {manifest_path} repeats JSON key {key!r}"
                )
            result[key] = value
        return result

    def reject_nonfinite_constant(value: str) -> None:
        raise ExperimentError(
            f"continuation manifest {manifest_path} contains unsupported "
            f"JSON constant {value!r}"
        )

    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_nonfinite_constant,
        )
    except ExperimentError:
        raise
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
    ) as exc:
        raise ExperimentError(
            f"could not read continuation manifest {manifest_path}: {exc}"
        ) from exc
    if not isinstance(manifest, dict):
        raise ExperimentError(
            f"continuation manifest {manifest_path} must contain a JSON object"
        )
    if "apiVersion" in manifest or "kind" in manifest:
        try:
            manifest_version = ProtocolVersion.from_document(manifest)
        except ProtocolIdentityError as exc:
            raise ExperimentError(
                f"continuation manifest {manifest_path} has invalid protocol identity: {exc}"
            ) from exc
        if manifest_version != RUNNER_MANIFEST_V1:
            raise ExperimentError(
                f"continuation manifest {manifest_path} uses unsupported "
                f"{manifest_version.kind.value}/v{manifest_version.major}"
            )
        if "schema_version" in manifest and (
            type(manifest["schema_version"]) is not int
            or manifest["schema_version"] != 1
        ):
            raise ExperimentError(
                f"continuation manifest {manifest_path} has invalid retained "
                f"schema_version {manifest['schema_version']!r}; expected integer 1"
            )
    elif (
        type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 1
    ):
        raise ExperimentError(
            f"continuation manifest {manifest_path} has no supported RunnerManifest/v1 "
            "identity or legacy schema_version 1"
        )
    run = manifest.get("run", {})
    command = manifest.get("command", {})
    paths = manifest.get("paths", {})
    recorded_git = manifest.get("git", {})
    mismatches: list[str] = []
    if run.get("status") != "yielded":
        mismatches.append(f"run status is {run.get('status')!r}, expected 'yielded'")
    if run.get("name") != request.name:
        mismatches.append("run name changed")
    if command.get("argv") != list(request.command):
        mismatches.append("child command changed")
    if bool(command.get("pty")) != bool(request.use_pty):
        mismatches.append("PTY mode changed")
    if Path(str(paths.get("cwd", ""))).resolve() != cwd:
        mismatches.append("working directory changed")
    if Path(str(paths.get("output_dir", ""))).resolve() != run_dir:
        mismatches.append("recorded output directory changed")
    if recorded_git.get("commit") != git_context.get("commit"):
        mismatches.append("Git commit changed")
    if mismatches:
        raise ExperimentError(
            "refusing continuation because runner identity differs: "
            + "; ".join(mismatches)
        )
    run_id = str(run.get("id") or run_dir.name)
    return run_id, run_dir, manifest_path, manifest


def run_experiment(request: ExperimentRequest) -> ExperimentResult:
    """Prepare a run directory, execute the command, and update the manifest."""

    if not request.command:
        raise ExperimentError("cannot run an experiment without a command")

    cwd = request.cwd.resolve()
    output_root = _resolve_from_cwd(request.output_root, cwd)
    resolved_config_paths = resolve_config_paths(request.config_paths, cwd)
    resolved_output_root = output_root.resolve()
    for config_path in resolved_config_paths:
        resolved_config = config_path.resolve()
        if config_path.is_dir() and (
            resolved_config == resolved_output_root
            or resolved_config in resolved_output_root.parents
        ):
            raise ExperimentError(
                f"config directory {config_path} contains output root {output_root}; "
                "select a narrower config directory to avoid recursively copying the run"
            )

    git_context = collect_git_context(cwd)
    if request.require_clean:
        if not git_context.get("available"):
            raise ExperimentError(
                "--require-clean requires a git worktree with a resolved commit; "
                f"git context was unavailable: {git_context.get('error')}"
            )
        if not git_context.get("commit"):
            raise ExperimentError(
                "--require-clean requires a resolved git commit; commit the "
                "repository before launching an exact run"
            )
        if git_context.get("dirty"):
            status = str(git_context.get("status") or "").strip()
            detail = f"\nDirty files:\n{status}" if status else ""
            raise ExperimentError(
                "git worktree is dirty; commit, stash, or rerun without "
                f"--require-clean after accepting that the run is not exact.{detail}"
            )

    continuation = _load_continuation_manifest(
        request,
        cwd=cwd,
        git_context=git_context,
    )
    append_logs = continuation is not None
    if continuation is None:
        _ensure_output_root(output_root)
        run_id, run_dir = _create_run_dir(
            output_root=output_root,
            timestamp=_timestamp_for_id(),
            name=request.name,
            short_commit=str(git_context.get("short_commit") or ""),
        )
        copied_configs = copy_config_paths(resolved_config_paths, run_dir)
        write_git_snapshots(git_context, cwd, run_dir)
        rsync_pull_command = build_rsync_pull_command(
            remote=request.remote,
            remote_run_dir=run_dir,
            local_output_root=request.local_output_root or request.output_root,
            run_id=run_id,
        )
        manifest = build_manifest(
            request=request,
            cwd=cwd,
            run_id=run_id,
            run_dir=run_dir,
            git_context=git_context,
            copied_configs=copied_configs,
            rsync_pull_command=rsync_pull_command,
        )
        manifest_path = run_dir / MANIFEST_NAME
    else:
        run_id, run_dir, manifest_path, manifest = continuation
        rsync_pull_command = manifest.get("sync", {}).get("rsync_pull_command")

    segment_number = len(manifest.setdefault("segments", [])) + 1
    receipt_path_value = os.environ.get(RUNNER_RECEIPT_ENV)
    receipt_path = (
        _resolve_from_cwd(Path(receipt_path_value), cwd)
        if receipt_path_value
        else run_dir / RUNNER_RECEIPT_NAME
    )
    segment = {
        "segment": segment_number,
        "status": "running",
        "started_at": utc_now_iso(),
        "finished_at": None,
        "duration_seconds": None,
        "return_code": None,
        "continuation_checkpoint": os.environ.get(
            "EXPERIMENT_QUEUE_CONTINUATION_CHECKPOINT"
        ),
        "gpu_uuid": os.environ.get("EXPERIMENT_QUEUE_GPU_UUID"),
    }
    manifest["segments"].append(segment)
    manifest["run"]["status"] = "running"
    manifest["run"]["finished_at"] = None
    manifest["run"]["return_code"] = None
    started = time.monotonic()

    def publish_receipt(status: str, return_code: int | None) -> None:
        """Publish the machine contract only after its referenced manifest is current."""

        receipt = build_runner_receipt(
            run_id=run_id,
            segment=segment_number,
            status=status,
            return_code=return_code,
            run_dir=run_dir,
            manifest_path=manifest_path,
            rsync_pull_command=rsync_pull_command,
        )
        try:
            write_runner_receipt(receipt_path, receipt)
        except OSError as exc:
            raise ExperimentError(
                f"could not publish runner receipt {receipt_path}: {exc}"
            ) from exc

    def finish_manifest(
        status: str,
        command_return_code: int,
        *,
        runner_return_code: int | None = None,
    ) -> None:
        """Record child outcome and the runner process's observable exit code.

        The manifest describes the attempted child command.  RunnerReceipt's
        ``return_code`` describes the runner CLI process observed by its
        scheduler.  They differ when child launch fails: the manifest records
        the conventional 127 launch sentinel while the CLI preserves its
        established setup-error exit code 2.
        """

        duration = round(time.monotonic() - started, 3)
        segment.update(
            status=status,
            return_code=command_return_code,
            finished_at=utc_now_iso(),
            duration_seconds=duration,
        )
        manifest["run"].update(
            status=status,
            return_code=command_return_code,
            finished_at=segment["finished_at"],
            duration_seconds=round(
                sum(float(row.get("duration_seconds") or 0.0) for row in manifest["segments"]),
                3,
            ),
        )
        write_manifest(manifest_path, manifest)
        publish_receipt(
            status,
            (
                _runner_process_return_code(command_return_code)
                if runner_return_code is None
                else runner_return_code
            ),
        )

    with _translate_termination_signals_to_interrupt():
        try:
            # Publish only after termination signals have a cleanup handler;
            # monitoring code treats manifest visibility as launch readiness.
            write_manifest(manifest_path, manifest)
            publish_receipt("running", None)
            try:
                return_code = launch_and_stream(
                    request.command,
                    cwd,
                    run_dir,
                    run_id,
                    use_pty=request.use_pty,
                    append=append_logs,
                )
            except OSError as exc:
                return_code = 127
                finish_manifest("launch_failed", return_code, runner_return_code=2)
                raise ExperimentError(
                    f"could not start command {request.command[0]!r}: "
                    f"{exc.strerror or exc}"
                ) from exc
            if return_code == 0:
                status = "succeeded"
            elif (
                return_code == YIELD_EXIT_CODE
                and os.environ.get(YIELD_RECEIPT_ENV)
            ):
                status = "yielded"
            elif return_code == 130:
                status = "interrupted"
            else:
                status = "failed"
            finish_manifest(status, return_code)
        except KeyboardInterrupt:
            return_code = 130
            finish_manifest("interrupted", return_code)

    return ExperimentResult(
        run_id=run_id,
        run_dir=run_dir,
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        return_code=return_code,
        rsync_pull_command=rsync_pull_command,
    )


@contextlib.contextmanager
def _translate_termination_signals_to_interrupt() -> Iterable[None]:
    """Let queue-requested SIGTERM follow the runner's clean interrupt path.

    Signal handlers can only be installed by the main thread. Direct library
    callers in worker threads retain the platform defaults, while normal CLI
    execution converts SIGTERM into the same child cleanup and manifest update
    already used for Ctrl-C.
    """

    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous: dict[int, Any] = {}
    interruption_started = False

    def interrupt(_signum: int, _frame: Any) -> None:
        nonlocal interruption_started
        if interruption_started:
            return
        interruption_started = True
        raise KeyboardInterrupt

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, interrupt)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def build_manifest(
    *,
    request: ExperimentRequest,
    cwd: Path,
    run_id: str,
    run_dir: Path,
    git_context: dict[str, Any],
    copied_configs: list[dict[str, str]],
    rsync_pull_command: str | None,
) -> dict[str, Any]:
    """Create the manifest structure that records run provenance."""

    return {
        **RUNNER_MANIFEST_V1.document_identity(),
        # Retained additively for readers of the extracted v1 manifest. New
        # readers use the independent apiVersion/kind identity above.
        "schema_version": 1,
        "run": {
            "id": run_id,
            "name": request.name,
            "status": "running",
            "created_at": utc_now_iso(),
            "finished_at": None,
            "duration_seconds": None,
            "return_code": None,
            "notes": request.notes,
        },
        "command": {
            "argv": list(request.command),
            "shell": False,
            "pty": bool(request.use_pty),
        },
        "paths": {
            "cwd": str(cwd),
            "output_dir": str(run_dir),
            "manifest": str(run_dir / MANIFEST_NAME),
        },
        "logs": {
            "stdout": "stdout.log",
            "stderr": "stderr.log",
        },
        "configs": copied_configs,
        "git": git_context,
        "environment": {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python_executable": sys.executable,
            "python_version": platform.python_version(),
        },
        "child_environment": {
            "EXPERIMENT_RUN_ID": run_id,
            "EXPERIMENT_OUTPUT_DIR": str(run_dir),
        },
        "sync": {
            "rsync_pull_command": rsync_pull_command,
        },
    }


def build_runner_receipt(
    *,
    run_id: str,
    segment: int,
    status: str,
    return_code: int | None,
    run_dir: Path,
    manifest_path: Path,
    rsync_pull_command: str | None,
) -> dict[str, Any]:
    """Build the strict RunnerReceipt/v1 envelope consumed by the scheduler.

    Paths are absolute so scheduler recovery never depends on the launcher's
    working directory.  The optional queue item identity is observational;
    the scheduler independently validates it when the runner was queue-launched.
    ``return_code`` is the runner process exit code, while child-command details
    remain authoritative in the referenced manifest.
    """

    queue_item_value = os.environ.get("EXPERIMENT_QUEUE_ITEM_ID")
    queue_item_id: int | None = None
    if queue_item_value:
        try:
            queue_item_id = int(queue_item_value)
        except ValueError as exc:
            raise ExperimentError(
                "EXPERIMENT_QUEUE_ITEM_ID must be an integer when publishing "
                f"a runner receipt, got {queue_item_value!r}"
            ) from exc
        if queue_item_id < 1:
            raise ExperimentError(
                "EXPERIMENT_QUEUE_ITEM_ID must be positive when publishing "
                f"a runner receipt, got {queue_item_id}"
            )
    resolved_run_dir = run_dir.resolve()
    sync = (
        {
            "type": "rsync-pull",
            "command": rsync_pull_command,
        }
        if rsync_pull_command
        else None
    )
    return {
        **RUNNER_RECEIPT_V1.document_identity(),
        "run_id": run_id,
        "queue_item_id": queue_item_id,
        "segment": segment,
        "status": status,
        "return_code": return_code,
        "run_directory": str(resolved_run_dir),
        "manifest": str(manifest_path.resolve()),
        "logs": {
            "stdout": str((resolved_run_dir / "stdout.log").resolve()),
            "stderr": str((resolved_run_dir / "stderr.log").resolve()),
        },
        "sync": sync,
        "written_at": utc_now_iso(),
    }


def launch_and_stream(
    command: Sequence[str],
    cwd: Path,
    run_dir: Path,
    run_id: str,
    *,
    use_pty: bool = False,
    append: bool = False,
) -> int:
    """Launch a child command and tee stdout/stderr into run log files."""

    if use_pty:
        return _launch_and_stream_pty(command, cwd, run_dir, run_id, append=append)

    env = os.environ.copy()
    env["EXPERIMENT_RUN_ID"] = run_id
    env["EXPERIMENT_OUTPUT_DIR"] = str(run_dir)

    stdout_log = run_dir / "stdout.log"
    stderr_log = run_dir / "stderr.log"
    with subprocess.Popen(
        list(command),
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    ) as process:
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_thread = threading.Thread(
            target=_tee_stream,
            args=(process.stdout, stdout_log, sys.stdout, append),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_tee_stream,
            args=(process.stderr, stderr_log, sys.stderr, append),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        try:
            return_code = process.wait()
        except KeyboardInterrupt:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            return_code = 130
        stdout_thread.join()
        stderr_thread.join()
        return return_code


def _launch_and_stream_pty(
    command: Sequence[str],
    cwd: Path,
    run_dir: Path,
    run_id: str,
    *,
    append: bool = False,
) -> int:
    """Launch a child command under a PTY and tee combined output."""

    if os.name == "nt":
        raise OSError("PTY mode is not supported on Windows")

    import pty

    env = os.environ.copy()
    env["EXPERIMENT_RUN_ID"] = run_id
    env["EXPERIMENT_OUTPUT_DIR"] = str(run_dir)

    stdout_log = run_dir / "stdout.log"
    stderr_log = run_dir / "stderr.log"
    with stderr_log.open("a" if append else "w", encoding="utf-8") as handle:
        handle.write("PTY mode merges child stdout and stderr into stdout.log.\n")

    master_fd, slave_fd = pty.openpty()
    _configure_pty_window_size(slave_fd)
    process = None
    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=env,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )
        os.close(slave_fd)
        slave_fd = -1
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        with stdout_log.open("a" if append else "w", encoding="utf-8") as log_file:
            if append:
                log_file.write(f"\n=== continuation started {utc_now_iso()} ===\n")
                log_file.flush()
            while True:
                if process.poll() is not None:
                    ready, _, _ = select.select([master_fd], [], [], 0)
                    if not ready:
                        break
                else:
                    ready, _, _ = select.select([master_fd], [], [], 0.1)
                    if not ready:
                        continue
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                text = decoder.decode(chunk)
                if text:
                    log_file.write(text)
                    log_file.flush()
                    sys.stdout.write(text)
                    sys.stdout.flush()
            tail = decoder.decode(b"", final=True)
            if tail:
                log_file.write(tail)
                log_file.flush()
                sys.stdout.write(tail)
                sys.stdout.flush()
        try:
            return_code = process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            return_code = process.wait()
        return return_code
    except KeyboardInterrupt:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        return 130
    finally:
        if slave_fd != -1:
            os.close(slave_fd)
        os.close(master_fd)


def _configure_pty_window_size(slave_fd: int) -> None:
    """Give the child PTY a useful size for progress-bar rendering."""

    try:
        terminal_size = os.get_terminal_size(sys.stdout.fileno())
        rows = terminal_size.lines
        columns = terminal_size.columns
    except (AttributeError, OSError, ValueError):
        rows = 40
        columns = 120

    try:
        import fcntl
        import struct
        import termios

        packed_size = struct.pack("HHHH", rows, columns, 0, 0)
        fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, packed_size)
    except OSError:
        return


def collect_git_context(cwd: Path) -> dict[str, Any]:
    """Collect git metadata without failing when the directory is not a repo."""

    inside = _git(cwd, "rev-parse", "--is-inside-work-tree")
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        return {
            "available": False,
            "dirty": None,
            "error": _clean_error(inside.stderr) or "not inside a git worktree",
        }

    commit = _git(cwd, "rev-parse", "HEAD")
    branch = _git(cwd, "rev-parse", "--abbrev-ref", "HEAD")
    root = _git(cwd, "rev-parse", "--show-toplevel")
    status = _git(cwd, "status", "--short")
    dirty = bool(status.stdout.strip())
    commit_sha = commit.stdout.strip() if commit.returncode == 0 else None

    return {
        "available": True,
        "root": root.stdout.strip() if root.returncode == 0 else None,
        "branch": branch.stdout.strip() if branch.returncode == 0 else None,
        "commit": commit_sha,
        "short_commit": commit_sha[:8] if commit_sha else None,
        "dirty": dirty,
        "status": status.stdout.rstrip(),
    }


def resolve_config_paths(config_paths: Sequence[Path], cwd: Path) -> list[Path]:
    """Resolve and validate config paths before creating a run directory."""

    resolved_paths: list[Path] = []
    for source in config_paths:
        source_path = _resolve_from_cwd(source, cwd)
        if not source_path.exists():
            raise ExperimentError(f"config path does not exist: {source_path}")
        if not source_path.is_file() and not source_path.is_dir():
            raise ExperimentError(f"config path is not a file or directory: {source_path}")
        resolved_paths.append(source_path)
    return resolved_paths


def copy_config_paths(
    config_paths: Sequence[Path],
    run_dir: Path,
) -> list[dict[str, str]]:
    """Copy requested config files or directories into the run directory."""

    copied: list[dict[str, str]] = []
    if not config_paths:
        return copied

    config_root = run_dir / "configs"
    config_root.mkdir()
    for source_path in config_paths:
        target = _unique_path(config_root / source_path.name)
        if source_path.is_dir():
            shutil.copytree(
                source_path,
                target,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"),
                symlinks=True,
            )
        else:
            shutil.copy2(source_path, target)

        copied.append(
            {
                "source": str(source_path),
                "copy": str(target.relative_to(run_dir)),
            }
        )
    return copied


def write_git_snapshots(git_context: dict[str, Any], cwd: Path, run_dir: Path) -> None:
    """Write git status and diff snapshots for dirty worktrees."""

    if not git_context.get("available") or not git_context.get("dirty"):
        return

    status_text = str(git_context.get("status") or "")
    (run_dir / "git_status.txt").write_text(status_text + "\n", encoding="utf-8")

    diff = _git(cwd, "diff", "--binary")
    if diff.stdout:
        (run_dir / "git_diff.patch").write_text(diff.stdout, encoding="utf-8")

    staged_diff = _git(cwd, "diff", "--cached", "--binary")
    if staged_diff.stdout:
        (run_dir / "git_diff_cached.patch").write_text(
            staged_diff.stdout,
            encoding="utf-8",
        )


def build_rsync_pull_command(
    *,
    remote: str | None,
    remote_run_dir: Path,
    local_output_root: Path,
    run_id: str,
) -> str | None:
    """Build the command a local workstation can use to pull remote outputs."""

    if not remote:
        return None

    local_destination = local_output_root / run_id
    remote_source = f"{remote}:{remote_run_dir}/"
    local_target = f"{local_destination}/"
    return (
        "rsync -avh --progress -- "
        f"{shlex.quote(remote_source)} "
        f"{shlex.quote(local_target)}"
    )


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Atomically replace a stable, human-readable JSON manifest."""

    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        try:
            mode = path.stat().st_mode & 0o777
        except FileNotFoundError:
            # NamedTemporaryFile creates mode 0600. Keep that conservative
            # default rather than overriding a caller's restrictive umask.
            pass
        else:
            temporary.chmod(mode)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def write_runner_receipt(path: Path, receipt: dict[str, Any]) -> None:
    """Atomically replace the scheduler-facing runner receipt.

    The manifest and receipt share the same adjacent-temporary-file primitive,
    so readers observe either the previous complete document or the next one.
    """

    write_manifest(path, receipt)


def slugify(value: str) -> str:
    """Convert a run name into a filesystem-friendly slug."""

    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "run"


def utc_now_iso() -> str:
    """Return the current UTC time in manifest-friendly ISO format."""

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _timestamp_for_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _create_run_dir(
    *,
    output_root: Path,
    timestamp: str,
    name: str,
    short_commit: str,
) -> tuple[str, Path]:
    """Atomically claim one unique run directory across concurrent runners."""

    parts = [timestamp, slugify(name)]
    if short_commit:
        parts.append(short_commit)
    base_run_id = "_".join(parts)

    for index in range(1, 1000):
        run_id = base_run_id if index == 1 else f"{base_run_id}_{index:03d}"
        run_dir = output_root / run_id
        try:
            run_dir.mkdir()
        except FileExistsError:
            continue
        except OSError as exc:
            raise ExperimentError(f"could not create run directory {run_dir}: {exc}") from exc
        else:
            return run_id, run_dir
    raise ExperimentError(f"could not create an unused run directory under {output_root}")


def _ensure_output_root(output_root: Path) -> None:
    if output_root.exists() and not output_root.is_dir():
        raise ExperimentError(f"output root exists but is not a directory: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)


def _resolve_from_cwd(path: Path, cwd: Path) -> Path:
    return path if path.is_absolute() else (cwd / path).resolve()


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(2, 1000):
        candidate = path.with_name(f"{path.stem}_{index:03d}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise ExperimentError(f"could not choose an unused destination for {path}")


def _tee_stream(
    pipe: IO[str],
    log_path: Path,
    destination: IO[str],
    append: bool = False,
) -> None:
    with log_path.open("a" if append else "w", encoding="utf-8") as log_file:
        if append:
            log_file.write(f"\n=== continuation started {utc_now_iso()} ===\n")
            log_file.flush()
        for line in iter(pipe.readline, ""):
            log_file.write(line)
            log_file.flush()
            destination.write(line)
            destination.flush()


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    command = ["git", *args]
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else "git command timed out"
        return subprocess.CompletedProcess(command, 124, stdout, stderr)


def _clean_error(stderr: str) -> str:
    return stderr.strip().replace("\n", " ")


def _normalize_remainder(command: Iterable[str]) -> list[str]:
    normalized = list(command)
    if normalized and normalized[0] == "--":
        return normalized[1:]
    return normalized
