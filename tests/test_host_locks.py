"""Verify one tamper-resistant host GPU lock namespace across queue versions."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

import experiment_queue.host_locks as host_locks
from experiment_queue.host_locks import HostGpuLockError, acquire_host_gpu_lock
import experiment_queue.queue as legacy_queue
import experiment_queue.scheduler_service_v5 as scheduler_v5


GPU_UUID = "GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _root(parent: Path) -> Path:
    return parent / f"experiment-queue-host-gpu-locks-v1-{os.geteuid()}"


def _lock_path(parent: Path) -> Path:
    digest = hashlib.sha256(GPU_UUID.encode("utf-8")).hexdigest()
    return _root(parent) / f"{digest}.lock"


def test_packaged_v4_and_v5_share_one_lock_helper() -> None:
    assert legacy_queue.acquire_host_gpu_lock is acquire_host_gpu_lock
    assert scheduler_v5.acquire_host_gpu_lock is acquire_host_gpu_lock


def test_host_lock_is_exclusive_and_ignores_tmpdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(host_locks, "HOST_LOCK_PARENT", tmp_path)
    monkeypatch.setenv("TMPDIR", str(tmp_path / "different-temp"))

    first = acquire_host_gpu_lock(GPU_UUID)
    assert first is not None
    assert _root(tmp_path).stat().st_mode & 0o777 == 0o700
    assert _lock_path(tmp_path).stat().st_mode & 0o777 == 0o600
    assert acquire_host_gpu_lock(GPU_UUID) is None
    first.close()

    second = acquire_host_gpu_lock(GPU_UUID)
    assert second is not None
    second.close()


def test_host_lock_rejects_substituted_or_insecure_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(host_locks, "HOST_LOCK_PARENT", tmp_path)
    target = tmp_path / "attacker-root"
    target.mkdir(mode=0o700)
    _root(tmp_path).symlink_to(target, target_is_directory=True)
    with pytest.raises(HostGpuLockError, match="non-symlink directory"):
        acquire_host_gpu_lock(GPU_UUID)

    _root(tmp_path).unlink()
    _root(tmp_path).mkdir(mode=0o700)
    _root(tmp_path).chmod(0o755)
    with pytest.raises(HostGpuLockError, match="mode 0700"):
        acquire_host_gpu_lock(GPU_UUID)


def test_host_lock_rejects_symlink_wrong_mode_and_extra_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(host_locks, "HOST_LOCK_PARENT", tmp_path)
    root = _root(tmp_path)
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    path = _lock_path(tmp_path)
    external = tmp_path / "external"
    external.write_bytes(b"")
    external.chmod(0o600)
    path.symlink_to(external)
    with pytest.raises(HostGpuLockError, match="cannot safely open"):
        acquire_host_gpu_lock(GPU_UUID)

    path.unlink()
    path.write_bytes(b"")
    path.chmod(0o644)
    with pytest.raises(HostGpuLockError, match="mode 0600"):
        acquire_host_gpu_lock(GPU_UUID)

    path.chmod(0o600)
    extra = root / "extra-link"
    os.link(path, extra)
    with pytest.raises(HostGpuLockError, match="exactly one filesystem link"):
        acquire_host_gpu_lock(GPU_UUID)


def test_host_lock_rejects_wrong_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(host_locks, "HOST_LOCK_PARENT", tmp_path)
    root = _root(tmp_path)
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    actual_uid = os.geteuid()
    monkeypatch.setattr(host_locks.os, "geteuid", lambda: actual_uid + 1)
    with pytest.raises(HostGpuLockError, match="owned by uid"):
        acquire_host_gpu_lock(GPU_UUID)
