"""Verify explicit state configuration and stable project-key validation."""

from pathlib import Path

import pytest

from experiment_queue.config import STATE_DIR_ENV, StateDirectoryError, resolve_state_dir
from experiment_queue.identity import validate_project_key


def test_state_directory_cli_value_precedes_environment(tmp_path: Path) -> None:
    cli_path = tmp_path / "cli-state"
    env_path = tmp_path / "env-state"

    resolved = resolve_state_dir(
        cli_path,
        environ={STATE_DIR_ENV: str(env_path)},
    )

    assert resolved == cli_path.resolve()


def test_state_directory_uses_environment_when_cli_is_absent(tmp_path: Path) -> None:
    state_path = tmp_path / "queue-state"

    assert resolve_state_dir(None, environ={STATE_DIR_ENV: str(state_path)}) == state_path.resolve()


def test_state_directory_requires_an_explicit_value() -> None:
    with pytest.raises(StateDirectoryError, match="--state-dir"):
        resolve_state_dir(None, environ={})


@pytest.mark.parametrize("value", [Path("relative/state"), Path("state")])
def test_state_directory_rejects_relative_paths(value: Path) -> None:
    with pytest.raises(StateDirectoryError, match="absolute path"):
        resolve_state_dir(value, environ={})


@pytest.mark.parametrize(
    "value",
    [
        "flowers-3d-helmholtz",
        "project1",
        "a",
        "a1-b2-c3",
    ],
)
def test_project_key_accepts_portable_slugs(value: str) -> None:
    assert validate_project_key(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "Flowers",
        "1project",
        "project_queue",
        "project--queue",
        "project-",
        " project",
        "a" * 64,
    ],
)
def test_project_key_rejects_ambiguous_or_unsafe_values(value: str) -> None:
    with pytest.raises(ValueError, match="project key"):
        validate_project_key(value)
