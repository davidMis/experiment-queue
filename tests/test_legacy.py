"""Freeze the exact LegacyMarkdownCard/v0 parser compatibility contract."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from experiment_queue.legacy import (
    LEGACY_CARD_API_VERSION,
    LEGACY_CARD_KIND,
    LEGACY_COMMAND_HEADING,
    LegacyCardError,
    LegacyMarkdownCard,
    legacy_command_for_worktree,
)


COMMAND = """(
set -euo pipefail
cd ~/3D_Helmholtz
python3 scripts/run_experiment.py \\
  --name fixture-run --require-clean --remote mutton2
)"""


def card_source(*, command: str = COMMAND, language: str = "bash") -> bytes:
    """Return one parser-compatible immutable Markdown fixture."""

    return (
        "# TST-101: Exact legacy fixture\n\n"
        f"{LEGACY_COMMAND_HEADING}\n\n"
        f"```{language}\n{command}\n```\n\n"
        "## Expected Artifacts\n\nEvidence remains project-owned.\n"
    ).encode()


def test_exact_card_retains_source_hash_and_byte_equivalent_command() -> None:
    source = card_source()

    card = LegacyMarkdownCard.from_source(
        source,
        experiment_id=" tst-101 ",
        source_name="docs/experiments/TST-101.md",
    )

    assert card.api_version == LEGACY_CARD_API_VERSION
    assert card.kind == LEGACY_CARD_KIND
    assert card.experiment_id == "TST-101"
    assert card.source is source
    assert card.source_sha256 == hashlib.sha256(source).hexdigest()
    assert card.command_text == COMMAND
    assert card.runner_name == "fixture-run"


def test_sh_fence_is_the_only_alternate_language() -> None:
    card = LegacyMarkdownCard.from_source(
        card_source(language="sh"),
        experiment_id="TST-101",
    )
    assert card.command_text == COMMAND


@pytest.mark.parametrize(
    "experiment_id",
    ["", "101", "TST", "TST/101", "TST-ONE", " TST-101 extra"],
)
def test_experiment_id_grammar_remains_narrow(experiment_id: str) -> None:
    with pytest.raises(LegacyCardError, match="invalid experiment ID"):
        LegacyMarkdownCard.from_source(
            card_source(),
            experiment_id=experiment_id,
        )


def test_heading_must_match_the_selected_experiment() -> None:
    with pytest.raises(LegacyCardError, match="heading must start"):
        LegacyMarkdownCard.from_source(
            card_source(),
            experiment_id="TST-102",
        )


def test_alternate_command_heading_is_explicitly_unimportable() -> None:
    source = card_source().replace(
        LEGACY_COMMAND_HEADING.encode(),
        b"## Command",
    )
    with pytest.raises(LegacyCardError, match="lacks the required"):
        LegacyMarkdownCard.from_source(source, experiment_id="TST-101")


@pytest.mark.parametrize("language", ["", "shell", "console", "python"])
def test_non_bash_fences_are_not_broadened(language: str) -> None:
    with pytest.raises(LegacyCardError, match="found 0"):
        LegacyMarkdownCard.from_source(
            card_source(language=language),
            experiment_id="TST-101",
        )


def test_exactly_one_command_block_is_required() -> None:
    source = card_source().replace(
        b"## Expected Artifacts",
        b"```sh\npython3 scripts/run_experiment.py --name second "
        b"--require-clean --remote mutton2\n```\n\n## Expected Artifacts",
    )
    with pytest.raises(LegacyCardError, match="found 2"):
        LegacyMarkdownCard.from_source(source, experiment_id="TST-101")


def test_doubled_trailing_backslash_remains_rejected() -> None:
    doubled = COMMAND.replace("python3 scripts/run_experiment.py \\", "python3 scripts/run_experiment.py \\\\")
    with pytest.raises(LegacyCardError, match="doubled trailing backslash"):
        LegacyMarkdownCard.from_source(
            card_source(command=doubled),
            experiment_id="TST-101",
        )


@pytest.mark.parametrize(
    "fragment",
    ["scripts/run_experiment.py", "--require-clean", "--remote mutton2"],
)
def test_required_queue_fragments_remain_exact(fragment: str) -> None:
    with pytest.raises(LegacyCardError, match="not queue-compatible"):
        LegacyMarkdownCard.from_source(
            card_source(command=COMMAND.replace(fragment, "missing")),
            experiment_id="TST-101",
        )


def test_runner_name_must_be_a_simple_literal() -> None:
    source = card_source(command=COMMAND.replace("--name fixture-run", "--name $RUN"))
    with pytest.raises(LegacyCardError, match="simple --name"):
        LegacyMarkdownCard.from_source(source, experiment_id="TST-101")


def test_invalid_utf8_and_mutable_source_fail_at_the_boundary() -> None:
    with pytest.raises(LegacyCardError, match="not valid UTF-8"):
        LegacyMarkdownCard.from_source(b"\xff", experiment_id="TST-101")
    with pytest.raises(TypeError, match="immutable bytes"):
        LegacyMarkdownCard.from_source(  # type: ignore[arg-type]
            bytearray(card_source()),
            experiment_id="TST-101",
        )


def test_direct_and_subclass_construction_cannot_forge_evidence() -> None:
    with pytest.raises(TypeError, match="use from_source"):
        LegacyMarkdownCard()

    class ForgedCard(LegacyMarkdownCard):
        pass

    with pytest.raises(TypeError, match="not a subclass"):
        ForgedCard.from_source(card_source(), experiment_id="TST-101")


def test_legacy_worktree_redirect_is_exact_and_rejects_other_checkout_references(
    tmp_path: Path,
) -> None:
    worktree = (tmp_path / "worktree").resolve()
    command = "cd ~/3D_Helmholtz\npython scripts/run_experiment.py"
    assert legacy_command_for_worktree(command, worktree) == (
        'cd -- "$EXPERIMENT_QUEUE_WORKTREE"\n'
        "python scripts/run_experiment.py"
    )
    assert legacy_command_for_worktree("python run.py", worktree) == "python run.py"
    with pytest.raises(LegacyCardError, match="more than once"):
        legacy_command_for_worktree(
            "cd ~/3D_Helmholtz\ncd ~/3D_Helmholtz", worktree
        )
    with pytest.raises(LegacyCardError, match="unsupported primary-checkout"):
        legacy_command_for_worktree("echo ~/3D_Helmholtz", worktree)
