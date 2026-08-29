"""Exact, read-only adapter for the legacy Flowers Markdown card contract."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Self

from experiment_queue.protocols import LEGACY_MARKDOWN_CARD_V0


LEGACY_CARD_API_VERSION = LEGACY_MARKDOWN_CARD_V0.api_version
LEGACY_CARD_KIND = LEGACY_MARKDOWN_CARD_V0.kind.value
LEGACY_COMMAND_HEADING = "## Exact Manual Command On Mutton2"
_EXPERIMENT_ID_PATTERN = re.compile(r"[A-Z][A-Z0-9_-]*-[0-9]+\Z")


class LegacyCardError(ValueError):
    """Raised when source does not satisfy the frozen legacy parser contract."""


@dataclass(frozen=True, slots=True, init=False)
class LegacyMarkdownCard:
    """Immutable evidence parsed with exactly the schema-v4 Markdown rules.

    This adapter is intentionally narrow. It does not discover cards, broaden
    headings, resolve templates, or infer commands from other Markdown blocks.
    """

    api_version: str
    kind: str
    experiment_id: str
    source_name: str
    source: bytes
    source_sha256: str
    command_text: str
    runner_name: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "LegacyMarkdownCard is validated evidence; use from_source()"
        )

    @classmethod
    def from_source(
        cls,
        source: bytes,
        *,
        experiment_id: str,
        source_name: str = "<bytes>",
    ) -> Self:
        """Parse one exact legacy card without consulting a filesystem or Git."""

        if cls is not LegacyMarkdownCard:
            raise TypeError(
                "LegacyMarkdownCard.from_source() constructs exactly "
                "LegacyMarkdownCard, not a subclass"
            )
        if type(source) is not bytes:
            raise TypeError(
                f"source must be immutable bytes, got {type(source).__name__}"
            )
        if type(experiment_id) is not str:
            raise LegacyCardError(
                f"experiment_id must be text, got {type(experiment_id).__name__}"
            )
        if type(source_name) is not str or not source_name:
            raise LegacyCardError("source_name must be nonempty text")

        normalized_id = experiment_id.strip().upper()
        if _EXPERIMENT_ID_PATTERN.fullmatch(normalized_id) is None:
            raise LegacyCardError(
                f"invalid experiment ID {experiment_id!r}; expected a phase-scoped "
                "ID such as WCG-017"
            )
        try:
            text = source.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise LegacyCardError(
                f"legacy experiment card is not valid UTF-8: {source_name}: {exc}"
            ) from exc

        lines = text.splitlines()
        first_heading = lines[0] if lines else ""
        if not first_heading.startswith(f"# {normalized_id}:"):
            raise LegacyCardError(
                f"experiment card heading must start with '# {normalized_id}:': "
                f"{source_name}"
            )
        heading_offset = text.find(LEGACY_COMMAND_HEADING)
        if heading_offset < 0:
            raise LegacyCardError(
                f"experiment card lacks the required {LEGACY_COMMAND_HEADING!r} "
                f"section: {source_name}"
            )
        section_start = heading_offset + len(LEGACY_COMMAND_HEADING)
        next_heading = text.find("\n## ", section_start)
        section = (
            text[section_start:]
            if next_heading < 0
            else text[section_start:next_heading]
        )
        blocks = re.findall(
            r"^```(?:bash|sh)\s*\n(.*?)^```\s*$",
            section,
            flags=re.MULTILINE | re.DOTALL,
        )
        if len(blocks) != 1:
            raise LegacyCardError(
                f"expected exactly one bash command block under "
                f"{LEGACY_COMMAND_HEADING!r} in {source_name}; found {len(blocks)}"
            )
        command_text = blocks[0].strip()
        if re.search(r"\\\\[ \t]*$", command_text, flags=re.MULTILINE):
            raise LegacyCardError(
                "card command contains a doubled trailing backslash; use exactly "
                "one backslash for each shell line continuation in "
                f"{source_name}"
            )
        required_fragments = (
            "scripts/run_experiment.py",
            "--require-clean",
            "--remote mutton2",
        )
        missing = [
            fragment for fragment in required_fragments if fragment not in command_text
        ]
        if missing:
            raise LegacyCardError(
                "card command is not queue-compatible; missing "
                f"{', '.join(missing)} in {source_name}"
            )
        name_match = re.search(r"--name\s+([A-Za-z0-9_.-]+)", command_text)
        if name_match is None:
            raise LegacyCardError(
                f"card command does not contain a simple --name value: {source_name}"
            )

        instance = object.__new__(cls)
        for name, value in {
            "api_version": LEGACY_CARD_API_VERSION,
            "kind": LEGACY_CARD_KIND,
            "experiment_id": normalized_id,
            "source_name": source_name,
            "source": source,
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "command_text": command_text,
            "runner_name": name_match.group(1),
        }.items():
            object.__setattr__(instance, name, value)
        return instance


def legacy_command_for_worktree(command_text: str, worktree: Path) -> str:
    """Apply only the frozen schema-v4 isolated-worktree command redirect.

    Historical Flowers cards used one standalone ``cd ~/3D_Helmholtz`` line.
    The v3/v4 scheduler replaced exactly that line with its admitted worktree
    variable and rejected every other reference to the primary checkout. This
    function preserves that compatibility contract without interpreting or
    broadening arbitrary shell syntax.
    """

    if type(command_text) is not str or not command_text:
        raise LegacyCardError("legacy command_text must be non-empty text")
    if not isinstance(worktree, Path) or not worktree.is_absolute():
        raise LegacyCardError(
            f"legacy worktree must be an absolute pathlib.Path, got {worktree!r}"
        )
    replacement = 'cd -- "$EXPERIMENT_QUEUE_WORKTREE"'
    lines = command_text.splitlines()
    replaced = 0
    for index, line in enumerate(lines):
        if re.fullmatch(r"\s*cd\s+~/3D_Helmholtz\s*", line):
            lines[index] = replacement
            replaced += 1
    transformed = "\n".join(lines)
    if replaced > 1:
        raise LegacyCardError(
            "legacy command changes to ~/3D_Helmholtz more than once"
        )
    if "~/3D_Helmholtz" in transformed:
        raise LegacyCardError(
            "legacy command contains an unsupported primary-checkout reference; "
            "only one standalone 'cd ~/3D_Helmholtz' line is compatible with an "
            f"isolated worktree at {worktree}"
        )
    return transformed


__all__ = [
    "LEGACY_CARD_API_VERSION",
    "LEGACY_CARD_KIND",
    "LEGACY_COMMAND_HEADING",
    "LegacyCardError",
    "LegacyMarkdownCard",
    "legacy_command_for_worktree",
]
