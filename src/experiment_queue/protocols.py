"""Define independent, typed identities for experiment-queue protocols.

Protocol majors are scoped to a document kind.  A database version, for
example, never implies a queue-export or runner-manifest version even when the
integer happens to match.  New machine-readable documents should carry the
``apiVersion`` and ``kind`` fields returned by :meth:`ProtocolVersion.document_identity`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Mapping, Self


API_GROUP: Final = "experiment-queue"
_API_VERSION_PATTERN = re.compile(rf"{re.escape(API_GROUP)}/v(0|[1-9][0-9]*)\Z")


class ProtocolIdentityError(ValueError):
    """Raised when a serialized document has no valid protocol identity."""


class ProtocolKind(StrEnum):
    """Stable wire names for independently versioned protocol families."""

    DATABASE = "Database"
    PROJECT = "Project"
    EXPERIMENT_CARD = "ExperimentCard"
    LEGACY_MARKDOWN_CARD = "LegacyMarkdownCard"
    RUNNER_MANIFEST = "RunnerManifest"
    RUNNER_RECEIPT = "RunnerReceipt"
    QUEUE_EXPORT = "QueueExport"
    COOPERATIVE_YIELD_REQUEST = "CooperativeYieldRequest"
    COOPERATIVE_YIELD_RECEIPT = "CooperativeYieldReceipt"


@dataclass(frozen=True, slots=True)
class ProtocolVersion:
    """One protocol kind and its independently evolving major version.

    Major zero is reserved for explicitly named legacy adapters and fallbacks.
    Constructing an identity validates both fields so callers cannot silently
    create a version that belongs to an unknown protocol family.
    """

    kind: ProtocolKind
    major: int

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ProtocolKind):
            raise TypeError("protocol kind must be a ProtocolKind")
        if isinstance(self.major, bool) or not isinstance(self.major, int):
            raise TypeError("protocol major version must be an integer")
        if self.major < 0:
            raise ValueError("protocol major version must be zero or greater")

    @property
    def api_version(self) -> str:
        """Return the stable API-group spelling used in serialized documents."""

        return f"{API_GROUP}/v{self.major}"

    def document_identity(self) -> dict[str, str]:
        """Return fresh JSON/YAML-native identity fields for a document."""

        return {"apiVersion": self.api_version, "kind": self.kind.value}

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> Self:
        """Parse the identity fields of a JSON/YAML document without guessing.

        Missing, malformed, or undeclared identities fail closed.  A declared
        version may still be unsupported by a particular runtime reader; that
        compatibility check and payload validation remain the responsibility
        of the owning protocol parser.
        """

        kind_value = document.get("kind")
        api_version_value = document.get("apiVersion")
        if not isinstance(kind_value, str) or not kind_value:
            raise ProtocolIdentityError("protocol document requires a string 'kind'")
        if not isinstance(api_version_value, str) or not api_version_value:
            raise ProtocolIdentityError(
                "protocol document requires a string 'apiVersion'"
            )
        try:
            kind = ProtocolKind(kind_value)
        except ValueError as exc:
            raise ProtocolIdentityError(
                f"unsupported protocol kind {kind_value!r}"
            ) from exc
        match = _API_VERSION_PATTERN.fullmatch(api_version_value)
        if match is None:
            raise ProtocolIdentityError(
                f"unsupported apiVersion {api_version_value!r}; expected "
                f"'{API_GROUP}/v<major>'"
            )
        major_text = match.group(1)
        if len(major_text) > 9:
            raise ProtocolIdentityError(
                "unsupported apiVersion; major version is too large"
            )
        major = int(major_text)
        parsed = cls(kind=kind, major=major)
        if parsed not in DECLARED_PROTOCOL_VERSIONS:
            raise ProtocolIdentityError(
                f"unsupported protocol identity {kind.value}/v{parsed.major}"
            )
        return parsed


# Database inputs v1-v4 remain recognized by the extracted compatibility code.
# Database/v5 is the independently named target for the multi-project schema.
DATABASE_V1: Final = ProtocolVersion(ProtocolKind.DATABASE, 1)
DATABASE_V2: Final = ProtocolVersion(ProtocolKind.DATABASE, 2)
DATABASE_V3: Final = ProtocolVersion(ProtocolKind.DATABASE, 3)
DATABASE_V4: Final = ProtocolVersion(ProtocolKind.DATABASE, 4)
DATABASE_V5: Final = ProtocolVersion(ProtocolKind.DATABASE, 5)

# Portable authoring protocols are declared now so schemas can reference stable
# identities without depending on the database version.
PROJECT_V1: Final = ProtocolVersion(ProtocolKind.PROJECT, 1)
EXPERIMENT_CARD_V1: Final = ProtocolVersion(ProtocolKind.EXPERIMENT_CARD, 1)
LEGACY_MARKDOWN_CARD_V0: Final = ProtocolVersion(
    ProtocolKind.LEGACY_MARKDOWN_CARD,
    0,
)

RUNNER_MANIFEST_V1: Final = ProtocolVersion(ProtocolKind.RUNNER_MANIFEST, 1)
RUNNER_RECEIPT_V1: Final = ProtocolVersion(ProtocolKind.RUNNER_RECEIPT, 1)

# Human stdout scraping is an explicitly bounded RunnerReceipt/v0 fallback; it
# is not a machine-readable document and therefore cannot carry identity fields.
LEGACY_RUNNER_STDOUT_RECEIPT_V0: Final = ProtocolVersion(
    ProtocolKind.RUNNER_RECEIPT,
    0,
)

QUEUE_EXPORT_V1: Final = ProtocolVersion(ProtocolKind.QUEUE_EXPORT, 1)

# The extracted queue export is coupled to the database schema integer.  Name
# that compatibility representation v0 so new exports can evolve independently.
LEGACY_DATABASE_COUPLED_QUEUE_EXPORT_V0: Final = ProtocolVersion(
    ProtocolKind.QUEUE_EXPORT,
    0,
)

# The extracted schema-v4 queue's ``schema_version: 1`` yield documents are a
# distinct untyped compatibility format. Name them protocol v0 so the new
# typed v1 envelope never claims wire compatibility with their field shape.
LEGACY_COOPERATIVE_YIELD_REQUEST_V0: Final = ProtocolVersion(
    ProtocolKind.COOPERATIVE_YIELD_REQUEST,
    0,
)
LEGACY_COOPERATIVE_YIELD_RECEIPT_V0: Final = ProtocolVersion(
    ProtocolKind.COOPERATIVE_YIELD_RECEIPT,
    0,
)
COOPERATIVE_YIELD_REQUEST_V1: Final = ProtocolVersion(
    ProtocolKind.COOPERATIVE_YIELD_REQUEST,
    1,
)
COOPERATIVE_YIELD_RECEIPT_V1: Final = ProtocolVersion(
    ProtocolKind.COOPERATIVE_YIELD_RECEIPT,
    1,
)


# This tuple is intentionally exhaustive and contains no aliases.  It gives
# tooling and tests a stable typed registry while per-protocol owners decide
# which declared versions are currently emitted, accepted, or migration-only.
DECLARED_PROTOCOL_VERSIONS: Final[tuple[ProtocolVersion, ...]] = (
    DATABASE_V1,
    DATABASE_V2,
    DATABASE_V3,
    DATABASE_V4,
    DATABASE_V5,
    PROJECT_V1,
    EXPERIMENT_CARD_V1,
    LEGACY_MARKDOWN_CARD_V0,
    RUNNER_MANIFEST_V1,
    LEGACY_RUNNER_STDOUT_RECEIPT_V0,
    RUNNER_RECEIPT_V1,
    LEGACY_DATABASE_COUPLED_QUEUE_EXPORT_V0,
    QUEUE_EXPORT_V1,
    LEGACY_COOPERATIVE_YIELD_REQUEST_V0,
    COOPERATIVE_YIELD_REQUEST_V1,
    LEGACY_COOPERATIVE_YIELD_RECEIPT_V0,
    COOPERATIVE_YIELD_RECEIPT_V1,
)


__all__ = [
    "API_GROUP",
    "COOPERATIVE_YIELD_RECEIPT_V1",
    "COOPERATIVE_YIELD_REQUEST_V1",
    "DATABASE_V1",
    "DATABASE_V2",
    "DATABASE_V3",
    "DATABASE_V4",
    "DATABASE_V5",
    "DECLARED_PROTOCOL_VERSIONS",
    "EXPERIMENT_CARD_V1",
    "LEGACY_COOPERATIVE_YIELD_RECEIPT_V0",
    "LEGACY_COOPERATIVE_YIELD_REQUEST_V0",
    "LEGACY_DATABASE_COUPLED_QUEUE_EXPORT_V0",
    "LEGACY_MARKDOWN_CARD_V0",
    "LEGACY_RUNNER_STDOUT_RECEIPT_V0",
    "PROJECT_V1",
    "ProtocolIdentityError",
    "ProtocolKind",
    "ProtocolVersion",
    "QUEUE_EXPORT_V1",
    "RUNNER_MANIFEST_V1",
    "RUNNER_RECEIPT_V1",
]
