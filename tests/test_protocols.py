"""Verify independent protocol identities and their checked-in fixture catalog."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json
from pathlib import Path

import pytest

import experiment_queue.protocols as protocols
from experiment_queue.protocols import (
    DECLARED_PROTOCOL_VERSIONS,
    ProtocolIdentityError,
    ProtocolKind,
    ProtocolVersion,
)


IDENTITY_FIXTURE = Path(__file__).parent / "fixtures" / "protocol-identities.json"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_checked_in_identity_fixture_covers_the_declared_registry() -> None:
    rows = json.loads(IDENTITY_FIXTURE.read_text(encoding="utf-8"))
    actual = []
    for row in rows:
        version = getattr(protocols, row["constant"])
        assert isinstance(version, ProtocolVersion)
        assert version.document_identity() == {
            "apiVersion": row["apiVersion"],
            "kind": row["kind"],
        }
        actual.append(version)

    assert tuple(actual) == DECLARED_PROTOCOL_VERSIONS
    assert len(set(actual)) == len(actual)


def test_same_major_in_different_protocol_families_is_not_shared_identity() -> None:
    runner_manifest = protocols.RUNNER_MANIFEST_V1
    runner_receipt = protocols.RUNNER_RECEIPT_V1

    assert runner_manifest.major == runner_receipt.major == 1
    assert runner_manifest != runner_receipt
    assert runner_manifest.kind is ProtocolKind.RUNNER_MANIFEST
    assert runner_receipt.kind is ProtocolKind.RUNNER_RECEIPT


def test_compatibility_matrix_names_every_declared_identity() -> None:
    matrix = (REPOSITORY_ROOT / "docs" / "protocol-compatibility.md").read_text(
        encoding="utf-8"
    )

    for version in DECLARED_PROTOCOL_VERSIONS:
        assert f"`{version.kind.value}/v{version.major}`" in matrix


def test_adr_index_names_every_numbered_record() -> None:
    adr_directory = REPOSITORY_ROOT / "docs" / "adr"
    index = (adr_directory / "README.md").read_text(encoding="utf-8")

    for path in adr_directory.glob("[0-9][0-9][0-9][0-9]-*.md"):
        assert f"({path.name})" in index


@pytest.mark.parametrize("version", DECLARED_PROTOCOL_VERSIONS)
def test_document_identity_round_trips(version: ProtocolVersion) -> None:
    assert ProtocolVersion.from_document(version.document_identity()) == version


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"apiVersion": "experiment-queue/v1"}, "kind"),
        ({"kind": "RunnerReceipt"}, "apiVersion"),
        (
            {"apiVersion": "experiment-queue/v1", "kind": "Unknown"},
            "unsupported protocol kind",
        ),
        (
            {"apiVersion": "other/v1", "kind": "RunnerReceipt"},
            "unsupported apiVersion",
        ),
        (
            {"apiVersion": "experiment-queue/v01", "kind": "RunnerReceipt"},
            "unsupported apiVersion",
        ),
        (
            {"apiVersion": "experiment-queue/v-1", "kind": "RunnerReceipt"},
            "unsupported apiVersion",
        ),
        (
            {"apiVersion": "experiment-queue/v999", "kind": "RunnerReceipt"},
            "unsupported protocol identity",
        ),
        (
            {
                "apiVersion": "experiment-queue/v" + ("9" * 5000),
                "kind": "RunnerReceipt",
            },
            "major version is too large",
        ),
    ],
)
def test_document_identity_parser_fails_closed(
    document: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ProtocolIdentityError, match=message):
        ProtocolVersion.from_document(document)


@pytest.mark.parametrize("major", [True, 1.0, "1", None])
def test_protocol_version_rejects_non_integer_majors(major: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        ProtocolVersion(ProtocolKind.DATABASE, major)  # type: ignore[arg-type]


def test_protocol_version_is_immutable_and_rejects_negative_major() -> None:
    with pytest.raises(ValueError, match="zero or greater"):
        ProtocolVersion(ProtocolKind.DATABASE, -1)
    with pytest.raises(FrozenInstanceError):
        protocols.DATABASE_V4.major = 5  # type: ignore[misc]
