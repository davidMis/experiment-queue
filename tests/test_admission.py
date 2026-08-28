"""Verify mutable submissions compile into immutable admission evidence."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest

import experiment_queue.admission as admission_module
from experiment_queue.admission import (
    AdmissionError,
    AdmissionSnapshot,
    ExtensionSchemaEvidence,
    SchemaEvidence,
    Submission,
    SubmissionPolicy,
    compile_admission,
)
from experiment_queue.schema_registry import (
    EXPERIMENT_CARD_V1_SCHEMA,
    PROJECT_V1_SCHEMA,
)
from experiment_queue.serialization import (
    canonical_json_bytes,
    canonical_json_sha256,
    sha256_bytes,
)


PROJECT_KEY = "fixture-project"
PROJECT_REVISION = "fixture-project:revision-7"
GIT_COMMIT = "a" * 40
PACKAGE_VERSION = "0.1.0-test"


@pytest.fixture(autouse=True)
def deterministic_package_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep compiler provenance deterministic without a public override."""

    def installed_version(distribution: str) -> str:
        assert distribution == "experiment-queue"
        return PACKAGE_VERSION

    monkeypatch.setattr(
        admission_module,
        "package_version_for",
        installed_version,
    )


def project_document(
    *,
    extension_schema: dict[str, object] | None = None,
    extensions: dict[str, object] | None = None,
) -> dict[str, object]:
    """Return a portable Project/v1 fixture with two logical resources."""

    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": PROJECT_KEY,
            "displayName": "Admission fixture",
        },
        "spec": {
            "cardRoots": ["cards", "campaigns/approved"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True},
                {"name": "inputs", "access": "readOnly"},
            ],
            "environments": [{"name": "python"}],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": ["PATH"],
            },
            "supportedProtocols": [
                {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldRequest",
                },
                {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldReceipt",
                },
            ],
        },
    }
    if extension_schema is not None:
        spec = document["spec"]
        assert isinstance(spec, dict)
        spec["extensionSchema"] = extension_schema
    if extensions is not None:
        document["extensions"] = extensions
    return document


def card_document(
    *,
    card_extensions: dict[str, object] | None = None,
    job_extensions: dict[str, object] | None = None,
) -> dict[str, object]:
    """Return a two-job ExperimentCard/v1 fixture with one yield-capable job."""

    train: dict[str, object] = {
        "id": "train",
        "environment": "python",
        "workingDirectory": "work",
        "command": {
            "type": "argv",
            "argv": [
                "python",
                "train.py",
                "--literal",
                "optimizer.precision",
            ],
        },
        "resources": {"gpus": 1, "cpus": 4},
        "artifacts": [
            {
                "name": "checkpoint",
                "root": "scratch",
                "path": "runs/checkpoint.bin",
                "type": "file",
            }
        ],
        "capabilities": {
            "cooperativeYield": {
                "requestProtocol": {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldRequest",
                },
                "receiptProtocol": {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "CooperativeYieldReceipt",
                },
                "checkpointArtifacts": ["checkpoint"],
            }
        },
    }
    if job_extensions is not None:
        train["extensions"] = job_extensions
    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": PROJECT_KEY,
            "experimentId": "EXP-001",
            "title": "Admission snapshot fixture",
            "tags": ["fixture"],
        },
        "spec": {
            "parameters": {
                "epochs": 10,
                "optimizer": {"name": "adam", "precision": "fp32"},
            },
            "jobs": [
                train,
                {
                    "id": "analyze",
                    "environment": "python",
                    "command": {
                        "type": "wrapper",
                        "path": "scripts/analyze.sh",
                        "args": ["--summary"],
                    },
                    "artifacts": [
                        {
                            "name": "report",
                            "root": "scratch",
                            "path": "reports/summary.json",
                            "type": "file",
                        }
                    ],
                },
            ],
            "provenance": {
                "inputs": [
                    {
                        "name": "dataset",
                        "source": "logical-volume:inputs/dataset-v1",
                    }
                ],
                "notes": "synthetic admission fixture",
            },
        },
    }
    if card_extensions is not None:
        document["extensions"] = card_extensions
    return document


def source_bytes(document: dict[str, object], *, pretty: bool = False) -> bytes:
    """Encode fixture JSON, which is also within the strict YAML subset."""

    if pretty:
        return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode()
    return json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def submission(**changes: object) -> Submission:
    """Return one valid mutable train-job submission."""

    values: dict[str, object] = {
        "project_key": PROJECT_KEY,
        "card_path": "cards/EXP-001.yaml",
        "job_id": "train",
        "operator": "test:operator",
        "bindings": {
            "epochs": 20,
            "optimizer": {"name": "sgd"},
        },
        "priority": 50,
        "hold_reason": "waiting for an input review",
        "dependencies": [11, 12],
        "preemption_authorized": True,
    }
    values.update(changes)
    return Submission(**values)  # type: ignore[arg-type]


def compile_fixture(
    *,
    project: dict[str, object] | None = None,
    card: dict[str, object] | None = None,
    submitted: Submission | None = None,
    project_revision: str = PROJECT_REVISION,
    git_commit: str = GIT_COMMIT,
    extension_schema_source: bytes | None = None,
) -> AdmissionSnapshot:
    """Compile one fixture with explicit deterministic package evidence."""

    return compile_admission(
        project_source=source_bytes(project or project_document()),
        card_source=source_bytes(card or card_document()),
        submission=submitted or submission(),
        project_revision=project_revision,
        git_commit=git_commit,
        extension_schema_source=extension_schema_source,
        project_source_name="config/project.yaml",
    )


def test_compile_admission_retains_complete_immutable_evidence() -> None:
    project = project_document()
    card = card_document()
    project_source = source_bytes(project)
    card_source = source_bytes(card)
    submitted = submission()

    snapshot = compile_admission(
        project_source=project_source,
        card_source=card_source,
        submission=submitted,
        project_revision=PROJECT_REVISION,
        git_commit=GIT_COMMIT.upper(),
    )

    assert snapshot.project_source == project_source
    assert snapshot.project_source_sha256 == sha256_bytes(project_source)
    assert snapshot.project_normalized_json == canonical_json_bytes(project)
    assert snapshot.project_normalized_sha256 == canonical_json_sha256(project)
    assert snapshot.card_source == card_source
    assert snapshot.card_source_name == "cards/EXP-001.yaml"
    assert snapshot.card_source_sha256 == sha256_bytes(card_source)
    assert snapshot.card_normalized_json == canonical_json_bytes(card)
    assert snapshot.card_normalized_sha256 == canonical_json_sha256(card)
    assert snapshot.project_schema.schema_id == PROJECT_V1_SCHEMA.schema_id
    assert snapshot.project_schema.sha256 == PROJECT_V1_SCHEMA.sha256
    assert snapshot.card_schema.schema_id == EXPERIMENT_CARD_V1_SCHEMA.schema_id
    assert snapshot.card_schema.sha256 == EXPERIMENT_CARD_V1_SCHEMA.sha256
    assert snapshot.extension_schema is None
    assert snapshot.git_commit == GIT_COMMIT
    assert snapshot.project_revision == PROJECT_REVISION
    assert snapshot.package_version == PACKAGE_VERSION
    assert snapshot.command.to_document() == snapshot.resolved_document["job"]["command"]
    assert snapshot.resolved_sha256 == sha256_bytes(snapshot.resolved_json)

    resolved = snapshot.resolved_document
    assert resolved["project"] == project
    assert resolved["card"] == {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": card["metadata"],
        "provenance": card["spec"]["provenance"],
    }
    assert resolved["job"]["id"] == "train"
    assert resolved["parameters"] == {
        "epochs": 20,
        "optimizer": {"name": "sgd"},
    }
    assert resolved["environmentPolicy"] == project["spec"]["environmentPolicy"]
    assert resolved["projectRevision"] == PROJECT_REVISION
    assert resolved["gitCommit"] == GIT_COMMIT
    assert resolved["preemptionAuthorized"] is True
    assert resolved["compiler"] == {
        "package": "experiment-queue",
        "version": PACKAGE_VERSION,
    }
    assert resolved["extensions"] == {}
    for mutable_policy_name in ("priority", "holdReason", "dependencies", "operator"):
        assert mutable_policy_name not in resolved

    policy = snapshot.submission_policy
    assert policy.to_document() == {
        "projectKey": PROJECT_KEY,
        "cardPath": "cards/EXP-001.yaml",
        "jobId": "train",
        "bindings": {
            "epochs": 20,
            "optimizer": {"name": "sgd"},
        },
        "priority": 50,
        "holdReason": "waiting for an input review",
        "dependencies": [11, 12],
        "operator": "test:operator",
        "preemptionAuthorized": True,
    }
    with pytest.raises(FrozenInstanceError):
        snapshot.git_commit = "b" * 40  # type: ignore[misc]


@pytest.mark.parametrize(
    "evidence_type",
    [SubmissionPolicy, SchemaEvidence, ExtensionSchemaEvidence, AdmissionSnapshot],
)
def test_trusted_admission_evidence_rejects_direct_construction(
    evidence_type: type[object],
) -> None:
    with pytest.raises(TypeError, match="trusted admission evidence.*compile_admission"):
        evidence_type()


def test_snapshot_does_not_alias_submission_or_returned_documents() -> None:
    submitted = submission()
    snapshot = compile_fixture(submitted=submitted)
    original_resolved = snapshot.resolved_json

    submitted.priority = -999
    submitted.operator = "changed:operator"
    submitted.dependencies.append(99)
    optimizer = submitted.bindings["optimizer"]
    assert isinstance(optimizer, dict)
    optimizer["name"] = "mutated"
    submitted.bindings["new"] = True

    policy_bindings = snapshot.submission_policy.bindings
    optimizer_copy = policy_bindings["optimizer"]
    assert isinstance(optimizer_copy, dict)
    optimizer_copy["name"] = "also-mutated"
    returned_resolved = snapshot.resolved_document
    returned_parameters = returned_resolved["parameters"]
    assert isinstance(returned_parameters, dict)
    returned_parameters["epochs"] = 999

    assert snapshot.submission_policy.priority == 50
    assert snapshot.submission_policy.operator == "test:operator"
    assert snapshot.submission_policy.dependencies == (11, 12)
    assert snapshot.submission_policy.bindings["optimizer"] == {"name": "sgd"}
    assert snapshot.resolved_document["parameters"]["epochs"] == 20
    assert snapshot.resolved_json == original_resolved


def test_compiler_revalidates_a_submission_after_mutation() -> None:
    submitted = submission()
    compile_fixture(submitted=submitted)
    submitted.priority = True

    with pytest.raises(AdmissionError, match="signed 64-bit integer"):
        compile_fixture(submitted=submitted)


def test_recompiling_observes_new_mutable_state_without_changing_first_snapshot() -> None:
    submitted = submission()
    first = compile_fixture(submitted=submitted)
    submitted.bindings["epochs"] = 77
    submitted.dependencies.append(99)
    submitted.operator = "changed:operator"
    submitted.preemption_authorized = False

    second = compile_fixture(submitted=submitted)

    assert first.resolved_document["parameters"]["epochs"] == 20
    assert first.submission_policy.dependencies == (11, 12)
    assert first.submission_policy.operator == "test:operator"
    assert first.submission_policy.preemption_authorized is True
    assert second.resolved_document["parameters"]["epochs"] == 77
    assert second.submission_policy.dependencies == (11, 12, 99)
    assert second.submission_policy.operator == "changed:operator"
    assert second.submission_policy.preemption_authorized is False


def test_submission_subclasses_cannot_supply_stateful_field_properties() -> None:
    class StatefulSubmission(Submission):
        reads = 0

        @property
        def preemption_authorized(self) -> bool:  # type: ignore[override]
            self.reads += 1
            return self.reads == 1

        @preemption_authorized.setter
        def preemption_authorized(self, _value: object) -> None:
            pass

    submitted = StatefulSubmission(
        project_key=PROJECT_KEY,
        card_path="cards/EXP-001.yaml",
        job_id="train",
        operator="test:operator",
        bindings={"epochs": 20},
        dependencies=[],
    )

    with pytest.raises(TypeError, match="exactly a Submission.*plain Submission"):
        compile_fixture(submitted=submitted)

    assert submitted.reads == 0


def test_mutable_policy_does_not_change_execution_digest() -> None:
    first = compile_fixture(submitted=submission())
    second = compile_fixture(
        submitted=submission(
            priority=-50,
            hold_reason=None,
            dependencies=[91],
            operator="different:operator",
        )
    )

    assert first.resolved_json == second.resolved_json
    assert first.resolved_sha256 == second.resolved_sha256
    assert first.submission_policy.to_document() != second.submission_policy.to_document()


def test_execution_bindings_and_preemption_change_the_execution_digest() -> None:
    base = compile_fixture(submitted=submission(preemption_authorized=False))
    rebound = compile_fixture(
        submitted=submission(
            preemption_authorized=False,
            bindings={"epochs": 21},
        )
    )
    preemptible = compile_fixture(submitted=submission(preemption_authorized=True))

    assert len({base.resolved_sha256, rebound.resolved_sha256, preemptible.resolved_sha256}) == 3


def test_every_pinned_execution_identity_changes_the_resolved_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = compile_fixture()
    changed_card = card_document()
    card_spec = changed_card["spec"]
    assert isinstance(card_spec, dict)
    jobs = card_spec["jobs"]
    assert isinstance(jobs, list)
    selected_job = jobs[0]
    assert isinstance(selected_job, dict)
    command = selected_job["command"]
    assert isinstance(command, dict)
    command["argv"] = ["python", "train-v2.py"]

    changed_revision = compile_fixture(project_revision="fixture-project:revision-8")
    changed_commit = compile_fixture(git_commit="b" * 40)
    changed_card_snapshot = compile_fixture(card=changed_card)
    monkeypatch.setattr(
        admission_module,
        "package_version_for",
        lambda distribution: "0.1.1-test",
    )
    changed_package = compile_fixture()
    variants = (
        changed_revision,
        changed_commit,
        changed_package,
        changed_card_snapshot,
    )

    assert all(snapshot.resolved_sha256 != base.resolved_sha256 for snapshot in variants)
    assert len({base.resolved_sha256, *(item.resolved_sha256 for item in variants)}) == 5


def test_compiler_version_cannot_be_supplied_by_the_admission_caller() -> None:
    arguments: dict[str, object] = {
        "project_source": source_bytes(project_document()),
        "card_source": source_bytes(card_document()),
        "submission": submission(),
        "project_revision": PROJECT_REVISION,
        "git_commit": GIT_COMMIT,
        "package_version": "forged-version",
    }

    with pytest.raises(TypeError, match="unexpected keyword argument 'package_version'"):
        compile_admission(**arguments)  # type: ignore[arg-type]


def test_compiler_version_requires_installed_package_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_metadata(distribution: str) -> str:
        raise admission_module.PackageNotFoundError(distribution)

    monkeypatch.setattr(admission_module, "package_version_for", missing_metadata)

    with pytest.raises(AdmissionError, match="metadata is unavailable.*install"):
        compile_fixture()


def test_binding_replaces_a_whole_value_without_changing_the_command() -> None:
    snapshot = compile_fixture(
        submitted=submission(
            bindings={"optimizer": {"precision": "bf16"}},
        )
    )

    assert snapshot.resolved_document["parameters"]["optimizer"] == {
        "precision": "bf16"
    }
    assert snapshot.command.to_document()["argv"][-1] == "optimizer.precision"


def test_unknown_binding_and_reserved_placeholder_fail_closed() -> None:
    with pytest.raises(AdmissionError, match="unknown names.*new-parameter"):
        compile_fixture(submitted=submission(bindings={"new-parameter": 1}))

    with pytest.raises(ValueError, match=r"\$binding|placeholder"):
        compile_fixture(
            submitted=submission(bindings={"epochs": {"$binding": "epochs"}})
        )


@pytest.mark.parametrize(
    ("bindings", "token", "path"),
    [
        ({"epochs": "${STEPS}"}, "${STEPS}", "$.epochs"),
        ({"epochs": "${"}, "${", "$.epochs"),
        ({"epochs": "{{}}"}, "{{}}", "$.epochs"),
        ({"epochs": "{{unfinished"}, "{{unfinished", "$.epochs"),
        (
            {"optimizer": {"{{precision}}": "bf16"}},
            "{{precision}}",
            "$.optimizer['{{precision}}']",
        ),
    ],
)
def test_binding_values_and_keys_reject_obvious_placeholder_tokens(
    bindings: dict[str, object],
    token: str,
    path: str,
) -> None:
    with pytest.raises(
        AdmissionError,
        match="unresolved placeholder token",
    ) as exc_info:
        compile_fixture(submitted=submission(bindings=bindings))

    assert token in str(exc_info.value)
    assert path in str(exc_info.value)


@pytest.mark.parametrize(
    ("bindings", "message"),
    [
        ({"epochs": (1, 2)}, "non-JSON type tuple"),
        ({1: "value"}, "object key 1 is not a string"),
        ({"epochs": 2**53}, "safe JSON domain"),
        ({"epochs": float("nan")}, "non-finite"),
        ({"epochs": "\ud800"}, "lone surrogate"),
    ],
)
def test_bindings_must_be_exact_canonical_json(
    bindings: object,
    message: str,
) -> None:
    with pytest.raises(AdmissionError, match=message):
        compile_fixture(submitted=submission(bindings=bindings))


def test_binding_snapshot_translates_concurrent_mutation_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_canonicalization(_value: object) -> bytes:
        raise RuntimeError("dictionary changed size during iteration")

    monkeypatch.setattr(
        admission_module,
        "canonical_json_bytes",
        fail_canonicalization,
    )
    with pytest.raises(
        AdmissionError,
        match=r"submission\.bindings changed.*stop mutating.*retry admission",
    ) as exc_info:
        compile_fixture()

    assert isinstance(exc_info.value.__cause__, RuntimeError)


@pytest.mark.parametrize(
    "card_path",
    [
        "/cards/EXP-001.yaml",
        "../cards/EXP-001.yaml",
        "cards/../EXP-001.yaml",
        "cards\\EXP-001.yaml",
        "C:/cards/EXP-001.yaml",
        "~/cards/EXP-001.yaml",
        "cards//EXP-001.yaml",
        "cards",
        "unapproved/EXP-001.yaml",
    ],
)
def test_card_path_must_be_portable_and_beneath_a_declared_root(
    card_path: str,
) -> None:
    with pytest.raises(AdmissionError, match="card_path|cardRoots"):
        compile_fixture(submitted=submission(card_path=card_path))


def test_card_source_name_must_match_the_detached_submission_path() -> None:
    with pytest.raises(
        AdmissionError,
        match=r"card_source_name.*must equal normalized submission\.card_path.*Git resolver",
    ):
        compile_admission(
            project_source=source_bytes(project_document()),
            card_source=source_bytes(card_document()),
            submission=submission(),
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            card_source_name="cards/different.yaml",
        )


@pytest.mark.parametrize(
    "project_source_name",
    ["/config/project.yaml", "config/../project.yaml"],
)
def test_project_source_name_must_be_a_portable_git_tree_path(
    project_source_name: str,
) -> None:
    with pytest.raises(
        AdmissionError,
        match=r"project_source_name.*portable project-relative POSIX path",
    ):
        compile_admission(
            project_source=source_bytes(project_document()),
            card_source=source_bytes(card_document()),
            submission=submission(),
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            project_source_name=project_source_name,
        )


def test_nested_declared_card_root_is_accepted() -> None:
    snapshot = compile_fixture(
        submitted=submission(card_path="campaigns/approved/EXP-001.yaml")
    )
    assert snapshot.submission_policy.card_path == "campaigns/approved/EXP-001.yaml"


@pytest.mark.parametrize(
    ("dependencies", "message"),
    [
        ([True], "positive signed 64-bit"),
        ([0], "positive signed 64-bit"),
        ([-1], "positive signed 64-bit"),
        ([2**63], "positive signed 64-bit"),
        ([1, 1], "duplicate queue item ID 1"),
        ((1, 2), "must be a list"),
    ],
)
def test_dependencies_are_unique_positive_queue_item_ids(
    dependencies: object,
    message: str,
) -> None:
    with pytest.raises(AdmissionError, match=message):
        compile_fixture(submitted=submission(dependencies=dependencies))


@pytest.mark.parametrize("priority", [True, -(2**63) - 1, 2**63])
def test_priority_is_a_non_boolean_signed_64_bit_integer(priority: object) -> None:
    with pytest.raises(AdmissionError, match="signed 64-bit integer"):
        compile_fixture(submitted=submission(priority=priority))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"operator": ""}, "operator must not be empty"),
        ({"operator": " operator"}, "surrounding whitespace"),
        ({"operator": "x" * 257}, "256 characters or fewer"),
        ({"hold_reason": ""}, "hold_reason must not be empty"),
        ({"hold_reason": "invalid\nreason"}, "control or line"),
        ({"project_key": "Wrong-Key"}, "project_key is invalid"),
        ({"preemption_authorized": 1}, "must be true or false"),
    ],
)
def test_mutable_submission_fields_are_revalidated(
    change: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(AdmissionError, match=message):
        compile_fixture(submitted=submission(**change))


@pytest.mark.parametrize(
    ("revision", "commit", "message"),
    [
        ("", GIT_COMMIT, "project_revision must not be empty"),
        ("revision\n2", GIT_COMMIT, "control or line"),
        ("x" * 257, GIT_COMMIT, "256 characters or fewer"),
        (PROJECT_REVISION, "abcd", "full 40- or 64-character"),
        (PROJECT_REVISION, "g" * 40, "full 40- or 64-character"),
    ],
)
def test_revision_and_git_identity_are_bounded_and_actionable(
    revision: str,
    commit: str,
    message: str,
) -> None:
    with pytest.raises(AdmissionError, match=message):
        compile_admission(
            project_source=source_bytes(project_document()),
            card_source=source_bytes(card_document()),
            submission=submission(),
            project_revision=revision,
            git_commit=commit,
        )


def test_uppercase_git_input_is_deliberately_normalized() -> None:
    uppercase = compile_fixture(git_commit=GIT_COMMIT.upper())
    lowercase = compile_fixture(git_commit=GIT_COMMIT)
    assert uppercase.git_commit == GIT_COMMIT
    assert uppercase.resolved_sha256 == lowercase.resolved_sha256


def test_unknown_job_and_project_or_card_mismatch_fail_closed() -> None:
    with pytest.raises(
        AdmissionError,
        match=r"submission\.job_id 'missing'.*choose one of.*train.*analyze",
    ) as exc_info:
        compile_fixture(submitted=submission(job_id="missing"))
    assert exc_info.value.__cause__ is not None
    with pytest.raises(AdmissionError, match="does not match Project key"):
        compile_fixture(submitted=submission(project_key="another-project"))

    card = card_document()
    metadata = card["metadata"]
    assert isinstance(metadata, dict)
    metadata["projectKey"] = "another-project"
    with pytest.raises(ValueError, match="project key|projectKey"):
        compile_fixture(card=card)


def test_preemption_authorization_requires_the_selected_job_capability() -> None:
    with pytest.raises(AdmissionError, match="requires selected job.*cooperativeYield"):
        compile_fixture(
            submitted=submission(
                job_id="analyze",
                bindings={},
                preemption_authorized=True,
            )
        )

    snapshot = compile_fixture(
        submitted=submission(
            job_id="analyze",
            bindings={},
            preemption_authorized=False,
        )
    )
    assert snapshot.resolved_document["job"]["id"] == "analyze"
    assert "train" not in snapshot.resolved_json.decode()


def test_raw_presentation_changes_do_not_change_normalized_or_resolved_identity() -> None:
    project = project_document()
    card = card_document()
    compact_project = source_bytes(project)
    pretty_project = source_bytes(project, pretty=True)
    compact_card = source_bytes(card)
    pretty_card = source_bytes(card, pretty=True)

    compact = compile_admission(
        project_source=compact_project,
        card_source=compact_card,
        submission=submission(),
        project_revision=PROJECT_REVISION,
        git_commit=GIT_COMMIT,
    )
    pretty = compile_admission(
        project_source=pretty_project,
        card_source=pretty_card,
        submission=submission(),
        project_revision=PROJECT_REVISION,
        git_commit=GIT_COMMIT,
    )

    assert compact.project_source_sha256 != pretty.project_source_sha256
    assert compact.card_source_sha256 != pretty.card_source_sha256
    assert compact.project_normalized_json == pretty.project_normalized_json
    assert compact.card_normalized_json == pretty.card_normalized_json
    assert compact.resolved_sha256 == pretty.resolved_sha256


def extension_schema_fixture() -> tuple[dict[str, object], bytes]:
    """Return a Project reference and matching strict extension schema bytes."""

    schema: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:fixture-project:extensions:v1",
        "type": "object",
        "additionalProperties": False,
        "required": ["project", "card", "jobs"],
        "properties": {
            "project": {
                "type": "object",
                "required": ["dataset"],
                "properties": {"dataset": {"type": "string"}},
                "additionalProperties": False,
            },
            "card": {
                "type": "object",
                "required": ["campaign"],
                "properties": {"campaign": {"type": "string"}},
                "additionalProperties": False,
            },
            "jobs": {
                "type": "object",
                "required": ["train"],
                "properties": {
                    "train": {
                        "type": "object",
                        "required": ["tracker"],
                        "properties": {"tracker": {"type": "string"}},
                        "additionalProperties": False,
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    schema_source = source_bytes(schema, pretty=True)
    reference = {
        "path": "schemas/fixture-extensions.json",
        "sha256": canonical_json_sha256(schema),
    }
    return reference, schema_source


def test_extension_schema_and_namespaced_payloads_are_pinned() -> None:
    reference, schema_source = extension_schema_fixture()
    project = project_document(
        extension_schema=reference,
        extensions={PROJECT_KEY: {"dataset": "dataset-v1"}},
    )
    card = card_document(
        card_extensions={PROJECT_KEY: {"campaign": "baseline"}},
        job_extensions={PROJECT_KEY: {"tracker": "run-001"}},
    )

    snapshot = compile_fixture(
        project=project,
        card=card,
        extension_schema_source=schema_source,
    )

    evidence = snapshot.extension_schema
    assert evidence is not None
    assert evidence.source == schema_source
    assert evidence.source_sha256 == sha256_bytes(schema_source)
    assert evidence.canonical_sha256 == reference["sha256"]
    assert evidence.canonical_json == canonical_json_bytes(
        json.loads(schema_source.decode())
    )
    assert evidence.schema_id == "urn:fixture-project:extensions:v1"
    assert evidence.reference_path == "schemas/fixture-extensions.json"
    assert snapshot.resolved_document["extensions"] == {
        "project": {"dataset": "dataset-v1"},
        "card": {"campaign": "baseline"},
        "jobs": {"train": {"tracker": "run-001"}},
    }
    assert snapshot.resolved_document["extensionSchema"] == {
        "path": "schemas/fixture-extensions.json",
        "schemaId": "urn:fixture-project:extensions:v1",
        "sha256": reference["sha256"],
    }


def test_extension_schema_source_must_match_the_project_contract() -> None:
    reference, schema_source = extension_schema_fixture()
    project = project_document(extension_schema=reference)
    with pytest.raises(ValueError, match="declared extension schema.*missing"):
        compile_fixture(project=project)

    with pytest.raises(ValueError, match="does not declare|without.*reference|unexpected"):
        compile_fixture(extension_schema_source=schema_source)


@pytest.mark.parametrize("source_field", ["project_source", "card_source"])
def test_authoring_sources_must_be_immutable_bytes(source_field: str) -> None:
    arguments: dict[str, object] = {
        "project_source": source_bytes(project_document()),
        "card_source": source_bytes(card_document()),
        "submission": submission(),
        "project_revision": PROJECT_REVISION,
        "git_commit": GIT_COMMIT,
    }
    arguments[source_field] = bytearray(arguments[source_field])
    with pytest.raises(TypeError, match=f"{source_field} must be immutable bytes"):
        compile_admission(**arguments)  # type: ignore[arg-type]


def test_extension_schema_source_rejects_mutable_bytearray() -> None:
    _reference, schema_source = extension_schema_fixture()
    with pytest.raises(TypeError, match="extension_schema_source must be immutable bytes"):
        compile_admission(
            project_source=source_bytes(project_document()),
            card_source=source_bytes(card_document()),
            submission=submission(),
            project_revision=PROJECT_REVISION,
            git_commit=GIT_COMMIT,
            extension_schema_source=bytearray(schema_source),  # type: ignore[arg-type]
        )
