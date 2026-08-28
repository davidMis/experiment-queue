"""Verify validated, immutable Project and ExperimentCard authoring models."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError
import json
from typing import Mapping, cast

import pytest

import experiment_queue.authoring as authoring
from experiment_queue.authoring import (
    ArgvCommand,
    ArtifactType,
    AuthoringValidationError,
    EnvironmentInheritance,
    ExperimentCard,
    ExtensionSchemaReference,
    JobRole,
    Project,
    ShellCommand,
    VolumeAccess,
    WrapperCommand,
    is_reserved_environment_variable,
    validate_card_for_project,
)
from experiment_queue.protocols import (
    COOPERATIVE_YIELD_RECEIPT_V1,
    COOPERATIVE_YIELD_REQUEST_V1,
)


def project_document() -> dict[str, object]:
    """Return a complete Project/v1 exercising all typed optional views."""

    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "example-project",
            "displayName": "Example Project",
            "description": "Portable test project.",
        },
        "spec": {
            "cardRoots": ["experiments", "docs/cards"],
            "volumes": [
                {
                    "name": "scratch",
                    "access": "readWrite",
                    "required": True,
                    "description": "Durable experiment output.",
                },
                {"name": "datasets", "access": "readOnly"},
            ],
            "environments": [
                {"name": "training", "description": "Training environment."},
                {"name": "analysis"},
            ],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": ["TMPDIR", "LANG"],
            },
            "supportedProtocols": [
                COOPERATIVE_YIELD_REQUEST_V1.document_identity(),
                COOPERATIVE_YIELD_RECEIPT_V1.document_identity(),
            ],
            "extensionSchema": {
                "path": "schemas/example-extension.schema.json",
                "sha256": "1" * 64,
            },
        },
        "extensions": {"example-project": {"tracker": "local"}},
    }


def simple_card_document() -> dict[str, object]:
    """Return a complete single-job ExperimentCard/v1."""

    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "example-project",
            "experimentId": "EXP-001",
            "title": "Train a model",
            "description": "Simple authoring model test.",
            "tags": ["training", "smoke"],
        },
        "spec": {
            "parameters": {
                "steps": 10,
                "optimizer": {"name": "adam", "betas": [0.9, 0.999]},
            },
            "jobs": [
                {
                    "id": "train",
                    "role": "independent",
                    "description": "Run one training process.",
                    "environment": "training",
                    "workingDirectory": "training",
                    "command": {
                        "type": "argv",
                        "argv": [".venv/bin/python", "scripts/train.py", ""],
                    },
                    "parameters": {"seed": 7},
                    "resources": {
                        "gpus": 1,
                        "cpus": 4,
                        "memoryBytes": 1024,
                        "wallTimeSeconds": 60,
                    },
                    "artifacts": [
                        {
                            "name": "run-output",
                            "root": "scratch",
                            "path": "runs/EXP-001",
                            "type": "directory",
                            "required": True,
                        }
                    ],
                    "extensions": {"example-project": {"queueClass": "test"}},
                }
            ],
            "provenance": {
                "inputs": [
                    {
                        "name": "dataset",
                        "source": "dataset-v3",
                        "sha256": "2" * 64,
                    }
                ],
                "notes": "Input is immutable.",
            },
        },
        "extensions": {"example-project": {"trackerRun": "EXP-001"}},
    }


def coordinator_worker_card_document() -> dict[str, object]:
    """Return a multi-job card with all command and cooperative-yield variants."""

    document = simple_card_document()
    spec = cast(dict[str, object], document["spec"])
    spec["jobs"] = [
        {
            "id": "coordinator",
            "role": "coordinator",
            "environment": "analysis",
            "command": {
                "type": "wrapper",
                "path": "scripts/coordinator.sh",
                "args": ["--workers", "1"],
            },
            "resources": {"gpus": 0},
        },
        {
            "id": "worker-1",
            "role": "worker",
            "environment": "training",
            "command": {
                "type": "shell",
                "script": "exec ./legacy-worker --rank 1",
                "compatibilityReason": "Temporary legacy launcher.",
            },
            "artifacts": [
                {
                    "name": "checkpoint",
                    "root": "scratch",
                    "path": "runs/EXP-001/checkpoint.bin",
                    "type": "file",
                }
            ],
            "capabilities": {
                "cooperativeYield": {
                    "requestProtocol": (
                        COOPERATIVE_YIELD_REQUEST_V1.document_identity()
                    ),
                    "receiptProtocol": (
                        COOPERATIVE_YIELD_RECEIPT_V1.document_identity()
                    ),
                    "checkpointArtifacts": ["checkpoint"],
                }
            },
        },
    ]
    return document


def test_simple_models_expose_typed_nested_values_and_commands() -> None:
    project = Project.from_document(project_document())
    card = ExperimentCard.from_document(simple_card_document())

    assert project.key == "example-project"
    assert project.card_roots == ("experiments", "docs/cards")
    assert project.volumes[0].access is VolumeAccess.READ_WRITE
    assert project.volumes[1].access is VolumeAccess.READ_ONLY
    assert project.environment_policy.inherit is EnvironmentInheritance.ALLOWLIST
    assert project.environment_policy.allow_variables == (
        "TMPDIR",
        "LANG",
    )
    assert isinstance(project.extension_schema, ExtensionSchemaReference)
    assert project.extension_schema.path == "schemas/example-extension.schema.json"
    assert project.extension_schema.sha256 == "1" * 64

    assert card.project_key == project.key
    assert card.experiment_id == "EXP-001"
    assert card.tags == ("training", "smoke")
    job = card.job("train")
    assert job.role is JobRole.INDEPENDENT
    assert isinstance(job.command, ArgvCommand)
    assert job.command.type == "argv"
    assert job.command.argv[-1] == ""
    assert job.resources is not None
    assert job.resources.gpus == 1
    assert job.artifacts[0].type is ArtifactType.DIRECTORY
    assert card.provenance is not None
    assert card.provenance.inputs[0].source == "dataset-v3"
    validate_card_for_project(project, card)


def test_coordinator_worker_models_expose_commands_and_yield_capability() -> None:
    project = Project.from_document(project_document())
    source = coordinator_worker_card_document()
    card = ExperimentCard.from_document(source)

    coordinator = card.job("coordinator")
    worker = card.job("worker-1")
    assert coordinator.role is JobRole.COORDINATOR
    assert isinstance(coordinator.command, WrapperCommand)
    assert coordinator.command.args == ("--workers", "1")
    assert worker.role is JobRole.WORKER
    assert isinstance(worker.command, ShellCommand)
    assert worker.command.compatibility_reason == "Temporary legacy launcher."
    assert worker.capabilities is not None
    capability = worker.capabilities.cooperative_yield
    assert capability is not None
    assert capability.request_protocol == COOPERATIVE_YIELD_REQUEST_V1
    assert capability.receipt_protocol == COOPERATIVE_YIELD_RECEIPT_V1
    assert capability.checkpoint_artifacts == ("checkpoint",)
    assert worker.command.to_document() == cast(
        list[dict[str, object]], cast(dict[str, object], source["spec"])["jobs"]
    )[1]["command"]
    validate_card_for_project(project, card)


def test_from_yaml_and_to_document_preserve_exact_normalized_semantics() -> None:
    project_source = json.dumps(project_document()).encode("utf-8")
    card_source = json.dumps(simple_card_document()).encode("utf-8")
    expected_project = project_document()
    expected_card = simple_card_document()

    project = Project.from_yaml(project_source, source_name="project.yaml")
    card = ExperimentCard.from_yaml(card_source, source_name="EXP-001.yaml")

    assert project.to_document() == expected_project
    assert card.to_document() == expected_card
    assert Project.from_document(project.to_document()).to_document() == expected_project
    assert ExperimentCard.from_document(card.to_document()).to_document() == expected_card

    first = card.to_document()
    second = card.to_document()
    assert first is not second
    first_spec = cast(dict[str, object], first["spec"])
    second_spec = cast(dict[str, object], second["spec"])
    assert first_spec is not second_spec
    first_parameters = cast(dict[str, object], first_spec["parameters"])
    second_parameters = cast(dict[str, object], second_spec["parameters"])
    assert first_parameters is not second_parameters
    cast(dict[str, object], first_parameters["optimizer"])["name"] = "changed"
    assert cast(dict[str, object], second_parameters["optimizer"])["name"] == "adam"
    assert card.to_document() == expected_card


def test_models_detach_input_and_are_deeply_immutable() -> None:
    project_source = project_document()
    card_source = simple_card_document()
    expected_project = deepcopy(project_source)
    expected_card = deepcopy(card_source)
    project = Project.from_document(project_source)
    card = ExperimentCard.from_document(card_source)

    cast(dict[str, object], project_source["metadata"])["key"] = "changed"
    cast(dict[str, object], card_source["spec"])["parameters"] = {"changed": True}
    assert project.to_document() == expected_project
    assert card.to_document() == expected_card

    with pytest.raises(FrozenInstanceError):
        project.key = "changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        card.job("train").environment = "analysis"  # type: ignore[misc]
    with pytest.raises(TypeError):
        card.parameters["steps"] = 20  # type: ignore[index]
    optimizer = card.parameters["optimizer"]
    assert isinstance(optimizer, Mapping)
    with pytest.raises(TypeError):
        optimizer["name"] = "sgd"  # type: ignore[index]
    assert optimizer["betas"] == (0.9, 0.999)
    with pytest.raises(TypeError, match="from_document"):
        Project()
    with pytest.raises(TypeError, match="from_document"):
        ExperimentCard()


def test_root_subclasses_cannot_diverge_typed_fields_from_validated_documents() -> None:
    class EvilProject(Project):
        @property
        def key(self) -> str:  # type: ignore[override]
            return "another-project"

        @key.setter
        def key(self, _value: object) -> None:
            pass

    class EvilCard(ExperimentCard):
        @property
        def project_key(self) -> str:  # type: ignore[override]
            return "example-project"

        @project_key.setter
        def project_key(self, _value: object) -> None:
            pass

    with pytest.raises(TypeError, match="exactly Project.*typed fields"):
        EvilProject.from_document(project_document())
    with pytest.raises(TypeError, match="exactly Project.*typed fields"):
        EvilProject.from_yaml(json.dumps(project_document()).encode())
    with pytest.raises(TypeError, match="exactly ExperimentCard.*typed fields"):
        EvilCard.from_document(simple_card_document())
    with pytest.raises(TypeError, match="exactly ExperimentCard.*typed fields"):
        EvilCard.from_yaml(json.dumps(simple_card_document()).encode())

    forged_project = object.__new__(EvilProject)
    forged_card = object.__new__(EvilCard)
    with pytest.raises(TypeError, match="project must be exactly a Project"):
        validate_card_for_project(forged_project, forged_card)


def test_nested_views_cannot_bypass_validated_root_construction() -> None:
    mutable_document = {"path": "schemas/unvalidated.json"}

    with pytest.raises(TypeError, match="validated view.*from_document"):
        ExtensionSchemaReference(  # type: ignore[call-arg]
            "schemas/unvalidated.json",
            None,
            mutable_document,
        )
    with pytest.raises(TypeError, match="validated view.*from_document"):
        ArgvCommand(("python",), {"type": "argv", "argv": ["python"]})  # type: ignore[call-arg]


def test_owned_snapshot_is_validated_before_later_source_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = project_document()
    original_validate = authoring.validate_bundled_document

    def validate_then_mutate(
        protocol: object,
        owned_document: Mapping[str, object],
    ) -> None:
        assert owned_document is not source
        original_validate(protocol, owned_document)  # type: ignore[arg-type]
        metadata = cast(dict[str, object], source["metadata"])
        metadata["key"] = "mutated-after-validation"

    monkeypatch.setattr(authoring, "validate_bundled_document", validate_then_mutate)

    project = Project.from_document(source)

    assert project.key == "example-project"
    assert cast(dict[str, object], project.to_document()["metadata"])["key"] == (
        "example-project"
    )


def test_card_job_lookup_failure_is_actionable() -> None:
    card = ExperimentCard.from_document(simple_card_document())

    with pytest.raises(
        AuthoringValidationError,
        match=r"no job 'missing'.*'train'",
    ):
        card.job("missing")


@pytest.mark.parametrize("location", ["card", "job"])
def test_binding_placeholder_objects_are_rejected_from_core_parameters(
    location: str,
) -> None:
    document = simple_card_document()
    spec = cast(dict[str, object], document["spec"])
    if location == "card":
        parameters = cast(dict[str, object], spec["parameters"])
        parameters["learningRate"] = {
            "nested": [{"$binding": "submission.learningRate"}]
        }
    else:
        jobs = cast(list[dict[str, object]], spec["jobs"])
        jobs[0]["parameters"] = {"seed": {"$binding": "submission.seed"}}

    with pytest.raises(
        AuthoringValidationError,
        match=r"unsupported '\$binding'.*whole-parameter submission overrides",
    ):
        ExperimentCard.from_document(document)


def test_binding_key_remains_available_in_namespaced_extensions() -> None:
    document = simple_card_document()
    document["extensions"] = {
        "example-project": {"$binding": "extension-owned-literal"}
    }

    card = ExperimentCard.from_document(document)

    assert card.to_document() == document


@pytest.mark.parametrize(
    ("location", "token", "path_fragment"),
    [
        ("card-value", "${learningRate}", "spec.parameters"),
        ("card-value", "${", "spec.parameters"),
        ("card-value", "{{}}", "spec.parameters"),
        ("card-value", "{{unfinished", "spec.parameters"),
        ("card-key", "{{parameterName}}", "spec.parameters"),
        ("job-value", "{{ seed }}", "jobs[0].parameters"),
        ("argv", "--steps=${steps}", "command.argv[1]"),
        ("wrapper-path", "scripts/{{runner}}.sh", "command.path"),
        ("wrapper-arg", "${workers}", "command.args[0]"),
        ("working-directory", "runs/${experiment}", "workingDirectory"),
        ("artifact-path", "runs/{{experiment}}", "artifacts[0].path"),
    ],
)
def test_obvious_placeholder_tokens_fail_in_parameters_and_execution_fields(
    location: str,
    token: str,
    path_fragment: str,
) -> None:
    document = simple_card_document()
    spec = cast(dict[str, object], document["spec"])
    jobs = cast(list[dict[str, object]], spec["jobs"])
    job = jobs[0]
    if location == "card-value":
        cast(dict[str, object], spec["parameters"])["learningRate"] = token
    elif location == "card-key":
        cast(dict[str, object], spec["parameters"])[token] = 1
    elif location == "job-value":
        cast(dict[str, object], job["parameters"])["seed"] = token
    elif location == "argv":
        job["command"] = {"type": "argv", "argv": ["python", token]}
    elif location == "wrapper-path":
        job["command"] = {"type": "wrapper", "path": token}
    elif location == "wrapper-arg":
        job["command"] = {
            "type": "wrapper",
            "path": "scripts/run.sh",
            "args": [token],
        }
    elif location == "working-directory":
        job["workingDirectory"] = token
    else:
        artifacts = cast(list[dict[str, object]], job["artifacts"])
        artifacts[0]["path"] = token

    with pytest.raises(
        AuthoringValidationError,
        match="unresolved placeholder token",
    ) as exc_info:
        ExperimentCard.from_document(document)

    assert path_fragment in str(exc_info.value)


def test_shell_and_nonexecution_text_do_not_use_authoring_interpolation_rules() -> None:
    document = simple_card_document()
    metadata = cast(dict[str, object], document["metadata"])
    metadata["description"] = "Document literal ${description}."
    spec = cast(dict[str, object], document["spec"])
    provenance = cast(dict[str, object], spec["provenance"])
    provenance["notes"] = "Scientific note {{not-a-template}}."
    jobs = cast(list[dict[str, object]], spec["jobs"])
    jobs[0]["command"] = {
        "type": "shell",
        "script": "printf '%s' \"${HOME}\" # {{shell-owned}}",
        "compatibilityReason": "Legacy shell expansion is required.",
    }
    document["extensions"] = {
        "example-project": {"template": "${extension-owned}"}
    }

    card = ExperimentCard.from_document(document)

    assert isinstance(card.job("train").command, ShellCommand)
    assert card.to_document() == document


def test_structural_and_local_semantic_failures_use_authoring_error() -> None:
    document = simple_card_document()
    jobs = cast(
        list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"]
    )
    resources = cast(dict[str, object], jobs[0]["resources"])
    resources["gpus"] = 2
    with pytest.raises(
        AuthoringValidationError,
        match=r"ExperimentCard is invalid at .*gpus.*bundled ExperimentCard/v1 schema",
    ):
        ExperimentCard.from_document(document)

    document = simple_card_document()
    jobs = cast(
        list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"]
    )
    duplicate_job = deepcopy(jobs[0])
    duplicate_job["description"] = "Same logical id, distinct document value."
    jobs.append(duplicate_job)
    with pytest.raises(
        AuthoringValidationError,
        match=r"duplicate id 'train'.*internally consistent",
    ):
        ExperimentCard.from_document(document)


@pytest.mark.parametrize(
    "variable",
    ["CUDA_VISIBLE_DEVICES", "EXPERIMENT_QUEUE_ITEM_ID", "EXPERIMENT_QUEUE_CUSTOM"],
)
def test_project_rejects_queue_reserved_environment_allowlist(
    variable: str,
) -> None:
    document = project_document()
    spec = cast(dict[str, object], document["spec"])
    policy = cast(dict[str, object], spec["environmentPolicy"])
    policy["allowVariables"] = ["LANG", variable]

    assert is_reserved_environment_variable(variable)
    with pytest.raises(
        AuthoringValidationError,
        match=rf"environmentPolicy\.allowVariables.*{variable}.*queue service owns",
    ):
        Project.from_document(document)

    assert not is_reserved_environment_variable("LANG")


def test_validate_card_for_project_rejects_mismatched_project_key() -> None:
    document = simple_card_document()
    metadata = cast(dict[str, object], document["metadata"])
    metadata["projectKey"] = "another-project"

    with pytest.raises(
        AuthoringValidationError,
        match=r"projectKey 'another-project'.*selected Project key.*example-project",
    ):
        validate_card_for_project(
            Project.from_document(project_document()),
            ExperimentCard.from_document(document),
        )


def test_validate_card_for_project_rejects_undeclared_environment() -> None:
    document = simple_card_document()
    jobs = cast(
        list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"]
    )
    jobs[0]["environment"] = "missing-environment"

    with pytest.raises(
        AuthoringValidationError,
        match=r"job 'train'.*environment 'missing-environment'.*spec.environments",
    ):
        validate_card_for_project(
            Project.from_document(project_document()),
            ExperimentCard.from_document(document),
        )


def test_validate_card_for_project_rejects_undeclared_artifact_root() -> None:
    document = simple_card_document()
    jobs = cast(
        list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"]
    )
    artifacts = cast(list[dict[str, object]], jobs[0]["artifacts"])
    artifacts[0]["root"] = "missing-root"

    with pytest.raises(
        AuthoringValidationError,
        match=r"artifact 'run-output'.*job 'train'.*root 'missing-root'.*spec.volumes",
    ):
        validate_card_for_project(
            Project.from_document(project_document()),
            ExperimentCard.from_document(document),
        )


def test_validate_card_for_project_rejects_read_only_artifact_root() -> None:
    document = simple_card_document()
    jobs = cast(
        list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"]
    )
    artifacts = cast(list[dict[str, object]], jobs[0]["artifacts"])
    artifacts[0]["root"] = "datasets"

    with pytest.raises(
        AuthoringValidationError,
        match=(
            r"artifact 'run-output'.*job 'train'.*readOnly.*datasets.*"
            r"readWrite.*provenance.inputs"
        ),
    ):
        validate_card_for_project(
            Project.from_document(project_document()),
            ExperimentCard.from_document(document),
        )


@pytest.mark.parametrize(
    ("supported_protocol", "missing_field", "missing_identity"),
    [
        (
            COOPERATIVE_YIELD_RECEIPT_V1.document_identity(),
            "requestProtocol",
            "CooperativeYieldRequest/v1",
        ),
        (
            COOPERATIVE_YIELD_REQUEST_V1.document_identity(),
            "receiptProtocol",
            "CooperativeYieldReceipt/v1",
        ),
    ],
)
def test_validate_card_for_project_rejects_undeclared_yield_protocols(
    supported_protocol: dict[str, str],
    missing_field: str,
    missing_identity: str,
) -> None:
    project_source = project_document()
    project_spec = cast(dict[str, object], project_source["spec"])
    project_spec["supportedProtocols"] = [supported_protocol]

    with pytest.raises(
        AuthoringValidationError,
        match=rf"job 'worker-1'.*{missing_field}.*{missing_identity}.*supportedProtocols",
    ):
        validate_card_for_project(
            Project.from_document(project_source),
            ExperimentCard.from_document(coordinator_worker_card_document()),
        )
