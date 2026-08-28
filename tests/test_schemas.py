"""Verify immutable packaged Project/v1 and ExperimentCard/v1 schemas."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from importlib.resources import files
import json
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
import pytest
from referencing.exceptions import Unresolvable

from experiment_queue.protocols import EXPERIMENT_CARD_V1, PROJECT_V1, ProtocolVersion
import experiment_queue.schema_registry as schema_registry
from experiment_queue.schema_registry import (
    BUNDLED_SCHEMAS,
    BundledSchemaError,
    EXPERIMENT_CARD_V1_SCHEMA,
    JSON_SCHEMA_DIALECT,
    PROJECT_V1_SCHEMA,
    SemanticValidationError,
    bundled_schema_registry,
    editor_schema_bytes,
    load_bundled_schema,
    offline_validator,
    schema_canonical_bytes,
    schema_sha256,
    validate_bundled_document,
)
from experiment_queue.serialization import (
    CanonicalJSONError,
    load_strict_yaml,
    sha256_bytes,
)


def valid_project() -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {
            "key": "flowers-3d-helmholtz",
            "displayName": "Flowers 3D Helmholtz",
            "description": "Portable scientific project configuration.",
        },
        "spec": {
            "cardRoots": ["docs/experiments"],
            "volumes": [
                {
                    "name": "scratch",
                    "access": "readWrite",
                    "required": True,
                }
            ],
            "environments": [{"name": "training"}],
            "environmentPolicy": {"inherit": "none", "allowVariables": []},
            "supportedProtocols": [
                {
                    "apiVersion": "experiment-queue/v1",
                    "kind": "RunnerReceipt",
                }
            ],
            "extensionSchema": {
                "path": "schemas/flowers-extension.schema.json",
                "sha256": "1" * 64,
            },
        },
        "extensions": {"flowers-3d-helmholtz": {"tracker": "wandb"}},
    }


def valid_card() -> dict[str, object]:
    return {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": "flowers-3d-helmholtz",
            "experimentId": "WCG-023",
            "title": "Train the corrected model",
            "tags": ["training", "helmholtz"],
        },
        "spec": {
            "parameters": {"steps": 10, "learningRate": 0.0001},
            "jobs": [
                {
                    "id": "train",
                    "role": "independent",
                    "environment": "training",
                    "workingDirectory": "training",
                    "command": {
                        "type": "argv",
                        "argv": [".venv/bin/python", "scripts/train.py"],
                    },
                    "resources": {"gpus": 1, "cpus": 4},
                    "artifacts": [
                        {
                            "name": "run-output",
                            "root": "scratch",
                            "path": "runs/WCG-023",
                            "type": "directory",
                            "required": True,
                        }
                    ],
                }
            ],
            "provenance": {
                "inputs": [
                    {
                        "name": "dataset",
                        "source": "dataset-v3",
                        "sha256": "2" * 64,
                    }
                ]
            },
        },
        "extensions": {"flowers-3d-helmholtz": {"wandbProject": "flowers"}},
    }


def test_bundled_schema_catalog_has_fixed_ids_and_canonical_digests() -> None:
    assert set(BUNDLED_SCHEMAS) == {PROJECT_V1, EXPERIMENT_CARD_V1}
    assert PROJECT_V1_SCHEMA.sha256 == (
        "f654e4cd57f6939113c3e8ec32093d00b318b84dd94b653d7d37300e6bcfb23e"
    )
    assert EXPERIMENT_CARD_V1_SCHEMA.sha256 == (
        "0b5c9091d71c727428f92de45818fd6e4eb6d60f0e3a9a81d772255908794e83"
    )

    for protocol, descriptor in BUNDLED_SCHEMAS.items():
        document = load_bundled_schema(protocol)
        assert document["$schema"] == JSON_SCHEMA_DIALECT
        assert document["$id"] == descriptor.schema_id
        Draft202012Validator.check_schema(document)
        assert schema_sha256(protocol) == descriptor.sha256
        assert sha256_bytes(schema_canonical_bytes(protocol)) == descriptor.sha256


def test_schema_resources_load_through_installed_package_api() -> None:
    package = files("experiment_queue.schema_resources")

    for descriptor in BUNDLED_SCHEMAS.values():
        resource = package.joinpath(descriptor.resource_name)
        assert resource.is_file()
        assert json.loads(resource.read_text(encoding="utf-8"))["$id"] == (
            descriptor.schema_id
        )


def test_editor_schema_export_is_authenticated_readable_json() -> None:
    for protocol in BUNDLED_SCHEMAS:
        exported = editor_schema_bytes(protocol)
        assert exported.endswith(b"\n")
        assert json.loads(exported) == load_bundled_schema(protocol)
        assert exported != schema_canonical_bytes(protocol)


def test_project_and_simple_card_validate() -> None:
    validate_bundled_document(PROJECT_V1, valid_project())
    validate_bundled_document(EXPERIMENT_CARD_V1, valid_card())


def test_authored_yaml_project_runs_through_loader_and_bundled_schema() -> None:
    source = b"""\
apiVersion: experiment-queue/v1
kind: Project
metadata:
  key: example-project
  displayName: Example Project
spec:
  cardRoots: [experiments]
  volumes: []
  environments:
    - name: default
  environmentPolicy:
    inherit: none
    allowVariables: []
  supportedProtocols: []
"""
    document = load_strict_yaml(source, source_name="project.yaml")
    assert isinstance(document, dict)

    validate_bundled_document(PROJECT_V1, document)


def test_explicit_coordinator_and_worker_card_validates() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    spec["jobs"] = [
        {
            "id": "coordinator",
            "role": "coordinator",
            "environment": "training",
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
                "compatibilityReason": "temporary legacy launcher compatibility",
            },
                "resources": {"gpus": 1},
                "artifacts": [
                    {
                        "name": "run-output",
                        "root": "scratch",
                        "path": "runs/WCG-023-worker/checkpoint.bin",
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
                    "checkpointArtifacts": ["run-output"],
                }
            },
        },
    ]

    validate_bundled_document(EXPERIMENT_CARD_V1, card)


@pytest.mark.parametrize(
    ("protocol", "factory", "container_key", "unknown_key"),
    [
        (PROJECT_V1, valid_project, None, "checkoutPath"),
        (PROJECT_V1, valid_project, "spec", "credential"),
        (EXPERIMENT_CARD_V1, valid_card, None, "priority"),
        (EXPERIMENT_CARD_V1, valid_card, "spec", "dependencies"),
    ],
)
def test_unknown_core_and_mutable_submission_fields_fail_closed(
    protocol: ProtocolVersion,
    factory: Callable[[], dict[str, object]],
    container_key: str | None,
    unknown_key: str,
) -> None:
    document = factory()
    container = document if container_key is None else document[container_key]
    assert isinstance(container, dict)
    container[unknown_key] = "forbidden"

    with pytest.raises(ValidationError, match="Additional properties"):
        validate_bundled_document(protocol, document)


@pytest.mark.parametrize(
    "path",
    [
        "/absolute/path",
        "../outside",
        "cards/../outside",
        "C:/host/path",
        "~/host/path",
        "cards\\windows",
        "cards//double",
    ],
)
def test_project_schema_rejects_nonportable_card_roots(path: str) -> None:
    project = valid_project()
    spec = project["spec"]
    assert isinstance(spec, dict)
    spec["cardRoots"] = [path]

    with pytest.raises(ValidationError):
        validate_bundled_document(PROJECT_V1, project)


def test_card_schema_rejects_wrong_identity_empty_jobs_and_multi_gpu() -> None:
    card = valid_card()
    card["kind"] = "Project"
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_argv_allows_empty_arguments_but_not_an_empty_program() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    command = job["command"]
    assert isinstance(command, dict)
    command["argv"] = ["program", "", "--label", "value"]
    validate_bundled_document(EXPERIMENT_CARD_V1, card)

    command["argv"] = [""]
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


@pytest.mark.parametrize(
    "command",
    [
        {"type": "argv", "argv": ["program", "bad\x00argument"]},
        {"type": "argv", "argv": ["\x00"]},
        {
            "type": "wrapper",
            "path": "scripts/run.sh",
            "args": ["bad\x00argument"],
        },
        {
            "type": "shell",
            "script": "echo\x00bad",
            "compatibilityReason": "legacy launcher",
        },
    ],
)
def test_command_strings_reject_nul(command: dict[str, object]) -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["command"] = command

    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


@pytest.mark.parametrize("collection", ["volumes", "environments"])
def test_project_semantics_reject_duplicate_logical_names(collection: str) -> None:
    project = valid_project()
    spec = project["spec"]
    assert isinstance(spec, dict)
    values = spec[collection]
    assert isinstance(values, list)
    duplicate = dict(values[0])
    duplicate["description"] = "same logical identity, different declaration"
    values.append(duplicate)

    with pytest.raises(
        SemanticValidationError,
        match=f"spec.{collection} contains duplicate",
    ):
        validate_bundled_document(PROJECT_V1, project)


def test_card_semantics_reject_duplicate_job_and_artifact_names() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    duplicate_job = dict(jobs[0])
    duplicate_job["description"] = "same job identity, different declaration"
    jobs.append(duplicate_job)
    with pytest.raises(SemanticValidationError, match="spec.jobs contains duplicate id"):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    artifacts = job["artifacts"]
    assert isinstance(artifacts, list)
    duplicate_artifact = dict(artifacts[0])
    duplicate_artifact["path"] = "runs/WCG-023-second-path"
    artifacts.append(duplicate_artifact)
    with pytest.raises(SemanticValidationError, match="artifacts contains duplicate name"):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_card_semantics_require_declared_checkpoint_artifacts() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["capabilities"] = {
        "cooperativeYield": {
            "requestProtocol": {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldRequest",
            },
            "receiptProtocol": {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldReceipt",
            },
            "checkpointArtifacts": ["missing-checkpoint"],
        }
    }

    with pytest.raises(SemanticValidationError, match="undeclared job artifacts"):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    artifacts = job["artifacts"]
    assert isinstance(artifacts, list)
    checkpoint = artifacts[0]
    assert isinstance(checkpoint, dict)
    cooperative_yield = job["capabilities"]
    assert isinstance(cooperative_yield, dict)
    cooperative_yield = cooperative_yield["cooperativeYield"]
    assert isinstance(cooperative_yield, dict)
    cooperative_yield["checkpointArtifacts"] = [checkpoint["name"]]
    with pytest.raises(SemanticValidationError, match="requires file artifacts"):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_cooperative_yield_capability_requires_a_checkpoint_artifact() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["capabilities"] = {
        "cooperativeYield": {
            "requestProtocol": {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldRequest",
            },
            "receiptProtocol": {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldReceipt",
            },
            "checkpointArtifacts": [],
        }
    }

    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    spec["jobs"] = []
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    resources = job["resources"]
    assert isinstance(resources, dict)
    resources["gpus"] = 2
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_extension_names_are_project_slugs_and_values_are_objects() -> None:
    project = valid_project()
    project["extensions"] = {"Not-Portable": {}}
    with pytest.raises(ValidationError):
        validate_bundled_document(PROJECT_V1, project)

    card = valid_card()
    card["extensions"] = {"flowers-3d-helmholtz": "not-an-object"}
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


@pytest.mark.parametrize("terminator", ["\n", "\r", "\u0085", "\u2028", "\u2029"])
def test_protocol_identifiers_digests_and_paths_reject_terminal_line_characters(
    terminator: str,
) -> None:
    project = valid_project()
    metadata = project["metadata"]
    assert isinstance(metadata, dict)
    metadata["key"] = f"valid-key{terminator}"
    with pytest.raises(ValidationError):
        validate_bundled_document(PROJECT_V1, project)

    project = valid_project()
    spec = project["spec"]
    assert isinstance(spec, dict)
    volumes = spec["volumes"]
    assert isinstance(volumes, list)
    volume = volumes[0]
    assert isinstance(volume, dict)
    volume["name"] = f"scratch{terminator}"
    with pytest.raises(ValidationError):
        validate_bundled_document(PROJECT_V1, project)

    card = valid_card()
    metadata = card["metadata"]
    assert isinstance(metadata, dict)
    metadata["experimentId"] = f"valid-id{terminator}"
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    provenance = spec["provenance"]
    assert isinstance(provenance, dict)
    inputs = provenance["inputs"]
    assert isinstance(inputs, list)
    source = inputs[0]
    assert isinstance(source, dict)
    source["sha256"] = f"{'2' * 64}{terminator}"
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)

    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["workingDirectory"] = f"training{terminator}"
    with pytest.raises(ValidationError):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_project_schema_rejects_an_undeclared_protocol_major() -> None:
    project = valid_project()
    spec = project["spec"]
    assert isinstance(spec, dict)
    spec["supportedProtocols"] = [
        {
            "apiVersion": "experiment-queue/v999",
            "kind": "RunnerReceipt",
        }
    ]

    with pytest.raises(ValidationError):
        validate_bundled_document(PROJECT_V1, project)


@pytest.mark.parametrize(
    "identity",
    [
        {
            "apiVersion": "experiment-queue/v0",
            "kind": "RunnerReceipt",
        },
        {
            "apiVersion": "experiment-queue/v0",
            "kind": "CooperativeYieldRequest",
        },
        {
            "apiVersion": "experiment-queue/v0",
            "kind": "CooperativeYieldReceipt",
        },
    ],
)
def test_project_schema_accepts_declared_legacy_protocol_identities(
    identity: dict[str, str],
) -> None:
    project = valid_project()
    spec = project["spec"]
    assert isinstance(spec, dict)
    spec["supportedProtocols"] = [identity]

    validate_bundled_document(PROJECT_V1, project)


def test_card_schema_rejects_an_undeclared_yield_protocol_major() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    jobs = spec["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    artifacts = job["artifacts"]
    assert isinstance(artifacts, list)
    artifact = artifacts[0]
    assert isinstance(artifact, dict)
    artifact["type"] = "file"
    job["capabilities"] = {
        "cooperativeYield": {
            "requestProtocol": {
                "apiVersion": "experiment-queue/v999",
                "kind": "CooperativeYieldRequest",
            },
            "receiptProtocol": {
                "apiVersion": "experiment-queue/v1",
                "kind": "CooperativeYieldReceipt",
            },
            "checkpointArtifacts": ["run-output"],
        }
    }

    with pytest.raises(ValidationError, match="experiment-queue/v1") as exc_info:
        validate_bundled_document(EXPERIMENT_CARD_V1, card)
    assert "requestProtocol" in ".".join(
        str(part) for part in exc_info.value.absolute_path
    )


def test_document_validator_rejects_non_json_values_even_in_parameter_data() -> None:
    card = valid_card()
    spec = card["spec"]
    assert isinstance(spec, dict)
    parameters = spec["parameters"]
    assert isinstance(parameters, dict)
    parameters["when"] = date(2026, 8, 27)

    with pytest.raises(CanonicalJSONError, match="non-JSON type date"):
        validate_bundled_document(EXPERIMENT_CARD_V1, card)


def test_offline_registry_resolves_bundled_ids_without_network() -> None:
    wrapper = {
        "$schema": JSON_SCHEMA_DIALECT,
        "$ref": PROJECT_V1_SCHEMA.schema_id,
    }
    offline_validator(wrapper).validate(valid_project())

    remote = {
        "$schema": JSON_SCHEMA_DIALECT,
        "$ref": "https://example.invalid/not-bundled.schema.json",
    }
    with pytest.raises(Unresolvable, match="not-bundled"):
        offline_validator(remote).validate({})


def test_unsupported_schema_dialect_fails_before_validation() -> None:
    with pytest.raises(BundledSchemaError, match="must declare exactly"):
        offline_validator({"$schema": "http://json-schema.org/draft-07/schema#"})


@pytest.mark.parametrize(
    "unsupported_value",
    [date(2026, 8, 27), ("tuple",), float("nan")],
)
def test_offline_validator_rejects_noncanonical_python_schema_values(
    unsupported_value: object,
) -> None:
    schema = {
        "$schema": JSON_SCHEMA_DIALECT,
        "description": unsupported_value,
    }

    with pytest.raises(BundledSchemaError, match="strict canonical JSON domain"):
        offline_validator(schema)


def test_installed_schema_digest_detects_resource_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = files("experiment_queue.schema_resources").joinpath(
        PROJECT_V1_SCHEMA.resource_name
    )
    changed = json.loads(source.read_text(encoding="utf-8"))
    changed["title"] = "tampered"
    target = tmp_path / PROJECT_V1_SCHEMA.resource_name
    target.write_text(json.dumps(changed), encoding="utf-8")

    monkeypatch.setattr(schema_registry, "files", lambda _package: tmp_path)
    with pytest.raises(BundledSchemaError, match="digest"):
        load_bundled_schema(PROJECT_V1)


def test_missing_schema_resource_package_uses_the_public_package_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_package(_package: str) -> object:
        raise ModuleNotFoundError("synthetic missing schema package")

    monkeypatch.setattr(schema_registry, "files", missing_package)
    with pytest.raises(BundledSchemaError, match="could not read installed schema"):
        load_bundled_schema(PROJECT_V1)


def test_unowned_protocol_has_no_schema_fallback() -> None:
    from experiment_queue.protocols import RUNNER_RECEIPT_V1

    with pytest.raises(BundledSchemaError, match="no bundled JSON Schema owns"):
        load_bundled_schema(RUNNER_RECEIPT_V1)
