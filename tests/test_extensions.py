"""Exercise namespaced extension schemas as one offline validation envelope."""

from __future__ import annotations

import json

import pytest

from experiment_queue.authoring import (
    ExperimentCard,
    ExtensionSchemaReference,
    Project,
)
from experiment_queue.extensions import (
    ExtensionSchemaError,
    ExtensionValidationError,
    load_extension_schema,
    validate_namespaced_extensions,
)
from experiment_queue.schema_registry import JSON_SCHEMA_DIALECT
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes


PROJECT_KEY = "example-project"
SCHEMA_PATH = "schemas/example-extension.schema.json"


def project_document(
    *,
    extensions: dict[str, object] | None = None,
    reference: dict[str, object] | None = None,
) -> dict[str, object]:
    spec: dict[str, object] = {
        "cardRoots": ["experiments"],
        "volumes": [],
        "environments": [{"name": "default"}],
        "environmentPolicy": {"inherit": "none", "allowVariables": []},
        "supportedProtocols": [],
    }
    if reference is not None:
        spec["extensionSchema"] = reference
    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {"key": PROJECT_KEY, "displayName": "Example Project"},
        "spec": spec,
    }
    if extensions is not None:
        document["extensions"] = extensions
    return document


def card_document(
    *,
    extensions: dict[str, object] | None = None,
    job_extensions: dict[str, object] | None = None,
) -> dict[str, object]:
    job: dict[str, object] = {
        "id": "train",
        "environment": "default",
        "command": {"type": "argv", "argv": ["python", "train.py"]},
    }
    if job_extensions is not None:
        job["extensions"] = job_extensions
    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": PROJECT_KEY,
            "experimentId": "experiment-1",
            "title": "Example experiment",
        },
        "spec": {"parameters": {}, "jobs": [job]},
    }
    if extensions is not None:
        document["extensions"] = extensions
    return document


def schema_source(schema: dict[str, object]) -> bytes:
    return json.dumps(schema, separators=(",", ":"), ensure_ascii=False).encode()


def schema_reference(
    source: bytes,
    *,
    include_digest: bool = True,
) -> ExtensionSchemaReference:
    document = json.loads(source)
    digest = sha256_bytes(canonical_json_bytes(document)) if include_digest else None
    reference_fields: dict[str, object] = {"path": SCHEMA_PATH}
    if digest is not None:
        reference_fields["sha256"] = digest
    project = Project.from_document(project_document(reference=reference_fields))
    assert project.extension_schema is not None
    return project.extension_schema


def reference_document(reference: ExtensionSchemaReference) -> dict[str, object]:
    document: dict[str, object] = {"path": reference.path}
    if reference.sha256 is not None:
        document["sha256"] = reference.sha256
    return document


def envelope_schema() -> dict[str, object]:
    payload = {
        "type": "object",
        "additionalProperties": False,
        "required": ["enabled"],
        "properties": {"enabled": {"type": "boolean"}},
    }
    return {
        "$schema": JSON_SCHEMA_DIALECT,
        "$id": "urn:example:extension-schema:v1",
        "type": "object",
        "additionalProperties": False,
        "required": ["project", "card", "jobs"],
        "properties": {
            "project": payload,
            "card": payload,
            "jobs": {
                "type": "object",
                "additionalProperties": False,
                "required": ["train"],
                "properties": {"train": payload},
            },
        },
    }


def test_matching_namespace_remains_flexible_without_a_schema() -> None:
    project = Project.from_document(
        project_document(
            extensions={PROJECT_KEY: {"arbitrary": {"nested": [1, True, None]}}}
        )
    )
    card = ExperimentCard.from_document(
        card_document(
            extensions={PROJECT_KEY: {"tracker": "anything"}},
            job_extensions={PROJECT_KEY: {"custom": ["x", {"y": 2}]}},
        )
    )

    assert validate_namespaced_extensions(project, card) is None


def test_extension_validation_rejects_root_model_subclasses() -> None:
    class ProjectSubclass(Project):
        pass

    class CardSubclass(ExperimentCard):
        pass

    project = Project.from_document(project_document())
    card = ExperimentCard.from_document(card_document())

    with pytest.raises(TypeError, match="project must be exactly"):
        validate_namespaced_extensions(object.__new__(ProjectSubclass), card)
    with pytest.raises(TypeError, match="card must be exactly"):
        validate_namespaced_extensions(project, object.__new__(CardSubclass))


@pytest.mark.parametrize("location", ["project", "card", "job"])
def test_wrong_namespace_fails_with_its_source_and_path(location: str) -> None:
    wrong = {"other-project": {"value": 1}}
    project_extensions = wrong if location == "project" else None
    card_extensions = wrong if location == "card" else None
    job_extensions = wrong if location == "job" else None
    project = Project.from_document(project_document(extensions=project_extensions))
    card = ExperimentCard.from_document(
        card_document(
            extensions=card_extensions,
            job_extensions=job_extensions,
        )
    )

    with pytest.raises(
        ExtensionValidationError,
        match=rf"other-project.*only extensions\.{PROJECT_KEY}.*move",
    ) as exc_info:
        validate_namespaced_extensions(project, card)

    assert "/extensions/other-project" in str(exc_info.value)


def test_one_envelope_enforces_cross_location_required_fields() -> None:
    source = schema_source(envelope_schema())
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(
            reference=reference_document(reference),
            extensions={PROJECT_KEY: {"enabled": True}},
        )
    )
    complete_card = ExperimentCard.from_document(
        card_document(
            extensions={PROJECT_KEY: {"enabled": True}},
            job_extensions={PROJECT_KEY: {"enabled": True}},
        )
    )

    evidence = validate_namespaced_extensions(
        project,
        complete_card,
        schema_source=source,
    )
    assert evidence is not None
    assert evidence.path == SCHEMA_PATH
    assert evidence.sha256 == reference.sha256
    assert evidence.source_sha256 == sha256_bytes(source)
    assert evidence.schema_id == "urn:example:extension-schema:v1"

    missing_card_payload = ExperimentCard.from_document(
        card_document(job_extensions={PROJECT_KEY: {"enabled": True}})
    )
    with pytest.raises(
        ExtensionValidationError,
        match=r"extensions\.example-project.*at \$: 'card' is a required property.*fix",
    ):
        validate_namespaced_extensions(
            project,
            missing_card_payload,
            schema_source=source,
        )


def test_extension_schema_canonical_digest_mismatch_fails() -> None:
    source = schema_source(envelope_schema())
    mismatched_project = Project.from_document(
        project_document(reference={"path": SCHEMA_PATH, "sha256": "0" * 64})
    )
    reference = mismatched_project.extension_schema
    assert reference is not None

    with pytest.raises(
        ExtensionSchemaError,
        match=r"schemas/example-extension.*canonical SHA-256.*expects.*restore",
    ):
        load_extension_schema(source, reference)


def test_extension_schema_loader_requires_a_validated_reference() -> None:
    source = schema_source({"$schema": JSON_SCHEMA_DIALECT, "type": "object"})

    with pytest.raises(TypeError, match="validated ExtensionSchemaReference.*Project"):
        load_extension_schema(source, object())  # type: ignore[arg-type]


def test_extension_schema_loader_rejects_reference_subclasses() -> None:
    class ForgedReference(ExtensionSchemaReference):
        pass

    source = schema_source({"$schema": JSON_SCHEMA_DIALECT, "type": "object"})
    forged = object.__new__(ForgedReference)

    with pytest.raises(TypeError, match="exact validated ExtensionSchemaReference"):
        load_extension_schema(source, forged)


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (
            b'{"$schema":"https://json-schema.org/draft/2020-12/schema",'
            b'"type":"object","type":"array"}',
            "duplicate JSON object key",
        ),
        (
            b'{"$schema":"https://json-schema.org/draft/2020-12/schema",'
            b'"minimum":NaN}',
            "non-finite JSON constant",
        ),
        (
            b'\xef\xbb\xbf{"$schema":"https://json-schema.org/draft/2020-12/schema"}',
            "byte-order mark",
        ),
    ],
)
def test_extension_schema_rejects_non_strict_json(
    source: bytes,
    message: str,
) -> None:
    reference = schema_reference(
        schema_source({"$schema": JSON_SCHEMA_DIALECT}),
        include_digest=False,
    )

    with pytest.raises(ExtensionSchemaError, match=message) as exc_info:
        load_extension_schema(source, reference, source_name="extension.json")

    assert str(exc_info.value).startswith("extension.json:")
    assert "fix" in str(exc_info.value) or "save" in str(exc_info.value)


def test_extension_schema_requires_exact_draft_2020_12() -> None:
    source = schema_source(
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
        }
    )
    reference = schema_reference(source, include_digest=False)

    with pytest.raises(
        ExtensionSchemaError,
        match=r"extension.json:.*\$\.\$schema.*Draft 2020-12",
    ):
        load_extension_schema(source, reference, source_name="extension.json")


def test_extension_schema_is_meta_validated() -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "type": 17,
        }
    )
    reference = schema_reference(source, include_digest=False)

    with pytest.raises(ExtensionSchemaError) as exc_info:
        load_extension_schema(source, reference, source_name="extension.json")

    message = str(exc_info.value)
    assert message.startswith("extension.json: invalid Draft 2020-12")
    assert "schema['type']" in message
    assert "fix the reported schema keyword" in message


def test_extension_schema_remote_ref_fails_without_retrieval() -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$ref": "https://schemas.example.invalid/never-fetch.json",
        }
    )
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    with pytest.raises(
        ExtensionValidationError,
        match=r"schemas/example-extension.*extensions\.example-project.*"
        r"\$ref.*path \$.*network retrieval is disabled",
    ):
        validate_namespaced_extensions(project, schema_source=source)


@pytest.mark.parametrize(
    ("container", "keyword", "expected_path"),
    [
        ("defs", "$ref", "$/$defs/unused"),
        ("defs", "$dynamicRef", "$/$defs/unused"),
        ("branch", "$ref", "$/then"),
    ],
)
def test_dormant_remote_references_fail_offline_preflight(
    container: str,
    keyword: str,
    expected_path: str,
) -> None:
    remote = {keyword: "https://schemas.example.invalid/dormant.json"}
    schema: dict[str, object] = {
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
    }
    if container == "defs":
        schema["$defs"] = {"unused": remote}
    else:
        schema["if"] = {"required": ["never-present"]}
        schema["then"] = remote
    source = schema_source(schema)
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    with pytest.raises(ExtensionValidationError) as exc_info:
        validate_namespaced_extensions(project, schema_source=source)

    message = str(exc_info.value)
    assert keyword in message
    assert expected_path in message
    assert "network retrieval is disabled" in message


@pytest.mark.parametrize(
    "schema",
    [
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$ref": "#",
        },
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$ref": "#/$defs/first",
            "$defs": {
                "first": {"$ref": "#/$defs/second"},
                "second": {"$ref": "#/$defs/first"},
            },
        },
    ],
    ids=["direct", "mutual"],
)
def test_unconditional_recursive_references_fail_with_stable_error(
    schema: dict[str, object],
) -> None:
    source = schema_source(schema)
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    with pytest.raises(
        ExtensionValidationError,
        match=r"recursive \$ref or \$dynamicRef.*finite base case",
    ):
        validate_namespaced_extensions(project, schema_source=source)


@pytest.mark.parametrize(
    "reference_value",
    ["#/$defs/envelope", "urn:example:local-extension:v1#/$defs/envelope"],
    ids=["fragment", "absolute-same-resource"],
)
def test_local_same_resource_references_remain_valid(reference_value: str) -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$id": "urn:example:local-extension:v1",
            "$ref": reference_value,
            "$defs": {"envelope": {"type": "object"}},
        }
    )
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    assert validate_namespaced_extensions(project, schema_source=source) is not None


def test_nested_resource_ids_use_their_draft_base_uri() -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$id": "https://offline.example.invalid/root/extension.json",
            "type": "object",
            "$defs": {
                "target": {
                    "$id": "target.json",
                    "$anchor": "payload",
                    "type": "object",
                },
                "consumer": {"$ref": "target.json#payload"},
            },
        }
    )
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    assert validate_namespaced_extensions(project, schema_source=source) is not None


@pytest.mark.parametrize("target", [17, ["not", "a", "schema"], None, "text"])
def test_reference_target_must_itself_be_a_schema(target: object) -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "$ref": "#/const",
            "const": target,
        }
    )
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    with pytest.raises(
        ExtensionValidationError,
        match=r"resolves to .*not a JSON Schema object or boolean",
    ):
        validate_namespaced_extensions(project, schema_source=source)


def test_dormant_authenticated_bundled_reference_remains_valid() -> None:
    source = schema_source(
        {
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "$defs": {
                "bundled": {"$ref": "urn:experiment-queue:schema:project:v1"}
            },
        }
    )
    reference = schema_reference(source)
    project = Project.from_document(
        project_document(reference=reference_document(reference))
    )

    assert validate_namespaced_extensions(project, schema_source=source) is not None


def test_extension_schema_bytes_must_match_a_project_declaration() -> None:
    source = schema_source({"$schema": JSON_SCHEMA_DIALECT, "type": "object"})
    project_without_schema = Project.from_document(project_document())

    with pytest.raises(ExtensionSchemaError, match="unexpected.*declare"):
        validate_namespaced_extensions(project_without_schema, schema_source=source)

    reference = schema_reference(source)
    project_with_schema = Project.from_document(
        project_document(reference=reference_document(reference))
    )
    with pytest.raises(ExtensionSchemaError, match="declared.*missing.*provide"):
        validate_namespaced_extensions(project_with_schema)
