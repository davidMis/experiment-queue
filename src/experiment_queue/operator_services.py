"""Read-only authoring, Enrollment, doctor, and submission dry-run services.

These functions form operator-facing validation surfaces without opening queue
state or depending on a scheduler repository.  Trusted Git evidence comes only
from :mod:`experiment_queue.git_resolver`; this module never reimplements Git
plumbing and never creates refs, worktrees, database rows, or artifact paths.

Diagnostic reports intentionally omit ``apiVersion``/``kind``: no durable
validation-output protocol has been accepted, so these JSON objects must not be
persisted or mistaken for independently versioned queue evidence.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import json
from pathlib import Path
from typing import Final, cast

from experiment_queue.admission import Submission
from experiment_queue.authoring import (
    ExperimentCard,
    Project,
    validate_card_for_project,
)
from experiment_queue.execution import resolve_artifact_path
from experiment_queue.extensions import (
    ExtensionSchema,
    validate_namespaced_extensions,
)
from experiment_queue.git_resolver import (
    GitBlobEvidence,
    GitResolvedAdmission,
    GitResolvedProjectRevision,
    compile_admission_from_revision,
    verify_project_revision,
)
from experiment_queue.identity import validate_project_key
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    HostRootClaim,
    MountBinding,
    ProjectRevision,
)
from experiment_queue.schema_registry import (
    EXPERIMENT_CARD_V1_SCHEMA,
    PROJECT_V1_SCHEMA,
    BundledSchema,
    editor_schema_bytes,
)
from experiment_queue.serialization import (
    JSONValue,
    StrictYAMLError,
    canonical_json_bytes,
    load_strict_yaml,
    sha256_bytes,
)


_ENROLLMENT_FIELDS: Final = frozenset(
    {
        "apiVersion",
        "kind",
        "projectKey",
        "projectNormalizedSha256",
        "checkoutDirectory",
        "projectManifestPath",
        "mounts",
        "artifactRoots",
        "environments",
        "gitIgnoredCheckoutDescendants",
    }
)
_MOUNT_FIELDS: Final = frozenset({"name", "path", "access"})
_ARTIFACT_ROOT_FIELDS: Final = frozenset({"name", "path"})


class OperatorServiceError(ValueError):
    """Raised when a read-only operator request cannot be validated safely."""


def _plain_object(
    value: object,
    *,
    field_name: str,
    expected_fields: frozenset[str] | None = None,
) -> dict[str, object]:
    if type(value) is not dict:
        raise OperatorServiceError(
            f"{field_name} must be a plain JSON object, got "
            f"{type(value).__name__}"
        )
    document = cast(dict[object, object], value)
    non_text = [key for key in document if type(key) is not str]
    if non_text:
        raise OperatorServiceError(
            f"{field_name} object keys must be strings, got {non_text!r}"
        )
    result = cast(dict[str, object], document)
    if expected_fields is not None:
        fields = set(result)
        missing = sorted(expected_fields - fields)
        unknown = sorted(fields - expected_fields)
        if missing or unknown:
            details: list[str] = []
            if missing:
                details.append(f"missing fields {missing}")
            if unknown:
                details.append(f"unknown fields {unknown}")
            raise OperatorServiceError(
                f"{field_name} has invalid fields: {'; '.join(details)}"
            )
    return result


def _plain_array(value: object, *, field_name: str) -> list[object]:
    if type(value) is not list:
        raise OperatorServiceError(
            f"{field_name} must be a JSON array, got {type(value).__name__}"
        )
    return cast(list[object], value)


def _text(value: object, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise OperatorServiceError(
            f"{field_name} must be a non-empty string without surrounding "
            f"whitespace, got {value!r}"
        )
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise OperatorServiceError(
            f"{field_name} must contain valid Unicode scalar text"
        ) from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise OperatorServiceError(f"{field_name} must not contain control characters")
    return value


def _source_name(value: object) -> str:
    if type(value) is not str or not value:
        raise OperatorServiceError("source_name must be a non-empty string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise OperatorServiceError(
            "source_name must contain valid Unicode scalar text"
        ) from exc
    return value


def _load_object(source: bytes, *, source_name: str, kind: str) -> dict[str, object]:
    if type(source) is not bytes:
        raise TypeError(
            f"{kind} source must be exact bytes, got {type(source).__name__}"
        )
    name = _source_name(source_name)
    try:
        document = load_strict_yaml(source, source_name=name)
    except (StrictYAMLError, TypeError) as exc:
        raise OperatorServiceError(f"could not load {kind} from {name}: {exc}") from exc
    return _plain_object(document, field_name=f"{kind} at {name}")


def _schema_document(schema: BundledSchema) -> dict[str, JSONValue]:
    return {
        "apiVersion": schema.protocol.api_version,
        "kind": schema.protocol.kind.value,
        "id": schema.schema_id,
        "sha256": schema.sha256,
    }


def _extension_document(extension: ExtensionSchema | None) -> JSONValue:
    if extension is None:
        return None
    document: dict[str, JSONValue] = {
        "path": extension.reference_path,
        "sourceSizeBytes": len(extension.source_bytes),
        "sourceSha256": extension.source_sha256,
        "canonicalSha256": extension.canonical_sha256,
    }
    if extension.schema_id is not None:
        document["schemaId"] = extension.schema_id
    return document


def project_manifest_scaffold(*, key: str, display_name: str) -> bytes:
    """Return a minimal strict Project/v1 YAML scaffold after self-validation."""

    try:
        project_key = validate_project_key(key)
    except (TypeError, ValueError) as exc:
        raise OperatorServiceError(f"project key is invalid: {exc}") from exc
    title = _text(display_name, field_name="display_name")
    # JSON string spelling is valid YAML 1.2 and prevents display text from
    # becoming a tag, comment, number, or structural token in the scaffold.
    rendered_title = json.dumps(title, ensure_ascii=False)
    source = (
        "apiVersion: experiment-queue/v1\n"
        "kind: Project\n"
        "metadata:\n"
        f"  key: {project_key}\n"
        f"  displayName: {rendered_title}\n"
        "spec:\n"
        "  cardRoots:\n"
        "    - experiments\n"
        "  volumes:\n"
        "    - name: artifacts\n"
        "      access: readWrite\n"
        "      required: true\n"
        "  environments:\n"
        "    - name: python\n"
        "  environmentPolicy:\n"
        "    inherit: none\n"
        "    allowVariables: []\n"
        "  supportedProtocols: []\n"
    ).encode("utf-8")
    Project.from_yaml(source, source_name="Project.yaml scaffold")
    return source


def experiment_card_scaffold(
    *,
    project: Project,
    experiment_id: str,
    title: str,
    job_id: str = "run",
    environment: str | None = None,
    artifact_root: str | None = None,
) -> bytes:
    """Return a minimal card that validates against one exact Project.

    The scaffold intentionally uses a direct argv command and one GPU. It does
    not claim cooperative preemption or invent host paths. When ``artifact_root``
    is omitted, the first declared writable volume is used; Projects without a
    writable volume receive a valid card with no artifact declaration.
    """

    if type(project) is not Project:
        raise TypeError(
            f"project must be exactly Project, got {type(project).__name__}"
        )
    experiment = _text(experiment_id, field_name="experiment_id")
    card_title = _text(title, field_name="title")
    job = _text(job_id, field_name="job_id")
    environments = tuple(item.name for item in project.environments)
    selected_environment = environments[0] if environment is None else _text(
        environment, field_name="environment"
    )
    if selected_environment not in environments:
        raise OperatorServiceError(
            f"environment {selected_environment!r} is not declared by Project "
            f"{project.key!r}; choose one of {list(environments)}"
        )
    writable = tuple(
        volume.name for volume in project.volumes if volume.access.value == "readWrite"
    )
    selected_root = writable[0] if artifact_root is None and writable else artifact_root
    if selected_root is not None:
        selected_root = _text(selected_root, field_name="artifact_root")
        if selected_root not in writable:
            raise OperatorServiceError(
                f"artifact root {selected_root!r} is not a writable Project volume; "
                f"choose one of {list(writable)}"
            )
    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "ExperimentCard",
        "metadata": {
            "projectKey": project.key,
            "experimentId": experiment,
            "title": card_title,
        },
        "spec": {
            "parameters": {},
            "jobs": [
                {
                    "id": job,
                    "role": "independent",
                    "environment": selected_environment,
                    "command": {
                        "type": "argv",
                        "argv": ["python", "run.py"],
                    },
                    "resources": {"gpus": 1},
                }
            ],
        },
    }
    if selected_root is not None:
        jobs = cast(list[dict[str, object]], cast(dict[str, object], document["spec"])["jobs"])
        jobs[0]["artifacts"] = [
            {
                "name": "result",
                "root": selected_root,
                "path": f"runs/{experiment}/result.json",
                "type": "file",
                "required": True,
            }
        ]
    source = (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    card = ExperimentCard.from_yaml(source, source_name="ExperimentCard scaffold")
    validate_card_for_project(project, card)
    return source


def export_editor_schema(kind: str) -> bytes:
    """Return authenticated, pretty JSON Schema bytes for editor integration."""

    value = _text(kind, field_name="schema kind").lower()
    if value == "project":
        return editor_schema_bytes(PROJECT_V1_SCHEMA.protocol)
    if value in {"card", "experiment-card", "experimentcard"}:
        return editor_schema_bytes(EXPERIMENT_CARD_V1_SCHEMA.protocol)
    raise OperatorServiceError(
        f"schema kind {kind!r} is unsupported; choose 'project' or 'card'"
    )


def validate_project_source(
    *,
    source: bytes,
    source_name: str,
    extension_schema_source: bytes | None = None,
    explain: bool = False,
) -> dict[str, JSONValue]:
    """Validate one Project and return deterministic source/schema evidence."""

    project = Project.from_yaml(source, source_name=_source_name(source_name))
    extension = validate_namespaced_extensions(
        project,
        schema_source=extension_schema_source,
    )
    normalized = canonical_json_bytes(project.to_document())
    report: dict[str, JSONValue] = {
        "operation": "project.explain" if explain else "project.validate",
        "outputContract": "read-only diagnostic; not persistent protocol evidence",
        "valid": True,
        "wouldMutateState": False,
        "projectKey": project.key,
        "displayName": project.display_name,
        "source": {
            "name": source_name,
            "sizeBytes": len(source),
            "sha256": sha256_bytes(source),
            "normalizedSha256": sha256_bytes(normalized),
        },
        "schema": _schema_document(PROJECT_V1_SCHEMA),
        "extensionSchema": _extension_document(extension),
    }
    if explain:
        report["explanation"] = {
            "cardRoots": list(project.card_roots),
            "volumes": [volume.to_document() for volume in project.volumes],
            "environments": [
                environment.to_document() for environment in project.environments
            ],
            "environmentPolicy": project.environment_policy.to_document(),
            "supportedProtocols": [
                protocol.document_identity()
                for protocol in project.supported_protocols
            ],
            "normalizedDocument": project.to_document(),
        }
    return report


def validate_card_source(
    *,
    project_source: bytes,
    project_source_name: str,
    card_source: bytes,
    card_source_name: str,
    extension_schema_source: bytes | None = None,
    explain: bool = False,
) -> dict[str, JSONValue]:
    """Validate one card against its owning Project without compiling a job."""

    project = Project.from_yaml(
        project_source,
        source_name=_source_name(project_source_name),
    )
    card = ExperimentCard.from_yaml(
        card_source,
        source_name=_source_name(card_source_name),
    )
    # The authoring helper owns environment, artifact-root, and protocol
    # relations; extension validation separately owns namespace/schema rules.
    validate_card_for_project(project, card)
    extension = validate_namespaced_extensions(
        project,
        card,
        schema_source=extension_schema_source,
    )
    project_normalized = canonical_json_bytes(project.to_document())
    card_normalized = canonical_json_bytes(card.to_document())
    report: dict[str, JSONValue] = {
        "operation": "card.explain" if explain else "card.validate",
        "outputContract": "read-only diagnostic; not persistent protocol evidence",
        "valid": True,
        "wouldMutateState": False,
        "projectKey": project.key,
        "experimentId": card.experiment_id,
        "title": card.title,
        "jobs": [job.id for job in card.jobs],
        "projectSource": {
            "name": project_source_name,
            "sizeBytes": len(project_source),
            "sha256": sha256_bytes(project_source),
            "normalizedSha256": sha256_bytes(project_normalized),
        },
        "cardSource": {
            "name": card_source_name,
            "sizeBytes": len(card_source),
            "sha256": sha256_bytes(card_source),
            "normalizedSha256": sha256_bytes(card_normalized),
        },
        "projectSchema": _schema_document(PROJECT_V1_SCHEMA),
        "cardSchema": _schema_document(EXPERIMENT_CARD_V1_SCHEMA),
        "extensionSchema": _extension_document(extension),
    }
    if explain:
        jobs: list[JSONValue] = []
        for job in card.jobs:
            jobs.append(
                {
                    "id": job.id,
                    "role": None if job.role is None else job.role.value,
                    "environment": job.environment,
                    "workingDirectory": job.working_directory,
                    "command": job.command.to_document(),
                    "resources": (
                        None if job.resources is None else job.resources.to_document()
                    ),
                    "artifacts": [
                        artifact.to_document() for artifact in job.artifacts
                    ],
                    "capabilities": (
                        None
                        if job.capabilities is None
                        else job.capabilities.to_document()
                    ),
                }
            )
        card_document = card.to_document()
        card_spec = cast(dict[str, JSONValue], card_document["spec"])
        report["explanation"] = {
            "parameters": card_spec["parameters"],
            "jobs": jobs,
            "normalizedDocument": card.to_document(),
        }
    return report


def load_enrollment_document(
    *,
    source: bytes,
    source_name: str,
    project: Project,
    state_directory: str | Path,
    occupied_roots: Sequence[HostRootClaim] = (),
    verified_git_ignored_checkout_descendants: Sequence[str | Path] | None = None,
    git_ignore_verifier: (
        Callable[[tuple[str, ...]], Sequence[str | Path]] | None
    ) = None,
) -> Enrollment:
    """Load an exact versioned Enrollment and rederive every trusted field.

    The document is the already-defined :meth:`Enrollment.to_document` shape;
    no parallel host-configuration schema is invented here.  Derived project
    identity, artifact-root view, ordering, and canonical paths must reproduce
    exactly or loading fails.
    """

    if type(project) is not Project:
        raise TypeError(
            f"project must be exactly Project, got {type(project).__name__}"
        )
    document = _load_object(source, source_name=source_name, kind="Enrollment")
    document = _plain_object(
        document,
        field_name=f"Enrollment at {source_name}",
        expected_fields=_ENROLLMENT_FIELDS,
    )
    if (
        document["apiVersion"] != "experiment-queue/v1"
        or document["kind"] != "Enrollment"
    ):
        raise OperatorServiceError(
            f"Enrollment at {source_name} requires apiVersion "
            "'experiment-queue/v1' and kind 'Enrollment'"
        )
    if document["projectKey"] != project.key:
        raise OperatorServiceError(
            f"Enrollment at {source_name} names Project "
            f"{document['projectKey']!r}, not validated Project {project.key!r}"
        )
    project_digest = sha256_bytes(canonical_json_bytes(project.to_document()))
    if document["projectNormalizedSha256"] != project_digest:
        raise OperatorServiceError(
            f"Enrollment at {source_name} has projectNormalizedSha256 "
            f"{document['projectNormalizedSha256']!r}, expected {project_digest}; "
            "regenerate host bindings for this exact Project"
        )

    mounts: list[MountBinding] = []
    for index, value in enumerate(
        _plain_array(document["mounts"], field_name="Enrollment.mounts")
    ):
        mount = _plain_object(
            value,
            field_name=f"Enrollment.mounts[{index}]",
            expected_fields=_MOUNT_FIELDS,
        )
        mounts.append(
            MountBinding.create(
                name=cast(str, mount["name"]),
                path=cast(str, mount["path"]),
                access=cast(str, mount["access"]),
            )
        )

    environments: list[EnvironmentBinding] = []
    for index, value in enumerate(
        _plain_array(
            document["environments"],
            field_name="Enrollment.environments",
        )
    ):
        environment = _plain_object(
            value,
            field_name=f"Enrollment.environments[{index}]",
        )
        environments.append(EnvironmentBinding.from_document(environment))

    # Validate the derived view's shape before the final exact comparison so
    # secret-looking or independently configured fields get a local error.
    for index, value in enumerate(
        _plain_array(
            document["artifactRoots"],
            field_name="Enrollment.artifactRoots",
        )
    ):
        _plain_object(
            value,
            field_name=f"Enrollment.artifactRoots[{index}]",
            expected_fields=_ARTIFACT_ROOT_FIELDS,
        )

    ignored_values = _plain_array(
        document["gitIgnoredCheckoutDescendants"],
        field_name="Enrollment.gitIgnoredCheckoutDescendants",
    )
    ignored: list[str] = []
    for index, value in enumerate(ignored_values):
        ignored.append(
            _text(
                value,
                field_name=f"Enrollment.gitIgnoredCheckoutDescendants[{index}]",
            )
        )
    if (
        verified_git_ignored_checkout_descendants is not None
        and git_ignore_verifier is not None
    ):
        raise TypeError(
            "supply either verified Git-ignore descendants or a verifier, not both"
        )
    if ignored and (
        verified_git_ignored_checkout_descendants is None
        and git_ignore_verifier is None
    ):
        raise OperatorServiceError(
            f"Enrollment at {source_name} claims checkout-descendant Git-ignore "
            "proofs, but the standalone parser cannot authenticate them at the "
            "pinned commit; registration must supply trusted resolver proof rather "
            "than treating document path strings as authority"
        )
    if git_ignore_verifier is not None and ignored:
        verified_ignored = tuple(git_ignore_verifier(tuple(ignored)))
    else:
        verified_ignored = (
            ()
            if verified_git_ignored_checkout_descendants is None
            else tuple(verified_git_ignored_checkout_descendants)
        )

    enrollment = Enrollment.create(
        project=project,
        checkout_directory=cast(str, document["checkoutDirectory"]),
        project_manifest_path=cast(str, document["projectManifestPath"]),
        mounts=mounts,
        environments=environments,
        state_directory=state_directory,
        git_ignored_checkout_descendants=verified_ignored,
        occupied_roots=occupied_roots,
    )
    reconstructed = enrollment.to_document()
    if reconstructed != document:
        mismatches = sorted(
            field
            for field in _ENROLLMENT_FIELDS
            if reconstructed.get(field) != document.get(field)
        )
        raise OperatorServiceError(
            f"Enrollment at {source_name} differs from exact rederived host "
            f"evidence in fields {mismatches}; regenerate the document instead of "
            "editing derived identity, order, artifact roots, or canonical paths"
        )
    return enrollment


def _blob_document(blob: GitBlobEvidence | None) -> JSONValue:
    if blob is None:
        return None
    return {
        "path": blob.path,
        "objectId": blob.object_id,
        "mode": blob.mode,
        "sizeBytes": blob.size,
        "sourceSha256": blob.source_sha256,
    }


def _revision_identity_document(
    verified: GitResolvedProjectRevision | GitResolvedAdmission,
) -> dict[str, JSONValue]:
    return {
        "projectId": verified.project_id,
        "projectKey": verified.project_key,
        "projectRevisionId": verified.project_revision_id,
        "projectRevision": verified.project_revision_label,
        "gitCommit": verified.git_commit,
        "repositoryRoot": verified.repository_root,
    }


def doctor_project_revision(
    *,
    revision: ProjectRevision,
) -> dict[str, JSONValue]:
    """Verify current Enrollment paths and exact pinned Git source identity."""

    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got "
            f"{type(revision).__name__}"
        )
    revision.enrollment.validate_current_paths()
    verified = verify_project_revision(revision)
    return {
        "operation": "project.doctor",
        "outputContract": "read-only diagnostic; not persistent protocol evidence",
        "valid": True,
        "wouldMutateState": False,
        "revision": _revision_identity_document(verified),
        "git": {
            "canonicalToplevelVerified": True,
            "fullCommitVerified": True,
            "projectBlob": _blob_document(verified.project_blob),
            "extensionSchemaBlob": _blob_document(
                verified.extension_schema_blob
            ),
        },
        "enrollment": {
            "sha256": revision.enrollment.sha256,
            "currentPathsVerified": True,
            "checkoutDirectory": str(revision.enrollment.checkout_directory),
            "projectManifestPath": revision.enrollment.project_manifest_path,
            "mounts": [
                mount.to_document() for mount in revision.enrollment.mounts
            ],
            "artifactRoots": [
                root.to_document() for root in revision.enrollment.artifact_roots
            ],
            "environments": [
                environment.to_document()
                for environment in revision.enrollment.environments
            ],
            "gitIgnoredCheckoutDescendants": [
                str(path)
                for path in revision.enrollment.git_ignored_checkout_descendants
            ],
        },
        "scope": {
            "databaseRead": False,
            "crossProjectInventoryChecked": False,
            "gitIgnoreProofCheck": (
                "not-applicable"
                if not revision.enrollment.git_ignored_checkout_descendants
                else "not-available-without-registration resolver evidence"
            ),
            "note": (
                "the standalone doctor has no project registry; registration must "
                "supply occupied-root claims and authenticate any pinned Git-ignore "
                "proofs before persisting the revision"
            ),
        },
    }


def _artifact_documents(
    *,
    revision: ProjectRevision,
    job: dict[str, object],
) -> list[JSONValue]:
    values = job.get("artifacts", [])
    if type(values) is not list:
        raise OperatorServiceError("resolved job.artifacts must be a JSON array")
    documents: list[JSONValue] = []
    for index, value in enumerate(values):
        artifact = _plain_object(
            value,
            field_name=f"resolved job.artifacts[{index}]",
        )
        name = _text(artifact.get("name"), field_name=f"artifact[{index}].name")
        root_name = _text(
            artifact.get("root"), field_name=f"artifact {name!r}.root"
        )
        relative_path = _text(
            artifact.get("path"), field_name=f"artifact {name!r}.path"
        )
        root = revision.enrollment.artifact_root(root_name)
        resolved_path = resolve_artifact_path(
            root.path,
            relative_path,
            field_name=f"artifact {name!r}.path",
        )
        required = artifact.get("required", False)
        if type(required) is not bool:
            raise OperatorServiceError(
                f"artifact {name!r}.required must be a boolean"
            )
        documents.append(
            {
                "name": name,
                "root": root_name,
                "rootPath": str(root.path),
                "relativePath": relative_path,
                "resolvedPath": str(resolved_path),
                "type": _text(
                    artifact.get("type"), field_name=f"artifact {name!r}.type"
                ),
                "required": required,
            }
        )
    return documents


def _preemption_document(job: Mapping[str, object], authorized: bool) -> JSONValue:
    capabilities = job.get("capabilities")
    cooperative: object | None = None
    if type(capabilities) is dict:
        cooperative = capabilities.get("cooperativeYield")
    declared = type(cooperative) is dict
    return {
        "automatic": False,
        "authorized": authorized,
        "cooperativeYieldDeclared": declared,
        "eligibleForManualPreemption": declared and authorized,
        "cooperativeYield": (
            cast(dict[str, JSONValue], cooperative) if declared else None
        ),
    }


def submission_dry_run(
    *,
    revision: ProjectRevision,
    submission: Submission,
) -> dict[str, JSONValue]:
    """Resolve one submission into complete evidence without state mutation."""

    if type(revision) is not ProjectRevision:
        raise TypeError(
            f"revision must be exactly ProjectRevision, got "
            f"{type(revision).__name__}"
        )
    if type(submission) is not Submission:
        raise TypeError(
            f"submission must be exactly Submission, got "
            f"{type(submission).__name__}"
        )
    resolved = compile_admission_from_revision(
        revision=revision,
        submission=submission,
    )
    snapshot = resolved.snapshot
    execution = snapshot.resolved_document
    job_value = execution.get("job")
    if type(job_value) is not dict:
        raise OperatorServiceError("resolved execution has no job object")
    job = cast(dict[str, object], job_value)
    environment_name = _text(
        job.get("environment"), field_name="resolved job.environment"
    )
    environment = revision.enrollment.environment(environment_name)
    working_directory = job.get("workingDirectory")
    if working_directory is not None:
        working_directory = _text(
            working_directory,
            field_name="resolved job.workingDirectory",
        )
    command = job.get("command")
    if type(command) is not dict:
        raise OperatorServiceError("resolved job.command must be a JSON object")
    resources = job.get("resources", {})
    if type(resources) is not dict:
        raise OperatorServiceError("resolved job.resources must be a JSON object")
    policy = snapshot.submission_policy
    card_document = snapshot.card_document
    card_metadata = cast(dict[str, object], card_document["metadata"])

    return {
        "operation": "submission.dry-run",
        "outputContract": "read-only diagnostic; not persistent protocol evidence",
        "valid": True,
        "wouldMutateState": False,
        "identity": {
            **_revision_identity_document(resolved),
            "experimentId": cast(str, card_metadata["experimentId"]),
            "jobId": policy.job_id,
        },
        "git": {
            "canonicalToplevelVerified": True,
            "fullCommitVerified": True,
            "projectBlob": _blob_document(resolved.project_blob),
            "cardBlob": _blob_document(resolved.card_blob),
            "extensionSchemaBlob": _blob_document(
                resolved.extension_schema_blob
            ),
        },
        "digests": {
            "projectSourceSha256": snapshot.project_source_sha256,
            "projectNormalizedSha256": snapshot.project_normalized_sha256,
            "cardSourceSha256": snapshot.card_source_sha256,
            "cardNormalizedSha256": snapshot.card_normalized_sha256,
            "resolvedExecutionSha256": snapshot.resolved_sha256,
            "enrollmentSha256": revision.enrollment.sha256,
            "compilerPackageVersion": snapshot.package_version,
        },
        "schemas": {
            "project": {
                "apiVersion": snapshot.project_schema.api_version,
                "kind": snapshot.project_schema.kind,
                "id": snapshot.project_schema.schema_id,
                "sha256": snapshot.project_schema.sha256,
            },
            "card": {
                "apiVersion": snapshot.card_schema.api_version,
                "kind": snapshot.card_schema.kind,
                "id": snapshot.card_schema.schema_id,
                "sha256": snapshot.card_schema.sha256,
            },
            "extension": (
                None
                if snapshot.extension_schema is None
                else {
                    "path": snapshot.extension_schema.reference_path,
                    "schemaId": snapshot.extension_schema.schema_id,
                    "sourceSha256": snapshot.extension_schema.source_sha256,
                    "canonicalSha256": (
                        snapshot.extension_schema.canonical_sha256
                    ),
                }
            ),
        },
        "paths": {
            "checkoutDirectory": str(revision.enrollment.checkout_directory),
            "projectManifestPath": revision.project_source_path,
            "cardPath": policy.card_path,
            "worktree": {
                "created": False,
                "base": "queue-owned detached worktree at the pinned commit",
                "workingDirectory": cast(JSONValue, working_directory),
                "note": (
                    "actual canonical worktree and wrapper existence are checked "
                    "after a global queue item ID is allocated"
                ),
            },
            "volumes": [
                mount.to_document() for mount in revision.enrollment.mounts
            ],
            "artifacts": _artifact_documents(revision=revision, job=job),
        },
        "environment": {
            "name": environment.name,
            "executableSearchDirectories": [
                str(path) for path in environment.executable_search_directories
            ],
            "commandPrefixArgv": (
                None
                if environment.command_prefix_argv is None
                else list(environment.command_prefix_argv)
            ),
            "inheritVariableNames": list(environment.inherit_variables),
            "literalValuesIncluded": False,
            "queueInjectedNames": [
                "CUDA_VISIBLE_DEVICES",
                "EXPERIMENT_QUEUE_GIT_COMMIT",
                "EXPERIMENT_QUEUE_ITEM_ID",
                "EXPERIMENT_QUEUE_PROJECT_KEY",
                "EXPERIMENT_QUEUE_PROJECT_REVISION",
            ],
            "queueInjectedValuesAvailable": "only after item allocation/dispatch",
        },
        "command": cast(dict[str, JSONValue], command),
        "resources": cast(dict[str, JSONValue], resources),
        "preemption": _preemption_document(
            job,
            policy.preemption_authorized,
        ),
        "submissionPolicy": policy.to_document(),
        "resolvedExecution": execution,
        "scope": {
            "databaseRead": False,
            "crossProjectInventoryRechecked": False,
            "gitIgnoreProofsRechecked": False,
            "note": (
                "this dry-run consumes a validated immutable revision; standalone "
                "resolution does not reinterpret registry-owned overlap or "
                "Git-ignore proof evidence"
            ),
        },
    }


__all__ = [
    "OperatorServiceError",
    "doctor_project_revision",
    "experiment_card_scaffold",
    "export_editor_schema",
    "load_enrollment_document",
    "project_manifest_scaffold",
    "submission_dry_run",
    "validate_card_source",
    "validate_project_source",
]
