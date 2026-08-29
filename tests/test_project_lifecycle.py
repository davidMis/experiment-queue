"""Verify validated project enrollment, revision, lifecycle, and health models."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json
import os
from pathlib import Path

import pytest

import experiment_queue.project_lifecycle as lifecycle_module
from experiment_queue.authoring import Project, VolumeAccess
from experiment_queue.project_lifecycle import (
    ArtifactRootBinding,
    Enrollment,
    EnvironmentBinding,
    HostRootClaim,
    LifecycleValidationError,
    MountBinding,
    ProjectHealth,
    ProjectLifecycle,
    ProjectRevision,
    ProjectRuntimeState,
    RegisteredProject,
    validate_lifecycle_transition,
)
from experiment_queue.schema_registry import JSON_SCHEMA_DIALECT, PROJECT_V1_SCHEMA
from experiment_queue.serialization import canonical_json_bytes, sha256_bytes


NOW = "2026-08-28T12:34:56+00:00"
ACTOR = "test:operator"


def project_document(
    *,
    key: str = "fixture-project",
    display_name: str = "Fixture Project",
    allow_variables: list[str] | None = None,
    extension_schema: dict[str, object] | None = None,
) -> dict[str, object]:
    """Return one portable Project/v1 with required and optional bindings."""

    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "Project",
        "metadata": {"key": key, "displayName": display_name},
        "spec": {
            "cardRoots": ["cards"],
            "volumes": [
                {"name": "scratch", "access": "readWrite", "required": True},
                {"name": "inputs", "access": "readOnly"},
            ],
            "environments": [
                {"name": "python"},
                {"name": "analysis"},
            ],
            "environmentPolicy": {
                "inherit": "allowlist",
                "allowVariables": allow_variables
                if allow_variables is not None
                else ["WANDB_API_KEY", "HTTP_PROXY"],
            },
            "supportedProtocols": [],
        },
    }
    if extension_schema is not None:
        spec = document["spec"]
        assert isinstance(spec, dict)
        spec["extensionSchema"] = extension_schema
    return document


def source_bytes(project: Project) -> bytes:
    """Encode one validated Project as exact pretty source evidence."""

    return (json.dumps(project.to_document(), indent=2) + "\n").encode()


def extension_schema_source() -> bytes:
    """Return one strict Draft 2020-12 extension schema source."""

    return (
        json.dumps(
            {
                "$schema": JSON_SCHEMA_DIALECT,
                "$id": "urn:fixture:extension:v1",
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "project": {"type": "object"},
                    "card": {"type": "object"},
                    "jobs": {"type": "object"},
                },
            },
            indent=2,
        )
        + "\n"
    ).encode()


def mkdir(parent: Path, name: str) -> Path:
    """Create and return one host directory fixture."""

    path = parent / name
    path.mkdir(parents=True)
    return path


def executable(parent: Path) -> Path:
    """Create one inert regular file accepted as a command-prefix executable."""

    path = parent / "prefix"
    path.write_text("fixture\n", encoding="utf-8")
    return path


def environment_binding(
    name: str,
    root: Path,
    *,
    inherit_variables: tuple[str, ...] = (),
    prefix: Path | None = None,
) -> EnvironmentBinding:
    """Return one complete environment fixture."""

    return EnvironmentBinding.create(
        name=name,
        executable_search_directories=[root],
        inherit_variables=inherit_variables,
        command_prefix_argv=None if prefix is None else [prefix, "--fixture"],
    )


def enrollment_fixture(
    tmp_path: Path,
    *,
    project: Project | None = None,
    mounts: list[MountBinding] | None = None,
    environments: list[EnvironmentBinding] | None = None,
    ignored: list[Path] | None = None,
    occupied: list[HostRootClaim] | None = None,
) -> Enrollment:
    """Build a complete Enrollment using mutually disjoint temporary roots."""

    portable = project or Project.from_document(project_document())
    checkout = mkdir(tmp_path, "checkout")
    state = mkdir(tmp_path, "state")
    scratch = mkdir(tmp_path, "scratch")
    inputs = mkdir(tmp_path, "inputs")
    python = mkdir(tmp_path, "python-bin")
    analysis = mkdir(tmp_path, "analysis-bin")
    mount_values = mounts or [
        MountBinding.create(
            name="scratch",
            path=scratch,
            access=VolumeAccess.READ_WRITE,
        ),
        MountBinding.create(
            name="inputs",
            path=inputs,
            access=VolumeAccess.READ_ONLY,
        ),
    ]
    environment_values = environments or [
        environment_binding("python", python),
        environment_binding("analysis", analysis),
    ]
    return Enrollment.create(
        project=portable,
        checkout_directory=checkout,
        project_manifest_path="Project.yaml",
        mounts=mount_values,
        environments=environment_values,
        state_directory=state,
        git_ignored_checkout_descendants=ignored or [],
        occupied_roots=occupied or [],
    )


def revision_fixture(
    tmp_path: Path,
    *,
    revision_id: int = 11,
    project_id: int = 7,
    sequence: int = 1,
    project: Project | None = None,
    enrollment: Enrollment | None = None,
    git_commit: str = "a" * 40,
) -> ProjectRevision:
    """Return one complete immutable ProjectRevision."""

    portable = project or Project.from_document(project_document())
    frozen_enrollment = enrollment or enrollment_fixture(
        tmp_path,
        project=portable,
    )
    return ProjectRevision.create(
        revision_id=revision_id,
        project_id=project_id,
        sequence=sequence,
        project=portable,
        project_source_path="Project.yaml",
        project_source=source_bytes(portable),
        git_commit=git_commit,
        enrollment=frozen_enrollment,
        created_actor=ACTOR,
        created_at=NOW,
    )


def registered_fixture(tmp_path: Path) -> RegisteredProject:
    """Register one active first revision."""

    return RegisteredProject.register(
        revision=revision_fixture(tmp_path),
        reason="initial registration",
        actor=ACTOR,
        changed_at=NOW,
    )


@pytest.mark.parametrize(
    "model",
    [
        MountBinding,
        ArtifactRootBinding,
        EnvironmentBinding,
        HostRootClaim,
        Enrollment,
        ProjectRevision,
        RegisteredProject,
        ProjectRuntimeState,
    ],
)
def test_lifecycle_models_are_factory_only(model: type[object]) -> None:
    """No trusted lifecycle value can bypass its public validation factory."""

    with pytest.raises(TypeError, match="validated-only"):
        model()  # type: ignore[call-arg]


def test_bindings_are_frozen_and_artifact_roots_derive_from_writable_mount(
    tmp_path: Path,
) -> None:
    """Nested host bindings are immutable and never gain independent paths."""

    scratch = mkdir(tmp_path, "scratch")
    mount = MountBinding.create(
        name="scratch",
        path=scratch,
        access="readWrite",
    )
    artifact = ArtifactRootBinding.from_mount(mount)
    assert artifact.name == "scratch"
    assert artifact.path == scratch.resolve()
    with pytest.raises(FrozenInstanceError):
        mount.path = tmp_path  # type: ignore[misc]

    read_only = MountBinding.create(
        name="inputs",
        path=mkdir(tmp_path, "inputs"),
        access="readOnly",
    )
    with pytest.raises(LifecycleValidationError, match="readOnly.*readWrite"):
        ArtifactRootBinding.from_mount(read_only)


def test_binding_paths_must_be_existing_absolute_directories(tmp_path: Path) -> None:
    """Factories retain only canonical host directories."""

    with pytest.raises(LifecycleValidationError, match="must be absolute"):
        MountBinding.create(name="scratch", path="relative", access="readWrite")
    with pytest.raises(LifecycleValidationError, match="existing directory"):
        MountBinding.create(
            name="scratch",
            path=tmp_path / "missing",
            access="readWrite",
        )
    regular = tmp_path / "regular"
    regular.write_text("not a directory", encoding="utf-8")
    with pytest.raises(LifecycleValidationError, match="not a directory"):
        MountBinding.create(name="scratch", path=regular, access="readWrite")


def test_symlink_paths_are_resolved_before_storage(tmp_path: Path) -> None:
    """Stored paths retain canonical targets rather than mutable symlink names."""

    target = mkdir(tmp_path, "target")
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    binding = MountBinding.create(name="scratch", path=alias, access="readWrite")
    assert binding.path == target.resolve()
    assert str(alias) not in binding.to_document()["path"]


def test_environment_binding_round_trips_names_paths_and_prefix(tmp_path: Path) -> None:
    """EnvironmentBinding/v1 never captures inherited ambient values."""

    search = mkdir(tmp_path, "bin")
    prefix = executable(tmp_path)
    binding = EnvironmentBinding.create(
        name="python",
        executable_search_directories=[search],
        inherit_variables=["WANDB_API_KEY", "HTTP_PROXY"],
        command_prefix_argv=[prefix, "--name", "fixture"],
    )
    document = binding.to_document()
    assert document == {
        "apiVersion": "experiment-queue/v1",
        "kind": "EnvironmentBinding",
        "name": "python",
        "executableSearchDirectories": [str(search.resolve())],
        "inheritVariables": ["HTTP_PROXY", "WANDB_API_KEY"],
        "commandPrefixArgv": [
            str(prefix.resolve()),
            "--name",
            "fixture",
        ],
    }
    assert EnvironmentBinding.from_document(document) == binding
    assert "secret" not in json.dumps(document).lower()


@pytest.mark.parametrize(
    ("variable", "message"),
    [
        ("PATH", "construct.*PATH"),
        ("CUDA_VISIBLE_DEVICES", "service-owned"),
        ("EXPERIMENT_QUEUE_STATE_DIR", "service-owned"),
        ("TOKEN=value", "name/value assignment"),
    ],
)
def test_environment_binding_rejects_unsafe_inheritance(
    tmp_path: Path,
    variable: str,
    message: str,
) -> None:
    """Frozen bindings contain variable names only and protect queue ownership."""

    with pytest.raises(LifecycleValidationError, match=message):
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=[mkdir(tmp_path, "bin")],
            inherit_variables=[variable],
        )


def test_environment_binding_rejects_path_separator_in_search_directory(
    tmp_path: Path,
) -> None:
    """One frozen directory can never expand into multiple child PATH entries."""

    ambiguous = mkdir(tmp_path, f"environment{os.pathsep}shadow")
    with pytest.raises(LifecycleValidationError, match="PATH separator"):
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=(ambiguous,),
        )


def test_environment_binding_rejects_literal_values_and_secret_fields(
    tmp_path: Path,
) -> None:
    """No EnvironmentBinding entry point accepts literal ambient values."""

    prefix = executable(tmp_path)
    with pytest.raises(LifecycleValidationError, match="literal environment assignment"):
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=[mkdir(tmp_path, "bin")],
            command_prefix_argv=[prefix, "TOKEN=secret"],
        )

    document: dict[str, object] = {
        "apiVersion": "experiment-queue/v1",
        "kind": "EnvironmentBinding",
        "name": "python",
        "executableSearchDirectories": [str(tmp_path / "bin")],
        "inheritVariables": ["TOKEN"],
        "variables": {"TOKEN": "secret"},
    }
    with pytest.raises(LifecycleValidationError, match="secrets are forbidden"):
        EnvironmentBinding.from_document(document)
    with pytest.raises(LifecycleValidationError, match="keys must be strings"):
        EnvironmentBinding.from_document(
            {1: "not-json"}  # type: ignore[dict-item]
        )


def test_environment_command_prefix_requires_absolute_existing_file(
    tmp_path: Path,
) -> None:
    """Structured prefixes cannot depend on cwd lookup or missing commands."""

    search = mkdir(tmp_path, "bin")
    with pytest.raises(LifecycleValidationError, match="must be absolute"):
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=[search],
            command_prefix_argv=["env", "--fixture"],
        )
    with pytest.raises(LifecycleValidationError, match="existing file"):
        EnvironmentBinding.create(
            name="python",
            executable_search_directories=[search],
            command_prefix_argv=[tmp_path / "missing"],
        )


def test_enrollment_requires_all_required_mounts_and_environments(
    tmp_path: Path,
) -> None:
    """Portable required names must have one complete host binding."""

    project = Project.from_document(project_document())
    checkout = mkdir(tmp_path, "checkout")
    state = mkdir(tmp_path, "state")
    python = environment_binding("python", mkdir(tmp_path, "python"))
    analysis = environment_binding("analysis", mkdir(tmp_path, "analysis"))
    with pytest.raises(LifecycleValidationError, match="missing required.*scratch"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[],
            environments=[python, analysis],
            state_directory=state,
        )

    scratch = MountBinding.create(
        name="scratch",
        path=mkdir(tmp_path, "scratch"),
        access="readWrite",
    )
    with pytest.raises(LifecycleValidationError, match="missing declared.*analysis"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[scratch],
            environments=[python],
            state_directory=state,
        )


def test_optional_volume_may_be_absent_and_writable_mount_can_narrow(
    tmp_path: Path,
) -> None:
    """Enrollment may omit optional volumes and narrow declared access."""

    project = Project.from_document(project_document())
    scratch = MountBinding.create(
        name="scratch",
        path=mkdir(tmp_path, "scratch"),
        access="readOnly",
    )
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=mkdir(tmp_path, "checkout"),
        project_manifest_path="Project.yaml",
        mounts=[scratch],
        environments=[
            environment_binding("python", mkdir(tmp_path, "python")),
            environment_binding("analysis", mkdir(tmp_path, "analysis")),
        ],
        state_directory=mkdir(tmp_path, "state"),
    )
    assert [mount.name for mount in enrollment.mounts] == ["scratch"]
    assert enrollment.artifact_roots == ()


def test_enrollment_rejects_undeclared_duplicate_and_widened_mounts(
    tmp_path: Path,
) -> None:
    """Host binding cannot add names or widen portable volume authority."""

    project = Project.from_document(project_document())
    environment_values = [
        environment_binding("python", mkdir(tmp_path, "python")),
        environment_binding("analysis", mkdir(tmp_path, "analysis")),
    ]
    checkout = mkdir(tmp_path, "checkout")
    state = mkdir(tmp_path, "state")
    scratch = MountBinding.create(
        name="scratch",
        path=mkdir(tmp_path, "scratch"),
        access="readWrite",
    )
    duplicate = MountBinding.create(
        name="scratch",
        path=mkdir(tmp_path, "scratch-two"),
        access="readWrite",
    )
    with pytest.raises(LifecycleValidationError, match="repeats mount"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[scratch, duplicate],
            environments=environment_values,
            state_directory=state,
        )

    unknown = MountBinding.create(
        name="foreign",
        path=mkdir(tmp_path, "foreign"),
        access="readOnly",
    )
    with pytest.raises(LifecycleValidationError, match="undeclared volumes"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[scratch, unknown],
            environments=environment_values,
            state_directory=state,
        )

    widened = MountBinding.create(
        name="inputs",
        path=mkdir(tmp_path, "inputs"),
        access="readWrite",
    )
    with pytest.raises(LifecycleValidationError, match="widens.*readOnly.*readWrite"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[scratch, widened],
            environments=environment_values,
            state_directory=state,
        )


def test_enrollment_environment_allowlist_only_narrows_project(tmp_path: Path) -> None:
    """Host environment authority is an intersection, never an expansion."""

    project = Project.from_document(project_document())
    environments = [
        environment_binding(
            "python",
            mkdir(tmp_path, "python"),
            inherit_variables=("UNDECLARED_TOKEN",),
        ),
        environment_binding("analysis", mkdir(tmp_path, "analysis")),
    ]
    with pytest.raises(LifecycleValidationError, match="outside Project"):
        Enrollment.create(
            project=project,
            checkout_directory=mkdir(tmp_path, "checkout"),
            project_manifest_path="Project.yaml",
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=mkdir(tmp_path, "scratch"),
                    access="readWrite",
                )
            ],
            environments=environments,
            state_directory=mkdir(tmp_path, "state"),
        )


def test_enrollment_json_and_digest_are_exact_frozen_evidence(tmp_path: Path) -> None:
    """Canonical host-resolution bytes are stable and callers receive copies."""

    project = Project.from_document(project_document())
    checkout = mkdir(tmp_path, "checkout")
    state = mkdir(tmp_path, "state")
    scratch = MountBinding.create(
        name="scratch",
        path=mkdir(tmp_path, "scratch"),
        access="readWrite",
    )
    inputs = MountBinding.create(
        name="inputs",
        path=mkdir(tmp_path, "inputs"),
        access="readOnly",
    )
    # Supply reverse order; the frozen document follows portable declaration
    # order while preserving PATH-significant search-directory order.
    enrollment = Enrollment.create(
        project=project,
        checkout_directory=checkout,
        project_manifest_path="Project.yaml",
        mounts=[inputs, scratch],
        environments=[
            environment_binding("analysis", mkdir(tmp_path, "analysis")),
            environment_binding(
                "python",
                mkdir(tmp_path, "python"),
                inherit_variables=("WANDB_API_KEY",),
            ),
        ],
        state_directory=state,
    )
    document = enrollment.to_document()
    assert enrollment.enrollment_json == canonical_json_bytes(document)
    assert enrollment.enrollment_sha256 == sha256_bytes(enrollment.enrollment_json)
    assert [item["name"] for item in document["mounts"]] == ["scratch", "inputs"]
    assert [item["name"] for item in document["environments"]] == [
        "python",
        "analysis",
    ]
    assert document["artifactRoots"] == [
        {"name": "scratch", "path": str(scratch.path)}
    ]
    document["projectKey"] = "changed"
    assert enrollment.to_document()["projectKey"] == "fixture-project"
    with pytest.raises(FrozenInstanceError):
        enrollment.sha256 = "0" * 64  # type: ignore[misc]


@pytest.mark.parametrize("manifest", ["/Project.yaml", "../Project.yaml", "a/./b"])
def test_enrollment_requires_portable_manifest_path(
    tmp_path: Path,
    manifest: str,
) -> None:
    """Enrollment stores one repository-relative Project source name."""

    project = Project.from_document(project_document())
    with pytest.raises(LifecycleValidationError, match="project_manifest_path"):
        Enrollment.create(
            project=project,
            checkout_directory=mkdir(tmp_path, "checkout"),
            project_manifest_path=manifest,
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=mkdir(tmp_path, "scratch"),
                    access="readWrite",
                )
            ],
            environments=[
                environment_binding("python", mkdir(tmp_path, "python")),
                environment_binding("analysis", mkdir(tmp_path, "analysis")),
            ],
            state_directory=mkdir(tmp_path, "state"),
        )


def test_state_and_logical_root_overlap_fail_closed(tmp_path: Path) -> None:
    """State, mounts, and environments cannot equal or contain one another."""

    project = Project.from_document(project_document())
    checkout = mkdir(tmp_path, "checkout")
    state = mkdir(tmp_path, "state")
    nested_mount = mkdir(state, "nested-mount")
    with pytest.raises(LifecycleValidationError, match="state directory.*overlaps"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=nested_mount,
                    access="readWrite",
                )
            ],
            environments=[
                environment_binding("python", mkdir(tmp_path, "python")),
                environment_binding("analysis", mkdir(tmp_path, "analysis")),
            ],
            state_directory=state,
        )

    mount_root = mkdir(tmp_path, "mount")
    env_nested = mkdir(mount_root, "bin")
    with pytest.raises(LifecycleValidationError, match="logical roots.*overlap"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=mount_root,
                    access="readWrite",
                )
            ],
            environments=[
                environment_binding("python", env_nested),
                environment_binding("analysis", mkdir(tmp_path, "analysis-two")),
            ],
            state_directory=state,
        )


def test_checkout_local_root_requires_pinned_git_ignore_proof(tmp_path: Path) -> None:
    """A checkout descendant is admitted only through explicit ignore evidence."""

    project = Project.from_document(project_document())
    checkout = mkdir(tmp_path, "checkout")
    local_root = mkdir(checkout, ".venv")
    python_bin = mkdir(local_root, "bin")
    scratch = mkdir(tmp_path, "scratch")
    analysis = mkdir(tmp_path, "analysis")
    state = mkdir(tmp_path, "state")
    arguments = {
        "project": project,
        "checkout_directory": checkout,
        "project_manifest_path": "Project.yaml",
        "mounts": [
            MountBinding.create(
                name="scratch",
                path=scratch,
                access="readWrite",
            )
        ],
        "environments": [
            environment_binding("python", python_bin),
            environment_binding("analysis", analysis),
        ],
        "state_directory": state,
    }
    with pytest.raises(LifecycleValidationError, match="no Git-ignore proof"):
        Enrollment.create(**arguments)  # type: ignore[arg-type]

    enrollment = Enrollment.create(
        **arguments,  # type: ignore[arg-type]
        git_ignored_checkout_descendants=[local_root],
    )
    assert enrollment.environment("python").executable_search_directories == (
        python_bin.resolve(),
    )
    assert enrollment.git_ignored_checkout_descendants == (local_root.resolve(),)


def test_checkout_may_not_be_equal_to_or_contained_by_binding(tmp_path: Path) -> None:
    """The narrow checkout-local exception is descendant-only."""

    project = Project.from_document(project_document())
    checkout_parent = mkdir(tmp_path, "checkout-parent")
    checkout = mkdir(checkout_parent, "checkout")
    with pytest.raises(LifecycleValidationError, match="equals or contains checkout"):
        Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path="Project.yaml",
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=checkout_parent,
                    access="readWrite",
                )
            ],
            environments=[
                environment_binding("python", mkdir(tmp_path, "python")),
                environment_binding("analysis", mkdir(tmp_path, "analysis")),
            ],
            state_directory=mkdir(tmp_path, "state"),
        )


def test_cross_project_root_overlap_is_rejected_even_read_only(tmp_path: Path) -> None:
    """Version 1 has no implicit shared-volume exception."""

    project = Project.from_document(project_document())
    shared = mkdir(tmp_path, "shared")
    claim = HostRootClaim.create(
        project_key="other-project",
        role="read-only dataset",
        path=shared,
    )
    with pytest.raises(LifecycleValidationError, match="cross-project"):
        Enrollment.create(
            project=project,
            checkout_directory=mkdir(tmp_path, "checkout"),
            project_manifest_path="Project.yaml",
            mounts=[
                MountBinding.create(
                    name="scratch",
                    path=mkdir(tmp_path, "scratch"),
                    access="readWrite",
                ),
                MountBinding.create(
                    name="inputs",
                    path=shared,
                    access="readOnly",
                ),
            ],
            environments=[
                environment_binding("python", mkdir(tmp_path, "python")),
                environment_binding("analysis", mkdir(tmp_path, "analysis")),
            ],
            state_directory=mkdir(tmp_path, "state"),
            occupied_roots=[claim],
        )


def test_project_revision_freezes_exact_source_schema_and_enrollment(
    tmp_path: Path,
) -> None:
    """A revision pins source bytes, canonical content, schema, Git, and host JSON."""

    project = Project.from_document(project_document())
    enrollment = enrollment_fixture(tmp_path, project=project)
    source = source_bytes(project)
    revision = ProjectRevision.create(
        revision_id=19,
        project_id=4,
        sequence=7,
        project=project,
        project_source_path="Project.yaml",
        project_source=source,
        git_commit="A" * 40,
        enrollment=enrollment,
        created_actor=ACTOR,
        created_at=NOW,
    )
    assert revision.id == 19
    assert revision.project_id == 4
    assert revision.project_key == "fixture-project"
    assert revision.sequence == 7
    assert revision.label == "fixture-project:r7"
    assert revision.git_commit == "a" * 40
    assert revision.project_source is source
    assert revision.project_source_sha256 == sha256_bytes(source)
    assert revision.project_normalized_json == canonical_json_bytes(
        project.to_document()
    )
    assert revision.project_normalized_sha256 == sha256_bytes(
        revision.project_normalized_json
    )
    assert revision.project_schema_id == PROJECT_V1_SCHEMA.schema_id
    assert revision.project_schema_sha256 == PROJECT_V1_SCHEMA.sha256
    assert revision.project_schema_api_version == "experiment-queue/v1"
    assert revision.project_schema_kind == "Project"
    installed_version = lifecycle_module.package_version_for("experiment-queue")
    assert revision.validated_package_version == installed_version
    assert revision.extension_schema_source_path is None
    assert revision.extension_schema_source is None
    assert revision.extension_schema_source_sha256 is None
    assert revision.extension_schema_canonical_json is None
    assert revision.extension_schema_canonical_sha256 is None
    assert revision.extension_schema_id is None
    assert revision.enrollment is enrollment
    assert revision.to_document()["enrollmentSha256"] == enrollment.sha256
    assert revision.to_document()["validatedPackageVersion"] == installed_version


def test_project_revision_retains_exact_extension_schema_evidence(
    tmp_path: Path,
) -> None:
    """A declared extension schema is authenticated and frozen with its revision."""

    schema_source = extension_schema_source()
    canonical_schema = canonical_json_bytes(json.loads(schema_source))
    project = Project.from_document(
        project_document(
            extension_schema={
                "path": "schemas/extension.json",
                "sha256": sha256_bytes(canonical_schema),
            }
        )
    )
    enrollment = enrollment_fixture(tmp_path, project=project)
    revision = ProjectRevision.create(
        revision_id=23,
        project_id=7,
        sequence=2,
        project=project,
        project_source_path="Project.yaml",
        project_source=source_bytes(project),
        git_commit="b" * 40,
        enrollment=enrollment,
        created_actor=ACTOR,
        created_at=NOW,
        extension_schema_source=schema_source,
    )
    assert revision.extension_schema_source_path == "schemas/extension.json"
    assert revision.extension_schema_source is schema_source
    assert revision.extension_schema_source_sha256 == sha256_bytes(schema_source)
    assert revision.extension_schema_canonical_json == canonical_schema
    assert revision.extension_schema_canonical_sha256 == sha256_bytes(
        canonical_schema
    )
    assert revision.extension_schema_id == "urn:fixture:extension:v1"
    assert revision.to_document()["extensionSchema"] == {
        "path": "schemas/extension.json",
        "sourceSha256": sha256_bytes(schema_source),
        "canonicalSha256": sha256_bytes(canonical_schema),
        "schemaId": "urn:fixture:extension:v1",
    }
    with pytest.raises(FrozenInstanceError):
        revision.extension_schema_source = b"changed"  # type: ignore[misc]


def test_project_revision_requires_extension_schema_presence_to_match_project(
    tmp_path: Path,
) -> None:
    """Missing and unsolicited schema bytes both fail the revision boundary."""

    plain = Project.from_document(project_document())
    plain_enrollment = enrollment_fixture(tmp_path / "plain", project=plain)
    with pytest.raises(LifecycleValidationError, match="does not declare"):
        ProjectRevision.create(
            revision_id=1,
            project_id=1,
            sequence=1,
            project=plain,
            project_source_path="Project.yaml",
            project_source=source_bytes(plain),
            git_commit="a" * 40,
            enrollment=plain_enrollment,
            created_actor=ACTOR,
            created_at=NOW,
            extension_schema_source=extension_schema_source(),
        )

    declared = Project.from_document(
        project_document(
            extension_schema={"path": "schemas/extension.json"}
        )
    )
    declared_enrollment = enrollment_fixture(
        tmp_path / "declared",
        project=declared,
    )
    with pytest.raises(LifecycleValidationError, match="exact source bytes are missing"):
        ProjectRevision.create(
            revision_id=2,
            project_id=2,
            sequence=1,
            project=declared,
            project_source_path="Project.yaml",
            project_source=source_bytes(declared),
            git_commit="b" * 40,
            enrollment=declared_enrollment,
            created_actor=ACTOR,
            created_at=NOW,
        )
    with pytest.raises(LifecycleValidationError, match="extension schema.*invalid"):
        ProjectRevision.create(
            revision_id=2,
            project_id=2,
            sequence=1,
            project=declared,
            project_source_path="Project.yaml",
            project_source=source_bytes(declared),
            git_commit="b" * 40,
            enrollment=declared_enrollment,
            created_actor=ACTOR,
            created_at=NOW,
            extension_schema_source=b'{"type":"object"}',
        )


def rehydrate_revision(
    revision: ProjectRevision,
    **changes: object,
) -> ProjectRevision:
    """Round-trip one revision through the strict recorded-evidence factory."""

    values: dict[str, object] = {
        "revision_id": revision.id,
        "project_id": revision.project_id,
        "sequence": revision.sequence,
        "recorded_revision_label": revision.label,
        "recorded_display_name": revision.display_name,
        "project": revision.project,
        "project_source_path": revision.project_source_path,
        "project_source": revision.project_source,
        "project_source_sha256": revision.project_source_sha256,
        "project_normalized_json": revision.project_normalized_json,
        "project_normalized_sha256": revision.project_normalized_sha256,
        "project_schema_api_version": revision.project_schema_api_version,
        "project_schema_kind": revision.project_schema_kind,
        "project_schema_id": revision.project_schema_id,
        "project_schema_sha256": revision.project_schema_sha256,
        "git_commit": revision.git_commit,
        "enrollment": revision.enrollment,
        "enrollment_json": revision.enrollment.canonical_json,
        "enrollment_sha256": revision.enrollment.sha256,
        "extension_schema_source": revision.extension_schema_source,
        "extension_schema_source_path": revision.extension_schema_source_path,
        "extension_schema_source_sha256": (
            revision.extension_schema_source_sha256
        ),
        "extension_schema_canonical_json": (
            revision.extension_schema_canonical_json
        ),
        "extension_schema_canonical_sha256": (
            revision.extension_schema_canonical_sha256
        ),
        "extension_schema_id": revision.extension_schema_id,
        "validated_package_version": revision.validated_package_version,
        "created_actor": revision.created_actor,
        "created_at": revision.created_at,
    }
    values.update(changes)
    return ProjectRevision.from_recorded_evidence(**values)  # type: ignore[arg-type]


def test_revision_package_provenance_is_internal_and_rehydrates_old_versions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Creation trusts installed metadata while row loading preserves old evidence."""

    monkeypatch.setattr(
        lifecycle_module,
        "package_version_for",
        lambda distribution: "7.4.1-test",
    )
    revision = revision_fixture(tmp_path)
    assert revision.validated_package_version == "7.4.1-test"

    arguments: dict[str, object] = {
        "revision_id": 1,
        "project_id": 1,
        "sequence": 1,
        "project": revision.project,
        "project_source_path": revision.project_source_path,
        "project_source": revision.project_source,
        "git_commit": revision.git_commit,
        "enrollment": revision.enrollment,
        "created_actor": ACTOR,
        "created_at": NOW,
        "validated_package_version": "forged",
    }
    with pytest.raises(TypeError, match="unexpected keyword.*validated_package"):
        ProjectRevision.create(**arguments)  # type: ignore[arg-type]

    def must_not_read_current_metadata(_distribution: str) -> str:
        raise AssertionError("rehydration must not reinterpret an old package version")

    monkeypatch.setattr(
        lifecycle_module,
        "package_version_for",
        must_not_read_current_metadata,
    )
    loaded = rehydrate_revision(revision)
    assert loaded == revision
    assert loaded.validated_package_version == "7.4.1-test"


def test_revision_creation_requires_valid_installed_package_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing or malformed validator provenance cannot create trusted revisions."""

    def missing_metadata(distribution: str) -> str:
        raise lifecycle_module.PackageNotFoundError(distribution)

    monkeypatch.setattr(
        lifecycle_module,
        "package_version_for",
        missing_metadata,
    )
    with pytest.raises(LifecycleValidationError, match="metadata is unavailable"):
        revision_fixture(tmp_path / "missing")

    monkeypatch.setattr(
        lifecycle_module,
        "package_version_for",
        lambda _distribution: " bad-version ",
    )
    with pytest.raises(LifecycleValidationError, match="without surrounding"):
        revision_fixture(tmp_path / "malformed")


def test_recorded_revision_rehydration_authenticates_every_digest(
    tmp_path: Path,
) -> None:
    """A stored row cannot change exact source, schema, Enrollment, or version data."""

    revision = revision_fixture(tmp_path)
    assert rehydrate_revision(revision) == revision
    with pytest.raises(LifecycleValidationError, match="project_source_sha256"):
        rehydrate_revision(revision, project_source_sha256="0" * 64)
    with pytest.raises(LifecycleValidationError, match="enrollment_sha256"):
        rehydrate_revision(revision, enrollment_sha256="0" * 64)
    with pytest.raises(LifecycleValidationError, match="project_schema_id"):
        rehydrate_revision(revision, project_schema_id="urn:forged")
    with pytest.raises(LifecycleValidationError, match="validated_package_version"):
        rehydrate_revision(revision, validated_package_version="")


@pytest.mark.parametrize("git_commit", ["main", "abcd", "g" * 40, "a" * 41])
def test_project_revision_requires_full_git_object(
    tmp_path: Path,
    git_commit: str,
) -> None:
    """Branches, tags, abbreviations, and malformed object IDs are not identity."""

    with pytest.raises(LifecycleValidationError, match="full 40- or 64"):
        revision_fixture(tmp_path, git_commit=git_commit)


def test_project_revision_rejects_source_path_or_content_mismatch(
    tmp_path: Path,
) -> None:
    """Trusted resolver evidence must name and match the enrolled Project exactly."""

    project = Project.from_document(project_document())
    enrollment = enrollment_fixture(tmp_path, project=project)
    with pytest.raises(LifecycleValidationError, match="does not equal Enrollment"):
        ProjectRevision.create(
            revision_id=1,
            project_id=1,
            sequence=1,
            project=project,
            project_source_path="other.yaml",
            project_source=source_bytes(project),
            git_commit="a" * 40,
            enrollment=enrollment,
            created_actor=ACTOR,
            created_at=NOW,
        )

    changed = Project.from_document(
        project_document(display_name="Changed content")
    )
    with pytest.raises(LifecycleValidationError, match="does not normalize"):
        ProjectRevision.create(
            revision_id=1,
            project_id=1,
            sequence=1,
            project=changed,
            project_source_path="Project.yaml",
            project_source=source_bytes(project),
            git_commit="a" * 40,
            enrollment=enrollment,
            created_actor=ACTOR,
            created_at=NOW,
        )


def test_project_revision_rechecks_frozen_host_paths(tmp_path: Path) -> None:
    """Removed or redirected Enrollment roots cannot enter a new revision."""

    project = Project.from_document(project_document())
    enrollment = enrollment_fixture(tmp_path, project=project)
    environment_root = enrollment.environment(
        "python"
    ).executable_search_directories[0]
    environment_root.rmdir()
    with pytest.raises(LifecycleValidationError, match="existing directory"):
        ProjectRevision.create(
            revision_id=1,
            project_id=1,
            sequence=1,
            project=project,
            project_source_path="Project.yaml",
            project_source=source_bytes(project),
            git_commit="a" * 40,
            enrollment=enrollment,
            created_actor=ACTOR,
            created_at=NOW,
        )


def test_registration_requires_first_revision_and_keeps_current_identity(
    tmp_path: Path,
) -> None:
    """Every registered Project begins complete with one positive current revision."""

    revision = revision_fixture(tmp_path)
    registered = RegisteredProject.register(
        revision=revision,
        initial_lifecycle="paused",
        reason="operator requested paused registration",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert registered.id == revision.project_id
    assert registered.key == revision.project_key
    assert registered.display_name == revision.display_name
    assert registered.lifecycle is ProjectLifecycle.PAUSED
    assert registered.current_revision_id == revision.id
    assert registered.current_revision_sequence == 1

    with pytest.raises(LifecycleValidationError, match="first.*sequence.*1"):
        RegisteredProject.register(
            revision=revision_fixture(tmp_path / "later", sequence=2),
            reason="invalid registration",
            actor=ACTOR,
            changed_at=NOW,
        )
    with pytest.raises(LifecycleValidationError, match="not archived"):
        RegisteredProject.register(
            revision=revision,
            initial_lifecycle="archived",
            reason="invalid registration",
            actor=ACTOR,
            changed_at=NOW,
        )


def test_imported_history_adoption_requires_and_retains_later_typed_revision(
    tmp_path: Path,
) -> None:
    """A typed revision after legacy import is represented without fake sequence 1."""

    revision = revision_fixture(
        tmp_path / "adopted",
        revision_id=29,
        project_id=3,
        sequence=4,
    )
    adopted = RegisteredProject.adopt_imported_history(
        revision=revision,
        lifecycle="paused",
        reason="verified portable Project after offline import",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert adopted.id == 3
    assert adopted.key == revision.project_key
    assert adopted.current_revision_id == 29
    assert adopted.current_revision_sequence == 4
    assert adopted.lifecycle is ProjectLifecycle.PAUSED

    with pytest.raises(LifecycleValidationError, match="sequence greater than 1"):
        RegisteredProject.adopt_imported_history(
            revision=revision_fixture(tmp_path / "new", sequence=1),
            lifecycle="active",
            reason="not imported",
            actor=ACTOR,
            changed_at=NOW,
        )


def test_revision_activation_is_append_only_gap_tolerant_and_updates_display(
    tmp_path: Path,
) -> None:
    """Repointing activates a newer row and never mutates or reuses an old one."""

    first_project = Project.from_document(project_document())
    first = revision_fixture(tmp_path / "first", project=first_project)
    registered = RegisteredProject.register(
        revision=first,
        reason="registered",
        actor=ACTOR,
        changed_at=NOW,
    )
    later_project = Project.from_document(
        project_document(display_name="Renamed Project")
    )
    later = revision_fixture(
        tmp_path / "later",
        revision_id=22,
        project_id=first.project_id,
        sequence=4,
        project=later_project,
    )
    updated = registered.with_current_revision(later)
    assert updated.current_revision_id == 22
    assert updated.current_revision_sequence == 4
    assert updated.display_name == "Renamed Project"
    assert registered.current_revision_id == first.id
    assert updated.lifecycle is ProjectLifecycle.ACTIVE

    with pytest.raises(LifecycleValidationError, match="greater than current"):
        updated.with_current_revision(first)

    reused_id = revision_fixture(
        tmp_path / "reused-id",
        revision_id=updated.current_revision_id,
        project_id=updated.id,
        sequence=5,
        project=later_project,
    )
    with pytest.raises(LifecycleValidationError, match="reuses current revision id"):
        updated.with_current_revision(reused_id)


def test_revision_activation_requires_matching_registered_project(
    tmp_path: Path,
) -> None:
    """Revision ownership uses both positive ID and immutable project key."""

    registered = registered_fixture(tmp_path / "registered")
    wrong_id = revision_fixture(
        tmp_path / "wrong",
        revision_id=23,
        project_id=999,
        sequence=2,
    )
    with pytest.raises(LifecycleValidationError, match="belongs to Project"):
        registered.with_current_revision(wrong_id)


def test_lifecycle_edges_and_pause_semantics(tmp_path: Path) -> None:
    """Only active/paused toggles and paused-to-archived are admitted."""

    registered = registered_fixture(tmp_path)
    paused = registered.transition(
        "paused",
        reason="maintenance",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert paused.lifecycle is ProjectLifecycle.PAUSED
    assert paused.admission_allowed
    assert paused.revision_creation_allowed
    assert not paused.dispatch_allowed_by_lifecycle
    resumed = paused.transition(
        "active",
        reason="maintenance complete",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert resumed.lifecycle is ProjectLifecycle.ACTIVE
    assert resumed.dispatch_allowed_by_lifecycle

    with pytest.raises(LifecycleValidationError, match="paused before archival"):
        registered.transition(
            "archived",
            reason="invalid direct archive",
            actor=ACTOR,
            changed_at=NOW,
        )
    with pytest.raises(LifecycleValidationError, match="no-op"):
        registered.transition(
            "active",
            reason="no change",
            actor=ACTOR,
            changed_at=NOW,
        )


@pytest.mark.parametrize(
    ("states", "incomplete_cleanup", "message"),
    [
        (["succeeded", "running"], False, "nonterminal"),
        (["succeeded"], True, "cleanup is incomplete"),
        (["future-state"], False, "unknown state"),
    ],
)
def test_archival_requires_terminal_work_and_complete_cleanup(
    tmp_path: Path,
    states: list[str],
    incomplete_cleanup: bool,
    message: str,
) -> None:
    """History remains while active work and cleanup block permanent archival."""

    paused = registered_fixture(tmp_path).transition(
        "paused",
        reason="preparing archive",
        actor=ACTOR,
        changed_at=NOW,
    )
    with pytest.raises(LifecycleValidationError, match=message):
        paused.transition(
            "archived",
            reason="archive",
            actor=ACTOR,
            changed_at=NOW,
            queue_item_states=states,
            incomplete_cleanup=incomplete_cleanup,
        )


def test_archival_is_permanent_and_retains_current_revision(tmp_path: Path) -> None:
    """Archived Projects preserve current identity and expose no unarchive edge."""

    paused = registered_fixture(tmp_path).transition(
        "paused",
        reason="preparing archive",
        actor=ACTOR,
        changed_at=NOW,
    )
    archived = paused.transition(
        "archived",
        reason="retired",
        actor=ACTOR,
        changed_at=NOW,
        queue_item_states=["succeeded", "failed", "removed"],
        incomplete_cleanup=False,
    )
    assert archived.lifecycle is ProjectLifecycle.ARCHIVED
    assert archived.current_revision_id == paused.current_revision_id
    assert archived.current_revision_sequence == paused.current_revision_sequence
    assert not archived.admission_allowed
    assert not archived.revision_creation_allowed
    assert not archived.dispatch_allowed_by_lifecycle
    with pytest.raises(LifecycleValidationError, match="permanent"):
        archived.transition(
            "active",
            reason="forbidden unarchive",
            actor=ACTOR,
            changed_at=NOW,
        )
    with pytest.raises(LifecycleValidationError, match="archived"):
        archived.with_current_revision(
            revision_fixture(
                tmp_path / "later",
                revision_id=55,
                project_id=archived.id,
                sequence=2,
            )
        )


def test_free_transition_validator_returns_typed_target() -> None:
    """Storage layers may validate an edge before constructing row models."""

    assert (
        validate_lifecycle_transition("active", "paused")
        is ProjectLifecycle.PAUSED
    )


def test_runtime_health_is_project_scoped_and_separate_from_lifecycle(
    tmp_path: Path,
) -> None:
    """An open child circuit blocks only its matching active Project."""

    registered = registered_fixture(tmp_path)
    healthy = ProjectRuntimeState.create(
        project_id=registered.id,
        project_key=registered.key,
        reason="registered healthy",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert healthy.health is ProjectHealth.CLOSED
    assert healthy.circuit_failure_count == 0
    assert registered.dispatch_allowed(healthy)

    first_failure = healthy.record_failure(
        reason="child failed",
        actor="scheduler",
        changed_at=NOW,
        open_circuit=False,
    )
    assert first_failure.circuit_failure_count == 1
    assert not first_failure.blocks_dispatch
    opened = first_failure.record_failure(
        reason="failure threshold reached",
        actor="scheduler",
        changed_at=NOW,
        open_circuit=True,
    )
    assert opened.health is ProjectHealth.OPEN
    assert opened.circuit_failure_count == 2
    assert opened.blocks_dispatch
    assert not registered.dispatch_allowed(opened)
    assert registered.lifecycle is ProjectLifecycle.ACTIVE

    cleared = opened.close_circuit(
        reason="operator repaired project",
        actor=ACTOR,
        changed_at=NOW,
    )
    assert cleared.health is ProjectHealth.CLOSED
    assert cleared.circuit_failure_count == 0
    assert registered.dispatch_allowed(cleared)


def test_runtime_state_validates_identity_count_and_dispatch_owner(
    tmp_path: Path,
) -> None:
    """Health evidence cannot be negative, ownerless, or borrowed by a Project."""

    with pytest.raises(LifecycleValidationError, match="positive"):
        ProjectRuntimeState.create(
            project_id=0,
            project_key="fixture-project",
            reason="invalid",
            actor=ACTOR,
            changed_at=NOW,
        )
    with pytest.raises(LifecycleValidationError, match="positive.*failure"):
        ProjectRuntimeState.create(
            project_id=1,
            project_key="fixture-project",
            health="open",
            circuit_failure_count=0,
            reason="invalid",
            actor=ACTOR,
            changed_at=NOW,
        )

    registered = registered_fixture(tmp_path)
    foreign = ProjectRuntimeState.create(
        project_id=999,
        project_key="other-project",
        reason="healthy",
        actor=ACTOR,
        changed_at=NOW,
    )
    with pytest.raises(LifecycleValidationError, match="identity does not match"):
        registered.dispatch_allowed(foreign)


@pytest.mark.parametrize(
    "changed_at",
    ["20260828T123456+00:00", "2026-08-28 12:34:56+00:00", "not-a-time"],
)
def test_lifecycle_metadata_requires_rfc3339_timestamp(
    tmp_path: Path,
    changed_at: str,
) -> None:
    """Audit metadata cannot use ambiguous or Python-only timestamp spellings."""

    with pytest.raises(LifecycleValidationError, match="RFC 3339"):
        RegisteredProject.register(
            revision=revision_fixture(tmp_path),
            reason="registration",
            actor=ACTOR,
            changed_at=changed_at,
        )
