"""Production project-aware command line for exact schema-v5 queue state.

Presentation and argument parsing live here; every persistent read or mutation
is delegated to typed repositories, lifecycle/admission services, the scheduler
controller/service, or the explicit offline migration entry point.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path, PurePosixPath
import sys
from typing import cast

from experiment_queue import __version__
from experiment_queue.admission import AdmissionError, Submission
from experiment_queue.authoring import AuthoringValidationError, Project
from experiment_queue.config import StateDirectoryError, resolve_state_dir
from experiment_queue.continuation_v5 import V5ContinuationError
from experiment_queue.database_v5 import V5DatabaseError, V5QueueStore
from experiment_queue.execution import ExecutionValidationError
from experiment_queue.extensions import ExtensionSchemaError, ExtensionValidationError
from experiment_queue.git_resolver import (
    GitResolverError,
    compile_admission_from_revision,
    verify_git_ignored_checkout_descendants,
    verify_project_revision,
)
from experiment_queue.legacy_continuation_v0 import LegacyV0ContinuationError
from experiment_queue.migrate_v5 import V5MigrationError, migrate_legacy_state
from experiment_queue.operator_cli import (
    _bindings_object,
    _optional_source,
    _read_bytes,
    _write_scaffold,
)
from experiment_queue.operator_services import (
    OperatorServiceError,
    doctor_project_revision,
    experiment_card_scaffold,
    export_editor_schema,
    load_enrollment_document,
    project_manifest_scaffold,
    submission_dry_run,
    validate_card_source,
    validate_project_source,
)
from experiment_queue.reservation_v5 import (
    V5GpuReservation,
    V5ReservationError,
    V5ReservationService,
)
from experiment_queue.project_lifecycle import (
    Enrollment,
    EnvironmentBinding,
    LifecycleValidationError,
    ProjectLifecycle,
    ProjectRevision,
    ProjectRuntimeState,
    RegisteredProject,
)
from experiment_queue.queue_export import (
    QueueExport,
    QueueExportError,
    binary_evidence_document,
    database_instance_document,
    json_evidence_document,
    wire_evidence_document,
)
from experiment_queue.scheduler_service_v5 import (
    V5SchedulerService,
    V5SchedulerServiceError,
    query_gpus,
)
from experiment_queue.scheduler_v5 import V5SchedulerError, V5SchedulingController
from experiment_queue.serialization import JSONValue, canonical_json_bytes, sha256_bytes
from experiment_queue.v5_operator_repository import (
    V5ArtifactRecord,
    V5GpuAllowlistEntry,
    V5OperatorError,
    V5OperatorItemView,
    V5OperatorRepository,
    V5ProjectSummary,
    V5RevisionSummary,
)
from experiment_queue.v5_repository import (
    V5Event,
    V5ProjectRepository,
    V5QueueItem,
    V5RepositoryError,
)


_HANDLED_ERRORS = (
    AdmissionError,
    AuthoringValidationError,
    ExecutionValidationError,
    ExtensionSchemaError,
    ExtensionValidationError,
    GitResolverError,
    LegacyV0ContinuationError,
    LifecycleValidationError,
    OperatorServiceError,
    StateDirectoryError,
    V5ContinuationError,
    V5DatabaseError,
    V5MigrationError,
    V5OperatorError,
    V5RepositoryError,
    V5SchedulerError,
    V5SchedulerServiceError,
    V5ReservationError,
    QueueExportError,
    OSError,
)


@dataclass(frozen=True, slots=True)
class _Result:
    document: dict[str, JSONValue]
    readable: str
    protocol_source: bytes | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _output_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        help=(
            "Emit one machine-readable diagnostic JSON object instead of the "
            "default concise operator text; output is not a versioned protocol."
        ),
    )


def _project_selector(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--project",
        help=(
            "Registered Project key or positive database ID. If omitted, infer "
            "only when the canonical current directory is inside exactly one "
            "current registered checkout."
        ),
    )


def _actor_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--actor",
        required=True,
        help=(
            "Non-empty operator identity recorded in the append-only event or "
            "lifecycle evidence; use the authenticated local principal name."
        ),
    )


def _reason_argument(parser: argparse.ArgumentParser, *, action: str) -> None:
    parser.add_argument(
        "--reason",
        required=True,
        help=f"Log-safe reason preserved with the {action} audit event.",
    )


def _revision_input_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "checkout",
        type=Path,
        help=(
            "Canonical absolute existing Git checkout; no repository is inferred "
            "from the queue state or current directory."
        ),
    )
    parser.add_argument(
        "--manifest",
        default="Project.yaml",
        help=(
            "Portable repository-relative Project/v1 path at the pinned commit "
            "(default: Project.yaml); absolute paths and traversal are refused."
        ),
    )
    environment_source = parser.add_mutually_exclusive_group()
    environment_source.add_argument(
        "--enrollment",
        type=Path,
        help=(
            "Optional advanced host Enrollment document. Omit it for the trusted "
            "single-environment workflow: no volumes are bound and the declared "
            "environment uses CHECKOUT/.venv/bin."
        ),
    )
    environment_source.add_argument(
        "--environment-bin",
        type=Path,
        help=(
            "Executable directory for automatic enrollment (default: "
            "CHECKOUT/.venv/bin). A venv root or Python executable is also "
            "accepted and normalized. Relative paths are resolved beneath "
            "CHECKOUT, and the selected path must already exist. Cannot be "
            "combined with --enrollment."
        ),
    )
    parser.add_argument(
        "--git-commit",
        required=True,
        help=(
            "Exact full 40- or 64-character Git commit object ID. Branches, tags, "
            "abbreviations, fetching, and working-tree-only evidence are refused."
        ),
    )
    _actor_argument(parser)


def _submission_arguments(parser: argparse.ArgumentParser) -> None:
    _project_selector(parser)
    parser.add_argument(
        "--card-path",
        required=True,
        help=(
            "Repository-relative ExperimentCard/v1 path beneath a declared card "
            "root; exact bytes are read from the current revision commit tree."
        ),
    )
    parser.add_argument(
        "--job-id",
        required=True,
        help="Exact card job ID; sibling jobs are never selected implicitly.",
    )
    parser.add_argument(
        "--operator",
        required=True,
        help="Authenticated operator identity frozen into admission policy.",
    )
    parser.add_argument(
        "--bindings-json",
        default="{}",
        help=(
            "Strict inline JSON object of whole-value parameter replacements "
            "(default: {}); duplicate keys and non-finite values are refused."
        ),
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help=(
            "Signed 64-bit global scheduling priority (default: 0); changing "
            "priority never authorizes automatic preemption."
        ),
    )
    parser.add_argument(
        "--hold-reason",
        help="Optional initial hold reason; the admitted item starts held.",
    )
    parser.add_argument(
        "--dependency",
        type=int,
        action="append",
        default=[],
        help=(
            "Positive existing global queue-item ID dependency; repeat for more. "
            "Dependencies may cross Projects, are authorized by global ID, and "
            "are persisted in ascending ID order."
        ),
    )
    parser.add_argument(
        "--authorize-preemption",
        action="store_true",
        help=(
            "Authorize explicit manual cooperative preemption only when the job "
            "declares compatible request/receipt protocols."
        ),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the production v5 parser without changing package entry points."""

    parser = argparse.ArgumentParser(
        prog="python -m experiment_queue.cli_v5",
        description=(
            "Operate explicit multi-Project schema-v5 state. Existing legacy "
            "schemas are never migrated at startup."
        ),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help=(
            "Absolute schema-v5 state directory. Required unless "
            "EXPERIMENT_QUEUE_STATE_DIR is set; no cwd/project-relative fallback "
            "exists. Only explicit project register may create fresh v5 state; "
            "read/control/serve commands require an existing exact-v5 database."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True, title="commands")

    project = commands.add_parser(
        "project",
        help="Scaffold, register, inspect, revise, or transition a Project.",
    )
    project_actions = project.add_subparsers(
        dest="project_action", required=True, title="project commands"
    )
    initialize = project_actions.add_parser(
        "init", help="Create a minimal portable Project/v1 manifest scaffold."
    )
    initialize.add_argument(
        "--key",
        required=True,
        help="Immutable lowercase hyphenated Project key, at most 63 characters.",
    )
    initialize.add_argument(
        "--display-name",
        required=True,
        help="Human-readable Project name safely embedded as YAML text.",
    )
    initialize.add_argument(
        "--output",
        type=Path,
        default=Path("Project.yaml"),
        help=(
            "Manifest path to create (default: ./Project.yaml); the parent must "
            "exist and relative paths use the current directory."
        ),
    )
    initialize.add_argument(
        "--force",
        action="store_true",
        help="Atomically replace exactly --output; no directory is removed.",
    )
    _output_argument(initialize)

    for action in ("validate", "explain"):
        command = project_actions.add_parser(
            action,
            help=f"{action.title()} local Project/v1 source without queue mutation.",
        )
        command.add_argument(
            "--manifest",
            required=True,
            type=Path,
            help="Path to exact local Project/v1 YAML bytes; never modified.",
        )
        command.add_argument(
            "--extension-schema",
            type=Path,
            help=(
                "Exact extension-schema JSON path when declared by the Project; "
                "must be omitted otherwise."
            ),
        )
        _output_argument(command)

    register = project_actions.add_parser(
        "register",
        help=(
            "Register one resolver-verified first revision; automatic trusted "
            "enrollment is the default."
        ),
    )
    _revision_input_arguments(register)
    _reason_argument(register, action="registration")
    register.add_argument(
        "--paused",
        action="store_true",
        help="Register paused; otherwise lifecycle starts active.",
    )
    _output_argument(register)

    for action in ("list",):
        command = project_actions.add_parser(
            action, help="List every registered Project, including imported state."
        )
        command.add_argument(
            "--after-id",
            type=int,
            default=0,
            help="Return Projects with global database ID greater than this (default: 0).",
        )
        command.add_argument(
            "--limit",
            type=int,
            default=500,
            help="Maximum rows from 1 through 10000 (default: 500).",
        )
        _output_argument(command)

    show = project_actions.add_parser(
        "show", help="Show one Project and its current revision/health evidence."
    )
    _project_selector(show)
    _output_argument(show)

    doctor = project_actions.add_parser(
        "doctor",
        help="Recheck current typed paths and exact pinned Git blob identity.",
    )
    _project_selector(doctor)
    _output_argument(doctor)

    for action, target in (
        ("pause", "paused"),
        ("resume", "active"),
        ("archive", "archived"),
    ):
        command = project_actions.add_parser(
            action,
            help=(
                f"Transition one Project to {target}; archive is permanent and "
                "requires prior pause plus complete queue/worktree cleanup."
            ),
        )
        _project_selector(command)
        _reason_argument(command, action=f"Project {action}")
        _actor_argument(command)
        _output_argument(command)

    repair = project_actions.add_parser(
        "repair",
        help=(
            "Close one Project-local health circuit after its underlying "
            "problem has been repaired; lifecycle and host dispatch are unchanged."
        ),
    )
    _project_selector(repair)
    _reason_argument(repair, action="Project health-circuit repair")
    _actor_argument(repair)
    _output_argument(repair)

    append = project_actions.add_parser(
        "append-revision",
        help=(
            "Append a resolver-verified ProjectRevision, deriving trusted enrollment "
            "when omitted, and normally activate it."
        ),
    )
    _project_selector(append)
    _revision_input_arguments(append)
    append.add_argument(
        "--no-activate",
        action="store_true",
        help=(
            "Append without activation. Refused for the first typed revision after "
            "legacy import; later activation must move sequence forward."
        ),
    )
    _output_argument(append)

    activate = project_actions.add_parser(
        "activate-revision",
        help="Activate an already appended newer typed revision by positive ID.",
    )
    _project_selector(activate)
    activate.add_argument(
        "revision_id",
        type=int,
        help="Positive immutable ProjectRevision database ID owned by this Project.",
    )
    _actor_argument(activate)
    _output_argument(activate)

    card = commands.add_parser(
        "card", help="Validate or explain local ExperimentCard/v1 authoring input."
    )
    card_actions = card.add_subparsers(
        dest="card_action", required=True, title="card commands"
    )
    card_new = card_actions.add_parser(
        "new", help="Create a minimal card validated against one Project manifest."
    )
    card_new.add_argument(
        "--project-manifest",
        required=True,
        type=Path,
        help="Path to the exact local Project/v1 YAML used to validate the scaffold.",
    )
    card_new.add_argument(
        "--experiment-id",
        required=True,
        help="Stable experiment identifier to place in card metadata.",
    )
    card_new.add_argument(
        "--title", required=True, help="Human-readable non-empty experiment title."
    )
    card_new.add_argument(
        "--job-id",
        default="run",
        help="Explicit job identifier in the generated card (default: run).",
    )
    card_new.add_argument(
        "--environment",
        help="Declared Project environment; default is the first declared environment.",
    )
    card_new.add_argument(
        "--artifact-root",
        help="Declared writable volume; default is the first writable volume, if any.",
    )
    card_new.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Card path to create; its parent must exist and relative paths use cwd.",
    )
    card_new.add_argument(
        "--force",
        action="store_true",
        help="Atomically replace exactly --output; no directory is removed.",
    )
    _output_argument(card_new)
    for action in ("validate", "explain"):
        command = card_actions.add_parser(
            action, help=f"{action.title()} a card against its owning Project."
        )
        command.add_argument(
            "--project-manifest",
            required=True,
            type=Path,
            help="Path to exact owning Project/v1 YAML bytes; never modified.",
        )
        command.add_argument(
            "--card",
            required=True,
            type=Path,
            help="Path to exact ExperimentCard/v1 YAML bytes; never modified.",
        )
        command.add_argument(
            "--extension-schema",
            type=Path,
            help="Exact declared extension-schema JSON path; omit when undeclared.",
        )
        _output_argument(command)

    schema = commands.add_parser(
        "schema", help="Export authenticated bundled schemas for editors or CI."
    )
    schema_actions = schema.add_subparsers(
        dest="schema_action", required=True, title="schema commands"
    )
    schema_export = schema_actions.add_parser(
        "export", help="Write one bundled Project/v1 or ExperimentCard/v1 JSON Schema."
    )
    schema_export.add_argument(
        "kind",
        choices=("project", "card"),
        help="Schema kind to export: project or card.",
    )
    schema_export.add_argument(
        "--output",
        required=True,
        type=Path,
        help="JSON Schema path to create; its parent must already exist.",
    )
    schema_export.add_argument(
        "--force",
        action="store_true",
        help="Atomically replace exactly --output; no directory is removed.",
    )
    _output_argument(schema_export)

    submit = commands.add_parser(
        "submit",
        help="Compile Git-authenticated admission and add it, or print a dry-run.",
    )
    _submission_arguments(submit)
    submit.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Resolve complete revision/Git/blob/path/environment/resource/"
            "preemption evidence without allocating a global queue-item ID."
        ),
    )
    _output_argument(submit)

    status = commands.add_parser(
        "status", help="Show project-filtered queue status with global item IDs."
    )
    _project_selector(status)
    status.add_argument(
        "--state",
        action="append",
        default=[],
        help="Exact queue state filter; repeat for more states (default: all).",
    )
    status.add_argument(
        "--after-id",
        type=int,
        default=0,
        help="Return global queue-item IDs greater than this (default: 0).",
    )
    status.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum item rows from 1 through 10000 (default: 500).",
    )
    _output_argument(status)

    events = commands.add_parser(
        "events", help="List project-scoped append-only events with global IDs."
    )
    _project_selector(events)
    events.add_argument(
        "--after-id",
        type=int,
        default=0,
        help="Return event IDs greater than this (default: 0).",
    )
    events.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum event rows from 1 through 10000 (default: 500).",
    )
    _output_argument(events)

    item = commands.add_parser(
        "item", help="Show or mutate one project-authorized global queue item."
    )
    item_actions = item.add_subparsers(dest="item_action", required=True)
    for action in ("show", "release"):
        command = item_actions.add_parser(
            action, help=f"{action.title()} one exact global queue-item ID."
        )
        command.add_argument(
            "item_id", type=int, help="Positive global queue-item database ID."
        )
        _project_selector(command)
        if action == "release":
            _actor_argument(command)
        _output_argument(command)
    for action in ("hold", "remove"):
        command = item_actions.add_parser(
            action,
            help=(
                f"{action.title()} one pending global item while preserving all "
                "history and scientific artifacts."
            ),
        )
        command.add_argument(
            "item_id", type=int, help="Positive global queue-item database ID."
        )
        _project_selector(command)
        _reason_argument(command, action=f"item {action}")
        _actor_argument(command)
        _output_argument(command)
    priority = item_actions.add_parser(
        "priority",
        help="Change global priority without authorizing automatic preemption.",
    )
    priority.add_argument(
        "item_id", type=int, help="Positive global queue-item database ID."
    )
    priority.add_argument(
        "value", type=int, help="New signed 64-bit global scheduling priority."
    )
    _project_selector(priority)
    _actor_argument(priority)
    _output_argument(priority)
    preempt = item_actions.add_parser(
        "preempt",
        help="Explicitly request admitted cooperative checkpoint-and-requeue.",
    )
    preempt.add_argument(
        "item_id", type=int, help="Positive active typed global queue-item ID."
    )
    _project_selector(preempt)
    preempt.add_argument(
        "--note",
        required=True,
        help=(
            "Non-empty checkpoint request note persisted before request publication "
            "and process signaling."
        ),
    )
    _actor_argument(preempt)
    _output_argument(preempt)

    terminate = item_actions.add_parser(
        "terminate",
        help=(
            "Persist graceful termination for one active item, signal its "
            "authenticated process group, and let the scheduler escalate if needed."
        ),
    )
    terminate.add_argument(
        "item_id", type=int, help="Positive active global queue-item database ID."
    )
    _project_selector(terminate)
    _reason_argument(terminate, action="item termination")
    _actor_argument(terminate)
    _output_argument(terminate)

    force_kill = item_actions.add_parser(
        "force-kill",
        help=(
            "Persist an immediate force-kill for one active item and signal its "
            "authenticated process group with SIGKILL."
        ),
    )
    force_kill.add_argument(
        "item_id", type=int, help="Positive active global queue-item database ID."
    )
    _project_selector(force_kill)
    _reason_argument(force_kill, action="item force-kill")
    _actor_argument(force_kill)
    force_kill.add_argument(
        "--confirm",
        required=True,
        choices=("FORCE-KILL",),
        help=(
            "Required exact acknowledgement FORCE-KILL; this bypasses graceful "
            "checkpoint/cleanup in the child process."
        ),
    )
    _output_argument(force_kill)

    abandoned = item_actions.add_parser(
        "resolve-abandoned-launch",
        help=(
            "Fail an exact pre-launch or recorded-dead active attempt only after "
            "the operator proves no queue process group or GPU workload exists."
        ),
        description=(
            "Guarded recovery for a scheduler crash before durable launch identity "
            "or after an authenticated executor died without an exit receipt. Host "
            "dispatch must already be paused; the item must retain the exact "
            "confirmed GPU assignment. A null process identity is accepted only "
            "for state starting. A recorded identity is accepted only when its PID "
            "does not authenticate and its exact process group is absent. Live or "
            "extant database- or sidecar-named process groups are refused. The item "
            "is failed, its Project remains quarantined, the host remains paused, "
            "scientific output is preserved, and only authenticated queue-owned "
            "worktree cleanup is attempted."
        ),
    )
    abandoned.add_argument(
        "item_id",
        type=int,
        help="Positive global queue-item database ID in an ambiguous active state.",
    )
    _project_selector(abandoned)
    abandoned.add_argument(
        "--gpu-uuid",
        required=True,
        help=(
            "Exact GPU UUID currently assigned in the item row; verify externally "
            "that this GPU has no queue workload before confirming."
        ),
    )
    _reason_argument(abandoned, action="abandoned-launch resolution")
    _actor_argument(abandoned)
    abandoned.add_argument(
        "--confirm",
        required=True,
        choices=("RESOLVE-ABANDONED-LAUNCH",),
        help=(
            "Required exact acknowledgement RESOLVE-ABANDONED-LAUNCH that no "
            "executor, named process group, or workload remains on the assigned GPU."
        ),
    )
    _output_argument(abandoned)

    artifacts = commands.add_parser(
        "artifact", help="List immutable project-authorized artifact observations."
    )
    _project_selector(artifacts)
    artifacts.add_argument(
        "--item-id",
        type=int,
        help="Optional positive global item ID; it must belong to the selected Project.",
    )
    artifacts.add_argument(
        "--after-id", type=int, default=0, help="Return artifact IDs greater than this."
    )
    artifacts.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum artifact rows from 1 through 10000 (default: 500).",
    )
    _output_argument(artifacts)

    receipt = commands.add_parser(
        "receipt",
        help="Export complete authenticated persisted evidence for one Project.",
        description=(
            "Emit one strict QueueExport/v1 document containing authenticated "
            "Project, revision, item, event, artifact, and exact typed "
            "cooperative-yield evidence. Database/v5 does not retain exact "
            "ExecutorReceipt source bytes, so the export records that absence "
            "and never reconstructs an ExecutorReceipt document."
        ),
    )
    _project_selector(receipt)
    receipt.add_argument(
        "--actor",
        default="cli:receipt",
        help=(
            "Log-safe export principal recorded in QueueExport/v1 "
            "(default: cli:receipt); pass an operator identity for durable audit use."
        ),
    )
    receipt.add_argument(
        "--json",
        action="store_true",
        help="Emit the exact RFC 8785 canonical QueueExport/v1 JSON document.",
    )

    host = commands.add_parser("host", help="Control the host-global dispatch gate.")
    host_actions = host.add_subparsers(dest="host_action", required=True)
    pause_host = host_actions.add_parser(
        "pause", help="Pause new dispatch globally without changing running work."
    )
    _reason_argument(pause_host, action="host pause")
    _actor_argument(pause_host)
    _output_argument(pause_host)
    resume_host = host_actions.add_parser(
        "resume", help="Resume host dispatch without changing Project gates."
    )
    _actor_argument(resume_host)
    _output_argument(resume_host)

    gpu = commands.add_parser("gpu", help="Inspect or mutate the host GPU allowlist.")
    gpu_actions = gpu.add_subparsers(dest="gpu_action", required=True)
    gpu_show = gpu_actions.add_parser(
        "show", help="Show persisted enable/drain state and active global item IDs."
    )
    _output_argument(gpu_show)
    gpu_add = gpu_actions.add_parser(
        "add", help="Resolve one observed GPU and add its full UUID to the allowlist."
    )
    gpu_add.add_argument(
        "identifier",
        help="Observed GPU index, full UUID, or unambiguous UUID prefix.",
    )
    gpu_add.add_argument(
        "--nvidia-smi",
        default="nvidia-smi",
        help="nvidia-smi executable used for read-only identity resolution (default: nvidia-smi).",
    )
    _actor_argument(gpu_add)
    _output_argument(gpu_add)
    for action in ("enable", "disable", "drain", "undrain"):
        command = gpu_actions.add_parser(
            action,
            help=(
                f"{action.title()} one exact stored GPU UUID; running work is "
                "never interrupted by allowlist changes."
            ),
        )
        command.add_argument(
            "uuid", help="Exact full UUID already present in the persisted allowlist."
        )
        _actor_argument(command)
        _output_argument(command)

    reservation = commands.add_parser(
        "reservation", help="Inspect or mutate passive host-global GPU reservations."
    )
    reservation_actions = reservation.add_subparsers(
        dest="reservation_action", required=True, title="reservation commands"
    )
    reservation_list = reservation_actions.add_parser(
        "list", help="List reservation history without changing queue or GPU state."
    )
    reservation_list.add_argument(
        "--gpu-uuid", help="Optional exact GPU UUID filter (default: every GPU)."
    )
    reservation_list.add_argument(
        "--open-only",
        action="store_true",
        help="Show only pending or active reservations (default: complete history).",
    )
    _output_argument(reservation_list)
    reservation_request = reservation_actions.add_parser(
        "request",
        help="Reserve an idle GPU now or passively reserve a busy GPU when it frees.",
    )
    reservation_request.add_argument(
        "gpu_uuid", help="Exact full UUID of an enabled, undrained allowlisted GPU."
    )
    reservation_request.add_argument(
        "--duration-hours",
        required=True,
        type=int,
        help="Whole reservation duration from 1 through 24 hours; starts when active.",
    )
    reservation_request.add_argument(
        "--note",
        required=True,
        help="Required short identity/reason recorded with the reservation.",
    )
    _actor_argument(reservation_request)
    _output_argument(reservation_request)
    reservation_release = reservation_actions.add_parser(
        "release", help="Release one pending or active reservation by positive ID."
    )
    reservation_release.add_argument(
        "reservation_id", type=int, help="Positive global reservation ID."
    )
    _actor_argument(reservation_release)
    _output_argument(reservation_release)

    serve = commands.add_parser(
        "serve", help="Run the foreground project-aware GPU scheduler service."
    )
    serve.add_argument(
        "--poll-seconds",
        type=float,
        default=60.0,
        help="GPU telemetry poll interval in seconds (default: 60; must be positive).",
    )
    serve.add_argument(
        "--control-seconds",
        type=float,
        default=1.0,
        help="Queue/recovery control interval in seconds (default: 1; must be positive).",
    )
    serve.add_argument(
        "--min-free-memory-fraction",
        type=float,
        default=0.95,
        help="Minimum free GPU-memory fraction for dispatch (default: 0.95).",
    )
    serve.add_argument(
        "--max-utilization-percent",
        type=float,
        default=5.0,
        help="Maximum observed GPU utilization for dispatch (default: 5).",
    )
    serve.add_argument(
        "--min-free-disk-gib",
        type=float,
        default=50.0,
        help="Minimum free central/artifact filesystem GiB (default: 50).",
    )
    serve.add_argument(
        "--nvidia-smi",
        default="nvidia-smi",
        help="nvidia-smi executable used for live telemetry (default: nvidia-smi).",
    )
    serve.add_argument(
        "--once",
        action="store_true",
        help=(
            "Run one reconciliation/recovery-only iteration without dispatching "
            "new work, then exit."
        ),
    )
    _output_argument(serve)

    migrate = commands.add_parser(
        "migrate",
        help="Dispatch the explicit offline copy-only legacy-to-v5 importer.",
    )
    migrate.add_argument(
        "--source-state",
        required=True,
        type=Path,
        help="Absolute complete offline legacy state copy containing queue.sqlite3.",
    )
    migrate.add_argument(
        "--destination-state",
        required=True,
        type=Path,
        help="Absolute absent destination directory; never merged or overwritten.",
    )
    migrate.add_argument(
        "--project-key",
        required=True,
        help="New stable Project key owning every imported legacy row.",
    )
    migrate.add_argument(
        "--legacy-checkout",
        required=True,
        type=Path,
        help="Absolute Git checkout matching legacy metadata and recorded commits.",
    )
    _actor_argument(migrate)
    migrate.add_argument(
        "--receipt",
        required=True,
        type=Path,
        help="Fresh absolute external QueueMigrationReceipt/v1 path outside source/destination.",
    )
    migrate.add_argument(
        "--legacy-root",
        action="append",
        type=Path,
        default=[],
        help="Absolute protected legacy root destination must not overlap; repeat as needed.",
    )
    migrate.add_argument(
        "--operation-id",
        help="Optional stable receipt operation ID; default derives from source identity.",
    )
    migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and verify a temporary candidate, write receipt, and publish no destination.",
    )
    migrate.add_argument(
        "--confirm-source-is-copy",
        action="store_true",
        help="Required attestation that --source-state is an offline copy, never live state.",
    )
    _output_argument(migrate)
    return parser


def _summary_document(summary: V5ProjectSummary) -> dict[str, JSONValue]:
    return {
        "id": summary.id,
        "key": summary.key,
        "displayName": summary.display_name,
        "lifecycle": summary.lifecycle.value,
        "lifecycleReason": summary.lifecycle_reason,
        "lifecycleActor": summary.lifecycle_actor,
        "lifecycleChangedAt": summary.lifecycle_changed_at,
        "health": summary.health.value,
        "circuitFailureCount": summary.circuit_failure_count,
        "healthReason": summary.health_reason,
        "healthActor": summary.health_actor,
        "healthChangedAt": summary.health_changed_at,
        "currentRevision": {
            "id": summary.current_revision_id,
            "sequence": summary.current_revision_sequence,
            "label": summary.current_revision_label,
            "kind": summary.current_revision_kind,
            "gitCommit": summary.current_git_commit,
        },
        "hostDispatchPaused": summary.host_dispatch_paused,
        "hostPauseReason": summary.host_pause_reason,
        "dispatchAllowed": summary.dispatch_allowed,
        "queueCounts": {state: count for state, count in summary.queue_counts},
    }


def _item_document(view: V5OperatorItemView) -> dict[str, JSONValue]:
    item = view.item
    snapshot_document: JSONValue = None
    if item.snapshot is not None:
        snapshot = item.snapshot
        policy_source = canonical_json_bytes(snapshot.submission_policy.to_document())
        command_source = canonical_json_bytes(snapshot.command.to_document())
        dependencies_source = canonical_json_bytes(list(snapshot.submission_policy.dependencies))
        snapshot_document = {
            "id": item.snapshot_id,
            "projectRevision": snapshot.project_revision,
            "gitCommit": snapshot.git_commit,
            "packageVersion": snapshot.package_version,
            "projectSourceName": snapshot.project_source_name,
            "projectSource": binary_evidence_document(source=snapshot.project_source, source_sha256=snapshot.project_source_sha256),
            "projectNormalized": json_evidence_document(source=snapshot.project_normalized_json, source_sha256=snapshot.project_normalized_sha256, document=snapshot.project_document),
            "projectSchema": {"apiVersion": snapshot.project_schema.api_version, "kind": snapshot.project_schema.kind, "schemaId": snapshot.project_schema.schema_id, "sha256": snapshot.project_schema.sha256},
            "cardSourceName": snapshot.card_source_name,
            "cardSource": binary_evidence_document(source=snapshot.card_source, source_sha256=snapshot.card_source_sha256),
            "cardNormalized": json_evidence_document(source=snapshot.card_normalized_json, source_sha256=snapshot.card_normalized_sha256, document=snapshot.card_document),
            "cardSchema": {"apiVersion": snapshot.card_schema.api_version, "kind": snapshot.card_schema.kind, "schemaId": snapshot.card_schema.schema_id, "sha256": snapshot.card_schema.sha256},
            "extensionSchema": (
                None if snapshot.extension_schema is None else {
                    "sourceName": snapshot.extension_schema.source_name,
                    "referencePath": snapshot.extension_schema.reference_path,
                    "source": binary_evidence_document(source=snapshot.extension_schema.source, source_sha256=snapshot.extension_schema.source_sha256),
                    "canonical": json_evidence_document(source=snapshot.extension_schema.canonical_json, source_sha256=snapshot.extension_schema.canonical_sha256, document=cast(JSONValue, json.loads(snapshot.extension_schema.canonical_json))),
                    "schemaId": snapshot.extension_schema.schema_id,
                }
            ),
            "resolved": json_evidence_document(source=snapshot.resolved_json, source_sha256=snapshot.resolved_sha256, document=snapshot.resolved_document),
            "command": json_evidence_document(source=command_source, source_sha256=sha256_bytes(command_source), document=snapshot.command.to_document()),
            "submissionPolicy": json_evidence_document(source=policy_source, source_sha256=sha256_bytes(policy_source), document=snapshot.submission_policy.to_document()),
            "policyBindings": json_evidence_document(source=snapshot.submission_policy.bindings_json, source_sha256=sha256_bytes(snapshot.submission_policy.bindings_json), document=snapshot.submission_policy.bindings),
            "policyDependencies": json_evidence_document(source=dependencies_source, source_sha256=sha256_bytes(dependencies_source), document=list(snapshot.submission_policy.dependencies)),
        }
    runtime_key = {
        "assigned_gpu_uuid": "assignedGpuUuid", "assigned_gpu_index": "assignedGpuIndex",
        "runtime_gpu_lease_held": "runtimeGpuLeaseHeld",
        "runtime_gpu_lease_released_at": "runtimeGpuLeaseReleasedAt",
        "pid": "pid", "pgid": "pgid", "proc_start_ticks": "processStartTicks",
        "started_at": "startedAt", "finished_at": "finishedAt", "return_code": "returnCode",
        "terminate_requested_at": "terminateRequestedAt", "terminate_reason": "terminateReason",
        "termination_stage": "terminationStage", "termination_signal_epoch": "terminationSignalEpoch",
        "contention_detected": "contentionDetected", "repo_drift_detected": "repoDriftDetected",
        "runner_run_dir": "runnerRunDirectory", "runner_manifest_path": "runnerManifestPath",
        "rsync_pull_command": "rsyncPullCommand", "yield_requested_at": "yieldRequestedAt",
        "yield_requested_by": "yieldRequestedBy", "yield_request_id": "yieldRequestId",
        "yield_note": "yieldNote", "yield_duration_hours": "yieldDurationHours",
        "continuation_checkpoint": "continuationCheckpoint", "continuation_checkpoint_sha256": "continuationCheckpointSha256",
        "continuation_checkpoint_metadata": "continuationCheckpointMetadata", "continuation_checkpoint_metadata_sha256": "continuationCheckpointMetadataSha256",
        "continuation_step": "continuationStep", "continuation_wandb_id": "continuationWandbId",
        "git_ref": "historicalGitRef", "worktree_path": "historicalWorktreePath",
        "worktree_created_at": "historicalWorktreeCreatedAt", "worktree_removed_at": "historicalWorktreeRemovedAt",
        "worktree_cleanup_error": "historicalWorktreeCleanupError", "runtime_git_ref": "runtimeGitRef",
        "runtime_worktree_path": "runtimeWorktreePath", "runtime_worktree_created_at": "runtimeWorktreeCreatedAt",
        "runtime_worktree_removed_at": "runtimeWorktreeRemovedAt", "runtime_worktree_cleanup_error": "runtimeWorktreeCleanupError",
    }
    runtime = {runtime_key[name]: value for name, value in view.persisted_runtime.items()}
    return {
        "id": item.id,
        "projectId": item.project_id,
        "projectKey": view.project_key,
        "revisionId": item.revision_id,
        "revisionLabel": view.revision_label,
        "admissionKind": item.admission_kind,
        "snapshotId": item.snapshot_id,
        "jobId": item.job_id,
        "experimentId": item.experiment_id,
        "attempt": item.attempt,
        "segment": item.segment,
        "state": item.state,
        "stateDetail": item.state_detail,
        "priority": item.priority,
        "resumeFront": item.resume_front,
        "preemptible": item.preemptible,
        "cardPath": item.card_path,
        "cardSha256": item.card_sha256,
        "gitCommit": item.git_commit,
        "addedAt": item.added_at,
        "addedBy": item.added_by,
        "commandText": item.command_text,
        "runnerName": item.runner_name,
        "admissionSnapshot": snapshot_document,
        "dependencies": [{
            "itemId": target.item_id, "projectId": target.project_id,
            "projectKey": target.project_key, "revisionId": target.revision_id,
            "revisionLabel": target.revision_label, "state": target.state,
            "external": target.project_id != item.project_id,
        } for target in view.dependency_targets],
        "runtime": runtime,
    }


def _event_document(event: V5Event) -> dict[str, JSONValue]:
    return {
        "id": event.id,
        "createdAt": event.created_at,
        "actor": event.actor,
        "eventType": event.event_type,
        "scope": event.scope,
        "projectId": event.project_id,
        "queueItemId": event.queue_item_id,
        "payload": event.payload,
    }


def _artifact_document(artifact: V5ArtifactRecord) -> dict[str, JSONValue]:
    return {
        "id": artifact.id,
        "queueItemId": artifact.queue_item_id,
        "projectId": artifact.project_id,
        "revisionId": artifact.revision_id,
        "segment": artifact.segment,
        "evidenceKind": artifact.evidence_kind,
        "name": artifact.artifact_name,
        "type": artifact.artifact_type,
        "root": artifact.root_name,
        "relativePath": artifact.relative_path,
        "absolutePath": str(artifact.absolute_path),
        "sizeBytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "recordedAt": artifact.recorded_at,
        "metadata": artifact.metadata,
    }


def _gpu_document(gpu: V5GpuAllowlistEntry) -> dict[str, JSONValue]:
    return {
        "uuid": gpu.uuid,
        "requestedIdentifier": gpu.requested_identifier,
        "lastIndex": gpu.last_index,
        "name": gpu.name,
        "enabled": gpu.enabled,
        "draining": gpu.draining,
        "updatedAt": gpu.updated_at,
        "assignedQueueItemIds": list(gpu.assigned_queue_item_ids),
    }


def _reservation_document(reservation: V5GpuReservation) -> dict[str, JSONValue]:
    return {
        "id": reservation.id,
        "gpuUuid": reservation.gpu_uuid,
        "queueItemIdAtRequest": reservation.queue_item_id,
        "status": reservation.status.value,
        "requestedAt": reservation.requested_at,
        "requestedBy": reservation.requested_by,
        "note": reservation.note,
        "durationHours": reservation.duration_hours,
        "startsAt": reservation.starts_at,
        "expiresAt": reservation.expires_at,
        "releasedAt": reservation.released_at,
        "releasedBy": reservation.released_by,
        "stateDetail": reservation.state_detail,
    }


def _revision_document(revision: V5RevisionSummary) -> dict[str, JSONValue]:
    document: dict[str, JSONValue] = {
        "id": revision.id,
        "projectId": revision.project_id,
        "sequence": revision.sequence,
        "label": revision.label,
        "kind": revision.kind,
        "displayName": revision.display_name,
        "gitCommit": revision.git_commit,
        "checkoutPath": str(revision.checkout_path),
        "enrollmentSha256": revision.enrollment_sha256,
        "enrollmentEvidence": json_evidence_document(
            source=revision.enrollment_source,
            source_sha256=revision.enrollment_sha256,
            document=cast(JSONValue, json.loads(revision.enrollment_source)),
        ),
        "createdAt": revision.created_at,
        "createdActor": revision.created_actor,
    }
    if revision.typed_revision is not None:
        typed = revision.typed_revision
        document["typedEvidence"] = {
            "identity": typed.to_document(),
            "projectSource": binary_evidence_document(source=typed.project_source, source_sha256=typed.project_source_sha256),
            "projectNormalized": json_evidence_document(source=typed.project_normalized_json, source_sha256=typed.project_normalized_sha256, document=typed.project.to_document()),
            "extensionSource": None if typed.extension_schema_source is None else binary_evidence_document(source=typed.extension_schema_source, source_sha256=cast(str, typed.extension_schema_source_sha256)),
            "extensionCanonical": None if typed.extension_schema_canonical_json is None else json_evidence_document(source=typed.extension_schema_canonical_json, source_sha256=cast(str, typed.extension_schema_canonical_sha256), document=cast(JSONValue, json.loads(typed.extension_schema_canonical_json))),
        }
    if revision.git_evidence is not None:
        git = revision.git_evidence
        document["gitEvidence"] = {
            "repositoryRoot": git.repository_root,
            "gitCommit": git.git_commit,
            "projectBlob": {
                "path": git.project_blob.path,
                "objectId": git.project_blob.object_id,
                "mode": git.project_blob.mode,
                "size": git.project_blob.size,
                "sourceSha256": git.project_blob.source_sha256,
            },
            "extensionSchemaBlob": (
                None
                if git.extension_schema_blob is None
                else {
                    "path": git.extension_schema_blob.path,
                    "objectId": git.extension_schema_blob.object_id,
                    "mode": git.extension_schema_blob.mode,
                    "size": git.extension_schema_blob.size,
                    "sourceSha256": git.extension_schema_blob.source_sha256,
                }
            ),
        }
    return document


def _canonical_checkout(path: Path) -> Path:
    if not path.is_absolute():
        raise OperatorServiceError(
            f"checkout must be an absolute path, got {str(path)!r}"
        )
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise OperatorServiceError(f"could not resolve checkout {path}: {exc}") from exc
    if resolved != path or not resolved.is_dir():
        raise OperatorServiceError(
            f"checkout must be its canonical existing directory path {resolved}, "
            f"got {path}"
        )
    return resolved


def _portable_path(value: str, *, field_name: str) -> str:
    if type(value) is not str or not value or "\\" in value:
        raise OperatorServiceError(
            f"{field_name} must be a non-empty POSIX repository-relative path"
        )
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise OperatorServiceError(
            f"{field_name} must be normalized and repository-relative, got {value!r}"
        )
    return path.as_posix()


def _checkout_source(checkout: Path, relative: str, *, purpose: str) -> bytes:
    name = _portable_path(relative, field_name=purpose)
    target = checkout.joinpath(*PurePosixPath(name).parts)
    try:
        resolved = target.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise OperatorServiceError(
            f"could not resolve {purpose} {name!r} beneath checkout {checkout}: {exc}"
        ) from exc
    if checkout not in resolved.parents or not resolved.is_file():
        raise OperatorServiceError(
            f"{purpose} {name!r} must resolve to a regular file beneath checkout "
            f"{checkout}, got {resolved}"
        )
    return _read_bytes(resolved, purpose=purpose)


def _automatic_environment_directory(
    *,
    checkout: Path,
    requested: Path | None,
) -> Path:
    """Normalize the trusted-project venv root, bin directory, or executable."""

    candidate = Path(".venv/bin") if requested is None else requested
    if not candidate.is_absolute():
        candidate = checkout / candidate
    # Inspect the spelling before resolving: venv Python executables are often
    # symlinks to a uv/CPython installation outside the checkout, while their
    # parent bin directory is the PATH entry the scientific child needs.
    if candidate.is_file():
        if not os.access(candidate, os.X_OK):
            raise OperatorServiceError(
                f"automatic project environment executable {candidate} is not "
                "executable; pass a venv root, its bin directory, or an "
                "executable Python path"
            )
        candidate = candidate.parent
    elif candidate.is_dir() and (candidate / "pyvenv.cfg").is_file():
        candidate = candidate / "bin"
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise OperatorServiceError(
            f"automatic project environment {candidate} cannot be resolved: {exc}; "
            "create the project .venv or pass --environment-bin"
        ) from exc
    if not resolved.is_dir():
        raise OperatorServiceError(
            f"automatic project environment {candidate} resolves to {resolved}, "
            "which is not an executable-search directory; pass a venv root, its "
            "bin directory, or a Python executable"
        )
    return resolved


def _automatic_environment_ignore_root(
    *,
    checkout: Path,
    environment_directory: Path,
    requested: Path | None,
) -> Path | None:
    """Return the checkout-local mutable root that must be Git-ignored."""

    if checkout not in environment_directory.parents:
        return None
    venv_root = environment_directory.parent
    default_venv = (
        requested is None
        and environment_directory == (checkout / ".venv" / "bin").resolve()
    )
    if environment_directory.name == "bin" and (
        default_venv or (venv_root / "pyvenv.cfg").is_file()
    ):
        # Prove the whole environment mutable, including pyvenv.cfg and
        # site-packages, while retaining only its bin directory on PATH.
        return venv_root
    return environment_directory


def _services(
    args: argparse.Namespace,
    *,
    initialize: bool = False,
) -> tuple[V5QueueStore, V5ProjectRepository, V5OperatorRepository]:
    state = resolve_state_dir(args.state_dir)
    store = V5QueueStore(state)
    if initialize:
        store.initialize()
    else:
        # Open-and-close validates exact-v5 identity without creating a state
        # directory, database, WAL, or migration side effect.
        connection = store.connect()
        connection.close()
    projects = V5ProjectRepository(store)
    return store, projects, V5OperatorRepository(store)


def _select_project(
    operator: V5OperatorRepository,
    selector: str | None,
) -> V5ProjectSummary:
    if selector is None:
        return operator.infer_project_from_cwd(Path.cwd().resolve(strict=True))
    if selector.isdecimal():
        return operator.get_project_summary(project_id=int(selector))
    return operator.get_project_summary(project_key=selector)


def _build_revision(
    args: argparse.Namespace,
    *,
    store: V5QueueStore,
    operator: V5OperatorRepository,
    project_id: int,
    revision_id: int,
    sequence: int,
    exclude_project_id: int | None,
) -> ProjectRevision:
    checkout = _canonical_checkout(args.checkout)
    manifest_path = _portable_path(args.manifest, field_name="--manifest")
    project_source = _checkout_source(
        checkout, manifest_path, purpose="Project manifest"
    )
    project = Project.from_yaml(project_source, source_name=manifest_path)
    extension_source = (
        None
        if project.extension_schema is None
        else _checkout_source(
            checkout,
            project.extension_schema.path,
            purpose="Project extension schema",
        )
    )
    validate_project_source(
        source=project_source,
        source_name=manifest_path,
        extension_schema_source=extension_source,
    )
    if args.enrollment is None:
        declared_volumes = sorted(volume.name for volume in project.volumes)
        if declared_volumes:
            raise OperatorServiceError(
                "automatic trusted-project enrollment requires Project "
                f"volumes: [], but found {declared_volumes}; remove those "
                "declarations when jobs use ordinary host paths, or pass "
                "--enrollment for explicit mounts"
            )
        if len(project.environments) != 1:
            raise OperatorServiceError(
                "automatic trusted-project enrollment requires exactly one declared "
                f"environment, got {[value.name for value in project.environments]}; "
                "declare one environment or pass --enrollment"
            )
        environment_path = _automatic_environment_directory(
            checkout=checkout,
            requested=args.environment_bin,
        )
        ignored_descendants: tuple[Path, ...] = ()
        ignored_root = _automatic_environment_ignore_root(
            checkout=checkout,
            environment_directory=environment_path,
            requested=args.environment_bin,
        )
        if ignored_root is not None:
            ignored_descendants = verify_git_ignored_checkout_descendants(
                repository_root=checkout,
                git_commit=args.git_commit,
                descendants=(ignored_root,),
            )
        environment = project.environments[0]
        enrollment = Enrollment.create(
            project=project,
            checkout_directory=checkout,
            project_manifest_path=manifest_path,
            mounts=(),
            environments=(
                EnvironmentBinding.create(
                    name=environment.name,
                    executable_search_directories=(environment_path,),
                    inherit_variables=project.environment_policy.allow_variables,
                ),
            ),
            state_directory=store.state_dir,
            git_ignored_checkout_descendants=ignored_descendants,
            occupied_roots=operator.occupied_roots(
                exclude_project_id=exclude_project_id
            ),
        )
    else:
        enrollment = load_enrollment_document(
            source=_read_bytes(args.enrollment, purpose="Enrollment"),
            source_name=str(args.enrollment),
            project=project,
            state_directory=store.state_dir,
            occupied_roots=operator.occupied_roots(
                exclude_project_id=exclude_project_id
            ),
            git_ignore_verifier=lambda descendants: (
                verify_git_ignored_checkout_descendants(
                    repository_root=checkout,
                    git_commit=args.git_commit,
                    descendants=descendants,
                )
            ),
        )
    if enrollment.checkout_directory != checkout:
        raise OperatorServiceError(
            f"Enrollment checkout {enrollment.checkout_directory} differs from "
            f"explicit checkout {checkout}; make them exact"
        )
    if enrollment.project_manifest_path != manifest_path:
        raise OperatorServiceError(
            f"Enrollment manifest path {enrollment.project_manifest_path!r} "
            f"differs from --manifest {manifest_path!r}"
        )
    return ProjectRevision.create(
        revision_id=revision_id,
        project_id=project_id,
        sequence=sequence,
        project=project,
        project_source_path=manifest_path,
        project_source=project_source,
        git_commit=args.git_commit,
        enrollment=enrollment,
        created_actor=args.actor,
        created_at=_now(),
        extension_schema_source=extension_source,
    )


def _submission(args: argparse.Namespace, revision: ProjectRevision) -> Submission:
    return Submission(
        project_key=revision.project_key,
        card_path=args.card_path,
        job_id=args.job_id,
        operator=args.operator,
        bindings=_bindings_object(args.bindings_json),
        priority=args.priority,
        hold_reason=args.hold_reason,
        dependencies=list(args.dependency),
        preemption_authorized=args.authorize_preemption,
    )


def _project_result(operation: str, summary: V5ProjectSummary) -> _Result:
    document: dict[str, JSONValue] = {
        "operation": operation,
        "outputContract": "diagnostic operator output; not a persistent protocol",
        "ok": True,
        "project": _summary_document(summary),
    }
    return _Result(
        document,
        (
            f"Project {summary.key} (id {summary.id}) is "
            f"{summary.lifecycle.value}; current {summary.current_revision_label} "
            f"[{summary.current_revision_kind}], health={summary.health.value}, "
            f"dispatch={'allowed' if summary.dispatch_allowed else 'blocked'}"
        ),
    )


def _dispatch(args: argparse.Namespace) -> _Result:
    if args.command == "project" and args.project_action == "init":
        source = project_manifest_scaffold(key=args.key, display_name=args.display_name)
        output = _write_scaffold(args.output, source, force=args.force)
        return _Result(
            {
                "operation": "project.init",
                "outputContract": "diagnostic metadata; Project/v1 is the protocol file",
                "ok": True,
                "path": str(output),
                "sourceSha256": sha256_bytes(source),
                "bytes": len(source),
            },
            f"created Project/v1 scaffold {output}",
        )
    if args.command == "project" and args.project_action in {"validate", "explain"}:
        source = _read_bytes(args.manifest, purpose="Project manifest")
        extension = _optional_source(
            args.extension_schema, purpose="Project extension schema"
        )
        document = validate_project_source(
            source=source,
            source_name=str(args.manifest),
            extension_schema_source=extension,
            explain=args.project_action == "explain",
        )
        return _Result(
            document,
            f"Project {document['projectKey']} is valid ({document['operation']})",
        )
    if args.command == "card":
        if args.card_action == "new":
            project_source = _read_bytes(
                args.project_manifest, purpose="Project manifest"
            )
            project = Project.from_yaml(
                project_source, source_name=str(args.project_manifest)
            )
            source = experiment_card_scaffold(
                project=project,
                experiment_id=args.experiment_id,
                title=args.title,
                job_id=args.job_id,
                environment=args.environment,
                artifact_root=args.artifact_root,
            )
            output = _write_scaffold(args.output, source, force=args.force)
            return _Result(
                {
                    "operation": "card.new",
                    "outputContract": (
                        "diagnostic metadata; ExperimentCard/v1 is the protocol file"
                    ),
                    "ok": True,
                    "path": str(output),
                    "projectKey": project.key,
                    "experimentId": args.experiment_id,
                    "sourceSha256": sha256_bytes(source),
                    "bytes": len(source),
                },
                f"created ExperimentCard/v1 scaffold {output}",
            )
        document = validate_card_source(
            project_source=_read_bytes(
                args.project_manifest, purpose="Project manifest"
            ),
            project_source_name=str(args.project_manifest),
            card_source=_read_bytes(args.card, purpose="ExperimentCard"),
            card_source_name=str(args.card),
            extension_schema_source=_optional_source(
                args.extension_schema, purpose="Project extension schema"
            ),
            explain=args.card_action == "explain",
        )
        return _Result(
            document,
            f"card {document['experimentId']} is valid ({document['operation']})",
        )

    if args.command == "schema":
        source = export_editor_schema(args.kind)
        output = _write_scaffold(args.output, source, force=args.force)
        return _Result(
            {
                "operation": "schema.export",
                "outputContract": "diagnostic metadata; output is authenticated JSON Schema",
                "ok": True,
                "schemaKind": args.kind,
                "path": str(output),
                "sourceSha256": sha256_bytes(source),
                "bytes": len(source),
            },
            f"exported authenticated {args.kind} schema to {output}",
        )

    if args.command == "migrate":
        outcome = migrate_legacy_state(
            source_state_copy=args.source_state,
            destination_state=args.destination_state,
            project_key=args.project_key,
            legacy_checkout=args.legacy_checkout,
            actor=args.actor,
            receipt_path=args.receipt,
            dry_run=args.dry_run,
            protected_roots=args.legacy_root,
            operation_id=args.operation_id,
            confirm_source_is_copy=args.confirm_source_is_copy,
        )
        action = "published" if outcome.published else "validated without publication"
        return _Result(
            {
                "operation": "migration.v5",
                "outputContract": (
                    "diagnostic wrapper; receipt is QueueMigrationReceipt/v1"
                ),
                "ok": True,
                "published": outcome.published,
                "destinationState": str(outcome.destination_state),
                "receiptPath": str(outcome.receipt_path),
                "receiptSha256": outcome.receipt.sha256,
                "receipt": outcome.receipt.to_document(),
            },
            (
                f"schema-v5 migration {action}: destination="
                f"{outcome.destination_state} receipt={outcome.receipt_path} "
                f"sha256={outcome.receipt.sha256}"
            ),
        )

    store, projects, operator = _services(
        args,
        initialize=(
            args.command == "project" and args.project_action == "register"
        ),
    )
    if args.command == "project":
        if args.project_action == "register":
            project_id, revision_id = operator.next_project_identity()
            revision = _build_revision(
                args,
                store=store,
                operator=operator,
                project_id=project_id,
                revision_id=revision_id,
                sequence=1,
                exclude_project_id=None,
            )
            resolved = verify_project_revision(revision)
            registered = RegisteredProject.register(
                revision=revision,
                initial_lifecycle=("paused" if args.paused else "active"),
                reason=args.reason,
                actor=args.actor,
                changed_at=revision.created_at,
            )
            runtime = ProjectRuntimeState.create(
                project_id=project_id,
                project_key=revision.project_key,
                reason="initial registration health circuit closed",
                actor=args.actor,
                changed_at=revision.created_at,
            )
            projects.register_project(registered, resolved, runtime)
            return _project_result(
                "project.register",
                operator.get_project_summary(project_id=project_id),
            )
        if args.project_action == "list":
            values = operator.list_project_summaries(
                after_id=args.after_id, limit=args.limit
            )
            document: dict[str, JSONValue] = {
                "operation": "project.list",
                "outputContract": "diagnostic operator output; not a persistent protocol",
                "ok": True,
                "projects": [_summary_document(value) for value in values],
            }
            lines = ["ID  KEY  LIFECYCLE  HEALTH  CURRENT  DISPATCH"]
            lines.extend(
                f"{value.id}  {value.key}  {value.lifecycle.value}  "
                f"{value.health.value}  {value.current_revision_label}  "
                f"{'yes' if value.dispatch_allowed else 'no'}"
                for value in values
            )
            return _Result(document, "\n".join(lines))
        summary = _select_project(operator, args.project)
        if args.project_action == "show":
            return _project_result("project.show", summary)
        if args.project_action == "doctor":
            if summary.typed_view is None:
                raise V5OperatorError(
                    f"Project {summary.key!r} current revision is legacy-v4; "
                    "append and activate a verified Project/v1 revision before doctor"
                )
            document = doctor_project_revision(
                revision=summary.typed_view.current_revision
            )
            return _Result(
                document,
                f"Project {summary.key} current revision passed path/Git doctor",
            )
        if args.project_action in {"pause", "resume", "archive"}:
            target = {
                "pause": ProjectLifecycle.PAUSED,
                "resume": ProjectLifecycle.ACTIVE,
                "archive": ProjectLifecycle.ARCHIVED,
            }[args.project_action]
            updated = operator.transition_project(
                project_id=summary.id,
                target=target,
                reason=args.reason,
                actor=args.actor,
                changed_at=_now(),
            )
            return _project_result(f"project.{args.project_action}", updated)
        if args.project_action == "repair":
            updated = operator.close_project_circuit(
                project_id=summary.id,
                reason=args.reason,
                actor=args.actor,
                changed_at=_now(),
            )
            return _project_result("project.repair", updated)
        if args.project_action == "append-revision":
            revision_id, sequence = operator.next_revision_identity(summary.id)
            revision = _build_revision(
                args,
                store=store,
                operator=operator,
                project_id=summary.id,
                revision_id=revision_id,
                sequence=sequence,
                exclude_project_id=summary.id,
            )
            if revision.project_key != summary.key:
                raise V5OperatorError(
                    f"new Project manifest key {revision.project_key!r} differs "
                    f"from registered immutable key {summary.key!r}"
                )
            projects.append_revision(
                verify_project_revision(revision), activate=not args.no_activate
            )
            return _project_result(
                "project.append-revision",
                operator.get_project_summary(project_id=summary.id),
            )
        assert args.project_action == "activate-revision"
        projects.activate_revision(
            project_id=summary.id,
            revision_id=args.revision_id,
            actor=args.actor,
            changed_at=_now(),
        )
        return _project_result(
            "project.activate-revision",
            operator.get_project_summary(project_id=summary.id),
        )

    if args.command == "submit":
        summary = _select_project(operator, args.project)
        if summary.typed_view is None:
            raise V5OperatorError(
                f"Project {summary.key!r} has no active typed Project/v1 revision; "
                "append and activate one before submission"
            )
        revision = summary.typed_view.current_revision
        submission = _submission(args, revision)
        if args.dry_run:
            document = submission_dry_run(
                revision=revision, submission=submission
            )
            return _Result(
                document,
                (
                    f"dry-run valid for {summary.key}/{args.card_path}:{args.job_id}; "
                    "no global item ID allocated"
                ),
            )
        item = projects.admit(
            compile_admission_from_revision(
                revision=revision, submission=submission
            ),
            added_at=_now(),
        )
        view = operator.get_item(item.id, project_id=summary.id)
        return _Result(
            {
                "operation": "submit",
                "outputContract": "diagnostic operator output; immutable evidence is persisted in v5",
                "ok": True,
                "item": _item_document(view),
            },
            (
                f"admitted global queue item {item.id}: {summary.key}/"
                f"{item.experiment_id}/a{item.attempt} ({item.state})"
            ),
        )

    if args.command == "status":
        summary = _select_project(operator, args.project)
        items = operator.list_items(
            project_id=summary.id,
            states=tuple(args.state),
            after_id=args.after_id,
            limit=args.limit,
        )
        document = {
            "operation": "status",
            "outputContract": "diagnostic operator output; not a persistent protocol",
            "ok": True,
            "project": _summary_document(summary),
            "items": [_item_document(item) for item in items],
        }
        lines = [
            f"Project {summary.key}: lifecycle={summary.lifecycle.value} "
            f"health={summary.health.value} dispatch="
            f"{'allowed' if summary.dispatch_allowed else 'blocked'}",
            "GLOBAL-ID  STATE  PRI  EXPERIMENT/ATTEMPT  REVISION",
        ]
        lines.extend(
            f"{view.item.id}  {view.item.state}  {view.item.priority}  "
            f"{view.item.experiment_id}/a{view.item.attempt}  {view.revision_label}"
            for view in items
        )
        return _Result(cast(dict[str, JSONValue], document), "\n".join(lines))

    if args.command == "events":
        summary = _select_project(operator, args.project)
        values = operator.list_events(
            project_id=summary.id, after_id=args.after_id, limit=args.limit
        )
        return _Result(
            {
                "operation": "events",
                "outputContract": "diagnostic operator output; payloads are exact persisted canonical JSON",
                "ok": True,
                "projectId": summary.id,
                "projectKey": summary.key,
                "events": [_event_document(value) for value in values],
            },
            "\n".join(
                ["EVENT-ID  CREATED  TYPE  GLOBAL-ITEM-ID"]
                + [
                    f"{value.id}  {value.created_at}  {value.event_type}  "
                    f"{value.queue_item_id if value.queue_item_id is not None else '-'}"
                    for value in values
                ]
            ),
        )

    if args.command == "item":
        summary = _select_project(operator, args.project)
        timestamp = _now()
        if args.item_action == "show":
            view = operator.get_item(args.item_id, project_id=summary.id)
        elif args.item_action == "hold":
            view = operator.hold_item(
                args.item_id,
                project_id=summary.id,
                reason=args.reason,
                actor=args.actor,
                changed_at=timestamp,
            )
        elif args.item_action == "release":
            view = operator.release_item(
                args.item_id,
                project_id=summary.id,
                actor=args.actor,
                changed_at=timestamp,
            )
        elif args.item_action == "priority":
            view = operator.set_item_priority(
                args.item_id,
                project_id=summary.id,
                priority=args.value,
                actor=args.actor,
                changed_at=timestamp,
            )
        elif args.item_action == "remove":
            view = operator.remove_item(
                args.item_id,
                project_id=summary.id,
                reason=args.reason,
                actor=args.actor,
                changed_at=timestamp,
            )
        elif args.item_action == "preempt":
            operator.get_item(args.item_id, project_id=summary.id)
            pending = V5SchedulerService(store).request_manual_preemption(
                args.item_id,
                note=args.note,
                actor=args.actor,
                requested_at=timestamp,
            )
            request_id = getattr(pending, "request_id", None)
            if request_id is None:
                request_id = pending.request.request_id
            return _Result(
                {
                    "operation": "item.preempt",
                    "outputContract": "diagnostic operator output; request is persisted CooperativeYieldRequest/v1 evidence",
                    "ok": True,
                    "projectId": pending.project_id,
                    "revisionId": pending.revision_id,
                    "queueItemId": pending.queue_item_id,
                    "segment": pending.segment,
                    "requestId": request_id,
                    "requestSha256": pending.request_sha256,
                    "requestPath": str(pending.request_path),
                    "receiptPath": str(pending.receipt_path),
                },
                (
                    f"manual preemption request {request_id} "
                    f"persisted for global queue item {pending.queue_item_id}; "
                    "checkpoint receipt will be reconciled by the scheduler"
                ),
            )
        elif args.item_action == "resolve-abandoned-launch":
            # Resolve the project-qualified identity before invoking the
            # scheduler-owned filesystem/process safety boundary.
            operator.get_item(args.item_id, project_id=summary.id)
            outcome = V5SchedulerService(store).resolve_abandoned_launch(
                args.item_id,
                project_id=summary.id,
                gpu_uuid=args.gpu_uuid,
                reason=args.reason,
                actor=args.actor,
                confirm=args.confirm,
                changed_at=timestamp,
            )
            resolution = outcome.resolution
            return _Result(
                {
                    "operation": "item.resolve-abandoned-launch",
                    "outputContract": (
                        "diagnostic operator output; durable resolution is the "
                        f"{resolution.event_type} event"
                    ),
                    "ok": True,
                    "projectId": resolution.project_id,
                    "queueItemId": resolution.item_id,
                    "gpuUuid": resolution.gpu_uuid,
                    "previousState": resolution.previous_state,
                    "eventType": resolution.event_type,
                    "state": resolution.state,
                    "reason": resolution.reason,
                    "resolvedAt": resolution.resolved_at,
                    "launchReceiptStatus": outcome.launch_receipt_status,
                    "worktreeCleanupError": outcome.worktree_cleanup_error,
                    "hostDispatchPaused": True,
                    "projectRepairRequired": True,
                },
                (
                    f"abandoned launch for global queue item {resolution.item_id} "
                    "resolved as failed; host remains paused and Project repair "
                    "is still required"
                ),
            )
        else:
            assert args.item_action in {"terminate", "force-kill"}
            # Authorize the project-scoped item before a short-lived service
            # mutates host process state for its global identifier.
            operator.get_item(args.item_id, project_id=summary.id)
            outcome = V5SchedulerService(store).request_termination(
                args.item_id,
                reason=args.reason,
                actor=args.actor,
                force=args.item_action == "force-kill",
                requested_at=timestamp,
            )
            action = outcome.action
            return _Result(
                {
                    "operation": f"item.{args.item_action}",
                    "outputContract": (
                        "diagnostic operator output; durable termination intent "
                        "and signal attempts are persisted as schema-v5 events"
                    ),
                    "ok": True,
                    "projectId": action.project_id,
                    "queueItemId": action.item_id,
                    "segment": action.segment,
                    "state": action.state,
                    "stage": action.stage,
                    "requestedAt": action.requested_at,
                    "reason": action.reason,
                    "signalDelivered": outcome.signal_delivered,
                },
                (
                    f"{action.stage} termination persisted for global queue item "
                    f"{action.item_id}; initial signal "
                    f"{'delivered' if outcome.signal_delivered else 'not delivered'}"
                ),
            )
        return _Result(
            {
                "operation": f"item.{args.item_action}",
                "outputContract": "diagnostic operator output; not a persistent protocol",
                "ok": True,
                "item": _item_document(view),
            },
            (
                f"global queue item {view.item.id}: state={view.item.state} "
                f"priority={view.item.priority} project={view.project_key}"
            ),
        )

    if args.command == "artifact":
        summary = _select_project(operator, args.project)
        values = operator.list_artifacts(
            project_id=summary.id,
            queue_item_id=args.item_id,
            after_id=args.after_id,
            limit=args.limit,
        )
        return _Result(
            {
                "operation": "artifact.list",
                "outputContract": "diagnostic operator output of immutable v5 artifact evidence",
                "ok": True,
                "projectId": summary.id,
                "projectKey": summary.key,
                "artifacts": [_artifact_document(value) for value in values],
            },
            "\n".join(
                ["ARTIFACT-ID  GLOBAL-ITEM-ID  SEGMENT  NAME  PATH"]
                + [
                    f"{value.id}  {value.queue_item_id}  {value.segment}  "
                    f"{value.artifact_name}  {value.absolute_path}"
                    for value in values
                ]
            ),
        )

    if args.command == "receipt":
        summary = _select_project(operator, args.project)
        exported = operator.project_export(summary.id)
        host_event = exported.host_state.provenance_event
        queue_export = QueueExport.create(
            package_version=__version__,
            database=database_instance_document(
                state_directory=str(store.state_dir),
                database_path=str(store.database_path),
                instance_identity=store.instance_identity(),
            ),
            exported_at=_now(),
            actor=args.actor,
            host_state={
                "dispatchPaused": exported.host_state.dispatch_paused,
                "reason": exported.host_state.reason,
                "provenance": None if host_event is None else {
                    "eventId": host_event.id,
                    "createdAt": host_event.created_at,
                    "actor": host_event.actor,
                    "eventType": host_event.event_type,
                    "payload": host_event.payload,
                },
            },
            project=_summary_document(exported.project),
            revisions=[_revision_document(value) for value in exported.revisions],
            items=[_item_document(value) for value in exported.items],
            events=[_event_document(value) for value in exported.events],
            artifacts=[_artifact_document(value) for value in exported.artifacts],
            yield_requests=[
                wire_evidence_document(
                    queue_item_id=value.request.queue_item_id,
                    project_id=value.project_id,
                    revision_id=value.revision_id,
                    request_id=value.request.request_id,
                    source=value.source,
                    source_sha256=value.sha256,
                    document=value.request.to_document(),
                )
                for value in exported.yield_requests
            ],
            yield_receipts=[
                wire_evidence_document(
                    queue_item_id=value.receipt.queue_item_id,
                    project_id=value.project_id,
                    revision_id=value.revision_id,
                    request_id=value.receipt.request_id,
                    source=value.source,
                    source_sha256=value.sha256,
                    document=value.receipt.to_document(),
                )
                for value in exported.yield_receipts
            ],
        )
        return _Result(
            queue_export.to_document(),
            (
                f"Project {summary.key} QueueExport/v1: "
                f"{len(exported.revisions)} revisions, {len(exported.items)} items, "
                f"{len(exported.events)} events, {len(exported.artifacts)} artifacts, "
                f"{len(exported.yield_requests)} typed yield requests, "
                f"{len(exported.yield_receipts)} typed yield receipts; exact "
                "ExecutorReceipt bytes are truthfully recorded as unavailable"
            ),
            queue_export.canonical_json,
        )

    if args.command == "host":
        controller = V5SchedulingController(store)
        timestamp = _now()
        if args.host_action == "pause":
            changed = controller.pause_host(
                reason=args.reason, actor=args.actor, changed_at=timestamp
            )
        else:
            changed = controller.resume_host(actor=args.actor, changed_at=timestamp)
        paused, reason = controller.host_dispatch_state()
        return _Result(
            {
                "operation": f"host.{args.host_action}",
                "outputContract": "diagnostic operator output; not a persistent protocol",
                "ok": True,
                "changed": changed,
                "dispatchPaused": paused,
                "reason": reason,
            },
            (
                f"host dispatch is {'paused' if paused else 'running'}"
                + (f": {reason}" if reason else "")
                + ("" if changed else " (already in requested state)")
            ),
        )

    if args.command == "gpu":
        timestamp = _now()
        if args.gpu_action == "show":
            values = operator.list_gpus()
        elif args.gpu_action == "add":
            snapshots = query_gpus(args.nvidia_smi)
            matches = {
                gpu.uuid: gpu
                for gpu in snapshots
                if gpu.index == args.identifier
                or gpu.uuid == args.identifier
                or gpu.uuid.startswith(args.identifier)
            }
            if not matches:
                raise V5OperatorError(
                    f"GPU identifier {args.identifier!r} matched no observed index "
                    "or UUID; check nvidia-smi and retry"
                )
            if len(matches) != 1:
                raise V5OperatorError(
                    f"GPU identifier {args.identifier!r} is ambiguous; use a "
                    "longer or full UUID"
                )
            gpu = next(iter(matches.values()))
            operator.add_gpu(
                uuid=gpu.uuid,
                requested_identifier=args.identifier,
                last_index=gpu.index,
                name=gpu.name,
                actor=args.actor,
                changed_at=timestamp,
            )
            values = operator.list_gpus()
        else:
            transition = {
                "enable": operator.enable_gpu,
                "disable": operator.disable_gpu,
                "drain": operator.drain_gpu,
                "undrain": operator.undrain_gpu,
            }[args.gpu_action]
            transition(args.uuid, actor=args.actor, changed_at=timestamp)
            values = operator.list_gpus()
        return _Result(
            {
                "operation": f"gpu.{args.gpu_action}",
                "outputContract": "diagnostic operator output; not a persistent protocol",
                "ok": True,
                "gpus": [_gpu_document(value) for value in values],
            },
            "\n".join(
                ["INDEX  UUID  ENABLED  DRAINING  GLOBAL-ITEM-IDS"]
                + [
                    f"{value.last_index}  {value.uuid}  "
                    f"{'yes' if value.enabled else 'no'}  "
                    f"{'yes' if value.draining else 'no'}  "
                    f"{','.join(str(item) for item in value.assigned_queue_item_ids) or '-'}"
                    for value in values
                ]
            ),
        )

    if args.command == "reservation":
        reservations = V5ReservationService(store)
        timestamp = _now()
        if args.reservation_action == "list":
            values = reservations.list_reservations(
                gpu_uuid=args.gpu_uuid,
                open_only=args.open_only,
            )
        elif args.reservation_action == "request":
            values = (
                reservations.request_reservation(
                    gpu_uuid=args.gpu_uuid,
                    duration_hours=args.duration_hours,
                    note=args.note,
                    requested_by=args.actor,
                    requested_at=timestamp,
                ),
            )
        else:
            assert args.reservation_action == "release"
            values = (
                reservations.release_reservation(
                    args.reservation_id,
                    released_by=args.actor,
                    released_at=timestamp,
                ),
            )
        return _Result(
            {
                "operation": f"reservation.{args.reservation_action}",
                "outputContract": "diagnostic operator output; reservation rows are schema-v5 state",
                "ok": True,
                "reservations": [_reservation_document(value) for value in values],
            },
            "\n".join(
                ["ID  GPU-UUID  STATUS  REQUESTED-BY  STARTS  EXPIRES"]
                + [
                    f"{value.id}  {value.gpu_uuid}  {value.status.value}  "
                    f"{value.requested_by}  {value.starts_at or '-'}  "
                    f"{value.expires_at or '-'}"
                    for value in values
                ]
            ),
        )

    assert args.command == "serve"
    service = V5SchedulerService(
        store,
        poll_seconds=args.poll_seconds,
        control_seconds=args.control_seconds,
        min_free_memory_fraction=args.min_free_memory_fraction,
        max_utilization_percent=args.max_utilization_percent,
        min_free_disk_gib=args.min_free_disk_gib,
        gpu_provider=lambda: query_gpus(args.nvidia_smi),
    )
    service.run(once=args.once)
    return _Result(
        {
            "operation": "serve",
            "outputContract": "diagnostic operator output; not a persistent protocol",
            "ok": True,
            "once": args.once,
            "stopped": True,
        },
        "schema-v5 scheduler pass completed" if args.once else "schema-v5 scheduler stopped",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run one production v5 command and return a process-style exit code."""

    args = build_arg_parser().parse_args(argv)
    json_output = bool(getattr(args, "json", False))
    try:
        result = _dispatch(args)
    except _HANDLED_ERRORS as exc:
        if json_output:
            print(
                json.dumps(
                    {
                        "operation": "operator.error",
                        "outputContract": "diagnostic error; not a persistent protocol",
                        "ok": False,
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    },
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
        else:
            print(f"experiment queue error: {exc}", file=sys.stderr)
        return 2
    if json_output:
        if result.protocol_source is not None:
            sys.stdout.write(result.protocol_source.decode("utf-8"))
        else:
            print(
                json.dumps(
                    result.document,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
            )
    else:
        print(result.readable)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_arg_parser", "main"]
