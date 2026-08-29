"""Standalone, mountable CLI for read-only v5 authoring/operator services."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Protocol, cast

from experiment_queue.admission import AdmissionError, Submission
from experiment_queue.authoring import AuthoringValidationError, Project
from experiment_queue.execution import ExecutionValidationError
from experiment_queue.extensions import ExtensionSchemaError, ExtensionValidationError
from experiment_queue.git_resolver import GitResolverError, MAX_GIT_SOURCE_BYTES
from experiment_queue.operator_services import (
    OperatorServiceError,
    doctor_project_revision,
    load_enrollment_document,
    project_manifest_scaffold,
    submission_dry_run,
    validate_card_source,
    validate_project_source,
)
from experiment_queue.project_lifecycle import (
    LifecycleValidationError,
    ProjectRevision,
)
from experiment_queue.serialization import (
    JSONValue,
    canonical_json_bytes,
    sha256_bytes,
)


_READ_ONLY_ACTOR = "operator-cli:read-only-validation"
_VALIDATION_ERRORS = (
    AdmissionError,
    AuthoringValidationError,
    ExecutionValidationError,
    ExtensionSchemaError,
    ExtensionValidationError,
    GitResolverError,
    LifecycleValidationError,
    OperatorServiceError,
)
_HANDLED_ERRORS = _VALIDATION_ERRORS + (OSError,)


class _SubparserFactory(Protocol):
    """Small argparse surface needed to mount commands under a future v5 CLI."""

    def add_parser(self, name: str, **kwargs: object) -> argparse.ArgumentParser: ...


def _read_bytes(path: Path, *, purpose: str) -> bytes:
    """Read one bounded regular-file input with an actionable path error."""

    try:
        if path.is_symlink():
            # Local authoring links are acceptable, but resolve once so error
            # output names the actual source used for this read-only request.
            path = path.resolve(strict=True)
        if not path.is_file():
            raise OperatorServiceError(
                f"{purpose} path {str(path)!r} is not an existing regular file"
            )
        size = path.stat().st_size
        if size > MAX_GIT_SOURCE_BYTES:
            raise OperatorServiceError(
                f"{purpose} path {str(path)!r} is {size} bytes; the maximum is "
                f"{MAX_GIT_SOURCE_BYTES} bytes"
            )
        with path.open("rb") as source_file:
            source = source_file.read(MAX_GIT_SOURCE_BYTES + 1)
    except OperatorServiceError:
        raise
    except OSError as exc:
        raise OperatorServiceError(
            f"could not read {purpose} path {str(path)!r}: {exc}"
        ) from exc
    if len(source) > MAX_GIT_SOURCE_BYTES:
        raise OperatorServiceError(
            f"{purpose} path {str(path)!r} grew beyond the "
            f"{MAX_GIT_SOURCE_BYTES}-byte limit while being read"
        )
    return source


def _optional_source(path: Path | None, *, purpose: str) -> bytes | None:
    return None if path is None else _read_bytes(path, purpose=purpose)


def _write_scaffold(
    path: Path,
    source: bytes,
    *,
    force: bool,
) -> Path:
    """Atomically publish one scaffold, refusing overwrite by default."""

    target = path.absolute()
    parent = target.parent
    if not parent.is_dir():
        raise OperatorServiceError(
            f"scaffold parent {str(parent)!r} is not an existing "
            "directory; create the parent or choose another --output"
        )
    if os.path.lexists(target) and not force:
        raise OperatorServiceError(
            f"scaffold output {str(target)!r} already exists; choose a new "
            "path or pass --force to replace exactly that file"
        )
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(source)
            temporary.flush()
            os.fsync(temporary.fileno())
        if force:
            os.replace(temporary_name, target)
        else:
            try:
                # Publishing a same-filesystem hard link is atomic and, unlike
                # rename/replace, cannot overwrite a path that appears during
                # the write.
                os.link(temporary_name, target)
            except FileExistsError as exc:
                raise OperatorServiceError(
                    f"scaffold output {str(target)!r} appeared while "
                    "writing; nothing was replaced"
                ) from exc
            os.unlink(temporary_name)
        temporary_name = None
    except OperatorServiceError:
        raise
    except OSError as exc:
        raise OperatorServiceError(
            f"could not publish scaffold at {str(target)!r}: {exc}"
        ) from exc
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
    return target


def _add_project_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help=(
            "Path to exact Project/v1 YAML bytes; relative paths are resolved from "
            "the current directory and the file is never modified."
        ),
    )
    parser.add_argument(
        "--extension-schema",
        type=Path,
        help=(
            "Path to exact extension-schema JSON bytes; required only when the "
            "Project declares spec.extensionSchema and never read otherwise."
        ),
    )


def _add_revision_arguments(parser: argparse.ArgumentParser) -> None:
    _add_project_source_arguments(parser)
    parser.add_argument(
        "--enrollment",
        required=True,
        type=Path,
        help=(
            "Path to a complete versioned Enrollment document matching "
            "Enrollment.to_document(); parsing rederives every path and binding."
        ),
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        type=Path,
        help=(
            "Existing absolute queue state directory used only for overlap "
            "validation; this command creates no state files."
        ),
    )
    parser.add_argument(
        "--project-id",
        required=True,
        type=int,
        help="Positive registered Project database ID to include in evidence.",
    )
    parser.add_argument(
        "--revision-id",
        required=True,
        type=int,
        help="Positive immutable ProjectRevision database ID to verify.",
    )
    parser.add_argument(
        "--revision-sequence",
        required=True,
        type=int,
        help=(
            "Positive per-project revision sequence; evidence label becomes "
            "<project-key>:r<sequence>."
        ),
    )
    parser.add_argument(
        "--git-commit",
        required=True,
        help=(
            "Exact full 40- or 64-character commit object ID; branches, tags, "
            "abbreviations, and network fetching are refused."
        ),
    )


def add_operator_subcommands(subparsers: _SubparserFactory) -> None:
    """Mount all read-only operator commands beneath an existing parser."""

    project = subparsers.add_parser(
        "project",
        help="Scaffold, validate, explain, or doctor a Project without registration.",
        description=(
            "Read-only Project/v1 and host Enrollment tools. Registration and "
            "lifecycle mutation are intentionally outside this standalone surface."
        ),
    )
    project_actions = project.add_subparsers(
        dest="project_action",
        required=True,
        title="project commands",
    )
    initialize = project_actions.add_parser(
        "init",
        help="Write a minimal validated Project/v1 manifest scaffold.",
        description=(
            "Create one minimal Project.yaml scaffold. Existing output is preserved "
            "unless --force explicitly authorizes replacement."
        ),
    )
    initialize.add_argument(
        "--key",
        required=True,
        help=(
            "Immutable lowercase hyphenated Project key, at most 63 characters "
            "(for example, asteroid-inversion)."
        ),
    )
    initialize.add_argument(
        "--display-name",
        required=True,
        help="Human-readable Project name embedded safely as one YAML string.",
    )
    initialize.add_argument(
        "--output",
        type=Path,
        default=Path("Project.yaml"),
        help=(
            "Manifest path to create (default: ./Project.yaml); the parent must "
            "already exist and relative paths use the current directory."
        ),
    )
    initialize.add_argument(
        "--force",
        action="store_true",
        help=(
            "Atomically replace exactly --output if it exists; no directory or "
            "other project file is removed."
        ),
    )

    for action, description in (
        ("validate", "Validate Project source and return exact digest evidence."),
        ("explain", "Validate and explain Project declarations and normalized data."),
    ):
        command = project_actions.add_parser(
            action,
            help=description,
            description=description,
        )
        _add_project_source_arguments(command)

    doctor = project_actions.add_parser(
        "doctor",
        help="Verify current Enrollment paths and pinned Git blob identity.",
        description=(
            "Reconstruct one immutable revision, recheck current canonical paths, "
            "and authenticate Project/extension blobs at the full commit. No Git "
            "ref, worktree, database, or artifact is changed."
        ),
    )
    _add_revision_arguments(doctor)

    card = subparsers.add_parser(
        "card",
        help="Validate or explain an ExperimentCard against a Project.",
        description=(
            "Read-only ExperimentCard/v1 structural, cross-Project, protocol, "
            "artifact-root, and extension validation."
        ),
    )
    card_actions = card.add_subparsers(
        dest="card_action",
        required=True,
        title="card commands",
    )
    for action, description in (
        ("validate", "Validate a card and return exact source/schema digests."),
        ("explain", "Validate and explain every explicit job in a card."),
    ):
        command = card_actions.add_parser(
            action,
            help=description,
            description=description,
        )
        command.add_argument(
            "--project-manifest",
            required=True,
            type=Path,
            help=(
                "Path to owning Project/v1 YAML bytes; relative paths use the "
                "current directory and the file is never modified."
            ),
        )
        command.add_argument(
            "--card",
            required=True,
            type=Path,
            help=(
                "Path to ExperimentCard/v1 YAML bytes to validate; this local path "
                "is a source label, not a trusted Git admission claim."
            ),
        )
        command.add_argument(
            "--extension-schema",
            type=Path,
            help=(
                "Path to exact extension-schema JSON bytes when declared by the "
                "Project; omitted Projects must not supply it."
            ),
        )

    submission = subparsers.add_parser(
        "submission",
        help="Resolve a pinned submission without adding it to queue state.",
        description=(
            "Compile resolver-authenticated Git blobs and frozen Enrollment into "
            "machine-readable execution evidence without allocating an item ID."
        ),
    )
    submission_actions = submission.add_subparsers(
        dest="submission_action",
        required=True,
        title="submission commands",
    )
    dry_run = submission_actions.add_parser(
        "dry-run",
        help="Print resolved Git/path/env/resource/preemption evidence only.",
        description=(
            "Read Project/card/optional schema blobs from the pinned commit, "
            "compile one selected job, and print complete non-mutating evidence."
        ),
    )
    _add_revision_arguments(dry_run)
    dry_run.add_argument(
        "--card-path",
        required=True,
        help=(
            "Normalized repository-relative ExperimentCard path beneath a declared "
            "Project card root; bytes are read only from the pinned commit."
        ),
    )
    dry_run.add_argument(
        "--job-id",
        required=True,
        help="Explicit card job ID to resolve; sibling jobs are never implicit.",
    )
    dry_run.add_argument(
        "--operator",
        required=True,
        help="Non-empty operator identity copied into dry-run audit policy.",
    )
    dry_run.add_argument(
        "--bindings-json",
        default="{}",
        help=(
            "Inline JSON object of whole-value top-level parameter replacements "
            "(default: {}); interpolation and undeclared names are refused."
        ),
    )
    dry_run.add_argument(
        "--priority",
        type=int,
        default=0,
        help=(
            "Signed 64-bit global scheduling priority recorded in policy "
            "(default: 0); it never authorizes automatic preemption."
        ),
    )
    dry_run.add_argument(
        "--hold-reason",
        help=(
            "Optional non-empty initial hold reason copied into policy; dry-run "
            "does not create a held queue item."
        ),
    )
    dry_run.add_argument(
        "--dependency",
        type=int,
        action="append",
        default=[],
        help=(
            "Positive existing global queue-item ID dependency; repeat for more. "
            "Dry-run validates shape but does not query unfinished repository state."
        ),
    )
    dry_run.add_argument(
        "--authorize-preemption",
        action="store_true",
        help=(
            "Authorize manual cooperative preemption only when the selected job "
            "declares compatible yield protocols; never enables auto-preemption."
        ),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the standalone parser while sharing mountable command definitions."""

    parser = argparse.ArgumentParser(
        prog="python -m experiment_queue.operator_cli",
        description=(
            "Validate Project/Enrollment/card inputs and resolve submissions "
            "without mutating experiment-queue state."
        ),
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="commands",
    )
    add_operator_subcommands(cast(_SubparserFactory, subparsers))
    return parser


def _read_revision(args: argparse.Namespace) -> ProjectRevision:
    project_source = _read_bytes(args.manifest, purpose="Project manifest")
    extension_source = _optional_source(
        args.extension_schema,
        purpose="Project extension schema",
    )
    project = Project.from_yaml(project_source, source_name=str(args.manifest))
    # Validate Project-owned namespace/schema before any host paths are trusted.
    validate_project_source(
        source=project_source,
        source_name=str(args.manifest),
        extension_schema_source=extension_source,
    )
    enrollment_source = _read_bytes(args.enrollment, purpose="Enrollment")
    enrollment = load_enrollment_document(
        source=enrollment_source,
        source_name=str(args.enrollment),
        project=project,
        state_directory=args.state_dir,
    )
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return ProjectRevision.create(
        revision_id=args.revision_id,
        project_id=args.project_id,
        sequence=args.revision_sequence,
        project=project,
        project_source_path=enrollment.project_manifest_path,
        project_source=project_source,
        git_commit=args.git_commit,
        enrollment=enrollment,
        created_actor=_READ_ONLY_ACTOR,
        created_at=created_at,
        extension_schema_source=extension_source,
    )


def _reject_json_constant(value: str) -> object:
    raise OperatorServiceError(
        f"--bindings-json contains non-finite JSON constant {value!r}; use finite "
        "JSON values"
    )


def _bindings_object(source: str) -> dict[str, JSONValue]:
    def object_without_duplicates(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise OperatorServiceError(
                    f"--bindings-json repeats object key {key!r}; provide it once"
                )
            document[key] = value
        return document

    try:
        value = json.loads(
            source,
            parse_constant=_reject_json_constant,
            object_pairs_hook=object_without_duplicates,
        )
    except OperatorServiceError:
        raise
    except (json.JSONDecodeError, RecursionError, UnicodeError) as exc:
        raise OperatorServiceError(
            f"--bindings-json is not one strict JSON object: {exc}"
        ) from exc
    if type(value) is not dict:
        raise OperatorServiceError(
            f"--bindings-json must decode to an object, got {type(value).__name__}"
        )
    # The admission compiler will copy and validate the same value.  Canonical
    # encoding here supplies an early safe-integer/depth/Unicode error.
    try:
        canonical_json_bytes(cast(dict[str, JSONValue], value))
    except (TypeError, ValueError) as exc:
        raise OperatorServiceError(f"--bindings-json is not portable: {exc}") from exc
    return cast(dict[str, JSONValue], value)


def _dispatch(args: argparse.Namespace) -> dict[str, JSONValue]:
    if args.command == "project":
        if args.project_action == "init":
            source = project_manifest_scaffold(
                key=args.key,
                display_name=args.display_name,
            )
            output = _write_scaffold(args.output, source, force=args.force)
            return {
                "operation": "project.init",
                "outputContract": (
                    "diagnostic metadata; the created Project/v1 file is the "
                    "versioned protocol document"
                ),
                "created": True,
                "path": str(output),
                "sourceSha256": sha256_bytes(source),
                "bytes": len(source),
            }
        if args.project_action in {"validate", "explain"}:
            source = _read_bytes(args.manifest, purpose="Project manifest")
            extension = _optional_source(
                args.extension_schema,
                purpose="Project extension schema",
            )
            return validate_project_source(
                source=source,
                source_name=str(args.manifest),
                extension_schema_source=extension,
                explain=args.project_action == "explain",
            )
        assert args.project_action == "doctor"
        return doctor_project_revision(revision=_read_revision(args))

    if args.command == "card":
        project_source = _read_bytes(
            args.project_manifest,
            purpose="Project manifest",
        )
        card_source = _read_bytes(args.card, purpose="ExperimentCard")
        extension = _optional_source(
            args.extension_schema,
            purpose="Project extension schema",
        )
        return validate_card_source(
            project_source=project_source,
            project_source_name=str(args.project_manifest),
            card_source=card_source,
            card_source_name=str(args.card),
            extension_schema_source=extension,
            explain=args.card_action == "explain",
        )

    assert args.command == "submission" and args.submission_action == "dry-run"
    revision = _read_revision(args)
    submission = Submission(
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
    return submission_dry_run(revision=revision, submission=submission)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one operator request and emit one JSON document."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        document = _dispatch(args)
    except _HANDLED_ERRORS as exc:
        error = {
            "operation": "operator.error",
            "outputContract": "diagnostic error; not a persistent protocol",
            "ok": False,
            "errorType": type(exc).__name__,
            "message": str(exc),
        }
        print(
            json.dumps(error, indent=2, sort_keys=True, ensure_ascii=False),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["add_operator_subcommands", "build_arg_parser", "main"]
