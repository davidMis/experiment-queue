"""Load the strict YAML authoring subset and produce canonical JSON evidence."""

from __future__ import annotations

from codecs import BOM_UTF8
from hashlib import sha256
import math
from typing import TypeAlias

import rfc8785
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from ruamel.yaml.events import (
    AliasEvent,
    DocumentStartEvent,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
)
from ruamel.yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


JSONScalar: TypeAlias = None | bool | int | float | str
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

SAFE_INTEGER_MIN = -(2**53) + 1
SAFE_INTEGER_MAX = 2**53 - 1
# Count child edges from the document root. This keeps validation comfortably
# below Python and parser recursion limits while admitting ordinary manifests.
MAX_NESTING_DEPTH = 64

_YAML_MAP_TAG = "tag:yaml.org,2002:map"
_YAML_SEQUENCE_TAG = "tag:yaml.org,2002:seq"
_YAML_STRING_TAG = "tag:yaml.org,2002:str"
_YAML_TIMESTAMP_TAG = "tag:yaml.org,2002:timestamp"
_YAML_MERGE_TAG = "tag:yaml.org,2002:merge"
_ALLOWED_IMPLICIT_TAGS = frozenset(
    {
        _YAML_MAP_TAG,
        _YAML_SEQUENCE_TAG,
        _YAML_STRING_TAG,
        "tag:yaml.org,2002:null",
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
    }
)


class StrictYAMLError(ValueError):
    """Raised when source bytes are outside the admitted YAML 1.2 subset."""


class CanonicalJSONError(ValueError):
    """Raised when a value cannot be represented by the canonical JSON contract."""


def _yaml_parser() -> YAML:
    """Return a fresh pure-Python YAML 1.2 safe loader.

    ruamel.yaml documents that a parser which raised is not reusable. Creating
    an instance for each phase also prevents parser/composer state from leaking
    between operator-supplied documents.
    """

    parser = YAML(typ="safe", pure=True)
    parser.version = (1, 2)
    parser.allow_duplicate_keys = False
    return parser


def _strict_utf8(source: bytes, *, source_name: str) -> str:
    if type(source) is not bytes:
        raise TypeError(
            f"{source_name}: strict YAML input must be bytes, got "
            f"{type(source).__name__}"
        )
    if source.startswith(BOM_UTF8):
        raise StrictYAMLError(
            f"{source_name}: UTF-8 byte-order marks are not allowed; save as UTF-8 "
            "without BOM"
        )
    try:
        return source.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise StrictYAMLError(
            f"{source_name}: input must be valid UTF-8; invalid byte at offset "
            f"{exc.start}"
        ) from exc


def _presentation_preflight(text: str, *, source_name: str) -> None:
    document_count = 0
    open_containers = 0
    try:
        for event in _yaml_parser().parse(text):
            # Event parsing is streaming, so enforce the depth contract before
            # compose() allocates a complete node tree. The number of open
            # containers is exactly the child-edge depth of the next node.
            if isinstance(event, (MappingStartEvent, SequenceStartEvent)):
                if open_containers > MAX_NESTING_DEPTH:
                    raise StrictYAMLError(
                        f"{source_name}: YAML nesting exceeds the maximum depth of "
                        f"{MAX_NESTING_DEPTH}"
                    )
                open_containers += 1
            elif isinstance(event, ScalarEvent):
                if open_containers > MAX_NESTING_DEPTH:
                    raise StrictYAMLError(
                        f"{source_name}: YAML nesting exceeds the maximum depth of "
                        f"{MAX_NESTING_DEPTH}"
                    )
            elif isinstance(event, (MappingEndEvent, SequenceEndEvent)):
                open_containers -= 1

            if isinstance(event, DocumentStartEvent):
                document_count += 1
                if event.version not in (None, (1, 2)):
                    raise StrictYAMLError(
                        f"{source_name}: YAML directive must declare version 1.2, "
                        f"got {event.version!r}"
                    )
                if event.tags:
                    raise StrictYAMLError(
                        f"{source_name}: YAML tag directives are not allowed"
                    )
            if isinstance(event, AliasEvent):
                raise StrictYAMLError(
                    f"{source_name}: YAML aliases are not allowed"
                )
            if getattr(event, "anchor", None) is not None:
                raise StrictYAMLError(
                    f"{source_name}: YAML anchors are not allowed"
                )
            if getattr(event, "tag", None) is not None:
                raise StrictYAMLError(
                    f"{source_name}: explicit YAML tags are not allowed"
                )
    except StrictYAMLError:
        raise
    except RecursionError as exc:
        raise StrictYAMLError(
            f"{source_name}: YAML nesting exceeds the maximum depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc
    except YAMLError as exc:
        raise StrictYAMLError(f"{source_name}: invalid YAML: {exc}") from exc

    if document_count != 1:
        raise StrictYAMLError(
            f"{source_name}: expected exactly one non-empty YAML document, "
            f"found {document_count}"
        )


def _node_preflight(
    node: Node,
    *,
    source_name: str,
    path: str = "$",
    depth: int = 0,
    active_node_ids: set[int] | None = None,
) -> None:
    if depth > MAX_NESTING_DEPTH:
        raise StrictYAMLError(
            f"{source_name}: {path} exceeds the maximum nesting depth of "
            f"{MAX_NESTING_DEPTH}"
        )

    if active_node_ids is None:
        active_node_ids = set()
    node_id = id(node)
    if node_id in active_node_ids:
        raise StrictYAMLError(
            f"{source_name}: {path} contains a recursive YAML node"
        )

    if node.tag == _YAML_TIMESTAMP_TAG:
        raise StrictYAMLError(
            f"{source_name}: {path} is an implicit YAML timestamp; quote it to "
            "preserve it as a string"
        )
    if node.tag == _YAML_MERGE_TAG:
        raise StrictYAMLError(f"{source_name}: {path} uses a YAML merge key")
    if node.tag not in _ALLOWED_IMPLICIT_TAGS:
        raise StrictYAMLError(
            f"{source_name}: {path} resolved to unsupported YAML tag {node.tag!r}"
        )

    active_node_ids.add(node_id)
    try:
        if isinstance(node, MappingNode):
            seen_keys: set[str] = set()
            for key_node, value_node in node.value:
                _node_preflight(
                    key_node,
                    source_name=source_name,
                    path=f"{path} key",
                    depth=depth + 1,
                    active_node_ids=active_node_ids,
                )
                if (
                    not isinstance(key_node, ScalarNode)
                    or key_node.tag != _YAML_STRING_TAG
                ):
                    raise StrictYAMLError(
                        f"{source_name}: {path} mapping keys must be strings"
                    )
                key = key_node.value
                if key in seen_keys:
                    raise StrictYAMLError(
                        f"{source_name}: {path} contains duplicate key {key!r}"
                    )
                seen_keys.add(key)
                _node_preflight(
                    value_node,
                    source_name=source_name,
                    path=f"{path}.{key}",
                    depth=depth + 1,
                    active_node_ids=active_node_ids,
                )
        elif isinstance(node, SequenceNode):
            for index, item in enumerate(node.value):
                _node_preflight(
                    item,
                    source_name=source_name,
                    path=f"{path}[{index}]",
                    depth=depth + 1,
                    active_node_ids=active_node_ids,
                )
    finally:
        active_node_ids.remove(node_id)


def _compose_preflight(text: str, *, source_name: str) -> None:
    try:
        node = _yaml_parser().compose(text)
    except RecursionError as exc:
        raise StrictYAMLError(
            f"{source_name}: YAML nesting exceeds the maximum depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc
    except YAMLError as exc:
        raise StrictYAMLError(f"{source_name}: invalid YAML: {exc}") from exc
    if node is None:
        raise StrictYAMLError(f"{source_name}: YAML document must not be empty")
    if (
        isinstance(node, ScalarNode)
        and node.tag == "tag:yaml.org,2002:null"
        and node.value == ""
    ):
        raise StrictYAMLError(f"{source_name}: YAML document must not be empty")
    try:
        _node_preflight(node, source_name=source_name)
    except RecursionError as exc:
        raise StrictYAMLError(
            f"{source_name}: YAML nesting exceeds the maximum depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc


def _json_problem(
    value: object,
    *,
    path: str = "$",
    depth: int = 0,
    active_container_ids: set[int] | None = None,
) -> str | None:
    if depth > MAX_NESTING_DEPTH:
        return (
            f"{path} exceeds the maximum nesting depth of "
            f"{MAX_NESTING_DEPTH}"
        )

    value_type = type(value)
    if value is None or value_type is bool:
        return None
    if value_type is int:
        if value < SAFE_INTEGER_MIN or value > SAFE_INTEGER_MAX:
            return (
                f"{path} integer {value} is outside the safe JSON domain "
                f"[{SAFE_INTEGER_MIN}, {SAFE_INTEGER_MAX}]; encode it as a string"
            )
        return None
    if value_type is float:
        if not math.isfinite(value):
            return f"{path} contains a non-finite number"
        return None
    if value_type is str:
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            return f"{path} contains a lone surrogate or non-UTF-8 code point"
        return None
    if value_type is list or value_type is dict:
        if active_container_ids is None:
            active_container_ids = set()
        container_id = id(value)
        if container_id in active_container_ids:
            return f"{path} contains a recursive JSON container"
        active_container_ids.add(container_id)
        try:
            if value_type is list:
                for index, item in enumerate(value):
                    problem = _json_problem(
                        item,
                        path=f"{path}[{index}]",
                        depth=depth + 1,
                        active_container_ids=active_container_ids,
                    )
                    if problem is not None:
                        return problem
                return None

            for key, item in value.items():
                if type(key) is not str:
                    return f"{path} object key {key!r} is not a string"
                problem = _json_problem(
                    key,
                    path=f"{path} object key",
                    depth=depth + 1,
                    active_container_ids=active_container_ids,
                )
                if problem is not None:
                    return problem
                problem = _json_problem(
                    item,
                    path=f"{path}.{key}",
                    depth=depth + 1,
                    active_container_ids=active_container_ids,
                )
                if problem is not None:
                    return problem
            return None
        finally:
            active_container_ids.remove(container_id)
    return f"{path} has unsupported non-JSON type {value_type.__name__}"


def load_strict_yaml(source: bytes, *, source_name: str = "<bytes>") -> JSONValue:
    """Parse one UTF-8 YAML document under the fail-closed ADR 0006 subset.

    Paths deeper than :data:`MAX_NESTING_DEPTH` child edges are rejected so
    parser recursion limits cannot become an input-dependent failure mode.
    """

    text = _strict_utf8(source, source_name=source_name)
    _presentation_preflight(text, source_name=source_name)
    _compose_preflight(text, source_name=source_name)
    try:
        value = _yaml_parser().load(text)
    except RecursionError as exc:
        raise StrictYAMLError(
            f"{source_name}: YAML nesting exceeds the maximum depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc
    except YAMLError as exc:
        raise StrictYAMLError(f"{source_name}: invalid YAML: {exc}") from exc

    try:
        problem = _json_problem(value)
    except RecursionError as exc:
        raise StrictYAMLError(
            f"{source_name}: YAML nesting exceeds the maximum depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc
    if problem is not None:
        raise StrictYAMLError(f"{source_name}: {problem}")
    return value


def canonical_json_bytes(value: JSONValue) -> bytes:
    """Return RFC 8785 bytes for JSON no deeper than ``MAX_NESTING_DEPTH``."""

    try:
        problem = _json_problem(value)
    except RecursionError as exc:
        raise CanonicalJSONError(
            f"JSON nesting exceeds the maximum depth of {MAX_NESTING_DEPTH}"
        ) from exc
    if problem is not None:
        raise CanonicalJSONError(problem)
    try:
        return rfc8785.dumps(value)
    except RecursionError as exc:
        raise CanonicalJSONError(
            f"RFC 8785 canonicalization exceeded the maximum nesting depth of "
            f"{MAX_NESTING_DEPTH}"
        ) from exc
    except rfc8785.CanonicalizationError as exc:
        raise CanonicalJSONError(f"RFC 8785 canonicalization failed: {exc}") from exc


def sha256_bytes(value: bytes) -> str:
    """Return the lowercase SHA-256 hex digest of exact bytes."""

    if type(value) is not bytes:
        raise TypeError(f"SHA-256 input must be bytes, got {type(value).__name__}")
    return sha256(value).hexdigest()


def canonical_json_sha256(value: JSONValue) -> str:
    """Return SHA-256 over exactly the RFC 8785 canonical bytes for a value."""

    return sha256_bytes(canonical_json_bytes(value))


__all__ = [
    "CanonicalJSONError",
    "JSONScalar",
    "JSONValue",
    "MAX_NESTING_DEPTH",
    "SAFE_INTEGER_MAX",
    "SAFE_INTEGER_MIN",
    "StrictYAMLError",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "load_strict_yaml",
    "sha256_bytes",
]
