"""Exercise strict YAML rejection and RFC 8785 canonicalization golden cases."""

from __future__ import annotations

from datetime import date
import math

import pytest

import experiment_queue.serialization as serialization
from experiment_queue.serialization import (
    CanonicalJSONError,
    MAX_NESTING_DEPTH,
    SAFE_INTEGER_MAX,
    SAFE_INTEGER_MIN,
    StrictYAMLError,
    canonical_json_bytes,
    canonical_json_sha256,
    load_strict_yaml,
    sha256_bytes,
)


def test_strict_yaml_accepts_json_native_yaml_12_values() -> None:
    source = b"""\
# Author comments and presentation order do not enter normalized data.
word: yes
enabled: TRUE
disabled: false
octal: 0o10
decimal: 010
nested: {items: [null, 1.5, text]}
block: |-
  line one
  line two
"""

    assert load_strict_yaml(source, source_name="card.yaml") == {
        "word": "yes",
        "enabled": True,
        "disabled": False,
        "octal": 8,
        "decimal": 10,
        "nested": {"items": [None, 1.5, "text"]},
        "block": "line one\nline two",
    }


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (b"a: 1\na: 2\n", "duplicate key 'a'"),
        (b"outer:\n  a: 1\n  a: 2\n", "duplicate key 'a'"),
        ('"é": 1\n"\\u00e9": 2\n'.encode(), "duplicate key 'é'"),
        (b"a: &unused 1\n", "anchors are not allowed"),
        (b"a: *missing\n", "aliases are not allowed"),
        (b"a: !!str 1\n", "explicit YAML tags are not allowed"),
        (
            b"%TAG !e! tag:example.invalid,2026:\n---\na: 1\n",
            "tag directives are not allowed",
        ),
        (b"base: {a: 1}\nmerged: {<<: {b: 2}}\n", "merge key"),
        (b"when: 2026-08-27\n", "implicit YAML timestamp"),
        (b"%YAML 1.1\n---\na: yes\n", "must declare version 1.2"),
        (b"---\na: 1\n---\nb: 2\n", "exactly one"),
        (b"1: value\n", "mapping keys must be strings"),
        (b"# comments only\n", "exactly one non-empty"),
        (b"---\n", "must not be empty"),
    ],
)
def test_strict_yaml_rejects_unsupported_presentation_constructs(
    source: bytes,
    message: str,
) -> None:
    with pytest.raises(StrictYAMLError, match=message):
        load_strict_yaml(source, source_name="manifest.yaml")


def test_quoted_timestamp_and_merge_spelling_remain_strings() -> None:
    assert load_strict_yaml(b'when: "2026-08-27"\n"<<": value\n') == {
        "when": "2026-08-27",
        "<<": "value",
    }


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (b"\xef\xbb\xbfkind: Project\n", "byte-order marks"),
        (b"value: \xff\n", "invalid byte at offset"),
        (b'value: "\\uD800"\n', "lone surrogate"),
    ],
)
def test_strict_yaml_rejects_non_portable_unicode_bytes(
    source: bytes,
    message: str,
) -> None:
    with pytest.raises(StrictYAMLError, match=message):
        load_strict_yaml(source)


def test_strict_yaml_enforces_rfc8785_numeric_domain() -> None:
    loaded = load_strict_yaml(
        f"minimum: {SAFE_INTEGER_MIN}\nmaximum: {SAFE_INTEGER_MAX}\n".encode()
    )
    assert loaded == {"minimum": SAFE_INTEGER_MIN, "maximum": SAFE_INTEGER_MAX}

    for value in (SAFE_INTEGER_MIN - 1, SAFE_INTEGER_MAX + 1):
        with pytest.raises(StrictYAMLError, match="outside the safe JSON domain"):
            load_strict_yaml(f"value: {value}\n".encode())

    for spelling in (".nan", ".inf", "-.inf", "1e400"):
        with pytest.raises(StrictYAMLError, match="non-finite"):
            load_strict_yaml(f"value: {spelling}\n".encode())

    rounded = load_strict_yaml(b"value: 333333333.33333329\n")
    assert canonical_json_bytes(rounded) == b'{"value":333333333.3333333}'


def test_strict_yaml_rejects_excessive_nesting_during_event_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nesting = MAX_NESTING_DEPTH + 2
    source = ("value: " + "[" * nesting + "0" + "]" * nesting + "\n").encode()

    def unexpected_compose(_text: str, *, source_name: str) -> None:
        raise AssertionError(f"compose called for {source_name}")

    monkeypatch.setattr(serialization, "_compose_preflight", unexpected_compose)

    with pytest.raises(
        StrictYAMLError,
        match=rf"deep.yaml: .*maximum depth of {MAX_NESTING_DEPTH}",
    ) as exc_info:
        load_strict_yaml(source, source_name="deep.yaml")

    assert exc_info.value.__cause__ is None


def test_strict_yaml_rejects_recursive_alias_before_construction() -> None:
    with pytest.raises(
        StrictYAMLError,
        match=r"cycle.yaml: YAML anchors are not allowed",
    ):
        load_strict_yaml(b"value: &value [*value]\n", source_name="cycle.yaml")


def test_strict_yaml_wraps_parser_recursion_with_source_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecursiveParser:
        def parse(self, _text: str) -> object:
            raise RecursionError("parser stack exhausted")

    monkeypatch.setattr(serialization, "_yaml_parser", RecursiveParser)

    with pytest.raises(
        StrictYAMLError,
        match=rf"recursive.yaml: .*maximum depth of {MAX_NESTING_DEPTH}",
    ) as exc_info:
        load_strict_yaml(b"value: 1\n", source_name="recursive.yaml")

    assert isinstance(exc_info.value.__cause__, RecursionError)


def test_rfc8785_reference_number_vector_and_negative_zero() -> None:
    value = {
        "numbers": [333333333.33333329, 1e30, 4.50, 2e-3, 1e-27],
        "negativeZero": -0.0,
    }

    assert canonical_json_bytes(value) == (
        b'{"negativeZero":0,"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27]}'
    )


def test_rfc8785_uses_utf16_property_order_and_lowercase_control_escapes() -> None:
    value = {
        "\ue000": "private-use",
        "\U00010000": "supplementary",
        "controls": "\x00\n\t",
    }

    assert canonical_json_bytes(value) == (
        b'{"controls":"\\u0000\\n\\t",'
        b'"\xf0\x90\x80\x80":"supplementary",'
        b'"\xee\x80\x80":"private-use"}'
    )


def test_unicode_is_not_normalized_before_digesting() -> None:
    composed = {"value": "\u00e9"}
    decomposed = {"value": "e\u0301"}

    assert canonical_json_bytes(composed) != canonical_json_bytes(decomposed)
    assert canonical_json_sha256(composed) != canonical_json_sha256(decomposed)


def test_source_and_canonical_digests_are_distinct_evidence() -> None:
    first = b"a: 1\nb: [true]\n"
    second = b'{"b": [true], "a": 1}\n'
    first_value = load_strict_yaml(first)
    second_value = load_strict_yaml(second)

    assert first_value == second_value
    assert sha256_bytes(first) != sha256_bytes(second)
    assert canonical_json_sha256(first_value) == canonical_json_sha256(second_value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"value": math.nan}, "non-finite"),
        ({"value": math.inf}, "non-finite"),
        ({"value": SAFE_INTEGER_MAX + 1}, "safe JSON domain"),
        ({"value": (1, 2)}, "non-JSON type tuple"),
        ({"value": date(2026, 8, 27)}, "non-JSON type date"),
        ({"value": "\ud800"}, "lone surrogate"),
        ({1: "value"}, "object key 1 is not a string"),
    ],
)
def test_canonical_json_rejects_values_outside_the_exact_json_model(
    value: object,
    message: str,
) -> None:
    with pytest.raises(CanonicalJSONError, match=message):
        canonical_json_bytes(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("container_type", [list, dict])
def test_canonical_json_rejects_recursive_containers(
    container_type: type[list[object]] | type[dict[str, object]],
) -> None:
    if container_type is list:
        value: list[object] | dict[str, object] = []
        value.append(value)
    else:
        value = {}
        value["self"] = value

    with pytest.raises(CanonicalJSONError, match="recursive JSON container"):
        canonical_json_bytes(value)  # type: ignore[arg-type]


def test_canonical_json_rejects_excessive_nesting() -> None:
    value: object = None
    for _ in range(MAX_NESTING_DEPTH + 1):
        value = [value]

    with pytest.raises(
        CanonicalJSONError,
        match=rf"maximum nesting depth of {MAX_NESTING_DEPTH}",
    ):
        canonical_json_bytes(value)  # type: ignore[arg-type]


def test_canonical_json_wraps_canonicalizer_recursion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def recursive_dumps(_value: object) -> bytes:
        raise RecursionError("canonicalizer stack exhausted")

    monkeypatch.setattr(serialization.rfc8785, "dumps", recursive_dumps)

    with pytest.raises(CanonicalJSONError, match="canonicalization exceeded") as exc_info:
        canonical_json_bytes({"value": 1})

    assert isinstance(exc_info.value.__cause__, RecursionError)


def test_canonical_sha256_is_over_exact_bytes() -> None:
    value = {"z": 0, "a": [True, None, "\u00e9"]}
    expected = b'{"a":[true,null,"\xc3\xa9"],"z":0}'

    assert canonical_json_bytes(value) == expected
    assert canonical_json_sha256(value) == (
        "c3def2e9f3389c01aaa26f79114911addc9fbcfbb4130c412bd98ea9c38a33bb"
    )
    assert canonical_json_sha256(value) == sha256_bytes(expected)


def test_byte_oriented_apis_reject_implicit_coercion() -> None:
    with pytest.raises(TypeError, match="must be bytes"):
        load_strict_yaml("a: 1\n")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be bytes"):
        sha256_bytes(bytearray(b"a"))  # type: ignore[arg-type]
