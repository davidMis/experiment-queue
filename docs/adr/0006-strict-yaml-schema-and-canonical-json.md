# ADR 0006: Strict YAML, JSON Schema, and canonical JSON

Status: accepted, 2026-08-27.

## Context

Project manifests and ExperimentCards are author-facing YAML, but their admitted
meaning and digests must be portable across supported hosts. A generic YAML safe
loader is not sufficient: even in YAML 1.2 mode it may construct aliases,
merges, timestamps, or non-JSON values. Likewise, ordinary `json.dumps()` does
not implement the cross-language number and property-order rules needed for a
protocol digest.

This decision refines ADR 0003. It does not change the independent version
lineages for Project, ExperimentCard, database, runner, export, or yield
protocols.

## Decision

### Dependencies and supported API surface

The schema foundation will declare these direct runtime dependencies:

- `ruamel.yaml>=0.19.1,<0.20`, using only
  `YAML(typ="safe", pure=True)` and documented parser/composer event and node
  APIs;
- `jsonschema>=4.26.0,<5`, using `Draft202012Validator` explicitly and without
  either optional format extra;
- `rfc8785>=0.1.4,<0.2`, using its public `dumps()` API.

The lower bounds were evaluated with CPython 3.14.4. `ruamel.yaml` 0.19.1 and
`jsonschema` 4.26.0 publish Python 3.14 support; `rfc8785` 0.1.4 is a pure-Python,
no-dependency `py3-none-any` wheel with `Requires-Python >=3.8` and was exercised
locally on Python 3.14.4. The base `ruamel.yaml` distribution is required, not
its `libyaml` or `oldlibyaml` extras, and `pure=True` is mandatory so installed
optional extensions cannot change parsing behavior.

These are compatibility bounds, not permission to accept untested semantic
changes. Minimum-bound and newest-allowed dependency runs must pass the same
golden parser, schema, RFC 8785, and digest fixtures before a release. Crossing
an upper bound requires a new accepted ADR or an ADR that supersedes this one.

### Strict YAML 1.2 subset

The loader accepts one YAML 1.2 document and produces only JSON-native values.
Each load uses fresh parser instances configured with `version = (1, 2)` and
`allow_duplicate_keys = False`; a parser instance that raised an exception is
never reused. An absent `%YAML` directive means 1.2. An explicit directive is
accepted only when it declares 1.2. Multiple documents and `%TAG` directives
are rejected.

Input bytes must be valid UTF-8 without a byte-order mark. Comments, block and
flow collections, quoted strings, and block strings are presentation only and
do not survive normalization. Mapping keys must construct as strings. Values
must recursively have exactly the JSON data-model types: object, array, string,
integer, finite binary64 number, boolean, or null. Python dates, datetimes,
sets, tuples, bytes, custom objects, and non-string mapping keys are rejected.

The implementation performs a parser-event and composed-node preflight before
constructing values. This preflight makes the following behavior mandatory:

- **Duplicate keys:** rejected at every mapping depth. Equality is evaluated
  after scalar resolution; no first-key or last-key behavior is permitted.
- **Aliases and anchors:** every alias and every anchor, including an unused
  anchor, is rejected. The normalized value is always a tree, never a YAML
  representation graph with shared or cyclic identity.
- **Tags:** every author-written local, global, or standard tag is rejected,
  including `!!str`; only the loader's implicit core scalar resolution is used.
- **Merges:** a node resolved as `tag:yaml.org,2002:merge` is rejected before
  construction. A quoted string key whose literal value is `<<` is an ordinary
  string key and is not a merge.
- **Timestamps:** a node resolved as `tag:yaml.org,2002:timestamp` is rejected
  before construction. Dates and timestamps are allowed only when quoted, in
  which case they remain unchanged strings. There is no implicit conversion to
  `date` or `datetime`.
- **Floats and integers:** finite YAML 1.2 numeric scalars are admitted as
  Python binary64 floats or integers, subject to schema validation. NaN,
  positive or negative infinity, and overflow to a non-finite value are
  rejected. Integers are restricted to the RFC 8785 implementation's safe
  domain `[-(2**53)+1, (2**53)-1]`; larger exact values must be strings. Decimal
  spellings may round to binary64 as RFC 8785 specifies, and negative zero
  canonicalizes to `0`.
- **Unicode:** strings are preserved code point for code point. NFC, NFD, case,
  and newline normalization are not performed. Lone surrogates and any value
  that cannot be emitted as valid UTF-8 are rejected. Canonically equivalent
  but differently encoded Unicode strings therefore remain different values
  and produce different digests.

YAML 1.2 core scalar behavior otherwise applies. In particular, `true` and
`false` variants are booleans, while YAML 1.1 words such as `yes`, `no`, `on`,
and `off` are strings. A schema remains responsible for deciding which of the
admitted JSON-native types and values are valid for each field.

The preflight is part of the security and provenance boundary. Validation must
not first construct and then silently discard an alias, merge, timestamp, tag,
duplicate key, or unsupported value.

### Draft 2020-12 validation

Every bundled Project and ExperimentCard schema identifies
`https://json-schema.org/draft/2020-12/schema` in `$schema`. At package test and
schema-load time, the service calls `Draft202012Validator.check_schema()` and
then instantiates `Draft202012Validator` directly. It does not use the
convenience validator's changing "latest draft" default.

All schema resources and allowed extension schemas are placed in an explicit
offline registry. An unresolved `$ref`, an unsupported dialect, or an attempt
to retrieve a schema over the network fails validation. Core constraints that
affect admission use assertions such as `pattern`, `enum`, and numeric bounds;
they do not depend on the optional `format` extras. A future custom format
checker must be explicitly named, tested, and recorded before it can affect
admission.

### Canonical JSON and digests

The canonical form is RFC 8785 JSON Canonicalization Scheme (JCS), emitted by
`rfc8785.dumps()` as UTF-8 bytes with no byte-order mark, insignificant
whitespace, or trailing newline. Objects are sorted recursively by property
name expressed as UTF-16 code units; array order is preserved; strings retain
their original Unicode code points; and finite numbers use RFC 8785's
ECMAScript-compatible serialization.

The admission pipeline is:

1. hash and retain the exact source bytes;
2. apply the strict YAML preflight and construct a JSON-native value;
3. validate that value against the identified Draft 2020-12 schema and its
   offline registry;
4. canonicalize the validated value with RFC 8785;
5. compute SHA-256 over exactly those canonical UTF-8 bytes.

Schema documents and normalized protocol documents use the same RFC 8785
canonicalization and SHA-256 rule when their identities require a digest.
Source-byte hashes and canonical-value hashes are distinct evidence and neither
substitutes for the other.

## Consequences

Equivalent mapping order, whitespace, comments, and accepted numeric spellings
produce the same normalized digest, while source hashes still distinguish the
authored files. Unicode normalization is intentionally not one of those
equivalences. Authors must quote timestamp-like scientific identifiers and
large or precision-sensitive numbers.

The queue gains three bounded runtime dependencies. In return, it avoids a
bespoke YAML parser, a partial schema implementation, and a custom
ECMAScript-number serializer. PyYAML was not selected because its common loader
behavior and duplicate-key handling require more correction for this contract.
Standard-library JSON serialization was not selected because sorting Python
strings and formatting Python floats is not RFC 8785 canonicalization.

## Required conformance fixtures

Implementation is not complete until golden tests cover, at minimum:

- duplicate keys at the root and in nested mappings;
- anchors, aliases, explicit tags, tag directives, merge nodes, and multiple
  documents;
- quoted and unquoted dates/timestamps;
- YAML 1.1 boolean words under the forced YAML 1.2 resolver;
- finite float vectors from RFC 8785, negative zero, NaN, infinities, overflow,
  and both safe-integer boundaries;
- string-only mapping keys, supplementary Unicode property ordering, composed
  versus decomposed Unicode, control escaping, a lone surrogate, invalid UTF-8,
  and a byte-order mark;
- Draft 2020-12 meta-schema checking, unknown core fields, an offline bundled
  `$ref`, an unresolved or remote `$ref`, and an unsupported dialect;
- RFC 8785 reference vectors and stable SHA-256 results on every supported
  platform and allowed dependency boundary.

## References

- [YAML 1.2.2 specification](https://yaml.org/spec/1.2.2/) and its
  [YAML 1.2 changes](https://yaml.org/spec/1.2.2/ext/changes/)
- [`ruamel.yaml` basic use](https://yaml.dev/doc/ruamel.yaml/basicuse/),
  [API behavior](https://yaml.dev/doc/ruamel.yaml/api/), and
  [0.19.1 package metadata](https://pypi.org/project/ruamel.yaml/)
- [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12) and
  [`jsonschema` validator API](https://python-jsonschema.readthedocs.io/en/stable/validate/)
- [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html) and the
  [`rfc8785` implementation](https://github.com/trailofbits/rfc8785.py/tree/v0.1.4)
