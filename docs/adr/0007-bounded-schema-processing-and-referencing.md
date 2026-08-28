# ADR 0007: Bounded schema processing and explicit referencing dependency

Status: accepted, 2026-08-27.

## Context

ADR 0006 selected the strict YAML, Draft 2020-12, and RFC 8785 stack. Its
implementation exposed four details that need a durable decision before the
Project/v1 and ExperimentCard/v1 boundary is used for admission:

- the service directly imports `referencing.Registry` and
  `referencing.Resource`, so relying on `jsonschema` to install a compatible
  transitive version would leave an undeclared runtime API dependency;
- recursive or excessively nested input can otherwise escape as a raw Python
  `RecursionError`;
- JSON Schema cannot express uniqueness by a field inside an array or every
  cross-item reference required by the authoring contracts; and
- ADR 0006's Unicode wording could be read to deny YAML's own presentation
  rules for folding and normalizing physical line breaks.

This record narrows those implementation boundaries. It does not change the
selected parser, schema draft, or canonicalization algorithm.

## Decision

### Direct dependency ownership

Declare `referencing>=0.37.0,<0.38` as a direct runtime dependency. The service
uses only the public `Registry` and `Resource` APIs to construct an offline
registry. The lower bound is exercised with Python 3.14; a release must test
the minimum and newest allowed direct dependency set together. Crossing the
upper bound requires an accepted superseding ADR.

This supplements the dependency inventory in ADR 0006. It does not alter that
record's bounds for `ruamel.yaml`, `jsonschema`, or `rfc8785`.

### Bounded document trees

Strict YAML and direct canonical-JSON inputs may contain at most 64 child edges
from the document root. The same bound applies before YAML value construction
and during JSON-domain validation. Recursive YAML nodes and cyclic Python list
or dictionary inputs are invalid even when a lower-level parser or serializer
could represent them.

Parser, composer, loader, traversal, and canonicalizer recursion failures are
translated to `StrictYAMLError` or `CanonicalJSONError`. YAML errors retain the
operator-visible source name. No raw `RecursionError` crosses the public
boundary.

### Structural and semantic Project/Card validation

The bundled Draft 2020-12 resources own structural validation and editor
assistance. Version-owned semantic validators are a mandatory second stage for
cross-item rules that the schemas cannot express. Project/v1 requires unique
logical volume and environment names. ExperimentCard/v1 requires unique job
IDs and per-job artifact names, and each cooperative-yield checkpoint name
must reference an artifact declared by that job. A cooperative-yield
capability must declare at least one checkpoint name, and every referenced
artifact must have `type: file`, matching the v1 helper's regular-file hashing
contract.

Both layers belong to the same Project/v1 or ExperimentCard/v1 contract. A
caller that performs admission must use `validate_bundled_document()` rather
than invoking the underlying `jsonschema` validator alone. Changing an
admission semantic incompatibly requires a new protocol major just as changing
the bundled schema does.

Authenticated editor export is presentation JSON and is deliberately distinct
from RFC 8785 evidence bytes. Schema-resource packaging must be verified from
a built and unpacked or installed wheel before release; source-tree resource
loading alone is insufficient evidence.

### YAML presentation versus value normalization

No post-construction Unicode normalization is applied: NFC/NFD form, case, and
constructed string code points remain significant. YAML presentation rules
still apply before construction. In particular, the YAML parser normalizes
physical line breaks and folds multiline plain, quoted, and block scalars as
specified by YAML 1.2. Authors who need exact source-byte distinctions retain
them in the separately stored source hash.

This paragraph supersedes only the ambiguous newline-normalization wording in
ADR 0006; its remaining Unicode and canonicalization decisions stand.

## Consequences

Malformed depth, recursion, and direct non-JSON inputs now fail through stable
public exceptions. Dependency resolution cannot silently select an untested
`referencing` API. Editors can consume readable authenticated schemas while
admission still enforces semantic identity and reference invariants that Draft
2020-12 cannot encode.

The wheel-content regression remains a required release check even when all
source-tree tests pass.
