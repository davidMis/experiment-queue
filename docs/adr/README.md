# Architecture decision record index

This directory contains the accepted architectural decisions for
`experiment-queue`. Accepted ADRs are immutable: a later decision supersedes an
earlier one with a new ADR rather than rewriting its rationale. Runtime support
and compatibility evidence are indexed separately in the
[protocol compatibility matrix](../protocol-compatibility.md).

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-product-and-project-identity.md) | Accepted, 2026-08-19 | Product naming and immutable project-key grammar |
| [0002](0002-explicit-state-directory.md) | Accepted, 2026-08-19 | Explicit operator-selected state directory |
| [0003](0003-versioned-project-and-card-schemas.md) | Accepted, 2026-08-19; implementation pending | Strict versioned Project and ExperimentCard documents |
| [0004](0004-legacy-compatibility-and-cutover.md) | Accepted, 2026-08-19 | Legacy-card compatibility and offline, copy-only migration |
| [0005](0005-platform-and-trust-boundary.md) | Accepted, 2026-08-19 | Linux/NVIDIA production scope and trusted-code boundary |
| [0006](0006-strict-yaml-schema-and-canonical-json.md) | Accepted, 2026-08-27 | Strict YAML subset, Draft 2020-12 schemas, and canonical JSON |
| [0007](0007-bounded-schema-processing-and-referencing.md) | Accepted, 2026-08-27 | Bounded schema processing, semantic validation, and direct `referencing` ownership |
| [0008](0008-typed-authoring-extensions-and-admission-snapshots.md) | Accepted, 2026-08-28 | Typed immutable authoring, project extensions, mutable Submission, and admission evidence |

New records use the next four-digit number. Their title, status, and supersedes
relationship must be added here when the record is accepted.
