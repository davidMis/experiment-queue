# ADR 0003: Versioned YAML project and experiment schemas

Status: accepted, 2026-08-19; implementation pending.

Portable project manifests and experiment cards will use a strict YAML 1.2
subset validated by bundled JSON Schema Draft 2020-12 schemas. Documents carry
independent identities such as:

```yaml
apiVersion: experiment-queue/v1
kind: Project
```

Core schemas reject unknown properties. Flexible scientific data belongs only
under a namespaced `extensions.<project-key>` object and may be validated by a
project-supplied schema. Parsed values must be JSON-native; duplicate keys,
custom tags, merge keys, aliases, and non-finite numbers are rejected.

Admission stores original bytes and SHA-256, validated canonical JSON, schema
identity and digest, and the `experiment-queue` version. Prefer argv commands;
shell text is an explicit compatibility escape hatch. Priority, holds,
dependencies, bindings, device identity, and manual-preemption requests belong
to mutable submissions rather than immutable cards.

Database, project, card, runner-manifest, runner-receipt, export, and
cooperative-yield versions evolve independently.
