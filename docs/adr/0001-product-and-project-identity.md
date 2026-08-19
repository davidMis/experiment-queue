# ADR 0001: Product and project identity

Status: accepted, 2026-08-19.

The repository and Python distribution are named `experiment-queue`; the import
package is `experiment_queue`, and the primary executable is
`experiment-queue`. Python `>=3.14` matches the originating project.

A project key is explicit, immutable, and unique within one queue instance. It
is independent of display name, directory name, package name, and Git remote.
The accepted grammar is:

```text
^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$
```

Keys contain at most 63 ASCII characters. This keeps them safe in CLI
references, URLs, database indexes, worktree paths, and Git-ref components.
`flowers-3d-helmholtz` is the initial project key.
