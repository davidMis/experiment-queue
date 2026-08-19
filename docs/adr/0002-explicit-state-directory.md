# ADR 0002: Explicit operator-configured state directory

Status: accepted, 2026-08-19.

Every stateful command resolves its queue state directory in this order:

1. `--state-dir /absolute/path`;
2. `EXPERIMENT_QUEUE_STATE_DIR`;
3. an actionable error.

There is no working-directory, project-relative, or platform-default fallback.
Paths expand `~`, must then be absolute, and are canonicalized before use. This
prevents a command run from the wrong checkout from silently opening or creating
another queue.

The state directory is operator data on a local filesystem suitable for SQLite.
It will hold the database, instance identity, scheduler lease, internal
worktrees, service logs, and control receipts. Scientific artifacts remain in
project-configured locations. Project registration will reject state paths
inside a project checkout, mount, or artifact root.
