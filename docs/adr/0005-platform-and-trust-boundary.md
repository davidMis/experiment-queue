# ADR 0005: Platform and trust boundary

Status: accepted, 2026-08-19.

Production version-1 support is Linux, CPython 3.14+, SQLite, Git worktrees,
POSIX processes and PTYs, and NVIDIA GPUs discoverable through `nvidia-smi`.
macOS supports development, validation, migration rehearsal, and unit tests.
Windows, non-NVIDIA accelerators, distributed schedulers, container isolation,
and gang/DDP preemption are out of scope.

Scientific commands may use any declared project environment. The queue has its
own environment and does not import project dependencies.

Registering a project authorizes committed project code to execute as the queue
service account. The queue is not a sandbox. Deploy with a dedicated non-root
account, private state, least privilege, authenticated network access, and no
credentials in cards. Path and role checks prevent mistakes and unauthorized
web actions; they do not contain malicious submitted code.
