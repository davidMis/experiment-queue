# ADR 0011: Durable runtime GPU lease and telemetry-gated release

Status: accepted, 2026-08-28.

## Context

A terminal executor leader or terminal queue state does not prove that project
work stopped using its GPU. Scientific work may outlive the leader in another
session, and a scheduler can crash after recording terminal evidence but before
cleanup. Historical `assigned_gpu_uuid` and `assigned_gpu_index` must also
remain intact for provenance and legacy-import comparison, so null assignment
cannot safely represent resource release.

## Decision

Schema v5 separates immutable historical assignment from a scheduler-critical
runtime GPU lease. `runtime_gpu_lease_held` is set by the claim transaction and
blocks every other claim and passive-reservation activation for that GPU.
`runtime_gpu_lease_released_at` records a successful runtime release; assignment
and process fields remain historical evidence.

Active states (`starting`, `running`, `yielding`, `terminating`, and
`force_killing`) must hold a complete assigned lease. A held lease is otherwise
permitted only on `succeeded`, `failed`, `interrupted`, or `force_killed` while
release is pending. Requeue after a validated cooperative continuation clears
the lease in the same guarded state transition and resets the release timestamp
on the next claim. Imported pending and terminal rows start unheld with a null
release timestamp; production import still refuses active legacy rows.

Terminal evidence is committed before ordinary terminal lease release. While
holding the host-wide GPU lock, the scheduler obtains a fresh, finite,
unambiguous telemetry set and requires exactly one record for the assigned UUID,
no compute PIDs, and the configured idle memory/utilization thresholds. It then
clears the lease with a guarded transaction and appends
`GPU_RUNTIME_LEASE_RELEASED` before worktree cleanup or host-lock close. A valid
busy observation retains the lease and retries without imposing a new global
pause. Missing, duplicate, malformed, or unavailable telemetry, or inability
to own the host GPU lock, fails closed and pauses host dispatch.

Restart recovery explicitly reconciles terminal rows whose leases remain held,
including a crash after terminal commit and before release. Orphan worktree
cleanup excludes those rows. Abandoned-launch resolution performs the same
idle-telemetry barrier before terminal transition and release. QueueExport/v1,
operator views, and the web surface expose both lease fields so a terminal item
cannot be mistaken for a reusable GPU.

## Consequences

Terminal state, resource availability, and cleanup are separate durable facts.
A stale or unavailable telemetry source can intentionally retain a GPU until an
operator restores trustworthy observation; historical assignments remain
queryable without blocking reuse once the lease is released.

Telemetry and the database commit are not atomic, so a small time-of-check to
time-of-use interval remains. The host lock excludes cooperating v5 services,
not arbitrary project code or deprecated v4 writers. Project code remains
trusted service-account code rather than sandboxed code; service-manager
exclusion and the migration cutover rules remain mandatory.
