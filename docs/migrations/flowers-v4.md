# Flowers v4 migration plan

## Extraction provenance

The standalone history was filtered from
`/Users/david/Projects/flowers-3d-helmholtz` at source commit
`0082945b4d2771dcc1ed93de1c55552df5761f72`. Its filtered pre-reorganization
head is `39c29fbc59abe9f71f991a4ced5362024b70a54b`.

The Flowers queue implementation and wrappers remain in place during
development. This repository must prove package, CLI, process, preemption, and
restart parity before those wrappers delegate here.

David confirmed on 2026-08-27 that SPECFEM dataset generation and its
synchronized evidence closeout are complete. The remaining production gates
are implementation readiness, an idle writer-free legacy queue, a consistent
backup/copy, the external-path inventory, and explicit cutover authorization.

## Safe state migration

1. Validate every supported v4 shape against comprehensive copied fixtures; a
   distinct dress rehearsal against production state is not required. At
   cutover, always migrate an offline copy—never the source. The legacy source
   may be grandfathered inside the Flowers checkout, but the v5 destination
   must be outside every registered checkout, mount, and artifact root.
2. Require the operator to provide the stable project key
   `flowers-3d-helmholtz`; never infer identity from a path or remote.
3. Inventory runner, checkpoint, and metadata paths and approve any authorized
   artifact roots outside the checkout.
4. Seed the legacy project revision and rebuild queue-item uniqueness as
   `(project_id, experiment_id, attempt)` while preserving IDs.
5. Validate SQLite integrity and foreign keys, row and event counts, Git refs,
   worktrees, active process identity, and continuation digests.
6. Run a fresh-state two-project smoke before production cutover.
7. At cutover, require no item in `starting`, `running`, `yielding`,
   `terminating`, or `force_killing`; pending and held items may migrate. Stop
   the legacy scheduler and web writers, create a consistent SQLite backup, and
   migrate a copy offline. Historical PID/process metadata is verified as
   provenance and is not treated as a live process to recover.
8. Start only the standalone service. Keep the old code and original database
   immutable for rollback.

If the importer or standalone runtime fails, stop its writers, retain the
failure receipt, fix and verify the standalone code against fixtures, and retry
from a fresh copy of the untouched v4 source. Do not repair production history
in place merely because the queue currently has one operator.

No development or migration command in this repository may inspect or mutate a
scientific project's remote host directly.
