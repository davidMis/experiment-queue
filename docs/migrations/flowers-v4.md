# Flowers v4 migration plan

## Extraction provenance

The standalone history was filtered from
`/Users/david/Projects/flowers-3d-helmholtz` at source commit
`0082945b4d2771dcc1ed93de1c55552df5761f72`. Its filtered pre-reorganization
head is `39c29fbc59abe9f71f991a4ced5362024b70a54b`.

The Flowers queue implementation and wrappers remain in place during
development. This repository must prove package, CLI, process, preemption, and
restart parity before those wrappers delegate here.

Production migration and Flowers queue removal are additionally gated on David
confirming that SPECFEM dataset generation and its synchronized evidence
closeout are complete. Local implementation and copied-state rehearsal may
proceed before that gate; live cutover may not.

## Safe state migration

1. Rehearse against a copied v4 state directory. The legacy source may be
   grandfathered inside the Flowers checkout, but the v5 destination must be
   outside every registered checkout, mount, and artifact root.
2. Require the operator to provide the stable project key
   `flowers-3d-helmholtz`; never infer identity from a path or remote.
3. Inventory runner, checkpoint, and metadata paths and approve any authorized
   artifact roots outside the checkout.
4. Seed the legacy project revision and rebuild queue-item uniqueness as
   `(project_id, experiment_id, attempt)` while preserving IDs.
5. Validate SQLite integrity and foreign keys, row and event counts, Git refs,
   worktrees, active process identity, and continuation digests.
6. Run a fresh-state two-project smoke before production cutover.
7. After the SPECFEM gate, require no item in `starting`, `running`, `yielding`,
   `terminating`, or `force_killing`; pending and held items may migrate. Stop
   the legacy scheduler and web writers, create a consistent SQLite backup, and
   migrate a copy offline. Historical PID/process metadata is verified as
   provenance and is not treated as a live process to recover.
8. Start only the standalone service. Keep the old code and original database
   immutable for rollback.

No development or migration command in this repository may inspect or mutate a
scientific project's remote host directly.
