# Status

This is the sole mutable source of the repository's current implementation
phase, blockers, active risks, ownership, and next authorized actions. Durable
architecture belongs in `docs/`, accepted decisions in `docs/adr/`,
forward-looking work in `llm/todo.md`, and chronological evidence in
`llm/log.md`.

Last updated: 2026-08-29 by Codex from David's decisions and local repository
evidence.

## Current Phase

The repository is locally ready for schema-v5 release and offline migration
preparation. The standalone multi-project database, pinned-revision admission,
typed execution,
reservations, manual cooperative preemption, termination, web/CLI control,
offline v1-v4 importer, destination-owned legacy runtime, recovery controls,
examples, compatibility fixtures, packaging, and operator documentation are
implemented locally. Schema v5 is the primary standalone entrypoint;
schema-v4 and `LegacyMarkdownCard/v0` are explicitly bounded compatibility
surfaces.

No production migration has occurred. The readiness candidate and its
test-harness portability correction are committed and pushed, and the complete
Linux CI workflow passes. The approved release identity is `v0.2.0`; release
publication remains incomplete until that tag identifies the reviewed release
commit and the exact tag-built wheel digest is retained. The remaining
operator-supplied offline cutover evidence is also still required. The cutover
procedure is copy-only and receipt-driven; startup never upgrades a database
in place.

## Verified Local Evidence

- Runtime/development version: Python 3.14.4; declared support is `>=3.14`.
- Package entry points: primary `experiment-queue` and `experiment-queue-web`,
  offline `experiment-queue-migrate-v5`, rollback-only
  `experiment-queue-legacy-v4` and `experiment-queue-web-legacy-v4`, and
  scientific runner `run-experiment`.
- Focused process/executor verification: `29 passed, 1 skipped`.
- Focused scheduler-service verification: `59 passed` within the final combined
  process-control run.
- Focused CLI/web verification: `35 passed`.
- Focused controller/runtime verification: `39 passed`.
- Combined focused controller/runtime/executor/CLI/web verification:
  `103 passed, 1 skipped`.
- Focused runner/executor/attempt/scheduler-service verification after
  process-group signal hardening: `120 passed, 1 skipped, 12 subtests passed`.
- Focused legacy-v0 continuation verification: `7 passed`.
- Final full Python 3.14.4 suite after the Linux CI portability correction:
  `1005 passed, 1 skipped, 32 subtests passed` in `205.28 s`; exit code `0`.
- Explicit shared-process-group reproduction of the GitHub runner layout:
  `97 passed, 1 skipped, 32 subtests passed` in `11.72 s`.
- Final wheel:
  `experiment_queue-0.2.0-py3-none-any.whl`, SHA-256
  `a1b84a981e152934943ac99b6b3fa4c263404b35833b352471e6b53b964e2264`;
  isolated verification authenticated all runtime modules, all six entry-point
  help surfaces, metadata/license identity, and bundled schema resources.
- Final audit: independent code review found no remaining actionable issue;
  the migration procedure re-audit resolved all findings; all `85` local
  Markdown targets and `5` local fragment anchors resolve (`9` external URLs
  were inventory-counted but not availability-checked); all `197` argparse
  options have actionable help; Python compilation and `git diff --check` pass.
- `origin` is `https://github.com/davidMis/experiment-queue.git`; local `main`
  and `origin/main` both resolve to corrective commit
  `1a3765f30f59f61a2b17919df9bbb140d8b1368f`.
- GitHub Actions run `33262155259` completed successfully on Ubuntu 24.04 and
  Python 3.14: checkout, dependency installation, the complete suite, wheel
  construction/verification, and all six installed command-help checks passed.
  It supersedes failed run `33255737427`; that failure was confined to portable
  test-harness expectations and required no production source change.

## Accepted Product And Safety Decisions

- Database/v5 stores immutable canonical `database_instance_id` identity;
  exports and successful migration receipts bind source and destination
  database identities exactly.
- Queue items bind an immutable project revision, canonical admission snapshot,
  logical mounts, artifact roots, declared resources, and one independently
  schedulable NVIDIA GPU.
- Priority is global but never authorizes automatic preemption. Manual yield is
  explicit and durable. Signal senders are at-least-once across crash recovery;
  each executor coalesces scientific broadcasts to at most one `SIGINT` and one
  `SIGTERM` per segment.
- Graceful signals reach the complete scientific process group. Terminal
  evidence and GPU release wait for authenticated process-group exit and fresh
  GPU telemetry. Ambiguous attempts remain assigned, host-paused, and
  project-quarantined until guarded operator reconciliation.
- The executor control module starts in isolated Python mode. The scientific
  child still receives its validated environment; imported legacy v0 jobs
  intentionally inherit the minimal, non-secret service environment for v4
  compatibility.
- Immutable launch/exit receipts use no-clobber durable publication. Recovery
  confirms same-inode staging residue by fsyncing the directory before trust,
  and fails closed on staging-only or mismatched evidence.
- Project cleanup never deletes scientific artifacts. Runtime-worktree cleanup
  refuses any ignored, untracked, or shared compatibility content.
- `serve --once` is reconciliation/recovery-only and never dispatches new work.
- Version 1 retains host-admin/reservation authorization. Named multi-team
  principals and gang/DDP scheduling remain future designs, not migration
  prerequisites.

## Ownership And Operational Boundary

- This repository owns generic queue, runner, web, schema, migration,
  compatibility, release, and onboarding development.
- `flowers-3d-helmholtz` owns its scientific cards, historical evidence, and
  legacy operational state. Live Flowers card classification must come from an
  operator-supplied offline inventory; Codex has not inspected Flowers state.
- David is the sole operator of `mutton2`. Codex does not connect to, inspect,
  launch on, monitor, or synchronize with that machine.
- David owns remote service control, GPU allowlists, credentials, production
  backup/copy creation, external-path inventory, and final cutover
  authorization. Codex supplies reproducible commands and evaluates only local
  or operator-supplied artifacts.

## Satisfied Gates

- David confirmed that SPECFEM dataset generation is complete.
- David confirmed synchronized scientific evidence closeout is complete.
- David waived a separate production-state dress rehearsal because this is a
  single-operator queue. Copied fixtures, local two-project integration, exact
  migration receipts, and the production offline checks remain required.
- The publication remote is configured.
- Authoritative local tests, wheel construction/verification, documentation
  and CLI audits, and independent adversarial review are complete.
- The corrective candidate is pushed and the complete Linux Python 3.14 CI
  workflow passes.

## Open Publication And Operator Gates

- Publish and verify tag `v0.2.0` from the reviewed release commit, then retain
  the exact tag-built wheel digest and changelog evidence.
- Obtain an operator-supplied offline Flowers database/card/external-path
  inventory. Do not infer live classification from this repository.
- At cutover, prove the legacy queue is idle, stop and disable every legacy
  database writer and automatic restart path, create a consistent backup and
  writer-free source copy, and retain the source/code read-only for rollback.
- Run dry-run and build migration against copies, validate exact receipts and
  inventories, and obtain David's explicit cutover authorization before
  starting exactly one schema-v5 scheduler/web deployment.

## Active Risks

- The readiness candidate is published and Linux CI passes, but release
  publication remains incomplete until tag `v0.2.0` and its exact immutable
  wheel artifact are both retained.
- Scheduler-owned GPU flocks are not continuous across a scheduler crash while
  a durable executor survives. Restart reconciliation fails closed if it cannot
  reacquire the lock, but cross-version exclusion still relies on stopping and
  disabling legacy services and running exactly one v5 service under an
  authoritative service manager.
- Live external paths, historical card classifications, and source-writer state
  cannot be established without David's offline inventory and cutover checks.
- macOS is a development, migration-rehearsal, and unit-test platform only;
  production GPU dispatch and process telemetry require the documented Linux
  environment.
- Any ambiguous process, receipt, cleanup, or GPU-telemetry state deliberately
  pauses/quarantines instead of guessing. Operators must use the guarded
  recovery commands and preserve scientific evidence.

## Next Authorized Actions

1. Publish and verify tag `v0.2.0` from the reviewed release commit, then retain
   the release wheel digest and changelog evidence.
2. Collect the operator-supplied offline inventory and writer-free source copy,
   then execute the exact dry-run/build/verify checklist in
   `docs/migrations/flowers-v4.md`.
3. Await David's explicit cutover authorization. Do not start migration against
   production state or activate the production scheduler/web deployment before
   it. The migration guide's bounded foreground web-only review against the
   destination copy remains an authorized pre-cutover verification step.

## Explicitly Out Of Scope

- Scientific experiment status or result interpretation.
- Automatic priority-triggered preemption.
- Gang scheduling and coordinated DDP/multi-GPU preemption.
- Non-NVIDIA accelerators, distributed queue instances, and containerization as
  a security sandbox.
- Named multi-team principals and a general workflow DAG/matrix engine before a
  separately accepted design.
