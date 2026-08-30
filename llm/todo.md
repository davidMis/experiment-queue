# TODO

This file contains forward-looking work only. Current phase, blockers,
ownership, and risks live in `llm/status.md`; completed implementation is
summarized in `llm/log.md` and the durable documents.

Each item has a stable ID, owner, dependencies, and completion criterion.

## Release Candidate Verification And Publication

- [ ] **EQ-RELEASE-003 — David with Codex support:** publish and verify tag
  `v0.2.0` from the approved release commit, then retain its exact tag-built
  wheel digest and changelog. Depends on the pushed corrective commit and
  successful Linux CI recorded in `llm/status.md` and `llm/log.md`. Complete
  when the cutover checklist names an immutable, remotely verified release
  artifact rather than an untagged checkout.

## Flowers Offline Inventory And Cutover

- [ ] **EQ-CUTOVER-001 — David:** provide an offline inventory of the Flowers
  source database, runnable/historical cards, refs/worktrees, external artifact
  and scratch paths, continuation evidence, legacy service units, and automatic
  restart paths. Depends on no repository access by Codex. Complete when every
  live-path/card disposition required by the migration guide is explicit in
  operator-supplied evidence.
- [ ] **EQ-CUTOVER-002 — David with Codex instructions:** prove the legacy queue
  idle, stop and disable all legacy database writers and automatic restarts,
  create a consistent backup and writer-free source copy, and retain old
  code/state read-only. Depends on `EQ-CUTOVER-001` and `EQ-RELEASE-003`.
  Complete when database identity, backup/copy hashes, idle state, stopped
  writers, disabled restart paths, and rollback locations are recorded.
- [ ] **EQ-CUTOVER-003 — Codex using only operator-supplied copies:** run the
  exact dry-run/build/verify sequence with project key
  `flowers-3d-helmholtz`, compare inventories and strict migration receipts,
  and leave the source untouched. Depends on `EQ-CUTOVER-002`. Complete when
  both receipts validate exact source/destination instance identities, counts,
  events, refs/worktrees, process evidence, paths, and continuations with no
  unexplained difference.
- [ ] **EQ-CUTOVER-004 — David:** explicitly authorize production cutover after
  reviewing the release and migration evidence. Depends on `EQ-CUTOVER-003`.
  Complete only when authorization is recorded; SPECFEM completion, evidence
  closeout, and the dress-rehearsal waiver do not substitute for this gate.
- [ ] **EQ-CUTOVER-005 — David with Codex handoff:** activate exactly one
  schema-v5 scheduler/web deployment and run the approved operational smoke.
  Depends on `EQ-CUTOVER-004`. Complete when service exclusivity, dispatch,
  recovery, logs, receipts, GPU controls, priority, manual yield, termination,
  and rollback readiness are recorded.

## Compatibility Deprecation

- [ ] **EQ-DEPRECATE-001 — Codex:** observe the standalone deployment and ship
  at least one release that announces legacy-admission deprecation. Depends on
  `EQ-CUTOVER-005` and the accepted release policy. Complete when the support
  window and release evidence meet `docs/release-policy.md`.
- [ ] **EQ-DEPRECATE-002 — David with Codex verification:** remove duplicated
  Flowers queue implementation only after proving no active state, card,
  receipt, import, or provenance dependency. Depends on `EQ-DEPRECATE-001`.
  Complete when Flowers retains historical scientific evidence but no longer
  depends on the deprecated product code.
- [ ] **EQ-DEPRECATE-003 — Codex with David approval:** retire
  `LegacyMarkdownCard/v0` only after every supported project is migrated and
  the compatibility policy permits removal. Depends on `EQ-DEPRECATE-002`.
  Complete when no supported card/state requires v0 and the migration records
  remain usable.

## Explicitly Deferred

These are not active migration TODOs: named multi-team principals, gang
scheduling and DDP checkpoint coordination, non-NVIDIA accelerators,
distributed queue instances, containerization as a security sandbox, automatic
priority-triggered preemption, and a general workflow DAG/matrix engine.
