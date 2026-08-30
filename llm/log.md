# Log

This is the concise chronological record of substantive repository sessions.
Live implementation state belongs in `llm/status.md`, and future actions belong
in `llm/todo.md`. Older entries may be moved verbatim to
`llm/session_logs/`.

## 2026-08-19 - Establish Standalone Repository

Goal: extract the experiment runner, queue, and web application from
`flowers-3d-helmholtz` into an independently maintained project without
changing the live production queue.

Result:

- Filtered the relevant history from Flowers source commit
  `0082945b4d2771dcc1ed93de1c55552df5761f72`; the filtered pre-reorganization
  head is `39c29fbc59abe9f71f991a4ced5362024b70a54b`.
- Created `/Users/david/Projects/experiment-queue`, reorganized the code under
  `src/experiment_queue/`, added Python `>=3.14` packaging and the
  `experiment-queue`, `experiment-queue-web`, and `run-experiment` entry points.
- Kept the service standard-library-only and left Flowers-specific scientific
  integration tests in the Flowers repository.
- Required an explicit absolute state directory through `--state-dir` or
  `EXPERIMENT_QUEUE_STATE_DIR` and added the accepted project-key validator.
- Recorded initial architecture, schema, compatibility, platform, trust,
  migration, and onboarding decisions. Committed the baseline as `09dbe41`.
- Verified the package in its own Python 3.14.4 `.venv`: `82` tests and `22`
  subtests pass; all three installed CLI help paths pass; missing state fails
  safely; Git history and both worktrees were clean.

Open:

- The executable remains a single-project schema-v4 compatibility baseline.
- Multi-project implementation and any production migration remain pending.

## 2026-08-19 - Consolidate Plans And Deprecation Ownership

Goal: make this repository the sole home for queue-product planning and Codex
state while clearly retaining the Flowers implementation only for current
SPECFEM production.

Result:

- Added `llm/status.md`, `llm/todo.md`, and `llm/log.md` with explicit ownership
  and session-maintenance rules in `AGENTS.md`.
- Consolidated the phased implementation, acceptance gates, risk controls,
  onboarding work, and Flowers cutover dependency in
  `docs/implementation-plan.md` and the live TODO.
- Recorded that production cutover and legacy removal cannot begin until David
  confirms SPECFEM data generation and evidence closeout are complete.
- Updated Flowers live guidance separately to deprecate new in-repo queue
  development while leaving current scientific production actions and
  historical records intact.

Verification:

- The standalone suite remains green: `82` tests and `22` subtests passed in
  `9.39 s`.
- All `68` live TODO IDs are unique, every dependency reference resolves, every
  task has a completion criterion, all local Markdown links resolve, and both
  repositories pass `git diff --check`.
- Flowers source tests were not rerun because its changes are documentation and
  live-ledger ownership only; no source, wrapper, card, or test changed.

Open:

- Protocol/schema foundations and database v5 are the next code milestones.
- A real production state copy and publication remote have not been supplied;
  neither blocks local implementation.

## 2026-08-27 - Begin Cutover Foundation Implementation

Goal: begin the recorded standalone cutover work after David confirmed the
Flowers scientific gate was complete, without touching live Flowers state or
`mutton2`.

Decisions:

- David explicitly confirmed SPECFEM dataset generation and synchronized
  evidence closeout are complete and authorized work toward cutover.
- David waived a separate production-state dress rehearsal for this
  single-operator queue. Fixture/importer verification and a fresh two-project
  smoke remain mandatory; cutover still migrates only an offline copy with a
  dry run, machine receipt, verification, and untouched v4 rollback source.
- Accepted ADR 0006 for strict YAML/Draft 2020-12/RFC 8785 behavior and ADR
  0007 for bounded trees, direct `referencing` ownership, semantic validation,
  and precise YAML presentation semantics.
- Named the extracted schema-v4 yield request/receipt shapes v0 compatibility;
  typed cooperative-yield v1 remains a distinct wire protocol.

Result:

- Completed `EQ-GOV-003`, `EQ-PROTO-001` through `EQ-PROTO-005`, and
  `EQ-SCHEMA-001` through `EQ-SCHEMA-003`; removed them from the live TODO.
- Added the independent protocol registry, identity fixtures, ADR index, and
  compatibility/ownership matrix.
- Added atomic RunnerManifest/v1 and RunnerReceipt/v1 emission/ingestion,
  restart recovery, signal-exit normalization, strict path authorization, and
  an exact RunnerReceipt/v0 stdout fallback for legacy jobs.
- Added bounded strict YAML, RFC 8785 canonical JSON, digest-authenticated
  Project/v1 and ExperimentCard/v1 schemas, version-owned semantic checks,
  editor export, and installed-wheel schema verification. Runtime dependencies
  are explicitly bounded in `pyproject.toml`.
- Added the dependency-light CooperativeYield/v1 request, receipt, helper, and
  conformance APIs with strict interoperable JSON, typed progress, opaque
  resume bytes, exact admitted checkpoint-name checks, path-bound stable file
  hashing, and continuation identity over spec/revision/Git/run/prior receipt.
- Updated the runner guide, README, implementation plan, migration guide,
  status, and TODO for the completed scientific gate and the no-separate-dress-
  rehearsal decision.

Verification:

- Full Python 3.14.4 suite: `277 passed, 26 subtests passed` in `10.77 s`.
- Final wheel build succeeded; isolated wheel import authenticated both schema
  resources, pinned canonical digests, and editor exports.
- Independent adversarial review reported no remaining actionable code or
  documentation findings; `git diff --check` passed.
- No Flowers source/card/wrapper/live-state file changed, no GPU allowlist or
  production state was used, and no connection to `mutton2` was attempted.

Open:

- The executable remains schema-v4/single-project. `EQ-PROTO-006` stays open
  until typed continuation evidence is wired into scheduler holds and failure
  isolation.
- Typed Project/ExperimentCard models, extensions, Submission/admission
  snapshots, the Project lifecycle ADR, schema v5, offline importer, and
  two-project isolation are next.
- The publication remote is unset. A non-blocking setuptools license-metadata
  deprecation is tracked as `EQ-GOV-005` before its 2027 deadline.

## 2026-08-28 - Implement Typed Authoring And Admission Evidence

Goal: complete the storage-neutral Project/v1, ExperimentCard/v1, extension,
Submission, and admission-snapshot foundation before database-v5 work, without
touching live Flowers state.

Decisions:

- Accepted ADR 0008: validated authoring models are deeply immutable; mutable
  scheduling policy lives in Submission; bindings replace only complete
  declared parameter values; and one offline project-owned schema validates the
  `extensions.<project-key>` envelope.
- Admission compilation is pure. A future trusted ProjectRevision/Git resolver
  must supply exact named blobs from the pinned full commit before persistence.
  Compiler provenance is always read from installed package metadata.

Result:

- Completed `EQ-SCHEMA-004` through `EQ-SCHEMA-008` and removed them from the
  live TODO.
- Added typed Project/card/nested command-resource-artifact models, strict
  cross-document references, service-owned environment protection, placeholder
  rejection, and exact JSON-native round trips.
- Added strict offline Draft 2020-12 extension-schema validation with canonical
  digest authentication, dormant-reference preflight, recursion handling, and
  exact source/canonical evidence.
- Added mutable Submission input and factory-only immutable policy/schema/
  snapshot evidence with detached inputs, selected-job resolution, bounded
  whole-value bindings, full Git identity, and stable resolved execution
  digests. Later caller mutations cannot alter snapshots.
- Hardened wheel verification to require the new authoring modules and recorded
  `EQ-PROJECT-009` as the Git-tree resolver gate before database admission.

Verification:

- Full Python 3.14 suite: `409 passed, 26 subtests passed` in `15.26 s`.
- Focused authoring/extension/admission suite: `132 passed` in `3.72 s`;
  `py_compile` and `git diff --check` passed.
- A fresh wheel built successfully and isolated verification confirmed the
  authoring/admission modules, compiler provenance against wheel metadata, both
  bundled schemas, pinned canonical digests, and editor exports. The known
  setuptools license warning remains tracked as `EQ-GOV-005`.
- No Flowers source/card/wrapper/live-state file changed, no GPU allowlist or
  production state was used, and no connection to `mutton2` was attempted.

Open:

- The executable remains schema-v4/single-project; typed snapshots are not yet
  persisted or executed.
- The Project lifecycle ADR/models, trusted Git resolver, schema-v5 storage,
  typed-yield scheduler integration, offline importer, and two-project
  isolation remain next.

## 2026-08-28 - Assemble Schema-v5 Migration-readiness Candidate

Goal: complete and adversarially harden the standalone schema-v5 queue through
the point where only release verification/publication and operator-controlled
offline Flowers cutover gates remain.

Decisions:

- David confirmed SPECFEM generation and synchronized evidence closeout are
  complete. He waived a separate production-state dress rehearsal for this
  single-operator queue, but did not waive the writer-free copy, offline
  inventory, exact receipt, release, or explicit cutover-authorization gates.
- Schema v5 is the primary standalone interface. Schema-v4 databases and
  `LegacyMarkdownCard/v0` remain bounded import/execution compatibility
  protocols rather than the target authoring API.
- Durable control senders use an explicit at-least-once crash-recovery
  contract. Each executor coalesces the scientific process-group broadcast to
  at most one `SIGINT` and one `SIGTERM` per segment; manual yield and initial
  termination therefore coalesce when both request `SIGINT`.
- Ambiguous launches, exits, process identity, cleanup, and GPU telemetry fail
  closed with assignments retained, host dispatch paused, and the Project
  quarantined until authenticated recovery or the guarded operator command.
- Production exclusion still requires the legacy writers and automatic
  restarts disabled plus exactly one v5 service. Scheduler-owned flocks cannot
  provide continuous cross-version exclusion across a scheduler crash.

Result:

- Implemented schema-v5 Project/Enrollment/Revision lifecycle, canonical Git
  admission, database instance identity, project-aware queue/events, typed
  reservations, manual preemption, termination/force-kill, logical mounts,
  artifact roots, CLI/web operations, and offline v1-v4 migration with strict
  receipts.
- Added destination-owned legacy runtime reconstruction at the pinned old
  commit without mutating historical resources. Cleanup refuses ignored,
  untracked, shared-compatibility, or otherwise uncertain content.
- Hardened durable execution: isolated executor bootstrap, authenticated launch
  sidecars, immutable no-clobber launch/exit evidence, staging-residue recovery
  confirmation, full-group graceful signaling, descendant drain, crash-safe
  manual-yield replay, guarded abandoned-attempt reconciliation, and
  telemetry-gated GPU lease release.
- Made `serve --once` recovery-only, made termination refuse the pre-launch
  `starting` state, secured web authentication file loading, made auth rotation
  explicitly stop/setup/restart, and added actionable IPv4/IPv6 bind handling.
- Prevented `run-experiment` from adding `SIGTERM` after an executor process-
  group `SIGINT`, or duplicating a group `SIGTERM`, in both PTY and pipe modes;
  queue-group cleanup preserves the child's actual outcome (including
  cooperative exit `75`), while standalone PID-only cleanup retains its child
  escalation and interruption result.
- Added two-project CLI/web/preemption/restart integration, compatibility and
  migration fixtures, typed onboarding examples, durable architecture and
  operator guidance, exact Flowers cutover/rollback procedure, changelog, and
  release/deprecation/support policy.
- Closed the final adversarial findings around factory-authenticated execution
  plans, exact Git blob authentication/materialization, partial-clone and
  checkout-filter rejection, process-group launch uncertainty, private
  worktree/attempt-directory boundaries, and byte-preserving POSIX worktree
  registry parsing.
- Confirmed `origin` is
  `https://github.com/davidMis/experiment-queue.git`; local `main` and
  `origin/main` both name
  `353cbfeb2e264fcc83a87d9b8f8034d20a84fc30` before this uncommitted change
  set. The Linux CI workflow exists locally but remains untracked/unpublished.
- No Flowers source/state/card inventory was inspected or changed, and no
  connection to `mutton2` was attempted. Live classification remains dependent
  on David's operator-supplied offline inventory.

Verification:

- Focused executor suite: `29 passed, 1 skipped`.
- Focused scheduler-service suite: `59 passed` within the final combined
  process-control run.
- Focused CLI/web suite: `35 passed`.
- Focused controller/runtime suite: `39 passed`.
- Combined focused controller/runtime/executor/CLI/web suite:
  `103 passed, 1 skipped`.
- Focused runner/executor/attempt/scheduler-service suite after process-group
  signal hardening: `120 passed, 1 skipped, 12 subtests passed`.
- Focused legacy-v0 continuation suite: `7 passed`.
- Final full Python 3.14.4 suite: `1004 passed, 1 skipped, 32 subtests passed`
  in `183.93 s`; exit code `0`, with no warnings or failures.
- Final wheel: `experiment_queue-0.2.0-py3-none-any.whl`, SHA-256
  `0b25ad880f25fea57ebf48d23a7c969dc072deb9f3c39249b590aad061b07683`;
  isolated verification authenticated all runtime modules, six entry-point
  help surfaces, metadata/license identity, and bundled schema resources.
- Final audit: the independent code review and migration-procedure re-audit
  reported no remaining actionable findings; all `85` local Markdown targets
  and `5` local fragment anchors resolve (`9` external URLs were
  inventory-counted but not availability-checked); all `197` argparse options
  have actionable help; compilation and `git diff --check` pass.

Open:

- Commit/push the readiness change set, publish and pass Linux CI, and create
  the approved release commit/tag.
- David must supply the offline Flowers card/path/service inventory, prove the
  source idle and writer-free, stop and disable legacy automatic restarts,
  create the consistent backup/copy, review exact migration receipts, and
  explicitly authorize cutover.

## 2026-08-29 - Repair Linux CI Portability

Goal: diagnose and correct the first pushed Linux CI failure without weakening
the production executor or migration-readiness invariants.

Decisions:

- Retain the executor's fail-closed requirement that its process group equal
  its PID. Production creates that boundary with `start_new_session=True`.
- Treat a bounded, syntactically valid deep JSON array as the same safe
  rejection whether a Python patch release reports a decoder error or parses
  it before the protocol's required-object check.

Result:

- Inspected GitHub Actions run `33255737427`, which ran Ubuntu 24.04 with
  Python 3.14.7 and failed `12` of `1005` tests.
- Updated direct in-process executor unit tests to model the scheduler's real
  private process-group launch without changing the runner's actual group, and
  added an explicit regression proving a mismatched group is still refused.
- Made the two deep-JSON tests accept either decoder rejection or the exact
  protocol-level non-object rejection across Python 3.14 patch releases.
- No production source, schema, protocol, workflow, or migration behavior
  changed.

Verification:

- Shared-process-group reproduction: `97 passed, 1 skipped, 32 subtests passed`
  in `11.72 s`.
- Full Python 3.14.4 suite: `1005 passed, 1 skipped, 32 subtests passed` in
  `205.28 s`; exit code `0`.
- Fresh `experiment_queue-0.2.0-py3-none-any.whl` verification authenticated
  all runtime modules, six entry-point help surfaces, metadata/license identity,
  and bundled schemas; SHA-256
  `a1b84a981e152934943ac99b6b3fa4c263404b35833b352471e6b53b964e2264`.
- Full compilation and `git diff --check` pass.

Open:

- Commit and push this test/documentation correction and require a clean Linux
  CI rerun before creating the release tag.
- The operator-controlled inventory, writer-free copy, receipt review, and
  explicit production cutover authorization remain unchanged.

## 2026-08-29 - Confirm Corrective Linux CI

Goal: verify the pushed correction remotely and advance the release gate only
after every Linux CI step completed successfully.

Result and verification:

- Local `main` and `origin/main` both name corrective commit
  `1a3765f30f59f61a2b17919df9bbb140d8b1368f`.
- GitHub Actions run `33262155259` completed successfully on Ubuntu 24.04 with
  Python 3.14. Checkout, dependency installation, the complete suite, wheel
  construction/verification, and all six installed command-help checks passed.
- Removed completed `EQ-RELEASE-002` from the forward TODO. No queue source,
  protocol, migration procedure, Flowers state, or `mutton2` state changed.

Open:

- Create the approved release commit/tag and retain its immutable wheel digest
  and changelog evidence.
- Obtain the operator-supplied Flowers inventory and writer-free source copy
  before running the exact offline migration procedure.

## 2026-08-29 - Prepare Release 0.2.0

Goal: create the clean reviewed release commit that will own tag `v0.2.0`
after its own Linux CI run succeeds.

Result:

- Updated the `0.2.0` changelog date to the actual release date, 2026-08-29.
- Recorded the successful corrective Linux CI gate and selected immutable
  release identity `v0.2.0`.
- Kept `EQ-RELEASE-003` open through exact tag verification and retention of
  the tag-built wheel digest; a tag alone does not authorize production
  migration or scheduler activation.

Open:

- Push this release commit, require its Linux CI run to pass, then publish and
  verify annotated tag `v0.2.0`.
- Build and retain the exact tag wheel before beginning the writer-free
  production-copy gate.

## 2026-08-29 - Select Fresh Schema-v5 Deployment

Goal: replace the legacy-database migration plan with the minimal fresh-state
startup selected by David.

Decision:

- David confirmed that no queue experiments are running, the legacy scheduler
  and web service are stopped, and the legacy database does not need to be
  migrated.
- The new deployment will initialize an absent schema-v5 database through
  first-Project registration. Legacy inventory, backup/copy, importer, receipt,
  and migrated-item comparison steps are no longer active work.
- David selected the existing cloned GitHub checkout as the production source;
  the retained wheel remains release verification evidence and will not be the
  installation path.
- David selected `/home/sdm11/experiment-queue` for the source clone and
  `/home/sdm11/srv/experiment-queue` for mutable service data. These disjoint
  roots keep the state directory outside the checkout.

Result:

- Verified the published remote annotated tag `v0.2.0`: tag object
  `9fa5860bc47b8b30092eba7d2f2cbacd5c6f443e` dereferences to reviewed release
  commit `9a146a5cfa51125ff13b45cf9211358d2ad2e64e`.
- Built from a temporary detached worktree at that exact tag, passed the full
  wheel verifier, and retained
  `dist/experiment_queue-0.2.0-py3-none-any.whl` with SHA-256
  `5ff574c3201c8f7a50ee2103f9c2c19af0190d3b262ed0dd81831ebad75b12b8`.
- Closed `EQ-RELEASE-003`; the release artifact is ready to install.
- Replaced the five-step Flowers migration/cutover TODO with three fresh
  deployment tasks: prepare the typed Project and Enrollment, initialize state,
  and activate/smoke the services.
- Updated current status and operator-facing documentation so the inactive
  import checklist is no longer presented as a production prerequisite.
- Recorded the supported pinned-checkout startup: a repository-local Python
  3.14 `.venv`, editable installation from `pyproject.toml`, and the thin queue
  and web wrappers under `scripts/`.
- No queue code, Flowers repository, legacy database, or remote service state
  changed.

Verification:

- `git diff --check` passes for the documentation-only change set.
- The retained tagged wheel passes `scripts/verify_wheel.py`; no product code
  changed, so the already successful release CI remains the code verification.

Open:

- Pin and prepare the production clone, then register the committed Flowers
  Project into fresh state, add the GPU, initialize web credentials, start the
  v5 services, and submit one typed card.

## 2026-08-29 - Simplify Trusted-Project Onboarding

Goal: remove unnecessary path and environment setup from the fresh Flowers
startup while retaining useful queue identity and the explicit advanced model.

Decisions:

- Treat registered scientific projects as trusted service-account code. Empty
  volume declarations do not restrict filesystem access, and ordinary jobs do
  not need dataset/output/scratch inventories or artifact declarations.
- Keep scientific environments inside their project roots. The normal workflow
  binds one existing checkout-local `.venv/bin` automatically and requires no
  operator-authored Enrollment file.
- Keep exact committed Project/card admission and pinned worktrees, but do not
  claim absolute reproducibility as the product goal.
- Continue with a fresh schema-v5 database; legacy import remains inactive.

Result:

- Changed the default Project scaffold to `volumes: []`; its generated card has
  no artifacts.
- Made `--enrollment` optional for register and append. Automatic mode requires
  `volumes: []` plus one environment, freezes Enrollment/v1 with no mounts, and
  uses `<checkout>/.venv/bin` by default.
- Added `--environment-bin` for an alternate venv root, bin directory, or
  executable. Relative values resolve beneath the checkout. A symlinked Python
  is normalized before target resolution, fixing the reported uv-style path
  failure.
- Automatic mode authenticates the complete checkout-local `.venv` against the
  pinned commit's `.gitignore`, rather than proving only its `bin` subdirectory.
  Explicit Enrollment remains backward compatible for advanced mounts,
  artifacts, multiple environments, and checkpointing.
- Added accepted ADR 0012 and revised the README, security boundary, architecture,
  implementation plan, onboarding, operator guidance, examples, changelog, and
  current records. Package version is prepared as `0.2.1`.
- Drafted six concise public wiki pages for installation, adding a project,
  daily operations, troubleshooting, navigation, and the wiki home. Publication
  remains pending action-time confirmation and a public code revision that
  contains the documented behavior.
- No Flowers repository, queue state, legacy database, or `mutton2` service was
  inspected or changed.

Verification:

- Independent implementation and documentation reviews found the version-pin,
  whole-venv proof, optional-volume admission, executable normalization, CLI
  exclusivity, environment-policy, relative-output, and command-name issues;
  each applicable finding was corrected.
- Focused CLI/operator/example suite: `48 passed` in `19.62 s`.
- Full Python 3.14.4 suite: `1011 passed, 1 skipped, 32 subtests passed` in
  `182.82 s`.
- `experiment_queue-0.2.1-py3-none-any.whl` passes the complete verifier;
  SHA-256 is
  `92ab165b3b45f29ee0f1a948a43e48baba0d5d6dc0083651a38f8309d6f8f658`.
- Python compilation, `git diff --check`, all `80` local Markdown targets, and
  all links among the six wiki drafts pass.

Open:

- With David's confirmation, publish the reviewed `0.2.1` code/release, require
  successful Linux CI, and publish the public wiki pages.
- Tomorrow, update the `mutton2` source clone and perform the documented fresh
  registration and one-job smoke run without an Enrollment file or importer.
