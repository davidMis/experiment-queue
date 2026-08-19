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
