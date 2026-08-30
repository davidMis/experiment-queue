# Flowers schema-v4 to schema-v5 cutover and rollback

> **Inactive for the current Flowers deployment.** David selected a fresh
> schema-v5 database on 2026-08-29 because no legacy jobs are running, the
> legacy services are stopped, and the legacy database does not need to be
> imported. Do not run this procedure for the fresh deployment. It remains as
> the exact operator procedure only if legacy import is deliberately revived.

This is the operator checklist for the one production migration from the
deprecated Flowers queue. It does not authorize Codex or this repository to
inspect or change the Flowers checkout, live state, or `mutton2`. All live
paths, service names, card classifications, and external-path dispositions must
come from David through an operator-supplied offline inventory.

## Prerequisites if legacy import is revived

David confirmed on 2026-08-27 that SPECFEM dataset generation and synchronized
scientific evidence closeout are complete. That scientific gate is satisfied.
Because the queue currently has one operator, David waived a separate dress
rehearsal against production state. Exhaustive v1-v4 fixtures, importer refusal
cases, and a fresh two-project smoke remain required evidence.

The following production gates remain open until the cutover session:

1. David supplies and approves the offline external-path and live-card
   inventory described below.
2. Every item is out of `starting`, `running`, `yielding`, `terminating`,
   and `force_killing`.
3. Both legacy database writers are stopped and the old/new schedulers cannot
   share the GPU pool.
4. A complete raw backup and a sidecar-free consistent migration copy exist.
5. Dry-run and real QueueMigrationReceipt/v1 documents succeed and their
   comparisons are reviewed.
6. David explicitly authorizes cutover after reviewing those artifacts.

Pending, held, blocked, and terminal items may migrate. The importer rejects a
running-like state, unresolved SQLite sidecar, changed source, inconsistent Git
or card evidence, corrupt continuation evidence, existing destination, or
overlapping protected root.

Imported pending and terminal rows start with `runtime_gpu_lease_held = 0` and
a null `runtime_gpu_lease_released_at`. Any complete historical
`assigned_gpu_uuid`/`assigned_gpu_index` and PID/process metadata remain exact
v4 evidence; they are not erased or reinterpreted as a live lease. Fresh v5
claims set the new lease, and only cooperative requeue or current-idle telemetry
can clear it.

## Provenance and immutable rollback source

The standalone history was filtered from the Flowers source commit
`0082945b4d2771dcc1ed93de1c55552df5761f72`; the filtered
pre-reorganization head is
`39c29fbc59abe9f71f991a4ced5362024b70a54b`.

Schema v5 never upgrades the live database and never downgrades a destination.
The importer opens the supplied source copy with immutable/read-only SQLite,
builds a temporary sibling destination, verifies it, and atomically publishes
only on success. Rollback always reopens the untouched v4 state with the
explicit legacy-v4 package entry point.

## Operator-supplied offline inventory

Create an inventory file outside both state trees. For every absolute path,
record its canonical path, owner, access expectation, referring item IDs, and
cutover disposition. At minimum include:

- live v4 state, Flowers checkout, environment, data/input mounts, output and
  artifact roots, external scratch, and synchronization destinations;
- every recorded legacy worktree, Git ref, runner run directory, stdout/stderr,
  manifest/receipt, checkpoint, and checkpoint-metadata path;
- GPU allowlist and reservation expectations; and
- the proposed external v5 state, Enrollment, backup, receipt, and TLS paths.

No proposed v5 state path may equal, contain, or be contained by a checkout,
mount, artifact root, environment root, source copy, raw backup, or other
protected legacy root. Cross-Project roots must also be disjoint.

Classify each operator-supplied live Flowers Markdown card using exact offline
evidence, not the representative fixture:

| Evidence | Required disposition |
| --- | --- |
| card path, SHA-256, recorded full commit, referring item IDs/states | Preserve as immutable LegacyMarkdownCard/v0 history |
| current/future scientific use | Convert to a committed ExperimentCard/v1 or explicitly retire |
| pending grandfathered item | Record whether it will drain under v4 or run from its pinned commit in a destination-owned v5 runtime worktree |
| preemptible pending legacy item | Drain/finish under v4 or explicitly retain its already-admitted exact v0 cooperative-yield behavior; import never upgrades it to typed v1 |
| worktree/ref and external artifact/checkpoint paths | Verify exact identity and preserve; never replace historical columns |

Every card must have one explicit classification: `historical-only`,
`pending-grandfathered`, `typed-replacement`, or `retired`. A card may have
both historical preservation and a separate typed replacement. Never rewrite
an immutable historical card or fabricate Project/v1/admission evidence for it.

The local
[`examples/flowers-compatibility`](../../examples/flowers-compatibility)
fixture covers representative simple, tracker-aware cooperative, and
independently schedulable SPECFEM-shaped jobs. It is test material, not a live
card/path inventory and was created without inspecting Flowers or `mutton2`.

## Cutover variables

Before running commands, substitute and record canonical absolute paths. Keep
the cutover workspace outside the Flowers checkout, live state, scientific
mounts, and proposed v5 state.

```bash
set -euo pipefail
export LEGACY_STATE=/absolute/path/to/live-flowers-queue-state
export FLOWERS_CHECKOUT=/absolute/path/to/flowers-3d-helmholtz
export CUTOVER_ROOT=/absolute/path/to/cutover-evidence
export RAW_BACKUP=/absolute/path/to/cutover-evidence/flowers-v4-raw
export MIGRATION_COPY=/absolute/path/to/cutover-evidence/flowers-v4-import-copy
export V5_STATE=/absolute/path/to/new-experiment-queue-state
export DRY_RECEIPT=/absolute/path/to/cutover-evidence/dry-run-receipt.json
export IMPORT_RECEIPT=/absolute/path/to/cutover-evidence/import-receipt.json
export COPY_IDENTITY=/absolute/path/to/cutover-evidence/flowers-v4-import-copy.sha256
export COPY_RECHECK=/absolute/path/to/cutover-evidence/flowers-v4-import-copy.recheck.sha256
export V5_TLS_CERT=/absolute/path/to/private-web-certificate.pem
export V5_TLS_KEY=/absolute/path/to/private-web-key.pem

require_no_open_files() {
  local inspected_path="$1" probe_dir status
  probe_dir=$(mktemp -d)
  if lsof +D "$inspected_path" >"$probe_dir/out" 2>"$probe_dir/err"; then
    status=0
  else
    status=$?
  fi
  if [ "$status" -eq 0 ] || [ "$status" -ne 1 ] || \
     [ -s "$probe_dir/out" ] || [ -s "$probe_dir/err" ]; then
    cat "$probe_dir/out" "$probe_dir/err" >&2
    rm -f "$probe_dir/out" "$probe_dir/err"
    rmdir "$probe_dir"
    echo "open-file inspection failed or found a handle under $inspected_path" >&2
    return 1
  fi
  rm -f "$probe_dir/out" "$probe_dir/err"
  rmdir "$probe_dir"
}
```

Review every expanded value as a canonical absolute path. `RAW_BACKUP`,
`MIGRATION_COPY`, `V5_STATE`, both receipt files, and both copy-identity files
must not exist before their creation step. The TLS files must already exist and
belong to the approved private web deployment.

## 1. Drain and stop every legacy writer

1. Stop admitting work.
2. Let or explicitly terminate every running-like item; do not migrate active
   process state.
3. Stop the legacy scheduler and legacy web/database writer through their
   recorded service-manager units.
4. Confirm both units are stopped, no automatic restart is enabled for the
   cutover window, and no other process has the database open.
5. Record the stop commands, service status, time, package commit, Flowers
   commit, and GPU pool ownership in the cutover record.

The site-specific service names are inventory inputs; do not guess them. As an
additional check after the service manager reports both writers stopped:

```bash
require_no_open_files "$LEGACY_STATE"
```

Any writer/file handle is a stop condition. Do not start a v5 scheduler yet.
The queue's host GPU locks are owned only for the lifetime of a v5 service
process and do not exclude deprecated v4 across a crash. The stopped-and-
disabled service-manager state is therefore a hard cutover invariant, not a
redundant check.

Before taking the copy, list every remaining item that could run later:

```bash
sqlite3 "$LEGACY_STATE/queue.sqlite3" \
  "SELECT id, state FROM queue_items
   WHERE state IN ('queued','held','blocked') ORDER BY id;"
```

Compare every returned global item ID with the signed operator inventory. An
item classified "drain under v4" must already be terminal or removed and must
not appear. Every item that remains queued, held, or blocked must be explicitly
approved for exact pinned legacy execution under v5. Stop on a missing,
unclassified, or differently classified ID; Project resume later makes all
otherwise eligible imported pending items dispatchable.

## 2. Preserve a raw backup, then make a consistent import copy

Create and copy only after the writers are stopped:

```bash
install -d -m 0700 "$CUTOVER_ROOT"
test ! -e "$RAW_BACKUP"
test ! -e "$CUTOVER_ROOT/flowers-v4-raw.sha256"
test ! -e "$CUTOVER_ROOT/flowers-v4-raw-tree.txt"
install -d -m 0700 "$RAW_BACKUP"
rsync -a "$LEGACY_STATE/" "$RAW_BACKUP/"
```

Record a file digest manifest outside the backup:

```bash
(cd "$RAW_BACKUP" && find . -type f -print0 | sort -z | xargs -0 sha256sum) \
  > "$CUTOVER_ROOT/flowers-v4-raw.sha256"
(cd "$RAW_BACKUP" && find . -printf '%y %p -> %l\n' | sort) \
  > "$CUTOVER_ROOT/flowers-v4-raw-tree.txt"
```

Do not mutate `RAW_BACKUP` after this point. Create a disposable working copy,
then use SQLite's backup API on that copy so committed WAL content is folded
into one database file without touching the raw rollback source:

```bash
test ! -e "$MIGRATION_COPY"
install -d -m 0700 "$MIGRATION_COPY"
rsync -a "$RAW_BACKUP/" "$MIGRATION_COPY/"
test ! -e "$MIGRATION_COPY/queue.sqlite3.consistent"
sqlite3 "$MIGRATION_COPY/queue.sqlite3" \
  ".backup '$MIGRATION_COPY/queue.sqlite3.consistent'"
mv "$MIGRATION_COPY/queue.sqlite3.consistent" \
  "$MIGRATION_COPY/queue.sqlite3"
rm -f "$MIGRATION_COPY/queue.sqlite3-wal" \
  "$MIGRATION_COPY/queue.sqlite3-shm" \
  "$MIGRATION_COPY/queue.sqlite3-journal"
```

The removals apply only to the disposable migration copy after the SQLite
backup succeeded; the raw backup remains complete. Verify the copy:

```bash
test ! -e "$MIGRATION_COPY/queue.sqlite3-wal"
test ! -e "$MIGRATION_COPY/queue.sqlite3-shm"
test ! -e "$MIGRATION_COPY/queue.sqlite3-journal"
sqlite3 "$MIGRATION_COPY/queue.sqlite3" \
  'PRAGMA integrity_check; PRAGMA foreign_key_check;'
sqlite3 "$MIGRATION_COPY/queue.sqlite3" \
  "SELECT id, state FROM queue_items
   WHERE state IN ('starting','running','yielding','terminating','force_killing')
   ORDER BY id;"
test ! -e "$COPY_IDENTITY"
(cd "$MIGRATION_COPY" && \
  env -u POSIXLY_CORRECT -u TAR_OPTIONS \
  tar --sort=name --format=posix \
    --pax-option='exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime' \
    --numeric-owner --owner=0 --group=0 -cf - .) | \
  sha256sum | awk '{print $1}' > "$COPY_IDENTITY"
```

Required output is `ok`, no foreign-key rows, and no active-state rows. Stop on
anything else. Preserve all non-database state files because the importer
hashes the complete tree and may need card, receipt, log, and continuation
evidence.
The copy-identity digest covers names, regular-file bytes, symlink targets,
directory entries, modes, sizes, and mtimes while normalizing ownership and
read-time metadata that the importer does not treat as source identity. Record
and review `COPY_IDENTITY` with the dry-run receipt.

## 3. Run the full dry run

The destination remains absent. The receipt path must be fresh and outside
source/destination:

```bash
experiment-queue migrate \
  --source-state "$MIGRATION_COPY" \
  --destination-state "$V5_STATE" \
  --project-key flowers-3d-helmholtz \
  --legacy-checkout "$FLOWERS_CHECKOUT" \
  --actor david \
  --receipt "$DRY_RECEIPT" \
  --legacy-root "$LEGACY_STATE" \
  --legacy-root "$RAW_BACKUP" \
  --legacy-root "$MIGRATION_COPY" \
  --dry-run \
  --confirm-source-is-copy \
  --json
```

`experiment-queue-migrate-v5` is the equivalent standalone importer entry
point and accepts the options after `migrate` above except the primary CLI's
diagnostic `--json` flag; it prints one concise outcome line while writing the
same typed receipt.

Review the typed QueueMigrationReceipt/v1, not only exit status:

```bash
python -m json.tool "$DRY_RECEIPT"
sha256sum "$DRY_RECEIPT"
test ! -e "$V5_STATE"
```

Require `result: succeeded`, `mode: dry-run`, `published: false`, source
schema 1–4, `integrity_check: ok`, zero foreign-key violations, identical
source/destination row counts and table digests, preserved IDs/sequences,
complete path inventory, and every continuation check either verified or
not-applicable. Compare the receipt's card/path disposition with the separate
operator inventory. A failed receipt is evidence to retain and fix against
fixtures; it is never permission to continue.

## 4. Publish the real v5 destination

Reconfirm the legacy services are still stopped, then reproduce and compare the
complete source-copy identity before publication. `V5_STATE`, `IMPORT_RECEIPT`,
and `COPY_RECHECK` must still be absent:

```bash
test ! -e "$V5_STATE"
test ! -e "$IMPORT_RECEIPT"
test ! -e "$COPY_RECHECK"
(cd "$MIGRATION_COPY" && \
  env -u POSIXLY_CORRECT -u TAR_OPTIONS \
  tar --sort=name --format=posix \
    --pax-option='exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime' \
    --numeric-owner --owner=0 --group=0 -cf - .) | \
  sha256sum | awk '{print $1}' > "$COPY_RECHECK"
cmp --silent "$COPY_IDENTITY" "$COPY_RECHECK"
```

Any comparison difference is a stop condition: preserve both identity files,
discard only the disposable copy, and restart from the untouched raw backup.
After an exact match, run the same import without `--dry-run`:

```bash
experiment-queue migrate \
  --source-state "$MIGRATION_COPY" \
  --destination-state "$V5_STATE" \
  --project-key flowers-3d-helmholtz \
  --legacy-checkout "$FLOWERS_CHECKOUT" \
  --actor david \
  --receipt "$IMPORT_RECEIPT" \
  --legacy-root "$LEGACY_STATE" \
  --legacy-root "$RAW_BACKUP" \
  --legacy-root "$MIGRATION_COPY" \
  --confirm-source-is-copy \
  --json
```

Publication is one atomic no-replace rename of a completely verified sibling
candidate; a destination created in the publication race window is preserved
and the import fails closed.
The canonical destination and external-receipt ancestor chains must be owned by
root or the service UID and non-writable by other accounts, except for sticky
shared ancestors. Their device/inode identities are rechecked across state and
receipt publication so an ancestor substitution cannot redirect success
evidence.
For a real import, the successful `published: true` receipt is staged inside
that candidate as its commit record. It becomes a truthful published receipt
only through the atomic rename, after which the importer reopens the final
database, matches its immutable instance UUID and embedded receipt bytes, and
only then publishes the external receipt. A leftover candidate is never a
published destination even if it contains the staged commit record.
If the destination rename cannot be directory-fsynced, the importer attempts
an exclusive rollback and fsyncs that rollback before normal cleanup. If
either step is uncertain, it preserves the visible destination/candidate names,
writes no misleading failed receipt, and requires operator inspection before
any retry.
External receipt publication likewise fsyncs a private staging file and its
directory entry, hard-links the final receipt without replacement, fsyncs that
publication, then removes and fsyncs the staging name. A final-link durability
failure preserves both receipt hard links and the published destination.
The imported Project is paused, legacy revisions/admissions are explicitly
marked, historical PID data is provenance rather than a recoverable process,
and the original v4 columns remain unchanged.

Verify before adding a typed revision or starting services:

```bash
python -m json.tool "$IMPORT_RECEIPT"
sha256sum "$IMPORT_RECEIPT"
sqlite3 "$V5_STATE/queue.sqlite3" \
  'PRAGMA integrity_check; PRAGMA foreign_key_check;'
experiment-queue --state-dir "$V5_STATE" project list --json
experiment-queue --state-dir "$V5_STATE" \
  receipt --project flowers-3d-helmholtz --json
```

Require a successful import receipt with `published: true`, `ok`, no
foreign-key rows, a canonical destination `database_instance_id` matching the
opened v5 database, a paused imported Project, explainable
item/event/dependency/reservation/artifact counts, and exact legacy
revision/card/ref/worktree evidence. QueueExport runtime records must include
the explicit `runtimeGpuLeaseHeld` and `runtimeGpuLeaseReleasedAt` fields; a
terminal imported historical assignment is unheld with no fabricated release
time.

Historical v4 `git_ref` and `worktree_path` columns remain immutable rollback
evidence. If an imported pending item runs under v5, the scheduler creates a
separate project-qualified runtime ref/worktree under the v5 state directory,
executes the exact pinned commit there, and cleans only that destination-owned
runtime identity. It never adopts, executes from, or deletes the historical
v4 ref/worktree as v5 runtime state.

## 5. Adopt a typed Flowers revision while dispatch remains blocked

The importer never fabricates Project/v1 evidence. Using the operator-approved
portable Flowers manifest, extension schema, Enrollment, and full commit,
append the first resolver-authenticated typed revision:

```bash
experiment-queue --state-dir "$V5_STATE" \
  project append-revision --project flowers-3d-helmholtz \
  "$FLOWERS_CHECKOUT" \
  --manifest Project.yaml \
  --enrollment /absolute/path/to/flowers-enrollment.json \
  --git-commit FULL_APPROVED_FLOWERS_COMMIT \
  --actor david

experiment-queue --state-dir "$V5_STATE" \
  project doctor --project flowers-3d-helmholtz
```

The first typed revision after import must activate atomically, but Project
lifecycle remains paused. Verify its logical mounts, artifacts, environment,
exact blob evidence, and the live-card inventory. Imported pending items remain
pinned to their grandfathered revisions; new submissions use the typed current
revision.

Configure web credentials and review Project/item/event/artifact pages without
starting dispatch. First set the durable host-global pause while no service is
running, then start only the web process in a dedicated foreground terminal:

```bash
experiment-queue-web --state-dir "$V5_STATE" auth-setup \
  --operator-project flowers-3d-helmholtz \
  --viewer-project flowers-3d-helmholtz
experiment-queue --state-dir "$V5_STATE" host pause \
  --reason "cutover verification" --actor david
experiment-queue-web --state-dir "$V5_STATE" serve \
  --host 127.0.0.1 --port 8443 \
  --tls-cert "$V5_TLS_CERT" --tls-key "$V5_TLS_KEY"
```

Review the authenticated host-admin, Project-operator, viewer, and reserver
surfaces over `https://127.0.0.1:8443`, including finite Project scope and
read-only behavior. Then stop the foreground web process and prove it exited.
Do not start the scheduler. Run `require_no_open_files "$V5_STATE"`; any
remaining handle or inspection error is a stop condition before authorization.

## 6. Explicit authorization and single-writer start

Stop here until David explicitly authorizes cutover after reviewing:

- raw backup and digest manifest;
- dry-run and import receipts;
- external-path/card inventory;
- typed revision and doctor output;
- CLI/web presentation and authorization; and
- exact rollback service commands.

After authorization, confirm the already-recorded host pause, ensure the legacy
units remain stopped, and start only the v5 scheduler/web units. Then
deliberately open the Project and final host gates:

```bash
experiment-queue --state-dir "$V5_STATE" \
  project resume --project flowers-3d-helmholtz \
  --reason "approved schema-v5 cutover" --actor david
experiment-queue --state-dir "$V5_STATE" gpu show
experiment-queue --state-dir "$V5_STATE" status \
  --project flowers-3d-helmholtz
experiment-queue --state-dir "$V5_STATE" host resume --actor david
```

Before the final host resume, compare live telemetry against every enabled v5
allowlist UUID and confirm no unexpected compute PID, memory use, duplicate
UUID/index record, or other scheduler owns that pool. The first v5 claim creates
a durable per-item runtime GPU lease. Thereafter terminal state alone never
authorizes reuse: recovery must authenticate current idle telemetry, commit the
lease-release event, and only then permit cleanup, reservation activation, or
redispatch.

Run the approved small operational check. Confirm recovery, required artifact
observation, events, receipts, GPU controls, passive reservation, graceful
termination, and cooperative preemption: typed v1 only on a typed authorized
item, plus exact v0 only if an imported item was already recorded preemptible.
Because David waived a separate production-state rehearsal, faults discovered
here are fixed in the standalone repository, verified against disposable
fixtures, and retried from controlled state; the v4 rollback source is never
repaired in place.

## Rollback

Rollback is a service selection, never a database conversion:

1. Pause v5 dispatch if it is responsive, then stop both v5 scheduler and web
   writers through their recorded service-manager units.
2. Confirm no v5 process owns a GPU or has `V5_STATE/queue.sqlite3` open.
3. Preserve `V5_STATE`, all receipts/logs, and scientific artifacts for
   diagnosis. Do not delete them and do not attempt v5→v4 SQL.
4. Verify the raw-backup digest manifest and select the untouched original v4
   state (or an operator-approved restored copy of `RAW_BACKUP`).
5. Confirm the v5 units cannot restart, then start only the recorded
   `experiment-queue-legacy-v4` scheduler/web configuration.
6. Record rollback time, reason, writer ownership, database/code identity, and
   any item whose scientific process or artifact changed after cutover.

If rollback occurs before any v5 dispatch, the untouched v4 state is the exact
operational history. If v5 dispatched or controlled an item, v4 cannot contain
those new events. Stop and reconcile those item/process/artifact outcomes from
preserved v5 evidence before resubmission; never merge databases or pretend the
old history advanced.

For an importer/runtime defect found before authorization, retain every failed
receipt, fix and validate the standalone code against fixtures, and use fresh,
previously absent `MIGRATION_COPY`, `COPY_IDENTITY`, `COPY_RECHECK`,
`DRY_RECEIPT`, `IMPORT_RECEIPT`, and `V5_STATE` names for the next attempt.
Discard only an ordinary unpublished candidate that the importer explicitly
reports as cleaned, create the new copy from the untouched raw backup, and
restart with dry run. If the importer reports publication or cleanup as
indeterminate, preserve every named candidate/destination/staging/receipt path
and resolve that evidence before any retry; never delete it merely to make the
same name reusable. Never repair production history in place or overwrite a
no-clobber receipt.
