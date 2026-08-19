# ADR 0004: Legacy compatibility and offline cutover

Status: accepted, 2026-08-19.

Historical Markdown cards remain unchanged. A future explicit
`LegacyMarkdownCard/v0` adapter will implement exactly the current parser; it
will not accumulate broader heuristics.

The v4-to-multi-project database migration is offline, one-way, dry-run capable,
and receipt-producing. It operates on a copy and preserves item IDs, attempts,
events, commits, worktree and Git-ref identities, process metadata, and
continuation data. Rollback means using the untouched v4 database with old
code—never attempting a schema downgrade.

Flowers checkout wrappers will temporarily forward to the installed package.
Legacy admission can be retired only after nonterminal legacy items drain,
active cards and documented commands migrate, and at least one release warns of
deprecation. Old and new schedulers must never dispatch concurrently against
the same GPU pool.
