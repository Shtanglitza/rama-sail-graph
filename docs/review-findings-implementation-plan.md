# Review Findings Implementation Plan

Date: 2026-03-26
Updated: 2026-03-27

## Purpose

This note captures the current review findings so the next implementation pass can address:
- correctness gaps
- deletion-mode and tombstone design risks
- open-source release blockers
- production readiness blockers

## Priority Findings

### 1. Soft-delete ordering semantics are not production-safe

Status: partially fixed

Files:
- `src/rama_sail/core.clj`
- `src/rama_sail/sail/adapter.clj`

Problem:
- Last-write-wins behavior is driven by client-generated `tx-time` values from `System/currentTimeMillis`.
- Under multiple writers, clock skew, retries, or replayed messages, an older delete or clear may hide a newer write in the default soft-delete mode.

What was fixed:
- `:clear-context` now respects last-write-wins ordering — reads `$$quad-tx-time` for each quad and only tombstones if the clear's tx-time >= the quad's creation time. A newer add survives an older clear.

What changed since the previous review:
- The module now also supports a `*hard-delete?*` mode that physically removes quads instead of writing tombstones.
- That mode reduces the tombstone-specific ordering/compaction burden, but it is currently a topology-time dynamic binding, not a documented product-level operating mode.

What remains:
- Replace client-clock ordering with a stronger monotonic ordering source if possible.
- If ordering must stay timestamp-based, define and document the consistency envelope explicitly.
- Review all stats/materialized-view maintenance paths under out-of-order delivery assumptions.

Tests to add:
- older delete should not hide newer add
- older clear-context should not hide newer add in same context
- retry/replay of delete should remain idempotent
- multi-writer ordering scenarios with explicit timestamps

### 2. Soft-delete tombstones still have no compaction / garbage-collection story

Status: critical design gap for the default soft-delete path

Files:
- `src/rama_sail/core.clj`
- query paths that consult `$$tombstones`

Problem:
- Deleted quads remain in primary indexes forever and reads keep paying tombstone checks.
- There is no reaper, retention policy, archive strategy, or physical compaction path.
- High-churn workloads will accumulate permanent storage and read amplification.

Current nuance:
- This is no longer the only deletion model in the codebase because hard-delete mode exists.
- However, soft delete remains the documented/default behavior, so the compaction gap still blocks production guidance for that mode.

Implementation direction:
- Decide which deletion mode is actually supported for the product:
  - soft delete
  - hard delete
  - both, with explicit tradeoffs and operating guidance
- Decide whether tombstones are needed for:
  - temporal queries
  - audit/history
  - replay safety
- If yes, add a compaction model:
  - retention window
  - snapshot + purge
  - archive/offload
  - background cleanup topology
- If no, hard delete may be the simpler supported default, but it needs to be surfaced and documented as such.

Tests to add:
- compaction preserves visible state
- compaction preserves intended historical guarantees
- stats/materialized views remain correct after cleanup

## Release And Readiness Findings

### A. Repo is not ready for a clean open-source release yet

Status: release blocker

Files:
- `project.clj`
- `README.md`
- repository root

Problems:
- Build is not self-contained for a normal external user:
  - custom Maven repository
  - provided Rama dependency
- License expression and README license text are inconsistent.
- There is no visible CI configuration in the repo.
- The current repository snapshot is largely uncommitted / dirty, which is not a release state.
- The README still describes tombstone-based soft deletes as the product story and does not explain the newer hard-delete mode or its intended status.

Implementation direction:
- Decide whether the intended release is:
  - fully public OSS
  - source-available for existing Rama users
  - internal/public preview
- Make the build story explicit in the README.
- Resolve the license statement inconsistency.
- Add CI for at least:
  - `lein check`
  - focused unit tests
  - IPC-backed integration tests where environment allows
- Create a release checklist and versioned changelog policy.

Checklist:
- clean worktree for release branch/tag
- CI green
- explicit supported environment matrix
- explicit dependency access instructions
- resolved license text

### B. Repo is not ready for production use yet

Status: production blocker

Files:
- `src/rama_sail/core.clj`
- `src/rama_sail/sail/adapter.clj`
- `src/rama_sail/server/sparql.clj`

Problems:
- No tombstone compaction strategy.
- Unsupported operators can fall back to local RDF4J evaluation, creating unpredictable scaling behavior.
- Query timeout cancellation is best-effort only; backend work may continue after client timeout.
- SPARQL endpoint currently has permissive CORS and no visible auth / rate limiting / transport hardening model.
- Deletion mode is not yet a clear operating contract: hard delete exists in code, but is not exposed as a stable documented runtime choice.

Implementation direction:
- Treat the SPARQL server as dev/internal until security and resource-control expectations are defined.
- Explicitly classify fallback execution paths:
  - acceptable in dev
  - warn-only in staging
  - disabled or guarded in production
- Add operational guidance for:
  - memory sizing
  - churn/deletion workloads
  - timeout behavior
  - observability and alerting

Checklist:
- compaction or retention strategy
- predictable query execution model
- auth and deployment hardening plan
- explicit supported deletion mode
- load/perf characterization beyond functional tests

## Secondary Improvements

### C. `GROUP BY` currently centralizes at origin

Files:
- `src/rama_sail/core.clj`

Observation:
- The current group topology routes to `|origin` before combining, so aggregation is effectively centralized.

Potential improvement:
- Push partial aggregation to workers before final merge.
- Re-benchmark after any change because correctness is higher priority than distribution elegance.

## Suggested Implementation Order

1. Decide the supported deletion mode and product contract:
   - soft delete
   - hard delete
   - or both with explicit semantics
2. If soft delete remains supported, define and implement tombstone compaction / retention strategy.
3. Replace client-clock ordering with monotonic source, or document consistency envelope.
4. Decide OSS release target and resolve build/license/CI blockers.
5. Define production operating model for fallback execution, timeouts, and SPARQL endpoint hardening.
6. Optimize distributed `GROUP BY`.

## Completed Items

The following findings were fixed and verified (all tests passing):

- **`hasStatementInternal` ignores pending writes** — Reworked to check pending adds, pending deletes, and cleared contexts. 3 tests added in `transaction_test.clj`.
- **Empty-input aggregate without `GROUP BY` returns no row** — `finalize-group-results` now produces one row with identity values when group-vars is empty and input is empty.
- **`DISTINCT` aggregates parsed but not enforced** — Aggregate state tracks seen-value sets when `:distinct true`, with correct distributed merge via `clojure.set/union`.
- **`MIN`/`MAX` only work for numeric values** — `compare-rdf-terms` compares numerics numerically and all others lexicographically. Tests updated for string support.
- **SPARQL content negotiation too strict** — `media-range-matches?` supports `type/*` ranges. `parse-accept` uses stable sort for equal `q` values.
- **Context deduplication inconsistent** — `getStatementsInternal` and `hasStatementInternal` now deduplicate contexts with `distinct`, matching `sizeInternal`.
- **`:clear-context` ignores tx-time ordering** — Now reads `$$quad-tx-time` and only tombstones quads where clear tx-time >= creation time.

The following additional implementation exists in the current tree, but should be treated as product-status work rather than a closed review item:

- **Optional hard-delete mode exists** — `*hard-delete?*` switches the module to physical removal semantics, and dedicated tests exist. This is real implementation progress, but it is not yet surfaced or documented as the supported default operating mode.

## Tombstone Design Assessment

Current assessment:
- Tombstones make sense only if the product genuinely needs one or more of:
  - temporal visibility
  - audit/history
  - replay-friendly append-only mutation model
- If those are real requirements, the approach is defensible.
- If not, the current design may be paying too much complexity and read overhead for limited practical benefit.
- Hard delete is now a concrete alternative in the codebase, not just a hypothetical simplification path.

Decision to make explicitly:
- keep tombstones as a first-class architectural choice and invest in ordering + compaction
- or simplify toward physical delete semantics and make that a documented supported mode

## Verification Baseline

Current baseline observed from this repository snapshot:

```bash
lein check
```

Notes:
- `lein check` passes in the current tree.
- IPC-backed tests require an environment that allows the in-process Rama/ZooKeeper cluster to bind local sockets.
- The older command list below should be treated as a target verification baseline, not as re-verified truth for this exact snapshot until rerun in an unrestricted environment:

```bash
lein test :only rama-sail.sail.adapter-test rama-sail.sail.optimizer-test
lein test :only rama-sail.server.sparql-test
lein test :only rama-sail.query.aggregate-test
lein test :only rama-sail.query.transaction-test
lein test :only rama-sail.sail.e2e-test
lein test :only rama-sail.sail.compliance-test
```

## Decisions Made (2026-03-27)

### Deletion mode: Hard delete only

**Decision:** Soft-delete (tombstones) removed from codebase. Hard delete is the only supported mode.

**Rationale:** Eliminates tombstone compaction gap, simplifies query paths, removes `*hard-delete?*` toggle complexity. Soft-delete was paying read amplification and storage overhead without a concrete temporal/audit use case.

**What was done:**
- Removed `*hard-delete?*` dynamic var and all branching
- Removed `$$tombstones` PState
- Removed `find-triples-unfiltered-query-topology` (inlined scan into `find-triples`)
- Simplified all query topologies (removed tombstone filtering branches)
- Simplified ETL operations (`:add`, `:del`, `:clear-context`) to hard-delete only
- Kept `$$quad-tx-time` for idempotent stats tracking
- Deleted soft-delete test files, updated remaining tests

**This resolves:** Finding #1 (soft-delete ordering), Finding #2 (tombstone compaction), and the Tombstone Design Assessment section.

### Release target: Public OSS preview

**Decision:** Release as Apache 2.0 open source with preview status.

**What was done:**
- README updated with preview status notice
- Known limitations section added
- SPARQL endpoint documented as dev-only
- Features list updated to reflect hard-delete semantics

### Code quality fixes applied

- `DEFAULT-CONTEXT-VAL` consolidated to single source of truth in `serialization.clj`
- BNode skolemization wired up in SAIL connection (`addStatementInternal`)
- Statistics decrement guarded with floor-at-zero to prevent negative counts
- `ask-result` topology optimized with LIMIT 1 short-circuit

## Reference Status Snapshot

Current review conclusion (2026-03-27):
- Public OSS preview: ready
- Production deployment: not yet (no CI, no SPARQL endpoint hardening, no operational guidance)
