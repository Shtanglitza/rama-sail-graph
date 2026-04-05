# Rama Performance Opportunities

Reviewed on April 5, 2026.

This note updates the earlier performance write-up against the current codebase. It focuses on places where Rama-native execution can still improve the system, but it also calls out semantic issues that affect whether those optimizations are safe.

## Current Status

Several important optimizations already exist and should not be described as missing:

- `self-join` detection and execution are implemented in `src/rama_sail/sail/optimization.clj` and `src/rama_sail/module/queries.clj`.
- `batch-enrich` is implemented for property-lookup join shapes.
- DP-based multi-way join reordering already exists.
- `LIMIT` pushdown to `:bgp` / `:join` / `:self-join` is implemented.
- `sizeInternal` already uses `get-global-stats` for all-context counts when there are no pending ops, so "`count-statements(nil)` via global stats" is no longer a top read-path priority at the adapter level.
- Bag semantics are restored across all relational operators (join, left-join, union, bind, filter, self-join helpers). Set aggregation is retained only in `find-bgp` (correct for quad→triple collapse), `distinct`, and `list-contexts`.

Several earlier recommendations should therefore be treated as done or deprioritized:

- Do not prioritize a one-line `sizeInternal` optimization. It is already in place.
- Do not describe self-join, batch-enrich, or join-chain reordering as future work.
- Do not treat the current `LIMIT` work as fully solved. It improved throughput, but the remaining semantics are still risky enough to deserve follow-up.
- Do not describe bag-vs-set semantics as an open issue. It is resolved.

## Executive Summary

The highest-leverage remaining work is:

1. Replace heuristic `LIMIT` pushdown with semantics-preserving early termination, especially for `ORDER BY ... LIMIT`.
2. Make joins more distributed and less origin-centric.
3. Reduce the repeated `invoke-query -> full collection -> explode again` execution pattern.
4. Rebuild property-path execution around distributed iteration or precomputed reachability.

The common anti-pattern is still:

1. Execute a sub-plan.
2. Materialize the entire result collection.
3. Re-explode it in the next operator.
4. Aggregate again at the origin.

That limits scalability even when the logical optimization is already good.

## Priority 1 (Resolved): Bag Semantics

Bag semantics have been restored across all relational operators. The following operators now use bag-preserving aggregation (`+vec-agg`, `+vec-concat-combiner`, `+vec-union-combiner`):

- `join`, `left-join`, `union`, `bind`, `filter`, `colocated-subject-join`
- Helper functions: `process-subject-groups`, `batch-enrich-merge-results`, `apply-multi-left-join`

Set aggregation (`+set-agg`) is retained only where the algebra requires it:

- `find-bgp`: correct — BGP evaluates over a graph (set of triples); quad storage can produce identical bindings from different contexts when context is unbound, so dedup collapses these correctly
- `distinct`: correct — DISTINCT explicitly requires dedup
- `list-contexts`: correct — admin query returning unique context IRIs

Cluster benchmarks (51K BSBM triples, 50 iterations) confirmed no performance regression.

## Priority 2: Replace Heuristic LIMIT Pushdown With Safe Early-Termination

### Why this matters

The current `LIMIT` work improved performance, but some of it is heuristic rather than semantics-preserving:

- limits are pushed through `FILTER` using a fixed multiplier
- limits are pushed through `DISTINCT` using a fixed multiplier
- `ORDER BY` still allows pushdown into joins as a heuristic

Relevant code:

- `src/rama_sail/sail/optimization.clj`
  - `push-limit-down`
- `src/rama_sail/module/queries.clj`
  - `find-bgp-query-topology`
  - `join-query-topology`

This means some plans can still return the wrong top-N or too few qualifying rows under selective filters / high-duplication inputs.

### Recommended change

Split the current work into two buckets:

- keep exact early termination where it is semantics-preserving
- remove or fence off heuristic truncation where correctness is not guaranteed

The most Rama-native next step is a real distributed top-N path for:

- `ORDER BY ... LIMIT N`
- `ORDER BY ... OFFSET M LIMIT N`

using a heap-based combiner of size `offset + limit`.

### Expected payoff

- Preserves correctness while keeping the current latency gains
- Removes the need for fragile multiplier heuristics
- Makes `ORDER BY + LIMIT` materially cheaper

### Rama rationale

This fits Rama's combiner model much better than full sort followed by slice.

Official docs:

- Aggregators: <https://redplanetlabs.com/docs/~/aggregators.html>
- Clojure dataflow language: <https://redplanetlabs.com/docs/~/clj-dataflow-lang.html>

## Priority 3: Make Join Execution More Rama-Native

### Why this matters

The optimizer already does useful logical work:

- join reordering
- self-join detection
- batch enrich

But physical execution of generic joins is still mostly origin-centric:

- materialize right side
- build hash map in Clojure
- materialize left side
- probe after materialization

Relevant code:

- `src/rama_sail/module/queries.clj`
  - `join-query-topology`
  - `left-join-query-topology`
  - `multi-left-join-query-topology`
- `src/rama_sail/query/helpers.clj`
  - `build-hash-index`
  - `probe-hash-index`
  - `apply-left-join-with-condition`

### Recommended change

Move generic joins toward distributed probe execution:

- keep the smaller build side materialized
- repartition the probe side by join key with `|hash` or `|shuffle`
- combine results with bag-preserving combiners
- update the join cost model together with execution changes

### Expected payoff

- Less origin-task memory pressure
- Better scaling with task count
- Better tail latency on larger joins

### Rama rationale

This aligns with partitioners, co-location, and two-phase aggregation instead of treating Rama as a transport around local Clojure joins.

Official docs:

- Partitioners: <https://redplanetlabs.com/docs/~/partitioners.html>
- Aggregators: <https://redplanetlabs.com/docs/~/aggregators.html>
- PStates: <https://redplanetlabs.com/docs/~/pstates.html>

## Priority 4: Reduce Full-Result Materialization Between Operators

### Why this matters

The code still repeatedly executes a sub-plan into a full collection and then explodes it again in the next operator.

This shows up in:

- `project`
- `distinct`
- `slice`
- `group`
- `bind`
- `order`
- `batch-enrich`
- `self-join`
- generic joins

Relevant code:

- `src/rama_sail/module/queries.clj`

### Recommended change

Push operators toward one of these patterns:

- stream rows through partitioners and combiners
- keep operator-specific work local to the task that owns the relevant partition
- materialize only where the data is reused enough to justify it

This work should be done after bag semantics are fixed, otherwise the new execution path will preserve the wrong semantics more efficiently.

### Expected payoff

- Lower peak memory
- Less origin-task fan-in
- Better throughput on wider result sets

## Priority 5: Rebuild Property Paths Around Distributed Iteration

### Why this matters

Property paths are still the least Rama-native part of the query engine:

- wildcard the step plan
- execute it fully
- collect all edges in memory
- compute closure in local Clojure data structures
- for `*`, scan the whole store again to get all nodes

Relevant code:

- `src/rama_sail/module/queries.clj`
  - `compute-transitive-closure`
  - `compute-zero-or-more-closure`
  - `arbitrary-length-path-query-topology`
  - `zero-length-path-query-topology`

### Recommended change

Choose one of two directions:

1. Distributed frontier-style iteration using Rama loops and partitioned state.
2. Incremental reachability materialization for a small set of hot predicates.

Direction 1 is the better general solution.
Direction 2 is worthwhile only if path workloads are concentrated on a few predicates.

### Expected payoff

- Removes a major single-node memory ceiling
- Makes path workloads scale with cluster resources
- Avoids repeated full-store scans for `*` and `?`-adjacent cases

### Rama rationale

This is a natural fit for Rama loops and partitioned state rather than origin-side closure building.

Official docs:

- Intermediate dataflow and loops: <https://redplanetlabs.com/docs/~/intermediate-dataflow.html>
- Clojure dataflow language: <https://redplanetlabs.com/docs/~/clj-dataflow-lang.html>

## Priority 6: Make Planner Stats Fetch Selective

### Why this matters

The adapter still fetches all predicate stats per connection cache refresh:

- `get-all-predicate-stats`
- `get-global-stats`

Relevant code:

- `src/rama_sail/sail/adapter.clj`
  - `fetch-stats!`
- `src/rama_sail/module/queries.clj`
  - `get-all-predicate-stats-query-topology`
  - `get-global-stats-query-topology`

This was overstated in the earlier doc. It is real, but lower priority than the execution-path issues above because the current cache amortizes most of the cost.

### Recommended change

If profiling shows planning overhead is material:

1. compile the raw plan
2. extract bound predicates
3. fetch only those predicate stats
4. fetch global stats separately

### Expected payoff

- Lower planning latency for small queries over large predicate catalogs
- Less cluster work spent on stats fanout

### Recommended priority

Treat this as profiling-driven work, not a front-line optimization.

## Priority 7: Add a Planner Rewrite for Co-Located Subject Joins

### Why this matters

There is already a specialized `colocated-subject-join-query-topology`, but the optimizer does not emit it automatically.

Relevant code:

- `src/rama_sail/module/queries.clj`
  - `colocated-subject-join-query-topology`
- `src/rama_sail/query/helpers.clj`
  - `join-subject-locally`

### Recommended change

Add a rewrite only for tightly constrained cases where both sides are clearly subject-local and no intervening semantics make the rewrite unsafe.

### Why this moved down

This is still attractive, but it is more correctness-sensitive than the earlier document implied:

- context constraints matter
- filters / optional structure matter
- the shared subject must really align with the physical partition key

Do this after the bag-semantics and generic-join work is in better shape.

## Priority 8: Add Context Metadata PStates If Admin Paths Matter

### Why this matters

`list-contexts` still scans `$$cspo`, and `getContextIDsInternal` may do extra verification work when there are pending deletes.

Relevant code:

- `src/rama_sail/module/queries.clj`
  - `list-contexts-query-topology`
- `src/rama_sail/sail/adapter.clj`
  - `getContextIDsInternal`

### Recommended change

Only if context-admin paths are hot:

- maintain `context -> live-count`
- maintain a live context set

This is still a valid Rama-style improvement, but it is not as important as the query-engine items above.

## Suggested Implementation Order

### Phase 1: Correctness and safe execution foundations

1. ~~Fix bag-vs-set semantics across core operators.~~ Done.
2. Replace heuristic `LIMIT` pushdown with safe early termination rules.
3. Add end-to-end tests for duplicate-sensitive aggregates and `ORDER BY + LIMIT` correctness.

### Phase 2: Core distributed execution improvements

4. Rework generic join execution around repartitioned probe paths.
5. Reduce full-result materialization in `project`, `group`, `bind`, `order`, and join-adjacent operators.
6. Add top-N combiner support for `ORDER BY + LIMIT`.

### Phase 3: Larger architectural work

7. Rebuild property-path execution using distributed iteration or selective reachability materialization.
8. Add optimizer rewrite for `:colocated-subject-join`.
9. Make planner stats fetching selective if profiling justifies it.
10. Add context metadata PStates if admin/context enumeration paths show up in production profiles.

## Testing Gaps To Close

The current test suite is strong on plan-shape and feature coverage, but weaker in the areas that matter most for the remaining work:

- `ORDER BY + LIMIT` correctness under selective filters and join-heavy plans
- larger property-path workloads that expose materialization costs

These should be added before major execution rewrites land.

## Files Most Worth Changing First

- `src/rama_sail/module/queries.clj`
- `src/rama_sail/query/helpers.clj`
- `src/rama_sail/sail/optimization.clj`
- `src/rama_sail/query/aggregation.clj`
- `src/rama_sail/sail/adapter.clj`
- `src/rama_sail/module/indexer.clj`

## Source References

- `src/rama_sail/module/queries.clj`
- `src/rama_sail/query/helpers.clj`
- `src/rama_sail/query/aggregation.clj`
- `src/rama_sail/module/indexer.clj`
- `src/rama_sail/sail/optimization.clj`
- `src/rama_sail/sail/adapter.clj`
- `test/rama_sail/sail/optimizer_test.clj`
- `test/rama_sail/sail/property_path_test.clj`
- `test/rama_sail/query/aggregate_test.clj`
- `test/rama_sail/query/union_test.clj`
