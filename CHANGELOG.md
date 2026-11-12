# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** This project is not published to Maven Central or Clojars and has no git
> tags yet. Consume it via a `:git/sha`-pinned [git dependency](README.md#installation).
> The headings below are development milestones, not tagged releases.

## Unreleased

### Security
- **SPARQL UPDATE endpoint is now read-only by default.** Previously the HTTP endpoint
  accepted unauthenticated SPARQL UPDATE with `Access-Control-Allow-Origin: *`; since a
  form-encoded POST is a CORS "simple request", any web page could fire
  `DELETE WHERE { ?s ?p ?o }` at a reachable server. Updates now require `--allow-updates`,
  an optional `--auth-token` (Bearer) gate, and a configurable `--cors-origin`.

### Added
- **Queries that grew with the data now stay flat (1M triples).** A profile showed where the seven
  slow BSBM queries read too much:
  - Limited index scans: a new `find-triples-limit` query topology reads the index in key order and
    stops after about as many quads as it needs. `find-bgp` uses it for a BGP with a result limit
    (not under `GRAPH ?g`), and self-join LIMIT attempts read only that many triples. The driver of
    a LIMIT over lookups no longer reads the whole predicate.
  - Filtered property lookups: a join of a few rows with `?s p ?o FILTER(...)` becomes a batched
    lookup followed by the filter, instead of a filtered scan of the whole property (Q1, Q4, Q10).
  - Lookups by object: `batch-enrich` can key on the object (`:by-object?`, new
    `batch-lookup-by-object` query topology on `$$posc`), so `?y p ?x` with `?x` bound by a few
    rows no longer scans the predicate (Q5). LIMIT attempts still chain only lookups by subject,
    which cannot multiply rows.

  BSBM on 1,005,547 triples (local single-node cluster, runs alternated between main and this
  change on the same store), p50 before -> after: Q1 181 -> 7.5 ms, Q4 317 -> 8.1, Q5 421 -> 18.7,
  Q10 535 -> 5.8, QJ1 441 -> 20.6, QJ3 571 -> 20.9, QJ4 127 -> 6.2; the other six unchanged; query
  mix 2.6-2.9 s -> 0.16-0.20 s.
- **Bulk loading, about 18x faster on 1M triples.** A profile of the indexer showed it spends a load
  committing microbatches to disk, not indexing: its worker threads were 40% idle and 44% in RocksDB
  batch writes, fsync and log writes, with about 16% in the indexing dataflow. Three changes follow:
  - Module launch options (`core/indexer-launch-options`): 5,000 records per depot partition per
    microbatch and a PState flush batch of 100,000 paths. They apply when a module is first
    launched; on an existing deployment set the two Rama dynamic options by hand (README, Bulk
    Loading).
  - `rama-sail.bulk`: `load-file!` (N-Triples, Turtle, TriG, RDF/XML, streamed) and `load-quads!`
    append `:commit` records with backpressure, and return when the data is queryable.
    `await-applied!` waits on a marker appended to the commit log, so it also covers records the
    indexer has not read yet.
  - `[:config {option value}]` depot records (`core/set-indexer-option!`) change
    `commit-ops-per-microbatch` and `clear-quads-per-microbatch` at runtime. The default commit
    budget is now 10,000 (was 4,000).

  Loading 1,005,547 BSBM triples on a local single-node cluster: 1,899 s before (530/s), 825 s with
  per-triple appends and the launch options, 107 s with the bulk loader (9,378/s).
- 1M-triple scale benchmark in the README: the query mix grows from 225 ms at 59K triples to about
  2.4 s; point lookups stay flat, filtered scans and `LIMIT`-over-join queries grow 10-19x.
- Bench: `cluster-bench :bulk true :bulk-budget N`; the loader's indexing deadline scales with the
  load size (it was a fixed 2 minutes).
- **CI runs every test namespace.** Eight namespaces were never run in CI, among them the
  SPARQL server's UPDATE security tests, the RDF4J SAIL compliance tests and the property-path
  caps. The test lists now live in `project.clj` (`lein test :unit`, `:integration`, `:w3c`),
  the release workflow runs the same gate as CI instead of two unit namespaces, and
  `ci-gate-test` fails when a new test namespace is in none of them. `find-bgp-test` runs
  against the real module again.
- SPARQL server flags: `--allow-updates`, `--auth-token`, `--cors-origin`; startup warns on
  the risky writable + wildcard-CORS + no-token combination.
- Property-path evaluation cap (`*path-materialization-limit*`): unbounded `?x :p* ?y` over a
  large store now fails fast with a clear error instead of OOM-ing the worker.
- Library infrastructure: CI workflows (unit + integration + W3C conformance), API stability
  boundaries, this changelog, and community/validation docs.
- Generative N-Triples serialization round-trip suite and metamorphic optimizer-equivalence suite.
- Differential test suite against RDF4J's MemoryStore (`memorystore-parity-test`, runs in CI).
- Randomized query suite (`random-query-parity-test`, runs in CI): seeded random SPARQL over
  OPTIONAL/UNION/FILTER/BIND/VALUES/subqueries/GRAPH; checks that unoptimized, optimized and
  statistics-optimized plans agree, and matches RDF4J where RDF4J follows the spec. Failures
  are shrunk to a minimal query. `RANDOM_QUERY_SEED` / `RANDOM_QUERY_COUNT` override the run.

### Changed
- **RamaSail advertises the isolation levels it provides.** It inherited RDF4J's `AbstractSail`
  defaults, under which every transaction nominally ran as `SERIALIZABLE`; RDF4J's
  `SailIsolationLevelTest`, now part of the compliance tests, fails that level against this store.
  Supported levels are now `NONE`, `READ_UNCOMMITTED` and `READ_COMMITTED` (default). Behavior
  change: `begin()` with `SNAPSHOT_READ`, `SNAPSHOT` or `SERIALIZABLE` throws instead of silently
  running with weaker guarantees. Transactions started without a level are unaffected.
- **A SAIL commit is one depot record.** `commit()` appended one record per operation, one network
  round trip each, and a failure partway left half a commit in the store. It now appends the whole
  commit as a single `[:commit [op ...]]` record (several for commits over 5,000 operations,
  `:commit-chunk-size`), so a commit of up to that size is accepted all-or-nothing. The indexer
  stores it in a new `$$commit-queue` PState and applies a bounded number of operations per microbatch
  (`commit-ops-per-microbatch`), oldest commit first: every commit record goes to one depot
  partition, so commits take effect in the order they were appended. On a local cluster a
  20,000-statement commit went from 597 s to return (599 s until visible) to 0.24 s to return (21 s
  until visible). Direct per-quad depot appends are unchanged (59K load 69.1 s vs 70.6 s).
  Readers can see part of a large commit while it is applied; the new `pending-commit-count` query
  topology returns 0 when every commit is fully applied, and `:sync-commits` waits for it.
  **Deploy order is free:** the SAIL sends commit records only when the module has that query
  topology and otherwise appends one record per operation as before. Tests that waited for "N
  records processed" after an N-statement commit must use depot offsets instead
  (`test-helpers/wait-indexed!`).
- **`LIMIT` over property lookups stops reading early.** The planner adds cheaper attempts that
  truncate the driving pattern, and the executor falls back to the full plan when an attempt yields
  too few rows, so results stay exact. On BSBM with 59K triples the median query mix time fell from
  496 ms to 237 ms; QJ1 went from 103 ms to 29 ms, QJ3 from 162 ms to 36 ms and QJ4 from 47 ms to
  9 ms. An older module ignores the attempts and still returns correct results.
- **Property lookups scale with the input instead of the store.** `batch-enrich` now applies to
  either side of a join when the input is no larger than the property, and to `OPTIONAL` property
  patterns with no filter. On BSBM with 59K triples the median query mix time fell from 1,055 ms
  to 488 ms; Q7 went from 178 ms to 2.6 ms and Q8 from 115 ms to 2.0 ms. **Deploy the module before
  the clients:** an older module ignores the plan's `:optional?` flag and drops unmatched rows.
- Upgraded Rama 1.6.0 → 1.9.0 (1.9.0 fixes a yielding `local-select>` in a microbatch topology reading a closed RocksDB instance; the indexer yields on large scans). (Run `lein clean` after changing the Rama dependency to clear
  stale AOT bytecode.)
- **Expression engine rebuilt** with a typed numeric tower (Long/BigInteger/BigDecimal/Double)
  and SPARQL three-valued logic / type-error semantics. Behavior changes: plain/untyped literals
  no longer compare numerically (`"10" < "9"` is `true` as strings); MIN/MAX over untyped values
  order lexically; arithmetic is datatype-typed (`1+1` → `"2"^^xsd:integer`, integer/integer →
  decimal); EBV of unbound/IRI is a type error.
- Aggregates: exact SUM/AVG (no double coercion), datatype-aware MIN/MAX, correct `COUNT(DISTINCT *)`.
- Indexer: cooperative yielding (`{:allow-yield? true}`) on large subindexed scans; stat updates
  use no-read `termval` writes.

- **One term comparator for `=` / `!=` / `IN`, `<` / `<=` / `>` / `>=`, ORDER BY and MIN/MAX**
  (checked cell by cell against Apache Jena). Behavior changes:
  - Ordering two IRIs (or blank nodes, or literals of an unknown datatype) is now a type
    error, as in strict SPARQL and RDF4J. `FILTER(?a < ?b)` over IRIs returns no rows; write
    `FILTER(STR(?a) < STR(?b))` (the self-join optimization accepts both). ORDER BY still
    sorts IRIs.
  - Literals of different value classes are unequal instead of a type error
    (`"4"^^xsd:integer = "a"@en` is false, `!=` true), as in Jena; a literal whose value is
    unknown (unknown datatype, ill-typed) compared with a different term is still an error.
    `NaN = NaN` is false.
  - Date/time literals (dateTime, date, time, gYear, ...) compare as XSD values with
    timezones (`<` is a type error when indeterminate) instead of as strings.
  - Literals that `<` cannot compare sort as in RDF4J: numbers, then date/time, then by
    datatype IRI (so language-tagged strings come before plain strings).
  - `REGEX` requires a string or language-tagged text and a plain string pattern (numbers,
    IRIs and custom datatypes are a type error), as in SPARQL, RDF4J and Jena.

### Fixed
- **A malformed depot record no longer takes the indexer down.** The depot accepts any object, and a
  wrong-shaped `*triple-depot` record (missing terms, a non-string term, a non-integer tx-time) threw
  in the indexer, which crashed the worker and was retried forever. The indexer now drops such
  records with a warning. `rama-sail.core/valid-depot-record?` lets direct depot writers check a
  record before appending it.
- **Clearing a large graph no longer freezes the indexer.** `CLEAR GRAPH`, `DROP GRAPH`, `clear()`
  and `:clear-context` depot records deleted every quad of the graph in one microbatch. A microbatch
  that runs past Rama's phase timeout is retried from the start with the same data, so clearing a
  large graph (40,000 quads on a 4-task in-process cluster) never finished and blocked every later
  write. A clear is now registered in a new `$$pending-clears` PState and applied in bounded slices,
  up to 1,000 quads per task per microbatch (`clear-quads-per-microbatch`). Behavior change: a graph
  larger than that empties over several microbatches, so readers can see it partly cleared; the new
  `pending-clear-count` query topology returns 0 when every clear is fully applied, and
  `:sync-commits` waits for it. Module-side: update the module (it adds a PState and a query
  topology). `:del` and clears now share one `delete-quad` operation.
- Bench loader lost data silently: unacknowledged appends overran the depot's append buffer, so
  only 46,393 of the 59,328 triples in the 1,000-product dataset were indexed. It now sends
  acknowledged appends through a sliding window with retries and throws if any append still fails.
  Cluster runs wait for the indexer instead of sleeping for a fixed time, and the reported load time
  now covers the whole load up to fully indexed.
- **Wrong results: ORDER BY, MIN and MAX compared serialized N-Triples strings** (W5):
  `"New York"` sorted before `"New"`, `<item10>` before `<item1>`, `MIN(<a>, "x")` was
  `"x"`, and dates compared as text. They now use term values. Module-side change: redeploy
  the module.
- **Wrong results: REGEX matched escaped text** (W4): `REGEX(?x, "\\d")` never matched a
  digit (also `\\s`, `\\w`, quotes, newlines). Text and pattern are now unescaped.
- **Boolean results vs boolean literals** (W6): `BOUND(?x) = false` was false when `?x` was
  unbound; `BOUND(?x) = true`, `… IN (true)`, `sameTerm(BOUND(?x), true)` threw
  `ClassCastException` (aborting BIND/ORDER BY); `LANG(true)` was an error.
- **Locale-dependent canonical numbers** (W9): a JVM with e.g. a German default locale stored
  `"1,0"^^xsd:double`; language ranges were lower-cased with the default locale (Turkish
  dotless i). Canonical forms now use `Locale/ROOT`.
- **Data loss: CLEAR/DROP after a bulk commit left most quads behind.** Each op of a commit
  was stamped one millisecond later than the previous one, dating large commits seconds into
  the future, so a subsequent clear (which deletes only quads not newer than itself) skipped
  them. A commit now uses at most two stamps regardless of size.
- **Data loss: `CLEAR GRAPH g` + re-inserting unchanged quads in one transaction deleted
  them** (the "replace graph" pattern). Re-adding an existing quad now refreshes its creation
  stamp. Module-side change: redeploy the module.
- **Data loss: `remove(s, null, null, (Resource) null)` inside a transaction also cancelled
  uncommitted adds of matching statements in every named graph**; a null context now means
  the default graph only.
- **Wrong results: self-joins dropped every `?a = ?b` row.** `?a :p ?o . ?b :p ?o` (and the
  same with `FILTER(?a <= ?b)` / `>=`) never returned a triple joined with itself, so a
  predicate with unique objects returned no rows at all. Module-side change: redeploy the module.
- **Wrong results: `SELECT DISTINCT … ORDER BY … LIMIT n` returned arbitrary rows.** DISTINCT
  discarded the ORDER BY order; it now keeps the first occurrence of each row in order.
  Module-side change.
- **Wrong results: `GRAPH ?g { … }` also matched the default graph** (with `?g` left unbound).
  It now ranges over named graphs only; property paths inside `GRAPH` fall back to RDF4J
  until the path operators carry a graph. Module-side change.
- **Wrong results: the optimizer treated variables that may be unbound as always bound.**
  Four rewrites could change answers on queries with OPTIONAL, UNION, BIND, VALUES or
  subqueries: a FILTER pushed onto an OPTIONAL side (dropping rows); a property lookup
  rewritten so it overwrote an OPTIONAL-bound value or dropped rows whose lookup variable was
  unbound; join reordering keying hash joins on possibly-unbound variables; and a self-join
  FILTER folded into a subquery that hides its variables. All rewrites now use one shared
  always-bound / possibly-bound analysis. Client-side only.
- **Wrong results: self-join `<`/`<=`/`>`/`>=` paired blank nodes and triple terms** by
  string order, while the same FILTER evaluated normally is a type error for them. The
  self-join now uses the expression engine's comparator, so it always agrees with normal
  evaluation (since the comparator change above, ordering subjects keeps no pair; use
  `STR(?a) < STR(?b)`). Module-side change: redeploy the module.
- **Wrong results: two OPTIONALs sharing a variable could be applied in the wrong order.**
  When the optimizer folds a chain of OPTIONALs into one multi-left-join, the optionals ran
  concurrently and were applied in whichever order they finished; OPTIONALs that share a
  variable do not commute, so e.g. `OPTIONAL { … ?a } OPTIONAL { ?a :s ?e }` could return the
  second pattern's rows instead of the first's. Module-side change: redeploy the module.
- **Wrong results: UNION (and projected subqueries) could drop duplicate rows**, depending on
  timing: when a branch that returns a set (an empty group `{}`, or a basic graph pattern)
  finished first, the result combiner de-duplicated the other branch. Module-side change:
  redeploy the module.
- **Query planning could crash with `ArithmeticException: long overflow`.** The cost model's
  capped multiplication fell back to checked long arithmetic, so two large estimates (e.g. a
  cross product inside a join chain) overflowed instead of capping. BSBM QJ1/QJ3 failed to plan
  whenever no statistics were available (e.g. a fresh store). Estimates are now capped in
  floating point.
- Repeated variables in a pattern (`?x <knows> ?x`) no longer match everything.
- ORDER BY no longer crashes on mixed-type columns; term-type ordering follows the spec.
- Self-join FILTER rewrite no longer moves filters incorrectly through UNION / OPTIONAL / SLICE.
- Unsupported constructs (FunctionCall, `IN`, non-BGP triple-ref partners, aliasing projections
  such as `SELECT (?x AS ?y)` and CONSTRUCT) now fail fast and fall back to RDF4J instead of
  silently returning empty results.
- `FROM` / `FROM NAMED` dataset clauses are no longer silently ignored.
- Joins no longer drop rows with unbound join variables.
- `hasStatement` wildcard-context read-your-own-writes hole.
- Indexer statistics races, missing `$$global-stats` counter maintenance, and un-pruned empty
  containers after deletes; `:clear-ns` now clears all partitions; sync-commit barrier derived
  from depot end-offsets.
- Two N-Triples serializer round-trip bugs: NUL-character (U+0000) corruption, and a
  `StringIndexOutOfBounds` crash on `>>` inside a literal within a nested `<< … >>` term.
- `get-plan-vars` miscomputed variables for `:bind` and `:group` (latent join-planning bug).
- LIMIT pushdown fenced to semantics-preserving cases; join-order DP capped (Θ(3ⁿ) → greedy above
  12 relations) with planning moved inside the query timeout.

### Removed
- Dead BNode-skolemization machinery and the unused `bnode-map` transaction atom.

## 0.1.0 — Initial implementation

### Added
- Quad storage with four index PStates (SPOC, POSC, OSPC, CSPO)
- SPARQL query engine: BGP, JOIN, OPTIONAL, UNION, FILTER, ORDER BY, GROUP BY, BIND, VALUES, DISTINCT, LIMIT/OFFSET, ASK, CONSTRUCT, DESCRIBE
- RDF-Star triple term support — storage, query, and decomposition of `<< s p o >>` terms
- Property paths: `+` (one-or-more), `*` (zero-or-more), `?` (zero-or-one), `/` (sequence), `|` (alternative), `^` (inverse)
- Query optimizer: cardinality-based join ordering, filter pushdown, LIMIT pushdown, self-join optimization, colocated subject joins, batch property lookups
- Per-predicate and global cardinality statistics for adaptive query planning
- SPARQL HTTP endpoint (W3C SPARQL Protocol compatible)
- RDF4J SAIL compliance: 40/40 RDFStoreTest passing
- W3C SPARQL 1.1 property path conformance: 29/33 tests passing
- W3C SPARQL 1.2 triple term conformance: 5/41 tests passing (36 quarantined pending RDF4J parser support)
- Prometheus observability: `/metrics`, `/health`, query latency histograms, connection tracking
- Transaction support with snapshot isolation, rollback, and idempotent add/delete
- Namespace prefix management with SPARQL Protocol support
- Physical deletes with index removal across all four indices
- Composable module building blocks for extending the base module
