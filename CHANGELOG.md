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
- SPARQL server flags: `--allow-updates`, `--auth-token`, `--cors-origin`; startup warns on
  the risky writable + wildcard-CORS + no-token combination.
- Property-path evaluation cap (`*path-materialization-limit*`): unbounded `?x :p* ?y` over a
  large store now fails fast with a clear error instead of OOM-ing the worker.
- Library infrastructure: CI workflows (unit + integration + W3C conformance), API stability
  boundaries, this changelog, and community/validation docs.
- Generative N-Triples serialization round-trip suite and metamorphic optimizer-equivalence suite.

### Changed
- Upgraded Rama 1.6.0 → 1.8.0. (Run `lein clean` after changing the Rama dependency to clear
  stale AOT bytecode.)
- **Expression engine rebuilt** with a typed numeric tower (Long/BigInteger/BigDecimal/Double)
  and SPARQL three-valued logic / type-error semantics. Behavior changes: plain/untyped literals
  no longer compare numerically (`"10" < "9"` is `true` as strings); MIN/MAX over untyped values
  order lexically; arithmetic is datatype-typed (`1+1` → `"2"^^xsd:integer`, integer/integer →
  decimal); EBV of unbound/IRI is a type error.
- Aggregates: exact SUM/AVG (no double coercion), datatype-aware MIN/MAX, correct `COUNT(DISTINCT *)`.
- Indexer: cooperative yielding (`{:allow-yield? true}`) on large subindexed scans; stat updates
  use no-read `termval` writes.

### Fixed
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
