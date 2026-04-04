# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-05

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

[Unreleased]: https://github.com/Shtanglitza/rama-sail-graph/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Shtanglitza/rama-sail-graph/releases/tag/v0.1.0
