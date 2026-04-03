# rama-sail-graph

> **Status: Public Preview** — This project is functional and tested but not yet production-hardened. APIs may change. Feedback and contributions welcome.

A Rama-backed RDF quad store with SPARQL query evaluation and HTTP endpoint.

## Overview

rama-sail-graph integrates the [Rama](https://redplanetlabs.com/rama) distributed data processing framework with the [RDF4J](https://rdf4j.org/) SAIL API, enabling distributed RDF quad storage with SPARQL query support.

### Features

- **Quad Storage**: Four index PStates (SPOC, POSC, OSPC, CSPO) for efficient lookups across any access pattern
- **SPARQL Query Engine**: Server-side query execution via Rama topologies — joins, filters, aggregates, ORDER BY, OPTIONAL, UNION, BIND, VALUES, ASK
- **RDF-Star / SPARQL 1.2 Triple Terms**: Store and query triple terms (`<< s p o >>`) as subjects and objects — enables provenance, confidence, and temporal annotations
- **Query Optimization**: Cardinality-based join ordering, self-join optimization, colocated subject joins, batch property lookups, materialized type views
- **Statistics**: Per-predicate and global cardinality tracking for adaptive query planning
- **Physical Deletes**: Idempotent add/delete with physical index removal
- **SPARQL HTTP Endpoint**: W3C SPARQL Protocol compatible, works with tools like gdotv
- **Observability**: Prometheus metrics (`/metrics`), health checks (`/health`), query latency histograms, connection tracking
- **Namespace Management**: Full prefix/IRI namespace support

## Quick Start

### Prerequisites

- Java 17+
- [Leiningen](https://leiningen.org/)
- Rama 1.6.0 (see [Rama installation](https://redplanetlabs.com/docs))

### Open Source And Commercial Use

This project is open source under the Apache License 2.0.

You can use, modify, and ship this code in commercial products, subject to Apache 2.0 notice requirements. However, this project depends on Rama, which has its own platform licensing terms. Per Red Planet Labs, Rama includes an embedded free license for clusters up to two Supervisor nodes; larger clusters require a separate paid license.

### Build & Test

```bash
# Check compilation (~30s)
lein check

# Run unit tests (~5s, no Rama cluster needed)
lein test :only rama-sail.sail.optimizer-test rama-sail.sail.adapter-test

# Run full test suite (~2-5min, starts in-process Rama cluster)
lein test

# Run RDF4J SAIL compliance tests only
lein test :only rama-sail.sail.compliance-test

# Run RDF-star / triple term integration tests
lein test :only rama-sail.sail.rdf-star-test

# Run W3C SPARQL 1.2 eval-triple-terms conformance suite
lein test :only rama-sail.sail.w3c.sparql12-test
```

### SPARQL HTTP Endpoint

```bash
# Start with in-process cluster (dev mode)
lein run -m rama-sail.server.sparql -- --port 7200 --mode ipc

# Start with external Rama cluster
lein run -m rama-sail.server.sparql -- --port 7200 --mode cluster --host localhost --rama-port 1973

# Query
curl -G http://localhost:7200/sparql --data-urlencode "query=SELECT * WHERE { ?s ?p ?o } LIMIT 10"
```

> **Note:** The SPARQL endpoint is intended for development and internal use. It does not currently include authentication, rate limiting, or transport security.

## Usage

### Rama Module (Direct)

```clojure
(require '[com.rpl.rama :as rama])
(require '[rama-sail.core :as core])

;; In-process cluster (dev/test)
(with-open [ipc (com.rpl.rama.test/create-ipc)]
  (com.rpl.rama.test/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
  (let [module-name (rama/get-module-name core/RdfStorageModule)
        depot (rama/foreign-depot ipc module-name "*triple-depot")]
    ;; Add triples
    (rama/foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" "::rama-internal::default-graph"]])
    ;; Query
    (rama/foreign-invoke-query
      (rama/foreign-query ipc module-name "find-triples")
      "<alice>" nil nil nil)))
```

### RDF4J SAIL API

```clojure
(require '[rama-sail.sail.adapter :as sail])
(import '[org.eclipse.rdf4j.repository.sail SailRepository])

(let [rsail (sail/create-rama-sail ipc module-name)
      repo (SailRepository. rsail)]
  (.init repo)
  (with-open [conn (.getConnection repo)]
    ;; Use standard RDF4J API for SPARQL queries
    (let [query (.prepareTupleQuery conn "SELECT * WHERE { ?s ?p ?o } LIMIT 10")]
      (with-open [result (.evaluate query)]
        (doseq [bs (iterator-seq result)]
          (println bs))))))
```

### Triple Terms (RDF-Star / SPARQL 1.2)

Triple terms allow statements about statements — attach provenance, confidence scores, or temporal metadata directly to triples:

```clojure
;; Store a triple with a confidence annotation
(rama/foreign-append! depot [:add ["<< <alice> <knows> <bob> >>" "<confidence>" "\"0.95\"^^<http://www.w3.org/2001/XMLSchema#double>" "::rama-internal::default-graph"]])
```

```sparql
# Find all annotations on a specific triple
SELECT ?pred ?val WHERE {
  << <http://ex/alice> <http://ex/knows> <http://ex/bob> >> ?pred ?val .
}

# Extract components from annotated triples
SELECT ?s ?p ?o ?confidence WHERE {
  << ?s ?p ?o >> <http://ex/confidence> ?confidence .
  FILTER(?confidence > 0.8)
}

# Provenance tracking via SPARQL UPDATE
INSERT { << ?s ?p ?o >> <http://ex/source> ?g }
WHERE { GRAPH ?g { ?s ?p ?o } }
```

Triple terms are stored as first-class values in all four quad indexes, using `<< <s> <p> <o> >>` canonical serialization. Nested triple terms are supported to arbitrary depth.

## Architecture

### Storage Layer (`core.clj`, `module/`)

Rama module with quad-based storage using four complementary indices. The module is defined in `core.clj` and assembled from composable building blocks in `module/indexer.clj` (PStates, ETL), `module/namespaces.clj` (prefix management), and `module/queries.clj` (query topologies):

| Index | Structure | Optimized For |
|-------|-----------|---------------|
| `$$spoc` | S -> P -> O -> {C} | Subject-based lookups |
| `$$posc` | P -> O -> S -> {C} | Predicate/object lookups |
| `$$ospc` | O -> S -> P -> {C} | Object-based lookups |
| `$$cspo` | C -> S -> P -> {O} | Named graph queries |

Deletion uses physical index removal — deleted quads are removed from all four indices immediately. Adds and deletes are idempotent (set semantics for indices, `$$quad-tx-time` tracking for statistics correctness).

### Query Topologies

| Topology | Purpose |
|----------|---------|
| `find-triples` | Pattern matching across indices |
| `find-bgp` | Basic Graph Pattern evaluation |
| `join` / `left-join` | Hash join / left outer join |
| `union` | UNION operator |
| `filter` | Expression evaluation (distributed) |
| `project` / `distinct` / `slice` | Projection, dedup, pagination |
| `order` | ORDER BY with ASC/DESC |
| `bind` | BIND expressions |
| `group` | GROUP BY with aggregates (COUNT, SUM, AVG, MIN, MAX) |
| `self-join` | Optimized same-predicate joins |
| `colocated-subject-join` | Partition-local subject joins |
| `batch-lookup` | Batch property fetching |
| `ask-result` | Boolean ASK query support |
| `execute-plan` | Recursive plan executor |

### Triple Term Support

Triple terms (`<< s p o >>`) are handled across the stack:

| Layer | What | How |
|-------|------|-----|
| **Storage** | Triple terms in S/O positions | Stored as canonical strings in all 4 indexes, O(1) hash lookups |
| **Compilation** | `TripleRef` algebra nodes | Rewritten to filter+bind plans with component extraction |
| **Expressions** | `:triple-subject/predicate/object` | Extract components from bound triple term values |
| **Serialization** | RDF4J `Triple` <-> string | `val->str` / `str->val` with nested term support |

### RDF4J SAIL Compliance

RamaSail passes 40 of 40 tests in the RDF4J **RDFStoreTest** compliance suite (`rdf4j-sail-testsuite` 5.2.0), which validates:

- Statement CRUD (add, remove, get, has) with pattern matching and wildcards
- Value round-trips (IRIs, BNodes, typed literals, decimals, timezones, long URIs/literals)
- Named graph / context management (add to context, list contexts, clear)
- Transaction semantics (commit, rollback, isolation)
- Namespace management (get, set, remove, clear)
- Duplicate handling and statement counting
- Concurrent add-while-querying
- Dual connections — two separate `SailConnection` instances operating concurrently on the same store, each maintaining independent transaction state
- BNode reuse — blank node identity preserved when the same BNode is used across multiple statements within a connection

**SPARQL operators** handled server-side via Rama topologies:

| Supported | Falls Back To RDF4J Local Evaluation | Explicitly Unsupported / Not Yet Working |
|-----------|--------------------------------------|------------------------------------------|
| SELECT, ASK, DESCRIBE, CONSTRUCT | MINUS | SERVICE (federated queries) |
| JOIN, LEFT JOIN (OPTIONAL), UNION | | Property paths (for example `+` / `*`) |
| FILTER (comparisons, logic, regex, IN) | | RDF 1.2 `rdf:reifies` semantics |
| GROUP BY, HAVING, aggregates | | RDF 1.2 `<<( )>>` syntax (needs RDF4J upgrade) |
| ORDER BY, DISTINCT, REDUCED, LIMIT/OFFSET | | SPARQL 1.2 `{| |}` annotation syntax |
| BIND, VALUES, COALESCE, IF | | |
| STR, LANG, DATATYPE, LANGMATCHES | | |
| ISIRI, ISBNODE, ISLITERAL, ISNUMERIC | | |
| SAMETERM, REGEX | | |
| Triple term patterns (`<< s p o >> ?p ?o`) | | |
| TripleRef decomposition (`<< ?s ?p ?o >>`) | | |

Some unsupported operators are automatically handled by falling back to RDF4J's `DefaultEvaluationStrategy`, which evaluates locally via `getStatements`. This ensures correct results but without distributed execution benefits. `SERVICE` is a special case and is currently rejected explicitly rather than evaluated through fallback, and property-path operators are not yet a reliable fallback path.

REPL verification showed that basic nested `SELECT` subqueries can compile into Rama plans and run server-side, so subquery support is at least partial and should not be described as fallback-only.

### W3C SPARQL 1.2 Conformance

The project includes a vendored copy of the W3C [eval-triple-terms](https://github.com/w3c/rdf-tests/tree/main/sparql/sparql12/eval-triple-terms) test suite (41 tests) in `test/resources/w3c/sparql12/eval-triple-terms/`.

| Result | Count | Details |
|--------|-------|---------|
| **Pass** | 5 | `basic-4`, `basic-5`, `basic-6`, `pattern-9`, `update-1` |
| **Quarantined** | 36 | Blocked on RDF4J parser limits (30) or missing `rdf:reifies` (6) |
| **Unexpected failures** | 0 | |

Quarantine breakdown:

| Category | Count | Blocked on |
|----------|-------|------------|
| `<<( s p o )>>` triple term syntax in data | 10 | RDF4J 5.2.x parser (no released RDF 1.2 support) |
| `~ :reifier` syntax in data | 9 | RDF4J 5.2.x parser |
| SPARQL 1.2 query syntax (`{| |}`, `TRIPLE()`) | 7 | RDF4J 5.2.x SPARQL parser |
| Mixed data + query syntax | 4 | Both parser limitations |
| `rdf:reifies` semantics | 6 | Not yet implemented in RamaSail |

The quarantined tests will become unblocked as RDF4J ships RDF 1.2 parser support (no released version exists as of April 2026) and as `rdf:reifies` semantics are implemented.

### Observability

Prometheus metrics are exposed at `/metrics` when running the SPARQL endpoint:

- `ramasail_query_latency_seconds` — query execution latency histogram
- `ramasail_queries_total` — query count by status (success, timeout, error, fallback)
- `ramasail_active_connections` — active SAIL connections gauge
- `ramasail_query_result_size` — result size summary (p50, p90, p99)
- `ramasail_transactions_total` — transaction count by status
- `ramasail_transaction_ops_total` — triple operations by type (add, del)

A `/health` endpoint is also available for liveness checks.

## Benchmarking

### BSBM Benchmark Suite

The project includes a [Berlin SPARQL Benchmark (BSBM)](https://dblp.org/rec/journals/ijswis/BizerS09.html)-compatible benchmark suite with synthetic data generation. Implements 9 of 12 original BSBM SELECT queries (Q1–Q5, Q7, Q8, Q10, Q11) plus 4 custom join-focused queries. Q6 (INSERT), Q9 (DESCRIBE), and Q12 (CONSTRUCT) are omitted as they test write/export operations rather than read performance. Benchmarks can run against an in-process cluster (IPC) for development or a real Rama cluster for production-like numbers.

### Generate Datasets

Datasets must be generated before running benchmarks:

```bash
lein with-profile +dev run -m clojure.main -e "
(require '[rama-sail.bench.bsbm.infra.data-generator :refer [generate-standard-datasets!]])
(generate-standard-datasets!)
"
```

This creates three datasets in `test/resources/bsbm/`:

| File | Products | Triples |
|------|----------|---------|
| `dataset_100.nt` | 100 | ~6K |
| `dataset_500.nt` | 500 | ~30K |
| `dataset_1000.nt` | 1,000 | ~59K |

### IPC Benchmarks (No Cluster Required)

```bash
lein test :only rama-sail.bench.bsbm.bsbm-bench/test-bsbm-small
```

### Cluster Benchmarks

#### 1. Set up a local single-node Rama cluster

```bash
cd $RAMA_HOME

# Start ZooKeeper, conductor, and supervisor (each in background)
./rama devZookeeper &
sleep 5
./rama conductor &
sleep 10
./rama supervisor &
sleep 10

# Verify cluster is ready
./rama conductorReady        # should print: true
./rama numSupervisors        # should print: 1
```

#### 2. Build and deploy the module

```bash
# From the project directory
lein uberjar

# From the Rama installation directory
cd $RAMA_HOME
./rama deploy --action launch \
  --jar /path/to/rama-sail-graph/target/rama-sail-graph-0.1.0-SNAPSHOT-standalone.jar \
  --module "rama-sail.core/RdfStorageModule" \
  --tasks 4 --threads 2 --workers 1

# Verify module is running
./rama moduleStatus "rama-sail.core/RdfStorageModule"
```

#### 3. Run benchmarks

```bash
# From the project directory

# Load data + run all queries including joins
lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :load true :joins true

# Re-run without reloading data
lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :joins true

# Joins only
lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :joins-only true

# Custom dataset and iteration count
lein run -m rama-sail.bench.cluster.cluster-bench \
  :host localhost \
  :dataset test/resources/bsbm/dataset_1000.nt \
  :load true \
  :iterations 100 \
  :joins true
```

### Benchmark Queries

The suite includes 9 BSBM queries and 4 join-focused queries:

| Query | Description | Operators |
|-------|-------------|-----------|
| Q1 | Find products by type and features | BGP, JOIN, FILTER |
| Q2 | Retrieve product information | BGP, LEFT JOIN, UNION |
| Q3 | Products with features and numeric constraints | BGP, JOIN, FILTER |
| Q4 | Products matching two feature sets | BGP, UNION, FILTER |
| Q5 | Similar products | BGP, JOIN, FILTER |
| Q7 | Product reviews with reviewer details | BGP, JOIN, ORDER BY |
| Q8 | Recent reviews for a product | BGP, JOIN, ORDER BY, LIMIT |
| Q10 | Cheap offers with fast delivery | BGP, JOIN, FILTER, ORDER BY |
| Q11 | Offer details with union patterns | BGP, UNION |
| QJ1 | 3-way join: products → producers → offers → vendors | Multi-way JOIN |
| QJ2 | Star join: products with features and properties | Star JOIN |
| QJ3 | Chain join: reviews → reviewers → products → producers | Chain JOIN |
| QJ4 | Self-join: product pairs from same producer | Self JOIN |

### Reference Results

These benchmark results were recorded on this development machine:

- Host: MacBook Pro
- Chip: Apple M5
- CPU: 10 cores (4 performance, 6 efficiency)
- Memory: 24 GB
- OS: macOS 26.4 
- Runtime/config: Rama 1.6.0, single-node cluster, 6 GB heap, 4 tasks, 2 threads
- Benchmark settings: 50 iterations, 5 warmup

#### 6K Triples (100 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 26.43 | 97.45 | 168.15 | 35.89 | 0.0 |
| Q2 | 9.58 | 31.05 | 78.85 | 12.80 | 3.3 |
| Q3 | 45.17 | 109.28 | 168.22 | 51.64 | 3.6 |
| Q4 | 42.80 | 111.54 | 187.03 | 50.97 | 1.6 |
| Q5 | 62.89 | 159.77 | 176.09 | 74.15 | 4.7 |
| Q7 | 160.36 | 233.29 | 351.72 | 176.10 | 5.5 |
| Q8 | 92.62 | 145.91 | 169.98 | 102.97 | 0.0 |
| Q10 | 78.60 | 145.92 | 220.08 | 90.29 | 0.0 |
| Q11 | 1.07 | 80.18 | 101.64 | 8.14 | 5.0 |
| QJ1 | 148.99 | 212.05 | 217.03 | 163.62 | 100.0 |
| QJ2 | 46.08 | 147.32 | 164.02 | 61.38 | 2.5 |
| QJ3 | 132.12 | 175.09 | 277.64 | 144.08 | 100.0 |
| QJ4 | 77.62 | 98.08 | 98.95 | 81.24 | 100.0 |

#### 59K Triples (1,000 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 26.73 | 134.43 | 562.69 | 52.42 | 0.0 |
| Q2 | 9.22 | 20.04 | 65.96 | 11.84 | 3.1 |
| Q3 | 46.06 | 168.08 | 698.92 | 89.63 | 4.2 |
| Q4 | 43.69 | 189.10 | 650.29 | 72.44 | 1.5 |
| Q5 | 65.24 | 525.02 | 705.95 | 121.77 | 4.5 |
| Q7 | 169.42 | 583.64 | 766.94 | 221.93 | 8.6 |
| Q8 | 87.24 | 159.37 | 245.30 | 99.48 | 0.0 |
| Q10 | 78.07 | 141.51 | 179.79 | 87.23 | 0.0 |
| Q11 | 0.94 | 1.22 | 1.32 | 0.95 | 5.0 |
| QJ1 | 148.90 | 225.29 | 345.31 | 165.09 | 100.0 |
| QJ2 | 46.44 | 99.90 | 223.75 | 55.61 | 2.8 |
| QJ3 | 132.94 | 180.43 | 276.93 | 147.07 | 100.0 |
| QJ4 | 77.79 | 100.58 | 152.55 | 83.41 | 100.0 |

#### Scaling: p50 Comparison (6K → 59K)

| Query | 6K p50 | 59K p50 | Slowdown |
|-------|--------|---------|----------|
| Q1 | 26.4 | 26.7 | 1.0x |
| Q2 | 9.6 | 9.2 | 1.0x |
| Q3 | 45.2 | 46.1 | 1.0x |
| Q4 | 42.8 | 43.7 | 1.0x |
| Q5 | 62.9 | 65.2 | 1.0x |
| Q7 | 160.4 | 169.4 | 1.1x |
| Q8 | 92.6 | 87.2 | 0.9x |
| Q10 | 78.6 | 78.1 | 1.0x |
| Q11 | 1.1 | 0.9 | 0.9x |
| QJ1 | 149.0 | 148.9 | 1.0x |
| QJ2 | 46.1 | 46.4 | 1.0x |
| QJ3 | 132.1 | 132.9 | 1.0x |
| QJ4 | 77.6 | 77.8 | 1.0x |

With query optimizations (LIMIT pushdown, adaptive filter selectivity, AND decomposition for filter pushdown), p50 latencies are now nearly flat across 6K to 59K triples. The previous worst scaler (QJ4 at 15.8x) now shows 1.0x scaling.

### Query Optimizations

The following optimizations are applied automatically during query planning:

- **LIMIT pushdown**: Propagates LIMIT through joins and self-joins for early termination, preventing unbounded result generation
- **Adaptive filter selectivity**: Estimates filter selectivity by operator type (equality=0.05, range=0.3, inequality=0.8) instead of a flat 0.3, improving join ordering decisions
- **AND decomposition**: Splits compound AND filters into independent conjuncts that can be pushed separately to each join side
- **Selinger-style join ordering**: Dynamic programming optimizer for multi-way join chains using predicate statistics
- **Self-join optimization**: Detects same-predicate joins with inequality filters and uses group-and-pair generation instead of Cartesian product
- **Colocated subject joins**: Exploits subject hash partitioning for partition-local joins
- **Batch property enrichment**: Replaces N individual property lookups with single batch query
- **Materialized type views**: Pre-computed indices for `rdf:type` pattern lookups

## Known Limitations

- **Query fallback**: Some unsupported SPARQL operators fall back to RDF4J's local evaluation strategy, which may not scale for large datasets; `SERVICE` is explicitly unsupported and property paths are not yet a reliable fallback path
- **RDF 1.2 partial**: Triple terms work via RDF-star (`<< s p o >>`), but RDF 1.2 `<<( s p o )>>` syntax, `~ :reifier` syntax, `{| |}` annotations, and `rdf:reifies` semantics are not yet supported. 5/41 W3C SPARQL 1.2 conformance tests pass; 36 are quarantined
- **Triple term decomposition is client-side**: Extracting subject/predicate/object from triple terms uses string parsing on the SAIL client, not distributed Rama topologies. Constant triple-term lookups are O(1) distributed, but variable decomposition and component filtering are post-fetch
- **Query timeout**: Cancellation is best-effort; Rama cluster queries may continue after client timeout
- **SPARQL endpoint**: No authentication, rate limiting, or TLS — intended for development use
- **No CI**: Tests are run locally; CI configuration is not yet included

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
