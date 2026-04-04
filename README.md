# rama-sail-graph

[![CI](https://github.com/Shtanglitza/rama-sail-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/Shtanglitza/rama-sail-graph/actions/workflows/ci.yml)

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

### API Stability

| Tier | Namespaces | Guarantee |
|------|-----------|-----------|
| **Stable** | `rama-sail.core`, `rama-sail.sail.adapter`, `rama-sail.server.connection`, `rama-sail.server.sparql` | Semver-protected. Breaking changes require major version bump. |
| **Extension** | `rama-sail.module.*`, `rama-sail.metrics`, `rama-sail.metrics-server` | Documented and supported, may evolve with notice in CHANGELOG. |
| **Internal** | `rama-sail.query.*`, `rama-sail.sail.compilation`, `rama-sail.sail.optimization`, `rama-sail.sail.serialization`, `rama-sail.errors` | No stability guarantees. May change without notice. |

## Quick Start

### Prerequisites

- Java 17+
- [Leiningen](https://leiningen.org/)
- Rama 1.6.0 (see [Rama installation](https://redplanetlabs.com/docs))

### Open Source And Commercial Use

This project is open source under the Apache License 2.0.

You can use, modify, and ship this code in commercial products, subject to Apache 2.0 notice requirements. However, this project depends on Rama, which has its own platform licensing terms. Per Red Planet Labs, Rama includes an embedded free license for clusters up to two Supervisor nodes; larger clusters require a separate paid license.

### Installation

**deps.edn (git dependency):**
```clojure
{:deps {io.github.Shtanglitza/rama-sail-graph
        {:git/tag "v0.1.0" :git/sha "<sha>"}}}
```

**Leiningen (local JAR):**

Download the JAR from [GitHub Releases](https://github.com/Shtanglitza/rama-sail-graph/releases) and install locally:
```bash
mvn install:install-file -Dfile=rama-sail-graph-0.1.0.jar -DgroupId=com.shtanglitza -DartifactId=rama-sail-graph -Dversion=0.1.0 -Dpackaging=jar
```

Then add to `project.clj`:
```clojure
[com.shtanglitza/rama-sail-graph "0.1.0"]
```

> **Note:** Rama 1.6.0 is required at runtime. Add the [Red Planet Labs Maven repository](https://nexus.redplanetlabs.com/repository/maven-public-releases) to resolve the Rama dependency.

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

# Run W3C SPARQL 1.1 property path conformance suite
lein test :only rama-sail.sail.w3c.sparql11-property-path-test
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
| `arbitrary-length-path` | Property path `+`/`*` (transitive closure) |
| `zero-length-path` | Property path `?` (zero-or-one) |
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
| JOIN, LEFT JOIN (OPTIONAL), UNION | | RDF 1.2 `rdf:reifies` semantics |
| FILTER (comparisons, logic, regex, IN) | | RDF 1.2 `<<( )>>` syntax (needs RDF4J upgrade) |
| GROUP BY, HAVING, aggregates | | SPARQL 1.2 `{| |}` annotation syntax |
| ORDER BY, DISTINCT, REDUCED, LIMIT/OFFSET | | |
| BIND, VALUES, COALESCE, IF | | |
| STR, LANG, DATATYPE, LANGMATCHES | | |
| ISIRI, ISBNODE, ISLITERAL, ISNUMERIC | | |
| SAMETERM, REGEX | | |
| Property paths (`+`, `*`, `?`, `/`, `\|`, `^`) | | |
| Triple term patterns (`<< s p o >> ?p ?o`) | | |
| TripleRef decomposition (`<< ?s ?p ?o >>`) | | |

Some unsupported operators are automatically handled by falling back to RDF4J's `DefaultEvaluationStrategy`, which evaluates locally via `getStatements`. This ensures correct results but without distributed execution benefits. `SERVICE` is a special case and is currently rejected explicitly rather than evaluated through fallback.

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

### W3C SPARQL 1.1 Property Path Conformance

The project includes a vendored copy of the W3C [property-path](https://github.com/w3c/rdf-tests/tree/main/sparql/sparql11/property-path) test suite (33 tests) in `test/resources/w3c/sparql11/property-path/`.

| Result | Count | Details |
|--------|-------|---------|
| **Pass** | 29 | Sequence, alternative, inverse, transitive (`+`/`*`), zero-or-one (`?`), cycles, diamonds, negated property sets, operator precedence |
| **Quarantined** | 4 | 3 named graph tests, 1 nested star `(*)*` |
| **Unexpected failures** | 0 | |

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
| Q1 | 30.98 | 95.01 | 108.78 | 42.39 | 0.0 |
| Q2 | 10.34 | 15.07 | 68.26 | 11.89 | 3.2 |
| Q3 | 48.97 | 112.19 | 129.99 | 54.77 | 3.8 |
| Q4 | 49.07 | 119.00 | 150.66 | 56.76 | 1.1 |
| Q5 | 68.15 | 132.60 | 138.83 | 76.76 | 4.6 |
| Q7 | 182.21 | 284.50 | 373.69 | 198.67 | 7.8 |
| Q8 | 97.55 | 173.29 | 259.01 | 111.92 | 0.0 |
| Q10 | 86.06 | 142.91 | 144.33 | 92.42 | 0.0 |
| Q11 | 0.98 | 1.04 | 1.14 | 0.97 | 5.5 |
| QJ1 | 162.49 | 233.83 | 241.95 | 177.10 | 100.0 |
| QJ2 | 58.91 | 156.81 | 169.85 | 68.10 | 2.7 |
| QJ3 | 146.18 | 203.05 | 243.12 | 159.83 | 100.0 |
| QJ4 | 81.54 | 104.50 | 181.00 | 88.02 | 100.0 |

#### 59K Triples (1,000 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 28.42 | 131.37 | 134.57 | 40.34 | 0.0 |
| Q2 | 7.33 | 8.43 | 17.76 | 7.61 | 3.3 |
| Q3 | 48.98 | 122.81 | 222.04 | 58.03 | 4.6 |
| Q4 | 49.05 | 117.05 | 119.68 | 54.92 | 2.2 |
| Q5 | 65.92 | 167.55 | 188.96 | 77.81 | 4.6 |
| Q7 | 176.22 | 277.94 | 348.82 | 195.34 | 5.2 |
| Q8 | 97.07 | 166.19 | 179.38 | 109.81 | 0.0 |
| Q10 | 101.29 | 178.64 | 208.85 | 116.89 | 0.0 |
| Q11 | 1.69 | 3.05 | 88.63 | 3.95 | 5.5 |
| QJ1 | 162.50 | 239.36 | 342.28 | 179.14 | 100.0 |
| QJ2 | 52.61 | 91.75 | 155.22 | 60.67 | 2.5 |
| QJ3 | 148.59 | 195.12 | 222.34 | 159.63 | 100.0 |
| QJ4 | 81.15 | 105.27 | 106.30 | 84.77 | 100.0 |

#### Scaling: p50 Comparison (6K → 59K)

| Query | 6K p50 | 59K p50 | Slowdown |
|-------|--------|---------|----------|
| Q1 | 31.0 | 28.4 | 0.9x |
| Q2 | 10.3 | 7.3 | 0.7x |
| Q3 | 49.0 | 49.0 | 1.0x |
| Q4 | 49.1 | 49.1 | 1.0x |
| Q5 | 68.2 | 65.9 | 1.0x |
| Q7 | 182.2 | 176.2 | 1.0x |
| Q8 | 97.6 | 97.1 | 1.0x |
| Q10 | 86.1 | 101.3 | 1.2x |
| Q11 | 1.0 | 1.7 | 1.7x |
| QJ1 | 162.5 | 162.5 | 1.0x |
| QJ2 | 58.9 | 52.6 | 0.9x |
| QJ3 | 146.2 | 148.6 | 1.0x |
| QJ4 | 81.5 | 81.2 | 1.0x |

With query optimizations (LIMIT pushdown to BGPs, adaptive filter selectivity, AND decomposition for filter pushdown), p50 latencies scale near-linearly across 6K to 59K triples. Most queries show 1.0x for a 10x data increase.

### Query Optimizations

The following optimizations are applied automatically during query planning:

- **LIMIT pushdown**: Propagates LIMIT into BGPs, joins, and self-joins for early termination — truncates quads before binding construction. Pushes through transparent operators (PROJECT, BIND), with safety multipliers for FILTER (4x) and DISTINCT (2x). Correctly blocks at ORDER BY for BGP targets (ORDER BY needs all rows to sort)
- **Adaptive filter selectivity**: Estimates filter selectivity by operator type (equality=0.05, range=0.3, inequality=0.8) instead of a flat 0.3, improving join ordering decisions
- **AND decomposition**: Splits compound AND filters into independent conjuncts that can be pushed separately to each join side
- **Selinger-style join ordering**: Dynamic programming optimizer for multi-way join chains using predicate statistics
- **Self-join optimization**: Detects same-predicate joins with inequality filters and uses group-and-pair generation instead of Cartesian product
- **Colocated subject joins**: Exploits subject hash partitioning for partition-local joins
- **Batch property enrichment**: Replaces N individual property lookups with single batch query
- **Materialized type views**: Pre-computed indices for `rdf:type` pattern lookups

## Known Limitations

- **Query fallback**: Some unsupported SPARQL operators fall back to RDF4J's local evaluation strategy, which may not scale for large datasets; `SERVICE` is explicitly unsupported
- **RDF 1.2 partial**: Triple terms work via RDF-star (`<< s p o >>`), but RDF 1.2 `<<( s p o )>>` syntax, `~ :reifier` syntax, `{| |}` annotations, and `rdf:reifies` semantics are not yet supported. 5/41 W3C SPARQL 1.2 conformance tests pass; 36 are quarantined
- **Triple term decomposition is client-side**: Extracting subject/predicate/object from triple terms uses string parsing on the SAIL client, not distributed Rama topologies. Constant triple-term lookups are O(1) distributed, but variable decomposition and component filtering are post-fetch
- **Query timeout**: Cancellation is best-effort; Rama cluster queries may continue after client timeout
- **SPARQL endpoint**: No authentication, rate limiting, or TLS — intended for development use
- **No CI**: Tests are run locally; CI configuration is not yet included

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
