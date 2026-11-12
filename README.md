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
- Rama 1.9.0 (see [Rama installation](https://redplanetlabs.com/docs))

### Open Source And Commercial Use

This project is open source under the Apache License 2.0.

You can use, modify, and ship this code in commercial products, subject to Apache 2.0 notice requirements. However, this project depends on Rama, which has its own platform licensing terms. Per Red Planet Labs, Rama includes an embedded free license for clusters up to two Supervisor nodes; larger clusters require a separate paid license.

### Installation

There is no published Maven/Clojars artifact yet — consume the library directly from GitHub as a git dependency.

**deps.edn (git dependency):**
```clojure
{:deps {io.github.Shtanglitza/rama-sail-graph
        {:git/sha "<full-40-char-commit-sha>"}}}
```
Pin `:git/sha` to a commit on `main`. Add a matching `:git/tag` once release tags are published.

**Leiningen:** Leiningen has no native git-dependency support. Clone the repo and install it into your local Maven cache:
```bash
git clone https://github.com/Shtanglitza/rama-sail-graph.git
cd rama-sail-graph
lein install
```
Then add the locally installed coordinate to `project.clj`:
```clojure
[rama-sail-graph "0.2.0-SNAPSHOT"]
```

> **Note:** Rama 1.9.0 is required at runtime. Add the [Red Planet Labs Maven repository](https://nexus.redplanetlabs.com/repository/maven-public-releases) to resolve the Rama dependency.

### Build & Test

```bash
# Check compilation (~30s)
lein check

# Run unit tests (no Rama cluster needed)
lein test :unit

# Run the integration tests CI runs (in-process Rama cluster)
lein test :integration

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
# Start with in-process cluster (dev mode). Read-only by default.
lein run -m rama-sail.server.sparql -- --port 7200 --mode ipc

# Start with external Rama cluster
lein run -m rama-sail.server.sparql -- --port 7200 --mode cluster --host localhost --rama-port 1973

# Enable SPARQL UPDATE (off by default), require a Bearer token, and restrict CORS
lein run -m rama-sail.server.sparql -- --port 7200 --mode ipc \
  --allow-updates --auth-token s3cret --cors-origin https://app.example

# Query
curl -G http://localhost:7200/sparql --data-urlencode "query=SELECT * WHERE { ?s ?p ?o } LIMIT 10"

# Update (only when the server was started with --allow-updates + --auth-token)
curl -X POST http://localhost:7200/sparql \
  -H "Authorization: Bearer s3cret" \
  -H "Content-Type: application/sparql-update" \
  --data 'INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }'
```

Server flags:

| Flag | Default | Purpose |
|------|---------|---------|
| `--allow-updates` | off | Enable SPARQL UPDATE; otherwise UPDATE returns `403`. |
| `--auth-token <t>` | none | When set, UPDATE requires `Authorization: Bearer <t>` (else `401`). |
| `--cors-origin <o>` | `*` | Value for `Access-Control-Allow-Origin`; set a specific origin when updates are enabled. |

> **Note:** The endpoint is **read-only by default** — a form-encoded POST is a CORS "simple request", so leaving UPDATE open would let any web page issue `DELETE WHERE { ?s ?p ?o }`. Enable writes explicitly with `--allow-updates`, and gate them with `--auth-token` and a non-wildcard `--cors-origin`. The endpoint still provides no rate limiting or transport security (TLS); front it with a reverse proxy for internet-facing deployments.

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
    ;; Add triples. A record is [op [s p o c]] or [op [s p o c] tx-time-millis], op :add / :del
    ;; (four string terms) or :clear-context (graph in the fourth position), or
    ;; [:commit [record ...]] to have up to 10,000 of them accepted all-or-nothing. The indexer
    ;; drops anything else with a warning; core/valid-depot-record? checks a record before appending.
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

### Bulk Loading

For loading a file, use `rama-sail.bulk` instead of a SAIL transaction. A transaction keeps every statement in client memory until commit; the bulk loader streams the file and appends it as `:commit` depot records, 5,000 quads per round trip, pausing when the indexer's queue is full.

```clojure
(require '[rama-sail.bulk :as bulk])

;; N-Triples, Turtle, TriG or RDF/XML, by file extension. Returns when everything is queryable.
(bulk/load-file! cluster module-name "data.nt")
;; => {:count 1005547, :time-ms ..., :triples-per-sec ..., :total-time-ms ...}

;; Apply faster for the duration of this load (store-wide while it runs, see below)
(bulk/load-file! cluster module-name "data.nt" :commit-ops-per-microbatch 60000)

;; Or from any sequence of [s p o c] canonical term strings
(bulk/load-quads! cluster module-name quads)
```

Ingest speed is set by three things, which a profile of the indexer identified (it spends most of a load committing microbatches to disk, not indexing):

- **Module launch options** (`core/indexer-launch-options`): 5,000 records per depot partition per microbatch and a PState flush batch of 100,000 paths, instead of Rama's defaults. They are applied when the module is first launched. A module that is *updated* keeps the options it was launched with: set `depot.microbatch.max.records` on `*triple-depot` and `topology.microbatch.pstate.flush.path.count` on the `indexer` topology with Rama's dynamic-option command or UI.
- **The apply budget** for queued commits, `commit-ops-per-microbatch` (default 10,000). `(core/set-indexer-option! depot :commit-ops-per-microbatch n)` changes it at runtime, and `:commit-ops-per-microbatch` on a bulk load raises it for that load. One microbatch must finish well inside Rama's microbatch phase timeout, or it is retried forever, so raise it only on a cluster where you have measured how long a microbatch takes.
- **Disk sync cost.** Every microbatch ends with RocksDB writes and fsyncs. The figures below are from a laptop running macOS, where fsync is slow.

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

A SAIL `commit()` appends the whole commit as one `[:commit [op ...]]` depot record (one per 5,000 operations for larger commits), so it is accepted all-or-nothing in one round trip. All commit records go to a single depot partition, the commit log; the indexer stores them in `$$commit-queue` and applies up to `commit-ops-per-microbatch` operations (default 10,000, adjustable at runtime) per microbatch, oldest commit first, so commits take effect in append order. A commit within that budget is fully applied in the microbatch that receives it; a larger one becomes visible over several microbatches, and `pending-commit-count` returns 0 once every commit is applied. Single-operation records appended directly to the depot are applied immediately and are not ordered against queued commits.

Clearing a graph (`CLEAR GRAPH`, `DROP GRAPH`, `clear()`, or a `:clear-context` depot record) is applied in bounded slices: the indexer registers the clear in `$$pending-clears` and deletes up to `clear-quads-per-microbatch` quads (default 1,000) per task per microbatch until the graph is empty. A small graph is still gone in the microbatch that carries its clear. A large one empties over several microbatches, during which readers see part of it; the `pending-clear-count` query topology returns 0 once every clear is fully applied. The bound exists because a microbatch that runs past Rama's phase timeout is retried from the start with the same data, so an unbounded clear of a large graph would block the indexer.

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

**Transaction isolation.** RamaSail supports `NONE`, `READ_UNCOMMITTED` and `READ_COMMITTED` (the default), and passes RDF4J's **SailIsolationLevelTest** for them (10 of 10). A transaction's operations are buffered on its connection until commit, so no transaction sees another's uncommitted data. `begin()` with `SNAPSHOT_READ`, `SNAPSHOT` or `SERIALIZABLE` throws: there is no snapshot of the store and no conflict detection. A query reads the live indices, so it can observe a commit that lands while it runs, including part of a large one, and two transactions that read and then write the same data both succeed. A commit is durable when `commit()` returns but becomes queryable afterwards, when the indexer has applied it.

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
  --jar /path/to/rama-sail-graph/target/rama-sail-graph-0.2.0-SNAPSHOT-standalone.jar \
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
- OS: macOS 27.0
- Runtime/config: Rama 1.9.0, single-node cluster, 6 GB client heap, 4 tasks, 2 threads, 1 worker
- Benchmark settings: 50 iterations, 5 warmup
- Method: each dataset is loaded into a fresh deployment and the loader waits until indexing has finished. The first run is discarded as warm-up, and each table is the run with the median query mix time out of 3 further runs
- Loading: 59,328 triples took about 65 seconds from the first append to fully indexed (about 900 triples/sec)

Q8 and Q10 return 0 rows on these generated datasets and Q1 almost always does, so their timings measure empty queries.

#### 6K Triples (100 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 4.43 | 52.84 | 104.44 | 9.74 | 0.0 |
| Q2 | 6.01 | 33.16 | 108.38 | 9.99 | 2.9 |
| Q3 | 2.88 | 7.32 | 45.31 | 4.41 | 1.3 |
| Q4 | 4.83 | 6.48 | 15.09 | 5.20 | 1.3 |
| Q5 | 8.27 | 22.41 | 108.32 | 11.56 | 3.2 |
| Q7 | 2.60 | 4.09 | 9.44 | 2.89 | 7.6 |
| Q8 | 2.00 | 3.33 | 35.65 | 2.80 | 0.0 |
| Q10 | 5.26 | 61.85 | 192.29 | 12.47 | 0.0 |
| Q11 | 0.96 | 1.68 | 101.43 | 3.24 | 5.0 |
| QJ1 | 12.70 | 27.41 | 69.48 | 15.42 | 100.0 |
| QJ2 | 3.00 | 114.19 | 118.27 | 15.90 | 11.9 |
| QJ3 | 13.60 | 41.52 | 118.77 | 18.24 | 100.0 |
| QJ4 | 4.12 | 5.92 | 22.93 | 4.79 | 100.0 |

#### 59K Triples (1,000 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 14.57 | 49.42 | 118.18 | 19.54 | 0.0 |
| Q2 | 7.26 | 10.82 | 112.69 | 9.95 | 2.9 |
| Q3 | 4.28 | 6.34 | 63.18 | 5.69 | 5.4 |
| Q4 | 18.21 | 52.17 | 118.67 | 22.12 | 1.3 |
| Q5 | 28.24 | 67.81 | 179.50 | 36.04 | 4.7 |
| Q7 | 2.83 | 3.20 | 4.12 | 2.91 | 6.2 |
| Q8 | 2.27 | 8.26 | 102.44 | 6.49 | 0.0 |
| Q10 | 26.79 | 56.76 | 134.35 | 31.60 | 0.0 |
| Q11 | 1.31 | 73.69 | 102.94 | 6.88 | 5.0 |
| QJ1 | 29.82 | 91.79 | 137.14 | 38.32 | 100.0 |
| QJ2 | 4.10 | 4.99 | 104.80 | 8.06 | 40.1 |
| QJ3 | 35.83 | 95.36 | 121.85 | 43.59 | 100.0 |
| QJ4 | 9.32 | 103.44 | 109.20 | 16.59 | 100.0 |

#### Scaling: p50 Comparison (6K → 59K)

| Query | 6K p50 | 59K p50 | Slowdown |
|-------|--------|---------|----------|
| Q1 | 4.4 | 14.6 | 3.3x |
| Q2 | 6.0 | 7.3 | 1.2x |
| Q3 | 2.9 | 4.3 | 1.5x |
| Q4 | 4.8 | 18.2 | 3.8x |
| Q5 | 8.3 | 28.2 | 3.4x |
| Q7 | 2.6 | 2.8 | 1.1x |
| Q8 | 2.0 | 2.3 | 1.1x |
| Q10 | 5.3 | 26.8 | 5.1x |
| Q11 | 1.0 | 1.3 | 1.4x |
| QJ1 | 12.7 | 29.8 | 2.3x |
| QJ2 | 3.0 | 4.1 | 1.4x |
| QJ3 | 13.6 | 35.8 | 2.6x |
| QJ4 | 4.1 | 9.3 | 2.3x |

For a 10x data increase, Q2, Q3, Q7, Q8, Q11 and QJ2 stay within 1.5x, QJ1, QJ3 and QJ4 grow 2-3x, and Q1, Q4, Q5 and Q10 grow 3-5x.

#### 1M Triples (17,000 products)

Same laptop and single-node cluster (4 tasks, 2 threads), dataset generated with `:num-products 17000`. Median p50 of three runs after a per-triple load:

| Query | 59K p50 | 1M p50 | Slowdown (17x data) |
|-------|---------|--------|---------------------|
| Q1 | 14.6 | 144.7 | 9.9x |
| Q2 | 7.3 | 6.6 | 0.9x |
| Q3 | 4.3 | 6.2 | 1.4x |
| Q4 | 18.2 | 266.4 | 14.6x |
| Q5 | 28.2 | 385.8 | 13.7x |
| Q7 | 2.8 | 5.5 | 2.0x |
| Q8 | 2.3 | 2.0 | 0.9x |
| Q10 | 26.8 | 519.3 | 19.4x |
| Q11 | 1.3 | 1.0 | 0.8x |
| QJ1 | 29.8 | 374.9 | 12.6x |
| QJ2 | 4.1 | 8.2 | 2.0x |
| QJ3 | 35.8 | 564.6 | 15.8x |
| QJ4 | 9.3 | 111.0 | 11.9x |

Query mix: about 2.4 s (225 ms at 59K). Lookups by subject or by a selective property stay flat (Q2, Q3, Q7, Q8, Q11, QJ2). The other seven grew with the data because a whole property was read where a few lookups or a short scan would do.

**After limited scans and lookups instead of scans** (same 1M store, module updated in place, runs alternated main, branch, branch, main; median p50 in ms):

| Query | Before | After | Speedup |
|-------|--------|-------|---------|
| Q1 | 181.4 | 7.5 | 24x |
| Q2 | 10.4 | 11.4 | - |
| Q3 | 8.8 | 7.6 | - |
| Q4 | 317.3 | 8.1 | 39x |
| Q5 | 421.1 | 18.7 | 23x |
| Q7 | 8.6 | 8.1 | - |
| Q8 | 3.5 | 2.9 | - |
| Q10 | 535.4 | 5.8 | 92x |
| Q11 | 1.9 | 1.2 | - |
| QJ1 | 441.0 | 20.6 | 21x |
| QJ2 | 11.0 | 9.3 | - |
| QJ3 | 571.4 | 20.9 | 27x |
| QJ4 | 127.2 | 6.2 | 21x |

Query mix: 2.6-2.9 s before, 0.16-0.20 s after, which is below the 225 ms mix measured at 59K triples before these changes. Two runs per variant; Q1, Q8 and Q10 return no rows on this dataset. `ORDER BY ... LIMIT` and `DISTINCT ... LIMIT` over a large intermediate result still compute every row.

**Loading 1,005,547 triples:**

| Path | Time | Rate |
|------|------|------|
| Per-triple depot appends, Rama's default microbatch options | 1,899 s | 530/s |
| Per-triple depot appends, module launch options | 825 s | 1,218/s |
| `rama-sail.bulk/load-file!` with `:commit-ops-per-microbatch 60000`, module launch options | 107 s | 9,378/s |

On-disk size after the bulk load: 1.8 GB. See [Bulk Loading](#bulk-loading) for what the options are. For scale: dedicated offline bulk loaders of mature single-machine stores report 100,000 to over 1,000,000 triples per second on server hardware; this is an online, replicated write path measured on a laptop.

### Query Optimizations

The following optimizations are applied automatically during query planning:

- **LIMIT pushdown with limited scans**: Applies LIMIT inside BGPs and self-joins, passing through PROJECT and BIND, where truncating cannot change the result. A limited BGP reads the index in key order and stops after about as many quads as it needs, instead of reading every match. A LIMIT over a chain of property lookups (or a self-join followed by lookups) first tries reading a small multiple of the rows it needs from the driving pattern and falls back to the full plan if that yields too few rows, so results stay exact. Plans with ORDER BY or DISTINCT still compute every row
- **Adaptive filter selectivity**: Estimates filter selectivity by operator type (equality=0.05, range=0.3, inequality=0.8) instead of a flat 0.3, improving join ordering decisions
- **AND decomposition**: Splits compound AND filters into independent conjuncts that can be pushed separately to each join side
- **Selinger-style join ordering**: Dynamic programming optimizer for multi-way join chains using predicate statistics
- **Self-join optimization**: Detects same-predicate joins with inequality filters and uses group-and-pair generation instead of Cartesian product
- **Colocated subject joins**: Exploits subject hash partitioning for partition-local joins
- **Batch property enrichment**: Replaces a hash join on a property with a batched lookup keyed by the other input's bindings, by subject or, when the other input binds the object, by object. It applies on either side of a join, when the input is no larger than the property, and to `OPTIONAL` property patterns with no filter. A filter on the property's values is applied after the lookup, so `?s :price ?p FILTER(?p < 100)` joined with a few subjects reads their prices instead of scanning every price
- **Materialized type views**: Pre-computed indices for `rdf:type` pattern lookups

## Known Limitations

- **Query fallback**: Some unsupported SPARQL operators fall back to RDF4J's local evaluation strategy, which may not scale for large datasets; `SERVICE` is explicitly unsupported
- **RDF 1.2 partial**: Triple terms work via RDF-star (`<< s p o >>`), but RDF 1.2 `<<( s p o )>>` syntax, `~ :reifier` syntax, `{| |}` annotations, and `rdf:reifies` semantics are not yet supported. 5/41 W3C SPARQL 1.2 conformance tests pass; 36 are quarantined
- **Triple term decomposition is client-side**: Extracting subject/predicate/object from triple terms uses string parsing on the SAIL client, not distributed Rama topologies. Constant triple-term lookups are O(1) distributed, but variable decomposition and component filtering are post-fetch
- **Query timeout**: Cancellation is best-effort; Rama cluster queries may continue after client timeout
- **SPARQL endpoint**: Read-only by default; SPARQL UPDATE must be enabled explicitly and can require a Bearer token. There is no TLS, rate limiting or request size limit, and results are buffered in memory, so put it behind a reverse proxy outside development

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
