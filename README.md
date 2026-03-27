# rama-sail-graph

> **Status: Public Preview** — This project is functional and tested but not yet production-hardened. APIs may change. Feedback and contributions welcome.

A Rama-backed RDF quad store with SPARQL query evaluation and HTTP endpoint.

## Overview

rama-sail-graph integrates the [Rama](https://redplanetlabs.com/rama) distributed data processing framework with the [RDF4J](https://rdf4j.org/) SAIL API, enabling distributed RDF quad storage with SPARQL query support.

### Features

- **Quad Storage**: Four index PStates (SPOC, POSC, OSPC, CSPO) for efficient lookups across any access pattern
- **SPARQL Query Engine**: Server-side query execution via Rama topologies — joins, filters, aggregates, ORDER BY, OPTIONAL, UNION, BIND, VALUES, ASK
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
- Rama 1.4.0 (see [Rama installation](https://redplanetlabs.com/docs))

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

## Architecture

### Storage Layer (`core.clj`)

Rama module with quad-based storage using four complementary indices:

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

### RDF4J SAIL Compliance

RamaSail passes the RDF4J **RDFStoreTest** compliance suite (`rdf4j-sail-testsuite` 5.2.0), which validates:

- Statement CRUD (add, remove, get, has) with pattern matching and wildcards
- Value round-trips (IRIs, BNodes, typed literals, decimals, timezones, long URIs/literals)
- Named graph / context management (add to context, list contexts, clear)
- Transaction semantics (commit, rollback, isolation)
- Namespace management (get, set, remove, clear)
- Duplicate handling and statement counting
- Concurrent add-while-querying
- Dual connections and BNode reuse

**SPARQL operators** handled server-side via Rama topologies:

| Supported | Not Yet Supported (falls back to RDF4J local evaluation) |
|-----------|----------------------------------------------------------|
| SELECT, ASK, DESCRIBE, CONSTRUCT | SERVICE (federated queries) |
| JOIN, LEFT JOIN (OPTIONAL), UNION | Property paths |
| FILTER (comparisons, logic, regex, IN) | MINUS |
| GROUP BY, HAVING, aggregates | Subqueries |
| ORDER BY, DISTINCT, LIMIT/OFFSET | |
| BIND, VALUES, COALESCE, IF | |
| STR, LANG, DATATYPE, LANGMATCHES | |
| ISIRI, ISBNODE, ISLITERAL, ISNUMERIC | |
| SAMETERM, REGEX | |

Unsupported operators are automatically handled by falling back to RDF4J's `DefaultEvaluationStrategy`, which evaluates locally via `getStatements`. This ensures correct results but without distributed execution benefits.

### Observability

Prometheus metrics are exposed at `/metrics` when running the SPARQL endpoint:

- `ramasail_query_latency_seconds` — query execution latency histogram
- `ramasail_queries_total` — query count by status (success, timeout, error, fallback)
- `ramasail_active_connections` — active SAIL connections gauge
- `ramasail_query_result_size` — result size summary (p50, p90, p99)
- `ramasail_transactions_total` — transaction count by status
- `ramasail_transaction_ops_total` — triple operations by type (add, del)

A `/health` endpoint is also available for liveness checks.

## Known Limitations

- **Query fallback**: Unsupported SPARQL operators fall back to RDF4J's local evaluation strategy, which may not scale for large datasets
- **Query timeout**: Cancellation is best-effort; Rama cluster queries may continue after client timeout
- **SPARQL endpoint**: No authentication, rate limiting, or TLS — intended for development use
- **No CI**: Tests are run locally; CI configuration is not yet included

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
