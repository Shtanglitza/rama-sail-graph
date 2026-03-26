# rama-sail-graph

A Rama-backed RDF quad store with SPARQL query evaluation and HTTP endpoint.

## Overview

rama-sail-graph integrates the [Rama](https://redplanetlabs.com/rama) distributed data processing framework with the [RDF4J](https://rdf4j.org/) SAIL API, enabling distributed RDF quad storage with SPARQL query support.

### Features

- **Quad Storage**: Four index PStates (SPOC, POSC, OSPC, CSPO) for efficient lookups across any access pattern
- **SPARQL Query Engine**: Server-side query execution via Rama topologies — joins, filters, aggregates, ORDER BY, OPTIONAL, UNION, BIND, VALUES
- **Query Optimization**: Cardinality-based join ordering, self-join optimization, colocated subject joins, batch property lookups, materialized type views
- **Statistics**: Per-predicate and global cardinality tracking for adaptive query planning
- **Soft Deletes**: Tombstone-based deletion preserves data integrity
- **SPARQL HTTP Endpoint**: W3C SPARQL Protocol compatible, works with tools like gdotv
- **Namespace Management**: Full prefix/IRI namespace support

## Quick Start

### Prerequisites

- Java 17+
- [Leiningen](https://leiningen.org/)
- Rama 1.4.0 (see [Rama installation](https://redplanetlabs.com/docs))

### Open Source And Commercial Use

This repository is intended to be published as open source under the Apache License 2.0.

That means you can use, modify, and ship this code in future commercial products, subject to Apache 2.0 notice requirements. However, this project depends on Rama, and Rama has its own platform licensing terms. Per Red Planet Labs, Rama includes an embedded free license for clusters up to two Supervisor nodes; larger clusters require a separate paid license.

### Build & Test

```bash
# Check compilation (~30s)
lein check

# Run unit tests (~5s, no Rama cluster needed)
lein test :only rama-sail.sail.optimizer-test rama-sail.sail.adapter-test

# Run full test suite (~2-5min, starts in-process Rama cluster)
lein test
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

### Query Topologies

| Topology | Purpose |
|----------|---------|
| `find-triples` | Pattern matching with tombstone filtering |
| `find-bgp` | Basic Graph Pattern evaluation |
| `join` / `left-join` | Hash join / left outer join |
| `union` | UNION operator |
| `filter` | Expression evaluation (distributed) |
| `project` / `distinct` / `slice` | Projection, dedup, pagination |
| `order` | ORDER BY with ASC/DESC |
| `bind` | BIND expressions |
| `group` | GROUP BY with aggregates |
| `self-join` | Optimized same-predicate joins |
| `colocated-subject-join` | Partition-local subject joins |
| `batch-lookup` | Batch property fetching |
| `execute-plan` | Recursive plan executor |

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
