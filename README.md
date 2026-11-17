# rama-sail-graph

[![CI](https://github.com/Shtanglitza/rama-sail-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/Shtanglitza/rama-sail-graph/actions/workflows/ci.yml)

> **Status: Public Preview** — This project is functional and tested but not yet production-hardened. APIs may change. Feedback and contributions welcome.

A Rama-backed RDF quad store with SPARQL query evaluation and HTTP endpoint.

## Overview

rama-sail-graph integrates the [Rama](https://redplanetlabs.com/rama) distributed data processing framework with the [RDF4J](https://rdf4j.org/) SAIL API, enabling distributed RDF quad storage with SPARQL query support.

### Features

- **Quad Storage**: Four index PStates (SPOC, POSC, OSPC, CSPO) for efficient lookups across any access pattern
- **SPARQL Query Engine**: Server-side query execution via Rama topologies — joins, filters, aggregates, ORDER BY, OPTIONAL, UNION, BIND, VALUES, ASK
- **RDF 1.2 / SPARQL 1.2 triple terms and reified triples**: Store triple terms (`<<( s p o )>>`) and query statements about statements (`<< s p o >> :source ?x`, `~ ?reifier`, `{| ... |}` annotations), for provenance, confidence and temporal annotations
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

- Java 25+ (RDF4J 6 requires it). The Rama workers run the module, which does not load RDF4J, so they need only what Rama requires
- [Leiningen](https://leiningen.org/)
- Rama 1.10.0 or later (see [Rama installation](https://redplanetlabs.com/docs)); the module jar carries Ring 1.15 / Jetty 12 classes, which match Rama 1.10 and not earlier releases

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

> **Note:** Rama 1.10.0 (with Clojure 1.12.6) is required at runtime. Add the [Red Planet Labs Maven repository](https://nexus.redplanetlabs.com/repository/maven-public-releases) to resolve the Rama dependency.

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

# Run triple term / reified triple integration tests
lein test :only rama-sail.sail.reified-triples-test

# Run the W3C conformance suites (SPARQL 1.2, RDF 1.2 formats, SPARQL 1.1 property paths)
lein test :w3c
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
    ;; [:commit [record ...]] to have up to 10,000 of them accepted all-or-nothing and applied in
    ;; order (the indexer stamps them itself: a tx-time inside a commit is ignored). The indexer
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

### Triple Terms and Reified Triples (RDF 1.2 / SPARQL 1.2)

A triple term `<<( s p o )>>` is a value that denotes a triple; RDF 1.2 allows it as an object. Statements about a triple go through a reifier, a resource that `rdf:reifies` the triple term, and SPARQL 1.2 reads `<< s p o >>` as such a reifier:

```sparql
# Data: a reifier of <alice knows bob>, with a confidence and a source
INSERT DATA {
  <http://ex/alice> <http://ex/knows> <http://ex/bob> ~ <http://ex/r1>
      {| <http://ex/confidence> 0.95 ; <http://ex/source> <http://ex/hr> |} .
}

# All statements about the triple (including the reifier's rdf:reifies)
SELECT ?pred ?val WHERE {
  << <http://ex/alice> <http://ex/knows> <http://ex/bob> >> ?pred ?val .
}

# Decompose: which triples have a confidence above 0.8
SELECT ?s ?p ?o ?confidence WHERE {
  << ?s ?p ?o >> <http://ex/confidence> ?confidence .
  FILTER(?confidence > 0.8)
}

# The reifier itself, and triple terms as plain values
SELECT ?r ?t WHERE {
  << <http://ex/alice> <http://ex/knows> <http://ex/bob> ~ ?r >> <http://ex/source> <http://ex/hr> .
  ?r <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> ?t .
  FILTER(isTRIPLE(?t) && SUBJECT(?t) = <http://ex/alice>)
}
```

Triple terms are stored as values in all four quad indexes, using the canonical string `<< <s> <p> <o> >>`, and may nest (in object position). A constant triple term is an index lookup; `<< s p o >>` with a selective annotation such as `:source :hr` starts from that annotation and looks up `rdf:reifies` per reifier.

**Before upgrading to RDF4J 6 (RDF 1.2).** Only a store written by an RDF4J 5 version that holds triple terms in subject position (`<< s p o >> :source :hr`) needs this; RDF 1.2 allows a triple term only as an object. Rewrite such data with `rama-sail.migrate` while the store still runs the RDF4J 5 release: each `<< s p o >> :x :y g` becomes `R rdf:reifies << s p o >> g` plus `R :x :y g`, with one blank-node reifier per distinct triple term (the namespace docstring has the details). RDF4J 6 answers the original queries through the reifier.

<details>
<summary>Migration procedure</summary>

Plan the migration and the upgrade as one maintenance window. Once the data is migrated, the RDF4J 5 server no longer answers RDF-star queries such as `<< :a :knows :b >> :source ?s` (they now read `?r rdf:reifies << :a :knows :b >> ; :source ?s`); RDF4J 6 answers the original query again, because RDF 1.2 SPARQL reads it through the reifier. In order:

1. Update the running module to the last RDF4J 5 release, the first with `rama-sail.migrate` (`rama deploy --action update --jar <that release's jar> --module rama-sail.core/RdfStorageModule`). It adds the read-only `triple-term-quads` query the migration uses; without it the migration stops with an error saying so. The data stays in place.
2. Stop every writer: the SPARQL server and any loaders. The migration reads a page, then writes its rewrite.
3. From a Clojure REPL with that release on the classpath, connect to the cluster and run a dry run, then the migration:

   ```clojure
   (require '[com.rpl.rama :as rama] 'rama-sail.core '[rama-sail.migrate :as migrate])
   (def cluster (rama/open-cluster-manager {"conductor.host" "conductor-host" "conductor.port" 1973}))
   (migrate/dry-run cluster "rama-sail.core/RdfStorageModule")   ;; {:quads n :reifiers n :nested n :samples [...]}
   (migrate/migrate! cluster "rama-sail.core/RdfStorageModule")  ;; rewrites, waits, checks: (:quads (:remaining result)) is 0
   ```

   Review `:samples`, and `:nested` if it is not 0, before running `migrate!`: `:nested` counts quads with a triple term inside the subject of another triple term, whose rewrite changes what the outer term denotes. If `:remaining` is not 0, a writer was still running: stop it and run `migrate!` again.
4. Deploy the RDF4J 6 release and restart the writers.

On a single-node local cluster, a store with 150K quads, 100K of them with a triple term as subject, took 6 s for the dry run and 78 s for the migration.

</details>

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

A SAIL `commit()` appends the whole commit as one `[:commit [op ...]]` depot record (one per 5,000 operations for larger commits), so it is accepted all-or-nothing in one round trip. All commit records go to a single depot partition, the commit log; the indexer stores them in `$$commit-queue` and applies up to `commit-ops-per-microbatch` operations (default 10,000, adjustable at runtime) per microbatch, oldest commit first, so commits take effect in append order. A commit within that budget is fully applied in the microbatch that receives it; a larger one becomes visible over several microbatches, and `pending-commit-count` returns 0 once every commit is applied.

The indexer stamps the operations of a commit when it queues the commit, from a clock it keeps with the queue (`$$commit-clock`: its wall clock in milliseconds, forced to increase from one commit to the next). A graph clear removes the quads stamped at or before it, so a clear removes every quad committed before it and no quad committed after it, whatever the clocks of the committing clients say; the tx-times a writer puts in a commit record are replaced. Within a commit, operations take effect in the order they are listed.

Single-operation records appended directly to the depot are applied immediately and keep the tx-time their writer gave them (the indexer's time if they have none). They are not ordered against queued commits, nor against each other across depot partitions: between a directly appended add and a directly appended clear only those stamps decide, and an add that the indexer reads after the clear has finished stays. Use commit records when the order of a clear and an add matters.

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
| **Storage** | Triple terms in object position | Stored as canonical strings in all 4 indexes, O(1) hash lookups |
| **Compilation** | `TripleRef`, `ReifiedTripleRef`, `AnnotationTripleRef` algebra nodes | Paired with the pattern that binds the term (`rama-sail.sail.triple-refs`): a constant term becomes that pattern's constant, a term with variables becomes filter+bind over it |
| **Expressions** | `isTRIPLE`, `SUBJECT`/`PREDICATE`/`OBJECT`, `TRIPLE()` | Evaluated on canonical strings in the module |
| **Serialization** | RDF4J `TripleTerm` <-> string | `val->str` / `str->val`; a triple term in subject position (data written before RDF 1.2) is an error naming `rama-sail.migrate` |

### RDF4J SAIL Compliance

RamaSail passes 41 of 41 tests in the RDF4J **RDFStoreTest** compliance suite (`rdf4j-sail-testsuite` 6.1.0), which validates:

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
| JOIN, LEFT JOIN (OPTIONAL), UNION | | |
| FILTER (comparisons, logic, regex, IN) | | |
| GROUP BY, HAVING, aggregates | | |
| ORDER BY, DISTINCT, REDUCED, LIMIT/OFFSET | | |
| BIND, VALUES, COALESCE, IF | | |
| STR, LANG, DATATYPE, LANGMATCHES | | |
| ISIRI, ISBNODE, ISLITERAL, ISNUMERIC | | |
| SAMETERM, REGEX | | |
| Property paths (`+`, `*`, `?`, `/`, `\|`, `^`) | | |
| Triple terms (`<<( s p o )>>`), reified triples (`<< s p o >>`, `~ ?r`, `{| |}`) | | |
| `isTRIPLE`, `SUBJECT`, `PREDICATE`, `OBJECT`, `TRIPLE()` | | |

Some unsupported operators are automatically handled by falling back to RDF4J's `DefaultEvaluationStrategy`, which evaluates locally via `getStatements`. This ensures correct results but without distributed execution benefits. `SERVICE` is a special case and is currently rejected explicitly rather than evaluated through fallback.

REPL verification showed that basic nested `SELECT` subqueries can compile into Rama plans and run server-side, so subquery support is at least partial and should not be described as fallback-only.

### W3C RDF 1.2 and SPARQL 1.2 Conformance

The project includes vendored copies of the W3C [rdf-tests](https://github.com/w3c/rdf-tests) suites
for SPARQL 1.2 and the RDF 1.2 formats (`test/resources/w3c/`, source commit in
`test/resources/w3c/SOURCE.md`). Every test runs through the store: the data is loaded into RamaSail
and the query runs there, or, for the format suites, the parsed input is stored and read back.

| Suites | Tests | Pass | Expected failures |
|--------|-------|------|-------------------|
| SPARQL 1.2: triple terms (evaluation, syntax), codepoint escapes, expressions, grouping, language direction, version, syntax | 271 | 260 | 11 |
| RDF 1.2: Turtle, TriG, N-Triples, N-Quads (syntax, evaluation, C14N), RDF/XML | 351 | 328 | 23 |

Expected failures are listed by test id with their reason in `sparql12_test.clj` and `rdf12_test.clj`;
the run fails if one of them passes. Their causes:

| Cause | Tests |
|-------|-------|
| RDF4J 6.1's parsers: invalid language tags, surrogates, escapes or `@base` accepted, two valid codepoint escapes rejected, two RDF/XML annotations read differently (RDF4J's MemoryStore fails the same tests) | 30 |
| Numeric literals stored in canonical form (Known Limitations): `"001"^^xsd:integer` groups with `"1"`, `1.0` reads back as `1` | 3 |
| Language tags matched case-sensitively in graph patterns (Known Limitations) | 1 |

Not included: the RDF 1.2 semantics (entailment) suites.

### W3C SPARQL 1.1 Property Path Conformance

The project includes a vendored copy of the W3C [property-path](https://github.com/w3c/rdf-tests/tree/main/sparql/sparql11/property-path) test suite (33 tests) in `test/resources/w3c/sparql11/property-path/`, run like the suites above.

| Result | Count | Details |
|--------|-------|---------|
| **Pass** | 31 | Sequence, alternative, inverse, transitive (`+`/`*`), zero-or-one (`?`), cycles, diamonds, negated property sets, operator precedence, paths inside `GRAPH` |
| **Expected failures** | 2 | `pp35` (a zero-length match lost in `GRAPH ?g { ?s :p1* ?t }`), `pp37` (nested star `(*)*`) |

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
- Method: each dataset is loaded into a fresh deployment with `rama-sail.bulk` (`:commit-ops-per-microbatch 60000`) and the loader waits until indexing has finished. The first run is discarded as warm-up, and each table is the run with the median query mix time out of 3 further runs
- Loading: 59,328 triples in 9.4 seconds (about 6,300 triples/sec); the 1M load is measured separately below

Q8 and Q10 return 0 rows on these generated datasets and Q1 almost always does, so their timings measure empty queries.

#### 6K Triples (100 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 2.74 | 5.10 | 83.78 | 4.74 | 0.1 |
| Q2 | 5.99 | 7.15 | 19.62 | 6.42 | 3.1 |
| Q3 | 3.25 | 4.71 | 104.26 | 5.52 | 1.2 |
| Q4 | 2.50 | 2.93 | 3.16 | 2.53 | 1.1 |
| Q5 | 6.15 | 6.77 | 105.95 | 8.25 | 3.3 |
| Q7 | 2.38 | 3.40 | 103.86 | 4.59 | 6.6 |
| Q8 | 1.76 | 5.96 | 84.39 | 4.22 | 0.0 |
| Q10 | 2.13 | 45.98 | 102.68 | 7.09 | 0.0 |
| Q11 | 0.88 | 1.29 | 101.34 | 3.68 | 5.0 |
| QJ1 | 13.53 | 29.78 | 35.05 | 16.07 | 100.0 |
| QJ2 | 2.58 | 3.19 | 102.55 | 5.27 | 12.0 |
| QJ3 | 13.60 | 45.10 | 225.05 | 21.55 | 100.0 |
| QJ4 | 4.58 | 6.13 | 73.64 | 6.55 | 100.0 |

Query mix: 96 ms.

#### 59K Triples (1,000 products)

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 3.55 | 33.66 | 108.46 | 8.59 | 0.0 |
| Q2 | 6.44 | 8.68 | 101.45 | 8.73 | 3.0 |
| Q3 | 4.02 | 5.25 | 104.60 | 7.15 | 4.9 |
| Q4 | 3.37 | 21.66 | 62.90 | 5.62 | 1.3 |
| Q5 | 8.67 | 45.37 | 110.77 | 14.26 | 4.6 |
| Q7 | 2.69 | 3.59 | 26.92 | 3.22 | 7.0 |
| Q8 | 1.94 | 12.03 | 102.11 | 4.99 | 0.0 |
| Q10 | 2.28 | 3.26 | 36.46 | 3.05 | 0.0 |
| Q11 | 0.91 | 1.06 | 1.26 | 0.91 | 5.0 |
| QJ1 | 17.93 | 46.68 | 62.03 | 21.09 | 100.0 |
| QJ2 | 3.74 | 3.98 | 4.04 | 3.74 | 40.0 |
| QJ3 | 14.87 | 18.66 | 57.00 | 16.82 | 100.0 |
| QJ4 | 5.04 | 8.86 | 106.84 | 8.19 | 100.0 |

Query mix: 106 ms.

#### 1M Triples (17,000 products)

Dataset generated with `:num-products 17000`.

| Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results |
|-------|----------|----------|----------|-----------|-------------|
| Q1 | 6.05 | 75.52 | 107.34 | 12.85 | 0.0 |
| Q2 | 7.41 | 12.73 | 86.70 | 9.32 | 3.0 |
| Q3 | 6.26 | 85.27 | 107.52 | 14.75 | 10.0 |
| Q4 | 5.53 | 57.15 | 77.16 | 9.79 | 0.7 |
| Q5 | 17.91 | 47.65 | 119.41 | 23.45 | 5.0 |
| Q7 | 5.28 | 8.64 | 26.65 | 6.04 | 6.0 |
| Q8 | 1.94 | 2.43 | 34.19 | 2.62 | 0.0 |
| Q10 | 4.62 | 22.44 | 80.42 | 8.01 | 0.0 |
| Q11 | 0.93 | 1.38 | 77.93 | 3.63 | 5.0 |
| QJ1 | 19.13 | 38.35 | 59.61 | 21.40 | 100.0 |
| QJ2 | 8.48 | 14.60 | 96.53 | 11.07 | 50.0 |
| QJ3 | 19.35 | 45.33 | 65.18 | 22.24 | 100.0 |
| QJ4 | 6.00 | 10.15 | 68.86 | 8.12 | 100.0 |

Query mix: 153 ms.

#### Scaling: p50 Comparison (6K → 59K → 1M)

| Query | 6K p50 | 59K p50 | 1M p50 | 6K → 1M |
|-------|--------|---------|--------|---------|
| Q1 | 2.7 | 3.5 | 6.0 | 2.2x |
| Q2 | 6.0 | 6.4 | 7.4 | 1.2x |
| Q3 | 3.2 | 4.0 | 6.3 | 1.9x |
| Q4 | 2.5 | 3.4 | 5.5 | 2.2x |
| Q5 | 6.2 | 8.7 | 17.9 | 2.9x |
| Q7 | 2.4 | 2.7 | 5.3 | 2.2x |
| Q8 | 1.8 | 1.9 | 1.9 | 1.1x |
| Q10 | 2.1 | 2.3 | 4.6 | 2.2x |
| Q11 | 0.9 | 0.9 | 0.9 | 1.1x |
| QJ1 | 13.5 | 17.9 | 19.1 | 1.4x |
| QJ2 | 2.6 | 3.7 | 8.5 | 3.3x |
| QJ3 | 13.6 | 14.9 | 19.4 | 1.4x |
| QJ4 | 4.6 | 5.0 | 6.0 | 1.3x |

For 167 times more data the query mix grows from 96 ms to 153 ms, and no query's p50 grows more than 3.3x. `ORDER BY ... LIMIT` and `DISTINCT ... LIMIT` over a large intermediate result still compute every row; none of these 13 queries is slow for that reason at this size.

#### Before Limited Scans and Lookups Instead of Scans

Earlier versions read a whole property where a few lookups or a short scan would do, so seven of the queries grew with the data: the query mix was 225 ms at 59K triples and 2.4 s at 1M. Measured on one loaded 1M store with the module updated in place, runs alternated before, after, after, before (median p50 in ms, two runs per variant):

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

Query mix: 2.6-2.9 s before, 0.16-0.20 s after.

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
- **Triple terms with variables are filtered, not indexed**: a constant triple term is an index lookup, but `<< ?s :p ?o >>` reads every triple term in that position (every `rdf:reifies` statement, or every object of the pattern) and filters by its parts, unless another pattern in the query is more selective
- **Numeric and boolean literals are stored in canonical form**: `"1.0"^^xsd:decimal` reads back as `"1"^^xsd:decimal`, `"1e0"^^xsd:double` as `"1.0"^^xsd:double`, `"TRUE"^^xsd:boolean` as `"true"^^xsd:boolean`. The value is kept, the lexical form is not, so equal values match each other in queries, but a graph read back is not always identical to the one loaded
- **Language tags match case-sensitively in graph patterns**: a pattern with `"abc"@EN` does not find a stored `"abc"@en` (RDF treats the two as the same literal). Filters with `=` and `LANGMATCHES` compare tags case-insensitively. The bulk loader stores tags as written
- **`ORDER BY ... LIMIT` and `DISTINCT ... LIMIT`**: Over a large intermediate result these compute every row before the limit applies; there is no distributed top-N yet
- **Query timeout**: Cancellation is best-effort; Rama cluster queries may continue after client timeout
- **SPARQL endpoint**: Read-only by default; SPARQL UPDATE must be enabled explicitly and can require a Bearer token. There is no TLS, rate limiting or request size limit, and results are buffered in memory, so put it behind a reverse proxy outside development

## License

Copyright (c) 2025-2026 Shtanglitza. Licensed under the Apache License 2.0.
See `LICENSE`, `NOTICE`, and `COPYRIGHT.md`.
