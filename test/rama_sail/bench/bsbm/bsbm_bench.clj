(ns ^{:perf true :bench true} rama-sail.bench.bsbm.bsbm-bench
  "Berlin SPARQL Benchmark (BSBM) integration for Rama SAIL.

   Executes industry-standard BSBM queries through the full RDF4J SAIL stack
   to measure end-to-end query performance.

   Supports two modes:
   - :ipc (InProcessCluster) for testing/development
   - :cluster for real Rama cluster benchmarks

   NOTE: IPC mode simulates distributed behavior in a single JVM. Real cluster
   performance should be significantly better due to true parallelism and
   elimination of IPC RPC overhead (~37ms per foreign-append! call in IPC)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [rama-sail.bench.infra.cluster-config :as cc]
            [rama-sail.bench.bsbm.infra.ntriples-loader :as loader]
            [rama-sail.bench.bsbm.infra.query-templates :as qt]
            [rama-sail.bench.bsbm.infra.param-sampler :as ps])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model Resource]))

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

(def ^:const WARMUP-ITERATIONS 5)
(def ^:const BENCHMARK-ITERATIONS 50)
(def ^:const TASKS 4)
(def ^:const THREADS 2)

;; Default dataset path - bundled 100-product dataset
(def default-dataset-path "test/resources/bsbm/dataset_100.nt")

;;; ---------------------------------------------------------------------------
;;; Query Execution
;;; ---------------------------------------------------------------------------

(defn- execute-sparql
  "Execute a SPARQL SELECT query and return results count and timing."
  [conn sparql]
  (try
    (let [query (.prepareTupleQuery conn sparql)
          {:keys [result ms]} (bh/time-ms-only
                               (with-open [iter (.evaluate query)]
                                 (count (iterator-seq iter))))]
      {:success true
       :result-count result
       :time-ms ms})
    (catch Exception e
      {:success false
       :error (.getMessage e)
       :time-ms 0})))

(defn- benchmark-query
  "Benchmark a query template with multiple iterations.

   Returns:
   {:name \"Q2\"
    :description \"...\"
    :stats {:min :max :p50 :p95 :p99 :mean :count}
    :avg-results float
    :success-rate float}"
  [conn template param-pool warmup-n bench-n]
  (println (str "  Running " (:name template) ": " (:description template)))

  ;; Warmup phase
  (dotimes [_ warmup-n]
    (let [query (ps/instantiate-query template param-pool)]
      (execute-sparql conn query)))

  ;; Benchmark phase
  (let [results (vec (for [_ (range bench-n)]
                       (let [query (ps/instantiate-query template param-pool)]
                         (execute-sparql conn query))))
        successes (filter :success results)
        timings (map :time-ms successes)
        result-counts (map :result-count successes)]
    {:name (:name template)
     :description (:description template)
     :operators (:operators template)
     :stats (bh/calculate-stats timings)
     :avg-results (if (seq result-counts)
                    (/ (reduce + result-counts) (double (count result-counts)))
                    0.0)
     :success-rate (if (seq results)
                     (/ (count successes) (double (count results)))
                     0.0)}))

;;; ---------------------------------------------------------------------------
;;; Result Formatting
;;; ---------------------------------------------------------------------------

(defn- print-query-result
  "Print benchmark result for a single query."
  [result]
  (let [{:keys [name stats avg-results success-rate]} result]
    (println (str "    " (bh/format-stats stats)))
    (println (format "    Avg result count: %.1f | Success rate: %.1f%%"
                     avg-results (* 100 success-rate)))))

(defn- print-summary-table
  "Print summary table for all query results."
  [results]
  (println)
  (println "  Query | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | Avg Results")
  (println "  ------|----------|----------|----------|-----------|------------")
  (doseq [{:keys [name stats avg-results]} results]
    (when stats
      (println (format "  %-5s | %8.2f | %8.2f | %8.2f | %9.2f | %11.1f"
                       name
                       (or (:p50 stats) 0.0)
                       (or (:p95 stats) 0.0)
                       (or (:p99 stats) 0.0)
                       (or (:mean stats) 0.0)
                       avg-results)))))

(defn- calculate-qmph
  "Calculate Query Mixes per Hour.
   A query mix is one execution of each query template."
  [results]
  (let [total-time-ms (reduce + (map #(or (get-in % [:stats :mean]) 0.0) results))
        ms-per-hour 3600000.0]
    (if (pos? total-time-ms)
      (/ ms-per-hour total-time-ms)
      0.0)))

;;; ---------------------------------------------------------------------------
;;; Main Benchmark
;;; ---------------------------------------------------------------------------

(defn run-bsbm-benchmark
  "Run the BSBM benchmark suite.

   Options:
   - :mode - :ipc (default) or :cluster
   - :conductor-host - For cluster mode (default: localhost)
   - :conductor-port - For cluster mode (default: 1973)
   - :module-name - For cluster mode (default: rama-sail.core/RdfStorageModule)
   - :dataset-path - Path to N-Triples file (default: bundled 100-product dataset)
   - :warmup-n - Warmup iterations per query (default: 5)
   - :bench-n - Benchmark iterations per query (default: 50)
   - :tasks - Rama task count for IPC mode (default: 4)
   - :threads - Rama thread count for IPC mode (default: 2)
   - :include-joins - If true, include join-focused queries (default: false)
   - :joins-only - If true, only run join-focused queries (default: false)
   - :skip-load - Skip data loading (for cluster with pre-loaded data)

   Returns benchmark results."
  [& {:keys [mode conductor-host conductor-port module-name
             dataset-path warmup-n bench-n tasks threads
             include-joins joins-only skip-load]
      :or {mode :ipc
           conductor-host "localhost"
           conductor-port 1973
           module-name "rama-sail.core/RdfStorageModule"
           dataset-path default-dataset-path
           warmup-n WARMUP-ITERATIONS
           bench-n BENCHMARK-ITERATIONS
           tasks TASKS
           threads THREADS
           include-joins false
           joins-only false
           skip-load false}}]

  (bh/print-section (str "BSBM Benchmark (" (name mode) " mode)"))

  (when (= mode :ipc)
    (bh/print-ipc-caveat!))

  ;; Check if dataset exists (unless skipping load)
  (when (and (not skip-load) (not (.exists (io/file dataset-path))))
    (println (str "  ERROR: Dataset not found: " dataset-path))
    (println "  Please generate BSBM data first. See test/resources/bsbm/README.md")
    (throw (ex-info "Dataset not found" {:path dataset-path})))

  (cc/with-cluster [conn mode
                    :conductor-host conductor-host
                    :conductor-port conductor-port
                    :module-name module-name]

    ;; For IPC, launch the module
    (let [conn (if (= mode :ipc)
                 (cc/launch-module! conn core/RdfStorageModule {:tasks tasks :threads threads})
                 conn)
          cluster (cc/get-cluster-manager conn)
          mod-name (cc/get-module-name conn)
          depot (rama/foreign-depot cluster mod-name "*triple-depot")]

      (cc/print-connection-info conn)

      ;; Load data
      (bh/print-subsection "Loading BSBM Data")
      (let [load-stats (if skip-load
                         (do
                           (println "  Skipping data load (--skip-load)")
                           {:count 0 :time-ms 0 :triples-per-sec 0})
                         (let [stats (loader/load-ntriples-with-connection! conn depot dataset-path)]
                           (println (format "  Loaded %d triples in %.2fms (%.0f triples/sec)"
                                            (:count stats)
                                            (:time-ms stats)
                                            (:triples-per-sec stats)))
                           stats))

            ;; Create SAIL and connection
            sail (rsail/create-rama-sail cluster mod-name)
            repo (SailRepository. sail)]
        (.init repo)

        (try
          (let [rdf-conn (.getConnection repo)]
            (try
              ;; Build parameter pool
              (bh/print-subsection "Building Parameter Pool")
              (let [param-pool (ps/build-param-pool rdf-conn)
                    summary (ps/pool-summary param-pool)]
                (println (format "  Products: %d, Types: %d, Features: %d, Offers: %d"
                                 (:products summary)
                                 (:product-types summary)
                                 (:features summary)
                                 (:offers summary)))

                ;; Select templates based on options
                (let [templates (cond
                                  joins-only qt/join-benchmark-templates
                                  include-joins qt/all-benchmark-templates
                                  :else qt/benchmark-templates)
                      ;; Run benchmarks
                      _ (bh/print-subsection "Query Benchmarks")
                      results (vec (for [template templates]
                                     (let [result (benchmark-query rdf-conn template param-pool warmup-n bench-n)]
                                       (print-query-result result)
                                       (println)
                                       result)))]

                  ;; Print summary
                  (bh/print-subsection "Summary")
                  (print-summary-table results)

                  (let [qmph (calculate-qmph results)]
                    (println)
                    (println (format "  Query Mix Time: %.2fms"
                                     (reduce + (map #(or (get-in % [:stats :mean]) 0.0) results))))
                    (println (format "  Query Mixes per Hour (QMpH): %.0f" qmph)))

                  ;; Return results
                  {:mode mode
                   :load-stats load-stats
                   :param-pool-summary summary
                   :query-results results
                   :qmph (calculate-qmph results)}))

              (finally
                (.close rdf-conn))))

          (finally
            (.shutDown repo)))))))

;;; ---------------------------------------------------------------------------
;;; Test Entry Points
;;; ---------------------------------------------------------------------------

(deftest test-bsbm-small
  (testing "BSBM benchmark with bundled 100-product dataset"
    (let [dataset-path default-dataset-path]
      (if (.exists (io/file dataset-path))
        (let [results (run-bsbm-benchmark :dataset-path dataset-path
                                          :warmup-n 3
                                          :bench-n 20)]
          (is (pos? (:qmph results)))
          (is (every? #(> (:success-rate %) 0.5) (:query-results results))))
        (do
          (println "Skipping BSBM test - dataset not found at:" dataset-path)
          (println "Generate dataset with: java -cp bsbmtools.jar benchmark.generator.Generator -pc 100 -fc -fn dataset_100")
          (is true "Test skipped - no dataset"))))))

(deftest ^:scale test-bsbm-large
  (testing "BSBM benchmark with 1000-product dataset"
    (let [dataset-path "test/resources/bsbm/dataset_1000.nt"]
      (if (.exists (io/file dataset-path))
        (let [results (run-bsbm-benchmark :dataset-path dataset-path
                                          :warmup-n 5
                                          :bench-n 50
                                          :tasks 8
                                          :threads 2)]
          (is (pos? (:qmph results))))
        (do
          (println "Skipping large BSBM test - dataset not found at:" dataset-path)
          (is true "Test skipped - no dataset"))))))

;;; ---------------------------------------------------------------------------
;;; Individual Query Tests (for debugging)
;;; ---------------------------------------------------------------------------

(deftest test-bsbm-loader
  (testing "N-Triples loader parses BSBM format correctly"
    (let [test-lines ["<http://example.org/product1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Product> ."
                      "# This is a comment"
                      ""
                      "<http://example.org/product1> <http://www.w3.org/2000/01/rdf-schema#label> \"Product 1\" ."
                      "<http://example.org/product1> <http://example.org/price> \"10.50\"^^<http://www.w3.org/2001/XMLSchema#double> ."
                      "_:bnode1 <http://example.org/hasReview> <http://example.org/review1> ."]]

      (let [parsed (map loader/parse-ntriples-line test-lines)]
        ;; First line - IRI triple
        (is (= ["<http://example.org/product1>"
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                "<http://example.org/Product>"]
               (nth parsed 0)))

        ;; Comment line
        (is (nil? (nth parsed 1)))

        ;; Empty line
        (is (nil? (nth parsed 2)))

        ;; String literal
        (is (= ["<http://example.org/product1>"
                "<http://www.w3.org/2000/01/rdf-schema#label>"
                "\"Product 1\""]
               (nth parsed 3)))

        ;; Typed literal
        (is (= ["<http://example.org/product1>"
                "<http://example.org/price>"
                "\"10.50\"^^<http://www.w3.org/2001/XMLSchema#double>"]
               (nth parsed 4)))

        ;; Blank node
        (is (= ["_:bnode1"
                "<http://example.org/hasReview>"
                "<http://example.org/review1>"]
               (nth parsed 5)))))))

(deftest test-query-templates
  (testing "Query templates have correct structure"
    (doseq [template qt/benchmark-templates]
      (is (string? (:name template)))
      (is (string? (:description template)))
      (is (set? (:operators template)))
      (is (vector? (:params template)))
      (is (string? (:template template))))))

(deftest test-param-substitution
  (testing "Parameter substitution works correctly"
    (let [template-str "SELECT * WHERE { %ProductXYZ% ?p ?o . FILTER(?v > %x%) }"
          pool {:products ["http://example.org/product1"]
                :product-types ["http://example.org/Type1"]
                :features ["http://example.org/Feature1" "http://example.org/Feature2"]
                :offers ["http://example.org/offer1"]
                :numeric-1 {:p25 50}
                :numeric-3 {:p75 200}
                :current-date "\"2008-06-20\""}
          result (ps/substitute-params template-str pool)]
      (is (not (clojure.string/includes? result "%ProductXYZ%")))
      (is (not (clojure.string/includes? result "%x%"))))))

(comment
  ;; Run individual benchmarks
  (test-bsbm-loader)
  (test-query-templates)
  (test-param-substitution)

  ;; Run full benchmark
  (run-bsbm-benchmark)

  ;; Run with custom options
  (run-bsbm-benchmark :warmup-n 3 :bench-n 10)

  ;; Print template info
  (qt/print-template-info)

  ;; Run tests from command line:
  ;; lein test :only rama-sail.bench.bsbm.bsbm-bench
  ;; lein with-profile +scale test :only rama-sail.bench.bsbm.bsbm-bench/test-bsbm-large
  )
