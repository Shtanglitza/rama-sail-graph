(ns rama-sail.bench.count-statements-bench
  "Benchmark proving the performance benefit of count-statements(nil) via $$global-stats.

   Compares the old path (full $$spoc scan via count-statements topology)
   against the new path (point lookup on $$global-stats via get-global-stats topology).

   Run: lein test :only rama-sail.bench.count-statements-bench"
  (:require [clojure.test :refer [deftest testing is]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [rama-sail.bench.infra.data-gen :as gen])
  (:import [org.eclipse.rdf4j.model Resource]
           [org.eclipse.rdf4j.repository.sail SailRepository]))

(def WARMUP 5)
(def ITERATIONS 50)

(deftest test-count-statements-global-stats-benefit
  (testing "count-statements(nil) via $$global-stats vs full scan"
    (bh/with-bench-module [ipc module-name depot q-plan]

      (bh/print-section "count-statements(nil) Benchmark")
      (println)

      ;; Load dataset: 5000 entities × 3 triples = 15000 quads
      (println "Loading 15,000 quads (5,000 entities × 3 triples)...")
      (let [quads (gen/generate-typed-entities 5000)
            load-stats (bh/load-and-wait! ipc module-name depot quads)]
        (println (format "  Loaded %d quads in %.0fms" (:count load-stats) (:time-ms load-stats)))
        (println)

        ;; --- Path 1: Old path — count-statements topology (full $$spoc scan) ---
        (let [count-qt (rama/foreign-query ipc module-name "count-statements")]

          (bh/print-subsection "Old path: count-statements topology (full $$spoc scan)")
          (let [result (rama/foreign-invoke-query count-qt nil)]
            (println (format "  Result: %d triples" result))
            (is (= 15000 result)))

          (let [stats-old (bh/bench-warmup WARMUP ITERATIONS
                                           (rama/foreign-invoke-query count-qt nil))]
            (bh/print-result "Latency" stats-old)

            ;; --- Path 2: New path — get-global-stats topology (point lookup) ---
            (let [global-stats-qt (rama/foreign-query ipc module-name "get-global-stats")]

              (bh/print-subsection "New path: get-global-stats topology (point lookup)")
              (let [result (rama/foreign-invoke-query global-stats-qt)]
                (println (format "  Result: %s" (pr-str result)))
                (is (= 15000 (:total-triples result))))

              (let [stats-new (bh/bench-warmup WARMUP ITERATIONS
                                               (rama/foreign-invoke-query global-stats-qt))]
                (bh/print-result "Latency" stats-new)

                (let [speedup (/ (:mean stats-old) (:mean stats-new))]
                  (println (format "\n  >>> Speedup: %.1fx faster with $$global-stats point lookup <<<" speedup))
                  (is (> speedup 1.0)
                      "Point lookup should be faster than full scan"))))))

        ;; --- Path 3: End-to-end via RDF4J repo.size() ---
        (bh/print-subsection "End-to-end: repo.getConnection().size() (uses new path)")
        (let [sail (rsail/create-rama-sail ipc module-name)
              repo (SailRepository. sail)]
          (.init repo)
          (try
            (let [^org.eclipse.rdf4j.repository.RepositoryConnection conn (.getConnection repo)]
              (try
                (let [size (.size conn (into-array Resource []))]
                  (println (format "  repo.size() = %d" size))
                  (is (= 15000 size)))

                (let [stats-e2e (bh/bench-warmup WARMUP ITERATIONS
                                                 (.size conn (into-array Resource [])))]
                  (bh/print-result "Latency" stats-e2e))
                (finally (.close conn))))
            (finally (.shutDown repo))))

        (println)
        (println "Benchmark complete.")))))
