(ns rama-sail.bench.limit-early-termination-bench
  "Benchmark proving the performance benefit of LIMIT early-termination.

   Compares query execution with and without :result-limit on BGP nodes.
   The optimization truncates quads before building binding maps and
   running +set-agg deduplication, saving per-row work for discarded rows.

   Run: lein test :only rama-sail.bench.limit-early-termination-bench"
  (:require [clojure.test :refer [deftest testing is]]
            [com.rpl.rama :as rama]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [rama-sail.bench.infra.data-gen :as gen]))

(def WARMUP 5)
(def ITERATIONS 30)
(def ENTITY-COUNT 10000)

(deftest test-limit-early-termination-benefit
  (testing "LIMIT early-termination reduces query latency"
    (bh/with-bench-module [ipc module-name depot q-plan]

      (bh/print-section "LIMIT Early-Termination Benchmark")
      (println)

      ;; Load a substantial dataset: 15000 entities × 3 triples = 45000 quads
      (println (format "Loading %d quads (%d entities × 3 triples)..."
                       (* ENTITY-COUNT 3) ENTITY-COUNT))
      (let [quads (gen/generate-typed-entities ENTITY-COUNT)
            load-stats (bh/load-and-wait! ipc module-name depot quads)]
        (println (format "  Loaded %d quads in %.0fms (%.0f quads/sec)"
                         (:count load-stats) (:time-ms load-stats) (:quads-per-sec load-stats)))
        (println)

        ;; ---------------------------------------------------------------
        ;; Benchmark 1: Wildcard BGP scan — LIMIT 10 vs full scan
        ;; ---------------------------------------------------------------
        (bh/print-section "Wildcard BGP: SELECT * WHERE { ?s ?p ?o }")

        ;; Plan WITHOUT limit (scans all 15000 quads, builds all bindings)
        (let [plan-no-limit {:op :slice
                             :offset 0
                             :limit 10
                             :sub-plan {:op :bgp
                                        :pattern {:s "?s" :p "?p" :o "?o" :c nil}}}

              ;; Plan WITH limit (optimizer would add this; we set it manually for A/B test)
              plan-with-limit {:op :slice
                               :offset 0
                               :limit 10
                               :sub-plan {:op :bgp
                                          :result-limit 10
                                          :pattern {:s "?s" :p "?p" :o "?o" :c nil}}}]

          ;; Verify both return same count
          (let [res-no-limit (rama/foreign-invoke-query q-plan plan-no-limit)
                res-with-limit (rama/foreign-invoke-query q-plan plan-with-limit)]
            (println (format "  Without :result-limit → %d results" (count res-no-limit)))
            (println (format "  With    :result-limit → %d results" (count res-with-limit)))
            (is (= 10 (count res-no-limit)))
            (is (= 10 (count res-with-limit))))

          (let [stats-no-limit (bh/bench-warmup WARMUP ITERATIONS
                                                (rama/foreign-invoke-query q-plan plan-no-limit))
                stats-with-limit (bh/bench-warmup WARMUP ITERATIONS
                                                  (rama/foreign-invoke-query q-plan plan-with-limit))]
            (bh/print-subsection "Without :result-limit (full materialization)")
            (bh/print-result "Latency" stats-no-limit)
            (bh/print-subsection "With :result-limit 10 (early termination)")
            (bh/print-result "Latency" stats-with-limit)
            (let [speedup (/ (:mean stats-no-limit) (:mean stats-with-limit))]
              (println (format "\n  >>> Speedup: %.1fx faster with early termination <<<" speedup))
              ;; The optimization should provide measurable improvement
              (is (> speedup 1.0)
                  "Early termination should be faster than full materialization"))))

        ;; ---------------------------------------------------------------
        ;; Benchmark 2: Predicate-bound BGP — LIMIT 10 vs full scan
        ;; ---------------------------------------------------------------
        (bh/print-section "Predicate-bound BGP: SELECT * WHERE { ?s <pred/name> ?o }")

        (let [plan-no-limit {:op :slice
                             :offset 0
                             :limit 10
                             :sub-plan {:op :bgp
                                        :pattern {:s "?s"
                                                  :p "<http://bench.example/pred/name>"
                                                  :o "?o"
                                                  :c nil}}}
              plan-with-limit {:op :slice
                               :offset 0
                               :limit 10
                               :sub-plan {:op :bgp
                                          :result-limit 10
                                          :pattern {:s "?s"
                                                    :p "<http://bench.example/pred/name>"
                                                    :o "?o"
                                                    :c nil}}}]

          (let [res-no-limit (rama/foreign-invoke-query q-plan plan-no-limit)
                res-with-limit (rama/foreign-invoke-query q-plan plan-with-limit)]
            (println (format "  Without :result-limit → %d results" (count res-no-limit)))
            (println (format "  With    :result-limit → %d results" (count res-with-limit)))
            (is (= 10 (count res-no-limit)))
            (is (= 10 (count res-with-limit))))

          (let [stats-no-limit (bh/bench-warmup WARMUP ITERATIONS
                                                (rama/foreign-invoke-query q-plan plan-no-limit))
                stats-with-limit (bh/bench-warmup WARMUP ITERATIONS
                                                  (rama/foreign-invoke-query q-plan plan-with-limit))]
            (bh/print-subsection "Without :result-limit (full materialization)")
            (bh/print-result "Latency" stats-no-limit)
            (bh/print-subsection "With :result-limit 10 (early termination)")
            (bh/print-result "Latency" stats-with-limit)
            (let [speedup (/ (:mean stats-no-limit) (:mean stats-with-limit))]
              (println (format "\n  >>> Speedup: %.1fx faster with early termination <<<" speedup))
              (is (> speedup 1.0)
                  "Early termination should be faster for predicate-bound BGP"))))

        ;; ---------------------------------------------------------------
        ;; Benchmark 3: Project + BGP (common SPARQL shape) — LIMIT 10
        ;; ---------------------------------------------------------------
        (bh/print-section "Project + BGP: SELECT ?s WHERE { ?s ?p ?o } LIMIT 10")

        (let [plan-no-limit {:op :slice
                             :offset 0
                             :limit 10
                             :sub-plan {:op :project
                                        :vars ["?s"]
                                        :sub-plan {:op :bgp
                                                   :pattern {:s "?s" :p "?p" :o "?o" :c nil}}}}
              plan-with-limit {:op :slice
                               :offset 0
                               :limit 10
                               :sub-plan {:op :project
                                          :vars ["?s"]
                                          :sub-plan {:op :bgp
                                                     :result-limit 10
                                                     :pattern {:s "?s" :p "?p" :o "?o" :c nil}}}}]

          (let [res-no-limit (rama/foreign-invoke-query q-plan plan-no-limit)
                res-with-limit (rama/foreign-invoke-query q-plan plan-with-limit)]
            (println (format "  Without :result-limit → %d results" (count res-no-limit)))
            (println (format "  With    :result-limit → %d results" (count res-with-limit)))
            (is (= 10 (count res-no-limit)))
            (is (= 10 (count res-with-limit))))

          (let [stats-no-limit (bh/bench-warmup WARMUP ITERATIONS
                                                (rama/foreign-invoke-query q-plan plan-no-limit))
                stats-with-limit (bh/bench-warmup WARMUP ITERATIONS
                                                  (rama/foreign-invoke-query q-plan plan-with-limit))]
            (bh/print-subsection "Without :result-limit (full materialization)")
            (bh/print-result "Latency" stats-no-limit)
            (bh/print-subsection "With :result-limit 10 (early termination)")
            (bh/print-result "Latency" stats-with-limit)
            (let [speedup (/ (:mean stats-no-limit) (:mean stats-with-limit))]
              (println (format "\n  >>> Speedup: %.1fx faster with early termination <<<" speedup))
              (is (> speedup 1.0)
                  "Early termination should be faster for project + BGP"))))

        ;; ---------------------------------------------------------------
        ;; Benchmark 4: Scaling — LIMIT 10 stays constant as data grows?
        ;; This compares the LIMIT 10 WITH optimization against full scan
        ;; to show the gap grows with dataset size.
        ;; ---------------------------------------------------------------
        (bh/print-section "Scaling: Full scan grows, LIMIT 10 stays constant")
        (println "  (Already loaded 15K quads)")

        (let [plan-full {:op :bgp
                         :pattern {:s "?s" :p "?p" :o "?o" :c nil}}
              plan-limit-10 {:op :slice
                             :offset 0
                             :limit 10
                             :sub-plan {:op :bgp
                                        :result-limit 10
                                        :pattern {:s "?s" :p "?p" :o "?o" :c nil}}}]
          (let [full-count (count (rama/foreign-invoke-query q-plan plan-full))
                limit-count (count (rama/foreign-invoke-query q-plan plan-limit-10))]
            (println (format "  Full scan returns: %d bindings" full-count))
            (println (format "  LIMIT 10 returns:  %d bindings" limit-count)))

          (let [stats-full (bh/bench-warmup WARMUP ITERATIONS
                                            (rama/foreign-invoke-query q-plan plan-full))
                stats-limit (bh/bench-warmup WARMUP ITERATIONS
                                             (rama/foreign-invoke-query q-plan plan-limit-10))]
            (bh/print-subsection "Full scan (all 15K quads)")
            (bh/print-result "Latency" stats-full)
            (bh/print-subsection "LIMIT 10 with early termination")
            (bh/print-result "Latency" stats-limit)
            (let [speedup (/ (:mean stats-full) (:mean stats-limit))]
              (println (format "\n  >>> Full scan vs LIMIT 10: %.1fx difference <<<" speedup)))))

        (println)
        (println "Benchmark complete.")))))
