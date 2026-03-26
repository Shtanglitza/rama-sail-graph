(ns ^:perf rama-sail.bench.micro.lookup-bench
  "Single quad lookup latency benchmarks.
   Tests point lookups and pattern scans at various selectivities.

   NOTE: These benchmarks run on InProcessCluster (IPC), not a real Rama cluster.
   IPC simulates distributed behavior in a single JVM, which means:
   - All distributed overhead without true parallelism benefits
   - Concentrated GC pressure causing latency spikes
   - Thread contention in a single process

   Real cluster performance should be significantly better.
   See rama-sail.bench.infra.bench-helpers for detailed explanation."
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [rama-sail.bench.infra.data-gen :as gen]))

;;; ---------------------------------------------------------------------------
;;; Test Data Setup
;;; ---------------------------------------------------------------------------

(def ^:const LOOKUP-DATA-SIZE 10000)
(def ^:const LOOKUP-ITERATIONS 100)
(def ^:const WARMUP-ITERATIONS 10)

;;; ---------------------------------------------------------------------------
;;; Point Lookup Tests (S P O C all bound)
;;; ---------------------------------------------------------------------------

(deftest test-point-lookup-latency
  (testing "Point lookup latency (exact quad match)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Point Lookup Benchmark")

      ;; Load test data
      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ LOOKUP-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Sample quads for lookup
        (let [sample-quads (take LOOKUP-ITERATIONS (shuffle quads))]

          ;; Warmup
          (bh/print-subsection "Warmup")
          (doseq [[s p o _] (take WARMUP-ITERATIONS sample-quads)]
            (rama/foreign-invoke-query q-triples s p o core/DEFAULT-CONTEXT-VAL))
          (println (format "  %d warmup queries" WARMUP-ITERATIONS))

          ;; Benchmark
          (bh/print-subsection "Point Lookup (find-triples S P O C)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [[s p o _] (rand-nth (vec sample-quads))]
                                  (rama/foreign-invoke-query q-triples s p o core/DEFAULT-CONTEXT-VAL)))]
            (bh/print-result "Latency" stats)

            ;; Check targets
            (bh/report-target-check "Point lookup" (bh/check-latency-target stats :point-lookup :p50))
            (bh/report-target-check "Point lookup" (bh/check-latency-target stats :point-lookup :p99)))

          ;; BGP version
          (bh/print-subsection "Point Lookup (find-bgp)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [[s p o _] (rand-nth (vec sample-quads))]
                                  (rama/foreign-invoke-query q-bgp {:s s :p p :o o})))]
            (bh/print-result "Latency" stats)))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Subject Scan Tests (S ? ? ?)
;;; ---------------------------------------------------------------------------

(deftest test-subject-scan-latency
  (testing "Subject scan latency (S bound, P O C wildcard)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Subject Scan Benchmark")

      ;; Load test data - typed entities have 3 quads per subject
      (bh/print-subsection "Setup")
      (let [num-entities (/ LOOKUP-DATA-SIZE 3)
            quads (gen/generate-typed-entities num-entities)]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads (%d unique subjects)" (count quads) num-entities))

        ;; Sample subjects
        (let [subjects (distinct (map first quads))
              sample-subjects (take LOOKUP-ITERATIONS (shuffle subjects))]

          ;; Warmup
          (doseq [s (take WARMUP-ITERATIONS sample-subjects)]
            (rama/foreign-invoke-query q-triples s nil nil nil))

          ;; Benchmark find-triples
          (bh/print-subsection "Subject Scan (find-triples S ? ? ?)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [s (rand-nth (vec sample-subjects))]
                                  (rama/foreign-invoke-query q-triples s nil nil nil)))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Subject scan" (bh/check-latency-target stats :subject-scan :p50))
            (bh/report-target-check "Subject scan" (bh/check-latency-target stats :subject-scan :p99)))

          ;; BGP version with variable
          (bh/print-subsection "Subject Scan (find-bgp)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [s (rand-nth (vec sample-subjects))]
                                  (rama/foreign-invoke-query q-bgp {:s s :p "?p" :o "?o"})))]
            (bh/print-result "Latency" stats)))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Predicate Scan Tests (? P ? ?)
;;; ---------------------------------------------------------------------------

(deftest test-predicate-scan-latency
  (testing "Predicate scan latency (P bound, S O C wildcard)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Predicate Scan Benchmark")

      ;; Load data with varied predicates
      (bh/print-subsection "Setup")
      (let [num-predicates 20
            quads (gen/generate-dense-predicates LOOKUP-DATA-SIZE num-predicates)]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads across %d predicates" (count quads) num-predicates))

        ;; Sample predicates
        (let [predicates (distinct (map second quads))
              sample-predicates (take LOOKUP-ITERATIONS (cycle predicates))]

          ;; Warmup
          (doseq [p (take WARMUP-ITERATIONS sample-predicates)]
            (rama/foreign-invoke-query q-triples nil p nil nil))

          ;; Benchmark
          (bh/print-subsection "Predicate Scan (find-triples ? P ? ?)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [p (rand-nth (vec predicates))]
                                  (rama/foreign-invoke-query q-triples nil p nil nil)))]
            (bh/print-result "Latency" stats)
            ;; Predicate scans return more data, so use subject-scan targets
            (bh/report-target-check "Predicate scan" (bh/check-latency-target stats :subject-scan :p50))
            (bh/report-target-check "Predicate scan" (bh/check-latency-target stats :subject-scan :p99))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Object Scan Tests (? ? O ?)
;;; ---------------------------------------------------------------------------

(deftest test-object-scan-latency
  (testing "Object scan latency (O bound, S P C wildcard)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Object Scan Benchmark")

      ;; Load data with shared objects
      (bh/print-subsection "Setup")
      (let [num-shared 50
            quads (gen/generate-shared-objects LOOKUP-DATA-SIZE num-shared)]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads pointing to %d shared objects" (count quads) num-shared))

        ;; Sample objects
        (let [objects (distinct (map #(nth % 2) quads))
              sample-objects (take LOOKUP-ITERATIONS (cycle objects))]

          ;; Warmup
          (doseq [o (take WARMUP-ITERATIONS sample-objects)]
            (rama/foreign-invoke-query q-triples nil nil o nil))

          ;; Benchmark
          (bh/print-subsection "Object Scan (find-triples ? ? O ?)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [o (rand-nth (vec objects))]
                                  (rama/foreign-invoke-query q-triples nil nil o nil)))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Object scan" (bh/check-latency-target stats :subject-scan :p50))
            (bh/report-target-check "Object scan" (bh/check-latency-target stats :subject-scan :p99))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Context Scan Tests (? ? ? C)
;;; ---------------------------------------------------------------------------

(deftest test-context-scan-latency
  (testing "Context scan latency (C bound, S P O wildcard)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Context Scan Benchmark")

      ;; Load data across multiple contexts
      (bh/print-subsection "Setup")
      (let [num-contexts 10
            quads (gen/generate-multi-context LOOKUP-DATA-SIZE num-contexts)]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads across %d contexts" (count quads) num-contexts))

        ;; Sample contexts
        (let [contexts (distinct (map #(nth % 3) quads))
              sample-contexts (take LOOKUP-ITERATIONS (cycle contexts))]

          ;; Warmup
          (doseq [c (take WARMUP-ITERATIONS sample-contexts)]
            (rama/foreign-invoke-query q-triples nil nil nil c))

          ;; Benchmark
          (bh/print-subsection "Context Scan (find-triples ? ? ? C)")
          (let [stats (bh/bench LOOKUP-ITERATIONS
                                (let [c (rand-nth (vec contexts))]
                                  (rama/foreign-invoke-query q-triples nil nil nil c)))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Context scan" (bh/check-latency-target stats :subject-scan :p50))
            (bh/report-target-check "Context scan" (bh/check-latency-target stats :subject-scan :p99))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Full Scan Test (? ? ? ?)
;;; ---------------------------------------------------------------------------

(deftest test-full-scan-latency
  (testing "Full scan latency (all wildcards)"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Full Scan Benchmark")

      ;; Load smaller dataset for full scan
      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities 1000)] ;; 3K quads
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Warmup
        (dotimes [_ 3]
          (rama/foreign-invoke-query q-triples nil nil nil nil))

        ;; Benchmark - fewer iterations due to cost
        (bh/print-subsection "Full Scan (find-triples ? ? ? ?)")
        (let [stats (bh/bench 10
                              (rama/foreign-invoke-query q-triples nil nil nil nil))]
          (bh/print-result "Latency" stats)
          ;; Full scans are expensive, use complex-query targets
          (bh/report-target-check "Full scan" (bh/check-latency-target stats :complex-query :p50))
          (bh/report-target-check "Full scan" (bh/check-latency-target stats :complex-query :p99)))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Combined Lookup Benchmark Summary
;;; ---------------------------------------------------------------------------

(deftest test-lookup-summary
  (testing "Lookup latency summary across all patterns"
    (bh/with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp]
      (bh/print-section "Lookup Latency Summary")

      ;; Load comprehensive test data
      (let [quads (gen/generate-typed-entities 3334)] ;; ~10K quads
        (bh/load-and-wait! ipc module-name depot quads)

        (let [subjects (vec (distinct (map first quads)))
              predicates (vec (distinct (map second quads)))
              results (atom {})]

          ;; Warmup all patterns
          (rama/foreign-invoke-query q-triples (first subjects) nil nil nil)
          (rama/foreign-invoke-query q-triples nil (first predicates) nil nil)
          (rama/foreign-invoke-query q-triples nil nil nil nil)

          ;; Point lookup (S P O)
          (let [[s p o _] (rand-nth quads)
                stats (bh/bench 50 (rama/foreign-invoke-query q-triples s p o core/DEFAULT-CONTEXT-VAL))]
            (swap! results assoc :point-lookup stats))

          ;; Subject scan (S ? ? ?)
          (let [s (rand-nth subjects)
                stats (bh/bench 50 (rama/foreign-invoke-query q-triples s nil nil nil))]
            (swap! results assoc :subject-scan stats))

          ;; Predicate scan (? P ? ?)
          (let [p (rand-nth predicates)
                stats (bh/bench 50 (rama/foreign-invoke-query q-triples nil p nil nil))]
            (swap! results assoc :predicate-scan stats))

          ;; Print summary table
          (bh/print-subsection "Results Summary")
          (println "  Pattern        | p50 (ms) | p99 (ms) | Target p50 | Target p99")
          (println "  ---------------|----------|----------|------------|----------")
          (doseq [[pattern stats] @results]
            (let [targets (get bh/latency-targets pattern (get bh/latency-targets :subject-scan))]
              (println (format "  %-14s | %8.2f | %8.2f | %10.1f | %10.1f"
                               (name pattern)
                               (:p50 stats)
                               (:p99 stats)
                               (:p50 targets)
                               (:p99 targets))))))

        (is true)))))

(comment
  ;; Run individual tests
  (test-point-lookup-latency)
  (test-subject-scan-latency)
  (test-predicate-scan-latency)
  (test-object-scan-latency)
  (test-full-scan-latency)
  (test-lookup-summary)

  ;; Run all lookup benchmarks
  ;; lein test :only rama-sail.bench.micro.lookup-bench
  )
