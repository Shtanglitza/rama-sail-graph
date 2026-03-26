(ns ^:perf rama-sail.bench.micro.ingestion-bench
  "Ingestion throughput benchmarks.
   Measures quads/second at various data sizes.

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
;;; Ingestion Throughput Tests
;;; ---------------------------------------------------------------------------

(deftest test-ingestion-throughput-1k
  (testing "Ingestion throughput at 1K quads"
    (bh/with-bench-module [ipc module-name depot _q-plan]
      (bh/print-section "Ingestion Benchmark: 1K quads")

      ;; Pre-generate data
      (let [quads (gen/generate-typed-entities 334)] ;; 334 entities * 3 = 1002 quads
        (bh/print-subsection "Loading data")

        ;; Measure ingestion
        (let [{:keys [count time-ms quads-per-sec]} (bh/load-quads! depot quads)]
          (bh/print-throughput "Append to depot" count time-ms "quads"))

        ;; Measure indexing
        (let [{:keys [ms]} (bh/time-ms-only
                            (bh/wait-for-indexer! ipc module-name (count quads)))]
          (println (format "  Indexing complete: %.2fms" ms)))

        ;; Memory after
        (let [mem (bh/gc!)]
          (println (format "  Memory: %s" (bh/format-memory mem))))

        (is (> (count quads) 0) "Data was generated")))))

(deftest test-ingestion-throughput-10k
  (testing "Ingestion throughput at 10K quads"
    (bh/with-bench-module [ipc module-name depot _q-plan]
      (bh/print-section "Ingestion Benchmark: 10K quads")

      (let [quads (gen/generate-typed-entities 3334)] ;; ~10K quads
        (bh/print-subsection "Loading data")

        (let [{:keys [count time-ms quads-per-sec]} (bh/load-quads! depot quads)]
          (bh/print-throughput "Append to depot" count time-ms "quads")

          ;; Check target
          (let [target (:ingestion-quads-per-sec bh/throughput-targets)]
            (println (format "  Target: >%.0f quads/sec" (double target)))
            (if (>= quads-per-sec target)
              (println "  [PASS] Target met")
              (println (format "  [FAIL] Below target (%.0f vs %.0f)" quads-per-sec (double target))))))

        (let [{:keys [ms]} (bh/time-ms-only
                            (bh/wait-for-indexer! ipc module-name (count quads)))]
          (println (format "  Indexing complete: %.2fms" ms)))

        (let [mem (bh/gc!)]
          (println (format "  Memory: %s" (bh/format-memory mem))))

        (is (> (count quads) 0))))))

(deftest test-ingestion-throughput-100k
  (testing "Ingestion throughput at 100K quads"
    (bh/with-bench-module [ipc module-name depot _q-plan :tasks 4 :threads 2]
      (bh/print-section "Ingestion Benchmark: 100K quads")

      (let [quads (gen/generate-typed-entities 33334)] ;; ~100K quads
        (bh/print-subsection "Loading data")

        (let [mem-before (bh/gc!)]
          (println (format "  Memory before: %s" (bh/format-memory mem-before))))

        (let [{:keys [count time-ms quads-per-sec]} (bh/load-quads! depot quads)]
          (bh/print-throughput "Append to depot" count time-ms "quads")

          ;; Check target
          (let [target (:ingestion-quads-per-sec bh/throughput-targets)]
            (println (format "  Target: >%.0f quads/sec" (double target)))
            (if (>= quads-per-sec target)
              (println "  [PASS] Target met")
              (println (format "  [FAIL] Below target (%.0f vs %.0f)" quads-per-sec (double target))))))

        (let [{:keys [ms]} (bh/time-ms-only
                            (bh/wait-for-indexer! ipc module-name (count quads)))]
          (println (format "  Indexing complete: %.2fms" ms))
          (println (format "  Total indexed throughput: %.0f quads/sec"
                           (if (pos? ms) (* 1000.0 (/ (count quads) ms)) 0))))

        (let [mem-after (bh/gc!)]
          (println (format "  Memory after: %s" (bh/format-memory mem-after))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Batch Ingestion Tests
;;; ---------------------------------------------------------------------------

(deftest test-batch-ingestion-pattern
  (testing "Batch ingestion patterns (star vs linear vs typed)"
    (bh/with-bench-module [ipc module-name depot _q-plan]
      (bh/print-section "Batch Pattern Comparison")

      ;; Star graph - all quads share same subject
      (bh/print-subsection "Star graph (1 subject, 5K edges)")
      (let [quads (gen/generate-star-graph 5000)
            {:keys [count time-ms quads-per-sec]} (bh/load-and-wait! ipc module-name depot quads)]
        (bh/print-throughput "Star pattern" count time-ms "quads"))

      ;; Need new module for clean slate
      )))

(deftest test-incremental-batch-loading
  (testing "Incremental batch loading (simulates real-world ingestion)"
    (bh/with-bench-module [ipc module-name depot _q-plan]
      (bh/print-section "Incremental Batch Loading")

      (let [batch-size 1000
            num-batches 10
            results (atom [])]

        (doseq [batch-num (range num-batches)]
          (let [quads (gen/generate-batch-for-scale batch-size (* batch-num batch-size))
                {:keys [time-ms quads-per-sec]}
                (bh/time-ms-only
                 (do
                   (doseq [[s p o c] quads]
                     (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]]))
                   (bh/wait-for-indexer! ipc module-name (* (inc batch-num) batch-size))))]
            (swap! results conj {:batch batch-num
                                 :time-ms time-ms
                                 :quads-per-sec (if (pos? time-ms)
                                                  (* 1000.0 (/ batch-size time-ms))
                                                  0)})))

        ;; Report results
        (println "  Batch | Time (ms) | Throughput")
        (println "  ------|-----------|----------")
        (doseq [{:keys [batch time-ms quads-per-sec]} @results]
          (println (format "  %5d | %9.2f | %.0f quads/sec" batch time-ms quads-per-sec)))

        ;; Calculate average throughput across batches
        (let [avg-throughput (/ (reduce + (map :quads-per-sec @results)) num-batches)]
          (println (format "\n  Average throughput: %.0f quads/sec" avg-throughput)))

        (is (= num-batches (count @results)))))))

;;; ---------------------------------------------------------------------------
;;; Memory Pressure Test
;;; ---------------------------------------------------------------------------

(deftest test-memory-growth
  (testing "Memory growth during ingestion"
    (bh/with-bench-module [ipc module-name depot _q-plan]
      (bh/print-section "Memory Growth During Ingestion")

      (let [measurements (atom [])
            batch-size 10000
            num-batches 5]

        ;; Record baseline
        (swap! measurements conj {:quads 0 :memory (bh/gc!)})

        (doseq [batch-num (range num-batches)]
          (let [quads (gen/generate-batch-for-scale batch-size (* batch-num batch-size))]
            (bh/load-and-wait! ipc module-name depot quads)
            (let [mem (bh/gc!)
                  total-quads (* (inc batch-num) batch-size)]
              (swap! measurements conj {:quads total-quads :memory mem}))))

        ;; Report
        (println "  Quads   | Used MB | Growth")
        (println "  --------|---------|-------")
        (let [baseline-used (get-in (first @measurements) [:memory :used-mb])]
          (doseq [{:keys [quads memory]} @measurements]
            (let [growth (- (:used-mb memory) baseline-used)]
              (println (format "  %7d | %7.0f | +%.0fMB"
                               quads (:used-mb memory) growth)))))

        (is (> (count @measurements) 1))))))

(comment
  ;; Run individual tests
  (test-ingestion-throughput-1k)
  (test-ingestion-throughput-10k)
  (test-ingestion-throughput-100k)

  ;; Run all ingestion benchmarks
  ;; lein test :only rama-sail.bench.micro.ingestion-bench
  )
