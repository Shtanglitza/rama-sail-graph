(ns ^{:bench true} rama-sail.bench.join-comparison-bench
  "Benchmark comparing standard vs parallel probe hash join implementations.

   Runs identical join queries using both implementations in IPC mode
   to measure performance differences before deploying to cluster.

   Key insight: Rama query topologies don't allow partitioning after aggregation.
   So we compare:
   - Standard join: Build hash table, probe sequentially at origin
   - Shuffle-probe join: Build hash table at origin, shuffle probe rows, parallel probe

   Run with: lein test :only rama-sail.bench.join-comparison-bench"
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest])
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]))

;;; ---------------------------------------------------------------------------
;;; Helper Functions
;;; ---------------------------------------------------------------------------

(defn- extract-join-key
  "Extract join key values from a binding map."
  [binding join-vars]
  (when binding
    (let [values (mapv #(get binding %) join-vars)]
      (when (every? some? values)
        values))))

(defn- build-hash-index
  "Build a hash index from a collection of bindings."
  [bindings join-vars]
  (reduce
   (fn [idx binding]
     (let [key (extract-join-key binding join-vars)]
       (if key
         (update idx key (fnil conj []) binding)
         idx)))
   {}
   bindings))

(defn- probe-hash-index
  "Probe hash index with a binding. Returns seq of merged bindings or nil."
  [hash-idx binding join-vars]
  (let [key (extract-join-key binding join-vars)]
    (when key
      (when-let [matches (get hash-idx key)]
        (map #(merge % binding) matches)))))

;; Combiner for set union - only used at final aggregation
(def +set-union-combiner
  (combiner
   (fn [set1 set2] (into (or set1 #{}) set2))
   :init-fn (fn [] #{})))

;;; ---------------------------------------------------------------------------
;;; Comparison Module Definition
;;; ---------------------------------------------------------------------------

(defmodule JoinComparisonModule [setup topologies]
  ;; Simple test data PState: table-name -> [{row1} {row2} ...]
  (let [mb (microbatch-topology topologies "data-loader")]
    (declare-pstate mb $$test-data {String (vector-schema {String String})})

    (<<sources mb
               (source> *data-depot :> %microbatch)
               (%microbatch :> [*table-name *row])
               (|hash *table-name)
               (local-transform> [(keypath *table-name) NIL->VECTOR AFTER-ELEM (termval *row)] $$test-data)))

  (declare-depot setup *data-depot (hash-by first))

  ;; Query to load test data
  (<<query-topology topologies "get-table"
                    [*table-name :> *rows]
                    (|hash *table-name)
                    (local-select> [(keypath *table-name) (view vec)] $$test-data :> *data)
                    (<<if (nil? *data)
                          (identity [] :> *rows)
                          (else>)
                          (identity *data :> *rows))
                    (|origin))

  ;; === STANDARD JOIN (single-task hash join) ===
  ;; All work happens on origin task:
  ;; - Build hash table
  ;; - Probe sequentially
  (<<query-topology topologies "standard-join"
                    [*left-table *right-table *join-vars :> *results]
    ;; Get data
                    (invoke-query "get-table" *left-table :> *left-results)
                    (invoke-query "get-table" *right-table :> *right-results)

    ;; Build hash index on right side (at origin)
                    (build-hash-index *right-results *join-vars :> *hash-idx)

    ;; Probe with left side (sequential at origin)
                    (ops/explode *left-results :> *left-bind)
                    (probe-hash-index *hash-idx *left-bind *join-vars :> *matches)
                    (filter> (some? *matches))
                    (ops/explode *matches :> *joined)

                    (|origin)
                    (aggs/+set-agg *joined :> *results))

  ;; === SHUFFLE-PROBE JOIN (parallel probe) ===
  ;; Build phase at origin, probe phase distributed:
  ;; - Build hash table at origin
  ;; - Shuffle probe rows across all tasks
  ;; - Each task probes (parallel)
  ;; - Combine results at origin with two-phase aggregation
  (<<query-topology topologies "shuffle-probe-join"
                    [*left-table *right-table *join-vars :> *results]
    ;; Get data
                    (invoke-query "get-table" *left-table :> *left-results)
                    (invoke-query "get-table" *right-table :> *right-results)

    ;; Build hash index on right side (at origin - same as standard)
                    (build-hash-index *right-results *join-vars :> *hash-idx)

    ;; Probe with left side - DISTRIBUTED via shuffle
                    (ops/explode *left-results :> *left-bind)
                    (|shuffle)  ;; Distribute probe work across all tasks
                    (probe-hash-index *hash-idx *left-bind *join-vars :> *matches)
                    (filter> (some? *matches))
                    (ops/explode *matches :> *joined)

    ;; Wrap in set for combiner (enables two-phase aggregation)
                    (hash-set *joined :> *result-set)
                    (|origin)
                    (+set-union-combiner *result-set :> *results)))

;;; ---------------------------------------------------------------------------
;;; Benchmark Helpers
;;; ---------------------------------------------------------------------------

(defn- time-query
  "Execute query and return [result-count time-ms]."
  [query-fn]
  (let [start (System/nanoTime)
        result (query-fn)
        end (System/nanoTime)
        ms (/ (- end start) 1000000.0)]
    [(count result) ms]))

(defn- benchmark-query
  "Run query multiple times and return statistics."
  [query-fn warmup-n bench-n]
  ;; Warmup
  (dotimes [_ warmup-n]
    (query-fn))

  ;; Benchmark
  (let [results (vec (for [_ (range bench-n)]
                       (second (time-query query-fn))))
        sorted (sort results)]
    {:min (first sorted)
     :max (last sorted)
     :p50 (nth sorted (int (* 0.5 (count sorted))))
     :p95 (nth sorted (min (dec (count sorted)) (int (* 0.95 (count sorted)))))
     :mean (/ (reduce + results) (double (count results)))
     :n bench-n}))

(defn- format-stats [stats]
  (format "n=%d | min=%.2fms | p50=%.2fms | p95=%.2fms | mean=%.2fms"
          (:n stats) (:min stats) (:p50 stats) (:p95 stats) (:mean stats)))

;;; ---------------------------------------------------------------------------
;;; Data Generation
;;; ---------------------------------------------------------------------------

(defn- generate-products
  "Generate product data: [{\"?id\" \"p1\" \"?name\" \"Product 1\" \"?producer\" \"prod1\"} ...]"
  [n num-producers]
  (vec (for [i (range n)]
         {"?id" (str "p" i)
          "?name" (str "Product " i)
          "?producer" (str "prod" (mod i num-producers))})))

(defn- generate-producers
  "Generate producer data: [{\"?producer\" \"prod1\" \"?country\" \"US\"} ...]"
  [n]
  (vec (for [i (range n)]
         {"?producer" (str "prod" i)
          "?country" (if (zero? (mod i 2)) "US" "EU")})))

(defn- generate-offers
  "Generate offer data: [{\"?id\" \"p1\" \"?price\" \"100\"} ...]"
  [n num-products]
  (vec (for [i (range n)]
         {"?id" (str "p" (mod i num-products))
          "?price" (str (+ 10 (rand-int 990)))})))

(defn- load-test-data!
  "Load test data into the module."
  [ipc depot products producers offers]
  (println (format "  Loading %d products, %d producers, %d offers..."
                   (count products) (count producers) (count offers)))

  ;; Load products
  (doseq [p products]
    (rama/foreign-append! depot ["products" p]))

  ;; Load producers
  (doseq [p producers]
    (rama/foreign-append! depot ["producers" p]))

  ;; Load offers
  (doseq [o offers]
    (rama/foreign-append! depot ["offers" o]))

  ;; Wait for processing
  (rtest/wait-for-microbatch-processed-count ipc "rama-sail.bench.join-comparison-bench/JoinComparisonModule" "data-loader" 1))

;;; ---------------------------------------------------------------------------
;;; Benchmark Tests
;;; ---------------------------------------------------------------------------

(defn run-join-comparison
  "Run comparison benchmark with configurable data sizes.

   Options:
   - :num-products - Number of products (default: 1000)
   - :num-producers - Number of producers (default: 100)
   - :num-offers - Number of offers (default: 5000)
   - :warmup-n - Warmup iterations (default: 5)
   - :bench-n - Benchmark iterations (default: 20)
   - :tasks - Rama tasks (default: 8)
   - :threads - Rama threads (default: 2)"
  [& {:keys [num-products num-producers num-offers warmup-n bench-n tasks threads]
      :or {num-products 1000
           num-producers 100
           num-offers 5000
           warmup-n 5
           bench-n 20
           tasks 8
           threads 2}}]

  (println)
  (println "=== Join Implementation Comparison Benchmark ===")
  (println)
  (println (format "Configuration: %d products, %d producers, %d offers"
                   num-products num-producers num-offers))
  (println (format "IPC Config: %d tasks, %d threads" tasks threads))
  (println)

  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc JoinComparisonModule {:tasks tasks :threads threads})

    (let [module-name "rama-sail.bench.join-comparison-bench/JoinComparisonModule"
          depot (rama/foreign-depot ipc module-name "*data-depot")

          ;; Generate test data
          products (generate-products num-products num-producers)
          producers (generate-producers num-producers)
          offers (generate-offers num-offers num-products)

          ;; Load data
          _ (load-test-data! ipc depot products producers offers)

          ;; Get query handles
          standard-join-qt (rama/foreign-query ipc module-name "standard-join")
          shuffle-join-qt (rama/foreign-query ipc module-name "shuffle-probe-join")]

      (println)
      (println "--- Test 1: Products ⋈ Producers (on ?producer) ---")
      (println (format "  Join sizes: %d × %d, expected ~%d results"
                       num-products num-producers num-products))

      ;; Verify correctness first
      (let [std-result (rama/foreign-invoke-query standard-join-qt "products" "producers" ["?producer"])
            shuffle-result (rama/foreign-invoke-query shuffle-join-qt "products" "producers" ["?producer"])]
        (println (format "  Result counts: standard=%d, shuffle-probe=%d"
                         (count std-result) (count shuffle-result)))

        ;; Verify correctness
        (when (not= (count std-result) (count shuffle-result))
          (println "  WARNING: Result count mismatch!")))

      (println)
      (println "  Benchmarking standard join (single-task)...")
      (let [std-stats (benchmark-query
                       #(rama/foreign-invoke-query standard-join-qt "products" "producers" ["?producer"])
                       warmup-n bench-n)]
        (println (str "    Standard:        " (format-stats std-stats))))

      (println "  Benchmarking shuffle-probe join (parallel probe)...")
      (let [shuffle-stats (benchmark-query
                           #(rama/foreign-invoke-query shuffle-join-qt "products" "producers" ["?producer"])
                           warmup-n bench-n)]
        (println (str "    Shuffle-probe:   " (format-stats shuffle-stats))))

      (println)
      (println "--- Test 2: Offers ⋈ Products (on ?id, larger join) ---")
      (println (format "  Join sizes: %d × %d" num-offers num-products))

      ;; Verify
      (let [std-result (rama/foreign-invoke-query standard-join-qt "offers" "products" ["?id"])
            shuffle-result (rama/foreign-invoke-query shuffle-join-qt "offers" "products" ["?id"])]
        (println (format "  Result counts: standard=%d, shuffle-probe=%d"
                         (count std-result) (count shuffle-result))))

      (println)
      (println "  Benchmarking standard join...")
      (let [std-stats (benchmark-query
                       #(rama/foreign-invoke-query standard-join-qt "offers" "products" ["?id"])
                       warmup-n bench-n)]
        (println (str "    Standard:        " (format-stats std-stats))))

      (println "  Benchmarking shuffle-probe join...")
      (let [shuffle-stats (benchmark-query
                           #(rama/foreign-invoke-query shuffle-join-qt "offers" "products" ["?id"])
                           warmup-n bench-n)]
        (println (str "    Shuffle-probe:   " (format-stats shuffle-stats))))

      (println)
      (println "=== Benchmark Complete ==="))))

(deftest test-join-comparison-small
  (testing "Join comparison with small dataset"
    (run-join-comparison
     :num-products 100
     :num-producers 20
     :num-offers 500
     :warmup-n 3
     :bench-n 10
     :tasks 4
     :threads 2)
    (is true)))

(deftest test-join-comparison-medium
  (testing "Join comparison with medium dataset"
    (run-join-comparison
     :num-products 1000
     :num-producers 100
     :num-offers 5000
     :warmup-n 5
     :bench-n 20
     :tasks 8
     :threads 2)
    (is true)))

(comment
  ;; Run benchmarks manually
  (run-join-comparison
   :num-products 100
   :num-producers 20
   :num-offers 500
   :warmup-n 3
   :bench-n 10)

  ;; Larger test
  (run-join-comparison
   :num-products 5000
   :num-producers 200
   :num-offers 25000
   :warmup-n 5
   :bench-n 20
   :tasks 16
   :threads 2))
