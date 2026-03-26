(ns rama-sail.bench.bsbm.infra.ingestion-diagnostic
  "Diagnostic tests to identify ingestion bottlenecks.

   Potential bottlenecks:
   1. N-Triples parsing (CPU)
   2. Individual foreign-append! calls (IPC RPC overhead)
   3. Four index updates per triple (write amplification)
   4. Microbatch processing overhead
   5. wait-for-microbatch-processed-count blocking"
  (:require [clojure.java.io :as io]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.bench.bsbm.infra.ntriples-loader :as loader]
            [rama-sail.bench.infra.bench-helpers :as bh]))

;;; ---------------------------------------------------------------------------
;;; Diagnostic 1: Pure Parsing Speed
;;; ---------------------------------------------------------------------------

(defn bench-parsing-only
  "Measure pure N-Triples parsing speed without any Rama calls."
  [file-path]
  (println "\n=== Diagnostic 1: Pure Parsing Speed ===")
  (let [start (System/nanoTime)
        count (atom 0)]
    (with-open [rdr (io/reader file-path)]
      (doseq [line (line-seq rdr)]
        (when-let [triple (loader/parse-ntriples-line line)]
          (swap! count inc))))
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
          rate (/ @count (/ elapsed-ms 1000))]
      (println (format "  Parsed %d triples in %.2fms (%.0f triples/sec)"
                       @count elapsed-ms rate))
      {:count @count :time-ms elapsed-ms :rate rate})))

;;; ---------------------------------------------------------------------------
;;; Diagnostic 2: Append Without Waiting
;;; ---------------------------------------------------------------------------

(defn bench-append-no-wait
  "Measure append speed without waiting for indexer."
  [ipc module-name file-path]
  (println "\n=== Diagnostic 2: Append Without Waiting ===")
  (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
        start (System/nanoTime)
        count (atom 0)]
    (with-open [rdr (io/reader file-path)]
      (doseq [line (line-seq rdr)]
        (when-let [[s p o] (loader/parse-ntriples-line line)]
          (rama/foreign-append! depot [:add [s p o core/DEFAULT-CONTEXT-VAL]])
          (swap! count inc))))
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
          rate (/ @count (/ elapsed-ms 1000))]
      (println (format "  Appended %d triples in %.2fms (%.0f triples/sec)"
                       @count elapsed-ms rate))
      {:count @count :time-ms elapsed-ms :rate rate})))

;;; ---------------------------------------------------------------------------
;;; Diagnostic 3: Indexer Processing Time
;;; ---------------------------------------------------------------------------

(defn bench-indexer-wait
  "Measure time for indexer to process already-appended triples."
  [ipc module-name expected-count]
  (println "\n=== Diagnostic 3: Indexer Processing Time ===")
  (let [start (System/nanoTime)]
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" expected-count)
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
          rate (/ expected-count (/ elapsed-ms 1000))]
      (println (format "  Indexed %d triples in %.2fms (%.0f triples/sec)"
                       expected-count elapsed-ms rate))
      {:count expected-count :time-ms elapsed-ms :rate rate})))

;;; ---------------------------------------------------------------------------
;;; Diagnostic 4: Batch Append (Async)
;;; ---------------------------------------------------------------------------

(defn bench-batch-append-async
  "Measure append speed using async appends in batches."
  [ipc module-name file-path batch-size]
  (println (format "\n=== Diagnostic 4: Batch Async Append (batch=%d) ===" batch-size))
  (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
        start (System/nanoTime)
        count (atom 0)
        futures (atom [])]
    (with-open [rdr (io/reader file-path)]
      (doseq [lines (partition-all batch-size (line-seq rdr))]
        (doseq [line lines]
          (when-let [[s p o] (loader/parse-ntriples-line line)]
            ;; Use foreign-append! but don't wait - let them queue up
            (rama/foreign-append! depot [:add [s p o core/DEFAULT-CONTEXT-VAL]])
            (swap! count inc)))))
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
          rate (/ @count (/ elapsed-ms 1000))]
      (println (format "  Appended %d triples in %.2fms (%.0f triples/sec)"
                       @count elapsed-ms rate))
      {:count @count :time-ms elapsed-ms :rate rate})))

;;; ---------------------------------------------------------------------------
;;; Diagnostic 5: Single vs Multiple Index Cost
;;; ---------------------------------------------------------------------------

(defn estimate-index-overhead
  "Estimate the overhead of 4-index updates."
  []
  (println "\n=== Diagnostic 5: Index Write Amplification ===")
  (println "  Each triple update writes to 4 PStates:")
  (println "    - $$spoc: S -> P -> O -> Set<C>")
  (println "    - $$posc: P -> O -> S -> Set<C>")
  (println "    - $$ospc: O -> S -> P -> Set<C>")
  (println "    - $$cspo: C -> S -> P -> Set<O>")
  (println "  Write amplification factor: 4x")
  (println "  Plus repartitioning (|hash) for each index"))

;;; ---------------------------------------------------------------------------
;;; Full Diagnostic Run
;;; ---------------------------------------------------------------------------

(defn run-diagnostics
  "Run all ingestion diagnostics."
  [file-path]
  (println "\n" (apply str (repeat 70 "=")))
  (println "INGESTION BOTTLENECK DIAGNOSTICS")
  (println (apply str (repeat 70 "=")))

  ;; 1. Pure parsing
  (let [parse-stats (bench-parsing-only file-path)]

    ;; 2-4. Rama operations
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
      (let [module-name (rama/get-module-name core/RdfStorageModule)]

        ;; 2. Append without waiting
        (let [append-stats (bench-append-no-wait ipc module-name file-path)]

          ;; 3. Indexer processing
          (let [index-stats (bench-indexer-wait ipc module-name (:count append-stats))]

            ;; 5. Index overhead explanation
            (estimate-index-overhead)

            ;; Summary
            (println "\n" (apply str (repeat 70 "=")))
            (println "SUMMARY")
            (println (apply str (repeat 70 "=")))
            (println (format "  Pure parsing:     %.0f triples/sec" (:rate parse-stats)))
            (println (format "  Append (no wait): %.0f triples/sec" (:rate append-stats)))
            (println (format "  Indexer wait:     %.0f triples/sec" (:rate index-stats)))
            (println)
            (println "  Bottleneck Analysis:")
            (let [parse-pct (* 100.0 (/ (:time-ms parse-stats)
                                        (+ (:time-ms parse-stats) (:time-ms append-stats) (:time-ms index-stats))))
                  append-pct (* 100.0 (/ (:time-ms append-stats)
                                         (+ (:time-ms parse-stats) (:time-ms append-stats) (:time-ms index-stats))))
                  index-pct (* 100.0 (/ (:time-ms index-stats)
                                        (+ (:time-ms parse-stats) (:time-ms append-stats) (:time-ms index-stats))))]
              (println (format "    Parsing:  %5.1f%% of total time" parse-pct))
              (println (format "    Appending: %5.1f%% of total time" append-pct))
              (println (format "    Indexing: %5.1f%% of total time" index-pct)))

            {:parse parse-stats
             :append append-stats
             :index index-stats}))))))

(comment
  ;; Run diagnostics on 100-product dataset
  (run-diagnostics "test/resources/bsbm/dataset_100.nt")

  ;; Run on larger dataset
  (run-diagnostics "test/resources/bsbm/dataset_500.nt"))
