(ns rama-sail.bench.regression-test
  "Performance regression tests to catch optimization regressions.

   These tests run quick benchmarks and assert that p50 latencies
   stay below defined thresholds. Run as part of CI to catch regressions."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.sail.adapter :as sail]
            [rama-sail.bench.infra.bench-helpers :as bh])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]))

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

;; Latency thresholds in milliseconds (p50)
(def latency-thresholds
  {:q2-simple-lookup 50      ; Simple product lookup
   :q7-multi-join 200        ; Product reviews with joins
   :qj4-self-join 300        ; Self-join query
   :ingestion-rate 50})      ; Triples per second minimum

;; Test dataset - small for fast CI runs
(def test-triples
  "Small test dataset for regression testing"
  [;; Products and producers
   ["<http://ex/product1>" "<http://ex/producer>" "<http://ex/producer1>"]
   ["<http://ex/product2>" "<http://ex/producer>" "<http://ex/producer1>"]
   ["<http://ex/product3>" "<http://ex/producer>" "<http://ex/producer2>"]
   ["<http://ex/product4>" "<http://ex/producer>" "<http://ex/producer2>"]
   ;; Labels
   ["<http://ex/product1>" "<http://www.w3.org/2000/01/rdf-schema#label>" "\"Product One\""]
   ["<http://ex/product2>" "<http://www.w3.org/2000/01/rdf-schema#label>" "\"Product Two\""]
   ["<http://ex/product3>" "<http://www.w3.org/2000/01/rdf-schema#label>" "\"Product Three\""]
   ["<http://ex/product4>" "<http://www.w3.org/2000/01/rdf-schema#label>" "\"Product Four\""]
   ;; Types
   ["<http://ex/product1>" "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" "<http://ex/Product>"]
   ["<http://ex/product2>" "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" "<http://ex/Product>"]
   ;; Reviews
   ["<http://ex/review1>" "<http://ex/reviewFor>" "<http://ex/product1>"]
   ["<http://ex/review1>" "<http://ex/rating>" "\"5\""]
   ["<http://ex/review2>" "<http://ex/reviewFor>" "<http://ex/product2>"]
   ["<http://ex/review2>" "<http://ex/rating>" "\"4\""]])

;;; ---------------------------------------------------------------------------
;;; Test Helpers
;;; ---------------------------------------------------------------------------

(def ^:dynamic *ipc* nil)
(def ^:dynamic *sail* nil)
(def ^:dynamic *repo* nil)

(defn load-test-data [ipc module-name]
  (let [depot (rama/foreign-depot ipc module-name "*triple-depot")]
    (doseq [[s p o] test-triples]
      (rama/foreign-append! depot [:add [s p o]]))
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)))

(defn benchmark-query
  "Run query n times and return timing statistics."
  [conn sparql n]
  (let [query (.prepareTupleQuery conn sparql)
        ;; Warmup
        _ (dotimes [_ 3]
            (with-open [result (.evaluate query)]
              (count (iterator-seq result))))
        ;; Benchmark
        times (vec (for [_ (range n)]
                     (let [start (System/nanoTime)
                           _ (with-open [result (.evaluate query)]
                               (count (iterator-seq result)))
                           end (System/nanoTime)]
                       (/ (- end start) 1000000.0))))
        sorted (sort times)]
    {:min (first sorted)
     :p50 (nth sorted (int (* 0.5 (count sorted))))
     :max (last sorted)
     :mean (/ (reduce + times) (count times))}))

;;; ---------------------------------------------------------------------------
;;; Regression Tests
;;; ---------------------------------------------------------------------------

(deftest test-simple-lookup-latency
  (testing "Simple product lookup stays fast"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
      (load-test-data ipc "rama-sail.core/RdfStorageModule")

      (let [sail (sail/create-rama-sail ipc "rama-sail.core/RdfStorageModule")
            repo (SailRepository. sail)]
        (.init repo)
        (try
          (with-open [conn (.getConnection repo)]
            (let [query "SELECT ?label WHERE { <http://ex/product1> <http://www.w3.org/2000/01/rdf-schema#label> ?label }"
                  stats (benchmark-query conn query 10)]
              (is (< (:p50 stats) (:q2-simple-lookup latency-thresholds))
                  (str "Simple lookup p50 " (:p50 stats) "ms exceeds threshold "
                       (:q2-simple-lookup latency-thresholds) "ms"))))
          (finally
            (.shutDown repo)))))))

(deftest test-self-join-optimization
  (testing "Self-join query uses optimization"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
      (load-test-data ipc "rama-sail.core/RdfStorageModule")

      (let [sail (sail/create-rama-sail ipc "rama-sail.core/RdfStorageModule")
            repo (SailRepository. sail)]
        (.init repo)
        (try
          (with-open [conn (.getConnection repo)]
            (let [query "SELECT ?p1 ?p2 WHERE {
                           ?p1 <http://ex/producer> ?prod .
                           ?p2 <http://ex/producer> ?prod .
                           FILTER (?p1 < ?p2)
                         }"
                  stats (benchmark-query conn query 10)]
              ;; With 4 products (2 per producer), should get 2 pairs
              ;; and be fast due to self-join optimization
              (is (< (:p50 stats) (:qj4-self-join latency-thresholds))
                  (str "Self-join p50 " (:p50 stats) "ms exceeds threshold "
                       (:qj4-self-join latency-thresholds) "ms"))))
          (finally
            (.shutDown repo)))))))

(deftest test-type-lookup-optimization
  (testing "Type lookup uses materialized view"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
      (load-test-data ipc "rama-sail.core/RdfStorageModule")

      (let [sail (sail/create-rama-sail ipc "rama-sail.core/RdfStorageModule")
            repo (SailRepository. sail)]
        (.init repo)
        (try
          (with-open [conn (.getConnection repo)]
            (let [query "SELECT ?x WHERE { ?x a <http://ex/Product> }"
                  stats (benchmark-query conn query 10)]
              ;; Should be fast due to type view
              (is (< (:p50 stats) 100)
                  (str "Type lookup p50 " (:p50 stats) "ms should be under 100ms"))))
          (finally
            (.shutDown repo)))))))

(deftest test-result-correctness
  (testing "Optimized queries return correct results"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
      (load-test-data ipc "rama-sail.core/RdfStorageModule")

      (let [sail (sail/create-rama-sail ipc "rama-sail.core/RdfStorageModule")
            repo (SailRepository. sail)]
        (.init repo)
        (try
          (with-open [conn (.getConnection repo)]
            ;; Self-join should return product pairs from same producer
            (let [query "SELECT ?p1 ?p2 WHERE {
                           ?p1 <http://ex/producer> ?prod .
                           ?p2 <http://ex/producer> ?prod .
                           FILTER (?p1 < ?p2)
                         }"
                  tq (.prepareTupleQuery conn query)]
              (with-open [result (.evaluate tq)]
                (let [results (vec (iterator-seq result))]
                  ;; 2 producers with 2 products each = 2 pairs total
                  (is (= 2 (count results))
                      "Self-join should return 2 pairs")))))
          (finally
            (.shutDown repo)))))))

(comment
  ;; Run regression tests from REPL
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.bench.regression-test)

  ;; Run specific test
  (test-self-join-optimization))
