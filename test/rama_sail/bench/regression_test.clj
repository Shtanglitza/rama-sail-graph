(ns rama-sail.bench.regression-test
  "Performance regression tests to catch optimization regressions.

   These tests run quick benchmarks and assert that p50 latencies
   stay below defined thresholds. Benchmark selector only, not part of CI:
   lein test :only rama-sail.bench.regression-test"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.sail.adapter :as sail])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository SailRepositoryConnection]))

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

(def ^:dynamic *repo* nil)

(defn- load-test-data!
  "Insert `test-triples` through the SAIL; a sync commit returns once they are visible."
  [^SailRepository repo]
  (with-open [conn (.getConnection repo)]
    (.begin conn)
    (.execute (.prepareUpdate conn (str "INSERT DATA {\n"
                                        (apply str (for [[s p o] test-triples] (str s " " p " " o " .\n")))
                                        "}")))
    (.commit conn)))

(defn- repo-fixture
  "One in-process cluster for the namespace, loaded once."
  [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})
    (let [repo (doto (SailRepository. (sail/create-rama-sail ipc (rama/get-module-name core/RdfStorageModule)
                                                             {:sync-commits true}))
                 (.init))]
      (try
        (load-test-data! repo)
        (binding [*repo* repo] (f))
        (finally (.shutDown repo))))))

(use-fixtures :once repo-fixture)

(defn- run-query [^SailRepositoryConnection conn ^String sparql]
  (with-open [result (.evaluate (.prepareTupleQuery conn sparql))]
    (count (iterator-seq result))))

(defn benchmark-query
  "Run query n times and return timing statistics and the row count."
  [conn sparql n]
  (let [rows (run-query conn sparql)
        _ (dotimes [_ 3] (run-query conn sparql))
        times (vec (for [_ (range n)]
                     (let [start (System/nanoTime)]
                       (run-query conn sparql)
                       (/ (- (System/nanoTime) start) 1000000.0))))
        sorted (sort times)]
    {:rows rows
     :min (first sorted)
     :p50 (nth sorted (int (* 0.5 (count sorted))))
     :max (last sorted)
     :mean (/ (reduce + times) (count times))}))

(def ^:private self-join-query
  "SELECT ?p1 ?p2 WHERE {
     ?p1 <http://ex/producer> ?prod .
     ?p2 <http://ex/producer> ?prod .
     FILTER (STR(?p1) < STR(?p2))
   }")

;;; ---------------------------------------------------------------------------
;;; Regression Tests
;;; ---------------------------------------------------------------------------

;; Each test also checks the row count: a latency check over an empty store passes
;; whatever the code does.

(deftest test-simple-lookup-latency
  (testing "Simple product lookup stays fast"
    (with-open [conn (.getConnection ^SailRepository *repo*)]
      (let [stats (benchmark-query conn "SELECT ?label WHERE { <http://ex/product1> <http://www.w3.org/2000/01/rdf-schema#label> ?label }" 10)]
        (is (= 1 (:rows stats)))
        (is (< (:p50 stats) (:q2-simple-lookup latency-thresholds))
            (str "Simple lookup p50 " (:p50 stats) "ms exceeds threshold "
                 (:q2-simple-lookup latency-thresholds) "ms"))))))

(deftest test-self-join-optimization
  (testing "Self-join query uses optimization"
    (with-open [conn (.getConnection ^SailRepository *repo*)]
      (let [stats (benchmark-query conn self-join-query 10)]
        ;; 2 producers with 2 products each = 2 pairs
        (is (= 2 (:rows stats)))
        (is (< (:p50 stats) (:qj4-self-join latency-thresholds))
            (str "Self-join p50 " (:p50 stats) "ms exceeds threshold "
                 (:qj4-self-join latency-thresholds) "ms"))))))

(deftest test-type-lookup-optimization
  (testing "Type lookup uses materialized view"
    (with-open [conn (.getConnection ^SailRepository *repo*)]
      (let [stats (benchmark-query conn "SELECT ?x WHERE { ?x a <http://ex/Product> }" 10)]
        (is (= 2 (:rows stats)))
        (is (< (:p50 stats) 100)
            (str "Type lookup p50 " (:p50 stats) "ms should be under 100ms"))))))

(comment
  ;; Run regression tests from REPL
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.bench.regression-test))
