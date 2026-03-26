(ns rama-sail.query.filter-test
  (:require [clojure.test :refer :all]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.test :as rtest]
            [com.rpl.rama.ops :as ops]
            [clojure.string :as str])
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:import (java.util.regex Pattern)))

;; -----------------------------------------------------------------------------
;; 1. Helper Logic (Parsing & Evaluation)
;; -----------------------------------------------------------------------------

(def ^Pattern LITERAL-PATTERN #"^\"(.*?)\"")

(defn parse-numeric [s]
  ;; FIX: Convert input to string immediately to handle Integer constants (e.g., 5000)
  (try
    (let [s-str (str s)
          clean (if (str/starts-with? s-str "\"")
                  (let [m (.matcher LITERAL-PATTERN s-str)]
                    (if (.find m)
                      (.group m 1)
                      s-str))
                  s-str)]
      (Double/parseDouble clean))
    (catch Exception _ nil)))

(defn eval-expr [expr bindings]
  (case (:type expr)
    :const (:val expr)
    :var   (get bindings (:name expr))
    :cmp   (let [l-raw (eval-expr (:left expr) bindings)
                 r-raw (eval-expr (:right expr) bindings)
                 op    (:op expr)]
             (if (and l-raw r-raw)
               (let [l-num (parse-numeric l-raw)
                     r-num (parse-numeric r-raw)]
                 (if (and l-num r-num)
                   (case op
                     :gt (> l-num r-num)
                     :lt (< l-num r-num)
                     :eq (== l-num r-num)
                     false)
                   (case op
                     :eq (= l-raw r-raw)
                     false)))
               false))
    false))

(defn evaluate-filter-cond [expr binding-map]
  ;; SIMULATION: Artificial delay to mimic complex RDF logic (e.g., Regex)
  ;; 0.5ms per item * 10,000 items = ~5 seconds of work if done serially.
  (Thread/sleep 0 500000) ;; Sleep 0.5ms (500,000 nanoseconds)
  (boolean (eval-expr expr binding-map)))

;(defn evaluate-filter-cond [expr binding-map]
;  (boolean (eval-expr expr binding-map)))

;; -----------------------------------------------------------------------------
;; 2. Custom Combiner
;; -----------------------------------------------------------------------------

;; A combiner allows Rama to aggregate locally on each task (Phase 1)
;; before sending the reduced result to the origin (Phase 2).
(def +set-union
  (combiner
   (fn [set1 set2] (into (or set1 #{}) set2))
   :init-fn (fn [] #{})))

;; -----------------------------------------------------------------------------
;; 3. Module Definition (Comparing Both Approaches)
;; -----------------------------------------------------------------------------

(defmodule FilterComparisonModule [setup topologies]
  ;; --- MOCK DATA SOURCE ---
  ;; Simulates a sub-query returning 10,000 items
  (<<query-topology topologies "execute-plan" [*ignore :> *results]
    ;; Generate numbers 0 to 9999
                    (ops/range> 0 10000 :> *i)
    ;; Create a binding map {"?x" "123"}
                    (hash-map "?x" (str *i) :> *binding)
    ;; Aggregate into a list to simulate a bulk result
                    (|origin)
                    (aggs/+vec-agg *binding :> *results))

  ;; --- 1. ORIGINAL (Serial / Unoptimized) ---
  (<<query-topology topologies "filter-original" [*sub-plan *expr :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
    ;; Logic runs serially on the single task handling the query
                    (filter> (evaluate-filter-cond *expr *binding))
                    (|origin)
                    (aggs/+set-agg *binding :> *results))

  ;; --- 2. OPTIMIZED (Parallel / Combiner) ---
  (<<query-topology topologies "filter-optimized" [*sub-plan *expr :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
    ;; Distribute work to all cores
                    (|shuffle)
    ;; Filter in parallel
                    (filter> (evaluate-filter-cond *expr *binding))
    ;; Pre-wrap in a set for the combiner
                    (hash-set *binding :> *binding-set)
    ;; Return to origin
                    (|origin)
    ;; Two-phase aggregation happens here automatically because of the combiner
                    (+set-union *binding-set :> *results)))

;; -----------------------------------------------------------------------------
;; 4. The Test
;; -----------------------------------------------------------------------------

(deftest compare-filter-performance-test
  (with-open [ipc (rtest/create-ipc)]
    (testing "Launch Module"
      ;; Launch with 4 tasks to simulate a distributed environment
      (rtest/launch-module! ipc FilterComparisonModule {:tasks 4 :threads 4}))

    (let [module-name (get-module-name FilterComparisonModule)
          filter-original (foreign-query ipc module-name "filter-original")
          filter-optimized (foreign-query ipc module-name "filter-optimized")
          ;; Define a filter: ?x > 5000
          filter-expr {:type :cmp
                       :op   :gt
                       :left {:type :var :name "?x"}
                       :right {:type :const :val 5000}}

          ;; Helper to time execution
          time-query (fn [topology-name]
                       (let [start (System/nanoTime)
                             res   (foreign-invoke-query topology-name nil filter-expr)
                             end   (System/nanoTime)]
                         {:result res
                          :ms     (/ (- end start) 1e6)}))]

      (testing "Correctness & Comparison"
        ;; Run Original
        (let [res-orig (time-query filter-original)]
          (println (format "Original Duration: %.2f ms" (:ms res-orig)))

          ;; Run Optimized
          (let [res-opt (time-query filter-optimized)]
            (println (format "Optimized Duration: %.2f ms" (:ms res-opt)))

            ;; 1. Check Correctness: Results must be identical
            (is (= (:result res-orig) (:result res-opt))
                "Results of original and optimized queries should be identical")

            ;; 2. Check Logic: We expect roughly 4999 results (5001 to 9999)
            (is (= 4999 (count (:result res-opt)))
                "Filter logic should have removed items <= 5000")))))))

(comment

  ;; To run the test, uncomment the following line:
  (compare-filter-performance-test))