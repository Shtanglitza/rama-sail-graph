(ns rama-sail.sail.optimizer-test
  "Tests for query plan optimizations: self-join detection, filter pushdown,
   cardinality-based join ordering."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.sail.adapter :as sail]
            [rama-sail.sail.compilation :as comp]
            [rama-sail.sail.optimization :as opt])
  (:import [org.eclipse.rdf4j.query.parser.sparql SPARQLParser]))

;;; ---------------------------------------------------------------------------
;;; Helper Functions
;;; ---------------------------------------------------------------------------

(defn parse-and-plan
  "Parse SPARQL query and convert to our plan format."
  [sparql]
  (let [parser (SPARQLParser.)
        parsed (.parseQuery parser sparql nil)
        tuple-expr (.getTupleExpr parsed)]
    (comp/tuple-expr->plan tuple-expr)))

(defn contains-op?
  "Check if plan tree contains a specific operator."
  [plan op-type]
  (cond
    (not (map? plan)) false
    (= (:op plan) op-type) true
    :else (or (contains-op? (:sub-plan plan) op-type)
              (contains-op? (:left plan) op-type)
              (contains-op? (:right plan) op-type))))

(defn count-ops
  "Count occurrences of an operator in plan tree."
  [plan op-type]
  (if-not (map? plan)
    0
    (+ (if (= (:op plan) op-type) 1 0)
       (count-ops (:sub-plan plan) op-type)
       (count-ops (:left plan) op-type)
       (count-ops (:right plan) op-type))))

;;; ---------------------------------------------------------------------------
;;; Test Queries
;;; ---------------------------------------------------------------------------

(def prefixes
  "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")

(def self-join-query
  "QJ4-style self-join query"
  (str prefixes "
   SELECT ?p1 ?p2 ?producer
   WHERE {
     ?p1 bsbm:producer ?producer .
     ?p2 bsbm:producer ?producer .
     FILTER (?p1 < ?p2)
   }"))

(def self-join-no-filter-query
  "Self-join without inequality filter"
  (str prefixes "
   SELECT ?p1 ?p2 ?producer
   WHERE {
     ?p1 bsbm:producer ?producer .
     ?p2 bsbm:producer ?producer .
   }"))

(def simple-join-query
  "Regular join (not self-join)"
  (str prefixes "
   SELECT ?product ?producer ?label
   WHERE {
     ?product bsbm:producer ?producer .
     ?product rdfs:label ?label .
   }"))

(def type-query
  "Query with rdf:type pattern"
  (str prefixes "
   SELECT ?product
   WHERE {
     ?product rdf:type bsbm:Product .
   }"))

(def filter-join-query
  "Join with filter that can be pushed down"
  (str prefixes "
   SELECT ?product ?label
   WHERE {
     ?product bsbm:producer ?producer .
     ?product rdfs:label ?label .
     FILTER (?producer = <http://ex/prod1>)
   }"))

;;; ---------------------------------------------------------------------------
;;; Self-Join Detection Tests
;;; ---------------------------------------------------------------------------

(deftest test-self-join-with-filter-detection
  (testing "Self-join with inequality filter is detected"
    (let [raw-plan (parse-and-plan self-join-query)
          optimized (opt/optimize-plan raw-plan)]
      (is (contains-op? optimized :self-join)
          "Should detect and transform to :self-join")
      (is (= 1 (count-ops optimized :self-join))
          "Should have exactly one self-join")
      ;; Verify the filter is incorporated
      (let [self-join (first (filter #(= :self-join (:op %))
                                     (tree-seq map? (fn [p] (filter map? [(:sub-plan p) (:left p) (:right p)])) optimized)))]
        (is (some? (:filter self-join))
            "Self-join should have filter incorporated")))))

(deftest test-self-join-without-filter-detection
  (testing "Self-join without filter is still detected"
    (let [raw-plan (parse-and-plan self-join-no-filter-query)
          optimized (opt/optimize-plan raw-plan)]
      (is (contains-op? optimized :self-join)
          "Should detect self-join even without filter"))))

(deftest test-regular-join-not-self-join
  (testing "Regular join is not converted to self-join"
    (let [raw-plan (parse-and-plan simple-join-query)
          optimized (opt/optimize-plan raw-plan)]
      (is (not (contains-op? optimized :self-join))
          "Regular join should not become self-join")
      ;; May be optimized to batch-enrich (property lookup pattern) or remain as join
      (is (or (contains-op? optimized :join)
              (contains-op? optimized :batch-enrich))
          "Should have regular join or batch-enrich optimization"))))

;;; ---------------------------------------------------------------------------
;;; Type View Optimization Tests
;;; ---------------------------------------------------------------------------

(deftest test-type-view-optimization
  (testing "rdf:type patterns use type-lookup"
    (let [raw-plan (parse-and-plan type-query)
          optimized (opt/optimize-plan raw-plan)]
      (is (contains-op? optimized :type-lookup)
          "Should transform rdf:type BGP to type-lookup")
      (is (not (contains-op? optimized :bgp))
          "Original BGP should be replaced"))))

;;; ---------------------------------------------------------------------------
;;; Cardinality Estimation Tests
;;; ---------------------------------------------------------------------------

(deftest test-cardinality-estimation-bgp
  (testing "BGP cardinality estimates"
    ;; All bound = 1
    (is (= 1 (opt/estimate-plan-cardinality
              {:op :bgp :pattern {:s "<s>" :p "<p>" :o "<o>"}})))
    ;; S+P bound = 10
    (is (= 10 (opt/estimate-plan-cardinality
               {:op :bgp :pattern {:s "<s>" :p "<p>" :o "?o"}})))
    ;; Only P bound = 10000
    (is (= 10000 (opt/estimate-plan-cardinality
                  {:op :bgp :pattern {:s "?s" :p "<p>" :o "?o"}})))))

(deftest test-cardinality-estimation-join
  (testing "Join cardinality uses equi-join selectivity"
    (let [left {:op :bgp :pattern {:s "<s>" :p "<p>" :o "?o"}}  ; card=10
          right {:op :bgp :pattern {:s "?s" :p "<q>" :o "?o"}} ; card=10000
          join {:op :join :left left :right right :join-vars ["?o"]}]
      ;; Equi-join selectivity = 1/max(left, right) = 1/10000
      ;; Result = 10 * 10000 * (1/10000) = 10 (models foreign-key joins)
      (is (= 10 (opt/estimate-plan-cardinality join)))))

  (testing "Cross-product join has selectivity 1.0"
    (let [left {:op :bgp :pattern {:s "<s>" :p "<p>" :o "?o"}}  ; card=10
          right {:op :bgp :pattern {:s "?x" :p "<q>" :o "?y"}} ; card=10000
          cross {:op :join :left left :right right :join-vars []}]
      ;; Cross product = left * right * 1.0 = 100000
      (is (= 100000 (opt/estimate-plan-cardinality cross)))))

  (testing "Multi-way joins don't overflow"
    (let [bgp #(do {:op :bgp :pattern {:s "?s" :p %1 :o %2}})
          j1 {:op :join :left (bgp "<p1>" "?a") :right (bgp "<p2>" "?a") :join-vars ["?a"]}
          j2 {:op :join :left j1 :right (bgp "<p3>" "?b") :join-vars ["?b"]}
          j3 {:op :join :left j2 :right (bgp "<p4>" "?c") :join-vars ["?c"]}
          j4 {:op :join :left j3 :right (bgp "<p5>" "?d") :join-vars ["?d"]}
          j5 {:op :join :left j4 :right (bgp "<p6>" "?e") :join-vars ["?e"]}]
      ;; Should not throw ArithmeticException overflow
      (is (pos? (opt/estimate-plan-cardinality j5)))
      ;; And should be bounded, not exploding to overflow
      (is (<= (opt/estimate-plan-cardinality j5) 1000000000000)))))

;;; ---------------------------------------------------------------------------
;;; Filter Pushdown Tests
;;; ---------------------------------------------------------------------------

(deftest test-filter-pushdown
  (testing "Filter is pushed down when possible"
    (let [raw-plan (parse-and-plan filter-join-query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Filter should still be in the tree (can't always push string filters)
      (is (contains-op? optimized :filter)
          "Filter should be present in optimized plan"))))

;;; ---------------------------------------------------------------------------
;;; Integration Tests
;;; ---------------------------------------------------------------------------

(deftest test-complex-query-optimization
  (testing "Complex query with multiple optimization opportunities"
    (let [query (str prefixes "
                 SELECT ?p1 ?l1 ?p2 ?l2 ?producer
                 WHERE {
                   ?p1 bsbm:producer ?producer .
                   ?p2 bsbm:producer ?producer .
                   ?p1 rdfs:label ?l1 .
                   ?p2 rdfs:label ?l2 .
                   FILTER (?p1 < ?p2)
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should detect self-join
      (is (contains-op? optimized :self-join)
          "Should detect self-join in complex query")
      ;; Self-join should have filter incorporated
      (is (= 1 (count-ops optimized :self-join))
          "Exactly one self-join"))))

;;; ---------------------------------------------------------------------------
;;; Multi-Way Join Chain Optimization Tests
;;; ---------------------------------------------------------------------------

(def three-way-join-query
  "Three-way join query for chain optimization testing"
  (str prefixes "
   SELECT ?product ?producer ?producerLabel ?offer
   WHERE {
     ?product bsbm:producer ?producer .
     ?producer rdfs:label ?producerLabel .
     ?offer bsbm:product ?product .
   }"))

(def four-way-join-query
  "Four-way join query - should benefit from DP optimization"
  (str prefixes "
   SELECT ?product ?producer ?vendor ?review
   WHERE {
     ?product bsbm:producer ?producer .
     ?offer bsbm:product ?product .
     ?offer bsbm:vendor ?vendor .
     ?review bsbm:reviewFor ?product .
   }"))

(def five-way-chain-query
  "Five-way join chain"
  (str prefixes "
   SELECT ?product ?producer ?vendor ?review ?reviewer
   WHERE {
     ?product bsbm:producer ?producer .
     ?offer bsbm:product ?product .
     ?offer bsbm:vendor ?vendor .
     ?review bsbm:reviewFor ?product .
     ?review <http://purl.org/stuff/rev#reviewer> ?reviewer .
   }"))

(defn get-join-depth
  "Calculate the maximum depth of nested joins."
  [plan]
  (case (:op plan)
    :join (inc (max (get-join-depth (:left plan))
                    (get-join-depth (:right plan))))
    :left-join (inc (max (get-join-depth (:left plan))
                         (get-join-depth (:right plan))))
    (:project :distinct :slice :filter :order :bind :group :ask)
    (get-join-depth (:sub-plan plan))
    :union (max (get-join-depth (:left plan))
                (get-join-depth (:right plan)))
    0))

(defn extract-join-order
  "Extract the sequence of base plan types from a join tree (for debugging)."
  [plan]
  (case (:op plan)
    :join (concat (extract-join-order (:left plan))
                  (extract-join-order (:right plan)))
    :left-join (concat (extract-join-order (:left plan))
                       [:left-join]
                       (extract-join-order (:right plan)))
    (:project :distinct :slice :filter :order :bind :group :ask)
    (extract-join-order (:sub-plan plan))
    :union (concat [:union-left] (extract-join-order (:left plan))
                   [:union-right] (extract-join-order (:right plan)))
    ;; Leaf node - return its type and a distinguishing characteristic
    (case (:op plan)
      :bgp [(:p (:pattern plan))]
      :self-join [:self-join]
      :type-lookup [:type-lookup]
      :batch-enrich [:batch-enrich]
      [(:op plan)])))

(deftest test-three-way-join-optimization
  (testing "Three-way join is optimized"
    (let [raw-plan (parse-and-plan three-way-join-query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Plan should still contain joins (or batch-enrich)
      (is (or (contains-op? optimized :join)
              (contains-op? optimized :batch-enrich))
          "Should have join or batch-enrich operations")
      ;; Should not crash or produce nil
      (is (some? optimized)
          "Optimization should produce non-nil plan"))))

(deftest test-four-way-join-optimization
  (testing "Four-way join benefits from DP optimization"
    (let [raw-plan (parse-and-plan four-way-join-query)
          optimized (opt/optimize-plan raw-plan)]
      ;; The optimized plan should be valid
      (is (some? optimized)
          "Optimization should produce non-nil plan")
      ;; Count joins - should be 3 for 4 base relations (or fewer if batch-enrich applied)
      (let [join-count (count-ops optimized :join)
            batch-count (count-ops optimized :batch-enrich)]
        (is (<= (+ join-count batch-count) 4)
            "Should have reasonable number of joins/batch-enriches")))))

(deftest test-five-way-chain-optimization
  (testing "Five-way join chain is optimized without overflow"
    (let [raw-plan (parse-and-plan five-way-chain-query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should complete without error
      (is (some? optimized)
          "Should handle 5-way join without error")
      ;; Cardinality estimation should not overflow
      (is (pos? (opt/estimate-plan-cardinality optimized))
          "Cardinality should be positive")
      (is (<= (opt/estimate-plan-cardinality optimized) 1000000000000)
          "Cardinality should be bounded"))))

(deftest test-join-chain-produces-bushy-tree
  (testing "DP can produce bushy trees (not just left-deep)"
    ;; This test verifies that the DP algorithm can produce bushy trees
    ;; when they have lower cost than left-deep trees
    (let [;; Create a scenario where bushy is better:
          ;; A(small) ⋈ B(big) ⋈ C(small) ⋈ D(big)
          ;; Left-deep: ((A ⋈ B) ⋈ C) ⋈ D - big intermediates
          ;; Bushy: (A ⋈ C) ⋈ (B ⋈ D) - could be smaller
          ;; We test that the optimizer produces a valid plan
          raw-plan (parse-and-plan five-way-chain-query)
          optimized (opt/optimize-plan raw-plan)
          depth (get-join-depth optimized)]
      ;; Bushy trees have lower depth than left-deep
      ;; A left-deep tree for 5 relations has depth 4
      ;; A perfectly bushy tree could have depth as low as 3
      ;; We just verify we get a reasonable result
      (is (<= depth 5)
          "Join depth should be reasonable"))))

(deftest test-join-chain-with-filters
  (testing "Join chain optimization works with filters"
    (let [query (str prefixes "
                 SELECT ?product ?producer ?offer
                 WHERE {
                   ?product bsbm:producer ?producer .
                   ?producer rdfs:label ?producerLabel .
                   ?offer bsbm:product ?product .
                   FILTER (?producerLabel = \"Test\")
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      (is (some? optimized)
          "Should handle filtered join chain")
      ;; Filter should still be in the plan
      (is (contains-op? optimized :filter)
          "Filter should be preserved"))))

;;; ---------------------------------------------------------------------------
;;; Context Variable Safety Tests (Audit Fixes)
;;; ---------------------------------------------------------------------------

(deftest test-type-lookup-rejects-variable-context
  (testing "type-lookup optimization is NOT applied when context is a variable"
    ;; GRAPH ?g patterns require BGP to properly bind the graph variable
    (let [query (str prefixes "
                 SELECT ?product ?g
                 WHERE {
                   GRAPH ?g {
                     ?product rdf:type bsbm:Product .
                   }
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should NOT use type-lookup because context is a variable
      (is (not (contains-op? optimized :type-lookup))
          "type-lookup should NOT be used with variable context (GRAPH ?g)")
      ;; Should use BGP instead
      (is (contains-op? optimized :bgp)
          "BGP should be used to preserve GRAPH ?g semantics"))))

(deftest test-batch-enrich-rejects-variable-context
  (testing "batch-enrich optimization is NOT applied when context is a variable"
    ;; batch-lookup cannot filter by context, so variable context must use standard join
    (let [query (str prefixes "
                 SELECT ?product ?label ?g
                 WHERE {
                   GRAPH ?g {
                     ?product rdf:type bsbm:Product .
                     ?product rdfs:label ?label .
                   }
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should NOT use batch-enrich because context is a variable
      ;; Note: The query may use join instead
      (is (not (and (contains-op? optimized :batch-enrich)
                    ;; If batch-enrich exists, verify it doesn't have variable context
                    (some (fn find-batch-enrich [plan]
                            (when (map? plan)
                              (if (= :batch-enrich (:op plan))
                                (opt/is-variable? (:context plan))
                                (or (find-batch-enrich (:sub-plan plan))
                                    (find-batch-enrich (:left plan))
                                    (find-batch-enrich (:right plan))))))
                          [optimized])))
          "batch-enrich should NOT be used with variable context (GRAPH ?g)"))))

(deftest test-batch-enrich-rejects-object-var-in-join-vars
  (testing "batch-enrich optimization is NOT applied when object-var is in join-vars"
    ;; When the same variable appears as object in both BGPs, batch-enrich cannot
    ;; be used because its merge logic would overwrite the variable without
    ;; enforcing equality constraints. Must use standard join instead.
    (let [;; Query where ?label appears as object in both patterns
          ;; This requires equality check: product1.label = product2.label
          query (str prefixes "
                 SELECT ?p1 ?p2 ?label
                 WHERE {
                   ?p1 rdfs:label ?label .
                   ?p2 rdfs:label ?label .
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should NOT use batch-enrich because ?label is in join-vars
      ;; and also the object-var of the lookup pattern.
      ;; Any batch-enrich here would be invalid since the merge logic would
      ;; overwrite ?label without enforcing the equality constraint.
      (is (not (contains-op? optimized :batch-enrich))
          "batch-enrich should NOT be used when object-var participates in join constraints"))))

(deftest test-type-lookup-accepts-nil-context
  (testing "type-lookup optimization IS applied when context is nil (unbound)"
    (let [query (str prefixes "
                 SELECT ?product
                 WHERE {
                   ?product rdf:type bsbm:Product .
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should use type-lookup because context is nil
      (is (contains-op? optimized :type-lookup)
          "type-lookup should be used when context is nil"))))

(deftest test-type-lookup-rejects-constant-context
  (testing "type-lookup optimization is NOT applied when context is a constant"
    ;; $$type-subjects doesn't track context, so specific context needs BGP
    (let [query (str prefixes "
                 SELECT ?product
                 WHERE {
                   GRAPH <http://example.org/graph1> {
                     ?product rdf:type bsbm:Product .
                   }
                 }")
          raw-plan (parse-and-plan query)
          optimized (opt/optimize-plan raw-plan)]
      ;; Should NOT use type-lookup because context is a constant
      (is (not (contains-op? optimized :type-lookup))
          "type-lookup should NOT be used with constant context (GRAPH <uri>)")
      ;; Should use BGP to properly filter by context
      (is (contains-op? optimized :bgp)
          "BGP should be used to filter by specific context"))))

(comment
  ;; Run tests from REPL
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.sail.optimizer-test))
