(ns rama-sail.core
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [rama-sail.query.expr :refer [eval-expr parse-numeric evaluate-filter-cond]]
            [rama-sail.query.aggregation :as agg]
            [rama-sail.query.helpers :as qh]
            [rama-sail.sail.serialization :as ser]))

;; Re-export constants for backward compatibility with tests
(def ^{:doc "Default context value for the default graph"} DEFAULT-CONTEXT-VAL ser/DEFAULT-CONTEXT-VAL)
(def ^{:doc "RDF type predicate IRI"} RDF-TYPE-PREDICATE qh/RDF-TYPE-PREDICATE)

(defn depot-partition-key [[op payload _tx-time-or-opts]]
  ;; Partition key extraction for different operation formats:
  ;; - [:add [s p o c] tx-time] -> partition by s
  ;; - [:del [s p o c] tx-time] -> partition by s
  ;; - [:clear-context [nil nil nil c] tx-time] -> partition by c
  (case op
    :clear-context (nth payload 3)  ;; [:clear-context [nil nil nil c] tx-time] -> partition by c
    (first payload)))               ;; [:add/:del [s p o c] tx-time] -> partition by s

(defn namespace-depot-partition-key
  "Partition key for namespace depot operations.
   Hash by prefix for set/remove operations to ensure same prefix goes to same task.
   Returns nil for clear operations (handled specially in SAIL layer)."
  [[op & args]]
  (case op
    :set-ns (first args)      ;; hash by prefix
    :remove-ns (first args)   ;; hash by prefix
    :clear-ns nil))

;; DEFAULT-CONTEXT-VAL and RDF-TYPE-PREDICATE are now in rama-sail.query.helpers
;; and referred into this namespace via :refer

(defn dec-floor-zero
  "Decrement a value but never go below zero. Guards against negative statistics
   under edge cases like out-of-order delivery or double-delete."
  [n]
  (max 0 (dec (or n 0))))

;; --- Empty Container Cleanup ---
;; After deleting an element from a nested map structure, empty containers
;; should be removed to prevent memory growth over time.

(defn empty->none
  "Transform function for Rama's `term` navigator.
   Returns NONE (causing key removal) if collection is empty, otherwise returns unchanged.
   Used to clean up empty containers after element deletion.
   Note: Works with Java collections and Clojure persistent collections."
  [coll]
  (cond
    (nil? coll) com.rpl.rama.path/NONE
    (instance? java.util.Collection coll) (if (.isEmpty ^java.util.Collection coll)
                                            com.rpl.rama.path/NONE
                                            coll)
    (instance? java.util.Map coll) (if (.isEmpty ^java.util.Map coll)
                                     com.rpl.rama.path/NONE
                                     coll)
    (coll? coll) (if (empty? coll)
                   com.rpl.rama.path/NONE
                   coll)
    :else coll))

;; Helper: Generic traversal for indices structured as Level1 -> Level2 -> Level3 -> Set<Level4>
;; Used for $$spoc, $$posc, $$ospc.
(deframaop scan-3-levels-index [*k1 *k2 *k3 *k4 *pstate]
		;; 2. Conditional Logic: Traverse based on which keys are known (non-nil)
  (<<cond
		;; Case A: All keys known (Exact Quad check)
   (case> (and> *k2 *k3 *k4))
   (local-select> [(keypath *k1 *k2 *k3 *k4)] *pstate :> *exists?)
		;; Emit only if exists
   (filter> *exists?)
   (:> *k1 *k2 *k3 *k4)

		;; Case B: k4 is wildcard (Find all C for specific S,P,O)
   (case> (and> *k2 *k3 (nil? *k4)))
   (local-select> [(keypath *k1 *k2 *k3) ALL] *pstate :> *out4)
   (:> *k1 *k2 *k3 *out4)

		;; Case C: k3, k4 are wildcards (Find all O,C for specific S,P)
   (case> (and> *k2 (nil? *k3) (nil? *k4)))
   (local-select>
    [*k1 *k2 ALL (collect-one FIRST) LAST ALL] *pstate
    :> [*out3 *out4])
   (:> *k1 *k2 *out3 *out4)

		;; Case C1: k2 is wildcard, k4 known, k3 known (Find all P for specific S,O,C)
   (case> (and> (nil? *k2) *k3 *k4))
   (local-select>
    [(keypath *k1) ALL
     (selected? LAST *k3 ALL (pred= *k4)) FIRST] *pstate
    :> *out2)
   (:> *k1 *out2 *k3 *k4)

		;; Case C2: k3 is wildcard, k2 known, k4 known (Find all O for specific S,P,C)
   (case> (and> *k2 (nil? *k3) *k4))
   (local-select>
    [(keypath *k1 *k2) ALL
     (selected? LAST ALL (pred= *k4)) FIRST] *pstate
    :> *out3)
   (:> *k1 *k2 *out3 *k4)

		; Case C3: k2 and k4 are wildcards, k3 known (Find all P for specific S,O)
   (case> (and> (nil? *k2) *k3 (nil? *k4)))
   (local-select>
    [(keypath *k1) ALL (collect-one FIRST) LAST (keypath *k3) ALL] *pstate
    :> [*out2 *out4])
   (:> *k1 *out2 *k3 *out4)

		;; Case C4: k2 and k3 are wildcards, k4 known (Find all P,O for specific S,C)
   (case> (and> (nil? *k2) (nil? *k3) *k4))
   (local-select>
    [(keypath *k1) ALL
     (collect-one FIRST) LAST ALL (selected? LAST ALL #{*k4}) FIRST] *pstate
    :> [*out2 *out3])
   (:> *k1 *out2 *out3 *k4)

		;; Case D: k2, k3, k4 wildcards (Find all P,O,C for specific S)
   (default>)
   (local-select>
    [(keypath *k1) ALL
     (collect-one FIRST) LAST ALL (collect-one FIRST) LAST ALL] *pstate
    :> [*out2 *out3 *out4])
   (:> *k1 *out2 *out3 *out4)))

(defn find-triples-query-topology [topologies]
  (<<query-topology topologies "find-triples"
                    [*s *p *o *c :> *quads]
                    (<<cond
                     (case> (some? *s))
                     (|hash *s)
                     (scan-3-levels-index *s *p *o *c $$spoc :> *f-s *f-p *f-o *f-c)

                     (case> (some? *p))
                     (|hash *p)
                     (scan-3-levels-index *p *o *s *c $$posc :> *f-p *f-o *f-s *f-c)

                     (case> (some? *o))
                     (|hash *o)
                     (scan-3-levels-index *o *s *p *c $$ospc :> *f-o *f-s *f-p *f-c)

                     (case> (some? *c))
                     (|hash *c)
                     (scan-3-levels-index *c *s *p *o $$cspo :> *f-c *f-s *f-p *f-o)

                     (default>)
                     (|all)
                     (local-select> [ALL
                                     (collect-one FIRST)
                                     LAST ALL
                                     (collect-one FIRST)
                                     LAST ALL
                                     (collect-one FIRST)
                                     LAST ALL] $$spoc :> [*f-s *f-p *f-o *f-c]))
                    (vector *f-s *f-p *f-o *f-c :> *quad)
                    (|origin)
                    (aggs/+vec-agg *quad :> *quads)))

(defn find-bgp-query-topology [topologies]
  (<<query-topology topologies "find-bgp" [*pattern :> *bindings]
		;; We use Keywords as functions to extract map values
                    (identity *pattern :> {:keys [*s *p *o *c]})
		;; Runtime check for wildcards (strings starting with "?")
                    (<<if (str/starts-with? *s "?") (identity nil :> *s-const) (else>) (identity *s :> *s-const))
                    (<<if (str/starts-with? *p "?") (identity nil :> *p-const) (else>) (identity *p :> *p-const))
                    (<<if (str/starts-with? *o "?") (identity nil :> *o-const) (else>) (identity *o :> *o-const))
		;; Context logic: If not provided or wildcard variable, use nil (match all)
		;; If provided as constant, use it.
                    (<<if (or> (nil? *c) (str/starts-with? *c "?"))
                          (identity nil :> *c-const) (else>) (identity *c :> *c-const))
		;; Execute find
                    (invoke-query "find-triples" *s-const *p-const *o-const *c-const :> *quads)
                    (ops/explode *quads :> [*f-s *f-p *f-o *f-c])
		;; Bind results to variables if the pattern asked for a variable
                    (identity {} :> *b-init)
                    (<<if (nil? *s-const) (assoc *b-init *s *f-s :> *b-1) (else>) (identity *b-init :> *b-1))
                    (<<if (nil? *p-const) (assoc *b-1 *p *f-p :> *b-2) (else>) (identity *b-1 :> *b-2))
                    (<<if (nil? *o-const) (assoc *b-2 *o *f-o :> *b-3) (else>) (identity *b-2 :> *b-3))
		;; Bind context variable if requested (and it wasn't a constant)
                    (<<if (and> (some? *c) (str/starts-with? *c "?"))
                          (assoc *b-3 *c *f-c :> *b-final) (else>) (identity *b-3 :> *b-final))
                    (|origin)
                    (aggs/+set-agg *b-final :> *bindings)))

;;; ---------------------------------------------------------------------------
;;; Hash Join Implementation
;;; ---------------------------------------------------------------------------
;;;
;;; Implements hash join with O(n + m) complexity:
;;; 1. BUILD phase: Execute one side, build hash index keyed by join variables
;;; 2. PROBE phase: Execute other side, lookup in hash index, emit merged rows
;;;
;;; This replaces the previous gen>-based approach which had O(n × m) behavior.

;; Hash join helpers moved to rama-sail.query.helpers

;; --- Hash Join (Inner Join) ---
;; Build hash index from right side, probe with left side.
;; Complexity: O(n + m) where n = left size, m = right size

(defn join-query-topology [topologies]
  (<<query-topology topologies "join"
                    [*left-plan *right-plan *join-vars *result-limit :> *results]

                    ;; BUILD PHASE: Execute right plan, build hash index
                    (invoke-query "execute-plan" *right-plan :> *right-results)
                    (qh/build-hash-index *right-results *join-vars :> *hash-idx)

                    ;; PROBE PHASE: Execute left plan, probe hash index
                    (invoke-query "execute-plan" *left-plan :> *left-results)
                    ;; When result-limit is set, truncate left side to limit probes
                    ;; This is safe because each left row produces at least one join result
                    ;; so limiting probes limits results (conservative upper bound)
                    (<<if (some? *result-limit)
                          (vec *left-results :> *left-vec)
                          (identity (take *result-limit *left-vec) :> *left-limited)
                          (else>)
                          (identity *left-results :> *left-limited))
                    (ops/explode *left-limited :> *left-bind)
                    (qh/probe-hash-index *hash-idx *left-bind *join-vars :> *matches)
                    (filter> (some? *matches))
                    (ops/explode *matches :> *joined)

                    (|origin)
                    (aggs/+set-agg *joined :> *results)))

;;; ---------------------------------------------------------------------------
;;; Self-Join Optimization
;;; ---------------------------------------------------------------------------
;;;
;;; For self-joins on the same predicate with inequality filters (QJ4 pattern),
;;; we avoid the N×K Cartesian product by:
;;; 1. Execute single BGP to get all (subject, object) pairs
;;; 2. Group subjects by object (the join key)
;;; 3. Generate pairs within each group, applying inequality during generation

;; Self-join helpers moved to rama-sail.query.helpers

(defn self-join-query-topology [topologies]
  (<<query-topology topologies "self-join"
                    [*predicate *join-var *left-subject *right-subject *filter *context *result-limit :> *results]

                    ;; Execute single BGP to get all (subject, object) pairs for this predicate
                    (identity {:s "?s" :p *predicate :o "?o" :c *context} :> *pattern)
                    (invoke-query "find-bgp" *pattern :> *bgp-results)

                    ;; Group subjects by their object value (the join key)
                    (qh/build-subject-groups *bgp-results :> *groups)

                    ;; Generate pairs within each group, applying filter during generation
                    ;; result-limit enables early termination when enough pairs are found
                    (qh/process-subject-groups *groups *left-subject *right-subject *join-var *filter *result-limit :> *results)

                    (|origin)))

;; Combiner for union - merges two Sets for distributed aggregation
;; Defined here early so it can be used by colocated-subject-join-query-topology
(def +set-union
  (combiner
   (fn [set1 set2] (into (or set1 #{}) set2))
   :init-fn (fn [] #{})))

;; --- Batch Lookup ---
;; Fetch properties for multiple subjects in one query.
;; Used after self-join to avoid N individual label lookups.

(defn batch-lookup-query-topology
  "Query topology to fetch predicate values for multiple subjects.
   Returns vector of [subject object] pairs for ALL objects (handles multi-valued predicates).
   Routes each subject to its partition (since $$spoc is hash-partitioned by subject)."
  [topologies]
  (<<query-topology topologies "batch-lookup"
                    [*subjects *predicate *context :> *results]

                    ;; Explode subjects set and route each to its partition
                    (ops/explode *subjects :> *subj)
                    (|hash *subj)
                    (local-select> [(keypath *subj *predicate) ALL (collect-one FIRST) LAST ALL] $$spoc :> [*obj *ctx])
                    (filter> (some? *obj))
                    ;; Filter by context if specified
                    (<<if (some? *context)
                          (filter> (= *ctx *context))
                          (else>)
                          (identity true :> *_pass))
                    ;; Collect back at origin
                    (|origin)
                    (vector *subj *obj :> *pair)
                    (aggs/+vec-agg *pair :> *results)))

;; Batch enrich helpers moved to rama-sail.query.helpers

;; --- Co-located Subject Join ---
;; Optimized join for two BGP patterns that share a subject variable.
;; Exploits the fact that $$spoc is hash-partitioned by subject.
;; Each task locally joins its subjects without network shuffling.

;; Colocated join helpers moved to rama-sail.query.helpers

(defn colocated-subject-join-query-topology [topologies]
  (<<query-topology topologies "colocated-subject-join"
                    [*left-pattern *right-pattern *subject-var :> *results]
    ;; This topology exploits subject partitioning:
    ;; 1. Broadcast to all tasks (each owns a subset of subjects)
    ;; 2. Each task iterates through its local subjects
    ;; 3. For each subject, match both patterns and join locally
    ;; 4. Aggregate results at origin
                    (|all)
    ;; Iterate through each subject on this task
    ;; local-select> with [ALL] on map emits MapEntry objects
                    (local-select> [ALL] $$spoc :> *entry)
    ;; Extract key (subject) and value (pred-map) from entry
                    (key *entry :> *subj)
                    (val *entry :> *pred-map)
    ;; Join patterns for this subject
                    (qh/join-subject-locally *left-pattern *right-pattern *subj *pred-map :> *subj-results)
    ;; Explode and aggregate
                    (ops/explode *subj-results :> *result)
                    (|origin)
                    (aggs/+set-agg *result :> *results)))

;; --- Left Hash Join (Left Outer Join for OPTIONAL) ---
;; Build hash index from right side, probe with left side.
;; For non-matches, returns left binding unchanged (OPTIONAL semantics).
;; Supports FILTER condition on the OPTIONAL clause.

;; Left join helpers (check-optional-condition, apply-left-join-with-condition) moved to rama-sail.query.helpers

(defn left-join-query-topology [topologies]
  (<<query-topology topologies "left-join"
                    [*left-plan *right-plan *join-vars *condition :> *results]

                    ;; BUILD PHASE: Execute right plan, build hash index
                    (invoke-query "execute-plan" *right-plan :> *right-results)
                    (qh/build-hash-index *right-results *join-vars :> *hash-idx)

                    ;; PROBE PHASE: Execute left plan, probe with left-join semantics
                    (invoke-query "execute-plan" *left-plan :> *left-results)
                    (ops/explode *left-results :> *left-bind)
                    (qh/apply-left-join-with-condition *hash-idx *left-bind *join-vars *condition :> *join-results)
                    (ops/explode *join-results :> *joined)

                    (|origin)
                    (aggs/+set-agg *joined :> *results)))

;; apply-multi-left-join moved to rama-sail.query.helpers

(defn multi-left-join-query-topology [topologies]
  (<<query-topology topologies "multi-left-join"
                    [*base-plan *optionals :> *results]
                    ;; Execute base plan ONCE
                    (invoke-query "execute-plan" *base-plan :> *base-results)
                    ;; Execute each optional plan and build hash indices
                    (ops/explode *optionals :> *opt)
                    (get *opt :plan :> *opt-plan)
                    (get *opt :join-vars :> *opt-jv)
                    (get *opt :condition nil :> *opt-cond)
                    (invoke-query "execute-plan" *opt-plan :> *opt-results)
                    (qh/build-hash-index *opt-results *opt-jv :> *opt-idx)
                    (hash-map :hash-idx *opt-idx :join-vars *opt-jv :condition *opt-cond :> *opt-entry)
                    (|origin)
                    ;; Aggregate optional indices and preserve base results
                    (aggs/+vec-agg *opt-entry :> *all-indices)
                    (aggs/+last *base-results :> *base-agg)
                    ;; Apply all optionals to base results in single pass
                    (qh/apply-multi-left-join *base-agg *all-indices :> *results)))

(defn union-query-topology [topologies]
  (<<query-topology topologies "union"
                    [*left-plan *right-plan :> *results]
                    ;; Combine plans into vector, explode for parallel execution
                    (vector *left-plan *right-plan :> *plans)
                    (ops/explode *plans :> *plan)
                    (|shuffle) ;; Distribute across tasks
                    (invoke-query "execute-plan" *plan :> *res)
                    (|origin)
                    ;; Use +set-union combiner for distributed aggregation
                    (+set-union *res :> *results)))

;; Combiner for ordered results - preserves insertion order
(def +vec-concat-combiner
  (combiner
   (fn [v1 v2] (into (or v1 []) v2))
   :init-fn (fn [] [])))

(defn project-query-topology [topologies]
  ;; Always preserves order using vector aggregation.
  ;; This is necessary to support ORDER BY results flowing through PROJECT.
  ;; The SAIL layer iterates over results regardless of collection type.
  (<<query-topology topologies "project" [*sub-plan *proj-vars :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *all-res)
                    (ops/explode *all-res :> *binding)
                    (select-keys *binding *proj-vars :> *final-binding)
                    ;; Aggregate as vector to preserve order for ORDER BY
                    (vector *final-binding :> *wrapped)
                    (|origin)
                    (+vec-concat-combiner *wrapped :> *results)))

(defn distinct-query-topology [topologies]
  ;; Implements SPARQL DISTINCT - removes duplicate bindings.
  ;; Uses set aggregation which naturally eliminates duplicates.
  (<<query-topology topologies "distinct" [*sub-plan :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
                    (|origin)
                    (aggs/+set-agg *binding :> *results)))

(defn slice-query-topology [topologies]
  ;; Implements SPARQL SLICE (OFFSET/LIMIT) per W3C SPARQL 1.1 and RDF4J semantics:
  ;; - offset < 0 means "not set" (treat as 0)
  ;; - limit < 0 means "not set" (no limit, return all from offset)
  ;; - offset >= count returns empty vector
  ;; - Always preserves order using vector aggregation (for ORDER BY support)
  ;;
  ;; Optimization: Uses select> with srange navigator which is ~4x faster than
  ;; explode-indexed + filter for large result sets.
  (<<query-topology topologies "slice" [*sub-plan *offset *limit :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *all-res)
                    ;; Convert to vector for indexed slicing
                    (vec *all-res :> *all-vec)
                    (count *all-vec :> *total)
                    ;; Safe offset: treat negative as 0 (per RDF4J hasOffset semantics)
                    (max 0 *offset :> *safe-offset)
                    ;; Compute upper bound based on limit (-1 means no limit)
                    (<<if (< *limit 0)
                          (identity *total :> *upper)
                          (else>)
                          (min (+ *safe-offset *limit) *total :> *upper))
                    ;; Use srange navigator for efficient slicing (4x faster than explode + filter)
                    (<<if (>= *safe-offset *total)
                          ;; Offset exceeds results - return empty
                          (identity [] :> *results)
                          (else>)
                          ;; srange directly navigates to slice without iterating all elements
                          (select> [(srange *safe-offset *upper)] *all-vec :> *results))
                    (|origin)))

(defn ask-result-query-topology [topologies]
  (<<query-topology topologies "ask-result" [*sub-plan :> *results]
		;; 1. Wrap sub-plan with LIMIT 1 to short-circuit evaluation
                    (hash-map :op :slice :sub-plan *sub-plan :offset 0 :limit 1 :> *limited-plan)
                    (invoke-query "execute-plan" *limited-plan :> *sub-results)
		;; 2. Check if the result set is non-empty
                    (select> [(view count)] *sub-results :> *count)
                    (<<if (> *count 0)
			;; ASK is TRUE: return a set containing one empty map.
                          (identity #{{}} :> *results)
                          (else>)
			;; ASK is FALSE: return an empty set.
                          (identity #{} :> *results))
                    (|origin)))

;; --- 9. BIND Operator (SPARQL BIND clause) ---
(defn apply-bindings
  "Apply a sequence of binding expressions to a row, returning extended row."
  [row bindings]
  (reduce (fn [r {:keys [var expr]}]
            (let [val (eval-expr expr r)]
              (if val
                (assoc r var val)
                r)))
          row
          bindings))

(defn bind-query-topology [topologies]
  (<<query-topology topologies "bind" [*sub-plan *bindings :> *results]
    ;; 1. Execute sub-plan
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)

    ;; 2. For each row, apply all bindings
                    (ops/explode *sub-results :> *row)
                    (apply-bindings *row *bindings :> *extended-row)

                    (|origin)
                    (aggs/+set-agg *extended-row :> *results)))

;; --- 11. ORDER BY Operator (SPARQL ORDER BY clause) ---

;; ORDER BY helpers (DescString, compute-sort-key, sort-keyed-rows, extract-sorted-rows)
;; moved to rama-sail.query.helpers

(defn order-query-topology [topologies]
  (<<query-topology topologies "order" [*sub-plan *order-specs :> *results]
    ;; 1. Execute sub-plan to get unsorted results
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)

    ;; 2. Explode results into stream of rows
                    (ops/explode *sub-results :> *row)

    ;; 3. Compute sort key for each row
                    (qh/compute-sort-key *row *order-specs :> *sort-key)

    ;; 4. Wrap as single-element vector of [key, row] tuple
                    (vector (vector *sort-key *row) :> *single-keyed)

    ;; 5. Aggregate to origin using concat combiner (two-phase aggregation)
                    (|origin)
                    (+vec-concat-combiner *single-keyed :> *all-keyed)

    ;; 6. Sort by keys and extract rows
                    (qh/sort-keyed-rows *all-keyed :> *sorted-keyed)
                    (qh/extract-sorted-rows *sorted-keyed :> *results)))

;; --- 12. LIST CONTEXTS (for clearInternal support) ---
(defn list-contexts-query-topology [topologies]
  (<<query-topology topologies "list-contexts" [:> *contexts]
                    (|all)
                    (local-select> [ALL (collect-one FIRST)
                                    LAST ALL (collect-one FIRST)
                                    LAST ALL (collect-one FIRST)
                                    LAST ALL] $$cspo :> [*ctx *s *p *o])
                    (|origin)
                    (aggs/+set-agg *ctx :> *contexts)))

;; --- 12b. COUNT STATEMENTS (for sizeInternal support) ---
(defn count-statements-query-topology [topologies]
  (<<query-topology topologies "count-statements" [*context :> *count]
                    (|all)
                    (<<if (nil? *context)
                          (local-select> [ALL (collect-one FIRST)
                                          LAST ALL (collect-one FIRST)
                                          LAST ALL (collect-one FIRST)
                                          LAST ALL] $$spoc :> [*s *p *o *c])
                          (else>)
                          (local-select> [(keypath *context) ALL (collect-one FIRST)
                                          LAST ALL (collect-one FIRST)
                                          LAST ALL] $$cspo :> [*s *p *o])
                          (identity *context :> *c))
                    (|origin)
                    (aggs/+count :> *count)))

;; --- Statistics Query Topologies ---
;; Used for cardinality estimation and adaptive query optimization

(defn get-predicate-stats-query-topology [topologies]
  (<<query-topology topologies "get-predicate-stats" [*predicate :> *stats]
    ;; Hash to the partition owning this predicate's statistics
                    (|hash *predicate)
                    (local-select> [(keypath *predicate)] $$predicate-stats :> *stats)
                    (|origin)))

(defn get-all-predicate-stats-query-topology [topologies]
  (<<query-topology topologies "get-all-predicate-stats" [:> *all-stats]
    ;; Broadcast to all tasks to gather all predicate statistics
                    (|all)
                    (local-select> [ALL] $$predicate-stats :> *entry)
                    (|origin)
    ;; Aggregate as map: predicate -> stats
                    (aggs/+map-agg (first *entry) (second *entry) :> *all-stats)))

(defn get-global-stats-query-topology [topologies]
  (<<query-topology topologies "get-global-stats" [:> *stats]
    ;; Global stats are stored under "" key
                    (|hash "")
                    (local-select> [(keypath "")] $$global-stats :> *stats)
                    (|origin)))

;; --- Materialized View Query Topologies ---

(defn get-subjects-by-type-query-topology [topologies]
  (<<query-topology topologies "get-subjects-by-type" [*type :> *subjects]
    ;; Get all subjects that have a specific rdf:type
    ;; Returns a set of subject IRIs
                    (|hash *type)
                    (local-select> [(keypath *type) (view vec)] $$type-subjects :> *data)
                    (<<if (nil? *data)
                          (identity [] :> *subjects)
                          (else>)
                          (identity *data :> *subjects))
                    (|origin)))

(defn get-types-of-subject-query-topology [topologies]
  (<<query-topology topologies "get-types-of-subject" [*subject :> *types]
    ;; Get all rdf:type values for a specific subject
    ;; Returns a set of type IRIs
                    (|hash *subject)
                    (local-select> [(keypath *subject) (view vec)] $$subject-types :> *data)
                    (<<if (nil? *data)
                          (identity [] :> *types)
                          (else>)
                          (identity *data :> *types))
                    (|origin)))

;; --- 13. GROUP BY / Aggregates ---

(defn group-query-topology [topologies]
  (<<query-topology topologies "group"
                    [*sub-plan *group-vars *aggregates :> *results]
    ;; 1. Execute sub-plan to get input bindings
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
    ;; 2. Explode bindings into stream for distributed processing
                    (ops/explode *sub-results :> *binding)
    ;; 3. Build group entry with initial aggregate states per binding
                    (agg/build-group-entry *binding *group-vars *aggregates :> *group-entry)
    ;; 4. Shuffle to distribute work (optional optimization)
                    (|origin)
    ;; 5. Combine all group entries using two-phase aggregation
                    (agg/+group-agg-combiner *group-entry :> *combined-groups)
    ;; 6. Finalize: compute final values and format results
                    (agg/finalize-group-results *combined-groups *group-vars *aggregates :> *results)))

(defn filter-query-topology-parallel [topologies]
  (<<query-topology topologies "filter" [*sub-plan *expr :> *results]
		;; 1. Execute sub-plan (Result is likely a list of maps)
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
		;; 2. Explode the results into a stream
                    (ops/explode *sub-results :> *binding)
		;; 3. PARALLELIZE: Shuffle the stream across all tasks.
		;; This allows `evaluate-filter-cond` to run in parallel on all cores.
                    (|shuffle)
		;; 4. Filter distributedly
                    (filter> (evaluate-filter-cond *expr *binding))
		;; 5. Wrap the single binding into a Set.
		;; The combiner expects to merge two values of the same type (Set + Set).
                    (hash-set *binding :> *binding-set)
                    (|origin)
		;; 6. Aggregate using the Combiner.
		;; Because we are in a batch context (Query Topology) and using a Combiner,
		;; Rama performs Two-Phase Aggregation:
		;;   Phase 1: Each task unions its local *binding-sets into a partial result.
		;;   Phase 2: Partial results are sent to the origin task and unioned there.
                    (+set-union *binding-set :> *results)))

(defmodule RdfStorageModule [setup topologies]
  (declare-depot setup *triple-depot (hash-by depot-partition-key))
  ;; Namespace operations depot - hash by prefix for set/remove, nil for clear (handled specially)
  (declare-depot setup *namespace-depot (hash-by namespace-depot-partition-key))

  (let [mb (microbatch-topology topologies "indexer")]
		;; Quads indices:
		;; $$spoc: S -> P -> O -> Set<C>
    (declare-pstate mb $$spoc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
		;; $$posc: P -> O -> S -> Set<C>
    (declare-pstate mb $$posc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
		;; $$ospc: O -> S -> P -> Set<C>
    (declare-pstate mb $$ospc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
		;; $$cspo: C -> S -> P -> Set<O> (Useful for "Graph <g> { ... }" queries)
    (declare-pstate mb $$cspo {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})

    ;; --- Statistics PStates for Query Optimization ---
    ;; These enable cardinality-based join ordering and adaptive query planning

    ;; $$predicate-stats: Predicate -> {:count Long, :distinct-subjects Long, :distinct-objects Long}
    ;; Tracks per-predicate statistics for selectivity estimation
    (declare-pstate mb $$predicate-stats {String (fixed-keys-schema
                                                  {:count Long
                                                   :distinct-subjects Long
                                                   :distinct-objects Long})})

    ;; $$global-stats: Single key "" -> {:total-triples Long, :total-predicates Long, :total-subjects Long}
    ;; Global statistics for overall cardinality estimation
    (declare-pstate mb $$global-stats {String (fixed-keys-schema
                                               {:total-triples Long
                                                :total-predicates Long
                                                :total-subjects Long})})

    ;; $$pred-subj-count: pred -> subject -> count of triples
    ;; Used to maintain accurate distinct-subjects in $$predicate-stats
    (declare-pstate mb $$pred-subj-count {String (map-schema String Long {:subindex? true})})

    ;; $$pred-obj-count: pred -> object -> count of triples
    ;; Used to maintain accurate distinct-objects in $$predicate-stats
    (declare-pstate mb $$pred-obj-count {String (map-schema String Long {:subindex? true})})

    ;; --- Materialized Views for Common Query Patterns ---
    ;; These pre-compute common join patterns for faster query execution

    ;; $$type-subjects: Type IRI -> Set of subjects with that type
    ;; Enables fast lookup of all entities of a given type
    ;; Used for queries like: ?x rdf:type <Type> . ?x ?prop ?val
    (declare-pstate mb $$type-subjects {String (set-schema String {:subindex? true})})

    ;; $$subject-types: Subject -> Set of type IRIs
    ;; Inverse index for looking up types of a specific subject
    (declare-pstate mb $$subject-types {String (set-schema String {:subindex? true})})

    ;; --- Context-Aware Type View Cardinality Tracking ---
    ;; These track how many contexts have each (type, subject) / (subject, type) pair
    ;; to ensure deleting a type in one context doesn't remove it from type views
    ;; when the same s-p-o exists in other contexts.

    ;; $$type-subject-count: type -> subject -> count of contexts with this (type, subject)
    ;; Used to maintain $$type-subjects correctly across multi-context scenarios
    (declare-pstate mb $$type-subject-count {String (map-schema String Long {:subindex? true})})

    ;; $$subject-type-count: subject -> type -> count of contexts with this (subject, type)
    ;; Used to maintain $$subject-types correctly across multi-context scenarios
    (declare-pstate mb $$subject-type-count {String (map-schema String Long {:subindex? true})})

    ;; --- Temporal Tracking PState ---
    ;; $$quad-tx-time: s -> p -> o -> c -> tx-time (epoch millis when quad was created)
    ;; Tracks when each quad was added to the store (used for idempotent add detection)
    (declare-pstate mb $$quad-tx-time {String (map-schema String (map-schema String (map-schema String Long {:subindex? true}) {:subindex? true}) {:subindex? true})})

    (<<sources mb
               (source> *triple-depot :> %microbatch)
               ;; Format: [op [s p o c] tx-time]
               (%microbatch :> *msg)
               (first *msg :> *op)

               (<<cond
                ;; --- ADD/DEL/CLEAR-CONTEXT: Quad operations ---
                (default>)
                ;; Format: [op [s p o c] tx-time]
                (identity (second *msg) :> [*s *p *o *c])
                (identity (nth *msg 2 nil) :> *raw-tx-time)
                (<<if (nil? *raw-tx-time)
                      (identity (System/currentTimeMillis) :> *tx-time)
                      (else>)
                      (identity *raw-tx-time :> *tx-time))

                (<<cond
                 (case> (= *op :add))
                 ;; Check if this quad already exists (for idempotent stats tracking)
                 (|hash *s)
                 (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *existing-tx-time)
                 ;; Quad is becoming visible iff it doesn't already exist
                 (identity (nil? *existing-tx-time) :> *becoming-visible)

                 ;; Update indices (idempotent - set semantics)
                 (|hash *s) (local-transform> [(keypath *s *p *o) NIL->SET NONE-ELEM (termval *c)] $$spoc)
                 (|hash *p) (local-transform> [(keypath *p *o *s) NIL->SET NONE-ELEM (termval *c)] $$posc)
                 (|hash *o) (local-transform> [(keypath *o *s *p) NIL->SET NONE-ELEM (termval *c)] $$ospc)
                 (|hash *c) (local-transform> [(keypath *c *s *p) NIL->SET NONE-ELEM (termval *o)] $$cspo)

                 ;; Store transaction time for this quad
                 (|hash *s)
                 (<<if (nil? *existing-tx-time)
                       (local-transform> [(keypath *s *p *o *c) (termval *tx-time)] $$quad-tx-time))

                 ;; Only update statistics if quad is BECOMING VISIBLE (not already visible)
                 ;; This ensures idempotent adds don't inflate counts
                 (<<if *becoming-visible
                       ;; Update statistics with accurate distinct tracking
                       (|hash *p)
                       (local-transform> [(keypath *p :count) (nil->val 0) (term inc)] $$predicate-stats)

                       ;; Track distinct subjects: increment only when first triple for (pred, subject)
                       (local-select> [(keypath *p *s)] $$pred-subj-count :> *prev-subj-count)
                       (local-transform> [(keypath *p *s) (nil->val 0) (term inc)] $$pred-subj-count)
                       (<<if (nil? *prev-subj-count)
                             (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term inc)] $$predicate-stats))

                       ;; Track distinct objects: increment only when first triple for (pred, object)
                       (local-select> [(keypath *p *o)] $$pred-obj-count :> *prev-obj-count)
                       (local-transform> [(keypath *p *o) (nil->val 0) (term inc)] $$pred-obj-count)
                       (<<if (nil? *prev-obj-count)
                             (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term inc)] $$predicate-stats))

                       ;; Update global statistics (partition by empty string for single location)
                       (|hash "")
                       (local-transform> [(keypath "" :total-triples) (nil->val 0) (term inc)] $$global-stats)

                       ;; --- Materialized View Maintenance (add) ---
                       ;; If this is an rdf:type triple, update type views with context-aware cardinality.
                       (<<if (= *p RDF-TYPE-PREDICATE)
                             ;; *o is the type, *s is the subject
                             ;; Track type->subject occurrence count
                             (|hash *o)
                             (local-select> [(keypath *o *s)] $$type-subject-count :> *prev-ts-count)
                             (local-transform> [(keypath *o *s) (nil->val 0) (term inc)] $$type-subject-count)
                             ;; Only add to type view if this is the FIRST occurrence
                             (<<if (nil? *prev-ts-count)
                                   (local-transform> [(keypath *o) NIL->SET NONE-ELEM (termval *s)] $$type-subjects))
                             ;; Track subject->type occurrence count (symmetric)
                             (|hash *s)
                             (local-select> [(keypath *s *o)] $$subject-type-count :> *prev-st-count)
                             (local-transform> [(keypath *s *o) (nil->val 0) (term inc)] $$subject-type-count)
                             ;; Only add to subject-types view if this is the FIRST occurrence
                             (<<if (nil? *prev-st-count)
                                   (local-transform> [(keypath *s) NIL->SET NONE-ELEM (termval *o)] $$subject-types))))

                 (case> (= *op :del))
                 ;; Delete a quad: physically remove from all indices

                 (|hash *s)
                 ;; Check if quad ever existed
                 (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *quad-existed)

                 (<<if (some? *quad-existed)
                       ;; Remove from all 4 indices
                       (|hash *s) (local-transform> [(keypath *s *p *o) (set-elem *c) NONE>] $$spoc)
                       (|hash *p) (local-transform> [(keypath *p *o *s) (set-elem *c) NONE>] $$posc)
                       (|hash *o) (local-transform> [(keypath *o *s *p) (set-elem *c) NONE>] $$ospc)
                       (|hash *c) (local-transform> [(keypath *c *s *p) (set-elem *o) NONE>] $$cspo)
                       ;; Remove quad-tx-time entry
                       (|hash *s) (local-transform> [(keypath *s *p *o *c) NONE>] $$quad-tx-time))

                 ;; Only update statistics if quad actually existed
                 (<<if (some? *quad-existed)

                       ;; Update statistics - decrement counts for deleted triple
                       ;; Update statistics - decrement counts for deleted triple
                       (|hash *p)
                       (local-transform> [(keypath *p :count) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)

                       ;; Decrement distinct subjects only when last triple for (pred, subject) removed
                       (local-select> [(keypath *p *s)] $$pred-subj-count :> *cur-subj-count)
                       (local-transform> [(keypath *p *s) (nil->val 0) (term dec-floor-zero)] $$pred-subj-count)
                       (<<if (= *cur-subj-count 1)
                             ;; Last occurrence - decrement distinct count AND cleanup the mapping
                             (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                             (local-transform> [(keypath *p *s) NONE>] $$pred-subj-count))

                       ;; Decrement distinct objects only when last triple for (pred, object) removed
                       (local-select> [(keypath *p *o)] $$pred-obj-count :> *cur-obj-count)
                       (local-transform> [(keypath *p *o) (nil->val 0) (term dec-floor-zero)] $$pred-obj-count)
                       (<<if (= *cur-obj-count 1)
                             ;; Last occurrence - decrement distinct count AND cleanup the mapping
                             (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                             (local-transform> [(keypath *p *o) NONE>] $$pred-obj-count))

                       ;; Update global statistics
                       (|hash "")
                       (local-transform> [(keypath "" :total-triples) (nil->val 0) (term dec-floor-zero)] $$global-stats)

                       ;; --- Materialized View Maintenance (delete) ---
                       ;; If this is an rdf:type triple, update type views with context-aware cardinality.
                       ;; Only remove from view when this is the LAST context having this (type, subject) pair.
                       (<<if (= *p RDF-TYPE-PREDICATE)
                             ;; *o is the type, *s is the subject
                             ;; Decrement type->subject occurrence count
                             (|hash *o)
                             (local-select> [(keypath *o *s)] $$type-subject-count :> *cur-ts-count)
                             (local-transform> [(keypath *o *s) (nil->val 0) (term dec-floor-zero)] $$type-subject-count)
                             ;; Only remove from type view if this was the LAST occurrence
                             (<<if (= *cur-ts-count 1)
                                   (local-transform> [(keypath *o) (set-elem *s) NONE>] $$type-subjects)
                                   (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$type-subjects)
                                   (local-transform> [(keypath *o *s) NONE>] $$type-subject-count))
                             ;; Decrement subject->type occurrence count (symmetric)
                             (|hash *s)
                             (local-select> [(keypath *s *o)] $$subject-type-count :> *cur-st-count)
                             (local-transform> [(keypath *s *o) (nil->val 0) (term dec-floor-zero)] $$subject-type-count)
                             ;; Only remove from subject-types view if this was the LAST occurrence
                             (<<if (= *cur-st-count 1)
                                   (local-transform> [(keypath *s) (set-elem *o) NONE>] $$subject-types)
                                   (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$subject-types)
                                   (local-transform> [(keypath *s *o) NONE>] $$subject-type-count))))

                 (case> (= *op :clear-context))
                 ;; Clear all quads in a context. Mode-dependent deletion.
                 ;; Respects last-write-wins: only deletes quads whose creation time <= clear tx-time.
                 ;; 1. Read all (S, P, O) from $$cspo[c]
                 (local-select> [(keypath *c) ALL (collect-one FIRST)
                                 LAST ALL (collect-one FIRST)
                                 LAST ALL] $$cspo :> [*s *p *o])
                 ;; 2. Check existing state
                 (|hash *s)
                 (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *quad-creation-time)
                 ;; 3. Apply deletion if quad exists and clear is not older than creation
                 (<<if (and> (some? *quad-creation-time)
                             (>= *tx-time *quad-creation-time))
                       ;; Remove from all indices
                       (|hash *s) (local-transform> [(keypath *s *p *o) (set-elem *c) NONE>] $$spoc)
                       (|hash *p) (local-transform> [(keypath *p *o *s) (set-elem *c) NONE>] $$posc)
                       (|hash *o) (local-transform> [(keypath *o *s *p) (set-elem *c) NONE>] $$ospc)
                       (|hash *c) (local-transform> [(keypath *c *s *p) (set-elem *o) NONE>] $$cspo)
                       (|hash *s) (local-transform> [(keypath *s *p *o *c) NONE>] $$quad-tx-time)
                       (|hash *p)
                       (local-transform> [(keypath *p :count) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)

                       ;; Decrement distinct subjects only when last triple for (pred, subject) removed
                       (local-select> [(keypath *p *s)] $$pred-subj-count :> *cur-subj-count)
                       (local-transform> [(keypath *p *s) (nil->val 0) (term dec-floor-zero)] $$pred-subj-count)
                       (<<if (= *cur-subj-count 1)
                             ;; Last occurrence - decrement distinct count AND cleanup the mapping
                             (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                             (local-transform> [(keypath *p *s) NONE>] $$pred-subj-count))

                       ;; Decrement distinct objects only when last triple for (pred, object) removed
                       (local-select> [(keypath *p *o)] $$pred-obj-count :> *cur-obj-count)
                       (local-transform> [(keypath *p *o) (nil->val 0) (term dec-floor-zero)] $$pred-obj-count)
                       (<<if (= *cur-obj-count 1)
                             ;; Last occurrence - decrement distinct count AND cleanup the mapping
                             (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                             (local-transform> [(keypath *p *o) NONE>] $$pred-obj-count))

                       (|hash "")
                       (local-transform> [(keypath "" :total-triples) (nil->val 0) (term dec-floor-zero)] $$global-stats)
                       ;; Update type views if this is an rdf:type triple (context-aware cardinality)
                       (<<if (= *p RDF-TYPE-PREDICATE)
                             ;; Decrement type->subject occurrence count
                             (|hash *o)
                             (local-select> [(keypath *o *s)] $$type-subject-count :> *cur-ts-count)
                             (local-transform> [(keypath *o *s) (nil->val 0) (term dec-floor-zero)] $$type-subject-count)
                             ;; Only remove from type view if this was the LAST occurrence
                             (<<if (= *cur-ts-count 1)
                                   (local-transform> [(keypath *o) (set-elem *s) NONE>] $$type-subjects)
                                   (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$type-subjects)
                                   (local-transform> [(keypath *o *s) NONE>] $$type-subject-count))
                             ;; Decrement subject->type occurrence count (symmetric)
                             (|hash *s)
                             (local-select> [(keypath *s *o)] $$subject-type-count :> *cur-st-count)
                             (local-transform> [(keypath *s *o) (nil->val 0) (term dec-floor-zero)] $$subject-type-count)
                             ;; Only remove from subject-types view if this was the LAST occurrence
                             (<<if (= *cur-st-count 1)
                                   (local-transform> [(keypath *s) (set-elem *o) NONE>] $$subject-types)
                                   (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$subject-types)
                                   (local-transform> [(keypath *s *o) NONE>] $$subject-type-count)))))))

  ;; --- Namespace Storage ---
  ;; Separate microbatch topology for namespace operations
    (let [ns-mb (microbatch-topology topologies "ns-indexer")]
    ;; $$namespaces: prefix -> IRI mapping
      (declare-pstate ns-mb $$namespaces {String String})

      (<<sources ns-mb
                 (source> *namespace-depot :> %ns-batch)
                 (%ns-batch :> [*op & *args])
                 (<<cond
                  (case> (= *op :set-ns))
                ;; [:set-ns prefix iri] - set or update a namespace
                  (identity *args :> [*prefix *iri])
                  (local-transform> [(keypath *prefix) (termval *iri)] $$namespaces)

                  (case> (= *op :remove-ns))
                ;; [:remove-ns prefix] - remove a namespace
                  (identity *args :> [*prefix])
                  (local-transform> [(keypath *prefix) NONE>] $$namespaces)

                  (case> (= *op :clear-ns))
                ;; [:clear-ns] - clear all namespaces
                  (local-transform> [MAP-VALS NONE>] $$namespaces))))

  ;; --- Namespace Query Topologies ---
  ;; Get a single namespace by prefix
    (<<query-topology topologies "get-namespace" [*prefix :> *iri]
                      (|all)
                      (local-select> [(keypath *prefix) (view identity)] $$namespaces :> *val)
                      (filter> (some? *val))
                      (|origin)
                      ;; Collect all non-nil values (should be at most one)
                      (aggs/+vec-agg *val :> *vals)
                      (first *vals :> *iri))

  ;; List all namespaces as a map of prefix -> IRI
    (<<query-topology topologies "list-namespaces" [:> *namespaces]
                      (|all)
                      (local-select> [ALL] $$namespaces :> *entry)
                      (|origin)
                      (aggs/+map-agg (first *entry) (second *entry) :> *namespaces))

	; --- 2. Basic Pattern Finder ---
		; Assumes the Client sends explicit nils for wildcards
    (find-triples-query-topology topologies)
		; --- 3. BGP Executor ---
    (find-bgp-query-topology topologies)
		; --- 4. JOIN Operator ---
    (join-query-topology topologies)
		; --- 4. LEFT JOIN Operator ---
    (left-join-query-topology topologies)
    ; --- 4. MULTI-LEFT-JOIN Operator (flattened OPTIONAL chains) ---
    (multi-left-join-query-topology topologies)
		; --- 4a. CO-LOCATED SUBJECT JOIN (optimized for subject-key joins) ---
    (colocated-subject-join-query-topology topologies)
		; --- 4b. UNION Operator ---
    (union-query-topology topologies)
		; --- 5. PROJECT Operator ---
    (project-query-topology topologies)
    ; --- 5b. DISTINCT Operator ---
    (distinct-query-topology topologies)
		; --- 6. SLICE Operator ---
    (slice-query-topology topologies)
		; --- 7. ASK Result ---
    (ask-result-query-topology topologies)
    ; --- 8. FILTER Operator (using parallel version for better performance) ---
    (filter-query-topology-parallel topologies)
    ; --- 8b. SELF-JOIN Optimizer (critical for QJ4-style queries) ---
    (self-join-query-topology topologies)
    ; --- 8c. BATCH-LOOKUP for batch property fetching ---
    (batch-lookup-query-topology topologies)
    ; --- 9. BIND Operator ---
    (bind-query-topology topologies)
    ; --- 10. ORDER BY Operator ---
    (order-query-topology topologies)
    ; --- 11. LIST CONTEXTS (for clearInternal support) ---
    (list-contexts-query-topology topologies)
    ; --- 11b. COUNT STATEMENTS (for sizeInternal support) ---
    (count-statements-query-topology topologies)
    ; --- 11c. STATISTICS Query Topologies (for cardinality estimation) ---
    (get-predicate-stats-query-topology topologies)
    (get-all-predicate-stats-query-topology topologies)
    (get-global-stats-query-topology topologies)
    ; --- 11d. MATERIALIZED VIEW Query Topologies (for type-based queries) ---
    (get-subjects-by-type-query-topology topologies)
    (get-types-of-subject-query-topology topologies)
    ; --- 12. GROUP BY / Aggregates ---
    (group-query-topology topologies)

		; --- 4. Recursive Query Planner ---
    (<<query-topology topologies "execute-plan" [*plan :> *results]
                      (identity *plan :> {:keys [*op]})
                      (<<switch *op
                                (case> :bgp)
                                (get *plan :pattern :> *pattern)
                                (invoke-query "find-bgp" *pattern :> *results)

                                (case> :join)
                                (identity *plan :> {*left-plan :left *right-plan :right *join-vars :join-vars})
                                (get *plan :result-limit nil :> *result-limit)
                                (invoke-query "join" *left-plan *right-plan *join-vars *result-limit :> *results)

                                (case> :left-join)
                                (identity *plan :> {*left-plan :left *right-plan :right *join-vars :join-vars})
                                (get *plan :condition nil :> *condition)
                                (invoke-query "left-join" *left-plan *right-plan *join-vars *condition :> *results)

                                (case> :multi-left-join)
                                (identity *plan :> {*base-plan :base *optionals :optionals})
                                (invoke-query "multi-left-join" *base-plan *optionals :> *results)

                                (case> :union)
                                (identity *plan :> {*left-plan :left *right-plan :right})
                                (invoke-query "union" *left-plan *right-plan :> *results)

                                (case> :project)
                                (identity *plan :> {*sub-plan :sub-plan *proj-vars :vars})
                                (invoke-query "project" *sub-plan *proj-vars :> *results)

                                (case> :distinct)
                                (identity *plan :> {*sub-plan :sub-plan})
                                (invoke-query "distinct" *sub-plan :> *results)

                                (case> :slice)
                                (identity *plan :> {*sub-plan :sub-plan *offset :offset *limit :limit})
                                (invoke-query "slice" *sub-plan *offset *limit :> *results)

                                (case> :filter)
                                (identity *plan :> {*sub-plan :sub-plan *expr :expr})
                                (invoke-query "filter" *sub-plan *expr :> *results)

                                (case> :bind)
                                (identity *plan :> {*sub-plan :sub-plan *bindings :bindings})
                                (invoke-query "bind" *sub-plan *bindings :> *results)

                                (case> :order)
                                (identity *plan :> {*sub-plan :sub-plan *order-specs :order-specs})
                                (invoke-query "order" *sub-plan *order-specs :> *results)

                                (case> :ask)
                                (identity *plan :> {*sub-plan :sub-plan})
                                (invoke-query "ask-result" *sub-plan :> *results)

                                (case> :group)
                                (identity *plan :> {*sub-plan :sub-plan *group-vars :group-vars *aggregates :aggregates})
                                (invoke-query "group" *sub-plan *group-vars *aggregates :> *results)

                                (case> :self-join)
                                (identity *plan :> {*predicate :predicate *join-var :join-var
                                                    *left-subject :left-subject *right-subject :right-subject
                                                    *filter :filter *context :context})
                                (get *plan :result-limit nil :> *sj-result-limit)
                                (invoke-query "self-join" *predicate *join-var *left-subject *right-subject *filter *context *sj-result-limit :> *results)

                                (case> :colocated-subject-join)
                                (identity *plan :> {*left-pattern :left-pattern *right-pattern :right-pattern
                                                    *subject-var :subject-var})
                                (invoke-query "colocated-subject-join" *left-pattern *right-pattern *subject-var :> *results)

                                (case> :type-lookup)
                                ;; Use materialized type view for rdf:type patterns
                                ;; Returns bindings like [{subject-var subject1} {subject-var subject2} ...]
                                (identity *plan :> {*type-iri :type-iri *subject-var :subject-var})
                                (invoke-query "get-subjects-by-type" *type-iri :> *subjects)
                                ;; Convert subjects list to set of bindings
                                (qh/subjects-to-bindings *subjects *subject-var :> *results)

                                (case> :values)
                                (get *plan :bindings :> *results)

                                (case> :batch-enrich)
                                ;; Batch property lookup optimization
                                ;; Instead of N individual lookups, fetch all values in one batch query
                                (identity *plan :> {*sub-plan :sub-plan *predicate :predicate
                                                    *subject-var :subject-var *object-var :object-var
                                                    *context :context})
                                ;; 1. Execute sub-plan to get results
                                (invoke-query "execute-plan" *sub-plan :> *sub-results)
                                ;; 2. Extract unique subject values for batch lookup
                                (qh/batch-enrich-extract-subjects *sub-results *subject-var :> *subjects)
                                ;; 3. Call batch-lookup to get {subject -> object} map
                                ;; CRITICAL: Pass context to filter by graph
                                (invoke-query "batch-lookup" *subjects *predicate *context :> *lookup-map)
                                ;; 4. Merge object values into each binding
                                (qh/batch-enrich-merge-results *sub-results *lookup-map *subject-var *object-var :> *results)

                                (default>)
                                (identity #{} :> *results))
                      (|origin))))
