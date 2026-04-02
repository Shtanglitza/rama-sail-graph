(ns rama-sail.module.queries
  "Composable building blocks for all RDF query topologies and the recursive plan executor.
   Includes BGP, joins, filters, aggregates, ORDER BY, and all SPARQL operator topologies.
   Use these functions from within a defmodule body."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.string :as str]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [rama-sail.module.indexer :as idx]
            [rama-sail.query.expr :refer [eval-expr evaluate-filter-cond]]
            [rama-sail.query.aggregation :as agg]
            [rama-sail.query.helpers :as qh]))

(defn find-triples-query-topology [topologies]
  (<<query-topology topologies "find-triples"
                    [*s *p *o *c :> *quads]
                    (<<cond
                     (case> (some? *s))
                     (|hash *s)
                     (idx/scan-3-levels-index *s *p *o *c $$spoc :> *f-s *f-p *f-o *f-c)

                     (case> (some? *p))
                     (|hash *p)
                     (idx/scan-3-levels-index *p *o *s *c $$posc :> *f-p *f-o *f-s *f-c)

                     (case> (some? *o))
                     (|hash *o)
                     (idx/scan-3-levels-index *o *s *p *c $$ospc :> *f-o *f-s *f-p *f-c)

                     (case> (some? *c))
                     (|hash *c)
                     (idx/scan-3-levels-index *c *s *p *o $$cspo :> *f-c *f-s *f-p *f-o)

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
                    (identity *pattern :> {:keys [*s *p *o *c]})
                    (<<if (str/starts-with? *s "?") (identity nil :> *s-const) (else>) (identity *s :> *s-const))
                    (<<if (str/starts-with? *p "?") (identity nil :> *p-const) (else>) (identity *p :> *p-const))
                    (<<if (str/starts-with? *o "?") (identity nil :> *o-const) (else>) (identity *o :> *o-const))
                    (<<if (or> (nil? *c) (str/starts-with? *c "?"))
                          (identity nil :> *c-const) (else>) (identity *c :> *c-const))
                    (invoke-query "find-triples" *s-const *p-const *o-const *c-const :> *quads)
                    (ops/explode *quads :> [*f-s *f-p *f-o *f-c])
                    (identity {} :> *b-init)
                    (<<if (nil? *s-const) (assoc *b-init *s *f-s :> *b-1) (else>) (identity *b-init :> *b-1))
                    (<<if (nil? *p-const) (assoc *b-1 *p *f-p :> *b-2) (else>) (identity *b-1 :> *b-2))
                    (<<if (nil? *o-const) (assoc *b-2 *o *f-o :> *b-3) (else>) (identity *b-2 :> *b-3))
                    (<<if (and> (some? *c) (str/starts-with? *c "?"))
                          (assoc *b-3 *c *f-c :> *b-final) (else>) (identity *b-3 :> *b-final))
                    (|origin)
                    (aggs/+set-agg *b-final :> *bindings)))

;;; ---------------------------------------------------------------------------
;;; Hash Join Implementation
;;; ---------------------------------------------------------------------------

(defn join-query-topology [topologies]
  (<<query-topology topologies "join"
                    [*left-plan *right-plan *join-vars *result-limit :> *results]

                    (invoke-query "execute-plan" *right-plan :> *right-results)
                    (qh/build-hash-index *right-results *join-vars :> *hash-idx)

                    (invoke-query "execute-plan" *left-plan :> *left-results)
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

(defn self-join-query-topology [topologies]
  (<<query-topology topologies "self-join"
                    [*predicate *join-var *left-subject *right-subject *filter *context *result-limit :> *results]

                    (identity {:s "?s" :p *predicate :o "?o" :c *context} :> *pattern)
                    (invoke-query "find-bgp" *pattern :> *bgp-results)

                    (qh/build-subject-groups *bgp-results :> *groups)

                    (qh/process-subject-groups *groups *left-subject *right-subject *join-var *filter *result-limit :> *results)

                    (|origin)))

;; Combiner for union - merges two Sets for distributed aggregation
(def +set-union
  (combiner
   (fn [set1 set2] (into (or set1 #{}) set2))
   :init-fn (fn [] #{})))

;; --- Batch Lookup ---

(defn batch-lookup-query-topology [topologies]
  (<<query-topology topologies "batch-lookup"
                    [*subjects *predicate *context :> *results]

                    (ops/explode *subjects :> *subj)
                    (|hash *subj)
                    (local-select> [(keypath *subj *predicate) ALL (collect-one FIRST) LAST ALL] $$spoc :> [*obj *ctx])
                    (filter> (some? *obj))
                    (<<if (some? *context)
                          (filter> (= *ctx *context))
                          (else>)
                          (identity true :> *_pass))
                    (|origin)
                    (vector *subj *obj :> *pair)
                    (aggs/+vec-agg *pair :> *results)))

;; --- Co-located Subject Join ---

(defn colocated-subject-join-query-topology [topologies]
  (<<query-topology topologies "colocated-subject-join"
                    [*left-pattern *right-pattern *subject-var :> *results]
                    (|all)
                    (local-select> [ALL] $$spoc :> *entry)
                    (key *entry :> *subj)
                    (val *entry :> *pred-map)
                    (qh/join-subject-locally *left-pattern *right-pattern *subj *pred-map :> *subj-results)
                    (ops/explode *subj-results :> *result)
                    (|origin)
                    (aggs/+set-agg *result :> *results)))

;; --- Left Hash Join (Left Outer Join for OPTIONAL) ---

(defn left-join-query-topology [topologies]
  (<<query-topology topologies "left-join"
                    [*left-plan *right-plan *join-vars *condition :> *results]

                    (invoke-query "execute-plan" *right-plan :> *right-results)
                    (qh/build-hash-index *right-results *join-vars :> *hash-idx)

                    (invoke-query "execute-plan" *left-plan :> *left-results)
                    (ops/explode *left-results :> *left-bind)
                    (qh/apply-left-join-with-condition *hash-idx *left-bind *join-vars *condition :> *join-results)
                    (ops/explode *join-results :> *joined)

                    (|origin)
                    (aggs/+set-agg *joined :> *results)))

(defn multi-left-join-query-topology [topologies]
  (<<query-topology topologies "multi-left-join"
                    [*base-plan *optionals :> *results]
                    (invoke-query "execute-plan" *base-plan :> *base-results)
                    (ops/explode *optionals :> *opt)
                    (get *opt :plan :> *opt-plan)
                    (get *opt :join-vars :> *opt-jv)
                    (get *opt :condition nil :> *opt-cond)
                    (invoke-query "execute-plan" *opt-plan :> *opt-results)
                    (qh/build-hash-index *opt-results *opt-jv :> *opt-idx)
                    (hash-map :hash-idx *opt-idx :join-vars *opt-jv :condition *opt-cond :> *opt-entry)
                    (|origin)
                    (aggs/+vec-agg *opt-entry :> *all-indices)
                    (aggs/+last *base-results :> *base-agg)
                    (qh/apply-multi-left-join *base-agg *all-indices :> *results)))

(defn union-query-topology [topologies]
  (<<query-topology topologies "union"
                    [*left-plan *right-plan :> *results]
                    (vector *left-plan *right-plan :> *plans)
                    (ops/explode *plans :> *plan)
                    (|shuffle)
                    (invoke-query "execute-plan" *plan :> *res)
                    (|origin)
                    (+set-union *res :> *results)))

;; Combiner for ordered results - preserves insertion order
(def +vec-concat-combiner
  (combiner
   (fn [v1 v2] (into (or v1 []) v2))
   :init-fn (fn [] [])))

(defn project-query-topology [topologies]
  (<<query-topology topologies "project" [*sub-plan *proj-vars :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *all-res)
                    (ops/explode *all-res :> *binding)
                    (select-keys *binding *proj-vars :> *final-binding)
                    (vector *final-binding :> *wrapped)
                    (|origin)
                    (+vec-concat-combiner *wrapped :> *results)))

(defn distinct-query-topology [topologies]
  (<<query-topology topologies "distinct" [*sub-plan :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
                    (|origin)
                    (aggs/+set-agg *binding :> *results)))

(defn slice-query-topology [topologies]
  (<<query-topology topologies "slice" [*sub-plan *offset *limit :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *all-res)
                    (vec *all-res :> *all-vec)
                    (count *all-vec :> *total)
                    (max 0 *offset :> *safe-offset)
                    (<<if (< *limit 0)
                          (identity *total :> *upper)
                          (else>)
                          (min (+ *safe-offset *limit) *total :> *upper))
                    (<<if (>= *safe-offset *total)
                          (identity [] :> *results)
                          (else>)
                          (select> [(srange *safe-offset *upper)] *all-vec :> *results))
                    (|origin)))

(defn ask-result-query-topology [topologies]
  (<<query-topology topologies "ask-result" [*sub-plan :> *results]
                    (hash-map :op :slice :sub-plan *sub-plan :offset 0 :limit 1 :> *limited-plan)
                    (invoke-query "execute-plan" *limited-plan :> *sub-results)
                    (select> [(view count)] *sub-results :> *count)
                    (<<if (> *count 0)
                          (identity #{{}} :> *results)
                          (else>)
                          (identity #{} :> *results))
                    (|origin)))

;; --- BIND Operator ---

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
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *row)
                    (apply-bindings *row *bindings :> *extended-row)
                    (|origin)
                    (aggs/+set-agg *extended-row :> *results)))

;; --- ORDER BY Operator ---

(defn order-query-topology [topologies]
  (<<query-topology topologies "order" [*sub-plan *order-specs :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *row)
                    (qh/compute-sort-key *row *order-specs :> *sort-key)
                    (vector (vector *sort-key *row) :> *single-keyed)
                    (|origin)
                    (+vec-concat-combiner *single-keyed :> *all-keyed)
                    (qh/sort-keyed-rows *all-keyed :> *sorted-keyed)
                    (qh/extract-sorted-rows *sorted-keyed :> *results)))

;; --- LIST CONTEXTS ---

(defn list-contexts-query-topology [topologies]
  (<<query-topology topologies "list-contexts" [:> *contexts]
                    (|all)
                    (local-select> [ALL (collect-one FIRST)
                                    LAST ALL (collect-one FIRST)
                                    LAST ALL (collect-one FIRST)
                                    LAST ALL] $$cspo :> [*ctx *s *p *o])
                    (|origin)
                    (aggs/+set-agg *ctx :> *contexts)))

;; --- COUNT STATEMENTS ---

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

(defn get-predicate-stats-query-topology [topologies]
  (<<query-topology topologies "get-predicate-stats" [*predicate :> *stats]
                    (|hash *predicate)
                    (local-select> [(keypath *predicate)] $$predicate-stats :> *stats)
                    (|origin)))

(defn get-all-predicate-stats-query-topology [topologies]
  (<<query-topology topologies "get-all-predicate-stats" [:> *all-stats]
                    (|all)
                    (local-select> [ALL] $$predicate-stats :> *entry)
                    (|origin)
                    (aggs/+map-agg (first *entry) (second *entry) :> *all-stats)))

(defn get-global-stats-query-topology [topologies]
  (<<query-topology topologies "get-global-stats" [:> *stats]
                    (|global)
                    (local-select> [(keypath :total-triples)] $$global-stats :> *total-triples)
                    (local-select> [(keypath :total-predicates)] $$global-stats :> *total-predicates)
                    (local-select> [(keypath :total-subjects)] $$global-stats :> *total-subjects)
                    (hash-map :total-triples *total-triples
                              :total-predicates *total-predicates
                              :total-subjects *total-subjects :> *stats)
                    (|origin)))

;; --- Materialized View Query Topologies ---

(defn get-subjects-by-type-query-topology [topologies]
  (<<query-topology topologies "get-subjects-by-type" [*type :> *subjects]
                    (|hash *type)
                    (local-select> [(keypath *type) (view vec)] $$type-subjects :> *data)
                    (<<if (nil? *data)
                          (identity [] :> *subjects)
                          (else>)
                          (identity *data :> *subjects))
                    (|origin)))

(defn get-types-of-subject-query-topology [topologies]
  (<<query-topology topologies "get-types-of-subject" [*subject :> *types]
                    (|hash *subject)
                    (local-select> [(keypath *subject) (view vec)] $$subject-types :> *data)
                    (<<if (nil? *data)
                          (identity [] :> *types)
                          (else>)
                          (identity *data :> *types))
                    (|origin)))

;; --- GROUP BY / Aggregates ---

(defn group-query-topology [topologies]
  (<<query-topology topologies "group"
                    [*sub-plan *group-vars *aggregates :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
                    (agg/build-group-entry *binding *group-vars *aggregates :> *group-entry)
                    (|origin)
                    (agg/+group-agg-combiner *group-entry :> *combined-groups)
                    (agg/finalize-group-results *combined-groups *group-vars *aggregates :> *results)))

;; --- FILTER Operator ---

(defn filter-query-topology-parallel [topologies]
  (<<query-topology topologies "filter" [*sub-plan *expr :> *results]
                    (invoke-query "execute-plan" *sub-plan :> *sub-results)
                    (ops/explode *sub-results :> *binding)
                    (|shuffle)
                    (filter> (evaluate-filter-cond *expr *binding))
                    (hash-set *binding :> *binding-set)
                    (|origin)
                    (+set-union *binding-set :> *results)))

;; ============================================================================
;; Registration Functions
;; ============================================================================

(defn register-rdf-query-topologies
  "Registers all core RDF query topologies (BGP, joins, filters, etc.).
   Call from within a defmodule body."
  [topologies]
  (find-triples-query-topology topologies)
  (find-bgp-query-topology topologies)
  (join-query-topology topologies)
  (left-join-query-topology topologies)
  (multi-left-join-query-topology topologies)
  (colocated-subject-join-query-topology topologies)
  (union-query-topology topologies)
  (project-query-topology topologies)
  (distinct-query-topology topologies)
  (slice-query-topology topologies)
  (ask-result-query-topology topologies)
  (filter-query-topology-parallel topologies)
  (self-join-query-topology topologies)
  (batch-lookup-query-topology topologies)
  (bind-query-topology topologies)
  (order-query-topology topologies)
  (list-contexts-query-topology topologies)
  (count-statements-query-topology topologies)
  (get-predicate-stats-query-topology topologies)
  (get-all-predicate-stats-query-topology topologies)
  (get-global-stats-query-topology topologies)
  (get-subjects-by-type-query-topology topologies)
  (get-types-of-subject-query-topology topologies)
  (group-query-topology topologies))

(defn execute-plan-query-topology
  "Registers the recursive query plan executor topology.
   Routes each plan node to the appropriate operator topology by :op field.
   Call from within a defmodule body after all operator topologies are registered."
  [topologies]
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
                              (identity *plan :> {*type-iri :type-iri *subject-var :subject-var})
                              (invoke-query "get-subjects-by-type" *type-iri :> *subjects)
                              (qh/subjects-to-bindings *subjects *subject-var :> *results)

                              (case> :values)
                              (get *plan :bindings :> *results)

                              (case> :batch-enrich)
                              (identity *plan :> {*sub-plan :sub-plan *predicate :predicate
                                                  *subject-var :subject-var *object-var :object-var
                                                  *context :context})
                              (invoke-query "execute-plan" *sub-plan :> *sub-results)
                              (qh/batch-enrich-extract-subjects *sub-results *subject-var :> *subjects)
                              (invoke-query "batch-lookup" *subjects *predicate *context :> *lookup-map)
                              (qh/batch-enrich-merge-results *sub-results *lookup-map *subject-var *object-var :> *results)

                              (default>)
                              (identity #{} :> *results))
                    (|origin)))
