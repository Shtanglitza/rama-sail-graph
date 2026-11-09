(ns ^:no-doc rama-sail.query.helpers
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [rama-sail.query.expr :refer [eval-expr error error? evaluate-filter-cond
                                          terms-equal compare-terms order-key
                                          order-comparable?]]))

;; RDF type predicate IRI - used for materialized view maintenance
(def RDF-TYPE-PREDICATE "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")

;;; ---------------------------------------------------------------------------
;;; Hash Join Helpers
;;; ---------------------------------------------------------------------------

(defn extract-join-key
  "Extract join key values from a binding map.
   Returns vector of values for join variables, or nil if any are missing."
  [binding join-vars]
  (when binding
    (let [values (mapv #(get binding %) join-vars)]
      (when (every? some? values)
        values))))

(defn subjects-to-bindings
  "Convert a collection of subject values to a set of binding maps.
   Each binding is {var-name subject-value}."
  [subjects var-name]
  (set (map (fn [s] {var-name s}) subjects)))

(defn compatible-bindings?
  "SPARQL solution compatibility: two bindings are compatible when every
   variable bound in both maps to the same value. A variable bound in only
   one binding is compatible with anything."
  [b1 b2]
  (reduce-kv (fn [_ k v]
               (let [other (get b2 k ::absent)]
                 (if (or (= other ::absent) (= other v))
                   true
                   (reduced false))))
             true
             b1))

(defn build-hash-index
  "Build a hash index from a collection of bindings.
   Returns {:keyed {join-key -> [binding ...]} :wildcard [binding ...]}.
   Empty join-vars creates a single key [] (cross-product with the
   compatibility check applied at probe time). Bindings missing a join-key
   value go into :wildcard — per SPARQL compatibility an unbound variable
   joins with anything, so such rows must be candidates for every probe."
  [bindings join-vars]
  (reduce
   (fn [idx binding]
     (if-let [key (extract-join-key binding join-vars)]
       (update-in idx [:keyed key] (fnil conj []) binding)
       (update idx :wildcard conj binding)))
   {:keyed {} :wildcard []}
   bindings))

(defn hash-index-candidates
  "Candidate matches for a probe binding: the bucket for its join key plus
   all wildcard rows. A probe binding missing a join-key value must consider
   every indexed row (it is compatible with any key)."
  [hash-idx binding join-vars]
  (let [key (extract-join-key binding join-vars)]
    (if key
      (concat (get (:keyed hash-idx) key) (:wildcard hash-idx))
      (concat (mapcat val (:keyed hash-idx)) (:wildcard hash-idx)))))

(defn probe-hash-index
  "Probe hash index with a binding. Returns seq of merged bindings or nil.
   Candidates are filtered for full SPARQL compatibility (every shared
   variable — join key or not — must agree) before merging."
  [hash-idx binding join-vars]
  (seq (keep #(when (compatible-bindings? binding %) (merge % binding))
             (hash-index-candidates hash-idx binding join-vars))))

;;; ---------------------------------------------------------------------------
;;; DISTINCT / GRAPH-scope Helpers
;;; ---------------------------------------------------------------------------

(defn distinct-preserving-order
  "SPARQL DISTINCT that keeps the first occurrence of each solution in input
   order, so an ORDER BY below it survives (and a LIMIT above it takes the
   true top-k)."
  [bindings]
  (into [] (distinct) bindings))

(defn drop-default-graph-quads
  "Remove quads stored in the default graph. `GRAPH ?g { ... }` ranges over
   NAMED graphs only (SPARQL 1.1 §13.3); the default graph is not a named
   graph, so its quads must not match — previously they did, with ?g bound
   to the internal default-graph sentinel (and then silently unbound)."
  [quads default-context]
  (into [] (remove #(= default-context (nth % 3))) quads))

;;; ---------------------------------------------------------------------------
;;; Self-Join Helpers
;;; ---------------------------------------------------------------------------

(defn- self-join-operand
  "The value a self-join filter compares for subject s: the subject itself,
   or its STR() when the filter is STR(?a) op STR(?b) — evaluated by the
   expression engine, so it agrees with the unrewritten FILTER."
  [s filter-spec]
  (if (:str? filter-spec)
    (let [v (eval-expr {:type :str :arg {:type :const :val s}} {})]
      (when-not (error? v) v))
    s))

(defn apply-inequality-filter
  "Check if s1 and s2 satisfy the inequality filter.
   Filter format: {:op :lt/:gt/:le/:ge/:ne, :left var, :right var, :str? bool}

   Uses the expression engine's comparator, so the pair is kept exactly when
   the same FILTER, evaluated normally, would be true: != is RDFterm-equal
   (`expr/terms-equal`); < <= > >= use `expr/compare-terms`, under which
   subjects (IRIs, blank nodes, triple terms) are never order-comparable —
   a type error, i.e. the pair is dropped."
  [s1 s2 filter-spec left-subject right-subject]
  (if (nil? filter-spec)
    true  ;; No filter - all pairs valid
    (let [op (:op filter-spec)
          filter-left (:left filter-spec)
          ;; Determine which value goes where based on variable names
          [v1 v2] (map #(self-join-operand % filter-spec)
                       (if (= filter-left left-subject) [s1 s2] [s2 s1]))]
      (case op
        :ne (and v1 v2 (false? (terms-equal v1 v2)))
        (:lt :le :gt :ge)
        (let [c (if (and v1 v2) (compare-terms v1 v2) error)]
          (and (not (error? c))
               (case op
                 :lt (neg? c)
                 :le (not (pos? c))
                 :gt (pos? c)
                 :ge (not (neg? c)))))
        true))))

(defn generate-self-join-pairs
  "Generate valid pairs from a group of subjects sharing the same join key.
   Applies inequality filter during generation to avoid creating invalid pairs.
   When result-limit is non-nil, stops generating after that many pairs.

   Optimization: For :lt or :gt filters where filter-left matches left-subject,
   we SORT the subjects first, then generate only pairs where i < j (for :lt)
   or i > j (for :gt), cutting the number of pairs in half."
  [subjects join-value left-subject right-subject join-var filter-spec result-limit]
  (let [;; Check if we can apply the half-pairs optimization
        ;; Only when filter-left matches left-subject and op is :lt or :gt
        optimized-range? (and filter-spec
                              (= (:left filter-spec) left-subject)
                              (#{:lt :gt} (:op filter-spec)))
        ;; CRITICAL: Sort subjects for the optimization to work correctly.
        ;; Only order-comparable terms can satisfy < or > (see
        ;; apply-inequality-filter); subjects never are under strict SPARQL,
        ;; so this yields no pairs — but stays correct if that ever changes.
        ;; Sorted by the compared value (the subject, or its STR); keys kept
        ;; alongside so equal values (no strict order) never pair.
        keyed (when optimized-range?
                (->> subjects
                     (keep (fn [s] (let [v (self-join-operand s filter-spec)]
                                     (when (and v (order-comparable? v)) [(order-key v) s]))))
                     (sort-by first)
                     vec))
        subject-vec (if optimized-range? (mapv second keyed) (vec subjects))
        n (count subject-vec)
        pairs (if optimized-range?
                ;; Optimized: subjects are sorted, generate only ordered index pairs
                (let [op (:op filter-spec)]
                  (for [i (range n)
                        j (if (= op :lt)
                            (range (inc i) n)
                            (range 0 i))
                        :when (not= (first (keyed i)) (first (keyed j)))
                        :let [s1 (subject-vec i)
                              s2 (subject-vec j)]]
                    {left-subject s1
                     right-subject s2
                     join-var join-value}))
                ;; Standard: generate ALL ordered pairs, including the
                ;; reflexive i = j pair — `?a :p ?o . ?b :p ?o` matches
                ;; ?a = ?b (every triple joins with itself). Only the filter
                ;; may exclude it (:ne / strict :lt :gt reject equal subjects;
                ;; :le / :ge / no filter keep them).
                (for [i (range n)
                      j (range n)
                      :let [s1 (subject-vec i)
                            s2 (subject-vec j)]
                      :when (apply-inequality-filter s1 s2 filter-spec left-subject right-subject)]
                  {left-subject s1
                   right-subject s2
                   join-var join-value}))]
    (if result-limit
      (take result-limit pairs)
      pairs)))

(defn build-subject-groups
  "Build a map grouping subjects by their object value.
   Input: collection of {\"?s\" subject, \"?o\" object} bindings
   Output: {object -> [subjects...]}"
  [bgp-results]
  (reduce
   (fn [groups binding]
     (let [obj (get binding "?o")
           subj (get binding "?s")]
       (update groups obj (fnil conj []) subj)))
   {}
   bgp-results))

(defn- reflexive-pairs-excluded?
  "True when the self-join filter rejects every s1 = s2 pair (strict
   inequality or !=), so single-subject groups can yield nothing."
  [filter-spec]
  (contains? #{:lt :gt :ne} (:op filter-spec)))

(defn process-subject-groups
  "Process grouped subjects and generate all valid self-join pairs.
   When result-limit is non-nil, stops after collecting that many pairs.

   A group of ONE subject still yields the reflexive pair (?a = ?b) unless
   the filter is strict (:lt / :gt / :ne) — skipping singleton groups
   unconditionally silently dropped those rows."
  ([groups left-subject right-subject join-var filter-spec]
   (process-subject-groups groups left-subject right-subject join-var filter-spec nil))
  ([groups left-subject right-subject join-var filter-spec result-limit]
   (let [min-group-size (if (reflexive-pairs-excluded? filter-spec) 2 1)]
     (if result-limit
       ;; With limit: use reduce to short-circuit once we have enough
       (reduce
        (fn [acc [join-value subjects]]
          (if (>= (count acc) result-limit)
            (reduced acc)
            (if (>= (count subjects) min-group-size)
              (let [remaining (- result-limit (count acc))
                    pairs (generate-self-join-pairs subjects join-value left-subject right-subject join-var filter-spec remaining)]
                (into acc pairs))
              acc)))
        []
        groups)
       (into []
             (mapcat
              (fn [[join-value subjects]]
                (when (>= (count subjects) min-group-size)
                  (generate-self-join-pairs subjects join-value left-subject right-subject join-var filter-spec nil))))
             groups)))))

;;; ---------------------------------------------------------------------------
;;; Batch Enrich Helpers
;;; ---------------------------------------------------------------------------

(defn batch-enrich-extract-subjects
  "Extract unique subject values from bindings for batch lookup."
  [bindings subject-var]
  (into #{} (keep #(get % subject-var) bindings)))

(defn pairs-to-grouped-map
  "Convert a vector of [subject object] pairs to a map of {subject -> #{objects}}.
   Groups all objects by their subject for efficient lookup."
  [pairs]
  (reduce (fn [acc [subj obj]]
            (update acc subj (fnil conj #{}) obj))
          {}
          pairs))

(defn batch-enrich-merge-results
  "Merge batch lookup results into bindings.
   lookup-pairs is a vector of [subject object] pairs from batch-lookup.
   For each binding, looks up the subject-var's value and adds matching objects.
   - If no lookup result exists, the binding is EXCLUDED (inner-join semantics),
     or kept unchanged when `optional?` (OPTIONAL semantics).
   - If multiple objects exist for a subject (multi-valued predicate),
     the binding is exploded into multiple bindings."
  ([bindings lookup-pairs subject-var object-var]
   (batch-enrich-merge-results bindings lookup-pairs subject-var object-var false))
  ([bindings lookup-pairs subject-var object-var optional?]
   (let [lookup-map (pairs-to-grouped-map lookup-pairs)]
     (into []
           (mapcat (fn [binding]
                     ;; objs is a set of objects (may be 1 or more)
                     (if-let [objs (get lookup-map (get binding subject-var))]
                       (map (fn [obj] (assoc binding object-var obj)) objs)
                       (when optional? [binding]))))
           bindings))))

;;; ---------------------------------------------------------------------------
;;; Colocated Join Helpers
;;; ---------------------------------------------------------------------------

(defn match-pattern-for-subject
  "Match a BGP pattern for a specific subject against pred-map.
   Returns seq of bindings where subject is bound to subj.
   Pattern format: {:s subject-var :p pred-or-var :o obj-or-var :c ctx-or-nil}"
  [pattern subj pred-map]
  (let [{:keys [s p o c]} pattern
        p-var? (and (string? p) (str/starts-with? p "?"))
        o-var? (and (string? o) (str/starts-with? o "?"))]
    (for [[pred obj-map] pred-map
          :when (or p-var? (= p pred))
          [obj ctx-set] obj-map
          :when (or o-var? (= o obj))
          ctx ctx-set
          :when (or (nil? c) (= c ctx))]
      (cond-> {s subj}
        p-var? (assoc p pred)
        o-var? (assoc o obj)))))

(defn match-pattern-locally
  "Match a BGP pattern against local $$spoc data.
   Returns seq of bindings for subjects on this task.
   Pattern format: {:s subject-or-var :p pred-or-var :o obj-or-var :c ctx-or-nil}

   Optimization: Uses direct get/get-in for bound keys (O(1)) instead of
   scanning all entries (O(n)). Benchmarks show 2-3x speedup."
  [pattern spoc-data]
  (let [{:keys [s p o c]} pattern
        s-var? (and (string? s) (str/starts-with? s "?"))
        p-var? (and (string? p) (str/starts-with? p "?"))
        o-var? (and (string? o) (str/starts-with? o "?"))
        c-filter (if (nil? c) (constantly true) #(= c %))]
    (cond
      ;; All bound - direct lookup O(1)
      (and (not s-var?) (not p-var?) (not o-var?))
      (when-let [ctx-set (get-in spoc-data [s p o])]
        (for [ctx ctx-set :when (c-filter ctx)]
          {}))

      ;; Subject bound - direct lookup for subject O(1)
      (not s-var?)
      (when-let [pred-map (get spoc-data s)]
        (if (not p-var?)
          ;; S+P bound - direct lookup O(1)
          (when-let [obj-map (get pred-map p)]
            (if (not o-var?)
              ;; S+P+O bound (shouldn't reach here due to first cond)
              (when-let [ctx-set (get obj-map o)]
                (for [ctx ctx-set :when (c-filter ctx)]
                  {}))
              ;; S+P bound, O variable
              (for [[obj ctx-set] obj-map
                    ctx ctx-set
                    :when (c-filter ctx)]
                {o obj})))
          ;; S bound, P variable
          (for [[pred obj-map] pred-map
                [obj ctx-set] (if o-var? obj-map [[o (get obj-map o)]])
                :when (some? ctx-set)
                ctx ctx-set
                :when (c-filter ctx)]
            (cond-> {p pred}
              o-var? (assoc o obj)))))

      ;; Subject is variable - must scan all subjects
      :else
      (for [[subj pred-map] spoc-data
            [pred obj-map] (if p-var? pred-map [[p (get pred-map p)]])
            :when (some? obj-map)
            [obj ctx-set] (if o-var? obj-map [[o (get obj-map o)]])
            :when (some? ctx-set)
            ctx ctx-set
            :when (c-filter ctx)]
        (cond-> {s subj}
          p-var? (assoc p pred)
          o-var? (assoc o obj))))))

(defn colocated-join-local
  "Perform local hash join on two sets of bindings.
   join-vars specifies which variables to join on."
  [left-bindings right-bindings join-vars]
  (if (empty? join-vars)
    ;; Cross product (compatibility-checked: sides may still share variables)
    (for [l left-bindings
          r right-bindings
          :when (compatible-bindings? l r)]
      (merge l r))
    ;; Hash join
    (let [right-idx (build-hash-index right-bindings join-vars)]
      (mapcat #(probe-hash-index right-idx % join-vars) left-bindings))))

(defn join-subject-locally
  "Join two patterns for a single subject.
   Both patterns must have the same subject variable.
   Returns seq of joined bindings for this subject."
  [left-pattern right-pattern subj pred-map]
  (let [left-matches (match-pattern-for-subject left-pattern subj pred-map)
        right-matches (match-pattern-for-subject right-pattern subj pred-map)]
    ;; Both patterns share the same subject, so we can just cross-product
    ;; the other variables (they're already joined on subject)
    (for [l left-matches
          r right-matches]
      (merge l r))))

;;; ---------------------------------------------------------------------------
;;; Left Join Helpers
;;; ---------------------------------------------------------------------------

(defn apply-left-join-with-condition
  "Apply left join logic with optional condition.
   Returns seq of result bindings.
   Candidates are filtered for full SPARQL compatibility (every shared
   variable must agree), so a left row with an unbound join variable still
   matches compatible right rows instead of being treated as unmatched."
  [hash-idx left-bind join-vars condition]
  (let [matches (filter #(compatible-bindings? left-bind %)
                        (hash-index-candidates hash-idx left-bind join-vars))]
    (if (seq matches)
      ;; Has matches - apply condition filter
      (let [merged-results (map #(merge left-bind %) matches)]
        (if condition
          ;; With condition: filter merged results, fall back to left if none pass
          (let [passing (filter #(evaluate-filter-cond condition %) merged-results)]
            (if (seq passing)
              passing
              [left-bind]))
          ;; No condition: return all merged
          merged-results))
      ;; No matches - return left unchanged
      [left-bind])))

(defn apply-multi-left-join
  "Apply multiple optional joins to base bindings in a single pass, as the
   left-deep chain LeftJoin(LeftJoin(base, o0), o1)... Each optional-index has
   {:idx :hash-idx :join-vars :condition}; they are applied in :idx (query)
   order, because the topology collects them in completion order and
   OPTIONALs that share a variable do not commute."
  [base-bindings optional-indices]
  (reduce
   (fn [bindings {:keys [hash-idx join-vars condition]}]
     (into []
           (mapcat #(apply-left-join-with-condition hash-idx % join-vars condition))
           bindings))
   base-bindings
   (sort-by :idx optional-indices)))

;;; ---------------------------------------------------------------------------
;;; ORDER BY Helpers
;;; ---------------------------------------------------------------------------

(defn compute-sort-key
  "ORDER BY key of a row: one `expr/order-key` per order spec (SPARQL's total
   order: unbound/error < blank nodes < IRIs < literals < triple terms; `<`
   within a value class). Keys are pure data — direction is applied by
   `compare-sort-keys` — so they can be computed on any task and shipped."
  [row order-specs]
  (mapv (fn [{:keys [expr]}] (order-key (eval-expr expr row))) order-specs))

(defn compare-sort-keys
  "Compare two compute-sort-key results column by column, honouring each
   spec's direction (:ascending; DESC reverses the column, so unbound sorts
   last)."
  [order-specs ka kb]
  (loop [i 0 specs (seq order-specs)]
    (if (nil? specs)
      0
      (let [c (compare (nth ka i) (nth kb i))]
        (if (zero? c)
          (recur (inc i) (next specs))
          (if (:ascending (first specs)) c (- c)))))))

(defn sort-keyed-rows
  "Sort [sort-key, row] tuples by sort-key per the order specs' directions
   (stable, so ties keep input order) and return a sorted vector."
  [keyed-rows order-specs]
  (vec (sort (fn [a b] (compare-sort-keys order-specs (first a) (first b))) keyed-rows)))

(defn extract-sorted-rows
  "Extract just the rows from sorted [sort-key, row] tuples."
  [sorted-keyed-rows]
  (mapv second sorted-keyed-rows))
