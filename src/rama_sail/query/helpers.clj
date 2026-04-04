(ns rama-sail.query.helpers
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [rama-sail.query.expr :refer [eval-expr parse-numeric evaluate-filter-cond]]))

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

(defn build-hash-index
  "Build a hash index from a collection of bindings.
   Returns {join-key -> [binding1, binding2, ...]}
   Empty join-vars creates a single key [] for cross-product joins.
   Bindings with missing join key values are dropped with debug logging."
  [bindings join-vars]
  (reduce
   (fn [idx binding]
     (let [key (extract-join-key binding join-vars)]
       (if key
         (update idx key (fnil conj []) binding)
         (do
           (log/debug "Dropping binding with missing join key(s)"
                      {:join-vars join-vars :binding binding})
           idx))))
   {}
   bindings))

(defn probe-hash-index
  "Probe hash index with a binding. Returns seq of merged bindings or nil."
  [hash-idx binding join-vars]
  (let [key (extract-join-key binding join-vars)]
    (when key
      (when-let [matches (get hash-idx key)]
        (map #(merge % binding) matches)))))

;;; ---------------------------------------------------------------------------
;;; Self-Join Helpers
;;; ---------------------------------------------------------------------------

(defn apply-inequality-filter
  "Check if s1 and s2 satisfy the inequality filter.
   Filter format: {:op :lt/:gt/:le/:ge/:ne, :left var, :right var}
   Returns false if types don't match (ClassCastException from compare)."
  [s1 s2 filter-spec left-subject right-subject]
  (if (nil? filter-spec)
    true  ;; No filter - all pairs valid
    (let [op (:op filter-spec)
          filter-left (:left filter-spec)
          ;; Determine which value goes where based on variable names
          [v1 v2] (if (= filter-left left-subject)
                    [s1 s2]
                    [s2 s1])]
      (try
        (case op
          :lt (neg? (compare v1 v2))
          :le (not (pos? (compare v1 v2)))
          :gt (pos? (compare v1 v2))
          :ge (not (neg? (compare v1 v2)))
          :ne (not= v1 v2)
          true)
        (catch ClassCastException _
          ;; Types don't match for comparison - filter out this pair
          false)))))

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
        ;; CRITICAL: Sort subjects for the optimization to work correctly
        subject-vec (if optimized-range?
                      (vec (sort subjects))
                      (vec subjects))
        n (count subject-vec)
        pairs (if optimized-range?
                ;; Optimized: subjects are sorted, generate only ordered index pairs
                (let [op (:op filter-spec)]
                  (for [i (range n)
                        j (if (= op :lt)
                            (range (inc i) n)
                            (range 0 i))
                        :let [s1 (subject-vec i)
                              s2 (subject-vec j)]]
                    {left-subject s1
                     right-subject s2
                     join-var join-value}))
                ;; Standard: generate all pairs (not= i j), apply filter
                (for [i (range n)
                      j (range n)
                      :when (not= i j)
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

(defn process-subject-groups
  "Process grouped subjects and generate all valid self-join pairs.
   When result-limit is non-nil, stops after collecting that many pairs."
  ([groups left-subject right-subject join-var filter-spec]
   (process-subject-groups groups left-subject right-subject join-var filter-spec nil))
  ([groups left-subject right-subject join-var filter-spec result-limit]
   (if result-limit
     ;; With limit: use reduce to short-circuit once we have enough
     (reduce
      (fn [acc [join-value subjects]]
        (if (>= (count acc) result-limit)
          (reduced acc)
          (if (> (count subjects) 1)
            (let [remaining (- result-limit (count acc))
                  pairs (generate-self-join-pairs subjects join-value left-subject right-subject join-var filter-spec remaining)]
              (into acc pairs))
            acc)))
      #{}
      groups)
     ;; Without limit: original behavior
     (into #{}
           (mapcat
            (fn [[join-value subjects]]
              (when (> (count subjects) 1)
                (generate-self-join-pairs subjects join-value left-subject right-subject join-var filter-spec nil))))
           groups))))

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
  "Merge batch lookup results into bindings using INNER JOIN semantics.
   lookup-pairs is a vector of [subject object] pairs from batch-lookup.
   For each binding, looks up the subject-var's value and adds matching objects.
   - If no lookup result exists, the binding is EXCLUDED (inner-join semantics).
   - If multiple objects exist for a subject (multi-valued predicate),
     the binding is exploded into multiple bindings."
  [bindings lookup-pairs subject-var object-var]
  (let [lookup-map (pairs-to-grouped-map lookup-pairs)]
    (into #{}
          (mapcat (fn [binding]
                    (when-let [subj (get binding subject-var)]
                      (when-let [objs (get lookup-map subj)]
                        ;; objs is a set of objects (may be 1 or more)
                        ;; Explode into multiple bindings for multi-valued predicates
                        (map (fn [obj] (assoc binding object-var obj)) objs)))))
          bindings)))

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
    ;; Cross product
    (for [l left-bindings, r right-bindings]
      (merge l r))
    ;; Hash join
    (let [right-idx (build-hash-index right-bindings join-vars)]
      (for [l left-bindings
            :let [key (extract-join-key l join-vars)]
            :when key
            r (get right-idx key [])]
        (merge l r)))))

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
   Returns seq of result bindings."
  [hash-idx left-bind join-vars condition]
  (let [key (extract-join-key left-bind join-vars)
        matches (and key (get hash-idx key))]
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
  "Apply multiple optional joins to base bindings in a single pass.
   Each optional-index has {:hash-idx :join-vars :condition}."
  [base-bindings optional-indices]
  (reduce
   (fn [bindings {:keys [hash-idx join-vars condition]}]
     (into #{}
           (mapcat #(apply-left-join-with-condition hash-idx % join-vars condition))
           bindings))
   base-bindings
   optional-indices))

;;; ---------------------------------------------------------------------------
;;; ORDER BY Helpers
;;; ---------------------------------------------------------------------------

;; Comparable wrapper for DESC string ordering.
;; Reverses natural string comparison so larger strings sort first.
(deftype DescString [^String s]
  Comparable
  (compareTo [_ other]
    ;; Reverse comparison: compare other to this instead of this to other
    (compare (.s ^DescString other) s))

  Object
  (equals [_ other]
    (and (instance? DescString other)
         (= s (.s ^DescString other))))
  (hashCode [_]
    (.hashCode s))
  (toString [_]
    (str "DescString[" s "]")))

(defn compute-sort-key
  "Compute a composite sort key where natural vector ordering produces correct result.
   Each element is [category value] where:
   - category 0: nil (ASC) - sorts first
   - category 1: actual value (possibly negated for DESC numerics, wrapped for DESC strings)
   - category 2: nil (DESC) - sorts last
   This ensures proper SPARQL null ordering and ASC/DESC behavior."
  [row order-specs]
  (mapv (fn [{:keys [expr ascending]}]
          (let [raw (eval-expr expr row)
                parsed (when raw (parse-numeric raw))]
            (cond
              ;; nil: use category to control null ordering
              (nil? raw)
              (if ascending [0 nil] [2 nil])

              ;; numeric: negate for DESC so larger values sort first
              parsed
              [1 (if ascending parsed (- parsed))]

              ;; string/other: wrap in DescString for DESC to reverse comparison
              :else
              [1 (if ascending raw (->DescString raw))])))
        order-specs))

(defn sort-keyed-rows
  "Sort [sort-key, row] tuples by sort-key and return sorted vector."
  [keyed-rows]
  (vec (sort-by first keyed-rows)))

(defn extract-sorted-rows
  "Extract just the rows from sorted [sort-key, row] tuples."
  [sorted-keyed-rows]
  (mapv second sorted-keyed-rows))
