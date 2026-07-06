(ns ^:no-doc rama-sail.query.aggregation
  (:use [com.rpl.rama])
  (:require [clojure.set]
            [rama-sail.query.expr :refer [eval-expr term-numeric-value
                                          compare-numbers add-numbers error?]]))

;; --- GROUP BY / Aggregates ---

;; XSD type URIs for aggregate result formatting
(def ^:private xsd-integer "http://www.w3.org/2001/XMLSchema#integer")
(def ^:private xsd-decimal "http://www.w3.org/2001/XMLSchema#decimal")
(def ^:private xsd-double "http://www.w3.org/2001/XMLSchema#double")

(defn- format-typed-literal
  "Format a value as an RDF typed literal string."
  [value dtype]
  (str "\"" value "\"^^<" dtype ">"))

(defn- integral-number? [v]
  (or (instance? Long v) (instance? Integer v)
      (instance? BigInteger v) (instance? clojure.lang.BigInt v)))

(defn- format-numeric-result
  "Format a numeric result by its type. Exact integer sums stay integers of
   any magnitude (no silent long overflow), decimals serialize canonically,
   and non-finite doubles use xsd:double lexical forms instead of throwing."
  [value]
  (cond
    (integral-number? value)
    (format-typed-literal value xsd-integer)

    (instance? BigDecimal value)
    (format-typed-literal (.toPlainString (.stripTrailingZeros ^BigDecimal value))
                          xsd-decimal)

    (number? value)
    (let [d (double value)]
      (cond
        (Double/isNaN d) (format-typed-literal "NaN" xsd-double)
        (Double/isInfinite d) (format-typed-literal (if (pos? d) "INF" "-INF") xsd-double)
        ;; whole in-range doubles keep the historical integer formatting
        (and (== d (Math/floor d))
             (<= (double Long/MIN_VALUE) d (double Long/MAX_VALUE)))
        (format-typed-literal (long d) xsd-integer)
        :else (format-typed-literal d xsd-decimal)))

    :else (str value)))

(defn- compare-rdf-terms
  "Compare two RDF term values for MIN/MAX ordering.
   Numeric literals (per datatype) compare numerically and exactly;
   everything else compares lexicographically on the serialized form.
   Returns negative if a < b, positive if a > b, zero if equal."
  [a b]
  (let [na (if (number? a) a (when (string? a) (term-numeric-value a)))
        nb (if (number? b) b (when (string? b) (term-numeric-value b)))]
    (if (and na nb)
      (compare-numbers na nb)
      (compare (str a) (str b)))))

(defn- format-rdf-term-result
  "Format a MIN/MAX result. If the value is already an N-Triples term, return as-is.
   If it's a raw number (shouldn't happen with new logic), format as numeric."
  [value]
  (if (string? value)
    value
    (format-numeric-result value)))

;; Aggregate strategy registry - consolidates init/update/merge/compute/format
;; for each aggregate function into a single lookup table.
(def ^:private agg-strategies
  {:count {:init (constantly 0)
           :update (fn [state raw-value]
                     (if (some? raw-value) (inc state) state))
           :merge (fn [s1 s2] (+ (or s1 0) (or s2 0)))
           :compute identity
           :format (fn [v] (format-typed-literal v xsd-integer))}

   :sum {:init (constantly 0)
         :update (fn [state raw-value]
                   (if-let [num-val (when (string? raw-value) (term-numeric-value raw-value))]
                     (add-numbers state num-val)
                     state))
         :merge (fn [s1 s2] (add-numbers (or s1 0) (or s2 0)))
         :compute identity
         :format format-numeric-result}

   :min {:init (constantly nil)
         :update (fn [state raw-value]
                   (if (some? raw-value)
                     (if (nil? state)
                       raw-value
                       (let [cmp (compare-rdf-terms state raw-value)]
                         (if (pos? cmp) raw-value state)))
                     state))
         :merge (fn [s1 s2]
                  (cond (nil? s1) s2 (nil? s2) s1
                        :else (if (pos? (compare-rdf-terms s1 s2)) s2 s1)))
         :compute identity
         :format format-rdf-term-result}

   :max {:init (constantly nil)
         :update (fn [state raw-value]
                   (if (some? raw-value)
                     (if (nil? state)
                       raw-value
                       (let [cmp (compare-rdf-terms state raw-value)]
                         (if (neg? cmp) raw-value state)))
                     state))
         :merge (fn [s1 s2]
                  (cond (nil? s1) s2 (nil? s2) s1
                        :else (if (neg? (compare-rdf-terms s1 s2)) s2 s1)))
         :compute identity
         :format format-rdf-term-result}

   :avg {:init (constantly [0 0])
         :update (fn [state raw-value]
                   (if-let [num-val (when (string? raw-value) (term-numeric-value raw-value))]
                     [(add-numbers (first state) num-val) (inc (second state))]
                     state))
         :merge (fn [s1 s2]
                  (let [[sum1 cnt1] (or s1 [0 0])
                        [sum2 cnt2] (or s2 [0 0])]
                    [(add-numbers sum1 sum2) (+ cnt1 cnt2)]))
         :compute (fn [state]
                    (let [[sum cnt] (or state [0 0])]
                      (when (pos? cnt)
                        ;; AVG of exact inputs is exact decimal (SPARQL: AVG of
                        ;; integers is xsd:decimal); any double input → double
                        (if (instance? Double sum)
                          (/ ^double sum cnt)
                          (.divide (bigdec sum) (bigdec cnt)
                                   java.math.MathContext/DECIMAL64)))))
         :format (fn [v]
                   (if (instance? BigDecimal v)
                     (format-typed-literal (.toPlainString (.stripTrailingZeros ^BigDecimal v))
                                           xsd-decimal)
                     (format-typed-literal v xsd-decimal)))}})

(defn init-agg-state
  "Initialize state for an aggregate function.
   When distinct? is true, state is {:base <normal-state> :seen #{}}."
  ([agg-fn] (init-agg-state agg-fn false))
  ([agg-fn distinct?]
   (let [base ((get-in agg-strategies [agg-fn :init] (constantly nil)))]
     (if distinct?
       {:base base :seen #{}}
       base))))

(defn update-agg-state
  "Update aggregate state with a new value.
   When state is a distinct-tracking map, deduplicates before updating."
  [agg-fn state raw-value]
  (if-let [update-fn (get-in agg-strategies [agg-fn :update])]
    (if (map? state)
      ;; Distinct mode: only update if value not yet seen
      (if (or (nil? raw-value) (contains? (:seen state) raw-value))
        state
        (-> state
            (update :base #(update-fn % raw-value))
            (update :seen conj raw-value)))
      (update-fn state raw-value))
    state))

(defn merge-agg-states
  "Merge two aggregate states for distributed aggregation (combiner pattern).
   For distinct aggregates, merges seen-value sets and recomputes base state."
  [agg-fn s1 s2]
  (if-let [merge-fn (get-in agg-strategies [agg-fn :merge])]
    (if (and (map? s1) (map? s2))
      ;; Distinct mode: merge seen sets, recompute base from combined set
      (let [combined-seen (clojure.set/union (or (:seen s1) #{}) (or (:seen s2) #{}))
            init ((get-in agg-strategies [agg-fn :init] (constantly nil)))
            update-fn (get-in agg-strategies [agg-fn :update])
            recomputed (reduce #(update-fn %1 %2) init combined-seen)]
        {:base recomputed :seen combined-seen})
      (merge-fn s1 s2))
    s1))

(defn compute-final-agg
  "Compute final aggregate value from accumulated state."
  [agg-fn state]
  (if-let [compute-fn (get-in agg-strategies [agg-fn :compute])]
    (compute-fn (if (map? state) (:base state) state))
    nil))

(defn format-agg-result
  "Format an aggregate result as an RDF literal string."
  [agg-fn value]
  (when value
    (if-let [format-fn (get-in agg-strategies [agg-fn :format])]
      (format-fn value)
      (str value))))

(defn extract-value-for-agg
  "Extract the value to aggregate from a binding based on agg-spec.
   For COUNT(*) the value is the binding row itself, so COUNT(DISTINCT *)
   deduplicates whole rows instead of a shared constant marker.
   An expression evaluation error contributes nothing (treated as unbound)."
  [binding agg-spec]
  (let [arg (:arg agg-spec)]
    (if (nil? arg)
      binding
      (let [v (if (= :var (:type arg))
                (get binding (:name arg))
                (eval-expr arg binding))]
        (when-not (error? v) v)))))

(defn build-group-entry
  "Build a single group entry with aggregate states from one binding.
   Returns {group-key -> {agg-name -> [agg-fn, state]}}"
  [binding group-vars aggregates]
  (let [group-key (mapv #(get binding %) group-vars)
        agg-states (into {}
                         (for [{:keys [name agg]} aggregates
                               :let [agg-fn (:fn agg)
                                     distinct? (boolean (:distinct agg))
                                     raw-val (extract-value-for-agg binding agg)
                                     init-state (init-agg-state agg-fn distinct?)
                                     updated (update-agg-state agg-fn init-state raw-val)]]
                           [name [agg-fn updated]]))]
    {group-key agg-states}))

(defn finalize-group-results
  "Convert accumulated group states to final result bindings.
   SPARQL requires that aggregates without GROUP BY over empty input produce
   one row with identity values (COUNT=0, SUM=0, AVG/MIN/MAX=unbound)."
  [group-map group-vars aggregates]
  (if (and (empty? group-map) (empty? group-vars))
    ;; No group-vars and no input rows: produce one row with identity aggregate values
    (let [identity-row (into {}
                             (for [{:keys [name agg]} aggregates
                                   :let [agg-fn (:fn agg)
                                         init-state (init-agg-state agg-fn)
                                         final-val (compute-final-agg agg-fn init-state)
                                         formatted (format-agg-result agg-fn final-val)]
                                   :when formatted]
                               [name formatted]))]
      #{identity-row})
    (set
     (for [[group-key agg-states] group-map]
       (let [;; Start with group variable bindings
             base (zipmap group-vars group-key)
             ;; Add computed aggregates
             agg-bindings (into {}
                                (for [{:keys [name]} aggregates
                                      :let [[agg-fn state] (get agg-states name)
                                            final-val (compute-final-agg agg-fn state)
                                            formatted (format-agg-result agg-fn final-val)]
                                      :when formatted]
                                  [name formatted]))]
         (merge base agg-bindings))))))

;; Combiner for group aggregate maps - merges group states distributedly
(def +group-agg-combiner
  (combiner
   (fn [m1 m2]
     (merge-with
      (fn [aggs1 aggs2]
        (merge-with
         (fn [[agg-fn s1] [_ s2]]
           [agg-fn (merge-agg-states agg-fn s1 s2)])
         aggs1 aggs2))
      (or m1 {})
      (or m2 {})))
   :init-fn (fn [] {})))
