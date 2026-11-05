(ns ^:no-doc rama-sail.sail.optimization
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]))

;; Dynamic vars for statistics-based cardinality estimation
;; These are bound during query evaluation to provide actual statistics
(def ^:dynamic *predicate-stats*
  "Map of predicate string -> {:count N :distinct-subjects N :distinct-objects N}
   When bound, used by estimate-plan-cardinality for accurate estimates."
  nil)

(def ^:dynamic *global-stats*
  "Global statistics {:total-triples N :total-predicates N :total-subjects N}
   When bound, used for overall cardinality estimation."
  nil)

;; RDF type predicate IRI - used for adaptive plan selection
(def RDF-TYPE-PREDICATE "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")

(defn is-variable?
  "Check if a pattern component is a variable (starts with '?')."
  [s]
  (and (string? s) (.startsWith ^String s "?")))

;; Forward declaration for cardinality estimation (defined below)
(declare estimate-plan-cardinality)

;;; --- Variable Extraction for Filter Pushdown ---

(defn extract-expr-vars
  "Extract all variable names referenced in an expression.
   Returns a set of variable strings (e.g., #{\"?x\" \"?y\"}).

   For an unknown expression type the result contains ::unknown-vars, a
   sentinel that can never be a plan variable. Subset tests against plan
   vars then fail, so passes like filter pushdown leave the expression
   where it is instead of relocating a filter whose variables they
   under-counted."
  [expr]
  (letfn [(binary [e] (into (extract-expr-vars (:left e))
                            (extract-expr-vars (:right e))))
          (unary [e] (extract-expr-vars (:arg e)))]
    (case (:type expr)
      :var #{(:name expr)}
      :const #{}
      :cmp (binary expr)
      :logic (if (= :not (:op expr))
               (unary expr)
               (binary expr))
      :math (binary expr)
      (:str :lang :datatype :type-check :is-triple
            :triple-subject :triple-predicate :triple-object) (unary expr)
      :coalesce (reduce into #{} (map extract-expr-vars (:args expr)))
      :bound #{(:var expr)}
      :agg (if (:arg expr) (unary expr) #{})
      :if (into (extract-expr-vars (:condition expr))
                (into (extract-expr-vars (:then expr))
                      (extract-expr-vars (:else expr))))
      (:langmatches :same-term) (binary expr)
      :regex (cond-> (into (unary expr) (extract-expr-vars (:pattern expr)))
               (:flags expr) (into (extract-expr-vars (:flags expr))))
      :in (reduce into (unary expr) (map extract-expr-vars (:set expr)))
      :triple-constructor (into (extract-expr-vars (:subject expr))
                                (into (extract-expr-vars (:predicate expr))
                                      (extract-expr-vars (:object expr))))
      ;; Unknown type: poison the result so callers cannot prove safety
      #{::unknown-vars})))

;;; --- Variable scope: always-bound vs possibly-bound ---
;;;
;;; Every optimizer guard that moves or rewrites an operator must know, per
;;; plan, which variables are bound in EVERY solution (:always) and which may be
;;; bound in SOME solution (:maybe, a superset of :always). Conflating the two
;;; was the shared root cause of W7: filters pushed onto an OPTIONAL side,
;;; batch-enrich overwriting/dropping possibly-bound vars, DP hash keys on
;;; possibly-unbound vars.

(defn- vars-scope
  "Scope of a leaf that binds every listed variable in every solution."
  [terms]
  (let [vs (into #{} (filter is-variable?) terms)]
    {:always vs :maybe vs :unknown? false}))

(def ^:private empty-scope {:always #{} :maybe #{} :unknown? false})

;; Per-planning-pass memo of plan-var-scope, keyed by plan node IDENTITY.
;; Guards and the cost model ask for the same sub-plans' scopes over and over
;; (DP estimates every subset; each :join estimate needs both children's
;; vars), so without the memo the analysis is recomputed from the leaves each
;; time (measured: ~2.5x slower planning for BSBM Q2/Q5). Identity keys avoid
;; hashing whole plan trees; plans are immutable, so entries never go stale.
(def ^:dynamic ^java.util.IdentityHashMap *scope-cache* nil)

(defn with-scope-cache*
  "Call f with a plan-var-scope memo bound, reusing an enclosing one."
  [f]
  (if *scope-cache*
    (f)
    (binding [*scope-cache* (java.util.IdentityHashMap.)]
      (f))))

(defmacro with-scope-cache
  "Evaluate body with a plan-var-scope memo bound (see *scope-cache*)."
  [& body]
  `(with-scope-cache* (fn [] ~@body)))

(declare plan-var-scope)

(defn- compute-plan-var-scope
  "Return {:always #{..} :maybe #{..} :unknown? bool} for a plan.

   :always   - variables bound in every solution the plan produces
   :maybe    - variables bound in at least one possible solution (⊇ :always)
   :unknown? - the plan contains an operator this function does not model;
               callers must then refuse any rewrite that relies on :maybe
               (see `may-bind?`)."
  [plan]
  (let [sub #(plan-var-scope (:sub-plan plan))
        both (fn [l r] {:always (set/union (:always l) (:always r))
                        :maybe (set/union (:maybe l) (:maybe r))
                        :unknown? (or (:unknown? l) (:unknown? r))})]
    (case (:op plan)
      :bgp (let [{:keys [s p o c]} (:pattern plan)] (vars-scope [s p o c]))
      :join (both (plan-var-scope (:left plan)) (plan-var-scope (:right plan)))
      ;; OPTIONAL: only the base side is guaranteed
      :left-join (let [l (plan-var-scope (:left plan))
                       r (plan-var-scope (:right plan))]
                   (assoc (both l r) :always (:always l)))
      :multi-left-join (let [b (plan-var-scope (:base plan))
                             opts (map #(plan-var-scope (:plan %)) (:optionals plan))]
                         (assoc (reduce both b opts) :always (:always b)))
      ;; UNION: guaranteed only if both branches bind it; possible if either does
      :union (let [l (plan-var-scope (:left plan))
                   r (plan-var-scope (:right plan))]
               (assoc (both l r) :always (set/intersection (:always l) (:always r))))
      (:filter :distinct :slice :order :ask) (sub)
      ;; Projection bounds the scope: a projected var the sub-plan never binds
      ;; is unbound, a hidden var is out of scope.
      :project (let [vs (set (:vars plan))]
                 {:always (set/intersection vs (:always (sub)))
                  :maybe vs
                  :unknown? false})
      ;; BIND outputs may be unbound (the expression can raise a type error)
      :bind (update (sub) :maybe into (map :var (:bindings plan)))
      :group (let [gv (set (:group-vars plan))]
               {:always (set/intersection gv (:always (sub)))
                :maybe (into gv (map :name (:aggregates plan)))
                :unknown? false})
      ;; VALUES: UNDEF cells are absent from the row map -> only possibly bound
      :values (let [vs (set (:vars plan))
                    rows (:bindings plan)]
                {:always (if (seq rows)
                           (reduce set/intersection vs (map (comp set keys) rows))
                           #{})
                 :maybe vs
                 :unknown? false})
      :type-lookup (vars-scope [(:subject-var plan)])
      :self-join (vars-scope [(:left-subject plan) (:right-subject plan) (:join-var plan)])
      ;; batch-enrich has inner-join semantics: it binds object-var in every row
      :batch-enrich (let [s (sub)]
                      (-> s
                          (update :always conj (:object-var plan))
                          (update :maybe conj (:object-var plan))))
      :triple-ref (vars-scope [(:subject-var plan) (:predicate-var plan)
                               (:object-var plan) (:expr-var plan)])
      (:zero-length-path :arbitrary-length-path) (vars-scope [(:subject plan) (:object plan)])
      (:singleton :empty) empty-scope
      ;; Unmodelled operator: claim nothing, poison :maybe
      {:always #{} :maybe #{} :unknown? true})))

(defn plan-var-scope
  "Return {:always #{..} :maybe #{..} :unknown? bool} for a plan (see
   `compute-plan-var-scope`). Memoized per node within `with-scope-cache`."
  [plan]
  (if-let [^java.util.IdentityHashMap cache *scope-cache*]
    (if (.containsKey cache plan)
      (.get cache plan)
      (let [scope (compute-plan-var-scope plan)]
        (.put cache plan scope)
        scope))
    (compute-plan-var-scope plan)))

(defn always-bound-vars
  "Variables bound in every solution of `plan`."
  [plan]
  (:always (plan-var-scope plan)))

(defn may-bind?
  "True when `scope` (from `plan-var-scope`) may bind variable `v` — also true
   whenever the scope contains an unmodelled operator."
  [scope v]
  (or (:unknown? scope) (contains? (:maybe scope) v)))

;;; --- Filter Pushdown Optimization ---

(defn- filter-pushable-into?
  "A filter over Join(S, Other) may move onto S only if it evaluates the same on
   S's solutions as on the joined ones: every filter var must be either always
   bound in S (the joined value equals S's) or never bound by Other (the joined
   binding state equals S's). A var only possibly bound in S but bound by Other
   breaks this — e.g. FILTER(?x != ?n) pushed onto an OPTIONAL side that may
   leave ?x unbound while the other side binds it."
  [filter-vars s-scope other-scope]
  (and (not (:unknown? s-scope))
       (every? #(or (contains? (:always s-scope) %)
                    (not (may-bind? other-scope %)))
               filter-vars)))

(defn- push-filter-into-join
  "Try to push a filter into a join's children.
   Returns {:pushed-left plan, :pushed-right plan, :remaining-filters [exprs]}.

   Pushes onto exactly one side: the side whose variables cover the filter and
   for which the move is semantics-preserving (`filter-pushable-into?`). A
   filter both sides could take, or that spans both, stays above the join."
  [filter-expr join-plan]
  (let [left (:left join-plan)
        right (:right join-plan)
        left-scope (plan-var-scope left)
        right-scope (plan-var-scope right)
        filter-vars (extract-expr-vars filter-expr)
        covers? (fn [scope] (set/subset? filter-vars (:maybe scope)))]
    (cond
      ;; Filter can be pushed to left side only
      (and (covers? left-scope)
           (not (covers? right-scope))
           (filter-pushable-into? filter-vars left-scope right-scope))
      {:pushed-left {:op :filter :sub-plan left :expr filter-expr}
       :pushed-right right
       :remaining-filters []}

      ;; Filter can be pushed to right side only
      (and (covers? right-scope)
           (not (covers? left-scope))
           (filter-pushable-into? filter-vars right-scope left-scope))
      {:pushed-left left
       :pushed-right {:op :filter :sub-plan right :expr filter-expr}
       :remaining-filters []}

      ;; Filter spans both sides, or moving it would change its meaning
      :else
      {:pushed-left left
       :pushed-right right
       :remaining-filters [filter-expr]})))

(declare push-filters-down)

(defn- optimize-join-with-filters
  "Optimize a join by pushing applicable filters down."
  [join-plan filters-to-push]
  (let [;; Recursively optimize children first
        optimized-left (push-filters-down (:left join-plan))
        optimized-right (push-filters-down (:right join-plan))
        ;; Try to push each filter down
        result (reduce
                (fn [{:keys [left right remaining]} filter-expr]
                  (let [{:keys [pushed-left pushed-right remaining-filters]}
                        (push-filter-into-join filter-expr {:left left :right right})]
                    {:left pushed-left
                     :right pushed-right
                     :remaining (into remaining remaining-filters)}))
                {:left optimized-left
                 :right optimized-right
                 :remaining []}
                filters-to-push)
        ;; Re-estimate cardinality and potentially swap
        new-left (:left result)
        new-right (:right result)
        left-card (estimate-plan-cardinality new-left)
        right-card (estimate-plan-cardinality new-right)
        ;; Swap if beneficial (smaller side should be hash table = right)
        [final-left final-right] (if (< left-card right-card)
                                   [new-right new-left]
                                   [new-left new-right])
        new-join {:op :join
                  :left final-left
                  :right final-right
                  :join-vars (:join-vars join-plan)}]
    ;; Wrap with remaining filters that couldn't be pushed
    (reduce (fn [plan expr]
              {:op :filter :sub-plan plan :expr expr})
            new-join
            (:remaining result))))

(defn- decompose-and-filters
  "Decompose an AND expression into independent conjuncts.
   (AND (AND a b) c) -> [a b c]
   Non-AND expressions return as single-element vector."
  [expr]
  (if (and (= :logic (:type expr)) (= :and (:op expr)))
    (into (decompose-and-filters (:left expr))
          (decompose-and-filters (:right expr)))
    [expr]))

(defn push-filters-down
  "Push filter expressions down into joins and sub-plans where possible.
   Decomposes AND expressions into independent conjuncts that can be
   pushed separately, giving each join side the most selective filters."
  [plan]
  (case (:op plan)
    :filter
    (let [sub-plan (:sub-plan plan)
          expr (:expr plan)
          ;; Decompose AND expressions for finer-grained pushdown
          conjuncts (decompose-and-filters expr)]
      (case (:op sub-plan)
        ;; Filter over Join - try to push each conjunct independently
        :join
        (optimize-join-with-filters sub-plan conjuncts)

        ;; Filter over Filter - collect and push both
        :filter
        (let [inner-conjuncts (decompose-and-filters (:expr sub-plan))
              all-conjuncts (into conjuncts inner-conjuncts)
              inner-sub (:sub-plan sub-plan)]
          (if (= :join (:op inner-sub))
            (optimize-join-with-filters inner-sub all-conjuncts)
            ;; Otherwise just optimize the inner plan
            (let [optimized-inner (push-filters-down inner-sub)]
              (reduce (fn [p e] {:op :filter :sub-plan p :expr e})
                      optimized-inner
                      all-conjuncts))))

        ;; Default: optimize sub-plan
        {:op :filter
         :sub-plan (push-filters-down sub-plan)
         :expr expr}))

    :join
    ;; No filter to push, just optimize children
    (let [optimized-left (push-filters-down (:left plan))
          optimized-right (push-filters-down (:right plan))
          left-card (estimate-plan-cardinality optimized-left)
          right-card (estimate-plan-cardinality optimized-right)
          ;; Re-check ordering after child optimization
          [final-left final-right] (if (< left-card right-card)
                                     [optimized-right optimized-left]
                                     [optimized-left optimized-right])]
      {:op :join
       :left final-left
       :right final-right
       :join-vars (:join-vars plan)})

    :left-join
    ;; Can't swap sides for left join, but can optimize children
    {:op :left-join
     :left (push-filters-down (:left plan))
     :right (push-filters-down (:right plan))
     :join-vars (:join-vars plan)
     :condition (:condition plan)}

    :union
    {:op :union
     :left (push-filters-down (:left plan))
     :right (push-filters-down (:right plan))}

    :project
    {:op :project
     :sub-plan (push-filters-down (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (push-filters-down (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (push-filters-down (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :order
    {:op :order
     :sub-plan (push-filters-down (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (push-filters-down (:sub-plan plan))
     :bindings (:bindings plan)}

    :group
    {:op :group
     :sub-plan (push-filters-down (:sub-plan plan))
     :group-vars (:group-vars plan)
     :aggregates (:aggregates plan)}

    :ask
    {:op :ask
     :sub-plan (push-filters-down (:sub-plan plan))}

    :multi-left-join
    {:op :multi-left-join
     :base (push-filters-down (:base plan))
     :optionals (mapv (fn [opt] (update opt :plan push-filters-down))
                      (:optionals plan))}

    ;; BGP and other leaf nodes - no children to optimize
    plan))

;;; --- Self-Join Detection and Optimization ---
;;; Critical for QJ4-style queries that join same predicate on itself.
;;; Instead of N×K Cartesian product followed by filter, we:
;;; 1. Group subjects by their common object (join key)
;;; 2. Generate pairs directly within each group
;;; 3. Apply inequality filter during generation

(defn- is-self-join-candidate?
  "Check if a join is a self-join candidate:
   - Both sides are BGPs
   - Same predicate (bound constant)
   - Same object variable (join variable)
   - Different subject variables"
  [join-plan]
  (let [left (:left join-plan)
        right (:right join-plan)]
    (when (and (= :bgp (:op left))
               (= :bgp (:op right)))
      (let [left-pattern (:pattern left)
            right-pattern (:pattern right)
            left-p (:p left-pattern)
            right-p (:p right-pattern)
            left-o (:o left-pattern)
            right-o (:o right-pattern)
            left-s (:s left-pattern)
            right-s (:s right-pattern)]
        ;; Same predicate (bound), same object (variable), different subjects (variables),
        ;; and same context
        (and (not (is-variable? left-p))
             (= left-p right-p)
             (is-variable? left-o)
             (is-variable? right-o)
             (= left-o right-o)  ;; Join on object
             (is-variable? left-s)
             (is-variable? right-s)
             (not= left-s right-s)
             ;; Context must match and must not be a variable: the :self-join
             ;; node only carries the two subjects and the join var, so a
             ;; GRAPH ?g binding would silently vanish from the output.
             (= (:c left-pattern) (:c right-pattern))
             (not (is-variable? (:c left-pattern))))))))

(defn- find-self-join-in-plan
  "Recursively find a self-join node in a plan and return [path, self-join-node].
   Path is a vector of keys to reach the self-join from root.

   Descends only through operators a conjunctive filter commutes with:
   :join (inner), :filter, :project, :distinct, :order, :bind. It must NOT
   descend through :union (the filter would be dropped for the other branch),
   :left-join (moving a filter into or below OPTIONAL changes its scope), or
   :slice (filter-after-limit is not filter-before-limit).

   A :project is crossed only when it exposes every filter variable: a var it
   projects away is a DIFFERENT (unbound) variable outside the subquery, so
   `{ {SELECT ?o {?a :p ?o . ?b :p ?o}} FILTER(?a != ?b) }` must not fold the
   outer filter into the inner self-join."
  [plan path filter-vars]
  (when plan
    (let [down #(find-self-join-in-plan (% plan) (conj path %) filter-vars)]
      (case (:op plan)
        :self-join [path plan]
        :join (or (down :left) (down :right))
        (:filter :distinct :order :bind) (down :sub-plan)
        :project (when (set/subset? filter-vars (set (:vars plan)))
                   (down :sub-plan))
        nil))))

(defn- self-join-operands
  "For a comparison usable by the self-join rewrite, [left-var right-var str?]:
   both operands bare variables (?a < ?b), or both STR of a variable
   (STR(?a) < STR(?b) — the strict-SPARQL way to order IRIs). Else nil."
  [expr]
  (when (and (= :cmp (:type expr)) (#{:lt :le :gt :ge :ne} (:op expr)))
    (let [{l :left r :right} expr
          str-var? (fn [o] (and (= :str (:type o)) (= :var (get-in o [:arg :type]))))]
      (cond
        (and (= :var (:type l)) (= :var (:type r))) [(:name l) (:name r) false]
        (and (str-var? l) (str-var? r)) [(get-in l [:arg :name]) (get-in r [:arg :name]) true]
        :else nil))))

(defn- self-join-filter-spec
  "The self-join :filter for expr when it compares exactly the two subjects
   (in either order), else nil. :str? marks STR() operands."
  [expr subject-a subject-b]
  (when-let [[left-var right-var str?] (self-join-operands expr)]
    (when (or (and (= left-var subject-a) (= right-var subject-b))
              (and (= left-var subject-b) (= right-var subject-a)))
      (cond-> {:op (:op expr) :left left-var :right right-var}
        str? (assoc :str? true)))))

(defn- is-self-join-inequality?
  "Check if an expression is an inequality filter that applies to self-join subjects."
  [expr self-join]
  (some? (self-join-filter-spec expr (:left-subject self-join) (:right-subject self-join))))

(defn- incorporate-filter-into-self-join
  "If a filter is an inequality on self-join subjects, incorporate it into the self-join."
  [plan]
  (when (= :filter (:op plan))
    (let [expr (:expr plan)
          [path self-join] (find-self-join-in-plan (:sub-plan plan) []
                                                   (extract-expr-vars expr))]
      (when (and self-join
                 (nil? (:filter self-join))  ;; Self-join doesn't already have a filter
                 (is-self-join-inequality? expr self-join))
        ;; Update the self-join with the filter
        (let [updated-self-join (assoc self-join :filter
                                       (self-join-filter-spec expr (:left-subject self-join)
                                                              (:right-subject self-join)))
              ;; Update the plan tree to include the modified self-join
              updated-sub-plan (assoc-in (:sub-plan plan) path updated-self-join)]
          ;; Return the sub-plan without the filter (it's now in the self-join)
          updated-sub-plan)))))

(defn- extract-inequality-filter
  "Check if an expression is an inequality filter between two variables
   (bare or both wrapped in STR). Returns {:op :left :right [:str? true]} or nil."
  [expr var1 var2]
  (self-join-filter-spec expr var1 var2))

(defn- detect-self-join-with-filter
  "Detect a filter over a self-join pattern.
   Returns a self-join plan or nil if not applicable."
  [filter-plan]
  (let [expr (:expr filter-plan)
        sub-plan (:sub-plan filter-plan)]
    (when (= :join (:op sub-plan))
      (when (is-self-join-candidate? sub-plan)
        (let [left-pattern (get-in sub-plan [:left :pattern])
              right-pattern (get-in sub-plan [:right :pattern])
              left-s (:s left-pattern)
              right-s (:s right-pattern)
              predicate (:p left-pattern)
              join-var (:o left-pattern)  ;; The object is the join variable
              inequality (extract-inequality-filter expr left-s right-s)]
          (when inequality
            {:op :self-join
             :predicate predicate
             :join-var join-var
             :left-subject left-s
             :right-subject right-s
             :filter inequality
             ;; Include context if specified
             :context (:c left-pattern)}))))))

(defn transform-self-join
  "Transform a detected self-join pattern into optimized plan."
  [plan]
  (case (:op plan)
    :filter
    (or (detect-self-join-with-filter plan)
        ;; Check if we can incorporate this filter into a self-join deeper in the tree
        (let [transformed-sub (transform-self-join (:sub-plan plan))
              filter-plan {:op :filter :sub-plan transformed-sub :expr (:expr plan)}]
          (or (incorporate-filter-into-self-join filter-plan)
              filter-plan)))

    :join
    ;; Plain self-join without inequality filter is still beneficial
    (if (is-self-join-candidate? plan)
      (let [left-pattern (get-in plan [:left :pattern])
            right-pattern (get-in plan [:right :pattern])]
        {:op :self-join
         :predicate (:p left-pattern)
         :join-var (:o left-pattern)
         :left-subject (:s left-pattern)
         :right-subject (:s right-pattern)
         :filter nil  ;; No inequality filter
         :context (:c left-pattern)})
      ;; Not a self-join, optimize children
      {:op :join
       :left (transform-self-join (:left plan))
       :right (transform-self-join (:right plan))
       :join-vars (:join-vars plan)})

    :left-join
    {:op :left-join
     :left (transform-self-join (:left plan))
     :right (transform-self-join (:right plan))
     :join-vars (:join-vars plan)
     :condition (:condition plan)}

    :union
    {:op :union
     :left (transform-self-join (:left plan))
     :right (transform-self-join (:right plan))}

    :project
    {:op :project
     :sub-plan (transform-self-join (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (transform-self-join (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (transform-self-join (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :order
    {:op :order
     :sub-plan (transform-self-join (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (transform-self-join (:sub-plan plan))
     :bindings (:bindings plan)}

    :group
    {:op :group
     :sub-plan (transform-self-join (:sub-plan plan))
     :group-vars (:group-vars plan)
     :aggregates (:aggregates plan)}

    :ask
    {:op :ask
     :sub-plan (transform-self-join (:sub-plan plan))}

    :multi-left-join
    {:op :multi-left-join
     :base (transform-self-join (:base plan))
     :optionals (mapv (fn [opt] (update opt :plan transform-self-join))
                      (:optionals plan))}

    ;; Leaf nodes and unknown ops - return unchanged
    plan))

(defn use-type-view
  "Transform rdf:type BGP patterns to use materialized type views.

   Patterns like: ?x rdf:type <SomeType> (subject variable, type constant)
   become: {:op :type-lookup :type-iri \"<SomeType>\" :subject-var \"?x\"}

   This leverages the pre-computed $$type-subjects index for faster type lookups.
   Only applies when:
   - Predicate is exactly rdf:type
   - Object (the type) is a constant (not a variable)
   - Subject is a variable

   Other patterns (constant subject, variable type) continue using standard BGP."
  [plan]
  (case (:op plan)
    :bgp
    (let [pattern (:pattern plan)
          p (:p pattern)
          s (:s pattern)
          o (:o pattern)]
      (if (and (= p RDF-TYPE-PREDICATE)
               (is-variable? s)
               (not (is-variable? o))
               ;; Only optimize if context is unbound (nil).
               ;; $$type-subjects doesn't track context, so any context constraint
               ;; (including variable context for GRAPH ?g) requires standard BGP.
               (nil? (:c pattern)))
        ;; Transform to type-lookup using materialized view
        {:op :type-lookup
         :type-iri o
         :subject-var s
         :context (:c pattern)}
        ;; Not a type pattern we can optimize (or context is constrained)
        plan))

    :join
    {:op :join
     :left (use-type-view (:left plan))
     :right (use-type-view (:right plan))
     :join-vars (:join-vars plan)}

    :left-join
    {:op :left-join
     :left (use-type-view (:left plan))
     :right (use-type-view (:right plan))
     :join-vars (:join-vars plan)
     :condition (:condition plan)}

    :union
    {:op :union
     :left (use-type-view (:left plan))
     :right (use-type-view (:right plan))}

    :filter
    {:op :filter
     :sub-plan (use-type-view (:sub-plan plan))
     :expr (:expr plan)}

    :project
    {:op :project
     :sub-plan (use-type-view (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (use-type-view (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (use-type-view (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :order
    {:op :order
     :sub-plan (use-type-view (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (use-type-view (:sub-plan plan))
     :bindings (:bindings plan)}

    :ask
    {:op :ask
     :sub-plan (use-type-view (:sub-plan plan))}

    :self-join
    plan  ;; Self-join already optimized

    :multi-left-join
    {:op :multi-left-join
     :base (use-type-view (:base plan))
     :optionals (mapv (fn [opt] (update opt :plan use-type-view))
                      (:optionals plan))}

    ;; Default: return unchanged
    plan))

;;; --- Batch Enrich Optimization ---
;;; Transforms join(results, property-lookup-bgp) into batch-enrich.
;;; Instead of N individual lookups, fetches all values in one batch query.

(defn- is-property-lookup-bgp?
  "Check if a BGP is a simple property lookup:
   - Subject is a variable
   - Predicate is a constant
   - Object is a variable
   Returns the subject variable if match, nil otherwise."
  [plan]
  (when (= :bgp (:op plan))
    (let [pattern (:pattern plan)
          s (:s pattern)
          p (:p pattern)
          o (:o pattern)]
      ;; CRITICAL: Context must NOT be a variable - batch-lookup cannot filter by context.
      ;; Variable context patterns must use standard join to preserve GRAPH ?g semantics.
      (when (and (is-variable? s)
                 (not (is-variable? p))
                 (is-variable? o)
                 (not (is-variable? (:c pattern))))
        s))))

(defn- transform-to-batch-enrich
  "Transform a join with property lookup into batch-enrich.
   Pattern: join(sub-plan, bgp(?var pred ?result))
   Becomes: batch-enrich(sub-plan, pred, ?var, ?result)

   Decided from the sub-plan's variable scope, not from :join-vars (which the
   DP planner and the compiler compute differently):
   - ?var must be bound in EVERY sub-plan solution: batch-enrich looks values
     up by ?var and drops rows where it is unbound, whereas the join would
     pair such a row with every lookup row (SPARQL compatibility).
   - ?result must NOT be bound by ANY sub-plan solution: the merge assoc's the
     looked-up value, silently overwriting (instead of equality-checking) an
     existing binding, e.g. one coming from an OPTIONAL."
  [join-plan]
  (let [left (:left join-plan)
        right (:right join-plan)]
    (when-let [lookup-var (is-property-lookup-bgp? right)]
      (let [left-scope (plan-var-scope left)
            object-var (get-in right [:pattern :o])]
        (when (and (not (:unknown? left-scope))
                   (contains? (:always left-scope) lookup-var)
                   (not (may-bind? left-scope object-var))
                   ;; ?var p ?var: object and lookup var coincide; keep the join
                   (not= lookup-var object-var))
          {:op :batch-enrich
           :sub-plan left
           :predicate (get-in right [:pattern :p])
           :subject-var lookup-var
           :object-var object-var
           :context (get-in right [:pattern :c])})))))

(defn apply-batch-enrich
  "Apply batch-enrich transformation to joins where applicable.
   Recursively processes the plan tree."
  [plan]
  (case (:op plan)
    :join
    (let [;; First, recursively optimize children
          optimized-left (apply-batch-enrich (:left plan))
          optimized-right (apply-batch-enrich (:right plan))
          updated-plan (assoc plan :left optimized-left :right optimized-right)]
      ;; Then try to transform this join to batch-enrich
      (or (transform-to-batch-enrich updated-plan)
          updated-plan))

    :left-join
    {:op :left-join
     :left (apply-batch-enrich (:left plan))
     :right (apply-batch-enrich (:right plan))
     :join-vars (:join-vars plan)
     :condition (:condition plan)}

    :union
    {:op :union
     :left (apply-batch-enrich (:left plan))
     :right (apply-batch-enrich (:right plan))}

    :project
    {:op :project
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (apply-batch-enrich (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :filter
    {:op :filter
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :expr (:expr plan)}

    :order
    {:op :order
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :bindings (:bindings plan)}

    :group
    {:op :group
     :sub-plan (apply-batch-enrich (:sub-plan plan))
     :group-vars (:group-vars plan)
     :aggregates (:aggregates plan)}

    :ask
    {:op :ask
     :sub-plan (apply-batch-enrich (:sub-plan plan))}

    :multi-left-join
    {:op :multi-left-join
     :base (apply-batch-enrich (:base plan))
     :optionals (mapv (fn [opt] (update opt :plan apply-batch-enrich))
                      (:optionals plan))}

    ;; Leaf nodes (bgp, self-join, type-lookup, batch-enrich) - return unchanged
    plan))

(defn count-ops
  "Count occurrences of specific operators in a plan tree."
  [plan op-type]
  (if-not (map? plan)
    0
    (+ (if (= (:op plan) op-type) 1 0)
       (count-ops (:sub-plan plan) op-type)
       (count-ops (:left plan) op-type)
       (count-ops (:right plan) op-type)
       (count-ops (:base plan) op-type)
       (reduce + 0 (map #(count-ops (:plan %) op-type) (or (:optionals plan) []))))))

;;; ---------------------------------------------------------------------------
;;; Multi-Way Join Chain Optimization
;;; ---------------------------------------------------------------------------
;;; For join chains like ((A ⋈ B) ⋈ C) ⋈ D, find optimal ordering using
;;; dynamic programming to minimize estimated intermediate result sizes.
;;;
;;; The algorithm:
;;; 1. Detect left-deep chains of inner joins (3+ relations)
;;; 2. Extract base plans being joined
;;; 3. Use DP to find optimal order: Best[S] = min over splits of S
;;; 4. Reconstruct plan with optimal bushy tree

(defn- is-inner-join?
  "Check if a plan is an inner join (not left-join, union, etc.)."
  [plan]
  (= :join (:op plan)))

(defn- extract-join-chain
  "Extract a left-deep chain of inner joins into a vector of base plans.
   Returns vector of plans if chain has 3+ relations, nil otherwise.

   Example: ((A ⋈ B) ⋈ C) ⋈ D -> [A, B, C, D]"
  [plan]
  (when (is-inner-join? plan)
    (loop [current plan
           base-plans []]
      (if (is-inner-join? current)
        ;; Continue down the left side, collect right side
        (recur (:left current)
               (conj base-plans (:right current)))
        ;; Reached a non-join node - this is the leftmost base plan
        (let [all-plans (conj base-plans current)]
          ;; Only optimize chains of 3+ joins (4+ relations)
          (when (>= (count all-plans) 3)
            (vec (reverse all-plans))))))))

(defn get-plan-vars
  "Variables a plan MAY bind (possibly-bound semantics, the :maybe view of
   `plan-var-scope`). Used for connectivity and cost heuristics, where a
   variable bound in only some rows still indicates a related relation. Never
   use it for hash-join keys or rewrite guards — see `always-bound-vars`."
  [plan]
  (:maybe (plan-var-scope plan)))

(defn- compute-join-vars-for-plans
  "Hash-join key variables between two plans: those bound in EVERY solution of
   both sides (the compiler's getAssuredBindingNames semantics). A possibly-
   bound shared var must not be a key — rows lacking it are compatible with
   every row of the other side; the executor's compatibility check still
   enforces equality on all shared vars."
  [left-plan right-plan]
  (vec (set/intersection (always-bound-vars left-plan)
                         (always-bound-vars right-plan))))

(defn- build-join-node
  "Build a join node between two plans with computed join variables."
  [left right]
  (let [join-vars (compute-join-vars-for-plans left right)
        ;; Order: smaller on right (hash table side)
        left-card (estimate-plan-cardinality left)
        right-card (estimate-plan-cardinality right)]
    (if (< left-card right-card)
      {:op :join :left right :right left :join-vars join-vars}
      {:op :join :left left :right right :join-vars join-vars})))

(def ^:private dp-join-relation-limit
  "Above this many relations, subset-enumeration DP (Θ(3^n) splits) is
   replaced by greedy ordering: at n=12 the DP explores ~500k splits in
   milliseconds, but every additional relation triples the work and the
   planner runs on the caller's thread."
  12)

(defn- greedy-join-order
  "Greedy left-deep join ordering for chains too large for DP.
   Starts from the lowest-cardinality relation and repeatedly joins the
   relation yielding the smallest estimated intermediate result, preferring
   connected relations (shared join vars) to avoid cross products."
  [base-plans]
  (let [plans (vec base-plans)
        start (apply min-key #(estimate-plan-cardinality (plans %))
                     (range (count plans)))]
    (loop [current (plans start)
           remaining (disj (set (range (count plans))) start)]
      (if (empty? remaining)
        current
        (let [current-vars (get-plan-vars current)
              connected (filter #(seq (set/intersection
                                       current-vars (get-plan-vars (plans %))))
                                remaining)
              candidates (if (seq connected) connected remaining)
              best-i (apply min-key
                            #(estimate-plan-cardinality
                              (build-join-node current (plans %)))
                            candidates)]
          (recur (build-join-node current (plans best-i))
                 (disj remaining best-i)))))))

(defn- dp-optimal-join-order
  "Use dynamic programming to find optimal join order for a set of relations.
   Returns the optimal plan tree.

   Algorithm: Standard DP for join ordering (Selinger-style)
   - For each subset S of relations, find the best way to join them
   - Best[S] = min over all proper subsets S1, S2 where S1 ∪ S2 = S
               of cost(Best[S1]) + cost(Best[S2]) + cost(join result)

   Falls back to greedy-join-order above dp-join-relation-limit relations."
  [base-plans]
  (let [n (count base-plans)]
    (cond
      (> n dp-join-relation-limit)
      (do (log/debug "Join chain of" n "relations exceeds DP limit"
                     dp-join-relation-limit "- using greedy ordering")
          (greedy-join-order base-plans))

      (<= n 2)
      ;; For 2 or fewer plans, just do simple ordering
      (if (= n 2)
        (build-join-node (first base-plans) (second base-plans))
        (first base-plans))

      :else

      ;; DP for 3+ plans
      ;; Key: bit mask representing which plans are included
      ;; Value: {:plan <best-plan> :cost <cumulative-cost>}
      (let [plans-vec (vec base-plans)

            ;; Initialize with single plans (subsets of size 1)
            initial-memo
            (into {}
                  (for [i (range n)]
                    [(bit-shift-left 1 i)
                     {:plan (plans-vec i)
                      :cost (estimate-plan-cardinality (plans-vec i))}]))

            ;; Generate all non-empty proper subsets of a mask
            subsets-of
            (fn [mask]
              (loop [sub (dec mask) result []]
                (if (zero? sub)
                  result
                  (recur (bit-and (dec sub) mask)
                         (if (and (pos? sub) (< sub mask))
                           (conj result sub)
                           result)))))

            ;; DP: build up from smaller sets to larger
            final-memo
            (reduce
             (fn [memo size]
               (reduce
                (fn [memo mask]
                  (if-not (= (Long/bitCount mask) size)
                    memo
                    ;; Try all ways to split this subset into two non-empty parts
                    (let [subs (subsets-of mask)
                          best
                          (reduce
                           (fn [best sub1]
                             (let [sub2 (bit-and-not mask sub1)]
                               (if (or (zero? sub2)
                                       ;; Avoid counting same split twice (sub1, sub2) and (sub2, sub1)
                                       (< sub2 sub1))
                                 best
                                 (let [left-info (get memo sub1)
                                       right-info (get memo sub2)]
                                   (if (and left-info right-info)
                                     (let [joined (build-join-node (:plan left-info)
                                                                   (:plan right-info))
                                           join-cost (estimate-plan-cardinality joined)
                                           total-cost (+ (:cost left-info)
                                                         (:cost right-info)
                                                         join-cost)]
                                       (if (or (nil? best) (< total-cost (:cost best)))
                                         {:plan joined :cost total-cost}
                                         best))
                                     best)))))
                           nil
                           subs)]
                      (if best
                        (assoc memo mask best)
                        memo))))
                memo
                (range 1 (bit-shift-left 1 n))))
             initial-memo
             (range 2 (inc n)))

            ;; Get the optimal plan for all relations
            all-mask (dec (bit-shift-left 1 n))]
        (:plan (get final-memo all-mask))))))

(defn optimize-join-chains
  "Optimize a plan by finding and reordering multi-way join chains.
   Recursively processes the plan tree."
  [plan]
  (case (:op plan)
    :join
    (if-let [chain (extract-join-chain plan)]
      ;; Found a chain of 3+ base plans - optimize with DP
      (let [;; First recursively optimize each base plan
            optimized-bases (mapv optimize-join-chains chain)]
        (dp-optimal-join-order optimized-bases))
      ;; Not a chain (only 2 relations), just optimize children
      (let [left (optimize-join-chains (:left plan))
            right (optimize-join-chains (:right plan))
            ;; Re-check ordering after optimizing children
            left-card (estimate-plan-cardinality left)
            right-card (estimate-plan-cardinality right)]
        (if (< left-card right-card)
          {:op :join :left right :right left :join-vars (:join-vars plan)}
          {:op :join :left left :right right :join-vars (:join-vars plan)})))

    :left-join
    {:op :left-join
     :left (optimize-join-chains (:left plan))
     :right (optimize-join-chains (:right plan))
     :join-vars (:join-vars plan)
     :condition (:condition plan)}

    :union
    {:op :union
     :left (optimize-join-chains (:left plan))
     :right (optimize-join-chains (:right plan))}

    :filter
    {:op :filter
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :expr (:expr plan)}

    :project
    {:op :project
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (optimize-join-chains (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :order
    {:op :order
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :bindings (:bindings plan)}

    :group
    {:op :group
     :sub-plan (optimize-join-chains (:sub-plan plan))
     :group-vars (:group-vars plan)
     :aggregates (:aggregates plan)}

    :ask
    {:op :ask
     :sub-plan (optimize-join-chains (:sub-plan plan))}

    :multi-left-join
    {:op :multi-left-join
     :base (optimize-join-chains (:base plan))
     :optionals (mapv (fn [opt] (update opt :plan optimize-join-chains))
                      (:optionals plan))}

    ;; Leaf nodes (bgp, self-join, type-lookup, batch-enrich) - return unchanged
    plan))

;;; --- Multi-Left-Join Flattening ---
;;; Converts nested left-join chains into a single :multi-left-join node.
;;; This avoids re-executing the shared base plan at each nesting level.

(defn- collect-left-join-chain
  "Walk down nested :left-join nodes sharing the same base.
   Returns [base-plan [{:plan p :join-vars jv :condition c} ...]]
   or nil if not a chain of 2+."
  [plan]
  (when (= :left-join (:op plan))
    (loop [current plan
           optionals []]
      (if (= :left-join (:op current))
        (recur (:left current)
               (conj optionals {:plan (:right current)
                                :join-vars (:join-vars current)
                                :condition (:condition current)}))
        ;; Reached the base - need 2+ optionals to be worth flattening
        (when (>= (count optionals) 2)
          [current (vec (reverse optionals))])))))

(defn flatten-left-join-chains
  "Tree-walk that converts 2+ nested left-joins into :multi-left-join."
  [plan]
  (case (:op plan)
    :left-join
    (if-let [[base optionals] (collect-left-join-chain plan)]
      {:op :multi-left-join
       :base (flatten-left-join-chains base)
       :optionals (mapv (fn [opt]
                          (update opt :plan flatten-left-join-chains))
                        optionals)}
      ;; Only 1 left-join, just recurse
      {:op :left-join
       :left (flatten-left-join-chains (:left plan))
       :right (flatten-left-join-chains (:right plan))
       :join-vars (:join-vars plan)
       :condition (:condition plan)})

    :join
    {:op :join
     :left (flatten-left-join-chains (:left plan))
     :right (flatten-left-join-chains (:right plan))
     :join-vars (:join-vars plan)}

    :union
    {:op :union
     :left (flatten-left-join-chains (:left plan))
     :right (flatten-left-join-chains (:right plan))}

    :filter
    {:op :filter
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :expr (:expr plan)}

    :project
    {:op :project
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :vars (:vars plan)}

    :distinct
    {:op :distinct
     :sub-plan (flatten-left-join-chains (:sub-plan plan))}

    :slice
    {:op :slice
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :offset (:offset plan)
     :limit (:limit plan)}

    :order
    {:op :order
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :order-specs (:order-specs plan)}

    :bind
    {:op :bind
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :bindings (:bindings plan)}

    :group
    {:op :group
     :sub-plan (flatten-left-join-chains (:sub-plan plan))
     :group-vars (:group-vars plan)
     :aggregates (:aggregates plan)}

    :ask
    {:op :ask
     :sub-plan (flatten-left-join-chains (:sub-plan plan))}

    ;; Leaf nodes - return unchanged
    plan))

;;; ---------------------------------------------------------------------------
;;; LIMIT Pushdown Optimization
;;; ---------------------------------------------------------------------------
;;; When a :slice (LIMIT) wraps a target operator through cardinality-
;;; preserving operators (:project, :bind), propagate :result-limit into the
;;; target so it can stop early. Only semantics-preserving pushdowns are
;;; performed; the :slice above still enforces the exact offset/limit.

(defn- find-effective-limit
  "Walk down from a :slice through transparent operators to find the effective limit.
   Returns the limit value or nil if no LIMIT found."
  [plan]
  (when (= :slice (:op plan))
    (let [limit (:limit plan)]
      (when (and limit (pos? limit))
        limit))))

(defn bgp-limit-safe?
  "True when annotating this :bgp with :result-limit cannot change results.

   find-bgp truncates quads BEFORE building bindings and deduplicating
   (+set-agg), so the limit is sound only when distinct quads are guaranteed
   to produce distinct bindings:
   - the context position must be a constant or a variable; when it is nil
     (no GRAPH clause) quads differing only by context collapse into one
     binding, so truncating quads can under-produce results
   - no variable may repeat across positions; repeated variables collapse
     distinct quads into the same binding"
  [plan]
  (let [{:keys [s p o c]} (:pattern plan)
        vars (filterv is-variable? [s p o c])]
    (and (some? c)
         (or (< (count vars) 2)
             (apply distinct? vars)))))

(defn push-limit-down
  "Push LIMIT hints into operators that can terminate early without changing
   results. Walks the plan tree and, when a :slice with a positive limit wraps
   only cardinality-preserving operators (:project, :bind), annotates the
   target operator with :result-limit.

   Semantics-preserving targets:
   - :bgp — only when `bgp-limit-safe?` holds (context bound or projected,
     no repeated variables)
   - :self-join — the executor truncates final output pairs after applying
     the pair filter, so any N of them satisfy LIMIT-without-ORDER

   Pushdown STOPS at operators where truncating input can change results:
   - :join — the executor applies :result-limit to the join's LEFT INPUT
     before probing; unmatched left rows would silently shrink the output
   - :filter / :distinct — a truncated input can under-produce qualifying
     rows; the former x4 / x2 multiplier heuristics were unsound and removed
   - :order — ORDER BY needs all input rows to produce the correct top-N;
     a distributed top-N combiner is future work (see
     docs/rama-performance-opportunities.md, Priority 2)"
  [plan]
  (letfn [(push-limit [plan limit]
            ;; Push limit to a semantics-preserving target operator
            (case (:op plan)
              :bgp
              (if (bgp-limit-safe? plan)
                (assoc plan :result-limit limit)
                plan)

              :self-join
              (assoc plan :result-limit limit)

              ;; Transparent operators (1:1 cardinality): push through unchanged
              :project
              (update plan :sub-plan #(push-limit % limit))

              :bind
              (update plan :sub-plan #(push-limit % limit))

              ;; For all other operators (:join, :filter, :distinct, :order, ...)
              ;; truncation is not semantics-preserving: stop pushing but keep
              ;; scanning the subtree for nested slices.
              (push-limit-down plan)))

          (push-limit-down [plan]
            (case (:op plan)
              :slice
              (let [limit (find-effective-limit plan)]
                (if limit
                  ;; Push the limit into sub-plan, accounting for offset
                  (let [offset (max 0 (or (:offset plan) 0))
                        effective-limit (+ limit offset)]
                    (update plan :sub-plan #(push-limit % effective-limit)))
                  ;; No limit to push, just recurse
                  (update plan :sub-plan push-limit-down)))

              :join
              (-> plan
                  (update :left push-limit-down)
                  (update :right push-limit-down))

              :left-join
              (-> plan
                  (update :left push-limit-down)
                  (update :right push-limit-down))

              :union
              (-> plan
                  (update :left push-limit-down)
                  (update :right push-limit-down))

              :filter
              (update plan :sub-plan push-limit-down)

              :project
              (update plan :sub-plan push-limit-down)

              :distinct
              (update plan :sub-plan push-limit-down)

              :order
              (update plan :sub-plan push-limit-down)

              :bind
              (update plan :sub-plan push-limit-down)

              :group
              (update plan :sub-plan push-limit-down)

              :ask
              (update plan :sub-plan push-limit-down)

              :multi-left-join
              (-> plan
                  (update :base push-limit-down)
                  (update :optionals (fn [opts] (mapv #(update % :plan push-limit-down) opts))))

              :batch-enrich
              (update plan :sub-plan push-limit-down)

              ;; Leaf nodes - return unchanged
              plan))]
    (push-limit-down plan)))

(defn optimize-plan
  "Apply all plan optimizations:
   1. Push filters down into joins
   2. Optimize multi-way join chains using DP (Selinger-style)
   3. Detect and transform self-join patterns
   4. Use materialized type views for rdf:type patterns (adaptive)
   5. Transform property-lookup joins to batch-enrich
   6. Push LIMIT into joins and self-joins for early termination"
  [plan]
  (let [optimized (with-scope-cache
                    (-> plan
                      push-filters-down
                      flatten-left-join-chains
                      optimize-join-chains
                      transform-self-join
                      use-type-view
                      apply-batch-enrich
                      push-limit-down))]
    (when (log/enabled? :debug)
      (let [self-joins-before (count-ops plan :self-join)
            self-joins-after (count-ops optimized :self-join)
            type-lookups (count-ops optimized :type-lookup)
            batch-enriches (count-ops optimized :batch-enrich)]
        (when (or (pos? self-joins-after) (pos? type-lookups) (pos? batch-enriches))
          (log/debug (str "Plan optimizations applied:"
                          " self-joins: " self-joins-before "->" self-joins-after
                          " type-lookups: " type-lookups
                          " batch-enriches: " batch-enriches)))))
    optimized))

;; Maximum cardinality to prevent overflow in join estimation
;; 10^12 is safely within Long range and represents a very large result set
(def ^:const MAX-CARDINALITY 1000000000000)

(defn- safe-multiply-cardinalities
  "Multiply cardinalities with overflow protection and capping at
   MAX-CARDINALITY. Returns a long.

   The running product stays a double throughout: coercing it back to a long
   between steps made the next `*` a checked long multiplication, which threw
   ArithmeticException (long overflow) once two capped estimates (1e12 x 1e12)
   met — crashing query planning instead of capping. That happened e.g. for
   BSBM QJ1/QJ3 planned without statistics."
  [& nums]
  (long (reduce (fn [^double acc n]
                  (min (* acc (double n)) (double MAX-CARDINALITY)))
                1.0
                nums)))

(defn- equi-join-selectivity
  "Calculate join selectivity for equi-joins on shared variables.
   For foreign-key joins, result size ≈ max(left, right).
   For many-to-many joins, result size grows but is bounded."
  [left-card right-card num-join-vars]
  (if (zero? num-join-vars)
    1.0  ;; Cross product
    ;; For equi-joins: selectivity = 1 / max(left, right)
    ;; This models foreign-key relationships where join doesn't explode
    ;; With multiple join vars, selectivity is more selective
    (let [max-card (max left-card right-card)
          base-selectivity (/ 1.0 (max 1.0 (double max-card)))]
      ;; Multiple join vars are more selective
      (if (> num-join-vars 1)
        (* base-selectivity (Math/pow 0.5 (dec num-join-vars)))
        base-selectivity))))

(defn estimate-filter-selectivity
  "Estimate selectivity of a filter expression based on operator type.
   Returns a fraction between 0.0 and 1.0.
   More informed than a flat 0.3 — uses operator semantics:
   - Equality (=): very selective (0.05)
   - Inequality (!=): passes most rows (0.8)
   - Range (<, >, <=, >=): moderately selective (0.3)
   - AND: product of children selectivities
   - OR: sum minus product (union of independent events)
   - NOT: 1 - child selectivity
   - BOUND/ISIRI/etc: passes most rows (0.8)
   - REGEX: low selectivity (0.1)"
  [expr]
  (case (:type expr)
    :cmp (case (:op expr)
           :eq 0.05
           :ne 0.8
           (:lt :le :gt :ge) 0.3
           0.3)
    :logic (case (:op expr)
             :and (let [left-sel (estimate-filter-selectivity (:left expr))
                        right-sel (estimate-filter-selectivity (:right expr))]
                    (* left-sel right-sel))
             :or (let [left-sel (estimate-filter-selectivity (:left expr))
                       right-sel (estimate-filter-selectivity (:right expr))]
                   (min 1.0 (- (+ left-sel right-sel) (* left-sel right-sel))))
             :not (- 1.0 (estimate-filter-selectivity (:arg expr)))
             0.3)
    :regex 0.1
    :bound 0.8
    :func-call (case (:name expr)
                 ("isIRI" "isURI" "isBlank" "isLiteral" "isNumeric") 0.8
                 ("sameTerm") 0.05
                 ("langMatches") 0.3
                 0.3)
    ;; Default
    0.3))

(defn estimate-plan-cardinality
  "Estimate the cardinality of a query plan for join ordering decisions.
   Returns a relative estimate (not exact counts).

   When *predicate-stats* is bound, uses actual statistics for predicates.
   Otherwise falls back to heuristics:
   - BGP with S+P+O bound: 1 (exact match)
   - BGP with S+P bound: 10 (few objects per subject-predicate)
   - BGP with P+O bound: 100 (predicate-object can match many subjects)
   - BGP with only S bound: 100 (all predicates for a subject)
   - BGP with only P bound: 10000 (many subjects with this predicate)
   - BGP with nothing bound: 1000000 (full scan)
   - Join: left × right × selectivity (using distinct counts when available)
   - Filter: sub-plan × 0.3 selectivity
   - Other: sub-plan cardinality unchanged"
  [plan]
  (case (:op plan)
    :bgp
    (let [pattern (:pattern plan)
          s (:s pattern)
          p (:p pattern)
          o (:o pattern)
          s-bound? (not (is-variable? s))
          p-bound? (not (is-variable? p))
          o-bound? (not (is-variable? o))
          ;; Try to get statistics for this predicate
          pred-stats (when (and p-bound? *predicate-stats*)
                       (get *predicate-stats* p))
          pred-count (or (:count pred-stats) 10000)
          distinct-subj (max 1 (or (:distinct-subjects pred-stats) (max 1 (quot pred-count 10))))
          distinct-obj (max 1 (or (:distinct-objects pred-stats) (max 1 (quot pred-count 10))))]
      (cond
        ;; All three bound - exact match
        (and s-bound? p-bound? o-bound?) 1
        ;; Subject + Predicate bound - estimate based on distinct objects per subject
        (and s-bound? p-bound?)
        (if pred-stats
          (max 1 (quot pred-count distinct-subj))  ; avg objects per subject
          10)
        ;; Subject + Object bound - rare combination
        (and s-bound? o-bound?) 5
        ;; Predicate + Object bound - estimate based on distinct subjects per object
        (and p-bound? o-bound?)
        (if pred-stats
          (max 1 (quot pred-count distinct-obj))  ; avg subjects per object
          100)
        ;; Only Subject bound - all predicates for subject
        s-bound? 100
        ;; Only Predicate bound - use actual count from statistics
        p-bound?
        (if pred-stats pred-count 10000)
        ;; Only Object bound - reverse lookup
        o-bound? 1000
        ;; Nothing bound - use global stats or fallback
        :else (or (:total-triples *global-stats*) 1000000)))

    :join
    (let [left-card (estimate-plan-cardinality (:left plan))
          right-card (estimate-plan-cardinality (:right plan))
          ;; Selectivity counts every shared var, incl. possibly-bound ones:
          ;; :join-vars holds only always-bound hash keys, and counting just
          ;; those would make joins on optional vars look like cross products.
          num-join-vars (max (count (:join-vars plan))
                             (count (set/intersection (get-plan-vars (:left plan))
                                                      (get-plan-vars (:right plan)))))
          selectivity (equi-join-selectivity left-card right-card num-join-vars)]
      (safe-multiply-cardinalities left-card right-card selectivity))

    :left-join
    ;; Left join preserves at least left cardinality
    (let [left-card (estimate-plan-cardinality (:left plan))
          right-card (estimate-plan-cardinality (:right plan))
          num-join-vars (count (:join-vars plan))
          selectivity (equi-join-selectivity left-card right-card num-join-vars)]
      (max left-card
           (safe-multiply-cardinalities left-card right-card selectivity)))

    :union
    (+ (estimate-plan-cardinality (:left plan))
       (estimate-plan-cardinality (:right plan)))

    :filter
    (let [sub-card (estimate-plan-cardinality (:sub-plan plan))
          expr (:expr plan)
          selectivity (estimate-filter-selectivity expr)]
      (max 1 (long (* sub-card selectivity))))

    :project
    (estimate-plan-cardinality (:sub-plan plan))

    :distinct
    ;; Assume 80% are distinct
    (long (* (estimate-plan-cardinality (:sub-plan plan)) 0.8))

    :slice
    (let [sub-card (estimate-plan-cardinality (:sub-plan plan))
          limit (:limit plan)]
      (if (and limit (pos? limit))
        (min sub-card limit)
        sub-card))

    :bind
    (estimate-plan-cardinality (:sub-plan plan))

    :order
    (estimate-plan-cardinality (:sub-plan plan))

    :group
    ;; Grouping typically reduces cardinality significantly
    (long (* (estimate-plan-cardinality (:sub-plan plan)) 0.1))

    :values
    ;; VALUES clause has known cardinality - exactly the number of binding sets
    (count (:bindings plan))

    :self-join
    ;; Self-join groups subjects by object, then generates pairs
    ;; Estimate based on predicate selectivity with pair generation factor
    1000

    :type-lookup
    ;; Type lookup uses materialized view - estimate based on rdf:type statistics
    (if-let [type-stats (and *predicate-stats* (get *predicate-stats* RDF-TYPE-PREDICATE))]
      ;; Use actual count of type triples divided by estimated types
      (let [type-count (:count type-stats)]
        (max 1 (quot type-count 10)))  ;; Assume ~10 types on average
      100)  ;; Default estimate

    :batch-enrich
    ;; Batch-enrich has same cardinality as sub-plan (just adds a column)
    (estimate-plan-cardinality (:sub-plan plan))

    :multi-left-join
    ;; Multi-left-join preserves at least base cardinality (optionals don't reduce rows)
    (estimate-plan-cardinality (:base plan))

    :triple-ref
    ;; TripleRef is always joined with a StatementPattern; on its own it's unbounded.
    ;; Use a high estimate to ensure the StatementPattern side drives the join.
    (or (:total-triples *global-stats*) 100000)

    :singleton
    1  ;; Exactly one empty row

    :empty
    0  ;; No rows

    :zero-length-path
    ;; Subject=object identity bindings — bounded by number of distinct subjects
    (or (:total-subjects *global-stats*) 1000)

    :arbitrary-length-path
    ;; Transitive closure can be large — use a high estimate
    (let [step-card (estimate-plan-cardinality (:step-plan plan))]
      (* step-card 10))

    ;; Default: return a moderate estimate
    1000))
