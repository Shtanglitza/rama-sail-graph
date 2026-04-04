(ns ^:no-doc rama-sail.sail.compilation
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [rama-sail.sail.serialization :refer [val->str DEFAULT-CONTEXT-VAL]]
            [rama-sail.sail.optimization :refer [is-variable? extract-plan-vars extract-expr-vars estimate-plan-cardinality optimize-plan get-plan-vars]])
  (:import [org.eclipse.rdf4j.query.algebra And ArbitraryLengthPath BindingSetAssignment Compare Compare$CompareOp Distinct EmptySet Reduced Filter LeftJoin Not Or TupleExpr StatementPattern Join Projection SingletonSet Union ValueConstant ValueExpr Var QueryRoot Slice ZeroLengthPath
            Extension ExtensionElem MathExpr MathExpr$MathOp Str Coalesce Bound FunctionCall
            Order OrderElem
            Group GroupElem Count Sum Avg Min Max
            If Regex In ListMemberOperator IsURI IsBNode IsLiteral IsNumeric Lang LangMatches Datatype SameTerm
            TripleRef ValueExprTripleRef]
           [org.eclipse.rdf4j.query BindingSet]))

;;; -----------------------------------------------------------------------------
;;; Query Planner - Plan Compilation
;;; -----------------------------------------------------------------------------

;; Common pattern helpers

(defn compute-join-vars
  "Compute the join variables between two tuple expressions.
   Returns a vector of variable names with '?' prefix.
   Excludes synthetic constant variables (those starting with '_const_')
   which RDF4J generates for shared constant values in patterns."
  [left-expr right-expr]
  (let [l-vars (set (.getBindingNames ^TupleExpr left-expr))
        r-vars (set (.getBindingNames ^TupleExpr right-expr))
        shared-vars (set/intersection l-vars r-vars)
        ;; Filter out RDF4J's synthetic constant variables
        real-vars (remove #(str/starts-with? % "_const_") shared-vars)]
    (vec (map #(str "?" %) real-vars))))

(defn binding-names->vars
  "Convert binding names to variable strings with '?' prefix."
  [binding-names]
  (vec (map #(str "?" %) binding-names)))

;; Forward declaration for value-expr->plan (used by LeftJoin for condition)
(declare value-expr->plan)

(defmulti tuple-expr->plan (fn [^TupleExpr expr] (class expr)))

(defmethod tuple-expr->plan QueryRoot [^QueryRoot expr]
  (tuple-expr->plan (.getArg expr)))

(defmethod tuple-expr->plan Slice [^Slice expr]
  {:op :slice
   :sub-plan (tuple-expr->plan (.getArg expr))
   :offset (.getOffset expr) ; Extract the offset (long)
   :limit (.getLimit expr)})   ; Extract the limit (long)

(defmethod tuple-expr->plan StatementPattern [^StatementPattern sp]
  (let [get-val (fn [^Var v]
                  (if (.hasValue v)
                    (val->str (.getValue v)) ;; Constant -> "<http://...>"
                    (str "?" (.getName v)))) ;; Variable -> "?varName"
        s-val (get-val (.getSubjectVar sp))
        p-val (get-val (.getPredicateVar sp))
        o-val (get-val (.getObjectVar sp))
        c-var (.getContextVar sp)
        ;; SPARQL semantics: no GRAPH clause = match ALL contexts (wildcard = nil)
        ;; GRAPH <uri> = match specific context
        ;; GRAPH ?var = match any context, binding the variable
        c-val (when c-var (get-val c-var))]
    {:op :bgp
     :pattern {:s s-val
               :p p-val
               :o o-val
               :c c-val}}))

(defn- rewrite-triple-ref-join
  "Rewrite Join(TripleRef, Plan) into an efficient plan.

   Two cases:
   1. All s/p/o are constants: Construct the triple term string and substitute
      it as a constant into the other plan's pattern, eliminating the join entirely.
      Example: << <alice> <knows> <bob> >> ?pred ?val
              → BGP('<< <alice> <knows> <bob> >>', ?pred, ?val)

   2. Some s/p/o are variables: Keep the other plan as-is (it returns the triple
      term string as a binding), then add bind expressions to decompose it.
      Example: << ?s ?p ?o >> <confidence> ?val
              → Bind(BGP(?_anon, <confidence>, ?val), [?s=SUBJECT(?_anon), ...])"
  [triple-ref other-plan]
  (let [{:keys [subject-var predicate-var object-var expr-var]} triple-ref
        s-const? (not (is-variable? subject-var))
        p-const? (not (is-variable? predicate-var))
        o-const? (not (is-variable? object-var))]
    (if (and s-const? p-const? o-const?)
      ;; Case 1: Construct triple term and substitute into other plan as constant
      (let [constructed (str "<< " subject-var " " predicate-var " " object-var " >>")]
        ;; Replace the expr-var in the other plan's pattern with the constructed value
        (if (and (= :bgp (:op other-plan)) (is-variable? expr-var))
          ;; Direct pattern substitution for BGP
          (let [pattern (:pattern other-plan)
                new-pattern (into {}
                                  (for [[k v] pattern]
                                    (if (= v expr-var)
                                      [k constructed]
                                      [k v])))]
            {:op :bgp :pattern new-pattern})
          ;; General case: wrap in bind that sets expr-var = constructed value
          ;; This shouldn't normally happen but handles edge cases
          other-plan))
      ;; Case 2: Decompose triple term variable into s/p/o components.
      ;; For shared variables (already in other-plan), add filter BEFORE bind.
      ;; For new variables, add bind to extract the component.
      ;; For constants, add filter to verify the component matches.
      (let [make-extract (fn [part-type]
                           {:type part-type :arg {:type :var :name expr-var}})

            ;; Detect which variables the other plan already provides
            other-vars (cond
                         (= :bgp (:op other-plan))
                         (set (filter is-variable? (vals (:pattern other-plan))))
                         :else #{})

            components [{:var subject-var :extract (make-extract :triple-subject)}
                        {:var predicate-var :extract (make-extract :triple-predicate)}
                        {:var object-var :extract (make-extract :triple-object)}]

            ;; Pre-filters: for shared variables and constants, applied BEFORE bind
            ;; This ensures we filter using the original bound value before bind overwrites
            pre-filters (vec (for [{:keys [var extract]} components
                                   :when (some? var)
                                   :when (or (not (is-variable? var))
                                             (contains? other-vars var))]
                               (if (is-variable? var)
                                 {:type :cmp :op :eq
                                  :left extract
                                  :right {:type :var :name var}}
                                 {:type :cmp :op :eq
                                  :left extract
                                  :right {:type :const :val var}})))

            ;; Bind only NEW variables (not already in other-plan)
            bindings (vec (for [{:keys [var extract]} components
                                :when (and (is-variable? var)
                                           (not (contains? other-vars var)))]
                            {:var var :expr extract}))

            combined-pre-filter (when (seq pre-filters)
                                  (reduce (fn [acc f]
                                            {:type :logic :op :and :left acc :right f})
                                          (first pre-filters)
                                          (rest pre-filters)))

            ;; Apply filter first, then bind new variables
            plan (cond-> other-plan
                   combined-pre-filter (as-> p {:op :filter :sub-plan p :expr combined-pre-filter})
                   (seq bindings) (as-> p {:op :bind :sub-plan p :bindings bindings}))]
        plan))))

(defmethod tuple-expr->plan Join [^Join j]
  (let [left-plan (tuple-expr->plan (.getLeftArg j))
        right-plan (tuple-expr->plan (.getRightArg j))
        ;; Detect TripleRef + other plan joins and rewrite.
        ;; TripleRef can't produce rows standalone — it decomposes/constructs triple term strings.
        triple-ref (cond
                     (= :triple-ref (:op left-plan)) left-plan
                     (= :triple-ref (:op right-plan)) right-plan
                     :else nil)
        other-plan (when triple-ref
                     (if (= :triple-ref (:op left-plan)) right-plan left-plan))]
    (if triple-ref
      (rewrite-triple-ref-join triple-ref other-plan)
      ;; Normal join
      (let [join-vars (compute-join-vars (.getLeftArg j) (.getRightArg j))
            left-card (estimate-plan-cardinality left-plan)
            right-card (estimate-plan-cardinality right-plan)
            should-swap? (< left-card right-card)]
        (if should-swap?
          {:op :join
           :left right-plan
           :right left-plan
           :join-vars join-vars}
          {:op :join
           :left left-plan
           :right right-plan
           :join-vars join-vars})))))

(defmethod tuple-expr->plan LeftJoin [^LeftJoin j]
  (cond-> {:op :left-join
           :left (tuple-expr->plan (.getLeftArg j))
           :right (tuple-expr->plan (.getRightArg j))
           :join-vars (compute-join-vars (.getLeftArg j) (.getRightArg j))}
    (.hasCondition j) (assoc :condition (value-expr->plan (.getCondition j)))))

(defmethod tuple-expr->plan Union [^Union u]
  {:op :union
   :left (tuple-expr->plan (.getLeftArg u))
   :right (tuple-expr->plan (.getRightArg u))})

(defmethod tuple-expr->plan Projection [^Projection p]
  {:op :project
   :vars (binding-names->vars (.getBindingNames p))
   :sub-plan (tuple-expr->plan (.getArg p))})

(defmethod tuple-expr->plan Distinct [^Distinct d]
  {:op :distinct
   :sub-plan (tuple-expr->plan (.getArg d))})

(defmethod tuple-expr->plan Reduced [^Reduced r]
  ;; Reduced is a weaker form of Distinct — treat identically
  {:op :distinct
   :sub-plan (tuple-expr->plan (.getArg r))})

(defmethod tuple-expr->plan BindingSetAssignment [^BindingSetAssignment bsa]
  (let [var-names (vec (.getBindingNames bsa))
        bindings  (vec (for [^BindingSet bs (.getBindingSets bsa)]
                         (into {}
                               (for [name var-names
                                     :let [value (.getValue bs name)]
                                     :when value]
                                 [(str "?" name) (val->str value)]))))]
    {:op :values
     :vars (mapv #(str "?" %) var-names)
     :bindings bindings}))

(defmethod tuple-expr->plan TripleRef [^TripleRef tr]
  ;; TripleRef decomposes/constructs a triple term << s p o >> ↔ exprVar.
  ;; In SPARQL-star, << ?s ?p ?o >> :annPred ?annVal is parsed as:
  ;;   Join(TripleRef(?s, ?p, ?o, ?exprVar), StatementPattern(?exprVar, :annPred, ?annVal))
  ;; At execution time, this plan decomposes ?exprVar into ?s/?p/?o (or constructs it).
  (let [get-val (fn [^Var v]
                  (if (.hasValue v)
                    (val->str (.getValue v))
                    (str "?" (.getName v))))]
    {:op :triple-ref
     :subject-var (get-val (.getSubjectVar tr))
     :predicate-var (get-val (.getPredicateVar tr))
     :object-var (get-val (.getObjectVar tr))
     :expr-var (get-val (.getExprVar tr))}))

(defmethod tuple-expr->plan SingletonSet [^SingletonSet _]
  ;; SingletonSet produces exactly one empty binding row — the identity for joins.
  ;; Used internally by RDF4J (e.g., in BIND without a preceding pattern).
  {:op :singleton})

(defmethod tuple-expr->plan EmptySet [^EmptySet _]
  ;; EmptySet produces zero rows — used for contradictions or pruned branches.
  {:op :empty})

(defmethod tuple-expr->plan ZeroLengthPath [^ZeroLengthPath zlp]
  ;; :p? path — matches zero-length (subject = object) or one step.
  ;; RDF4J represents :p? as Union(ZeroLengthPath, StatementPattern).
  ;; ZeroLengthPath alone just binds subject=object for all subjects in the store.
  (let [get-val (fn [^Var v]
                  (if (.hasValue v)
                    (val->str (.getValue v))
                    (str "?" (.getName v))))
        s-val (get-val (.getSubjectVar zlp))
        o-val (get-val (.getObjectVar zlp))]
    {:op :zero-length-path
     :subject s-val
     :object o-val}))

(defmethod tuple-expr->plan ArbitraryLengthPath [^ArbitraryLengthPath alp]
  ;; Property path :p+ (minLength=1) or :p* (minLength=0).
  ;; The path expression is typically a StatementPattern for a single predicate.
  (let [get-val (fn [^Var v]
                  (if (.hasValue v)
                    (val->str (.getValue v))
                    (str "?" (.getName v))))
        s-val (get-val (.getSubjectVar alp))
        o-val (get-val (.getObjectVar alp))
        min-length (.getMinLength alp)
        ;; Extract the predicate from the path expression (usually a StatementPattern)
        path-expr (.getPathExpression alp)
        step-plan (tuple-expr->plan path-expr)]
    {:op :arbitrary-length-path
     :subject s-val
     :object o-val
     :step-plan step-plan
     :min-length min-length}))

(defmethod tuple-expr->plan :default [expr]
  (throw (UnsupportedOperationException.
          (str "RamaSail does not yet support operator: " (.getSimpleName (class expr))))))

;;; -----------------------------------------------------------------------------
;;; Value Expression Planner
;;; -----------------------------------------------------------------------------

;; Operator mapping tables for O(1) lookup instead of O(n) condp
(def ^:private compare-ops
  {Compare$CompareOp/EQ :eq
   Compare$CompareOp/NE :ne
   Compare$CompareOp/LT :lt
   Compare$CompareOp/LE :le
   Compare$CompareOp/GT :gt
   Compare$CompareOp/GE :ge})

(def ^:private math-ops
  {MathExpr$MathOp/PLUS :plus
   MathExpr$MathOp/MINUS :minus
   MathExpr$MathOp/MULTIPLY :multiply
   MathExpr$MathOp/DIVIDE :divide})

(defmulti value-expr->plan (fn [^ValueExpr expr] (class expr)))

(defmethod value-expr->plan Var [^Var v]
  (if (.hasValue v)
    {:type :const :val (val->str (.getValue v))}
    {:type :var :name (str "?" (.getName v))}))

(defmethod value-expr->plan ValueConstant [^ValueConstant vc]
  {:type :const :val (val->str (.getValue vc))})

(defmethod value-expr->plan Compare [^Compare c]
  {:type :cmp
   :op (get compare-ops (.getOperator c))
   :left (value-expr->plan (.getLeftArg c))
   :right (value-expr->plan (.getRightArg c))})

(defmethod value-expr->plan And [^And a]
  {:type :logic :op :and
   :left (value-expr->plan (.getLeftArg a))
   :right (value-expr->plan (.getRightArg a))})

(defmethod value-expr->plan Or [^Or o]
  {:type :logic :op :or
   :left (value-expr->plan (.getLeftArg o))
   :right (value-expr->plan (.getRightArg o))})

(defmethod value-expr->plan Not [^Not n]
  {:type :logic :op :not
   :arg (value-expr->plan (.getArg n))})

;;; --- BIND Expression Support ---

(defmethod value-expr->plan MathExpr [^MathExpr m]
  {:type :math
   :op (get math-ops (.getOperator m))
   :left (value-expr->plan (.getLeftArg m))
   :right (value-expr->plan (.getRightArg m))})

(defmethod value-expr->plan Str [^Str s]
  {:type :str
   :arg (value-expr->plan (.getArg s))})

(defmethod value-expr->plan Coalesce [^Coalesce c]
  {:type :coalesce
   :args (mapv value-expr->plan (.getArguments c))})

(defmethod value-expr->plan Bound [^Bound b]
  {:type :bound
   :var (str "?" (-> b .getArg .getName))})

;;; --- Aggregate Expression Support ---

(defmethod value-expr->plan Count [^Count c]
  {:type :agg
   :fn :count
   :arg (when (.getArg c) (value-expr->plan (.getArg c)))
   :distinct (.isDistinct c)})

(defmethod value-expr->plan Sum [^Sum s]
  {:type :agg
   :fn :sum
   :arg (value-expr->plan (.getArg s))
   :distinct (.isDistinct s)})

(defmethod value-expr->plan Avg [^Avg a]
  {:type :agg
   :fn :avg
   :arg (value-expr->plan (.getArg a))
   :distinct (.isDistinct a)})

(defmethod value-expr->plan Min [^Min m]
  {:type :agg
   :fn :min
   :arg (value-expr->plan (.getArg m))
   :distinct false})  ;; DISTINCT has no effect on MIN

(defmethod value-expr->plan Max [^Max m]
  {:type :agg
   :fn :max
   :arg (value-expr->plan (.getArg m))
   :distinct false})  ;; DISTINCT has no effect on MAX

(defmethod value-expr->plan FunctionCall [^FunctionCall fc]
  {:type :function-call
   :uri (.getURI fc)
   :args (mapv value-expr->plan (.getArgs fc))})

;;; --- Phase 1 SPARQL Functions ---

(defmethod value-expr->plan If [^If expr]
  {:type :if
   :condition (value-expr->plan (.getCondition expr))
   :then (value-expr->plan (.getResult expr))
   :else (value-expr->plan (.getAlternative expr))})

(defmethod value-expr->plan IsURI [^IsURI expr]
  {:type :type-check :check :is-iri :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan IsBNode [^IsBNode expr]
  {:type :type-check :check :is-bnode :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan IsLiteral [^IsLiteral expr]
  {:type :type-check :check :is-literal :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan IsNumeric [^IsNumeric expr]
  {:type :type-check :check :is-numeric :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan Lang [^Lang expr]
  {:type :lang :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan Datatype [^Datatype expr]
  {:type :datatype :arg (value-expr->plan (.getArg expr))})

(defmethod value-expr->plan LangMatches [^LangMatches expr]
  {:type :langmatches
   :left (value-expr->plan (.getLeftArg expr))
   :right (value-expr->plan (.getRightArg expr))})

(defmethod value-expr->plan Regex [^Regex r]
  {:type :regex
   :arg (value-expr->plan (.getArg r))
   :pattern (value-expr->plan (.getPatternArg r))
   :flags (when (.getFlagsArg r) (value-expr->plan (.getFlagsArg r)))})

(defmethod value-expr->plan SameTerm [^SameTerm expr]
  {:type :same-term
   :left (value-expr->plan (.getLeftArg expr))
   :right (value-expr->plan (.getRightArg expr))})

(defmethod value-expr->plan In [^In expr]
  ;; In is a subquery-based IN operator (not list-based).
  ;; SPARQL's FILTER(?x IN (...)) is parsed as ListMemberOperator, not In.
  ;; Delegate to getArg + getSubQuery for the subquery form.
  {:type :in-subquery
   :arg (value-expr->plan (.getArg expr))
   :sub-query (.getSubQuery expr)})

(defmethod value-expr->plan ListMemberOperator [^ListMemberOperator expr]
  (let [args (.getArguments expr)]
    {:type :in
     :arg (value-expr->plan (first args))
     :set (mapv value-expr->plan (rest args))}))

;;; --- RDF-star Triple Term Support ---

(defmethod value-expr->plan ValueExprTripleRef [^ValueExprTripleRef expr]
  ;; TRIPLE(?s, ?p, ?o) constructor — builds a triple term from components
  {:type :triple-constructor
   :subject (value-expr->plan (.getSubjectVar expr))
   :predicate (value-expr->plan (.getPredicateVar expr))
   :object (value-expr->plan (.getObjectVar expr))})

;; Fallback for unhandled expressions
(defmethod value-expr->plan :default [expr]
  (throw (UnsupportedOperationException.
          (str "RamaSail: Unsupported ValueExpr: " (class expr)))))

;;; --- Additional tuple-expr->plan methods (require value-expr->plan) ---

(defmethod tuple-expr->plan Filter [^Filter f]
  {:op :filter
   :sub-plan (tuple-expr->plan (.getArg f))
   :expr (value-expr->plan (.getCondition f))})

(defmethod tuple-expr->plan Extension [^Extension ext]
  {:op :bind
   :sub-plan (tuple-expr->plan (.getArg ext))
   :bindings (vec (for [^ExtensionElem elem (.getElements ext)]
                    {:var (str "?" (.getName elem))
                     :expr (value-expr->plan (.getExpr elem))}))})

(defmethod tuple-expr->plan Order [^Order ord]
  {:op :order
   :sub-plan (tuple-expr->plan (.getArg ord))
   :order-specs (vec (for [^OrderElem elem (.getElements ord)]
                       {:expr (value-expr->plan (.getExpr elem))
                        :ascending (.isAscending elem)}))})

(defmethod tuple-expr->plan Group [^Group g]
  ;; GROUP BY with aggregates
  ;; Group bindings = the GROUP BY variables
  ;; Group elements = the aggregate expressions (COUNT, SUM, etc.)
  (let [group-vars (binding-names->vars (.getGroupBindingNames g))
        aggregates (vec (for [^GroupElem elem (.getGroupElements g)]
                          {:name (str "?" (.getName elem))
                           :agg (value-expr->plan (.getOperator elem))}))]
    {:op :group
     :sub-plan (tuple-expr->plan (.getArg g))
     :group-vars group-vars
     :aggregates aggregates}))

;;; -----------------------------------------------------------------------------
;;; Initial Binding Set Support
;;; -----------------------------------------------------------------------------

(defn- substitute-var
  "If var-str starts with '?' and that variable has a binding, return the bound value.
   Otherwise return the original var-str."
  [var-str bindings-map]
  (if (and (string? var-str) (str/starts-with? var-str "?"))
    (let [var-name (subs var-str 1)]  ;; Remove "?" prefix
      (if-let [bound-val (get bindings-map var-name)]
        bound-val
        var-str))
    var-str))

(defn- substitute-in-pattern
  "Substitute bound variables in a BGP pattern with their constant values."
  [pattern bindings-map]
  {:s (substitute-var (:s pattern) bindings-map)
   :p (substitute-var (:p pattern) bindings-map)
   :o (substitute-var (:o pattern) bindings-map)
   :c (substitute-var (:c pattern) bindings-map)})

(defn- substitute-in-expr
  "Substitute bound variables in a value expression."
  [expr bindings-map]
  (case (:type expr)
    :var (let [var-name (:name expr)
               var-key (if (str/starts-with? var-name "?")
                         (subs var-name 1)
                         var-name)]
           (if-let [bound-val (get bindings-map var-key)]
             {:type :const :val bound-val}
             expr))
    :const expr
    :cmp (assoc expr
                :left (substitute-in-expr (:left expr) bindings-map)
                :right (substitute-in-expr (:right expr) bindings-map))
    :logic (if (= :not (:op expr))
             (assoc expr :arg (substitute-in-expr (:arg expr) bindings-map))
             (assoc expr
                    :left (substitute-in-expr (:left expr) bindings-map)
                    :right (substitute-in-expr (:right expr) bindings-map)))
    :math (assoc expr
                 :left (substitute-in-expr (:left expr) bindings-map)
                 :right (substitute-in-expr (:right expr) bindings-map))
    :str (assoc expr :arg (substitute-in-expr (:arg expr) bindings-map))
    :coalesce (assoc expr :args (mapv #(substitute-in-expr % bindings-map) (:args expr)))
    :bound expr  ;; BOUND checks the variable itself, don't substitute
    :if (assoc expr
               :condition (substitute-in-expr (:condition expr) bindings-map)
               :then (substitute-in-expr (:then expr) bindings-map)
               :else (substitute-in-expr (:else expr) bindings-map))
    (:type-check :lang :datatype) (assoc expr :arg (substitute-in-expr (:arg expr) bindings-map))
    :langmatches (assoc expr
                        :left (substitute-in-expr (:left expr) bindings-map)
                        :right (substitute-in-expr (:right expr) bindings-map))
    :regex (cond-> (assoc expr
                          :arg (substitute-in-expr (:arg expr) bindings-map)
                          :pattern (substitute-in-expr (:pattern expr) bindings-map))
             (:flags expr) (assoc :flags (substitute-in-expr (:flags expr) bindings-map)))
    :in (assoc expr
               :arg (substitute-in-expr (:arg expr) bindings-map)
               :set (mapv #(substitute-in-expr % bindings-map) (:set expr)))
    :same-term (assoc expr
                      :left (substitute-in-expr (:left expr) bindings-map)
                      :right (substitute-in-expr (:right expr) bindings-map))
    :triple-constructor (assoc expr
                               :subject (substitute-in-expr (:subject expr) bindings-map)
                               :predicate (substitute-in-expr (:predicate expr) bindings-map)
                               :object (substitute-in-expr (:object expr) bindings-map))
    expr))

(declare substitute-in-plan)

(defn- remove-substituted-vars
  "Remove variables from join-vars that have been substituted with constants.
   Variables are stored with '?' prefix (e.g., '?Y'), bindings-map keys are without (e.g., 'Y')."
  [join-vars bindings-map]
  (vec (remove (fn [var-name]
                 ;; var-name is like "?Y", bindings-map keys are like "Y"
                 (let [key (if (str/starts-with? var-name "?")
                             (subs var-name 1)
                             var-name)]
                   (contains? bindings-map key)))
               join-vars)))

(defn- substitute-in-plan
  "Recursively substitute bound variables with constant values throughout a plan."
  [plan bindings-map]
  (case (:op plan)
    :bgp (assoc plan :pattern (substitute-in-pattern (:pattern plan) bindings-map))
    :values plan  ;; VALUES bindings are already constants, nothing to substitute
    :join (assoc plan
                 :left (substitute-in-plan (:left plan) bindings-map)
                 :right (substitute-in-plan (:right plan) bindings-map)
                 :join-vars (remove-substituted-vars (:join-vars plan) bindings-map))
    :left-join (cond-> (assoc plan
                              :left (substitute-in-plan (:left plan) bindings-map)
                              :right (substitute-in-plan (:right plan) bindings-map)
                              :join-vars (remove-substituted-vars (:join-vars plan) bindings-map))
                 (:condition plan) (assoc :condition (substitute-in-expr (:condition plan) bindings-map)))
    :union (assoc plan
                  :left (substitute-in-plan (:left plan) bindings-map)
                  :right (substitute-in-plan (:right plan) bindings-map))
    :filter (assoc plan
                   :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map)
                   :expr (substitute-in-expr (:expr plan) bindings-map))
    :project (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :distinct (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :slice (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :order (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :bind (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :group (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :ask (assoc plan :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map))
    :self-join (assoc plan
                      :predicate (substitute-var (:predicate plan) bindings-map)
                      :join-var (substitute-var (:join-var plan) bindings-map)
                      :left-subject (substitute-var (:left-subject plan) bindings-map)
                      :right-subject (substitute-var (:right-subject plan) bindings-map)
                      :context (substitute-var (:context plan) bindings-map))
    :type-lookup (assoc plan
                        :subject-var (substitute-var (:subject-var plan) bindings-map)
                        :type-iri (substitute-var (:type-iri plan) bindings-map)
                        :context (substitute-var (:context plan) bindings-map))
    :batch-enrich (assoc plan
                         :sub-plan (substitute-in-plan (:sub-plan plan) bindings-map)
                         :predicate (substitute-var (:predicate plan) bindings-map)
                         :subject-var (substitute-var (:subject-var plan) bindings-map)
                         :object-var (substitute-var (:object-var plan) bindings-map)
                         :context (substitute-var (:context plan) bindings-map))
    :triple-ref (assoc plan
                       :subject-var (substitute-var (:subject-var plan) bindings-map)
                       :predicate-var (substitute-var (:predicate-var plan) bindings-map)
                       :object-var (substitute-var (:object-var plan) bindings-map)
                       :expr-var (substitute-var (:expr-var plan) bindings-map))
    (:singleton :empty) plan
    :zero-length-path (assoc plan
                             :subject (substitute-var (:subject plan) bindings-map)
                             :object (substitute-var (:object plan) bindings-map))
    :arbitrary-length-path (assoc plan
                                  :subject (substitute-var (:subject plan) bindings-map)
                                  :object (substitute-var (:object plan) bindings-map)
                                  :step-plan (substitute-in-plan (:step-plan plan) bindings-map))
    plan))

(defn apply-initial-bindings
  "Substitute bound variables in a plan with their constant values.
   Used when evaluateInternal receives initial bindings that should constrain results.
   This transforms the plan so that variables with bindings become constants in BGP patterns."
  [plan ^BindingSet bindings]
  (let [bindings-map (into {}
                           (for [name (.getBindingNames bindings)
                                 :let [value (.getValue bindings name)]
                                 :when value]
                             [name (val->str value)]))]
    (if (empty? bindings-map)
      plan
      (substitute-in-plan plan bindings-map))))
