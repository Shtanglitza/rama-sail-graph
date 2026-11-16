(ns rama-sail.sail.triple-refs
  "Rewrites the triple-term patterns of a query so that both the native
   compiler and RDF4J's own evaluation answer them correctly.

   RDF4J parses `:carol :claims << ?a :knows :bob >>` into a TripleRef, which
   builds or takes apart the triple term in an anonymous variable ?t, joined
   with the pattern `:carol :claims ?t`. The TripleRef is placed BEFORE that
   pattern and joined with whatever precedes it in the group, so its partner
   is not its neighbour. RDF4J itself evaluates a TripleRef as reification
   (`?t rdf:type rdf:Statement`, ...) unless the store enumerates triple
   terms, so the fallback strategy cannot evaluate one either.

   For each group of joined patterns, on a copy of the query:
   - a term whose parts are all constants becomes a constant in the pattern
     that uses it (an index lookup), innermost first for nested terms;
   - any other term is attached to a group member that always binds ?t, as
     filters and binds: isTriple(?t), sameTerm(SUBJECT(?t), ...) for constants
     and variables that member binds, and SUBJECT(?t) etc. for new variables;
   - a term no member binds stays a TripleRef, which neither the compiler nor
     the fallback evaluates (see `unpaired?`)."
  (:import (java.util ArrayList LinkedHashSet)
           (org.eclipse.rdf4j.model IRI Resource Value)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.model.vocabulary RDF)
           (org.eclipse.rdf4j.query.algebra And Bound Extension ExtensionElem Filter FunctionCall If Join
                                            QueryModelNode QueryRoot SameTerm TripleRef TupleExpr
                                            ValueConstant ValueExpr Var)
           (org.eclipse.rdf4j.query.algebra.helpers AbstractQueryModelVisitor)))

(def ^:private VF (SimpleValueFactory/getInstance))

(def is-triple-uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#isTriple")
(def subject-uri (str RDF/SUBJECT))
(def predicate-uri (str RDF/PREDICATE))
(def object-uri (str RDF/OBJECT))

(defn- collect
  "Nodes under `root` (inclusive) that satisfy `pred`, in visiting order."
  [^QueryModelNode root pred]
  (let [found (ArrayList.)]
    (.visit root (proxy [AbstractQueryModelVisitor] []
                   (meetNode [^QueryModelNode node]
                     (when (pred node) (.add found node))
                     (.visitChildren node this))))
    (vec found)))

(defn- triple-ref? [node] (instance? TripleRef node))

(defn unpaired?
  "True when `tuple-expr` still contains a TripleRef after `pair-triple-refs`."
  [^TupleExpr tuple-expr]
  (boolean (seq (collect tuple-expr triple-ref?))))

(defn- leaves
  "The members of a group of joined patterns."
  [^TupleExpr e]
  (if (instance? Join e)
    (into (leaves (.getLeftArg ^Join e)) (leaves (.getRightArg ^Join e)))
    [e]))

(defn- group-root? [node]
  (and (instance? Join node) (not (instance? Join (.getParentNode ^QueryModelNode node)))))

(defn- parts [^TripleRef tr] [(.getSubjectVar tr) (.getPredicateVar tr) (.getObjectVar tr)])

(defn- constant-triple
  "The triple term of `tr` when all its parts are constants, else nil."
  [^TripleRef tr]
  (let [[s p o] (map #(when (.hasValue ^Var %) (.getValue ^Var %)) (parts tr))]
    (when (and (instance? Resource s) (instance? IRI p) (some? o))
      (.createTriple ^SimpleValueFactory VF ^Resource s ^IRI p ^Value o))))

(defn- remove-node [nodes node] (vec (remove #(identical? node %) nodes)))

(defn- fold-constants
  "Replaces each all-constant term's variable with the term itself in the
   other members, and drops the term, until no all-constant term is left (an
   inner term becoming constant makes its outer term constant)."
  [members]
  (if-let [[tr t] (first (for [m members :when (triple-ref? m)
                               :let [t (constant-triple m)] :when t]
                           [m t]))]
    (let [ext (.getName (.getExprVar ^TripleRef tr))
          others (remove-node members tr)]
      (doseq [m others
              ^Var v (collect m #(and (instance? Var %) (= ext (.getName ^Var %))))]
        ;; a `_const_` name, as RDF4J names constants: compute-join-vars
        ;; must not take a constant used by two patterns for a join key
        (.replaceWith v (Var. (str "_const_" ext) t true true)))
      (recur others))
    members))

(defn- call [uri ^Var ext]
  (FunctionCall. ^String uri ^"[Lorg.eclipse.rdf4j.query.algebra.ValueExpr;"
                 (into-array ValueExpr [(.clone ext)])))

(defn- pairable?
  "`member` always binds the term's variable, and none of the term's
   variables is only sometimes bound by it."
  [^TupleExpr member ^TripleRef tr]
  (let [always (set (.getAssuredBindingNames member))
        sometimes (set (.getBindingNames member))]
    (and (contains? always (.getName (.getExprVar tr)))
         (not-any? #(let [n (.getName ^Var %)]
                      (and (not (.hasValue ^Var %)) (contains? sometimes n) (not (contains? always n))))
                   (parts tr)))))

(defn- parts-extension
  "An Extension binding `elems` (named `names`) on `arg`, reporting them as
   always bound: `arg` only yields triple terms, whose parts always exist.
   A plain Extension does not count its bindings as assured, which would
   leave the parts out of the join keys of an enclosing join."
  [^TupleExpr arg elems names]
  (proxy [Extension] [arg ^"[Lorg.eclipse.rdf4j.query.algebra.ExtensionElem;" (into-array ExtensionElem elems)]
    (getAssuredBindingNames []
      (doto (LinkedHashSet. (.getAssuredBindingNames (.getArg ^Extension this)))
        (.addAll names)))))

(defn- pair
  "`member` with the term `tr` attached: filtered to triple terms that match
   the constants and the variables `member` binds, then extended with the
   term's other variables and checked against them. RDF4J evaluates a join
   by passing the left side's bindings into the right side, and an Extension
   would overwrite such a binding, so a part is bound only where its
   variable is still unbound, and checked in every case (this also checks a
   variable repeated in the term)."
  [^TupleExpr member ^TripleRef tr]
  (let [always (set (.getAssuredBindingNames member))
        ext (.getExprVar tr)
        {:keys [guards elems added checks]}
        (reduce (fn [acc [^Var v uri]]
                  (let [f (call uri ext)
                        n (.getName v)]
                    (cond
                      (.hasValue v) (update acc :guards conj (SameTerm. f (ValueConstant. (.getValue v))))
                      (contains? always n) (update acc :guards conj (SameTerm. f (.clone v)))
                      (contains? (:added acc) n) (update acc :checks conj (SameTerm. f (.clone v)))
                      :else (-> acc
                                (update :elems conj (ExtensionElem. (If. (Bound. (.clone v)) (.clone v) f) n))
                                (update :added conj n)
                                (update :checks conj (SameTerm. (call uri ext) (.clone v)))))))
                {:guards [(call is-triple-uri ext)] :elems [] :added #{} :checks []}
                (map vector (parts tr) [subject-uri predicate-uri object-uri]))
        all-of (fn [conds] (reduce #(And. %1 %2) conds))
        guarded (Filter. member ^ValueExpr (all-of guards))
        extended (if (seq elems) (parts-extension guarded elems added) guarded)]
    (if (seq checks)
      (Filter. ^TupleExpr extended ^ValueExpr (all-of checks))
      extended)))

(defn- rewrite-group!
  [^Join root]
  (let [members (leaves root)]
    (when (some triple-ref? members)
      (let [members (fold-constants members)
            [units pending]
            (loop [units (vec (remove triple-ref? members))
                   pending (filterv triple-ref? members)]
              (if-let [[tr i] (first (for [tr pending
                                           [i u] (map-indexed vector units)
                                           :when (pairable? u tr)]
                                       [tr i]))]
                (recur (assoc units i (pair (nth units i) tr)) (remove-node pending tr))
                [units pending]))]
        (.replaceWith root ^TupleExpr (reduce #(Join. %1 %2) (into units pending)))))))

(defn pair-triple-refs
  "`tuple-expr` with its triple-term patterns rewritten (see the namespace
   docstring), on a copy; `tuple-expr` itself when it has none."
  ^TupleExpr [^TupleExpr tuple-expr]
  (if-not (unpaired? tuple-expr)
    tuple-expr
    (let [copy ^TupleExpr (.clone tuple-expr)
          holder (if (instance? QueryRoot copy) copy (QueryRoot. copy))]
      (doseq [j (collect holder group-root?)]
        (rewrite-group! j))
      (if (identical? holder copy)
        copy
        (doto (.getArg ^QueryRoot holder) (.setParentNode nil))))))
