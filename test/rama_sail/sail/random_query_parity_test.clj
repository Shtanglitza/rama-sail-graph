(ns rama-sail.sail.random-query-parity-test
  "Randomized testing of the Rama query engine.

   A seeded generator builds small SPARQL SELECT queries from the constructs
   where variables may be unbound — OPTIONAL (with and without a condition),
   UNION, FILTER, BIND, VALUES with UNDEF, projecting subqueries, GRAPH — over
   a tiny dense dataset. Each query is run as

     raw        the unoptimized Rama plan (tuple-expr->plan)
     opt        the optimized plan, no statistics
     opt+stats  the optimized plan with the module's real statistics (the
                production path: same bindings and scope memo as the adapter)

   and two properties are checked:

   1. PLANS AGREE (every query): raw, opt and opt+stats return the same bag.
      Needs no oracle; this is the property optimizer bugs break.
   2. MATCHES RDF4J (queries in the RDF4J-safe fragment): opt returns the same
      bag as RDF4J's MemoryStore.

   RDF4J 5.2.2 itself deviates from SPARQL's bottom-up semantics on several
   shapes (it evaluates nested groups with outer bindings injected, treats
   BIND/subquery variables as always bound, pushes FILTERs into VALUES with
   UNDEF, ...). Those were confirmed against Apache Jena's reference engine
   (a literal implementation of the SPARQL algebra), which agreed with Rama
   on every one. `rdf4j-violations` recognises them syntactically so that
   check 2 only runs where RDF4J is trustworthy; test-rdf4j-deviations pins
   down one example of each (and fails once RDF4J fixes it, so the
   classifier can be relaxed).

   A failure is shrunk (drop elements, inline OPTIONAL/UNION/subquery
   bodies, simplify filters) to a minimal valid query before reporting.

   Reproducible: java.util.Random with a fixed seed (no test.check in this
   project). Override with RANDOM_QUERY_SEED / RANDOM_QUERY_COUNT, e.g.
     RANDOM_QUERY_COUNT=5000 lein test :only rama-sail.sail.random-query-parity-test

   Divergences the generator avoids by construction:
   - ordering comparisons (< <= > >=) between two variables (Rama orders IRIs
     as an extension; see memorystore-parity-test);
   - literals of different datatypes compared with = / != (Rama raises a type
     error, RDFterm-equal; RDF4J and Jena return false): the dataset and the
     constants use one literal datatype, xsd:integer;
   - LIMIT/OFFSET without a total order (nondeterministic);
   - the same triple stored in several graphs (test-triple-in-several-graphs-divergence);
   - a BIND whose expression can fail, joined on its variable: failable BINDs
     bind ?x/?y, which no triple pattern uses (constant BINDs may bind any
     variable)."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.module.queries]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.compilation :as compilation]
            [rama-sail.sail.optimization :as opt]
            [rama-sail.sail.serialization :as ser])
  (:import (java.io StringReader)
           (java.util Random)
           (org.eclipse.rdf4j.model Model Resource ValueFactory)
           (org.eclipse.rdf4j.query Binding)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.model.vocabulary XSD)
           (org.eclipse.rdf4j.query.parser.sparql SPARQLParser)
           (org.eclipse.rdf4j.repository.sail SailRepository)
           (org.eclipse.rdf4j.rio Rio RDFFormat)
           (org.eclipse.rdf4j.sail.memory MemoryStore)))

;;; ---------------------------------------------------------------------------
;;; Dataset: dense and small, so random joins produce rows and cross products
;;; stay small. Gaps are deliberate (n5/n6 have no :r, b1 no :s, ...) so
;;; OPTIONAL leaves variables unbound.
;;; ---------------------------------------------------------------------------

(def ^:private EX "http://ex/")
(def ^:private XSD-INT "http://www.w3.org/2001/XMLSchema#integer")

(def ^:private data
  "Default-graph N-Triples (named-graph statements: see `named-quads`)."
  (str/join
   "\n"
   (concat
    (for [[s o] [["n1" "n2"] ["n2" "n3"] ["n3" "n1"] ["n1" "n1"] ["n4" "n2"] ["n5" "n6"]]]
      (format "<%s%s> <%sp> <%s%s> ." EX s EX EX o))
    ["_:b1 <http://ex/p> <http://ex/n3> ."
     "_:b2 <http://ex/p> <http://ex/n4> ."
     "_:b1 <http://ex/q> _:b2 ."]
    (for [[s o] [["n2" "n4"] ["n3" "n4"] ["n4" "n5"] ["n6" "n1"] ["n1" "n3"]]]
      (format "<%s%s> <%sq> <%s%s> ." EX s EX EX o))
    (for [[s n] [["n1" 1] ["n2" 2] ["n3" 2] ["n4" 3] ["n1" 4]]]
      (format "<%s%s> <%sr> \"%d\"^^<%s> ." EX s EX n XSD-INT))
    ["_:b2 <http://ex/r> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> ."]
    (for [[s n] [["n1" 10] ["n2" 20] ["n3" 10] ["n6" 20] ["n4" 30]]]
      (format "<%s%s> <%ss> \"%d\"^^<%s> ." EX s EX n XSD-INT))
    [""])))

(def ^:private named-quads
  "[s p o graph] local names; o is an IRI local name or an integer. No triple
   is stored in two graphs (see test-triple-in-several-graphs-divergence)."
  [["n2" "p" "n5" "g1"]
   ["n5" "q" "n1" "g1"]
   ["n2" "r" 3 "g2"]
   ["n6" "s" 10 "g2"]])

;;; ---------------------------------------------------------------------------
;;; Query AST + generator
;;;
;;; group   = {:els [el ...]}
;;; el      = [:triple s p o] | [:optional group cond-expr?] | [:union group group]
;;;         | [:filter expr] | [:bind expr var] | [:values [var ...] [[term|nil ...] ...]]
;;;         | [:subquery distinct? [var ...] group] | [:graph term-or-var group]
;;; query   = {:distinct? bool :group group}
;;; ---------------------------------------------------------------------------

(def ^:private VARS
  "Variables used by triple patterns and VALUES (i.e. the join keys)."
  ["?a" "?b" "?c" "?d" "?e"])
(def ^:private BIND-VARS
  "Targets of BINDs whose expression can fail (never join keys; see ns doc)."
  ["?x" "?y"])
(def ^:private EXPR-VARS (into VARS BIND-VARS))
(def ^:private NODES (mapv #(str "<" EX "n" % ">") (range 1 7)))
(def ^:private LINK-PREDS ["<http://ex/p>" "<http://ex/q>"])
(def ^:private PREDS (into LINK-PREDS ["<http://ex/r>" "<http://ex/s>"]))
(def ^:private LITERALS
  "Integer literals only: see the ns doc on comparing literals of different
   datatypes."
  ["1" "2" "3" "10" "20"])
(def ^:private GRAPHS ["<http://ex/g1>" "<http://ex/g2>"])

(defn- pick [^Random rnd coll] (nth coll (.nextInt rnd (count coll))))
(defn- chance? [^Random rnd p] (< (.nextDouble rnd) p))

(defn- weighted [^Random rnd pairs]
  (let [total (reduce + (map second pairs))
        x (* total (.nextDouble rnd))]
    (loop [[[v w] & more] pairs acc 0.0]
      (if (or (empty? more) (< x (+ acc w))) v (recur more (+ acc w))))))

(defn- gen-term [rnd] (if (chance? rnd 0.6) (pick rnd NODES) (pick rnd LITERALS)))

(defn- gen-triple [rnd]
  (let [p (if (chance? rnd 0.08) "?pp" (pick rnd PREDS))
        s (if (chance? rnd 0.12) (pick rnd NODES) (pick rnd VARS))
        o (cond (chance? rnd 0.75) (pick rnd VARS)
                (contains? (set LINK-PREDS) p) (pick rnd NODES)
                :else (pick rnd LITERALS))]
    [:triple s p o]))

(defn- gen-expr [^Random rnd depth]
  (let [v #(pick rnd EXPR-VARS)]
    (if (and (pos? depth) (chance? rnd 0.3))
      (case (int (.nextInt rnd 3))
        0 [:and (gen-expr rnd (dec depth)) (gen-expr rnd (dec depth))]
        1 [:or (gen-expr rnd (dec depth)) (gen-expr rnd (dec depth))]
        2 [:not (gen-expr rnd (dec depth))])
      (weighted rnd [[[:ne (v) (v)] 3] [[:eq (v) (v)] 2] [[:eq (v) (gen-term rnd)] 2]
                     [[:bound (v)] 2] [[:lt (v) (pick rnd ["1" "2" "3"])] 1.5]
                     [[:gt (v) (pick rnd ["1" "2" "3"])] 1.5]
                     [[:sameterm (v) (v)] 1] [[:isiri (v)] 1] [[:isliteral (v)] 1]]))))

(defn- gen-failable-bind-expr
  "A BIND expression that errors when its variable is unbound (or, for +1,
   not numeric) — so its target may stay unbound."
  [^Random rnd]
  (let [v (pick rnd EXPR-VARS)]
    (weighted rnd [[[:var v] 3] [[:plus1 v] 1.5] [[:coalesce v (gen-term rnd)] 1.5]])))

(declare in-scope-vars)

(defn- el-scope
  "Variables an element puts in scope (SPARQL 18.2.1): FILTER binds nothing,
   a subquery only its projection."
  [[t & args]]
  (case t
    :triple (set (filter #(str/starts-with? % "?") args))
    :optional (in-scope-vars (first args))
    :union (into (in-scope-vars (first args)) (in-scope-vars (second args)))
    :filter #{}
    :bind #{(second args)}
    :values (set (first args))
    :subquery (set (second args))
    :graph (cond-> (in-scope-vars (second args))
             (str/starts-with? (first args) "?") (conj (first args)))))

(defn- in-scope-vars [group] (reduce into #{} (map el-scope (:els group))))

(declare gen-group)

(defn- gen-el [^Random rnd depth scope]
  (let [kind (weighted rnd (cond-> [[:triple 5] [:filter 2] [:bind 1] [:values 0.8]]
                             (pos? depth) (into [[:optional 2.5] [:union 1.5]
                                                 [:subquery 1] [:graph 0.6]])))]
    (case kind
      :triple (gen-triple rnd)
      :filter [:filter (gen-expr rnd 2)]
      :bind (let [constant? (chance? rnd 0.35)
                  free (seq (remove scope (if constant? VARS BIND-VARS)))]
              (cond (not free) (gen-triple rnd)
                    constant? [:bind [:const (gen-term rnd)] (pick rnd (vec free))]
                    :else [:bind (gen-failable-bind-expr rnd) (pick rnd (vec free))]))
      :values (let [vs (vec (distinct (repeatedly (inc (.nextInt rnd 2)) #(pick rnd VARS))))]
                [:values vs (vec (repeatedly (inc (.nextInt rnd 3))
                                             (fn [] (mapv (fn [_] (when-not (chance? rnd 0.3) (gen-term rnd))) vs))))])
      :optional [:optional (gen-group rnd (dec depth))
                 (when (chance? rnd 0.25) (gen-expr rnd 1))]
      :union [:union (gen-group rnd (dec depth)) (gen-group rnd (dec depth))]
      :subquery (let [g (gen-group rnd (dec depth))
                      avail (vec (in-scope-vars g))
                      proj (if (seq avail)
                             (vec (distinct (repeatedly (inc (.nextInt rnd (count avail))) #(pick rnd avail))))
                             [(pick rnd EXPR-VARS)])]
                  [:subquery (chance? rnd 0.3) proj g])
      :graph [:graph (if (chance? rnd 0.7) "?g" (pick rnd GRAPHS))
              {:els [(gen-triple rnd)]}])))

(defn- gen-group [^Random rnd depth]
  (loop [n (inc (.nextInt rnd 3)) els [] scope #{}]
    (if (zero? n)
      {:els els}
      (let [el (gen-el rnd depth scope)]
        (recur (dec n) (conj els el) (into scope (el-scope el)))))))

(defn- triple-count [x]
  (cond (and (vector? x) (= :triple (first x))) 1
        (map? x) (reduce + (map triple-count (vals x)))
        (coll? x) (reduce + (map triple-count x))
        :else 0))

(defn- gen-query [^Random rnd]
  ;; Bound the number of triple patterns so worst-case cross products stay small.
  (loop []
    (let [q {:distinct? (chance? rnd 0.2) :group (gen-group rnd 2)}]
      (if (<= 1 (triple-count q) 5) q (recur)))))

;;; ---------------------------------------------------------------------------
;;; Rendering
;;; ---------------------------------------------------------------------------

(defn- render-expr [[t a b]]
  (case t
    :and (str "(" (render-expr a) " && " (render-expr b) ")")
    :or (str "(" (render-expr a) " || " (render-expr b) ")")
    :not (str "!(" (render-expr a) ")")
    :ne (str "(" a " != " b ")")
    :eq (str "(" a " = " b ")")
    :lt (str "(" a " < " b ")")
    :gt (str "(" a " > " b ")")
    :bound (str "BOUND(" a ")")
    :sameterm (str "sameTerm(" a ", " b ")")
    :isiri (str "isIRI(" a ")")
    :isliteral (str "isLiteral(" a ")")
    :var a
    :const a
    :plus1 (str "(" a " + 1)")
    :coalesce (str "COALESCE(" a ", " b ")")))

(declare render-group)

(defn- render-el [[t & args]]
  (case t
    :triple (str (str/join " " args) " .")
    :optional (let [[g c] args]
                (str "OPTIONAL { " (render-group g)
                     (when c (str " FILTER(" (render-expr c) ")")) " }"))
    :union (str "{ " (render-group (first args)) " } UNION { " (render-group (second args)) " }")
    :filter (str "FILTER(" (render-expr (first args)) ")")
    :bind (str "BIND(" (render-expr (first args)) " AS " (second args) ")")
    :values (let [[vs rows] args]
              (str "VALUES (" (str/join " " vs) ") { "
                   (str/join " " (for [r rows] (str "(" (str/join " " (map #(or % "UNDEF") r)) ")")))
                   " }"))
    :subquery (let [[d? vs g] args]
                (str "{ SELECT " (when d? "DISTINCT ") (str/join " " vs) " WHERE { " (render-group g) " } }"))
    :graph (str "GRAPH " (first args) " { " (render-group (second args)) " }")))

(defn- render-group [g] (str/join " " (map render-el (:els g))))

(defn- render [q]
  (str "SELECT " (when (:distinct? q) "DISTINCT ") "* WHERE { " (render-group (:group q)) " }"))

;;; ---------------------------------------------------------------------------
;;; Shrinking: smaller candidate queries, most aggressive first.
;;; ---------------------------------------------------------------------------

(defn- shrink-expr [[t a b]]
  (case t
    (:and :or) (concat [a b] (map #(vector t % b) (shrink-expr a)) (map #(vector t a %) (shrink-expr b)))
    :not (cons a (map #(vector :not %) (shrink-expr a)))
    nil))

(declare shrink-group)

(defn- shrink-el
  "Replacements for one element: each is a vector of elements spliced in."
  [[t & args]]
  (case t
    :optional (let [[g c] args]
                (concat [(:els g)]
                        (when c [[[:optional g nil]]])
                        (map (fn [g'] [[:optional g' c]]) (shrink-group g))))
    :union (let [[l r] args]
             (concat [(:els l) (:els r)]
                     (map (fn [l'] [[:union l' r]]) (shrink-group l))
                     (map (fn [r'] [[:union l r']]) (shrink-group r))))
    :subquery (let [[d? vs g] args]
                (concat [(:els g)]
                        (when d? [[[:subquery false vs g]]])
                        (for [i (range (count vs)) :when (> (count vs) 1)]
                          [[:subquery d? (into (subvec vs 0 i) (subvec vs (inc i))) g]])
                        (map (fn [g'] [[:subquery d? vs g']]) (shrink-group g))))
    :graph [(:els (second args))]
    :filter (map (fn [e] [[:filter e]]) (shrink-expr (first args)))
    :values (let [[vs rows] args]
              (for [i (range (count rows)) :when (> (count rows) 1)]
                [[:values vs (into (subvec rows 0 i) (subvec rows (inc i)))]]))
    nil))

(defn- shrink-group [{:keys [els]}]
  (concat
   (for [i (range (count els)) :when (> (count els) 1)]
     {:els (into (subvec els 0 i) (subvec els (inc i)))})
   (for [i (range (count els)) repl (shrink-el (els i))]
     {:els (-> (subvec els 0 i) (into repl) (into (subvec els (inc i))))})))

(defn- shrink-query [q]
  (concat (when (:distinct? q) [(assoc q :distinct? false)])
          (map #(assoc q :group %) (shrink-group (:group q)))))

;;; ---------------------------------------------------------------------------
;;; Fixture: the same parsed dataset loaded into Rama and into MemoryStore
;;; ---------------------------------------------------------------------------

(def ^:private module-name (rama/get-module-name RdfStorageModule))

(def ^:dynamic *ipc* nil)
(def ^:dynamic *mem* nil)
(def ^:dynamic *rama* nil)

(defn- wait-mb! [ipc]
  (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
        n (:num-partitions (rama/foreign-object-info depot))
        total (reduce + (for [part (range n)]
                          (:end-offset (rama/foreign-depot-partition-info depot part))))]
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" total)))

(def ^:private no-contexts (make-array Resource 0))

(defn- build-model ^Model []
  (let [^Model model (Rio/parse (StringReader. ^String data) "" RDFFormat/NTRIPLES
                                ^"[Lorg.eclipse.rdf4j.model.Resource;" no-contexts)
        ^ValueFactory vf (SimpleValueFactory/getInstance)
        iri (fn [local] (.createIRI vf (str EX local)))]
    (doseq [[s p o g] named-quads]
      (.add model ^Resource (iri s) (iri p)
            (if (integer? o) (.createLiteral vf ^String (str o) XSD/INTEGER) (iri o))
            ^"[Lorg.eclipse.rdf4j.model.Resource;" (into-array Resource [(iri g)])))
    model))

(defn- fixture [f]
  (let [model (build-model)
        mem (doto (SailRepository. (MemoryStore.)) (.init))]
    (with-open [c (.getConnection mem)]
      (.add c ^Iterable model ^"[Lorg.eclipse.rdf4j.model.Resource;" no-contexts))
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
      (let [rr (doto (SailRepository. (rsail/create-rama-sail ipc module-name)) (.init))]
        (try
          ;; Same Model object for both stores, so blank-node ids match.
          (with-open [c (.getConnection rr)]
            (.begin c)
            (.add c ^Iterable model ^"[Lorg.eclipse.rdf4j.model.Resource;" no-contexts)
            (.commit c))
          (wait-mb! ipc)
          (binding [*ipc* ipc *mem* mem *rama* rr] (f))
          (finally (.shutDown rr) (.shutDown mem)))))))

(use-fixtures :once fixture)

;;; ---------------------------------------------------------------------------
;;; The RDF4J-safe fragment
;;;
;;; Each rule names a shape on which RDF4J 5.2.2 was observed to disagree
;;; with Jena's reference engine (and with Rama); see test-rdf4j-deviations.
;;; ---------------------------------------------------------------------------

(defn- qvar? [x] (and (string? x) (str/starts-with? x "?")))
(defn- expr-vars [e] (cond (qvar? e) #{e} (coll? e) (into #{} (mapcat expr-vars) e) :else #{}))

(declare certain-vars)

(defn- certain-el
  "Variables an element binds in EVERY solution."
  [[t & args]]
  (case t
    :triple (set (filter qvar? args))
    :values (let [[vs rows] args]
              (set (for [i (range (count vs)) :when (every? #(some? (nth % i)) rows)] (vs i))))
    :bind (let [[e v] args] (if (= :const (first e)) #{v} #{}))
    :union (set/intersection (certain-vars (first args)) (certain-vars (second args)))
    :subquery (let [[_ vs g] args] (set/intersection (set vs) (certain-vars g)))
    :graph (cond-> (certain-vars (second args)) (qvar? (first args)) (conj (first args)))
    (:optional :filter) #{}))

(defn- certain-vars [group] (reduce into #{} (map certain-el (:els group))))

(defn- mentions-el
  "Variables an element mentions as seen from outside it (a subquery exposes
   only its projection)."
  [[t & args :as el]]
  (if (= t :subquery) (set (second args)) (expr-vars el)))

(defn- mentions [group] (reduce into #{} (map mentions-el (:els group))))

(defn- has-undef-values? [els]
  (some (fn [[t & args]] (and (= t :values) (some #(some nil? %) (second args)))) els))

(declare group-violations)

(defn- el-violations
  "RDF4J-unsafe shapes at element i of a group. `outside` = variables
   mentioned outside the group; `nested?` = the group is not the query's
   top-level group (RDF4J evaluates nested groups with outer bindings
   injected, so a nested group must not depend on outer variables it does not
   bind itself)."
  [els i outside nested?]
  (let [[t & args :as el] (els i)
        others (reduce into outside (map mentions-el (concat (subvec els 0 i) (subvec els (inc i)))))
        prefix-certain (reduce into #{} (map certain-el (subvec els 0 i)))
        outer-refs (fn [vs bound] (seq (remove bound (filter others vs))))]
    (case t
      :optional (let [[g c] args]
                  (concat
                   ;; well-designed: shared variables are bound before the OPTIONAL
                   (when (seq (remove prefix-certain (filter others (into (mentions g) (expr-vars c)))))
                     [[:optional-not-well-designed el]])
                   ;; OPTIONAL { {SELECT ...} } loses the left row when the
                   ;; subquery is empty and ignores the OPTIONAL's FILTER
                   (when (some #(= :subquery (first %)) (:els g))
                     [[:optional-over-subquery el]])
                   (when (and c (has-undef-values? (:els g)))
                     [[:filter-with-undef-values el]])
                   (group-violations g others true)))
      :union (concat (group-violations (first args) others true)
                     (group-violations (second args) others true))
      :graph (group-violations (second args) others true)
      :subquery (let [[_ vs g] args]
                  (concat
                   ;; a projected variable the body may not bind is treated as bound
                   (when (seq (remove (certain-vars g) vs))
                     [[:subquery-projects-unbound el]])
                   ;; non-projected body variables leak into the outer query
                   (when (seq (filter others (remove (set vs) (expr-vars g))))
                     [[:subquery-hidden-var-clash el]])
                   (group-violations g (set/intersection (set vs) others) true)))
      :filter (when (and nested? (outer-refs (expr-vars (first args)) (certain-vars {:els els})))
                [[:nested-filter-outer-ref el]])
      :bind (let [[e v] args]
              (when (and nested? (or (outer-refs (expr-vars e) prefix-certain) (others v)))
                [[:nested-bind-outer-ref el]]))
      :values (when (and nested?
                         (outer-refs (first args)
                                     (reduce into #{} (map certain-el (concat (subvec els 0 i) (subvec els (inc i)))))))
                [[:nested-values-outer-ref el]])
      :triple nil)))

(defn- group-violations [group outside nested?]
  (let [els (vec (:els group))]
    (concat
     ;; FILTERs get pushed into VALUES rows that have UNDEF
     (when (and (has-undef-values? els) (some #(= :filter (first %)) els))
       [[:filter-with-undef-values group]])
     (mapcat #(el-violations els % outside nested?) (range (count els))))))

(defn- rdf4j-violations [q] (group-violations (:group q) #{} false))

(defn- valid-query?
  "SPARQL 18.2.1: a BIND variable must not already be in scope. RDF4J's
   parser does not enforce it, and shrinking by inlining groups can break it."
  [q]
  (letfn [(ok-group? [{:keys [els]}]
            (loop [[el & more] els scope #{}]
              (if-not el
                true
                (let [[t & args] el]
                  (and (or (not= t :bind) (not (scope (second args))))
                       (every? ok-group? (filter map? args))
                       (recur more (into scope (el-scope el))))))))]
    (ok-group? (:group q))))

;;; ---------------------------------------------------------------------------
;;; Evaluation
;;; ---------------------------------------------------------------------------

(def ^:private MAX-ROWS
  "Oracle results larger than this are skipped (cross-product guard)."
  5000)

(defn- repo-rows [^SailRepository repo ^String sparql]
  (with-open [c (.getConnection repo)
              it (.evaluate (.prepareTupleQuery c sparql))]
    (mapv (fn [bs] (into {} (for [^Binding b bs] [(.getName b) (str (.getValue b))])))
          (iterator-seq it))))

(defn- oracle-rows [sparql] (repo-rows *mem* sparql))

(defn- rama-rows [plan]
  (let [qt (rama/foreign-query *ipc* module-name "execute-plan")]
    (mapv (fn [m] (into {} (for [^Binding b (ser/rama-result->binding-set m)]
                             [(.getName b) (str (.getValue b))])))
          (rama/foreign-invoke-query qt plan))))

(defn- fetch-stats []
  {:predicate-stats (or (rama/foreign-invoke-query
                         (rama/foreign-query *ipc* module-name "get-all-predicate-stats")) {})
   :global-stats (or (rama/foreign-invoke-query
                      (rama/foreign-query *ipc* module-name "get-global-stats")) {})})

(defn- check-query
  "Run `q` everywhere. :status is :ok, :plans-disagree, :rdf4j-mismatch,
   :unsupported (compiles to an RDF4J fallback) or :skipped (huge result)."
  [q stats]
  (let [sparql (render q)
        oracle (try (oracle-rows sparql) (catch Exception _ ::oracle-error))]
    (if (and (vector? oracle) (> (count oracle) MAX-ROWS))
      {:status :skipped :sparql sparql}
      (let [te (.getTupleExpr (.parseQuery (SPARQLParser.) sparql nil))
            raw (try (compilation/tuple-expr->plan te)
                     (catch UnsupportedOperationException _ ::unsupported))]
        (if (= raw ::unsupported)
          {:status :unsupported :sparql sparql}
          (let [plans {:raw raw
                       :opt (opt/optimize-plan raw)
                       :opt+stats (binding [opt/*predicate-stats* (:predicate-stats stats)
                                            opt/*global-stats* (:global-stats stats)]
                                    (opt/with-scope-cache
                                      (opt/optimize-plan (compilation/tuple-expr->plan te))))}
                bags (into {} (for [[k plan] plans]
                                [k (try (frequencies (rama-rows plan))
                                        (catch Throwable t {::error (str (class t) ": " (.getMessage t))}))]))
                safe? (empty? (rdf4j-violations q))
                expected (when (vector? oracle) (frequencies oracle))
                base {:sparql sparql :safe? safe? :bags bags :plans plans :expected expected
                      :rows (when (vector? oracle) (count oracle))}]
            (assoc base :status
                   (cond (not (apply = (vals bags))) :plans-disagree
                         (and safe? expected (not= expected (:opt bags))) :rdf4j-mismatch
                         :else :ok))))))))

(defn- shrink
  "Greedy shrink: take the first smaller VALID candidate that fails the same
   way, until none does (bounded number of evaluations)."
  [q status stats]
  (let [fails? #(and (valid-query? %)
                     (= status (:status (try (check-query % stats) (catch Throwable _ nil)))))]
    (loop [q q budget 400]
      (if-let [smaller (and (pos? budget) (first (filter fails? (take budget (shrink-query q)))))]
        (recur smaller (- budget 5))
        q))))

(defn- env-long [k default]
  (if-let [v (System/getenv k)] (Long/parseLong v) default))

(deftest test-random-queries
  (let [seed (env-long "RANDOM_QUERY_SEED" 20261104)
        n (env-long "RANDOM_QUERY_COUNT" 400)
        rnd (Random. seed)
        stats (fetch-stats)
        results (vec (for [i (range n)]
                       (let [q (gen-query rnd)]
                         (assoc (check-query q stats) :i i :query q))))
        by-status (group-by :status results)
        checked (filter #(and (:safe? %) (:expected %)) results)
        informative (count (filter #(pos? (:rows % 0)) checked))]
    (println (format (str "random queries: seed=%d n=%d ok=%d plans-disagree=%d rdf4j-mismatch=%d "
                          "unsupported=%d skipped=%d | checked against RDF4J: %d (%d with rows)")
                     seed n (count (:ok by-status)) (count (:plans-disagree by-status))
                     (count (:rdf4j-mismatch by-status)) (count (:unsupported by-status))
                     (count (:skipped by-status)) (count checked) informative))
    (testing "the generator exercises the Rama path with informative queries"
      (is (< (count (:unsupported by-status)) (* 0.05 n))
          (pr-str (map :sparql (take 5 (:unsupported by-status)))))
      (is (> informative (* 0.2 n)) "too few RDF4J-checked queries return rows"))
    (doseq [status [:plans-disagree :rdf4j-mismatch]
            m (take 3 (sort-by #(count (:sparql %)) (status by-status)))]
      (let [r (check-query (shrink (:query m) status stats) stats)]
        (is false
            (str (name status) " (seed " seed ", query #" (:i m) ")"
                 "\n  original: " (:sparql m)
                 "\n  shrunk:   " (:sparql r)
                 (when (= status :rdf4j-mismatch) (str "\n  rdf4j:    " (pr-str (:expected r))))
                 (str/join (for [[k v] (:bags r)] (str "\n  " (name k) ": " (pr-str v))))
                 "\n  plans:    " (pr-str (:plans r))))))))

(defn- sparql->plan [^String q]
  (opt/optimize-plan (compilation/tuple-expr->plan (.getTupleExpr (.parseQuery (SPARQLParser.) q nil)))))

(defn- with-extra-statements
  "Add N-Triples/N-Quads-free statements [s p o g?] (IRI strings) to BOTH
   stores, run f, then remove them again, so the shared dataset that the
   random queries see is unchanged."
  [stmts f]
  (let [vf (SimpleValueFactory/getInstance)
        ->ctx (fn [g] (if g (into-array Resource [(.createIRI vf g)]) no-contexts))
        apply! (fn [repo add?]
                 (with-open [c (.getConnection ^SailRepository repo)]
                   (.begin c)
                   (doseq [[s p o g] stmts]
                     (let [args [(.createIRI vf s) (.createIRI vf p) (.createIRI vf o)]]
                       (if add?
                         (.add c ^Resource (args 0) ^org.eclipse.rdf4j.model.IRI (args 1)
                               ^org.eclipse.rdf4j.model.Value (args 2)
                               ^"[Lorg.eclipse.rdf4j.model.Resource;" (->ctx g))
                         (.remove c ^Resource (args 0) ^org.eclipse.rdf4j.model.IRI (args 1)
                                  ^org.eclipse.rdf4j.model.Value (args 2)
                                  ^"[Lorg.eclipse.rdf4j.model.Resource;" (->ctx g)))))
                   (.commit c)))]
    (apply! *mem* true) (apply! *rama* true) (wait-mb! *ipc*)
    (try (f)
         (finally (apply! *mem* false) (apply! *rama* false) (wait-mb! *ipc*)))))

(deftest test-triple-in-several-graphs-divergence
  ;; DOCUMENTED DIVERGENCE. The same triple is stored in the default graph and
  ;; in <g1>. Outside GRAPH, a pattern matches the default graph, which both
  ;; engines treat as all graphs merged. SPARQL defines that graph as a SET of
  ;; triples, which is what Rama returns (find-bgp collects with +set-agg);
  ;; RDF4J's MemoryStore returns one row per stored copy.
  (with-extra-statements
    [["http://ex/n1" "http://ex/dup" "http://ex/n2" nil]
     ["http://ex/n1" "http://ex/dup" "http://ex/n2" "http://ex/g1"]]
    (fn []
      (let [q "SELECT ?s ?o WHERE { ?s <http://ex/dup> ?o }"
            row {"s" "http://ex/n1" "o" "http://ex/n2"}]
        (is (= [row row] (oracle-rows q)) "RDF4J: one row per stored copy")
        (is (= [row] (rama-rows (sparql->plan q))) "Rama: the merged default graph is a set")
        (testing "GRAPH ?g: both engines see the copy in <g1> only"
          (let [q "SELECT ?g WHERE { GRAPH ?g { ?s <http://ex/dup> ?o } }"]
            (is (= [{"g" "http://ex/g1"}] (oracle-rows q) (rama-rows (sparql->plan q))))))))))

(def ^:private rdf4j-deviations
  "One example per rdf4j-violations rule (plus the failable-BIND join the
   generator avoids). :q is evaluated through the Rama SAIL; the SPARQL answer
   — confirmed with Apache Jena's reference engine — is that of the simpler
   :same-as query (evaluated by RDF4J, which gets it right) or :expected."
  [{:name "failed BIND joined on its variable"
    ;; Extend keeps the row with ?b unbound (SPARQL 18.5); RDF4J treats ?b as
    ;; always bound when joining and drops it
    :q "SELECT * WHERE { BIND(?e AS ?b) ?b <http://ex/s> ?d . }"
    :same-as "SELECT ?b ?d WHERE { ?b <http://ex/s> ?d . }"}
   {:name "BIND inside OPTIONAL sees only its own group"
    ;; ?b is unbound inside the OPTIONAL group; RDF4J injects the outer ?b
    :q "SELECT * WHERE { ?b <http://ex/p> ?c . OPTIONAL { BIND(?b AS ?x) } }"
    :same-as "SELECT ?b ?c WHERE { ?b <http://ex/p> ?c . }"}
   {:name "BIND inside OPTIONAL does not overwrite the outer value"
    :q "SELECT * WHERE { ?b <http://ex/r> ?a . OPTIONAL { BIND(2 AS ?b) } }"
    :same-as "SELECT ?b ?a WHERE { ?b <http://ex/r> ?a . }"}
   {:name "non-well-designed OPTIONALs are evaluated in order"
    ;; the first OPTIONAL fails (its FILTER errors), the second binds ?c = n1,
    ;; and only then is the group joined with ?d :p ?c
    :q (str "SELECT * WHERE { OPTIONAL { ?e <http://ex/s> ?c . FILTER(isLiteral(?a)) } "
            "OPTIONAL { ?c <http://ex/q> <http://ex/n3> . } ?d <http://ex/p> ?c . }")
    :same-as "SELECT ?c ?d WHERE { ?c <http://ex/q> <http://ex/n3> . ?d <http://ex/p> ?c . }"}
   {:name "a FILTER is not pushed into VALUES rows with UNDEF"
    :q "SELECT * WHERE { VALUES (?e) { (UNDEF) } ?e <http://ex/s> ?b . FILTER(isIRI(?e)) }"
    :same-as "SELECT ?e ?b WHERE { ?e <http://ex/s> ?b . FILTER(isIRI(?e)) }"}
   {:name "OPTIONAL over an empty subquery keeps the left row"
    :q "SELECT * WHERE { OPTIONAL { { SELECT ?a WHERE { ?a <http://ex/p> <http://ex/none> . } } } }"
    :expected [{}]}
   {:name "a projected subquery variable may be unbound"
    :q "SELECT * WHERE { ?a <http://ex/p> ?a . { SELECT ?a WHERE { <http://ex/n4> <http://ex/r> 3 . } } }"
    :same-as "SELECT ?a WHERE { ?a <http://ex/p> ?a . }"}
   {:name "a subquery's non-projected variables stay hidden"
    :q (str "SELECT * WHERE { { SELECT ?e WHERE { VALUES (?b) { (<http://ex/n6>) } "
            "VALUES (?e) { (<http://ex/n3>) (<http://ex/n5>) } } } ?b <http://ex/s> ?a . }")
    :same-as (str "SELECT ?b ?a ?e WHERE { ?b <http://ex/s> ?a . "
                  "VALUES (?e) { (<http://ex/n3>) (<http://ex/n5>) } }")}])

(deftest test-rdf4j-deviations
  (doseq [{:keys [name q same-as expected]} rdf4j-deviations]
    (testing name
      (let [spec (if same-as (frequencies (oracle-rows same-as)) (frequencies expected))]
        (is (= spec (frequencies (repo-rows *rama* q))) (str "Rama must return the SPARQL answer for " q))
        (is (not= spec (frequencies (oracle-rows q)))
            (str "RDF4J now answers " q " correctly: relax the matching rdf4j-violations rule"))))))

;;; ---------------------------------------------------------------------------
;;; Regressions found by test-random-queries
;;; ---------------------------------------------------------------------------

(deftest test-multi-left-join-applies-optionals-in-query-order
  ;; The optimizer folds the two OPTIONALs into one :multi-left-join. Its
  ;; topology ran the optionals concurrently and applied them in completion
  ;; order; OPTIONALs that share ?a do not commute, so the faster second one
  ;; went first and the answer became the :s rows instead of the :r ones.
  (let [q (str "SELECT * WHERE { OPTIONAL { BIND(<http://ex/n6> AS ?c) <http://ex/n2> <http://ex/r> ?a . } "
               "OPTIONAL { ?a <http://ex/s> ?e . } }")
        plan (sparql->plan q)
        spec (frequencies (oracle-rows "SELECT ?c ?a WHERE { <http://ex/n2> <http://ex/r> ?a . BIND(<http://ex/n6> AS ?c) }"))]
    (is (= :multi-left-join (get-in plan [:sub-plan :op])))
    (dotimes [_ 5]
      (is (= spec (frequencies (rama-rows plan)))))))

(deftest test-union-keeps-duplicate-rows
  ;; :singleton returns #{{}}; when that branch finished first, the union's
  ;; combiner used the set as its accumulator and `into` de-duplicated the
  ;; other branch (?e has duplicates). Timing-dependent, hence the repetition.
  (let [q "SELECT * WHERE { {} UNION { SELECT ?e WHERE { ?c <http://ex/q> ?e . } } }"
        plan (sparql->plan q)
        spec (frequencies (oracle-rows q))]
    (is (some #(> % 1) (vals spec)) "the subquery branch must contain duplicate rows")
    (is (= 25 (count (filter (fn [_] (= spec (frequencies (rama-rows plan)))) (range 25))))))
  (testing "bag-concat never de-duplicates, whatever the accumulator's type"
    (let [bag-concat @#'rama-sail.module.queries/bag-concat]
      (is (= [{} {:a 1} {:a 1}] (bag-concat #{{}} [{:a 1} {:a 1}])))
      (is (= [{:a 1} {:a 1}] (bag-concat nil [{:a 1} {:a 1}])))
      (is (= [{:a 1} {:a 1}] (bag-concat [{:a 1}] #{{:a 1}}))))))
