(ns rama-sail.sail.optimizer-equivalence-test
  "Metamorphic tests verifying that plan optimization preserves query
   semantics: for each SPARQL query the UNOPTIMIZED plan (straight out of
   tuple-expr->plan) and the OPTIMIZED plan (optimize-plan) are executed
   against the same data on a shared IPC, and the result BAGS (frequencies of
   binding maps — SPARQL is bag-semantics) must be identical.

   Uses the shared :once IPC fixture pattern (see module/indexer_test.clj) and
   the depot end-offset barrier from query/transaction_test.clj."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.compilation :as compilation]
            [rama-sail.sail.optimization :as optimization]
            [rama-sail.sail.serialization :refer [DEFAULT-CONTEXT-VAL]])
  (:import (org.eclipse.rdf4j.query.parser ParsedQuery)
           (org.eclipse.rdf4j.query.parser.sparql SPARQLParser)))

(def module-name (rama/get-module-name RdfStorageModule))

(def ^:dynamic *ipc* nil)

;;; ---------------------------------------------------------------------------
;;; Test data: ~30 quads of varied shapes
;;; ---------------------------------------------------------------------------

(def ^:private xsd-int "http://www.w3.org/2001/XMLSchema#integer")
(def ^:private rdf-type "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")

(defn- iri [local] (str "<http://ex/" local ">"))
(defn- int-lit [n] (str "\"" n "\"^^<" xsd-int ">"))
(defn- str-lit [s] (str "\"" s "\""))

(def ^:private test-quads
  "Quads as [s p o c]. Default graph uses DEFAULT-CONTEXT-VAL; two triples
   live in the named graph <http://ex/g>. Ages are all DISTINCT so that
   ORDER BY ?age LIMIT n is deterministic as a bag. Names and cities contain
   duplicate values across subjects so joins/DISTINCT are meaningful."
  (let [g (iri "g")
        dc DEFAULT-CONTEXT-VAL]
    [;; names (bob and dave share a name -> DISTINCT meaningful)
     [(iri "alice") (iri "name") (str-lit "Alice") dc]
     [(iri "bob")   (iri "name") (str-lit "Bob")   dc]
     [(iri "carol") (iri "name") (str-lit "Carol") dc]
     [(iri "dave")  (iri "name") (str-lit "Bob")   dc]
     [(iri "eve")   (iri "name") (str-lit "Eve")   dc]
     ;; ages: typed integer literals, all distinct
     [(iri "alice") (iri "age") (int-lit 30) dc]
     [(iri "bob")   (iri "age") (int-lit 25) dc]
     [(iri "carol") (iri "age") (int-lit 41) dc]
     [(iri "dave")  (iri "age") (int-lit 19) dc]
     [(iri "eve")   (iri "age") (int-lit 33) dc]
     ;; knows edges, incl. one self-loop
     [(iri "alice") (iri "knows") (iri "bob")   dc]
     [(iri "alice") (iri "knows") (iri "carol") dc]
     [(iri "alice") (iri "knows") (iri "alice") dc] ;; self-loop
     [(iri "bob")   (iri "knows") (iri "carol") dc]
     [(iri "bob")   (iri "knows") (iri "dave")  dc]
     [(iri "carol") (iri "knows") (iri "dave")  dc]
     [(iri "dave")  (iri "knows") (iri "eve")   dc]
     [(iri "eve")   (iri "knows") (iri "alice") dc]
     ;; rdf:type usage (eve deliberately untyped -> OPTIONAL meaningful)
     [(iri "alice") rdf-type (iri "Person") dc]
     [(iri "bob")   rdf-type (iri "Person") dc]
     [(iri "carol") rdf-type (iri "Person") dc]
     [(iri "dave")  rdf-type (iri "Robot")  dc]
     ;; cities: shared objects across subjects (self-join fodder)
     [(iri "alice") (iri "city") (str-lit "NYC") dc]
     [(iri "bob")   (iri "city") (str-lit "NYC") dc]
     [(iri "carol") (iri "city") (str-lit "NYC") dc]
     [(iri "dave")  (iri "city") (str-lit "LA")  dc]
     [(iri "eve")   (iri "city") (str-lit "LA")  dc]
     ;; named graph <http://ex/g>
     [(iri "alice") (iri "inGraph") (str-lit "gv1") g]
     [(iri "bob")   (iri "inGraph") (str-lit "gv2") g]]))

;;; ---------------------------------------------------------------------------
;;; Fixture
;;; ---------------------------------------------------------------------------

(defn wait-mb!
  "Barrier: wait until the indexer has processed every depot record appended
   so far. wait-for-microbatch-processed-count counts DEPOT RECORDS (not
   iterations), so the exact target is the sum of the depot partitions'
   end-offsets."
  []
  (let [depot (rama/foreign-depot *ipc* module-name "*triple-depot")
        num-partitions (:num-partitions (rama/foreign-object-info depot))
        total (reduce + (for [part (range num-partitions)]
                          (:end-offset (rama/foreign-depot-partition-info depot part))))]
    (rtest/wait-for-microbatch-processed-count *ipc* module-name "indexer" total)))

(defn equivalence-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc]
      (let [depot (rama/foreign-depot *ipc* module-name "*triple-depot")]
        (doseq [quad test-quads]
          (rama/foreign-append! depot [:add quad])))
      (wait-mb!)
      (f))))

(use-fixtures :once equivalence-fixture)

;;; ---------------------------------------------------------------------------
;;; Plan compilation & execution helpers
;;; ---------------------------------------------------------------------------

(defn- sparql->raw-plan [^String sparql]
  (let [parser (SPARQLParser.)
        ^ParsedQuery parsed (.parseQuery parser sparql nil)]
    (compilation/tuple-expr->plan (.getTupleExpr parsed))))

(defn- execute-plan* [plan]
  (let [qt (rama/foreign-query *ipc* module-name "execute-plan")]
    (rama/foreign-invoke-query qt plan)))

(defn- run-both
  "Compile sparql, execute the raw and the optimized plan, return
   {:raw results :opt results :raw-plan plan :opt-plan plan}."
  [sparql]
  (let [raw-plan (sparql->raw-plan sparql)
        opt-plan (optimization/optimize-plan raw-plan)]
    {:raw-plan raw-plan
     :opt-plan opt-plan
     :raw (execute-plan* raw-plan)
     :opt (execute-plan* opt-plan)}))

(defn- assert-equivalent!
  "Execute both plans and assert equal result bags. Returns the run map."
  [sparql]
  (let [{:keys [raw opt raw-plan opt-plan] :as run} (run-both sparql)]
    (is (= (frequencies raw) (frequencies opt))
        (str "optimizer changed the result bag for query:\n" sparql
             "\n  raw plan:       " (pr-str raw-plan)
             "\n  optimized plan: " (pr-str opt-plan)
             "\n  raw results (freq):       " (pr-str (frequencies raw))
             "\n  optimized results (freq): " (pr-str (frequencies opt))))
    run))

(defn- age-of
  "Extract the numeric value of the \"?age\" binding (typed integer literal)."
  [binding-map]
  (Long/parseLong (second (re-find #"\"(-?\d+)\"" (get binding-map "?age")))))

;;; ---------------------------------------------------------------------------
;;; Queries
;;; ---------------------------------------------------------------------------

(deftest test-single-bgp
  (assert-equivalent!
   "SELECT ?s ?name WHERE { ?s <http://ex/name> ?name }"))

(deftest test-bgp-with-numeric-filter
  (assert-equivalent!
   "SELECT ?s ?age WHERE { ?s <http://ex/age> ?age . FILTER(?age > 24) }"))

(deftest test-two-pattern-join
  (assert-equivalent!
   "SELECT ?s ?f ?name WHERE {
      ?s <http://ex/knows> ?f .
      ?f <http://ex/name> ?name }"))

(deftest test-three-pattern-join-chain
  (assert-equivalent!
   "SELECT ?a ?b ?c WHERE {
      ?a <http://ex/knows> ?b .
      ?b <http://ex/knows> ?c .
      ?c <http://ex/age> ?age }"))

(deftest test-optional
  (assert-equivalent!
   "SELECT ?s ?t WHERE {
      ?s <http://ex/name> ?n .
      OPTIONAL { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t } }"))

(deftest test-union
  (assert-equivalent!
   "SELECT ?x WHERE {
      { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Person> }
      UNION
      { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Robot> } }"))

(deftest test-select-distinct
  (assert-equivalent!
   "SELECT DISTINCT ?name WHERE { ?s <http://ex/name> ?name }"))

(deftest test-order-by-limit
  ;; Ages are all distinct, so ORDER BY ?age LIMIT 3 is deterministic as a
  ;; bag even without a total tie-break. Compare result bags AND assert both
  ;; result sequences are actually ordered by the sort key.
  (let [sparql "SELECT ?s ?age WHERE { ?s <http://ex/age> ?age } ORDER BY ?age LIMIT 3"
        {:keys [raw opt]} (assert-equivalent! sparql)]
    (doseq [[label results] [["raw" raw] ["optimized" opt]]]
      (let [ages (map age-of results)]
        (is (= ages (sort ages))
            (str label " plan results not ordered by ?age for query:\n" sparql
                 "\n  ages in result order: " (pr-str ages)))))))

(deftest test-self-join-with-filter
  ;; Two patterns on the same predicate + FILTER(?a < ?b) over IRI subjects —
  ;; the shape targeted by the optimizer's self-join transformation.
  (assert-equivalent!
   "SELECT ?a ?b WHERE {
      ?a <http://ex/city> ?c .
      ?b <http://ex/city> ?c .
      FILTER(?a < ?b) }"))

(deftest test-named-graph
  (assert-equivalent!
   "SELECT ?s ?o WHERE { GRAPH <http://ex/g> { ?s <http://ex/inGraph> ?o } }"))

;;; ---------------------------------------------------------------------------
;;; Phase 6 wrong-results regressions (W1-W3). Besides raw-vs-optimized
;;; equivalence, these assert the EXPECTED answer: a bug present in both
;;; plans (W2, W3) is invisible to equivalence alone.
;;; ---------------------------------------------------------------------------

(defn- pairs-of [results]
  (frequencies (map (juxt #(get % "?a") #(get % "?b")) results)))

(deftest test-plain-self-join-keeps-reflexive-rows
  ;; W1: the optimizer turns this into :self-join, which used to drop every
  ;; ?a = ?b row. Cities: NYC x3 -> 9 pairs, LA x2 -> 4 pairs.
  (let [{:keys [raw opt opt-plan]} (assert-equivalent!
                                    "SELECT ?a ?b WHERE {
                                       ?a <http://ex/city> ?c .
                                       ?b <http://ex/city> ?c }")]
    (is (= :self-join (get-in opt-plan [:sub-plan :op]))
        "the optimized plan must exercise the :self-join operator")
    (is (= 13 (count raw)) (str "raw: " (pr-str (pairs-of raw))))
    (is (= 13 (count opt)) (str "optimized: " (pr-str (pairs-of opt))))
    (is (contains? (pairs-of opt) [(iri "alice") (iri "alice")]))))

(deftest test-self-join-on-unique-objects
  ;; W1: every age is distinct, so each triple joins only with itself —
  ;; 5 rows, previously 0 on the optimized path.
  (let [{:keys [opt]} (assert-equivalent!
                       "SELECT ?a ?b WHERE {
                          ?a <http://ex/age> ?x .
                          ?b <http://ex/age> ?x }")]
    (is (= 5 (count opt)))
    (is (every? (fn [[a b]] (= a b)) (keys (pairs-of opt))))))

(deftest test-self-join-non-strict-and-ne-filters
  ;; W1: <= / >= keep the equal pair, != drops it. Cities: NYC {alice bob
  ;; carol}, LA {dave eve}.
  ;;   <=, >= : NYC 3 equal + 3 ordered, LA 2 equal + 1 ordered -> 9
  ;;   !=     : NYC 6 ordered pairs, LA 2                       -> 8
  (doseq [[op expected] [["<=" 9] [">=" 9] ["!=" 8]]]
    (testing op
      (let [{:keys [opt]} (assert-equivalent!
                           (str "SELECT ?a ?b WHERE {
                                   ?a <http://ex/city> ?c .
                                   ?b <http://ex/city> ?c .
                                   FILTER(?a " op " ?b) }"))]
        (is (= expected (count opt))
            (str op " -> " (pr-str (pairs-of opt))))))))

(deftest test-distinct-order-by-limit-keeps-order
  ;; W2: plan is (:slice (:distinct (:project (:order ...)))); DISTINCT used
  ;; to be an unordered set, so LIMIT returned arbitrary rows.
  (testing "DISTINCT + ORDER BY DESC + LIMIT over duplicated values"
    (let [sparql "SELECT DISTINCT ?name WHERE { ?s <http://ex/name> ?name }
                  ORDER BY DESC(?name) LIMIT 2"
          {:keys [raw opt]} (assert-equivalent! sparql)]
      (doseq [[label results] [["raw" raw] ["optimized" opt]]]
        (is (= [(str-lit "Eve") (str-lit "Carol")] (mapv #(get % "?name") results))
            (str label " plan: " (pr-str results))))))
  (testing "DISTINCT + ORDER BY + LIMIT over a projected sort key"
    (let [sparql "SELECT DISTINCT ?s ?age WHERE { ?s <http://ex/age> ?age }
                  ORDER BY ?age LIMIT 3"
          {:keys [raw opt]} (assert-equivalent! sparql)]
      (doseq [[label results] [["raw" raw] ["optimized" opt]]]
        (is (= [19 25 30] (mapv age-of results))
            (str label " plan: " (pr-str results))))))
  (testing "DISTINCT + ORDER BY without LIMIT returns rows in order"
    (let [{:keys [raw opt]} (assert-equivalent!
                             "SELECT DISTINCT ?c WHERE { ?s <http://ex/city> ?c } ORDER BY ?c")]
      (doseq [[label results] [["raw" raw] ["optimized" opt]]]
        (is (= [(str-lit "LA") (str-lit "NYC")] (mapv #(get % "?c") results))
            (str label " plan: " (pr-str results)))))))

(deftest test-graph-var-excludes-default-graph
  ;; W3: GRAPH ?g ranges over NAMED graphs only. 27 of the 29 quads live in
  ;; the default graph; only the two <http://ex/g> quads may match.
  (let [g (iri "g")
        expected {{"?g" g "?s" (iri "alice")} 1
                  {"?g" g "?s" (iri "bob")} 1}]
    (testing "unbounded"
      (let [{:keys [raw opt]} (assert-equivalent!
                               "SELECT ?g ?s WHERE { GRAPH ?g { ?s ?p ?o } }")]
        (is (= expected (frequencies raw)))
        (is (= expected (frequencies opt)))))
    (testing "LIMIT is applied AFTER the default-graph exclusion"
      (let [{:keys [raw opt]} (assert-equivalent!
                               "SELECT ?g ?s WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 2")]
        (is (= expected (frequencies raw)))
        (is (= expected (frequencies opt)))))
    (testing "a constant-predicate pattern inside GRAPH ?g"
      (let [{:keys [opt]} (assert-equivalent!
                           "SELECT ?g ?s ?o WHERE { GRAPH ?g { ?s <http://ex/city> ?o } }")]
        (is (empty? opt) "all city triples are in the default graph")))))
