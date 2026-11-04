(ns rama-sail.sail.memorystore-parity-test
  "Differential tests: the same SPARQL over the same data must return the same
   solutions from the Rama SAIL and from RDF4J's reference MemoryStore.

   Unlike the optimizer-equivalence suite (raw vs optimized plan, both on Rama),
   this catches bugs present in BOTH Rama plans, with an independent oracle for
   the expected answer. Cases are the Phase 6 wrong-results regressions
   (W1-W3, W7, self-join ordering over blank nodes) plus general shapes.

   Result bags are compared as frequencies of {var -> N-Triples-ish string};
   queries with ORDER BY on a total key are also compared in order."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import (java.io StringReader)
           (org.eclipse.rdf4j.model Resource)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.repository.sail SailRepository)
           (org.eclipse.rdf4j.rio Rio RDFFormat)
           (org.eclipse.rdf4j.sail.memory MemoryStore)))

(def ^:private data
  "Small default-graph dataset (N-Triples) covering the regression shapes;
   named-graph statements are added in `build-model`."
  "<http://ex/alice> <http://ex/name> \"Alice\" .
<http://ex/bob> <http://ex/name> \"Bob\" .
<http://ex/carol> <http://ex/name> \"Carol\" .
<http://ex/dave> <http://ex/name> \"Dave\" .
<http://ex/alice> <http://ex/city> \"NYC\" .
<http://ex/bob> <http://ex/city> \"NYC\" .
<http://ex/carol> <http://ex/city> \"LA\" .
<http://ex/dave> <http://ex/age> \"19\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/carol> <http://ex/age> \"41\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/alice> <http://ex/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/carol> <http://ex/w7p> <http://ex/v> .
<http://ex/dave> <http://ex/w7alt> <http://ex/v1> .
<http://ex/dave> <http://ex/w7p> <http://ex/v2> .
<http://ex/v> <http://ex/w7q> <http://ex/w1> .
_:b1 <http://ex/w7c> \"X\" .
_:b2 <http://ex/w7c> \"X\" .
<http://ex/i1> <http://ex/w7c> \"X\" .
<http://ex/i2> <http://ex/w7c> \"X\" .
")

(defn- build-model
  "Parse `data` and add two statements in the named graph <http://ex/g>.
   (No N-Quads parser on the classpath, so named-graph quads are built here.)"
  []
  (let [model (Rio/parse (StringReader. data) "" RDFFormat/NTRIPLES (make-array Resource 0))
        vf (SimpleValueFactory/getInstance)
        g (.createIRI vf "http://ex/g")
        p (.createIRI vf "http://ex/inGraph")]
    (doseq [[s o] [["http://ex/alice" "gv1"] ["http://ex/bob" "gv2"]]]
      (.add model (.createIRI vf s) p (.createLiteral vf ^String o) (into-array Resource [g])))
    model))

(def ^:private module-name (rama/get-module-name RdfStorageModule))

(defn- wait-mb!
  "Depot end-offset barrier (counts depot RECORDS; see test/CLAUDE.md)."
  [ipc]
  (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
        n (:num-partitions (rama/foreign-object-info depot))
        total (reduce + (for [part (range n)]
                          (:end-offset (rama/foreign-depot-partition-info depot part))))]
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" total)))

(def ^:dynamic *rama* nil)
(def ^:dynamic *mem* nil)

(defn- parity-fixture [f]
  (let [model (build-model)
        mem (doto (SailRepository. (MemoryStore.)) (.init))]
    (with-open [c (.getConnection mem)] (.add c model (make-array Resource 0)))
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
      (let [rr (doto (SailRepository. (rsail/create-rama-sail ipc module-name))
                 (.init))]
        (try
          ;; The same Model object feeds both stores, so blank node ids match.
          (with-open [c (.getConnection rr)]
            (.begin c) (.add c model (make-array Resource 0)) (.commit c))
          (wait-mb! ipc)
          (binding [*rama* rr *mem* mem] (f))
          (finally (.shutDown rr) (.shutDown mem)))))))

(use-fixtures :once parity-fixture)

(defn- rows [repo sparql]
  (with-open [c (.getConnection repo)
              it (.evaluate (.prepareTupleQuery c sparql))]
    (mapv (fn [bs] (into {} (for [b bs] [(.getName b) (str (.getValue b))])))
          (iterator-seq it))))

(defn- parity!
  "Assert Rama and MemoryStore agree on `sparql` (bag; also order when
   `ordered?`). Returns the MemoryStore rows."
  ([sparql] (parity! sparql false))
  ([sparql ordered?]
   (let [m (rows *mem* sparql)
         r (rows *rama* sparql)]
     (is (= (frequencies m) (frequencies r))
         (str "result bags differ for:\n" sparql "\n  memorystore: " (pr-str m) "\n  rama:        " (pr-str r)))
     (when ordered?
       (is (= m r) (str "result order differs for:\n" sparql)))
     m)))

(def ^:private P "PREFIX : <http://ex/> ")

(deftest test-w7-parity
  (testing "filter over OPTIONAL var bound by the other side (pushdown)"
    (is (= 1 (count (parity! (str P "SELECT ?s ?n ?x { ?s :name ?n OPTIONAL { ?s :w7alt ?x }
                                                       ?s :w7p ?x FILTER(?x != ?n) }"))))))
  (testing "OPTIONAL-bound var vs property lookup (batch-enrich)"
    (is (= 1 (count (parity! (str P "SELECT ?s ?n ?o { ?s :name ?n OPTIONAL { ?s :w7alt ?o }
                                                       ?s :w7p ?o }"))))))
  (testing "self-join filter outside a projecting subquery"
    (is (empty? (parity! (str P "SELECT * { { SELECT ?o { ?a :city ?o . ?b :city ?o } }
                                           FILTER(?a != ?b) }")))))
  (testing "OPTIONAL var used by a later triple (unbound -> joins with all)"
    (parity! (str P "SELECT ?s ?x ?o { ?s :name ?n OPTIONAL { ?s :w7alt ?x } ?x :w7q ?o }"))))

(deftest test-self-join-ordering-parity
  ;; Subjects of :w7c are {_:b1 _:b2 :i1 :i2}.
  (testing "!= : full parity (plain term inequality, blank nodes included)"
    (is (= 12 (count (parity! (str P "SELECT ?a ?b { ?a :w7c ?o . ?b :w7c ?o FILTER(?a != ?b) }"))))))
  ;; Ordering: DOCUMENTED DIVERGENCE. Strict SPARQL / RDF4J make < <= > >= on
  ;; IRIs a type error (0 rows); Rama's expression engine and self-join order
  ;; IRIs by codepoint as an extension (BSBM QJ4 relies on it — see
  ;; expr/order-compare-terms). What must hold everywhere: no pair involving a
  ;; blank node (a type error in both engines) — Rama returns exactly the
  ;; IRI-IRI pairs, RDF4J none.
  (let [i1 "http://ex/i1" i2 "http://ex/i2"]
    (doseq [[op expected] [["<"  #{[i1 i2]}]
                           ["<=" #{[i1 i1] [i1 i2] [i2 i2]}]
                           [">"  #{[i2 i1]}]
                           [">=" #{[i1 i1] [i2 i1] [i2 i2]}]]]
      (testing op
        (let [q (str P "SELECT ?a ?b { ?a :w7c ?o . ?b :w7c ?o FILTER(?a " op " ?b) }")
              r (rows *rama* q)]
          (is (empty? (rows *mem* q)) "RDF4J: ordering IRIs is a type error")
          (is (= (count expected) (count r)) (pr-str r))
          (is (= expected (set (map (juxt #(get % "a") #(get % "b")) r)))
              (str "Rama must return only IRI-IRI pairs, never blank nodes: " (pr-str r)))))))
  (testing "plain self-join keeps ?a = ?b rows (W1)"
    (is (= 5 (count (parity! (str P "SELECT ?a ?b { ?a :city ?o . ?b :city ?o }"))))))
  (testing "self-join on unique objects (W1)"
    (is (= 3 (count (parity! (str P "SELECT ?a ?b { ?a :age ?x . ?b :age ?x }")))))))

(deftest test-w2-w3-parity
  (testing "DISTINCT + ORDER BY + LIMIT keeps the top-k in order (W2)"
    (parity! (str P "SELECT DISTINCT ?c { ?s :city ?c } ORDER BY DESC(?c) LIMIT 1") true)
    (parity! (str P "SELECT DISTINCT ?s ?age { ?s :age ?age } ORDER BY ?age LIMIT 2") true))
  (testing "GRAPH ?g ranges over named graphs only (W3)"
    (is (= 2 (count (parity! (str P "SELECT ?g ?s { GRAPH ?g { ?s ?p ?o } }")))))
    (parity! (str P "SELECT ?g ?s { GRAPH ?g { ?s ?p ?o } } LIMIT 5"))))
