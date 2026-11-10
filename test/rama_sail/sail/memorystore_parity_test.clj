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

(def ^:private sparse-join-data
  "Two predicates of 63 triples whose subjects overlap on only 3 rows, and those
   sort last: the driver of a LIMIT query must read past many unmatched rows."
  (apply str
         (concat
          (for [p ["lp" "lq"] i (range 60)]
            (format "<http://ex/%s/s%02d> <http://ex/%s> \"v%02d\" .\n" p i p i))
          (for [p ["lp" "lq"] i (range 3)]
            (format "<http://ex/both/s%d> <http://ex/%s> \"zzz\" .\n" i p)))))

(def ^:private data
  "Small default-graph dataset (N-Triples) covering the regression shapes;
   named-graph statements are added in `build-model`."
  (str sparse-join-data "<http://ex/alice> <http://ex/name> \"Alice\" .
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
<http://ex/item10> <http://ex/label> \"New York\" .
<http://ex/item1> <http://ex/label> \"New\" .
<http://ex/item2> <http://ex/label> \"a\\\"b\" .
<http://ex/item3> <http://ex/label> \"line1\\nline2\" .
<http://ex/item4> <http://ex/label> \"x9\" .
<http://ex/item5> <http://ex/label> \"caf\u00e9\"@fr .
<http://ex/item1> <http://ex/when> \"2008-06-21T05:00:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex/item2> <http://ex/when> \"2008-06-20T23:00:00-05:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex/item3> <http://ex/when> \"2008-06-22T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex/alice> <http://ex/nick> \"Al\" .
<http://ex/alice> <http://ex/nick> \"Ali\" .
<http://ex/bob> <http://ex/nick> \"Bobby\" .
"))

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
  ;; Ordering (< <= > >=) between subjects — IRIs and blank nodes — is a type
  ;; error in strict SPARQL, in RDF4J and (since Nov 4, 2026) in Rama: no rows.
  (doseq [op ["<" "<=" ">" ">="]]
    (testing op
      (is (empty? (parity! (str P "SELECT ?a ?b { ?a :w7c ?o . ?b :w7c ?o FILTER(?a " op " ?b) }"))))))
  (testing "STR(?a) < STR(?b): the strict-SPARQL ordering of IRI subjects (BSBM QJ4)"
    (doseq [op ["<" "<=" ">" ">=" "!="]]
      (parity! (str P "SELECT ?a ?b { ?a :city ?o . ?b :city ?o FILTER(STR(?a) " op " STR(?b)) }"))))
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

(deftest test-expression-parity
  ;; W4-W6: the shared term comparator (expr/terms-equal, compare-terms,
  ;; order-key) end to end — ORDER BY and MIN/MAX run in the module.
  (testing "ORDER BY strings / IRIs / dates by value (W5)"
    (parity! (str P "SELECT ?l { ?s :label ?l } ORDER BY ?l") true)
    (parity! (str P "SELECT ?l { ?s :label ?l } ORDER BY DESC(?l)") true)
    (parity! (str P "SELECT ?s { ?s :label ?l } ORDER BY ?s") true)
    (parity! (str P "SELECT ?s ?w { ?s :when ?w } ORDER BY ?w") true))
  (testing "MIN / MAX by value (W5)"
    (is (= [{"m" "\"New\""}] (parity! (str P "SELECT (MIN(?l) AS ?m) { ?s :label ?l FILTER(!isLiteral(?l) || LANG(?l) = \"\") }"))))
    (parity! (str P "SELECT (MAX(?l) AS ?m) (MIN(?s) AS ?n) { ?s :label ?l }"))
    (parity! (str P "SELECT (MIN(?w) AS ?m) (MAX(?w) AS ?n) { ?s :when ?w }")))
  (testing "REGEX on unescaped values (W4)"
    (is (= 2 (count (parity! (str P "SELECT ?s { ?s :label ?l FILTER(REGEX(?l, \"\\\\d\")) }"))))
        "x9 and line1\\nline2")
    (is (= 1 (count (parity! (str P "SELECT ?s { ?s :label ?l FILTER(REGEX(?l, \"\\\"\")) }")))))
    (is (= 1 (count (parity! (str P "SELECT ?s { ?s :label ?l FILTER(REGEX(?l, \"1.l\", \"s\")) }")))))
    (parity! (str P "SELECT ?s { ?s :label ?l FILTER(REGEX(?l, \"^caf\")) }")))
  (testing "boolean results vs boolean literals (W6)"
    (parity! (str P "SELECT ?s { ?s :name ?n OPTIONAL { ?s :age ?a } FILTER(BOUND(?a) = false) }"))
    (parity! (str P "SELECT ?s { ?s :name ?n OPTIONAL { ?s :age ?a } FILTER(BOUND(?a) IN (true)) }"))
    (parity! (str P "SELECT ?s ?b { ?s :name ?n OPTIONAL { ?s :age ?a } BIND(BOUND(?a) AS ?b) } ORDER BY ?b ?s") true))
  (testing "literals of different datatypes are unequal (= / !=)"
    (parity! (str P "SELECT ?s { ?s :age ?a FILTER(?a != \"x\"@en) }"))
    (parity! (str P "SELECT ?s { ?s :label ?l FILTER(?l != \"caf\u00e9\"@fr) }"))))

(deftest test-property-lookup-rewrites-parity
  (testing "OPTIONAL lookups: missing and multi-valued properties"
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :nick ?k } }"))
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :nick ?k } OPTIONAL { ?s :age ?a } }"))
    (parity! (str P "SELECT ?s ?k { ?s :name ?n OPTIONAL { ?s :nick ?k } } ORDER BY ?k ?s") true))
  (testing "OPTIONALs sharing a variable do not commute"
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :city ?x } OPTIONAL { ?s :age ?x } }"))
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :age ?x } OPTIONAL { ?s :city ?x } }")))
  (testing "the optional variable is only possibly bound afterwards"
    (parity! (str P "SELECT ?s { ?s :name ?n OPTIONAL { ?s :nick ?k } FILTER(!BOUND(?k)) }"))
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :nick ?k } ?s :city ?c }"))
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :nick ?k } ?t :nick ?k }")))
  (testing "OPTIONAL with a filter stays conditional"
    (parity! (str P "SELECT * { ?s :name ?n OPTIONAL { ?s :age ?a FILTER(?a > 20) } }")))
  (testing "OPTIONAL over a possibly-unbound subject"
    (parity! (str P "SELECT * { { ?s :name ?n } UNION { ?t :city ?c } OPTIONAL { ?s :nick ?k } }")))
  (testing "property lookup on either side of an inner join"
    (parity! (str P "SELECT * { ?s :w7p <http://ex/v> . ?s :name ?n }"))
    (parity! (str P "SELECT * { ?s :name ?n . ?s :city \"NYC\" . ?s :nick ?k }"))))

(deftest test-limit-with-many-unmatched-rows
  (let [q (fn [tail] (str P "SELECT ?s { ?s :lp ?a . ?s :lq ?b } " tail))
        both? #(re-find #"http://ex/both/" (get % "s"))]
    (testing "without LIMIT the join has exactly the overlapping rows"
      (is (= 3 (count (parity! (q ""))))))
    (doseq [[tail expected] [["LIMIT 2" 2] ["LIMIT 3" 3] ["LIMIT 5" 3] ["OFFSET 1 LIMIT 5" 2] ["OFFSET 2 LIMIT 1" 1]]]
      (testing tail
        (let [r (rows *rama* (q tail))]
          (is (= expected (count r)))
          (is (= (count r) (count (rows *mem* (q tail)))))
          (is (every? both? r)))))
    (testing "OPTIONAL keeps unmatched rows, so LIMIT is met by the first attempt"
      (is (= 5 (count (rows *rama* (str P "SELECT ?s { ?s :lp ?a OPTIONAL { ?s :lq ?b } } LIMIT 5"))))))))
