(ns rama-sail.sail.e2e-test
  "End-to-end RDF4J SPARQL tests against the Rama SAIL.

   Uses ONE shared IPC for the whole namespace (see `e2e-fixture`). The store
   accumulates data across tests, so every test isolates its data with
   `unique-iri` (unique subjects, predicates, classes, or graphs) and never
   asserts over unscoped patterns unless extra rows cannot affect the result."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]
           (org.eclipse.rdf4j.query QueryInterruptedException)))

(def ^org.eclipse.rdf4j.model.ValueFactory VF (SimpleValueFactory/getInstance))
(def module-name (rama/get-module-name RdfStorageModule))

;; Shared IPC for all tests in this namespace
(def ^:dynamic *ipc* nil)

(defn e2e-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc]
      (f))))

(use-fixtures :once e2e-fixture)

(defn wait-mb!
  "Barrier: wait until the indexer has processed every depot record appended
   so far. wait-for-microbatch-processed-count counts DEPOT RECORDS, so the
   exact target is the sum of the depot partitions' end-offsets."
  []
  (let [depot (rama/foreign-depot *ipc* module-name "*triple-depot")
        num-partitions (:num-partitions (rama/foreign-object-info depot))
        total (reduce + (for [part (range num-partitions)]
                          (:end-offset (rama/foreign-depot-partition-info depot part))))]
    (rtest/wait-for-microbatch-processed-count *ipc* module-name "indexer" total)))

(defn unique-iri
  "Generate unique IRI string for test isolation on the shared store."
  [base]
  (str "http://ex/" base "-" (System/nanoTime)))

(deftest test-e2e-rdf4j-integration
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subjects AND predicates: the joins/ASK below match by
            ;; predicate+object (e.g. age = 30) which other tests also use.
            alice-iri (unique-iri "alice")
            bob-iri (unique-iri "bob")
            knows-iri (unique-iri "knows")
            name-iri (unique-iri "name")
            age-iri (unique-iri "age")
            alice (.createIRI VF alice-iri)
            bob (.createIRI VF bob-iri)
            knows (.createIRI VF knows-iri)
            name-prop (.createIRI VF name-iri)
            age-prop (.createIRI VF age-iri)
            bob-name (.createLiteral VF "Bob" "en")
            bob-age (.createLiteral VF "30" (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer"))]
        (try
          (testing "Add Data via Repository"
            (.begin conn)
            ;; FIX: Pass empty array for contexts varargs
            (.add conn alice knows bob (into-array Resource []))
            (.add conn bob name-prop bob-name (into-array Resource []))
            (.add conn bob age-prop bob-age (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "Simple BGP Query (getStatements)"
            ;; FIX: Pass empty array for the 5th argument (contexts varargs)
            (let [iter (.getStatements conn bob nil nil true (into-array Resource []))
                  stmts (iterator-seq iter)]
              (is (= 2 (count stmts)))
              (is (some #(= bob-name (.getObject %)) stmts))
              (is (some #(= bob-age (.getObject %)) stmts))))

          (testing "SPARQL Query with JOIN and PROJECTION"
            ;; Query: Find names of people Alice knows
            (let [sparql (format "SELECT ?name WHERE {
                                    <%s> <%s> ?friend .
                                    ?friend <%s> ?name .
                                  }" alice-iri knows-iri name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 1 (count res)))
              (let [bs (first res)
                    val (.getValue bs "name")]
                (is (= "Bob" (.getLabel val)))
                (is (= "en" (.get (.getLanguage val)))) ;; Verify Lang tag preserved
                )))

          (testing "SPARQL Query with Join on Literals (Datatype check)"
            ;; Query: Find friend with age 30 (unique age predicate scopes this)
            (let [sparql (format "SELECT ?friend WHERE {
                                     ?friend <%s> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                                   }" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= bob-iri (-> res first (.getValue "friend") .stringValue)))))

          (testing "Deletion"
            (println "Before deletion:"
                     (with-open [iter (.getStatements
                                       conn alice knows bob true (into-array Resource []))]
                       (vec (iterator-seq iter))))

            (.begin conn)
            (.remove conn alice knows bob (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            ;; 1. Verify deletion via getStatements (bypasses SPARQL pipeline)
            (let [res (with-open [iter (.getStatements
                                        conn alice knows bob true (into-array Resource []))]
                        (vec (iterator-seq iter)))
                  _ (println "Statements after deletion:" res)]
              (is (= 0 (count res)) "Triple <alice, knows, bob> should be removed"))

            ;; Verify deletion via SPARQL. No exception tolerance: a thrown
            ;; QueryTopologyInvokeFailed used to be caught and printed, which
            ;; made this assertion unable to fail.
            (let [sparql (format "ASK { <%s> <%s> <%s> }" alice-iri knows-iri bob-iri)
                  query (.prepareBooleanQuery conn sparql)
                  res (.evaluate query)]
              (is (false? res) "ASK must be false after the triple is deleted")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-named-graphs
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subject, graphs, and object literal: the default-graph
            ;; and wildcard queries must be scoped so cross-test data can't
            ;; leak into the counts.
            s-iri (unique-iri "ng-s")
            g1-iri (unique-iri "g1")
            g2-iri (unique-iri "g2")
            value-str (str "value-" (System/nanoTime))
            s (.createIRI VF s-iri)
            p (.createIRI VF "http://ex/p")
            o (.createLiteral VF value-str)
            g1 (.createIRI VF g1-iri)
            g2 (.createIRI VF g2-iri)]
        (try
          (testing "Add Statements to Named Graphs"
            (.begin conn)
            (.add conn s p o (into-array Resource [g1]))
            (.add conn s p o (into-array Resource [g2]))
            ;; Add to default graph
            (.add conn s p (.createLiteral VF "default") (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "Query Specific Named Graph"
            (let [iter (.getStatements conn nil nil nil true (into-array Resource [g1]))
                  stmts (vec (iterator-seq iter))]
              (is (= 1 (count stmts)))
              (is (= g1 (.getContext (first stmts))))))

          (testing "Query Default Graph Only"
            ;; Scoped by the unique subject: the shared default graph contains
            ;; other tests' data.
            (let [iter (.getStatements conn s nil nil true (into-array Resource [nil]))
                  stmts (vec (iterator-seq iter))]
              (is (= 1 (count stmts)))
              (is (= "default" (.getLabel (.getObject (first stmts)))))
              (is (nil? (.getContext (first stmts))))))

          (testing "Wildcard Query (All Graphs)"
            ;; Scoped by the unique subject (was an unscoped store-wide scan).
            (let [iter (.getStatements conn s nil nil true (into-array Resource []))
                  stmts (vec (iterator-seq iter))]
              (is (= 3 (count stmts)))))

          (testing "SPARQL GRAPH Query"
            ;; The unique object literal scopes the pattern to this test's graphs.
            (let [sparql (format "SELECT ?g WHERE { GRAPH ?g { ?s ?p \"%s\" } }" value-str)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 2 (count res)))
              (let [graphs (set (map #(str (.getValue % "g")) res))]
                (is (contains? graphs g1-iri))
                (is (contains? graphs g2-iri)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-full-pushdown-optional
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subjects; shared predicates are fine because every
            ;; pattern is scoped through the unique alice.
            alice-iri (unique-iri "alice")
            bob-iri (unique-iri "bob")
            charlie-iri (unique-iri "charlie")
            alice (.createIRI VF alice-iri)
            bob (.createIRI VF bob-iri)
            charlie (.createIRI VF charlie-iri)
            knows (.createIRI VF "http://ex/knows")
            email (.createIRI VF "http://ex/email")
            bob-mail (.createLiteral VF "bob@example.com")]
        (try
          (testing "Setup Data"
            (.begin conn)
            ;; Alice knows Bob (who has an email)
            (.add conn alice knows bob (into-array Resource []))
            (.add conn bob email bob-mail (into-array Resource []))

            ;; Alice knows Charlie (who has NO email)
            (.add conn alice knows charlie (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "Execute OPTIONAL Query (Full Pushdown)"
            ;; This query contains OPTIONAL.
            ;; The Rama `execute-plan` topology now supports OPTIONAL (LeftJoin),
            ;; so the SAIL compiles and executes the full plan on Rama,
            ;; without falling back to RDF4J's engine.
            ;; Rama still uses `.getStatements` internally to resolve the basic
            ;; graph patterns, but performs the LeftJoin inside the topology.

            (let [sparql (format "SELECT ?friend ?mail WHERE {
                                    <%s> <http://ex/knows> ?friend .
                                    OPTIONAL { ?friend <http://ex/email> ?mail }
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res   (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]

              (is (= 2 (count res)) "Should return both Bob and Charlie")

              ;; Sort results to ensure stability for assertions
              ;; ("bob-..." sorts before "charlie-...")
              (let [sorted-res          (sort-by #(-> % (.getValue "friend") .stringValue) res)
                    [row-bob row-charlie] sorted-res]

                ;; Verify Bob (Has Email)
                (is (= bob-iri (-> row-bob (.getValue "friend") .stringValue)))
                (is (= "bob@example.com" (-> row-bob (.getValue "mail") .stringValue)))

                ;; Verify Charlie (No Email - OPTIONAL worked end-to-end in Rama)
                (is (= charlie-iri (-> row-charlie (.getValue "friend") .stringValue)))
                (is (nil? (.getValue row-charlie "mail")) "Charlie should have nil email"))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-filter
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            ;; Unique age predicate: the FILTER query scans it unscoped.
            age-iri (unique-iri "age")
            age-prop (.createIRI VF age-iri)
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data for Filter"
            (.begin conn)
            ;; Alice is 25, Bob is 30, Charlie is 20
            (.add conn alice age-prop (.createLiteral VF "25" int-type) (into-array Resource []))
            (.add conn bob age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
            (.add conn charlie age-prop (.createLiteral VF "20" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL FILTER (Numeric GT)"
            ;; Find people older than 22
            (let [sparql (format "SELECT ?person ?age WHERE {
                                    ?person <%s> ?age .
                                    FILTER (?age > 22)
                                  }" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res   (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]

              ;; Should find Alice (25) and Bob (30), but NOT Charlie (20)
              (is (= 2 (count res)))

              (let [ages (map #(-> % (.getValue "age") .stringValue Integer/parseInt) res)]
                (is (some #{25} ages))
                (is (some #{30} ages))
                (is (not-any? #{20} ages)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-bind-math
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            item1 (.createIRI VF "http://ex/bind-item1")
            item2 (.createIRI VF "http://ex/bind-item2")
            ;; Unique predicates: the BIND query scans them unscoped.
            price-iri (unique-iri "price")
            qty-iri (unique-iri "quantity")
            price (.createIRI VF price-iri)
            qty (.createIRI VF qty-iri)
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data for BIND"
            (.begin conn)
            ;; Item1: price 10, quantity 3
            (.add conn item1 price (.createLiteral VF "10" int-type) (into-array Resource []))
            (.add conn item1 qty (.createLiteral VF "3" int-type) (into-array Resource []))
            ;; Item2: price 25, quantity 2
            (.add conn item2 price (.createLiteral VF "25" int-type) (into-array Resource []))
            (.add conn item2 qty (.createLiteral VF "2" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL BIND with Math (multiply)"
            ;; Calculate total = price * quantity
            (let [sparql (format "SELECT ?item ?total WHERE {
                                    ?item <%s> ?p .
                                    ?item <%s> ?q .
                                    BIND(?p * ?q AS ?total)
                                  }" price-iri qty-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 2 (count res)))

              ;; Sort by item for stable assertions
              (let [sorted (sort-by #(-> % (.getValue "item") .stringValue) res)
                    [r1 r2] sorted]
                ;; Item1: 10 * 3 = 30
                (is (= "http://ex/bind-item1" (-> r1 (.getValue "item") .stringValue)))
                (is (= 30.0 (-> r1 (.getValue "total") .doubleValue)))
                ;; Item2: 25 * 2 = 50
                (is (= "http://ex/bind-item2" (-> r2 (.getValue "item") .stringValue)))
                (is (= 50.0 (-> r2 (.getValue "total") .doubleValue))))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-bind-coalesce
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subjects + unique Person class: the query is scoped by
            ;; the class, and name/nickname patterns by the unique subjects.
            alice-iri (unique-iri "alice")
            bob-iri (unique-iri "bob")
            person-iri (unique-iri "Person")
            alice (.createIRI VF alice-iri)
            bob (.createIRI VF bob-iri)
            person (.createIRI VF person-iri)
            nickname (.createIRI VF "http://ex/nickname")
            name-prop (.createIRI VF "http://ex/name")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]
        (try
          (testing "Setup Data for COALESCE"
            (.begin conn)
            ;; Alice has both name and nickname
            (.add conn alice rdf-type person (into-array Resource []))
            (.add conn alice name-prop (.createLiteral VF "Alice Smith") (into-array Resource []))
            (.add conn alice nickname (.createLiteral VF "Ali") (into-array Resource []))
            ;; Bob has only name, no nickname
            (.add conn bob rdf-type person (into-array Resource []))
            (.add conn bob name-prop (.createLiteral VF "Bob Jones") (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL BIND with COALESCE"
            ;; Use nickname if available, otherwise use name
            (let [sparql (format "SELECT ?person ?display WHERE {
                                    ?person a <%s> .
                                    ?person <http://ex/name> ?name .
                                    OPTIONAL { ?person <http://ex/nickname> ?nick }
                                    BIND(COALESCE(?nick, ?name) AS ?display)
                                  }" person-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 2 (count res)))

              ;; Sort by person for stable assertions ("alice-..." < "bob-...")
              (let [sorted (sort-by #(-> % (.getValue "person") .stringValue) res)
                    [alice-row bob-row] sorted]
                ;; Alice should show nickname "Ali"
                (is (= alice-iri (-> alice-row (.getValue "person") .stringValue)))
                (is (= "Ali" (-> alice-row (.getValue "display") .stringValue)))
                ;; Bob should show name "Bob Jones" (no nickname)
                (is (= bob-iri (-> bob-row (.getValue "person") .stringValue)))
                (is (= "Bob Jones" (-> bob-row (.getValue "display") .stringValue))))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-clear-specific-context
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique graphs: all queries are scoped by context.
            s (.createIRI VF (unique-iri "clear-ctx-s"))
            p (.createIRI VF "http://ex/p")
            o1 (.createLiteral VF "value1")
            o2 (.createLiteral VF "value2")
            g1 (.createIRI VF (unique-iri "clear-g1"))
            g2 (.createIRI VF (unique-iri "clear-g2"))]
        (try
          (testing "Setup Data in Multiple Contexts"
            (.begin conn)
            (.add conn s p o1 (into-array Resource [g1]))
            (.add conn s p o2 (into-array Resource [g2]))
            (.commit conn)
            (wait-mb!))

          (testing "Verify Data Before Clear"
            (let [stmts-g1 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g1]))]
                             (vec (iterator-seq iter)))
                  stmts-g2 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g2]))]
                             (vec (iterator-seq iter)))]
              (is (= 1 (count stmts-g1)))
              (is (= 1 (count stmts-g2)))))

          (testing "Clear Specific Context g1"
            (.begin conn)
            (.clear conn (into-array Resource [g1]))
            (.commit conn)
            ;; Wait for clear operation
            (wait-mb!))

          (testing "Verify g1 is Empty, g2 is Intact"
            (let [stmts-g1 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g1]))]
                             (vec (iterator-seq iter)))
                  stmts-g2 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g2]))]
                             (vec (iterator-seq iter)))]
              (is (= 0 (count stmts-g1)) "g1 should be cleared")
              (is (= 1 (count stmts-g2)) "g2 should still have data")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-order-by
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            dave (.createIRI VF "http://ex/dave")
            ;; Unique predicates: the ORDER BY queries scan them unscoped.
            age-iri (unique-iri "age")
            name-iri (unique-iri "name")
            age-prop (.createIRI VF age-iri)
            name-prop (.createIRI VF name-iri)
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data for ORDER BY"
            (.begin conn)
            ;; Alice: age 25
            (.add conn alice age-prop (.createLiteral VF "25" int-type) (into-array Resource []))
            (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
            ;; Bob: age 30
            (.add conn bob age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
            (.add conn bob name-prop (.createLiteral VF "Bob") (into-array Resource []))
            ;; Charlie: age 20
            (.add conn charlie age-prop (.createLiteral VF "20" int-type) (into-array Resource []))
            (.add conn charlie name-prop (.createLiteral VF "Charlie") (into-array Resource []))
            ;; Dave: age 25 (same as Alice for tie-breaking test)
            (.add conn dave age-prop (.createLiteral VF "25" int-type) (into-array Resource []))
            (.add conn dave name-prop (.createLiteral VF "Dave") (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL ORDER BY ASC"
            (let [sparql (format "SELECT ?person ?age WHERE {
                                    ?person <%s> ?age .
                                  } ORDER BY ASC(?age)" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 4 (count res)))
              ;; Should be ordered: Charlie(20), Alice/Dave(25), Bob(30)
              (let [ages (mapv #(-> % (.getValue "age") .intValue) res)]
                (is (= 20 (first ages)) "First should be youngest (20)")
                (is (= 30 (last ages)) "Last should be oldest (30)")
                (is (= ages (sort ages)) "Ages should be in ascending order"))))

          (testing "SPARQL ORDER BY DESC"
            (let [sparql (format "SELECT ?person ?age WHERE {
                                    ?person <%s> ?age .
                                  } ORDER BY DESC(?age)" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 4 (count res)))
              ;; Should be ordered: Bob(30), Alice/Dave(25), Charlie(20)
              (let [ages (mapv #(-> % (.getValue "age") .intValue) res)]
                (is (= 30 (first ages)) "First should be oldest (30)")
                (is (= 20 (last ages)) "Last should be youngest (20)")
                (is (= ages (reverse (sort ages))) "Ages should be in descending order"))))

          (testing "SPARQL ORDER BY with LIMIT"
            (let [sparql (format "SELECT ?person ?age WHERE {
                                    ?person <%s> ?age .
                                  } ORDER BY ASC(?age) LIMIT 2" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 2 (count res)))
              ;; Should be the two youngest
              (let [ages (mapv #(-> % (.getValue "age") .intValue) res)]
                (is (= 20 (first ages)) "First should be youngest (20)")
                (is (<= (second ages) 25) "Second should be 25 or less"))))

          (testing "SPARQL ORDER BY multiple columns"
            ;; Order by age ASC, then name ASC (for tie-breaking)
            (let [sparql (format "SELECT ?person ?name ?age WHERE {
                                    ?person <%s> ?age .
                                    ?person <%s> ?name .
                                  } ORDER BY ASC(?age) ASC(?name)" age-iri name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 4 (count res)))
              ;; Expected order: Charlie(20), Alice(25), Dave(25), Bob(30)
              (let [names (mapv #(-> % (.getValue "name") .stringValue) res)]
                (is (= "Charlie" (first names)) "First should be Charlie (youngest)")
                (is (= "Bob" (last names)) "Last should be Bob (oldest)")
                ;; Alice and Dave both have age 25, Alice should come first alphabetically
                (is (= "Alice" (second names)) "Alice should come before Dave (same age, alphabetical)")
                (is (= "Dave" (nth names 2)) "Dave should come after Alice"))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-clear-all-contexts
  ;; PRIVATE IPC: this test clears the ENTIRE store (.clear with no contexts)
  ;; and asserts store-wide emptiness via unscoped scans. That is inherently
  ;; incompatible with the shared cluster, so it keeps its own IPC.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            s (.createIRI VF "http://ex/s")
            p (.createIRI VF "http://ex/p")
            o1 (.createLiteral VF "value1")
            o2 (.createLiteral VF "value2")
            o3 (.createLiteral VF "default-value")
            g1 (.createIRI VF "http://ex/g1")
            g2 (.createIRI VF "http://ex/g2")]

        (testing "Setup Data in Multiple Contexts and Default Graph"
          (.begin conn)
          (.add conn s p o1 (into-array Resource [g1]))
          (.add conn s p o2 (into-array Resource [g2]))
          (.add conn s p o3 (into-array Resource []))  ;; Default graph
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "Verify Data Before Clear"
          (let [all-stmts (with-open [iter (.getStatements conn nil nil nil true (into-array Resource []))]
                            (vec (iterator-seq iter)))]
            (is (= 3 (count all-stmts)))))

        (testing "Clear All Contexts"
          (.begin conn)
          (.clear conn (into-array Resource []))  ;; Empty array = clear all
          (.commit conn)
          ;; Wait for clear operations (3 contexts cleared)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 6))

        (testing "Verify All Data is Gone"
          (let [all-stmts (with-open [iter (.getStatements conn nil nil nil true (into-array Resource []))]
                            (vec (iterator-seq iter)))]
            (is (= 0 (count all-stmts)) "All data should be cleared")))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-distinct
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            ;; Unique predicates: the DISTINCT queries scan them unscoped.
            knows-iri (unique-iri "knows")
            likes-iri (unique-iri "likes")
            knows (.createIRI VF knows-iri)
            likes (.createIRI VF likes-iri)
            music (.createIRI VF "http://ex/music")
            sports (.createIRI VF "http://ex/sports")]
        (try
          (testing "Setup Data with Duplicates"
            (.begin conn)
            ;; Alice knows Bob (will appear in results)
            (.add conn alice knows bob (into-array Resource []))
            ;; Alice likes Music
            (.add conn alice likes music (into-array Resource []))
            ;; Alice likes Sports (same subject, will create duplicate ?person)
            (.add conn alice likes sports (into-array Resource []))
            ;; Bob knows Charlie
            (.add conn bob knows charlie (into-array Resource []))
            ;; Bob likes Music (same as Alice, will create duplicate ?hobby)
            (.add conn bob likes music (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL SELECT DISTINCT single variable"
            ;; Without DISTINCT, would return: alice, alice, bob (alice appears twice due to two likes)
            ;; With DISTINCT, should return: alice, bob
            (let [sparql (format "SELECT DISTINCT ?person WHERE {
                                    ?person <%s> ?hobby .
                                  }" likes-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))
                  persons (set (map #(str (.getValue % "person")) res))]
              (is (= 2 (count res)) "DISTINCT should return 2 unique persons")
              (is (= #{"http://ex/alice" "http://ex/bob"} persons))))

          (testing "SPARQL SELECT DISTINCT with multiple variables"
            ;; Each person-hobby combination should be unique
            (let [sparql (format "SELECT DISTINCT ?person ?hobby WHERE {
                                    ?person <%s> ?hobby .
                                  }" likes-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              ;; alice-music, alice-sports, bob-music = 3 unique combinations
              (is (= 3 (count res)) "Should have 3 unique person-hobby combinations")))

          (testing "SPARQL SELECT DISTINCT with no duplicates"
            ;; Query that naturally has no duplicates should still work
            (let [sparql (format "SELECT DISTINCT ?s ?o WHERE {
                                    ?s <%s> ?o .
                                  }" knows-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 2 (count res)) "Should have 2 knows relationships")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-transaction-rollback
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subject: all assertions are scoped by it.
            s (.createIRI VF (unique-iri "rollback-s"))
            p (.createIRI VF "http://ex/p")
            o-committed (.createLiteral VF "committed")
            o-rolled-back (.createLiteral VF "rolled-back")]
        (try
          (testing "Committed transaction persists data"
            (.begin conn)
            (.add conn s p o-committed (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            (let [stmts (with-open [iter (.getStatements conn s p nil true (into-array Resource []))]
                          (vec (iterator-seq iter)))]
              (is (= 1 (count stmts)))
              (is (= "committed" (.getLabel (.getObject (first stmts)))))))

          (testing "Rolled back transaction discards data"
            (.begin conn)
            (.add conn s p o-rolled-back (into-array Resource []))
            ;; Verify data is buffered but not yet visible in queries
            ;; (queries only see committed state)
            (.rollback conn)

            ;; After rollback, the added triple should NOT be persisted
            ;; Only the committed triple should exist
            (let [stmts (with-open [iter (.getStatements conn s p nil true (into-array Resource []))]
                          (vec (iterator-seq iter)))]
              (is (= 1 (count stmts)) "Only committed data should be visible")
              (is (= "committed" (.getLabel (.getObject (first stmts))))
                  "Rolled back data should not appear")))

          (testing "Multiple operations rolled back together"
            (.begin conn)
            (.add conn s (.createIRI VF "http://ex/p1") (.createLiteral VF "v1") (into-array Resource []))
            (.add conn s (.createIRI VF "http://ex/p2") (.createLiteral VF "v2") (into-array Resource []))
            (.add conn s (.createIRI VF "http://ex/p3") (.createLiteral VF "v3") (into-array Resource []))
            (.rollback conn)

            ;; None of these should be visible
            (let [stmts (with-open [iter (.getStatements conn s nil nil true (into-array Resource []))]
                          (vec (iterator-seq iter)))]
              (is (= 1 (count stmts)) "Only the original committed triple should exist")
              (is (= "committed" (.getLabel (.getObject (first stmts)))))))

          (testing "Transaction after rollback works correctly"
            (.begin conn)
            (.add conn s (.createIRI VF "http://ex/after-rollback") (.createLiteral VF "success") (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            (let [stmts (with-open [iter (.getStatements conn s nil nil true (into-array Resource []))]
                          (vec (iterator-seq iter)))]
              (is (= 2 (count stmts)) "New transaction after rollback should work")
              (let [values (set (map #(.getLabel (.getObject %)) stmts))]
                (is (contains? values "committed"))
                (is (contains? values "success")))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-query-timeout-configuration
  ;; Unique subject: the visibility checks are scoped by it (the shared store
  ;; contains other tests' data, so unscoped counts would be wrong).
  (let [s (.createIRI VF (unique-iri "timeout-s"))
        p (.createIRI VF "http://ex/p")
        o (.createLiteral VF "test")]

    (testing "Default timeout works for normal queries"
      ;; Create sail with default timeout
      (let [sail (rsail/create-rama-sail *ipc* module-name)
            repo (SailRepository. sail)]
        (.init repo)
        (let [conn (.getConnection repo)]
          (.begin conn)
          (.add conn s p o (into-array Resource []))
          (.commit conn)
          (wait-mb!)

          ;; Query should complete within default timeout
          (let [stmts (with-open [iter (.getStatements conn s nil nil true (into-array Resource []))]
                        (vec (iterator-seq iter)))]
            (is (= 1 (count stmts))))
          (.close conn))
        (.shutDown repo)))

    (testing "Custom timeout is configurable"
      ;; Create sail with explicit timeout
      (let [sail (rsail/create-rama-sail *ipc* module-name {:query-timeout-ms 30000})
            repo (SailRepository. sail)]
        (.init repo)
        (let [conn (.getConnection repo)
              stmts (with-open [iter (.getStatements conn s nil nil true (into-array Resource []))]
                      (vec (iterator-seq iter)))]
          ;; Should still work - the earlier testing block added data
          (is (= 1 (count stmts)))
          (.close conn))
        (.shutDown repo)))

    (testing "Very short timeout throws QueryInterruptedException"
      ;; Create sail with 1ms timeout - this should timeout on any query
      (let [sail (rsail/create-rama-sail *ipc* module-name {:query-timeout-ms 1})
            repo (SailRepository. sail)]
        (.init repo)
        (let [conn (.getConnection repo)]
          ;; Query should timeout and throw
          (is (thrown? QueryInterruptedException
                       (.getStatements conn nil nil nil true (into-array Resource []))))
          (.close conn))
        (.shutDown repo)))))

(deftest test-datatype-equality-in-queries
  ;; Verifies that canonicalized numeric values work correctly in joins and queries.
  ;; This is a critical RDF semantics requirement: "1"^^xsd:integer and "01"^^xsd:integer
  ;; must be treated as the same value.
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            ;; Unique predicate: the queries scan it unscoped with exact counts.
            has-score-iri (unique-iri "hasScore")
            has-score (.createIRI VF has-score-iri)
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data with Numeric Values"
            (.begin conn)
            ;; Alice has score "01" (with leading zero)
            (.add conn alice has-score (.createLiteral VF "01" int-type) (into-array Resource []))
            ;; Bob has score "1" (canonical form)
            (.add conn bob has-score (.createLiteral VF "1" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "Query with canonical value matches non-canonical storage"
            ;; Query for score "1" should match Alice's "01"
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                                  }" has-score-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              ;; Both Alice and Bob should match because "01" was canonicalized to "1"
              (is (= 2 (count res)) "Both alice and bob should match query for '1'")))

          (testing "Query with non-canonical value also matches"
            ;; Query for score "01" should also work (query values are also canonicalized)
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                                  }" has-score-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 2 (count res)) "Query with '01' should also match both")))

          (testing "Filter comparison with canonicalized values"
            ;; Query with FILTER should also work with canonicalized values
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> ?score .
                                    FILTER(?score = \"001\"^^<http://www.w3.org/2001/XMLSchema#integer>)
                                  }" has-score-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              ;; Both should match since filter comparison should also canonicalize
              (is (= 2 (count res)) "Filter with '001' should match both alice and bob")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-if-function
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            ;; Unique predicate: the IF query scans it unscoped.
            age-iri (unique-iri "age")
            age-prop (.createIRI VF age-iri)
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
            (.add conn bob age-prop (.createLiteral VF "20" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "SPARQL IF function"
            (let [sparql (format "SELECT ?person ?cat WHERE {
                                    ?person <%s> ?age .
                                    BIND(IF(?age > 25, \"senior\", \"junior\") AS ?cat)
                                  }" age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

              (is (= 2 (count res)))
              (let [sorted (sort-by #(-> % (.getValue "person") .stringValue) res)
                    [alice-row bob-row] sorted]
                (is (= "senior" (-> alice-row (.getValue "cat") .stringValue)))
                (is (= "junior" (-> bob-row (.getValue "cat") .stringValue))))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-type-checks
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subject: the queries select ALL properties of alice, so
            ;; any other test writing to a shared alice would leak in.
            alice-iri (unique-iri "alice")
            alice (.createIRI VF alice-iri)
            bob (.createIRI VF "http://ex/bob")
            knows (.createIRI VF "http://ex/knows")
            name-prop (.createIRI VF "http://ex/name")
            age-prop (.createIRI VF "http://ex/age")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice knows bob (into-array Resource []))
            (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
            (.add conn alice age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "isIRI filter"
            (let [sparql (format "SELECT ?val WHERE {
                                    <%s> ?p ?val .
                                    FILTER(isIRI(?val))
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "http://ex/bob" (-> res first (.getValue "val") .stringValue)))))

          (testing "isLiteral filter"
            (let [sparql (format "SELECT ?val WHERE {
                                    <%s> ?p ?val .
                                    FILTER(isLiteral(?val))
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 2 (count res)))))

          (testing "isNumeric filter"
            (let [sparql (format "SELECT ?val WHERE {
                                    <%s> ?p ?val .
                                    FILTER(isNumeric(?val))
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "30" (-> res first (.getValue "val") .stringValue)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-lang-datatype
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subject scopes all queries below.
            alice-iri (unique-iri "alice")
            alice (.createIRI VF alice-iri)
            label (.createIRI VF "http://ex/label")
            score (.createIRI VF "http://ex/score")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice label (.createLiteral VF "Alice" "en") (into-array Resource []))
            (.add conn alice score (.createLiteral VF "95" int-type) (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "LANG function"
            (let [sparql (format "SELECT ?lang WHERE {
                                    <%s> <http://ex/label> ?lbl .
                                    BIND(LANG(?lbl) AS ?lang)
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "en" (-> res first (.getValue "lang") .stringValue)))))

          (testing "DATATYPE function"
            (let [sparql (format "SELECT ?dt WHERE {
                                    <%s> <http://ex/score> ?s .
                                    BIND(DATATYPE(?s) AS ?dt)
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "http://www.w3.org/2001/XMLSchema#integer" (-> res first (.getValue "dt") .stringValue)))))

          (testing "LANGMATCHES filter"
            (let [sparql (format "SELECT ?lbl WHERE {
                                    <%s> <http://ex/label> ?lbl .
                                    FILTER(LANGMATCHES(LANG(?lbl), \"en\"))
                                  }" alice-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "Alice" (-> res first (.getValue "lbl") .stringValue)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-regex
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            ;; Unique predicate: the REGEX queries scan it unscoped.
            name-iri (unique-iri "name")
            name-prop (.createIRI VF name-iri)]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
            (.add conn bob name-prop (.createLiteral VF "Bob") (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "REGEX filter case-insensitive"
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> ?name .
                                    FILTER(REGEX(?name, \"^ali\", \"i\"))
                                  }" name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "http://ex/alice" (-> res first (.getValue "person") .stringValue)))))

          (testing "REGEX filter no flags"
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> ?name .
                                    FILTER(REGEX(?name, \"ob\"))
                                  }" name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)))
              (is (= "http://ex/bob" (-> res first (.getValue "person") .stringValue)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-in-not-in
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            ;; Unique classes: `?s a ?type` is unscoped over the shared store,
            ;; so the type IRIs themselves provide the isolation.
            person-iri (unique-iri "Person")
            agent-iri (unique-iri "Agent")
            bot-iri (unique-iri "Bot")
            person (.createIRI VF person-iri)
            agent-type (.createIRI VF agent-iri)
            bot (.createIRI VF bot-iri)]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice rdf-type person (into-array Resource []))
            (.add conn bob rdf-type agent-type (into-array Resource []))
            (.add conn charlie rdf-type bot (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "IN filter"
            (let [sparql (format "SELECT ?s WHERE {
                                    ?s a ?type .
                                    FILTER(?type IN (<%s>, <%s>))
                                  }" person-iri agent-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))
                  subjects (set (map #(-> % (.getValue "s") .stringValue) res))]
              (is (= 2 (count res)))
              (is (contains? subjects "http://ex/alice"))
              (is (contains? subjects "http://ex/bob"))))

          (testing "NOT IN filter"
            ;; The store is shared, so a bare NOT IN would match every typed
            ;; resource from other tests. Scope ?type to this test's unique
            ;; classes first; NOT IN then excludes Bot as before.
            (let [sparql (format "SELECT ?s WHERE {
                                    ?s a ?type .
                                    FILTER(?type IN (<%s>, <%s>, <%s>))
                                    FILTER(?type NOT IN (<%s>))
                                  }" person-iri agent-iri bot-iri bot-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))
                  subjects (set (map #(-> % (.getValue "s") .stringValue) res))]
              (is (= 2 (count res)))
              (is (contains? subjects "http://ex/alice"))
              (is (contains? subjects "http://ex/bob"))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-sparql-having
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            ;; Unique classes: `?s a ?type` is unscoped, and HAVING(COUNT > 1)
            ;; would match other tests' classes (e.g. 50 Items) otherwise.
            person-iri (unique-iri "Person")
            bot-iri (unique-iri "Bot")
            person (.createIRI VF person-iri)
            bot (.createIRI VF bot-iri)]
        (try
          (testing "Setup Data"
            (.begin conn)
            (.add conn alice rdf-type person (into-array Resource []))
            (.add conn bob rdf-type person (into-array Resource []))
            (.add conn charlie rdf-type bot (into-array Resource []))
            (.commit conn)
            (wait-mb!))

          (testing "GROUP BY with HAVING"
            ;; Scope ?type to this test's unique classes before grouping so
            ;; other tests' typed data can't reach the HAVING clause.
            (let [sparql (format "SELECT ?type (COUNT(*) AS ?c) WHERE {
                                    ?s a ?type .
                                    FILTER(?type IN (<%s>, <%s>))
                                  } GROUP BY ?type HAVING(COUNT(*) > 1)" person-iri bot-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              ;; Only Person has count > 1 (alice, bob)
              (is (= 1 (count res)))
              (is (= person-iri (-> res first (.getValue "type") .stringValue)))
              (is (= 2 (-> res first (.getValue "c") .intValue)))))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-bnode-skolemization
  ;; Verifies that blank nodes are properly skolemized:
  ;; - Same BNode ID within a transaction refers to the same node
  ;; - Same BNode ID across transactions refers to different nodes
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique predicates: every query below scans them unscoped and
            ;; asserts exact counts.
            knows-iri (unique-iri "knows")
            name-iri (unique-iri "name")
            age-iri (unique-iri "age")
            knows (.createIRI VF knows-iri)
            name-prop (.createIRI VF name-iri)
            age-prop (.createIRI VF age-iri)]
        (try
          (testing "Same BNode ID within transaction refers to same node"
            (.begin conn)
            ;; Use _:person in multiple statements - should be the same node
            (let [person-bnode (.createBNode VF "person")]
              (.add conn person-bnode name-prop (.createLiteral VF "Alice") (into-array Resource []))
              (.add conn person-bnode age-prop (.createLiteral VF "30") (into-array Resource [])))
            (.commit conn)
            (wait-mb!)

            ;; Query: find nodes that have both name and age
            (let [sparql (format "SELECT ?person WHERE {
                                    ?person <%s> ?name .
                                    ?person <%s> ?age .
                                  }" name-iri age-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)) "Same BNode used twice should result in one node with both properties")))

          (testing "Same BNode ID across transactions refers to different nodes"
            ;; First transaction: add _:x with name "Bob"
            (.begin conn)
            (.add conn (.createBNode VF "x") name-prop (.createLiteral VF "Bob") (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            ;; Second transaction: add _:x with name "Charlie"
            (.begin conn)
            (.add conn (.createBNode VF "x") name-prop (.createLiteral VF "Charlie") (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            ;; Both should exist as separate nodes
            (let [sparql (format "SELECT ?name WHERE { ?person <%s> ?name }" name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))
                  names (set (map #(-> % (.getValue "name") .stringValue) res))]
              ;; Should have Alice (from first test), Bob, and Charlie
              (is (contains? names "Alice"))
              (is (contains? names "Bob"))
              (is (contains? names "Charlie"))
              (is (= 3 (count names)) "Each transaction's _:x should be a different node")))

          (testing "BNode consistency within complex transaction"
            (.begin conn)
            ;; Create a chain: _:a knows _:b, _:b knows _:c
            (let [a (.createBNode VF "a")
                  b (.createBNode VF "b")
                  c (.createBNode VF "c")]
              (.add conn a knows b (into-array Resource []))
              (.add conn b knows c (into-array Resource []))
              (.add conn a name-prop (.createLiteral VF "NodeA") (into-array Resource []))
              (.add conn c name-prop (.createLiteral VF "NodeC") (into-array Resource [])))
            (.commit conn)
            (wait-mb!)

            ;; Query: find someone who knows someone who knows someone else
            (let [sparql (format "SELECT ?start ?end WHERE {
                                    ?start <%s> ?middle .
                                    ?middle <%s> ?end .
                                    ?start <%s> \"NodeA\" .
                                    ?end <%s> \"NodeC\" .
                                  }" knows-iri knows-iri name-iri name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)) "Should find the chain from NodeA to NodeC through middle node")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-phase1-correctness-fixes
  ;; End-to-end coverage for the Phase 1 fail-fast fixes: FunctionCall
  ;; fallback, FROM dataset fallback, repeated-variable patterns, and
  ;; mixed-type ORDER BY.
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            ;; Unique subjects, predicates, and graph: the CONTAINS/ORDER BY
            ;; queries scan predicates unscoped, the self-loop pattern must
            ;; only match this test's edge, and FROM needs a private graph.
            alice-iri (unique-iri "alice")
            bob-iri (unique-iri "bob")
            knows-iri (unique-iri "knows")
            name-iri (unique-iri "name")
            val-iri (unique-iri "val")
            graph-a-iri (unique-iri "graphA")
            alice (.createIRI VF alice-iri)
            bob (.createIRI VF bob-iri)
            knows (.createIRI VF knows-iri)
            name-prop (.createIRI VF name-iri)
            val-prop (.createIRI VF val-iri)
            graph-a (.createIRI VF graph-a-iri)
            xsd-int (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]
        (try
          (.begin conn)
          ;; Default graph: a self-loop and a normal edge
          (.add conn alice knows alice (into-array Resource []))
          (.add conn alice knows bob (into-array Resource []))
          (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
          (.add conn bob name-prop (.createLiteral VF "Bob") (into-array Resource []))
          ;; Mixed-type objects on one predicate (used to kill ORDER BY)
          (.add conn alice val-prop (.createLiteral VF "5" xsd-int) (into-array Resource []))
          (.add conn bob val-prop (.createLiteral VF "abc") (into-array Resource []))
          ;; A named-graph triple for the FROM test
          (.add conn alice name-prop (.createLiteral VF "GraphAlice") (into-array Resource [graph-a]))
          (.commit conn)
          (wait-mb!)

          (testing "FILTER with SPARQL function falls back to RDF4J and is correct"
            ;; Previously compiled to an unevaluatable :function-call that
            ;; silently returned unbound (zero or wrong rows).
            (let [sparql (format "SELECT ?s WHERE { ?s <%s> ?n . FILTER(CONTAINS(?n, \"ob\")) }" name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)) "Only Bob's name contains 'ob'")
              (is (= bob (.getValue (first res) "s")))))

          (testing "FROM <graph> is honored via fallback instead of silently ignored"
            (let [sparql (format "SELECT ?n FROM <%s> WHERE { ?s <%s> ?n }" graph-a-iri name-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)) "Only the named-graph triple is in scope")
              (is (= "GraphAlice" (.stringValue (.getValue (first res) "n"))))))

          (testing "repeated-variable pattern matches only self-loops"
            (let [sparql (format "SELECT ?x WHERE { ?x <%s> ?x }" knows-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 1 (count res)) "Only alice knows herself")
              (is (= alice (.getValue (first res) "x")))))

          (testing "ORDER BY over mixed-type values returns all rows sorted"
            ;; Previously threw ClassCastException inside the order topology.
            (let [sparql (format "SELECT ?v WHERE { ?s <%s> ?v } ORDER BY ?v" val-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 2 (count res)) "Both the numeric and the string value are returned")
              (is (= ["5" "abc"]
                     (mapv #(.stringValue (.getValue % "v")) res))
                  "Numeric literal sorts before plain literal")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))

(deftest test-limit-early-termination
  ;; Verifies that LIMIT queries return the correct number of results
  ;; and that the early-termination optimization via :result-limit works correctly.
  (let [sail (rsail/create-rama-sail *ipc* module-name)
        repo (SailRepository. sail)]
    (.init repo)
    (try
      (let [conn (.getConnection repo)
            type-prop (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            ;; Unique class, label predicate, and subject base: the "returns
            ;; all results" assertion needs exactly this test's 50 items.
            item-type-iri (unique-iri "Item")
            label-iri (unique-iri "label")
            item-base (unique-iri "item")
            item-type (.createIRI VF item-type-iri)
            label-prop (.createIRI VF label-iri)]
        (try
          ;; Load 100 triples: 50 items with type and label
          (.begin conn)
          (doseq [i (range 50)]
            (let [subj (.createIRI VF (str item-base "-" i))]
              (.add conn subj type-prop item-type (into-array Resource []))
              (.add conn subj label-prop (.createLiteral VF (str "Item " i)) (into-array Resource []))))
          (.commit conn)
          (wait-mb!)

          (testing "LIMIT returns exactly the requested number of results"
            ;; Unscoped pattern is fine here: this test alone guarantees the
            ;; store has >= 5 rows, and extra rows cannot change the count.
            (let [sparql "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5"
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 5 (count res))
                  "LIMIT 5 should return exactly 5 results")))

          (testing "OFFSET + LIMIT returns correct count"
            ;; Unscoped is fine: store guaranteed to have >= 8 rows.
            (let [sparql "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5 OFFSET 3"
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 5 (count res))
                  "OFFSET 3 LIMIT 5 should return exactly 5 results")))

          (testing "LIMIT with PROJECT returns correct count"
            (let [sparql (format "SELECT ?s WHERE { ?s <%s> ?label } LIMIT 10" label-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 10 (count res))
                  "SELECT ?s ... LIMIT 10 should return exactly 10 results")))

          (testing "ASK query works correctly (uses synthetic LIMIT 1)"
            (let [sparql (format "ASK { ?s <%s> ?o }" label-iri)
                  query (.prepareBooleanQuery conn sparql)
                  res (.evaluate query)]
              (is (true? res)
                  "ASK should return true when matching triples exist")))

          (testing "ASK query returns false when no match"
            ;; Unique IRIs guarantee no test ever writes this triple.
            (let [sparql (format "ASK { <%s> <%s> ?o }"
                                 (unique-iri "nonexistent") (unique-iri "nonexistent-p"))
                  query (.prepareBooleanQuery conn sparql)
                  res (.evaluate query)]
              (is (false? res)
                  "ASK should return false when no matching triples exist")))

          (testing "LIMIT larger than result set returns all results"
            ;; Scoped by the unique item class: must return exactly 50.
            (let [sparql (format "SELECT ?s WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <%s> } LIMIT 1000"
                                 item-type-iri)
                  query (.prepareTupleQuery conn sparql)
                  res (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]
              (is (= 50 (count res))
                  "LIMIT larger than actual results should return all 50 items")))
          (finally
            (.close conn))))
      (finally
        (.shutDown repo)))))
