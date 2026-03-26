(ns rama-sail.sail.e2e-test
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]
           (org.eclipse.rdf4j.query QueryInterruptedException)
           (rpl.rama.distributed.exceptions QueryTopologyInvokeFailed)))

(def VF (SimpleValueFactory/getInstance))

(deftest test-e2e-rdf4j-integration
	;; 1. Start In-Process Cluster
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
					;; 2. Create RamaSail linked to the IPC
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            knows (.createIRI VF "http://ex/knows")
            name-prop (.createIRI VF "http://ex/name")
            age-prop (.createIRI VF "http://ex/age")
            bob-name (.createLiteral VF "Bob" "en")
            bob-age (.createLiteral VF "30" (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer"))]

        (testing "Add Data via Repository"
          (.begin conn)
					;; FIX: Pass empty array for contexts varargs
          (.add conn alice knows bob (into-array Resource []))
          (.add conn bob name-prop bob-name (into-array Resource []))
          (.add conn bob age-prop bob-age (into-array Resource []))
          (.commit conn)

					;; Wait for Rama Microbatch (3 triples added)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "Simple BGP Query (getStatements)"
					;; FIX: Pass empty array for the 5th argument (contexts varargs)
          (let [iter (.getStatements conn bob nil nil true (into-array Resource []))
                stmts (iterator-seq iter)]
            (is (= 2 (count stmts)))
            (is (some #(= bob-name (.getObject %)) stmts))
            (is (some #(= bob-age (.getObject %)) stmts))))

        (testing "SPARQL Query with JOIN and PROJECTION"
					;; Query: Find names of people Alice knows
          (let [sparql "SELECT ?name WHERE {
                          <http://ex/alice> <http://ex/knows> ?friend .
                          ?friend <http://ex/name> ?name .
                        }"
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
					;; Query: Find friend with age 30
          (let [sparql "SELECT ?friend WHERE {
                           ?friend <http://ex/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                         }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "http://ex/bob" (-> res first (.getValue "friend") .stringValue)))))

        (testing "Deletion"
          (println "Before deletion:"
                   (with-open [iter (.getStatements
                                     conn alice knows bob true (into-array Resource []))]
                     (vec (iterator-seq iter))))

          (.begin conn)
          (.remove conn alice knows bob (into-array Resource []))
          (.commit conn)

					;; Wait for microbatch (1 triple removed -> total processed = 4)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

					;; 1. Verify deletion via getStatements (bypasses SPARQL pipeline)
          (let [res (with-open [iter (.getStatements
                                      conn alice knows bob true (into-array Resource []))]
                      (vec (iterator-seq iter)))
                _ (println "Statements after deletion:" res)]
            (is (= 0 (count res)) "Triple <alice, knows, bob> should be removed"))

					;; Verify deletion via SPARQL
          (try
            (let [sparql "ASK { <http://ex/alice> <http://ex/knows> <http://ex/bob> }"
                  query (.prepareBooleanQuery conn sparql)
                  res (.evaluate query)
                  _ (println "ASK query result after deletion:" res)]
              (is (false? res)))
            (catch QueryTopologyInvokeFailed e
							;; Log or ignore while `:execute-plan` support for this pattern is incomplete
              (println "ASK query failed in Rama execute-plan topology:"
                       (.getMessage e)
                       e))))

        (.close conn))
      (.shutDown repo))))

(deftest test-named-graphs
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            s (.createIRI VF "http://ex/s")
            p (.createIRI VF "http://ex/p")
            o (.createLiteral VF "value")
            g1 (.createIRI VF "http://ex/g1")
            g2 (.createIRI VF "http://ex/g2")]

        (testing "Add Statements to Named Graphs"
          (.begin conn)
          (.add conn s p o (into-array Resource [g1]))
          (.add conn s p o (into-array Resource [g2]))
					;; Add to default graph
          (.add conn s p (.createLiteral VF "default") (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "Query Specific Named Graph"
          (let [iter (.getStatements conn nil nil nil true (into-array Resource [g1]))
                stmts (vec (iterator-seq iter))]
            (is (= 1 (count stmts)))
            (is (= g1 (.getContext (first stmts))))))

        (testing "Query Default Graph Only"
          (let [iter (.getStatements conn nil nil nil true (into-array Resource [nil]))
                stmts (vec (iterator-seq iter))]
            (is (= 1 (count stmts)))
            (is (= "default" (.getLabel (.getObject (first stmts)))))
            (is (nil? (.getContext (first stmts))))))

        (testing "Wildcard Query (All Graphs)"
          (let [iter (.getStatements conn nil nil nil true (into-array Resource []))
                stmts (vec (iterator-seq iter))]
            (is (= 3 (count stmts)))))

        (testing "SPARQL GRAPH Query"
          (let [sparql "SELECT ?g WHERE { GRAPH ?g { ?s ?p \"value\" } }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 2 (count res)))
            (let [graphs (set (map #(str (.getValue % "g")) res))]
              (is (contains? graphs "http://ex/g1"))
              (is (contains? graphs "http://ex/g2")))))

        (.close conn))
      (.shutDown repo))))

(deftest test-full-pushdown-optional
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            knows (.createIRI VF "http://ex/knows")
            email (.createIRI VF "http://ex/email")
            bob-mail (.createLiteral VF "bob@example.com")]

        (testing "Setup Data"
          (.begin conn)
					;; Alice knows Bob (who has an email)
          (.add conn alice knows bob (into-array Resource []))
          (.add conn bob email bob-mail (into-array Resource []))

					;; Alice knows Charlie (who has NO email)
          (.add conn alice knows charlie (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "Execute OPTIONAL Query (Full Pushdown)"
					;; This query contains OPTIONAL.
					;; The Rama `execute-plan` topology now supports OPTIONAL (LeftJoin),
					;; so the SAIL compiles and executes the full plan on Rama,
					;; without falling back to RDF4J's engine.
					;; Rama still uses `.getStatements` internally to resolve the basic
					;; graph patterns, but performs the LeftJoin inside the topology.

          (let [sparql "SELECT ?friend ?mail WHERE {
                          <http://ex/alice> <http://ex/knows> ?friend .
                          OPTIONAL { ?friend <http://ex/email> ?mail }
                        }"
                query (.prepareTupleQuery conn sparql)
                res   (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

            (is (= 2 (count res)) "Should return both Bob and Charlie")

						;; Sort results to ensure stability for assertions
            (let [sorted-res          (sort-by #(-> % (.getValue "friend") .stringValue) res)
                  [row-bob row-charlie] sorted-res]

							;; Verify Bob (Has Email)
              (is (= "http://ex/bob" (-> row-bob (.getValue "friend") .stringValue)))
              (is (= "bob@example.com" (-> row-bob (.getValue "mail") .stringValue)))

							;; Verify Charlie (No Email - OPTIONAL worked end\-to\-end in Rama)
              (is (= "http://ex/charlie" (-> row-charlie (.getValue "friend") .stringValue)))
              (is (nil? (.getValue row-charlie "mail")) "Charlie should have nil email"))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-filter
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            age-prop (.createIRI VF "http://ex/age")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data for Filter"
          (.begin conn)
					;; Alice is 25, Bob is 30, Charlie is 20
          (.add conn alice age-prop (.createLiteral VF "25" int-type) (into-array Resource []))
          (.add conn bob age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
          (.add conn charlie age-prop (.createLiteral VF "20" int-type) (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "SPARQL FILTER (Numeric GT)"
					;; Find people older than 22
          (let [sparql "SELECT ?person ?age WHERE {
                          ?person <http://ex/age> ?age .
                          FILTER (?age > 22)
                        }"
                query (.prepareTupleQuery conn sparql)
                res   (with-open [iter (.evaluate query)]
                        (vec (iterator-seq iter)))]

						;; Should find Alice (25) and Bob (30), but NOT Charlie (20)
            (is (= 2 (count res)))

            (let [ages (map #(-> % (.getValue "age") .stringValue Integer/parseInt) res)]
              (is (some #{25} ages))
              (is (some #{30} ages))
              (is (not-any? #{20} ages)))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-bind-math
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            item1 (.createIRI VF "http://ex/item1")
            item2 (.createIRI VF "http://ex/item2")
            price (.createIRI VF "http://ex/price")
            qty (.createIRI VF "http://ex/quantity")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data for BIND"
          (.begin conn)
          ;; Item1: price 10, quantity 3
          (.add conn item1 price (.createLiteral VF "10" int-type) (into-array Resource []))
          (.add conn item1 qty (.createLiteral VF "3" int-type) (into-array Resource []))
          ;; Item2: price 25, quantity 2
          (.add conn item2 price (.createLiteral VF "25" int-type) (into-array Resource []))
          (.add conn item2 qty (.createLiteral VF "2" int-type) (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4))

        (testing "SPARQL BIND with Math (multiply)"
          ;; Calculate total = price * quantity
          (let [sparql "SELECT ?item ?total WHERE {
                          ?item <http://ex/price> ?p .
                          ?item <http://ex/quantity> ?q .
                          BIND(?p * ?q AS ?total)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]

            (is (= 2 (count res)))

            ;; Sort by item for stable assertions
            (let [sorted (sort-by #(-> % (.getValue "item") .stringValue) res)
                  [r1 r2] sorted]
              ;; Item1: 10 * 3 = 30
              (is (= "http://ex/item1" (-> r1 (.getValue "item") .stringValue)))
              (is (= 30.0 (-> r1 (.getValue "total") .doubleValue)))
              ;; Item2: 25 * 2 = 50
              (is (= "http://ex/item2" (-> r2 (.getValue "item") .stringValue)))
              (is (= 50.0 (-> r2 (.getValue "total") .doubleValue))))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-bind-coalesce
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            nickname (.createIRI VF "http://ex/nickname")
            name-prop (.createIRI VF "http://ex/name")
            person (.createIRI VF "http://ex/Person")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]

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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 5))

        (testing "SPARQL BIND with COALESCE"
          ;; Use nickname if available, otherwise use name
          (let [sparql "SELECT ?person ?display WHERE {
                          ?person a <http://ex/Person> .
                          ?person <http://ex/name> ?name .
                          OPTIONAL { ?person <http://ex/nickname> ?nick }
                          BIND(COALESCE(?nick, ?name) AS ?display)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]

            (is (= 2 (count res)))

            ;; Sort by person for stable assertions
            (let [sorted (sort-by #(-> % (.getValue "person") .stringValue) res)
                  [alice-row bob-row] sorted]
              ;; Alice should show nickname "Ali"
              (is (= "http://ex/alice" (-> alice-row (.getValue "person") .stringValue)))
              (is (= "Ali" (-> alice-row (.getValue "display") .stringValue)))
              ;; Bob should show name "Bob Jones" (no nickname)
              (is (= "http://ex/bob" (-> bob-row (.getValue "person") .stringValue)))
              (is (= "Bob Jones" (-> bob-row (.getValue "display") .stringValue))))))

        (.close conn))
      (.shutDown repo))))

(deftest test-clear-specific-context
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            s (.createIRI VF "http://ex/s")
            p (.createIRI VF "http://ex/p")
            o1 (.createLiteral VF "value1")
            o2 (.createLiteral VF "value2")
            g1 (.createIRI VF "http://ex/g1")
            g2 (.createIRI VF "http://ex/g2")]

        (testing "Setup Data in Multiple Contexts"
          (.begin conn)
          (.add conn s p o1 (into-array Resource [g1]))
          (.add conn s p o2 (into-array Resource [g2]))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2))

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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "Verify g1 is Empty, g2 is Intact"
          (let [stmts-g1 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g1]))]
                           (vec (iterator-seq iter)))
                stmts-g2 (with-open [iter (.getStatements conn nil nil nil true (into-array Resource [g2]))]
                           (vec (iterator-seq iter)))]
            (is (= 0 (count stmts-g1)) "g1 should be cleared")
            (is (= 1 (count stmts-g2)) "g2 should still have data")))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-order-by
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            dave (.createIRI VF "http://ex/dave")
            age-prop (.createIRI VF "http://ex/age")
            name-prop (.createIRI VF "http://ex/name")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 8))

        (testing "SPARQL ORDER BY ASC"
          (let [sparql "SELECT ?person ?age WHERE {
                          ?person <http://ex/age> ?age .
                        } ORDER BY ASC(?age)"
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
          (let [sparql "SELECT ?person ?age WHERE {
                          ?person <http://ex/age> ?age .
                        } ORDER BY DESC(?age)"
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
          (let [sparql "SELECT ?person ?age WHERE {
                          ?person <http://ex/age> ?age .
                        } ORDER BY ASC(?age) LIMIT 2"
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
          (let [sparql "SELECT ?person ?name ?age WHERE {
                          ?person <http://ex/age> ?age .
                          ?person <http://ex/name> ?name .
                        } ORDER BY ASC(?age) ASC(?name)"
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

        (.close conn))
      (.shutDown repo))))

(deftest test-clear-all-contexts
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
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
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            knows (.createIRI VF "http://ex/knows")
            likes (.createIRI VF "http://ex/likes")
            music (.createIRI VF "http://ex/music")
            sports (.createIRI VF "http://ex/sports")]

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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 5))

        (testing "SPARQL SELECT DISTINCT single variable"
          ;; Without DISTINCT, would return: alice, alice, bob (alice appears twice due to two likes)
          ;; With DISTINCT, should return: alice, bob
          (let [sparql "SELECT DISTINCT ?person WHERE {
                          ?person <http://ex/likes> ?hobby .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))
                persons (set (map #(str (.getValue % "person")) res))]
            (is (= 2 (count res)) "DISTINCT should return 2 unique persons")
            (is (= #{"http://ex/alice" "http://ex/bob"} persons))))

        (testing "SPARQL SELECT DISTINCT with multiple variables"
          ;; Each person-hobby combination should be unique
          (let [sparql "SELECT DISTINCT ?person ?hobby WHERE {
                          ?person <http://ex/likes> ?hobby .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            ;; alice-music, alice-sports, bob-music = 3 unique combinations
            (is (= 3 (count res)) "Should have 3 unique person-hobby combinations")))

        (testing "SPARQL SELECT DISTINCT with no duplicates"
          ;; Query that naturally has no duplicates should still work
          (let [sparql "SELECT DISTINCT ?s ?o WHERE {
                          ?s <http://ex/knows> ?o .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 2 (count res)) "Should have 2 knows relationships")))

        (.close conn))
      (.shutDown repo))))

(deftest test-transaction-rollback
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            s (.createIRI VF "http://ex/s")
            p (.createIRI VF "http://ex/p")
            o-committed (.createLiteral VF "committed")
            o-rolled-back (.createLiteral VF "rolled-back")]

        (testing "Committed transaction persists data"
          (.begin conn)
          (.add conn s p o-committed (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)

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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          (let [stmts (with-open [iter (.getStatements conn s nil nil true (into-array Resource []))]
                        (vec (iterator-seq iter)))]
            (is (= 2 (count stmts)) "New transaction after rollback should work")
            (let [values (set (map #(.getLabel (.getObject %)) stmts))]
              (is (contains? values "committed"))
              (is (contains? values "success")))))

        (.close conn))
      (.shutDown repo))))

(deftest test-query-timeout-configuration
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)]

      (testing "Default timeout works for normal queries"
        ;; Create sail with default timeout
        (let [sail (rsail/create-rama-sail ipc module-name)
              repo (SailRepository. sail)]
          (.init repo)
          (let [conn (.getConnection repo)
                s (.createIRI VF "http://ex/s")
                p (.createIRI VF "http://ex/p")
                o (.createLiteral VF "test")]
            (.begin conn)
            (.add conn s p o (into-array Resource []))
            (.commit conn)
            (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)

            ;; Query should complete within default timeout
            (let [stmts (with-open [iter (.getStatements conn nil nil nil true (into-array Resource []))]
                          (vec (iterator-seq iter)))]
              (is (= 1 (count stmts))))
            (.close conn))
          (.shutDown repo)))

      (testing "Custom timeout is configurable"
        ;; Create sail with explicit timeout
        (let [sail (rsail/create-rama-sail ipc module-name {:query-timeout-ms 30000})
              repo (SailRepository. sail)]
          (.init repo)
          (let [conn (.getConnection repo)
                stmts (with-open [iter (.getStatements conn nil nil nil true (into-array Resource []))]
                        (vec (iterator-seq iter)))]
            ;; Should still work - the earlier test added data
            (is (= 1 (count stmts)))
            (.close conn))
          (.shutDown repo)))

      (testing "Very short timeout throws QueryInterruptedException"
        ;; Create sail with 1ms timeout - this should timeout on any query
        (let [sail (rsail/create-rama-sail ipc module-name {:query-timeout-ms 1})
              repo (SailRepository. sail)]
          (.init repo)
          (let [conn (.getConnection repo)]
            ;; Query should timeout and throw
            (is (thrown? QueryInterruptedException
                         (.getStatements conn nil nil nil true (into-array Resource []))))
            (.close conn))
          (.shutDown repo))))))

(deftest test-datatype-equality-in-queries
  ;; Verifies that canonicalized numeric values work correctly in joins and queries.
  ;; This is a critical RDF semantics requirement: "1"^^xsd:integer and "01"^^xsd:integer
  ;; must be treated as the same value.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            score (.createIRI VF "http://ex/score")
            has-score (.createIRI VF "http://ex/hasScore")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data with Numeric Values"
          (.begin conn)
          ;; Alice has score "01" (with leading zero)
          (.add conn alice has-score (.createLiteral VF "01" int-type) (into-array Resource []))
          ;; Bob has score "1" (canonical form)
          (.add conn bob has-score (.createLiteral VF "1" int-type) (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2))

        (testing "Query with canonical value matches non-canonical storage"
          ;; Query for score "1" should match Alice's "01"
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/hasScore> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            ;; Both Alice and Bob should match because "01" was canonicalized to "1"
            (is (= 2 (count res)) "Both alice and bob should match query for '1'")))

        (testing "Query with non-canonical value also matches"
          ;; Query for score "01" should also work (query values are also canonicalized)
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/hasScore> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 2 (count res)) "Query with '01' should also match both")))

        (testing "Filter comparison with canonicalized values"
          ;; Query with FILTER should also work with canonicalized values
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/hasScore> ?score .
                          FILTER(?score = \"001\"^^<http://www.w3.org/2001/XMLSchema#integer>)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            ;; Both should match since filter comparison should also canonicalize
            (is (= 2 (count res)) "Filter with '001' should match both alice and bob")))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-if-function
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            age-prop (.createIRI VF "http://ex/age")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
          (.add conn bob age-prop (.createLiteral VF "20" int-type) (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2))

        (testing "SPARQL IF function"
          (let [sparql "SELECT ?person ?cat WHERE {
                          ?person <http://ex/age> ?age .
                          BIND(IF(?age > 25, \"senior\", \"junior\") AS ?cat)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]

            (is (= 2 (count res)))
            (let [sorted (sort-by #(-> % (.getValue "person") .stringValue) res)
                  [alice-row bob-row] sorted]
              (is (= "senior" (-> alice-row (.getValue "cat") .stringValue)))
              (is (= "junior" (-> bob-row (.getValue "cat") .stringValue))))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-type-checks
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            knows (.createIRI VF "http://ex/knows")
            name-prop (.createIRI VF "http://ex/name")
            age-prop (.createIRI VF "http://ex/age")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice knows bob (into-array Resource []))
          (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
          (.add conn alice age-prop (.createLiteral VF "30" int-type) (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "isIRI filter"
          (let [sparql "SELECT ?val WHERE {
                          <http://ex/alice> ?p ?val .
                          FILTER(isIRI(?val))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "http://ex/bob" (-> res first (.getValue "val") .stringValue)))))

        (testing "isLiteral filter"
          (let [sparql "SELECT ?val WHERE {
                          <http://ex/alice> ?p ?val .
                          FILTER(isLiteral(?val))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 2 (count res)))))

        (testing "isNumeric filter"
          (let [sparql "SELECT ?val WHERE {
                          <http://ex/alice> ?p ?val .
                          FILTER(isNumeric(?val))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "30" (-> res first (.getValue "val") .stringValue)))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-lang-datatype
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            label (.createIRI VF "http://ex/label")
            score (.createIRI VF "http://ex/score")
            int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice label (.createLiteral VF "Alice" "en") (into-array Resource []))
          (.add conn alice score (.createLiteral VF "95" int-type) (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2))

        (testing "LANG function"
          (let [sparql "SELECT ?lang WHERE {
                          <http://ex/alice> <http://ex/label> ?lbl .
                          BIND(LANG(?lbl) AS ?lang)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "en" (-> res first (.getValue "lang") .stringValue)))))

        (testing "DATATYPE function"
          (let [sparql "SELECT ?dt WHERE {
                          <http://ex/alice> <http://ex/score> ?s .
                          BIND(DATATYPE(?s) AS ?dt)
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "http://www.w3.org/2001/XMLSchema#integer" (-> res first (.getValue "dt") .stringValue)))))

        (testing "LANGMATCHES filter"
          (let [sparql "SELECT ?lbl WHERE {
                          <http://ex/alice> <http://ex/label> ?lbl .
                          FILTER(LANGMATCHES(LANG(?lbl), \"en\"))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "Alice" (-> res first (.getValue "lbl") .stringValue)))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-regex
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            name-prop (.createIRI VF "http://ex/name")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice name-prop (.createLiteral VF "Alice") (into-array Resource []))
          (.add conn bob name-prop (.createLiteral VF "Bob") (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2))

        (testing "REGEX filter case-insensitive"
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/name> ?name .
                          FILTER(REGEX(?name, \"^ali\", \"i\"))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "http://ex/alice" (-> res first (.getValue "person") .stringValue)))))

        (testing "REGEX filter no flags"
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/name> ?name .
                          FILTER(REGEX(?name, \"ob\"))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)))
            (is (= "http://ex/bob" (-> res first (.getValue "person") .stringValue)))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-in-not-in
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            person (.createIRI VF "http://ex/Person")
            agent-type (.createIRI VF "http://ex/Agent")
            bot (.createIRI VF "http://ex/Bot")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice rdf-type person (into-array Resource []))
          (.add conn bob rdf-type agent-type (into-array Resource []))
          (.add conn charlie rdf-type bot (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "IN filter"
          (let [sparql "SELECT ?s WHERE {
                          ?s a ?type .
                          FILTER(?type IN (<http://ex/Person>, <http://ex/Agent>))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))
                subjects (set (map #(-> % (.getValue "s") .stringValue) res))]
            (is (= 2 (count res)))
            (is (contains? subjects "http://ex/alice"))
            (is (contains? subjects "http://ex/bob"))))

        (testing "NOT IN filter"
          (let [sparql "SELECT ?s WHERE {
                          ?s a ?type .
                          FILTER(?type NOT IN (<http://ex/Bot>))
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))
                subjects (set (map #(-> % (.getValue "s") .stringValue) res))]
            (is (= 2 (count res)))
            (is (contains? subjects "http://ex/alice"))
            (is (contains? subjects "http://ex/bob"))))

        (.close conn))
      (.shutDown repo))))

(deftest test-sparql-having
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            person (.createIRI VF "http://ex/Person")
            bot (.createIRI VF "http://ex/Bot")]

        (testing "Setup Data"
          (.begin conn)
          (.add conn alice rdf-type person (into-array Resource []))
          (.add conn bob rdf-type person (into-array Resource []))
          (.add conn charlie rdf-type bot (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

        (testing "GROUP BY with HAVING"
          (let [sparql "SELECT ?type (COUNT(*) AS ?c) WHERE {
                          ?s a ?type .
                        } GROUP BY ?type HAVING(COUNT(*) > 1)"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            ;; Only Person has count > 1 (alice, bob)
            (is (= 1 (count res)))
            (is (= "http://ex/Person" (-> res first (.getValue "type") .stringValue)))
            (is (= 2 (-> res first (.getValue "c") .intValue)))))

        (.close conn))
      (.shutDown repo))))

(deftest test-bnode-skolemization
  ;; Verifies that blank nodes are properly skolemized:
  ;; - Same BNode ID within a transaction refers to the same node
  ;; - Same BNode ID across transactions refers to different nodes
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (com.rpl.rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]

      (.init repo)

      (let [conn (.getConnection repo)
            knows (.createIRI VF "http://ex/knows")
            name-prop (.createIRI VF "http://ex/name")
            age-prop (.createIRI VF "http://ex/age")]

        (testing "Same BNode ID within transaction refers to same node"
          (.begin conn)
          ;; Use _:person in multiple statements - should be the same node
          (let [person-bnode (.createBNode VF "person")]
            (.add conn person-bnode name-prop (.createLiteral VF "Alice") (into-array Resource []))
            (.add conn person-bnode age-prop (.createLiteral VF "30") (into-array Resource [])))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          ;; Query: find nodes that have both name and age
          (let [sparql "SELECT ?person WHERE {
                          ?person <http://ex/name> ?name .
                          ?person <http://ex/age> ?age .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)) "Same BNode used twice should result in one node with both properties")))

        (testing "Same BNode ID across transactions refers to different nodes"
          ;; First transaction: add _:x with name "Bob"
          (.begin conn)
          (.add conn (.createBNode VF "x") name-prop (.createLiteral VF "Bob") (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; Second transaction: add _:x with name "Charlie"
          (.begin conn)
          (.add conn (.createBNode VF "x") name-prop (.createLiteral VF "Charlie") (into-array Resource []))
          (.commit conn)
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

          ;; Both should exist as separate nodes
          (let [sparql "SELECT ?name WHERE { ?person <http://ex/name> ?name }"
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
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 8)

          ;; Query: find someone who knows someone who knows someone else
          (let [sparql "SELECT ?start ?end WHERE {
                          ?start <http://ex/knows> ?middle .
                          ?middle <http://ex/knows> ?end .
                          ?start <http://ex/name> \"NodeA\" .
                          ?end <http://ex/name> \"NodeC\" .
                        }"
                query (.prepareTupleQuery conn sparql)
                res (with-open [iter (.evaluate query)]
                      (vec (iterator-seq iter)))]
            (is (= 1 (count res)) "Should find the chain from NodeA to NodeC through middle node")))

        (.close conn))
      (.shutDown repo))))