(ns rama-sail.sail.rdf-star-test
  "Integration tests for RDF-star / RDF 1.2 triple term support.
   Tests SPARQL-star queries with triple term patterns (<< ?s ?p ?o >>)
   for provenance, confidence, and temporal annotation use cases."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]))

(def VF (SimpleValueFactory/getInstance))

(defn- query-results
  "Execute a SPARQL query and return results as a vector of maps."
  [conn sparql]
  (let [query (.prepareTupleQuery conn sparql)]
    (with-open [iter (.evaluate query)]
      (vec (for [bs (iterator-seq iter)]
             (into {} (for [name (.getBindingNames bs)
                            :let [v (.getValue bs name)]
                            :when v]
                        [name (.stringValue v)])))))))

(deftest test-rdf-star-store-and-query-annotations
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [module-name (rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            knows (.createIRI VF "http://ex/knows")
            confidence (.createIRI VF "http://ex/confidence")
            asserted-by (.createIRI VF "http://ex/assertedBy")
            carol (.createIRI VF "http://ex/carol")
            triple-term (.createTriple VF alice knows bob)]

        ;; Store base triple + 2 annotation triples
        (.begin conn)
        (.add conn alice knows bob (into-array Resource []))
        (.add conn triple-term confidence
              (.createLiteral VF "0.95" (.createIRI VF "http://www.w3.org/2001/XMLSchema#double"))
              (into-array Resource []))
        (.add conn triple-term asserted-by carol (into-array Resource []))
        (.commit conn)
        ;; 3 depot appends (1 base + 2 annotations)
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

        (testing "Query annotation by known triple term subject"
          (let [sparql "SELECT ?pred ?val WHERE {
                          << <http://ex/alice> <http://ex/knows> <http://ex/bob> >> ?pred ?val .
                        }"
                results (query-results conn sparql)]
            (is (= 2 (count results)) "Should find confidence and assertedBy annotations")
            (let [by-pred (group-by #(get % "pred") results)]
              (is (some? (get by-pred "http://ex/confidence")))
              (is (some? (get by-pred "http://ex/assertedBy"))))))

        (testing "Query with triple term variable decomposition"
          (let [sparql "SELECT ?s ?p ?o ?val WHERE {
                          << ?s ?p ?o >> <http://ex/confidence> ?val .
                        }"
                results (query-results conn sparql)]
            (is (= 1 (count results)))
            (let [r (first results)]
              (is (= "http://ex/alice" (get r "s")))
              (is (= "http://ex/knows" (get r "p")))
              (is (= "http://ex/bob" (get r "o")))
              (is (some? (get r "val"))))))

        (.close conn)))))

(deftest test-rdf-star-provenance-query
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [module-name (rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            works-at (.createIRI VF "http://ex/worksAt")
            acme (.createIRI VF "http://ex/acme")
            globex (.createIRI VF "http://ex/globex")
            source (.createIRI VF "http://ex/source")
            hr-system (.createIRI VF "http://ex/hr-system")
            linkedin (.createIRI VF "http://ex/linkedin")
            fact1 (.createTriple VF alice works-at acme)
            fact2 (.createTriple VF bob works-at globex)]

        (.begin conn)
        ;; Base triples
        (.add conn alice works-at acme (into-array Resource []))
        (.add conn bob works-at globex (into-array Resource []))
        ;; Provenance annotations
        (.add conn fact1 source hr-system (into-array Resource []))
        (.add conn fact2 source linkedin (into-array Resource []))
        (.commit conn)
        ;; 4 depot appends
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

        (testing "Find all facts from a specific source"
          (let [sparql "SELECT ?s ?o WHERE {
                          << ?s <http://ex/worksAt> ?o >> <http://ex/source> <http://ex/hr-system> .
                        }"
                results (query-results conn sparql)]
            (is (= 1 (count results)))
            (is (= "http://ex/alice" (get (first results) "s")))
            (is (= "http://ex/acme" (get (first results) "o")))))

        (testing "Find source for a specific fact"
          (let [sparql "SELECT ?source WHERE {
                          << <http://ex/bob> <http://ex/worksAt> <http://ex/globex> >> <http://ex/source> ?source .
                        }"
                results (query-results conn sparql)]
            (is (= 1 (count results)))
            (is (= "http://ex/linkedin" (get (first results) "source")))))

        (.close conn)))))

(deftest test-rdf-star-confidence-filter
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [module-name (rama/get-module-name RdfStorageModule)
          sail (rsail/create-rama-sail ipc module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (let [conn (.getConnection repo)
            alice (.createIRI VF "http://ex/alice")
            bob (.createIRI VF "http://ex/bob")
            charlie (.createIRI VF "http://ex/charlie")
            dave (.createIRI VF "http://ex/dave")
            knows (.createIRI VF "http://ex/knows")
            confidence (.createIRI VF "http://ex/confidence")
            xsd-double (.createIRI VF "http://www.w3.org/2001/XMLSchema#double")
            facts [[alice bob "0.95"]
                   [alice charlie "0.3"]
                   [bob dave "0.85"]]]

        (.begin conn)
        (doseq [[s o conf-val] facts]
          (let [triple (.createTriple VF s knows o)]
            ;; Base triple
            (.add conn s knows o (into-array Resource []))
            ;; Confidence annotation
            (.add conn triple confidence
                  (.createLiteral VF conf-val xsd-double)
                  (into-array Resource []))))
        (.commit conn)
        ;; 6 depot appends (3 base + 3 annotations)
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 6)

        (testing "Filter by confidence > 0.8"
          (let [sparql "SELECT ?s ?o ?conf WHERE {
                          << ?s <http://ex/knows> ?o >> <http://ex/confidence> ?conf .
                          FILTER(?conf > \"0.8\"^^<http://www.w3.org/2001/XMLSchema#double>)
                        }"
                results (query-results conn sparql)]
            (is (= 2 (count results)) "Should find alice->bob (0.95) and bob->dave (0.85)")
            (let [objects (set (map #(get % "o") results))]
              (is (contains? objects "http://ex/bob"))
              (is (contains? objects "http://ex/dave"))
              (is (not (contains? objects "http://ex/charlie"))))))

        (.close conn)))))

(comment
  (run-tests 'rama-sail.sail.rdf-star-test))
