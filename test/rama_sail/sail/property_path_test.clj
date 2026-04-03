(ns rama-sail.sail.property-path-test
  "Integration tests for SPARQL property path support.
   Tests transitive paths (+/*), zero-length paths (?), inverse paths (^),
   sequence paths (/), and alternative paths (|)."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import [org.eclipse.rdf4j.model Resource]
           [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]))

(def VF (SimpleValueFactory/getInstance))
(def ^:private module-name (rama/get-module-name RdfStorageModule))

;;; ---------------------------------------------------------------------------
;;; Shared IPC fixture
;;; ---------------------------------------------------------------------------

(def ^:dynamic *repo* nil)
(def ^:dynamic *ipc* nil)
(def ^:dynamic *mb-count* nil)

(defn property-path-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [sail (rsail/create-rama-sail ipc module-name {:sync-commits true})
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (binding [*repo* repo
                  *ipc* ipc
                  *mb-count* (atom 0)]
          (f))
        (finally
          (.shutDown repo))))))

(use-fixtures :once property-path-fixture)

(defn- wait-mb! [n]
  (swap! *mb-count* + n)
  (rtest/wait-for-microbatch-processed-count *ipc* module-name "indexer" @*mb-count*))

(defn- query-results
  "Execute a SPARQL SELECT query and return results as a vector of maps."
  [sparql]
  (let [conn (.getConnection *repo*)]
    (try
      (let [query (.prepareTupleQuery conn sparql)
            result (.evaluate query)
            binding-names (.getBindingNames result)
            results (vec (for [bs (iterator-seq result)]
                           (into {} (for [name binding-names
                                          :let [v (.getValue bs name)]
                                          :when v]
                                      [name (.stringValue v)]))))]
        results)
      (finally
        (.close conn)))))

(defn- add-triples!
  "Add triples to the store. Each triple is [s p o]."
  [triples]
  (let [conn (.getConnection *repo*)]
    (try
      (.begin conn)
      (doseq [[s p o] triples]
        (.add conn
              (.createIRI VF s)
              (.createIRI VF p)
              (.createIRI VF o)
              (into-array Resource [])))
      (.commit conn)
      (wait-mb! (count triples))
      (finally
        (.close conn)))))

;;; ---------------------------------------------------------------------------
;;; Tests
;;; ---------------------------------------------------------------------------

(deftest test-transitive-path-plus
  (testing "rdfs:subClassOf+ finds transitive chain"
    (add-triples! [["http://ex/Dog" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://ex/Animal"]
                   ["http://ex/Poodle" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://ex/Dog"]])

    (let [results (query-results
                   "SELECT ?super WHERE {
                      <http://ex/Poodle> <http://www.w3.org/2000/01/rdf-schema#subClassOf>+ ?super
                    }")
          supers (set (map #(get % "super") results))]
      ;; Poodle -> Dog (direct) and Poodle -> Animal (transitive)
      (is (contains? supers "http://ex/Dog"))
      (is (contains? supers "http://ex/Animal"))
      (is (= 2 (count supers))))))

(deftest test-transitive-path-star
  (testing "rdfs:subClassOf* includes self"
    (add-triples! [["http://ex/Cat" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://ex/Feline"]
                   ["http://ex/Feline" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://ex/Mammal"]])

    (let [results (query-results
                   "SELECT ?class WHERE {
                      <http://ex/Cat> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?class
                    }")
          classes (set (map #(get % "class") results))]
      ;; * includes self (Cat), plus transitive (Feline, Mammal)
      (is (contains? classes "http://ex/Cat"))
      (is (contains? classes "http://ex/Feline"))
      (is (contains? classes "http://ex/Mammal"))
      (is (= 3 (count classes))))))

(deftest test-sequence-path
  (testing ":knows/:name sequence path"
    (add-triples! [["http://ex/alice" "http://ex/knows" "http://ex/bob"]
                   ["http://ex/bob" "http://ex/name" "http://ex/BobName"]])

    (let [results (query-results
                   "SELECT ?name WHERE {
                      <http://ex/alice> <http://ex/knows>/<http://ex/name> ?name
                    }")]
      ;; alice -> knows -> bob -> name -> BobName
      (is (= 1 (count results)))
      (is (= "http://ex/BobName" (get (first results) "name"))))))

(deftest test-alternative-path
  (testing ":knows|:friendOf alternative path"
    (add-triples! [["http://ex/alice" "http://ex/knows" "http://ex/bob"]
                   ["http://ex/alice" "http://ex/friendOf" "http://ex/carol"]])

    (let [results (query-results
                   "SELECT ?person WHERE {
                      <http://ex/alice> (<http://ex/knows>|<http://ex/friendOf>) ?person
                    }")
          people (set (map #(get % "person") results))]
      (is (contains? people "http://ex/bob"))
      (is (contains? people "http://ex/carol"))
      (is (= 2 (count people))))))

(deftest test-inverse-path
  (testing "^:knows inverse path"
    (add-triples! [["http://ex/alice" "http://ex/knows" "http://ex/bob"]
                   ["http://ex/carol" "http://ex/knows" "http://ex/bob"]])

    (let [results (query-results
                   "SELECT ?knower WHERE {
                      <http://ex/bob> ^<http://ex/knows> ?knower
                    }")
          knowers (set (map #(get % "knower") results))]
      (is (contains? knowers "http://ex/alice"))
      (is (contains? knowers "http://ex/carol"))
      (is (= 2 (count knowers))))))

(comment
  (run-tests 'rama-sail.sail.property-path-test))
