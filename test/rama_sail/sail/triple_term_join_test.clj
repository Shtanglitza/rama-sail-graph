(ns rama-sail.sail.triple-term-join-test
  "Triple-term patterns joined with other patterns, in either order.

   RDF4J parses `<< s p o >>` into a TripleRef that builds the term into an
   anonymous variable, joined into the group BEFORE the pattern that uses that
   variable. Each query runs in both pattern orders, natively and through the
   RDF4J fallback (a pending write in the same transaction), against data with
   near misses: other triple terms, a non-triple object, a second subject."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.metrics :as metrics]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.test-helpers :as th])
  (:import (io.prometheus.client Counter$Child)
           (org.eclipse.rdf4j.model Resource)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.query.algebra Join StatementPattern TripleRef Var)
           (org.eclipse.rdf4j.query.impl EmptyBindingSet)
           (org.eclipse.rdf4j.repository.sail SailRepository)))

(def ^:dynamic *repo* nil)

(def prefixes "PREFIX ex: <http://ex/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ")

(def data
  "INSERT DATA {
     ex:alice ex:knows ex:bob .
     ex:dave ex:knows ex:bob .
     << ex:alice ex:knows ex:bob >> ex:source ex:hr .
     << ex:dave ex:knows ex:bob >> ex:source ex:crm .
     << ex:alice ex:knows ex:bob >> ex:confidence \"0.9\" .
     << ex:dave ex:knows ex:bob >> ex:confidence \"0.5\" .
     ex:carol ex:claims << ex:alice ex:knows ex:bob >> .
     ex:carol ex:claims << ex:erin ex:likes ex:bob >> .
     ex:carol ex:claims << ex:bob ex:knows ex:bob >> .
     ex:carol ex:claims ex:x .
     << << ex:alice ex:knows ex:bob >> ex:source ex:hr >> ex:certainty \"high\" .
     << << ex:dave ex:knows ex:bob >> ex:source ex:crm >> ex:certainty \"low\" .
     _:r1 rdf:reifies << ex:alice ex:knows ex:bob >> ; ex:source ex:hr .
     _:r2 rdf:reifies << _:r1 ex:source ex:hr >> ; ex:certainty \"high\" .
     _:r3 rdf:reifies << ex:dave ex:knows ex:bob >> ; ex:source ex:hr .
     GRAPH ex:g { ex:alice ex:knows ex:bob .
                  << ex:alice ex:knows ex:bob >> ex:source ex:wiki . } }")

(use-fixtures :once
  (fn [f]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
      (let [module-name (rama/get-module-name RdfStorageModule)
            repo (doto (SailRepository. (rsail/create-rama-sail ipc module-name)) (.init))]
        (with-open [conn (.getConnection repo)]
          (.execute (.prepareUpdate conn (str prefixes data))))
        (th/wait-indexed! ipc module-name)
        (try (binding [*repo* repo] (f))
             (finally (.shutDown repo)))))))

(defn- rows [conn q]
  (with-open [it (.evaluate (.prepareTupleQuery conn (str prefixes q)))]
    (set (for [bs (iterator-seq it)]
           (into {} (for [n (.getBindingNames bs) :let [v (.getValue bs n)] :when v]
                      [n (.stringValue v)]))))))

(defn- fallbacks []
  (.get ^Counter$Child (.labels metrics/query-total (into-array String ["fallback"]))))

(defn- native [q]
  (with-open [conn (.getConnection *repo*)]
    (let [before (fallbacks)
          result (rows conn q)]
      (is (= before (fallbacks)) (str "answered natively: " q))
      result)))

(defn- with-pending-write
  "Through the RDF4J fallback: a transaction with an uncommitted write."
  [q]
  (with-open [conn (.getConnection *repo*)]
    (.begin conn)
    (let [vf (SimpleValueFactory/getInstance)]
      (.add conn (.createIRI vf "http://ex/unrelated") (.createIRI vf "http://ex/p") (.createIRI vf "http://ex/o")
            (into-array Resource [])))
    (try (rows conn q)
         (finally (.rollback conn)))))

(defn- check [expected & orders]
  (doseq [q orders]
    (is (= expected (native q)) (str "native: " q))
    (is (= expected (with-pending-write q)) (str "fallback: " q))))

(def alice "http://ex/alice")
(def dave "http://ex/dave")
(def bob "http://ex/bob")

(deftest test-term-with-variables-after-another-pattern
  (check #{{"a" alice}}
         "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims << ?a ex:knows ex:bob >> }"
         "SELECT ?a WHERE { ex:carol ex:claims << ?a ex:knows ex:bob >> . ?a ex:knows ex:bob }"))

(deftest test-constant-term-after-another-pattern
  (testing "the term is claimed: every ?a"
    (check #{{"a" alice} {"a" dave}}
           "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims << ex:alice ex:knows ex:bob >> }"
           "SELECT ?a WHERE { ex:carol ex:claims << ex:alice ex:knows ex:bob >> . ?a ex:knows ex:bob }"))
  (testing "the term is not claimed: nothing"
    (check #{}
           "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims << ex:dave ex:knows ex:bob >> }"
           "SELECT ?a WHERE { ex:carol ex:claims << ex:dave ex:knows ex:bob >> . ?a ex:knows ex:bob }")))

(deftest test-only-triple-terms-decompose
  (check #{{"s" alice "p" "http://ex/knows" "o" bob}
           {"s" "http://ex/erin" "p" "http://ex/likes" "o" bob}
           {"s" bob "p" "http://ex/knows" "o" bob}}
         "SELECT ?s ?p ?o WHERE { ex:carol ex:claims << ?s ?p ?o >> }")
  (testing "a variable repeated inside the term"
    (check #{{"x" bob}}
           "SELECT ?x WHERE { ex:carol ex:claims << ?x ex:knows ?x >> }")))

(deftest test-nested-terms
  (check #{{"a" alice "b" bob "s" "http://ex/hr" "c" "high"}
           {"a" dave "b" bob "s" "http://ex/crm" "c" "low"}}
         "SELECT ?a ?b ?s ?c WHERE { << << ?a ex:knows ?b >> ex:source ?s >> ex:certainty ?c }")
  (check #{{"a" alice "c" "high"}}
         "SELECT ?a ?c WHERE { ?a ex:knows ex:bob . << << ?a ex:knows ex:bob >> ex:source ex:hr >> ex:certainty ?c }"
         "SELECT ?a ?c WHERE { << << ?a ex:knows ex:bob >> ex:source ex:hr >> ex:certainty ?c . ?a ex:knows ex:bob }")
  (check #{{"c" "high"}}
         "SELECT ?c WHERE { << << ex:alice ex:knows ex:bob >> ex:source ex:hr >> ex:certainty ?c }"))

(deftest test-reifier-chain
  (check #{{"c" "high"}}
         "SELECT ?c WHERE { ?r2 rdf:reifies << ?x ex:source ex:hr >> ; ex:certainty ?c . ?x rdf:reifies << ex:alice ex:knows ex:bob >> }"
         "SELECT ?c WHERE { ?x rdf:reifies << ex:alice ex:knows ex:bob >> . ?r2 rdf:reifies << ?x ex:source ex:hr >> ; ex:certainty ?c }"))

(deftest test-term-inside-graph
  (check #{{"a" alice "s" "http://ex/wiki"}}
         "SELECT ?a ?s WHERE { GRAPH ex:g { ?a ex:knows ex:bob . << ?a ex:knows ex:bob >> ex:source ?s } }"
         "SELECT ?a ?s WHERE { GRAPH ex:g { << ?a ex:knows ex:bob >> ex:source ?s . ?a ex:knows ex:bob } }"))

(deftest test-from-dataset
  (testing "FROM also goes through the fallback"
    (with-open [conn (.getConnection *repo*)]
      (is (= #{{"a" alice "s" "http://ex/wiki"}}
             (rows conn "SELECT ?a ?s FROM ex:g WHERE { ?a ex:knows ex:bob . << ?a ex:knows ex:bob >> ex:source ?s }"))))))

(deftest test-unpaired-term-fails-loudly
  (testing "a triple term no pattern binds: an error, not an empty result"
    (let [te (Join. (TripleRef. (Var. "s") (Var. "p") (Var. "o") (Var. "t"))
                    (StatementPattern. (Var. "x") (Var. "y") (Var. "z")))]
      (with-open [conn (.getConnection (.getSail ^SailRepository *repo*))]
        (is (thrown-with-msg? Exception #"no other pattern binds"
                              (with-open [it (.evaluate conn te nil (EmptyBindingSet/getInstance) false)]
                                (doall (iterator-seq it)))))))))

(deftest test-term-used-by-two-patterns
  (testing "constant term"
    (check #{{"s" "http://ex/hr" "c" "0.9"} {"s" "http://ex/wiki" "c" "0.9"}}
           "SELECT ?s ?c WHERE { << ex:alice ex:knows ex:bob >> ex:source ?s ; ex:confidence ?c }"))
  (testing "term with variables: both patterns are about the same term"
    (check #{{"a" alice "s" "http://ex/hr" "c" "0.9"} {"a" alice "s" "http://ex/wiki" "c" "0.9"}
             {"a" dave "s" "http://ex/crm" "c" "0.5"}}
           "SELECT ?a ?s ?c WHERE { << ?a ex:knows ex:bob >> ex:source ?s ; ex:confidence ?c }"))
  (check #{{"a" alice "c" "0.9"}}
         "SELECT ?a ?c WHERE { << ?a ex:knows ex:bob >> ex:source ex:hr ; ex:confidence ?c }"))

(deftest test-term-inside-optional
  (check #{{"a" alice "c" "0.9"} {"a" dave}}
         "SELECT ?a ?c WHERE { ?a ex:knows ex:bob OPTIONAL { << ?a ex:knows ex:bob >> ex:source ex:hr ; ex:confidence ?c } }"))

(deftest test-initial-binding-inside-term
  (let [vf (SimpleValueFactory/getInstance)
        run (fn [conn]
              (let [q (.prepareTupleQuery conn (str prefixes "SELECT ?s WHERE { << ?a ex:knows ex:bob >> ex:source ?s }"))]
                (.setBinding q "a" (.createIRI vf dave))
                (with-open [it (.evaluate q)]
                  (set (map #(.stringValue (.getValue % "s")) (iterator-seq it))))))]
    (with-open [conn (.getConnection *repo*)]
      (is (= #{"http://ex/crm"} (run conn)) "native"))
    (with-open [conn (.getConnection *repo*)]
      (.begin conn)
      (.add conn (.createIRI vf "http://ex/unrelated") (.createIRI vf "http://ex/p") (.createIRI vf "http://ex/o")
            (into-array Resource []))
      (try (is (= #{"http://ex/crm"} (run conn)) "fallback")
           (finally (.rollback conn))))))
