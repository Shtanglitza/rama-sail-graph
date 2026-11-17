(ns rama-sail.sail.serialization-golden-test
  "The canonical strings the store is keyed by must not change across RDF4J
   upgrades: data stored by one version must be found by the next. This
   compares `val->str` of a corpus of terms (built with the value factory,
   parsed from Turtle, and parsed from SPARQL) with strings recorded on
   RDF4J 5.2.2, and checks that `str->val` reads every one back."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.test :refer [deftest is testing]]
            [rama-sail.bulk :as bulk]
            [rama-sail.sail.serialization :as ser])
  (:import (clojure.lang Reflector)
           (java.io StringReader)
           (org.eclipse.rdf4j.model Statement Value)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.query.algebra Var)
           (org.eclipse.rdf4j.query.algebra.helpers AbstractQueryModelVisitor)
           (org.eclipse.rdf4j.query.parser.sparql SPARQLParser)
           (org.eclipse.rdf4j.rio RDFFormat)
           (org.eclipse.rdf4j.rio.helpers StatementCollector)))

(def golden-path "test/resources/serialization-golden.edn")

(def ^:private ^SimpleValueFactory VF (SimpleValueFactory/getInstance))

(defn- iri [s] (.createIRI VF s))

(defn- triple-term
  "createTripleTerm on RDF4J 6, createTriple on RDF4J 5."
  [s p o]
  (let [args (object-array [s p o])]
    (try (Reflector/invokeInstanceMethod VF "createTripleTerm" args)
         (catch IllegalArgumentException _
           (Reflector/invokeInstanceMethod VF "createTriple" args)))))

(defn- built []
  (let [a (iri "http://ex/a") p (iri "http://ex/p") b (.createBNode VF "b1")]
    [["iri" a]
     ["iri with unicode" (iri "http://ex/café")]
     ["bnode" b]
     ["plain literal" (.createLiteral VF "hello")]
     ["xsd:string literal" (.createLiteral VF "hello" (iri "http://www.w3.org/2001/XMLSchema#string"))]
     ["escapes" (.createLiteral VF "tab\there \"q\" back\\slash\nnew é 😀")]
     ["lang lower" (.createLiteral VF "chat" "fr")]
     ["lang mixed case" (.createLiteral VF "colour" "en-GB")]
     ["lang upper" (.createLiteral VF "x" "EN-US")]
     ["integer" (.createLiteral VF "042" (iri "http://www.w3.org/2001/XMLSchema#integer"))]
     ["decimal" (.createLiteral VF "1.50" (iri "http://www.w3.org/2001/XMLSchema#decimal"))]
     ["double" (.createLiteral VF "1e3" (iri "http://www.w3.org/2001/XMLSchema#double"))]
     ["boolean" (.createLiteral VF "TRUE" (iri "http://www.w3.org/2001/XMLSchema#boolean"))]
     ["date" (.createLiteral VF "2026-11-16" (iri "http://www.w3.org/2001/XMLSchema#date"))]
     ["custom datatype" (.createLiteral VF "abc" (iri "http://ex/dt"))]
     ["triple term" (triple-term a p (.createLiteral VF "v" "en"))]
     ["triple term with bnode" (triple-term b p a)]
     ["nested in object" (triple-term a p (triple-term a p b))]]))

(def turtle
  "@prefix ex: <http://ex/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
   ex:s ex:p \"plain\", \"lang\"@en-GB, \"LANG\"@DE, 1, 1.0, 1.0e0, true, \"2026-11-16\"^^xsd:date,
          \"multi\\nline\", '''long \"quoted\"''', _:x, <http://ex/café> .")

(defn- parsed-turtle
  "Parsed as bulk/load-file! parses."
  []
  (let [collector (StatementCollector.)
        _ (doto (bulk/create-parser RDFFormat/TURTLE)
            (.setRDFHandler collector)
            (.parse (StringReader. turtle) ""))
        stmts (.getStatements collector)]
    (for [^Statement st stmts
          :let [o (.getObject st)]]
      ["turtle" (if (instance? org.eclipse.rdf4j.model.BNode o) (.createBNode VF "x") o)])))

(def sparql
  "PREFIX ex: <http://ex/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
   SELECT * WHERE { ?s ex:p \"plain\" . ?s ex:q \"lang\"@en-GB . ?s ex:r \"LANG\"@DE .
                    ?s ex:t 1 . ?s ex:u 1.0 . ?s ex:v 1.0e0 . ?s ex:w true .
                    ?s ex:x \"2026-11-16\"^^xsd:date . ?s ex:y \"tab\\tq\\\"\" }")

(defn- parsed-sparql []
  (let [found (java.util.ArrayList.)
        te (.getTupleExpr (.parseQuery (SPARQLParser.) sparql nil))]
    (.visit te (proxy [AbstractQueryModelVisitor] []
                 (meetNode [node]
                   (when (and (instance? Var node) (.hasValue ^Var node))
                     (.add found (.getValue ^Var node)))
                   (.visitChildren node this))))
    (for [v found] ["sparql" v])))

(defn corpus
  "[[description canonical-string] ...], then [\"turtle\" sorted-strings] and
   [\"sparql\" sorted-strings] for the parsed terms."
  []
  (let [parsed (fn [entries] (vec (sort (distinct (map (comp ser/val->str second) entries)))))]
    (conj (vec (for [[d ^Value v] (built)] [d (ser/val->str v)]))
          ["turtle" (parsed (parsed-turtle))]
          ["sparql" (parsed (parsed-sparql))])))

(defn write-golden!
  "Records the current strings. Run only on the version that defines them."
  []
  (with-open [w (io/writer golden-path)]
    (binding [*out* w]
      (println ";; val->str on RDF4J 5.2.2 (rama-sail main, Nov 16, 2026). Do not regenerate after an upgrade.")
      (pprint/pprint (corpus)))))

(deftest test-canonical-strings-unchanged
  (let [golden (edn/read-string (slurp golden-path))
        current (corpus)]
    (is (= (map first golden) (map first current)) "same corpus")
    (doseq [[[d expected] [_ actual]] (map vector golden current)]
      (is (= expected actual) d))))

(deftest test-strings-read-back
  (doseq [[d strs] (edn/read-string (slurp golden-path))
          s (if (string? strs) [strs] strs)]
    (testing d
      (is (= s (ser/val->str (ser/str->val s)))))))
