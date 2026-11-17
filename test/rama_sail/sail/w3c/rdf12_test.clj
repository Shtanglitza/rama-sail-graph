(ns rama-sail.sail.w3c.rdf12-test
  "W3C RDF 1.2 format test suites (Turtle, TriG, N-Triples, N-Quads, RDF/XML;
   test/resources/w3c/rdf12, see test/resources/w3c/SOURCE.md) through
   RamaSail: every input is loaded into the store and read back. See
   rama-sail.sail.w3c.runner for what each kind of test checks. Not included:
   rdf-semantics (entailment).

   Expected failures, by cause:
   - RDF4J's parsers (RDF4J's MemoryStore fails them too): they accept some
     invalid inputs (language tags with a bad or missing tag before the
     direction, lone surrogates, @base in a bad place), and RDF4J's RDF/XML
     parser reads two annotation tests differently.
   - RamaSail stores numeric literals in canonical form, so a graph with
     1.0 or 1e0 does not read back with the same lexical form."
  (:require [clojure.test :refer [deftest use-fixtures]]
            [rama-sail.sail.w3c.runner :as runner]))

(use-fixtures :once runner/repo-fixture)

(def ^:private parser-accepts "RDF4J's parser accepts this invalid input")
(def ^:private canonical-numbers "numeric literals are stored in canonical form: 1.0 reads back as \"1\"^^xsd:decimal")

(defn- run [suite expected-failures]
  (runner/run-suite runner/*repo* (str "w3c/rdf12/" suite "/") expected-failures))

(deftest turtle-syntax
  (run "rdf-turtle/syntax"
       {"nt-ttl12-langdir-bad-3" parser-accepts
        "nt-ttl12-langdir-bad-4" parser-accepts
        "nt-ttl12-langdir-bad-5" parser-accepts
        "turtle12-surrogate-pair-bad-01" parser-accepts
        "turtle12-surrogate-pair-bad-02" parser-accepts
        "turtle12-surrogates-bad-01" parser-accepts
        "turtle12-surrogates-bad-02" parser-accepts
        "turtle12-surrogates-bad-03" parser-accepts
        "turtle12-surrogates-bad-04" parser-accepts
        "turtle12-surrogates-bad-05" parser-accepts}))

(deftest turtle-eval
  (run "rdf-turtle/eval"
       {"turtle12-rt-09" canonical-numbers
        "turtle12-tt-05" canonical-numbers}))

(deftest trig-syntax
  (run "rdf-trig/syntax"
       {"trig12-base-bad-3" parser-accepts
        "trig12-base-bad-4" parser-accepts
        "trig12-base-bad-5" parser-accepts}))

(deftest trig-eval (run "rdf-trig/eval" {}))

(deftest n-triples-syntax
  (run "rdf-n-triples/syntax"
       {"ntriples-langdir-bad-3" parser-accepts
        "ntriples-langdir-bad-4" parser-accepts
        "ntriples-langdir-bad-5" parser-accepts}))

(deftest n-triples-c14n (run "rdf-n-triples/c14n" {}))

(deftest n-quads-syntax
  (run "rdf-n-quads/syntax"
       {"nquads-langdir-bad-3" parser-accepts
        "nquads-langdir-bad-4" parser-accepts
        "nquads-langdir-bad-5" parser-accepts}))

(deftest n-quads-c14n (run "rdf-n-quads/c14n" {}))

(deftest rdf-xml-eval
  (run "rdf-xml/eval"
       {"rdf12-xml-an-13" "RDF4J's RDF/XML parser reads this annotation differently (MemoryStore fails it too)"
        "rdf12-xml-an-14" "RDF4J's RDF/XML parser reads this annotation differently (MemoryStore fails it too)"}))
