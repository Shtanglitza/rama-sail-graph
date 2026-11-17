(ns rama-sail.sail.w3c.sparql12-test
  "W3C SPARQL 1.2 test suites (test/resources/w3c/sparql12, see
   test/resources/w3c/SOURCE.md) against RamaSail. See rama-sail.sail.w3c.runner
   for what each kind of test checks.

   Expected failures, by cause:
   - RDF4J's SPARQL parser (RDF4J's MemoryStore fails them too): it accepts
     some invalid codepoint and surrogate escapes, and rejects two queries
     whose codepoint escapes SPARQL 1.2 no longer expands before parsing.
   - RamaSail stores numeric literals in canonical form (\"001\"^^xsd:integer
     is stored as \"1\"), so terms that differ only in lexical form group
     together.
   - RamaSail matches language tags case-sensitively in graph patterns."
  (:require [clojure.test :refer [deftest use-fixtures]]
            [rama-sail.sail.w3c.runner :as runner]))

(use-fixtures :once runner/repo-fixture)

(def ^:private parser-accepts "RDF4J's SPARQL parser accepts this invalid escape")
(def ^:private parser-rejects "RDF4J's SPARQL parser rejects the codepoint escape in this query")

(defn- run [suite expected-failures]
  (runner/run-suite runner/*repo* (str "w3c/sparql12/" suite "/") expected-failures))

(deftest eval-triple-terms (run "eval-triple-terms" {}))

(deftest syntax-triple-terms-positive (run "syntax-triple-terms-positive" {}))

(deftest syntax-triple-terms-negative (run "syntax-triple-terms-negative" {}))

(deftest codepoint-escapes
  (run "codepoint-escapes"
       {"codepoint-esc-01-bad" parser-accepts
        "codepoint-esc-03-bad" parser-accepts
        "surrogate-esc-01-bad" parser-accepts
        "surrogate-esc-02-bad" parser-accepts
        "surrogate-esc-03-bad" parser-accepts
        "surrogate-esc-04-bad" parser-accepts
        "surrogate-esc-05-bad" parser-accepts
        "codepoint-esc-08" parser-rejects
        "codepoint-esc-09" parser-rejects}))

(deftest expression (run "expression" {}))

(deftest grouping
  (run "grouping"
       {"group01" "numeric literals are stored in canonical form: \"001\"^^xsd:integer groups with \"1\""}))

(deftest lang-basedir
  (run "lang-basedir"
       {"lang-case-strlang" "graph patterns match language tags case-sensitively"}))

(deftest rdf11 (run "rdf11" {}))

(deftest syntax (run "syntax" {}))

(deftest version (run "version" {}))
