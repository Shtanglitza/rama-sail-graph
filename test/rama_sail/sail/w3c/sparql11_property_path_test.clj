(ns rama-sail.sail.w3c.sparql11-property-path-test
  "W3C SPARQL 1.1 property path conformance tests against RamaSail, through
   rama-sail.sail.w3c.runner.

   Tests are loaded from vendored files in test/resources/w3c/sparql11/property-path/.
   Source: https://github.com/w3c/rdf-tests/tree/main/sparql/sparql11/property-path"
  (:require [clojure.test :refer [deftest use-fixtures]]
            [rama-sail.sail.w3c.runner :as runner]))

(use-fixtures :once runner/repo-fixture)

(deftest sparql11-property-path
  (runner/run-suite runner/*repo* "w3c/sparql11/property-path/"
                    {"pp37" "nested star paths: ((:P)*)* needs recursive ALP handling"}))
