(ns rama-sail.sail.w3c.sparql11-property-path-test
  "W3C SPARQL 1.1 property path conformance tests against RamaSail, through
   rama-sail.sail.w3c.runner.

   Tests are loaded from vendored files in test/resources/w3c/sparql11/property-path/.
   Source: https://github.com/w3c/rdf-tests/tree/main/sparql/sparql11/property-path"
  (:require [clojure.test :refer [deftest use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.w3c.runner :as runner])
  (:import (org.eclipse.rdf4j.repository.sail SailRepository)))

(def ^:dynamic *repo* nil)

(use-fixtures :once
  (fn [f]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
      (let [repo (doto (SailRepository. (rsail/create-rama-sail ipc (rama/get-module-name RdfStorageModule)
                                                                 {:sync-commits true}))
                   (.init))]
        (try (binding [*repo* repo] (f))
             (finally (.shutDown repo)))))))

(deftest sparql11-property-path
  (runner/run-suite *repo* "w3c/sparql11/property-path/"
                    {"pp35" (str "GRAPH ?g { ?s :p1* ?t } loses the zero-length match :a :p1* :a in"
                                 " RDF4J's fallback evaluation over RamaSail (MemoryStore finds it)")
                     "pp37" "nested star paths: ((:P)*)* needs recursive ALP handling"}))
