(ns rama-sail.query.left-join-test
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer :all]
            [rama-sail.core :as core]))

(deftest test-rdf-storage-module-left-join
	;; Create an In-Process Cluster (IPC)
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name RdfStorageModule)

					;; --- Clients ---
          depot      (foreign-depot ipc module-name "*triple-depot")
          p-spoc     (foreign-pstate ipc module-name "$$spoc")
          p-posc     (foreign-pstate ipc module-name "$$posc")
          p-ospc     (foreign-pstate ipc module-name "$$ospc")
          p-cspo     (foreign-pstate ipc module-name "$$cspo")

          q-triples  (foreign-query ipc module-name "find-triples")
          q-bgp      (foreign-query ipc module-name "find-bgp")
          q-plan     (foreign-query ipc module-name "execute-plan")]

			;; ===================================================================
			;; 1. TEST: ETL & Direct PState Access
			;; ===================================================================
      (testing "ETL: Ingestion and PState Indexing"
				;; Append data with Explicit Context (Default Graph)
				;; <alice> <knows> <bob>
        (foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" core/DEFAULT-CONTEXT-VAL]])
				;; <bob> <age> "30"
        (foreign-append! depot [:add ["<bob>" "<age>" "30" core/DEFAULT-CONTEXT-VAL]])
				;; <charlie> <knows> <dave>
        (foreign-append! depot [:add ["<charlie>" "<knows>" "<dave>" core/DEFAULT-CONTEXT-VAL]])

				;; Wait for microbatch "indexer" to process 3 records
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

      (testing "Query: execute-plan (Recursive Engine)"

				;; --- 4a. Simple BGP via Plan ---
				;; Query: {:s <alice> :p <knows> :o ?friend}
        (let [bgp-plan {:op :bgp
                        :pattern {:s "<alice>" :p "<knows>" :o "?friend"}}
              res      (first (foreign-invoke-query q-plan bgp-plan))]
          (is (= "<bob>" (get res "?friend"))))

				;; --- 4e. LEFT JOIN Operation ---
				;; Query: Find all ?person who <knows> ?friend, and optionally get ?friend's ?age
				;; SPARQL: SELECT ?person ?friend ?age WHERE { ?person <knows> ?friend . OPTIONAL { ?friend <age> ?age } }
        (let [left-join-plan {:op :left-join
                              :join-vars ["?friend"]
                              :left {:op :bgp
                                     :pattern {:s "?person" :p "<knows>" :o "?friend"}}
                              :right {:op :bgp
                                      :pattern {:s "?friend" :p "<age>" :o "?age"}}}
              results (foreign-invoke-query q-plan left-join-plan)
              _       (println "Left Join Results:" results)

							;; Sort results to make assertions deterministic
              sorted-res (sort-by #(get % "?person") results)
              row1       (first sorted-res)  ;; alice
              row2       (second sorted-res)] ;; charlie

          (is (= 2 (count results)))

					;; Verify Alice -> Bob (Has Age)
          (is (= "<alice>" (get row1 "?person")))
          (is (= "<bob>"   (get row1 "?friend")))
          (is (= "30"      (get row1 "?age")))

					;; Verify Charlie -> Dave (No Age - OPTIONAL worked)
          (is (= "<charlie>" (get row2 "?person")))
          (is (= "<dave>"    (get row2 "?friend")))
          (is (nil? (get row2 "?age")) "Age should be nil for friend with no age"))))))

(comment

  (test-rdf-storage-module-left-join)

  (run-tests 'rama-sail.query.left-join-test))