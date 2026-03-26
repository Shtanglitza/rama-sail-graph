(ns rama-sail.query.union-test
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer :all]
            [rama-sail.core :as core]))

(deftest test-rdf-storage-module-union
  ;; Create an In-Process Cluster (IPC)
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name RdfStorageModule)

          ;; --- Clients ---
          depot      (foreign-depot ipc module-name "*triple-depot")
          q-plan     (foreign-query ipc module-name "execute-plan")]

      ;; ===================================================================
      ;; 1. TEST: ETL & Setup
      ;; ===================================================================
      (testing "ETL: Ingestion for Union Tests"
        ;; Data for left branch: people who <knows> someone
        (foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" core/DEFAULT-CONTEXT-VAL]])
        (foreign-append! depot [:add ["<charlie>" "<knows>" "<dave>" core/DEFAULT-CONTEXT-VAL]])

        ;; Data for right branch: people who <likes> something
        (foreign-append! depot [:add ["<eve>" "<likes>" "<coffee>" core/DEFAULT-CONTEXT-VAL]])
        (foreign-append! depot [:add ["<frank>" "<likes>" "<tea>" core/DEFAULT-CONTEXT-VAL]])

        ;; Shared data (appears in both branches to test deduplication)
        (foreign-append! depot [:add ["<alice>" "<likes>" "<music>" core/DEFAULT-CONTEXT-VAL]])

        ;; Wait for microbatch "indexer" to process records
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 5))

      ;; ===================================================================
      ;; 2. TEST: Basic UNION Operation
      ;; ===================================================================
      (testing "Query: UNION of two BGPs"
        ;; SPARQL: SELECT ?x ?y WHERE { { ?x <knows> ?y } UNION { ?x <likes> ?y } }
        (let [union-plan {:op :union
                          :left {:op :bgp
                                 :pattern {:s "?x" :p "<knows>" :o "?y"}}
                          :right {:op :bgp
                                  :pattern {:s "?x" :p "<likes>" :o "?y"}}}
              results (foreign-invoke-query q-plan union-plan)
              _       (println "Union Results:" results)]

          ;; Should have 5 results total:
          ;; From left: alice->bob, charlie->dave
          ;; From right: eve->coffee, frank->tea, alice->music
          (is (= 5 (count results)))

          ;; Verify results contain expected bindings
          (let [result-set (set (map (fn [r] [(get r "?x") (get r "?y")]) results))]
            (is (contains? result-set ["<alice>" "<bob>"]))
            (is (contains? result-set ["<charlie>" "<dave>"]))
            (is (contains? result-set ["<eve>" "<coffee>"]))
            (is (contains? result-set ["<frank>" "<tea>"]))
            (is (contains? result-set ["<alice>" "<music>"])))))

      ;; ===================================================================
      ;; 3. TEST: UNION with duplicate elimination
      ;; ===================================================================
      (testing "Query: UNION with overlapping results"
        ;; Add data that will appear in both branches
        (foreign-append! depot [:add ["<alice>" "<knows>" "<carol>" core/DEFAULT-CONTEXT-VAL]])
        (foreign-append! depot [:add ["<alice>" "<likes>" "<carol>" core/DEFAULT-CONTEXT-VAL]])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 7)

        ;; SPARQL: SELECT ?y WHERE { { <alice> <knows> ?y } UNION { <alice> <likes> ?y } }
        (let [union-plan {:op :union
                          :left {:op :bgp
                                 :pattern {:s "<alice>" :p "<knows>" :o "?y"}}
                          :right {:op :bgp
                                  :pattern {:s "<alice>" :p "<likes>" :o "?y"}}}
              results (foreign-invoke-query q-plan union-plan)
              _       (println "Union with duplicates Results:" results)
              y-vals  (set (map #(get % "?y") results))]

          ;; alice <knows> bob, carol
          ;; alice <likes> music, carol
          ;; carol appears in both - but since bindings are different (same ?y but different pattern matched),
          ;; they are actually the same binding map {?y <carol>} - should be deduplicated
          (is (= #{"<bob>" "<carol>" "<music>"} y-vals)))))))

(comment

  (test-rdf-storage-module-union)

  (run-tests 'rama-sail.query.union-test))
