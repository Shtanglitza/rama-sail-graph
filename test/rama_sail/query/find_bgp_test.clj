(ns rama-sail.query.find-bgp-test
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

(deftest test-find-bgp-bindings
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (rama/get-module-name core/RdfStorageModule)
          depot       (rama/foreign-depot ipc module-name "*triple-depot")
          find-bgp    (rama/foreign-query ipc module-name "find-bgp")

          ;; Helper to run BGP query
          query-bgp (fn [pattern]
                      (let [res (rama/foreign-invoke-query find-bgp pattern nil)]
                        (set res)))]

      ;; --- DATA SETUP ---
      ;; Default Graph: <alice> <knows> <bob>
      (rama/foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" core/DEFAULT-CONTEXT-VAL]])
      ;; Named Graph <g1>: <bob> <age> 30
      (rama/foreign-append! depot [:add ["<bob>" "<age>" "30" "<g1>"]])
      ;; Named Graph <g2>: <charlie> <knows> <dave>
      (rama/foreign-append! depot [:add ["<charlie>" "<knows>" "<dave>" "<g2>"]])

      (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

      (testing "1. Simple Binding (No Context)"
        ;; Pattern: {:s <alice> :p <knows> :o ?who}
        ;; Should find <bob> in default graph (or any graph if C is not restricted)
        (let [res (query-bgp {:s "<alice>" :p "<knows>" :o "?who"})]
          (is (= 1 (count res)))
          (is (= #{"<bob>"} (set (map #(get % "?who") res))))))

      (testing "2. Binding with Explicit Context"
        ;; Pattern: {:s ?s :p ?p :o ?o :c <g1>}
        ;; Should find only Bob's age
        (let [res (query-bgp {:s "?s" :p "?p" :o "?o" :c "<g1>"})]
          (is (= 1 (count res)))
          (let [r (first res)]
            (is (= "<bob>" (get r "?s")))
            (is (= "<age>" (get r "?p")))
            (is (= "30" (get r "?o"))))))

      (testing "3. Binding with Context Variable (?g)"
        ;; Pattern: {:s ?s :p <knows> :o ?o :c ?g}
        ;; Should find Alice (default) and Charlie (<g2>)
        (let [res (query-bgp {:s "?s" :p "<knows>" :o "?o" :c "?g"})]
          (is (= 2 (count res)))
          ;; Check result contents
          (let [by-subj (group-by #(get % "?s") res)]
            (let [alice-row (first (get by-subj "<alice>"))
                  charlie-row (first (get by-subj "<charlie>"))]
              ;; Alice -> Bob in Default Graph
              (is (= "<bob>" (get alice-row "?o")))
              (is (= core/DEFAULT-CONTEXT-VAL (get alice-row "?g")))

              ;; Charlie -> Dave in <g2>
              (is (= "<dave>" (get charlie-row "?o")))
              (is (= "<g2>" (get charlie-row "?g")))))))

      (testing "4. Wildcard Context (Nil in pattern)"
        ;; Pattern: {:s <bob> :p <age> :o ?age} (Implicit wildcard C)
        (let [res (query-bgp {:s "<bob>" :p "<age>" :o "?age"})]
          (is (= 1 (count res)))
          (is (= "30" (get (first res) "?age")))))

      (testing "5. Full Scan Binding"
        ;; Pattern: {:s ?s :p ?p :o ?o :c ?c}
        (let [res (query-bgp {:s "?s" :p "?p" :o "?o" :c "?c"})]
          (is (= 3 (count res)))
          ;; Ensure we got all graphs
          (is (contains? (set (map #(get % "?c") res)) "<g1>"))
          (is (contains? (set (map #(get % "?c") res)) "<g2>"))
          (is (contains? (set (map #(get % "?c") res)) core/DEFAULT-CONTEXT-VAL)))))))

(comment

  (run-tests 'rama-sail.query.find-bgp-test))
