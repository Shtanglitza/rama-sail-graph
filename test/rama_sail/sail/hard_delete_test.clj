(ns rama-sail.sail.hard-delete-test
  "Tests for hard-delete mode where quads are physically removed from indices."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

(deftest test-hard-delete-basic
  (testing "Basic add and delete with hard delete (physical removal)"
    (binding [core/*hard-delete?* true]
      (with-open [ipc (rtest/create-ipc)]
        (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

        (let [module-name (get-module-name core/RdfStorageModule)
              depot (foreign-depot ipc module-name "*triple-depot")
              p-spoc (foreign-pstate ipc module-name "$$spoc")
              tombstones (foreign-pstate ipc module-name "$$tombstones")
              q-triples (foreign-query ipc module-name "find-triples")
              tx-time (System/currentTimeMillis)]

          ;; Add a single quad
          (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] tx-time])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)

          ;; Verify it exists
          (let [result (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))]
            (is (= #{"<c1>"} result) "Quad should exist after add"))

          ;; Delete the quad (hard delete)
          (Thread/sleep 10)
          (let [del-time (System/currentTimeMillis)]
            (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] del-time])
            (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

            ;; Hard delete: data physically removed from index
            (let [raw-result (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc)]
              (is (empty? raw-result) "Quad should be physically removed from index"))

            ;; No tombstone created
            (let [tombstone (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c1>") tombstones)]
              (is (nil? tombstone) "No tombstone should exist in hard-delete mode"))

            ;; find-triples also returns nothing
            (let [result (foreign-invoke-query q-triples "<s1>" nil nil nil)]
              (is (empty? result) "find-triples should return nothing after hard delete"))))))))

(deftest test-hard-delete-readd
  (testing "Re-adding a previously hard-deleted quad"
    (binding [core/*hard-delete?* true]
      (with-open [ipc (rtest/create-ipc)]
        (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

        (let [module-name (get-module-name core/RdfStorageModule)
              depot (foreign-depot ipc module-name "*triple-depot")
              q-triples (foreign-query ipc module-name "find-triples")
              t1 (System/currentTimeMillis)]

          ;; Add, delete, re-add
          (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] t1])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)

          (Thread/sleep 10)
          (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] (System/currentTimeMillis)])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          (Thread/sleep 10)
          (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] (System/currentTimeMillis)])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; Should exist again
          (let [result (foreign-invoke-query q-triples "<s1>" nil nil nil)]
            (is (= 1 (count result)) "Re-added quad should be visible")))))))

(deftest test-hard-delete-clear-context
  (testing "Clear context with hard delete physically removes quads"
    (binding [core/*hard-delete?* true]
      (with-open [ipc (rtest/create-ipc)]
        (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

        (let [module-name (get-module-name core/RdfStorageModule)
              depot (foreign-depot ipc module-name "*triple-depot")
              p-spoc (foreign-pstate ipc module-name "$$spoc")
              q-triples (foreign-query ipc module-name "find-triples")
              t1 (System/currentTimeMillis)]

          ;; Add two quads in same context
          (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<ctx>"] t1])
          (foreign-append! depot [:add ["<s2>" "<p2>" "<o2>" "<ctx>"] t1])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          ;; Verify both exist
          (is (= 1 (count (foreign-invoke-query q-triples "<s1>" nil nil nil))))
          (is (= 1 (count (foreign-invoke-query q-triples "<s2>" nil nil nil))))

          ;; Clear context
          (Thread/sleep 10)
          (foreign-append! depot [:clear-context [nil nil nil "<ctx>"] (System/currentTimeMillis)])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; Both should be gone from index
          (let [r1 (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc)
                r2 (foreign-select [(keypath "<s2>" "<p2>" "<o2>") ALL] p-spoc)]
            (is (empty? r1) "First quad should be physically removed")
            (is (empty? r2) "Second quad should be physically removed"))

          ;; find-triples confirms
          (is (empty? (foreign-invoke-query q-triples "<s1>" nil nil nil)))
          (is (empty? (foreign-invoke-query q-triples "<s2>" nil nil nil))))))))

(deftest test-hard-delete-stats-consistency
  (testing "Statistics are correct after hard delete"
    (binding [core/*hard-delete?* true]
      (with-open [ipc (rtest/create-ipc)]
        (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

        (let [module-name (get-module-name core/RdfStorageModule)
              depot (foreign-depot ipc module-name "*triple-depot")
              p-global (foreign-pstate ipc module-name "$$global-stats")
              t1 (System/currentTimeMillis)]

          ;; Add 3 quads
          (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] t1])
          (foreign-append! depot [:add ["<s2>" "<p1>" "<o2>" "<c1>"] t1])
          (foreign-append! depot [:add ["<s3>" "<p2>" "<o3>" "<c1>"] t1])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          (is (= 3 (foreign-select-one [(keypath "" :total-triples)] p-global))
              "Should have 3 total triples")

          ;; Delete one
          (Thread/sleep 10)
          (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] (System/currentTimeMillis)])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

          (is (= 2 (foreign-select-one [(keypath "" :total-triples)] p-global))
              "Should have 2 total triples after delete"))))))
