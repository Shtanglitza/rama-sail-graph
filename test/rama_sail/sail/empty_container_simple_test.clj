(ns rama-sail.sail.empty-container-simple-test
  "Simple test for empty container cleanup with soft delete.
   Phase 3a: Deletion creates tombstones instead of physical removal.
   Data remains in indices but is filtered by find-triples queries."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

(deftest test-simple-add-delete
  (testing "Basic add and delete with soft delete (tombstones)"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (get-module-name core/RdfStorageModule)
            depot (foreign-depot ipc module-name "*triple-depot")
            p-spoc (foreign-pstate ipc module-name "$$spoc")
            tombstones (foreign-pstate ipc module-name "$$tombstones")
            q-triples (foreign-query ipc module-name "find-triples")
            tx-time (System/currentTimeMillis)]

        ;; Add a single quad with tx-time
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] tx-time])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 1)

        ;; Verify it exists using foreign-select with ALL navigator
        (let [result (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))]
          (println "After add:" result)
          (is (= #{"<c1>"} result) "Quad should exist"))

        ;; Delete the quad (soft delete creates tombstone)
        (Thread/sleep 10) ;; Ensure different tx-time
        (let [del-time (System/currentTimeMillis)]
          (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] del-time])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          ;; Soft delete: data still exists in raw index
          (let [raw-result (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))]
            (is (= #{"<c1>"} raw-result) "Quad still exists in raw index (soft delete)"))

          ;; But tombstone was created
          (let [tombstone (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c1>") tombstones)]
            (is (some? tombstone) "Tombstone should exist")
            (is (= del-time tombstone) "Tombstone time should match delete time"))

          ;; find-triples filters tombstoned data
          (let [result (foreign-invoke-query q-triples "<s1>" nil nil nil)]
            (println "After delete (query):" result)
            (is (empty? result) "find-triples should filter tombstoned data")))))))

(comment
  (run-tests 'rama-sail.sail.empty-container-simple-test))
