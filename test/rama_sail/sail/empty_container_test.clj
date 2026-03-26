(ns rama-sail.sail.empty-container-test
  "Tests for soft delete behavior after quad deletion.
   Phase 3a: Deletion creates tombstones instead of physical removal.
   Data remains in indices but is filtered by find-triples queries."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

(deftest test-empty-container-cleanup-single-quad
  (testing "Deleting the only quad filters it from queries (soft delete)"
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

        ;; Verify it exists
        (let [result (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))]
          (is (= #{"<c1>"} result) "Quad should exist in $$spoc"))

        ;; Delete the quad (soft delete)
        (Thread/sleep 10)
        (let [del-time (System/currentTimeMillis)]
          (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] del-time])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

          ;; Soft delete: data still exists in raw index
          (let [raw-result (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))]
            (is (= #{"<c1>"} raw-result) "Quad still in raw index (soft delete)"))

          ;; But tombstone was created
          (is (some? (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c1>") tombstones))
              "Tombstone should exist")

          ;; find-triples filters tombstoned data
          (let [result (foreign-invoke-query q-triples "<s1>" nil nil nil)]
            (is (empty? result) "No triples should be found for <s1> after deletion")))))))

(deftest test-partial-deletion-preserves-siblings
  (testing "Deleting one quad should preserve sibling entries (soft delete)"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (get-module-name core/RdfStorageModule)
            depot (foreign-depot ipc module-name "*triple-depot")
            p-spoc (foreign-pstate ipc module-name "$$spoc")
            tombstones (foreign-pstate ipc module-name "$$tombstones")
            q-triples (foreign-query ipc module-name "find-triples")
            tx-time (System/currentTimeMillis)]

        ;; Add two quads with same S, P but different O
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] tx-time])
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o2>" "<c1>"] tx-time])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

        ;; Verify both exist
        (is (= #{"<c1>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc))))
        (is (= #{"<c1>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o2>") ALL] p-spoc))))

        ;; Delete first quad (soft delete)
        (Thread/sleep 10)
        (let [del-time (System/currentTimeMillis)]
          (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] del-time])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; Soft delete: first quad still in raw index but has tombstone
          (is (= #{"<c1>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc)))
              "<o1> still in raw index (soft delete)")
          (is (some? (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c1>") tombstones))
              "<o1> should have tombstone")

          ;; Second quad should still exist without tombstone
          (is (= #{"<c1>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o2>") ALL] p-spoc)))
              "<o2> entry should be preserved")
          (is (nil? (foreign-select-one (keypath "<s1>" "<p1>" "<o2>" "<c1>") tombstones))
              "<o2> should NOT have tombstone")

          ;; Query should still find only the second quad (first is filtered)
          (let [result (foreign-invoke-query q-triples "<s1>" "<p1>" nil nil)]
            (is (= 1 (count result)) "Should find exactly one quad")))))))

(deftest test-cascading-cleanup
  (testing "Deleting all quads at each level filters them from queries (soft delete)"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (get-module-name core/RdfStorageModule)
            depot (foreign-depot ipc module-name "*triple-depot")
            q-triples (foreign-query ipc module-name "find-triples")
            tx-time (System/currentTimeMillis)]

        ;; Add three quads: same S, different P
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] tx-time])
        (foreign-append! depot [:add ["<s1>" "<p2>" "<o2>" "<c1>"] tx-time])
        (foreign-append! depot [:add ["<s1>" "<p3>" "<o3>" "<c1>"] tx-time])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

        ;; Verify 3 quads exist for subject
        (is (= 3 (count (foreign-invoke-query q-triples "<s1>" nil nil nil)))
            "Should have 3 quads under <s1>")

        ;; Delete first quad (soft delete)
        (Thread/sleep 10)
        (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

        ;; Should have 2 quads visible (third tombstoned)
        (is (= 2 (count (foreign-invoke-query q-triples "<s1>" nil nil nil)))
            "Should have 2 quads after first deletion")

        ;; Delete second quad
        (Thread/sleep 10)
        (foreign-append! depot [:del ["<s1>" "<p2>" "<o2>" "<c1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 5)

        ;; Should have 1 quad visible
        (is (= 1 (count (foreign-invoke-query q-triples "<s1>" nil nil nil)))
            "Should have 1 quad after second deletion")

        ;; Delete third quad
        (Thread/sleep 10)
        (foreign-append! depot [:del ["<s1>" "<p3>" "<o3>" "<c1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 6)

        ;; Should have no quads visible (all tombstoned)
        (is (empty? (foreign-invoke-query q-triples "<s1>" nil nil nil))
            "All quads should be filtered after deletion")))))

(deftest test-multiple-contexts-same-triple
  (testing "Soft delete handles multiple contexts for same S,P,O"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (get-module-name core/RdfStorageModule)
            depot (foreign-depot ipc module-name "*triple-depot")
            p-spoc (foreign-pstate ipc module-name "$$spoc")
            tombstones (foreign-pstate ipc module-name "$$tombstones")
            q-triples (foreign-query ipc module-name "find-triples")
            tx-time (System/currentTimeMillis)]

        ;; Add same triple in two different contexts
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c1>"] tx-time])
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<c2>"] tx-time])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

        ;; Verify both contexts in set
        (is (= #{"<c1>" "<c2>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc)))
            "Should have both contexts")

        ;; Delete first context (soft delete)
        (Thread/sleep 10)
        (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

        ;; Soft delete: both contexts still in raw index
        (is (= #{"<c1>" "<c2>"} (set (foreign-select [(keypath "<s1>" "<p1>" "<o1>") ALL] p-spoc)))
            "Both contexts still in raw index (soft delete)")
        ;; But c1 has tombstone
        (is (some? (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c1>") tombstones))
            "<c1> should have tombstone")
        (is (nil? (foreign-select-one (keypath "<s1>" "<p1>" "<o1>" "<c2>") tombstones))
            "<c2> should NOT have tombstone")

        ;; Query should only return c2
        (let [result (set (foreign-invoke-query q-triples "<s1>" nil nil nil))]
          (is (= 1 (count result)) "Should find only one quad")
          (is (= #{["<s1>" "<p1>" "<o1>" "<c2>"]} result)))

        ;; Delete second context
        (Thread/sleep 10)
        (foreign-append! depot [:del ["<s1>" "<p1>" "<o1>" "<c2>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

        ;; No triples should be found (all tombstoned)
        (is (empty? (foreign-invoke-query q-triples "<s1>" nil nil nil))
            "All quads should be filtered after deletion")))))

(deftest test-clear-context-cleanup
  (testing "clear-context should create tombstones for all quads in context"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc core/RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (get-module-name core/RdfStorageModule)
            depot (foreign-depot ipc module-name "*triple-depot")
            q-triples (foreign-query ipc module-name "find-triples")
            tx-time (System/currentTimeMillis)]

        ;; Add quads in a context
        (foreign-append! depot [:add ["<s1>" "<p1>" "<o1>" "<g1>"] tx-time])
        (foreign-append! depot [:add ["<s2>" "<p2>" "<o2>" "<g1>"] tx-time])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)

        ;; Verify they exist
        (is (= 1 (count (foreign-invoke-query q-triples "<s1>" nil nil nil))))
        (is (= 1 (count (foreign-invoke-query q-triples "<s2>" nil nil nil))))

        ;; Clear the context (this creates tombstones for all quads in context)
        (Thread/sleep 10)
        (foreign-append! depot [:clear-context [nil nil nil "<g1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

        ;; All quads in context should be filtered out
        (is (empty? (foreign-invoke-query q-triples "<s1>" nil nil nil))
            "Should not find <s1> quads after clear-context")
        (is (empty? (foreign-invoke-query q-triples "<s2>" nil nil nil))
            "Should not find <s2> quads after clear-context")))))

(deftest test-empty->none-function
  (testing "empty->none helper function"
    (is (= com.rpl.rama.path/NONE (core/empty->none #{}))
        "Empty set should return NONE")
    (is (= com.rpl.rama.path/NONE (core/empty->none {}))
        "Empty map should return NONE")
    (is (= com.rpl.rama.path/NONE (core/empty->none []))
        "Empty vector should return NONE")
    (is (= #{1 2} (core/empty->none #{1 2}))
        "Non-empty set should be returned unchanged")
    (is (= {:a 1} (core/empty->none {:a 1}))
        "Non-empty map should be returned unchanged")
    (is (= "string" (core/empty->none "string"))
        "Non-collection should be returned unchanged")
    (is (= 42 (core/empty->none 42))
        "Number should be returned unchanged")))

(comment
  (run-tests 'rama-sail.sail.empty-container-test))
