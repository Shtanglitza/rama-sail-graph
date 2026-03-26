(ns rama-sail.query.transaction-test
  "Transaction semantics tests for RamaSail.

   These tests verify correct behavior of:
   - Transaction lifecycle (begin/commit/rollback)
   - Operation ordering within transactions
   - BNode skolemization consistency

   Uses a shared IPC fixture with unique IRIs for test isolation."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]))

(def VF (SimpleValueFactory/getInstance))
(def module-name (rama/get-module-name RdfStorageModule))

;; Shared IPC and microbatch counter
(def ^:dynamic *ipc* nil)
(def ^:dynamic *mb-counter* nil)

(defn transaction-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc
              *mb-counter* (atom 0)]
      (f))))

(use-fixtures :once transaction-fixture)

(defn wait-mb!
  "Wait for microbatch and increment counter."
  []
  (swap! *mb-counter* inc)
  (rtest/wait-for-microbatch-processed-count *ipc* module-name "indexer" @*mb-counter*))

(defn unique-iri
  "Generate unique IRI for test isolation."
  [base]
  (str "http://ex/" base "-" (System/nanoTime)))

;; Helper to count statements matching a pattern
(defn count-statements [conn s p o contexts]
  (with-open [iter (.getStatements conn s p o true (into-array Resource contexts))]
    (count (iterator-seq iter))))

;;; ---------------------------------------------------------------------------
;;; Core Transaction Tests
;;; ---------------------------------------------------------------------------

(deftest test-add-commit-makes-data-visible
  (testing "Data added and committed should be visible after microbatch processing"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "commit-test"))
              p (.createIRI VF "http://ex/p1")
              o (.createLiteral VF "value1")]
          (try
            ;; Before transaction - should be empty
            (is (= 0 (count-statements conn s p o [])))

            ;; Begin transaction, add, commit
            (.begin conn)
            (.add conn s p o (into-array Resource []))
            (.commit conn)

            (wait-mb!)

            ;; After commit - data should be visible
            (is (= 1 (count-statements conn s p o [])))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-add-rollback-discards-data
  (testing "Data added but rolled back should NOT be visible"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "rollback-test"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "should-not-exist")]
          (try
            ;; Begin transaction, add, rollback
            (.begin conn)
            (.add conn s p o (into-array Resource []))
            (.rollback conn)

            ;; Give some time for any potential (incorrect) processing
            (Thread/sleep 100)

            ;; Data should NOT be visible
            (is (= 0 (count-statements conn s p o [])))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-add-delete-same-transaction-explicit-context
  (testing "Add and delete of the same triple with explicit default context - last write wins"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "add-del-same"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "value")
              ;; Use explicit nil (default graph) context
              default-ctx (into-array Resource [nil])]
          (try
            ;; Add then delete in same transaction with explicit default context
            (.begin conn)
            (.add conn s p o default-ctx)
            (.remove conn s p o default-ctx)
            (.commit conn)

            ;; Wait for 2 operations (add + del)
            (wait-mb!)
            (wait-mb!)

            ;; Triple should NOT exist (delete was after add)
            (is (= 0 (count-statements conn s p o [])))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-delete-add-same-transaction-explicit-context
  (testing "Delete then add of the same triple with explicit context - last write wins"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "del-add-same"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "value")
              default-ctx (into-array Resource [nil])]
          (try
            ;; Delete (no-op since doesn't exist) then add in same transaction
            (.begin conn)
            (.remove conn s p o default-ctx)
            (.add conn s p o default-ctx)
            (.commit conn)

            ;; Wait for 2 operations (del + add)
            (wait-mb!)
            (wait-mb!)

            ;; Triple SHOULD exist (add was after delete)
            (is (= 1 (count-statements conn s p o [])))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-read-your-own-writes
  (testing "Uncommitted changes visible to same connection (read-your-own-writes)"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "isolation-test"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "pending")]
          (try
            ;; Begin transaction, add, but don't commit yet
            (.begin conn)
            (.add conn s p o (into-array Resource []))

            ;; Data SHOULD be visible to same connection (read-your-own-writes)
            (is (= 1 (count-statements conn s p o []))
                "Read-your-own-writes: uncommitted data visible to same connection")

            ;; Now commit
            (.commit conn)
            (wait-mb!)

            ;; Now it should be visible
            (is (= 1 (count-statements conn s p o [])))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-bnode-consistency-within-transaction
  (testing "Same BNode ID within a transaction should refer to the same node"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              ;; Create a BNode that will be used as both subject and object
              bnode (.createBNode VF (str "localId-" (System/nanoTime)))
              p1 (.createIRI VF "http://ex/name")
              p2 (.createIRI VF "http://ex/knows")
              alice (.createIRI VF (unique-iri "alice"))
              name-val (.createLiteral VF "Anonymous")]
          (try
            ;; In single transaction: _:localId has name, alice knows _:localId
            (.begin conn)
            (.add conn bnode p1 name-val (into-array Resource []))
            (.add conn alice p2 bnode (into-array Resource []))
            (.commit conn)

            (wait-mb!)
            (wait-mb!)

            ;; Query: Find who Alice knows and their name
            (let [sparql (format "SELECT ?name WHERE { <%s> <http://ex/knows> ?x . ?x <http://ex/name> ?name . }"
                                 (.stringValue alice))
                  query (.prepareTupleQuery conn sparql)
                  results (with-open [iter (.evaluate query)]
                            (vec (iterator-seq iter)))]
              (is (= 1 (count results)) "Should find exactly one result - BNode linked correctly")
              (when (seq results)
                (is (= "Anonymous" (.stringValue (.getValue (first results) "name"))))))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-empty-transaction
  (testing "Empty transaction (begin + commit with no ops) should work fine"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)]
          (try
            ;; Empty transaction 1
            (.begin conn)
            (.commit conn)

            ;; Empty transaction 2 with rollback
            (.begin conn)
            (.rollback conn)

            ;; Should not throw, system should be in good state
            ;; Add something to verify state is good
            (let [s (.createIRI VF (unique-iri "after-empty"))
                  p (.createIRI VF "http://ex/p")
                  o (.createLiteral VF "works")]
              (.begin conn)
              (.add conn s p o (into-array Resource []))
              (.commit conn)
              (wait-mb!)
              (is (= 1 (count-statements conn s p o []))))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-named-graph-transaction
  (testing "Named graph operations within transaction should work correctly"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "named-graph-s"))
              p (.createIRI VF "http://ex/p")
              o1 (.createLiteral VF "val1")
              o2 (.createLiteral VF "val2")
              g1 (.createIRI VF (unique-iri "graph1"))
              g2 (.createIRI VF (unique-iri "graph2"))]
          (try
            ;; Add to multiple named graphs in single transaction
            (.begin conn)
            (.add conn s p o1 (into-array Resource [g1]))
            (.add conn s p o2 (into-array Resource [g2]))
            (.commit conn)

            (wait-mb!)
            (wait-mb!)

            ;; Verify: Each graph has correct data
            (is (= 1 (count-statements conn s p o1 [g1])) "Graph1 should have o1")
            (is (= 0 (count-statements conn s p o2 [g1])) "Graph1 should NOT have o2")
            (is (= 0 (count-statements conn s p o1 [g2])) "Graph2 should NOT have o1")
            (is (= 1 (count-statements conn s p o2 [g2])) "Graph2 should have o2")

            ;; Total across all graphs
            (is (= 2 (count-statements conn s p nil [])) "Total should be 2 statements")
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

;;; ---------------------------------------------------------------------------
;;; Context Visibility Integration Tests (Audit Fixes)
;;; ---------------------------------------------------------------------------

(deftest test-context-ids-add-then-delete-same-quad
  (testing "Context should NOT appear after add-then-delete of same quad in pending state"
    ;; This tests the fix for the sticky has-adds? bug.
    ;; After add-then-delete of the same quad, the context should have 0 visible
    ;; statements and should NOT appear in getContextIDs().
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "add-del-ctx"))
              s (.createIRI VF (unique-iri "subj"))
              p (.createIRI VF "http://ex/pred")
              o (.createLiteral VF "value")]
          (try
            (.begin conn)
            ;; Add a quad to a new context
            (.add conn s p o (into-array Resource [ctx]))
            ;; Delete the same quad
            (.remove conn s p o (into-array Resource [ctx]))

            ;; Context should NOT appear in getContextIDs (net-visible adds = 0)
            (let [ctx-iter (.getContextIDs conn)
                  ctx-ids (into #{} (map str (iterator-seq ctx-iter)))]
              (.close ctx-iter)
              (is (not (contains? ctx-ids (str ctx)))
                  "Context with add-then-delete should NOT appear in getContextIDs"))

            ;; Also verify no statements exist
            (is (= 0 (count-statements conn s p o [ctx]))
                "No statements should exist after add-then-delete")

            (.rollback conn)
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-context-ids-add-then-delete-different-quad
  (testing "Context SHOULD appear when add exists but unrelated delete also exists"
    ;; This tests that unrelated deletes don't hide real adds.
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "mixed-ctx"))
              s1 (.createIRI VF (unique-iri "subj1"))
              s2 (.createIRI VF (unique-iri "subj2"))
              p (.createIRI VF "http://ex/pred")
              o (.createLiteral VF "value")]
          (try
            (.begin conn)
            ;; Add a quad to a new context
            (.add conn s1 p o (into-array Resource [ctx]))
            ;; Delete a DIFFERENT (non-existent) quad in same context
            (.remove conn s2 p o (into-array Resource [ctx]))

            ;; Context SHOULD appear (has net-visible add)
            (let [ctx-iter (.getContextIDs conn)
                  ctx-ids (into #{} (map str (iterator-seq ctx-iter)))]
              (.close ctx-iter)
              (is (contains? ctx-ids (str ctx))
                  "Context with real add should appear even with unrelated delete"))

            ;; Verify the added statement exists
            (is (= 1 (count-statements conn s1 p o [ctx]))
                "Added statement should be visible")

            (.rollback conn)
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-clear-all-includes-pending-only-context
  (testing "clear() should clear pending-only contexts created in same transaction"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "pending-clear-ctx"))
              s (.createIRI VF (unique-iri "subj"))
              p (.createIRI VF "http://ex/pred")
              o (.createLiteral VF "value")]
          (try
            (.begin conn)
            ;; Add to a brand new context (pending-only)
            (.add conn s p o (into-array Resource [ctx]))

            ;; Verify it exists in pending state
            (is (= 1 (count-statements conn s p o [ctx]))
                "Added statement should be visible before clear")

            ;; Clear all contexts
            (.clear conn (into-array Resource []))

            ;; Verify the pending-only context was cleared
            (is (= 0 (count-statements conn s p o [ctx]))
                "Statement should be gone after clear")

            ;; Context should not appear in getContextIDs
            (let [ctx-iter (.getContextIDs conn)
                  ctx-ids (into #{} (map str (iterator-seq ctx-iter)))]
              (.close ctx-iter)
              (is (not (contains? ctx-ids (str ctx)))
                  "Cleared pending-only context should not appear in getContextIDs"))

            (.rollback conn)
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

;;; ---------------------------------------------------------------------------
;;; hasStatement Pending-State Consistency Tests
;;; ---------------------------------------------------------------------------

(deftest test-has-statement-sees-pending-add
  (testing "hasStatement sees an uncommitted add in the same transaction"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "has-stmt-add"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "pending-value")]
          (try
            ;; Before transaction
            (is (not (.hasStatement conn s p o true (into-array Resource [])))
                "Should not exist before add")

            ;; Begin transaction, add but don't commit
            (.begin conn)
            (.add conn s p o (into-array Resource []))

            ;; hasStatement should see the pending add
            (is (.hasStatement conn s p o true (into-array Resource []))
                "hasStatement must see uncommitted add (read-your-own-writes)")

            (.commit conn)
            (wait-mb!)

            ;; After commit, still visible
            (is (.hasStatement conn s p o true (into-array Resource []))
                "Should exist after commit")
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-has-statement-respects-pending-delete
  (testing "hasStatement does not see a statement after uncommitted delete"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "has-stmt-del"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "to-delete")]
          (try
            ;; First, add and commit the triple
            (.begin conn)
            (.add conn s p o (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            ;; Verify it exists
            (is (.hasStatement conn s p o true (into-array Resource []))
                "Should exist after initial commit")

            ;; Begin new transaction, delete but don't commit
            (.begin conn)
            (.remove conn s p o (into-array Resource []))

            ;; hasStatement should NOT see the deleted triple
            (is (not (.hasStatement conn s p o true (into-array Resource [])))
                "hasStatement must not see pending-deleted triple")

            (.commit conn)
            (wait-mb!)
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-has-statement-respects-pending-clear
  (testing "hasStatement respects pending clear of a context"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [conn (.getConnection repo)
              s (.createIRI VF (unique-iri "has-stmt-clear"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "clear-me")
              ctx (.createIRI VF (unique-iri "graph-clear"))]
          (try
            ;; Add triple in a named graph and commit
            (.begin conn)
            (.add conn s p o (into-array Resource [ctx]))
            (.commit conn)
            (wait-mb!)

            ;; Verify it exists in that context
            (is (.hasStatement conn s p o true (into-array Resource [ctx]))
                "Should exist in named graph after commit")

            ;; Begin new transaction, clear the context
            (.begin conn)
            (.clear conn (into-array Resource [ctx]))

            ;; hasStatement should NOT see the triple in the cleared context
            (is (not (.hasStatement conn s p o true (into-array Resource [ctx])))
                "hasStatement must not see triples in pending-cleared context")

            (.rollback conn)
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))
