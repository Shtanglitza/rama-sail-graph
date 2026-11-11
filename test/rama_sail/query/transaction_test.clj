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
            [com.rpl.rama.path :refer [keypath]]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.module.indexer :as idx]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.serialization :as ser])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.query BindingSet]
           [org.eclipse.rdf4j.model Literal]
           [org.eclipse.rdf4j.repository.sail SailRepository SailRepositoryConnection]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]))

(def ^:private ^org.eclipse.rdf4j.model.ValueFactory VF (SimpleValueFactory/getInstance))
(def module-name (rama/get-module-name RdfStorageModule))

;; Shared IPC
(def ^:dynamic *ipc* nil)

(defn transaction-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc]
      (f))))

(use-fixtures :once transaction-fixture)

(defn wait-mb!
  "Barrier: wait until the indexer has processed every depot record appended
   so far. wait-for-microbatch-processed-count counts DEPOT RECORDS (not
   iterations), so the exact target is the sum of the depot partitions'
   end-offsets — no manual counter to drift when a commit appends several
   records. Queued commits and graph clears are applied in bounded slices over
   several microbatches (the indexer appends a wake-up record per slice), so
   the barrier repeats until neither is pending."
  []
  (let [depot (rama/foreign-depot *ipc* module-name "*triple-depot")
        num-partitions (:num-partitions (rama/foreign-object-info depot))
        pending-commits (rama/foreign-query *ipc* module-name "pending-commit-count")
        pending-clears (rama/foreign-query *ipc* module-name "pending-clear-count")]
    (loop []
      (rtest/wait-for-microbatch-processed-count
       *ipc* module-name "indexer"
       (reduce + (for [part (range num-partitions)]
                   (:end-offset (rama/foreign-depot-partition-info depot part)))))
      (when (or (pos? (rama/foreign-invoke-query pending-commits))
                (pos? (rama/foreign-invoke-query pending-clears)))
        (Thread/sleep 20)
        (recur)))))

(defn unique-iri
  "Generate unique IRI for test isolation."
  [base]
  (str "http://ex/" base "-" (System/nanoTime)))

;; Helper to count statements matching a pattern
(defn count-statements [^SailRepositoryConnection conn s p o contexts]
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
              s (.createIRI VF (unique-iri "rollback-test"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "should-not-exist")
              sentinel-s (.createIRI VF (unique-iri "rollback-sentinel"))
              sentinel-o (.createLiteral VF "sentinel")]
          (try
            ;; Begin transaction, add, rollback
            (.begin conn)
            (.add conn s p o (into-array Resource []))
            (.rollback conn)

            ;; Deterministic barrier: commit a sentinel triple on the same
            ;; connection, then wait until ALL appended depot records are
            ;; processed. If the rolled-back add had (incorrectly) reached the
            ;; depot, it would have been indexed by now too.
            (.begin conn)
            (.add conn sentinel-s p sentinel-o (into-array Resource []))
            (.commit conn)
            (wait-mb!)

            ;; Sentinel IS visible - proves the barrier covers the commit
            (is (= 1 (count-statements conn sentinel-s p sentinel-o []))
                "Sentinel committed after rollback must be visible")
            ;; Rolled-back data is NOT visible - it never landed in the depot
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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

            ;; Wait for 1 net operation: add+del of same quad deduplicates to last-write (del)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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

            ;; Wait for 1 net operation: del+add of same quad deduplicates to 1 add
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
                (is (= "Anonymous" (.stringValue ^org.eclipse.rdf4j.model.Value (.getValue ^BindingSet (first results) "name"))))))
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
        (let [^SailRepositoryConnection conn (.getConnection repo)]
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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
        (let [^SailRepositoryConnection conn (.getConnection repo)
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

;;; ---------------------------------------------------------------------------
;;; Post-Commit Persistence Tests for Clear + Add Ordering
;;; ---------------------------------------------------------------------------

(deftest test-clear-then-add-persists
  (testing "clear(ctx) then add(quad) in same transaction: quad must exist after commit"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [^SailRepositoryConnection conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "clear-add-ctx"))
              s (.createIRI VF (unique-iri "clear-add-s"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "survives-clear")]
          (try
            ;; First: commit an existing quad in the context
            (.begin conn)
            (.add conn s p o (into-array Resource [ctx]))
            (.commit conn)
            (wait-mb!)

            ;; Second: clear the context, then add a new quad, in one transaction
            (let [s2 (.createIRI VF (unique-iri "clear-add-s2"))
                  o2 (.createLiteral VF "post-clear-value")]
              (.begin conn)
              (.clear conn (into-array Resource [ctx]))
              (.add conn s2 p o2 (into-array Resource [ctx]))
              (.commit conn)

              ;; 2 ops committed: clear-context + add
              (wait-mb!)
              (wait-mb!)

              ;; The original quad should be gone (cleared)
              (is (= 0 (count-statements conn s p o [ctx]))
                  "Original quad should be removed by clear")
              ;; The post-clear add should survive
              (is (= 1 (count-statements conn s2 p o2 [ctx]))
                  "Quad added after clear must exist after commit"))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-add-clear-add-persists
  (testing "add(q) then clear(ctx) then add(q) in same transaction: quad must exist after commit"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [^SailRepositoryConnection conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "add-clear-add-ctx"))
              s (.createIRI VF (unique-iri "aca-s"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "aca-value")]
          (try
            (.begin conn)
            (.add conn s p o (into-array Resource [ctx]))
            (.clear conn (into-array Resource [ctx]))
            (.add conn s p o (into-array Resource [ctx]))
            (.commit conn)

            ;; 2 ops committed: clear-context + add (pre-clear add is discarded)
            (wait-mb!)
            (wait-mb!)

            ;; The post-clear re-add should survive
            (is (= 1 (count-statements conn s p o [ctx]))
                "Quad re-added after clear must exist after commit")
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-add-then-clear-removes
  (testing "add(q) then clear(ctx) in same transaction: quad must NOT exist after commit"
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [^SailRepositoryConnection conn (.getConnection repo)
              ctx (.createIRI VF (unique-iri "add-clear-ctx"))
              s (.createIRI VF (unique-iri "ac-s"))
              p (.createIRI VF "http://ex/p")
              o (.createLiteral VF "ac-value")]
          (try
            (.begin conn)
            (.add conn s p o (into-array Resource [ctx]))
            (.clear conn (into-array Resource [ctx]))
            (.commit conn)

            ;; 1 op committed: clear-context (add is discarded, subsumed by clear)
            (wait-mb!)

            ;; The quad should be gone (clear came after add)
            (is (= 0 (count-statements conn s p o [ctx]))
                "Quad added before clear must NOT exist after commit")
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

(deftest test-wildcard-hasstatement-sees-named-graph-pending-ops
  (testing "hasStatement with wildcard context honors pending deletes/clears in named graphs"
    ;; Regression (M7): with an empty contexts array the ASK matched a quad in
    ;; ANY graph, but the pending-delete check only tested the default-context
    ;; quad — a statement deleted (or its graph cleared) in the current
    ;; transaction still reported true.
    (let [sail (rsail/create-rama-sail *ipc* module-name)
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (let [^SailRepositoryConnection conn (.getConnection repo)
              s (.createIRI VF (unique-iri "ryow-del"))
              p (.createIRI VF "http://ex/ryow-p")
              o (.createLiteral VF "ryow-value")
              g (.createIRI VF (unique-iri "ryow-graph"))
              s2 (.createIRI VF (unique-iri "ryow-clear"))
              g2 (.createIRI VF (unique-iri "ryow-clear-graph"))]
          (try
            ;; Commit statements into named graphs
            (.begin conn)
            (.add conn s p o (into-array Resource [g]))
            (.add conn s2 p o (into-array Resource [g2]))
            (.commit conn)
            (wait-mb!)
            (is (true? (.hasStatement conn s p o true (into-array Resource []))))
            (is (true? (.hasStatement conn s2 p o true (into-array Resource []))))

            (testing "pending remove from a named graph"
              (.begin conn)
              (.remove conn s p o (into-array Resource [g]))
              (is (false? (.hasStatement conn s p o true (into-array Resource [])))
                  "Wildcard hasStatement must see the pending named-graph delete")
              (.rollback conn)
              (is (true? (.hasStatement conn s p o true (into-array Resource [])))
                  "After rollback the statement is visible again"))

            (testing "pending clear of a named graph"
              (.begin conn)
              (.clear conn (into-array Resource [g2]))
              (is (false? (.hasStatement conn s2 p o true (into-array Resource [])))
                  "Wildcard hasStatement must see the pending graph clear")
              (.rollback conn))
            (finally
              (.close conn))))
        (finally
          (.shutDown repo))))))

;;; ---------------------------------------------------------------------------
;;; Phase 6 data-loss regressions (D1-D3)
;;; ---------------------------------------------------------------------------

(defn- with-conn
  "Run (f conn) against a fresh RamaSail repository on the shared IPC."
  [f]
  (let [repo (SailRepository. (rsail/create-rama-sail *ipc* module-name))]
    (.init repo)
    (try
      (let [^SailRepositoryConnection conn (.getConnection repo)]
        (try (f conn) (finally (.close conn))))
      (finally (.shutDown repo)))))

(defn- sparql-count
  "Number of solutions of a SELECT query (exercises the Rama query path / posc index)."
  [^SailRepositoryConnection conn ^String sparql]
  (with-open [iter (.evaluate (.prepareTupleQuery conn sparql))]
    (count (iterator-seq iter))))

(deftest test-clear-then-readd-existing-quads-persists
  (testing "D2: CLEAR GRAPH g + re-INSERT the SAME already-committed quads, one transaction"
    ;; The 'replace graph' pattern (Graph Store PUT, DROP+INSERT DATA). The re-add of
    ;; an already-visible quad used to be a no-op that left its old creation stamp,
    ;; so when it ran before the clear in the same microbatch (clear and add live on
    ;; different partitions) the clear (clear-tx >= old stamp) deleted it.
    (with-conn
      (fn [^SailRepositoryConnection conn]
        (let [ctx (.createIRI VF (unique-iri "readd-ctx"))
              p (.createIRI VF (unique-iri "readd-p"))
              n 60
              quads (vec (for [i (range n)]
                           [(.createIRI VF (unique-iri (str "readd-s" i)))
                            (.createLiteral VF (str "v" i))]))]
          (.begin conn)
          (doseq [[s o] quads] (.add conn s p o (into-array Resource [ctx])))
          (.commit conn)
          (wait-mb!)
          (is (= n (count-statements conn nil nil nil [ctx])))

          (.begin conn)
          (.clear conn (into-array Resource [ctx]))
          (doseq [[s o] quads] (.add conn s p o (into-array Resource [ctx])))
          (.commit conn)
          (wait-mb!)

          (is (= n (count-statements conn nil nil nil [ctx]))
              "Every re-added quad must survive the clear that preceded it")
          (is (= n (sparql-count conn (str "SELECT ?s ?o WHERE { GRAPH <" ctx "> { ?s <" p "> ?o } }")))
              "Predicate-index (posc) view must agree with the context index")

          (testing "a later clear still removes the refreshed quads"
            (.begin conn)
            (.clear conn (into-array Resource [ctx]))
            (.commit conn)
            (wait-mb!)
            (is (= 0 (count-statements conn nil nil nil [ctx])))))))))

(deftest test-bulk-commit-then-clear-removes-everything
  (testing "D1: a CLEAR issued right after a large commit removes ALL of its quads"
    ;; Each op used to be stamped tx-time + op-index ms, so a 5000-op commit was
    ;; dated up to 5 s into the future and an immediate CLEAR (clear-tx < stamp)
    ;; skipped every quad stamped after the clear's wall-clock time.
    (with-conn
      (fn [^SailRepositoryConnection conn]
        (let [ctx (.createIRI VF (unique-iri "bulk-ctx"))
              p (.createIRI VF "http://ex/bulk-p")
              n 5000]
          (.begin conn)
          (dotimes [i n]
            (.add conn (.createIRI VF (str "http://ex/bulk-s-" i)) p
                  (.createLiteral VF (long i)) (into-array Resource [ctx])))
          (.commit conn)
          (let [committed-at (System/currentTimeMillis)]
            (wait-mb!)
            (is (= n (count-statements conn nil p nil [ctx])))

            ;; Deterministic check (the clear below only catches the bug when depot
            ;; appends outpace 1/ms): every quad of the commit carries ONE stamp,
            ;; and none is dated after the commit returned.
            (let [tx-times (rama/foreign-pstate *ipc* module-name "$$quad-tx-time")
                  stamp (fn [i] (rama/foreign-select-one
                                 [(keypath (str "<http://ex/bulk-s-" i ">") (ser/val->str p)
                                           (ser/val->str (.createLiteral VF (long i)))
                                           (ser/val->str ctx))]
                                 tx-times))
                  first-stamp (stamp 0)
                  last-stamp (stamp (dec n))]
              (is (some? first-stamp))
              (is (= first-stamp last-stamp)
                  "All ops of a clear-free commit share the commit stamp (was tx-time + op-index)")
              (is (<= last-stamp committed-at)
                  "No quad may be stamped in the future")))

          (.begin conn)
          (.clear conn (into-array Resource [ctx]))
          (.commit conn)
          (wait-mb!)
          (is (= 0 (count-statements conn nil nil nil [ctx]))
              "No quad of the bulk commit may survive the subsequent clear"))))))

(deftest test-remove-default-graph-wildcard-spares-named-graph-pending-adds
  (testing "D3: remove(s, *, *, (Resource) null) targets ONLY the default graph"
    ;; A null context entry means the default graph. It was mapped to nil, which
    ;; the pending-add matcher treats as 'any graph', so the remove also cancelled
    ;; uncommitted adds of the same subject in every named graph.
    (with-conn
      (fn [^SailRepositoryConnection conn]
        (let [s (.createIRI VF (unique-iri "nullctx-s"))
              p (.createIRI VF "http://ex/nullctx-p")
              o (.createLiteral VF "named")
              o-def (.createLiteral VF "default")
              g (.createIRI VF (unique-iri "nullctx-g"))
              default-only (into-array Resource [nil])]
          (.begin conn)
          (.add conn s p o (into-array Resource [g]))
          (.add conn s p o-def (into-array Resource []))
          (.remove conn s nil nil default-only)
          (is (= 1 (count-statements conn s nil nil [g]))
              "Pending named-graph add must survive a default-graph remove (RYOW)")
          (is (= 0 (count-statements conn s nil nil [nil]))
              "Pending default-graph add is removed")
          (.commit conn)
          (wait-mb!)
          (is (= 1 (count-statements conn s p o [g]))
              "Named-graph quad must be committed")
          (is (= 0 (count-statements conn s nil nil [nil]))
              "Default-graph quad must not be committed")

          (testing "same rule for committed data"
            (.begin conn)
            (.add conn s p o-def (into-array Resource []))
            (.commit conn)
            (wait-mb!)
            (.begin conn)
            (.remove conn s nil nil default-only)
            (.commit conn)
            (wait-mb!)
            (is (= 1 (count-statements conn s nil nil [g])))
            (is (= 0 (count-statements conn s nil nil [nil])))))))))

(deftest test-sync-commit-waits-for-a-clear-applied-in-slices
  ;; The indexer deletes a bounded number of quads per microbatch (see
  ;; indexer-test). With :sync-commits the commit of a clear must return only
  ;; once every slice is applied, not after the microbatch that registered it.
  (with-redefs [idx/clear-quads-per-microbatch 7
                idx/clear-subjects-per-page 3]
    (let [repo (doto (SailRepository. (rsail/create-rama-sail *ipc* module-name {:sync-commits true}))
                 (.init))]
      (try
        (with-open [^SailRepositoryConnection conn (.getConnection repo)]
          (let [g (.createIRI VF (unique-iri "sliced-clear-g"))
                p (.createIRI VF "http://ex/sliced-clear-p")
                in-g (into-array Resource [g])]
            (.begin conn)
            (dotimes [i 40]
              (.add conn (.createIRI VF (unique-iri (str "sliced-clear-s" i))) p (.createLiteral VF (str i)) in-g))
            (.commit conn)
            (is (= 40 (count-statements conn nil nil nil [g])))

            (.begin conn)
            (.clear conn in-g)
            (.commit conn)
            (is (= 0 (count-statements conn nil nil nil [g]))
                "all 40 quads are gone when the commit returns, though the clear took several microbatches")

            (.begin conn)
            (.add conn (.createIRI VF (unique-iri "sliced-clear-after")) p (.createLiteral VF "after") in-g)
            (.commit conn)
            (is (= 1 (count-statements conn nil nil nil [g])))))
        (finally (.shutDown repo))))))

(deftest test-commit-is-sent-as-commit-records
  (let [stamped (vec (for [i (range 12)] [:add [(str "<s" i ">") "<p>" "<o>" "<g>"] 1000]))
        records @#'rsail/commit-depot-records]
    (testing "one :commit record per chunk, ops in order"
      (is (= [[:commit stamped]] (records stamped true 5000)))
      (is (= [[:commit (subvec stamped 0 5)] [:commit (subvec stamped 5 10)] [:commit (subvec stamped 10 12)]]
             (records stamped true 5))))
    (testing "a module without commit records gets one record per op"
      (is (= stamped (records stamped false 5))))))

(deftest test-large-commit-in-chunks-and-slices
  ;; 60 ops with 25 per :commit record and 7 ops applied per microbatch: three
  ;; records, applied over about nine microbatches. :sync-commits returns only
  ;; when all of it is visible.
  (with-redefs [idx/commit-ops-per-microbatch 7]
    (let [repo (doto (SailRepository. (rsail/create-rama-sail *ipc* module-name {:sync-commits true
                                                                                 :commit-chunk-size 25}))
                 (.init))]
      (try
        (with-open [^SailRepositoryConnection conn (.getConnection repo)]
          (let [g (.createIRI VF (unique-iri "chunked-g"))
                p (.createIRI VF "http://ex/chunked-p")
                in-g (into-array Resource [g])
                subj (vec (for [i (range 60)] (.createIRI VF (unique-iri (str "chunked-s" i)))))]
            (.begin conn)
            (doseq [s subj] (.add conn s p (.createLiteral VF "v") in-g))
            (.commit conn)
            (is (= 60 (count-statements conn nil nil nil [g])))

            (testing "a following commit that removes half and clears nothing"
              (.begin conn)
              (doseq [s (take 30 subj)] (.remove conn s p nil in-g))
              (.commit conn)
              (is (= 30 (count-statements conn nil nil nil [g]))))

            (testing "clear and re-add in one commit"
              (.begin conn)
              (.clear conn in-g)
              (doseq [s (take 40 subj)] (.add conn s p (.createLiteral VF "again") in-g))
              (.commit conn)
              (is (= 40 (count-statements conn nil nil nil [g]))))))
        (finally (.shutDown repo))))))

