(ns rama-sail.module.indexer-test
  "Module-level tests for indexer statistics consistency, tombstone pruning,
   and namespace clearing. Appends directly to the depots (bypassing the SAIL
   adapter's per-transaction netting) to exercise microbatch-level behavior.

   Uses a shared IPC fixture with unique IRIs for test isolation. State
   visibility uses settle-polling: wait-for-microbatch-processed-count counts
   depot RECORDS (not iterations), so ad-hoc counting drifts; polling on the
   observable state is simplest for these assertions.

   NOTE on probes: $$spoc etc. hold subindexed nested structures, which cannot
   be fetched wholesale by foreign-select — probe existence with (view some?)."
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.path :refer [keypath view]]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]))

(def module-name (rama/get-module-name RdfStorageModule))

(def ^:dynamic *ipc* nil)

(defn indexer-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc]
      (f))))

(use-fixtures :once indexer-fixture)

(defn unique-iri [base]
  (str "<http://ex/" base "-" (System/nanoTime) ">"))

(defn- depot [] (rama/foreign-depot *ipc* module-name "*triple-depot"))
(defn- pstate [name] (rama/foreign-pstate *ipc* module-name name))

(defn- global-stat [k]
  (or (rama/foreign-select-one (keypath k) (pstate "$$global-stats")) 0))

(defn- subject-present? [s]
  (boolean (rama/foreign-select-one [(keypath s) (view some?)] (pstate "$$spoc"))))

(defn settle!
  "Poll until f returns truthy (up to ~15s), then allow a grace period for
   any straddled microbatch iterations to land. Returns the last value of f."
  [f]
  (loop [i 0]
    (let [v (f)]
      (cond
        v (do (Thread/sleep 500) (f))
        (>= i 60) v
        :else (do (Thread/sleep 250) (recur (inc i)))))))

;;; ---------------------------------------------------------------------------
;;; S1: duplicate operations must not double-count statistics
;;; ---------------------------------------------------------------------------

(deftest test-duplicate-adds-count-once
  (testing "two :add appends of the same quad increment stats exactly once"
    ;; Regression: the existence check and the $$quad-tx-time write were
    ;; separated by partitioner hops, so duplicates in one microbatch each saw
    ;; 'not present' and stats were permanently double-counted.
    (let [s (unique-iri "dup-add-s")
          p (unique-iri "dup-add-p")
          o "\"dup-value\""
          c (unique-iri "dup-add-g")
          triples-before (global-stat :total-triples)
          subjects-before (global-stat :total-subjects)
          preds-before (global-stat :total-predicates)]
      (rama/foreign-append! (depot) [:add [s p o c]])
      (rama/foreign-append! (depot) [:add [s p o c]])
      (is (settle! #(some? (rama/foreign-select-one (keypath p :count)
                                                    (pstate "$$predicate-stats"))))
          "add must become visible")
      (is (= 1 (rama/foreign-select-one (keypath p :count) (pstate "$$predicate-stats")))
          "predicate :count must be 1, not 2")
      (is (= 1 (rama/foreign-select-one (keypath p :distinct-subjects) (pstate "$$predicate-stats"))))
      (is (= (inc triples-before) (global-stat :total-triples))
          ":total-triples must grow by exactly 1")
      (is (= (inc subjects-before) (global-stat :total-subjects))
          "S3: a brand-new subject increments :total-subjects once")
      (is (= (inc preds-before) (global-stat :total-predicates))
          "S3: a brand-new predicate increments :total-predicates once"))))

(deftest test-duplicate-dels-count-once
  (testing "two :del appends of the same quad decrement stats exactly once"
    (let [s (unique-iri "dup-del-s")
          p (unique-iri "dup-del-p")
          o "\"dup-del-value\""
          c (unique-iri "dup-del-g")]
      (rama/foreign-append! (depot) [:add [s p o c]])
      (is (settle! #(subject-present? s)))
      (let [triples-after-add (global-stat :total-triples)]
        (rama/foreign-append! (depot) [:del [s p o c]])
        (rama/foreign-append! (depot) [:del [s p o c]])
        (settle! #(not (subject-present? s)))
        (is (not (subject-present? s)) "quad deleted")
        (is (= 0 (or (rama/foreign-select-one (keypath p :count) (pstate "$$predicate-stats")) 0))
            "predicate :count must return to 0")
        (is (= (dec triples-after-add) (global-stat :total-triples))
            ":total-triples must shrink by exactly 1, not 2")))))

;;; ---------------------------------------------------------------------------
;;; S2: deleting the last quad prunes empty containers
;;; ---------------------------------------------------------------------------

(deftest test-delete-prunes-empty-containers
  (testing "deleting a subject's only quad removes its entries from all indices"
    ;; Regression: empty sets/maps were left behind forever, growing the
    ;; indices without bound under churn workloads.
    (let [s (unique-iri "prune-s")
          p (unique-iri "prune-p")
          o (unique-iri "prune-o")
          c (unique-iri "prune-g")
          present? (fn [pstate-name k]
                     (boolean (rama/foreign-select-one [(keypath k) (view some?)]
                                                       (pstate pstate-name))))]
      (rama/foreign-append! (depot) [:add [s p o c]])
      (is (settle! #(subject-present? s)))
      (rama/foreign-append! (depot) [:del [s p o c]])
      (settle! #(not (subject-present? s)))
      (is (not (present? "$$spoc" s))
          "$$spoc subject entry must be pruned, not left as an empty map")
      (is (not (present? "$$posc" p)) "$$posc predicate entry must be pruned")
      (is (not (present? "$$ospc" o)) "$$ospc object entry must be pruned")
      (is (not (present? "$$cspo" c)) "$$cspo context entry must be pruned")
      (is (not (present? "$$quad-tx-time" s))
          "$$quad-tx-time subject entry must be pruned"))))

(deftest test-subject-and-predicate-vanish-decrements-totals
  (testing "S3: deleting a subject's/predicate's last triple decrements the global totals"
    (let [s (unique-iri "vanish-s")
          p (unique-iri "vanish-p")
          o "\"vanish-value\""
          c (unique-iri "vanish-g")
          subjects-before (global-stat :total-subjects)
          preds-before (global-stat :total-predicates)]
      (rama/foreign-append! (depot) [:add [s p o c]])
      (is (settle! #(subject-present? s)))
      (is (= (inc subjects-before) (global-stat :total-subjects)))
      (is (= (inc preds-before) (global-stat :total-predicates)))
      (rama/foreign-append! (depot) [:del [s p o c]])
      (settle! #(not (subject-present? s)))
      (is (= subjects-before (global-stat :total-subjects))
          "subject vanished — :total-subjects back to baseline")
      (is (= preds-before (global-stat :total-predicates))
          "predicate vanished — :total-predicates back to baseline"))))

;;; ---------------------------------------------------------------------------
;;; S4: :clear-ns must clear every partition
;;; ---------------------------------------------------------------------------

(deftest test-clear-ns-clears-all-partitions
  (testing ":clear-ns broadcast empties $$namespaces on every task"
    ;; Regression: the record landed on one task (and with a nil partition key
    ;; never reached the topology at all) so partitions kept their entries.
    (let [ns-depot (rama/foreign-depot *ipc* module-name "*namespace-depot")
          list-ns (rama/foreign-query *ipc* module-name "list-namespaces")
          prefixes (mapv #(str "p" % "-" (System/nanoTime)) (range 8))
          all-present? #(let [m (rama/foreign-invoke-query list-ns)]
                          (every? (fn [p] (contains? m p)) prefixes))
          none-present? #(let [m (rama/foreign-invoke-query list-ns)]
                           (not-any? (fn [p] (contains? m p)) prefixes))]
      (doseq [prefix prefixes]
        (rama/foreign-append! ns-depot [:set-ns prefix (str "http://ex/" prefix)]))
      (is (settle! all-present?)
          "all prefixes stored (spread across partitions)")
      (rama/foreign-append! ns-depot [:clear-ns])
      (is (settle! none-present?)
          "every partition must be cleared, not just the one the record landed on"))))
