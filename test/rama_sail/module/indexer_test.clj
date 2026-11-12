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
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.module.indexer :as idx]
            [rama-sail.test-helpers :as th]))

(def module-name (rama/get-module-name RdfStorageModule))

(def ^:dynamic *ipc* nil)

(defn indexer-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (binding [*ipc* ipc]
      (f))))

(use-fixtures :once indexer-fixture)

(defn- quiesce!
  "Wait until the indexer has applied everything appended so far, statistics
   included. Quads become visible per task before the microbatch that writes
   the statistics has finished everywhere, so polling on quads alone let
   statistics assertions (and the next test's baselines) race the indexer."
  []
  (th/wait-indexed! *ipc* module-name))

;; Each test starts from a settled indexer: baselines read at the start of a
;; test must not move because an earlier test's records are still in flight.
(use-fixtures :each (fn [f] (quiesce!) (f)))

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
  (quiesce!)
  (loop [i 0]
    (let [v (f)]
      (cond
        v (do (quiesce!) (f))
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

;;; ---------------------------------------------------------------------------
;;; Graph clears run in bounded slices
;;; ---------------------------------------------------------------------------
;;; A :clear-context used to delete every quad of the graph in the microbatch
;;; that carried it. Past Rama's phase timeout that microbatch fails and is
;;; retried from the start with the same data, forever: clearing a 40K-quad
;;; graph froze the indexer. Clears are now registered in $$pending-clears and
;;; applied a bounded number of quads per microbatch.

(deftest test-clear-slice
  (let [page [["s1" "p" "a"] ["s1" "p" "b"] ["s2" "p" "a"] ["s3" "p" "a"] ["s3" "q" "b"]]]
    (testing "takes whole subjects while they fit the budget"
      (is (= {:quads (subvec page 0 3) :next-cursor "s2" :exhausted? false}
             (idx/clear-slice page 10 3)))
      (is (= {:quads (subvec page 0 3) :next-cursor "s2" :exhausted? false}
             (idx/clear-slice page 10 4))
          "s3 has two quads and only one fits"))
    (testing "a subject larger than the budget is still taken, so the clear progresses"
      (is (= {:quads (subvec page 0 2) :next-cursor "s1" :exhausted? false}
             (idx/clear-slice page 10 1))))
    (testing "a short page taken whole ends the scan"
      (is (= {:quads page :next-cursor "s3" :exhausted? true}
             (idx/clear-slice page 10 100))))
    (testing "a full page may have more subjects after it"
      (is (false? (:exhausted? (idx/clear-slice page 3 100)))))
    (testing "an empty graph is done"
      (is (= {:quads [] :next-cursor "" :exhausted? true}
             (idx/clear-slice [] 10 100))))))

(deftest test-merge-pending-clear
  (is (= {:tx 5 :cursor ""} (idx/merge-pending-clear nil 5)))
  (is (= {:tx 9 :cursor ""} (idx/merge-pending-clear {:tx 5 :cursor "s7"} 9))
      "a later clear restarts the scan: quads stamped between the two may lie behind the cursor")
  (is (= {:tx 5 :cursor "s7"} (idx/merge-pending-clear {:tx 5 :cursor "s7"} 5)))
  (is (= {:tx 5 :cursor "s7"} (idx/merge-pending-clear {:tx 5 :cursor "s7"} 3))))

(defn- wait-until
  "Poll until f returns truthy or timeout-ms passes. Returns the last value of f."
  [timeout-ms f]
  (quiesce!)
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [v (f)]
        (if (or v (> (System/currentTimeMillis) deadline))
          (do (quiesce!) v)
          (do (Thread/sleep 250) (recur)))))))

(defn- graph-quads [g]
  (set (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "find-triples") nil nil nil g)))

(defn- pending-clears []
  (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "pending-clear-count")))

(deftest test-clear-larger-than-budget-runs-in-slices
  ;; 7 quads per task per microbatch and 3 subjects per page: clearing this
  ;; 61-quad graph takes a dozen microbatches, each woken by the previous one.
  (with-redefs [idx/clear-quads-per-microbatch 7
                idx/clear-subjects-per-page 3]
    (let [g (unique-iri "sliced-g")
          other (unique-iri "sliced-other-g")
          p (unique-iri "sliced-p")
          subj #(str "<http://ex/sliced-s" (format "%03d" %) ">")
          ;; 40 subjects with 1-2 quads each, plus one with 10 quads (> budget)
          quads (concat (for [i (range 40), j (range (inc (mod i 2)))]
                          [(subj i) p (str "\"v" j "\"") g])
                        (for [j (range 10)] [(subj 999) p (str "\"big" j "\"") g]))
          kept [(subj 5) p "\"added after the clear\"" g]
          other-quads (for [i (range 5)] [(subj i) p "\"other\"" other])
          triples-before (global-stat :total-triples)]
      (is (= 70 (count quads)))
      (doseq [q (concat quads other-quads)]
        (rama/foreign-append! (depot) [:add q 1000]))
      (is (settle! #(= (+ triples-before 75) (global-stat :total-triples))) "load visible")

      (rama/foreign-append! (depot) [:clear-context [nil nil nil g] 2000])
      (rama/foreign-append! (depot) [:add kept 3000])
      ;; about a dozen microbatches, one slice each
      (is (wait-until 90000 #(and (zero? (pending-clears)) (= #{kept} (graph-quads g))))
          "the clear finishes and removes exactly the quads stamped at or before it")
      (is (= #{kept} (graph-quads g)))
      (is (= (set other-quads) (graph-quads other)) "another graph is untouched")
      (is (= (+ triples-before 6) (global-stat :total-triples))
          "each cleared quad is counted out exactly once")
      (is (= 6 (rama/foreign-select-one (keypath p :count) (pstate "$$predicate-stats"))))

      (testing "the indexer keeps processing after a multi-slice clear"
        (let [late [(subj 1) p "\"late\"" g]]
          (rama/foreign-append! (depot) [:add late 4000])
          (is (settle! #(= #{kept late} (graph-quads g)))))))))

(deftest test-clear-of-empty-graph-leaves-nothing-pending
  (rama/foreign-append! (depot) [:clear-context [nil nil nil (unique-iri "no-such-g")] 1000])
  (Thread/sleep 1500)
  (is (settle! #(zero? (pending-clears)))))

;;; ---------------------------------------------------------------------------
;;; Malformed depot records are dropped, not retried forever
;;; ---------------------------------------------------------------------------
;;; A record that throws in the topology fails its microbatch, which Rama
;;; retries from the start with the same data, forever. The depot accepts any
;;; object, so the indexer must not let a wrong-shaped record throw.

(deftest test-valid-depot-record
  (let [q ["<s>" "<p>" "\"o\"" "<g>"]]
    (is (idx/valid-depot-record? [:add q]))
    (is (idx/valid-depot-record? [:add q 1000]))
    (is (idx/valid-depot-record? [:del q nil]))
    (is (idx/valid-depot-record? [:clear-context [nil nil nil "<g>"] 1000]))
    (doseq [bad [nil "garbage" [] [:add] [:add nil] [:add (pop q)] [:add (conj q "<x>")]
                 [:add (assoc q 2 5)] [:add q "not-a-time"] [:add q 1000 :extra]
                 [:del (assoc q 1 nil)] [:clear-context [nil nil nil nil]]
                 [:no-such-op q] [:clear-continue ["<g>"]]]]
      (is (not (idx/valid-depot-record? bad)) (pr-str bad)))
    (testing ":config records"
      (is (idx/valid-depot-record? [:config {:commit-ops-per-microbatch 60000}]))
      (is (idx/valid-depot-record? [:config {:commit-ops-per-microbatch 1 :clear-quads-per-microbatch 5000}]))
      (doseq [bad [[:config] [:config {}] [:config nil] [:config {:no-such-option 5}]
                   [:config {:commit-ops-per-microbatch 0}] [:config {:commit-ops-per-microbatch "many"}]
                   [:config {:commit-ops-per-microbatch 5} :extra]]]
        (is (not (idx/valid-depot-record? bad)) (pr-str bad))))
    (testing ":commit records"
      (is (idx/valid-depot-record? [:commit [[:add q 1000] [:del q 1000] [:clear-context [nil nil nil "<g>"] 1000]]]))
      (doseq [bad [[:commit] [:commit nil] [:commit []] [:commit [[:add q] [:add (pop q)]]]
                   [:commit [[:commit [[:add q]]]]] [:commit [[:add q]] :extra]
                   [:commit (vec (repeat (inc idx/max-commit-ops) [:add q 1]))]]]
        (is (not (idx/valid-depot-record? bad)) (pr-str (take 2 bad)))))))

(deftest test-malformed-records-do-not-block-the-indexer
  (let [g (unique-iri "malformed-g")
        good [(unique-iri "malformed-s") (unique-iri "malformed-p") "\"ok\"" g]]
    (doseq [bad [[:add nil]
                 [:add ["<http://ex/only-s>" "<http://ex/only-p>"]]
                 [:add ["<http://ex/s>" "<http://ex/p>" 5 g]]
                 [:add ["<http://ex/s>" "<http://ex/p>" "\"o\"" g] "not-a-time"]
                 [:del ["<http://ex/s>" nil nil g]]
                 [:clear-context ["<http://ex/x>" nil nil nil]]
                 [:no-such-op ["<http://ex/s>" "<http://ex/p>" "\"o\"" g]]]]
      (rama/foreign-append! (depot) bad))
    (rama/foreign-append! (depot) [:add good])
    (is (wait-until 30000 #(= #{good} (graph-quads g)))
        "a valid record appended after malformed ones is still indexed")))

;;; ---------------------------------------------------------------------------
;;; :commit records are queued and applied in order, in bounded slices
;;; ---------------------------------------------------------------------------

(deftest test-commit-slice
  (let [ops [:a :b :c :d :e]]
    (is (= {:part [:a :b] :next-pos 2 :done? false} (idx/commit-slice ops 0 2)))
    (is (= {:part [:c :d :e] :next-pos 5 :done? true} (idx/commit-slice ops 2 10)))
    (is (= {:part [:e] :next-pos 5 :done? true} (idx/commit-slice ops 4 1)))
    (is (= {:part [] :next-pos 2 :done? false} (idx/commit-slice ops 2 0)))))

(defn- pending-commits []
  (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "pending-commit-count")))

(defn- idle? [] (and (zero? (pending-commits)) (zero? (pending-clears))))

(deftest test-commit-record-applies-all-its-ops
  (let [g (unique-iri "commit-g")
        p (unique-iri "commit-p")
        q (fn [i] [(str "<http://ex/commit-s" i ">") p (str "\"v" i "\"") g])]
    (rama/foreign-append! (depot) [:commit (vec (for [i (range 20)] [:add (q i) 1000]))])
    (is (wait-until 30000 #(= 20 (count (graph-quads g)))))
    (testing "a commit that clears the graph and adds to it: the add is stamped later and survives"
      (rama/foreign-append! (depot) [:commit [[:clear-context [nil nil nil g] 2000]
                                              [:add (q 100) 2001]
                                              [:del (q 3) 2001]]])
      (is (wait-until 30000 #(and (idle?) (= #{(q 100)} (graph-quads g))))))
    (is (= 1 (rama/foreign-select-one (keypath p :count) (pstate "$$predicate-stats"))))))

(deftest test-commits-apply-in-append-order
  ;; Commits touching the same quads must take effect in the order they were
  ;; appended: every :commit record goes to one depot partition and the queue
  ;; is drained oldest first, also across slices (budget 7 ops per microbatch).
  (with-redefs [idx/commit-ops-per-microbatch 7]
    (let [g (unique-iri "ordered-g")
          p (unique-iri "ordered-p")
          quads (vec (for [i (range 10)] [(str "<http://ex/ordered-s" i ">") p "\"v\"" g]))
          ops (fn [op tx] (mapv (fn [q] [op q tx]) quads))]
      ;; 15 rounds of add-all then delete-all, then a final add of the even ones
      (dotimes [r 15]
        (rama/foreign-append! (depot) [:commit (ops :add (+ 1000 (* 2 r)))])
        (rama/foreign-append! (depot) [:commit (ops :del (+ 1001 (* 2 r)))]))
      (rama/foreign-append! (depot) [:commit (vec (take-nth 2 (ops :add 5000)))])
      (is (wait-until 120000 #(and (idle?) (= (set (take-nth 2 quads)) (graph-quads g))))
          "final state is that of the last commit")
      (is (= (set (take-nth 2 quads)) (graph-quads g)))
      (is (= 5 (rama/foreign-select-one (keypath p :count) (pstate "$$predicate-stats")))
          "statistics match: every add and delete was applied exactly once, in order"))))

(deftest test-malformed-commit-is-dropped-whole
  (let [g (unique-iri "bad-commit-g")
        good [(unique-iri "bad-commit-s") (unique-iri "bad-commit-p") "\"ok\"" g]]
    (rama/foreign-append! (depot) [:commit [[:add good 1000] [:add ["<http://ex/s>" "<http://ex/p>" 5 g] 1000]]])
    (rama/foreign-append! (depot) [:add (assoc good 2 "\"marker\"")])
    (is (wait-until 30000 #(= #{(assoc good 2 "\"marker\"")} (graph-quads g)))
        "no op of a commit with a malformed op is applied; later records are")))

(deftest test-config-record-changes-the-commit-budget
  ;; Budget 3 ops per microbatch, set at runtime: a 30-op commit then needs
  ;; ten microbatches. Afterwards the default is restored for the other tests.
  (let [g (unique-iri "config-g")
        p (unique-iri "config-p")
        quads (vec (for [i (range 30)] [(str "<http://ex/config-s" i ">") p "\"v\"" g]))
        option (fn [] (rama/foreign-select-one (keypath :commit-ops-per-microbatch)
                                               (pstate "$$indexer-config") {:pkey 0}))]
    (try
      (rama-sail.core/set-indexer-option! (depot) :commit-ops-per-microbatch 3)
      (is (wait-until 30000 #(= 3 (option))) "the setting reaches the indexer")
      (rama/foreign-append! (depot) [:commit (mapv (fn [q] [:add q 1000]) quads)])
      (let [seen (loop [seen #{}]
                   (let [n (count (graph-quads g))]
                     (if (or (= 30 n) (> (count seen) 200)) (conj seen n) (do (Thread/sleep 100) (recur (conj seen n))))))]
        (is (contains? seen 30))
        (is (some #(< 0 % 30) seen) "the commit became visible in slices, so the budget applied"))
      (finally
        (rama-sail.core/set-indexer-option! (depot) :commit-ops-per-microbatch idx/commit-ops-per-microbatch)
        (wait-until 30000 #(= idx/commit-ops-per-microbatch (option)))))))

