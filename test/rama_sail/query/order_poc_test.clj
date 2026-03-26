(ns ^:perf rama-sail.query.order-poc-test
  "Proof of concept for ORDER BY implementation using Rama dataflow.
   Tests different sorting approaches and measures performance."
  (:use [com.rpl.rama]
        [com.rpl.rama.path]
        [clojure.test])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]))

;; --- PoC Module for testing ORDER BY patterns ---

(def ^:const MAX-RESULTS 100000)
(def ^:const TEST-DATA-SIZE 10000)

;; Generate test data: rows with name and age
(defn generate-test-data [n]
  (vec (for [i (range n)]
         {"?name" (str "person-" i)
          "?age" (str (+ 18 (mod (* i 7) 80)))}))) ;; ages 18-97, pseudo-random distribution

(def test-data (generate-test-data TEST-DATA-SIZE))

;; Composite sort key for multiple keys - returns a comparable vector
(defn compute-composite-key [row order-specs]
  (mapv (fn [{:keys [var ascending]}]
          (let [raw (get row var)
                parsed (try (Double/parseDouble raw) (catch Exception _ nil))]
            (cond
              (nil? raw) (if ascending [0 nil] [2 nil])
              parsed [1 (if ascending parsed (- parsed))]
              :else [1 raw])))
        order-specs))

;; Sort keyed rows and extract
(defn sort-and-extract [keyed-rows]
  (->> keyed-rows
       (sort-by first)
       (mapv second)))

;; Sort rows by order-specs (for approach 3)
(defn sort-rows-by-specs [rows order-specs]
  (->> rows
       (sort-by #(compute-composite-key % order-specs))
       vec))

;; Depot partition key extractor
(defn get-row-name [row]
  (get row "?name"))

;; Extract age as sort value for +top-monotonic
(defn get-age-as-double [row]
  (try (Double/parseDouble (get row "?age")) (catch Exception _ 0.0)))

;; Identity function for id extraction (each row is unique)
(defn row-identity [row]
  (get row "?name"))

;; -----------------------------------------------------------------------------
;; Approach 4: Custom Sorting Combiner
;; -----------------------------------------------------------------------------
;; A combiner that collects [sort-key, row] tuples and keeps them sorted.
;; Two-phase aggregation: each task accumulates sorted partial results,
;; then origin merges all partial results.

(defn merge-sorted-keyed
  "Merge two sorted lists of [key, row] tuples into one sorted list.
   Uses a simple merge algorithm for O(n+m) efficiency."
  [list1 list2]
  (loop [l1 list1
         l2 list2
         result []]
    (cond
      (empty? l1) (into result l2)
      (empty? l2) (into result l1)
      :else
      (let [[k1 _] (first l1)
            [k2 _] (first l2)]
        (if (neg? (compare k1 k2))
          (recur (rest l1) l2 (conj result (first l1)))
          (recur l1 (rest l2) (conj result (first l2))))))))

;; Combiner for sorted keyed rows - merge two sorted lists
(def +sorted-keyed-combiner
  (combiner
   (fn [list1 list2]
     (merge-sorted-keyed (or list1 []) (or list2 [])))
   :init-fn (fn [] [])))

;; Simple concat combiner - just concatenate, sort at the end
(def +concat-combiner
  (combiner
   (fn [list1 list2]
     (into (or list1 []) list2))
   :init-fn (fn [] [])))

;; Hybrid combiner: concat locally, sort when merging larger lists
;; Theory: sort only when both inputs have significant size (cross-task merge)
(def +hybrid-sort-combiner
  (combiner
   (fn [list1 list2]
     (let [l1 (or list1 [])
           l2 (or list2 [])
           combined (into l1 l2)]
       ;; Sort when both lists have >1 element (likely cross-task merge)
       (if (and (> (count l1) 1) (> (count l2) 1))
         (vec (sort-by first combined))
         combined)))
   :init-fn (fn [] [])))

;; Extract rows from sorted keyed list
(defn extract-rows [keyed-rows]
  (mapv second keyed-rows))

;; Sort keyed rows (for concat combiner approach)
(defn sort-keyed-rows [keyed-rows]
  (vec (sort-by first keyed-rows)))

(defmodule OrderPocModule [setup topologies]
  ;; Simple depot for test data - use hash by name to ensure unique keys
  (declare-depot setup *data-depot (hash-by get-row-name))

  ;; PState to store test data - key by name to avoid collisions
  (let [mb (microbatch-topology topologies "indexer")]
    (declare-pstate mb $$rows {String (fixed-keys-schema {"?name" String "?age" String})})

    (<<sources mb
               (source> *data-depot :> %microbatch)
               (%microbatch :> *row)
               (get *row "?name" :> *name)
               (|hash *name)
               (local-transform> [(keypath *name) (termval *row)] $$rows)))

  ;; --- Approach 1: +vec-agg then sort with Clojure (WORKING) ---
  ;; Collects all [sort-key, row] tuples, then sorts in Clojure
  (<<query-topology topologies "order-v1" [*order-specs :> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (compute-composite-key *row *order-specs :> *sort-key)
                    (vector *sort-key *row :> *keyed)
                    (|origin)
                    (aggs/+vec-agg *keyed :> *keyed-vec)
                    (sort-and-extract *keyed-vec :> *results))

  ;; --- Approach 2: +top-monotonic with ASCENDING (hardcoded) ---
  ;; LIMITATION: :sort-type must be a literal keyword, cannot be parameterized!
  (<<query-topology topologies "order-v2-asc" [:> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (|origin)
                    (aggs/+top-monotonic [MAX-RESULTS] *row
                                         :+options {:id-fn row-identity
                                                    :sort-val-fn get-age-as-double
                                                    :sort-type :ascending}
                                         :> *results))

  ;; --- Approach 2b: +top-monotonic with DESCENDING (hardcoded) ---
  (<<query-topology topologies "order-v2-desc" [:> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (|origin)
                    (aggs/+top-monotonic [MAX-RESULTS] *row
                                         :+options {:id-fn row-identity
                                                    :sort-val-fn get-age-as-double
                                                    :sort-type :descending}
                                         :> *results))

  ;; --- Approach 3: +vec-agg then Clojure sort-by (WORKING) ---
  ;; Simpler: just collect all rows then sort
  (<<query-topology topologies "order-v3" [*order-specs :> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (|origin)
                    (aggs/+vec-agg *row :> *all-rows)
                    (sort-rows-by-specs *all-rows *order-specs :> *results))

  ;; --- Approach 4a: Custom merge-sort combiner ---
  ;; Each partition computes [key, row] tuples, combiner merges sorted lists
  (<<query-topology topologies "order-v4-merge" [*order-specs :> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (compute-composite-key *row *order-specs :> *sort-key)
                    ;; Wrap each [key, row] as a single-element sorted list
                    (vector (vector *sort-key *row) :> *single-keyed)
                    (|origin)
                    ;; Combiner merges sorted lists
                    (+sorted-keyed-combiner *single-keyed :> *merged-keyed)
                    ;; Extract just the rows
                    (extract-rows *merged-keyed :> *results))

  ;; --- Approach 4b: Concat combiner + final sort ---
  ;; Concat all tuples, sort once at the end (may be faster for small datasets)
  (<<query-topology topologies "order-v4-concat" [*order-specs :> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (compute-composite-key *row *order-specs :> *sort-key)
                    ;; Wrap as single-element vector
                    (vector (vector *sort-key *row) :> *single-keyed)
                    (|origin)
                    ;; Just concat all vectors
                    (+concat-combiner *single-keyed :> *all-keyed)
                    ;; Sort once at the end
                    (sort-keyed-rows *all-keyed :> *sorted-keyed)
                    (extract-rows *sorted-keyed :> *results))

  ;; --- Approach 4c: Hybrid combiner - sort during cross-task merge ---
  ;; Attempts to sort only when merging partial results from different tasks
  (<<query-topology topologies "order-v4-hybrid" [*order-specs :> *results]
                    (|all)
                    (local-select> [ALL LAST] $$rows :> *row)
                    (compute-composite-key *row *order-specs :> *sort-key)
                    (vector (vector *sort-key *row) :> *single-keyed)
                    (|origin)
                    (+hybrid-sort-combiner *single-keyed :> *sorted-keyed)
                    (extract-rows *sorted-keyed :> *results)))

;; --- Performance measurement helper ---
(defmacro time-ms [expr]
  `(let [start# (System/nanoTime)
         result# ~expr
         end# (System/nanoTime)]
     {:result result#
      :time-ms (/ (- end# start#) 1000000.0)}))

;; --- Tests ---

(deftest test-order-poc-performance
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc OrderPocModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name OrderPocModule)
          depot (foreign-depot ipc module-name "*data-depot")
          order-v1 (foreign-query ipc module-name "order-v1")
          order-v2-asc (foreign-query ipc module-name "order-v2-asc")
          order-v2-desc (foreign-query ipc module-name "order-v2-desc")
          order-v3 (foreign-query ipc module-name "order-v3")
          order-v4-merge (foreign-query ipc module-name "order-v4-merge")
          order-v4-concat (foreign-query ipc module-name "order-v4-concat")
          order-v4-hybrid (foreign-query ipc module-name "order-v4-hybrid")
          order-specs-asc [{:var "?age" :ascending true}]
          order-specs-desc [{:var "?age" :ascending false}]]

      ;; Add test data
      (println (str "\n=== Loading " TEST-DATA-SIZE " rows ==="))
      (let [{:keys [time-ms]} (time-ms
                               (doseq [row test-data]
                                 (foreign-append! depot row)))]
        (println (format "Data loading: %.2f ms" time-ms)))

      ;; Wait for indexing
      (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" TEST-DATA-SIZE)
      (println "Indexing complete.\n")

      ;; Warmup run
      (println "=== Warmup run ===")
      (foreign-invoke-query order-v1 order-specs-asc)
      (foreign-invoke-query order-v3 order-specs-asc)
      (foreign-invoke-query order-v4-merge order-specs-asc)
      (foreign-invoke-query order-v4-concat order-specs-asc)
      (foreign-invoke-query order-v4-hybrid order-specs-asc)
      (println "Warmup complete.\n")

      ;; --- Test Approach 1: vec-agg + sort-and-extract ---
      (println "=== Approach 1: +vec-agg then sort-and-extract ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v1 order-specs-asc))
            first-age (get (first result) "?age")
            last-age (get (last result) "?age")]
        (println (format "  ASC: %d results in %.2f ms" (count result) time-ms))
        (println (format "  First age: %s, Last age: %s" first-age last-age))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age) "First should be youngest (18)"))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v1 order-specs-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC: %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age) "First should be oldest (97)"))

      ;; --- Test Approach 2: +top-monotonic (hardcoded) ---
      (println "\n=== Approach 2: +top-monotonic (hardcoded sort-type) ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v2-asc))
            first-age (get (first result) "?age")
            last-age (get (last result) "?age")]
        (println (format "  ASC (hardcoded): %d results in %.2f ms" (count result) time-ms))
        (println (format "  First age: %s, Last age: %s" first-age last-age))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age) "First should be youngest (18)"))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v2-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC (hardcoded): %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age) "First should be oldest (97)"))

      (println "\n  LIMITATION: :sort-type requires literal :ascending/:descending")
      (println "  Cannot parameterize at runtime!")

      ;; --- Test Approach 3: vec-agg + Clojure sort-by ---
      (println "\n=== Approach 3: +vec-agg then sort-rows-by-specs ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v3 order-specs-asc))
            first-age (get (first result) "?age")]
        (println (format "  ASC: %d results in %.2f ms" (count result) time-ms))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age)))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v3 order-specs-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC: %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age)))

      ;; --- Test Approach 4a: Custom merge-sort combiner ---
      (println "\n=== Approach 4a: Custom merge-sort combiner ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-merge order-specs-asc))
            first-age (get (first result) "?age")]
        (println (format "  ASC: %d results in %.2f ms" (count result) time-ms))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age)))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-merge order-specs-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC: %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age)))

      ;; --- Test Approach 4b: Concat combiner + final sort ---
      (println "\n=== Approach 4b: Concat combiner + final sort ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-concat order-specs-asc))
            first-age (get (first result) "?age")]
        (println (format "  ASC: %d results in %.2f ms" (count result) time-ms))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age)))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-concat order-specs-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC: %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age)))

      ;; --- Test Approach 4c: Hybrid combiner ---
      (println "\n=== Approach 4c: Hybrid combiner (sort on cross-task merge) ===")
      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-hybrid order-specs-asc))
            first-age (get (first result) "?age")]
        (println (format "  ASC: %d results in %.2f ms" (count result) time-ms))
        (is (= TEST-DATA-SIZE (count result)))
        (is (= "18" first-age)))

      (let [{:keys [result time-ms]} (time-ms (foreign-invoke-query order-v4-hybrid order-specs-desc))
            first-age (get (first result) "?age")]
        (println (format "  DESC: %d results in %.2f ms" (count result) time-ms))
        (is (= "97" first-age)))

      (println "\n=== Summary ===")
      (println "Approach 1 (vec-agg + sort-and-extract): parameterizable, keyed tuples")
      (println "Approach 2 (+top-monotonic): NOT parameterizable")
      (println "Approach 3 (vec-agg + sort-rows-by-specs): parameterizable, simple")
      (println "Approach 4a (merge-sort combiner): parameterizable, O(n²) - SLOW")
      (println "Approach 4b (concat combiner + sort): parameterizable, O(n log n)")
      (println "Approach 4c (hybrid combiner): parameterizable, sort on merge"))))

(comment
  ;; Run the test
  (test-order-poc-performance))
