(ns ^{:bench true} rama-sail.bench.colocated-join-bench
  "Benchmark comparing co-located subject join vs standard hash join.

   The co-located join exploits the fact that $$spoc is hash-partitioned by subject.
   When both sides of a join share a subject variable, we can join locally
   on each task without network shuffling.

   Run with: lein test :only rama-sail.bench.colocated-join-bench"
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]))

(def ^:private module-name "rama-sail.core/RdfStorageModule")

(defn- load-triples!
  "Load triples into the module and wait for processing."
  [ipc depot triples context]
  (let [n (count triples)]
    (doseq [[s p o] triples]
      (rama/foreign-append! depot [:add [s p o context]]))
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" n)
    (Thread/sleep 100)
    n))

(defn- time-query
  "Execute query and return [result-count time-ms]."
  [query-fn]
  (let [start (System/nanoTime)
        result (query-fn)
        end (System/nanoTime)]
    [(count result) (/ (- end start) 1000000.0)]))

(defn- benchmark-query
  "Run query multiple times and return statistics."
  [query-fn warmup-n bench-n]
  (dotimes [_ warmup-n] (query-fn))
  (let [results (vec (for [_ (range bench-n)]
                       (second (time-query query-fn))))
        sorted (sort results)]
    {:min (first sorted)
     :max (last sorted)
     :p50 (nth sorted (int (* 0.5 (count sorted))))
     :mean (/ (reduce + results) (double (count results)))
     :n bench-n}))

(defn- format-stats [stats]
  (format "n=%d | min=%.2fms | p50=%.2fms | mean=%.2fms"
          (:n stats) (:min stats) (:p50 stats) (:mean stats)))

;;; ---------------------------------------------------------------------------
;;; Co-located Join Tests
;;; ---------------------------------------------------------------------------

(deftest test-colocated-join-correctness
  (testing "Co-located subject join produces correct results"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            execute-plan (rama/foreign-query ipc module-name "execute-plan")
            ctx "::rama-internal::default-graph"

            ;; Test data: persons with names and ages
            ;; Alice, Bob have both; Charlie has only name; Dave has only age
            triples [["<alice>" "<name>" "\"Alice\""]
                     ["<alice>" "<age>" "\"30\""]
                     ["<bob>" "<name>" "\"Bob\""]
                     ["<bob>" "<age>" "\"25\""]
                     ["<charlie>" "<name>" "\"Charlie\""]
                     ["<dave>" "<age>" "\"40\""]]]

        (load-triples! ipc depot triples ctx)

        ;; Test: Join ?x <name> ?n with ?x <age> ?a (should get Alice and Bob)
        (let [left-pattern {:s "?x" :p "<name>" :o "?n" :c nil}
              right-pattern {:s "?x" :p "<age>" :o "?a" :c nil}
              colocated-plan {:op :colocated-subject-join
                              :left-pattern left-pattern
                              :right-pattern right-pattern
                              :subject-var "?x"}
              results (rama/foreign-invoke-query execute-plan colocated-plan)]

          (println)
          (println "=== Co-located Join Correctness Test ===")
          (println "Results:" (pr-str results))

          (is (= 2 (count results)) "Should have 2 results (Alice and Bob)")
          (let [subjects (set (map #(get % "?x") results))]
            (is (= #{"<alice>" "<bob>"} subjects)
                "Should find Alice and Bob")))))))

(deftest test-colocated-join-performance
  (testing "Co-located join is faster than standard join for subject-key joins"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 8 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            execute-plan (rama/foreign-query ipc module-name "execute-plan")
            ctx "::rama-internal::default-graph"
            num-entities 500

            ;; Generate test data: entities with two properties each
            triples (vec (concat
                          (for [i (range num-entities)]
                            [(str "<entity" i ">") "<propA>" (str "\"valueA" i "\"")])
                          (for [i (range num-entities)]
                            [(str "<entity" i ">") "<propB>" (str "\"valueB" i "\"")])))]

        (load-triples! ipc depot triples ctx)

        (println)
        (println "=== Co-located Join Performance Test ===")
        (println (format "Created %d entities with 2 properties each (%d triples)"
                         num-entities (* 2 num-entities)))

        ;; Standard join plan
        (let [standard-plan {:op :join
                             :left {:op :bgp :pattern {:s "?x" :p "<propA>" :o "?a" :c nil}}
                             :right {:op :bgp :pattern {:s "?x" :p "<propB>" :o "?b" :c nil}}
                             :join-vars ["?x"]}
              standard-fn #(rama/foreign-invoke-query execute-plan standard-plan)

              ;; Verify correctness first
              [std-count _] (time-query standard-fn)]

          (println (format "Standard join result count: %d" std-count))
          (is (>= std-count (* 0.95 num-entities))
              "Standard join should find most entities")

          ;; Benchmark standard join
          (let [std-stats (benchmark-query standard-fn 3 10)]
            (println (str "Standard join:   " (format-stats std-stats)))

            ;; Co-located join plan
            (let [colocated-plan {:op :colocated-subject-join
                                  :left-pattern {:s "?x" :p "<propA>" :o "?a" :c nil}
                                  :right-pattern {:s "?x" :p "<propB>" :o "?b" :c nil}
                                  :subject-var "?x"}
                  colocated-fn #(rama/foreign-invoke-query execute-plan colocated-plan)

                  ;; Verify correctness
                  [coloc-count _] (time-query colocated-fn)]

              (println (format "Co-located join result count: %d" coloc-count))
              (is (>= coloc-count (* 0.95 num-entities))
                  "Co-located join should find most entities")

              ;; Benchmark co-located join
              (let [coloc-stats (benchmark-query colocated-fn 3 10)
                    speedup (/ (:mean std-stats) (:mean coloc-stats))]
                (println (str "Co-located join: " (format-stats coloc-stats)))
                (println (format "Speedup: %.2fx" speedup))

                ;; Both should produce same result count
                (is (= std-count coloc-count)
                    "Both joins should produce same result count")))))))))

(deftest test-colocated-join-no-match
  (testing "Co-located join handles no-match case correctly"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            execute-plan (rama/foreign-query ipc module-name "execute-plan")
            ctx "::rama-internal::default-graph"

            ;; Data where left and right patterns don't share subjects
            triples [["<alice>" "<propA>" "\"a\""]
                     ["<bob>" "<propB>" "\"b\""]]]

        (load-triples! ipc depot triples ctx)

        (let [plan {:op :colocated-subject-join
                    :left-pattern {:s "?x" :p "<propA>" :o "?a" :c nil}
                    :right-pattern {:s "?x" :p "<propB>" :o "?b" :c nil}
                    :subject-var "?x"}
              results (rama/foreign-invoke-query execute-plan plan)]

          (println)
          (println "=== Co-located Join No-Match Test ===")
          (println "Results:" (pr-str results))

          (is (empty? results) "Should have no results when patterns don't share subjects"))))))

(comment
  ;; Run individual tests
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.bench.colocated-join-bench)

  ;; Run specific test
  (test-colocated-join-correctness)
  (test-colocated-join-performance))
