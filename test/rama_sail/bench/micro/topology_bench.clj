(ns ^:perf rama-sail.bench.micro.topology-bench
  "Query topology performance benchmarks.
   Tests joins, filters, ORDER BY, aggregations, and scale.

   NOTE: These benchmarks run on InProcessCluster (IPC), not a real Rama cluster.
   IPC simulates distributed behavior in a single JVM, which means:
   - All distributed overhead without true parallelism benefits
   - Concentrated GC pressure causing latency spikes
   - Thread contention in a single process

   Real cluster performance should be significantly better.
   See rama-sail.bench.infra.bench-helpers for detailed explanation."
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [rama-sail.bench.infra.data-gen :as gen]))

;;; ---------------------------------------------------------------------------
;;; Constants
;;; ---------------------------------------------------------------------------

(def ^:const TOPO-DATA-SIZE 10000)
(def ^:const TOPO-ITERATIONS 50)
(def ^:const WARMUP-ITERATIONS 5)

;;; ---------------------------------------------------------------------------
;;; Join Benchmarks
;;; ---------------------------------------------------------------------------

(deftest test-two-way-join
  (testing "2-way join performance (BGP + BGP on shared variable)"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "2-Way Join Benchmark")

      ;; Load join-friendly data
      (bh/print-subsection "Setup")
      (let [quads (gen/generate-join-friendly (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: ?person knows ?other, ?person worksAt ?company
        (let [plan {:op :join
                    :left {:op :bgp
                           :pattern {:s "?person"
                                     :p (gen/predicate-uri "knows")
                                     :o "?other"}}
                    :right {:op :bgp
                            :pattern {:s "?person"
                                      :p (gen/predicate-uri "worksAt")
                                      :o "?company"}}
                    :join-vars ["?person"]}]

          ;; Warmup
          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          ;; Benchmark
          (bh/print-subsection "2-Way Join (?person knows ?other, ?person worksAt ?company)")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "2-way join" (bh/check-latency-target stats :two-way-join :p50))
            (bh/report-target-check "2-way join" (bh/check-latency-target stats :two-way-join :p99))

            ;; Report result count
            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        (is (> (count quads) 0))))))

(deftest test-three-way-join
  (testing "3-way join performance (chain of 3 BGPs)"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "3-Way Join Benchmark")

      ;; Load join-friendly data
      (bh/print-subsection "Setup")
      (let [quads (gen/generate-join-friendly (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: ?person worksAt ?company, ?company locatedIn ?city
        (let [plan {:op :join
                    :left {:op :join
                           :left {:op :bgp
                                  :pattern {:s "?person"
                                            :p (gen/predicate-uri "knows")
                                            :o "?other"}}
                           :right {:op :bgp
                                   :pattern {:s "?person"
                                             :p (gen/predicate-uri "worksAt")
                                             :o "?company"}}
                           :join-vars ["?person"]}
                    :right {:op :bgp
                            :pattern {:s "?company"
                                      :p (gen/predicate-uri "locatedIn")
                                      :o "?city"}}
                    :join-vars ["?company"]}]

          ;; Warmup
          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          ;; Benchmark
          (bh/print-subsection "3-Way Join (?person knows ?other, worksAt ?company, ?company locatedIn ?city)")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "3-way join" (bh/check-latency-target stats :complex-query :p50))
            (bh/report-target-check "3-way join" (bh/check-latency-target stats :complex-query :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        (is (> (count quads) 0))))))

(deftest test-left-join
  (testing "LEFT JOIN (OPTIONAL) performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "LEFT JOIN (OPTIONAL) Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: ?s rdf:type ?type OPTIONAL { ?s name ?name }
        (let [plan {:op :left-join
                    :left {:op :bgp
                           :pattern {:s "?s"
                                     :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                     :o "?type"}}
                    :right {:op :bgp
                            :pattern {:s "?s"
                                      :p (gen/predicate-uri "name")
                                      :o "?name"}}
                    :join-vars ["?s"]}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "LEFT JOIN (?s type ?type OPTIONAL ?s name ?name)")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Left join" (bh/check-latency-target stats :two-way-join :p50))
            (bh/report-target-check "Left join" (bh/check-latency-target stats :two-way-join :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        (is (> (count quads) 0))))))

(deftest test-union
  (testing "UNION performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "UNION Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: { ?s type Person } UNION { ?s type Company }
        (let [plan {:op :union
                    :left {:op :bgp
                           :pattern {:s "?s"
                                     :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                     :o (gen/type-uri "Person")}}
                    :right {:op :bgp
                            :pattern {:s "?s"
                                      :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                      :o (gen/type-uri "Company")}}}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "UNION ({?s type Person} UNION {?s type Company})")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Union" (bh/check-latency-target stats :two-way-join :p50))
            (bh/report-target-check "Union" (bh/check-latency-target stats :two-way-join :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Filter Benchmarks
;;; ---------------------------------------------------------------------------

(deftest test-filter-numeric
  (testing "Numeric filter performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "Filter (Numeric) Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: ?s age ?age FILTER(?age > 50)
        (let [plan {:op :filter
                    :sub-plan {:op :bgp
                               :pattern {:s "?s"
                                         :p (gen/predicate-uri "age")
                                         :o "?age"}}
                    :expr {:type :cmp
                           :op :gt
                           :left {:type :var :name "?age"}
                           :right {:type :const :val "50"}}}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "FILTER (?s age ?age FILTER ?age > 50)")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Filter numeric" (bh/check-latency-target stats :two-way-join :p50))
            (bh/report-target-check "Filter numeric" (bh/check-latency-target stats :two-way-join :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings (filtered)" (count results))))))

        (is (> (count quads) 0))))))

(deftest test-filter-with-join
  (testing "Filter with join performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "Filter + Join Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: ?s type ?type, ?s age ?age FILTER(?age > 30 AND ?age < 70)
        (let [plan {:op :filter
                    :sub-plan {:op :join
                               :left {:op :bgp
                                      :pattern {:s "?s"
                                                :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                                :o "?type"}}
                               :right {:op :bgp
                                       :pattern {:s "?s"
                                                 :p (gen/predicate-uri "age")
                                                 :o "?age"}}
                               :join-vars ["?s"]}
                    :expr {:type :logic
                           :op :and
                           :left {:type :cmp
                                  :op :gt
                                  :left {:type :var :name "?age"}
                                  :right {:type :const :val "30"}}
                           :right {:type :cmp
                                   :op :lt
                                   :left {:type :var :name "?age"}
                                   :right {:type :const :val "70"}}}}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "JOIN + FILTER (type + age, FILTER 30 < age < 70)")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "Filter+Join" (bh/check-latency-target stats :complex-query :p50))
            (bh/report-target-check "Filter+Join" (bh/check-latency-target stats :complex-query :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; ORDER BY Benchmarks
;;; ---------------------------------------------------------------------------

(deftest test-order-by
  (testing "ORDER BY performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "ORDER BY Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query: SELECT ?s ?age WHERE { ?s age ?age } ORDER BY ?age
        (let [plan {:op :order
                    :sub-plan {:op :bgp
                               :pattern {:s "?s"
                                         :p (gen/predicate-uri "age")
                                         :o "?age"}}
                    :order-specs [{:expr {:type :var :name "?age"} :ascending true}]}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "ORDER BY ASC (?s age ?age ORDER BY ?age)")
          (let [stats (bh/bench 20 ;; Fewer iterations - ORDER BY is expensive
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)
            (bh/report-target-check "ORDER BY" (bh/check-latency-target stats :order-by :p50))
            (bh/report-target-check "ORDER BY" (bh/check-latency-target stats :order-by :p99))

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings" (count results))))))

        ;; DESC order
        (let [plan {:op :order
                    :sub-plan {:op :bgp
                               :pattern {:s "?s"
                                         :p (gen/predicate-uri "age")
                                         :o "?age"}}
                    :order-specs [{:expr {:type :var :name "?age"} :ascending false}]}]

          (bh/print-subsection "ORDER BY DESC")
          (let [stats (bh/bench 20
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)))

        (is (> (count quads) 0))))))

(deftest test-order-by-with-limit
  (testing "ORDER BY with LIMIT performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "ORDER BY + LIMIT Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Query with LIMIT
        (let [plan {:op :slice
                    :sub-plan {:op :order
                               :sub-plan {:op :bgp
                                          :pattern {:s "?s"
                                                    :p (gen/predicate-uri "age")
                                                    :o "?age"}}
                               :order-specs [{:expr {:type :var :name "?age"} :ascending true}]}
                    :offset 0
                    :limit 100}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "ORDER BY + LIMIT 100")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Result count: %d bindings (limited)" (count results)))
              (is (<= (count results) 100)))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Projection and DISTINCT
;;; ---------------------------------------------------------------------------

(deftest test-projection
  (testing "Projection performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "Projection Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; Project to single variable
        (let [plan {:op :project
                    :sub-plan {:op :bgp
                               :pattern {:s "?s"
                                         :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                         :o "?type"}}
                    :vars ["?type"]}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "PROJECT to ?type only")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)))

        (is (> (count quads) 0))))))

(deftest test-distinct
  (testing "DISTINCT performance"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "DISTINCT Benchmark")

      (bh/print-subsection "Setup")
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Loaded %d quads" (count quads)))

        ;; DISTINCT types
        (let [plan {:op :distinct
                    :sub-plan {:op :project
                               :sub-plan {:op :bgp
                                          :pattern {:s "?s"
                                                    :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                                                    :o "?type"}}
                               :vars ["?type"]}}]

          (dotimes [_ WARMUP-ITERATIONS]
            (rama/foreign-invoke-query q-plan plan))

          (bh/print-subsection "SELECT DISTINCT ?type")
          (let [stats (bh/bench TOPO-ITERATIONS
                                (rama/foreign-invoke-query q-plan plan))]
            (bh/print-result "Latency" stats)

            (let [results (rama/foreign-invoke-query q-plan plan)]
              (println (format "  Distinct values: %d" (count results))))))

        (is (> (count quads) 0))))))

;;; ---------------------------------------------------------------------------
;;; Scale Testing
;;; ---------------------------------------------------------------------------

(deftest ^:scale test-scale-1m
  (testing "Scale test at 1M triples"
    (bh/with-bench-module [ipc module-name depot q-plan :tasks 4 :threads 2]
      (bh/print-section "Scale Test: 1M Triples")

      (bh/print-subsection "Loading data (this may take a while)")
      (let [mem-before (bh/gc!)]
        (println (format "  Memory before: %s" (bh/format-memory mem-before))))

      ;; Load 1M triples in batches
      (let [batch-size 50000
            num-batches 20 ;; 50K * 20 = 1M
            total-loaded (atom 0)]

        (doseq [batch-num (range num-batches)]
          (let [quads (gen/generate-batch-for-scale batch-size (* batch-num batch-size))]
            (doseq [[s p o c] quads]
              (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]]))
            (swap! total-loaded + batch-size)
            (when (zero? (mod (inc batch-num) 5))
              (println (format "  Loaded %dK quads..." (/ @total-loaded 1000))))))

        (bh/wait-for-indexer! ipc module-name @total-loaded)
        (println (format "  Total loaded: %d quads" @total-loaded))

        (let [mem-after (bh/gc!)]
          (println (format "  Memory after: %s" (bh/format-memory mem-after))))

        ;; Test point lookup at scale
        (bh/print-subsection "Point Lookup at 1M scale")
        (let [q-triples (rama/foreign-query ipc module-name "find-triples")
              sample-subjects (mapv #(gen/entity-uri %) (take 100 (shuffle (range @total-loaded))))]

          ;; Warmup
          (doseq [s (take 10 sample-subjects)]
            (rama/foreign-invoke-query q-triples s nil nil nil))

          (let [stats (bh/bench 100
                                (let [s (rand-nth sample-subjects)]
                                  (rama/foreign-invoke-query q-triples s nil nil nil)))]
            (bh/print-result "Subject lookup" stats)
            (bh/report-target-check "1M point lookup" (bh/check-latency-target stats :point-lookup :p50))
            (bh/report-target-check "1M point lookup" (bh/check-latency-target stats :point-lookup :p99))))

        ;; Test join at scale
        (bh/print-subsection "Join at 1M scale")
        (let [plan {:op :slice
                    :sub-plan {:op :bgp
                               :pattern {:s "?s"
                                         :p (gen/predicate-uri "prop0")
                                         :o "?o"}}
                    :offset 0
                    :limit 1000}
              stats (bh/bench 20
                              (rama/foreign-invoke-query q-plan plan))]
          (bh/print-result "BGP + LIMIT" stats)))

      (is true))))

(deftest ^:scale test-scale-10m
  (testing "Scale test at 10M triples"
    (bh/with-bench-module [ipc module-name depot q-plan :tasks 8 :threads 2]
      (bh/print-section "Scale Test: 10M Triples")

      (bh/print-subsection "Loading data (this will take a while)")
      (let [mem-before (bh/gc!)]
        (println (format "  Memory before: %s" (bh/format-memory mem-before))))

      ;; Load 10M triples in batches
      (let [batch-size 100000
            num-batches 100 ;; 100K * 100 = 10M
            total-loaded (atom 0)]

        (doseq [batch-num (range num-batches)]
          (let [quads (gen/generate-batch-for-scale batch-size (* batch-num batch-size))]
            (doseq [[s p o c] quads]
              (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]]))
            (swap! total-loaded + batch-size)
            (when (zero? (mod (inc batch-num) 10))
              (println (format "  Loaded %dM quads..." (/ @total-loaded 1000000))))))

        (bh/wait-for-indexer! ipc module-name @total-loaded)
        (println (format "  Total loaded: %d quads" @total-loaded))

        (let [mem-after (bh/gc!)]
          (println (format "  Memory after: %s" (bh/format-memory mem-after))))

        ;; Test point lookup at 10M scale
        (bh/print-subsection "Point Lookup at 10M scale")
        (let [q-triples (rama/foreign-query ipc module-name "find-triples")
              sample-subjects (mapv #(gen/entity-uri %) (take 100 (shuffle (range @total-loaded))))]

          ;; Warmup
          (doseq [s (take 10 sample-subjects)]
            (rama/foreign-invoke-query q-triples s nil nil nil))

          (let [stats (bh/bench 100
                                (let [s (rand-nth sample-subjects)]
                                  (rama/foreign-invoke-query q-triples s nil nil nil)))]
            (bh/print-result "Subject lookup" stats)
            (bh/report-target-check "10M point lookup" (bh/check-latency-target stats :point-lookup :p50))
            (bh/report-target-check "10M point lookup" (bh/check-latency-target stats :point-lookup :p99))))

        ;; Test predicate scan at 10M scale
        (bh/print-subsection "Predicate Scan at 10M scale")
        (let [q-triples (rama/foreign-query ipc module-name "find-triples")
              stats (bh/bench 20
                              (rama/foreign-invoke-query q-triples nil (gen/predicate-uri "prop0") nil nil))]
          (bh/print-result "Predicate scan (prop0)" stats)))

      (is true))))

;;; ---------------------------------------------------------------------------
;;; Comprehensive Summary
;;; ---------------------------------------------------------------------------

(deftest test-topology-summary
  (testing "Query topology performance summary"
    (bh/with-bench-module [ipc module-name depot q-plan]
      (bh/print-section "Query Topology Performance Summary")

      ;; Load test data
      (let [quads (gen/generate-typed-entities (/ TOPO-DATA-SIZE 3))]
        (bh/load-and-wait! ipc module-name depot quads)
        (println (format "  Data: %d quads\n" (count quads)))

        (let [results (atom {})
              q-triples (rama/foreign-query ipc module-name "find-triples")]

          ;; 1. BGP
          (let [stats (bh/bench 50 (rama/foreign-invoke-query q-triples nil (gen/predicate-uri "age") nil nil))]
            (swap! results assoc :bgp stats))

          ;; 2. Join
          (let [plan {:op :join
                      :left {:op :bgp :pattern {:s "?s" :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" :o "?type"}}
                      :right {:op :bgp :pattern {:s "?s" :p (gen/predicate-uri "age") :o "?age"}}
                      :join-vars ["?s"]}
                stats (bh/bench 50 (rama/foreign-invoke-query q-plan plan))]
            (swap! results assoc :join stats))

          ;; 3. Filter
          (let [plan {:op :filter
                      :sub-plan {:op :bgp :pattern {:s "?s" :p (gen/predicate-uri "age") :o "?age"}}
                      :expr {:type :cmp :op :gt :left {:type :var :name "?age"} :right {:type :const :val "50"}}}
                stats (bh/bench 50 (rama/foreign-invoke-query q-plan plan))]
            (swap! results assoc :filter stats))

          ;; 4. ORDER BY
          (let [plan {:op :order
                      :sub-plan {:op :bgp :pattern {:s "?s" :p (gen/predicate-uri "age") :o "?age"}}
                      :order-specs [{:expr {:type :var :name "?age"} :ascending true}]}
                stats (bh/bench 20 (rama/foreign-invoke-query q-plan plan))]
            (swap! results assoc :order-by stats))

          ;; 5. DISTINCT
          (let [plan {:op :distinct
                      :sub-plan {:op :project
                                 :sub-plan {:op :bgp :pattern {:s "?s" :p "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" :o "?type"}}
                                 :vars ["?type"]}}
                stats (bh/bench 50 (rama/foreign-invoke-query q-plan plan))]
            (swap! results assoc :distinct stats))

          ;; Print summary table
          (println "  Operation  | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms)")
          (println "  -----------|----------|----------|----------|----------")
          (doseq [[op stats] (sort-by first @results)]
            (println (format "  %-10s | %8.2f | %8.2f | %8.2f | %8.2f"
                             (name op)
                             (:p50 stats)
                             (:p95 stats)
                             (:p99 stats)
                             (:mean stats))))))

      (is true))))

(comment
  ;; Run individual tests
  (test-two-way-join)
  (test-three-way-join)
  (test-filter-numeric)
  (test-order-by)
  (test-topology-summary)

  ;; Run all topology benchmarks
  ;; lein test :only rama-sail.bench.micro.topology-bench

  ;; Run scale tests (require :scale profile)
  ;; lein with-profile +scale test :only rama-sail.bench.micro.topology-bench/test-scale-1m
  ;; lein with-profile +scale test :only rama-sail.bench.micro.topology-bench/test-scale-10m
  )
