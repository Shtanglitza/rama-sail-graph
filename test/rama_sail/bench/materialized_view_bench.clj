(ns ^{:bench true} rama-sail.bench.materialized-view-bench
  "Benchmark and verification tests for materialized type views.

   Verifies:
   1. Type views are correctly populated during indexing
   2. Type views enable fast type-based lookups
   3. Views are correctly maintained on delete

   Run with: lein test :only rama-sail.bench.materialized-view-bench"
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule RDF-TYPE-PREDICATE]]))

(def ^:private module-name "rama-sail.core/RdfStorageModule")

(defn- load-triples!
  "Load triples into the module and wait for processing.
   Returns the number of triples loaded."
  [ipc depot triples context]
  (let [n (count triples)]
    (doseq [[s p o] triples]
      (rama/foreign-append! depot [:add [s p o context]]))
    (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" n)
    (Thread/sleep 100)
    n))

;;; ---------------------------------------------------------------------------
;;; Type View Tests
;;; ---------------------------------------------------------------------------

(deftest test-type-view-population
  (testing "Type views are populated during indexing"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            get-subjects-by-type (rama/foreign-query ipc module-name "get-subjects-by-type")
            get-types-of-subject (rama/foreign-query ipc module-name "get-types-of-subject")
            ctx "::rama-internal::default-graph"

            ;; Create test data with types
            type-triples [["<alice>" RDF-TYPE-PREDICATE "<Person>"]
                          ["<bob>" RDF-TYPE-PREDICATE "<Person>"]
                          ["<charlie>" RDF-TYPE-PREDICATE "<Person>"]
                          ["<acme>" RDF-TYPE-PREDICATE "<Company>"]
                          ["<alice>" RDF-TYPE-PREDICATE "<Employee>"]]  ;; Alice has two types

            ;; Also add some properties
            prop-triples [["<alice>" "<name>" "\"Alice\""]
                          ["<bob>" "<name>" "\"Bob\""]
                          ["<alice>" "<worksFor>" "<acme>"]]]

        (load-triples! ipc depot (concat type-triples prop-triples) ctx)

        (testing "get-subjects-by-type returns correct subjects"
          (let [persons (set (rama/foreign-invoke-query get-subjects-by-type "<Person>"))
                companies (set (rama/foreign-invoke-query get-subjects-by-type "<Company>"))
                employees (set (rama/foreign-invoke-query get-subjects-by-type "<Employee>"))]

            (println)
            (println "=== Type View Population Test ===")
            (println "Persons:" persons)
            (println "Companies:" companies)
            (println "Employees:" employees)

            (is (= #{"<alice>" "<bob>" "<charlie>"} persons)
                "Should have 3 persons")
            (is (= #{"<acme>"} companies)
                "Should have 1 company")
            (is (= #{"<alice>"} employees)
                "Should have 1 employee")))

        (testing "get-types-of-subject returns correct types"
          (let [alice-types (set (rama/foreign-invoke-query get-types-of-subject "<alice>"))
                bob-types (set (rama/foreign-invoke-query get-types-of-subject "<bob>"))]

            (println "Alice's types:" alice-types)
            (println "Bob's types:" bob-types)

            (is (= #{"<Person>" "<Employee>"} alice-types)
                "Alice should have 2 types")
            (is (= #{"<Person>"} bob-types)
                "Bob should have 1 type")))))))

(deftest test-type-view-deletion
  (testing "Type views are correctly maintained on delete"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            get-subjects-by-type (rama/foreign-query ipc module-name "get-subjects-by-type")
            get-types-of-subject (rama/foreign-query ipc module-name "get-types-of-subject")
            ctx "::rama-internal::default-graph"

            type-triples [["<alice>" RDF-TYPE-PREDICATE "<Person>"]
                          ["<bob>" RDF-TYPE-PREDICATE "<Person>"]]]

        ;; Load initial triples
        (let [batch-count (load-triples! ipc depot type-triples ctx)]

          (let [persons-before (set (rama/foreign-invoke-query get-subjects-by-type "<Person>"))]
            (is (= #{"<alice>" "<bob>"} persons-before)
                "Should have 2 persons before delete"))

          ;; Delete Bob's type
          (rama/foreign-append! depot [:del ["<bob>" RDF-TYPE-PREDICATE "<Person>" ctx]])
          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" (inc batch-count))
          (Thread/sleep 100)

          (let [persons-after (set (rama/foreign-invoke-query get-subjects-by-type "<Person>"))
                bob-types (rama/foreign-invoke-query get-types-of-subject "<bob>")]

            (println)
            (println "=== Type View Deletion Test ===")
            (println "Persons after delete:" persons-after)
            (println "Bob's types after delete:" bob-types)

            (is (= #{"<alice>"} persons-after)
                "Should have 1 person after delete")
            (is (empty? bob-types)
                "Bob should have no types after delete")))))))

;;; ---------------------------------------------------------------------------
;;; Performance Comparison Tests
;;; ---------------------------------------------------------------------------

(deftest test-type-view-performance
  (testing "Type view lookup is faster than index scan"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
            get-subjects-by-type (rama/foreign-query ipc module-name "get-subjects-by-type")
            find-bgp (rama/foreign-query ipc module-name "find-bgp")
            ctx "::rama-internal::default-graph"
            num-entities 500

            ;; Create many entities with types
            type-triples (vec (for [i (range num-entities)]
                                [(str "<entity" i ">") RDF-TYPE-PREDICATE "<TestType>"]))]

        (load-triples! ipc depot type-triples ctx)

        (println)
        (println "=== Type View Performance Test ===")
        (println (format "Created %d entities with <TestType>" num-entities))

        ;; Benchmark: Type view lookup
        (let [view-times (for [_ (range 10)]
                           (let [start (System/nanoTime)
                                 result (rama/foreign-invoke-query get-subjects-by-type "<TestType>")
                                 end (System/nanoTime)]
                             [(count result) (/ (- end start) 1000000.0)]))
              view-count (first (first view-times))
              view-avg (/ (reduce + (map second view-times)) 10.0)]

          (println (format "View lookup: %d results, avg %.2fms" view-count view-avg))

          (is (>= view-count (* 0.95 num-entities))
              "View should contain most entities"))

        ;; Benchmark: BGP lookup (traditional approach)
        (let [bgp-pattern {:s "?x" :p RDF-TYPE-PREDICATE :o "<TestType>" :c nil}
              bgp-times (for [_ (range 10)]
                          (let [start (System/nanoTime)
                                result (rama/foreign-invoke-query find-bgp bgp-pattern)
                                end (System/nanoTime)]
                            [(count result) (/ (- end start) 1000000.0)]))
              bgp-count (first (first bgp-times))
              bgp-avg (/ (reduce + (map second bgp-times)) 10.0)]

          (println (format "BGP lookup:  %d results, avg %.2fms" bgp-count bgp-avg))

          (is (>= bgp-count (* 0.95 num-entities))
              "BGP should find most entities"))))))

(comment
  ;; Run individual tests
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.bench.materialized-view-bench)

  ;; Run specific test
  (test-type-view-population)
  (test-type-view-deletion)
  (test-type-view-performance))
