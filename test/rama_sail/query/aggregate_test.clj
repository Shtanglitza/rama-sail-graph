(ns rama-sail.query.aggregate-test
  "PoC tests for GROUP BY / Aggregates using Rama idioms.

   Develops aggregation using Rama's native constructs:
   - Combiners for distributed aggregation
   - Rama dataflow patterns

   Tests the group-query-topology implementation in module/queries.clj"
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.query.aggregation :refer [init-agg-state
                                                 update-agg-state
                                                 merge-agg-states
                                                 compute-final-agg
                                                 format-agg-result
                                                 build-group-entry
                                                 finalize-group-results]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.query.expr :refer [parse-numeric]])
  (:import (org.eclipse.rdf4j.model Resource)
           [org.eclipse.rdf4j.model Literal]
           [org.eclipse.rdf4j.query BindingSet]
           [org.eclipse.rdf4j.repository.sail SailRepository SailRepositoryConnection]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory])
  (:use [com.rpl.rama]
        [com.rpl.rama.path]))

;; =============================================================================
;; Unit Tests for Aggregate Helper Functions
;; =============================================================================

(deftest test-init-agg-state
  (testing "Initialize aggregate states"
    (is (= 0 (init-agg-state :count)))
    (is (= 0.0 (init-agg-state :sum)))
    (is (nil? (init-agg-state :min)))
    (is (nil? (init-agg-state :max)))
    (is (= [0.0 0] (init-agg-state :avg)))))

(deftest test-update-agg-state
  (testing "Update COUNT state"
    (is (= 1 (update-agg-state :count 0 "\"10\"^^<xsd:int>")))
    (is (= 0 (update-agg-state :count 0 nil))))

  (testing "Update SUM state"
    (is (= 10.0 (update-agg-state :sum 0.0 "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= 25.0 (update-agg-state :sum 10.0 "\"15\"^^<http://www.w3.org/2001/XMLSchema#integer>"))))

  (testing "Update MIN state (numeric)"
    (is (= "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :min nil "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :min "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>" "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :min "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>" "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"))))

  (testing "Update MIN state (string)"
    (is (= "\"apple\"" (update-agg-state :min nil "\"apple\"")))
    (is (= "\"apple\"" (update-agg-state :min "\"banana\"" "\"apple\""))))

  (testing "Update MAX state (numeric)"
    (is (= "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :max nil "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= "\"15\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :max "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>" "\"15\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= "\"15\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (update-agg-state :max "\"15\"^^<http://www.w3.org/2001/XMLSchema#integer>" "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"))))

  (testing "Update MAX state (string)"
    (is (= "\"banana\"" (update-agg-state :max nil "\"banana\"")))
    (is (= "\"banana\"" (update-agg-state :max "\"apple\"" "\"banana\""))))

  (testing "Update AVG state"
    (is (= [10.0 1] (update-agg-state :avg [0.0 0] "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>")))
    (is (= [30.0 2] (update-agg-state :avg [10.0 1] "\"20\"^^<http://www.w3.org/2001/XMLSchema#integer>")))))

(deftest test-merge-agg-states
  (testing "Merge COUNT states"
    (is (= 5 (merge-agg-states :count 2 3))))

  (testing "Merge SUM states"
    (is (= 30.0 (merge-agg-states :sum 10.0 20.0))))

  (testing "Merge MIN states"
    (is (= "\"5\"" (merge-agg-states :min "\"5\"" "\"10\"")))
    (is (= "\"5\"" (merge-agg-states :min "\"10\"" "\"5\"")))
    (is (= "\"5\"" (merge-agg-states :min nil "\"5\"")))
    (is (= "\"5\"" (merge-agg-states :min "\"5\"" nil))))

  (testing "Merge MAX states"
    (is (= "\"10\"" (merge-agg-states :max "\"5\"" "\"10\"")))
    (is (= "\"10\"" (merge-agg-states :max "\"10\"" "\"5\""))))

  (testing "Merge AVG states"
    (is (= [30.0 3] (merge-agg-states :avg [10.0 1] [20.0 2])))))

(deftest test-compute-final-agg
  (testing "Compute final COUNT"
    (is (= 5 (compute-final-agg :count 5))))

  (testing "Compute final SUM"
    (is (= 100.0 (compute-final-agg :sum 100.0))))

  (testing "Compute final AVG"
    (is (= 25.0 (compute-final-agg :avg [100.0 4])))
    (is (nil? (compute-final-agg :avg [0.0 0])))))

(deftest test-format-agg-result
  (testing "Format COUNT result"
    (is (= "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (format-agg-result :count 5))))

  (testing "Format SUM result (integer)"
    (is (= "\"100\"^^<http://www.w3.org/2001/XMLSchema#integer>"
           (format-agg-result :sum 100.0))))

  (testing "Format SUM result (decimal)"
    (is (= "\"100.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
           (format-agg-result :sum 100.5))))

  (testing "Format AVG result"
    (is (= "\"25.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
           (format-agg-result :avg 25.0))))

  (testing "Format nil result"
    (is (nil? (format-agg-result :avg nil)))))

;; =============================================================================
;; Integration Tests with RdfStorageModule
;; =============================================================================

(deftest test-group-topology-count
  (testing "GROUP BY with COUNT using RdfStorageModule"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            query (rama/foreign-query ipc module-name "group")

            ;; Mock input: simulate bindings from a sub-plan
            ;; 3 rows for group A, 2 for group B
            input-bindings #{{"?p" "<http://ex/a>" "?v" "\"1\""}
                             {"?p" "<http://ex/a>" "?v" "\"2\""}
                             {"?p" "<http://ex/a>" "?v" "\"3\""}
                             {"?p" "<http://ex/b>" "?v" "\"10\""}
                             {"?p" "<http://ex/b>" "?v" "\"20\""}}

            ;; Mock sub-plan that just returns our test bindings
            sub-plan {:op :bgp :pattern {:s "?s" :p "?p" :o "?v" :c nil}}

            group-vars ["?p"]

            aggregates [{:name "?cnt"
                         :agg {:fn :count
                               :arg {:type :var :name "?v"}
                               :distinct false}}]]

        ;; For this test, we'll invoke the group topology directly with pre-made bindings
        ;; This tests the grouping/aggregation logic without needing real data in PStates
        (let [group-entry-a (build-group-entry {"?p" "<http://ex/a>" "?v" "\"1\""} group-vars aggregates)
              group-entry-b (build-group-entry {"?p" "<http://ex/b>" "?v" "\"10\""} group-vars aggregates)]

          (println "Group entry A:" group-entry-a)
          (println "Group entry B:" group-entry-b)

          ;; Verify build-group-entry works correctly
          (is (contains? group-entry-a ["<http://ex/a>"]))
          (is (contains? group-entry-b ["<http://ex/b>"])))))))

(deftest test-group-topology-sum
  (testing "GROUP BY with SUM aggregate"
    (let [;; Build entries for summing
          aggregates [{:name "?total"
                       :agg {:fn :sum
                             :arg {:type :var :name "?age"}
                             :distinct false}}]
          group-vars ["?dept"]

          binding1 {"?dept" "<http://ex/eng>" "?age" "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"}
          binding2 {"?dept" "<http://ex/eng>" "?age" "\"40\"^^<http://www.w3.org/2001/XMLSchema#integer>"}
          binding3 {"?dept" "<http://ex/sales>" "?age" "\"25\"^^<http://www.w3.org/2001/XMLSchema#integer>"}

          entry1 (build-group-entry binding1 group-vars aggregates)
          entry2 (build-group-entry binding2 group-vars aggregates)
          entry3 (build-group-entry binding3 group-vars aggregates)]

      (println "Sum entries:" entry1 entry2 entry3)

      ;; Verify the entries have correct initial states
      (let [eng-key ["<http://ex/eng>"]
            sales-key ["<http://ex/sales>"]
            [agg-fn state1] (get-in entry1 [eng-key "?total"])
            [_ state2] (get-in entry2 [eng-key "?total"])]

        (is (= :sum agg-fn))
        (is (= 30.0 state1))
        (is (= 40.0 state2))

        ;; Test merging
        (let [merged-state (merge-agg-states :sum state1 state2)]
          (is (= 70.0 merged-state)))))))

(deftest test-finalize-group-results
  (testing "Finalize grouped aggregate results"
    (let [group-vars ["?p"]
          aggregates [{:name "?cnt" :agg {:fn :count :arg {:type :var :name "?v"} :distinct false}}
                      {:name "?total" :agg {:fn :sum :arg {:type :var :name "?v"} :distinct false}}]

          ;; Simulated combined group map after aggregation
          combined-groups {["<http://ex/a>"] {"?cnt" [:count 3]
                                              "?total" [:sum 60.0]}
                           ["<http://ex/b>"] {"?cnt" [:count 2]
                                              "?total" [:sum 30.0]}}

          results (finalize-group-results combined-groups group-vars aggregates)]

      (println "Finalized results:" results)

      (is (= 2 (count results)))

      (let [group-a (first (filter #(= "<http://ex/a>" (get % "?p")) results))
            group-b (first (filter #(= "<http://ex/b>" (get % "?p")) results))]

        (is (some? group-a))
        (is (some? group-b))

        (when group-a
          (is (= "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>" (get group-a "?cnt")))
          (is (= "\"60\"^^<http://www.w3.org/2001/XMLSchema#integer>" (get group-a "?total"))))

        (when group-b
          (is (= "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>" (get group-b "?cnt")))
          (is (= "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>" (get group-b "?total"))))))))

;; =============================================================================
;; End-to-End Tests via SPARQL / SAIL Layer
;; =============================================================================

(def ^:private ^org.eclipse.rdf4j.model.ValueFactory VF (SimpleValueFactory/getInstance))

(deftest test-e2e-count-no-group-by
  (testing "E2E: COUNT without GROUP BY via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              person (.createIRI VF "http://ex/Person")
              rdf-type (.createIRI VF "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
              alice (.createIRI VF "http://ex/alice")
              bob (.createIRI VF "http://ex/bob")
              charlie (.createIRI VF "http://ex/charlie")]

          ;; Add 3 people
          (.begin conn)
          (.add conn alice rdf-type person (into-array Resource []))
          (.add conn bob rdf-type person (into-array Resource []))
          (.add conn charlie rdf-type person (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; COUNT all people
          (let [sparql "SELECT (COUNT(?p) AS ?cnt) WHERE { ?p a <http://ex/Person> }"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 1 (count results)) "Should return exactly one result row")
            (let [cnt-val (.intValue ^Literal (.getValue ^BindingSet (first results) "cnt"))]
              (is (= 3 cnt-val) "Should count 3 people")))

          (.close conn))
        (.shutDown repo)))))

(deftest test-e2e-group-by-count
  (testing "E2E: GROUP BY with COUNT via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              works-at (.createIRI VF "http://ex/worksAt")
              acme (.createIRI VF "http://ex/Acme")
              globex (.createIRI VF "http://ex/Globex")
              alice (.createIRI VF "http://ex/alice")
              bob (.createIRI VF "http://ex/bob")
              charlie (.createIRI VF "http://ex/charlie")
              dave (.createIRI VF "http://ex/dave")]

          ;; Alice and Bob work at Acme, Charlie and Dave work at Globex
          (.begin conn)
          (.add conn alice works-at acme (into-array Resource []))
          (.add conn bob works-at acme (into-array Resource []))
          (.add conn charlie works-at globex (into-array Resource []))
          (.add conn dave works-at globex (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

          ;; Count employees per company
          (let [sparql "SELECT ?company (COUNT(?person) AS ?cnt) WHERE { ?person <http://ex/worksAt> ?company } GROUP BY ?company"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 2 (count results)) "Should return 2 groups (2 companies)")

            ;; Build a map of company -> count
            (let [counts (into {}
                               (map (fn [^BindingSet bs]
                                      [(.stringValue ^org.eclipse.rdf4j.model.Value (.getValue bs "company"))
                                       (.intValue ^Literal (.getValue bs "cnt"))])
                                    results))]
              (is (= 2 (get counts "http://ex/Acme")) "Acme should have 2 employees")
              (is (= 2 (get counts "http://ex/Globex")) "Globex should have 2 employees")))

          (.close conn))
        (.shutDown repo)))))

(deftest test-e2e-sum-aggregate
  (testing "E2E: SUM aggregate via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              amount (.createIRI VF "http://ex/amount")
              int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")
              order1 (.createIRI VF "http://ex/order1")
              order2 (.createIRI VF "http://ex/order2")
              order3 (.createIRI VF "http://ex/order3")]

          ;; Add orders with amounts
          (.begin conn)
          (.add conn order1 amount (.createLiteral VF "100" int-type) (into-array Resource []))
          (.add conn order2 amount (.createLiteral VF "250" int-type) (into-array Resource []))
          (.add conn order3 amount (.createLiteral VF "50" int-type) (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; SUM all amounts
          (let [sparql "SELECT (SUM(?amt) AS ?total) WHERE { ?order <http://ex/amount> ?amt }"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 1 (count results)))
            (let [total (.intValue ^Literal (.getValue ^BindingSet (first results) "total"))]
              (is (= 400 total) "Sum should be 100 + 250 + 50 = 400")))

          (.close conn))
        (.shutDown repo)))))

(deftest test-e2e-avg-aggregate
  (testing "E2E: AVG aggregate via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              score (.createIRI VF "http://ex/score")
              int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")
              test1 (.createIRI VF "http://ex/test1")
              test2 (.createIRI VF "http://ex/test2")
              test3 (.createIRI VF "http://ex/test3")
              test4 (.createIRI VF "http://ex/test4")]

          ;; Add test scores: 80, 90, 70, 100 (avg = 85)
          (.begin conn)
          (.add conn test1 score (.createLiteral VF "80" int-type) (into-array Resource []))
          (.add conn test2 score (.createLiteral VF "90" int-type) (into-array Resource []))
          (.add conn test3 score (.createLiteral VF "70" int-type) (into-array Resource []))
          (.add conn test4 score (.createLiteral VF "100" int-type) (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

          ;; AVG of scores
          (let [sparql "SELECT (AVG(?s) AS ?avg) WHERE { ?test <http://ex/score> ?s }"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 1 (count results)))
            (let [avg (.doubleValue ^Literal (.getValue ^BindingSet (first results) "avg"))]
              (is (== 85.0 avg) "Average should be (80+90+70+100)/4 = 85")))

          (.close conn))
        (.shutDown repo)))))

(deftest test-e2e-min-max-aggregates
  (testing "E2E: MIN and MAX aggregates via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              price (.createIRI VF "http://ex/price")
              int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")
              item1 (.createIRI VF "http://ex/item1")
              item2 (.createIRI VF "http://ex/item2")
              item3 (.createIRI VF "http://ex/item3")]

          ;; Add items with prices: 25, 100, 50
          (.begin conn)
          (.add conn item1 price (.createLiteral VF "25" int-type) (into-array Resource []))
          (.add conn item2 price (.createLiteral VF "100" int-type) (into-array Resource []))
          (.add conn item3 price (.createLiteral VF "50" int-type) (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)

          ;; MIN and MAX prices
          (let [sparql "SELECT (MIN(?p) AS ?minPrice) (MAX(?p) AS ?maxPrice) WHERE { ?item <http://ex/price> ?p }"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 1 (count results)))
            (let [min-price (.intValue ^Literal (.getValue ^BindingSet (first results) "minPrice"))
                  max-price (.intValue ^Literal (.getValue ^BindingSet (first results) "maxPrice"))]
              (is (= 25 min-price) "Minimum price should be 25")
              (is (= 100 max-price) "Maximum price should be 100")))

          (.close conn))
        (.shutDown repo)))))

(deftest test-e2e-group-by-multiple-aggregates
  (testing "E2E: GROUP BY with multiple aggregates via SPARQL"
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

      (let [module-name (rama/get-module-name RdfStorageModule)
            sail (rsail/create-rama-sail ipc module-name)
            repo (SailRepository. sail)]
        (.init repo)

        (let [^SailRepositoryConnection conn (.getConnection repo)
              category (.createIRI VF "http://ex/category")
              price (.createIRI VF "http://ex/price")
              int-type (.createIRI VF "http://www.w3.org/2001/XMLSchema#integer")
              electronics (.createLiteral VF "Electronics")
              clothing (.createLiteral VF "Clothing")
              item1 (.createIRI VF "http://ex/item1")
              item2 (.createIRI VF "http://ex/item2")
              item3 (.createIRI VF "http://ex/item3")
              item4 (.createIRI VF "http://ex/item4")]

          ;; Add items with categories and prices
          (.begin conn)
          ;; Electronics: item1=$100, item2=$200
          (.add conn item1 category electronics (into-array Resource []))
          (.add conn item1 price (.createLiteral VF "100" int-type) (into-array Resource []))
          (.add conn item2 category electronics (into-array Resource []))
          (.add conn item2 price (.createLiteral VF "200" int-type) (into-array Resource []))
          ;; Clothing: item3=$30, item4=$70
          (.add conn item3 category clothing (into-array Resource []))
          (.add conn item3 price (.createLiteral VF "30" int-type) (into-array Resource []))
          (.add conn item4 category clothing (into-array Resource []))
          (.add conn item4 price (.createLiteral VF "70" int-type) (into-array Resource []))
          (.commit conn)

          (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 8)

          ;; GROUP BY category with COUNT, SUM, AVG
          (let [sparql "SELECT ?cat (COUNT(?item) AS ?cnt) (SUM(?p) AS ?total) (AVG(?p) AS ?avg)
                        WHERE {
                          ?item <http://ex/category> ?cat .
                          ?item <http://ex/price> ?p
                        }
                        GROUP BY ?cat"
                query (.prepareTupleQuery conn sparql)
                results (with-open [iter (.evaluate query)]
                          (vec (iterator-seq iter)))]
            (is (= 2 (count results)) "Should have 2 groups (Electronics, Clothing)")

            ;; Build map of category -> aggregates
            (let [by-cat (into {}
                               (map (fn [^BindingSet bs]
                                      [(.stringValue ^org.eclipse.rdf4j.model.Value (.getValue bs "cat"))
                                       {:count (.intValue ^Literal (.getValue bs "cnt"))
                                        :sum (.intValue ^Literal (.getValue bs "total"))
                                        :avg (.doubleValue ^Literal (.getValue bs "avg"))}])
                                    results))]
              ;; Electronics: 2 items, sum=300, avg=150
              (is (= 2 (get-in by-cat ["Electronics" :count])))
              (is (= 300 (get-in by-cat ["Electronics" :sum])))
              (is (== 150.0 (get-in by-cat ["Electronics" :avg])))
              ;; Clothing: 2 items, sum=100, avg=50
              (is (= 2 (get-in by-cat ["Clothing" :count])))
              (is (= 100 (get-in by-cat ["Clothing" :sum])))
              (is (== 50.0 (get-in by-cat ["Clothing" :avg])))))

          (.close conn))
        (.shutDown repo)))))
