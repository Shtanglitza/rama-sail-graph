(ns rama-sail.bulk-test
  "Integration tests (IPC) for rama-sail.bulk."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.bulk :as bulk]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.module.indexer :as idx])
  (:import (java.io File)))

(def module-name (rama/get-module-name RdfStorageModule))
(def ^:dynamic *ipc* nil)

(use-fixtures :once (fn [f]
                      (with-open [ipc (rtest/create-ipc)]
                        (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
                        (binding [*ipc* ipc] (f)))))

(defn- graph-quads [g]
  (set (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "find-triples") nil nil nil g)))

(defn- lit [s] (str \" s \"))

(deftest test-load-quads
  (let [g (str "<http://ex/bulk-g-" (System/nanoTime) ">")
        quads (vec (for [i (range 1234)]
                     [(str "<http://ex/bulk-s" (quot i 3) ">") (str "<http://ex/bulk-p" (mod i 5) ">") (lit (str "v" i)) g]))
        ;; 100 quads per record and at most 2 records queued: 13 records, with backpressure
        stats (bulk/load-quads! *ipc* module-name quads :chunk-size 100 :max-queued 2)]
    (is (= 1234 (:count stats)))
    (is (pos? (:triples-per-sec stats)))
    (is (= (set quads) (graph-quads g)) "everything is queryable when load-quads! returns")
    (testing "loading the same quads again changes nothing"
      (bulk/load-quads! *ipc* module-name quads :chunk-size 500)
      (is (= (set quads) (graph-quads g))))))

(deftest test-load-quads-with-a-raised-budget
  (let [g (str "<http://ex/bulk-budget-g-" (System/nanoTime) ">")
        quads (vec (for [i (range 300)] [(str "<http://ex/bulk-b" i ">") "<http://ex/bulk-p>" (lit "v") g]))
        config #(rama/foreign-select-one (com.rpl.rama.path/keypath :commit-ops-per-microbatch)
                                         (rama/foreign-pstate *ipc* module-name "$$indexer-config") {:pkey 0})]
    (bulk/load-quads! *ipc* module-name quads :chunk-size 100 :commit-ops-per-microbatch 50000)
    (is (= (set quads) (graph-quads g)))
    (is (loop [i 0]
          (cond (= idx/commit-ops-per-microbatch (config)) true
                (> i 100) false
                :else (do (Thread/sleep 100) (recur (inc i)))))
        "the default budget is restored after the load")))

(deftest test-load-file
  (let [f (doto (File/createTempFile "bulk-test" ".nt") (.deleteOnExit))
        g (str "<http://ex/bulk-file-g-" (System/nanoTime) ">")
        nl "\n"]
    (spit f (str "<http://ex/f/s1> <http://ex/f/name> " (lit "Alice") " ." nl
                 "<http://ex/f/s1> <http://ex/f/age> " (lit "30") "^^<http://www.w3.org/2001/XMLSchema#integer> ." nl
                 "# a comment" nl
                 "<http://ex/f/s1> <http://ex/f/label> " (lit "Bonjour") "@fr ." nl
                 "<http://ex/f/s1> <http://ex/f/knows> _:b1 ." nl
                 "_:b1 <http://ex/f/name> " (lit "two words") " ." nl))
    (let [stats (bulk/load-file! *ipc* module-name f :context g)
          quads (graph-quads g)
          objects (set (map #(nth % 2) quads))]
      (is (= 5 (:count stats)))
      (is (= 5 (count quads)))
      (is (contains? objects (lit "Alice")))
      (is (contains? objects (str (lit "30") "^^<http://www.w3.org/2001/XMLSchema#integer>")))
      (is (contains? objects (str (lit "Bonjour") "@fr")))
      (testing "a blank node keeps one identity across the file"
        (let [bnode (first (filter #(.startsWith ^String % "_:") objects))]
          (is (some? bnode))
          (is (some #(= bnode (first %)) quads)))))))

(deftest test-loader-needs-a-module-with-commit-records
  (let [foreign-query rama/foreign-query]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not accept :commit records"
                          (with-redefs [rama/foreign-query
                                        (fn [cluster module query]
                                          (if (= query "pending-commit-count")
                                            (throw (Exception. "topology-id: pending-commit-count"))
                                            (foreign-query cluster module query)))]
                            (bulk/loader *ipc* module-name))))))
