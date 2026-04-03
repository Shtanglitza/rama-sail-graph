(ns rama-sail.sail.w3c.sparql11-property-path-test
  "W3C SPARQL 1.1 property path conformance tests.

   Runs the 33 W3C test cases from the property-path manifest against
   RamaSail to verify property path support.

   Tests are loaded from vendored files in test/resources/w3c/sparql11/property-path/.
   Source: https://github.com/w3c/rdf-tests/tree/main/sparql/sparql11/property-path"
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.w3c.manifest-parser :as manifest]
            [rama-sail.sail.w3c.result-compare :as rc])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model Resource]))

(def ^:private base-dir "w3c/sparql11/property-path/")
(def ^:private module-name (rama/get-module-name RdfStorageModule))

;; Tests expected to fail due to features not yet implemented in RamaSail
;; or requiring named graph loading support not in the test harness.
(def ^:private expected-failures
  {;; Named graph tests: require multiple named graph loading not yet supported
   "(pp07) Path with one graph"                 :named-graph-data
   "(pp34) Named Graph 1"                       :named-graph-data
   "(pp35) Named Graph 2"                       :named-graph-data

   ;; Nested star paths: ((:P)*)* requires recursive ALP handling
   "(pp37) Nested (*)*"                         :nested-star-path})

;;; ---------------------------------------------------------------------------
;;; Shared IPC fixture
;;; ---------------------------------------------------------------------------

(def ^:dynamic *repo* nil)

(defn sparql11-pp-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [sail (rsail/create-rama-sail ipc module-name {:sync-commits true})
          repo (SailRepository. sail)]
      (.init repo)
      (try
        (binding [*repo* repo]
          (f))
        (finally
          (.shutDown repo))))))

(use-fixtures :once sparql11-pp-fixture)

;;; ---------------------------------------------------------------------------
;;; Test helpers
;;; ---------------------------------------------------------------------------

(defn- clear-store! [conn]
  (.begin conn)
  (.clear conn (into-array Resource []))
  (.commit conn))

(defn- read-resource [path]
  (slurp (io/resource path)))

(defn- run-query-evaluation-test
  [{:keys [query-file data-file result-file]}]
  (let [conn (.getConnection *repo*)]
    (try
      (clear-store! conn)
      (when data-file
        (rc/load-data! conn (str base-dir data-file)))
      (let [query-string (read-resource (str base-dir query-file))
            query-type (rc/detect-query-type query-string)
            result-path (str base-dir result-file)]
        (case query-type
          :select (rc/run-select-test conn query-string result-path)
          :construct (rc/run-construct-test conn query-string result-path)
          :ask (rc/run-ask-test conn query-string result-path)))
      (finally
        (.close conn)))))

;;; ---------------------------------------------------------------------------
;;; Main test
;;; ---------------------------------------------------------------------------

(deftest sparql11-property-path
  (let [tests (manifest/parse-manifest (str base-dir "manifest.ttl"))
        results (atom {:pass 0 :fail 0 :expected-fail 0 :unexpected-pass 0 :total 0})]

    (doseq [{:keys [name type] :as test-entry} tests]
      (testing name
        (let [expected-fail? (contains? expected-failures name)
              result (try
                       (case type
                         :query-evaluation (run-query-evaluation-test test-entry)
                         {:pass? false :message (str "Unknown test type: " type)})
                       (catch Exception e
                         {:pass? false
                          :message (str "Exception: " (.getMessage e))
                          :error e}))]
          (swap! results update :total inc)
          (cond
            ;; Passed
            (:pass? result)
            (if expected-fail?
              (do (swap! results update :unexpected-pass inc)
                  (println (str "  UNEXPECTED PASS: " name " (was expected to fail)")))
              (do (swap! results update :pass inc)
                  (is true name)))

            ;; Failed but expected
            expected-fail?
            (swap! results update :expected-fail inc)

            ;; Failed unexpectedly
            :else
            (do (swap! results update :fail inc)
                (is false (str name " - " (:message result))))))))

    ;; Print summary
    (let [{:keys [pass fail expected-fail unexpected-pass total]} @results]
      (println)
      (println "=== W3C SPARQL 1.1 Property Path Conformance ===")
      (println (format "Total: %d | Pass: %d | Fail: %d | Expected fail: %d | Unexpected pass: %d"
                       total pass fail expected-fail unexpected-pass))
      (println (format "Pass rate: %.1f%% (excluding expected failures: %.1f%%)"
                       (if (pos? total) (* 100.0 (/ pass total)) 0.0)
                       (let [testable (- total expected-fail)]
                         (if (pos? testable) (* 100.0 (/ pass testable)) 0.0))))
      (println)
      ;; Assert no unexpected failures
      (is (zero? fail) (format "%d unexpected test failures" fail)))))
