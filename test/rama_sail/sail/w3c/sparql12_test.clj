(ns rama-sail.sail.w3c.sparql12-test
  "W3C SPARQL 1.2 conformance tests for eval-triple-terms.

   Runs the 39 W3C test cases from the eval-triple-terms manifest against
   RamaSail to verify SPARQL 1.2 triple term support.

   Tests are loaded from vendored files in test/resources/w3c/sparql12/eval-triple-terms/.
   Source: https://github.com/w3c/rdf-tests/tree/main/sparql/sparql12/eval-triple-terms"
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.w3c.manifest-parser :as manifest]
            [rama-sail.sail.w3c.result-compare :as rc])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model Resource]))

(def ^:private base-dir "w3c/sparql12/eval-triple-terms/")
(def ^:private module-name (rama/get-module-name RdfStorageModule))

;;; ---------------------------------------------------------------------------
;;; Shared IPC fixture
;;; ---------------------------------------------------------------------------

(def ^:dynamic *repo* nil)

(defn sparql12-fixture [f]
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

(use-fixtures :once sparql12-fixture)

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
  "Execute a single W3C QueryEvaluationTest."
  [{:keys [name query-file data-file result-file]}]
  (let [conn (.getConnection *repo*)]
    (try
      (clear-store! conn)
      ;; Load test data
      (when data-file
        (rc/load-data! conn (str base-dir data-file)))
      ;; Read and classify query
      (let [query-string (read-resource (str base-dir query-file))
            query-type (rc/detect-query-type query-string)
            result-path (str base-dir result-file)]
        (case query-type
          :select (rc/run-select-test conn query-string result-path)
          :construct (rc/run-construct-test conn query-string result-path)
          :ask {:pass? false :message "ASK queries not yet supported in test runner"}))
      (finally
        (.close conn)))))

(defn- run-update-evaluation-test
  "Execute a single W3C UpdateEvaluationTest."
  [{:keys [name update-file data-file result-file]}]
  (let [conn (.getConnection *repo*)]
    (try
      (clear-store! conn)
      ;; Load initial data
      (when data-file
        (rc/load-data! conn (str base-dir data-file)))
      ;; Execute update and compare
      (let [update-string (read-resource (str base-dir update-file))
            result-path (str base-dir result-file)]
        (rc/run-update-test conn update-string result-path))
      (finally
        (.close conn)))))

;;; ---------------------------------------------------------------------------
;;; Main test
;;; ---------------------------------------------------------------------------

(deftest sparql12-eval-triple-terms
  (let [tests (manifest/parse-manifest (str base-dir "manifest.ttl"))
        results (atom {:pass 0 :fail 0 :error 0 :total 0})]

    (doseq [{:keys [name type] :as test-entry} tests]
      (testing name
        (let [result (try
                       (case type
                         :query-evaluation (run-query-evaluation-test test-entry)
                         :update-evaluation (run-update-evaluation-test test-entry)
                         {:pass? false :message (str "Unknown test type: " type)})
                       (catch Exception e
                         {:pass? false
                          :message (str "Exception: " (.getMessage e))
                          :error e}))]
          (swap! results update :total inc)
          (if (:pass? result)
            (do
              (swap! results update :pass inc)
              (is true name))
            (do
              (swap! results update (if (:error result) :error :fail) inc)
              (is false (str name " - " (:message result))))))))

    ;; Print summary
    (let [{:keys [pass fail error total]} @results]
      (println)
      (println "=== W3C SPARQL 1.2 eval-triple-terms Conformance ===")
      (println (format "Total: %d | Pass: %d | Fail: %d | Error: %d"
                       total pass fail error))
      (println (format "Pass rate: %.1f%%" (if (pos? total) (* 100.0 (/ pass total)) 0.0)))
      (println))))
