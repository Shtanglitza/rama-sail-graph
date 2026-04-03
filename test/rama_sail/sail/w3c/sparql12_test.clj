(ns rama-sail.sail.w3c.sparql12-test
  "W3C SPARQL 1.2 conformance tests for eval-triple-terms.

   Runs the 41 W3C test cases from the eval-triple-terms manifest against
   RamaSail to verify SPARQL 1.2 triple term support.

   Tests are loaded from vendored files in test/resources/w3c/sparql12/eval-triple-terms/.
   Source: https://github.com/w3c/rdf-tests/tree/main/sparql/sparql12/eval-triple-terms

   Failure categories:
   - A: RDF4J Turtle parser doesn't support <<( s p o )>> triple term syntax
   - B: RDF4J Turtle parser doesn't support ~ reifier syntax
   - C: RDF4J SPARQL parser doesn't support {| |} annotation or <<()>> in queries
   - D: RDF 1.2 reification semantics (rdf:reifies) not yet implemented
   - E: Test infrastructure / minor issues"
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

(def ^:private base-dir "w3c/sparql12/eval-triple-terms/")
(def ^:private module-name (rama/get-module-name RdfStorageModule))

;; Tests expected to fail because they need RDF 1.2 features not available
;; in RDF4J 5.2.x or not yet implemented in RamaSail.
;; Key: test name -> reason code
(def ^:private expected-failures
  {;; Cat A: RDF4J Turtle parser doesn't support <<( s p o )>> syntax
   "Triple terms - all graph triples (JSON results)"           :rdf12-turtle-syntax
   "Triple terms - all graph triples (XML results)"            :rdf12-turtle-syntax
   "Triple terms - in VALUES"                                  :rdf12-turtle-syntax
   "Triple terms - in expressions and equality"                :rdf12-turtle-syntax
   "Reified triples - Pattern - literals or triple terms"      :rdf12-turtle-syntax
   "Triple terms - Pattern - literals or triple terms"         :rdf12-turtle-syntax

   ;; Cat B: RDF4J Turtle parser doesn't support ~ reifier syntax
   "Reified triples - Asserted and reified triple"             :rdf12-reifier-syntax
   "Reified triples - Pattern - Variable for reified triple reifier" :rdf12-reifier-syntax
   "Reified triples - Pattern - Variable for reified triple"   :rdf12-reifier-syntax
   "Reified triples - Pattern - No match"                      :rdf12-reifier-syntax
   "Reified triples - Pattern - match variables in triple terms" :rdf12-reifier-syntax
   "Reified triples - Pattern - Nesting 1"                     :rdf12-reifier-syntax
   "Reified triples - Pattern - Nesting 2"                     :rdf12-reifier-syntax
   "Reified triples - Pattern - Match and nesting"             :rdf12-reifier-syntax
   "Reified triples - Pattern - Match for non-equal reifier and nesting" :rdf12-reifier-syntax

   ;; Cat C: RDF4J SPARQL parser doesn't support {| |} or <<()>> in queries
   "Annotated triple - match constant reified triple"          :rdf12-sparql-syntax
   "Reified triples - CONSTRUCT with annotation syntax"        :rdf12-sparql-syntax
   "Reified triples - CONSTRUCT WHERE with annotation syntax"  :rdf12-sparql-syntax
   "Reified triples - Embedded triple - BIND - CONSTRUCT"      :rdf12-sparql-syntax
   "Reified triples - Embedded triple - Functions"             :rdf12-sparql-syntax
   "Reified triples - Update - annotation"                     :rdf12-sparql-syntax
   "Reified triples - Update - data"                           :rdf12-sparql-syntax

   ;; Cat B+C: Data uses {| |} (Turtle) making CONSTRUCT tests fail
   "Reified triples - CONSTRUCT with constant template"        :rdf12-turtle-syntax
   "Reified triples - CONSTRUCT WHERE with constant template"  :rdf12-turtle-syntax
   "Reified triples - CONSTRUCT - about every triple"          :rdf12-turtle-syntax

   ;; Cat A: Data uses <<( )>> syntax — parser failure
   "Reified triples - Embedded triple - sameTerm"              :rdf12-turtle-syntax
   "Reified triples - Embedded triple - value-equality"        :rdf12-turtle-syntax
   "Reified triples - Embedded triple - ORDER BY"              :rdf12-turtle-syntax
   "Reified triples - Embedded triple - ordering"              :rdf12-turtle-syntax

   ;; Cat D: Data loads but expected results require RDF 1.2 rdf:reifies semantics
   "Reified triples - all graph triples (JSON results)"        :rdf12-reifies-semantics
   "Reified triples - all graph triples (XML results)"         :rdf12-reifies-semantics
   "Reified triples - match constant reified triple"           :rdf12-reifies-semantics
   "Reified triples - match reified triple, var subject"       :rdf12-reifies-semantics
   "Reified triples - GRAPH"                                   :rdf12-reifies-semantics
   "Reified triples - GRAPHs with blank node"                  :rdf12-reifies-semantics})

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
          :ask {:pass? false :message "ASK queries not yet supported"}))
      (finally
        (.close conn)))))

(defn- run-update-evaluation-test
  [{:keys [update-file data-file result-file]}]
  (let [conn (.getConnection *repo*)]
    (try
      (clear-store! conn)
      (when data-file
        (rc/load-data! conn (str base-dir data-file)))
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
        results (atom {:pass 0 :fail 0 :expected-fail 0 :unexpected-pass 0 :total 0})]

    (doseq [{:keys [name type] :as test-entry} tests]
      (testing name
        (let [expected-fail? (contains? expected-failures name)
              result (try
                       (case type
                         :query-evaluation (run-query-evaluation-test test-entry)
                         :update-evaluation (run-update-evaluation-test test-entry)
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
      (println "=== W3C SPARQL 1.2 eval-triple-terms Conformance ===")
      (println (format "Total: %d | Pass: %d | Fail: %d | Expected fail: %d | Unexpected pass: %d"
                       total pass fail expected-fail unexpected-pass))
      (println (format "Pass rate: %.1f%% (excluding expected failures: %.1f%%)"
                       (if (pos? total) (* 100.0 (/ pass total)) 0.0)
                       (let [testable (- total expected-fail)]
                         (if (pos? testable) (* 100.0 (/ pass testable)) 0.0))))
      (println)
      ;; Assert no unexpected failures
      (is (zero? fail) (format "%d unexpected test failures" fail)))))
