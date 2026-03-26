(ns rama-sail.sail.compliance-test
  "RDF4J compliance tests runner.

   Runs the RDF4J RDFStoreTest suite against RamaSail to verify
   compliance with the SAIL storage interface.

   Note: These tests are JUnit-based and require special handling.
   Run with: lein test :only rama-sail.sail.compliance-test"
  (:require [clojure.test :refer :all])
  (:import [org.junit.platform.launcher.core LauncherDiscoveryRequestBuilder LauncherFactory]
           [org.junit.platform.launcher.listeners SummaryGeneratingListener TestExecutionSummary$Failure]
           [org.junit.platform.engine.discovery DiscoverySelectors ClassSelector]
           [rama_sail RamaSailRDFStoreTest]))

(deftest ^:compliance run-rdf4j-compliance-tests
  (testing "RDF4J RDFStoreTest compliance suite"
    (let [selector (DiscoverySelectors/selectClass RamaSailRDFStoreTest)
          request (-> (LauncherDiscoveryRequestBuilder/request)
                      (.selectors (into-array ClassSelector [selector]))
                      (.build))
          launcher (LauncherFactory/create)
          listener (SummaryGeneratingListener.)]
      (.registerTestExecutionListeners launcher (into-array org.junit.platform.launcher.TestExecutionListener [listener]))
      (.execute launcher request (into-array org.junit.platform.launcher.TestExecutionListener []))

      (let [summary (.getSummary listener)
            total (.getTestsFoundCount summary)
            succeeded (.getTestsSucceededCount summary)
            failed (.getTestsFailedCount summary)
            skipped (.getTestsSkippedCount summary)]

        (println)
        (println "=== RDF4J RDFStoreTest Compliance Results ===")
        (println (format "Total: %d | Passed: %d | Failed: %d | Skipped: %d"
                         total succeeded failed skipped))
        (println)

        ;; Print failures if any
        (when (pos? failed)
          (println "=== Failures ===")
          (doseq [^TestExecutionSummary$Failure failure (.getFailures summary)]
            (println)
            (println "Test:" (.getDisplayName (.getTestIdentifier failure)))
            (let [ex (.getException failure)]
              (println "Cause:" (.getMessage ex))
              (when-let [cause (.getCause ex)]
                (println "Root cause:" (.getMessage cause)))))
          (println))

        ;; Assert for clojure.test reporting
        (is (zero? failed)
            (format "RDF4J compliance: %d/%d tests failed" failed total))))))
