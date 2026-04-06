(ns ^:no-doc rama-sail.errors
  "Comprehensive error handling for Rama SAIL operations.

   Provides utilities to wrap Rama-specific exceptions with user-friendly
   context and map them to appropriate RDF4J exception types."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [org.eclipse.rdf4j.query QueryEvaluationException QueryInterruptedException]
           [org.eclipse.rdf4j.sail SailException]))

(defn- exception-class-name
  "Get the class name of an exception as a string."
  [^Throwable e]
  (when e
    (str (class e))))

(defn wrap-rama-exception
  "Wrap Rama exceptions with user-friendly context.

   Maps Rama-specific exceptions to appropriate RDF4J exception types:
   - TimeoutException -> QueryEvaluationException with timeout message
   - OutOfMemoryError -> QueryEvaluationException with OOM message
   - QueryTopologyInvokeFailed -> QueryEvaluationException with cluster message
   - TopologyDoesNotExistException -> SailException with deployment message
   - Other exceptions -> QueryEvaluationException with generic message"
  [^Throwable e operation-name]
  (let [class-name (exception-class-name e)
        original-msg (or (.getMessage e) "Unknown error")
        msg (str "Rama operation '" operation-name "' failed: " original-msg)]
    (log/error e msg)
    (cond
      ;; Timeout - already handled upstream, but catch here for completeness
      (instance? java.util.concurrent.TimeoutException e)
      (QueryEvaluationException. (str "Query timeout: " msg) e)

      ;; Out of memory - critical error during query
      (instance? OutOfMemoryError e)
      (QueryEvaluationException.
       (str "Out of memory during query execution. Consider reducing result size "
            "or increasing heap memory. Original error: " msg)
       e)

      ;; Rama cluster query failed
      (and class-name (str/includes? class-name "QueryTopologyInvokeFailed"))
      (QueryEvaluationException.
       (str "Rama cluster query failed. The cluster may be overloaded or experiencing "
            "issues. Original error: " msg)
       e)

      ;; Module not deployed
      (and class-name (str/includes? class-name "TopologyDoesNotExistException"))
      (SailException.
       (str "Rama module not deployed. Ensure the RdfStorageModule is deployed to the cluster. "
            "Original error: " msg)
       e)

      ;; Connection/network issues
      (and class-name
           (or (str/includes? class-name "ConnectException")
               (str/includes? class-name "SocketException")
               (str/includes? class-name "IOException")))
      (SailException.
       (str "Network error connecting to Rama cluster. Check cluster connectivity. "
            "Original error: " msg)
       e)

      ;; Task failure
      (and class-name (str/includes? class-name "TaskGlobalException"))
      (QueryEvaluationException.
       (str "Rama task execution failed. The query may be too complex or data may be "
            "corrupted. Original error: " msg)
       e)

      ;; Frame size errors (often data/memory related)
      (and original-msg (str/includes? original-msg "frame size"))
      (QueryEvaluationException.
       (str "Rama frame size exceeded. The query may return too much data per partition. "
            "Consider adding LIMIT or filtering. Original error: " msg)
       e)

      ;; Generic fallback
      :else
      (QueryEvaluationException. msg e))))

(defmacro with-rama-error-handling
  "Execute body with comprehensive Rama error handling.

   Re-throws QueryInterruptedException (timeouts) and UnsupportedOperationException
   (for fallback handling) as-is. All other exceptions are wrapped with
   user-friendly context via wrap-rama-exception.

   Usage:
   (with-rama-error-handling \"query-evaluation\"
     (invoke-rama-query ...))"
  [operation-name & body]
  `(try
     ~@body
     (catch QueryInterruptedException e#
       ;; Re-throw timeout exceptions as-is for proper handling upstream
       (throw e#))
     (catch UnsupportedOperationException e#
       ;; Re-throw for fallback handling in SAIL layer
       (throw e#))
     (catch Throwable e#
       (throw (wrap-rama-exception e# ~operation-name)))))

(defmacro with-transaction-error-handling
  "Execute transaction body with error handling and operation tracking.

   Tracks successful operations for partial failure diagnosis.
   On error, logs how many operations succeeded before failure.

   Usage:
   (with-transaction-error-handling \"commit\" committed-count-atom op-count
     (doseq [op ops]
       (execute op)
       (swap! committed-count-atom inc)))"
  [operation-name committed-atom total-count & body]
  `(try
     ~@body
     (catch Throwable e#
       (let [committed# @~committed-atom
             remaining# (- ~total-count committed#)]
         (log/error e# (str "Transaction '" ~operation-name "' failed after "
                            committed# " of " ~total-count " operations. "
                            remaining# " operations were not committed."))
         (throw (SailException.
                 (str "Transaction failed: " committed# " of " ~total-count
                      " operations committed before failure. "
                      "Data may be in inconsistent state. "
                      "Original error: " (.getMessage e#))
                 e#))))))
