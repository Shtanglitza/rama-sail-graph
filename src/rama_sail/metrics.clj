(ns rama-sail.metrics
  "Prometheus metrics for Rama SAIL observability.

   Provides SAIL-specific metrics complementing Rama's built-in monitoring:
   - Query latency (histogram)
   - Query counts by status (counter)
   - Active connections (gauge)
   - Query result sizes (summary)

   Rama's built-in dashboard (port 8888) provides cluster-level metrics:
   - Microbatch throughput and latency
   - Task-level performance
   - Queue depth and worker health
   - PState operation metrics

   Usage:
     ;; Record a query
     (with-query-timing
       (execute-query ...))

     ;; Track connections
     (inc-connections)
     (dec-connections)

     ;; Export metrics for scraping
     (metrics-text)"
  (:import [io.prometheus.client Counter Counter$Child Histogram Histogram$Builder Gauge Summary Summary$Builder CollectorRegistry]
           [io.prometheus.client.hotspot DefaultExports]
           [io.prometheus.client.exporter.common TextFormat]
           [java.io StringWriter]))

;; Initialize JVM metrics (memory, GC, threads, etc.)
(defonce ^:private init-hotspot
  (do (DefaultExports/initialize) true))

;; Query latency histogram with buckets appropriate for SPARQL queries
;; Buckets: 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 60s
(defonce ^Histogram query-latency
  (let [^Histogram$Builder builder (-> (Histogram/build)
                                       (.name "ramasail_query_latency_seconds")
                                       (.help "Query execution latency in seconds"))]
    (-> builder
        (.buckets (double-array [0.01 0.05 0.1 0.25 0.5 1.0 2.5 5.0 10.0 30.0 60.0]))
        (.register))))

;; Query counter with status label: success, timeout, error, fallback
(defonce ^Counter query-total
  (-> (Counter/build)
      (.name "ramasail_queries_total")
      (.help "Total number of queries executed")
      (.labelNames (into-array String ["status"]))
      (.register)))

;; Active SAIL connections gauge
(defonce ^Gauge active-connections
  (-> (Gauge/build)
      (.name "ramasail_active_connections")
      (.help "Number of active SAIL connections")
      (.register)))

;; Query result size summary with quantiles
(defonce ^Summary query-result-size
  (let [^Summary$Builder builder (-> (Summary/build)
                                     (.name "ramasail_query_result_size")
                                     (.help "Number of results returned per query"))]
    (-> builder
        (.quantile 0.5 0.05)
        (.quantile 0.9 0.01)
        (.quantile 0.99 0.001)
        (.register))))

;; Transaction counter with status label
(defonce ^Counter transaction-total
  (-> (Counter/build)
      (.name "ramasail_transactions_total")
      (.help "Total number of transactions")
      (.labelNames (into-array String ["status"]))
      (.register)))

;; Transaction operation count (triples added/removed)
(defonce ^Counter transaction-ops
  (-> (Counter/build)
      (.name "ramasail_transaction_ops_total")
      (.help "Total number of triple operations in transactions")
      (.labelNames (into-array String ["type"]))
      (.register)))

;;; ---------------------------------------------------------------------------
;;; Helper Functions
;;; ---------------------------------------------------------------------------

(defn record-query-latency
  "Record a query execution time in seconds."
  [seconds]
  (.observe query-latency seconds))

(defn inc-query-count
  "Increment query counter for given status: success, timeout, error, fallback"
  [status]
  (.inc ^Counter$Child (.labels query-total (into-array String [status]))))

(defn inc-connections
  "Increment active connection count."
  []
  (.inc active-connections))

(defn dec-connections
  "Decrement active connection count."
  []
  (.dec active-connections))

(defn record-result-size
  "Record the number of results returned by a query."
  [size]
  (.observe query-result-size (double size)))

(defn inc-transaction-count
  "Increment transaction counter for given status: commit, rollback, error"
  [status]
  (.inc ^Counter$Child (.labels transaction-total (into-array String [status]))))

(defn record-transaction-ops
  "Record number of operations in a transaction by type: add, del"
  [op-type count]
  (.inc ^Counter$Child (.labels transaction-ops (into-array String [(name op-type)])) (double count)))

;;; ---------------------------------------------------------------------------
;;; Timing Macros
;;; ---------------------------------------------------------------------------

(defmacro with-query-timing
  "Execute body and record query latency. Tracks success/timeout/error counts.

   Usage:
     (with-query-timing
       (execute-rama-query ...))"
  [& body]
  `(let [start# (System/nanoTime)]
     (try
       (let [result# (do ~@body)]
         (inc-query-count "success")
         (record-query-latency (/ (- (System/nanoTime) start#) 1e9))
         result#)
       (catch org.eclipse.rdf4j.query.QueryInterruptedException e#
         (inc-query-count "timeout")
         (record-query-latency (/ (- (System/nanoTime) start#) 1e9))
         (throw e#))
       (catch Exception e#
         (inc-query-count "error")
         (record-query-latency (/ (- (System/nanoTime) start#) 1e9))
         (throw e#)))))

(defmacro with-fallback-timing
  "Record fallback strategy usage (separate from Rama query timing)."
  [& body]
  `(let [start# (System/nanoTime)]
     (try
       (let [result# (do ~@body)]
         (inc-query-count "fallback")
         (record-query-latency (/ (- (System/nanoTime) start#) 1e9))
         result#)
       (catch Exception e#
         (inc-query-count "error")
         (record-query-latency (/ (- (System/nanoTime) start#) 1e9))
         (throw e#)))))

;;; ---------------------------------------------------------------------------
;;; Metrics Export
;;; ---------------------------------------------------------------------------

(defn metrics-text
  "Export all metrics in Prometheus text format for HTTP scraping."
  []
  (let [writer (StringWriter.)
        registry (CollectorRegistry/defaultRegistry)]
    (TextFormat/write004 writer (.metricFamilySamples registry))
    (.toString writer)))

(defn reset-metrics!
  "Reset all metrics. Useful for testing."
  []
  (.clear query-latency)
  (.clear query-total)
  (.set active-connections (double 0))
  (.clear query-result-size)
  (.clear transaction-total)
  (.clear transaction-ops))
