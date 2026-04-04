(ns rama-sail.bench.infra.cluster-config
  "Abstraction for IPC vs real cluster connections.

   Provides a unified interface for running benchmarks against either:
   - InProcessCluster (IPC) for testing and development
   - Real Rama cluster for production benchmarks

   The key insight is that Rama's client API (foreign-depot, foreign-pstate,
   foreign-query, foreign-append!, foreign-invoke-query) is identical between
   IPC and real clusters. The only difference is how you obtain the cluster manager."
  (:require [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

;;; ---------------------------------------------------------------------------
;;; Protocol Definition
;;; ---------------------------------------------------------------------------

(defprotocol ClusterConnection
  "Protocol for cluster connection operations."
  (get-cluster-manager [this] "Get the cluster manager instance")
  (get-module-name [this] "Get the deployed module name")
  (launch-module! [this module config] "Launch module (IPC only, throws for cluster)")
  (wait-for-indexer! [this count] "Wait for microbatch processing")
  (closeable? [this] "Returns true if this connection should be closed")
  (connection-type [this] "Returns :ipc or :cluster"))

;;; ---------------------------------------------------------------------------
;;; IPC Implementation
;;; ---------------------------------------------------------------------------

(defrecord IPCConnection [ipc module-name]
  ClusterConnection
  (get-cluster-manager [_] ipc)
  (get-module-name [_] module-name)
  (launch-module! [this module config]
    (rtest/launch-module! ipc module config)
    (assoc this :module-name (rama/get-module-name module)))
  (wait-for-indexer! [_ count]
    (when (pos? count)
      (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" count 120000)))
  (closeable? [_] true)
  (connection-type [_] :ipc)

  java.io.Closeable
  (close [_]
    (.close ipc)))

;;; ---------------------------------------------------------------------------
;;; Real Cluster Implementation
;;; ---------------------------------------------------------------------------

(defrecord RealClusterConnection [manager module-name poll-interval-ms max-wait-ms]
  ClusterConnection
  (get-cluster-manager [_] manager)
  (get-module-name [_] module-name)
  (launch-module! [_ _ _]
    (throw (ex-info "Module must be pre-deployed to cluster. Use scripts/deploy-module.sh" {})))
  (wait-for-indexer! [_ count]
    ;; For cluster mode, estimate wait time based on triple count.
    ;; Assume indexer processes ~5000 triples/sec (conservative for cluster).
    ;; Add base delay for microbatch coordination.
    (let [base-delay-ms 2000
          triples-per-sec 5000
          estimated-ms (+ base-delay-ms (* 1000 (/ (or count 0) triples-per-sec)))
          wait-ms (min estimated-ms (or max-wait-ms 120000))]
      (Thread/sleep (long wait-ms))))
  (closeable? [_] true)
  (connection-type [_] :cluster)

  java.io.Closeable
  (close [_]
    (.close manager)))

;; Cluster connection using a pooled manager. Does not close manager on close.
(defrecord PooledClusterConnection [manager module-name poll-interval-ms max-wait-ms]
  ClusterConnection
  (get-cluster-manager [_] manager)
  (get-module-name [_] module-name)
  (launch-module! [_ _ _]
    (throw (ex-info "Module must be pre-deployed to cluster. Use scripts/deploy-module.sh" {})))
  (wait-for-indexer! [_ count]
    ;; Same wait strategy as RealClusterConnection
    (let [base-delay-ms 2000
          triples-per-sec 5000
          estimated-ms (+ base-delay-ms (* 1000 (/ (or count 0) triples-per-sec)))
          wait-ms (min estimated-ms (or max-wait-ms 120000))]
      (Thread/sleep (long wait-ms))))
  (closeable? [_] false)  ;; Pooled connections don't need explicit closing
  (connection-type [_] :cluster-pooled)

  java.io.Closeable
  (close [_]
    ;; No-op: pooled manager is shared and closed via clear-pooled-managers!
    nil))

;;; ---------------------------------------------------------------------------
;;; Connection Pooling
;;; ---------------------------------------------------------------------------

;; Cache of cluster managers keyed by [host port]
(defonce ^:private pooled-managers (atom {}))

(defn get-pooled-manager
  "Get or create a pooled cluster manager for the given host/port.
   Managers are cached and reused to avoid connection overhead.

   Returns the cached manager or creates a new one if not present."
  [conductor-host conductor-port]
  (let [cache-key [conductor-host conductor-port]]
    (or (get @pooled-managers cache-key)
        (let [manager (rama/open-cluster-manager {"conductor.host" conductor-host
                                                  "conductor.port" conductor-port})]
          (swap! pooled-managers assoc cache-key manager)
          manager))))

(defn clear-pooled-managers!
  "Close and clear all pooled managers. Call during cleanup."
  []
  (doseq [[_ manager] @pooled-managers]
    (try (.close manager) (catch Exception _)))
  (reset! pooled-managers {}))

;;; ---------------------------------------------------------------------------
;;; Factory Functions
;;; ---------------------------------------------------------------------------

(defn create-ipc-connection
  "Create an IPC connection for testing.
   Returns an IPCConnection that must be closed when done."
  []
  (->IPCConnection (rtest/create-ipc) nil))

(defn create-cluster-connection
  "Create a connection to a real Rama cluster.

   Options:
   - :conductor-host   - Conductor hostname (default: localhost)
   - :conductor-port   - Conductor port (default: 1973)
   - :module-name      - Pre-deployed module name (default: rama-sail.core/RdfStorageModule)
   - :poll-interval-ms - Interval for wait-for-indexer! (default: 5000)
   - :pooled           - If true, use pooled manager (default: false)

   Returns a ClusterConnection. Pooled connections share the underlying manager
   and should not be explicitly closed; use clear-pooled-managers! for cleanup."
  [& {:keys [conductor-host conductor-port module-name poll-interval-ms pooled]
      :or {conductor-host "localhost"
           conductor-port 1973
           module-name "rama-sail.core/RdfStorageModule"
           poll-interval-ms 5000
           pooled false}}]
  (if pooled
    ;; Use pooled manager - shared across connections, not closed individually
    (let [manager (get-pooled-manager conductor-host conductor-port)]
      (->PooledClusterConnection manager module-name poll-interval-ms 120000))
    ;; Create dedicated manager - closed when connection is closed
    (let [manager (rama/open-cluster-manager {"conductor.host" conductor-host
                                              "conductor.port" conductor-port})]
      (->RealClusterConnection manager module-name poll-interval-ms 120000))))

(defn create-connection
  "Create a cluster connection based on mode.

   mode: :ipc, :cluster, or :cluster-pooled
   opts: options passed to create-ipc-connection or create-cluster-connection

   :cluster-pooled uses a pooled manager that is shared across connections
   and not closed when the connection is closed. Use clear-pooled-managers!
   to clean up pooled managers at shutdown."
  [mode & opts]
  (case mode
    :ipc (create-ipc-connection)
    :cluster (apply create-cluster-connection opts)
    :cluster-pooled (apply create-cluster-connection :pooled true opts)
    (throw (ex-info "Unknown mode, expected :ipc, :cluster, or :cluster-pooled" {:mode mode}))))

;;; ---------------------------------------------------------------------------
;;; Convenience Macro
;;; ---------------------------------------------------------------------------

(defmacro with-cluster
  "Execute body with a cluster connection, ensuring proper cleanup.

   mode can be :ipc or :cluster
   For :cluster, opts should include :conductor-host, :module-name, etc.

   Usage:
     (with-cluster [conn :ipc]
       ;; use conn
       )

     (with-cluster [conn :cluster :conductor-host \"localhost\" :module-name \"my.Module\"]
       ;; use conn
       )"
  [[conn-sym mode & opts] & body]
  `(let [~conn-sym (create-connection ~mode ~@opts)]
     (try
       ~@body
       (finally
         (when (closeable? ~conn-sym)
           (.close ~conn-sym))))))

;;; ---------------------------------------------------------------------------
;;; Helper Functions
;;; ---------------------------------------------------------------------------

(defn get-depot
  "Get a foreign depot from the cluster connection."
  [conn depot-name]
  (rama/foreign-depot (get-cluster-manager conn) (get-module-name conn) depot-name))

(defn get-query
  "Get a foreign query from the cluster connection."
  [conn query-name]
  (rama/foreign-query (get-cluster-manager conn) (get-module-name conn) query-name))

(defn print-connection-info
  "Print information about the cluster connection."
  [conn]
  (println (format "  Connection type: %s" (name (connection-type conn))))
  (println (format "  Module name: %s" (get-module-name conn))))

