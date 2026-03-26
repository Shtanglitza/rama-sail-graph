(ns rama-sail.bench.cluster.cluster-bench
  "Run BSBM benchmarks against a real Rama cluster.

   Prerequisites:
   1. Rama cluster running with conductor accessible
   2. Module deployed via scripts/deploy-module.sh
   3. Data loaded (or use :load-data true option)

   Usage from REPL:
     (run-cluster-benchmark! :conductor-host \"localhost\" :load-data true)

   Usage from command line:
     lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :load true"
  (:require [rama-sail.bench.bsbm.bsbm-bench :as bsbm]
            [rama-sail.bench.infra.cluster-config :as cc]
            [rama-sail.bench.infra.bench-helpers :as bh]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [com.rpl.rama :as rama])
  (:gen-class))

;;; ---------------------------------------------------------------------------
;;; Configuration
;;; ---------------------------------------------------------------------------

(def default-dataset-path "test/resources/bsbm/dataset_100.nt")

(def default-config
  {:conductor-host "localhost"
   :conductor-port 1973
   :module-name "rama-sail.core/RdfStorageModule"
   :dataset-path default-dataset-path
   :warmup-n 5
   :bench-n 50
   :load-data false
   :include-joins false})

;;; ---------------------------------------------------------------------------
;;; Cluster Health Check
;;; ---------------------------------------------------------------------------

(defn check-cluster-health
  "Check if the cluster is reachable and module is deployed.
   Returns {:healthy true/false :message \"...\"}"
  [& {:keys [conductor-host conductor-port module-name]
      :or {conductor-host "localhost"
           conductor-port 1973
           module-name "rama-sail.core/RdfStorageModule"}}]
  (println "Checking cluster health...")
  (try
    (cc/with-cluster [conn :cluster
                      :conductor-host conductor-host
                      :conductor-port conductor-port
                      :module-name module-name]
      (let [cluster (cc/get-cluster-manager conn)
            ;; Try to access the depot to verify module is deployed
            depot (rama/foreign-depot cluster module-name "*triple-depot")]
        (println (format "  Connected to cluster at %s:%d" conductor-host conductor-port))
        (println (format "  Module %s is accessible" module-name))
        {:healthy true
         :message "Cluster is healthy and module is deployed"}))
    (catch Exception e
      (println (format "  ERROR: %s" (.getMessage e)))
      {:healthy false
       :message (.getMessage e)})))

;;; ---------------------------------------------------------------------------
;;; Triple Count Check
;;; ---------------------------------------------------------------------------

(defn get-triple-count
  "Get the current triple count from the cluster by running a simple SPARQL query.
   Returns the count or nil if unable to connect.

   Note: This uses a SPARQL COUNT query rather than PState navigation
   since the $$spoc index structure is complex (nested maps)."
  [& {:keys [conductor-host conductor-port module-name]
      :or {conductor-host "localhost"
           conductor-port 1973
           module-name "rama-sail.core/RdfStorageModule"}}]
  ;; This is a placeholder - for actual triple counts, run a SPARQL query
  ;; through the SAIL interface, or inspect the cluster via Rama UI
  (println "Note: Triple count requires running a SPARQL query.")
  (println "      Check the Rama cluster UI for PState statistics.")
  nil)

;;; ---------------------------------------------------------------------------
;;; Main Benchmark Runner
;;; ---------------------------------------------------------------------------

(defn run-cluster-benchmark!
  "Run BSBM benchmark against a real Rama cluster.

   Options:
   - :conductor-host   - Cluster conductor hostname (default: localhost)
   - :conductor-port   - Cluster conductor port (default: 1973)
   - :module-name      - Deployed module name (default: rama-sail.core/RdfStorageModule)
   - :dataset-path     - Path to N-Triples file (default: test/resources/bsbm/dataset_100.nt)
   - :warmup-n         - Warmup iterations per query (default: 5)
   - :bench-n          - Benchmark iterations per query (default: 50)
   - :load-data        - If true, load data before benchmark (default: false)
   - :include-joins    - If true, include join-focused queries (default: false)
   - :joins-only       - If true, only run join-focused queries (default: false)

   Returns benchmark results map."
  [& {:keys [conductor-host conductor-port module-name
             dataset-path warmup-n bench-n
             load-data include-joins joins-only]
      :or {conductor-host "localhost"
           conductor-port 1973
           module-name "rama-sail.core/RdfStorageModule"
           dataset-path default-dataset-path
           warmup-n 5
           bench-n 50
           load-data false
           include-joins false
           joins-only false}}]

  (bh/print-section "Cluster BSBM Benchmark")
  (println)
  (println "Configuration:")
  (println (format "  Conductor: %s:%d" conductor-host conductor-port))
  (println (format "  Module: %s" module-name))
  (println (format "  Dataset: %s" dataset-path))
  (println (format "  Load data: %s" load-data))
  (println (format "  Warmup iterations: %d" warmup-n))
  (println (format "  Benchmark iterations: %d" bench-n))
  (println)

  ;; Check cluster health first
  (let [health (check-cluster-health
                :conductor-host conductor-host
                :conductor-port conductor-port
                :module-name module-name)]
    (when-not (:healthy health)
      (throw (ex-info "Cluster health check failed" health))))

  ;; Check if we need to load and dataset exists
  (when (and load-data (not (.exists (io/file dataset-path))))
    (println (format "ERROR: Dataset file not found: %s" dataset-path))
    (throw (ex-info "Dataset not found" {:path dataset-path})))

  ;; Run the benchmark
  (bsbm/run-bsbm-benchmark
   :mode :cluster
   :conductor-host conductor-host
   :conductor-port conductor-port
   :module-name module-name
   :dataset-path dataset-path
   :skip-load (not load-data)
   :warmup-n warmup-n
   :bench-n bench-n
   :include-joins include-joins
   :joins-only joins-only))

;;; ---------------------------------------------------------------------------
;;; Config File Support
;;; ---------------------------------------------------------------------------

(defn run-from-config
  "Run benchmark using configuration from an EDN file.

   Config file format:
   {:conductor-host \"localhost\"
    :conductor-port 1973
    :module-name \"rama-sail.core/RdfStorageModule\"
    :dataset-path \"path/to/data.nt\"
    :warmup-n 5
    :bench-n 100
    :load-data false
    :include-joins true}"
  [config-path]
  (let [config (edn/read-string (slurp config-path))]
    (println (format "Loading config from: %s" config-path))
    (apply run-cluster-benchmark! (mapcat identity config))))

;;; ---------------------------------------------------------------------------
;;; CLI Entry Point
;;; ---------------------------------------------------------------------------

(defn parse-cli-args
  "Parse command line arguments in keyword-value format.
   Example: :host localhost :port 1973 :load true"
  [args]
  (loop [args args
         result {}]
    (if (empty? args)
      result
      (let [[k v & rest] args
            key (if (string? k) (keyword (subs k 1)) k)
            val (cond
                  (= v "true") true
                  (= v "false") false
                  (re-matches #"\d+" (str v)) (Long/parseLong v)
                  :else v)]
        (recur rest (assoc result key val))))))

(defn -main
  "CLI entry point for cluster benchmarks.

   Usage:
     lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :load true

   Options:
     :host        - Conductor hostname (default: localhost)
     :port        - Conductor port (default: 1973)
     :module      - Module name (default: rama-sail.core/RdfStorageModule)
     :dataset     - Dataset path (default: test/resources/bsbm/dataset_100.nt)
     :warmup      - Warmup iterations (default: 5)
     :iterations  - Benchmark iterations (default: 50)
     :load        - Load data before benchmark (default: false)
     :joins       - Include join queries (default: false)
     :joins-only  - Only run join queries (default: false)
     :config      - Path to config EDN file (overrides other options)"
  [& args]
  (let [opts (parse-cli-args args)]

    ;; If config file provided, use that
    (if-let [config-path (:config opts)]
      (run-from-config config-path)

      ;; Otherwise use CLI args
      (run-cluster-benchmark!
       :conductor-host (or (:host opts) "localhost")
       :conductor-port (or (:port opts) 1973)
       :module-name (or (:module opts) "rama-sail.core/RdfStorageModule")
       :dataset-path (or (:dataset opts) default-dataset-path)
       :warmup-n (or (:warmup opts) 5)
       :bench-n (or (:iterations opts) 50)
       :load-data (or (:load opts) false)
       :include-joins (or (:joins opts) false)
       :joins-only (or (:joins-only opts) false)))))

;;; ---------------------------------------------------------------------------
;;; REPL Helpers
;;; ---------------------------------------------------------------------------

(comment
  ;; Check cluster health
  (check-cluster-health :conductor-host "localhost")

  ;; Get triple count
  (get-triple-count :conductor-host "localhost")

  ;; Run benchmark with data loading
  (run-cluster-benchmark!
   :conductor-host "localhost"
   :dataset-path "test/resources/bsbm/dataset_100.nt"
   :load-data true
   :warmup-n 3
   :bench-n 20)

  ;; Run benchmark without data loading (data already in cluster)
  (run-cluster-benchmark!
   :conductor-host "localhost"
   :bench-n 100
   :include-joins true)

  ;; Run from command line:
  ;; lein run -m rama-sail.bench.cluster.cluster-bench :host localhost :load true
  ;; lein run -m rama-sail.bench.cluster.cluster-bench :config cluster-config.edn
  )
