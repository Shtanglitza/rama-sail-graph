(ns rama-sail.bench.infra.bench-helpers
  "Benchmarking infrastructure: timing macros, percentile calculations, and test setup.

   IMPORTANT CAVEAT: InProcessCluster (IPC) vs Real Rama Cluster
   ==============================================================
   These benchmarks run on Rama's InProcessCluster, a testing utility that
   simulates distributed behavior within a single JVM. Results should be
   interpreted with this in mind:

   IPC Limitations (pessimistic factors):
   - All overhead, no parallelism benefit: IPC performs serialization, task
     coordination, and partition routing like a real cluster, but all 'tasks'
     compete for the same CPU cores, memory, and GC cycles.
   - GC pressure is concentrated: A single GC pause affects all 'distributed'
     tasks simultaneously, causing p99 latency spikes.
   - No true parallel I/O: Real clusters parallelize disk/network I/O across
     nodes; IPC serializes everything through one process.
   - Thread contention: Multiple task threads fight for CPU in one JVM.

   Real Rama Cluster Expectations:
   - Higher throughput from true parallelism across machines
   - More stable latencies with independent GC per node
   - Linear horizontal scaling (add nodes, get proportional capacity)
   - p99 outliers from shared-JVM effects would largely disappear

   Bottom line: IPC benchmarks establish a performance floor and verify
   correctness. Production performance on a real Rama cluster should be
   significantly better, especially for throughput and tail latencies."
  (:require [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]))

;;; ---------------------------------------------------------------------------
;;; Timing Utilities
;;; ---------------------------------------------------------------------------

(defmacro time-ns
  "Execute expr and return {:result <value> :ns <nanoseconds> :ms <milliseconds>}."
  [expr]
  `(let [start# (System/nanoTime)
         result# ~expr
         end# (System/nanoTime)]
     {:result result#
      :ns (- end# start#)
      :ms (/ (- end# start#) 1e6)}))

(defmacro time-ms-only
  "Execute expr and return {:result <value> :ms <milliseconds>}."
  [expr]
  `(let [start# (System/nanoTime)
         result# ~expr
         end# (System/nanoTime)]
     {:result result#
      :ms (/ (- end# start#) 1e6)}))

;;; ---------------------------------------------------------------------------
;;; Percentile Statistics
;;; ---------------------------------------------------------------------------

(defn percentile
  "Calculate the p-th percentile of a sorted vector of numbers.
   p should be between 0 and 100."
  [sorted-values p]
  (if (empty? sorted-values)
    nil
    (let [n (count sorted-values)
          idx (-> (* p (dec n))
                  (/ 100.0)
                  Math/ceil
                  int
                  (min (dec n))
                  (max 0))]
      (nth sorted-values idx))))

(defn calculate-stats
  "Calculate statistics from a collection of timing values (in ms).
   Returns {:min :max :p50 :p95 :p99 :mean :count}."
  [timings-ms]
  (when (seq timings-ms)
    (let [sorted (vec (sort timings-ms))
          n (count sorted)]
      {:min (first sorted)
       :max (last sorted)
       :p50 (percentile sorted 50)
       :p95 (percentile sorted 95)
       :p99 (percentile sorted 99)
       :mean (/ (reduce + sorted) (double n))
       :count n})))

(defn format-stats
  "Format stats map as a human-readable string."
  [{:keys [min max p50 p95 p99 mean count] :as stats}]
  (if stats
    (format "n=%d | min=%.2fms | p50=%.2fms | p95=%.2fms | p99=%.2fms | max=%.2fms | mean=%.2fms"
            count min p50 p95 p99 max mean)
    "no data"))

;;; ---------------------------------------------------------------------------
;;; Benchmark Execution
;;; ---------------------------------------------------------------------------

(defmacro bench
  "Run expr n times, return statistics map.
   Returns {:min :max :p50 :p95 :p99 :mean :count :timings}."
  [n expr]
  `(let [timings# (mapv (fn [_#]
                          (:ms (time-ms-only ~expr)))
                        (range ~n))
         stats# (calculate-stats timings#)]
     (assoc stats# :timings timings#)))

(defmacro bench-warmup
  "Run expr n times with m warmup iterations, return statistics (excluding warmup)."
  [warmup-n bench-n expr]
  `(do
     ;; Warmup phase
     (dotimes [_# ~warmup-n]
       ~expr)
     ;; Benchmark phase
     (bench ~bench-n ~expr)))

;;; ---------------------------------------------------------------------------
;;; Memory Tracking
;;; ---------------------------------------------------------------------------

(defn memory-stats
  "Get current JVM memory statistics."
  []
  (let [runtime (Runtime/getRuntime)
        total (.totalMemory runtime)
        free (.freeMemory runtime)
        max-mem (.maxMemory runtime)]
    {:used-mb (/ (- total free) 1024.0 1024.0)
     :total-mb (/ total 1024.0 1024.0)
     :max-mb (/ max-mem 1024.0 1024.0)
     :free-mb (/ free 1024.0 1024.0)}))

(defn format-memory
  "Format memory stats as human-readable string."
  [{:keys [used-mb total-mb max-mb]}]
  (format "Used: %.0fMB / Total: %.0fMB / Max: %.0fMB" used-mb total-mb max-mb))

(defn gc!
  "Force garbage collection and return memory stats after GC."
  []
  (System/gc)
  (Thread/sleep 100)
  (memory-stats))

;;; ---------------------------------------------------------------------------
;;; IPC Caveat Warning
;;; ---------------------------------------------------------------------------

(def ^:private ipc-caveat-printed? (atom false))

(defn print-ipc-caveat!
  "Print a one-time warning about InProcessCluster limitations."
  []
  (when (compare-and-set! ipc-caveat-printed? false true)
    (println)
    (println "================================================================================")
    (println "NOTE: Running on InProcessCluster (IPC) - NOT a real Rama cluster")
    (println "--------------------------------------------------------------------------------")
    (println "IPC simulates distributed behavior in a single JVM. This means:")
    (println "  - All distributed overhead without true parallelism benefits")
    (println "  - Concentrated GC pressure (single JVM) causing p99 latency spikes")
    (println "  - Thread contention between simulated 'distributed' tasks")
    (println "")
    (println "Real Rama cluster performance should be significantly better:")
    (println "  - True parallelism across machines")
    (println "  - Independent GC per node (stable latencies)")
    (println "  - Linear horizontal scaling")
    (println "")
    (println "These results establish a performance FLOOR, not a ceiling.")
    (println "================================================================================")
    (println)))

;;; ---------------------------------------------------------------------------
;;; Module Setup Helpers
;;; ---------------------------------------------------------------------------

(defmacro with-bench-module
  "Setup IPC, launch RdfStorageModule, bind depot and common queries.

   Usage:
     (with-bench-module [ipc module-name depot q-plan :tasks 4 :threads 2]
       ;; benchmark code
       )"
  [[ipc module-name depot q-plan & {:keys [tasks threads]
                                    :or {tasks 4 threads 2}}] & body]
  `(do
     (print-ipc-caveat!)
     (with-open [~ipc (rtest/create-ipc)]
       (rtest/launch-module! ~ipc core/RdfStorageModule {:tasks ~tasks :threads ~threads})
       (let [~module-name (rama/get-module-name core/RdfStorageModule)
             ~depot (rama/foreign-depot ~ipc ~module-name "*triple-depot")
             ~q-plan (rama/foreign-query ~ipc ~module-name "execute-plan")]
         ~@body))))

(defmacro with-bench-module-full
  "Extended module setup with all query topologies bound.

   Usage:
     (with-bench-module-full [ipc module-name depot q-plan q-triples q-bgp :tasks 8]
       ;; benchmark code
       )"
  [[ipc module-name depot q-plan q-triples q-bgp & {:keys [tasks threads]
                                                    :or {tasks 4 threads 2}}] & body]
  `(with-bench-module [~ipc ~module-name ~depot ~q-plan :tasks ~tasks :threads ~threads]
     (let [~q-triples (rama/foreign-query ~ipc ~module-name "find-triples")
           ~q-bgp (rama/foreign-query ~ipc ~module-name "find-bgp")]
       ~@body)))

;;; ---------------------------------------------------------------------------
;;; Data Loading Helpers
;;; ---------------------------------------------------------------------------

(defn load-quads!
  "Load quads into depot (blocking). Returns {:count :time-ms :quads-per-sec}."
  [depot quads]
  (let [n (count quads)
        {:keys [ms]} (time-ms-only
                      (doseq [[s p o c] quads]
                        (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]])))]
    {:count n
     :time-ms ms
     :quads-per-sec (if (pos? ms) (* 1000.0 (/ n ms)) 0)}))

(defn load-quads-async!
  "Load quads into depot using async fire-and-forget appends.
   FASTEST method - does not wait for acknowledgment.
   Returns {:count :time-ms :quads-per-sec}"
  [depot quads]
  (let [n (count quads)
        {:keys [ms]} (time-ms-only
                      (doseq [[s p o c] quads]
                        ;; nil ack level = fire-and-forget
                        (rama/foreign-append-async! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]] nil)))]
    {:count n
     :time-ms ms
     :quads-per-sec (if (pos? ms) (* 1000.0 (/ n ms)) 0)}))

(defn wait-for-indexer!
  "Wait for indexer to process expected-count operations."
  [ipc module-name expected-count]
  (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" expected-count))

(defn load-and-wait!
  "Load quads using async fire-and-forget appends and wait for indexing.
   Much faster than sync loading (~10x on IPC) because appends don't block
   on individual acknowledgments."
  [ipc module-name depot quads]
  (let [stats (load-quads-async! depot quads)]
    (wait-for-indexer! ipc module-name (:count stats))
    stats))

;;; ---------------------------------------------------------------------------
;;; Reporting Helpers
;;; ---------------------------------------------------------------------------

(defn print-section
  "Print a section header."
  [title]
  (println)
  (println (str "=== " title " ===")))

(defn print-subsection
  "Print a subsection header."
  [title]
  (println (str "--- " title " ---")))

(defn print-result
  "Print a benchmark result line."
  [label stats]
  (println (str "  " label ": " (format-stats stats))))

(defn print-throughput
  "Print throughput result."
  [label count-items time-ms unit]
  (let [throughput (if (pos? time-ms) (* 1000.0 (/ count-items time-ms)) 0)]
    (println (format "  %s: %d %s in %.2fms (%.0f %s/sec)"
                     label count-items unit time-ms throughput unit))))

;;; ---------------------------------------------------------------------------
;;; Target Checking
;;; ---------------------------------------------------------------------------

(def latency-targets
  "Target latency values in ms."
  {:point-lookup {:p50 1.0 :p99 5.0}
   :subject-scan {:p50 5.0 :p99 20.0}
   :two-way-join {:p50 10.0 :p99 50.0}
   :complex-query {:p50 50.0 :p99 200.0}
   :order-by {:p50 100.0 :p99 500.0}
   :group-by {:p50 50.0 :p99 200.0}})

(def throughput-targets
  "Target throughput values."
  {:ingestion-quads-per-sec 10000
   :point-queries-per-sec 1000
   :complex-queries-per-sec 100})

(defn check-latency-target
  "Check if stats meet the latency target. Returns {:pass :target :actual :metric}."
  [stats target-key metric]
  (when-let [target (get-in latency-targets [target-key metric])]
    (let [actual (get stats metric)]
      {:pass (and actual (<= actual target))
       :target target
       :actual actual
       :metric metric})))

(defn report-target-check
  "Report whether a target was met."
  [label check-result]
  (when check-result
    (let [{:keys [pass target actual metric]} check-result
          status (if pass "PASS" "FAIL")]
      (println (format "  [%s] %s %s: %.2fms (target: %.2fms)"
                       status label (name metric) (or actual 0.0) target)))))
