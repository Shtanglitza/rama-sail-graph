(ns rama-sail.bulk
  "Bulk loading: streams quads to the module as :commit depot records.

   A SAIL transaction keeps every statement in client memory until commit and
   nets its operations against each other. For loading a large file that is
   wasted work. This namespace appends quads in chunks, one `[:commit ...]`
   record per chunk (one round trip per 5,000 quads instead of one per quad),
   and keeps the indexer's commit queue short so memory stays bounded on both
   sides. The indexer applies the records in order, a bounded number of quads
   per microbatch (see rama-sail.module.indexer).

   Requires a module with :commit record support (the \"pending-commit-count\"
   query topology)."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.rpl.rama :as rama]
            [com.rpl.rama.path :refer [keypath]]
            [rama-sail.core :as core]
            [rama-sail.module.indexer :as idx]
            [rama-sail.sail.serialization :as ser])
  (:import (org.eclipse.rdf4j.model Statement)
           (org.eclipse.rdf4j.rio RDFFormat Rio)
           (org.eclipse.rdf4j.rio.helpers AbstractRDFHandler)))

(def default-chunk-size
  "Quads per :commit record."
  5000)

(def default-max-queued
  "Commit records allowed to wait in the indexer's queue before the loader
   pauses. Bounds the queue (it lives on one task) to about this many chunks,
   while leaving the indexer more than one microbatch of work."
  16)

(defn- pending-commits [pending-qt] (rama/foreign-invoke-query pending-qt))

(defn- sync-round!
  "Appends a marker to the commit log and waits until the indexer has
   processed it. Every :commit record appended before the marker has then been
   read (the commit log is one depot partition, read in order), and every
   microbatch before the one that read the marker has committed on all tasks.
   Returns false if the marker was not seen (another writer's marker replaced
   it, or the indexer is far behind); the caller tries again."
  [depot config-pstate]
  (let [token (inc (long (rand-int Integer/MAX_VALUE)))]
    (rama/foreign-append! depot [:config {:sync-token token}] :append-ack)
    (loop [tries 0]
      (cond
        (= token (rama/foreign-select-one (keypath :sync-token) config-pstate {:pkey 0})) true
        (> tries 200) false
        :else (do (Thread/sleep 25) (recur (inc tries)))))))

(defn await-applied!
  "Block until the indexer has applied every :commit record appended so far,
   and every graph clear. Unlike the pending-commit-count query alone, this
   also covers a record that was appended but that the indexer has not read
   yet. On a store with other writers it waits for their commits too. Throws
   after `timeout-ms` (default 1 hour)."
  ([cluster module-name] (await-applied! cluster module-name 3600000))
  ([cluster module-name timeout-ms]
   (let [depot (rama/foreign-depot cluster module-name "*triple-depot")
         config (rama/foreign-pstate cluster module-name "$$indexer-config")
         commits (rama/foreign-query cluster module-name "pending-commit-count")
         clears (rama/foreign-query cluster module-name "pending-clear-count")
         deadline (+ (System/currentTimeMillis) timeout-ms)
         idle? #(and (zero? (rama/foreign-invoke-query commits))
                     (zero? (rama/foreign-invoke-query clears)))]
     ;; Done when the queue is empty in two consecutive rounds: the first says
     ;; the last slice was handed out, the second that the microbatch which
     ;; applied it has committed on every task.
     (loop [idle-rounds 0]
       (when (> (System/currentTimeMillis) deadline)
         (throw (ex-info "Timed out waiting for the indexer to apply queued commits"
                         {:timeout-ms timeout-ms})))
       (let [seen? (sync-round! depot config)
             idle-rounds (if (and seen? (idle?)) (inc idle-rounds) 0)]
         (when (< idle-rounds 2)
           (when-not seen? (Thread/sleep 100))
           (recur idle-rounds)))))))

(defn loader
  "A bulk loader for `module-name` on `cluster` (a cluster manager or an
   in-process cluster). Returns a map for add-quad! and finish!.

   Options:
     :chunk-size - quads per :commit record (default 5000, at most the
                   indexer's max-commit-ops)
     :max-queued - commit records allowed to wait in the indexer's queue
                   before appending pauses (default 16)
     :tx-time    - creation stamp for every quad (default: now). A later
                   CLEAR of a graph removes quads stamped at or before it."
  [cluster module-name & {:keys [chunk-size max-queued tx-time]
                          :or {chunk-size default-chunk-size
                               max-queued default-max-queued}}]
  {:pre [(pos-int? chunk-size) (<= chunk-size idx/max-commit-ops) (pos-int? max-queued)]}
  {:depot (rama/foreign-depot cluster module-name "*triple-depot")
   :pending-qt (try (rama/foreign-query cluster module-name "pending-commit-count")
                    (catch Exception e
                      (throw (ex-info (str "Module " module-name " does not accept :commit records "
                                           "(no pending-commit-count query topology); update the module")
                                      {:module-name module-name} e))))
   :chunk-size chunk-size
   :max-queued max-queued
   :tx-time (or tx-time (System/currentTimeMillis))
   :buffer (volatile! (transient []))
   :count (volatile! 0)
   :started (System/nanoTime)})

(defn- flush-chunk! [{:keys [depot pending-qt max-queued buffer count]}]
  (let [ops (persistent! @buffer)]
    (vreset! buffer (transient []))
    (when (seq ops)
      ;; Backpressure: the queue is durable, but there is no point in parking
      ;; the whole file in it faster than the indexer drains it.
      (loop []
        (when (>= (pending-commits pending-qt) max-queued)
          (Thread/sleep 50)
          (recur)))
      (rama/foreign-append! depot [:commit ops] :append-ack)
      (vswap! count + (clojure.core/count ops)))))

(defn add-quad!
  "Queue one quad `[s p o c]` of canonical term strings (see
   rama-sail.sail.serialization/val->str; the default graph is
   core/DEFAULT-CONTEXT-VAL). Appends a :commit record when the chunk is full.
   Not thread-safe: use one loader per thread."
  [{:keys [buffer chunk-size tx-time] :as ldr} quad]
  (vswap! buffer conj! [:add quad tx-time])
  (when (>= (clojure.core/count @buffer) chunk-size)
    (flush-chunk! ldr)))

(defn finish!
  "Append what is left. Returns {:count :time-ms :triples-per-sec}: quads
   appended, and the time since the loader was created. The quads are durable
   when this returns; they are queryable once the indexer has applied them
   (await-applied!)."
  [{:keys [count started] :as ldr}]
  (flush-chunk! ldr)
  (let [ms (/ (- (System/nanoTime) started) 1e6)]
    {:count @count :time-ms ms :triples-per-sec (if (pos? ms) (/ @count (/ ms 1000.0)) 0.0)}))

(defn- run-load!
  "Runs `feed!` (which calls add-quad! on the loader), then finishes and
   optionally waits. With :commit-ops-per-microbatch, raises the indexer's
   apply budget for the duration of the load and restores the default after
   (which requires waiting for the load to be applied)."
  [cluster module-name {:keys [wait? commit-ops-per-microbatch] :or {wait? true} :as opts} feed!]
  (let [ldr (apply loader cluster module-name
                   (mapcat identity (select-keys opts [:chunk-size :max-queued :tx-time])))
        depot (:depot ldr)]
    (when commit-ops-per-microbatch
      (core/set-indexer-option! depot :commit-ops-per-microbatch commit-ops-per-microbatch))
    (try
      (feed! ldr)
      (let [stats (finish! ldr)]
        (when (or wait? commit-ops-per-microbatch) (await-applied! cluster module-name))
        (assoc stats :total-time-ms (/ (- (System/nanoTime) (:started ldr)) 1e6)))
      (finally
        (when commit-ops-per-microbatch
          (core/set-indexer-option! depot :commit-ops-per-microbatch idx/commit-ops-per-microbatch))))))

(defn load-quads!
  "Bulk-load a sequence of `[s p o c]` quads. Options as for `loader`, plus:
     :wait? - block until the indexer has applied everything (default true)
     :commit-ops-per-microbatch - raise the indexer's apply budget for this
       load (see core/set-indexer-option! for the risk) and restore the
       default afterwards. The setting is store-wide while the load runs.
   Returns {:count :time-ms :triples-per-sec :total-time-ms}."
  [cluster module-name quads & {:as opts}]
  (run-load! cluster module-name opts (fn [ldr] (doseq [q quads] (add-quad! ldr q)))))

(defn- rdf-format ^RDFFormat [file format]
  (or format
      (.orElse (Rio/getParserFormatForFileName (str file)) nil)
      ;; N-Triples is a subset of Turtle; the Turtle parser is always on the classpath
      RDFFormat/TURTLE))

(defn load-file!
  "Bulk-load an RDF file (N-Triples, Turtle, TriG, RDF/XML, ...), streaming:
   the file is never held in memory. Triples without a graph go to `:context`
   (default: the default graph). Options as for `load-quads!`, plus :format
   (an RDFFormat; default: from the file name) and :context (canonical graph
   string). Returns the load-quads! stats."
  [cluster module-name file & {:keys [format context] :or {context core/DEFAULT-CONTEXT-VAL} :as opts}]
  (let [fmt (rdf-format file format)
        parser (Rio/createParser fmt)]
    (log/info "Bulk loading" (str file) "as" (.getName fmt))
    (run-load! cluster module-name opts
               (fn [ldr]
                 (.setRDFHandler parser
                                 (proxy [AbstractRDFHandler] []
                                   (handleStatement [^Statement st]
                                     (add-quad! ldr [(ser/val->str (.getSubject st))
                                                     (ser/val->str (.getPredicate st))
                                                     (ser/val->str (.getObject st))
                                                     (if-let [c (.getContext st)] (ser/val->str c) context)]))))
                 (with-open [in (io/input-stream file)]
                   (.parse parser in ""))))))
