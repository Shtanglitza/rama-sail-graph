(ns rama-sail.bench.bsbm.infra.ntriples-loader
  "N-Triples file parser and quad loader for BSBM benchmarks.

   Parses standard N-Triples format (.nt files) and loads them into Rama depot."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.bench.infra.bench-helpers :as bh])
  (:import [java.util.concurrent Executors ExecutorService TimeUnit CompletableFuture]))

;;; ---------------------------------------------------------------------------
;;; N-Triples Parser
;;; ---------------------------------------------------------------------------

(defn- parse-iri
  "Parse an IRI from N-Triples format: <uri> -> <uri>"
  [^String s]
  (when (and (str/starts-with? s "<") (str/ends-with? s ">"))
    s))

(defn- parse-bnode
  "Parse a blank node: _:id -> _:id"
  [^String s]
  (when (str/starts-with? s "_:")
    s))

(defn- find-literal-end
  "Find the end position of a literal string, handling escape sequences.
   Returns the index of the closing quote."
  [^String s start]
  (loop [i (inc start)]
    (when (< i (count s))
      (let [c (.charAt s i)]
        (cond
          (= c \\) (recur (+ i 2)) ; Skip escaped char
          (= c \") i
          :else (recur (inc i)))))))

(defn- parse-literal
  "Parse a literal from N-Triples format.
   Returns the literal in our internal format: \"value\", \"value\"@lang, or \"value\"^^<type>"
  [^String s]
  (when (str/starts-with? s "\"")
    (let [end-quote (find-literal-end s 0)]
      (when end-quote
        (let [label (subs s 0 (inc end-quote))
              suffix (subs s (inc end-quote))]
          (cond
            (str/blank? suffix) label
            (str/starts-with? suffix "@") (str label suffix)
            (str/starts-with? suffix "^^") (str label suffix)
            :else label))))))

(defn- parse-term
  "Parse an N-Triples term (IRI, blank node, or literal)."
  [^String s]
  (let [s (str/trim s)]
    (or (parse-iri s)
        (parse-bnode s)
        (parse-literal s))))

(defn- skip-whitespace
  "Skip whitespace characters and return new position."
  [^String s ^long pos]
  (loop [i pos]
    (if (and (< i (count s))
             (Character/isWhitespace (.charAt s i)))
      (recur (inc i))
      i)))

(defn- read-term
  "Read a term from position, return [term new-pos] or nil."
  [^String s ^long pos]
  (let [pos (skip-whitespace s pos)]
    (when (< pos (count s))
      (let [c (.charAt s pos)]
        (cond
          ;; IRI: <...>
          (= c \<)
          (let [end (str/index-of s ">" pos)]
            (when end
              [(subs s pos (inc end)) (inc end)]))

          ;; Blank node: _:...
          (and (= c \_) (< (inc pos) (count s)) (= (.charAt s (inc pos)) \:))
          (let [end (loop [i (+ pos 2)]
                      (if (and (< i (count s))
                               (let [c (.charAt s i)]
                                 (or (Character/isLetterOrDigit c)
                                     (= c \-)
                                     (= c \_))))
                        (recur (inc i))
                        i))]
            [(subs s pos end) end])

          ;; Literal: "..."
          (= c \")
          (let [label-end (find-literal-end s pos)]
            (when label-end
              (let [suffix-start (inc label-end)
                    ;; Check for language tag or datatype
                    suffix-end (cond
                                 ;; Language tag @...
                                 (and (< suffix-start (count s))
                                      (= (.charAt s suffix-start) \@))
                                 (loop [i (inc suffix-start)]
                                   (if (and (< i (count s))
                                            (let [c (.charAt s i)]
                                              (or (Character/isLetterOrDigit c)
                                                  (= c \-))))
                                     (recur (inc i))
                                     i))

                                 ;; Datatype ^^<...>
                                 (and (< (inc suffix-start) (count s))
                                      (= (.charAt s suffix-start) \^)
                                      (= (.charAt s (inc suffix-start)) \^))
                                 (let [dt-start (+ suffix-start 2)]
                                   (if (and (< dt-start (count s))
                                            (= (.charAt s dt-start) \<))
                                     (let [dt-end (str/index-of s ">" dt-start)]
                                       (when dt-end (inc dt-end)))
                                     suffix-start))

                                 :else suffix-start)]
                (when suffix-end
                  [(subs s pos suffix-end) suffix-end]))))

          :else nil)))))

(defn parse-ntriples-line
  "Parse an N-Triples line into [s p o] or nil for comments/blanks.

   N-Triples format: <subject> <predicate> <object> .

   Returns nil for:
   - Empty lines
   - Comment lines (starting with #)
   - Malformed lines"
  [^String line]
  (let [line (str/trim line)]
    (when (and (not (str/blank? line))
               (not (str/starts-with? line "#")))
      (let [[s pos1] (read-term line 0)]
        (when (and s pos1)
          (let [[p pos2] (read-term line pos1)]
            (when (and p pos2)
              (let [[o pos3] (read-term line pos2)]
                (when (and o pos3)
                  ;; Verify line ends with . (possibly with trailing whitespace)
                  (let [rest (str/trim (subs line pos3))]
                    (when (or (= rest ".")
                              (str/starts-with? rest "."))
                      [s p o])))))))))))

;;; ---------------------------------------------------------------------------
;;; Data Loading
;;; ---------------------------------------------------------------------------

(defn load-ntriples!
  "Load N-Triples from a file into a Rama depot.

   NOTE: This is the legacy sequential loader. For better performance,
   use load-ntriples-parallel! which uses concurrent appends.

   Options:
     :batch-size - Number of triples to buffer before waiting (default 1000)
     :context    - Context URI for all triples (default: DEFAULT-CONTEXT-VAL)

   Returns {:count :time-ms :triples-per-sec}"
  [depot file-path & {:keys [batch-size context]
                      :or {batch-size 1000
                           context core/DEFAULT-CONTEXT-VAL}}]
  (let [start (System/nanoTime)
        count-atom (atom 0)]
    (with-open [rdr (io/reader file-path)]
      (doseq [line (line-seq rdr)]
        (when-let [[s p o] (parse-ntriples-line line)]
          (rama/foreign-append! depot [:add [s p o context]])
          (swap! count-atom inc))))
    (let [elapsed-ns (- (System/nanoTime) start)
          elapsed-ms (/ elapsed-ns 1e6)
          count @count-atom
          rate (if (pos? elapsed-ms) (* 1000.0 (/ count elapsed-ms)) 0)]
      {:count count
       :time-ms elapsed-ms
       :triples-per-sec rate})))

(defn load-ntriples-parallel!
  "Load N-Triples from a file into a Rama depot using parallel appends.

   This is the high-performance loader that:
   1. Parses all triples first (fast, CPU-bound)
   2. Uses a thread pool to send multiple appends concurrently
   3. Significantly reduces total load time by overlapping RPC calls

   Options:
     :context     - Context URI for all triples (default: DEFAULT-CONTEXT-VAL)
     :parallelism - Number of concurrent append threads (default: 8)
     :chunk-size  - Triples per work unit (default: 100)

   Returns {:count :time-ms :triples-per-sec :parse-time-ms :append-time-ms}"
  [depot file-path & {:keys [context parallelism chunk-size]
                      :or {context core/DEFAULT-CONTEXT-VAL
                           parallelism 8
                           chunk-size 100}}]
  (let [start (System/nanoTime)
        ;; Phase 1: Parse all triples (fast, CPU-bound)
        parse-start (System/nanoTime)
        triples (with-open [rdr (io/reader file-path)]
                  (->> (line-seq rdr)
                       (keep parse-ntriples-line)
                       (mapv (fn [[s p o]] [:add [s p o context]]))))
        parse-end (System/nanoTime)
        parse-ms (/ (- parse-end parse-start) 1e6)
        total-count (count triples)

        ;; Phase 2: Parallel appends using thread pool
        append-start (System/nanoTime)
        ^ExecutorService executor (Executors/newFixedThreadPool parallelism)
        chunks (partition-all chunk-size triples)
        futures (doall
                 (for [chunk chunks]
                   (.submit executor
                            ^Callable
                            (fn []
                              (doseq [op chunk]
                                (rama/foreign-append! depot op))
                              (count chunk)))))]
    ;; Wait for all chunks to complete
    (doseq [fut futures]
      @fut)
    (.shutdown executor)
    (.awaitTermination executor 60 TimeUnit/SECONDS)

    (let [append-end (System/nanoTime)
          append-ms (/ (- append-end append-start) 1e6)
          elapsed-ns (- (System/nanoTime) start)
          elapsed-ms (/ elapsed-ns 1e6)
          rate (if (pos? elapsed-ms) (* 1000.0 (/ total-count elapsed-ms)) 0)]
      {:count total-count
       :time-ms elapsed-ms
       :triples-per-sec rate
       :parse-time-ms parse-ms
       :append-time-ms append-ms})))

(defn load-ntriples-async!
  "Load N-Triples using async fire-and-forget appends for maximum throughput.

   This is the FASTEST loader that:
   1. Parses all triples first (fast, CPU-bound)
   2. Sends all appends asynchronously with AckLevel/NONE (fire-and-forget)
   3. No waiting for individual append acknowledgments

   IMPORTANT: Data is not guaranteed to be indexed when this returns!
   You MUST call wait-for-indexer separately if you need to query the data.

   Options:
     :context     - Context URI for all triples (default: DEFAULT-CONTEXT-VAL)
     :parallelism - Number of concurrent sender threads (default: 4)
     :batch-size  - Triples per batch before yielding (default: 1000)

   Returns {:count :time-ms :triples-per-sec :parse-time-ms :append-time-ms}"
  [depot file-path & {:keys [context parallelism batch-size]
                      :or {context core/DEFAULT-CONTEXT-VAL
                           parallelism 4
                           batch-size 1000}}]
  (let [start (System/nanoTime)
        ;; Phase 1: Parse all triples (fast, CPU-bound)
        parse-start (System/nanoTime)
        triples (with-open [rdr (io/reader file-path)]
                  (->> (line-seq rdr)
                       (keep parse-ntriples-line)
                       (mapv (fn [[s p o]] [:add [s p o context]]))))
        parse-end (System/nanoTime)
        parse-ms (/ (- parse-end parse-start) 1e6)
        total-count (count triples)

        ;; Phase 2: Async fire-and-forget appends
        append-start (System/nanoTime)
        ^ExecutorService executor (Executors/newFixedThreadPool parallelism)
        chunks (partition-all batch-size triples)
        futures (doall
                 (for [chunk chunks]
                   (.submit executor
                            ^Callable
                            (fn []
                              (doseq [op chunk]
                                ;; Fire-and-forget: nil ack level = no waiting
                                (rama/foreign-append-async! depot op nil))
                              (count chunk)))))]
    ;; Wait for all chunks to be SENT (not indexed)
    (doseq [fut futures]
      @fut)
    (.shutdown executor)
    (.awaitTermination executor 60 TimeUnit/SECONDS)

    (let [append-end (System/nanoTime)
          append-ms (/ (- append-end append-start) 1e6)
          elapsed-ns (- (System/nanoTime) start)
          elapsed-ms (/ elapsed-ns 1e6)
          rate (if (pos? elapsed-ms) (* 1000.0 (/ total-count elapsed-ms)) 0)]
      {:count total-count
       :time-ms elapsed-ms
       :triples-per-sec rate
       :parse-time-ms parse-ms
       :append-time-ms append-ms})))

(defn load-ntriples-and-wait!
  "Load N-Triples from a file and wait for indexing to complete.

   This is the preferred method for tests/benchmarks that need to query
   immediately after loading.

   Returns {:count :time-ms :triples-per-sec}"
  [ipc module-name depot file-path & opts]
  (let [load-stats (apply load-ntriples! depot file-path opts)]
    (when (pos? (:count load-stats))
      (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" (:count load-stats)))
    load-stats))

(defn load-ntriples-with-connection!
  "Load N-Triples from a file using a cluster connection abstraction.

   Works with both IPC and real cluster connections via the ClusterConnection protocol.
   Uses the wait-for-indexer! protocol method which handles the difference between
   IPC (microbatch polling) and cluster (time-based delay) modes.

   Arguments:
   - conn: ClusterConnection (IPCConnection or RealClusterConnection)
   - depot: The triple depot to load into
   - file-path: Path to N-Triples file

   Options:
   - :context - Context URI for all triples (default: DEFAULT-CONTEXT-VAL)
   - :async - Use async fire-and-forget appends (default: true, FASTEST)
   - :parallel - Use parallel loader when not async (default: true)
   - :parallelism - Thread count for loading (default: 4 for async, 8 for parallel)

   Loading modes (fastest to slowest):
   1. :async true (default) - Fire-and-forget, maximum throughput
   2. :parallel true - Concurrent blocking appends
   3. :parallel false - Sequential blocking appends (legacy)

   Returns {:count :time-ms :triples-per-sec}"
  [conn depot file-path & {:keys [async parallel parallelism context]
                           :or {async true
                                parallel true
                                parallelism nil
                                context core/DEFAULT-CONTEXT-VAL}
                           :as opts}]
  (let [;; Dynamically require to avoid circular dependency
        wait-fn (requiring-resolve 'rama-sail.bench.infra.cluster-config/wait-for-indexer!)
        effective-parallelism (or parallelism (if async 4 8))
        load-stats (cond
                     async
                     (load-ntriples-async! depot file-path
                                           :context context
                                           :parallelism effective-parallelism)
                     parallel
                     (load-ntriples-parallel! depot file-path
                                              :context context
                                              :parallelism effective-parallelism)
                     :else
                     (load-ntriples! depot file-path :context context))]
    (when (pos? (:count load-stats))
      (wait-fn conn (:count load-stats)))
    load-stats))

(defn count-ntriples
  "Count the number of valid triples in an N-Triples file without loading.
   Useful for estimating load times."
  [file-path]
  (with-open [rdr (io/reader file-path)]
    (count (filter parse-ntriples-line (line-seq rdr)))))

;;; ---------------------------------------------------------------------------
;;; File Discovery
;;; ---------------------------------------------------------------------------

(defn find-ntriples-files
  "Find all .nt files in a directory."
  [dir-path]
  (->> (io/file dir-path)
       file-seq
       (filter #(str/ends-with? (.getName %) ".nt"))
       (map str)
       sort))
