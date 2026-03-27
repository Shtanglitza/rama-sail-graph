(ns rama-sail.sail.adapter
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [com.rpl.rama :as rama]
            [rama-sail.errors :as errors]
            [rama-sail.metrics :as metrics]
            [rama-sail.sail.compilation :as comp :refer [tuple-expr->plan value-expr->plan apply-initial-bindings compute-join-vars binding-names->vars]]
            [rama-sail.sail.optimization :as opt :refer [*predicate-stats* *global-stats* RDF-TYPE-PREDICATE is-variable? optimize-plan estimate-plan-cardinality extract-plan-vars extract-expr-vars get-plan-vars]]
            [rama-sail.sail.serialization :as ser :refer [val->str str->val DEFAULT-CONTEXT-VAL VF]])
  (:import (java.util Iterator)
           (org.eclipse.rdf4j.query QueryInterruptedException)
           (org.eclipse.rdf4j.query.algebra.evaluation EvaluationStrategy TripleSource)
           (org.eclipse.rdf4j.query.algebra.evaluation.federation FederatedServiceResolver)
           (org.eclipse.rdf4j.query.algebra.evaluation.impl DefaultEvaluationStrategyFactory)
           [org.eclipse.rdf4j.sail.helpers AbstractSail AbstractSailConnection]
           [org.eclipse.rdf4j.sail UpdateContext]
           [org.eclipse.rdf4j.model Value Resource Statement IRI Literal BNode ValueFactory Namespace]
           [org.eclipse.rdf4j.model.impl SimpleNamespace]
           [org.eclipse.rdf4j.query.algebra TupleExpr]
           [org.eclipse.rdf4j.model Triple]
           [org.eclipse.rdf4j.sail SailException]
           [org.eclipse.rdf4j.query BindingSet Dataset]
           [org.eclipse.rdf4j.common.iteration CloseableIteration CloseableIteratorIteration]))

;; Default query timeout: 60 seconds
(def DEFAULT-QUERY-TIMEOUT-MS 60000)

;; Global map of [module-name topology-name] -> microbatch counter atom
;; Used for sync-commits mode to track microbatch expectations across SAIL instances
;; Separate counters for each topology (indexer, ns-indexer) since they have independent counts
(defonce ^:private microbatch-counters (atom {}))

(defn- estimate-query-complexity
  "Estimate query complexity by counting operators in the plan.
   Returns a map with :joins, :bgps, :optionals counts for complexity assessment."
  [plan]
  (let [join-count (atom 0)
        bgp-count (atom 0)
        optional-count (atom 0)]
    (walk/prewalk
     (fn [node]
       (when (map? node)
         (case (:op node)
           :join (swap! join-count inc)
           :left-join (swap! optional-count inc)
           :bgp (swap! bgp-count inc)
           nil))
       node)
     plan)
    {:joins @join-count :bgps @bgp-count :optionals @optional-count}))

(defn- warn-complex-query
  "Log warning for potentially slow queries based on complexity and timeout.
   Warns if multi-join query has insufficient timeout."
  [plan timeout-ms]
  (let [{:keys [joins bgps optionals]} (estimate-query-complexity plan)
        total-joins (+ joins optionals)]
    (when (and (> total-joins 3) (< timeout-ms 30000))
      (log/warn "Complex query with" total-joins "joins and" timeout-ms "ms timeout. "
                "Consider increasing timeout for multi-join queries."))
    (when (> total-joins 5)
      (log/debug "Query complexity:" joins "inner joins," optionals "left joins," bgps "BGPs"))))

(defn invoke-query-with-timeout
  "Invoke a Rama query with timeout. Throws QueryInterruptedException on timeout.

   IMPORTANT: Query cancellation is best-effort only. On timeout:
   - Client receives QueryInterruptedException immediately
   - Rama cluster query may continue to completion
   - Resources on cluster are NOT released until query finishes

   For long-running queries, consider:
   - Using smaller result limits (LIMIT clause)
   - Breaking into multiple smaller queries
   - Increasing the timeout (via :query-timeout-ms option)

   Args:
     query-qt   - The Rama query topology handle
     timeout-ms - Maximum time to wait in milliseconds
     args       - Arguments to pass to the query topology

   Returns:
     Query results if completed within timeout

   Throws:
     QueryInterruptedException if timeout is reached"
  [query-qt timeout-ms & args]
  (let [start-time (System/currentTimeMillis)
        result-future (future (apply rama/foreign-invoke-query query-qt args))
        result (deref result-future timeout-ms ::timeout)]
    (if (= result ::timeout)
      (do
        ;; Attempt to cancel. Returns true if cancellation was requested successfully,
        ;; false if the task already completed or was already cancelled.
        (let [cancelled (future-cancel result-future)
              elapsed (- (System/currentTimeMillis) start-time)]
          (log/warn (str "Query timeout after " elapsed "ms (limit: " timeout-ms "ms). "
                         "Cancellation " (if cancelled "requested" "skipped (task already completed)") ". "
                         "Note: Rama cluster query may continue running. "
                         "Consider reducing result size or query complexity.")))
        (throw (QueryInterruptedException.
                (str "Query timeout after " timeout-ms "ms. "
                     "Consider reducing result size or increasing timeout."))))
      result)))

;;; -----------------------------------------------------------------------------
;;; 4. SAIL Connection Helpers
;;; -----------------------------------------------------------------------------

;; --- Error Handling ---

(defn- with-sail-error-handling
  "Execute a function with standardized SAIL error handling.
   On exception, logs a warning and returns default-val."
  [label default-val f]
  (try
    (f)
    (catch Exception e
      (log/warn label "failed:" (.getMessage e))
      default-val)))

(defn- array-empty?
  "Check if a Java array is nil or empty. Safe for use with contexts parameters."
  [^objects arr]
  (or (nil? arr) (zero? (alength arr))))

(defn- ->empty-iteration
  "Create an empty CloseableIteratorIteration."
  []
  (CloseableIteratorIteration. (java.util.Collections/emptyIterator)))

(defn- ->closeable-iteration
  "Create a CloseableIteratorIteration from a collection."
  [^java.util.Collection coll]
  (CloseableIteratorIteration. (.iterator coll)))

(defn- ->counted-closeable-iteration
  "Create CloseableIteration that counts results lazily during iteration.
   Avoids forcing full materialization of lazy sequences for logging purposes.
   The count is logged when the iteration is closed."
  [coll]
  (let [^Iterator iter (.iterator ^java.util.Collection coll)
        result-count (atom 0)
        closed? (atom false)]
    (reify org.eclipse.rdf4j.common.iteration.CloseableIteration
      (hasNext [_] (.hasNext iter))
      (next [_]
        (swap! result-count inc)
        (.next iter))
      (remove [_] (.remove iter))
      (close [_]
        (when (compare-and-set! closed? false true)
          (log/debug "Query returned" @result-count "results"))))))

;; --- Statement Pattern Matching ---

(defn- matches-removal-pattern?
  "Check if a pending add operation matches the removal pattern."
  [[op [qs qp qo qc]] s-str p-str o-str c-str]
  (and (= op :add)
       (or (nil? s-str) (= s-str qs))
       (or (nil? p-str) (= p-str qp))
       (or (nil? o-str) (= o-str qo))
       (or (nil? c-str) (= c-str qc))))

(defn- matches-query-pattern?
  "Check if a quad matches a query pattern (s/p/o/c, nils are wildcards)."
  [[qs qp qo qc] s-pat p-pat o-pat c-pat]
  (and (or (nil? s-pat) (= s-pat qs))
       (or (nil? p-pat) (= p-pat qp))
       (or (nil? o-pat) (= o-pat qo))
       (or (nil? c-pat) (= c-pat qc))))

;; --- Pending Operations Processing ---

(defn- compute-pending-net-state
  "Compute net effect of pending ops (order matters!).
   Returns {:adds #{quads} :dels #{quads} :cleared-contexts #{contexts}}

   Handles :clear-context by:
   1. Removing all pending adds for that context
   2. Adding context to cleared-contexts set (so committed statements can be filtered)"
  [pending-ops]
  (reduce (fn [state [op quad]]
            (case op
              :add (-> state
                       (update :adds conj quad)
                       (update :dels disj quad))
              :del (-> state
                       (update :adds disj quad)
                       (update :dels conj quad))
              :clear-context
              ;; quad is [nil nil nil ctx] - extract context
              (let [ctx (nth quad 3)]
                (-> state
                    ;; Remove all pending adds for this context
                    (update :adds (fn [adds]
                                    (into #{} (remove #(= (nth % 3) ctx) adds))))
                    ;; Track cleared context so we can filter committed statements
                    (update :cleared-contexts (fnil conj #{}) ctx)))
              state))
          {:adds #{} :dels #{} :cleared-contexts #{}}
          pending-ops))

(defn- find-matching-pending-adds
  "Find pending add operations matching a removal pattern."
  [pending-ops s-str p-str o-str c-str]
  (filter #(matches-removal-pattern? % s-str p-str o-str c-str) pending-ops))

;; --- Statement Conversion ---

(defn- quad->statement
  "Convert a quad tuple to an RDF4J Statement."
  [[qs qp qo qc]]
  (.createStatement VF
                    (str->val qs)
                    (str->val qp)
                    (str->val qo)
                    (when (and qc (not= qc DEFAULT-CONTEXT-VAL))
                      (str->val qc))))

(defn- statement->quad
  "Convert an RDF4J Statement to a quad tuple."
  [^org.eclipse.rdf4j.model.Statement st]
  [(val->str (.getSubject st))
   (val->str (.getPredicate st))
   (val->str (.getObject st))
   (if-let [ctx (.getContext st)]
     (val->str ctx)
     DEFAULT-CONTEXT-VAL)])

;; --- Query Execution ---

(defn- execute-bgp-query
  "Execute a BGP query against Rama and return statements."
  [query-qt timeout-ms s p o c-val s-val p-val o-val]
  (let [plan-c (if (nil? c-val) "?c" c-val)
        plan {:op :bgp
              :pattern {:s s-val :p p-val :o o-val :c plan-c}}
        rama-results (invoke-query-with-timeout query-qt timeout-ms plan)]
    (map (fn [binding]
           (let [rs (if s s (str->val (get binding "?s")))
                 rp (if p p (str->val (get binding "?p")))
                 ro (if o o (str->val (get binding "?o")))
                 rc (if (some? c-val)
                      (if (= c-val DEFAULT-CONTEXT-VAL) nil (str->val c-val))
                      (let [res-c (get binding "?c")]
                        (if (and res-c (not= res-c DEFAULT-CONTEXT-VAL))
                          (str->val res-c)
                          nil)))]
             (.createStatement VF rs rp ro rc)))
         rama-results)))

;; --- Result Merging ---

(defn- merge-pending-with-committed
  "Merge pending operations with committed statements, returning final statement list."
  [committed-statements pending-del-set pending-add-stmts]
  (let [;; Filter out deleted statements
        filtered-committed (remove #(pending-del-set (statement->quad %)) committed-statements)
        ;; Get committed quads to avoid duplicates
        committed-quad-set (set (map statement->quad filtered-committed))
        ;; Only add new pending statements
        new-pending-adds (remove #(committed-quad-set (statement->quad %)) pending-add-stmts)]
    (vec (concat filtered-committed new-pending-adds))))

;;; -----------------------------------------------------------------------------
;;; 5. SAIL Connection & Factory
;;; -----------------------------------------------------------------------------

(defn- create-connection [parent-sail ipc triple-depot query-qt list-contexts-qt count-statements-qt
                          ns-depot get-ns-qt list-ns-qt
                          get-all-stats-qt get-global-stats-qt
                          timeout-ms sync-config]
  ;; Transaction buffers - operations are buffered until commit
  ;; BNode map tracks BNode IDs to their unique skolemized IDs within a transaction
  ;; sync-config: {:enabled true :module-name "..." :microbatch-count (atom 0)}
  ;; Statistics are fetched lazily on first query and cached for connection lifetime
  (let [pending-ops (atom [])  ;; Vector of [:add quad] or [:del quad] or [:clear-context quad]
        pending-ns-ops (atom [])  ;; Vector of namespace operations [:set-ns prefix iri], [:remove-ns prefix], [:clear-ns]
        bnode-map (atom {})    ;; Maps original BNode IDs to skolemized IDs
        stats-cache (atom nil) ;; Cached {:data {...} :fetched-at millis}
        stats-lock (Object.)  ;; Lock for thread-safe stats initialization
        stats-ttl-ms 10000    ;; Stats cache TTL: 10 seconds
        _metrics-init (metrics/inc-connections) ;; Track connection count for observability
        fetch-stats! (fn []
                       ;; Fetch statistics with TTL-based cache invalidation
                       ;; Double-checked locking pattern to avoid unnecessary synchronization
                       (let [cached @stats-cache
                             now (System/currentTimeMillis)
                             fresh? (and cached (< (- now (:fetched-at cached)) stats-ttl-ms))]
                         (if fresh?
                           (:data cached)
                           (locking stats-lock
                             ;; Re-check inside lock in case another thread refreshed
                             ;; Recompute `now` to avoid using stale timestamp from before lock acquisition
                             (let [cached @stats-cache
                                   now (System/currentTimeMillis)
                                   fresh? (and cached (< (- now (:fetched-at cached)) stats-ttl-ms))]
                               (if fresh?
                                 (:data cached)
                                 (try
                                   (let [pred-stats (rama/foreign-invoke-query get-all-stats-qt)
                                         global-stats (rama/foreign-invoke-query get-global-stats-qt)
                                         stats {:predicate-stats (or pred-stats {})
                                                :global-stats (or global-stats {})}]
                                     (reset! stats-cache {:data stats :fetched-at now})
                                     stats)
                                   (catch Exception e
                                     (log/warn "Failed to fetch statistics for cardinality estimation:" (.getMessage e))
                                     (let [empty-stats {:predicate-stats {} :global-stats {}}]
                                       (reset! stats-cache {:data empty-stats :fetched-at now})
                                       empty-stats)))))))))]
    (proxy [AbstractSailConnection] [parent-sail]

      ;; Override addStatement to validate that contexts don't contain Triple instances.
      ;; The actual addStatementInternal is called later during flush/commit via
      ;; bulkAddStatementsInternal, so we must validate here at the public API level.
      (addStatement [^UpdateContext op ^Resource s ^IRI p ^Value o ^"[Lorg.eclipse.rdf4j.model.Resource;" contexts]
        ;; Validate: context cannot be a Triple (RDF-star embedded triples as context)
        (doseq [c contexts]
          (when (instance? Triple c)
            (throw (SailException.
                    (str "context argument can not be of type Triple: " c)))))
        ;; Call parent implementation
        (proxy-super addStatement op s p o contexts))

      (addStatementInternal [^Resource s ^IRI p ^Value o ^"[Lorg.eclipse.rdf4j.model.Resource;" contexts]
        ;; RDF4J semantics: If contexts array is empty, add to Default Graph (nil).
        ;; If not empty, add statement to EVERY context specified.
        ;; Operations are buffered until commit.
        ;; BNode IDs are skolemized to ensure uniqueness across transactions.
        (if (array-empty? contexts)
          (let [quad (ser/skolemized-quad->tuple s p o nil bnode-map)]
            (log/trace "Buffering add to default graph:" (pr-str quad))
            (swap! pending-ops conj [:add quad]))
          (doseq [c contexts]
            (let [quad (ser/skolemized-quad->tuple s p o c bnode-map)]
              (log/trace "Buffering add to context" (when c (val->str c)) ":" (pr-str quad))
              (swap! pending-ops conj [:add quad])))))

      (removeStatementsInternal [^Resource s ^IRI p ^Value o ^objects contexts]
        (let [s-str (when s (val->str s))
              p-str (when p (val->str p))
              o-str (when o (val->str o))
              ;; Helper: buffer pending add deletions for a given context pattern
              buffer-pending-deletions! (fn [c-str]
                                          (doseq [[_ quad] (find-matching-pending-adds @pending-ops s-str p-str o-str c-str)]
                                            (log/trace "Buffering remove for pending add:" (pr-str quad))
                                            (swap! pending-ops conj [:del quad])))
              ;; Helper: buffer committed statement deletions from iterator
              buffer-committed-deletions! (fn [^CloseableIteration iter]
                                            (try
                                              (while (.hasNext iter)
                                                (let [^Statement st (.next iter)
                                                      quad (ser/quad->tuple (.getSubject st) (.getPredicate st)
                                                                            (.getObject st) (.getContext st))]
                                                  (log/trace "Buffering remove:" (pr-str quad))
                                                  (swap! pending-ops conj [:del quad])))
                                              (finally (.close iter))))]
          (if (array-empty? contexts)
            ;; Wildcard context removal: Remove (s, p, o) from *all* graphs.
            (do
              (log/trace "Buffering remove with wildcard context")
              (buffer-pending-deletions! nil)
              (buffer-committed-deletions! (.getStatements ^AbstractSailConnection this s p o false (into-array Resource []))))
            ;; Specific contexts removal
            (if (and s p o)
              ;; All components specified - can buffer direct delete
              (doseq [c contexts]
                (let [quad (ser/quad->tuple s p o c)]
                  (log/trace "Buffering remove from context" (val->str c) ":" (pr-str quad))
                  (swap! pending-ops conj [:del quad])))
              ;; Some wildcards - must query to find matching statements for each context
              (doseq [c contexts]
                (log/trace "Removing with wildcards from context" (when c (val->str c)))
                (buffer-pending-deletions! (when c (val->str c)))
                (buffer-committed-deletions! (.getStatements ^AbstractSailConnection this s p o false (into-array Resource [c]))))))))

      (evaluateInternal [^TupleExpr tuple-expr ^Dataset dataset ^BindingSet bindings include-inferred]
        ;; Helper to use RDF4J fallback strategy (goes through getStatementsInternal)
        (letfn [(use-fallback-strategy []
                  (let [connection-ref this
                        triple-source (reify TripleSource
                                        (getStatements [_ s p o contexts]
                                          (.getStatements ^AbstractSailConnection connection-ref s p o include-inferred contexts))
                                        (getValueFactory [_] VF))
                        service-resolver (reify FederatedServiceResolver
                                           (getService [_ serviceUrl]
                                             (throw (UnsupportedOperationException. "SERVICE not supported"))))
                        ^DefaultEvaluationStrategyFactory factory (DefaultEvaluationStrategyFactory. service-resolver)
                        ^EvaluationStrategy strategy (.createEvaluationStrategy factory dataset triple-source nil)]
                    (-> strategy
                        (.precompile tuple-expr)
                        (.evaluate bindings))))]
          ;; If there are pending ops, use fallback strategy to ensure read-your-own-writes
          ;; This goes through getStatementsInternal which properly handles pending ops
          (if (seq @pending-ops)
            (do
              (log/debug "Using fallback strategy due to pending ops")
              (metrics/with-fallback-timing
                (use-fallback-strategy)))
            ;; Wrap query evaluation with comprehensive error handling and metrics
            (errors/with-rama-error-handling "query-evaluation"
              (metrics/with-query-timing
                (try
                  ;; Fetch statistics for cardinality estimation
                  (let [stats (fetch-stats!)
                        ;; Bind statistics for use in plan optimization
                        plan (binding [*predicate-stats* (:predicate-stats stats)
                                       *global-stats* (:global-stats stats)]
                               (let [plan (tuple-expr->plan tuple-expr)
                                     ;; If initial bindings provided, substitute constants in plan
                                     plan-with-bindings (if (.isEmpty bindings)
                                                          plan
                                                          (apply-initial-bindings plan bindings))
                                     ;; Apply optimizations: filter pushdown and join reordering
                                     optimized-plan (optimize-plan plan-with-bindings)]
                                 optimized-plan))]
                    ;; Warn about complex queries with insufficient timeout
                    (warn-complex-query plan timeout-ms)
                    (log/debug "Executing optimized Rama plan:" (pr-str plan))
                    (let [rama-results (invoke-query-with-timeout query-qt timeout-ms plan)
                          binding-sets (map ser/rama-result->binding-set rama-results)]
                      ;; Use lazy counting to avoid OOM on large result sets
                      ;; Count is logged when iteration is closed
                      (->counted-closeable-iteration binding-sets)))

                  (catch UnsupportedOperationException e
                    ;; Fallback to RDF4J standard strategy for unsupported operators
                    (log/warn "Falling back to RDF4J evaluation strategy -" (.getMessage e))
                    (log/debug "Unsupported tuple expression:" (.getSimpleName (class tuple-expr)))
                    (metrics/inc-query-count "fallback")
                    (use-fallback-strategy))))))))

      (hasStatementInternal [^Resource s ^IRI p ^Value o include-inferred ^objects contexts]
        (let [s-str (when s (val->str s))
              p-str (when p (val->str p))
              o-str (when o (val->str o))
              contexts-empty? (array-empty? contexts)

              ;; Context strings for pattern matching against pending ops (deduplicated)
              ctx-strs (if contexts-empty?
                         [nil]  ;; wildcard - match any context
                         (distinct (map #(if % (val->str %) DEFAULT-CONTEXT-VAL) contexts)))

              ;; Compute net effect of pending ops (adds, dels, cleared-contexts)
              {:keys [adds dels cleared-contexts]} (compute-pending-net-state @pending-ops)

              ;; Check if any pending add matches the pattern
              pending-match? (some (fn [quad]
                                     (some #(matches-query-pattern? quad s-str p-str o-str %) ctx-strs))
                                   adds)]
          (if pending-match?
            true
            ;; No pending add matches — check committed state via Rama,
            ;; but exclude pending deletes and cleared contexts
            (let [s-val (if s (val->str s) "?s")
                  p-val (if p (val->str p) "?p")
                  o-val (if o (val->str o) "?o")
                  check-ctxs (if contexts-empty? [nil] (seq contexts))]
              (boolean
               (some (fn [c]
                       (let [actual-c-val (if (and contexts-empty? (nil? c))
                                            nil
                                            (if c (val->str c) DEFAULT-CONTEXT-VAL))]
                          ;; Skip entirely if this context was cleared
                         (when-not (and actual-c-val (contains? cleared-contexts actual-c-val))
                           (let [plan {:op :ask
                                       :sub-plan {:op :bgp
                                                  :pattern {:s s-val :p p-val :o o-val :c actual-c-val}}}
                                 results (invoke-query-with-timeout query-qt timeout-ms plan)]
                              ;; Filter out results that are in the pending delete set
                             (when (seq results)
                               (if (and s p o)
                                  ;; Fully bound — check if this exact quad is pending-deleted
                                 (let [c-val-for-del (or actual-c-val DEFAULT-CONTEXT-VAL)]
                                   (not (contains? dels [(val->str s) (val->str p) (val->str o) c-val-for-del])))
                                  ;; Partially bound — at least one result exists that isn't deleted
                                  ;; For wildcards we'd need to check each result; use getStatements as fallback
                                 (let [stmts (execute-bgp-query query-qt timeout-ms s p o actual-c-val s-val p-val o-val)
                                       live-stmts (remove #(contains? dels (statement->quad %)) stmts)]
                                   (seq live-stmts))))))))
                     check-ctxs))))))

      (getStatementsInternal [^Resource s ^IRI p ^Value o include-inferred ^objects contexts]
        (let [s-val (if s (val->str s) "?s")
              p-val (if p (val->str p) "?p")
              o-val (if o (val->str o) "?o")
              s-str (when s (val->str s))
              p-str (when p (val->str p))
              o-str (when o (val->str o))

              ;; Get context strings for matching (deduplicated to avoid duplicate results)
              ctx-strs (if (array-empty? contexts)
                         [nil]  ;; wildcard - query all contexts
                         (distinct (map #(if % (val->str %) DEFAULT-CONTEXT-VAL) contexts)))

              ;; Compute net effect of pending ops (including cleared contexts)
              {:keys [adds dels cleared-contexts]} (compute-pending-net-state @pending-ops)

              ;; Get net adds that match the query pattern
              pending-add-stmts (for [quad adds
                                      ctx-str ctx-strs
                                      :when (matches-query-pattern? quad s-str p-str o-str ctx-str)]
                                  (quad->statement quad))

              ;; Query committed statements from Rama
              committed-statements (if (array-empty? contexts)
                                     (execute-bgp-query query-qt timeout-ms s p o nil s-val p-val o-val)
                                     (mapcat (fn [c]
                                               (execute-bgp-query query-qt timeout-ms s p o
                                                                  (if c (val->str c) DEFAULT-CONTEXT-VAL)
                                                                  s-val p-val o-val))
                                             contexts))

              ;; Filter out committed statements from cleared contexts
              filtered-committed (if (empty? cleared-contexts)
                                   committed-statements
                                   (remove (fn [^org.eclipse.rdf4j.model.Statement st]
                                             (let [ctx (.getContext st)
                                                   ctx-str (if ctx (val->str ctx) DEFAULT-CONTEXT-VAL)]
                                               (contains? cleared-contexts ctx-str)))
                                           committed-statements))

              ;; Merge pending ops with committed statements
              all-statements (merge-pending-with-committed filtered-committed dels pending-add-stmts)]
          (->closeable-iteration all-statements)))

      (closeInternal []
        (metrics/dec-connections))
      (startTransactionInternal []
        (log/debug "Starting transaction - clearing buffers")
        (reset! pending-ops [])
        (reset! pending-ns-ops [])
        (reset! bnode-map {}))  ;; Fresh BNode mappings for each transaction
      (commitInternal []
        (let [ops @pending-ops
              ns-ops @pending-ns-ops
              op-count (count ops)
              ns-op-count (count ns-ops)
              committed-triple-ops (atom 0)
              committed-ns-ops (atom 0)
              ;; Capture transaction time once for all operations in this commit
              ;; This ensures atomicity: all ops in a transaction share the same timestamp
              tx-time (System/currentTimeMillis)]
          (log/debug "Committing transaction with" op-count "triple ops and" ns-op-count "namespace ops at tx-time" tx-time)

          ;; Commit triple operations with tracking for partial failure diagnosis
          ;; Each operation gets an incrementing tx-time to preserve ordering within
          ;; the transaction. This ensures "last write wins" semantics when the same
          ;; quad is added and deleted in the same transaction.
          (when (pos? op-count)
            (errors/with-transaction-error-handling "triple-commit" committed-triple-ops op-count
              (doseq [[idx [op-type quad]] (map-indexed vector ops)]
                ;; Append tx-time as third element with per-op offset for ordering
                (rama/foreign-append! triple-depot [op-type quad (+ tx-time idx)])
                (swap! committed-triple-ops inc))))
          ;; In sync mode, ALWAYS wait for microbatch processing to complete.
          ;; Even with 0 triple ops, we must sync to ensure any pending microbatches
          ;; from previous commits (e.g., other SAIL instances) are fully visible.
          ;; Without this barrier, list-contexts may see stale PState data.
          (when (:enabled sync-config)
            (let [counter (:indexer-count sync-config)
                  module-name (:module-name sync-config)
                  new-count (swap! counter + op-count)]
              (let [wait-fn (requiring-resolve 'com.rpl.rama.test/wait-for-microbatch-processed-count)]
                (wait-fn ipc module-name "indexer" new-count))))

          ;; Commit namespace operations with tracking
          (when (pos? ns-op-count)
            (errors/with-transaction-error-handling "namespace-commit" committed-ns-ops ns-op-count
              (doseq [op ns-ops]
                (rama/foreign-append! ns-depot op)
                (swap! committed-ns-ops inc))))
          ;; In sync mode, ALWAYS wait for ns-indexer too (same reasoning as indexer above)
          (when (and (:enabled sync-config) (pos? ns-op-count))
            (let [counter (:ns-indexer-count sync-config)
                  module-name (:module-name sync-config)
                  new-count (swap! counter + ns-op-count)]
              (log/debug "Sync mode: waiting for ns-indexer microbatch count" new-count "(+" ns-op-count "ops)")
              (let [wait-fn (requiring-resolve 'com.rpl.rama.test/wait-for-microbatch-processed-count)]
                (wait-fn ipc module-name "ns-indexer" new-count))))

          ;; Only clear buffers after successful commit
          (reset! pending-ops [])
          (reset! pending-ns-ops [])
          (reset! bnode-map {})

          ;; Record transaction metrics
          (metrics/inc-transaction-count "commit")
          (let [add-count (count (filter #(= :add (first %)) ops))
                del-count (count (filter #(= :del (first %)) ops))]
            (when (pos? add-count) (metrics/record-transaction-ops :add add-count))
            (when (pos? del-count) (metrics/record-transaction-ops :del del-count)))

          (log/debug "Transaction committed:" @committed-triple-ops "triple ops," @committed-ns-ops "namespace ops")))
      (rollbackInternal []
        (let [op-count (count @pending-ops)
              ns-op-count (count @pending-ns-ops)]
          (log/debug "Rolling back transaction - discarding" op-count "triple ops and" ns-op-count "namespace ops")
          (metrics/inc-transaction-count "rollback")
          (reset! pending-ops [])
          (reset! pending-ns-ops [])
          (reset! bnode-map {})))
      (sizeInternal [^objects contexts]
        ;; Return the number of statements in the specified contexts
        ;; Optimization: Use count topology when no pending ops (avoids full scan)
        (with-sail-error-handling "sizeInternal" -1
          #(cond
             ;; Fast path 1: All contexts + no pending ops
             (and (array-empty? contexts) (empty? @pending-ops))
             (or (invoke-query-with-timeout count-statements-qt timeout-ms nil) 0)

             ;; Fast path 2: Specific contexts + no pending ops
             ;; Sum counts for each context using the count topology
             ;; CRITICAL: Deduplicate contexts to avoid double-counting if caller passes duplicates
             (and (not (array-empty? contexts)) (empty? @pending-ops))
             (let [unique-ctx-strs (distinct (map (fn [ctx] (if ctx (val->str ctx) DEFAULT-CONTEXT-VAL)) contexts))]
               (reduce + 0
                       (for [ctx-str unique-ctx-strs]
                         (or (invoke-query-with-timeout count-statements-qt timeout-ms ctx-str) 0))))

             ;; Slow path: pending ops require iteration to properly merge
             ;; pending state with committed data
             :else
             (let [^CloseableIteration iter (.getStatements ^AbstractSailConnection this nil nil nil false contexts)]
               (try
                 (loop [cnt 0]
                   (if (.hasNext iter)
                     (do (.next iter) (recur (inc cnt)))
                     cnt))
                 (finally (.close iter)))))))
      (getNamespacesInternal []
        ;; Return an iteration over all namespace prefix->IRI mappings
        ;; Must include both committed namespaces AND pending (uncommitted) operations
        (with-sail-error-handling "getNamespacesInternal" (->empty-iteration)
          #(let [;; Get committed namespaces from Rama
                 committed-ns (or (invoke-query-with-timeout list-ns-qt timeout-ms) {})
                 ;; Apply pending operations to get effective namespace state
                 ;; Process in order: set overwrites, remove deletes, clear removes all
                 effective-ns (reduce
                               (fn [ns-map [op & args]]
                                 (case op
                                   :set-ns (let [[prefix iri] args]
                                             (assoc ns-map prefix iri))
                                   :remove-ns (let [[prefix] args]
                                                (dissoc ns-map prefix))
                                   :clear-ns {}
                                   ns-map))
                               committed-ns
                               @pending-ns-ops)
                 ;; Convert to Namespace objects
                 namespaces (map (fn [[prefix iri]]
                                   (SimpleNamespace. prefix iri))
                                 effective-ns)]
             (->closeable-iteration namespaces))))

      (getNamespaceInternal [^String prefix]
        ;; Return the namespace IRI for the given prefix, or nil if not found
        ;; Must consider pending operations
        (with-sail-error-handling (str "getNamespaceInternal(" prefix ")") nil
          #(let [;; Check pending operations first (most recent wins)
                 pending-result (reduce
                                 (fn [_result [op & args]]
                                   (case op
                                     :set-ns (let [[p iri] args]
                                               (if (= p prefix)
                                                 (reduced iri)
                                                 nil))
                                     :remove-ns (let [[p] args]
                                                  (if (= p prefix)
                                                    (reduced ::removed)
                                                    nil))
                                     :clear-ns (reduced ::removed)
                                     nil))
                                 nil
                                 (reverse @pending-ns-ops))]  ;; reverse to get most recent first
             (cond
               (= pending-result ::removed) nil
               (some? pending-result) pending-result
               :else (invoke-query-with-timeout get-ns-qt timeout-ms prefix)))))

      (setNamespaceInternal [^String prefix ^String name]
        ;; Buffer the namespace set operation until commit
        (log/trace "Buffering set namespace:" prefix "->" name)
        (swap! pending-ns-ops conj [:set-ns prefix name]))

      (removeNamespaceInternal [^String prefix]
        ;; Buffer the namespace remove operation until commit
        (log/trace "Buffering remove namespace:" prefix)
        (swap! pending-ns-ops conj [:remove-ns prefix]))

      (clearNamespacesInternal []
        ;; Clear all namespaces by removing each one individually
        ;; This ensures all partitions are cleared (since clear-ns only goes to one task)
        (log/trace "Clearing all namespaces")
        (with-sail-error-handling "clearNamespacesInternal" nil
          #(let [^CloseableIteration ns-iter (.getNamespaces ^AbstractSailConnection this)]
             (try
               (while (.hasNext ns-iter)
                 (let [^Namespace ns (.next ns-iter)]
                   (swap! pending-ns-ops conj [:remove-ns (.getPrefix ns)])))
               (finally (.close ns-iter))))))

      (getContextIDsInternal []
        ;; Return an iteration over all context IDs (named graphs) in the store
        ;; Excludes the default graph (which has no explicit context ID in RDF4J)
        ;; Must include both committed contexts AND pending (uncommitted) operations
        (with-sail-error-handling "getContextIDsInternal" (->empty-iteration)
          #(let [;; Get committed contexts from Rama
                 committed-contexts (or (invoke-query-with-timeout list-contexts-qt timeout-ms) #{})
                 ;; Use compute-pending-net-state for consistent net-visible model.
                 {:keys [adds dels cleared-contexts]} (compute-pending-net-state @pending-ops)
                 ;; Group net-visible adds/dels by context
                 contexts-with-adds (into #{} (map (fn [[_s _p _o ctx]] ctx) adds))
                 committed-set (set committed-contexts)
                 ;; For committed contexts with pending dels, check if ALL quads are deleted.
                 ;; If a context has pending dels, verify at least one committed quad survives.
                 verified-committed
                 (if (empty? dels)
                   committed-set
                   (into #{}
                         (filter
                          (fn [ctx]
                            (if (contains? contexts-with-adds ctx)
                              true
                              (let [plan {:op :bgp :pattern {:s "?s" :p "?p" :o "?o" :c ctx}}
                                    committed-quads (invoke-query-with-timeout query-qt timeout-ms plan)]
                                (some (fn [binding]
                                        (not (contains? dels [(get binding "?s") (get binding "?p")
                                                              (get binding "?o") ctx])))
                                      committed-quads)))))
                         committed-set))
                 ;; Build result: committed contexts (minus cleared) + pending-add contexts
                 result-contexts (-> verified-committed
                                     ;; Remove fully cleared contexts (unless they have new adds)
                                     (as-> s (reduce (fn [acc ctx]
                                                       (if (and (contains? cleared-contexts ctx)
                                                                (not (contains? contexts-with-adds ctx)))
                                                         (disj acc ctx)
                                                         acc))
                                                     s cleared-contexts))
                                     ;; Add contexts that have pending adds
                                     (into contexts-with-adds))
                 ;; Exclude default context
                 final-contexts (disj result-contexts DEFAULT-CONTEXT-VAL)
                 ;; Convert to RDF4J Resources
                 resources (->> final-contexts
                                (map str->val)
                                (filter some?))]
             (->closeable-iteration resources))))
      (clearInternal [^objects contexts]
        (let [contexts-to-clear (if (array-empty? contexts)
                                  ;; No contexts specified - clear ALL contexts
                                  ;; CRITICAL: Must include both committed AND pending-only contexts.
                                  ;; A pending-only context is one created in this transaction (has adds
                                  ;; but not yet committed). If we only clear committed contexts, pending
                                  ;; data in new contexts would survive the clear() call.
                                  ;; Use compute-pending-net-state for NET-VISIBLE adds (not raw :add ops)
                                  ;; to avoid queueing unnecessary clears for canceled adds.
                                  (do
                                    (log/info "Buffering clear for all contexts")
                                    (let [committed (or (invoke-query-with-timeout list-contexts-qt timeout-ms) #{})
                                          committed-set (set committed)
                                          ;; Use net-visible adds from compute-pending-net-state
                                          {:keys [adds]} (compute-pending-net-state @pending-ops)
                                          ;; Extract pending-only contexts: those with net-visible adds not in committed
                                          pending-only (->> adds
                                                            (map (fn [[_s _p _o ctx]] ctx))
                                                            (remove (fn [ctx] (contains? committed-set ctx)))
                                                            set)]
                                      (set/union committed-set pending-only)))
                                  ;; Specific contexts - convert to canonical strings
                                  (do
                                    (log/info "Buffering clear for specific contexts:" (pr-str (seq contexts)))
                                    (set (map (fn [ctx] (if ctx (val->str ctx) DEFAULT-CONTEXT-VAL)) contexts))))]
          (doseq [ctx contexts-to-clear]
            (log/debug "Buffering clear-context:" ctx)
            (swap! pending-ops conj [:clear-context [nil nil nil ctx]])))))))

(defn create-rama-sail
  "Create a RamaSail backed by a Rama module.
   Options:
     :query-timeout-ms - Query timeout in milliseconds (default: 60000)
     :sync-commits     - If true, wait for microbatch processing after each commit.
                         Use this in tests to ensure data is immediately visible.
                         Requires com.rpl.rama.test namespace to be available."
  ([ipc module-name]
   (create-rama-sail ipc module-name {}))
  ([ipc module-name {:keys [query-timeout-ms sync-commits]
                     :or {query-timeout-ms DEFAULT-QUERY-TIMEOUT-MS
                          sync-commits false}}]
   (log/info "Creating RamaSail for module:" module-name "with timeout:" query-timeout-ms "ms"
             (if sync-commits " (sync-commits enabled)" " (sync-commits disabled)"))
   (let [depot               (rama/foreign-depot ipc module-name "*triple-depot")
         qt                  (rama/foreign-query ipc module-name "execute-plan")
         list-contexts-qt    (rama/foreign-query ipc module-name "list-contexts")
         count-statements-qt (rama/foreign-query ipc module-name "count-statements")
         ;; Statistics queries for cardinality estimation
         get-all-stats-qt    (rama/foreign-query ipc module-name "get-all-predicate-stats")
         get-global-stats-qt (rama/foreign-query ipc module-name "get-global-stats")
         ;; Namespace operations
         ns-depot            (rama/foreign-depot ipc module-name "*namespace-depot")
         get-ns-qt           (rama/foreign-query ipc module-name "get-namespace")
         list-ns-qt          (rama/foreign-query ipc module-name "list-namespaces")
         ;; Sync config for synchronous commits in test mode
         ;; Use separate global counters per [module-name topology-name] to persist across SAIL instances
         ;; Each topology (indexer, ns-indexer) has its own independent microbatch count
         sync-config (when sync-commits
                       (let [get-or-create-counter (fn [topo-name]
                                                     (let [key [module-name topo-name]]
                                                       (get (swap! microbatch-counters
                                                                   update key
                                                                   (fn [existing] (or existing (atom 0))))
                                                            key)))]
                         {:enabled true
                          :module-name module-name
                          :indexer-count (get-or-create-counter "indexer")
                          :ns-indexer-count (get-or-create-counter "ns-indexer")}))]
     (proxy [AbstractSail] []
       (getConnectionInternal []
         (log/debug "Opening new SAIL connection")
         (create-connection this ipc depot qt list-contexts-qt count-statements-qt
                            ns-depot get-ns-qt list-ns-qt
                            get-all-stats-qt get-global-stats-qt
                            query-timeout-ms sync-config))
       (isWritable [] true)
       (getValueFactory [] VF)
       (shutDownInternal []
         (log/info "Shutting down RamaSail")
         nil)))))