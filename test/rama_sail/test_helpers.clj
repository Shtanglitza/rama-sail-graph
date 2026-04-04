(ns rama-sail.test-helpers
  "Common test utilities and macros for rama-sail tests.
   Provides fixtures for RDF module setup, data helpers, and assertion utilities."
  (:require [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :as core]
            [rama-sail.sail.adapter :as sail])
  (:import [org.eclipse.rdf4j.model Resource]
           [org.eclipse.rdf4j.repository.sail SailRepository]))

;;; ---------------------------------------------------------------------------
;;; Module Test Fixtures
;;; ---------------------------------------------------------------------------

(defmacro with-rdf-module
  "Launch RdfStorageModule and execute body with bindings for:
   - ipc: InProcessCluster
   - module-name: The launched module name
   - depot: Foreign depot for *triple-depot

   Options can be provided as :tasks and :threads (defaults: 4/2).

   Usage:
     (with-rdf-module [ipc module-name depot]
       ;; test code using ipc, module-name, depot
       )"
  [[ipc module-name depot & {:keys [tasks threads]
                             :or {tasks 4 threads 2}}] & body]
  `(with-open [~ipc (rtest/create-ipc)]
     (rtest/launch-module! ~ipc core/RdfStorageModule {:tasks ~tasks :threads ~threads})
     (let [~module-name (rama/get-module-name core/RdfStorageModule)
           ~depot (rama/foreign-depot ~ipc ~module-name "*triple-depot")]
       ~@body)))

(defmacro with-rdf-queries
  "Extend with-rdf-module to also bind common query topologies.

   Usage:
     (with-rdf-queries [ipc module-name depot q-plan q-triples q-bgp]
       ;; test code using all bindings
       )"
  [[ipc module-name depot q-plan q-triples q-bgp & {:keys [tasks threads]
                                                    :or {tasks 4 threads 2}}] & body]
  `(with-rdf-module [~ipc ~module-name ~depot :tasks ~tasks :threads ~threads]
     (let [~q-plan (rama/foreign-query ~ipc ~module-name "execute-plan")
           ~q-triples (rama/foreign-query ~ipc ~module-name "find-triples")
           ~q-bgp (rama/foreign-query ~ipc ~module-name "find-bgp")]
       ~@body)))

(defmacro with-sail-repository
  "Create a SAIL + Repository + Connection and execute body.
   Properly initializes, opens connection, and cleans up on exit.

   Usage:
     (with-sail-repository [repo conn] ipc module-name
       ;; test code using repo and conn
       )"
  [[repo conn] ipc module-name & body]
  `(let [sail# (sail/create-rama-sail ~ipc ~module-name {:sync-commits true})
         ~repo (SailRepository. sail#)]
     (.init ~repo)
     (try
       (let [~conn (.getConnection ~repo)]
         (try
           ~@body
           (finally (.close ~conn))))
       (finally (.shutDown ~repo)))))

(defmacro with-full-test-setup
  "Complete test setup: module + SAIL repository + connection.

   Usage:
     (with-full-test-setup [ipc module-name depot repo conn]
       ;; test code
       )"
  [[ipc module-name depot repo conn & {:keys [tasks threads]
                                       :or {tasks 4 threads 2}}] & body]
  `(with-rdf-module [~ipc ~module-name ~depot :tasks ~tasks :threads ~threads]
     (with-sail-repository [~repo ~conn] ~ipc ~module-name
       ~@body)))

;;; ---------------------------------------------------------------------------
;;; Data Helpers
;;; ---------------------------------------------------------------------------

(defn add-quad!
  "Add a single quad to the depot."
  [depot [s p o c]]
  (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]]))

(defn add-quads!
  "Add multiple quads to the depot. Quads are [s p o c] vectors.
   Context defaults to DEFAULT-CONTEXT-VAL if nil."
  [depot quads]
  (doseq [[s p o c] quads]
    (rama/foreign-append! depot [:add [s p o (or c core/DEFAULT-CONTEXT-VAL)]])))

(defn add-quads-and-wait!
  "Add multiple quads and wait for indexer to process them.
   Note: expected-count should be the total expected microbatch count including these quads."
  [ipc module-name depot quads expected-count]
  (add-quads! depot quads)
  (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" expected-count))

(defn wait-for-indexer!
  "Wait for the indexer to process the expected number of operations."
  [ipc module-name expected-count]
  (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" expected-count))

;;; ---------------------------------------------------------------------------
;;; Query Helpers
;;; ---------------------------------------------------------------------------

(defn execute-plan
  "Execute a query plan and return results."
  [q-plan plan]
  (rama/foreign-invoke-query q-plan plan))

(defn find-triples
  "Execute find-triples query with s/p/o/c pattern."
  [q-triples s p o c]
  (set (rama/foreign-invoke-query q-triples s p o c)))

(defn find-bgp
  "Execute find-bgp query with pattern map."
  [q-bgp pattern]
  (set (rama/foreign-invoke-query q-bgp pattern)))

;;; ---------------------------------------------------------------------------
;;; Statement Helpers (for SAIL layer tests)
;;; ---------------------------------------------------------------------------

(defn get-all-statements
  "Get all statements from a connection as a vector."
  [conn]
  (with-open [iter (.getStatements conn nil nil nil true (into-array Resource []))]
    (vec (iterator-seq iter))))

(defn statement->tuple
  "Convert an RDF4J Statement to a [s p o c] tuple of strings."
  [^org.eclipse.rdf4j.model.Statement stmt]
  [(.stringValue (.getSubject stmt))
   (.stringValue (.getPredicate stmt))
   (.stringValue (.getObject stmt))
   (when-let [ctx (.getContext stmt)] (.stringValue ctx))])

(defn statements->tuples
  "Convert a collection of statements to tuples."
  [statements]
  (mapv statement->tuple statements))

;;; ---------------------------------------------------------------------------
;;; Common Test Data
;;; ---------------------------------------------------------------------------

(def sample-quads
  "Sample quads for basic tests."
  [["<alice>" "<knows>" "<bob>" nil]
   ["<alice>" "<age>" "30" nil]
   ["<bob>" "<knows>" "<charlie>" nil]
   ["<charlie>" "<knows>" "<dave>" nil]])
