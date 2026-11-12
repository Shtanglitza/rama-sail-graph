(ns rama-sail.core
  "Rama module definition for RDF quad storage and SPARQL query evaluation.
   Assembles composable building blocks from rama-sail.module.* namespaces.

   To extend this module in another project, require the individual namespaces
   and compose your own defmodule:

     (ns my-project.module
       (:use [com.rpl.rama])
       (:require [rama-sail.module.indexer :as idx]
                 [rama-sail.module.namespaces :as ns-mod]
                 [rama-sail.module.queries :as queries]))

     (defmodule MyExtendedModule [setup topologies]
       (declare-rdf-depots setup)                         ;; from rama-sail.core
       (set-indexer-launch-options setup)                 ;; from rama-sail.core: ingest tuning
       (let [mb (microbatch-topology topologies \"indexer\")]
         (idx/declare-rdf-index-pstates mb)
         (declare-pstate mb $$my-custom-index {...})       ;; your additions
         (idx/rdf-indexer-sources mb))
       (ns-mod/setup-namespace-topology topologies)
       (ns-mod/register-namespace-query-topologies topologies)
       (queries/register-rdf-query-topologies topologies)
       (my-custom-query-topology topologies)               ;; your additions
       (my-execute-plan-topology topologies))               ;; your extended planner"
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [rama-sail.module.indexer :as idx]
            [rama-sail.module.namespaces :as ns-mod]
            [rama-sail.module.queries :as queries]
            [rama-sail.query.helpers :as qh]
            [rama-sail.sail.serialization :as ser]))

;; Re-export constants for backward compatibility with tests and external consumers
(def ^{:doc "Default context value for the default graph"} DEFAULT-CONTEXT-VAL ser/DEFAULT-CONTEXT-VAL)
(def ^{:doc "RDF type predicate IRI"} RDF-TYPE-PREDICATE qh/RDF-TYPE-PREDICATE)

(def ^{:doc "True for a *triple-depot record the indexer can apply; it drops anything else.
  Check records with this before appending them directly. See rama-sail.module.indexer/valid-depot-record?."}
  valid-depot-record? idx/valid-depot-record?)

(def ^:private COMMIT-LOG-KEY "rama-sail/commit-log")

(defn depot-partition-key
  "Routes a quad op by subject and a :clear-context by graph. Every :commit
   record goes to one partition, the commit log, so commits are queued and
   applied in the order they were appended; :config records go there too."
  [[op payload _tx-time-or-opts]]
  (case op
    :clear-context (nth payload 3)
    (:commit :config) COMMIT-LOG-KEY
    (first payload)))

(def indexer-launch-options
  "Rama dynamic options the module sets when it is first LAUNCHED. Profiling a
   bulk load showed the indexer mostly waiting on disk, not computing: Rama's
   defaults flush PState writes to RocksDB in small batches (40% of a load was
   spent in those writes and their fsyncs) and read at most 1000 records per
   depot partition per microbatch. Larger batches on both cut the cost per
   quad about fourfold.

   A module that is UPDATED keeps the options it was launched with; set these
   with Rama's dynamic-option command or UI on an existing deployment."
  {:depot {"depot.microbatch.max.records" 5000}
   :topology {"topology.microbatch.pstate.flush.path.count" 100000}})

(defn set-indexer-launch-options
  "Applies indexer-launch-options to `setup`. Call from within a defmodule body
   that declares the RDF depots and an \"indexer\" microbatch topology."
  [setup]
  (doseq [[option value] (:depot indexer-launch-options)]
    (set-launch-depot-dynamic-option!* setup "*triple-depot" option value))
  (doseq [[option value] (:topology indexer-launch-options)]
    (set-launch-topology-dynamic-option! setup "indexer" option value)))

(defn declare-rdf-depots
  "Declares the core RDF depots on `setup`. Call from within a defmodule body."
  [setup]
  (declare-depot setup *triple-depot (hash-by depot-partition-key))
  (declare-depot setup *namespace-depot (hash-by ns-mod/namespace-depot-partition-key)))

;; ============================================================================
;; Module Definition
;; ============================================================================

(defmodule RdfStorageModule [setup topologies]
  (declare-rdf-depots setup)
  (set-indexer-launch-options setup)
  (let [mb (microbatch-topology topologies "indexer")]
    (idx/declare-rdf-index-pstates mb)
    (idx/rdf-indexer-sources mb))
  (ns-mod/setup-namespace-topology topologies)
  (ns-mod/register-namespace-query-topologies topologies)
  (queries/register-rdf-query-topologies topologies)
  (queries/execute-plan-query-topology topologies))

(defn set-indexer-option!
  "Changes an indexer setting at runtime by appending a :config record to
   `depot` (the *triple-depot). Settings:

     :commit-ops-per-microbatch  - most queued commit ops applied per microbatch
     :clear-quads-per-microbatch - most quads deleted per task per microbatch
                                   for pending graph clears

   Higher values apply bulk loads and large clears faster. The work of one
   microbatch must stay well under Rama's microbatch phase timeout, or the
   microbatch is retried forever: raise them only on a cluster where you have
   measured how long a microbatch takes."
  [depot option value]
  {:pre [(idx/valid-depot-record? [:config {option value}])]}
  (foreign-append! depot [:config {option value}]))

