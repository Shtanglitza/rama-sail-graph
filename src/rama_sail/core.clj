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

(defn depot-partition-key [[op payload _tx-time-or-opts]]
  (case op
    :clear-context (nth payload 3)
    (first payload)))

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
  (let [mb (microbatch-topology topologies "indexer")]
    (idx/declare-rdf-index-pstates mb)
    (idx/rdf-indexer-sources mb))
  (ns-mod/setup-namespace-topology topologies)
  (ns-mod/register-namespace-query-topologies topologies)
  (queries/register-rdf-query-topologies topologies)
  (queries/execute-plan-query-topology topologies))
