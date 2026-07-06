(ns rama-sail.module.namespaces
  "Composable building blocks for namespace storage and query topologies.
   Manages prefix -> IRI mappings via a separate microbatch topology.
   Use these functions from within a defmodule body."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]))

(defn namespace-depot-partition-key
  "Partition key for namespace depot operations.
   Hash by prefix for set/remove operations to ensure same prefix goes to same task.
   Clear operations use a constant key — hash-by silently DROPS records whose
   extracted key is nil, so the key must never be nil. The record lands on one
   deterministic task and the topology broadcasts it to all partitions with |all."
  [[op & args]]
  (case op
    :set-ns (first args)      ;; hash by prefix
    :remove-ns (first args)   ;; hash by prefix
    :clear-ns "clear-ns"))

(defn setup-namespace-topology
  "Sets up the namespace storage microbatch topology with its PState and ETL sources.
   Call from within a defmodule body."
  [topologies]
  (let [ns-mb (microbatch-topology topologies "ns-indexer")]
    ;; $$namespaces: prefix -> IRI mapping
    (declare-pstate ns-mb $$namespaces {String String})

    (<<sources ns-mb
               (source> *namespace-depot :> %ns-batch)
               (%ns-batch :> [*op & *args])
               (<<cond
                (case> (= *op :set-ns))
                ;; [:set-ns prefix iri] - set or update a namespace
                (identity *args :> [*prefix *iri])
                (local-transform> [(keypath *prefix) (termval *iri)] $$namespaces)

                (case> (= *op :remove-ns))
                ;; [:remove-ns prefix] - remove a namespace
                (identity *args :> [*prefix])
                (local-transform> [(keypath *prefix) NONE>] $$namespaces)

                (case> (= *op :clear-ns))
                ;; [:clear-ns] - clear all namespaces.
                ;; The depot routes this record to one task (constant partition
                ;; key — it must never be nil), so broadcast before wiping:
                ;; every task holds a partition of $$namespaces.
                ;; NOTE: do NOT use local-clear> here — it kills the worker
                ;; when used inside a microbatch topology (verified empirically;
                ;; see test history for details).
                (|all)
                (local-transform> [MAP-VALS NONE>] $$namespaces)))))

(defn register-namespace-query-topologies
  "Registers query topologies for namespace lookups.
   Call from within a defmodule body."
  [topologies]
  ;; Get a single namespace by prefix. The depot partitions :set-ns/:remove-ns
  ;; by prefix, so the owning task is deterministic — one targeted lookup
  ;; instead of a broadcast to every task.
  (<<query-topology topologies "get-namespace" [*prefix :> *iri]
                    (|hash *prefix)
                    (local-select> [(keypath *prefix) (view identity)] $$namespaces :> *iri)
                    (|origin))

  ;; List all namespaces as a map of prefix -> IRI
  (<<query-topology topologies "list-namespaces" [:> *namespaces]
                    (|all)
                    (local-select> [ALL] $$namespaces :> *entry)
                    (|origin)
                    (aggs/+map-agg (first *entry) (second *entry) :> *namespaces)))
