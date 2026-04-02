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
   Returns nil for clear operations (handled specially in SAIL layer)."
  [[op & args]]
  (case op
    :set-ns (first args)      ;; hash by prefix
    :remove-ns (first args)   ;; hash by prefix
    :clear-ns nil))

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
                ;; [:clear-ns] - clear all namespaces
                (local-transform> [MAP-VALS NONE>] $$namespaces)))))

(defn register-namespace-query-topologies
  "Registers query topologies for namespace lookups.
   Call from within a defmodule body."
  [topologies]
  ;; Get a single namespace by prefix
  (<<query-topology topologies "get-namespace" [*prefix :> *iri]
                    (|all)
                    (local-select> [(keypath *prefix) (view identity)] $$namespaces :> *val)
                    (filter> (some? *val))
                    (|origin)
                    ;; Collect all non-nil values (should be at most one)
                    (aggs/+vec-agg *val :> *vals)
                    (first *vals :> *iri))

  ;; List all namespaces as a map of prefix -> IRI
  (<<query-topology topologies "list-namespaces" [:> *namespaces]
                    (|all)
                    (local-select> [ALL] $$namespaces :> *entry)
                    (|origin)
                    (aggs/+map-agg (first *entry) (second *entry) :> *namespaces)))
