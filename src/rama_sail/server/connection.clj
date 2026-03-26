(ns rama-sail.server.connection
  "Rama connection lifecycle for the SPARQL server.

   Manages depot handles, SAIL/Repository, and RepositoryConnection.
   Supports IPC mode (dev/test) and cluster mode (production)."
  (:require [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as sail])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]))

(def module-name (rama/get-module-name RdfStorageModule))

(defn start-ipc!
  "Start an in-process Rama cluster with the RDF storage module.
   Returns a connection state map:
     {:ipc ipc, :depot depot, :repo repo, :conn conn}"
  ([] (start-ipc! {}))
  ([{:keys [tasks threads] :or {tasks 4 threads 2}}]
   (let [ipc (rtest/create-ipc)]
     (try
       (rtest/launch-module! ipc RdfStorageModule {:tasks tasks :threads threads})
       (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
             find-triples-qt (rama/foreign-query ipc module-name "find-triples")
             rsail (sail/create-rama-sail ipc module-name)
             repo  (SailRepository. rsail)]
         (.init repo)
         (let [conn (.getConnection repo)]
           {:ipc ipc :depot depot :repo repo :conn conn
            :find-triples-qt find-triples-qt}))
       (catch Exception e
         (.close ipc)
         (throw e))))))

(defn start-cluster!
  "Connect to a pre-deployed Rama cluster.
   Returns a connection state map (same shape as start-ipc! minus :ipc):
     {:manager manager, :depot depot, :repo repo, :conn conn, ...}"
  ([] (start-cluster! {}))
  ([{:keys [host port] :or {host "localhost" port 1973}}]
   (let [manager (rama/open-cluster-manager {"conductor.host" host
                                             "conductor.port" port})]
     (try
       (let [depot (rama/foreign-depot manager module-name "*triple-depot")
             find-triples-qt (rama/foreign-query manager module-name "find-triples")
             rsail (sail/create-rama-sail manager module-name)
             repo  (SailRepository. rsail)]
         (.init repo)
         (let [conn (.getConnection repo)]
           {:manager manager :depot depot :repo repo :conn conn
            :find-triples-qt find-triples-qt}))
       (catch Exception e
         (.close manager)
         (throw e))))))

(defn stop!
  "Shut down all resources in a connection state map."
  [{:keys [conn repo ipc manager]}]
  (when conn
    (try (.close ^java.io.Closeable conn) (catch Exception _)))
  (when repo
    (try (.shutDown ^SailRepository repo) (catch Exception _)))
  (when ipc
    (try (.close ^java.io.Closeable ipc) (catch Exception _)))
  (when manager
    (try (.close ^java.io.Closeable manager) (catch Exception _))))
