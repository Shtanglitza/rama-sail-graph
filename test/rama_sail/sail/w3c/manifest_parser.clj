(ns rama-sail.sail.w3c.manifest-parser
  "Parses W3C SPARQL test manifest.ttl files into Clojure data structures.
   Uses RDF4J Rio to parse the Turtle manifest and extract test entries."
  (:require [clojure.java.io :as io])
  (:import [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.rio.turtlestar TurtleStarParser]
           [org.eclipse.rdf4j.rio.helpers StatementCollector]
           [org.eclipse.rdf4j.model.util Models Values]
           [org.eclipse.rdf4j.model.vocabulary RDF]
           [org.eclipse.rdf4j.model IRI Value Resource]
           [org.eclipse.rdf4j.model.impl LinkedHashModel]))

(def ^:private MF-NS "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
(def ^:private QT-NS "http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
(def ^:private UT-NS "http://www.w3.org/2009/sparql/tests/test-update#")

(def ^:private NO-CTX (into-array Resource []))

(defn- iri [ns local]
  (Values/iri ns local))

(def ^:private mf-entries (iri MF-NS "entries"))
(def ^:private mf-name (iri MF-NS "name"))
(def ^:private mf-action (iri MF-NS "action"))
(def ^:private mf-result (iri MF-NS "result"))
(def ^:private mf-QueryEvaluationTest (iri MF-NS "QueryEvaluationTest"))
(def ^:private mf-UpdateEvaluationTest (iri MF-NS "UpdateEvaluationTest"))
(def ^:private qt-query (iri QT-NS "query"))
(def ^:private qt-data (iri QT-NS "data"))
(def ^:private qt-graphData (iri QT-NS "graphData"))
(def ^:private ut-request (iri UT-NS "request"))
(def ^:private ut-data (iri UT-NS "data"))

(defn- get-object
  "Get the single object for (subject, predicate) in model, or nil."
  ^Value [^org.eclipse.rdf4j.model.Model model ^Resource subject ^IRI predicate]
  (.orElse (Models/object (.filter model subject predicate nil NO-CTX)) nil))

(defn- get-objects
  "Get all objects for (subject, predicate) in model as a set."
  [^org.eclipse.rdf4j.model.Model model ^Resource subject ^IRI predicate]
  (Models/objectResources (.filter model subject predicate nil NO-CTX)))

(defn- value->str [^Value v]
  (when v (.stringValue v)))

(defn- iri->filename
  "Extract filename from a Value by taking the last path segment."
  [^Value v]
  (when v
    (let [s (.stringValue v)
          idx (.lastIndexOf s (int \/))]
      (if (pos? idx)
        (subs s (inc idx))
        s))))

(defn- collect-rdf-list
  "Walk an RDF list (rdf:first/rdf:rest) and collect elements."
  [model head]
  (loop [node head
         acc []]
    (if (or (nil? node) (= node RDF/NIL))
      acc
      (let [first-val (get-object model node RDF/FIRST)
            rest-val (get-object model node RDF/REST)]
        (recur rest-val (if first-val (conj acc first-val) acc))))))

(defn- parse-query-test
  [model subject]
  (let [action (get-object model subject mf-action)
        result (get-object model subject mf-result)
        ;; Collect graph data files (for named graph tests)
        graph-data-objs (when action
                          (seq (get-objects model action qt-graphData)))]
    (cond-> {:iri (value->str subject)
             :name (value->str (get-object model subject mf-name))
             :type :query-evaluation
             :query-file (when action (iri->filename (get-object model action qt-query)))
             :data-file (when action (iri->filename (get-object model action qt-data)))
             :result-file (iri->filename result)}
      graph-data-objs (assoc :graph-data-files (mapv iri->filename graph-data-objs)))))

(defn- parse-update-test
  [model subject]
  (let [action (get-object model subject mf-action)
        result (get-object model subject mf-result)]
    {:iri (value->str subject)
     :name (value->str (get-object model subject mf-name))
     :type :update-evaluation
     :update-file (when action (iri->filename (get-object model action ut-request)))
     :data-file (when action (iri->filename (get-object model action ut-data)))
     :result-file (when result (iri->filename (get-object model result ut-data)))}))

(defn parse-manifest
  "Parse a W3C test manifest file and return a sequence of test descriptors.

   resource-path should be a classpath-relative path like
   \"w3c/sparql12/eval-triple-terms/manifest.ttl\""
  [resource-path]
  (let [url (io/resource resource-path)
        _ (when-not url (throw (ex-info "Manifest not found on classpath" {:path resource-path})))
        base-uri (.toString url)
        ^org.eclipse.rdf4j.model.Model model
        (let [parser (TurtleStarParser.)
              collector (StatementCollector.)]
          (.setRDFHandler parser collector)
          (with-open [in (.openStream url)]
            (.parse parser in base-uri))
          (LinkedHashModel. (.getStatements collector)))]
    ;; Find the manifest subject (any subject with mf:entries predicate)
    (let [manifest-subj (first (for [stmt (iterator-seq (.iterator model))
                                     :when (= (.getPredicate stmt) mf-entries)]
                                 (.getSubject stmt)))]
      (when-not manifest-subj
        (throw (ex-info "No mf:entries found in manifest"
                        {:path resource-path
                         :model-size (.size model)
                         :predicates (set (map #(.stringValue (.getPredicate %)) (iterator-seq (.iterator model))))})))
      ;; Get ordered list of test IRIs from RDF list
      (let [entries-head (get-object model manifest-subj mf-entries)
            test-iris (collect-rdf-list model entries-head)]
        (vec (for [test-iri test-iris
                   :let [types (get-objects model test-iri RDF/TYPE)]]
               (cond
                 (.contains types mf-QueryEvaluationTest)
                 (parse-query-test model test-iri)

                 (.contains types mf-UpdateEvaluationTest)
                 (parse-update-test model test-iri)

                 :else
                 {:iri (value->str test-iri)
                  :name "unknown"
                  :type :unknown})))))))
