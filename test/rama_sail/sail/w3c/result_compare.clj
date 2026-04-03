(ns rama-sail.sail.w3c.result-compare
  "Utilities for loading test data, executing queries, and comparing results
   against W3C expected outputs using RDF4J's built-in comparison logic."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.rio.helpers StatementCollector]
           [org.eclipse.rdf4j.rio.turtlestar TurtleStarParser]
           [org.eclipse.rdf4j.rio.trigstar TriGStarParser]
           [org.eclipse.rdf4j.model Resource]
           [org.eclipse.rdf4j.model.impl LinkedHashModel]
           [org.eclipse.rdf4j.model.util Models]
           [org.eclipse.rdf4j.query QueryResults]
           [org.eclipse.rdf4j.query.impl MutableTupleQueryResult]
           [org.eclipse.rdf4j.query.resultio.helpers QueryResultCollector]
           [org.eclipse.rdf4j.query.resultio.sparqljson SPARQLStarResultsJSONParser]
           [org.eclipse.rdf4j.query.resultio.sparqlxml SPARQLResultsXMLParser]))

(defn- detect-rdf-format
  "Detect RDF format from file extension."
  [filename]
  (cond
    (str/ends-with? filename ".ttl") RDFFormat/TURTLESTAR
    (str/ends-with? filename ".trig") RDFFormat/TRIGSTAR
    (str/ends-with? filename ".nq") RDFFormat/NQUADS
    (str/ends-with? filename ".nt") RDFFormat/NTRIPLES
    :else (throw (ex-info "Unknown RDF format" {:file filename}))))

(defn load-data!
  "Load RDF data from a classpath resource into an RDF4J connection.
   Handles Turtle, TriG, N-Quads, and N-Triples formats.
   Skips empty files (like empty.nq)."
  [conn resource-path]
  (let [url (io/resource resource-path)]
    (when-not url (throw (ex-info "Data file not found" {:path resource-path})))
    (let [content (slurp url)]
      (when-not (str/blank? content)
        (let [format (detect-rdf-format resource-path)
              base-uri (.toString url)]
          (.begin conn)
          (with-open [in (.openStream url)]
            (.add conn in base-uri format (into-array Resource [])))
          (.commit conn))))))

(defn- parse-expected-graph
  "Parse an expected graph result (.ttl or .trig) into a Model.
   Uses Star parsers to handle << >> triple term syntax."
  [resource-path]
  (let [url (io/resource resource-path)
        collector (StatementCollector.)
        parser (cond
                 (str/ends-with? resource-path ".trig") (TriGStarParser.)
                 (str/ends-with? resource-path ".ttl") (TurtleStarParser.)
                 :else (Rio/createParser (detect-rdf-format resource-path)))]
    (.setRDFHandler parser collector)
    (with-open [in (.openStream url)]
      (.parse parser in (.toString url)))
    (LinkedHashModel. (.getStatements collector))))

(defn- get-actual-model
  "Get all statements from a connection as a Model."
  [conn]
  (let [^LinkedHashModel model (LinkedHashModel.)
        stmts (with-open [iter (.getStatements conn nil nil nil false (into-array Resource []))]
                (vec (iterator-seq iter)))]
    (.addAll model stmts)
    model))

(defn- parse-expected-select-results
  "Parse expected SPARQL results (.srj or .srx) into a MutableTupleQueryResult."
  [resource-path]
  (let [url (io/resource resource-path)
        collector (QueryResultCollector.)
        parser (if (str/ends-with? resource-path ".srx")
                 (SPARQLResultsXMLParser.)
                 (SPARQLStarResultsJSONParser.))]
    (.setQueryResultHandler parser collector)
    (with-open [in (.openStream url)]
      (.parseQueryResult parser in))
    (MutableTupleQueryResult. (.getBindingNames collector) (.getBindingSets collector))))

(defn run-select-test
  "Run a SELECT query test. Returns {:pass? bool :message str}.
   Compares actual results to expected .srj/.srx file using QueryResults/equals."
  [conn query-string expected-resource-path]
  (try
    (let [actual-result (.evaluate (.prepareTupleQuery conn query-string))
          ;; Collect actual results for debug before consuming
          actual-list (vec (iterator-seq actual-result))
          actual-mutable (MutableTupleQueryResult. (.getBindingNames actual-result) actual-list)
          expected-mutable (parse-expected-select-results expected-resource-path)
          ;; Collect expected for debug
          expected-list (vec (iterator-seq expected-mutable))
          expected-mutable2 (MutableTupleQueryResult.
                             (.getBindingNames expected-mutable) expected-list)
          equal? (QueryResults/equals actual-mutable expected-mutable2)]
      {:pass? equal?
       :message (when-not equal?
                  (str "SELECT results do not match.\n"
                       "  Actual (" (count actual-list) " rows): "
                       (pr-str (mapv str (take 5 actual-list))) "\n"
                       "  Expected (" (count expected-list) " rows): "
                       (pr-str (mapv str (take 5 expected-list)))))})
    (catch Exception e
      {:pass? false
       :message (str "Exception: " (.getMessage e))
       :error e})))

(defn run-construct-test
  "Run a CONSTRUCT query test. Returns {:pass? bool :message str}.
   Compares actual graph to expected .ttl file using Models/isomorphic."
  [conn query-string expected-resource-path]
  (try
    (let [actual-model (LinkedHashModel.)
          _ (with-open [iter (.evaluate (.prepareGraphQuery conn query-string))]
              (doseq [^org.eclipse.rdf4j.model.Statement stmt (iterator-seq iter)]
                (.add actual-model ^org.eclipse.rdf4j.model.Statement stmt (into-array Resource []))))
          expected-model (parse-expected-graph expected-resource-path)
          iso? (Models/isomorphic actual-model expected-model)]
      {:pass? iso?
       :message (when-not iso?
                  (str "CONSTRUCT graphs not isomorphic. "
                       "Actual: " (.size actual-model) " stmts, "
                       "Expected: " (.size expected-model) " stmts"))})
    (catch Exception e
      {:pass? false
       :message (str "Exception: " (.getMessage e))
       :error e})))

(defn run-update-test
  "Run an UPDATE test. Returns {:pass? bool :message str}.
   Executes the update, then compares the resulting graph to expected .trig."
  [conn update-string expected-resource-path]
  (try
    (.begin conn)
    (.execute (.prepareUpdate conn update-string))
    (.commit conn)
    (let [actual-model (get-actual-model conn)
          expected-model (parse-expected-graph expected-resource-path)
          iso? (Models/isomorphic actual-model expected-model)]
      {:pass? iso?
       :message (when-not iso?
                  (str "UPDATE result graphs not isomorphic. "
                       "Actual: " (.size actual-model) " stmts, "
                       "Expected: " (.size expected-model) " stmts"))})
    (catch Exception e
      {:pass? false
       :message (str "Exception: " (.getMessage e))
       :error e})))

(defn- parse-expected-boolean
  "Parse expected ASK result (.srx) to extract the boolean value."
  [resource-path]
  (let [url (io/resource resource-path)
        content (slurp url)]
    (cond
      (str/includes? content "<boolean>true</boolean>") true
      (str/includes? content "<boolean>false</boolean>") false
      :else (throw (ex-info "Cannot parse boolean result" {:path resource-path})))))

(defn run-ask-test
  "Run an ASK query test. Returns {:pass? bool :message str}.
   Compares actual boolean result to expected .srx file."
  [conn query-string expected-resource-path]
  (try
    (let [actual (.evaluate (.prepareBooleanQuery conn query-string))
          expected (parse-expected-boolean expected-resource-path)]
      {:pass? (= actual expected)
       :message (when (not= actual expected)
                  (str "ASK result mismatch. Actual: " actual ", Expected: " expected))})
    (catch Exception e
      {:pass? false
       :message (str "Exception: " (.getMessage e))
       :error e})))

(defn detect-query-type
  "Detect whether a SPARQL query is SELECT, CONSTRUCT, or ASK
   by looking at the query string."
  [query-string]
  (let [normalized (-> query-string
                       (str/replace #"#[^\n]*" "")  ; strip comments
                       str/upper-case
                       str/trim)]
    (cond
      (re-find #"\bCONSTRUCT\b" normalized) :construct
      (re-find #"\bASK\b" normalized) :ask
      :else :select)))
